package api

// Tests for the review-remediation fixes in internal/api:
//   - T2/B2: nil-map guard in the rotate-password Secret update path.
//   - T3/C4: generic 500 body (no raw K8s error leak) on rotate-password read
//     failure.
//   - T9/B6: backup.enabled gate on delete-backup and restore endpoints
//     (D-B6), with NO RecordRestore("failed") on the validation reject.
//   - T10/B7: decodeOptionalJSON rejects trailing JSON garbage (D-B7a).
//   - T15/E3: rate-limiter EntriesLen + cloudberry_api_rate_limit_entries.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// adminRotateRequest builds an authenticated rotate-password request.
func adminRotateRequest() *http.Request {
	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	return req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
}

// TestHandleRotatePassword_NilSecretData covers T2/B2: a Secret persisted
// with Data == nil must not panic the handler; the rotated password lands in
// the (re-initialized) data map.
func TestHandleRotatePassword_NilSecretData(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "old-password", auth.PermissionAdmin)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.OperatorAdminPasswordSecretName,
			Namespace: util.OperatorNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: nil, // the panic trigger before the fix
	}
	s := newTestServerWithCredStore(credStore, secret)

	rec := httptest.NewRecorder()
	require.NotPanics(t, func() { s.handleRotatePassword(rec, adminRotateRequest()) })
	require.Equal(t, http.StatusOK, rec.Code)

	updated := &corev1.Secret{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(secret), updated))
	assert.NotEmpty(t, updated.Data[util.PasswordSecretKey],
		"the new password must be persisted under the password key")
}

// TestHandleRotatePassword_TransientGetError_GenericBody covers T3/C4: a
// transient Get failure returns 500 with a GENERIC message; the underlying
// K8s error text must never reach the client (it stays in logs only).
func TestHandleRotatePassword_TransientGetError_GenericBody(t *testing.T) {
	const secretErrText = "etcdserver leader changed"
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "old-password", auth.PermissionAdmin)

	scheme := newTestScheme()
	k8sClient := ctrlfake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch,
				_ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("%s", secretErrText)
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0, credStore))

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, adminRotateRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	envelope := decodeErrorEnvelope(t, rec)
	assert.Equal(t, errCodeInternal, envelope.Error.Code)
	assert.Equal(t, "failed to read admin password secret", envelope.Error.Message)
	assert.NotContains(t, envelope.Error.Message, secretErrText,
		"raw K8s error text must not leak to the client")
}

// restoreMetricsRecorder tracks RecordRestore calls (honest-metrics check).
type restoreMetricsRecorder struct {
	metrics.NoopRecorder
	mu       sync.Mutex
	statuses []string
}

func (r *restoreMetricsRecorder) RecordRestore(_, _ string, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses = append(r.statuses, status)
}

func (r *restoreMetricsRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.statuses))
	copy(out, r.statuses)
	return out
}

// backupDisabledClusters enumerates the two gated shapes: Spec.Backup == nil
// and Enabled == false.
func backupDisabledClusters() map[string]*cbv1alpha1.CloudberryCluster {
	nilBackup := newTestCluster("test-cluster", "default")

	disabled := newTestCluster("test-cluster", "default")
	disabled.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: false}

	return map[string]*cbv1alpha1.CloudberryCluster{
		"backup spec nil":      nilBackup,
		"backup spec disabled": disabled,
	}
}

// jobCount lists Jobs in the cluster namespace via the server's fake client.
func jobCount(t *testing.T, s *Server) int {
	t.Helper()
	jobs := &batchv1.JobList{}
	require.NoError(t, s.k8sClient.List(context.Background(), jobs,
		client.InNamespace("default")))
	return len(jobs.Items)
}

// TestHandleDeleteBackup_BackupDisabledGate covers T9/B6 for DELETE
// /backups/{ts}: gated clusters get 400 BACKUP_NOT_ENABLED and NO cleanup Job.
func TestHandleDeleteBackup_BackupDisabledGate(t *testing.T) {
	for name, cluster := range backupDisabledClusters() {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(cluster)

			req := httptest.NewRequest(http.MethodDelete,
				apiPrefix+"/clusters/test-cluster/backups/20260101010101?namespace=default", nil)
			req.SetPathValue("name", "test-cluster")
			req.SetPathValue("timestamp", "20260101010101")
			rec := httptest.NewRecorder()
			s.handleDeleteBackup(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, errCodeBackupNotEnabled, decodeErrorEnvelope(t, rec).Error.Code)
			assert.Zero(t, jobCount(t, s), "no cleanup Job may be created when gated")
		})
	}
}

// TestHandleRestoreBackup_BackupDisabledGate covers T9/B6 for POST restore:
// gated clusters get 400 BACKUP_NOT_ENABLED, NO restore Job, and NO
// RecordRestore("failed") increment (validation reject != restore outcome).
func TestHandleRestoreBackup_BackupDisabledGate(t *testing.T) {
	for name, cluster := range backupDisabledClusters() {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme()
			k8sClient := ctrlfake.NewClientBuilder().
				WithScheme(scheme).WithObjects(cluster).Build()
			rec := &restoreMetricsRecorder{}
			s := trackServer(NewServer(k8sClient, nil, nil, rec, nil, 0))

			w := httptest.NewRecorder()
			s.handleRestoreBackup(w, postRestoreRequest(t, "test-cluster", "20260101010101", `{}`))

			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, errCodeBackupNotEnabled, decodeErrorEnvelope(t, w).Error.Code)
			assert.Zero(t, jobCount(t, s), "no restore Job may be created when gated")
			assert.Empty(t, rec.recorded(),
				"RecordRestore must NOT fire on the validation reject (honest metrics)")
		})
	}
}

// TestBackupGate_EnabledStillAccepted pins that enabled clusters keep the 202
// behavior on both gated endpoints.
func TestBackupGate_EnabledStillAccepted(t *testing.T) {
	t.Run("delete backup", func(t *testing.T) {
		s := newTestServer(newBackupEnabledCluster())
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/backups/20260101010101?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("timestamp", "20260101010101")
		rec := httptest.NewRecorder()
		s.handleDeleteBackup(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})

	t.Run("restore backup", func(t *testing.T) {
		s := newTestServer(newBackupEnabledCluster())
		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec,
			postRestoreRequest(t, "test-cluster", "20260101010101", `{}`))
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})
}

// TestDecodeOptionalJSON covers T10/B7a directly on the helper.
func TestDecodeOptionalJSON(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
		wantTyp string
	}{
		{name: "empty body is a valid empty request", body: "", wantErr: false},
		// Whitespace-only and literal-null bodies take the EOF/no-op decode
		// path today and are treated as empty/valid requests — pinned so a
		// future decoder change cannot silently flip the contract (T-9).
		{name: "whitespace-only body treated as empty", body: "   \n\t ", wantErr: false},
		{name: "null body decodes as a no-op", body: "null", wantErr: false},
		{name: "single valid object decodes", body: `{"type":"full"}`, wantErr: false, wantTyp: "full"},
		{name: "trailing object rejected", body: `{} {}`, wantErr: true},
		{name: "trailing array rejected", body: `{}[]`, wantErr: true},
		{name: "trailing garbage rejected", body: `{}garbage`, wantErr: true},
		{name: "malformed JSON rejected", body: `{"type":`, wantErr: true},
		{name: "trailing whitespace accepted", body: `{"type":"full"}` + "\n  ", wantErr: false, wantTyp: "full"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			var v CreateBackupRequest
			err := decodeOptionalJSON(req, &v)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTyp, v.Type)
		})
	}
}

// TestHandleCreateBackup_TrailingGarbage400 covers T10 at the handler level:
// trailing JSON garbage propagates as 400 INVALID_REQUEST.
func TestHandleCreateBackup_TrailingGarbage400(t *testing.T) {
	s := newTestServer(newBackupEnabledCluster())
	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster",
		`{"databases":["mydb"]} {"junk":1}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, errCodeInvalidRequest, decodeErrorEnvelope(t, rec).Error.Code)
}

// TestRateLimiter_EntriesLen covers T15/E3: EntriesLen tracks distinct
// client entries and drops after cleanup of stale entries.
func TestRateLimiter_EntriesLen(t *testing.T) {
	rl := NewRateLimiter(5, time.Minute, nil)
	defer rl.Stop()

	assert.Zero(t, rl.EntriesLen())
	for i := 0; i < 3; i++ {
		rl.Allow(fmt.Sprintf("10.0.0.%d", i))
	}
	assert.Equal(t, 3, rl.EntriesLen())

	// Age one entry beyond 2*interval so cleanup() collects it.
	rl.mu.Lock()
	rl.entries["10.0.0.0"].lastRefill = time.Now().Add(-3 * time.Minute)
	rl.mu.Unlock()
	rl.cleanup()
	assert.Equal(t, 2, rl.EntriesLen())
}

// gaugeValue gathers a single label-less gauge value from the registry.
func gaugeValue(t *testing.T, reg *prometheus.Registry, family string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		require.NotEmpty(t, mf.GetMetric())
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge family %s not found", family)
	return 0
}

// TestRateLimitEntriesGauge covers the scrape-time gauge wiring: live
// servers' limiter entries are summed; a closed server's provider is
// unregistered so its entries stop counting.
func TestRateLimitEntriesGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	newSrv := func() *Server {
		return trackServer(NewServer(
			ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(),
			nil, nil, recorder, nil, 0))
	}

	s1 := newSrv()
	s2 := newSrv()

	assert.Zero(t, gaugeValue(t, reg, "cloudberry_api_rate_limit_entries"))

	s1.rateLimiter.Allow("10.1.0.1")
	s1.rateLimiter.Allow("10.1.0.2")
	s2.rateLimiter.Allow("10.2.0.1")
	assert.InDelta(t, 3.0, gaugeValue(t, reg, "cloudberry_api_rate_limit_entries"), 0.0001,
		"entries from both live servers must be summed")

	// Closing s1 unregisters its provider; only s2's entry remains.
	s1.Close()
	assert.InDelta(t, 1.0, gaugeValue(t, reg, "cloudberry_api_rate_limit_entries"), 0.0001)
}
