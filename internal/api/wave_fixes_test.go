package api

// Tests for the code-review remediation wave:
//   A-3: statusRecorder Flusher/Unwrap passthrough + ResponseController-based
//        copyLogStream.
//   A-4: streaming endpoints clear the per-connection write deadline.
//   B-4: handleRotatePassword distinguishes NotFound from transient errors.
//   B-5: conflict-safe mutating handlers (MergeFrom patch / RetryOnConflict).
//   B-10: shared cancelBackendByPID helper (behavior-preserving).

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ----------------------------------------------------------------------------
// A-3: statusRecorder Flush / Unwrap
// ----------------------------------------------------------------------------

func TestStatusRecorder_FlushDelegates(t *testing.T) {
	inner := &flushRecorder{}
	rec := &statusRecorder{ResponseWriter: inner}

	rec.Flush()
	rec.Flush()

	assert.Equal(t, 2, inner.flushes, "Flush must be delegated to the wrapped writer")
}

func TestStatusRecorder_FlushNonFlusherNoPanic(t *testing.T) {
	inner := &plainWriter{}
	rec := &statusRecorder{ResponseWriter: inner}

	assert.NotPanics(t, func() { rec.Flush() })
}

func TestStatusRecorder_UnwrapReturnsInner(t *testing.T) {
	inner := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: inner}

	assert.Same(t, http.ResponseWriter(inner), rec.Unwrap())
}

// TestCopyLogStream_ThroughStatusRecorder verifies the H-2 fix end to end:
// flushing reaches the real writer THROUGH the tracing middleware's
// statusRecorder wrapper via http.NewResponseController + Unwrap.
func TestCopyLogStream_ThroughStatusRecorder(t *testing.T) {
	inner := &flushRecorder{}
	wrapped := &statusRecorder{ResponseWriter: inner}

	src := bytes.NewBufferString("first chunk\n")
	copyLogStream(context.Background(), wrapped, src, true)

	assert.Equal(t, "first chunk\n", inner.buf.String())
	assert.GreaterOrEqual(t, inner.flushes, 1,
		"flush must propagate through statusRecorder to the underlying writer")
}

// TestCopyLogStream_FirstChunkBeforeClose asserts the follow contract: the
// first chunk is flushed to the client BEFORE the source stream closes.
func TestCopyLogStream_FirstChunkBeforeClose(t *testing.T) {
	inner := &flushRecorder{}
	wrapped := &statusRecorder{ResponseWriter: inner}

	firstChunkSeen := make(chan struct{})
	src := &signalingReader{
		chunks: [][]byte{[]byte("early chunk\n"), []byte("late chunk\n")},
		onSecondRead: func() {
			// Before serving the second chunk (i.e. before the stream is
			// done), the first chunk must already be written and flushed.
			if strings.Contains(inner.buf.String(), "early chunk") && inner.flushes >= 1 {
				close(firstChunkSeen)
			}
		},
		delay: jobLogsFlushInterval + 50*time.Millisecond,
	}

	copyLogStream(context.Background(), wrapped, src, true)

	select {
	case <-firstChunkSeen:
		// First chunk delivered before stream end — follow mode works.
	default:
		t.Fatal("first chunk was not flushed before the source stream closed")
	}
	assert.Equal(t, "early chunk\nlate chunk\n", inner.buf.String())
}

// signalingReader emits chunks with a delay before the second read and invokes
// onSecondRead before serving it so tests can inspect mid-stream state.
type signalingReader struct {
	chunks       [][]byte
	idx          int
	delay        time.Duration
	onSecondRead func()
}

func (s *signalingReader) Read(p []byte) (int, error) {
	if s.idx >= len(s.chunks) {
		return 0, context.Canceled // any non-nil error terminates the copy
	}
	if s.idx == 1 {
		time.Sleep(s.delay)
		if s.onSecondRead != nil {
			s.onSecondRead()
		}
	}
	n := copy(p, s.chunks[s.idx])
	s.idx++
	return n, nil
}

// ----------------------------------------------------------------------------
// A-4: write-deadline clearing for streaming routes
// ----------------------------------------------------------------------------

// deadlineRecorder is an http.ResponseWriter that records SetWriteDeadline
// calls (the controller reaches it via the ResponseController contract).
type deadlineRecorder struct {
	httptest.ResponseRecorder
	deadlines []time.Time
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.deadlines = append(d.deadlines, t)
	return nil
}

func TestClearWriteDeadline_SetsZeroDeadline(t *testing.T) {
	s := newTestServer()
	w := &deadlineRecorder{}

	s.clearWriteDeadline(w)

	require.Len(t, w.deadlines, 1)
	assert.True(t, w.deadlines[0].IsZero(), "deadline must be cleared with time.Time{}")
}

func TestClearWriteDeadline_ThroughStatusRecorder(t *testing.T) {
	s := newTestServer()
	inner := &deadlineRecorder{}
	wrapped := &statusRecorder{ResponseWriter: inner}

	s.clearWriteDeadline(wrapped)

	require.Len(t, inner.deadlines, 1)
	assert.True(t, inner.deadlines[0].IsZero())
}

func TestClearWriteDeadline_UnsupportedWriterNoPanic(t *testing.T) {
	s := newTestServer()
	// httptest.ResponseRecorder does not support write deadlines; the helper
	// must degrade gracefully (debug log, no panic).
	assert.NotPanics(t, func() { s.clearWriteDeadline(httptest.NewRecorder()) })
}

// The former synthetic TestStreamingExemption_FollowStreamSurvivesWriteTimeout
// (an inline handler mirroring the production streaming logic) was REPLACED by
// TestFollowMode_FullMiddlewareChain_SurvivesWriteTimeout in
// followmode_fullchain_test.go, which drives the REAL Handler() middleware
// chain and the real handleBackupJobLogs/clearWriteDeadline code path with
// wider real-clock margins (E-3 / H-2 / H-3).

// ----------------------------------------------------------------------------
// B-4: handleRotatePassword Get-error discrimination
// ----------------------------------------------------------------------------

func rotatePasswordRequest() *http.Request {
	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	return req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
}

func TestHandleRotatePassword_TransientGetError_NoCreateAttempted(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	createAttempted := false

	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return apierrors.NewInternalError(errBoom)
			},
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.CreateOption) error {
				createAttempted = true
				return nil
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0, credStore))

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, rotatePasswordRequest())

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.False(t, createAttempted, "transient Get error must NOT trigger a Create")
	// The body must surface the real cause instead of masking it.
	assert.Contains(t, rec.Body.String(), "failed to read admin password secret")
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

func TestHandleRotatePassword_NotFound_CreatesSecret(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	s := newTestServerWithCredStore(credStore)

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, rotatePasswordRequest())

	require.Equal(t, http.StatusOK, rec.Code)

	secret := &corev1.Secret{}
	err := s.k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.OperatorAdminPasswordSecretName,
		Namespace: util.OperatorNamespace,
	}, secret)
	require.NoError(t, err)
	assert.NotEmpty(t, secret.Data[util.PasswordSecretKey])
}

func TestHandleRotatePassword_AlreadyExistsRace_Handled(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()

	// Pre-seed the secret in the store, but make the FIRST Get report
	// NotFound to simulate the race: another replica creates the secret
	// between our Get and our Create.
	secret := &corev1.Secret{}
	secret.Name = util.OperatorAdminPasswordSecretName
	secret.Namespace = util.OperatorNamespace
	secret.Data = map[string][]byte{util.PasswordSecretKey: []byte("old")}

	firstGet := true
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(secret).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if firstGet {
					firstGet = false
					return apierrors.NewNotFound(
						schema.GroupResource{Resource: "secrets"}, key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0, credStore))

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, rotatePasswordRequest())

	assert.Equal(t, http.StatusOK, rec.Code,
		"AlreadyExists race on Create must be resolved by updating the racing secret")

	updated := &corev1.Secret{}
	require.NoError(t, s.k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.OperatorAdminPasswordSecretName,
		Namespace: util.OperatorNamespace,
	}, updated))
	assert.NotEqual(t, "old", string(updated.Data[util.PasswordSecretKey]),
		"racing secret must be updated with the newly generated password")
}

// ----------------------------------------------------------------------------
// B-5: conflict-safe mutating handlers
// ----------------------------------------------------------------------------

// conflictOnceInterceptor returns interceptor funcs that fail the first
// Update with a 409 Conflict and pass subsequent ones through.
func conflictOnceInterceptor(conflicts *int) interceptor.Funcs {
	return interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			if *conflicts > 0 {
				*conflicts--
				return apierrors.NewConflict(
					schema.GroupResource{Resource: "cloudberryclusters"},
					obj.GetName(), errBoom)
			}
			return c.Update(ctx, obj, opts...)
		},
	}
}

func newConflictTestServer(conflicts *int, cluster *cbv1alpha1.CloudberryCluster) *Server {
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(conflictOnceInterceptor(conflicts)).
		Build()
	return trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))
}

func TestHandleUpdateConfig_RetriesOnConflict(t *testing.T) {
	conflicts := 1
	cluster := newTestCluster("test-cluster", "default")
	s := newConflictTestServer(&conflicts, cluster)

	body := bytes.NewBufferString(`{"parameters":{"max_connections":"200"}}`)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/config?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "a single conflict must be retried, not surfaced")
	assert.Equal(t, 0, conflicts)
}

func TestHandleCreateWorkloadRule_RetriesOnConflict(t *testing.T) {
	conflicts := 1
	cluster := newTestCluster("test-cluster", "default")
	s := newConflictTestServer(&conflicts, cluster)

	body := bytes.NewBufferString(`{"name":"rule1","action":"cancel"}`)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleCreateWorkloadRule(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, got))
	require.NotNil(t, got.Spec.Workload)
	require.Len(t, got.Spec.Workload.Rules, 1)
	assert.Equal(t, "rule1", got.Spec.Workload.Rules[0].Name)
}

func TestHandleUpdateWorkloadRule_RetriesOnConflict(t *testing.T) {
	conflicts := 1
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Rules: []cbv1alpha1.WorkloadRule{{Name: "rule1", Action: "cancel"}},
	}
	s := newConflictTestServer(&conflicts, cluster)

	body := bytes.NewBufferString(`{"action":"move","moveTarget":"slow"}`)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/workload/rules/rule1?namespace=default", body)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("ruleName", "rule1")

	rec := httptest.NewRecorder()
	s.handleUpdateWorkloadRule(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleDeleteWorkloadRule_RetriesOnConflict(t *testing.T) {
	conflicts := 1
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Rules: []cbv1alpha1.WorkloadRule{{Name: "rule1", Action: "cancel"}},
	}
	s := newConflictTestServer(&conflicts, cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/rules/rule1?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("ruleName", "rule1")

	rec := httptest.NewRecorder()
	s.handleDeleteWorkloadRule(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestApplyScheduleUpdate_RetriesOnConflict(t *testing.T) {
	conflicts := 1
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true, Schedule: "0 1 * * *"}
	s := newConflictTestServer(&conflicts, cluster)

	body := bytes.NewBufferString(`{"schedule":"0 2 * * *"}`)
	req := httptest.NewRequest(http.MethodPatch,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, got))
	assert.Equal(t, "0 2 * * *", got.Spec.Backup.Schedule)
}

func TestUpdateClusterWithConflictRetry_ExhaustedReturns500(t *testing.T) {
	// Every Update conflicts → retries are exhausted → 500 with the conflict
	// cause in the body.
	cluster := newTestCluster("test-cluster", "default")
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				return apierrors.NewConflict(
					schema.GroupResource{Resource: "cloudberryclusters"},
					obj.GetName(), errBoom)
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	body := bytes.NewBufferString(`{"parameters":{"max_connections":"200"}}`)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/config?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "Operation cannot be fulfilled",
		"exhausted-retries response must carry the conflict cause")
}

// annotationVerbAsserter fails Update calls on CloudberryCluster objects so
// annotation-only handlers are proven to use Patch instead of Update.
func TestSetClusterAnnotation_UsesPatchNotUpdate(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	patched := false
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.UpdateOption) error {
				t.Fatal("annotation-only mutation must use Patch, not Update")
				return nil
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				patched = true
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleStartCluster(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.True(t, patched, "Patch verb must be used")

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, got))
	assert.Equal(t, "start", got.Annotations[util.AnnotationAction])
}

func TestSetMaintenanceAnnotation_UsesPatchNotUpdate(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.UpdateOption) error {
				t.Fatal("annotation-only mutation must use Patch, not Update")
				return nil
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/maintenance/vacuum?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleVacuum(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, got))
	assert.Equal(t, "vacuum", got.Annotations[util.AnnotationMaintenance])
}

func TestHandleStartRecovery_UsesPatchNotUpdate(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.UpdateOption) error {
				t.Fatal("annotation-only mutation must use Patch, not Update")
				return nil
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	body := bytes.NewBufferString(`{"type":"full"}`)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/recovery?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, got))
	assert.Equal(t, "full", got.Annotations[util.AnnotationRecovery])
}

// failingUpdateInterceptor fails every Update with the given error.
func failingUpdateInterceptor(updateErr error) interceptor.Funcs {
	return interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
			_ ...client.UpdateOption) error {
			return updateErr
		},
	}
}

// failingPatchInterceptor fails every Patch with the given error.
func failingPatchInterceptor(patchErr error) interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
			_ client.Patch, _ ...client.PatchOption) error {
			return patchErr
		},
	}
}

func newInterceptedServer(funcs interceptor.Funcs, cluster *cbv1alpha1.CloudberryCluster) *Server {
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(funcs).
		Build()
	return trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))
}

func TestSetClusterAnnotation_PatchError_Returns500(t *testing.T) {
	s := newInterceptedServer(failingPatchInterceptor(errBoom),
		newTestCluster("test-cluster", "default"))

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleStartCluster(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestSetMaintenanceAnnotation_PatchError_Returns500(t *testing.T) {
	s := newInterceptedServer(failingPatchInterceptor(errBoom),
		newTestCluster("test-cluster", "default"))

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/maintenance/vacuum?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleVacuum(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleStartRecovery_PatchError_Returns500(t *testing.T) {
	s := newInterceptedServer(failingPatchInterceptor(errBoom),
		newTestCluster("test-cluster", "default"))

	body := bytes.NewBufferString(`{"type":"full"}`)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/recovery?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestApplyScheduleUpdate_UpdateError_Returns500(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true, Schedule: "0 1 * * *"}
	s := newInterceptedServer(failingUpdateInterceptor(errBoom), cluster)

	body := bytes.NewBufferString(`{"schedule":"0 2 * * *"}`)
	req := httptest.NewRequest(http.MethodPatch,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleUpdateWorkloadRule_RuleVanishesMidRetry_Returns404(t *testing.T) {
	// The rule exists at pre-validation but disappears before the retried
	// mutation re-reads the object → 404 via the sentinel error.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Rules: []cbv1alpha1.WorkloadRule{{Name: "rule1", Action: "cancel"}},
	}

	firstGet := true
	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if cc, ok := obj.(*cbv1alpha1.CloudberryCluster); ok && !firstGet {
					cc.Spec.Workload = nil // rule vanished concurrently
				}
				firstGet = false
				return nil
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	body := bytes.NewBufferString(`{"action":"move"}`)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/workload/rules/rule1?namespace=default", body)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("ruleName", "rule1")

	rec := httptest.NewRecorder()
	s.handleUpdateWorkloadRule(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "RULE_NOT_FOUND")
}

func TestHandleRotatePassword_UpdateError_Returns500(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	secret := &corev1.Secret{}
	secret.Name = util.OperatorAdminPasswordSecretName
	secret.Namespace = util.OperatorNamespace
	secret.Data = map[string][]byte{util.PasswordSecretKey: []byte("old")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(secret).
		WithInterceptorFuncs(failingUpdateInterceptor(errBoom)).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0, credStore))

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, rotatePasswordRequest())

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "failed to update admin password secret")
}

func TestCreateAdminPasswordSecret_RaceThenGetFails_Returns500(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()

	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return apierrors.NewNotFound(
					schema.GroupResource{Resource: "secrets"}, key.Name)
			},
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Resource: "secrets"}, obj.GetName())
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0, credStore))

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, rotatePasswordRequest())

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "failed to create admin password secret")
}

// The data-loading job mutation endpoints are now fully implemented
// (Scenario 107); their error-envelope shape is covered by the dedicated
// Scenario 107 suite written separately.
