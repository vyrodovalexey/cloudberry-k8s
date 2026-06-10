package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newTestServerWithInterceptor builds a server whose fake client is wrapped with
// the given interceptor funcs so error paths (Create/List/Get failures) can be
// exercised deterministically.
func newTestServerWithInterceptor(funcs interceptor.Funcs, objs ...client.Object) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	return trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0))
}

// fetchJobScript fetches the named Job from the server's fake client and returns
// the rendered gpbackup/gprestore container script (Containers[0].Args[0]).
func fetchJobScript(t *testing.T, s *Server, jobName string) string {
	t.Helper()
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: "default"}, job))
	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// ----------------------------------------------------------------------------
// 85b — POST /backups : full Create Backup Request -> Job args (option mapping)
// ----------------------------------------------------------------------------

func TestHandleCreateBackup_FullOptionMapping_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)

	body := `{
		"type": "full",
		"databases": ["mydb"],
		"gpbackupOptions": {
			"singleDataFile": true,
			"copyQueueSize": 8,
			"includeSchemas": ["public"],
			"excludeTables": ["public.tmp"],
			"leafPartitionData": true,
			"withStats": true,
			"withoutGlobals": true,
			"compressionLevel": 5,
			"compressionType": "zstd"
		}
	}`

	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", body))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "backup started", resp["status"])
	assert.Equal(t, "full", resp["type"])
	jobName, ok := resp["job"].(string)
	require.True(t, ok)
	require.NotEmpty(t, jobName)

	// The render shell-quotes each token individually, so a flag/value pair is
	// rendered as '--flag' 'value' (each token wrapped in single quotes).
	script := fetchJobScript(t, s, jobName)
	for _, want := range []string{
		"'--dbname' 'mydb'",
		"'--single-data-file'",
		"'--copy-queue-size' '8'",
		"'--include-schema' 'public'",
		"'--exclude-table' 'public.tmp'",
		"'--leaf-partition-data'",
		"'--with-stats'",
		"'--without-globals'",
		"'--compression-level' '5'",
		"'--compression-type' 'zstd'",
	} {
		assert.Containsf(t, script, want, "expected script to contain %q", want)
	}
	// GAP-B: exactly one leaf flag, and NOT incremental (full backup).
	assert.Equal(t, 1, strings.Count(script, "'--leaf-partition-data'"))
	assert.NotContains(t, script, "'--incremental'")
	// single-data-file is mutually exclusive with --jobs.
	assert.NotContains(t, script, "'--jobs'")

	// The created object is a Job (not a CronJob) with the backup-operation label.
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
}

// ----------------------------------------------------------------------------
// 85e — POST /backups/{ts}/restore : full Restore Request -> Job args
// ----------------------------------------------------------------------------

func TestHandleRestoreBackup_FullOptionMapping_Scenario85(t *testing.T) {
	const ts = "20260101010101"
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)

	body := `{
		"databases": ["mydb"],
		"gprestoreOptions": {
			"jobs": 4,
			"redirectDb": "mydb_restored",
			"redirectSchema": "restored",
			"createDb": true,
			"includeTables": ["public.users", "public.orders"],
			"includeSchemas": ["public"],
			"excludeTables": ["public.audit"],
			"withGlobals": true,
			"withStats": true,
			"runAnalyze": true,
			"onErrorContinue": true,
			"truncateTable": true,
			"dataOnly": true,
			"resizeCluster": true
		}
	}`

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", ts, body))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "restore started", resp["status"])
	jobName, ok := resp["job"].(string)
	require.True(t, ok)

	script := fetchJobScript(t, s, jobName)
	for _, want := range []string{
		"'--timestamp' '" + ts + "'",
		"'--jobs' '4'",
		"'--redirect-db' 'mydb_restored'",
		"'--redirect-schema' 'restored'",
		"'--create-db'",
		"'--with-globals'",
		"'--run-analyze'",
		"'--on-error-continue'",
		"'--truncate-table'",
		"'--data-only'",
		"'--resize-cluster'",
		"'--include-table' 'public.users'",
		"'--include-table' 'public.orders'",
		"'--exclude-table' 'public.audit'",
	} {
		assert.Containsf(t, script, want, "expected script to contain %q", want)
	}
	// include-table wins over include-schema (mutual exclusivity).
	assert.NotContains(t, script, "'--include-schema'")
	// run-analyze wins over with-stats (mutual exclusivity).
	assert.NotContains(t, script, "'--with-stats'")
	// dataOnly set, metadataOnly not -> no --metadata-only.
	assert.NotContains(t, script, "'--metadata-only'")

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
}

// TestHandleRestoreBackup_DataAndMetadataOnly_Conflict_Scenario85 asserts the
// mutual-exclusivity guard: dataOnly AND metadataOnly both true => 400.
func TestHandleRestoreBackup_DataAndMetadataOnly_Conflict_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260101010101",
		`{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandleRestoreBackup_MetadataOnly_Scenario85 asserts metadataOnly alone maps
// to --metadata-only (and NOT --data-only).
func TestHandleRestoreBackup_MetadataOnly_Scenario85(t *testing.T) {
	const ts = "20260101010101"
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", ts,
		`{"gprestoreOptions":{"metadataOnly":true}}`))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, ok := resp["job"].(string)
	require.True(t, ok)

	script := fetchJobScript(t, s, jobName)
	assert.Contains(t, script, "'--metadata-only'")
	assert.NotContains(t, script, "'--data-only'")
}

// ----------------------------------------------------------------------------
// 85a — GET /backups : list from status.BackupHistory
// ----------------------------------------------------------------------------

func TestHandleListBackups_HistoryShape_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260101010101", Type: "full", Status: "Success"},
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260102010101", Type: "incremental", Status: "Success"},
	)
	cluster.Status.LastBackupTimestamp = "20260102010101"
	cluster.Status.LastBackupStatus = "Success"
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackups(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["total"])
	assert.Equal(t, "20260102010101", resp["lastBackupTimestamp"])
	assert.Equal(t, "Success", resp["lastBackupStatus"])
	backups, ok := resp["backups"].([]interface{})
	require.True(t, ok)
	assert.Len(t, backups, 2)
}

// ----------------------------------------------------------------------------
// 85c — GET /backups/{ts} : details / not-found / invalid format
// ----------------------------------------------------------------------------

func TestHandleGetBackup_Matrix_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260101010101", Type: "full", Status: "Success"},
	)
	s := newTestServer(cluster)

	tests := []struct {
		name     string
		ts       string
		wantCode int
	}{
		{"existing timestamp returns 200", "20260101010101", http.StatusOK},
		{"unknown 14-digit timestamp returns 404", "20200101000000", http.StatusNotFound},
		{"invalid timestamp format returns 400", "bk-1", http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet,
				apiPrefix+"/clusters/test-cluster/backups/"+tc.ts+"?namespace=default", nil)
			req.SetPathValue("name", "test-cluster")
			req.SetPathValue("timestamp", tc.ts)
			rec := httptest.NewRecorder()
			s.handleGetBackup(rec, req)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ----------------------------------------------------------------------------
// 85d — DELETE /backups/{ts} : cleanup Job creation + args
// ----------------------------------------------------------------------------

func TestHandleDeleteBackup_CleanupJobArgs_Scenario85(t *testing.T) {
	const ts = "20260101010101"
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/backups/"+ts+"?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", ts)
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "deleted", resp["status"])
	jobName, ok := resp["job"].(string)
	require.True(t, ok)

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation])
	// The cleanup Job runs a gpbackman retention/cleanup script.
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "backup-delete")
}

// ----------------------------------------------------------------------------
// 85f — GET /backups/jobs : backup/restore/cleanup Job statuses
// ----------------------------------------------------------------------------

func TestHandleListBackupJobs_Statuses_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	running := backupJob("test-cluster-backup-1", util.BackupOperationBackup)
	running.Status.Active = 1
	succeeded := backupJob("test-cluster-restore-1", util.BackupOperationRestore)
	succeeded.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: "True"},
	}
	cleanup := backupJob("test-cluster-cleanup-1", util.BackupOperationCleanup)
	unrelated := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-unrelated",
			Namespace: "default",
			Labels:    map[string]string{util.LabelCluster: "test-cluster"},
		},
	}
	s := newTestServerWithBatch(cluster, running, succeeded, cleanup, unrelated)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/jobs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackupJobs(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// 3 backup-op jobs returned, the unrelated job excluded.
	assert.Equal(t, float64(3), resp["total"])

	jobs, ok := resp["jobs"].([]interface{})
	require.True(t, ok)
	statuses := map[string]string{}
	for _, j := range jobs {
		jm := j.(map[string]interface{})
		statuses[jm["name"].(string)] = jm["status"].(string)
	}
	assert.Equal(t, "running", statuses["test-cluster-backup-1"])
	assert.Equal(t, "succeeded", statuses["test-cluster-restore-1"])
	assert.Equal(t, statusPending, statuses["test-cluster-cleanup-1"])
}

// ----------------------------------------------------------------------------
// 85g — GET /backups/schedule : CronJob present / absent + nextScheduleTime
// ----------------------------------------------------------------------------

func TestHandleGetBackupSchedule_Present_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	lastRun := metav1.NewTime(time.Date(2026, 5, 19, 3, 0, 0, 0, time.UTC))
	suspend := false
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName("test-cluster"),
			Namespace: "default",
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 2 * * *",
			Suspend:  &suspend,
		},
		Status: batchv1.CronJobStatus{LastScheduleTime: &lastRun},
	}
	s := newTestServerWithBatch(cluster, cronJob)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetBackupSchedule(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["scheduled"])
	assert.Equal(t, "0 2 * * *", resp["schedule"])
	assert.Equal(t, false, resp["suspend"])
	require.NotNil(t, resp["nextScheduleTime"])

	// nextScheduleTime is computed from the last schedule time, so it must be the
	// next cron tick strictly after lastScheduleTime (2026-05-19 03:00 -> the
	// 2026-05-20 02:00 occurrence for "0 2 * * *").
	nextStr, ok := resp["nextScheduleTime"].(string)
	require.True(t, ok)
	next, err := time.Parse(time.RFC3339, nextStr)
	require.NoError(t, err)
	assert.True(t, next.After(lastRun.Time), "nextScheduleTime %v must be after lastScheduleTime %v", next, lastRun.Time)
}

// TestHandleGetBackupSchedule_NextFromNow_Scenario85 verifies nextScheduleTime
// computes a FUTURE time (relative to now) when the CronJob has no recorded
// last-schedule time, exercising the now-based branch of nextScheduleTime.
func TestHandleGetBackupSchedule_NextFromNow_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName("test-cluster"),
			Namespace: "default",
		},
		// Every minute so the next tick is always within a minute of now.
		Spec: batchv1.CronJobSpec{Schedule: "* * * * *"},
	}
	s := newTestServerWithBatch(cluster, cronJob)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetBackupSchedule(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	nextStr, ok := resp["nextScheduleTime"].(string)
	require.True(t, ok)
	next, err := time.Parse(time.RFC3339, nextStr)
	require.NoError(t, err)
	assert.True(t, next.After(time.Now().UTC().Add(-time.Minute)),
		"nextScheduleTime %v must be a near-future time", next)
}

func TestHandleGetBackupSchedule_Absent_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetBackupSchedule(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["scheduled"])
}

// ----------------------------------------------------------------------------
// E — RBAC / negative (via the full router so withPermission is exercised)
// ----------------------------------------------------------------------------

// serveWithIdentity drives the request through the registered routes with the
// given identity in context so the RBAC middleware (withPermission) runs.
func serveWithIdentity(s *Server, req *http.Request, perm auth.PermissionLevel) *httptest.ResponseRecorder {
	identity := &auth.Identity{Username: "tester", Permission: perm}
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// ----------------------------------------------------------------------------
// Error paths (500) via interceptor-injected client failures.
// ----------------------------------------------------------------------------

// errAlways is a sentinel error returned by the failing interceptors.
var errAlways = errors.New("induced failure")

func TestHandleCreateBackup_CreateError_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errAlways
		},
	}, cluster)
	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster",
		`{"type":"full","databases":["mydb"]}`))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleDeleteBackup_CreateError_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errAlways
		},
	}, cluster)
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/backups/20260101010101?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", "20260101010101")
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleRestoreBackup_CreateError_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errAlways
		},
	}, cluster)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260101010101", `{}`))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleListBackupJobs_ListError_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithInterceptor(interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return errAlways
		},
	}, cluster)
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/jobs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackupJobs(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleGetBackupSchedule_GetError_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	// Fail only the CronJob Get (a non-NotFound error) while the cluster Get
	// (CloudberryCluster) succeeds.
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.CronJob); ok {
				return errAlways
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, cluster)
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/schedule?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetBackupSchedule(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// TestNextScheduleTime_UnparseableCron_Scenario85 covers the !ok branch of
// nextScheduleTime: an unparseable schedule yields a nil next time, and
// backupScheduleResponse omits the nextScheduleTime key.
func TestNextScheduleTime_UnparseableCron_Scenario85(t *testing.T) {
	cronJob := &batchv1.CronJob{
		Spec: batchv1.CronJobSpec{Schedule: "not a cron"},
	}
	assert.Nil(t, nextScheduleTime(cronJob))

	resp := backupScheduleResponse("test-cluster", cronJob)
	_, present := resp["nextScheduleTime"]
	assert.False(t, present, "nextScheduleTime must be omitted for an unparseable schedule")
}

func TestBackupEndpoints_RBAC_Scenario85(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		perm     auth.PermissionLevel
		wantCode int
	}{
		{
			name:     "create backup with Basic permission is forbidden",
			method:   http.MethodPost,
			path:     apiPrefix + "/clusters/test-cluster/backups?namespace=default",
			body:     `{"type":"full"}`,
			perm:     auth.PermissionBasic,
			wantCode: http.StatusForbidden,
		},
		{
			name:     "delete backup with Operator permission is forbidden",
			method:   http.MethodDelete,
			path:     apiPrefix + "/clusters/test-cluster/backups/20260101010101?namespace=default",
			perm:     auth.PermissionOperator,
			wantCode: http.StatusForbidden,
		},
		{
			name:     "restore with Operator permission is forbidden",
			method:   http.MethodPost,
			path:     apiPrefix + "/clusters/test-cluster/backups/20260101010101/restore?namespace=default",
			body:     `{}`,
			perm:     auth.PermissionOperator,
			wantCode: http.StatusForbidden,
		},
		{
			name:     "list backups missing cluster returns 404",
			method:   http.MethodGet,
			path:     apiPrefix + "/clusters/nonexistent/backups?namespace=default",
			perm:     auth.PermissionBasic,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "create backup missing cluster returns 404 with Operator permission",
			method:   http.MethodPost,
			path:     apiPrefix + "/clusters/nonexistent/backups?namespace=default",
			body:     `{"type":"full"}`,
			perm:     auth.PermissionOperator,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}
			rec := serveWithIdentity(s, req, tc.perm)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
