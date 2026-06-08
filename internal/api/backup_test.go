package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newTestServerWithBatch builds a server seeded with the given runtime objects
// (including batch resources such as Jobs and CronJobs).
func newTestServerWithBatch(objs ...runtime.Object) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)
}

func backupJob(name, operation string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				util.LabelCluster:         "test-cluster",
				util.LabelBackupOperation: operation,
			},
		},
	}
}

func TestHandleListBackupJobs(t *testing.T) {
	cluster := newBackupEnabledCluster()
	running := backupJob("test-cluster-backup-1", util.BackupOperationBackup)
	running.Status.Active = 1
	succeeded := backupJob("test-cluster-restore-1", util.BackupOperationRestore)
	succeeded.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: "True"},
	}
	unrelated := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-other",
			Namespace: "default",
			Labels:    map[string]string{util.LabelCluster: "test-cluster"},
		},
	}

	s := newTestServerWithBatch(cluster, running, succeeded, unrelated)
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/jobs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackupJobs(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["total"])
}

func TestHandleListBackupJobs_NotFound(t *testing.T) {
	s := newTestServerWithBatch()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/backups/jobs?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListBackupJobs(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetBackupSchedule(t *testing.T) {
	cluster := newBackupEnabledCluster()
	lastRun := metav1.NewTime(time.Date(2026, 5, 19, 3, 0, 0, 0, time.UTC))
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName("test-cluster"),
			Namespace: "default",
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 3 * * *"},
		Status: batchv1.CronJobStatus{
			LastScheduleTime: &lastRun,
		},
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
	assert.Equal(t, "0 3 * * *", resp["schedule"])
	assert.NotNil(t, resp["nextScheduleTime"])
}

func TestHandleGetBackupSchedule_NoCronJob(t *testing.T) {
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

func TestHandleGetBackupSchedule_ClusterNotFound(t *testing.T) {
	s := newTestServerWithBatch()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/backups/schedule?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetBackupSchedule(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func patchScheduleRequest(name, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPatch,
		apiPrefix+"/clusters/"+name+"/backups/schedule?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", name)
	return req
}

func TestHandleUpdateBackupSchedule_SetSchedule(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{"schedule":"0 4 * * *"}`))
	require.Equal(t, http.StatusOK, rec.Code)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "test-cluster", Namespace: "default"}, updated))
	assert.Equal(t, "0 4 * * *", updated.Spec.Backup.Schedule)
}

func TestHandleUpdateBackupSchedule_InvalidCron(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{"schedule":"not a cron"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleUpdateBackupSchedule_Suspend(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName("test-cluster"),
			Namespace: "default",
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 3 * * *"},
	}
	s := newTestServerWithBatch(cluster, cronJob)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{"suspend":true}`))
	require.Equal(t, http.StatusOK, rec.Code)

	updated := &batchv1.CronJob{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.BackupCronJobName("test-cluster"), Namespace: "default"}, updated))
	require.NotNil(t, updated.Spec.Suspend)
	assert.True(t, *updated.Spec.Suspend)
}

func TestHandleUpdateBackupSchedule_SuspendNoCronJob(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{"suspend":true}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleUpdateBackupSchedule_InvalidBody(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{bad`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleUpdateBackupSchedule_ClusterNotFound(t *testing.T) {
	s := newTestServerWithBatch()
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("nonexistent", `{"schedule":"0 3 * * *"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleUpdateBackupSchedule_NoBackupSpec(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleUpdateBackupSchedule(rec, patchScheduleRequest("test-cluster", `{"schedule":"0 3 * * *"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestBackupRouting_Precedence verifies that the literal /backups/jobs and
// /backups/schedule routes take precedence over /backups/{timestamp}.
func TestBackupRouting_Precedence(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}

	cases := []struct {
		path    string
		wantKey string
	}{
		{apiPrefix + "/clusters/test-cluster/backups/jobs?namespace=default", "jobs"},
		{apiPrefix + "/clusters/test-cluster/backups/schedule?namespace=default", "scheduled"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, "path %s", tc.path)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		_, ok := resp[tc.wantKey]
		assert.Truef(t, ok, "expected key %q in response for %s", tc.wantKey, tc.path)
	}
}

func TestIsValidBackupTimestamp(t *testing.T) {
	assert.True(t, isValidBackupTimestamp("20260519020000"))
	assert.False(t, isValidBackupTimestamp("2026"))
	assert.False(t, isValidBackupTimestamp("bk-1"))
}

func TestBackupTypeOrDefault(t *testing.T) {
	assert.Equal(t, util.BackupTypeFull, backupTypeOrDefault(""))
	assert.Equal(t, util.BackupTypeIncremental, backupTypeOrDefault(util.BackupTypeIncremental))
}

func TestMergeGpbackupOptions(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{
		CompressionLevel: 3,
		CompressionType:  "gzip",
	}
	out := mergeGpbackupOptions(cluster, &GpbackupOptionsRequest{
		Jobs:      4,
		WithStats: true,
	})
	// Defaults preserved when request leaves them zero.
	assert.Equal(t, int32(3), out.CompressionLevel)
	assert.Equal(t, "gzip", out.CompressionType)
	assert.Equal(t, int32(4), out.Jobs)
	assert.True(t, out.WithStats)
}

func TestMergeGprestoreOptions(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Gprestore = &cbv1alpha1.GprestoreOptions{Jobs: 2}
	out := mergeGprestoreOptions(cluster, &GprestoreOptionsRequest{
		CreateDb:   true,
		RunAnalyze: true,
	})
	assert.Equal(t, int32(2), out.Jobs)
	assert.True(t, out.CreateDb)
	assert.True(t, out.RunAnalyze)
}

func TestJobStatus(t *testing.T) {
	failed := &batchv1.Job{}
	failed.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: "True"}}
	assert.Equal(t, "failed", jobStatus(failed))

	complete := &batchv1.Job{}
	complete.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	assert.Equal(t, "succeeded", jobStatus(complete))

	running := &batchv1.Job{}
	running.Status.Active = 1
	assert.Equal(t, "running", jobStatus(running))

	pending := &batchv1.Job{}
	assert.Equal(t, statusPending, jobStatus(pending))
}

func TestIsBackupOperation(t *testing.T) {
	assert.True(t, isBackupOperation(util.BackupOperationBackup))
	assert.True(t, isBackupOperation(util.BackupOperationCleanup))
	assert.False(t, isBackupOperation("other"))
}
