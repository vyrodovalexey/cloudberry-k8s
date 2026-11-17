package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// DEF-6 / U018 §9 F-4 — restore Job name-collision regression tests
// ============================================================================
//
// The restore Job name is deterministic (<cluster>-restore-<timestamp>,
// util.RestoreJobName) because it is keyed off the RESTORED backup timestamp.
// After a failed restore attempt the leftover Job used to collide with the
// retry's Create and the API surfaced an opaque 500 "failed to create restore
// job" (U018 §9 F-4). The fix resolves the collision in handleRestoreBackup:
// a terminal (failed/succeeded) leftover is deleted and replaced so the retry
// proceeds; an in-flight (running/pending) Job is a 409 CONFLICT and is never
// clobbered. These tests pin all four outcomes plus the unchanged happy path.
// ============================================================================

// restoreCollisionRequest builds a POST …/backups/{timestamp}/restore request
// mirroring the U018 §5 repro shape (--create-db --redirect-db via options).
func restoreCollisionRequest(timestamp string) *http.Request {
	body := `{"databases":["mydb"],"gprestoreOptions":{` +
		`"createDb":true,"redirectDb":"mydb_restored"}}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/backups/"+timestamp+"/restore?namespace=default",
		strings.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", timestamp)
	return req
}

// restoreCollisionJob seeds the deterministic restore Job for timestamp with
// the given coarse status (the same classification jobStatus produces).
func restoreCollisionJob(t *testing.T, timestamp, status string) *batchv1.Job {
	t.Helper()
	job := backupJob(util.RestoreJobName("test-cluster", timestamp), util.BackupOperationRestore)
	switch status {
	case "failed":
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: "True"},
		}
	case "succeeded":
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobComplete, Status: "True"},
		}
	case "running":
		job.Status.Active = 1
	default: // "pending": created but no terminal condition and not active.
	}
	return job
}

// assertRestoreAccepted asserts the 202 envelope and that the live Job for the
// deterministic name is a FRESH run (no terminal conditions carried over from
// a replaced leftover) labelled as a restore operation.
func assertRestoreAccepted(t *testing.T, s *Server, timestamp string, rec *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "restore started", resp[responseKeyStatus])
	assert.Equal(t, util.RestoreJobName("test-cluster", timestamp), resp[responseKeyJob])

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.RestoreJobName("test-cluster", timestamp), Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
	assert.Equal(t, 0, int(job.Status.Active))
	for _, cond := range job.Status.Conditions {
		assert.NotEqual(t, "True", string(cond.Status),
			"replacement Job must not inherit terminal conditions")
	}
}

// TestHandleRestoreBackup_CollisionWithFailedJob_Proceeds is the DEF-6
// regression: re-restoring a timestamp whose restore Job FAILED must replace
// the leftover and return 202 — not the previous 500.
func TestHandleRestoreBackup_CollisionWithFailedJob_Proceeds(t *testing.T) {
	const timestamp = "20261115180358"
	cluster := newBackupEnabledCluster()
	failed := restoreCollisionJob(t, timestamp, "failed")
	s := newTestServerWithBatch(cluster, failed)

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
	assertRestoreAccepted(t, s, timestamp, rec)
}

// TestHandleRestoreBackup_CollisionWithSucceededJob_Proceeds covers the same
// terminal-leftover defect class for a SUCCEEDED restore Job: re-restoring an
// already-restored timestamp (e.g. into a different redirect DB) also replaces
// and proceeds rather than 500ing.
func TestHandleRestoreBackup_CollisionWithSucceededJob_Proceeds(t *testing.T) {
	const timestamp = "20261115180358"
	cluster := newBackupEnabledCluster()
	succeeded := restoreCollisionJob(t, timestamp, "succeeded")
	s := newTestServerWithBatch(cluster, succeeded)

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
	assertRestoreAccepted(t, s, timestamp, rec)
}

// TestHandleRestoreBackup_CollisionWithRunningJob_Conflict pins the
// never-clobber rule: while a restore of the timestamp is still in flight the
// retry is a 409 CONFLICT with an actionable message and the in-flight Job is
// left untouched.
func TestHandleRestoreBackup_CollisionWithRunningJob_Conflict(t *testing.T) {
	const timestamp = "20261115180358"
	cluster := newBackupEnabledCluster()
	running := restoreCollisionJob(t, timestamp, "running")
	s := newTestServerWithBatch(cluster, running)

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))

	require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	var errResp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	errObj, ok := errResp["error"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "CONFLICT", errObj["code"])
	assert.Contains(t, errObj["message"], util.RestoreJobName("test-cluster", timestamp))
	assert.Contains(t, errObj["message"], "still in progress")

	// The in-flight Job must survive the rejected retry.
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.RestoreJobName("test-cluster", timestamp), Namespace: "default"}, job))
	assert.Equal(t, int32(1), job.Status.Active)
}

// TestHandleRestoreBackup_CollisionWithPendingJob_Conflict covers the
// created-but-not-yet-active leftover: it is not terminal, so the retry is a
// 409 CONFLICT (never replaced).
func TestHandleRestoreBackup_CollisionWithPendingJob_Conflict(t *testing.T) {
	const timestamp = "20261115180358"
	cluster := newBackupEnabledCluster()
	pending := restoreCollisionJob(t, timestamp, "pending")
	s := newTestServerWithBatch(cluster, pending)

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))

	require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.RestoreJobName("test-cluster", timestamp), Namespace: "default"}, job))
	assert.Empty(t, job.Status.Conditions)
}

// TestHandleRestoreBackup_NoCollision_HappyPathUnchanged pins the untouched
// happy path: with no leftover Job the POST still creates the restore Job
// directly under the SAME deterministic name (job-name generation unchanged)
// with the restore-operation label and the requested --create-db /
// --redirect-db flags rendered into the gprestore invocation.
func TestHandleRestoreBackup_NoCollision_HappyPathUnchanged(t *testing.T) {
	const timestamp = "20261115180358"
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)

	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
	assertRestoreAccepted(t, s, timestamp, rec)

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.RestoreJobName("test-cluster", timestamp), Namespace: "default"}, job))
	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "--timestamp")
	assert.Contains(t, script, timestamp)
	assert.Contains(t, script, "--create-db")
	assert.Contains(t, script, "--redirect-db")
	assert.Contains(t, script, "mydb_restored")
}

// TestHandleRestoreBackup_CollisionReplaceErrorPaths covers the defensive
// error branches of the collision resolution: a failed inspect, a failed
// delete of the terminal leftover, and a failed replacement Create all
// surface an honest 500 (a real server-side failure), never a silent miss.
func TestHandleRestoreBackup_CollisionReplaceErrorPaths(t *testing.T) {
	const timestamp = "20261115180358"

	t.Run("inspect failure surfaces 500", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		failed := restoreCollisionJob(t, timestamp, "failed")
		s := newTestServerWithInterceptor(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return errAlways
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}, cluster, failed)

		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
		assert.Equal(t, http.StatusInternalServerError, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("delete failure surfaces 500", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		failed := restoreCollisionJob(t, timestamp, "failed")
		s := newTestServerWithInterceptor(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
				return errAlways
			},
		}, cluster, failed)

		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
		assert.Equal(t, http.StatusInternalServerError, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("replacement create failure surfaces 500", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		failed := restoreCollisionJob(t, timestamp, "failed")
		creates := 0
		s := newTestServerWithInterceptor(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				creates++
				// First Create hits the seeded leftover (AlreadyExists) via the
				// real client; only the replacement Create is failed.
				if creates > 1 {
					return errAlways
				}
				return c.Create(ctx, obj, opts...)
			},
		}, cluster, failed)

		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
		assert.Equal(t, http.StatusInternalServerError, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("replace race lost to concurrent retry surfaces 409", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		failed := restoreCollisionJob(t, timestamp, "failed")
		creates := 0
		s := newTestServerWithInterceptor(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				creates++
				// First Create hits the seeded leftover; the replacement
				// Create loses the race to a concurrent retry (AlreadyExists
				// from the twin's freshly created Job).
				if creates > 1 {
					return apierrors.NewAlreadyExists(
						schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}, cluster, failed)

		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, restoreCollisionRequest(timestamp))
		assert.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	})
}
