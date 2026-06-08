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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Scenario 75: Compression Matrix (gzip vs zstd) — api-level round-trip
// ============================================================================
//
// These white-box tests drive the full REST backup path (handleCreateBackup)
// for the Scenario 75 compression matrix. A POST with
// gpbackupOptions{compressionType:<gzip|zstd>, compressionLevel:6} must create a
// batchv1.Job (NOT a CronJob), labelled as a backup operation, whose gpbackup
// args contain --compression-type <type> + --compression-level 6. This confirms
// the per-request compressionType flows through mergeGpbackupOptions +
// buildBackupJobOptions + BuildBackupJob end-to-end through the handler. It
// mirrors the round-trip style in backup_nocompression_test.go.
// ============================================================================

// s75CreateBackupJobScript drives handleCreateBackup with the given gpbackup
// options body, asserts a 202 + a Job-not-CronJob result, and returns the
// rendered gpbackup container script.
func s75CreateBackupJobScript(t *testing.T, gpbackupOptions string) string {
	t.Helper()

	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Image = "cloudberry-backup:2.1.0"
	s := newTestServerWithBatch(cluster)

	body := `{"type":"full","databases":["mydb"],"gpbackupOptions":` + gpbackupOptions + `}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/backups?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, _ := resp[responseKeyJob].(string)
	require.NotEmpty(t, jobName)

	// The on-demand POST path creates a Job DIRECTLY (not a CronJob).
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])

	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// TestHandleCreateBackup_Scenario75Gzip drives the full REST path for the gzip
// arm of the compression matrix: a POST with
// gpbackupOptions{compressionType:"gzip",compressionLevel:6} must create a Job
// whose gpbackup args contain --compression-type gzip + --compression-level 6.
func TestHandleCreateBackup_Scenario75Gzip(t *testing.T) {
	script := s75CreateBackupJobScript(t,
		`{"compressionType":"gzip","compressionLevel":6}`)

	assert.Contains(t, script, "--compression-type")
	assert.Contains(t, script, "gzip")
	assert.Contains(t, script, "--compression-level")
	assert.Contains(t, script, "6")
	// gzip must not be mislabelled as zstd.
	assert.NotContains(t, script, "zstd")
	// On-demand compressed backup: --no-compression must NOT appear.
	assert.NotContains(t, script, "--no-compression")
}

// TestHandleCreateBackup_Scenario75Zstd drives the full REST path for the zstd
// arm of the compression matrix: a POST with
// gpbackupOptions{compressionType:"zstd",compressionLevel:6} must create a Job
// whose gpbackup args contain --compression-type zstd + --compression-level 6.
func TestHandleCreateBackup_Scenario75Zstd(t *testing.T) {
	script := s75CreateBackupJobScript(t,
		`{"compressionType":"zstd","compressionLevel":6}`)

	assert.Contains(t, script, "--compression-type")
	assert.Contains(t, script, "zstd")
	assert.Contains(t, script, "--compression-level")
	assert.Contains(t, script, "6")
	// zstd must not be mislabelled as gzip.
	assert.NotContains(t, script, "gzip")
	// On-demand compressed backup: --no-compression must NOT appear.
	assert.NotContains(t, script, "--no-compression")
}
