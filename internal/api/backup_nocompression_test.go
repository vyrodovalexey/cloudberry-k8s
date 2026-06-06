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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ============================================================================
// Scenario 73: On-Demand Backup with gpbackup Options — api-level coverage
// ============================================================================
//
// These white-box tests live in package api because mergeGpbackupOptions is
// unexported. They assert the per-request noCompression override (gaps G1/G2)
// flows from GpbackupOptionsRequest through mergeGpbackupOptions and, end-to-end,
// through handleCreateBackup into the created backup Job's gpbackup args.
// ============================================================================

// TestMergeGpbackupOptions_NoCompressionOverride asserts that a per-request
// noCompression=true wins even when the cluster default sets a compression
// level, and that omitting noCompression leaves it false while preserving the
// requested compression level (73b merge semantics).
func TestMergeGpbackupOptions_NoCompressionOverride(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 9}

	// (a) request noCompression=true overrides the cluster default.
	out := mergeGpbackupOptions(cluster, &GpbackupOptionsRequest{
		NoCompression:    true,
		CompressionLevel: 6,
	})
	require.NotNil(t, out)
	assert.True(t, out.NoCompression, "per-request noCompression=true must win")
	assert.Equal(t, int32(6), out.CompressionLevel,
		"compressionLevel is still carried; the builder ignores it when noCompression")

	// (b) omitted noCompression (false) leaves it false and preserves the level.
	out2 := mergeGpbackupOptions(cluster, &GpbackupOptionsRequest{CompressionLevel: 6})
	require.NotNil(t, out2)
	assert.False(t, out2.NoCompression, "omitted noCompression must remain false")
	assert.Equal(t, int32(6), out2.CompressionLevel)

	// (c) request-wins-false: cluster default sets noCompression=true, but a
	// request with noCompression=false (omitted) must override it to false.
	// This mirrors the WithStats/WithoutGlobals per-request override semantics
	// where the request value always wins, including the false direction.
	clusterDefaultNoCompression := newBackupEnabledCluster()
	clusterDefaultNoCompression.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{
		NoCompression:    true,
		CompressionLevel: 9,
	}
	out3 := mergeGpbackupOptions(clusterDefaultNoCompression,
		&GpbackupOptionsRequest{CompressionLevel: 6})
	require.NotNil(t, out3)
	assert.False(t, out3.NoCompression,
		"request noCompression=false must override cluster default true (request wins)")
	assert.Equal(t, int32(6), out3.CompressionLevel)
}

// TestHandleCreateBackup_NoCompressionOverride drives the full REST path: a POST
// with gpbackupOptions{noCompression:true,compressionLevel:6} must create a
// batchv1.Job (NOT a CronJob) whose gpbackup args contain --no-compression and
// do NOT contain --compression-level (compression level ignored, 73b).
func TestHandleCreateBackup_NoCompressionOverride(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Image = "cloudberry-backup:2.1.0"
	s := newTestServerWithBatch(cluster)

	body := `{"type":"full","databases":["mydb"],` +
		`"gpbackupOptions":{"noCompression":true,"compressionLevel":6}}`
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

	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "--no-compression")
	assert.NotContains(t, script, "--compression-level")
	assert.NotContains(t, script, "--compression-type")
}

// TestHandleCreateBackup_StandardOptions drives the full REST path for 73a:
// compressionLevel/Type/jobs/withStats/withoutGlobals/includeSchemas must all
// surface in the created Job's gpbackup args.
func TestHandleCreateBackup_StandardOptions(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Image = "cloudberry-backup:2.1.0"
	s := newTestServerWithBatch(cluster)

	body := `{"type":"full","databases":["mydb"],` +
		`"gpbackupOptions":{"compressionLevel":6,"compressionType":"zstd","jobs":4,` +
		`"withStats":true,"withoutGlobals":true,` +
		`"includeSchemas":["public","analytics"]}}`
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

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: jobName, Namespace: "default"}, job))

	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "--compression-level")
	assert.Contains(t, script, "--compression-type")
	assert.Contains(t, script, "zstd")
	assert.Contains(t, script, "--jobs")
	assert.Contains(t, script, "--with-stats")
	assert.Contains(t, script, "--without-globals")
	assert.Contains(t, script, "--include-schema")
	assert.Contains(t, script, "public")
	assert.Contains(t, script, "analytics")
}
