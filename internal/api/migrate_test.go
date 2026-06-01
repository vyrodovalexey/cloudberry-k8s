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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// migrateCluster returns a backup-enabled cluster with the given name and S3 bucket.
func migrateCluster(name, bucket string) *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster(name, "default")
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           bucket,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
		},
	}
	return cluster
}

func postMigrateRequest(source, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/"+source+"/migrate?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", source)
	return req
}

func TestHandleMigrate_CreatesTwoJobsSharedTimestamp(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	target := migrateCluster("dst", "shared-bucket")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src",
		`{"targetCluster":"dst","database":"mydb","tables":["public.users"],"truncate":true}`))

	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	timestamp, _ := resp[responseKeyTimestamp].(string)
	require.NotEmpty(t, timestamp)
	assert.Equal(t, "src", resp["sourceCluster"])
	assert.Equal(t, "dst", resp["targetCluster"])

	backupJobName, _ := resp["backupJob"].(string)
	restoreJobName, _ := resp["restoreJob"].(string)
	assert.Equal(t, util.BackupJobName("src", timestamp), backupJobName)
	assert.Equal(t, util.RestoreJobName("dst", timestamp), restoreJobName)

	// Both Jobs must actually exist in their respective clusters.
	ctx := context.Background()
	backupJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(ctx,
		client.ObjectKey{Name: backupJobName, Namespace: "default"}, backupJob))
	restoreJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(ctx,
		client.ObjectKey{Name: restoreJobName, Namespace: "default"}, restoreJob))

	// The backup Job must include the requested table and single-data-file.
	backupScript := backupJob.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, backupScript, "--include-table")
	assert.Contains(t, backupScript, "public.users")
	assert.Contains(t, backupScript, "--single-data-file")

	// The restore Job must share the timestamp and truncate (args are shell-quoted).
	restoreScript := restoreJob.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, restoreScript, timestamp)
	assert.Contains(t, restoreScript, "--truncate-table")
	assert.Contains(t, restoreScript, "--redirect-db")
	assert.Contains(t, restoreScript, "mydb")
}

func TestHandleMigrate_DifferentBuckets(t *testing.T) {
	source := migrateCluster("src", "bucket-a")
	target := migrateCluster("dst", "bucket-b")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"dst","database":"mydb"}`))

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "same S3 bucket")
}

func TestHandleMigrate_MissingTarget(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"database":"mydb"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleMigrate_TargetNotFound(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"dst","database":"mydb"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleMigrate_SourceEqualsTarget(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"src"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleMigrate_TargetNotBackupEnabled(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	target := newTestCluster("dst", "default")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"dst","database":"mydb"}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "target cluster must have backup enabled")
}

func TestHandleMigrate_SourceNotBackupEnabled(t *testing.T) {
	source := newTestCluster("src", "default")
	target := migrateCluster("dst", "shared-bucket")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"dst","database":"mydb"}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "source cluster must have backup enabled")
}

func TestHandleMigrate_SourceNotFound(t *testing.T) {
	target := migrateCluster("dst", "shared-bucket")
	s := newTestServerWithBatch(target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{"targetCluster":"dst","database":"mydb"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleMigrate_InvalidDatabase(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src",
		`{"targetCluster":"dst","database":"bad name"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleMigrate_RedirectDb(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	target := migrateCluster("dst", "shared-bucket")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src",
		`{"targetCluster":"dst","database":"mydb","redirectDb":"otherdb"}`))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	timestamp, _ := resp[responseKeyTimestamp].(string)
	restoreJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.RestoreJobName("dst", timestamp), Namespace: "default"}, restoreJob))
	script := restoreJob.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "otherdb")
}

func TestHandleMigrate_InvalidBody(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)
	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{bad`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleCreateBackup_IncrementalArgs(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Image = "cloudberry-backup:2.1.0"
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{LeafPartitionData: true}
	s := newTestServerWithBatch(cluster)

	body := `{"type":"incremental","databases":["mydb"],` +
		`"gpbackupOptions":{"incremental":true,"leafPartitionData":true,` +
		`"fromTimestamp":"20260518020000"}}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/backups?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, _ := resp[responseKeyJob].(string)
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: jobName, Namespace: "default"}, job))

	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "--incremental")
	assert.Contains(t, script, "--leaf-partition-data")
	assert.Contains(t, script, "--from-timestamp")
	assert.Contains(t, script, "20260518020000")
}

func TestRestoreOptionsConflict(t *testing.T) {
	assert.Empty(t, restoreOptionsConflict(nil))
	assert.Empty(t, restoreOptionsConflict(&GprestoreOptionsRequest{DataOnly: true}))
	assert.NotEmpty(t, restoreOptionsConflict(
		&GprestoreOptionsRequest{DataOnly: true, MetadataOnly: true}))
}

func TestHandleRestoreBackup_DataMetadataConflict(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260519020000",
		`{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "mutually exclusive")
}

func TestHandleRestoreBackup_DataOnlyArgs(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServerWithBatch(cluster)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260519020000",
		`{"gprestoreOptions":{"dataOnly":true,"resizeCluster":true}}`))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, _ := resp["job"].(string)
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: jobName, Namespace: "default"}, job))
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "--data-only")
	assert.Contains(t, script, "--resize-cluster")
	assert.NotContains(t, script, "--metadata-only")
}

func TestMergeGprestoreOptions_DataMetadataResize(t *testing.T) {
	cluster := newBackupEnabledCluster()
	out := mergeGprestoreOptions(cluster, &GprestoreOptionsRequest{
		DataOnly:      true,
		ResizeCluster: true,
	})
	assert.True(t, out.DataOnly)
	assert.False(t, out.MetadataOnly)
	assert.True(t, out.ResizeCluster)
}
