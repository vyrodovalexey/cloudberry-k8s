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

// migrateClusterWithFolder returns a backup-enabled S3 cluster with an explicit
// S3 folder, used to reproduce the Scenario 87 cross-cluster folder mismatch.
func migrateClusterWithFolder(name, bucket, folder string) *cbv1alpha1.CloudberryCluster {
	cluster := migrateCluster(name, bucket)
	cluster.Spec.Backup.Destination.S3.Folder = folder
	return cluster
}

// jobS3Folder returns the S3_FOLDER env value on the first container of the Job,
// or "" when absent.
func jobS3Folder(job *batchv1.Job) string {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "S3_FOLDER" {
			return env.Value
		}
	}
	return ""
}

func postMigrateRequest(source, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/"+source+"/migrate?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", source)
	return req
}

// TestHandleMigrate_CreatesSingleCoordinatedJob asserts the FINAL cross-cluster
// fix: the migration runs as ONE coordinated Job that captures the real gpbackup
// timestamp and feeds it to gprestore. The 202 envelope's backupJob/restoreJob/
// validationJob fields all reference that single migration Job.
func TestHandleMigrate_CreatesSingleCoordinatedJob(t *testing.T) {
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

	// The migration is a single Job; all phase fields reference it.
	migrationJobName, _ := resp["migrationJob"].(string)
	assert.Equal(t, util.MigrationJobName("src", timestamp), migrationJobName)
	assert.Equal(t, migrationJobName, resp["backupJob"])
	assert.Equal(t, migrationJobName, resp["restoreJob"])
	assert.Equal(t, migrationJobName, resp["validationJob"])

	// The single migration Job must exist and carry the migrate operation label.
	ctx := context.Background()
	migrationJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(ctx,
		client.ObjectKey{Name: migrationJobName, Namespace: "default"}, migrationJob))
	assert.Equal(t, util.BackupOperationMigrate,
		migrationJob.Labels[util.LabelBackupOperation])

	script := migrationJob.Spec.Template.Spec.Containers[0].Args[0]
	// Backup phase: requested table + single-data-file + dbname, and the REAL
	// gpbackup timestamp capture (the fix) — NOT the operator timestamp.
	assert.Contains(t, script, "--include-table")
	assert.Contains(t, script, "public.users")
	assert.Contains(t, script, "--single-data-file")
	assert.Contains(t, script, "--dbname")
	assert.Contains(t, script, "grep -oE 'Backup Timestamp = [0-9]{14}'")
	// Restore phase: fed the CAPTURED gpbackup timestamp ($7), redirect. It must
	// NOT use --truncate-table: the migration restores into a fresh empty target
	// DB (metadata + data), where --truncate-table would TRUNCATE not-yet-existing
	// objects during the pre-data metadata phase and abort with 42P01.
	assert.Contains(t, script, "gprestore --plugin-config")
	assert.Contains(t, script, "--timestamp \"$7\"")
	assert.NotContains(t, script, "--truncate-table",
		"migration restore must NOT use --truncate-table (fresh-DB restore)")
	assert.Contains(t, script, "--redirect-db")
	assert.Contains(t, script, "mydb")
	// truncate=true => clean target: the target DB is DROPped+recreated empty.
	assert.Contains(t, script, "clean+recreate target database (target coordinator)")
	assert.Contains(t, script, "DROP DATABASE IF EXISTS")
	// The restore must NOT pin the operator-chosen timestamp anywhere (the bug).
	assert.NotContains(t, script, "--timestamp '"+timestamp+"'")
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
	migrationJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.MigrationJobName("src", timestamp), Namespace: "default"}, migrationJob))
	script := migrationJob.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "otherdb")
}

func TestHandleMigrate_InvalidBody(t *testing.T) {
	source := migrateCluster("src", "shared-bucket")
	s := newTestServerWithBatch(source)
	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src", `{bad`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandleMigrate_RestoreUsesSourceFolder reproduces Scenario 87: the source
// cluster backs up to folder "scenario87-src" while the target's own folder is
// "scenario87-dst". The single migration Job's S3 plugin config "folder:" must
// point at the SOURCE folder for BOTH the backup and the (target) restore,
// otherwise gprestore looks under the target folder and fails with NotFound.
// This is the regression guard for the cross-cluster migration folder fix (spec
// 11 §Cross-Cluster Migration), preserved under the single-Job topology.
func TestHandleMigrate_RestoreUsesSourceFolder(t *testing.T) {
	source := migrateClusterWithFolder("src", "cloudberry-backups", "scenario87-src")
	target := migrateClusterWithFolder("dst", "cloudberry-backups", "scenario87-dst")
	s := newTestServerWithBatch(source, target)

	rec := httptest.NewRecorder()
	s.handleMigrate(rec, postMigrateRequest("src",
		`{"targetCluster":"dst","database":"mydb","tables":["public.users"]}`))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	timestamp, _ := resp[responseKeyTimestamp].(string)
	require.NotEmpty(t, timestamp)

	// The single migration Job must use the SOURCE folder (where gpbackup wrote)
	// for both the backup and the restore phases, NOT the target's own folder.
	migrationJob := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: util.MigrationJobName("src", timestamp), Namespace: "default"}, migrationJob))
	assert.Equal(t, "scenario87-src", jobS3Folder(migrationJob),
		"migration Job must read/write the source folder, not the target folder")
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
