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
// Scenario 74: Restore with all gprestore Options — api-level round-trip
// ============================================================================
//
// This white-box test drives the full REST restore path (handleRestoreBackup)
// for Scenario 74: a POST with the complete gprestoreOptions body must create a
// batchv1.Job (NOT a CronJob), labelled as a restore operation, whose gprestore
// args contain the full enabled flag set and OMIT the false bools
// (--with-globals, --truncate-table). It mirrors the round-trip style in
// backup_nocompression_test.go and exercises buildRestoreJobOptions +
// mergeGprestoreOptions end-to-end through the handler.
// ============================================================================

// TestHandleRestoreBackup_Scenario74Args drives the full restore REST path with
// the Scenario 74 option set and asserts the created Job's gprestore args.
func TestHandleRestoreBackup_Scenario74Args(t *testing.T) {
	cluster := newBackupEnabledCluster()
	cluster.Spec.Backup.Image = "cloudberry-backup:2.1.0"
	s := newTestServerWithBatch(cluster)

	const timestamp = "20260101010101"
	body := `{"databases":["mydb"],"gprestoreOptions":{` +
		`"jobs":4,"redirectDb":"mydb_restored","redirectSchema":"restored",` +
		`"createDb":true,"includeSchemas":["public","analytics"],` +
		`"includeTables":["public.users","public.orders"],` +
		`"withGlobals":false,"withStats":true,"runAnalyze":true,` +
		`"onErrorContinue":true,"truncateTable":false}}`

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/backups/"+timestamp+"/restore?namespace=default",
		strings.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", timestamp)
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, _ := resp[responseKeyJob].(string)
	require.NotEmpty(t, jobName)

	// The on-demand restore POST creates a Job DIRECTLY (not a CronJob).
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		client.ObjectKey{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])

	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	// Enabled flags must all surface.
	assert.Contains(t, script, "--timestamp")
	assert.Contains(t, script, timestamp)
	assert.Contains(t, script, "--jobs")
	assert.Contains(t, script, "--redirect-db")
	assert.Contains(t, script, "mydb_restored")
	assert.Contains(t, script, "--redirect-schema")
	assert.Contains(t, script, "restored")
	assert.Contains(t, script, "--create-db")
	// gprestore forbids --include-schema together with --include-table. With
	// both includeSchemas and includeTables supplied, the operator emits the
	// more specific --include-table (table-level precedence) and OMITS
	// --include-schema so the gprestore invocation stays valid.
	assert.Contains(t, script, "--include-table")
	assert.Contains(t, script, "public.users")
	assert.Contains(t, script, "public.orders")
	assert.NotContains(t, script, "--include-schema")
	// gprestore forbids --run-analyze together with --with-stats. With both
	// withStats and runAnalyze supplied, the operator emits --run-analyze
	// (precedence: ANALYZE supersedes restoring backed-up stats) and OMITS
	// --with-stats so the gprestore invocation stays valid.
	assert.Contains(t, script, "--run-analyze")
	assert.NotContains(t, script, "--with-stats")
	assert.Contains(t, script, "--on-error-continue")

	// The two false bools must NOT emit their flag.
	assert.NotContains(t, script, "--with-globals")
	assert.NotContains(t, script, "--truncate-table")
}
