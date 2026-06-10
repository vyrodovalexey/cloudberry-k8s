package api

// Tests for the empty-databases backup bug (live-deployment finding): a POST
// /backups without a databases array used to build a gpbackup Job with no
// --dbname, which failed at runtime with `required flag(s) "dbname" not set`.
// Defense in depth: the API rejects database-less requests with 400 (the CRD
// declares no default-database field), the builder refuses to render a broken
// command, and the handler guards a nil Job from the builder.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/httpjson"
)

// decodeErrorEnvelope decodes the unified {"error":{"code","message"}} body.
func decodeErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder) httpjson.ErrorEnvelope {
	t.Helper()
	var envelope httpjson.ErrorEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&envelope))
	return envelope
}

// TestHandleCreateBackup_EmptyDatabasesRejected pins the API-level guard: a
// create-backup request without databases is a 400 INVALID_REQUEST with an
// actionable envelope message, and NO Job is created.
func TestHandleCreateBackup_EmptyDatabasesRejected(t *testing.T) {
	bodies := []struct {
		name string
		body string
	}{
		{"empty body", ""},
		{"empty object", `{}`},
		{"type only", `{"type":"full"}`},
		{"empty databases array", `{"type":"full","databases":[]}`},
		{"options without databases", `{"gpbackupOptions":{"jobs":4}}`},
	}
	for _, tc := range bodies {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newBackupEnabledCluster()
			s := newTestServer(cluster)
			rec := httptest.NewRecorder()
			s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", tc.body))

			require.Equal(t, http.StatusBadRequest, rec.Code)
			envelope := decodeErrorEnvelope(t, rec)
			assert.Equal(t, errCodeInvalidRequest, envelope.Error.Code)
			assert.Contains(t, envelope.Error.Message, "at least one database")

			jobs := &batchv1.JobList{}
			require.NoError(t, s.k8sClient.List(t.Context(), jobs))
			assert.Empty(t, jobs.Items, "no Job may be created for a rejected request")
		})
	}
}

// TestHandleCreateBackup_WithDatabaseStillAccepted pins the existing happy
// path: an explicit database is accepted (202) and rendered into --dbname.
func TestHandleCreateBackup_WithDatabaseStillAccepted(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster",
		`{"type":"full","databases":["mydb"]}`))

	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jobName, ok := resp[responseKeyJob].(string)
	require.True(t, ok)
	script := fetchJobScript(t, s, jobName)
	assert.Contains(t, script, "'--dbname' 'mydb'")
}

// nilBackupJobBuilder simulates a builder refusing to render a backup Job so
// the handler's nil-Job guard (500, no panic) is exercised.
type nilBackupJobBuilder struct{ builder.ResourceBuilder }

func (nilBackupJobBuilder) BuildBackupJob(
	*cbv1alpha1.CloudberryCluster, *builder.BackupJobOptions,
) *batchv1.Job {
	return nil
}

// TestHandleCreateBackup_NilJobFromBuilder_Returns500 verifies the handler's
// defense-in-depth guard: a nil Job from the builder yields a clean 500
// envelope instead of a panic on Create.
func TestHandleCreateBackup_NilJobFromBuilder_Returns500(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)
	s.builder = nilBackupJobBuilder{ResourceBuilder: builder.NewBuilder()}

	rec := httptest.NewRecorder()
	s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster",
		`{"type":"full","databases":["mydb"]}`))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	envelope := decodeErrorEnvelope(t, rec)
	assert.Equal(t, errCodeInternal, envelope.Error.Code)
}

// TestHandleMigrate_EmptyDatabaseRejected pins the migration-path guard for
// the same bug class: the migration backup phase runs gpbackup, so a
// database-less migrate request is rejected with 400 before any Job exists.
func TestHandleMigrate_EmptyDatabaseRejected(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/src/migrate", strings.NewReader(`{"targetCluster":"dst"}`))
	req.SetPathValue("name", "src")
	s.handleMigrate(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	envelope := decodeErrorEnvelope(t, rec)
	assert.Equal(t, errCodeInvalidRequest, envelope.Error.Code)
	assert.Contains(t, envelope.Error.Message, "database is required")
}
