//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 79: Retention Cleanup, All Policies (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// retention cleanup path: the REST API delete-backup handler ->
// builder.BuildRetentionCleanupJob -> the (fake) Kubernetes client. They assert
// the resulting cleanup Job object actually persisted in the API server's k8s
// client carries the operation=cleanup label, the deterministic name and the
// gpbackman retention script (backup-info / backup-delete --cascade /
// backup-clean --older-than-days) with the FallbackToLogsOnError termination
// policy, covering:
//
//	79a/b/c : DELETE /backups/{ts} on a retention cluster creates a cleanup Job
//	          whose rendered script enforces fullCount (3), incrementalCount (10)
//	          and maxAge (30d) via real gpbackman commands.
//	79d     : the persisted cleanup Job is keyed off the requested timestamp
//	          (util.RetentionCleanupJobName) and carries the cleanup operation
//	          label so the operator's metrics loop can attribute its deletions.
//
// The HTTP boundary + auth + builder + k8s client wiring is real; only the
// cluster/k8s backend is a fake client (no live MPP cluster). This mirrors the
// scenario78 integration harness. The live count-based deletions are exercised
// by the e2e live script via coordinator-exec (the standalone cleanup Job pod
// does not carry the coordinator's gpbackup_history.db).
// ============================================================================

const (
	scenario79IntNamespace = "cloudberry-test"
	scenario79IntCluster   = "scenario79-s3"
	scenario79IntTS        = "20260608060000"
	scenario79APIPrefix    = "/api/v1alpha1"
)

// Scenario79IntegrationSuite drives the backup REST API against a fake k8s
// backend seeded with an all-policy retention cluster.
type Scenario79IntegrationSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	server *api.Server
	ctx    context.Context
}

func TestIntegration_Scenario79(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario79IntegrationSuite))
}

func (s *Scenario79IntegrationSuite) SetupTest() {
	s.ctx = context.Background()

	cluster := testutil.NewClusterBuilder(scenario79IntCluster, scenario79IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
			Incremental:      true,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "/backups",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests:  4,
					BackupMultipartChunksize:     "10MB",
					RestoreMaxConcurrentRequests: 4,
					RestoreMultipartChunksize:    "10MB",
				},
			},
		},
	}

	s.env = testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStore()
	// The DELETE backup endpoint requires PermissionAdmin.
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	s.server = api.NewServer(s.env.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)
}

// doRequest issues an authenticated (admin) request against the API server.
func (s *Scenario79IntegrationSuite) doRequest(
	method, path, body string,
) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth("admin", "adminpass")
	rec := httptest.NewRecorder()
	s.server.Handler().ServeHTTP(rec, req)
	return rec
}

// createdCleanupJob reads back the cleanup Job named by the requested timestamp
// from the fake k8s client.
func (s *Scenario79IntegrationSuite) createdCleanupJob(timestamp string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.RetentionCleanupJobName(scenario79IntCluster, timestamp),
		Namespace: scenario79IntNamespace,
	}, job), "the API-created cleanup Job must be persisted in k8s")
	return job
}

// decodeJobName extracts the "job" field from a delete-backup API response.
func (s *Scenario79IntegrationSuite) decodeJobName(rec *httptest.ResponseRecorder) string {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	job, ok := resp["job"].(string)
	require.True(s.T(), ok, "response must carry a job name: %v", resp)
	require.NotEmpty(s.T(), job)
	return job
}

// --- 79a/b/c/d: delete-backup creates the retention cleanup Job ---

// TestIntegration_Scenario79_DeleteBackupCreatesCleanupJob issues DELETE
// /backups/{ts} against the retention cluster and asserts the persisted cleanup
// Job carries the deterministic name + operation=cleanup label and renders the
// real gpbackman retention commands for all three policies.
func (s *Scenario79IntegrationSuite) TestIntegration_Scenario79_DeleteBackupCreatesCleanupJob() {
	rec := s.doRequest(http.MethodDelete,
		scenario79APIPrefix+"/clusters/"+scenario79IntCluster+
			"/backups/"+scenario79IntTS+"?namespace="+scenario79IntNamespace, "")
	require.Equal(s.T(), http.StatusAccepted, rec.Code,
		"DELETE /backups/{ts} should be accepted: %s", rec.Body.String())

	jobName := s.decodeJobName(rec)
	assert.Equal(s.T(),
		util.RetentionCleanupJobName(scenario79IntCluster, scenario79IntTS), jobName,
		"the response job name must be the deterministic cleanup Job name")

	job := s.createdCleanupJob(scenario79IntTS)
	assert.Equal(s.T(), util.BackupOperationCleanup,
		job.Labels[util.LabelBackupOperation],
		"the API-created cleanup Job must carry operation=cleanup")

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.Len(s.T(), container.Args, 1)
	script := container.Args[0]

	// 79a fullCount=3, 79b incrementalCount=10, 79c maxAge=30d — all enforced.
	assert.Contains(s.T(), script, "_gpbackman_timestamps 'full'",
		"the cleanup script must enforce full-count retention")
	assert.Contains(s.T(), script, "KEEP=3")
	assert.Contains(s.T(), script, "_gpbackman_timestamps 'incremental'",
		"the cleanup script must enforce incremental-count retention")
	assert.Contains(s.T(), script, "KEEP=10")
	assert.Contains(s.T(), script, "backup-clean --older-than-days 30",
		"the cleanup script must enforce maxAge retention")
	assert.Contains(s.T(), script, "backup-delete --timestamp \"$1\" --cascade",
		"excess backups must be deleted via backup-delete --cascade")
	assert.Contains(s.T(), script, "RETENTION_DELETED=",
		"the cleanup script must emit the RETENTION_DELETED marker")

	// FallbackToLogsOnError makes the deletion count recoverable from the log.
	assert.Equal(s.T(), corev1.TerminationMessageFallbackToLogsOnError,
		container.TerminationMessagePolicy)
}

// TestIntegration_Scenario79_DeleteBackupInvalidTimestamp asserts an invalid
// timestamp is rejected with 400 and creates no cleanup Job.
func (s *Scenario79IntegrationSuite) TestIntegration_Scenario79_DeleteBackupInvalidTimestamp() {
	rec := s.doRequest(http.MethodDelete,
		scenario79APIPrefix+"/clusters/"+scenario79IntCluster+
			"/backups/not-a-timestamp?namespace="+scenario79IntNamespace, "")
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"an invalid timestamp must be rejected: %s", rec.Body.String())

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), s.env.Client.List(s.ctx, jobs))
	for i := range jobs.Items {
		assert.NotEqual(s.T(), util.BackupOperationCleanup,
			jobs.Items[i].Labels[util.LabelBackupOperation],
			"a rejected delete must not create a cleanup Job")
	}
}
