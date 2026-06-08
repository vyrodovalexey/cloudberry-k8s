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
	"k8s.io/apimachinery/pkg/types"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 78: Incremental Backup Lifecycle (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// incremental backup path: the REST API handler -> buildBackupJobOptions ->
// builder.BuildBackupJob -> the (fake) Kubernetes client. They assert the
// resulting Job object actually persisted in the API server's k8s client carries
// the correct gpbackup args and the avsoft.io/backup-type label, covering:
//
//	78a : POST /backups on an incremental cluster creates a Job whose gpbackup
//	      args carry `--incremental --leaf-partition-data` (each exactly once) and
//	      whose metadata + pod template carry backup-type=incremental.
//	78c : POST /backups with gpbackupOptions.fromTimestamp creates a Job whose
//	      gpbackup args carry `--from-timestamp <full-ts>` (pinned base).
//
// The HTTP boundary + auth + builder + k8s client wiring is real; only the
// cluster/k8s backend is a fake client (no live MPP cluster). This mirrors the
// existing api_integration_test.go harness.
// ============================================================================

const (
	scenario78IntNamespace = "cloudberry-test"
	scenario78IntCluster   = "scenario78-s3"
	scenario78IntFullTS    = "20260608060000"
	scenario78APIPrefix    = "/api/v1alpha1"
)

// Scenario78IntegrationSuite drives the backup REST API against a fake k8s
// backend seeded with an incremental-enabled cluster.
type Scenario78IntegrationSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	server *api.Server
	ctx    context.Context
}

func TestIntegration_Scenario78(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario78IntegrationSuite))
}

func (s *Scenario78IntegrationSuite) SetupTest() {
	s.ctx = context.Background()

	cluster := testutil.NewClusterBuilder(scenario78IntCluster, scenario78IntNamespace).
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
			},
		},
	}

	s.env = testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	s.server = api.NewServer(s.env.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)
}

// doRequest issues an authenticated request against the API server.
func (s *Scenario78IntegrationSuite) doRequest(
	method, path, body string,
) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth("operator", "operatorpass")
	rec := httptest.NewRecorder()
	s.server.Handler().ServeHTTP(rec, req)
	return rec
}

// createdBackupJobScript reads back the Job named by the API response timestamp
// from the fake k8s client and returns the rendered gpbackup container script.
func (s *Scenario78IntegrationSuite) createdBackupJob(timestamp string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupJobName(scenario78IntCluster, timestamp),
		Namespace: scenario78IntNamespace,
	}, job), "the API-created backup Job must be persisted in k8s")
	return job
}

// decodeTimestamp extracts the "timestamp" field from a backup API response.
func (s *Scenario78IntegrationSuite) decodeTimestamp(rec *httptest.ResponseRecorder) string {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	ts, ok := resp["timestamp"].(string)
	require.True(s.T(), ok, "response must carry a timestamp: %v", resp)
	require.NotEmpty(s.T(), ts)
	return ts
}

// --- 78a: incremental backup via the REST API ---

// TestIntegration_Scenario78_IncrementalBackupArgs posts a backup against the
// incremental cluster and asserts the persisted Job's gpbackup args carry
// `--incremental --leaf-partition-data` once each AND the Job is labelled
// backup-type=incremental on metadata + pod template.
func (s *Scenario78IntegrationSuite) TestIntegration_Scenario78_IncrementalBackupArgs() {
	body := `{"databases":["mydb"]}`
	rec := s.doRequest(http.MethodPost,
		scenario78APIPrefix+"/clusters/"+scenario78IntCluster+
			"/backups?namespace="+scenario78IntNamespace, body)
	require.Equal(s.T(), http.StatusAccepted, rec.Code,
		"POST /backups should be accepted: %s", rec.Body.String())

	ts := s.decodeTimestamp(rec)
	job := s.createdBackupJob(ts)

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Args, "\n")
	assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"),
		"API-created incremental Job must render --incremental once")
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
		"API-created incremental Job must force --leaf-partition-data once")

	assert.Equal(s.T(), util.BackupTypeIncremental, job.Labels[util.LabelBackupType],
		"API-created incremental Job metadata must carry backup-type=incremental")
	assert.Equal(s.T(), util.BackupTypeIncremental,
		job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType],
		"API-created incremental Job pod template must carry backup-type=incremental")
}

// TestIntegration_Scenario78_PinnedFromTimestamp posts an incremental backup with
// gpbackupOptions.fromTimestamp and asserts the persisted Job's gpbackup args
// carry `--from-timestamp <full-ts>` (the pinned-base override, 78c).
func (s *Scenario78IntegrationSuite) TestIntegration_Scenario78_PinnedFromTimestamp() {
	body := `{"type":"incremental","databases":["mydb"],` +
		`"gpbackupOptions":{"incremental":true,"fromTimestamp":"` + scenario78IntFullTS + `"}}`
	rec := s.doRequest(http.MethodPost,
		scenario78APIPrefix+"/clusters/"+scenario78IntCluster+
			"/backups?namespace="+scenario78IntNamespace, body)
	require.Equal(s.T(), http.StatusAccepted, rec.Code,
		"POST /backups (pinned) should be accepted: %s", rec.Body.String())

	ts := s.decodeTimestamp(rec)
	job := s.createdBackupJob(ts)

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Args, "\n")
	assert.Contains(s.T(), script, "'--from-timestamp'",
		"a pinned incremental Job must render --from-timestamp")
	assert.Contains(s.T(), script, "'"+scenario78IntFullTS+"'",
		"a pinned incremental Job must render the exact pinned timestamp")
	assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"))
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"))
	assert.Equal(s.T(), util.BackupTypeIncremental, job.Labels[util.LabelBackupType])
}
