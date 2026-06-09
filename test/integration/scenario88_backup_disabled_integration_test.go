//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 88: Backup Disabled / No Schedule — integration
// ============================================================================
//
// This integration test wires the backup API path end-to-end against a real
// operator API: it starts the REAL api.Server behind a live httptest.Server
// (real HTTP transport + router + auth/RBAC) over a fake Kubernetes client, then
// drives the operator using the SAME internal/ctl.OperatorClient that
// cloudberry-ctl uses. An Admin Basic identity stands in for the live OIDC
// bearer (Scenario 88 has no RBAC dimension, so no Operator identity is seeded).
//
// It asserts the verified backup-disabled / no-schedule behavior:
//   - 88a-4 disabled cluster POST /backups => 400 BACKUP_NOT_ENABLED;
//   - 88a-4 nil-backup-spec cluster POST /backups => 400 BACKUP_NOT_ENABLED;
//   - 88a-5 disabled GET /backups => 200 with "enabled":false, empty history,
//     total 0 (GAP-2: the list endpoint NEVER errors and emits an "enabled"
//     boolean rather than a BACKUP_NOT_ENABLED state);
//   - 88a-6 disabled GET /backups/schedule => 200 {scheduled:false, enabled:false};
//   - 88b-2/88b-3 enabled + empty-schedule POST /backups => 202 + a backup Job
//     (operation=backup) actually exists in the fake client (read FROM the
//     envelope's "job" field, never hardcoded);
//   - 88b-4 enabled + empty-schedule GET /backups/schedule => 200 {scheduled:false,
//     enabled:true} (no CronJob; the API path does not run reconcile).
//
// GAP-1: the backup SA/Role are CHART-level (cloudberry-backup-sa /
// cloudberry-backup-role, gated by `backup.rbac.create`) and shared in the
// operator namespace; they are NOT per-cluster, so 88a asserts only the
// per-cluster effects (400/202/scheduled:false/enabled flag) here.
// ============================================================================

const (
	scenario88IntNamespace = "default"
	scenario88IntCluster   = "s88"
	scenario88IntPrefix    = "/api/v1alpha1"
	scenario88IntBucket    = "cloudberry-backups"
	scenario88IntRateLimit = 1000
	scenario88IntAdminUser = "adminuser"
	scenario88IntAdminPass = "adminpass"
)

// Scenario88BackupDisabledIntegrationSuite drives the CLI client against a live
// api.Server backed by a fake k8s client.
type Scenario88BackupDisabledIntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	srv    *httptest.Server
	server *api.Server
	client client.Client
	cli    *ctl.OperatorClient
}

func TestIntegration_Scenario88(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario88BackupDisabledIntegrationSuite))
}

func (s *Scenario88BackupDisabledIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario88BackupDisabledIntegrationSuite) TearDownTest() {
	if s.srv != nil {
		s.srv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario88IntBuildCluster builds a Running cluster parameterised by the backup
// enabled flag + schedule. nilBackup=true leaves Spec.Backup nil (88a-3 variant);
// otherwise a valid S3-destination spec is attached.
func scenario88IntBuildCluster(name string, nilBackup, enabled bool, schedule string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, scenario88IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	if nilBackup {
		return cluster
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  enabled,
		Image:    "cloudberry-backup:2.1.0",
		Schedule: schedule,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         scenario88IntBucket,
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "scenario88",
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
	return cluster
}

// boot seeds the given clusters into a fake client and starts the API server
// behind a live httptest.Server with an Admin credential + a configured CLI.
func (s *Scenario88BackupDisabledIntegrationSuite) boot(clusters ...*cbv1alpha1.CloudberryCluster) {
	objs := make([]client.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario88IntAdminUser, scenario88IntAdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger,
		scenario88IntRateLimit).
		WithClientset(k8sfake.NewSimpleClientset())
	s.srv = httptest.NewServer(s.server.Handler())

	s.cli = ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.srv.URL,
		AuthMethod: "basic",
		Username:   scenario88IntAdminUser,
		Password:   scenario88IntAdminPass,
		Timeout:    30 * time.Second,
	})
}

// scenario88IntPath builds a /clusters/{name}{suffix} path with the namespace query.
func scenario88IntPath(name, suffix string) string {
	return fmt.Sprintf("/clusters/%s%s?namespace=%s",
		url.PathEscape(name), suffix, scenario88IntNamespace)
}

func (s *Scenario88BackupDisabledIntegrationSuite) getJob(name string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario88IntNamespace}, job))
	return job
}

// rawRequest issues an arbitrary method/path with the Admin Basic identity over
// real HTTP and returns the status code + response body string.
func (s *Scenario88BackupDisabledIntegrationSuite) rawRequest(method, name, suffix, body string) (int, string) {
	reqURL := s.srv.URL + scenario88IntPrefix + scenario88IntPath(name, suffix)
	req, err := http.NewRequestWithContext(s.ctx, method, reqURL, strings.NewReader(body))
	require.NoError(s.T(), err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(scenario88IntAdminUser, scenario88IntAdminPass)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// --- 88a-4: disabled cluster POST /backups => 400 BACKUP_NOT_ENABLED ---

func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_DisabledCreate400() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, false, false, "0 2 * * *"))
	code, body := s.rawRequest(http.MethodPost, scenario88IntCluster, "/backups", `{}`)
	assert.Equal(s.T(), 400, code)
	assert.Contains(s.T(), body, "BACKUP_NOT_ENABLED")
	assert.Contains(s.T(), body, "backup is not enabled for this cluster")
}

// --- 88a-4 variant: nil backup spec POST /backups => 400 BACKUP_NOT_ENABLED ---

func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_NilBackupCreate400() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, true, false, ""))
	code, body := s.rawRequest(http.MethodPost, scenario88IntCluster, "/backups", `{}`)
	assert.Equal(s.T(), 400, code)
	assert.Contains(s.T(), body, "BACKUP_NOT_ENABLED")
}

// --- 88a-5: disabled GET /backups => 200 + enabled:false (GAP-2) ---

// TestIntegration_Scenario88_DisabledList200Disabled asserts the REAL list
// behavior. GAP-2: handleListBackups is unconditional 200 and surfaces the
// disabled state through the "enabled" boolean (false here) rather than a
// BACKUP_NOT_ENABLED error; the history is empty and total is 0.
func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_DisabledList200Disabled() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, false, false, "0 2 * * *"))
	resp, err := s.cli.Get(s.ctx, scenario88IntPath(scenario88IntCluster, "/backups"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), 200, resp.StatusCode)

	enabled, ok := resp.Body["enabled"].(bool)
	require.True(s.T(), ok, "list response must carry a boolean \"enabled\" field")
	assert.False(s.T(), enabled, "enabled must be false for a disabled cluster")

	total, _ := resp.Body["total"].(float64)
	assert.Equal(s.T(), float64(0), total, "total backups must be 0")
	backups, _ := resp.Body["backups"].([]interface{})
	assert.Empty(s.T(), backups, "backups history must be empty")
	assert.Empty(s.T(), resp.Body["lastBackupStatus"], "lastBackupStatus must be empty")
}

// --- 88a-6: disabled GET /backups/schedule => 200 {scheduled:false, enabled:false} ---

func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_DisabledScheduleFalse() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, false, false, "0 2 * * *"))
	resp, err := s.cli.Get(s.ctx, scenario88IntPath(scenario88IntCluster, "/backups/schedule"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), 200, resp.StatusCode)

	scheduled, _ := resp.Body["scheduled"].(bool)
	assert.False(s.T(), scheduled, "scheduled must be false when no CronJob exists")
	enabled, _ := resp.Body["enabled"].(bool)
	assert.False(s.T(), enabled, "enabled must be false for a disabled cluster")
	assert.Nil(s.T(), resp.Body["nextRun"], "no nextRun when not scheduled")
}

// --- 88b-2 + 88b-3: enabled + empty schedule POST /backups => 202 + Job exists ---

// TestIntegration_Scenario88_EnabledEmptyScheduleCreate202 proves on-demand
// backup works WITHOUT a schedule: the create returns 202 and the named backup
// Job (read FROM the envelope's "job" field) actually exists in the fake client
// with the operation=backup label.
func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_EnabledEmptyScheduleCreate202() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, false, true, ""))
	resp, err := s.cli.Post(s.ctx, scenario88IntPath(scenario88IntCluster, "/backups"),
		map[string]interface{}{})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 202, resp.StatusCode)
	assert.Equal(s.T(), "backup started", resp.Body["status"])

	jobName, _ := resp.Body["job"].(string)
	require.NotEmpty(s.T(), jobName, "202 envelope must carry the backup Job name")

	job := s.getJob(jobName)
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation],
		"the on-demand Job must carry the operation=backup label")
}

// --- 88b-4: enabled + empty schedule GET /backups/schedule => scheduled:false, enabled:true ---

func (s *Scenario88BackupDisabledIntegrationSuite) TestIntegration_Scenario88_EnabledEmptyScheduleScheduleFalse() {
	s.boot(scenario88IntBuildCluster(scenario88IntCluster, false, true, ""))
	resp, err := s.cli.Get(s.ctx, scenario88IntPath(scenario88IntCluster, "/backups/schedule"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), 200, resp.StatusCode)

	scheduled, _ := resp.Body["scheduled"].(bool)
	assert.False(s.T(), scheduled,
		"scheduled must be false for an enabled cluster with an empty schedule (no CronJob)")
	enabled, _ := resp.Body["enabled"].(bool)
	assert.True(s.T(), enabled, "enabled must be true for an enabled cluster")
}
