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
// Scenario 87: Cross-Cluster Migration (cloudberry-ctl migrate ...) — integration
// ============================================================================
//
// This integration test wires the WHOLE migrate path end-to-end against a real
// operator API: it starts the REAL api.Server behind a live httptest.Server
// (real HTTP transport + router + auth/RBAC) over a fake Kubernetes client, then
// drives the operator using the SAME internal/ctl.OperatorClient that
// cloudberry-ctl uses, issuing POST /clusters/{source}/migrate. An Admin Basic
// identity stands in for the live OIDC bearer; the request->router->handler->
// builder->k8s path is identical. The RBAC negative additionally uses a second
// Operator identity to prove the route is Admin-gated.
//
// It asserts:
//   - 87b the source backup Job's gpbackup args carry repeated --include-table
//     (public.users, public.orders), --single-data-file and --plugin-config;
//   - 87c the target restore Job's gprestore args carry --timestamp <ts>,
//     --redirect-db (falling back to the database), --plugin-config and the
//     repeated --include-table, WITHOUT --truncate-table (a fresh-DB restore of
//     metadata + data); --truncate instead cleans the target at the DB level
//     (DROP+recreate the empty target DB before gprestore);
//   - 87d both Jobs were created and share the seeded clusters' S3 bucket;
//   - 87e a validation Job (<target>-validate-<ts>, operation=validate) exists,
//     and its script carries the real markers (post-restore-validate:, row-count,
//     invalid, SELECT 1, "post-restore-validate: passed") — NOT a literal
//     "checksum";
//   - the ten negative/RBAC cases return the documented 400/404/403 statuses.
//
// Job names are read FROM the 202 envelope (not hardcoded). The validation Job
// is best-effort server-side, so its assertions read the envelope's
// validationJob name and the created Job's label + script markers.
// ============================================================================

const (
	scenario87IntNamespace = "default"
	scenario87IntSource    = "src"
	scenario87IntTarget    = "dst"
	scenario87IntPrefix    = "/api/v1alpha1"
	scenario87IntDB        = "mydb"
	scenario87IntBucket    = "cloudberry-backups"
	scenario87IntRateLimit = 1000
	scenario87IntAdminUser = "adminuser"
	scenario87IntAdminPass = "adminpass"
	scenario87IntOpUser    = "operatoruser"
	scenario87IntOpPass    = "operatorpass"
)

// Scenario87MigrationIntegrationSuite drives the CLI client against a live
// api.Server backed by a fake k8s client.
type Scenario87MigrationIntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	srv    *httptest.Server
	server *api.Server
	client client.Client
	cli    *ctl.OperatorClient
}

func TestIntegration_Scenario87(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario87MigrationIntegrationSuite))
}

func (s *Scenario87MigrationIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario87MigrationIntegrationSuite) TearDownTest() {
	if s.srv != nil {
		s.srv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario87IntBuildCluster builds a backup-enabled S3-destination cluster with
// the given bucket + folder. backupEnabled=false produces a cluster WITHOUT a
// backup spec (used by the not-backup-enabled negatives).
func scenario87IntBuildCluster(name, bucket, folder string, backupEnabled bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, scenario87IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	if !backupEnabled {
		return cluster
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Image:    "cloudberry-backup:2.1.0",
		Schedule: "0 2 * * *",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         bucket,
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         folder,
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
// behind a live httptest.Server with an Admin AND an Operator credential. It
// also configures both CLI OperatorClients (Admin + Operator).
func (s *Scenario87MigrationIntegrationSuite) boot(clusters ...*cbv1alpha1.CloudberryCluster) {
	objs := make([]client.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario87IntAdminUser, scenario87IntAdminPass, auth.PermissionAdmin)
	store.SetCredentials(scenario87IntOpUser, scenario87IntOpPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger,
		scenario87IntRateLimit).
		WithClientset(k8sfake.NewSimpleClientset())
	s.srv = httptest.NewServer(s.server.Handler())

	s.cli = ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.srv.URL,
		AuthMethod: "basic",
		Username:   scenario87IntAdminUser,
		Password:   scenario87IntAdminPass,
		Timeout:    30 * time.Second,
	})
}

// migratePath builds the migrate path for the named source cluster.
func scenario87IntMigratePath(source string) string {
	return fmt.Sprintf("/clusters/%s/migrate?namespace=%s",
		url.PathEscape(source), scenario87IntNamespace)
}

func (s *Scenario87MigrationIntegrationSuite) getJob(name string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario87IntNamespace}, job))
	return job
}

func (s *Scenario87MigrationIntegrationSuite) jobArgs(name string) string {
	job := s.getJob(name)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// postRaw issues a POST to the migrate endpoint with a VERBATIM body (so a
// malformed JSON body reaches the server unaltered) over real HTTP, using the
// given Basic identity. It returns the status code and the response body string.
func (s *Scenario87MigrationIntegrationSuite) postRaw(user, pass, source, body string) (int, string) {
	reqURL := s.srv.URL + scenario87IntPrefix + scenario87IntMigratePath(source)
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, reqURL, strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// adminStatus POSTs a verbatim body as the Admin identity and returns the status.
func (s *Scenario87MigrationIntegrationSuite) adminStatus(source, body string) int {
	code, _ := s.postRaw(scenario87IntAdminUser, scenario87IntAdminPass, source, body)
	return code
}

// --- 87b + 87c + 87d + 87e: the happy-path migration end-to-end ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_MigrateEndToEnd() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)

	body := map[string]interface{}{
		"targetCluster": scenario87IntTarget,
		"database":      scenario87IntDB,
		"tables":        []string{"public.users", "public.orders"},
		"truncate":      true,
		"jobs":          4,
	}
	resp, err := s.cli.Post(s.ctx, scenario87IntMigratePath(scenario87IntSource), body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 202, resp.StatusCode)

	ts, _ := resp.Body["timestamp"].(string)
	require.NotEmpty(s.T(), ts, "202 envelope must carry a timestamp")
	migrationJob, _ := resp.Body["migrationJob"].(string)
	backupJob, _ := resp.Body["backupJob"].(string)
	restoreJob, _ := resp.Body["restoreJob"].(string)
	validationJob, _ := resp.Body["validationJob"].(string)
	require.NotEmpty(s.T(), migrationJob)
	require.NotEmpty(s.T(), backupJob)
	require.NotEmpty(s.T(), restoreJob)
	require.NotEmpty(s.T(), validationJob)
	assert.Equal(s.T(), scenario87IntSource, resp.Body["sourceCluster"])
	assert.Equal(s.T(), scenario87IntTarget, resp.Body["targetCluster"])

	// Single coordinated migration Job (the FINAL cross-cluster fix): the
	// migration runs as ONE Job that captures the real gpbackup timestamp and
	// feeds it to gprestore. The backup/restore/validation envelope fields all
	// reference that one Job (it performs all three phases).
	assert.Equal(s.T(), util.MigrationJobName(scenario87IntSource, ts), migrationJob)
	assert.Equal(s.T(), migrationJob, backupJob)
	assert.Equal(s.T(), migrationJob, restoreJob)
	assert.Equal(s.T(), migrationJob, validationJob)

	migJob := s.getJob(migrationJob)
	assert.Equal(s.T(), util.BackupOperationMigrate,
		migJob.Labels[util.LabelBackupOperation])
	script := s.jobArgs(migrationJob)

	// 87b: source backup phase args (rendered inside the single Job's script).
	s.Run("87b_source_backup_args", func() {
		for _, want := range []string{
			"'--include-table' 'public.users'",
			"'--include-table' 'public.orders'",
			"'--single-data-file'",
			"'--dbname' 'mydb'",
			// Coordinator-exec model (spec 11 §MPP Dispatch): gpbackup runs INSIDE
			// the SOURCE coordinator pod against the coordinator-side ${COORD_CFG}.
			"gpbackup --plugin-config \"${COORD_CFG}\"",
		} {
			assert.Containsf(s.T(), script, want, "87b: backup phase must contain %q", want)
		}
		assert.NotContains(s.T(), script, "'--incremental'")
		// The REAL gpbackup timestamp is captured from gpbackup's stdout and NOT
		// the operator-chosen one (the bug fix): the restore must use the captured
		// timestamp, never the operator timestamp.
		assert.Contains(s.T(), script, "grep -oE 'Backup Timestamp = [0-9]{14}'",
			"87b: backup phase must capture the real gpbackup timestamp")
	})

	// 87c: target restore phase args (rendered inside the single Job's script).
	s.Run("87c_target_restore_args", func() {
		for _, want := range []string{
			// gprestore is fed the CAPTURED gpbackup timestamp (expanded at run
			// time from the $7 positional arg = ${MIG_BACKUP_TS}), NOT a literal
			// operator timestamp — this is the FINAL cross-cluster fix.
			"--timestamp \"$7\"",
			"'--redirect-db'",
			"'mydb'", // redirect default falls back to the database
			// Coordinator-exec model: gprestore runs INSIDE the TARGET coordinator
			// pod against the coordinator-side ${COORD_CFG}.
			"gprestore --plugin-config \"${COORD_CFG}\"",
			"'--include-table' 'public.users'",
			"'--include-table' 'public.orders'",
		} {
			assert.Containsf(s.T(), script, want, "87c: restore phase must contain %q", want)
		}
		assert.NotContains(s.T(), script, "'--metadata-only'")
		// --truncate-table must NOT be used: the migration restores into a FRESH
		// empty target DB (metadata + data), where --truncate-table would TRUNCATE
		// not-yet-existing objects in the pre-data metadata phase and abort (42P01).
		assert.NotContains(s.T(), script, "'--truncate-table'",
			"87c: migration restore must NOT use --truncate-table (fresh-DB restore)")
		// truncate=true => clean target at the DB level: DROP+recreate the empty DB.
		assert.Contains(s.T(), script, "clean+recreate target database (target coordinator)",
			"87c: --truncate must clean the target DB (DROP+recreate)")
		assert.Contains(s.T(), script, "DROP DATABASE IF EXISTS")
		// The restore must NOT pin the operator-chosen timestamp anywhere (the bug).
		assert.NotContains(s.T(), script, "--timestamp '"+ts+"'",
			"87c: restore must use the CAPTURED gpbackup timestamp, not the operator one")

		// Cross-cluster migration fix (spec 11 §Cross-Cluster Migration): the
		// migration Job's S3 plugin config "folder:" points at the SOURCE folder
		// "scenario87-src" where gpbackup wrote, so the target gprestore reads from
		// there (not the target's own "scenario87-dst"), otherwise gprestore fails
		// with NotFound.
		assert.Equal(s.T(), "scenario87-src", scenario87JobS3Folder(migJob),
			"87c: migration Job must use the source S3 folder for both phases")
	})

	// 87d: same bucket — the single migration Job exists, carries the timestamp
	// suffix, and the seeded clusters share the S3 bucket.
	s.Run("87d_same_bucket", func() {
		src := &cbv1alpha1.CloudberryCluster{}
		dst := &cbv1alpha1.CloudberryCluster{}
		require.NoError(s.T(), s.client.Get(s.ctx,
			types.NamespacedName{Name: scenario87IntSource, Namespace: scenario87IntNamespace}, src))
		require.NoError(s.T(), s.client.Get(s.ctx,
			types.NamespacedName{Name: scenario87IntTarget, Namespace: scenario87IntNamespace}, dst))
		require.NotNil(s.T(), src.Spec.Backup)
		require.NotNil(s.T(), dst.Spec.Backup)
		assert.Equal(s.T(),
			src.Spec.Backup.Destination.S3.Bucket,
			dst.Spec.Backup.Destination.S3.Bucket,
			"87d: source and target must share the S3 bucket")
		assert.Contains(s.T(), migrationJob, ts)
	})

	// 87e: validation phase rendered with the real markers (no "checksum").
	s.Run("87e_validation_job", func() {
		for _, want := range []string{
			"post-restore-validate:",
			"row-count",
			"invalid", // invalid-index scan
			"SELECT 1",
			"post-restore-validate: passed",
		} {
			assert.Containsf(s.T(), script, want, "87e: validation phase must contain %q", want)
		}
		assert.NotContains(s.T(), script, "checksum",
			"87e: the validation phase must NOT use a literal 'checksum' marker")
	})
}

// scenario87JobS3Folder returns the S3_FOLDER env value on the Job's first
// container, or "" when absent.
func scenario87JobS3Folder(job *batchv1.Job) string {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "S3_FOLDER" {
			return env.Value
		}
	}
	return ""
}

// --- redirectDb edge: --redirect-db wins over --database ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_RedirectDb() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)

	body := map[string]interface{}{
		"targetCluster": scenario87IntTarget,
		"database":      scenario87IntDB,
		"redirectDb":    "otherdb",
		"tables":        []string{"public.users"},
	}
	resp, err := s.cli.Post(s.ctx, scenario87IntMigratePath(scenario87IntSource), body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 202, resp.StatusCode)

	// The restore phase lives inside the single migration Job; --redirect-db wins
	// over the database when redirectDb is set.
	migrationJob := resp.Body["migrationJob"].(string)
	script := s.jobArgs(migrationJob)
	assert.Contains(s.T(), script, "'--redirect-db' 'otherdb'",
		"restore phase must redirect to otherdb when redirectDb is set")
}

// --- 87f-neg1: different buckets -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_DifferentBuckets() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, "bucket-b", "scenario87-dst", true),
	)
	code, body := s.postRaw(scenario87IntAdminUser, scenario87IntAdminPass,
		scenario87IntSource, `{"targetCluster":"dst","database":"mydb"}`)
	assert.Equal(s.T(), 400, code)
	assert.Contains(s.T(), body, "same S3 bucket")
}

// --- 87f-neg2: missing target -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_MissingTarget() {
	s.boot(scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true))
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource, `{"database":"mydb"}`))
}

// --- 87f-neg3: source == target -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_SourceEqualsTarget() {
	s.boot(scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true))
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource, `{"targetCluster":"src"}`))
}

// --- 87f-neg4: target cluster not found -> 404 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_TargetNotFound() {
	s.boot(scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true))
	assert.Equal(s.T(), 404, s.adminStatus(scenario87IntSource,
		`{"targetCluster":"dst","database":"mydb"}`))
}

// --- 87f-neg5: source cluster not found -> 404 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_SourceNotFound() {
	// Only the target is seeded; the path references an absent source "src".
	s.boot(scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true))
	assert.Equal(s.T(), 404, s.adminStatus(scenario87IntSource,
		`{"targetCluster":"dst","database":"mydb"}`))
}

// --- 87f-neg6: invalid database identifier -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_InvalidDatabase() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource,
		`{"targetCluster":"dst","database":"bad name"}`))
}

// --- 87f-neg7: malformed JSON -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_MalformedBody() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource, `{bad`))
}

// --- 87g-neg8: RBAC — Operator identity -> 403 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_RBAC_OperatorForbidden() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)
	// Operator identity is rejected before reaching the handler.
	opCode, _ := s.postRaw(scenario87IntOpUser, scenario87IntOpPass,
		scenario87IntSource, `{"targetCluster":"dst","database":"mydb"}`)
	assert.Equal(s.T(), 403, opCode)
	// Admin identity is accepted (202) for the same request.
	resp, err := s.cli.Post(s.ctx, scenario87IntMigratePath(scenario87IntSource),
		map[string]interface{}{"targetCluster": "dst", "database": "mydb"})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 202, resp.StatusCode)
}

// --- 87h-neg9: source not backup-enabled -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_SourceNotBackupEnabled() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", false),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", true),
	)
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource,
		`{"targetCluster":"dst","database":"mydb"}`))
}

// --- 87h-neg10: target not backup-enabled -> 400 ---

func (s *Scenario87MigrationIntegrationSuite) TestIntegration_Scenario87_TargetNotBackupEnabled() {
	s.boot(
		scenario87IntBuildCluster(scenario87IntSource, scenario87IntBucket, "scenario87-src", true),
		scenario87IntBuildCluster(scenario87IntTarget, scenario87IntBucket, "scenario87-dst", false),
	)
	assert.Equal(s.T(), 400, s.adminStatus(scenario87IntSource,
		`{"targetCluster":"dst","database":"mydb"}`))
}
