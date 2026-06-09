//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 87: Cross-Cluster Migration (cloudberry-ctl migrate ...) — E2E
// ============================================================================
//
// User journey: an admin migrates a database from a SOURCE cluster to a TARGET
// cluster that share one S3 bucket. The CLI POSTs to the source's /migrate
// endpoint (Admin-gated, OIDC bearer); the operator creates a coordinated trio
// of Jobs sharing one timestamp + bucket: a gpbackup Job on the source, a
// gprestore Job on the target, and a best-effort post-restore validation Job on
// the target. After restore the target's row counts match the source's.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder/request parity: the Jobs the migration causes the operator to
//     render carry exactly the gpbackup/gprestore flags (and validation-script
//     markers) the live script asserts via `kubectl get job -o jsonpath` — the
//     source backup args (87b), the target restore args (87c), the validation
//     Job markers (87e) and the shared-bucket invariant (87d). The builder is
//     driven with the SAME options handleMigrate constructs (migrateBackupOptions
//     / migrateRestoreOptions / {Timestamp, Database} for the validation Job).
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario87-cross-cluster-migration.sh, which builds
//     cloudberry-ctl, obtains an OIDC admin token, port-forwards the operator
//     API, runs `migrate`, and asserts Jobs/args, the shared bucket, and that the
//     validation Job completes with matching row counts.
//
// No assertion depends on a literal "checksum" string; the migration's
// row-count/integrity verification is asserted via the real validation-script
// markers (row-count probe, invalid-index scan, SELECT 1, "passed").
// ============================================================================

const (
	// envS87SourceCluster overrides the live source cluster name.
	envS87SourceCluster = "SCENARIO87_SOURCE_CLUSTER"
	// envS87TargetCluster overrides the live target cluster name.
	envS87TargetCluster = "SCENARIO87_TARGET_CLUSTER"
	// envS87Script overrides the live script path.
	envS87Script = "SCENARIO87_SCRIPT"

	scenario87E2EDB     = "mydb"
	scenario87E2ETS     = "20260601020000"
	scenario87E2EBucket = "cloudberry-backups"
)

// Scenario87MigrationE2ESuite tests builder/request parity (the Jobs the
// migration causes) plus the KUBECONFIG-gated live run.
type Scenario87MigrationE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario87(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario87MigrationE2ESuite))
}

func (s *Scenario87MigrationE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario87E2ECluster builds a running S3-destination backup-enabled cluster
// with the given folder; the bucket is shared (scenario87E2EBucket) so the two
// clusters satisfy the same-bucket migration precondition.
func scenario87E2ECluster(name, folder string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
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
				Bucket:         scenario87E2EBucket,
				Endpoint:       "http://host.docker.internal:9000",
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

// scenario87JobScript returns the rendered container script (args[0]) of a Job.
func scenario87JobScript(s *Scenario87MigrationE2ESuite, job *batchv1.Job) string {
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// scenario87MigrationJob builds the SINGLE coordinated migration Job the
// operator now renders (mirroring migrateJobOptions in internal/api/migrate.go):
// full backup with SingleDataFile + include-tables + database on the source, a
// gprestore on the target fed the CAPTURED gpbackup timestamp, and validation.
func scenario87MigrationJob(
	src, dst *cbv1alpha1.CloudberryCluster,
	tables []string,
	truncate bool,
	jobs int32,
) *batchv1.Job {
	b := builder.NewBuilder()
	return b.BuildMigrationJob(&builder.MigrationJobOptions{
		Timestamp:          scenario87E2ETS,
		Source:             src,
		Target:             dst,
		Database:           scenario87E2EDB,
		RedirectDb:         scenario87E2EDB,
		IncludeTables:      tables,
		SingleDataFile:     true,
		Truncate:           truncate,
		Jobs:               jobs,
		ValidationDatabase: scenario87E2EDB,
	})
}

// --- 87b: source backup phase parity (rendered inside the single migration Job) ---

func (s *Scenario87MigrationE2ESuite) TestE2E_Scenario87_SourceBackupJobParity() {
	src := scenario87E2ECluster("test-s87e2e-src", "scenario87-src")
	dst := scenario87E2ECluster("test-s87e2e-dst", "scenario87-dst")

	job := scenario87MigrationJob(src, dst, []string{"public.users", "public.orders"}, true, 4)
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationMigrate, job.Labels[util.LabelBackupOperation])

	script := scenario87JobScript(s, job)
	for _, want := range []string{
		"'--include-table' 'public.users'",
		"'--include-table' 'public.orders'",
		"'--single-data-file'",
		// Coordinator-exec model (spec 11 §MPP Dispatch): gpbackup runs INSIDE the
		// SOURCE coordinator pod against the coordinator-side ${COORD_CFG}.
		"gpbackup --plugin-config \"${COORD_CFG}\"",
		"'--dbname' '" + scenario87E2EDB + "'",
		// The REAL gpbackup timestamp is captured from gpbackup's stdout (the FINAL
		// cross-cluster fix) rather than using the operator-chosen one.
		"grep -oE 'Backup Timestamp = [0-9]{14}'",
	} {
		assert.Containsf(s.T(), script, want, "87b parity: must render %q", want)
	}
	assert.NotContains(s.T(), script, "'--incremental'")
}

// --- 87c: target restore phase parity (rendered inside the single migration Job) ---

func (s *Scenario87MigrationE2ESuite) TestE2E_Scenario87_TargetRestoreJobParity() {
	src := scenario87E2ECluster("test-s87e2e-src", "scenario87-src")
	dst := scenario87E2ECluster("test-s87e2e-dst", "scenario87-dst")

	job := scenario87MigrationJob(src, dst, []string{"public.users", "public.orders"}, true, 4)
	require.NotNil(s.T(), job)

	script := scenario87JobScript(s, job)
	for _, want := range []string{
		// gprestore is fed the CAPTURED gpbackup timestamp (expanded at run time
		// from $7 = ${MIG_BACKUP_TS}), NOT a literal operator timestamp.
		"--timestamp \"$7\"",
		"'--redirect-db' '" + scenario87E2EDB + "'",
		// Coordinator-exec model (spec 11 §MPP Dispatch).
		"gprestore --plugin-config \"${COORD_CFG}\"",
		"'--include-table' 'public.users'",
		"'--include-table' 'public.orders'",
		"'--jobs' '4'",
	} {
		assert.Containsf(s.T(), script, want, "87c parity: must render %q", want)
	}
	assert.NotContains(s.T(), script, "'--metadata-only'")
	// --truncate-table must NOT be used: a fresh-DB migration restore (metadata +
	// data) would TRUNCATE not-yet-existing objects during the pre-data metadata
	// phase and abort (42P01). The job is built with truncate=true; that intent is
	// honored at the DB level (DROP+recreate the empty target DB), not via
	// --truncate-table.
	assert.NotContains(s.T(), script, "'--truncate-table'",
		"87c parity: migration restore must NOT use --truncate-table (fresh-DB restore)")
	assert.Contains(s.T(), script, "clean+recreate target database (target coordinator)",
		"87c parity: --truncate must clean the target DB (DROP+recreate)")
	assert.Contains(s.T(), script, "DROP DATABASE IF EXISTS")
	// The restore must NOT pin the operator-chosen timestamp (the bug).
	assert.NotContains(s.T(), script, "--timestamp '"+scenario87E2ETS+"'",
		"87c: restore must use the CAPTURED gpbackup timestamp, not the operator one")
}

// --- 87e: validation phase parity (best-effort probe path, no ExpectedRowCounts) ---

func (s *Scenario87MigrationE2ESuite) TestE2E_Scenario87_ValidationJobParity() {
	src := scenario87E2ECluster("test-s87e2e-src", "scenario87-src")
	dst := scenario87E2ECluster("test-s87e2e-dst", "scenario87-dst")

	job := scenario87MigrationJob(src, dst, []string{"public.users"}, false, 0)
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.MigrationJobName(src.Name, scenario87E2ETS), job.Name)

	script := scenario87JobScript(s, job)
	for _, want := range []string{
		"post-restore-validate:",
		"row-count probe",
		"invalid",
		"SELECT 1",
		"post-restore-validate: passed",
	} {
		assert.Containsf(s.T(), script, want, "87e parity: must render %q", want)
	}
	// The migration validation path uses the probe, not the compare path, and
	// never a literal "checksum" marker.
	assert.NotContains(s.T(), script, "row-count compare vs gpbackup history")
	assert.NotContains(s.T(), script, "checksum")
}

// --- 87d: same-bucket parity ---

func (s *Scenario87MigrationE2ESuite) TestE2E_Scenario87_SameBucketParity() {
	src := scenario87E2ECluster("test-s87e2e-src", "scenario87-src")
	dst := scenario87E2ECluster("test-s87e2e-dst", "scenario87-dst")

	require.NotNil(s.T(), src.Spec.Backup)
	require.NotNil(s.T(), dst.Spec.Backup)
	assert.Equal(s.T(),
		src.Spec.Backup.Destination.S3.Bucket,
		dst.Spec.Backup.Destination.S3.Bucket,
		"87d: source and target must share the S3 bucket")

	// The single migration Job renders both tool invocations against the S3
	// plugin config (same bucket destination) and pins the SOURCE folder for both
	// the backup and the (target) restore.
	job := scenario87MigrationJob(src, dst, []string{"public.users"}, false, 0)
	script := scenario87JobScript(s, job)
	assert.Contains(s.T(), script, "gpbackup --plugin-config \"${COORD_CFG}\"")
	assert.Contains(s.T(), script, "gprestore --plugin-config \"${COORD_CFG}\"")
	// Both phases read/write the SOURCE folder where gpbackup wrote.
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "S3_FOLDER" {
			assert.Equal(s.T(), "scenario87-src", env.Value,
				"87d: migration Job must use the source S3 folder for both phases")
		}
	}
}

// --- 87 live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario87_LiveMigration is the live-cluster portion. It self-skips
// when KUBECONFIG is unset so the suite never requires a real cluster, Keycloak
// or backup tooling. When live, it shells out to the scenario87 live script,
// which builds cloudberry-ctl, obtains an OIDC admin bearer token, port-forwards
// the operator API, runs `migrate` between two clusters that share one S3 bucket
// and asserts Jobs/args, the shared bucket, and that the validation Job completes
// with matching source/target row counts.
func (s *Scenario87MigrationE2ESuite) TestE2E_Scenario87_LiveMigration() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cross-cluster migration verification")
	}

	source := os.Getenv(envS87SourceCluster)
	if source == "" {
		source = "scenario87-src"
	}
	target := os.Getenv(envS87TargetCluster)
	if target == "" {
		target = "scenario87-dst"
	}

	script := os.Getenv(envS87Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario87-cross-cluster-migration.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--source", source,
		"--target", target,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario87 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario87 live script must pass all migration checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
