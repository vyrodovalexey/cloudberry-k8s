//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
// Scenario 86: All CLI Commands (cloudberry-ctl backup ...) — E2E
// ============================================================================
//
// User journey: an admin drives EVERY cloudberry-ctl backup command against a
// deployed S3-destination cluster (scenario86-s3, backup + schedule +
// incremental enabled), over the OIDC-authed operator REST API. The CLI builds
// the cloudberry-ctl binary, obtains an OIDC admin token from Keycloak, the
// operator API service is port-forwarded, and the CLI is pointed at it via
// --operator-url / CLOUDBERRY_OPERATOR_URL + the bearer token. They:
//
//	86a create (full / single-data-file / incremental) -> backup Jobs;
//	86b list, 86c status, 86j jobs, 86k jobs logs (STREAMS pod logs);
//	86e restore --resize-cluster -> restore Job; 86d delete -> cleanup Job;
//	86f schedule show, 86g set --cron, 86h suspend, 86i resume -> CronJob changes.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - CLI-command/request parity: the Jobs the CLI's create/restore/delete
//     commands cause the operator to render carry exactly the gpbackup/gprestore
//     flags the live script asserts via `kubectl get job -o jsonpath` — including
//     the three create variants and --resize-cluster on restore.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario86-cli-commands.sh, which builds cloudberry-ctl,
//     obtains an OIDC bearer token, port-forwards the operator API, points the
//     CLI at it and runs every backup command 86a-k, asserting Jobs/args/CronJob
//     changes/streamed logs.
// ============================================================================

const (
	// envS86Cluster overrides the live CLI cluster name.
	envS86Cluster = "SCENARIO86_S3_CLUSTER"
	// envS86Script overrides the live script path.
	envS86Script = "SCENARIO86_SCRIPT"

	scenario86E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario86E2ECredSecret  = "backup-s3-credentials"
	scenario86E2EDB          = "mydb"
	scenario86E2ETS          = "20260601020000"
	scenario86E2EPriorTS     = "20260601010000"
)

// Scenario86CLICommandsE2ESuite tests CLI-command/request parity (builder
// rendering of the Jobs each command causes) plus the KUBECONFIG-gated live run.
type Scenario86CLICommandsE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario86(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario86CLICommandsE2ESuite))
}

func (s *Scenario86CLICommandsE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario86E2ECluster builds a running S3-destination backup cluster (backup +
// schedule + incremental enabled) mirroring the scenario86-s3 sample CR.
func scenario86E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Image:    scenario86E2EBackupImage,
		Schedule: "0 2 * * *",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			LeafPartitionData: true,
			CompressionType:   "zstd",
			CompressionLevel:  6,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario86",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario86E2ECredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// --- 86a-1: CLI `backup create` (full) -> backup Job args parity ---

func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_CreateFullJobParity() {
	cluster := scenario86E2ECluster("test-s86e2e-full")
	b := builder.NewBuilder()

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario86E2ETS,
		Type:      "full",
		Databases: []string{scenario86E2EDB},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
			Jobs:             4,
			WithStats:        util.Ptr(true),
			WithoutGlobals:   true,
		},
		IncludeSchemas: []string{"public"},
		ExcludeTables:  []string{"public.temp"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
	assert.Equal(s.T(), "full", job.Labels[util.LabelBackupType])

	script := scenario86JobScript(s, job)
	for _, want := range []string{
		"'--dbname' '" + scenario86E2EDB + "'",
		"'--compression-level' '6'",
		"'--compression-type' 'zstd'",
		"'--jobs' '4'",
		"'--include-schema' 'public'",
		"'--exclude-table' 'public.temp'",
		"'--with-stats'",
		"'--without-globals'",
	} {
		assert.Containsf(s.T(), script, want, "86a-1 parity: must render %q", want)
	}
	assert.NotContains(s.T(), script, "'--single-data-file'")
	assert.NotContains(s.T(), script, "'--incremental'")
}

// --- 86a-2: CLI `backup create --single-data-file` -> Job args parity ---

func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_CreateSingleDataFileJobParity() {
	cluster := scenario86E2ECluster("test-s86e2e-single")
	b := builder.NewBuilder()

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario86E2ETS,
		Type:      "full",
		Databases: []string{scenario86E2EDB},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			SingleDataFile: true,
			CopyQueueSize:  4,
		},
	})
	require.NotNil(s.T(), job)
	script := scenario86JobScript(s, job)
	assert.Contains(s.T(), script, "'--single-data-file'")
	assert.Contains(s.T(), script, "'--copy-queue-size' '4'")
	// single-data-file is mutually exclusive with --jobs.
	assert.NotContains(s.T(), script, "'--jobs'")
}

// --- 86a-3: CLI `backup create --incremental` -> Job args parity ---

func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_CreateIncrementalJobParity() {
	cluster := scenario86E2ECluster("test-s86e2e-incr")
	b := builder.NewBuilder()

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp:     scenario86E2ETS,
		Type:          "incremental",
		Databases:     []string{scenario86E2EDB},
		FromTimestamp: scenario86E2EPriorTS,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			Incremental:       true,
			LeafPartitionData: true,
		},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), "incremental", job.Labels[util.LabelBackupType])
	script := scenario86JobScript(s, job)
	assert.Contains(s.T(), script, "'--incremental'")
	assert.Contains(s.T(), script, "'--from-timestamp' '"+scenario86E2EPriorTS+"'")
	// --leaf-partition-data emitted exactly once.
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"))
}

// --- 86e: CLI `backup restore --resize-cluster` -> restore Job args parity ---

func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_RestoreResizeJobParity() {
	cluster := scenario86E2ECluster("test-s86e2e-restore")
	b := builder.NewBuilder()

	job := b.BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:      scenario86E2ETS,
		Databases:      []string{scenario86E2EDB},
		RedirectDb:     "mydb_restored",
		RedirectSchema: "restored",
		IncludeSchemas: []string{"public"},
		IncludeTables:  []string{"public.users"},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:            4,
			CreateDb:        true,
			WithStats:       util.Ptr(true),
			RunAnalyze:      true,
			OnErrorContinue: true,
			TruncateTable:   true,
			ResizeCluster:   true,
		},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])

	script := scenario86JobScript(s, job)
	for _, want := range []string{
		"'--timestamp' '" + scenario86E2ETS + "'",
		"'--jobs' '4'",
		"'--redirect-db' 'mydb_restored'",
		"'--redirect-schema' 'restored'",
		"'--create-db'",
		"'--run-analyze'",
		"'--on-error-continue'",
		"'--truncate-table'",
		"'--resize-cluster'",
		"'--include-table' 'public.users'",
	} {
		assert.Containsf(s.T(), script, want, "86e parity: must render %q", want)
	}
	// include-table wins over include-schema; run-analyze wins over with-stats.
	assert.NotContains(s.T(), script, "'--include-schema'")
	assert.NotContains(s.T(), script, "'--with-stats'")
}

// --- 86d: CLI `backup delete` -> cleanup Job parity ---

func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_CleanupJobParity() {
	cluster := scenario86E2ECluster("test-s86e2e-cleanup")
	b := builder.NewBuilder()

	job := b.BuildRetentionCleanupJob(cluster, scenario86E2ETS)
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation],
		"86d: cleanup Job must carry operation=cleanup")
	assert.Contains(s.T(), scenario86JobScript(s, job), "backup-delete",
		"86d: cleanup Job must run gpbackman backup-delete")
}

// scenario86JobScript returns the rendered container script (args[0]) of a Job.
func scenario86JobScript(s *Scenario86CLICommandsE2ESuite, job *batchv1.Job) string {
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// --- 86 live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario86_LiveCLICommands is the live-cluster portion. It self-skips
// when KUBECONFIG is unset so the suite never requires a real cluster, Keycloak
// or backup tooling. When live, it shells out to the scenario86 live script,
// which builds cloudberry-ctl, obtains an OIDC bearer token from Keycloak (realm
// test, an admin-role user), port-forwards the operator API, points the CLI at
// it (--operator-url + bearer token) and runs every backup command 86a-k,
// asserting Jobs/args, CronJob schedule/suspend changes and streamed Job logs.
func (s *Scenario86CLICommandsE2ESuite) TestE2E_Scenario86_LiveCLICommands() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live CLI-commands verification")
	}

	cluster := os.Getenv(envS86Cluster)
	if cluster == "" {
		cluster = "scenario86-s3"
	}

	script := os.Getenv(envS86Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario86-cli-commands.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario86 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario86 live script must pass all CLI-command checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
