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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 88: Backup Disabled / No Schedule — E2E
// ============================================================================
//
// User journey: an admin disables backup (or never sets a schedule) on a
// cluster. The operator must NOT run a scheduled CronJob, the scheduled CronJob
// must be absent and Status.CronJobName empty, while an on-demand backup is still
// possible whenever backup is ENABLED (even with an empty schedule). Re-enabling
// with a schedule recreates the CronJob.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: the schedule-driven CronJob the operator renders is nil for
//     a DISABLED cluster and for an ENABLED cluster with an EMPTY schedule, but
//     non-nil for a real schedule (the re-enable target state). The on-demand
//     BuildBackupJob always builds with the operation=backup label, proving the
//     on-demand path does not depend on a schedule (88b-5).
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario88-backup-disabled.sh, which disables backup, asserts
//     no CronJob + empty Status.CronJobName, asserts the API reports the disabled
//     state (create rejected, schedule:false), re-enables + asserts recreation,
//     sets an empty schedule + asserts no CronJob, and runs an on-demand backup
//     that completes.
//
// GAP-1: the backup SA/Role are CHART-level (cloudberry-backup-sa /
// cloudberry-backup-role, gated by the Helm value `backup.rbac.create`) and
// shared in the operator namespace; they are NOT per-cluster, so the parity tests
// here assert only the per-cluster CronJob/Job effects.
// ============================================================================

const (
	// envS88Cluster overrides the live cluster name.
	envS88Cluster = "SCENARIO88_CLUSTER"
	// envS88Script overrides the live script path.
	envS88Script = "SCENARIO88_SCRIPT"

	scenario88E2EDB     = "mydb"
	scenario88E2ETS     = "20260601020000"
	scenario88E2EBucket = "cloudberry-backups"
)

// Scenario88BackupDisabledE2ESuite tests builder parity plus the KUBECONFIG-gated
// live run.
type Scenario88BackupDisabledE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario88(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario88BackupDisabledE2ESuite))
}

func (s *Scenario88BackupDisabledE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario88E2ECluster builds a Running cluster with an S3-destination backup
// spec parameterised by the enabled flag + schedule.
func scenario88E2ECluster(name string, enabled bool, schedule string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
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
				Bucket:         scenario88E2EBucket,
				Endpoint:       "http://host.docker.internal:9000",
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

// --- 88b-5: enabled + empty schedule => no CronJob built, on-demand Job builds ---

func (s *Scenario88BackupDisabledE2ESuite) TestE2E_Scenario88_EmptyScheduleNoCronJobBuilt() {
	cluster := scenario88E2ECluster("test-s88e2e-empty", true, "")
	b := builder.NewBuilder()

	assert.Nil(s.T(), b.BuildBackupCronJob(cluster),
		"88b-5: BuildBackupCronJob must be nil for an empty schedule")

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario88E2ETS,
		Type:      util.BackupTypeFull,
		Databases: []string{scenario88E2EDB},
	})
	require.NotNil(s.T(), job, "88b-5: BuildBackupJob must build on-demand without a schedule")
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation],
		"88b-5: the on-demand Job must carry the operation=backup label")
}

// --- 88a: disabled (no schedule) => no CronJob built ---

func (s *Scenario88BackupDisabledE2ESuite) TestE2E_Scenario88_DisabledNoCronJobBuilt() {
	// BuildBackupCronJob gates on the SCHEDULE only (nil when Schedule == ""); the
	// backup-ENABLED gate is enforced one layer up in reconcileBackup (which
	// returns early for a disabled cluster, so no CronJob is ever reconciled and a
	// stale one is removed). The realistic disabled baseline therefore carries no
	// schedule, for which the builder returns nil.
	cluster := scenario88E2ECluster("test-s88e2e-disabled", false, "")
	b := builder.NewBuilder()
	assert.Nil(s.T(), b.BuildBackupCronJob(cluster),
		"88a: BuildBackupCronJob must be nil for a disabled cluster without a schedule")
}

// --- 88a-7 target state: a real schedule renders the CronJob ---

func (s *Scenario88BackupDisabledE2ESuite) TestE2E_Scenario88_ScheduleSetCronJobBuilt() {
	cluster := scenario88E2ECluster("test-s88e2e-sched", true, "0 2 * * *")
	b := builder.NewBuilder()

	cron := b.BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron,
		"88a-7 target: BuildBackupCronJob must be non-nil for a real schedule")
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), cron.Name)
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)
}

// --- 88 live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario88_LiveBackupDisabled is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster,
// Keycloak or backup tooling. When live it shells out to
// scenario88-backup-disabled.sh, which disables backup and asserts no CronJob +
// empty Status.CronJobName + API disabled, re-enables + asserts recreation, sets
// an empty schedule + asserts no CronJob, and runs an on-demand backup that
// completes. The script restores the CR's original backup config on exit.
func (s *Scenario88BackupDisabledE2ESuite) TestE2E_Scenario88_LiveBackupDisabled() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live backup-disabled verification")
	}

	cluster := os.Getenv(envS88Cluster)
	if cluster == "" {
		// Default to a deployed S3-backed cluster name; override via SCENARIO88_CLUSTER.
		cluster = "scenario88"
	}

	script := os.Getenv(envS88Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario88-backup-disabled.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
		"--db", scenario88E2EDB,
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario88 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario88 live script must pass all backup-disabled checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
