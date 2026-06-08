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
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 83: Backup Failure Handling (E2E)
// ============================================================================
//
// User journey: an operator-managed S3-destination cluster runs healthy backups
// (cloudberry_backup_last_status=0); when a backup FAILS — either because its S3
// destination is unreachable / has bad credentials (the Job retries up to
// backoffLimit=2 and ends terminal Failed with reason BackoffLimitExceeded) or
// because it exceeds a LOW activeDeadlineSeconds (Kubernetes kills it at the
// deadline with reason DeadlineExceeded) — the operator records
// status.lastBackupStatus=Failed and cloudberry_backup_last_status=1:
//
//	83-healthy : a real healthy gpbackup via coordinator-exec succeeds; a
//	    Succeeded backup Job + reconcile => cloudberry_backup_last_status=0.
//	83a force-failure : a backup Job (backoffLimit=2) whose container fails on a
//	    bad/unreachable S3 endpoint retries up to backoffLimit and ends Failed
//	    (BackoffLimitExceeded); reconcile => lastBackupStatus=Failed +
//	    cloudberry_backup_last_status=1.
//	83b deadline : a per-run backup Job (activeDeadlineSeconds=5 + sleep 600) is
//	    killed at the deadline (DeadlineExceeded); reconcile => lastBackupStatus=
//	    Failed + cloudberry_backup_last_status=1.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob with a jobTemplate.{backoffLimit:2,
//     activeDeadlineSeconds:5} carries backoffLimit==2 and the low
//     activeDeadlineSeconds in the rendered jobspec (the force-failure +
//     deadline knobs reach the spec).
//   - Default parity: with no jobTemplate the backup Job defaults to
//     backoffLimit==2 ("retries up to backoffLimit") + activeDeadlineSeconds==7200.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario83-backup-failure.sh, which drives the full
//     healthy/force-failure/deadline lifecycle (real gpbackup success,
//     backoffLimit retries -> BackoffLimitExceeded, deadline kill ->
//     DeadlineExceeded) and asserts lastBackupStatus + cloudberry_backup_last_status
//     in VictoriaMetrics against a running cluster.
//
// NOTE: the builder cannot prove the kubelet's pod-failure semantics (the actual
// backoffLimit retries / the deadline kill). Those are asserted by the live
// script; the deterministic portion proves the operator's rendered jobspec
// (backoffLimit + activeDeadlineSeconds) — split exactly as the live script
// documents.
// ============================================================================

const (
	// envS83Cluster overrides the live backup-failure cluster name.
	envS83Cluster = "SCENARIO83_S3_CLUSTER"
	// envS83Script overrides the live script path.
	envS83Script = "SCENARIO83_SCRIPT"

	scenario83E2EBackupImage           = "cloudberry-backup:2.1.0"
	scenario83E2ECredSecret            = "s3-credentials"
	scenario83E2ETS                    = "20260608030000"
	scenario83E2EDefaultBackoff  int32 = 2
	scenario83E2EDefaultDeadline int64 = 7200
	scenario83E2ELowDeadline     int64 = 5
)

// Scenario83BackupFailureE2ESuite tests the backup-failure Job rendering (builder
// parity) and the KUBECONFIG-gated live portion.
type Scenario83BackupFailureE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario83(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario83BackupFailureE2ESuite))
}

func (s *Scenario83BackupFailureE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario83E2ECluster builds a running cluster with an S3 backup destination
// mirroring the scenario83-s3 sample CR. When backoff/deadline are non-nil they
// are set as a jobTemplate override (the force-failure + deadline knobs).
func scenario83E2ECluster(name string, backoff *int32, deadline *int64) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario83E2EBackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario83",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario83E2ECredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	if backoff != nil || deadline != nil {
		cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
			BackoffLimit:          backoff,
			ActiveDeadlineSeconds: deadline,
		}
	}
	return cluster
}

func scenario83E2EBuildJob(c *cbv1alpha1.CloudberryCluster) *batchv1.Job {
	return builder.NewBuilder().BuildBackupJob(c, &builder.BackupJobOptions{
		Timestamp: scenario83E2ETS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
}

// --- 83.1: builder parity (infra-free) — default backoffLimit/deadline ---

// TestE2E_Scenario83_BackupJobDefaultParity verifies BuildBackupJob with NO
// jobTemplate defaults to backoffLimit==2 ("retries up to backoffLimit") and
// activeDeadlineSeconds==7200.
func (s *Scenario83BackupFailureE2ESuite) TestE2E_Scenario83_BackupJobDefaultParity() {
	cluster := scenario83E2ECluster("test-s83e2e-default", nil, nil)
	job := scenario83E2EBuildJob(cluster)
	require.NotNil(s.T(), job)

	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), scenario83E2EDefaultBackoff, *job.Spec.BackoffLimit,
		"default backoffLimit must be 2")
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), scenario83E2EDefaultDeadline, *job.Spec.ActiveDeadlineSeconds,
		"default activeDeadlineSeconds must be 7200")
}

// TestE2E_Scenario83_BackupJobOverrideParity verifies a jobTemplate with
// backoffLimit:2 + activeDeadlineSeconds:5 reaches the rendered backup jobspec
// (the force-failure + deadline knobs).
func (s *Scenario83BackupFailureE2ESuite) TestE2E_Scenario83_BackupJobOverrideParity() {
	backoff := scenario83E2EDefaultBackoff
	deadline := scenario83E2ELowDeadline
	cluster := scenario83E2ECluster("test-s83e2e-override", &backoff, &deadline)
	job := scenario83E2EBuildJob(cluster)
	require.NotNil(s.T(), job)

	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), backoff, *job.Spec.BackoffLimit,
		"jobTemplate backoffLimit==2 must reach the jobspec")
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), deadline, *job.Spec.ActiveDeadlineSeconds,
		"jobTemplate activeDeadlineSeconds==5 must reach the jobspec")
}

// --- 83.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario83_LiveBackupFailure is the live-cluster portion. It self-skips
// when KUBECONFIG is unset so the suite never requires a real cluster or backup
// tooling. When live, it shells out to the scenario83 live script, which drives
// the full healthy/force-failure/deadline lifecycle: a real healthy gpbackup
// (cloudberry_backup_last_status=0), a backoffLimit=2 force-failure Job that
// retries and ends Failed (BackoffLimitExceeded) -> lastBackupStatus=Failed +
// cloudberry_backup_last_status=1, and a per-run deadline Job
// (activeDeadlineSeconds=5 + sleep) killed at the deadline (DeadlineExceeded) ->
// Failed + last_status=1.
func (s *Scenario83BackupFailureE2ESuite) TestE2E_Scenario83_LiveBackupFailure() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live backup-failure verification")
	}

	cluster := os.Getenv(envS83Cluster)
	if cluster == "" {
		cluster = "scenario83-s3"
	}

	script := os.Getenv(envS83Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario83-backup-failure.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full backup-failure lifecycle and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario83 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario83 live script must pass all backup-failure checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
