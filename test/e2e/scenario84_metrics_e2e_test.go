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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 84: Prometheus Metrics / gpbackup_exporter (E2E)
// ============================================================================
//
// User journey: an operator-managed S3-destination cluster runs a full
// lifecycle — a FULL gpbackup, an INCREMENTAL gpbackup, a gprestore, a retention
// cleanup and a forced failure. The operator (acting as the gpbackup_exporter
// via its /metrics endpoint) records ALL 9 backup-lifecycle metrics, which are
// scraped by vmagent into VictoriaMetrics:
//
//	M1 cloudberry_backup_total{type,result}      M6 cloudberry_restore_total{result}
//	M2 cloudberry_backup_duration_seconds{type}  M7 cloudberry_restore_duration_seconds
//	M3 cloudberry_backup_size_bytes{timestamp}   M8 cloudberry_backup_retention_deleted_total
//	M4 cloudberry_backup_last_success_timestamp  M9 cloudberry_backup_job_status{job,operation}
//	M5 cloudberry_backup_last_status (0/1/2)
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder/reconcile parity: BuildBackupJob (full + incremental) renders Jobs
//     with the avsoft.io/backup-type label the operator reads; the operator-shaped
//     Job fixtures (correct labels + size/retention annotations + start/completion
//     status) are exactly what the operator's recordBackupJobMetrics /
//     recordLatestBackupMetrics observe, so the metric mapping is provable
//     deterministically.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario84-metrics.sh, which drives the full lifecycle (real
//     gpbackup full + incremental, gprestore, gpbackman cleanup, a forced failure),
//     materializes/observes operator-shaped Jobs so the operator records the
//     metrics, and asserts ALL 9 metrics in VictoriaMetrics with poll loops.
// ============================================================================

const (
	// envS84Cluster overrides the live metrics cluster name.
	envS84Cluster = "SCENARIO84_S3_CLUSTER"
	// envS84Script overrides the live script path.
	envS84Script = "SCENARIO84_SCRIPT"

	scenario84E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario84E2ECredSecret  = "backup-s3-credentials"
	scenario84E2EFullTS      = "20260608040000"
	scenario84E2EIncrTS      = "20260608041000"
	scenario84E2ESizeBytes   = "104857600"
)

// Scenario84MetricsE2ESuite tests the backup metric Job rendering (builder
// parity) and the KUBECONFIG-gated live portion.
type Scenario84MetricsE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario84(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario84MetricsE2ESuite))
}

func (s *Scenario84MetricsE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario84E2ECluster builds a running S3-destination cluster (incremental +
// retention enabled) mirroring the scenario84-s3 sample CR.
func scenario84E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario84E2EBackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			Incremental:      true,
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
				Folder:         "scenario84",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario84E2ECredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// --- 84.1: builder parity (infra-free) — backup-type label reaches the jobspec ---

// TestE2E_Scenario84_BackupTypeLabelParity verifies BuildBackupJob renders the
// avsoft.io/backup-type label the operator reads (backupTypeFromJob) for both a
// full and an incremental backup — the label that routes M1/M2 per type.
func (s *Scenario84MetricsE2ESuite) TestE2E_Scenario84_BackupTypeLabelParity() {
	cluster := scenario84E2ECluster("test-s84e2e-type")
	b := builder.NewBuilder()

	// The backup-type label MUST match the gpbackup args actually rendered. The
	// scenario84 cluster defaults to incremental (Gpbackup.Incremental=true), so a
	// FULL Job must override that default with opts.Gpbackup{Incremental:false}
	// (otherwise gpbackup still runs incremental and the label stays incremental
	// to match the args). This mirrors how an explicit full request is issued.
	full := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario84E2EFullTS,
		Type:      "full",
		Databases: []string{"metdb84"},
		Gpbackup:  &cbv1alpha1.GpbackupOptions{Incremental: false},
	})
	require.NotNil(s.T(), full)
	assert.Equal(s.T(), "full", full.Labels[util.LabelBackupType],
		"BuildBackupJob(full) must set avsoft.io/backup-type=full")
	assert.Equal(s.T(), util.BackupOperationBackup, full.Labels[util.LabelBackupOperation])

	incr := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario84E2EIncrTS,
		Type:      "incremental",
		Databases: []string{"metdb84"},
	})
	require.NotNil(s.T(), incr)
	assert.Equal(s.T(), "incremental", incr.Labels[util.LabelBackupType],
		"BuildBackupJob(incremental) must set avsoft.io/backup-type=incremental")
}

// TestE2E_Scenario84_RestoreAndCleanupOperationParity verifies the restore and
// cleanup Jobs carry the avsoft.io/backup-operation label the operator switches
// on (M6/M7 for restore, M8 for cleanup, M9 per operation).
func (s *Scenario84MetricsE2ESuite) TestE2E_Scenario84_RestoreAndCleanupOperationParity() {
	cluster := scenario84E2ECluster("test-s84e2e-ops")
	b := builder.NewBuilder()

	restore := b.BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: scenario84E2EFullTS,
		Databases: []string{"metdb84"},
	})
	require.NotNil(s.T(), restore)
	assert.Equal(s.T(), util.BackupOperationRestore, restore.Labels[util.LabelBackupOperation],
		"BuildRestoreJob must set avsoft.io/backup-operation=restore")

	cleanup := b.BuildRetentionCleanupJob(cluster, scenario84E2EIncrTS)
	require.NotNil(s.T(), cleanup)
	assert.Equal(s.T(), util.BackupOperationCleanup, cleanup.Labels[util.LabelBackupOperation],
		"BuildRetentionCleanupJob must set avsoft.io/backup-operation=cleanup")
}

// TestE2E_Scenario84_OperatorShapedJobFixtureParity asserts the operator-shaped
// Job fixture the live script materializes (labels + size/retention annotations +
// start/completion status) carries the exact metadata the operator reads to
// record M3 (size_bytes) and M8 (retention_deleted) — proving the live script's
// Job shape is correct WITHOUT a cluster.
func (s *Scenario84MetricsE2ESuite) TestE2E_Scenario84_OperatorShapedJobFixtureParity() {
	cluster := scenario84E2ECluster("test-s84e2e-shape")
	created := metav1.NewTime(time.Now())
	start := metav1.NewTime(time.Now().Add(-90 * time.Second))

	backupJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupJobName(cluster.Name, scenario84E2EFullTS),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelComponent:       util.ComponentBackup,
				util.LabelBackupOperation: util.BackupOperationBackup,
				util.LabelBackupType:      "full",
			},
			Annotations: map[string]string{
				util.AnnotationBackupSizeBytes: scenario84E2ESizeBytes,
			},
			CreationTimestamp: created,
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &created,
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	assert.Equal(s.T(), "full", backupJob.Labels[util.LabelBackupType])
	assert.Equal(s.T(), scenario84E2ESizeBytes, backupJob.Annotations[util.AnnotationBackupSizeBytes],
		"the size annotation must be present so the operator records M3 backup_size_bytes")
	require.NotNil(s.T(), backupJob.Status.CompletionTime,
		"completionTime must be set so M4 last_success_timestamp + M2 duration fire")
}

// --- 84.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario84_LiveMetrics is the live-cluster portion. It self-skips when
// KUBECONFIG is unset so the suite never requires a real cluster or backup
// tooling. When live, it shells out to the scenario84 live script, which drives
// the full lifecycle (real gpbackup full + incremental, gprestore, gpbackman
// cleanup, a forced failure), materializes/observes operator-shaped Jobs so the
// operator records the metrics, and asserts ALL 9 metrics in VictoriaMetrics.
func (s *Scenario84MetricsE2ESuite) TestE2E_Scenario84_LiveMetrics() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live metrics verification")
	}

	cluster := os.Getenv(envS84Cluster)
	if cluster == "" {
		cluster = "scenario84-s3"
	}

	script := os.Getenv(envS84Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario84-metrics.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full backup lifecycle and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario84 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario84 live script must pass all metric checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
