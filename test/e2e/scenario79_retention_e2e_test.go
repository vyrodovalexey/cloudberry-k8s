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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 79: Retention Cleanup, All Policies (E2E)
// ============================================================================
//
// User journey: an operator-managed cluster with retention.{fullCount:3,
// incrementalCount:10, maxAge:"30d"} takes backups, and after each successful
// backup the operator creates a single gpbackman-driven retention cleanup Job
// that enforces all three policies and feeds its deletion count into
// cloudberry_backup_retention_deleted_total:
//
//	79a fullCount=3        : 4 full backups => the oldest full is deleted, 3 kept.
//	79b incrementalCount=10: a full + 11 incrementals => the oldest incremental
//	    beyond 10 is deleted (re-enumerated so cascade neither over/under-counts).
//	79c maxAge="30d"       : a backup with a history timestamp older than 30 days
//	    is deleted by backup-clean --older-than-days 30 --cascade.
//	79d cleanup placement   : the operator creates EXACTLY ONE cleanup Job per
//	    latest Succeeded backup (idempotent) and the metric increments per
//	    deletion via the RETENTION_DELETED annotation plumbing.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildRetentionCleanupJob renders the gpbackman retention
//     script (backup-info --type full|incremental, backup-delete --cascade,
//     backup-clean --older-than-days 30), labels the Job operation=cleanup, runs
//     via sh -c and sets TerminationMessagePolicy=FallbackToLogsOnError.
//   - Controller parity: a Succeeded backup with retention set yields exactly one
//     cleanup Job (idempotent); a Succeeded cleanup Job carrying a
//     RETENTION_DELETED pod message is annotated and drives the retention metric.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario79-retention.sh, which drives the full 79a-d
//     retention lifecycle on a running cluster (real gpbackup/gpbackman via
//     coordinator-exec; cleanup-Job creation + annotation->metric asserted from
//     the rendered operator spec / materialized Succeeded cleanup Job).
//
// This Go test never requires gpbackman binaries or a real cluster for its
// deterministic parts; the actual delete cycle + metric delta are the live shell
// step (scenario79-retention.sh).
// ============================================================================

const (
	envS79Cluster = "SCENARIO79_S3_CLUSTER"
	envS79Script  = "SCENARIO79_SCRIPT"

	scenario79E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario79E2ELatestTS    = "20260608060000"
)

// scenario79E2ECountingMetrics embeds NoopRecorder and counts only retention
// deletions so the e2e suite can assert the metric delta without a live registry.
type scenario79E2ECountingMetrics struct {
	metrics.NoopRecorder
	retentionDeleted int
}

func (m *scenario79E2ECountingMetrics) RecordBackupRetentionDeleted(_, _ string, n int) {
	m.retentionDeleted += n
}

// Scenario79RetentionE2ESuite tests the retention cleanup Job rendering, the
// idempotent cleanup-Job creation, and the annotation->metric plumbing.
type Scenario79RetentionE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario79(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario79RetentionE2ESuite))
}

func (s *Scenario79RetentionE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario79RetentionE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// scenario79E2ERetentionCluster builds a running cluster with an S3 (MinIO)
// backup destination and the full retention policy (mirrors scenario79-s3).
func scenario79E2ERetentionCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario79E2EBackupImage,
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
	return cluster
}

// scenario79E2ESucceededBackupJob builds a Succeeded backup-operation Job named
// like an on-demand backup so the operator parses the 14-digit timestamp.
func scenario79E2ESucceededBackupJob(cluster, ts string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
				util.LabelBackupType:      util.BackupTypeFull,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1, StartTime: &start, CompletionTime: &completion,
		},
	}
}

// scenario79E2ESucceededCleanupJob builds a Succeeded cleanup-operation Job keyed
// off the latest backup timestamp.
func scenario79E2ESucceededCleanupJob(cluster, ts string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RetentionCleanupJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationCleanup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1, StartTime: &start, CompletionTime: &completion,
		},
	}
}

// --- 79.1: builder parity (infra-free) — gpbackman retention script + spec ---

// TestE2E_Scenario79_RetentionScriptParity verifies BuildRetentionCleanupJob
// renders the real gpbackman retention commands for all three policies, labels
// the Job operation=cleanup, runs via sh -c, and sets the
// FallbackToLogsOnError termination policy.
func (s *Scenario79RetentionE2ESuite) TestE2E_Scenario79_RetentionScriptParity() {
	cluster := scenario79E2ERetentionCluster("test-s79e2e-script")

	job := builder.NewBuilder().BuildRetentionCleanupJob(cluster, scenario79E2ELatestTS)
	require.NotNil(s.T(), job)
	assert.Equal(s.T(),
		util.RetentionCleanupJobName(cluster.Name, scenario79E2ELatestTS), job.Name)
	assert.Equal(s.T(), util.BackupOperationCleanup,
		job.Labels[util.LabelBackupOperation])

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.Len(s.T(), container.Args, 1)
	script := container.Args[0]

	// 79a fullCount=3 / 79b incrementalCount=10 / 79c maxAge=30d.
	assert.Equal(s.T(), 1, strings.Count(script, "_gpbackman_timestamps 'full'"))
	assert.Contains(s.T(), script, "KEEP=3")
	assert.Equal(s.T(), 1, strings.Count(script, "_gpbackman_timestamps 'incremental'"))
	assert.Contains(s.T(), script, "KEEP=10")
	assert.Equal(s.T(), 1, strings.Count(script, "backup-clean --older-than-days 30"))
	assert.Contains(s.T(), script, "backup-delete --timestamp \"$1\" --cascade")
	assert.Contains(s.T(), script, "RETENTION_DELETED=")
	assert.Contains(s.T(), script, "/dev/termination-log")

	require.GreaterOrEqual(s.T(), len(container.Command), 2)
	assert.Contains(s.T(), container.Command, "-c")
	assert.Equal(s.T(), corev1.TerminationMessageFallbackToLogsOnError,
		container.TerminationMessagePolicy)

	// No legacy/invalid gpbackman tokens.
	assert.NotContains(s.T(), script, "gpbackman delete")
	assert.NotContains(s.T(), script, "--keep-full")
	assert.NotContains(s.T(), script, "--older-than ")
}

// --- 79.2: controller parity (infra-free) — cleanup creation + metric ---

// TestE2E_Scenario79_EnsureCleanupParity reconciles against a fake client seeded
// with a Succeeded backup (retention set) and asserts the operator creates
// exactly one cleanup Job keyed off the latest backup ts and is idempotent.
func (s *Scenario79RetentionE2ESuite) TestE2E_Scenario79_EnsureCleanupParity() {
	cluster := scenario79E2ERetentionCluster("test-s79e2e-ensure")
	backup := scenario79E2ESucceededBackupJob(cluster.Name, scenario79E2ELatestTS)

	env := testutil.NewTestK8sEnv(cluster, backup)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cleanupName := util.RetentionCleanupJobName(cluster.Name, scenario79E2ELatestTS)
	created, err := env.GetJob(s.ctx, cleanupName, cluster.Namespace)
	require.NoError(s.T(), err, "a cleanup Job keyed off the latest backup ts must exist")
	assert.Equal(s.T(), util.BackupOperationCleanup,
		created.Labels[util.LabelBackupOperation])

	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	cleanups := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationCleanup {
			cleanups++
		}
	}
	assert.Equal(s.T(), 1, cleanups,
		"reconcile must be idempotent: exactly one cleanup Job per latest backup")
}

// TestE2E_Scenario79_RetentionMetricParity reconciles against a fake client
// seeded with a Succeeded cleanup Job carrying
// avsoft.io/backup-retention-deleted=3 and asserts the metrics loop records the
// retention deletion count (annotation->metric plumbing).
func (s *Scenario79RetentionE2ESuite) TestE2E_Scenario79_RetentionMetricParity() {
	cluster := scenario79E2ERetentionCluster("test-s79e2e-metric")
	backup := scenario79E2ESucceededBackupJob(cluster.Name, scenario79E2ELatestTS)
	cleanup := scenario79E2ESucceededCleanupJob(cluster.Name, scenario79E2ELatestTS)
	cleanup.Annotations = map[string]string{
		util.AnnotationBackupRetentionDeleted: "3",
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, backup, cleanup).
		Build()
	m := &scenario79E2ECountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, testutil.NewTestK8sEnv().Recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 3, m.retentionDeleted,
		"a Succeeded annotated cleanup Job must drive RecordBackupRetentionDeleted by its count")
}

// --- 79.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario79_LiveRetentionLifecycle is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackman binaries. When live, it shells out to the scenario79 live script,
// which drives the full 79a-d retention lifecycle: 4 full backups (delete oldest
// full, keep 3); full + 11 incrementals (delete oldest beyond 10); an aged
// (>30d) history entry deleted by backup-clean; and the operator's cleanup-Job
// creation + metric increment after a successful backup.
func (s *Scenario79RetentionE2ESuite) TestE2E_Scenario79_LiveRetentionLifecycle() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live retention-cleanup verification")
	}

	cluster := os.Getenv(envS79Cluster)
	if cluster == "" {
		cluster = "scenario79-s3"
	}

	script := os.Getenv(envS79Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario79-retention.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full retention lifecycle and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario79 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario79 live script must pass all retention-cleanup checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
