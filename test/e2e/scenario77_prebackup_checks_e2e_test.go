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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 77: Pre-Backup Health Checks (E2E)
// ============================================================================
//
// User journey: a backup Job's `pre-backup-check` init container validates
// cluster + destination health BEFORE the backup proceeds. When a sub-check
// fails the init container exits non-zero (the main gpbackup container never
// starts), the operator records status.lastBackupStatus=Failed and emits a
// Warning Kubernetes Event (reason BackupFailed); healing the fault lets a fresh
// backup reach Success. Four sub-checks:
//
//	77a Segments-up    : a segment is down (gp_segment_configuration status='d').
//	77b Long-running   : a transaction older than the threshold is open.
//	77c S3 reachability: a SigV4 HEAD against a wrong bucket/creds returns non-2xx.
//	77d Local disk     : the backup PVC free space is below minBackupDiskFreeKB.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob/BuildBackupCronJob prepend the
//     pre-backup-check init container and its script wires all four checks.
//   - Controller parity: a Failed backup Job yields lastBackupStatus=Failed and
//     a single Warning/BackupFailed Event; a Succeeded backup emits none.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to the scenario77-prebackup-checks.sh live script, which drives
//     each of 77a-77d (fault -> block -> status -> event -> heal -> success).
//
// This Go test never requires gpbackup binaries or a real cluster for its
// deterministic parts; the actual fault->block->heal->success cycle is the live
// shell step (scenario77-prebackup-checks.sh).
// ============================================================================

// envKubeconfig (KUBECONFIG) is declared by the Scenario 69 e2e suite and reused
// here to gate the live-cluster portion. envS77Cluster / envS77LocalCluster /
// envS77Script override the live target names + script path.
const (
	envS77Cluster      = "SCENARIO77_S3_CLUSTER"
	envS77LocalCluster = "SCENARIO77_LOCAL_CLUSTER"
	envS77Script       = "SCENARIO77_SCRIPT"

	scenario77E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario77E2ELocalPath   = "/backups"
)

// Scenario77PreBackupChecksE2ESuite tests the pre-backup-check init container +
// the backup-failure Warning Event flow.
type Scenario77PreBackupChecksE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario77(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario77PreBackupChecksE2ESuite))
}

func (s *Scenario77PreBackupChecksE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario77PreBackupChecksE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario77E2ES3Cluster builds a running cluster with an S3 (MinIO) backup
// destination (used for 77a/77b/77c parity).
func scenario77E2ES3Cluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario77E2EBackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
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

// scenario77E2ELocalCluster builds a running cluster with a local (PVC-backed)
// backup destination (used for 77d parity).
func scenario77E2ELocalCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario77E2EBackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			Local: &cbv1alpha1.LocalDestination{
				Path:                  scenario77E2ELocalPath,
				PersistentVolumeClaim: "scenario77-backup-pvc",
			},
		},
	}
	return cluster
}

// scenario77E2EFailedBackupJob builds a Failed backup-operation Job fixture.
func scenario77E2EFailedBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Failed:         1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
}

// scenario77E2EWarningBackupFailed drains the FakeRecorder and counts the
// Warning/BackupFailed events.
func scenario77E2EWarningBackupFailed(recorder *record.FakeRecorder) (int, []string) {
	count := 0
	var all []string
	for {
		select {
		case e := <-recorder.Events:
			all = append(all, e)
			if strings.Contains(e, corev1.EventTypeWarning) &&
				strings.Contains(e, cbv1alpha1.EventReasonBackupFailed) {
				count++
			}
		default:
			return count, all
		}
	}
}

// --- 77.1: builder parity (infra-free) — init container + four checks ---

// TestE2E_Scenario77_InitContainerParity verifies BuildBackupJob prepends the
// pre-backup-check init container for both S3 and local destinations and that
// its script wires the four sub-checks with their blocking semantics.
func (s *Scenario77PreBackupChecksE2ESuite) TestE2E_Scenario77_InitContainerParity() {
	s3 := scenario77E2ES3Cluster("test-s77e2e-s3")
	s3Job := builder.NewBuilder().BuildBackupJob(s3, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), s3Job)
	require.Len(s.T(), s3Job.Spec.Template.Spec.InitContainers, 1)
	assert.Equal(s.T(), "pre-backup-check", s3Job.Spec.Template.Spec.InitContainers[0].Name)
	s3Script := strings.Join(s3Job.Spec.Template.Spec.InitContainers[0].Args, "\n")

	// 77a + 77b are destination-independent.
	assert.Contains(s.T(), s3Script, "gp_segment_configuration")
	assert.Contains(s.T(), s3Script, "status='d'")
	assert.Contains(s.T(), s3Script, "pg_stat_activity")
	assert.Contains(s.T(), s3Script, "interval '3600 seconds'")
	// 77c S3 reachability (fail-closed SigV4 HEAD).
	assert.Contains(s.T(), s3Script, "AWS4-HMAC-SHA256")
	assert.Contains(s.T(), s3Script, "-X HEAD")
	assert.Contains(s.T(), s3Script, "${S3_BUCKET}")
	assert.Contains(s.T(), s3Script, "2??|3??")
	assert.Contains(s.T(), s3Script, "exit 1")

	local := scenario77E2ELocalCluster("test-s77e2e-local")
	localJob := builder.NewBuilder().BuildBackupJob(local, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), localJob)
	require.Len(s.T(), localJob.Spec.Template.Spec.InitContainers, 1)
	localScript := strings.Join(localJob.Spec.Template.Spec.InitContainers[0].Args, "\n")
	// 77d local disk-space.
	assert.Contains(s.T(), localScript, "df -Pk")
	assert.Contains(s.T(), localScript, scenario77E2ELocalPath)
	assert.Contains(s.T(), localScript, "1048576")
	assert.Contains(s.T(), localScript, "exit 1")
	// The local destination must NOT carry the S3 HEAD.
	assert.NotContains(s.T(), localScript, "AWS4-HMAC-SHA256")
}

// --- 77.2: controller failure-event parity (infra-free) ---

// TestE2E_Scenario77_FailedBackupEventParity reconciles against a fake client
// seeded with a Failed backup Job and asserts the operator records
// lastBackupStatus=Failed and emits exactly one Warning/BackupFailed Event.
func (s *Scenario77PreBackupChecksE2ESuite) TestE2E_Scenario77_FailedBackupEventParity() {
	cluster := scenario77E2ES3Cluster("test-s77e2e-failed")
	job := scenario77E2EFailedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608070000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)

	count, all := scenario77E2EWarningBackupFailed(recorder)
	assert.Equal(s.T(), 1, count,
		"expected exactly one Warning/BackupFailed event: %v", all)
}

// TestE2E_Scenario77_SucceededBackupNoEventParity asserts a Succeeded backup Job
// emits no BackupFailed Warning.
func (s *Scenario77PreBackupChecksE2ESuite) TestE2E_Scenario77_SucceededBackupNoEventParity() {
	cluster := scenario77E2ES3Cluster("test-s77e2e-success")
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupJobName(cluster.Name, "20260608070000"),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1, StartTime: &start, CompletionTime: &completion,
		},
	}

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)

	count, all := scenario77E2EWarningBackupFailed(recorder)
	assert.Equal(s.T(), 0, count,
		"a succeeded backup must not emit a BackupFailed Warning: %v", all)
}

// --- 77.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario77_LivePreBackupChecks is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it shells out to the scenario77 live script,
// which drives each of 77a-77d through fault -> init blocks -> Job Failed ->
// lastBackupStatus=Failed -> BackupFailed Warning Event -> heal -> Success.
func (s *Scenario77PreBackupChecksE2ESuite) TestE2E_Scenario77_LivePreBackupChecks() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live pre-backup-check verification")
	}

	cluster := os.Getenv(envS77Cluster)
	if cluster == "" {
		cluster = "scenario77-s3"
	}
	localCluster := os.Getenv(envS77LocalCluster)
	if localCluster == "" {
		localCluster = "scenario77-local"
	}

	script := os.Getenv(envS77Script)
	if script == "" {
		// Resolve the live script relative to this test file's package dir.
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario77-prebackup-checks.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// all four sub-checks and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--local-cluster", localCluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario77 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario77 live script must pass all four sub-checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
