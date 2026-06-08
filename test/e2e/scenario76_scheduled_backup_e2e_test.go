//go:build e2e

package e2e

import (
	"context"
	"os"
	"regexp"
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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 76: Scheduled Backup via CronJob + Status Population (E2E)
// ============================================================================
//
// User journey: a user sets spec.backup.schedule on a cluster; the operator
// reconciles a CronJob "{cluster}-backup-schedule" (ownerReferences ->
// CloudberryCluster, concurrencyPolicy Forbid, 3/3 history limits, jobTemplate
// pod restartPolicy Never). When the CronJob fires, Kubernetes spawns a Job; the
// operator discovers the completed backup Job and populates status.* and a
// backupHistory entry (timestamp / type / status / size / duration).
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity for the scheduled CronJob spec (name, ownerRefs, Forbid,
//     3/3 history, restartPolicy Never).
//   - Controller status-population parity: reconcile against a fake client seeded
//     with a completed backup Job (carrying the size annotation) populates
//     status.lastBackup* + a backupHistory entry; and the 14-digit timestamp
//     fallback for CronJob-spawned Job names.
//   - A live-cluster portion gated on KUBECONFIG that self-skips.
//
// This Go test never requires gpbackup binaries; the actual scheduled-CronJob
// fire + real gpbackup data cycle is the live shell step
// (scenario76-scheduled-backup.sh).
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion and is declared
// package-scoped by the Scenario 69 e2e suite.

const (
	scenario76E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario76E2ESchedule    = "0 2 * * *"
)

// scenario76E2ETimestampRegex validates a gpbackup-style 14-digit YYYYMMDDHHMMSS.
var scenario76E2ETimestampRegex = regexp.MustCompile(`^\d{14}$`)

// Scenario76ScheduledBackupE2ESuite tests the scheduled-backup CronJob + status.
type Scenario76ScheduledBackupE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario76(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario76ScheduledBackupE2ESuite))
}

func (s *Scenario76ScheduledBackupE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario76ScheduledBackupE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario76E2ECluster builds a running cluster with the Scenario 76 scheduled
// backup spec (full S3 destination + multipart tuning).
func scenario76E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: scenario76E2ESchedule,
		Image:    scenario76E2EBackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
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

// scenario76E2ECompletedBackupJob builds a succeeded backup Job fixture with the
// cluster/backup-operation labels and the size annotation.
func scenario76E2ECompletedBackupJob(cluster, name string, start, completion metav1.Time) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			Annotations: map[string]string{
				util.AnnotationBackupSizeBytes: "104857600",
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
}

// --- 76.1: builder parity (infra-free) — scheduled CronJob spec ---

// TestE2E_Scenario76_CronJobBuilderParity verifies BuildBackupCronJob produces
// the scheduled CronJob spec the contract requires.
func (s *Scenario76ScheduledBackupE2ESuite) TestE2E_Scenario76_CronJobBuilderParity() {
	cluster := scenario76E2ECluster("test-s76e2e")

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron, "CronJob must be built when a schedule is set")
	assert.IsType(s.T(), &batchv1.CronJob{}, cron)

	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), cron.Name)
	assert.Equal(s.T(), cluster.Name+"-backup-schedule", cron.Name)

	require.Len(s.T(), cron.OwnerReferences, 1)
	owner := cron.OwnerReferences[0]
	assert.Equal(s.T(), "CloudberryCluster", owner.Kind)
	assert.Equal(s.T(), cluster.Name, owner.Name)
	require.NotNil(s.T(), owner.Controller)
	assert.True(s.T(), *owner.Controller)

	assert.Equal(s.T(), batchv1.ForbidConcurrent, cron.Spec.ConcurrencyPolicy)
	require.NotNil(s.T(), cron.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(s.T(), cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), scenario76E2ESchedule, cron.Spec.Schedule)

	podSpec := cron.Spec.JobTemplate.Spec.Template.Spec
	assert.Equal(s.T(), corev1.RestartPolicyNever, podSpec.RestartPolicy)
}

// --- 76.2: controller status-population parity (infra-free) ---

// TestE2E_Scenario76_StatusPopulationParity reconciles against a fake client
// seeded with a completed backup Job (size annotation set) and asserts the
// operator populates status.lastBackup* and a backupHistory entry with a
// non-empty size + duration.
func (s *Scenario76ScheduledBackupE2ESuite) TestE2E_Scenario76_StatusPopulationParity() {
	cluster := scenario76E2ECluster("test-s76e2e-status")
	start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario76E2ECompletedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"), start, completion)

	env := testutil.NewTestK8sEnv(cluster, job)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)
	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), "full", updated.Status.LastBackupType)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	assert.Regexp(s.T(), scenario76E2ETimestampRegex, updated.Status.LastBackupTimestamp)
	require.NotNil(s.T(), updated.Status.LastBackupTime)

	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	entry := updated.Status.BackupHistory[0]
	assert.Regexp(s.T(), scenario76E2ETimestampRegex, entry.Timestamp)
	assert.Equal(s.T(), "full", entry.Type)
	assert.Equal(s.T(), "Success", entry.Status)
	assert.NotEmpty(s.T(), entry.Size)
	assert.NotEmpty(s.T(), entry.Duration)
}

// TestE2E_Scenario76_TimestampFallbackParity asserts the 14-digit timestamp
// fallback for CronJob-spawned Job names ("{cluster}-backup-schedule-<hash>").
func (s *Scenario76ScheduledBackupE2ESuite) TestE2E_Scenario76_TimestampFallbackParity() {
	cluster := scenario76E2ECluster("test-s76e2e-fallback")
	start := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	completion := metav1.NewTime(time.Now())
	jobName := util.BackupCronJobName(cluster.Name) + "-28abc12"
	job := scenario76E2ECompletedBackupJob(cluster.Name, jobName, start, completion)

	env := testutil.NewTestK8sEnv(cluster, job)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	assert.Regexp(s.T(), scenario76E2ETimestampRegex, updated.Status.LastBackupTimestamp)
	assert.Equal(s.T(), completion.UTC().Format("20060102150405"),
		updated.Status.LastBackupTimestamp)
}

// --- 76.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario76_LiveResourceCreation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it reconciles against a fake client seeded
// from the Scenario 76 spec, asserts the CronJob and S3 ConfigMap exist, and
// asserts the scheduled CronJob spec (parity with the live shell step).
func (s *Scenario76ScheduledBackupE2ESuite) TestE2E_Scenario76_LiveResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	cluster := scenario76E2ECluster("test-s76e2e-live")
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 ConfigMap should exist on a live-configured cluster")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be created when a schedule is set")
	assert.Equal(s.T(), batchv1.ForbidConcurrent, cron.Spec.ConcurrencyPolicy)
	require.NotNil(s.T(), cron.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(s.T(), cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), corev1.RestartPolicyNever,
		cron.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)
}
