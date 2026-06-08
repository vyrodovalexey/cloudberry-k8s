//go:build functional

package functional

import (
	"context"
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
// Scenario 76: Scheduled Backup via CronJob + Status Population (functional)
// ============================================================================
//
// Scenario 76 covers the SCHEDULED backup path: when spec.backup.schedule is set
// the operator reconciles a CronJob "{cluster}-backup-schedule" (ownerReferences
// -> CloudberryCluster, concurrencyPolicy Forbid, 3/3 history limits, jobTemplate
// pod restartPolicy Never). When the CronJob fires Kubernetes spawns a Job; the
// operator discovers the completed backup Job and populates status.* and a
// backupHistory entry (timestamp / type / status / size / duration).
//
// These tests black-box the operator through the public builder
// (BuildBackupCronJob) and the AdminReconciler with a fake client. They are
// deterministic and self-contained (no live infra). The matrix:
//
//	BuildBackupCronJob spec  -> TestFunctional_Scenario76_CronJobSpec
//	reconcile -> CronJobName -> TestFunctional_Scenario76_ReconcileCronJobName
//	status population        -> TestFunctional_Scenario76_StatusPopulation
//	14-digit TS fallback     -> TestFunctional_Scenario76_TimestampFallback
// ============================================================================

const (
	scenario76BackupImage = "cloudberry-backup:2.1.0"
	scenario76Schedule    = "0 2 * * *"
)

// scenario76TimestampRegex validates a gpbackup-style 14-digit YYYYMMDDHHMMSS.
var scenario76TimestampRegex = regexp.MustCompile(`^\d{14}$`)

// Scenario76Suite exercises the scheduled-backup CronJob + status population.
type Scenario76Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario76(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario76Suite))
}

func (s *Scenario76Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario76Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario76BackupSpec returns the Scenario 76 BackupSpec: a scheduled S3
// (MinIO) destination with Secret credentials and multipart tuning.
func scenario76BackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: scenario76Schedule,
		Image:    scenario76BackupImage,
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
}

// scenario76Cluster builds a Running cluster (pending generation) with the
// Scenario 76 scheduled backup spec.
func scenario76Cluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = scenario76BackupSpec()
	return cluster
}

// scenario76CompletedBackupJob builds a succeeded backup Job fixture carrying the
// cluster/backup-operation labels and the avsoft.io/backup-size-bytes annotation
// so the operator's status reconcile can populate Size.
func scenario76CompletedBackupJob(cluster, name string, start, completion metav1.Time) *batchv1.Job {
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

// --- BuildBackupCronJob spec ---

// TestFunctional_Scenario76_CronJobSpec asserts BuildBackupCronJob produces the
// CronJob the scheduled-backup contract requires: name {cluster}-backup-schedule,
// ownerReferences -> the CloudberryCluster (controller=true), concurrencyPolicy
// Forbid, successful/failed history limits 3, the configured schedule, the
// jobTemplate labels (op=backup + cluster) and the jobTemplate pod restartPolicy
// Never.
func (s *Scenario76Suite) TestFunctional_Scenario76_CronJobSpec() {
	cluster := scenario76Cluster("s76-cron")

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron, "CronJob must be built when a schedule is set")

	// Name.
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), cron.Name)
	assert.Equal(s.T(), cluster.Name+"-backup-schedule", cron.Name)

	// OwnerReferences -> CloudberryCluster (controller).
	require.Len(s.T(), cron.OwnerReferences, 1)
	owner := cron.OwnerReferences[0]
	assert.Equal(s.T(), "CloudberryCluster", owner.Kind)
	assert.Equal(s.T(), cluster.Name, owner.Name)
	require.NotNil(s.T(), owner.Controller)
	assert.True(s.T(), *owner.Controller, "owner reference must be the controller")

	// ConcurrencyPolicy Forbid.
	assert.Equal(s.T(), batchv1.ForbidConcurrent, cron.Spec.ConcurrencyPolicy)

	// History limits 3/3.
	require.NotNil(s.T(), cron.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(s.T(), cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.FailedJobsHistoryLimit)

	// Schedule.
	assert.Equal(s.T(), scenario76Schedule, cron.Spec.Schedule)

	// jobTemplate labels include op=backup + cluster.
	jobLabels := cron.Spec.JobTemplate.Labels
	assert.Equal(s.T(), util.BackupOperationBackup, jobLabels[util.LabelBackupOperation])
	assert.Equal(s.T(), cluster.Name, jobLabels[util.LabelCluster])

	// jobTemplate pod restartPolicy Never.
	podSpec := cron.Spec.JobTemplate.Spec.Template.Spec
	assert.Equal(s.T(), corev1.RestartPolicyNever, podSpec.RestartPolicy)
}

// --- reconcile sets Status.CronJobName ---

// TestFunctional_Scenario76_ReconcileCronJobName applies a cluster with backups
// enabled + a schedule, runs Reconcile and asserts the CronJob is created and
// status.cronJobName == {cluster}-backup-schedule.
func (s *Scenario76Suite) TestFunctional_Scenario76_ReconcileCronJobName() {
	cluster := scenario76Cluster("s76-recon")
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be created when a schedule is set")
	assert.Equal(s.T(), scenario76Schedule, cron.Spec.Schedule)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)
}

// --- status population (applyBackupJobToStatus via reconcile) ---

// TestFunctional_Scenario76_StatusPopulation seeds a completed backup Job (with
// the size annotation) and asserts the operator's status reconcile populates
// LastBackupStatus/Type/JobName/Timestamp(14-digit)/Time and a backupHistory
// entry carrying Timestamp(14-digit) / Type=full / Status=Success / Size!="" /
// Duration!="".
func (s *Scenario76Suite) TestFunctional_Scenario76_StatusPopulation() {
	cluster := scenario76Cluster("s76-status")
	start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario76CompletedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"), start, completion)

	env := testutil.NewTestK8sEnv(cluster, job)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), "full", updated.Status.LastBackupType)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	assert.Regexp(s.T(), scenario76TimestampRegex, updated.Status.LastBackupTimestamp,
		"lastBackupTimestamp must be 14-digit YYYYMMDDHHMMSS")
	require.NotNil(s.T(), updated.Status.LastBackupTime, "lastBackupTime must be set")

	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	entry := updated.Status.BackupHistory[0]
	assert.Regexp(s.T(), scenario76TimestampRegex, entry.Timestamp,
		"backupHistory[0].timestamp must be 14-digit")
	assert.Equal(s.T(), "full", entry.Type)
	assert.Equal(s.T(), "Success", entry.Status)
	assert.NotEmpty(s.T(), entry.Size, "backupHistory[0].size must be populated when the annotation is set")
	assert.NotEmpty(s.T(), entry.Duration, "backupHistory[0].duration must be populated")
}

// --- 14-digit timestamp fallback for CronJob-spawned Jobs ---

// TestFunctional_Scenario76_TimestampFallback seeds a Job named like a
// CronJob-spawned Job ("{cluster}-backup-schedule-<hash>") whose name does NOT
// carry a parseable 14-digit timestamp, and asserts the operator falls back to
// the Job's CompletionTime so status.lastBackupTimestamp is still a valid
// 14-digit value (derived from CompletionTime in UTC).
func (s *Scenario76Suite) TestFunctional_Scenario76_TimestampFallback() {
	cluster := scenario76Cluster("s76-fallback")
	start := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	completion := metav1.NewTime(time.Now())
	jobName := util.BackupCronJobName(cluster.Name) + "-28abc12"
	job := scenario76CompletedBackupJob(cluster.Name, jobName, start, completion)

	env := testutil.NewTestK8sEnv(cluster, job)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	assert.Regexp(s.T(), scenario76TimestampRegex, updated.Status.LastBackupTimestamp,
		"lastBackupTimestamp must fall back to a 14-digit value derived from CompletionTime")
	want := completion.UTC().Format("20060102150405")
	assert.Equal(s.T(), want, updated.Status.LastBackupTimestamp,
		"fallback timestamp must equal CompletionTime formatted as YYYYMMDDHHMMSS (UTC)")
}
