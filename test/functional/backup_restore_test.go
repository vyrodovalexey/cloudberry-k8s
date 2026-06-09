//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// BackupRestoreSuite tests the gpbackup-centric backup/restore reconcile path
// and the backup/restore resource builders using fake clients (no live infra).
type BackupRestoreSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_BackupRestore(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(BackupRestoreSuite))
}

func (s *BackupRestoreSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *BackupRestoreSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// newS3BackupSpec returns a representative gpbackup-centric S3 backup spec.
func newS3BackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        5,
			IncrementalCount: 20,
			MaxAge:           "90d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "/backups",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name: "backup-s3-credentials",
				},
			},
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
			Jobs:             4,
			WithStats:        util.Ptr(true),
		},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:     4,
			CreateDb: true,
		},
	}
}

func (s *BackupRestoreSuite) newReconciler() *controller.AdminReconciler {
	return controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)
}

// TestFunctional_BackupS3Destination_ReconcilesSuccessfully verifies the full
// backup reconcile path for an S3 destination: the operator builds the S3 plugin
// ConfigMap and the scheduled CronJob, and marks the BackupConfigured condition.
func (s *BackupRestoreSuite) TestFunctional_BackupS3Destination_ReconcilesSuccessfully() {
	cluster := testutil.NewClusterBuilder("test-backup-s3", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = newS3BackupSpec()
	s.env = testutil.NewTestK8sEnv(cluster)

	result, err := s.newReconciler().Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// The S3 plugin ConfigMap is created with the rendered plugin template.
	cm, err := s.env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "backup S3 ConfigMap should be created")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")
	assert.Contains(s.T(), cm.Data["s3-plugin-config.yaml.tpl"], "gpbackup_s3_plugin")
	assert.Contains(s.T(), cm.Data["s3-plugin-config.yaml.tpl"], "${S3_BUCKET}")

	// The scheduled backup CronJob is created with the schedule + Forbid policy.
	cron := &batchv1.CronJob{}
	err = s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron)
	require.NoError(s.T(), err, "backup CronJob should be created")
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)
	assert.Equal(s.T(), batchv1.ForbidConcurrent, cron.Spec.ConcurrencyPolicy)
	require.NotNil(s.T(), cron.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(s.T(), cron.Spec.FailedJobsHistoryLimit)
	assert.Equal(s.T(), int32(3), *cron.Spec.FailedJobsHistoryLimit)

	// Verify the BackupConfigured condition is set and CronJob recorded in status.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == string(cbv1alpha1.ConditionBackupConfigured) {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			break
		}
	}
	assert.True(s.T(), conditionFound, "BackupConfigured condition should be set")
}

// TestFunctional_BackupCronJob_GpbackupArgs asserts the scheduled CronJob's
// gpbackup container carries the configured compression/jobs/with-stats args and
// that the pre-backup-check init container is present.
func (s *BackupRestoreSuite) TestFunctional_BackupCronJob_GpbackupArgs() {
	cluster := testutil.NewClusterBuilder("test-backup-args", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = newS3BackupSpec()
	s.env = testutil.NewTestK8sEnv(cluster)

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)

	podSpec := cron.Spec.JobTemplate.Spec.Template.Spec
	require.Len(s.T(), podSpec.Containers, 1)
	container := podSpec.Containers[0]
	assert.Equal(s.T(), "gpbackup", container.Name)

	// The gpbackup command/args are rendered into a single bash script argument,
	// with each arg single-quoted (e.g. '--jobs' '4').
	script := strings.Join(container.Args, " ")
	assert.Contains(s.T(), script, "gpbackup")
	assert.Contains(s.T(), script, "'--compression-level' '6'")
	assert.Contains(s.T(), script, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), script, "'--jobs' '4'")
	assert.Contains(s.T(), script, "'--with-stats'")

	// The pre-backup health-check init container must run before gpbackup.
	require.NotEmpty(s.T(), podSpec.InitContainers)
	assert.Equal(s.T(), "pre-backup-check", podSpec.InitContainers[0].Name)
}

// TestFunctional_BackupJob_OnDemand asserts an on-demand backup Job is built with
// the right name, operation label and gpbackup args including single-data-file.
func (s *BackupRestoreSuite) TestFunctional_BackupJob_OnDemand() {
	cluster := testutil.NewClusterBuilder("test-backup-ondemand", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = newS3BackupSpec()
	cluster.Spec.Backup.Gpbackup.SingleDataFile = true
	cluster.Spec.Backup.Gpbackup.CopyQueueSize = 4
	cluster.Spec.Backup.Gpbackup.Jobs = 0

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260519020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupJobName(cluster.Name, "20260519020000"), job.Name)
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])

	script := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(s.T(), script, "'--single-data-file'")
	assert.Contains(s.T(), script, "'--copy-queue-size' '4'")
	assert.Contains(s.T(), script, "'--dbname' 'mydb'")
	assert.NotContains(s.T(), script, "'--jobs'")
}

// TestFunctional_BackupIncremental_ConfigApplied verifies the incremental flag is
// applied to gpbackup args via the cluster spec and per-request type.
func (s *BackupRestoreSuite) TestFunctional_BackupIncremental_ConfigApplied() {
	cluster := testutil.NewClusterBuilder("test-backup-incr", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = newS3BackupSpec()
	cluster.Spec.Backup.Schedule = "0 */6 * * *"
	cluster.Spec.Backup.Gpbackup.Incremental = true
	cluster.Spec.Backup.Gpbackup.LeafPartitionData = true

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp:     "20260519080000",
		Type:          "incremental",
		FromTimestamp: "20260519020000",
	})
	require.NotNil(s.T(), job)
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(s.T(), script, "'--incremental'")
	assert.Contains(s.T(), script, "'--leaf-partition-data'")
	assert.Contains(s.T(), script, "'--from-timestamp' '20260519020000'")
}

// TestFunctional_RestoreJob_BuildsGprestore asserts a restore Job is built with
// the gprestore container, timestamp and configured options.
func (s *BackupRestoreSuite) TestFunctional_RestoreJob_BuildsGprestore() {
	cluster := testutil.NewClusterBuilder("test-restore", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = newS3BackupSpec()

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.RestoreJobName(cluster.Name, "20260519020000"), job.Name)
	assert.Equal(s.T(), util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])

	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gprestore", container.Name)
	script := strings.Join(container.Args, " ")
	assert.Contains(s.T(), script, "gprestore")
	assert.Contains(s.T(), script, "'--timestamp' '20260519020000'")
	assert.Contains(s.T(), script, "'--create-db'")
	assert.Contains(s.T(), script, "'--jobs' '4'")
}

// TestFunctional_BackupRetentionPolicy_Applied verifies the retention policy maps
// to gpbackman cleanup args and survives reconciliation.
func (s *BackupRestoreSuite) TestFunctional_BackupRetentionPolicy_Applied() {
	cluster := testutil.NewClusterBuilder("test-backup-retention", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = newS3BackupSpec()
	s.env = testutil.NewTestK8sEnv(cluster)

	result, err := s.newReconciler().Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(5), updated.Spec.Backup.Retention.FullCount)
	assert.Equal(s.T(), "90d", updated.Spec.Backup.Retention.MaxAge)

	// The retention cleanup Job applies the retention policy via the REAL gpbackman
	// CLI: time-based retention uses `backup-clean --older-than-days <N>` and
	// count-based retention enumerates with `backup-info` + deletes the oldest
	// excess with `backup-delete --timestamp <ts> --cascade` (the older drafts'
	// `--older-than`/`--keep-full` flags do not exist in gpbackman — see spec 11
	// §Retention). maxAge="90d" -> --older-than-days 90; fullCount=5 -> the
	// count-based backup-info/backup-delete loop.
	cleanup := builder.NewBuilder().BuildRetentionCleanupJob(cluster, "20260519020000")
	require.NotNil(s.T(), cleanup)
	assert.Equal(s.T(), util.BackupOperationCleanup, cleanup.Labels[util.LabelBackupOperation])
	script := strings.Join(cleanup.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(s.T(), script, "backup-clean")
	assert.Contains(s.T(), script, "--older-than-days")
	assert.Contains(s.T(), script, "90")
	assert.Contains(s.T(), script, "backup-delete")
	assert.Contains(s.T(), script, "--cascade")
}

// TestFunctional_PostRestoreValidationJob_Built verifies the post-restore
// validation Job is built with the validation container.
func (s *BackupRestoreSuite) TestFunctional_PostRestoreValidationJob_Built() {
	cluster := testutil.NewClusterBuilder("test-validate", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = newS3BackupSpec()

	job := builder.NewBuilder().BuildPostRestoreValidationJob(cluster, &builder.ValidationJobOptions{
		Timestamp: "20260519020000",
		Database:  "mydb",
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(),
		util.PostRestoreValidationJobName(cluster.Name, "20260519020000"), job.Name)
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(s.T(), script, "post-restore-validate")
}

// TestFunctional_BackupDisabled_SkipsReconcile verifies no backup resources are
// created and the BackupConfigured condition is not set when backup is disabled.
func (s *BackupRestoreSuite) TestFunctional_BackupDisabled_SkipsReconcile() {
	cluster := testutil.NewClusterBuilder("test-backup-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	result, err := s.newReconciler().Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// No S3 ConfigMap should exist for a disabled backup.
	_, err = s.env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	assert.Error(s.T(), err, "no backup ConfigMap should be created when backup is disabled")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	for _, c := range updated.Status.Conditions {
		assert.NotEqual(s.T(), string(cbv1alpha1.ConditionBackupConfigured), c.Type,
			"BackupConfigured condition should not be set when backup is disabled")
	}
}

// TestFunctional_BackupLocalDestination_ReconcilesSuccessfully verifies a local
// (PVC-backed) destination reconciles without creating an S3 ConfigMap.
func (s *BackupRestoreSuite) TestFunctional_BackupLocalDestination_ReconcilesSuccessfully() {
	cluster := testutil.NewClusterBuilder("test-backup-local", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			Local: &cbv1alpha1.LocalDestination{
				Path:                  "/backups",
				PersistentVolumeClaim: "backup-pvc",
			},
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 3,
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	result, err := s.newReconciler().Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Local destinations do not produce an S3 plugin ConfigMap.
	assert.Nil(s.T(), builder.NewBuilder().BuildBackupS3ConfigMap(cluster))

	// The backup Job mounts the configured PVC at the backup path.
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260519020000",
	})
	require.NotNil(s.T(), job)
	var mountFound bool
	for _, vm := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if vm.MountPath == "/backups" {
			mountFound = true
		}
	}
	assert.True(s.T(), mountFound, "local backup Job should mount the PVC at the backup path")
}
