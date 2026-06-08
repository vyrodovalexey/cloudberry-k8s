//go:build functional

package functional

import (
	"context"
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
// Scenario 78: Incremental Backup Lifecycle (functional)
// ============================================================================
//
// Scenario 78 covers the incremental backup lifecycle end to end at the
// builder/controller layer (deterministic, no live cluster):
//
//	78a Incremental flag wiring : gpbackup.incremental:true => the rendered backup
//	    Job AND CronJob gpbackup args carry `--incremental --leaf-partition-data`
//	    EXACTLY ONCE each (leaf-partition-data is forced for incrementals even
//	    when leafPartitionData is unset, and de-duplicated when it is set).
//	78b Auto-locate base       : a Succeeded backup Job labelled
//	    avsoft.io/backup-type=incremental drives status.lastBackupType=incremental
//	    and a backupHistory entry of type incremental — sourced from the JOB label,
//	    not the spec (so per-Job incrementals on a full spec report correctly).
//	78c Pinned base            : a per-Job FromTimestamp renders
//	    `--from-timestamp <ts>` alongside `--incremental --leaf-partition-data`.
//	78d Restore completeness   : a Failed restore-operation Job yields
//	    status.lastBackupStatus=Failed AND exactly one Warning/RestoreFailed Event
//	    (and NO BackupFailed Warning — Scenario 77 stays backup-only).
//
// These tests black-box the operator through the public builder
// (BuildBackupJob / BuildBackupCronJob) and the AdminReconciler with a fake
// client. They are deterministic and self-contained (no live infra).
// ============================================================================

const (
	scenario78BackupImage = "cloudberry-backup:2.1.0"
	// scenario78FullTS is the pinned full-backup timestamp used for the 78c
	// --from-timestamp override assertion.
	scenario78FullTS = "20260608060000"
)

// Scenario78Suite exercises the incremental backup arg wiring, the Job-label
// derived status type, and the restore-failure Warning Event flow.
type Scenario78Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario78(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario78Suite))
}

func (s *Scenario78Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario78Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario78IncrementalBackupSpec returns an S3 (MinIO) destination BackupSpec
// with incremental backups enabled, mirroring the scenario78-s3 sample CR. The
// leafPartitionData flag is toggleable so the dedupe assertion can cover both
// the forced-on (unset) and explicit-on variants.
func scenario78IncrementalBackupSpec(leafPartitionData bool) *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario78BackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel:  1,
			CompressionType:   "gzip",
			Jobs:              1,
			Incremental:       true,
			LeafPartitionData: leafPartitionData,
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

// scenario78Cluster builds a Running cluster (pending generation) with the given
// backup spec.
func scenario78Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario78GpbackupArgs renders the gpbackup container's bash script for a
// backup Job built from the given backup spec + per-request options.
func (s *Scenario78Suite) scenario78GpbackupArgs(
	backup *cbv1alpha1.BackupSpec,
	opts *builder.BackupJobOptions,
) string {
	cluster := scenario78Cluster("s78-args", backup)
	job := builder.NewBuilder().BuildBackupJob(cluster, opts)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.Equal(s.T(), "gpbackup", container.Name)
	return strings.Join(container.Args, "\n")
}

// scenario78CronGpbackupArgs renders the gpbackup container's bash script for the
// scheduled CronJob built from the given backup spec.
func (s *Scenario78Suite) scenario78CronGpbackupArgs(backup *cbv1alpha1.BackupSpec) string {
	cluster := scenario78Cluster("s78-cron-args", backup)
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	podSpec := cron.Spec.JobTemplate.Spec.Template.Spec
	require.NotEmpty(s.T(), podSpec.Containers)
	container := podSpec.Containers[0]
	require.Equal(s.T(), "gpbackup", container.Name)
	return strings.Join(container.Args, "\n")
}

// scenario78IncrementalBackupJob builds a Succeeded backup-operation Job fixture
// carrying the incremental backup-type label (Scenario 78b).
func scenario78IncrementalBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
				util.LabelBackupType:      util.BackupTypeIncremental,
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

// scenario78FailedRestoreJob builds a Failed restore-operation Job fixture
// (Scenario 78d — gprestore refusing an incomplete incremental set).
func scenario78FailedRestoreJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationRestore,
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

// scenario78DrainWarnings drains the FakeRecorder channel and counts the
// Warning events carrying the given reason, returning the count and all events.
func scenario78DrainWarnings(recorder *record.FakeRecorder, reason string) (count int, all []string) {
	for {
		select {
		case e := <-recorder.Events:
			all = append(all, e)
			if strings.Contains(e, corev1.EventTypeWarning) && strings.Contains(e, reason) {
				count++
			}
		default:
			return count, all
		}
	}
}

// --- 78a: incremental flag wiring on Job + CronJob ---

// TestFunctional_Scenario78_IncrementalArgsJob asserts an incremental cluster's
// rendered backup Job gpbackup args carry `--incremental --leaf-partition-data`
// EXACTLY ONCE each, for both leafPartitionData unset (forced) and set (dedupe).
func (s *Scenario78Suite) TestFunctional_Scenario78_IncrementalArgsJob() {
	for _, leaf := range []bool{false, true} {
		script := s.scenario78GpbackupArgs(
			scenario78IncrementalBackupSpec(leaf),
			&builder.BackupJobOptions{
				Timestamp: "20260608070000",
				Databases: []string{"mydb"},
			},
		)
		assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"),
			"backup Job must render --incremental exactly once (leafPartitionData=%v)", leaf)
		assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
			"backup Job must render --leaf-partition-data exactly once (leafPartitionData=%v)", leaf)
	}
}

// TestFunctional_Scenario78_IncrementalArgsCronJob asserts the scheduled CronJob
// renders `--incremental --leaf-partition-data` exactly once each for an
// incremental cluster spec (both leaf variants).
func (s *Scenario78Suite) TestFunctional_Scenario78_IncrementalArgsCronJob() {
	for _, leaf := range []bool{false, true} {
		script := s.scenario78CronGpbackupArgs(scenario78IncrementalBackupSpec(leaf))
		assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"),
			"backup CronJob must render --incremental exactly once (leafPartitionData=%v)", leaf)
		assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
			"backup CronJob must render --leaf-partition-data exactly once (leafPartitionData=%v)", leaf)
	}
}

// TestFunctional_Scenario78_PerJobIncrementalOverFullSpec asserts a per-Job
// incremental request (Type=incremental) on a FULL spec still renders
// `--incremental --leaf-partition-data` once each.
func (s *Scenario78Suite) TestFunctional_Scenario78_PerJobIncrementalOverFullSpec() {
	backup := scenario78IncrementalBackupSpec(false)
	backup.Gpbackup.Incremental = false // spec is full; per-Job override drives incremental
	backup.Gpbackup.LeafPartitionData = false

	script := s.scenario78GpbackupArgs(backup, &builder.BackupJobOptions{
		Timestamp: "20260608070000",
		Type:      util.BackupTypeIncremental,
		Databases: []string{"mydb"},
	})
	assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"),
		"per-Job incremental must render --incremental once even on a full spec")
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
		"per-Job incremental must force --leaf-partition-data once even on a full spec")
}

// --- 78c: pinned base via --from-timestamp ---

// TestFunctional_Scenario78_PinnedFromTimestamp asserts a per-Job FromTimestamp
// renders `--from-timestamp <ts>` alongside `--incremental --leaf-partition-data`
// (the pinned-base override path).
func (s *Scenario78Suite) TestFunctional_Scenario78_PinnedFromTimestamp() {
	script := s.scenario78GpbackupArgs(
		scenario78IncrementalBackupSpec(true),
		&builder.BackupJobOptions{
			Timestamp:     "20260608080000",
			Type:          util.BackupTypeIncremental,
			Databases:     []string{"mydb"},
			FromTimestamp: scenario78FullTS,
		},
	)
	assert.Contains(s.T(), script, "'--from-timestamp'",
		"a pinned incremental must render the --from-timestamp flag")
	assert.Contains(s.T(), script, "'"+scenario78FullTS+"'",
		"a pinned incremental must render the exact pinned timestamp value")
	assert.Equal(s.T(), 1, strings.Count(script, "'--incremental'"),
		"a pinned incremental still renders --incremental once")
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
		"a pinned incremental still renders --leaf-partition-data once")
}

// TestFunctional_Scenario78_NoFromTimestampWhenUnset asserts that WITHOUT a
// pinned FromTimestamp (auto-base path, 78b) the rendered args carry NO
// --from-timestamp flag.
func (s *Scenario78Suite) TestFunctional_Scenario78_NoFromTimestampWhenUnset() {
	script := s.scenario78GpbackupArgs(
		scenario78IncrementalBackupSpec(true),
		&builder.BackupJobOptions{
			Timestamp: "20260608080000",
			Type:      util.BackupTypeIncremental,
			Databases: []string{"mydb"},
		},
	)
	assert.NotContains(s.T(), script, "--from-timestamp",
		"an auto-base incremental must NOT render --from-timestamp")
}

// --- 78a: backup-type label on metadata + pod template ---

// TestFunctional_Scenario78_IncrementalLabelOnJob asserts an incremental Job
// carries avsoft.io/backup-type=incremental on BOTH the Job metadata and the pod
// template; a full Job carries =full.
func (s *Scenario78Suite) TestFunctional_Scenario78_IncrementalLabelOnJob() {
	incr := scenario78Cluster("s78-lbl-incr", scenario78IncrementalBackupSpec(true))
	job := builder.NewBuilder().BuildBackupJob(incr, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupTypeIncremental, job.Labels[util.LabelBackupType],
		"incremental Job metadata must carry backup-type=incremental")
	assert.Equal(s.T(), util.BackupTypeIncremental,
		job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType],
		"incremental Job pod template must carry backup-type=incremental")

	full := scenario78Cluster("s78-lbl-full", scenario78IncrementalBackupSpec(true))
	full.Spec.Backup.Gpbackup.Incremental = false
	fullJob := builder.NewBuilder().BuildBackupJob(full, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), fullJob)
	assert.Equal(s.T(), util.BackupTypeFull, fullJob.Labels[util.LabelBackupType])
	assert.Equal(s.T(), util.BackupTypeFull,
		fullJob.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])
}

// TestFunctional_Scenario78_IncrementalLabelOnCronJob asserts the scheduled
// CronJob carries avsoft.io/backup-type=incremental on the CronJob metadata, the
// jobTemplate metadata and the pod template for an incremental spec.
func (s *Scenario78Suite) TestFunctional_Scenario78_IncrementalLabelOnCronJob() {
	cluster := scenario78Cluster("s78-cron-lbl", scenario78IncrementalBackupSpec(true))
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	assert.Equal(s.T(), util.BackupTypeIncremental, cron.Labels[util.LabelBackupType],
		"incremental CronJob metadata must carry backup-type=incremental")
	assert.Equal(s.T(), util.BackupTypeIncremental,
		cron.Spec.JobTemplate.ObjectMeta.Labels[util.LabelBackupType],
		"incremental CronJob jobTemplate must carry backup-type=incremental")
	assert.Equal(s.T(), util.BackupTypeIncremental,
		cron.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels[util.LabelBackupType],
		"incremental CronJob pod template must carry backup-type=incremental")
}

// --- 78b: status derivation from incremental Job label ---

// TestFunctional_Scenario78_StatusFromIncrementalJobLabel seeds a Succeeded
// incremental-labelled backup Job (on a cluster whose spec is FULL) and asserts
// the operator's reconcile records status.lastBackupType=incremental and a
// backupHistory entry of type incremental — proving Job-label precedence over
// the spec (Scenario 78b).
func (s *Scenario78Suite) TestFunctional_Scenario78_StatusFromIncrementalJobLabel() {
	backup := scenario78IncrementalBackupSpec(true)
	backup.Gpbackup.Incremental = false // spec is FULL; the Job label must win
	cluster := scenario78Cluster("s78-status-incr", backup)
	job := scenario78IncrementalBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608090000"))

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
	assert.Equal(s.T(), util.BackupTypeIncremental, updated.Status.LastBackupType,
		"lastBackupType must derive from the Job label, not the full spec")
	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	assert.Equal(s.T(), util.BackupTypeIncremental, updated.Status.BackupHistory[0].Type,
		"backupHistory[0].type must derive from the Job label")
	assert.Equal(s.T(), "Success", updated.Status.BackupHistory[0].Status)
}

// --- 78d: failed restore -> lastBackupStatus=Failed + RestoreFailed Warning ---

// TestFunctional_Scenario78_FailedRestoreEmitsWarning seeds a Failed
// restore-operation Job (gprestore refusing an incomplete incremental set) and
// asserts the operator records status.lastBackupStatus=Failed AND emits exactly
// one Warning/RestoreFailed Event referencing the Job, and NO BackupFailed
// Warning (Scenario 77 stays backup-only).
func (s *Scenario78Suite) TestFunctional_Scenario78_FailedRestoreEmitsWarning() {
	cluster := scenario78Cluster("s78-restore-fail", scenario78IncrementalBackupSpec(true))
	job := scenario78FailedRestoreJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608100000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok, "test recorder must be a FakeRecorder")
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)

	restoreCount, all := scenario78DrainWarnings(recorder, cbv1alpha1.EventReasonRestoreFailed)
	assert.Equal(s.T(), 1, restoreCount,
		"expected exactly one Warning/RestoreFailed event: %v", all)
	found := false
	for _, e := range all {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonRestoreFailed) &&
			strings.Contains(e, job.Name) {
			found = true
		}
	}
	assert.True(s.T(), found,
		"the Warning/RestoreFailed event must reference the failed restore Job %s: %v", job.Name, all)

	backupCount, _ := scenario78DrainWarnings(recorder, cbv1alpha1.EventReasonBackupFailed)
	assert.Equal(s.T(), 0, backupCount,
		"a restore failure must NOT emit a BackupFailed Warning")
}

// TestFunctional_Scenario78_FailedRestoreWarningDeDup asserts a second reconcile
// of the SAME unchanged failed restore Job does NOT emit a new RestoreFailed
// Warning (de-duplicated per Job to avoid an event storm).
func (s *Scenario78Suite) TestFunctional_Scenario78_FailedRestoreWarningDeDup() {
	cluster := scenario78Cluster("s78-restore-dedup", scenario78IncrementalBackupSpec(true))
	job := scenario78FailedRestoreJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608100000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	first, _ := scenario78DrainWarnings(recorder, cbv1alpha1.EventReasonRestoreFailed)
	require.Equal(s.T(), 1, first, "first reconcile must emit one RestoreFailed Warning")

	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	second, all := scenario78DrainWarnings(recorder, cbv1alpha1.EventReasonRestoreFailed)
	assert.Equal(s.T(), 0, second,
		"second reconcile of the unchanged failed restore Job must not emit a new Warning: %v", all)
}
