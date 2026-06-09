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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 83: Backup Failure Handling (functional)
// ============================================================================
//
// Scenario 83 hardens the operator's backup FAILURE handling at the
// builder/controller layer (deterministic, no live cluster). Two failure shapes
// must classify Failed and drive status.lastBackupStatus=Failed +
// cloudberry_backup_last_status=1:
//
//	83a force-failure : a backup whose S3 destination is unreachable / bad creds
//	    retries up to backoffLimit (2) and ends terminal Failed
//	    (JobFailed/BackoffLimitExceeded condition). The Job spec carries
//	    backoffLimit==2 (default or explicit jobTemplate).
//	83b deadline      : a backup Job with a LOW activeDeadlineSeconds running a
//	    long command is killed by Kubernetes at the deadline and gets a
//	    JobFailed/DeadlineExceeded condition — even when Status.Failed==0.
//	83c builder       : BuildBackupJob/BuildBackupCronJob (and the other Job
//	    builders) seed backoffLimit==2 by default and route a
//	    jobTemplate.{backoffLimit,activeDeadlineSeconds} override into every
//	    Job/CronJob jobspec.
//	83d status detect : a Failed backup Job (Status.Failed>0 OR a JobFailed
//	    condition with Status.Failed==0) classifies Failed and records
//	    lastBackupStatus=Failed + SetBackupLastStatus(1) + a single BackupFailed
//	    Warning; a Succeeded Job stays Success + last_status=0 (regression).
//
// The unexported backupJobStatus/backupJobStatusCode mappings (and the
// jobHasFailedCondition helper) are covered directly by the controller unit
// tests (internal/controller/backup_failure_scenario83_test.go). These
// functional tests black-box the operator through the public builder
// (BuildBackupJob/CronJob jobspec) and the AdminReconciler with a fake client,
// asserting the Failed-condition classification end-to-end through the status +
// metric + event wiring (which is the observable behaviour). The live
// force-failure (bad S3 -> BackoffLimitExceeded) and deadline kill
// (DeadlineExceeded) are exercised by the e2e live script, since the builder
// cannot prove the kubelet's pod-failure semantics.
// ============================================================================

const (
	scenario83BackupImage = "cloudberry-backup:2.1.0"
	// scenario83TS is a pinned 14-digit gpbackup-style timestamp.
	scenario83TS = "20260608030000"
	// scenario83DefaultBackoffLimit is internal/builder's defaultBackoffLimit.
	scenario83DefaultBackoffLimit int32 = 2
	// scenario83DefaultDeadline is internal/builder's defaultActiveDeadlineSeconds.
	scenario83DefaultDeadline int64 = 7200
	// scenario83LowDeadline is the LOW activeDeadlineSeconds the deadline path
	// drives through the jobTemplate override.
	scenario83LowDeadline int64 = 5
)

// scenario83CountingMetrics embeds NoopRecorder and records every
// SetBackupLastStatus call so the functional suite can assert the failed value
// (1) / success value (0) was emitted, without a live Prometheus registry.
type scenario83CountingMetrics struct {
	metrics.NoopRecorder
	lastStatus []float64
}

func (m *scenario83CountingMetrics) SetBackupLastStatus(_, _ string, status float64) {
	m.lastStatus = append(m.lastStatus, status)
}

func (m *scenario83CountingMetrics) last() (float64, bool) {
	if len(m.lastStatus) == 0 {
		return 0, false
	}
	return m.lastStatus[len(m.lastStatus)-1], true
}

// Scenario83Suite exercises the backup-failure builder defaults/override and the
// Failed-classification status/metric/event flow.
type Scenario83Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario83(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario83Suite))
}

func (s *Scenario83Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario83Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario83S3BackupSpec returns an S3 (MinIO) destination BackupSpec mirroring
// the scenario83-s3 sample CR. jobTemplate is set only when a backoffLimit or
// activeDeadlineSeconds override is supplied (nil pointers keep the defaults).
func scenario83S3BackupSpec(backoff *int32, deadline *int64) *cbv1alpha1.BackupSpec {
	spec := &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario83BackupImage,
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
				Folder:         "scenario83",
				Encryption:     "on",
				ForcePathStyle: true,
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests:  4,
					BackupMultipartChunksize:     "10MB",
					RestoreMaxConcurrentRequests: 4,
					RestoreMultipartChunksize:    "10MB",
				},
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	if backoff != nil || deadline != nil {
		spec.JobTemplate = &cbv1alpha1.BackupJobTemplate{
			BackoffLimit:          backoff,
			ActiveDeadlineSeconds: deadline,
		}
	}
	return spec
}

// scenario83Cluster builds a Running cluster (pending generation) with the given
// backup spec, mirroring the functional harness used by scenario77/82.
func scenario83Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario83BackupJob builds a backup-operation Job fixture for the given cluster
// with the supplied JobStatus (used to drive the Failed-condition classification
// through reconcile).
func scenario83BackupJob(cluster, name string, status batchv1.JobStatus) *batchv1.Job {
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelComponent:       util.ComponentBackup,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			CreationTimestamp: completion,
		},
		Status: status,
	}
}

// scenario83FailedCondition returns a JobFailed condition (status True) with the
// given reason (e.g. DeadlineExceeded, BackoffLimitExceeded).
func scenario83FailedCondition(reason string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:               batchv1.JobFailed,
		Status:             corev1.ConditionTrue,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
}

// drainWarningBackupFailed83 drains the FakeRecorder channel and counts the
// Warning events carrying the BackupFailed reason.
func drainWarningBackupFailed83(recorder *record.FakeRecorder) (count int, all []string) {
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

// reconcileBackupJob seeds the cluster + Job into a fake client (with the status
// subresource) and reconciles once, returning the counting metrics + recorder so
// the test can assert lastBackupStatus / SetBackupLastStatus / events.
func (s *Scenario83Suite) reconcileBackupJob(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (*cbv1alpha1.CloudberryCluster, *scenario83CountingMetrics, *record.FakeRecorder) {
	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, job).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario83CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, updated))
	return updated, m, recorder
}

// --- 83c: builder defaults + jobTemplate override reach the jobspec ---

// TestFunctional_Scenario83_BuilderBackoffLimitDefault asserts BuildBackupJob and
// BuildBackupCronJob seed the default backoffLimit==2 (matching Scenario 83
// "retries up to backoffLimit (2)") and the default activeDeadlineSeconds==7200
// when no jobTemplate is configured.
func (s *Scenario83Suite) TestFunctional_Scenario83_BuilderBackoffLimitDefault() {
	cluster := scenario83Cluster("s83-default", scenario83S3BackupSpec(nil, nil))

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario83TS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotNil(s.T(), job.Spec.BackoffLimit, "backup Job must set a backoffLimit")
	assert.Equal(s.T(), scenario83DefaultBackoffLimit, *job.Spec.BackoffLimit,
		"default backoffLimit must be 2 (retries up to backoffLimit)")
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), scenario83DefaultDeadline, *job.Spec.ActiveDeadlineSeconds,
		"default activeDeadlineSeconds must be 7200")

	cluster.Spec.Backup.Schedule = "0 2 * * *"
	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	require.NotNil(s.T(), cron.Spec.JobTemplate.Spec.BackoffLimit)
	assert.Equal(s.T(), scenario83DefaultBackoffLimit, *cron.Spec.JobTemplate.Spec.BackoffLimit,
		"CronJob JobTemplate must inherit the default backoffLimit==2")
}

// TestFunctional_Scenario83_BuilderJobTemplateOverride asserts a
// jobTemplate.{backoffLimit,activeDeadlineSeconds} override reaches EVERY Job the
// builder produces (backup / cronjob / restore / validate / cleanup), covering
// the low-activeDeadlineSeconds deadline path (TC-83b) and an explicit
// backoffLimit==2 (TC-83a).
func (s *Scenario83Suite) TestFunctional_Scenario83_BuilderJobTemplateOverride() {
	backoff := scenario83DefaultBackoffLimit
	deadline := scenario83LowDeadline
	b := builder.NewBuilder()

	builders := []struct {
		name string
		spec func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec
	}{
		{
			name: "BuildBackupJob",
			spec: func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec {
				job := b.BuildBackupJob(c, &builder.BackupJobOptions{
					Timestamp: scenario83TS, Type: "full", Databases: []string{"mydb"},
				})
				require.NotNil(s.T(), job)
				return job.Spec
			},
		},
		{
			name: "BuildBackupCronJob",
			spec: func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec {
				c.Spec.Backup.Schedule = "0 2 * * *"
				cron := b.BuildBackupCronJob(c)
				require.NotNil(s.T(), cron)
				return cron.Spec.JobTemplate.Spec
			},
		},
		{
			name: "BuildRestoreJob",
			spec: func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec {
				job := b.BuildRestoreJob(c, &builder.RestoreJobOptions{
					Timestamp: scenario83TS, Databases: []string{"mydb"},
				})
				require.NotNil(s.T(), job)
				return job.Spec
			},
		},
		{
			name: "BuildPostRestoreValidationJob",
			spec: func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec {
				job := b.BuildPostRestoreValidationJob(c, &builder.ValidationJobOptions{
					Timestamp: scenario83TS, Database: "mydb",
				})
				require.NotNil(s.T(), job)
				return job.Spec
			},
		},
		{
			name: "BuildRetentionCleanupJob",
			spec: func(c *cbv1alpha1.CloudberryCluster) batchv1.JobSpec {
				job := b.BuildRetentionCleanupJob(c, scenario83TS)
				require.NotNil(s.T(), job)
				return job.Spec
			},
		},
	}

	for _, bld := range builders {
		s.Run(bld.name, func() {
			cluster := scenario83Cluster("s83-override", scenario83S3BackupSpec(&backoff, &deadline))
			spec := bld.spec(cluster)
			require.NotNil(s.T(), spec.BackoffLimit)
			assert.Equal(s.T(), backoff, *spec.BackoffLimit,
				"%s: jobTemplate backoffLimit==2 must reach the jobspec", bld.name)
			require.NotNil(s.T(), spec.ActiveDeadlineSeconds)
			assert.Equal(s.T(), deadline, *spec.ActiveDeadlineSeconds,
				"%s: jobTemplate activeDeadlineSeconds==5 must reach the jobspec", bld.name)
		})
	}
}

// TestFunctional_Scenario83_BuilderPartialOverrideKeepsDefaults asserts a
// jobTemplate that sets only one knob keeps the other at its default (backoffLimit
// nil -> 2 retained; activeDeadlineSeconds nil -> 7200 retained).
func (s *Scenario83Suite) TestFunctional_Scenario83_BuilderPartialOverrideKeepsDefaults() {
	deadline := scenario83LowDeadline
	// backoffLimit nil -> default 2; activeDeadlineSeconds overridden -> 5.
	cluster := scenario83Cluster("s83-partial-a", scenario83S3BackupSpec(nil, &deadline))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario83TS, Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), scenario83DefaultBackoffLimit, *job.Spec.BackoffLimit,
		"nil backoffLimit override must retain the default 2")
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), deadline, *job.Spec.ActiveDeadlineSeconds)

	// activeDeadlineSeconds nil -> default 7200; backoffLimit overridden -> 2.
	backoff := scenario83DefaultBackoffLimit
	cluster2 := scenario83Cluster("s83-partial-b", scenario83S3BackupSpec(&backoff, nil))
	job2 := builder.NewBuilder().BuildBackupJob(cluster2, &builder.BackupJobOptions{
		Timestamp: scenario83TS, Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job2)
	require.NotNil(s.T(), job2.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), scenario83DefaultDeadline, *job2.Spec.ActiveDeadlineSeconds,
		"nil activeDeadlineSeconds override must retain the default 7200")
	require.NotNil(s.T(), job2.Spec.BackoffLimit)
	assert.Equal(s.T(), backoff, *job2.Spec.BackoffLimit)
}

// --- 83d / failure status: Failed classification -> status + metric + event ---

// TestFunctional_Scenario83_FailedPodCountSetsFailed seeds a backup Job whose
// failure signal is Status.Failed>0 (the classic failed-pod-count shape, e.g. the
// force-failure pod count) and asserts the operator records
// lastBackupStatus=Failed + SetBackupLastStatus(1) + a single BackupFailed
// Warning.
func (s *Scenario83Suite) TestFunctional_Scenario83_FailedPodCountSetsFailed() {
	cluster := scenario83Cluster("s83-failedpods", scenario83S3BackupSpec(nil, nil))
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario83BackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, scenario83TS),
		batchv1.JobStatus{Failed: 1, StartTime: &start, CompletionTime: &completion},
	)

	updated, m, recorder := s.reconcileBackupJob(cluster, job)

	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	assert.Equal(s.T(), "Failed", updated.Status.BackupHistory[0].Status)

	last, ok := m.last()
	require.True(s.T(), ok, "SetBackupLastStatus must be called")
	assert.Equal(s.T(), float64(1), last,
		"a Failed backup must set cloudberry_backup_last_status=1")

	count, all := drainWarningBackupFailed83(recorder)
	assert.Equal(s.T(), 1, count,
		"a Failed backup must emit exactly one Warning/BackupFailed event: %v", all)
}

// TestFunctional_Scenario83_DeadlineConditionSetsFailed seeds a backup Job whose
// ONLY failure signal is a JobFailed/DeadlineExceeded condition with
// Status.Failed==0 (the activeDeadlineSeconds-kill shape, TC-83b GAP-1). It must
// still classify Failed -> lastBackupStatus=Failed + SetBackupLastStatus(1) + a
// single BackupFailed Warning.
func (s *Scenario83Suite) TestFunctional_Scenario83_DeadlineConditionSetsFailed() {
	cluster := scenario83Cluster("s83-deadline", scenario83S3BackupSpec(nil, nil))
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario83BackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, scenario83TS),
		batchv1.JobStatus{
			Failed:         0,
			StartTime:      &start,
			CompletionTime: &completion,
			Conditions:     []batchv1.JobCondition{scenario83FailedCondition("DeadlineExceeded")},
		},
	)

	updated, m, recorder := s.reconcileBackupJob(cluster, job)

	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus,
		"a deadline-killed backup (condition-only, Status.Failed==0) must classify Failed")
	last, ok := m.last()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), last,
		"a deadline-killed backup must set cloudberry_backup_last_status=1")

	count, all := drainWarningBackupFailed83(recorder)
	assert.Equal(s.T(), 1, count,
		"a deadline-killed backup must emit exactly one Warning/BackupFailed event: %v", all)
}

// TestFunctional_Scenario83_BackoffExhaustedConditionSetsFailed seeds a backup Job
// carrying a JobFailed/BackoffLimitExceeded condition (the backoffLimit-exhausted
// force-failure shape, TC-83a) and asserts Failed + last_status=1.
func (s *Scenario83Suite) TestFunctional_Scenario83_BackoffExhaustedConditionSetsFailed() {
	cluster := scenario83Cluster("s83-backoff", scenario83S3BackupSpec(nil, nil))
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario83BackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, scenario83TS),
		batchv1.JobStatus{
			Failed:         scenario83DefaultBackoffLimit,
			StartTime:      &start,
			CompletionTime: &completion,
			Conditions:     []batchv1.JobCondition{scenario83FailedCondition("BackoffLimitExceeded")},
		},
	)

	updated, m, _ := s.reconcileBackupJob(cluster, job)

	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	last, ok := m.last()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), last,
		"a backoff-exhausted backup must set cloudberry_backup_last_status=1")
}

// TestFunctional_Scenario83_SucceededStaysSuccess is the success regression: a
// Succeeded backup Job sets lastBackupStatus=Success + SetBackupLastStatus(0) and
// emits NO BackupFailed Warning — even if a stale Failed condition is present
// (Succeeded precedence must win).
func (s *Scenario83Suite) TestFunctional_Scenario83_SucceededStaysSuccess() {
	cluster := scenario83Cluster("s83-success", scenario83S3BackupSpec(nil, nil))
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := scenario83BackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, scenario83TS),
		batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &completion,
			// A stale Failed condition must NOT override the success count.
			Conditions: []batchv1.JobCondition{scenario83FailedCondition("BackoffLimitExceeded")},
		},
	)

	updated, m, recorder := s.reconcileBackupJob(cluster, job)

	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus,
		"Succeeded precedence: a success count must win over a stale Failed condition")
	last, ok := m.last()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(0), last,
		"a Succeeded backup must set cloudberry_backup_last_status=0")

	count, all := drainWarningBackupFailed83(recorder)
	assert.Equal(s.T(), 0, count,
		"a Succeeded backup must not emit a BackupFailed Warning: %v", all)
}
