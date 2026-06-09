//go:build integration

package integration

import (
	"context"
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
// Scenario 83: Backup Failure Handling (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// backup-failure path: the builder renders the backup Job with the
// jobTemplate.{backoffLimit,activeDeadlineSeconds} override, the (fake)
// Kubernetes client persists it, and we read the materialized Job back from the
// API server's client to assert the persisted spec carries backoffLimit==2 and
// the low activeDeadlineSeconds. Then the AdminReconciler observes a Failed
// (condition-only, Status.Failed==0 — the activeDeadlineSeconds-kill shape)
// backup Job and we assert it drives status.lastBackupStatus=Failed +
// cloudberry_backup_last_status=1. The builder + controller + k8s client wiring
// is real; only the cluster/k8s backend is a fake client (no live MPP cluster).
// This mirrors the scenario80/82 integration harness.
//
//	83c : a persisted backup Job + CronJob for a cluster with
//	      jobTemplate.{backoffLimit:2, activeDeadlineSeconds:5} carries
//	      backoffLimit==2 and activeDeadlineSeconds==5 read back from the API.
//	83d : reconciling a Failed (JobFailed/DeadlineExceeded condition,
//	      Status.Failed==0) backup Job records lastBackupStatus=Failed and a
//	      SetBackupLastStatus(1) on the metrics recorder.
//
// The live force-failure (bad S3 -> BackoffLimitExceeded) and the deadline kill
// (DeadlineExceeded) are exercised by the e2e live script, since the fake client
// cannot reproduce the kubelet's pod-failure semantics.
// ============================================================================

const (
	scenario83IntNamespace = "cloudberry-test"
	scenario83IntCluster   = "scenario83-s3"
	scenario83IntTS        = "20260608030000"

	scenario83IntBackoffLimit int32 = 2
	scenario83IntDeadline     int64 = 5
)

// scenario83IntCountingMetrics records every SetBackupLastStatus call so the
// suite can assert the failed value (1) is emitted via the metric path.
type scenario83IntCountingMetrics struct {
	metrics.NoopRecorder
	lastStatus []float64
}

func (m *scenario83IntCountingMetrics) SetBackupLastStatus(_, _ string, status float64) {
	m.lastStatus = append(m.lastStatus, status)
}

// Scenario83IntegrationSuite drives the builder + fake k8s backend for the
// backup-failure jobTemplate override + Failed-classification path.
type Scenario83IntegrationSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestIntegration_Scenario83(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario83IntegrationSuite))
}

func (s *Scenario83IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

// cluster builds an S3-destination backup cluster mirroring the scenario83-s3
// sample CR (HA + segment mirroring, S3 destination) with a
// jobTemplate.{backoffLimit:2, activeDeadlineSeconds:5} override.
func (s *Scenario83IntegrationSuite) cluster() *cbv1alpha1.CloudberryCluster {
	backoff := scenario83IntBackoffLimit
	deadline := scenario83IntDeadline
	cluster := testutil.NewClusterBuilder(scenario83IntCluster, scenario83IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
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
		JobTemplate: &cbv1alpha1.BackupJobTemplate{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: &deadline,
		},
	}
	return cluster
}

func (s *Scenario83IntegrationSuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// --- 83c: persisted jobTemplate override (backoffLimit + activeDeadlineSeconds) ---

// TestIntegration_Scenario83_BackupJobPersistsJobTemplate builds the backup Job
// (and the scheduled CronJob), persists them through the fake client, reads them
// back and asserts the persisted spec carries backoffLimit==2 and
// activeDeadlineSeconds==5 (the override reaches the materialized jobspec).
func (s *Scenario83IntegrationSuite) TestIntegration_Scenario83_BackupJobPersistsJobTemplate() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	job := s.env.Builder.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario83IntTS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NoError(s.T(), s.env.Client.Create(s.ctx, job),
		"the backup Job must persist in the fake API server")

	got := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupJobName(scenario83IntCluster, scenario83IntTS),
		Namespace: scenario83IntNamespace,
	}, got), "the persisted backup Job must be readable from k8s")

	require.NotNil(s.T(), got.Spec.BackoffLimit)
	assert.Equal(s.T(), scenario83IntBackoffLimit, *got.Spec.BackoffLimit,
		"persisted backup Job must carry the jobTemplate backoffLimit==2")
	require.NotNil(s.T(), got.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), scenario83IntDeadline, *got.Spec.ActiveDeadlineSeconds,
		"persisted backup Job must carry the low activeDeadlineSeconds==5")

	// The CronJob JobTemplate must carry the same override end-to-end.
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	cron := s.env.Builder.BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	require.NoError(s.T(), s.env.Client.Create(s.ctx, cron))

	gotCron := &batchv1.CronJob{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      cron.Name,
		Namespace: scenario83IntNamespace,
	}, gotCron))
	require.NotNil(s.T(), gotCron.Spec.JobTemplate.Spec.BackoffLimit)
	assert.Equal(s.T(), scenario83IntBackoffLimit, *gotCron.Spec.JobTemplate.Spec.BackoffLimit,
		"persisted CronJob JobTemplate must carry backoffLimit==2")
	require.NotNil(s.T(), gotCron.Spec.JobTemplate.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), scenario83IntDeadline, *gotCron.Spec.JobTemplate.Spec.ActiveDeadlineSeconds,
		"persisted CronJob JobTemplate must carry activeDeadlineSeconds==5")
}

// --- 83d: reconcile a Failed (condition-only) backup Job -> lastBackupStatus ---

// TestIntegration_Scenario83_FailedConditionDrivesStatus seeds a Failed backup
// Job whose ONLY failure signal is a JobFailed/DeadlineExceeded condition
// (Status.Failed==0) and reconciles it. The persisted cluster status must read
// lastBackupStatus=Failed and the metrics recorder must have observed a
// SetBackupLastStatus(1) (cloudberry_backup_last_status=1).
func (s *Scenario83IntegrationSuite) TestIntegration_Scenario83_FailedConditionDrivesStatus() {
	cluster := s.cluster()
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupJobName(cluster.Name, scenario83IntTS),
			Namespace: scenario83IntNamespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelComponent:       util.ComponentBackup,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Failed:         0,
			StartTime:      &start,
			CompletionTime: &completion,
			Conditions: []batchv1.JobCondition{
				{
					Type:               batchv1.JobFailed,
					Status:             corev1.ConditionTrue,
					Reason:             "DeadlineExceeded",
					LastTransitionTime: completion,
				},
			},
		},
	}

	s.env = testutil.NewTestK8sEnv(cluster, job)
	m := &scenario83IntCountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, m, s.env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should classify the failed backup Job")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus,
		"a deadline-killed backup (condition-only) must drive lastBackupStatus=Failed")
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)

	require.NotEmpty(s.T(), m.lastStatus, "SetBackupLastStatus must be recorded")
	assert.Equal(s.T(), float64(1), m.lastStatus[len(m.lastStatus)-1],
		"a Failed backup must set cloudberry_backup_last_status=1")
}
