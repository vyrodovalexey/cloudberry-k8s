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
// Scenario 84: Prometheus Metrics / gpbackup_exporter (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// metric-recording path: operator-shaped backup / restore / cleanup Jobs are
// PERSISTED through the (fake) Kubernetes client, then the AdminReconciler lists
// them back from the API server's client and records the spec-11 backup metrics.
// A COUNTING metrics recorder captures each of the 9 recorder invocations so the
// suite can assert the operator records the corresponding metric for each
// observed Job. The builder + controller + k8s client wiring is real; only the
// cluster/k8s backend is a fake client (no live MPP cluster). Mirrors the
// scenario83 integration harness.
//
//	84a : a persisted Succeeded full backup Job (avsoft.io/backup-type=full +
//	      size annotation + start/completion) drives backup_total{full,success},
//	      backup_duration{full}, backup_size_bytes, last_success_timestamp,
//	      last_status=0 and backup_job_status{backup}=2.
//	84b : a persisted Succeeded incremental backup Job drives
//	      backup_total{incremental,success} + backup_duration{incremental}.
//	84c : a persisted Succeeded restore Job (start/completion) drives
//	      restore_total{success} + restore_duration + backup_job_status{restore}=2.
//	84d : a persisted Succeeded cleanup Job (retention-deleted annotation) drives
//	      backup_retention_deleted_total + backup_job_status{cleanup}=2.
//	84e : a persisted Failed backup Job (latest) drives last_status=1 +
//	      backup_job_status{backup}=3 + backup_total{full,failed}.
// ============================================================================

const (
	scenario84IntNamespace = "cloudberry-test"
	scenario84IntCluster   = "scenario84-s3"
	scenario84IntFullTS    = "20260608040000"
	scenario84IntIncrTS    = "20260608041000"
	scenario84IntSizeBytes = "104857600"
	scenario84IntRetN      = "3"
)

// scenario84IntCountingMetrics records every one of the 9 backup-lifecycle
// recorder invocations so the suite can assert the operator records them.
type scenario84IntCountingMetrics struct {
	metrics.NoopRecorder

	backupTotal      [][2]string // {type, result}
	backupDuration   []string    // type with d>0
	backupSize       []string    // timestamp
	lastSuccessTS    []float64
	lastStatus       []float64
	restoreTotal     []string
	restoreDuration  []float64
	retentionDeleted []int
	jobStatus        [][2]interface{} // {operation, code}
}

func (m *scenario84IntCountingMetrics) RecordBackup(_, _, backupType, result string) {
	m.backupTotal = append(m.backupTotal, [2]string{backupType, result})
}

func (m *scenario84IntCountingMetrics) ObserveBackupDuration(_, _, backupType string, d time.Duration) {
	if d > 0 {
		m.backupDuration = append(m.backupDuration, backupType)
	}
}

func (m *scenario84IntCountingMetrics) SetBackupSizeBytes(_, _, timestamp string, _ float64) {
	m.backupSize = append(m.backupSize, timestamp)
}

func (m *scenario84IntCountingMetrics) SetBackupLastSuccessTimestamp(_, _ string, ts float64) {
	m.lastSuccessTS = append(m.lastSuccessTS, ts)
}

func (m *scenario84IntCountingMetrics) SetBackupLastStatus(_, _ string, status float64) {
	m.lastStatus = append(m.lastStatus, status)
}

func (m *scenario84IntCountingMetrics) RecordRestore(_, _, result string) {
	m.restoreTotal = append(m.restoreTotal, result)
}

func (m *scenario84IntCountingMetrics) ObserveRestoreDuration(_, _ string, d time.Duration) {
	m.restoreDuration = append(m.restoreDuration, d.Seconds())
}

func (m *scenario84IntCountingMetrics) RecordBackupRetentionDeleted(_, _ string, n int) {
	m.retentionDeleted = append(m.retentionDeleted, n)
}

func (m *scenario84IntCountingMetrics) SetBackupJobStatus(_, _, _, operation string, status float64) {
	m.jobStatus = append(m.jobStatus, [2]interface{}{operation, status})
}

func (m *scenario84IntCountingMetrics) hasBackupTotal(backupType, result string) bool {
	for _, b := range m.backupTotal {
		if b[0] == backupType && b[1] == result {
			return true
		}
	}
	return false
}

func (m *scenario84IntCountingMetrics) hasBackupDuration(backupType string) bool {
	for _, t := range m.backupDuration {
		if t == backupType {
			return true
		}
	}
	return false
}

func (m *scenario84IntCountingMetrics) hasJobStatus(operation string, code float64) bool {
	for _, j := range m.jobStatus {
		if j[0] == operation && j[1] == code {
			return true
		}
	}
	return false
}

func (m *scenario84IntCountingMetrics) lastBackupStatus() (float64, bool) {
	if len(m.lastStatus) == 0 {
		return 0, false
	}
	return m.lastStatus[len(m.lastStatus)-1], true
}

// Scenario84IntegrationSuite drives the builder + fake k8s backend for the
// metric-recording path.
type Scenario84IntegrationSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestIntegration_Scenario84(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario84IntegrationSuite))
}

func (s *Scenario84IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

// cluster builds the scenario84-s3 cluster (S3 destination, incremental +
// retention enabled, HA + segment mirroring).
func (s *Scenario84IntegrationSuite) cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario84IntCluster, scenario84IntNamespace).
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
			Incremental:      true,
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
				Folder:         "scenario84",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

func (s *Scenario84IntegrationSuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// s84IntJob builds an operator-shaped Job fixture.
type s84IntJob struct {
	name       string
	operation  string
	backupType string
	sizeBytes  string
	retDeleted string
	succeeded  bool
	failed     bool
	withTimes  bool
	createdAt  time.Time
}

func scenario84IntBuildJob(cluster string, o s84IntJob) *batchv1.Job {
	labels := map[string]string{
		util.LabelCluster:         cluster,
		util.LabelComponent:       util.ComponentBackup,
		util.LabelBackupOperation: o.operation,
	}
	if o.backupType != "" {
		labels[util.LabelBackupType] = o.backupType
	}
	annotations := map[string]string{}
	if o.sizeBytes != "" {
		annotations[util.AnnotationBackupSizeBytes] = o.sizeBytes
	}
	if o.retDeleted != "" {
		annotations[util.AnnotationBackupRetentionDeleted] = o.retDeleted
	}
	created := o.createdAt
	if created.IsZero() {
		created = time.Now()
	}
	status := batchv1.JobStatus{}
	if o.withTimes {
		start := metav1.NewTime(created.Add(-90 * time.Second))
		completion := metav1.NewTime(created)
		status.StartTime = &start
		status.CompletionTime = &completion
	}
	switch {
	case o.succeeded:
		status.Succeeded = 1
	case o.failed:
		status.Failed = 1
		status.Conditions = []batchv1.JobCondition{{
			Type:   batchv1.JobFailed,
			Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded",
		}}
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              o.name,
			Namespace:         scenario84IntNamespace,
			Labels:            labels,
			Annotations:       annotations,
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: status,
	}
}

// reconcileWith persists the cluster + Jobs, reconciles once and returns the
// counting metrics recorder and the persisted cluster.
func (s *Scenario84IntegrationSuite) reconcileWith(
	cluster *cbv1alpha1.CloudberryCluster,
	jobs ...*batchv1.Job,
) (*scenario84IntCountingMetrics, *cbv1alpha1.CloudberryCluster) {
	s.env = testutil.NewTestK8sEnv(cluster)
	for _, j := range jobs {
		require.NoError(s.T(), s.env.Client.Create(s.ctx, j),
			"the operator-shaped Job must persist in the fake API server")
	}

	m := &scenario84IntCountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, m, s.env.Logger,
	)
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should record backup metrics")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	return m, updated
}

// --- 84a: persisted Succeeded full backup -> M1/M2/M3/M4/M5/M9 ---

// TestIntegration_Scenario84_FullBackupRecordsMetrics persists a Succeeded full
// backup Job (size annotation + start/completion), reconciles and asserts the
// full-backup metric family is recorded.
func (s *Scenario84IntegrationSuite) TestIntegration_Scenario84_FullBackupRecordsMetrics() {
	cluster := s.cluster()
	job := scenario84IntBuildJob(cluster.Name, s84IntJob{
		name:       util.BackupJobName(cluster.Name, scenario84IntFullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		sizeBytes:  scenario84IntSizeBytes,
		succeeded:  true,
		withTimes:  true,
	})

	m, updated := s.reconcileWith(cluster, job)

	assert.True(s.T(), m.hasBackupTotal("full", "success"),
		"M1: backup_total{type=full,result=success} must be recorded")
	assert.True(s.T(), m.hasBackupDuration("full"),
		"M2: backup_duration_seconds{type=full} must be observed")
	assert.Contains(s.T(), m.backupSize, scenario84IntFullTS,
		"M3: backup_size_bytes{timestamp} must be set for the full backup")
	require.NotEmpty(s.T(), m.lastSuccessTS, "M4: SetBackupLastSuccessTimestamp must be called")
	assert.Greater(s.T(), m.lastSuccessTS[0], float64(0))
	last, ok := m.lastBackupStatus()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(0), last, "M5: a Succeeded backup must set last_status=0")
	assert.True(s.T(), m.hasJobStatus(util.BackupOperationBackup, 2),
		"M9: backup_job_status{operation=backup}=2 (succeeded)")
	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)
}

// --- 84b: persisted Succeeded incremental backup -> M1/M2 (incremental) ---

// TestIntegration_Scenario84_IncrementalBackupRecordsMetrics persists a
// Succeeded incremental backup Job and asserts the incremental backup_total +
// duration family is recorded.
func (s *Scenario84IntegrationSuite) TestIntegration_Scenario84_IncrementalBackupRecordsMetrics() {
	cluster := s.cluster()
	job := scenario84IntBuildJob(cluster.Name, s84IntJob{
		name:       util.BackupJobName(cluster.Name, scenario84IntIncrTS),
		operation:  util.BackupOperationBackup,
		backupType: "incremental",
		succeeded:  true,
		withTimes:  true,
	})

	m, _ := s.reconcileWith(cluster, job)

	assert.True(s.T(), m.hasBackupTotal("incremental", "success"),
		"M1: backup_total{type=incremental,result=success} must be recorded")
	assert.True(s.T(), m.hasBackupDuration("incremental"),
		"M2: backup_duration_seconds{type=incremental} must be observed")
}

// --- 84c: persisted Succeeded restore -> M6/M7/M9(restore=2) ---

// TestIntegration_Scenario84_RestoreRecordsMetrics persists a Succeeded restore
// Job (start/completion) and asserts restore_total + restore_duration +
// backup_job_status{restore}=2 are recorded.
func (s *Scenario84IntegrationSuite) TestIntegration_Scenario84_RestoreRecordsMetrics() {
	cluster := s.cluster()
	job := scenario84IntBuildJob(cluster.Name, s84IntJob{
		name:      util.RestoreJobName(cluster.Name, scenario84IntFullTS),
		operation: util.BackupOperationRestore,
		succeeded: true,
		withTimes: true,
	})

	m, _ := s.reconcileWith(cluster, job)

	assert.Contains(s.T(), m.restoreTotal, "success",
		"M6: restore_total{result=success} must be recorded")
	require.NotEmpty(s.T(), m.restoreDuration, "M7: ObserveRestoreDuration must be called")
	assert.Greater(s.T(), m.restoreDuration[0], float64(0))
	assert.True(s.T(), m.hasJobStatus(util.BackupOperationRestore, 2),
		"M9: backup_job_status{operation=restore}=2 (succeeded)")
}

// --- 84d: persisted Succeeded cleanup -> M8/M9(cleanup=2) ---

// TestIntegration_Scenario84_CleanupRecordsMetrics persists a Succeeded cleanup
// Job (retention-deleted annotation) and asserts backup_retention_deleted_total
// + backup_job_status{cleanup}=2 are recorded.
func (s *Scenario84IntegrationSuite) TestIntegration_Scenario84_CleanupRecordsMetrics() {
	cluster := s.cluster()
	job := scenario84IntBuildJob(cluster.Name, s84IntJob{
		name:       util.RetentionCleanupJobName(cluster.Name, scenario84IntIncrTS),
		operation:  util.BackupOperationCleanup,
		retDeleted: scenario84IntRetN,
		succeeded:  true,
		withTimes:  true,
	})

	m, _ := s.reconcileWith(cluster, job)

	require.NotEmpty(s.T(), m.retentionDeleted, "M8: RecordBackupRetentionDeleted must be called")
	assert.Equal(s.T(), 3, m.retentionDeleted[0],
		"M8: retention_deleted must equal the annotated count")
	assert.True(s.T(), m.hasJobStatus(util.BackupOperationCleanup, 2),
		"M9: backup_job_status{operation=cleanup}=2 (succeeded)")
}

// --- 84e: persisted Failed backup (latest) -> M5=1/M9(backup=3) ---

// TestIntegration_Scenario84_FailedBackupRecordsMetrics persists a Failed backup
// Job as the latest backup and asserts last_status=1 + backup_job_status=3 +
// backup_total{full,failed}.
func (s *Scenario84IntegrationSuite) TestIntegration_Scenario84_FailedBackupRecordsMetrics() {
	cluster := s.cluster()
	job := scenario84IntBuildJob(cluster.Name, s84IntJob{
		name:       util.BackupJobName(cluster.Name, scenario84IntFullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		failed:     true,
	})

	m, updated := s.reconcileWith(cluster, job)

	last, ok := m.lastBackupStatus()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), last, "M5: a Failed latest backup must set last_status=1")
	assert.True(s.T(), m.hasJobStatus(util.BackupOperationBackup, 3),
		"M9: backup_job_status{operation=backup}=3 (failed)")
	assert.True(s.T(), m.hasBackupTotal("full", "failed"),
		"M1: backup_total{type=full,result=failed} must be recorded")
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
}
