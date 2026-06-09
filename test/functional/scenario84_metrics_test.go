//go:build functional

package functional

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
// Scenario 84: Prometheus Metrics / gpbackup_exporter (functional)
// ============================================================================
//
// Scenario 84 is a VERIFICATION scenario: all 9 backup-lifecycle metrics are
// already DEFINED + WIRED in the operator (internal/metrics + the
// AdminReconciler recorder call-sites). These functional tests black-box the
// operator through the public AdminReconciler with a fake client and a COUNTING
// metrics recorder, proving that reconciling operator-shaped backup / restore /
// cleanup Jobs (with the correct avsoft.io labels + annotations + Job status)
// drives EVERY one of the 9 recorders with the right labels/values across a full
// lifecycle.
//
// The exporter is implemented as the OPERATOR /metrics endpoint: the operator
// derives these metrics from the observed backup/restore/cleanup Jobs + their
// avsoft.io annotations (NOT a separate sidecar binary). refreshBackupStatus ->
// recordBackupJobMetrics (per-Job job_status + terminal restore/cleanup
// metrics) + applyBackupJobToStatus (latest backup/restore) ->
// recordLatestBackupMetrics (the per-type backup family).
//
// The 9 metrics (namespace "cloudberry") + recorder + trigger:
//
//	M1 backup_total{type,result}        RecordBackup            latest backup Job
//	M2 backup_duration_seconds{type}    ObserveBackupDuration   success + start+completion
//	M3 backup_size_bytes{timestamp}     SetBackupSizeBytes      success + size annotation
//	M4 backup_last_success_timestamp    SetBackupLastSuccessTimestamp success+completion
//	M5 backup_last_status               SetBackupLastStatus     latest backup Job (0/1/2)
//	M6 restore_total{result}            RecordRestore           latest restore Job
//	M7 restore_duration_seconds         ObserveRestoreDuration  succeeded restore + times
//	M8 backup_retention_deleted_total   RecordBackupRetentionDeleted succeeded cleanup + ann
//	M9 backup_job_status{job_name,operation} SetBackupJobStatus EVERY backup/restore/cleanup
//
// GAP-A guard: backup_total / restore_total carry the outcome label `result`
// (success|failed), NOT `status`. The counting recorder records the lowercased
// result string exactly as the operator emits it.
// ============================================================================

const (
	scenario84BackupImage = "cloudberry-backup:2.1.0"
	// scenario84FullTS / scenario84IncrTS are pinned 14-digit gpbackup-style
	// timestamps for the full + incremental backups.
	scenario84FullTS = "20260608040000"
	scenario84IncrTS = "20260608041000"
	// scenario84SizeBytes is the annotated backup size (~100MiB) the full backup
	// surfaces via avsoft.io/backup-size-bytes.
	scenario84SizeBytes = "104857600"
	// scenario84RetentionDeleted is the number of backups the cleanup Job removed.
	scenario84RetentionDeleted = "3"
)

// ----------------------------------------------------------------------------
// scenario84CountingMetrics is a counting metrics.Recorder that records every
// one of the 9 backup-lifecycle recorder invocations (with their labels/values)
// so the functional suite can assert each metric fires across the lifecycle
// WITHOUT a live Prometheus registry.
// ----------------------------------------------------------------------------

type s84BackupTotal struct {
	backupType string
	result     string
}

type s84Duration struct {
	backupType string
	seconds    float64
}

type s84Size struct {
	timestamp string
	bytes     float64
}

type s84JobStatus struct {
	job       string
	operation string
	code      float64
}

type scenario84CountingMetrics struct {
	metrics.NoopRecorder

	backupTotal      []s84BackupTotal
	backupDuration   []s84Duration
	backupSize       []s84Size
	lastSuccessTS    []float64
	lastStatus       []float64
	restoreTotal     []string
	restoreDuration  []float64
	retentionDeleted []int
	jobStatus        []s84JobStatus
}

func (m *scenario84CountingMetrics) RecordBackup(_, _, backupType, result string) {
	m.backupTotal = append(m.backupTotal, s84BackupTotal{backupType: backupType, result: result})
}

func (m *scenario84CountingMetrics) ObserveBackupDuration(_, _, backupType string, d time.Duration) {
	m.backupDuration = append(m.backupDuration, s84Duration{backupType: backupType, seconds: d.Seconds()})
}

func (m *scenario84CountingMetrics) SetBackupSizeBytes(_, _, timestamp string, bytes float64) {
	m.backupSize = append(m.backupSize, s84Size{timestamp: timestamp, bytes: bytes})
}

func (m *scenario84CountingMetrics) SetBackupLastSuccessTimestamp(_, _ string, ts float64) {
	m.lastSuccessTS = append(m.lastSuccessTS, ts)
}

func (m *scenario84CountingMetrics) SetBackupLastStatus(_, _ string, status float64) {
	m.lastStatus = append(m.lastStatus, status)
}

func (m *scenario84CountingMetrics) RecordRestore(_, _, result string) {
	m.restoreTotal = append(m.restoreTotal, result)
}

func (m *scenario84CountingMetrics) ObserveRestoreDuration(_, _ string, d time.Duration) {
	m.restoreDuration = append(m.restoreDuration, d.Seconds())
}

func (m *scenario84CountingMetrics) RecordBackupRetentionDeleted(_, _ string, n int) {
	m.retentionDeleted = append(m.retentionDeleted, n)
}

func (m *scenario84CountingMetrics) SetBackupJobStatus(_, _, job, operation string, status float64) {
	m.jobStatus = append(m.jobStatus, s84JobStatus{job: job, operation: operation, code: status})
}

// hasBackupTotal reports whether a backup_total{type,result} pair was recorded.
func (m *scenario84CountingMetrics) hasBackupTotal(backupType, result string) bool {
	for _, b := range m.backupTotal {
		if b.backupType == backupType && b.result == result {
			return true
		}
	}
	return false
}

// hasBackupDuration reports whether a backup duration was observed for a type.
func (m *scenario84CountingMetrics) hasBackupDuration(backupType string) bool {
	for _, d := range m.backupDuration {
		if d.backupType == backupType && d.seconds > 0 {
			return true
		}
	}
	return false
}

// lastBackupStatus returns the final SetBackupLastStatus value (latest wins).
func (m *scenario84CountingMetrics) lastBackupStatus() (float64, bool) {
	if len(m.lastStatus) == 0 {
		return 0, false
	}
	return m.lastStatus[len(m.lastStatus)-1], true
}

// jobStatusFor returns the code recorded for a given operation (last match).
func (m *scenario84CountingMetrics) jobStatusFor(operation string, code float64) bool {
	for _, j := range m.jobStatus {
		if j.operation == operation && j.code == code {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// Suite + fixtures
// ----------------------------------------------------------------------------

// Scenario84MetricsSuite drives operator-shaped Jobs through the AdminReconciler
// and asserts the counting metrics recorder observes each of the 9 metrics.
type Scenario84MetricsSuite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario84(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario84MetricsSuite))
}

func (s *Scenario84MetricsSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario84MetricsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// scenario84Cluster builds a Running S3-destination cluster (incremental +
// retention enabled) mirroring the scenario84-s3 sample CR.
func scenario84Cluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario84BackupImage,
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

// s84JobOpts configures an operator-shaped Job fixture.
type s84JobOpts struct {
	name       string
	operation  string // backup / restore / cleanup
	backupType string // full / incremental (backup ops)
	sizeBytes  string // avsoft.io/backup-size-bytes (backup ops)
	retDeleted string // avsoft.io/backup-retention-deleted (cleanup ops)
	succeeded  bool
	failed     bool
	withTimes  bool      // set startTime + completionTime
	createdAt  time.Time // CreationTimestamp (controls "latest")
}

// scenario84Job builds an operator-shaped Job (correct labels/annotations +
// status) for the given operation/type.
func scenario84Job(cluster string, o s84JobOpts) *batchv1.Job {
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
		status.Conditions = []batchv1.JobCondition{{
			Type:               batchv1.JobComplete,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(created),
		}}
	case o.failed:
		status.Failed = 1
		status.Conditions = []batchv1.JobCondition{{
			Type:               batchv1.JobFailed,
			Status:             corev1.ConditionTrue,
			Reason:             "BackoffLimitExceeded",
			LastTransitionTime: metav1.NewTime(created),
		}}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              o.name,
			Namespace:         "cloudberry-test",
			Labels:            labels,
			Annotations:       annotations,
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: status,
	}
}

// reconcileJobs seeds the cluster + Jobs into a fake client and reconciles once,
// returning the counting metrics recorder and the persisted cluster.
func (s *Scenario84MetricsSuite) reconcileJobs(
	cluster *cbv1alpha1.CloudberryCluster,
	jobs ...*batchv1.Job,
) (*scenario84CountingMetrics, *cbv1alpha1.CloudberryCluster) {
	scheme := testutil.NewTestK8sEnv().Scheme
	clientBuilder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster)
	for _, j := range jobs {
		clientBuilder = clientBuilder.WithObjects(j)
	}
	k8sClient := clientBuilder.Build()

	recorder := record.NewFakeRecorder(50)
	m := &scenario84CountingMetrics{}
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
	return m, updated
}

// newLifecycleHarness builds a fake client seeded with the cluster, a shared
// counting recorder and an AdminReconciler so a sequence of Jobs (each becoming
// the latest backup/restore Job in turn) can be reconciled like the real full
// lifecycle, accumulating the metric recorder calls across reconciles. This is
// the end-to-end reconcile-through-AdminReconciler harness the live script
// mirrors: the operator only records the per-type backup family (M1/M5) and the
// restore family (M6) for the LATEST backup/restore Job, so the lifecycle must
// reconcile between steps. It returns the shared recorder, a step(jobs...) closure
// that seeds + reconciles, and a getCluster() closure for the persisted CR.
func (s *Scenario84MetricsSuite) newLifecycleHarness(
	cluster *cbv1alpha1.CloudberryCluster,
) (*scenario84CountingMetrics, func(jobs ...*batchv1.Job), func() *cbv1alpha1.CloudberryCluster) {
	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster).
		Build()
	recorder := record.NewFakeRecorder(100)
	m := &scenario84CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	// step seeds the supplied Jobs and reconciles once, accumulating metrics.
	step := func(jobs ...*batchv1.Job) {
		for _, j := range jobs {
			require.NoError(s.T(), k8sClient.Create(s.ctx, j),
				"the operator-shaped Job must persist")
		}
		_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
		require.NoError(s.T(), err, "reconcile should succeed")
	}

	getCluster := func() *cbv1alpha1.CloudberryCluster {
		updated := &cbv1alpha1.CloudberryCluster{}
		require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}, updated))
		return updated
	}
	return m, step, getCluster
}

// --- TC-84-F-01: full-lifecycle recorder sweep ---

// TestFunctional_Scenario84_FullLifecycleRecorderSweep reconciles operator-shaped
// Jobs through the AdminReconciler across the full backup lifecycle in SEQUENCE
// (full backup -> incremental backup -> restore -> cleanup -> forced failure ->
// reset) — exactly as the live script drives it, since the operator records the
// per-type backup family (M1/M5) and the restore family (M6) only for the LATEST
// backup/restore Job. A single shared counting recorder accumulates the calls so
// the test asserts EVERY one of the 9 metrics fired with the right labels.
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_FullLifecycleRecorderSweep() {
	cluster := scenario84Cluster("s84-sweep")
	m, step, getCluster := s.newLifecycleHarness(cluster)
	base := time.Now()

	// STEP-full: a Succeeded full backup is the latest backup/restore Job.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, scenario84FullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		sizeBytes:  scenario84SizeBytes,
		succeeded:  true,
		withTimes:  true,
		createdAt:  base.Add(-50 * time.Minute),
	}))
	assert.True(s.T(), m.hasBackupTotal("full", "success"),
		"M1: backup_total{type=full,result=success} must be recorded")
	assert.True(s.T(), m.hasBackupDuration("full"),
		"M2: backup_duration_seconds{type=full} must be observed")
	require.NotEmpty(s.T(), m.backupSize, "M3: SetBackupSizeBytes must be called")
	require.NotEmpty(s.T(), m.lastSuccessTS, "M4: SetBackupLastSuccessTimestamp must be called")
	full, ok := m.lastBackupStatus()
	require.True(s.T(), ok, "M5: SetBackupLastStatus must be called")
	assert.Equal(s.T(), float64(0), full, "M5: a Succeeded full backup must set last_status=0")

	// STEP-incremental: a Succeeded incremental backup is now the latest backup.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, scenario84IncrTS),
		operation:  util.BackupOperationBackup,
		backupType: "incremental",
		succeeded:  true,
		withTimes:  true,
		createdAt:  base.Add(-40 * time.Minute),
	}))
	assert.True(s.T(), m.hasBackupTotal("incremental", "success"),
		"M1: backup_total{type=incremental,result=success} must be recorded")
	assert.True(s.T(), m.hasBackupDuration("incremental"),
		"M2: backup_duration_seconds{type=incremental} must be observed")

	// STEP-restore: a Succeeded restore is now the latest backup/restore Job.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:      util.RestoreJobName(cluster.Name, scenario84FullTS),
		operation: util.BackupOperationRestore,
		succeeded: true,
		withTimes: true,
		createdAt: base.Add(-30 * time.Minute),
	}))
	assert.Contains(s.T(), m.restoreTotal, "success",
		"M6: restore_total{result=success} must be recorded")
	require.NotEmpty(s.T(), m.restoreDuration, "M7: ObserveRestoreDuration must be called")
	assert.Greater(s.T(), m.restoreDuration[0], float64(0),
		"M7: restore duration must be > 0")

	// STEP-retention: a Succeeded cleanup Job (retention-deleted annotation). Use a
	// distinct cleanup timestamp so it never collides with an operator-created
	// retention cleanup Job for the same backup timestamp.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.RetentionCleanupJobName(cluster.Name, "20260608042000"),
		operation:  util.BackupOperationCleanup,
		retDeleted: scenario84RetentionDeleted,
		succeeded:  true,
		withTimes:  true,
		createdAt:  base.Add(-20 * time.Minute),
	}))
	require.NotEmpty(s.T(), m.retentionDeleted, "M8: RecordBackupRetentionDeleted must be called")
	assert.Equal(s.T(), 3, m.retentionDeleted[0],
		"M8: retention_deleted must equal the annotated count")

	// STEP-failure: a Failed backup becomes the latest backup -> last_status=1.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, "20260608049999"),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		failed:     true,
		createdAt:  base.Add(-10 * time.Minute),
	}))
	failStatus, ok := m.lastBackupStatus()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), failStatus,
		"M5: a Failed latest backup must set last_status=1")
	assert.True(s.T(), m.hasBackupTotal("full", "failed"),
		"M1: backup_total{type=full,result=failed} must be recorded")
	assert.True(s.T(), m.jobStatusFor(util.BackupOperationBackup, 3),
		"M9: backup_job_status{operation=backup} must observe 3 (failed)")

	// STEP-reset: a Succeeded backup becomes the latest -> last_status back to 0.
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, "20260608050000"),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		sizeBytes:  scenario84SizeBytes,
		succeeded:  true,
		withTimes:  true,
		createdAt:  base,
	}))
	resetStatus, ok := m.lastBackupStatus()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(0), resetStatus,
		"M5: steady-state last_status must return to 0 after a success")

	// M9 backup_job_status for every observed op reached succeeded(2).
	assert.True(s.T(), m.jobStatusFor(util.BackupOperationBackup, 2),
		"M9: backup_job_status{operation=backup} must observe 2 (succeeded)")
	assert.True(s.T(), m.jobStatusFor(util.BackupOperationRestore, 2),
		"M9: backup_job_status{operation=restore} must observe 2 (succeeded)")
	assert.True(s.T(), m.jobStatusFor(util.BackupOperationCleanup, 2),
		"M9: backup_job_status{operation=cleanup} must observe 2 (succeeded)")

	// The persisted CR reflects the final Succeeded backup.
	assert.Equal(s.T(), "Success", getCluster().Status.LastBackupStatus)
}

// --- TC-84-F: M3 size + M4 last-success-timestamp on a Succeeded full backup ---

// TestFunctional_Scenario84_FullBackupSizeAndTimestamp reconciles a single
// Succeeded full backup Job (size annotation + completionTime) and asserts the
// size gauge (M3) and last-success timestamp (M4) are set with the right values.
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_FullBackupSizeAndTimestamp() {
	cluster := scenario84Cluster("s84-size")
	job := scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, scenario84FullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		sizeBytes:  scenario84SizeBytes,
		succeeded:  true,
		withTimes:  true,
	})

	m, _ := s.reconcileJobs(cluster, job)

	// M3 backup_size_bytes{timestamp}.
	require.NotEmpty(s.T(), m.backupSize, "M3: SetBackupSizeBytes must be called")
	assert.Equal(s.T(), scenario84FullTS, m.backupSize[0].timestamp,
		"M3: backup_size_bytes must carry the 14-digit timestamp label")
	assert.Equal(s.T(), float64(104857600), m.backupSize[0].bytes,
		"M3: backup_size_bytes must equal the annotated byte count")

	// M4 backup_last_success_timestamp ~ completionTime.Unix().
	require.NotEmpty(s.T(), m.lastSuccessTS, "M4: SetBackupLastSuccessTimestamp must be called")
	assert.Greater(s.T(), m.lastSuccessTS[0], float64(0),
		"M4: backup_last_success_timestamp must be a positive unix time")

	// M2 backup_duration_seconds{full} (latest + only backup Job).
	assert.True(s.T(), m.hasBackupDuration("full"),
		"M2: backup_duration_seconds{type=full} must be observed")

	// Negative: a backup Job WITHOUT a size annotation must NOT set the size gauge.
	cluster2 := scenario84Cluster("s84-nosize")
	jobNoSize := scenario84Job(cluster2.Name, s84JobOpts{
		name:       util.BackupJobName(cluster2.Name, scenario84FullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		succeeded:  true,
		withTimes:  true,
	})
	m2, _ := s.reconcileJobs(cluster2, jobNoSize)
	assert.Empty(s.T(), m2.backupSize,
		"M3 negative: no size annotation => backup_size_bytes must NOT be set")
}

// --- TC-84-F-02: forced-failure last_status path (latest Failed -> 1) ---

// TestFunctional_Scenario84_ForcedFailureLastStatus reconciles a cluster whose
// LATEST backup Job is Failed and asserts cloudberry_backup_last_status=1 (M5)
// and backup_job_status{operation=backup}=3 (M9), plus the failed result on the
// backup_total counter.
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_ForcedFailureLastStatus() {
	cluster := scenario84Cluster("s84-fail")
	base := time.Now()

	// A prior Succeeded full backup, then a LATER Failed backup (the latest).
	okJob := scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, scenario84FullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		succeeded:  true,
		withTimes:  true,
		createdAt:  base.Add(-10 * time.Minute),
	})
	badJob := scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, "20260608049999"),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		failed:     true,
		createdAt:  base,
	})

	m, updated := s.reconcileJobs(cluster, okJob, badJob)

	last, ok := m.lastBackupStatus()
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), last,
		"M5: a Failed latest backup must set cloudberry_backup_last_status=1")
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus,
		"the latest Failed backup must drive status.lastBackupStatus=Failed")

	assert.True(s.T(), m.jobStatusFor(util.BackupOperationBackup, 3),
		"M9: backup_job_status{operation=backup} must observe 3 (failed) for the bad Job")
	assert.True(s.T(), m.hasBackupTotal("full", "failed"),
		"M1: backup_total{type=full,result=failed} must be recorded for the failed backup")
}

// --- TC-84-F-03: job-status code mapping 0/1/2/3 across a lifecycle ---

// TestFunctional_Scenario84_JobStatusLifecycle reconciles a backup Job at each
// of pending(0) -> running(1) -> succeeded(2) (and a failed(3) Job) and asserts
// the job_status gauge observes the right code each time (M9).
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_JobStatusLifecycle() {
	cases := []struct {
		name   string
		mutate func(j *batchv1.Job)
		code   float64
	}{
		{
			name:   "pending",
			mutate: func(j *batchv1.Job) { j.Status = batchv1.JobStatus{} },
			code:   0,
		},
		{
			name: "running",
			mutate: func(j *batchv1.Job) {
				start := metav1.NewTime(time.Now())
				j.Status = batchv1.JobStatus{Active: 1, StartTime: &start}
			},
			code: 1,
		},
		{
			name: "succeeded",
			mutate: func(j *batchv1.Job) {
				start := metav1.NewTime(time.Now().Add(-time.Minute))
				completion := metav1.NewTime(time.Now())
				j.Status = batchv1.JobStatus{
					Succeeded: 1, StartTime: &start, CompletionTime: &completion,
				}
			},
			code: 2,
		},
		{
			name: "failed",
			mutate: func(j *batchv1.Job) {
				j.Status = batchv1.JobStatus{
					Failed: 1,
					Conditions: []batchv1.JobCondition{{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
						Reason: "BackoffLimitExceeded",
					}},
				}
			},
			code: 3,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			cluster := scenario84Cluster("s84-jl-" + tc.name)
			job := scenario84Job(cluster.Name, s84JobOpts{
				name:       util.BackupJobName(cluster.Name, scenario84FullTS),
				operation:  util.BackupOperationBackup,
				backupType: "full",
			})
			tc.mutate(job)

			m, _ := s.reconcileJobs(cluster, job)
			assert.True(s.T(), m.jobStatusFor(util.BackupOperationBackup, tc.code),
				"M9: %s Job must record backup_job_status==%v", tc.name, tc.code)
		})
	}
}

// --- TC-84-F: succeeded restore => restore_total + restore_duration + js=2 ---

// TestFunctional_Scenario84_RestoreMetrics reconciles a Succeeded restore Job as
// the latest Job and asserts restore_total{success} (M6), restore_duration (M7)
// and backup_job_status{restore}=2 (M9).
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_RestoreMetrics() {
	cluster := scenario84Cluster("s84-restore")
	job := scenario84Job(cluster.Name, s84JobOpts{
		name:      util.RestoreJobName(cluster.Name, scenario84FullTS),
		operation: util.BackupOperationRestore,
		succeeded: true,
		withTimes: true,
	})

	m, _ := s.reconcileJobs(cluster, job)

	assert.Contains(s.T(), m.restoreTotal, "success",
		"M6: restore_total{result=success} must be recorded")
	require.NotEmpty(s.T(), m.restoreDuration, "M7: ObserveRestoreDuration must be called")
	assert.Greater(s.T(), m.restoreDuration[0], float64(0),
		"M7: restore_duration_seconds must be > 0")
	assert.True(s.T(), m.jobStatusFor(util.BackupOperationRestore, 2),
		"M9: backup_job_status{operation=restore} must observe 2 (succeeded)")

	// Negative: a restore Job WITHOUT completionTime records restore_total (latest
	// path) but NOT the duration (duration 0 => not observed).
	cluster2 := scenario84Cluster("s84-restore-noend")
	jobNoEnd := scenario84Job(cluster2.Name, s84JobOpts{
		name:      util.RestoreJobName(cluster2.Name, scenario84FullTS),
		operation: util.BackupOperationRestore,
		succeeded: true,
		withTimes: false,
	})
	m2, _ := s.reconcileJobs(cluster2, jobNoEnd)
	assert.Contains(s.T(), m2.restoreTotal, "success",
		"M6 negative: restore_total still records on the latest restore Job")
	assert.Empty(s.T(), m2.restoreDuration,
		"M7 negative: a restore Job without completionTime must NOT observe duration")
}

// --- TC-84-F-04: label-contract guard (GAP-A) ---

// TestFunctional_Scenario84_ResultLabelContract documents + guards GAP-A: the
// operator emits the outcome label `result` (success|failed) on backup_total /
// restore_total, NOT `status`. The counting recorder captures the exact string
// the operator passes; it must be lowercase success/failed.
func (s *Scenario84MetricsSuite) TestFunctional_Scenario84_ResultLabelContract() {
	cluster := scenario84Cluster("s84-label")
	m, step, _ := s.newLifecycleHarness(cluster)
	base := time.Now()

	// A backup then a restore (each the latest in turn) so both backup_total and
	// restore_total fire (the operator records each only for the latest Job).
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:       util.BackupJobName(cluster.Name, scenario84FullTS),
		operation:  util.BackupOperationBackup,
		backupType: "full",
		succeeded:  true,
		withTimes:  true,
		createdAt:  base.Add(-5 * time.Minute),
	}))
	step(scenario84Job(cluster.Name, s84JobOpts{
		name:      util.RestoreJobName(cluster.Name, scenario84FullTS),
		operation: util.BackupOperationRestore,
		succeeded: true,
		withTimes: true,
		createdAt: base,
	}))

	require.NotEmpty(s.T(), m.backupTotal)
	for _, b := range m.backupTotal {
		assert.Contains(s.T(), []string{"success", "failed"}, b.result,
			"GAP-A: backup_total outcome label `result` must be success|failed (got %q)", b.result)
	}
	require.NotEmpty(s.T(), m.restoreTotal)
	for _, r := range m.restoreTotal {
		assert.Contains(s.T(), []string{"success", "failed"}, r,
			"GAP-A: restore_total outcome label `result` must be success|failed (got %q)", r)
	}
}
