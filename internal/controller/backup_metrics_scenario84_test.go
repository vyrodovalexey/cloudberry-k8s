package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// Scenario 84 — Prometheus Metrics / gpbackup_exporter (controller wiring).
//
// These tests drive refreshBackupStatus (-> recordBackupJobMetrics ->
// applyBackupJobToStatus -> recordLatestBackupMetrics) with operator-shaped Jobs
// (correct avsoft.io labels/annotations + status) and assert, via a recording
// fake metrics.Recorder that captures the EXACT method + labels + value per
// call, that each of the 9 backup-lifecycle metrics is emitted as specified.

// recordedCall captures one metric method invocation with its label/value args
// so a test can assert the precise contract (method, type/operation/result,
// numeric value) the controller passed to the recorder.
type recordedCall struct {
	method    string
	cluster   string
	namespace string
	backType  string
	result    string
	timestamp string
	job       string
	operation string
	value     float64
	duration  time.Duration
}

// recordingBackupMetrics is a metrics.Recorder (via embedded NoopRecorder) that
// records every backup-lifecycle call. It is the Scenario-84 extension of the
// countingBackupMetrics fake: it keeps full argument tuples so tests can assert
// labels and values, not just counts.
type recordingBackupMetrics struct {
	metrics.NoopRecorder
	calls []recordedCall
}

func (m *recordingBackupMetrics) RecordBackup(cluster, ns, backType, result string) {
	m.calls = append(m.calls, recordedCall{
		method: "RecordBackup", cluster: cluster, namespace: ns,
		backType: backType, result: result,
	})
}

func (m *recordingBackupMetrics) ObserveBackupDuration(cluster, ns, backType string, d time.Duration) {
	m.calls = append(m.calls, recordedCall{
		method: "ObserveBackupDuration", cluster: cluster, namespace: ns,
		backType: backType, duration: d,
	})
}

func (m *recordingBackupMetrics) SetBackupSizeBytes(cluster, ns, timestamp string, bytes float64) {
	m.calls = append(m.calls, recordedCall{
		method: "SetBackupSizeBytes", cluster: cluster, namespace: ns,
		timestamp: timestamp, value: bytes,
	})
}

func (m *recordingBackupMetrics) SetBackupLastSuccessTimestamp(cluster, ns string, ts float64) {
	m.calls = append(m.calls, recordedCall{
		method: "SetBackupLastSuccessTimestamp", cluster: cluster, namespace: ns, value: ts,
	})
}

func (m *recordingBackupMetrics) SetBackupLastStatus(cluster, ns string, status float64) {
	m.calls = append(m.calls, recordedCall{
		method: "SetBackupLastStatus", cluster: cluster, namespace: ns, value: status,
	})
}

func (m *recordingBackupMetrics) ObserveRestoreDuration(cluster, ns string, d time.Duration) {
	m.calls = append(m.calls, recordedCall{
		method: "ObserveRestoreDuration", cluster: cluster, namespace: ns, duration: d,
	})
}

func (m *recordingBackupMetrics) RecordBackupRetentionDeleted(cluster, ns string, n int) {
	m.calls = append(m.calls, recordedCall{
		method: "RecordBackupRetentionDeleted", cluster: cluster, namespace: ns, value: float64(n),
	})
}

func (m *recordingBackupMetrics) SetBackupJobStatus(cluster, ns, job, operation string, status float64) {
	m.calls = append(m.calls, recordedCall{
		method: "SetBackupJobStatus", cluster: cluster, namespace: ns,
		job: job, operation: operation, value: status,
	})
}

func (m *recordingBackupMetrics) RecordRestore(cluster, ns, result string) {
	m.calls = append(m.calls, recordedCall{
		method: "RecordRestore", cluster: cluster, namespace: ns, result: result,
	})
}

// find returns the first recorded call matching the predicate, or false.
func (m *recordingBackupMetrics) find(pred func(recordedCall) bool) (recordedCall, bool) {
	for _, c := range m.calls {
		if pred(c) {
			return c, true
		}
	}
	return recordedCall{}, false
}

// has reports whether any recorded call matches the predicate.
func (m *recordingBackupMetrics) has(pred func(recordedCall) bool) bool {
	_, ok := m.find(pred)
	return ok
}

// typedBackupJob builds a succeeded backup Job carrying the avsoft.io/backup-type
// label and the size annotation, with start+completion timestamps so duration
// and size metrics are populated.
func typedBackupJob(cluster, name, backupType string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster, name, util.BackupOperationBackup, batchv1.JobStatus{
		Succeeded: 1, StartTime: &start, CompletionTime: &completion,
	})
	job.Labels[util.LabelBackupType] = backupType
	job.Annotations = map[string]string{util.AnnotationBackupSizeBytes: "104857600"}
	return job
}

// TestScenario84_FullBackupSucceeded drives a Succeeded FULL backup Job and
// asserts M1 (RecordBackup type=full,result=success), M5 (SetBackupLastStatus=0),
// M4 (SetBackupLastSuccessTimestamp ~ completion), M2 (ObserveBackupDuration
// type=full, dur>0), M3 (SetBackupSizeBytes ts,bytes) and M9 (SetBackupJobStatus
// operation=backup, code=2 succeeded).
func TestScenario84_FullBackupSucceeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := typedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608205700"), util.BackupTypeFull)
	completion := job.Status.CompletionTime

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	// M1: backup_total{type=full,result=success}.
	rb, ok := m.find(func(c recordedCall) bool { return c.method == "RecordBackup" })
	require.True(t, ok, "RecordBackup must be called")
	assert.Equal(t, util.BackupTypeFull, rb.backType)
	assert.Equal(t, "success", rb.result)
	assert.Equal(t, cluster.Name, rb.cluster)
	assert.Equal(t, cluster.Namespace, rb.namespace)

	// M5: backup_last_status == 0 (success).
	ls, ok := m.find(func(c recordedCall) bool { return c.method == "SetBackupLastStatus" })
	require.True(t, ok)
	assert.InDelta(t, backupLastStatusSuccess, ls.value, 0.001)

	// M4: backup_last_success_timestamp ~ completion.Unix().
	ts, ok := m.find(func(c recordedCall) bool { return c.method == "SetBackupLastSuccessTimestamp" })
	require.True(t, ok)
	assert.InDelta(t, float64(completion.Unix()), ts.value, 1.0)

	// M2: backup_duration_seconds{type=full} observed with dur>0.
	dur, ok := m.find(func(c recordedCall) bool { return c.method == "ObserveBackupDuration" })
	require.True(t, ok)
	assert.Equal(t, util.BackupTypeFull, dur.backType)
	assert.Positive(t, dur.duration)

	// M3: backup_size_bytes{timestamp}=104857600.
	sz, ok := m.find(func(c recordedCall) bool { return c.method == "SetBackupSizeBytes" })
	require.True(t, ok)
	assert.Equal(t, "20260608205700", sz.timestamp)
	assert.InDelta(t, 104857600.0, sz.value, 0.5)

	// M9: backup_job_status{operation=backup} == 2 (succeeded).
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationBackup &&
			c.value == backupJobStatusSucceeded
	}), "SetBackupJobStatus(operation=backup, code=succeeded) expected")
}

// TestScenario84_IncrementalBackupSucceeded drives a Succeeded INCREMENTAL
// backup Job and asserts M1 (RecordBackup type=incremental,result=success) and
// M9 (backup_job_status == 2).
func TestScenario84_IncrementalBackupSucceeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := typedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608210000"), util.BackupTypeIncremental)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	rb, ok := m.find(func(c recordedCall) bool { return c.method == "RecordBackup" })
	require.True(t, ok)
	assert.Equal(t, util.BackupTypeIncremental, rb.backType)
	assert.Equal(t, "success", rb.result)

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationBackup &&
			c.value == backupJobStatusSucceeded
	}))
}

// TestScenario84_FailedBackup drives a Failed backup Job and asserts M5
// (SetBackupLastStatus == 1 failed), M1 (RecordBackup result=failed) and M9
// (backup_job_status == 3 failed).
func TestScenario84_FailedBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608211500"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Failed: 1},
	)
	job.Labels[util.LabelBackupType] = util.BackupTypeFull

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	rb, ok := m.find(func(c recordedCall) bool { return c.method == "RecordBackup" })
	require.True(t, ok)
	assert.Equal(t, "failed", rb.result)

	ls, ok := m.find(func(c recordedCall) bool { return c.method == "SetBackupLastStatus" })
	require.True(t, ok)
	assert.InDelta(t, backupLastStatusFailed, ls.value, 0.001)

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationBackup &&
			c.value == backupJobStatusFailed
	}))

	// No success-only metrics for a failed backup.
	assert.False(t, m.has(func(c recordedCall) bool { return c.method == "SetBackupSizeBytes" }))
	assert.False(t, m.has(func(c recordedCall) bool { return c.method == "SetBackupLastSuccessTimestamp" }))
}

// TestScenario84_RunningBackup drives a running backup Job (StartTime set, no
// completion) and asserts M9 backup_job_status == 1 (running) and M5
// last_status == 2 (in-progress).
func TestScenario84_RunningBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608212000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Active: 1, StartTime: &start},
	)
	job.Labels[util.LabelBackupType] = util.BackupTypeFull

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationBackup &&
			c.value == backupJobStatusRunning
	}), "running backup must set job_status=1")

	ls, ok := m.find(func(c recordedCall) bool { return c.method == "SetBackupLastStatus" })
	require.True(t, ok)
	assert.InDelta(t, backupLastStatusInProgress, ls.value, 0.001)
}

// TestScenario84_PendingBackup drives a pending backup Job (no status signals)
// and asserts M9 backup_job_status == 0 (pending).
func TestScenario84_PendingBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608212500"),
		util.BackupOperationBackup,
		batchv1.JobStatus{},
	)
	job.Labels[util.LabelBackupType] = util.BackupTypeFull

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationBackup &&
			c.value == backupJobStatusPending
	}), "pending backup must set job_status=0")
}

// TestScenario84_RestoreSucceeded drives a Succeeded RESTORE Job and asserts M6
// (RecordRestore result=success), M7 (ObserveRestoreDuration dur>0) and M9
// (backup_job_status{operation=restore} == 2).
func TestScenario84_RestoreSucceeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608213000"),
		util.BackupOperationRestore,
		batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
	)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	rr, ok := m.find(func(c recordedCall) bool { return c.method == "RecordRestore" })
	require.True(t, ok, "RecordRestore must be called for a restore Job")
	assert.Equal(t, "success", rr.result)

	rd, ok := m.find(func(c recordedCall) bool { return c.method == "ObserveRestoreDuration" })
	require.True(t, ok, "ObserveRestoreDuration must be called for a succeeded restore with both timestamps")
	assert.Positive(t, rd.duration)

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationRestore &&
			c.value == backupJobStatusSucceeded
	}))

	// A restore must NOT emit a backup counter.
	assert.False(t, m.has(func(c recordedCall) bool { return c.method == "RecordBackup" }))
}

// TestScenario84_CleanupSucceeded drives a Succeeded CLEANUP Job with the
// retention-deleted annotation and asserts M8 (RecordBackupRetentionDeleted
// n=N) and M9 (backup_job_status{operation=cleanup} == 2).
func TestScenario84_CleanupSucceeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := backupJob(cluster.Name,
		util.RetentionCleanupJobName(cluster.Name, "20260608213500"),
		util.BackupOperationCleanup,
		batchv1.JobStatus{Succeeded: 1},
	)
	job.Annotations = map[string]string{util.AnnotationBackupRetentionDeleted: "4"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	rd, ok := m.find(func(c recordedCall) bool { return c.method == "RecordBackupRetentionDeleted" })
	require.True(t, ok, "RecordBackupRetentionDeleted must be called for a succeeded cleanup with the annotation")
	assert.InDelta(t, 4.0, rd.value, 0.001)

	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationCleanup &&
			c.value == backupJobStatusSucceeded
	}))
}

// TestScenario84_RestoreFailed asserts a Failed restore Job emits RecordRestore
// with result=failed and does NOT observe restore duration.
func TestScenario84_RestoreFailed(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608214000"),
		util.BackupOperationRestore,
		batchv1.JobStatus{Failed: 1},
	)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	rr, ok := m.find(func(c recordedCall) bool { return c.method == "RecordRestore" })
	require.True(t, ok)
	assert.Equal(t, "failed", rr.result)

	assert.False(t, m.has(func(c recordedCall) bool { return c.method == "ObserveRestoreDuration" }))
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			c.operation == util.BackupOperationRestore &&
			c.value == backupJobStatusFailed
	}))
}

// TestScenario84_NonBackupOperationSkipped verifies recordBackupJobMetrics
// ignores Jobs whose operation label is not backup/restore/cleanup (e.g. a
// validation Job): no backup_job_status gauge is emitted for it. This exercises
// the default/continue branch of the per-Job loop.
func TestScenario84_NonBackupOperationSkipped(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	// A validation-operation Job (not backup/restore/cleanup) plus an unlabeled
	// Job — both must be skipped by recordBackupJobMetrics.
	validateJob := backupJob(cluster.Name,
		util.PostRestoreValidationJobName(cluster.Name, "20260608215500"),
		util.BackupOperationValidate,
		batchv1.JobStatus{Succeeded: 1},
	)
	unlabeled := backupJob(cluster.Name, "unrelated-job", "", batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, validateJob, unlabeled).Build()
	m := &recordingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	// No backup_job_status gauge for a non-backup/restore/cleanup operation.
	assert.False(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" &&
			(c.operation == util.BackupOperationValidate || c.operation == "")
	}), "non-backup/restore/cleanup Jobs must be skipped")
}

// TestScenario84_FullLifecycle_AllNineMetrics drives the full backup lifecycle
// as a SEQUENCE of reconciles (mirroring the live STEP 1-6 ordering): each
// reconcile observes a new latest Job — full backup, incremental backup,
// restore, cleanup, then a forced failure — and asserts that across the
// lifecycle ALL 9 Scenario-84 metric recorders are invoked with their expected
// labels/values. (refreshBackupStatus records latest-Job backup metrics only for
// the newest backup/restore Job per pass, exactly like the live operator
// reconciling discrete on-demand Jobs.)
func TestScenario84_FullLifecycle_AllNineMetrics(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()

	now := time.Now()
	mk := func(offset time.Duration) *metav1.Time {
		tm := metav1.NewTime(now.Add(offset))
		return &tm
	}

	// (a) Succeeded FULL backup.
	fullJob := typedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608205700"), util.BackupTypeFull)
	fullJob.CreationTimestamp = *mk(-50 * time.Minute)

	// (b) Succeeded INCREMENTAL backup.
	incrJob := typedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608210000"), util.BackupTypeIncremental)
	incrJob.CreationTimestamp = *mk(-40 * time.Minute)

	// (c) Succeeded RESTORE.
	restoreJob := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608213000"),
		util.BackupOperationRestore,
		batchv1.JobStatus{Succeeded: 1, StartTime: mk(-32 * time.Minute), CompletionTime: mk(-30 * time.Minute)},
	)
	restoreJob.CreationTimestamp = *mk(-30 * time.Minute)

	// (d) Succeeded CLEANUP with retention-deleted annotation.
	cleanupJob := backupJob(cluster.Name,
		util.RetentionCleanupJobName(cluster.Name, "20260608213500"),
		util.BackupOperationCleanup,
		batchv1.JobStatus{Succeeded: 1},
	)
	cleanupJob.Annotations = map[string]string{util.AnnotationBackupRetentionDeleted: "3"}
	cleanupJob.CreationTimestamp = *mk(-20 * time.Minute)

	// (e) Failed backup that is the latest backup Job (drives last_status=1 + job_status=3).
	failJob := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608214500"),
		util.BackupOperationBackup,
		batchv1.JobStatus{
			Failed:     2,
			StartTime:  mk(-12 * time.Minute),
			Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
		},
	)
	failJob.Labels[util.LabelBackupType] = util.BackupTypeFull
	failJob.CreationTimestamp = *mk(-10 * time.Minute)

	m := &recordingBackupMetrics{}

	// Each step adds the new Job and reconciles, so the just-added Job is the
	// latest observed and its latest-Job backup/restore metrics are recorded —
	// exactly like the live operator reconciling discrete on-demand Jobs.
	steps := [][]*batchv1.Job{
		{fullJob},
		{fullJob, incrJob},
		{fullJob, incrJob, restoreJob},
		{fullJob, incrJob, restoreJob, cleanupJob},
		{fullJob, incrJob, restoreJob, cleanupJob, failJob},
	}
	for _, present := range steps {
		cb := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster)
		for _, j := range present {
			cb = cb.WithObjects(j)
		}
		k8sClient := cb.Build()
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
			builder.NewBuilder(), nil, m, nil)
		require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	}

	// M1: backup_total full+success AND incremental+success AND a failed.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "RecordBackup" && c.backType == util.BackupTypeFull && c.result == "success"
	}), "M1 backup_total{type=full,result=success}")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "RecordBackup" && c.backType == util.BackupTypeIncremental && c.result == "success"
	}), "M1 backup_total{type=incremental,result=success}")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "RecordBackup" && c.result == "failed"
	}), "M1 backup_total{...,result=failed}")

	// M2: backup_duration_seconds observed for full and incremental.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "ObserveBackupDuration" && c.backType == util.BackupTypeFull && c.duration > 0
	}), "M2 backup_duration_seconds{type=full}")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "ObserveBackupDuration" && c.backType == util.BackupTypeIncremental && c.duration > 0
	}), "M2 backup_duration_seconds{type=incremental}")

	// M3: backup_size_bytes set with positive bytes.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupSizeBytes" && c.value > 0 && c.timestamp != ""
	}), "M3 backup_size_bytes")

	// M4: backup_last_success_timestamp set.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupLastSuccessTimestamp" && c.value > 0
	}), "M4 backup_last_success_timestamp")

	// M5: backup_last_status — the latest is the failed Job => final value == 1.
	var lastStatus float64 = -1
	for _, c := range m.calls {
		if c.method == "SetBackupLastStatus" {
			lastStatus = c.value
		}
	}
	assert.InDelta(t, backupLastStatusFailed, lastStatus, 0.001,
		"M5 backup_last_status reflects the latest (failed) backup Job")

	// M6: restore_total{result=success}.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "RecordRestore" && c.result == "success"
	}), "M6 restore_total{result=success}")

	// M7: restore_duration_seconds observed.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "ObserveRestoreDuration" && c.duration > 0
	}), "M7 restore_duration_seconds")

	// M8: backup_retention_deleted_total == 3.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "RecordBackupRetentionDeleted" && c.value == 3
	}), "M8 backup_retention_deleted_total")

	// M9: backup_job_status — succeeded(2) for backup/restore/cleanup AND failed(3) for the bad backup.
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" && c.operation == util.BackupOperationBackup && c.value == backupJobStatusSucceeded
	}), "M9 backup_job_status{operation=backup}==2")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" && c.operation == util.BackupOperationRestore && c.value == backupJobStatusSucceeded
	}), "M9 backup_job_status{operation=restore}==2")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" && c.operation == util.BackupOperationCleanup && c.value == backupJobStatusSucceeded
	}), "M9 backup_job_status{operation=cleanup}==2")
	assert.True(t, m.has(func(c recordedCall) bool {
		return c.method == "SetBackupJobStatus" && c.operation == util.BackupOperationBackup && c.value == backupJobStatusFailed
	}), "M9 backup_job_status{operation=backup}==3 (forced failure)")

	// Sanity: the cluster reflects the latest failed backup.
	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
}
