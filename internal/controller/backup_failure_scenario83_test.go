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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// jobFailedCondition builds a Job condition of type Failed with the given status
// and reason — the shape Kubernetes sets when a Job is killed at
// activeDeadlineSeconds (DeadlineExceeded) or exhausts backoffLimit
// (BackoffLimitExceeded).
func jobFailedCondition(status corev1.ConditionStatus, reason string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:   batchv1.JobFailed,
		Status: status,
		Reason: reason,
	}
}

// jobCompleteCondition builds a Job condition of type Complete (used to assert
// that a non-Failed condition type is not mis-classified as failed).
func jobCompleteCondition(status corev1.ConditionStatus) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:   batchv1.JobComplete,
		Status: status,
		Reason: "",
	}
}

// TestJobHasFailedCondition_Scenario83 is the direct truth-table for
// jobHasFailedCondition: a JobFailed condition with status True (regardless of
// reason) is failed; a False JobFailed condition, a non-Failed condition type,
// and the no-conditions case are all not failed.
func TestJobHasFailedCondition_Scenario83(t *testing.T) {
	tests := []struct {
		name       string
		conditions []batchv1.JobCondition
		want       bool
	}{
		{
			name:       "deadline exceeded condition true is failed",
			conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			want:       true,
		},
		{
			name:       "backoff limit exceeded condition true is failed",
			conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
			want:       true,
		},
		{
			name:       "failed condition with status false is not failed",
			conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionFalse, "DeadlineExceeded")},
			want:       false,
		},
		{
			name:       "complete condition true is not failed",
			conditions: []batchv1.JobCondition{jobCompleteCondition(corev1.ConditionTrue)},
			want:       false,
		},
		{
			name:       "no conditions is not failed",
			conditions: nil,
			want:       false,
		},
		{
			name: "mixed conditions with a true failed wins",
			conditions: []batchv1.JobCondition{
				jobCompleteCondition(corev1.ConditionFalse),
				jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded"),
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: batchv1.JobStatus{Conditions: tc.conditions}}
			assert.Equal(t, tc.want, jobHasFailedCondition(job))
		})
	}
}

// TestBackupJobStatus_Scenario83 is the 9-row truth table for backupJobStatus,
// asserting GAP-1: a Job whose ONLY failure signal is a JobFailed condition
// (Status.Failed==0, e.g. DeadlineExceeded) is classified "Failed", while
// Succeeded precedence and the in-progress/no-signal cases are preserved.
func TestBackupJobStatus_Scenario83(t *testing.T) {
	start := metav1.NewTime(time.Now())
	tests := []struct {
		name   string
		status batchv1.JobStatus
		want   string
	}{
		{
			name:   "succeeded is success",
			status: batchv1.JobStatus{Succeeded: 1},
			want:   backupStatusSuccess,
		},
		{
			name:   "failed pod count is failed",
			status: batchv1.JobStatus{Failed: 1},
			want:   backupStatusFailed,
		},
		{
			name: "deadline exceeded condition with zero failed pods is failed",
			status: batchv1.JobStatus{
				Failed:     0,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			},
			want: backupStatusFailed,
		},
		{
			name: "backoff limit exceeded condition with zero failed pods is failed",
			status: batchv1.JobStatus{
				Failed:     0,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
			},
			want: backupStatusFailed,
		},
		{
			name: "failed pods and condition together is failed",
			status: batchv1.JobStatus{
				Failed:     3,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
			},
			want: backupStatusFailed,
		},
		{
			name: "succeeded precedence over a stale failed condition",
			status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			},
			want: backupStatusSuccess,
		},
		{
			name:   "active job is in progress",
			status: batchv1.JobStatus{Active: 1, StartTime: &start},
			want:   backupStatusInProgress,
		},
		{
			name: "failed condition false is in progress",
			status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionFalse, "DeadlineExceeded")},
			},
			want: backupStatusInProgress,
		},
		{
			name: "complete condition wrong type is in progress",
			status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{jobCompleteCondition(corev1.ConditionTrue)},
			},
			want: backupStatusInProgress,
		},
		{
			name:   "empty job is in progress",
			status: batchv1.JobStatus{},
			want:   backupStatusInProgress,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: tc.status}
			assert.Equal(t, tc.want, backupJobStatus(job))
		})
	}
}

// TestBackupJobStatusCode_Scenario83 mirrors the backupJobStatus truth table for
// the numeric gauge code, asserting Failed==3.0 for the JobFailed-condition-only
// shape (Status.Failed==0) as well as Status.Failed>0, with Succeeded==2.0
// precedence and Running==1.0 / Pending==0.0 preserved.
func TestBackupJobStatusCode_Scenario83(t *testing.T) {
	start := metav1.NewTime(time.Now())
	tests := []struct {
		name   string
		status batchv1.JobStatus
		want   float64
	}{
		{
			name:   "succeeded code",
			status: batchv1.JobStatus{Succeeded: 1},
			want:   backupJobStatusSucceeded,
		},
		{
			name:   "failed pod count code",
			status: batchv1.JobStatus{Failed: 1},
			want:   backupJobStatusFailed,
		},
		{
			name: "deadline exceeded condition only code is failed",
			status: batchv1.JobStatus{
				Failed:     0,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			},
			want: backupJobStatusFailed,
		},
		{
			name: "backoff limit exceeded condition only code is failed",
			status: batchv1.JobStatus{
				Failed:     0,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
			},
			want: backupJobStatusFailed,
		},
		{
			name: "succeeded precedence over stale failed condition code",
			status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			},
			want: backupJobStatusSucceeded,
		},
		{
			name:   "active is running code",
			status: batchv1.JobStatus{Active: 1},
			want:   backupJobStatusRunning,
		},
		{
			name:   "start time only is running code",
			status: batchv1.JobStatus{StartTime: &start},
			want:   backupJobStatusRunning,
		},
		{
			name: "failed condition false is pending code",
			status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionFalse, "DeadlineExceeded")},
			},
			want: backupJobStatusPending,
		},
		{
			name:   "empty job is pending code",
			status: batchv1.JobStatus{},
			want:   backupJobStatusPending,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: tc.status}
			assert.InDelta(t, tc.want, backupJobStatusCode(job), 0.0001)
		})
	}
}

// deadlineFailedBackupJob builds a backup-operation Job whose ONLY failure
// signal is a JobFailed/DeadlineExceeded condition with Status.Failed==0 (the
// activeDeadlineSeconds-kill shape).
func deadlineFailedBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster, name, util.BackupOperationBackup, batchv1.JobStatus{
		Failed:         0,
		StartTime:      &start,
		CompletionTime: &completion,
		Conditions:     []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
	})
	return job
}

// TestApplyBackupJobToStatus_DeadlineFailed_Scenario83 drives the deadline-kill
// failure end-to-end through refreshBackupStatus: a backup Job failed ONLY via
// the JobFailed/DeadlineExceeded condition (Status.Failed==0) must set
// LastBackupStatus=Failed, call SetBackupLastStatus with the failed value (1),
// and emit exactly one BackupFailed Warning event.
func TestApplyBackupJobToStatus_DeadlineFailed_Scenario83(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := deadlineFailedBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	assert.Equal(t, job.Name, cluster.Status.LastBackupJobName)

	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusFailed, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])
	require.Contains(t, m.setBackupJobStatus, backupJobStatusFailed)

	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningBackupFailed(events),
		"a deadline-killed backup must emit exactly one Warning/BackupFailed event: %v", events)
}

// TestApplyBackupJobToStatus_BackoffExhaustedFailed_Scenario83 covers the
// backoffLimit-exhausted shape (JobFailed/BackoffLimitExceeded condition true,
// Status.Failed==backoffLimit): classified Failed with last_status==1.
func TestApplyBackupJobToStatus_BackoffExhaustedFailed_Scenario83(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101030000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{
			Failed:         2,
			StartTime:      &start,
			CompletionTime: &completion,
			Conditions:     []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "BackoffLimitExceeded")},
		},
	)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusFailed, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])

	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningBackupFailed(events),
		"a backoff-exhausted backup must emit exactly one Warning/BackupFailed event: %v", events)
}

// TestApplyBackupJobToStatus_SucceededNoWarning_Scenario83 is the success
// regression: a Succeeded backup Job sets last_status to the success value (0)
// and emits NO BackupFailed Warning.
func TestApplyBackupJobToStatus_SucceededNoWarning_Scenario83(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := succeededBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101040000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusSuccess, cluster.Status.LastBackupStatus)
	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusSuccess, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])

	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningBackupFailed(events),
		"a succeeded backup must not emit a BackupFailed Warning: %v", events)
}

// TestValidationJobResult_Scenario83 verifies validationJobResult honors a
// JobFailed condition (Status.Failed==0) as a failed classification, and keeps
// the Succeeded / no-signal behavior intact.
func TestValidationJobResult_Scenario83(t *testing.T) {
	tests := []struct {
		name       string
		status     batchv1.JobStatus
		wantResult string
		wantOK     bool
	}{
		{
			name:       "succeeded is success result",
			status:     batchv1.JobStatus{Succeeded: 1},
			wantResult: validationResultSuccess,
			wantOK:     true,
		},
		{
			name:       "failed pod count is failed result",
			status:     batchv1.JobStatus{Failed: 1},
			wantResult: validationResultFailed,
			wantOK:     true,
		},
		{
			name: "deadline exceeded condition only is failed result",
			status: batchv1.JobStatus{
				Failed:     0,
				Conditions: []batchv1.JobCondition{jobFailedCondition(corev1.ConditionTrue, "DeadlineExceeded")},
			},
			wantResult: validationResultFailed,
			wantOK:     true,
		},
		{
			name:       "no terminal signal is not recorded",
			status:     batchv1.JobStatus{Active: 1},
			wantResult: "",
			wantOK:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: tc.status}
			result, ok := validationJobResult(job)
			assert.Equal(t, tc.wantResult, result)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}
