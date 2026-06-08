package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// drainEvents collects all currently-buffered events from a FakeRecorder
// channel without blocking, returning them in emission order.
func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

// countWarningBackupFailed counts how many drained events are Warning events
// carrying the BackupFailed reason.
func countWarningBackupFailed(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonBackupFailed) {
			n++
		}
	}
	return n
}

// failedBackupJob builds a failed backup-operation Job fixture.
func failedBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster, name, util.BackupOperationBackup, batchv1.JobStatus{
		Failed: 1, StartTime: &start, CompletionTime: &completion,
	})
	return job
}

// succeededBackupJob builds a succeeded backup-operation Job fixture.
func succeededBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return backupJob(cluster, name, util.BackupOperationBackup, batchv1.JobStatus{
		Succeeded: 1, StartTime: &start, CompletionTime: &completion,
	})
}

// TestEmitBackupFailureEvent_Scenario77 exercises the de-duplicated Warning
// Event emission directly through emitBackupFailureEvent. It is table-driven and
// asserts the exact number of Warning/BackupFailed events for each transition.
func TestEmitBackupFailureEvent_Scenario77(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		prevStatus  string
		prevJobName string
		jobName     string
		wantWarning int
	}{
		{
			name:        "new failed job emits exactly one warning",
			status:      backupStatusFailed,
			prevStatus:  "",
			prevJobName: "",
			jobName:     "backup-a",
			wantWarning: 1,
		},
		{
			name:        "same failed job de-duplicated (no new warning)",
			status:      backupStatusFailed,
			prevStatus:  backupStatusFailed,
			prevJobName: "backup-a",
			jobName:     "backup-a",
			wantWarning: 0,
		},
		{
			name:        "transition success->failed for new job emits one warning",
			status:      backupStatusFailed,
			prevStatus:  backupStatusSuccess,
			prevJobName: "backup-a",
			jobName:     "backup-b",
			wantWarning: 1,
		},
		{
			name:        "failed status but different prev job name still emits (new job)",
			status:      backupStatusFailed,
			prevStatus:  backupStatusFailed,
			prevJobName: "backup-old",
			jobName:     "backup-new",
			wantWarning: 1,
		},
		{
			name:        "success status emits no warning",
			status:      backupStatusSuccess,
			prevStatus:  backupStatusFailed,
			prevJobName: "backup-a",
			jobName:     "backup-a",
			wantWarning: 0,
		},
		{
			name:        "in-progress status emits no warning",
			status:      backupStatusInProgress,
			prevStatus:  "",
			prevJobName: "",
			jobName:     "backup-a",
			wantWarning: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := record.NewFakeRecorder(10)
			cluster := backupTestCluster()
			r := NewAdminReconciler(
				fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster).Build(),
				newTestScheme(), recorder, builder.NewBuilder(), nil,
				&countingBackupMetrics{}, nil,
			)
			job := failedBackupJob(cluster.Name, tc.jobName)

			r.emitBackupFailureEvent(cluster, job, tc.status, tc.prevStatus, tc.prevJobName)

			events := drainEvents(recorder)
			assert.Equal(t, tc.wantWarning, countWarningBackupFailed(events),
				"unexpected number of Warning/BackupFailed events: %v", events)
		})
	}
}

// TestApplyBackupJobToStatus_FailedEmitsWarning_Scenario77 drives the failure
// path through refreshBackupStatus (which calls applyBackupJobToStatus): a
// freshly-observed failed backup Job must set LastBackupStatus=Failed AND emit
// exactly one Warning/BackupFailed event.
func TestApplyBackupJobToStatus_FailedEmitsWarning_Scenario77(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := failedBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	assert.Equal(t, job.Name, cluster.Status.LastBackupJobName)

	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningBackupFailed(events),
		"expected exactly one Warning/BackupFailed event: %v", events)
	require.Len(t, events, 1)
	assert.Contains(t, events[0], corev1.EventTypeWarning)
	assert.Contains(t, events[0], cbv1alpha1.EventReasonBackupFailed)
	assert.Contains(t, events[0], job.Name)
}

// TestApplyBackupJobToStatus_FailedDeDup_Scenario77 verifies that reconciling
// the SAME failed backup Job twice (prevJobName==job.Name && prevStatus=Failed)
// emits the Warning only once: the second observation is de-duplicated.
func TestApplyBackupJobToStatus_FailedDeDup_Scenario77(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := failedBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	// First observation: emits one Warning and records LastBackupStatus=Failed.
	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	first := drainEvents(recorder)
	require.Equal(t, 1, countWarningBackupFailed(first))

	// Second observation of the same failed Job: de-duplicated, no new Warning.
	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	second := drainEvents(recorder)
	assert.Equal(t, 0, countWarningBackupFailed(second),
		"second reconcile of unchanged failed Job must not emit a new Warning: %v", second)
}

// TestApplyBackupJobToStatus_Succeeded_Scenario77 verifies a succeeded backup
// Job sets LastBackupStatus=Success and emits NO Warning/BackupFailed event.
func TestApplyBackupJobToStatus_Succeeded_Scenario77(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := succeededBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusSuccess, cluster.Status.LastBackupStatus)
	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningBackupFailed(events),
		"a succeeded backup must not emit a BackupFailed Warning: %v", events)
}

// TestApplyBackupJobToStatus_RestoreFailedExcluded_Scenario77 verifies that a
// FAILED restore-operation Job does NOT emit a BackupFailed Warning (Scenario 77
// is backup-only; restore failures are excluded).
func TestApplyBackupJobToStatus_RestoreFailedExcluded_Scenario77(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260101020000"),
		util.BackupOperationRestore,
		batchv1.JobStatus{Failed: 1, StartTime: &start, CompletionTime: &completion},
	)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	// The restore failure is still recorded as the last backup status...
	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	// ...but NO BackupFailed Warning is emitted for a restore operation.
	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningBackupFailed(events),
		"a restore failure must not emit a BackupFailed Warning: %v", events)
}

// TestApplyBackupJobToStatus_SuccessThenFailedTransition_Scenario77 verifies a
// transition from a previously Succeeded backup Job (A) to a Failed backup Job
// (B) emits exactly one Warning on the failing observation.
func TestApplyBackupJobToStatus_SuccessThenFailedTransition_Scenario77(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()

	// Seed prior state: a previously-succeeded backup Job A.
	cluster.Status.LastBackupStatus = backupStatusSuccess
	cluster.Status.LastBackupJobName = util.BackupJobName(cluster.Name, "20260101010000")

	// Now a NEW, more-recently-created failed backup Job B is observed.
	jobB := failedBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))
	jobB.CreationTimestamp = metav1.NewTime(time.Now().Add(time.Minute))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, jobB).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	assert.Equal(t, jobB.Name, cluster.Status.LastBackupJobName)
	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningBackupFailed(events),
		"success->failed transition must emit exactly one Warning: %v", events)
}
