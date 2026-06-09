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

// countWarningRestoreFailed counts how many drained events are Warning events
// carrying the RestoreFailed reason.
func countWarningRestoreFailed(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonRestoreFailed) {
			n++
		}
	}
	return n
}

// restoreJobWithStatus builds a restore-operation Job fixture with the given
// completion status (Scenario 78d).
func restoreJobWithStatus(cluster, name string, status batchv1.JobStatus) *batchv1.Job {
	if status.StartTime == nil {
		start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		status.StartTime = &start
	}
	if status.CompletionTime == nil {
		completion := metav1.NewTime(time.Now())
		status.CompletionTime = &completion
	}
	return backupJob(cluster, name, util.BackupOperationRestore, status)
}

// TestBackupTypeFromJob_Scenario78 is a table-driven test for the
// backupTypeFromJob helper: the Job's avsoft.io/backup-type label takes
// precedence over the spec, with a spec-based fallback when the label is absent.
func TestBackupTypeFromJob_Scenario78(t *testing.T) {
	tests := []struct {
		name        string
		jobLabel    string // "" => no LabelBackupType on the Job.
		specIncr    bool
		want        string
		description string
	}{
		{
			name:     "job labelled incremental over full spec",
			jobLabel: util.BackupTypeIncremental,
			specIncr: false,
			want:     util.BackupTypeIncremental,
		},
		{
			name:     "job labelled full over incremental spec",
			jobLabel: util.BackupTypeFull,
			specIncr: true,
			want:     util.BackupTypeFull,
		},
		{
			name:     "no label falls back to incremental spec",
			jobLabel: "",
			specIncr: true,
			want:     util.BackupTypeIncremental,
		},
		{
			name:     "no label falls back to full spec",
			jobLabel: "",
			specIncr: false,
			want:     util.BackupTypeFull,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := backupTestCluster()
			cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: tc.specIncr}
			job := backupJob(cluster.Name,
				util.BackupJobName(cluster.Name, "20260101010101"),
				util.BackupOperationBackup, batchv1.JobStatus{})
			if tc.jobLabel != "" {
				job.Labels[util.LabelBackupType] = tc.jobLabel
			}

			assert.Equal(t, tc.want, backupTypeFromJob(job, cluster))
		})
	}
}

// TestApplyBackupJobToStatus_IncrementalLabel_Scenario78 verifies that an
// incremental-labelled Succeeded backup Job drives LastBackupType and the latest
// BackupHistory entry to "incremental" EVEN when the cluster spec defaults to
// full (Job-label precedence, Scenario 78b).
func TestApplyBackupJobToStatus_IncrementalLabel_Scenario78(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	// Spec defaults to full; the Job label must win.
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: false}

	job := succeededBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))
	job.Labels[util.LabelBackupType] = util.BackupTypeIncremental

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusSuccess, cluster.Status.LastBackupStatus)
	assert.Equal(t, util.BackupTypeIncremental, cluster.Status.LastBackupType,
		"LastBackupType must derive from the Job label, not the full spec")
	require.NotEmpty(t, cluster.Status.BackupHistory)
	assert.Equal(t, util.BackupTypeIncremental, cluster.Status.BackupHistory[0].Type,
		"history entry type must derive from the Job label")
}

// TestEmitRestoreFailureEvent_Scenario78 exercises emitRestoreFailureEvent
// directly: it asserts the exact number of Warning/RestoreFailed events for each
// transition, mirroring the de-duplication of emitBackupFailureEvent.
func TestEmitRestoreFailureEvent_Scenario78(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		prevStatus  string
		prevJobName string
		jobName     string
		wantWarning int
	}{
		{
			name:        "new failed restore emits exactly one warning",
			status:      backupStatusFailed,
			prevStatus:  "",
			prevJobName: "",
			jobName:     "restore-a",
			wantWarning: 1,
		},
		{
			name:        "same failed restore de-duplicated",
			status:      backupStatusFailed,
			prevStatus:  backupStatusFailed,
			prevJobName: "restore-a",
			jobName:     "restore-a",
			wantWarning: 0,
		},
		{
			name:        "success->failed for new restore job emits one warning",
			status:      backupStatusFailed,
			prevStatus:  backupStatusSuccess,
			prevJobName: "restore-a",
			jobName:     "restore-b",
			wantWarning: 1,
		},
		{
			name:        "succeeded restore emits no warning",
			status:      backupStatusSuccess,
			prevStatus:  backupStatusFailed,
			prevJobName: "restore-a",
			jobName:     "restore-a",
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
			job := restoreJobWithStatus(cluster.Name, tc.jobName,
				batchv1.JobStatus{Failed: 1})

			r.emitRestoreFailureEvent(cluster, job, tc.status, tc.prevStatus, tc.prevJobName)

			events := drainEvents(recorder)
			assert.Equal(t, tc.wantWarning, countWarningRestoreFailed(events),
				"unexpected number of Warning/RestoreFailed events: %v", events)
		})
	}
}

// TestApplyBackupJobToStatus_RestoreFailedEmitsWarning_Scenario78 drives a
// FAILED restore Job through refreshBackupStatus: it must set
// LastBackupStatus=Failed AND emit exactly one Warning/RestoreFailed event and
// NO BackupFailed event (Scenario 77 stays backup-only).
func TestApplyBackupJobToStatus_RestoreFailedEmitsWarning_Scenario78(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := restoreJobWithStatus(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260101020000"),
		batchv1.JobStatus{Failed: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningRestoreFailed(events),
		"expected exactly one Warning/RestoreFailed event: %v", events)
	assert.Equal(t, 0, countWarningBackupFailed(events),
		"a restore failure must NOT emit a BackupFailed Warning: %v", events)
}

// TestApplyBackupJobToStatus_RestoreFailedDeDup_Scenario78 verifies reconciling
// the SAME failed restore Job twice emits the RestoreFailed Warning only once.
func TestApplyBackupJobToStatus_RestoreFailedDeDup_Scenario78(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := restoreJobWithStatus(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260101020000"),
		batchv1.JobStatus{Failed: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	first := drainEvents(recorder)
	require.Equal(t, 1, countWarningRestoreFailed(first))

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	second := drainEvents(recorder)
	assert.Equal(t, 0, countWarningRestoreFailed(second),
		"second reconcile of unchanged failed restore must not emit a new Warning: %v", second)
}

// TestApplyBackupJobToStatus_RestoreSucceeded_Scenario78 verifies a Succeeded
// restore Job emits NO RestoreFailed Warning.
func TestApplyBackupJobToStatus_RestoreSucceeded_Scenario78(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := restoreJobWithStatus(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260101020000"),
		batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusSuccess, cluster.Status.LastBackupStatus)
	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningRestoreFailed(events),
		"a succeeded restore must not emit a RestoreFailed Warning: %v", events)
}

// TestApplyBackupJobToStatus_BackupFailedNoRestoreEvent_Scenario78 verifies a
// FAILED BACKUP job emits a BackupFailed Warning (Scenario 77) but NO
// RestoreFailed Warning (Scenario 78d is restore-only).
func TestApplyBackupJobToStatus_BackupFailedNoRestoreEvent_Scenario78(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := failedBackupJob(cluster.Name, util.BackupJobName(cluster.Name, "20260101020000"))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingBackupMetrics{}, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, backupStatusFailed, cluster.Status.LastBackupStatus)
	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningBackupFailed(events),
		"a failed backup must emit a BackupFailed Warning: %v", events)
	assert.Equal(t, 0, countWarningRestoreFailed(events),
		"a failed backup must NOT emit a RestoreFailed Warning: %v", events)
}
