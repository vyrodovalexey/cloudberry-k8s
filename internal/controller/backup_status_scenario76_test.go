package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestBackupJobSizeHuman covers backupJobSizeHuman (Scenario 76): the BinarySI
// formatting of the avsoft.io/backup-size-bytes annotation, plus the empty-string
// best-effort fallback when the annotation is absent, zero, negative or invalid.
func TestBackupJobSizeHuman(t *testing.T) {
	t.Run("annotation present produces non-empty BinarySI", func(t *testing.T) {
		// 2516582400 bytes == 2400Mi exactly (2400 * 1024 * 1024).
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "2516582400"},
		}}
		got := backupJobSizeHuman(job)
		assert.NotEmpty(t, got)
		assert.Equal(t, "2400Mi", got)
	})

	t.Run("exact gibibyte boundary", func(t *testing.T) {
		// 1073741824 bytes == 1Gi.
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "1073741824"},
		}}
		assert.Equal(t, "1Gi", backupJobSizeHuman(job))
	})

	t.Run("no annotation returns empty", func(t *testing.T) {
		assert.Empty(t, backupJobSizeHuman(&batchv1.Job{}))
	})

	t.Run("zero bytes returns empty", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "0"},
		}}
		assert.Empty(t, backupJobSizeHuman(job))
	})

	t.Run("negative bytes returns empty", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "-100"},
		}}
		assert.Empty(t, backupJobSizeHuman(job))
	})

	t.Run("invalid annotation returns empty", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "not-a-number"},
		}}
		assert.Empty(t, backupJobSizeHuman(job))
	})
}

// TestBackupTimestampFromJob_Scenario76 covers the new timestamp resolution
// behavior: on-demand job names embedding a valid 14-digit timestamp are
// returned unchanged; CronJob-spawned jobs (whose hash suffix is not a valid
// timestamp) fall back to CompletionTime, then StartTime, formatted as a
// 14-digit YYYYMMDDHHMMSS string in UTC.
func TestBackupTimestampFromJob_Scenario76(t *testing.T) {
	cluster := backupTestCluster()

	t.Run("on-demand name with embedded 14-digit timestamp returned unchanged", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.BackupJobName(cluster.Name, "20260101020000"),
		}}
		got := backupTimestampFromJob(cluster, job)
		assert.Equal(t, "20260101020000", got)
		assert.Regexp(t, `^\d{14}$`, got)
	})

	t.Run("cronjob-spawned name with CompletionTime falls back to completion time", func(t *testing.T) {
		completion := metav1.NewTime(time.Date(2026, 1, 1, 7, 0, 0, 0, time.UTC))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: util.BackupCronJobName(cluster.Name) + "-abcde",
			},
			Status: batchv1.JobStatus{CompletionTime: &completion},
		}
		got := backupTimestampFromJob(cluster, job)
		assert.Regexp(t, `^\d{14}$`, got)
		assert.Equal(t, completion.UTC().Format("20060102150405"), got)
		assert.Equal(t, "20260101070000", got)
	})

	t.Run("name without timestamp and no CompletionTime falls back to StartTime", func(t *testing.T) {
		start := metav1.NewTime(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: util.BackupCronJobName(cluster.Name) + "-xyz99",
			},
			Status: batchv1.JobStatus{StartTime: &start},
		}
		got := backupTimestampFromJob(cluster, job)
		assert.Regexp(t, `^\d{14}$`, got)
		assert.Equal(t, start.UTC().Format("20060102150405"), got)
		assert.Equal(t, "20260203040506", got)
	})

	t.Run("CompletionTime preferred over StartTime", func(t *testing.T) {
		start := metav1.NewTime(time.Date(2026, 5, 5, 5, 5, 5, 0, time.UTC))
		completion := metav1.NewTime(time.Date(2026, 6, 6, 6, 6, 6, 0, time.UTC))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: util.BackupCronJobName(cluster.Name) + "-deff0",
			},
			Status: batchv1.JobStatus{StartTime: &start, CompletionTime: &completion},
		}
		got := backupTimestampFromJob(cluster, job)
		assert.Regexp(t, `^\d{14}$`, got)
		assert.Equal(t, "20260606060606", got)
	})

	t.Run("non-UTC completion time normalized to UTC", func(t *testing.T) {
		loc := time.FixedZone("UTC+5", 5*60*60)
		completion := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, loc))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: util.BackupCronJobName(cluster.Name) + "-zzzzz",
			},
			Status: batchv1.JobStatus{CompletionTime: &completion},
		}
		got := backupTimestampFromJob(cluster, job)
		assert.Regexp(t, `^\d{14}$`, got)
		// 12:00 at UTC+5 == 07:00 UTC.
		assert.Equal(t, "20260101070000", got)
	})

	t.Run("restore prefix with embedded timestamp returned unchanged", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.RestoreJobName(cluster.Name, "20260101070000"),
		}}
		got := backupTimestampFromJob(cluster, job)
		assert.Equal(t, "20260101070000", got)
		assert.Regexp(t, `^\d{14}$`, got)
	})
}

// TestApplyBackupJobToStatus_Scenario76 drives applyBackupJobToStatus through
// the exported refreshBackupStatus path and verifies the BackupHistoryEntry.Size
// behavior: populated when the size annotation is present, empty (best-effort)
// when absent, with all other status/history fields still set and the timestamp
// a valid 14-digit value.
func TestApplyBackupJobToStatus_Scenario76(t *testing.T) {
	t.Run("completed backup with size annotation populates history Size", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := backupTestCluster()
		start := metav1.NewTime(time.Now().Add(-3 * time.Minute))
		completion := metav1.NewTime(time.Now())
		job := backupJob(cluster.Name,
			util.BackupJobName(cluster.Name, "20260101020000"),
			util.BackupOperationBackup,
			batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
		)
		// 2516582400 bytes == 2400Mi.
		job.Annotations = map[string]string{util.AnnotationBackupSizeBytes: "2516582400"}

		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, job).Build()
		m := &countingBackupMetrics{}
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, m, nil)

		require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

		// Top-level status fields.
		assert.Equal(t, "Success", cluster.Status.LastBackupStatus)
		assert.Equal(t, "full", cluster.Status.LastBackupType)
		assert.Equal(t, job.Name, cluster.Status.LastBackupJobName)
		assert.Equal(t, "20260101020000", cluster.Status.LastBackupTimestamp)
		assert.Regexp(t, `^\d{14}$`, cluster.Status.LastBackupTimestamp)
		require.NotNil(t, cluster.Status.LastBackupTime)

		// History entry fully populated, including Size.
		require.NotEmpty(t, cluster.Status.BackupHistory)
		entry := cluster.Status.BackupHistory[len(cluster.Status.BackupHistory)-1]
		assert.Regexp(t, `^\d{14}$`, entry.Timestamp)
		assert.Equal(t, "20260101020000", entry.Timestamp)
		assert.Equal(t, "full", entry.Type)
		assert.Equal(t, "Success", entry.Status)
		assert.Equal(t, "2400Mi", entry.Size)
		assert.NotEmpty(t, entry.Size)
		assert.NotEmpty(t, entry.Duration)
	})

	t.Run("completed backup without size annotation leaves history Size empty", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := backupTestCluster()
		start := metav1.NewTime(time.Now().Add(-3 * time.Minute))
		completion := metav1.NewTime(time.Now())
		job := backupJob(cluster.Name,
			util.BackupJobName(cluster.Name, "20260101030000"),
			util.BackupOperationBackup,
			batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
		)
		// No size annotation set (best-effort empty).

		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, job).Build()
		m := &countingBackupMetrics{}
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, m, nil)

		require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

		assert.Equal(t, "Success", cluster.Status.LastBackupStatus)
		assert.Equal(t, "20260101030000", cluster.Status.LastBackupTimestamp)
		assert.Regexp(t, `^\d{14}$`, cluster.Status.LastBackupTimestamp)

		require.NotEmpty(t, cluster.Status.BackupHistory)
		entry := cluster.Status.BackupHistory[len(cluster.Status.BackupHistory)-1]
		// Best-effort: Size is empty when the size annotation is absent.
		assert.Empty(t, entry.Size)
		// Other fields still populated.
		assert.Equal(t, "20260101030000", entry.Timestamp)
		assert.Equal(t, "full", entry.Type)
		assert.Equal(t, "Success", entry.Status)
		assert.NotEmpty(t, entry.Duration)
		// The size bytes gauge is not emitted without the annotation.
		assert.Equal(t, 0, m.setBackupSizeBytes)
	})

	t.Run("cronjob-spawned backup derives 14-digit timestamp from completion time", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := backupTestCluster()
		start := metav1.NewTime(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
		completion := metav1.NewTime(time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC))
		job := backupJob(cluster.Name,
			util.BackupCronJobName(cluster.Name)+"-abcde",
			util.BackupOperationBackup,
			batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
		)
		job.Annotations = map[string]string{util.AnnotationBackupSizeBytes: "1073741824"}

		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, job).Build()
		m := &countingBackupMetrics{}
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, m, nil)

		require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

		assert.Equal(t, "Success", cluster.Status.LastBackupStatus)
		// CronJob hash name can't be parsed -> fall back to completion time.
		assert.Equal(t, "20260101020000", cluster.Status.LastBackupTimestamp)
		assert.Regexp(t, `^\d{14}$`, cluster.Status.LastBackupTimestamp)

		require.NotEmpty(t, cluster.Status.BackupHistory)
		entry := cluster.Status.BackupHistory[len(cluster.Status.BackupHistory)-1]
		assert.Regexp(t, `^\d{14}$`, entry.Timestamp)
		assert.Equal(t, "20260101020000", entry.Timestamp)
		assert.Equal(t, "1Gi", entry.Size)
	})
}
