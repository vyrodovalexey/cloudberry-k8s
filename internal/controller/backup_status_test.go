package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// countingBackupMetrics embeds NoopRecorder and counts only the backup/restore
// metric calls exercised by the backup status derivation paths. It lets tests
// assert which metric was emitted for a given Job status without re-implementing
// the full metrics.Recorder interface.
type countingBackupMetrics struct {
	metrics.NoopRecorder

	recordBackup          int
	recordRestore         int
	setBackupLastStatus   []float64
	lastSuccessTimestamp  int
	observeBackupDuration int
	observeRestoreDur     int
	setBackupJobStatus    []float64
	retentionDeleted      int
	setBackupSizeBytes    int
}

func (m *countingBackupMetrics) RecordBackup(_, _, _, _ string) { m.recordBackup++ }

func (m *countingBackupMetrics) RecordRestore(_, _, _ string) { m.recordRestore++ }

func (m *countingBackupMetrics) SetBackupLastStatus(_, _ string, status float64) {
	m.setBackupLastStatus = append(m.setBackupLastStatus, status)
}

func (m *countingBackupMetrics) SetBackupLastSuccessTimestamp(_, _ string, _ float64) {
	m.lastSuccessTimestamp++
}

func (m *countingBackupMetrics) ObserveBackupDuration(_, _, _ string, _ time.Duration) {
	m.observeBackupDuration++
}

func (m *countingBackupMetrics) ObserveRestoreDuration(_, _ string, _ time.Duration) {
	m.observeRestoreDur++
}

func (m *countingBackupMetrics) SetBackupJobStatus(_, _, _, _ string, status float64) {
	m.setBackupJobStatus = append(m.setBackupJobStatus, status)
}

func (m *countingBackupMetrics) RecordBackupRetentionDeleted(_, _ string, n int) {
	m.retentionDeleted += n
}

func (m *countingBackupMetrics) SetBackupSizeBytes(_, _, _ string, _ float64) {
	m.setBackupSizeBytes++
}

// backupTestCluster returns a Running cluster with an S3-backed BackupSpec.
func backupTestCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
		},
		Image: "cloudberry-backup:2.1.0",
	}
	return cluster
}

// backupJob builds a backup-operation Job fixture with the given status and labels.
func backupJob(cluster, name, operation string, status batchv1.JobStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: operation,
			},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Status: status,
	}
}

// TestEnsureBackupS3ConfigMap_Create verifies the S3 ConfigMap is created for an
// S3 destination when it does not yet exist.
func TestEnsureBackupS3ConfigMap_Create(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3ConfigMap(context.Background(), cluster))

	cm := &corev1.ConfigMap{}
	name := util.BackupS3ConfigMapName(cluster.Name)
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, cm))
	assert.NotEmpty(t, cm.Data)
}

// TestEnsureBackupS3ConfigMap_NonS3 verifies a local destination is a no-op.
func TestEnsureBackupS3ConfigMap_NonS3(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Destination = cbv1alpha1.BackupDestination{Type: "local"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3ConfigMap(context.Background(), cluster))

	cms := &corev1.ConfigMapList{}
	require.NoError(t, k8sClient.List(context.Background(), cms))
	assert.Empty(t, cms.Items)
}

// TestEnsureBackupS3ConfigMap_Update verifies an existing ConfigMap with stale
// data is updated in place.
func TestEnsureBackupS3ConfigMap_Update(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	stale := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupS3ConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{"stale": "value"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, stale).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3ConfigMap(context.Background(), cluster))

	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, cm))
	_, hadStale := cm.Data["stale"]
	assert.False(t, hadStale, "stale data should be replaced")
}

// TestEnsureBackupCronJob_Create verifies a CronJob is created and the status
// CronJobName is recorded when a schedule is configured.
func TestEnsureBackupCronJob_Create(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupCronJob(context.Background(), cluster))

	cron := &batchv1.CronJob{}
	name := util.BackupCronJobName(cluster.Name)
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, cron))
	assert.Equal(t, name, cluster.Status.CronJobName)
	assert.Equal(t, "0 2 * * *", cron.Spec.Schedule)
}

// TestEnsureBackupCronJob_Update verifies an existing CronJob with a changed
// schedule is updated in place.
func TestEnsureBackupCronJob_Update(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 5 * * *"
	stale := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 1 * * *"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, stale).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupCronJob(context.Background(), cluster))

	cron := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, cron))
	assert.Equal(t, "0 5 * * *", cron.Spec.Schedule)
	assert.Equal(t, stale.Name, cluster.Status.CronJobName)
}

// TestEnsureBackupCronJob_DeleteWhenScheduleCleared verifies the CronJob is
// deleted and the status cleared when the schedule is removed.
func TestEnsureBackupCronJob_DeleteWhenScheduleCleared(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "" // cleared
	cluster.Status.CronJobName = util.BackupCronJobName(cluster.Name)
	existing := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 2 * * *"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, existing).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupCronJob(context.Background(), cluster))

	cron := &batchv1.CronJob{}
	err := k8sClient.Get(context.Background(),
		types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, cron)
	assert.Error(t, err, "cronjob should be deleted")
	assert.Empty(t, cluster.Status.CronJobName)
}

// TestEnsureBackupCronJob_NoScheduleNoCronJob verifies a cleared schedule with
// no existing CronJob is a no-op that clears the status.
func TestEnsureBackupCronJob_NoScheduleNoCronJob(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = ""
	cluster.Status.CronJobName = "leftover"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupCronJob(context.Background(), cluster))
	assert.Empty(t, cluster.Status.CronJobName)
}

// TestRefreshBackupStatus_SuccessfulBackup verifies status fields, history and
// success metrics are populated from a succeeded backup Job.
func TestRefreshBackupStatus_SuccessfulBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
	)
	job.Annotations = map[string]string{util.AnnotationBackupSizeBytes: "1024"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, "Success", cluster.Status.LastBackupStatus)
	assert.Contains(t, cluster.Status.LastBackupTimestamp, "20260101020000")
	assert.Equal(t, "full", cluster.Status.LastBackupType)
	assert.Equal(t, job.Name, cluster.Status.LastBackupJobName)
	require.NotEmpty(t, cluster.Status.BackupHistory)
	assert.Equal(t, "Success", cluster.Status.BackupHistory[0].Status)

	assert.Equal(t, 1, m.recordBackup)
	assert.Equal(t, 1, m.lastSuccessTimestamp)
	assert.Equal(t, 1, m.observeBackupDuration)
	assert.Equal(t, 1, m.setBackupSizeBytes)
	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusSuccess, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])
	require.Contains(t, m.setBackupJobStatus, backupJobStatusSucceeded)
}

// TestRefreshBackupStatus_FailedBackup verifies failed-status handling.
func TestRefreshBackupStatus_FailedBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true}
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101030000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Failed: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, "Failed", cluster.Status.LastBackupStatus)
	assert.Equal(t, "incremental", cluster.Status.LastBackupType)
	assert.Equal(t, 1, m.recordBackup)
	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusFailed, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])
	require.Contains(t, m.setBackupJobStatus, backupJobStatusFailed)
}

// TestRefreshBackupStatus_InProgressBackup verifies running-status handling.
func TestRefreshBackupStatus_InProgressBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101040000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Active: 1, StartTime: &start},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, "InProgress", cluster.Status.LastBackupStatus)
	require.NotEmpty(t, m.setBackupLastStatus)
	assert.Equal(t, backupLastStatusInProgress, m.setBackupLastStatus[len(m.setBackupLastStatus)-1])
	require.Contains(t, m.setBackupJobStatus, backupJobStatusRunning)
}

// TestRefreshBackupStatus_RestoreJob verifies restore metrics and duration are
// recorded from a succeeded restore Job.
func TestRefreshBackupStatus_RestoreJob(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260101050000"),
		util.BackupOperationRestore,
		batchv1.JobStatus{Succeeded: 1, StartTime: &start, CompletionTime: &completion},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, "Success", cluster.Status.LastBackupStatus)
	assert.Equal(t, 1, m.recordRestore)
	assert.Equal(t, 1, m.observeRestoreDur)
	// A succeeded restore must not emit a backup counter.
	assert.Equal(t, 0, m.recordBackup)
}

// TestRefreshBackupStatus_CleanupRetention verifies retention deletions are
// recorded for a succeeded cleanup Job.
func TestRefreshBackupStatus_CleanupRetention(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := backupJob(cluster.Name,
		util.RetentionCleanupJobName(cluster.Name, "20260101060000"),
		util.BackupOperationCleanup,
		batchv1.JobStatus{Succeeded: 1},
	)
	job.Annotations = map[string]string{util.AnnotationBackupRetentionDeleted: "3"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))

	assert.Equal(t, 3, m.retentionDeleted)
	// Cleanup jobs do not influence LastBackup* fields.
	assert.Empty(t, cluster.Status.LastBackupStatus)
}

// TestRefreshBackupStatus_NoJobs verifies no status changes occur with no Jobs.
func TestRefreshBackupStatus_NoJobs(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	assert.Empty(t, cluster.Status.LastBackupStatus)
	assert.Empty(t, m.setBackupJobStatus)
}

// TestReconcileBackup_LocalNoS3ConfigMap verifies a local destination does not
// create an S3 ConfigMap during a full reconcile.
func TestReconcileBackup_LocalNoS3ConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Destination = cbv1alpha1.BackupDestination{Type: "local"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileBackup(context.Background(), cluster))

	cms := &corev1.ConfigMapList{}
	require.NoError(t, k8sClient.List(context.Background(), cms))
	assert.Empty(t, cms.Items)
}

// TestReconcileBackup_ScheduleCreatesCronJob verifies a full reconcile with a
// schedule set creates the CronJob and records it in status.
func TestReconcileBackup_ScheduleCreatesCronJob(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 3 * * *"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileBackup(context.Background(), cluster))
	assert.Equal(t, util.BackupCronJobName(cluster.Name), cluster.Status.CronJobName)
}

// TestAppendBackupHistory_TrimAndDedup verifies appendBackupHistory dedups by
// timestamp and trims to the limit.
func TestAppendBackupHistory_TrimAndDedup(t *testing.T) {
	var history []cbv1alpha1.BackupHistoryEntry
	for i := 0; i < backupHistoryLimit+5; i++ {
		history = appendBackupHistory(history, cbv1alpha1.BackupHistoryEntry{
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).Format("20060102150405"),
			Status:    "Success",
		})
	}
	assert.Len(t, history, backupHistoryLimit)

	// Re-appending an existing timestamp should not grow the slice.
	dup := history[1]
	out := appendBackupHistory(history, dup)
	assert.LessOrEqual(t, len(out), backupHistoryLimit)
	assert.Equal(t, dup.Timestamp, out[0].Timestamp)
}

// TestBackupJobHelpers covers the small derivation helpers directly across edge
// cases (empty/invalid annotations, no times, label-derived prefixes).
func TestBackupJobHelpers(t *testing.T) {
	cluster := backupTestCluster()

	t.Run("size bytes invalid and missing", func(t *testing.T) {
		assert.Zero(t, backupJobSizeBytes(&batchv1.Job{}))
		bad := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "nope"}}}
		assert.Zero(t, backupJobSizeBytes(bad))
		neg := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "-1"}}}
		assert.Zero(t, backupJobSizeBytes(neg))
		ok := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupSizeBytes: "2048"}}}
		assert.InDelta(t, 2048.0, backupJobSizeBytes(ok), 0.01)
	})

	t.Run("retention deleted invalid and missing", func(t *testing.T) {
		assert.Zero(t, backupRetentionDeletedCount(&batchv1.Job{}))
		bad := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupRetentionDeleted: "x"}}}
		assert.Zero(t, backupRetentionDeletedCount(bad))
		ok := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{util.AnnotationBackupRetentionDeleted: "5"}}}
		assert.Equal(t, 5, backupRetentionDeletedCount(ok))
	})

	t.Run("timestamp fallback to status", func(t *testing.T) {
		cluster.Status.LastBackupTimestamp = "fallbackts"
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "unrelated-job"}}
		assert.Equal(t, "fallbackts", backupTimestampFromJob(cluster, job))
	})

	t.Run("timestamp from restore prefix", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.RestoreJobName(cluster.Name, "20260101070000")}}
		assert.Contains(t, backupTimestampFromJob(cluster, job), "20260101070000")
	})

	t.Run("duration zero when missing times", func(t *testing.T) {
		assert.Equal(t, "", backupJobDuration(&batchv1.Job{}))
		assert.Zero(t, backupJobDurationValue(&batchv1.Job{}))
	})

	t.Run("status code pending", func(t *testing.T) {
		assert.Equal(t, backupJobStatusPending, backupJobStatusCode(&batchv1.Job{}))
	})

	t.Run("latest nil when no backup jobs", func(t *testing.T) {
		jobs := []batchv1.Job{*backupJob(cluster.Name, "c", util.BackupOperationCleanup,
			batchv1.JobStatus{Succeeded: 1})}
		assert.Nil(t, latestBackupJob(jobs))
	})
}

// notFoundErr returns an apierrors NotFound for the given resource so that
// interceptor Get hooks can simulate a missing object.
func notFoundErr(resource, name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Resource: resource}, name)
}

// TestEnsureBackupS3ConfigMap_CreateError verifies create errors are wrapped.
func TestEnsureBackupS3ConfigMap_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupS3ConfigMap(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating backup s3 configmap")
}

// TestEnsureBackupS3ConfigMap_GetError verifies non-NotFound Get errors propagate.
func TestEnsureBackupS3ConfigMap_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("get boom")
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupS3ConfigMap(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting backup s3 configmap")
}

// TestEnsureBackupCronJob_CreateError verifies create errors are wrapped.
func TestEnsureBackupCronJob_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating backup cronjob")
}

// TestEnsureBackupCronJob_GetError verifies non-NotFound Get errors propagate.
func TestEnsureBackupCronJob_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 2 * * *"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return fmt.Errorf("get boom")
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting backup cronjob")
}

// TestEnsureBackupCronJob_DeleteGetError verifies the delete branch wraps
// non-NotFound Get errors.
func TestEnsureBackupCronJob_DeleteGetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "" // triggers the delete branch
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return fmt.Errorf("get boom")
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting backup cronjob")
}

// TestEnsureBackupCronJob_DeleteError verifies the delete branch wraps Delete
// errors that are not NotFound.
func TestEnsureBackupCronJob_DeleteError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = ""
	existing := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(cluster.Name),
			Namespace: cluster.Namespace,
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
				return fmt.Errorf("delete boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleting backup cronjob")
}

// TestEnsureBackupCronJob_UpdateError verifies update errors are wrapped.
func TestEnsureBackupCronJob_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Schedule = "0 9 * * *"
	stale := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 1 * * *"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, stale).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
				return fmt.Errorf("update boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureBackupCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating backup cronjob")
}

// TestRefreshBackupStatus_ListError verifies list errors are wrapped.
func TestRefreshBackupStatus_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("list boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.refreshBackupStatus(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing backup jobs")
}

// TestReconcileBackup_PropagatesErrors verifies reconcileBackup surfaces errors
// from its sub-steps (here: the S3 ConfigMap ensure).
func TestReconcileBackup_PropagatesErrors(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.reconcileBackup(context.Background(), cluster)
	require.Error(t, err)
}

// TestCreateValidationJob_GetError verifies non-NotFound Get errors are wrapped.
func TestCreateValidationJob_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return fmt.Errorf("get boom")
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.createValidationJob(context.Background(), cluster, "20260101080000", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting validation job")
}

// TestCreateValidationJob_AlreadyExistsOnCreate verifies an AlreadyExists error
// from Create is treated as a no-op (idempotent under races).
func TestCreateValidationJob_AlreadyExistsOnCreate(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	name := util.PostRestoreValidationJobName(cluster.Name, "20260101090000")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return notFoundErr("jobs", name)
				}
				return nil
			},
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "jobs"}, name)
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.createValidationJob(context.Background(), cluster, "20260101090000", nil))
}

// TestEnsurePostRestoreValidation_EmptyTimestampSkipped verifies a succeeded
// restore Job whose name carries no timestamp suffix is skipped (no validation
// Job created).
func TestEnsurePostRestoreValidation_EmptyTimestampSkipped(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	// Name exactly equals the restore prefix => trimmed timestamp is empty.
	restoreJob := backupJob(cluster.Name,
		util.RestoreJobName(cluster.Name, ""),
		util.BackupOperationRestore,
		batchv1.JobStatus{Succeeded: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, restoreJob).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePostRestoreValidation(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	for i := range jobs.Items {
		assert.NotEqual(t, util.BackupOperationValidate,
			jobs.Items[i].Labels[util.LabelBackupOperation])
	}
}

// TestHandleConfigError verifies the config-error handler sets the failure
// condition, attempts a status patch and returns the original error with a
// requeue. It also exercises the status-patch error logging branch.
func TestHandleConfigError(t *testing.T) {
	t.Run("status patch succeeds", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster).WithStatusSubresource(cluster).Build()
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

		boom := fmt.Errorf("config boom")
		res, err := r.handleConfigError(context.Background(), util.LoggerFromContext(context.Background()), cluster, boom)
		require.Error(t, err)
		assert.Equal(t, boom, err)
		assert.NotZero(t, res.RequeueAfter)

		found := false
		for _, c := range cluster.Status.Conditions {
			if c.Type == string(cbv1alpha1.ConditionConfigApplied) {
				found = true
				assert.Equal(t, "False", string(c.Status))
			}
		}
		assert.True(t, found)
	})

	t.Run("status patch error is logged", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster).WithStatusSubresource(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourcePatch: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
					return fmt.Errorf("patch boom")
				},
			}).Build()
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

		boom := fmt.Errorf("config boom")
		_, err := r.handleConfigError(context.Background(), util.LoggerFromContext(context.Background()), cluster, boom)
		require.Error(t, err)
		assert.Equal(t, boom, err)
	})
}

// TestStatefulSetNameForPhase covers every branch of the phase->StatefulSet
// resolver, including the disabled-mirror/standby and default cases.
func TestStatefulSetNameForPhase(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	cluster := newTestCluster()
	assert.Equal(t, util.SegmentPrimaryName(cluster.Name),
		r.statefulSetNameForPhase(cluster, "primaries"))
	assert.Equal(t, util.CoordinatorName(cluster.Name),
		r.statefulSetNameForPhase(cluster, "coordinator"))
	assert.Empty(t, r.statefulSetNameForPhase(cluster, "mirrors"))
	assert.Empty(t, r.statefulSetNameForPhase(cluster, "standby"))
	assert.Empty(t, r.statefulSetNameForPhase(cluster, "unknown"))

	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	assert.Equal(t, util.SegmentMirrorName(cluster.Name),
		r.statefulSetNameForPhase(cluster, "mirrors"))
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	assert.Equal(t, util.StandbyName(cluster.Name),
		r.statefulSetNameForPhase(cluster, "standby"))
}
