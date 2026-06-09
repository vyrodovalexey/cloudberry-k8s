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

// retentionCluster returns an S3-backed cluster with a retention policy set.
func retentionCluster() *cbv1alpha1.CloudberryCluster {
	cluster := backupTestCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
		FullCount:        3,
		IncrementalCount: 10,
		MaxAge:           "30d",
	}
	return cluster
}

// TestEnsureRetentionCleanup_CreatesJob verifies a cleanup Job is created after
// the newest successful backup when a retention policy is set.
func TestEnsureRetentionCleanup_CreatesJob(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Succeeded: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	name := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, got))
	assert.Equal(t, util.BackupOperationCleanup, got.Labels[util.LabelBackupOperation])
}

// TestEnsureRetentionCleanup_Idempotent verifies a second call does not create a
// duplicate cleanup Job for the same latest backup timestamp.
func TestEnsureRetentionCleanup_Idempotent(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Succeeded: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))
	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	var cleanups int
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationCleanup {
			cleanups++
		}
	}
	assert.Equal(t, 1, cleanups)
}

// TestEnsureRetentionCleanup_NoPolicyNoop verifies no cleanup Job is created when
// no retention policy is configured.
func TestEnsureRetentionCleanup_NoPolicyNoop(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{}
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Succeeded: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	for i := range jobs.Items {
		assert.NotEqual(t, util.BackupOperationCleanup,
			jobs.Items[i].Labels[util.LabelBackupOperation])
	}
}

// TestEnsureRetentionCleanup_NoSucceededBackup verifies no cleanup Job is created
// when there is no successful backup Job.
func TestEnsureRetentionCleanup_NoSucceededBackup(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Failed: 1},
	)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	for i := range jobs.Items {
		assert.NotEqual(t, util.BackupOperationCleanup,
			jobs.Items[i].Labels[util.LabelBackupOperation])
	}
}

// TestEnsureRetentionCleanup_AnnotatesFromPod verifies the operator patches the
// avsoft.io/backup-retention-deleted annotation onto a Succeeded cleanup Job from
// the terminating pod's RETENTION_DELETED marker, and the metrics loop records it.
func TestEnsureRetentionCleanup_AnnotatesFromPod(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	backup := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup,
		batchv1.JobStatus{Succeeded: 1},
	)
	cleanupName := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	cleanup := backupJob(cluster.Name, cleanupName,
		util.BackupOperationCleanup, batchv1.JobStatus{Succeeded: 1})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              cleanupName + "-abcde",
			Namespace:         cluster.Namespace,
			Labels:            map[string]string{batchJobNameLabel: cleanupName},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message: "RETENTION_DELETED=2\n",
					},
				},
			}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, backup, cleanup, pod).Build()
	m := &countingBackupMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cleanupName, Namespace: cluster.Namespace}, got))
	assert.Equal(t, "2", got.Annotations[util.AnnotationBackupRetentionDeleted])

	// The existing metrics loop records the count once the annotation is set.
	require.NoError(t, r.refreshBackupStatus(context.Background(), cluster))
	assert.Equal(t, 2, m.retentionDeleted)
}

func TestParseRetentionDeletedMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    int
		ok      bool
	}{
		{"bare integer", "5", 5, true},
		{"marker", "RETENTION_DELETED=3", 3, true},
		{"marker with log tail", "some log\nRETENTION_DELETED=7\n", 7, true},
		{"empty", "", 0, false},
		{"no marker", "no count here", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := parseRetentionDeletedMessage(tc.message)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.want, n)
			}
		})
	}
}

// TestParseRetentionDeletedMessage_MarkerNoDigits covers the marker-present-but-
// no-trailing-digits branch (e.g. "RETENTION_DELETED=abc").
func TestParseRetentionDeletedMessage_MarkerNoDigits(t *testing.T) {
	n, ok := parseRetentionDeletedMessage("RETENTION_DELETED=abc\n")
	assert.False(t, ok)
	assert.Zero(t, n)
}

// TestRetentionPolicyActive covers retentionPolicyActive across nil backup,
// no-policy and each policy field.
func TestRetentionPolicyActive(t *testing.T) {
	t.Run("nil backup", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Backup = nil
		assert.False(t, retentionPolicyActive(cluster))
	})

	t.Run("no policy fields", func(t *testing.T) {
		cluster := backupTestCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{}
		assert.False(t, retentionPolicyActive(cluster))
	})

	t.Run("full count set", func(t *testing.T) {
		cluster := backupTestCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{FullCount: 1}
		assert.True(t, retentionPolicyActive(cluster))
	})

	t.Run("incremental count set", func(t *testing.T) {
		cluster := backupTestCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{IncrementalCount: 1}
		assert.True(t, retentionPolicyActive(cluster))
	})

	t.Run("max age set", func(t *testing.T) {
		cluster := backupTestCluster()
		cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{MaxAge: "30d"}
		assert.True(t, retentionPolicyActive(cluster))
	})
}

// TestEnsureRetentionCleanup_BackupDisabled verifies a no-op when backup is nil
// or disabled (no cleanup Job created, no error).
func TestEnsureRetentionCleanup_BackupDisabled(t *testing.T) {
	scheme := newTestScheme()

	t.Run("nil backup", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Backup = nil
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))
	})

	t.Run("backup disabled", func(t *testing.T) {
		cluster := retentionCluster()
		cluster.Spec.Backup.Enabled = false
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
			builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

		jobs := &batchv1.JobList{}
		require.NoError(t, k8sClient.List(context.Background(), jobs))
		assert.Empty(t, jobs.Items)
	})
}

// TestEnsureRetentionCleanup_ListError verifies a list failure is surfaced as a
// wrapped error.
func TestEnsureRetentionCleanup_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("list boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureRetentionCleanup(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing jobs for retention cleanup")
}

// TestEnsureRetentionCleanup_NoTimestamp verifies that a Succeeded backup Job
// without a derivable timestamp produces no cleanup Job (no error).
func TestEnsureRetentionCleanup_NoTimestamp(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	// A Succeeded backup Job whose name does not match the backup-name prefix and
	// whose status carries no start/completion time => no derivable timestamp.
	job := backupJob(cluster.Name, "unrelated-name",
		util.BackupOperationBackup, batchv1.JobStatus{Succeeded: 1})
	cluster.Status.LastBackupTimestamp = ""
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	for i := range jobs.Items {
		assert.NotEqual(t, util.BackupOperationCleanup,
			jobs.Items[i].Labels[util.LabelBackupOperation])
	}
}

// TestEnsureRetentionCleanup_EmitsEvent verifies a Normal event is recorded when
// the cleanup Job is created.
func TestEnsureRetentionCleanup_EmitsEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup, batchv1.JobStatus{Succeeded: 1})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	rec := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, rec,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRetentionCleanup(context.Background(), cluster))

	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, "Retention cleanup Job created")
	default:
		t.Fatal("expected a Normal event for cleanup Job creation")
	}
}

// TestCreateRetentionCleanupJob_GetError verifies a non-NotFound Get failure is
// surfaced as a wrapped error.
func TestCreateRetentionCleanupJob_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("get boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.createRetentionCleanupJob(context.Background(), cluster, "20260101020000")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting retention cleanup job")
}

// TestCreateRetentionCleanupJob_CreateError verifies a non-AlreadyExists Create
// failure is surfaced as a wrapped error.
func TestCreateRetentionCleanupJob_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.createRetentionCleanupJob(context.Background(), cluster, "20260101020000")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating retention cleanup job")
}

// TestEnsureRetentionCleanup_CreateErrorPropagates verifies a cleanup-Job create
// failure surfaced through ensureRetentionCleanup is returned to the caller.
func TestEnsureRetentionCleanup_CreateErrorPropagates(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	job := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup, batchv1.JobStatus{Succeeded: 1})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureRetentionCleanup(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating retention cleanup job")
}

// TestCreateRetentionCleanupJob_AlreadyExists verifies an AlreadyExists Create
// race is treated as a successful no-op.
func TestCreateRetentionCleanupJob_AlreadyExists(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName())
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.createRetentionCleanupJob(
		context.Background(), cluster, "20260101020000"))
}

// TestReconcileRetentionCleanupAnnotations_AlreadyAnnotated verifies a cleanup
// Job that already carries the annotation is not re-patched (idempotent).
func TestReconcileRetentionCleanupAnnotations_AlreadyAnnotated(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	cleanupName := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	cleanup := backupJob(cluster.Name, cleanupName,
		util.BackupOperationCleanup, batchv1.JobStatus{Succeeded: 1})
	cleanup.Annotations = map[string]string{
		util.AnnotationBackupRetentionDeleted: "7",
	}
	// A pod with a different count must be ignored because the Job is already
	// annotated (idempotent skip).
	pod := cleanupPod(cluster.Namespace, cleanupName, "RETENTION_DELETED=99\n")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, cleanup, pod).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileRetentionCleanupAnnotations(
		context.Background(), cluster, []batchv1.Job{*cleanup}))

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cleanupName, Namespace: cluster.Namespace}, got))
	assert.Equal(t, "7", got.Annotations[util.AnnotationBackupRetentionDeleted])
}

// TestReconcileRetentionCleanupAnnotations_NotRecoverable verifies that a
// Succeeded cleanup Job with no recoverable count is skipped without error and
// without an annotation patch.
func TestReconcileRetentionCleanupAnnotations_NotRecoverable(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	cleanupName := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	cleanup := backupJob(cluster.Name, cleanupName,
		util.BackupOperationCleanup, batchv1.JobStatus{Succeeded: 1})
	// No pod => count not recoverable.
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, cleanup).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileRetentionCleanupAnnotations(
		context.Background(), cluster, []batchv1.Job{*cleanup}))

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cleanupName, Namespace: cluster.Namespace}, got))
	_, ok := got.Annotations[util.AnnotationBackupRetentionDeleted]
	assert.False(t, ok)
}

// TestReconcileRetentionCleanupAnnotations_PatchError verifies that a patch
// failure is non-fatal (logged and skipped, no error returned).
func TestReconcileRetentionCleanupAnnotations_PatchError(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	cleanupName := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	cleanup := backupJob(cluster.Name, cleanupName,
		util.BackupOperationCleanup, batchv1.JobStatus{Succeeded: 1})
	pod := cleanupPod(cluster.Namespace, cleanupName, "RETENTION_DELETED=2\n")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, cleanup, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return fmt.Errorf("patch boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Non-fatal: the patch failure is logged, not returned.
	require.NoError(t, r.reconcileRetentionCleanupAnnotations(
		context.Background(), cluster, []batchv1.Job{*cleanup}))
}

// TestReconcileRetentionCleanupAnnotations_SkipsNonCleanupAndPending verifies the
// loop skips non-cleanup Jobs and not-yet-succeeded cleanup Jobs.
func TestReconcileRetentionCleanupAnnotations_SkipsNonCleanupAndPending(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	backup := backupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationBackup, batchv1.JobStatus{Succeeded: 1})
	pendingCleanup := backupJob(cluster.Name,
		util.RetentionCleanupJobName(cluster.Name, "20260101020000"),
		util.BackupOperationCleanup, batchv1.JobStatus{Active: 1})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, backup, pendingCleanup).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileRetentionCleanupAnnotations(
		context.Background(), cluster, []batchv1.Job{*backup, *pendingCleanup}))

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: pendingCleanup.Name, Namespace: cluster.Namespace}, got))
	_, ok := got.Annotations[util.AnnotationBackupRetentionDeleted]
	assert.False(t, ok)
}

// TestReadRetentionDeletedCount_ListError verifies a pod list error yields
// (0,false) without panicking (non-fatal).
func TestReadRetentionDeletedCount_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := retentionCluster()
	cleanupName := util.RetentionCleanupJobName(cluster.Name, "20260101020000")
	cleanup := backupJob(cluster.Name, cleanupName,
		util.BackupOperationCleanup, batchv1.JobStatus{Succeeded: 1})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, cleanup).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("list boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	n, ok := r.readRetentionDeletedCount(context.Background(), cluster, cleanup)
	assert.False(t, ok)
	assert.Zero(t, n)
}

// TestRetentionDeletedFromPod covers the running (no terminated state),
// empty-message and no-marker branches in addition to the success path.
func TestRetentionDeletedFromPod(t *testing.T) {
	t.Run("no terminated state", func(t *testing.T) {
		pod := &corev1.Pod{Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "gpbackman",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		}}
		n, ok := retentionDeletedFromPod(pod)
		assert.False(t, ok)
		assert.Zero(t, n)
	})

	t.Run("empty terminated message", func(t *testing.T) {
		pod := &corev1.Pod{Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: ""},
				},
			}},
		}}
		n, ok := retentionDeletedFromPod(pod)
		assert.False(t, ok)
		assert.Zero(t, n)
	})

	t.Run("terminated message without marker", func(t *testing.T) {
		pod := &corev1.Pod{Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: "no count here"},
				},
			}},
		}}
		n, ok := retentionDeletedFromPod(pod)
		assert.False(t, ok)
		assert.Zero(t, n)
	})

	t.Run("terminated message with marker", func(t *testing.T) {
		pod := &corev1.Pod{Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: "RETENTION_DELETED=4\n"},
				},
			}},
		}}
		n, ok := retentionDeletedFromPod(pod)
		assert.True(t, ok)
		assert.Equal(t, 4, n)
	})
}

// cleanupPod builds a cleanup pod fixture with a terminated gpbackman container
// carrying the given message, labeled with the owning Job name.
func cleanupPod(namespace, jobName, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              jobName + "-xyz12",
			Namespace:         namespace,
			Labels:            map[string]string{batchJobNameLabel: jobName},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: message},
				},
			}},
		},
	}
}
