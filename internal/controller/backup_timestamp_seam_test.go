package controller

// Tests for the real-gpbackup-timestamp capture seam (handoff task T-13):
// backupRealTimestampFromPod, readBackupRealTimestamp,
// patchBackupTimestampAnnotation and the reconcileBackupTimestampAnnotations
// driver — previously the largest contiguous untested area in the package.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

const btTestTimestamp = "20260315041500"

// terminatedPod builds a pod carrying the given terminated-container message,
// labeled to belong to jobName.
func terminatedPod(namespace, name, jobName, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{batchJobNameLabel: jobName},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: message},
				},
			}},
		},
	}
}

func TestBackupRealTimestampFromPod(t *testing.T) {
	tests := []struct {
		name   string
		pod    *corev1.Pod
		wantTS string
		wantOK bool
	}{
		{
			name: "terminated container with valid marker",
			pod: terminatedPod("default", "p", "j",
				"gpbackup finished\nBACKUP_TIMESTAMP="+btTestTimestamp+"\ntail"),
			wantTS: btTestTimestamp,
			wantOK: true,
		},
		{
			name:   "marker with non-14-digit timestamp rejected",
			pod:    terminatedPod("default", "p", "j", "BACKUP_TIMESTAMP=20260101"),
			wantOK: false,
		},
		{
			name:   "no marker in message",
			pod:    terminatedPod("default", "p", "j", "just logs, no marker"),
			wantOK: false,
		},
		{
			name:   "terminated with empty message",
			pod:    terminatedPod("default", "p", "j", ""),
			wantOK: false,
		},
		{
			name: "running container is skipped",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}},
				},
			},
			wantOK: false,
		},
		{
			name: "second container carries the marker",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
						{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							Message: "BACKUP_TIMESTAMP=" + btTestTimestamp,
						}}},
					},
				},
			},
			wantTS: btTestTimestamp,
			wantOK: true,
		},
		{
			name:   "no container statuses at all",
			pod:    &corev1.Pod{},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, ok := backupRealTimestampFromPod(tt.pod)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantTS, ts)
		})
	}
}

// newBackupTimestampEnv builds an AdminReconciler over the given objects.
func newBackupTimestampEnv(
	objects []client.Object,
	funcs interceptor.Funcs,
) (*AdminReconciler, client.Client) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithInterceptorFuncs(funcs).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
	return r, k8sClient
}

// backupOpJob builds a Succeeded backup-operation Job.
func backupOpJob(cluster *cbv1alpha1.CloudberryCluster, name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}
}

func TestReadBackupRealTimestamp(t *testing.T) {
	cluster := newTestCluster()
	job := backupOpJob(cluster, "test-cluster-backup-"+btTestTimestamp)

	t.Run("recovers marker from the job pod", func(t *testing.T) {
		pod := terminatedPod(cluster.Namespace, "backup-pod", job.Name,
			"BACKUP_TIMESTAMP="+btTestTimestamp)
		r, _ := newBackupTimestampEnv([]client.Object{cluster, job, pod}, interceptor.Funcs{})

		ts, ok := r.readBackupRealTimestamp(context.Background(), cluster, job)
		require.True(t, ok)
		assert.Equal(t, btTestTimestamp, ts)
	})

	t.Run("pod list failure is non-fatal and reports not-found", func(t *testing.T) {
		r, _ := newBackupTimestampEnv([]client.Object{cluster, job}, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch,
				list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return fmt.Errorf("pod list boom")
				}
				return c.List(ctx, list, opts...)
			},
		})

		ts, ok := r.readBackupRealTimestamp(context.Background(), cluster, job)
		assert.False(t, ok)
		assert.Empty(t, ts)
	})

	t.Run("pod without marker reports not-found", func(t *testing.T) {
		pod := terminatedPod(cluster.Namespace, "backup-pod", job.Name, "no marker here")
		r, _ := newBackupTimestampEnv([]client.Object{cluster, job, pod}, interceptor.Funcs{})

		_, ok := r.readBackupRealTimestamp(context.Background(), cluster, job)
		assert.False(t, ok)
	})

	t.Run("no pods at all reports not-found", func(t *testing.T) {
		r, _ := newBackupTimestampEnv([]client.Object{cluster, job}, interceptor.Funcs{})

		_, ok := r.readBackupRealTimestamp(context.Background(), cluster, job)
		assert.False(t, ok)
	})
}

func TestPatchBackupTimestampAnnotation(t *testing.T) {
	t.Run("persists the annotation (nil annotations map)", func(t *testing.T) {
		cluster := newTestCluster()
		job := backupOpJob(cluster, "test-cluster-backup-x")
		r, k8sClient := newBackupTimestampEnv([]client.Object{cluster, job}, interceptor.Funcs{})

		require.NoError(t, r.patchBackupTimestampAnnotation(
			context.Background(), job, btTestTimestamp))

		got := &batchv1.Job{}
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, got))
		assert.Equal(t, btTestTimestamp, got.Annotations[util.AnnotationBackupTimestamp],
			"the real gpbackup timestamp must be persisted on the Job")
	})

	t.Run("patch failure is surfaced with the job name", func(t *testing.T) {
		cluster := newTestCluster()
		job := backupOpJob(cluster, "test-cluster-backup-x")
		r, _ := newBackupTimestampEnv([]client.Object{cluster, job}, interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch,
				obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return fmt.Errorf("job patch boom")
				}
				return fmt.Errorf("unexpected patch")
			},
		})

		err := r.patchBackupTimestampAnnotation(context.Background(), job, btTestTimestamp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "patching backup job test-cluster-backup-x")
	})
}

func TestReconcileBackupTimestampAnnotations(t *testing.T) {
	t.Run("annotates the succeeded backup job from the pod marker", func(t *testing.T) {
		cluster := newTestCluster()
		job := backupOpJob(cluster, "test-cluster-backup-a")
		pod := terminatedPod(cluster.Namespace, "backup-pod", job.Name,
			"log tail\nBACKUP_TIMESTAMP="+btTestTimestamp)
		r, k8sClient := newBackupTimestampEnv(
			[]client.Object{cluster, job, pod}, interceptor.Funcs{})

		r.reconcileBackupTimestampAnnotations(context.Background(), cluster,
			[]batchv1.Job{*job})

		got := &batchv1.Job{}
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, got))
		assert.Equal(t, btTestTimestamp, got.Annotations[util.AnnotationBackupTimestamp])
	})

	t.Run("skips non-backup, unfinished, already-annotated and marker-less jobs", func(t *testing.T) {
		cluster := newTestCluster()

		restoreJob := backupOpJob(cluster, "test-cluster-restore-b")
		restoreJob.Labels[util.LabelBackupOperation] = util.BackupOperationRestore

		runningJob := backupOpJob(cluster, "test-cluster-backup-running")
		runningJob.Status.Succeeded = 0

		annotated := backupOpJob(cluster, "test-cluster-backup-done")
		annotated.Annotations = map[string]string{
			util.AnnotationBackupTimestamp: btTestTimestamp,
		}

		markerless := backupOpJob(cluster, "test-cluster-backup-nomarker")

		r, k8sClient := newBackupTimestampEnv(
			[]client.Object{cluster, restoreJob, runningJob, annotated, markerless},
			interceptor.Funcs{})

		r.reconcileBackupTimestampAnnotations(context.Background(), cluster,
			[]batchv1.Job{*restoreJob, *runningJob, *annotated, *markerless})

		for _, name := range []string{restoreJob.Name, runningJob.Name, markerless.Name} {
			got := &batchv1.Job{}
			require.NoError(t, k8sClient.Get(context.Background(),
				types.NamespacedName{Name: name, Namespace: cluster.Namespace}, got))
			assert.Empty(t, got.Annotations[util.AnnotationBackupTimestamp],
				"job %s must stay un-annotated", name)
		}
	})

	t.Run("patch failure is logged and non-fatal", func(t *testing.T) {
		cluster := newTestCluster()
		job := backupOpJob(cluster, "test-cluster-backup-c")
		pod := terminatedPod(cluster.Namespace, "backup-pod", job.Name,
			"BACKUP_TIMESTAMP="+btTestTimestamp)
		r, k8sClient := newBackupTimestampEnv(
			[]client.Object{cluster, job, pod}, interceptor.Funcs{
				Patch: func(_ context.Context, _ client.WithWatch,
					obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
					if _, ok := obj.(*batchv1.Job); ok {
						return fmt.Errorf("job patch boom")
					}
					return fmt.Errorf("unexpected patch")
				},
			})

		assert.NotPanics(t, func() {
			r.reconcileBackupTimestampAnnotations(context.Background(), cluster,
				[]batchv1.Job{*job})
		}, "a failed annotation patch must never break reconciliation")

		got := &batchv1.Job{}
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, got))
		assert.Empty(t, got.Annotations[util.AnnotationBackupTimestamp])
	})
}
