package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clientpkg "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// restoreJobNamed returns a Succeeded restore-operation Job.
func restoreJobNamed(name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				util.LabelCluster:         "test-cluster",
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}
}

// restorePodFor returns a terminated pod for the Job carrying the given
// termination message.
func restorePodFor(jobName, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: "default",
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

func TestParseRestorePartialMessage(t *testing.T) {
	tests := []struct {
		name       string
		message    string
		wantDetail string
		wantOK     bool
	}{
		{name: "bare marker defaults to stats", message: "GPRESTORE_PARTIAL=", wantDetail: "stats", wantOK: true},
		{name: "marker with detail", message: "GPRESTORE_PARTIAL=stats", wantDetail: "stats", wantOK: true},
		{name: "marker embedded in log tail", message: "restore done\nGPRESTORE_PARTIAL=stats\n", wantDetail: "stats", wantOK: true},
		{name: "detail stops at non-token rune", message: "GPRESTORE_PARTIAL=stats; done", wantDetail: "stats", wantOK: true},
		{name: "no marker", message: "restore completed cleanly", wantOK: false},
		{name: "empty message", message: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			detail, ok := parseRestorePartialMessage(tc.message)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.wantDetail, detail)
			}
		})
	}
}

func TestRestorePartialFromPod(t *testing.T) {
	t.Run("terminated container with marker", func(t *testing.T) {
		pod := restorePodFor("job-a", "GPRESTORE_PARTIAL=stats")
		detail, ok := restorePartialFromPod(pod)
		assert.True(t, ok)
		assert.Equal(t, "stats", detail)
	})

	t.Run("running container ignored", func(t *testing.T) {
		pod := restorePodFor("job-a", "")
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}
		_, ok := restorePartialFromPod(pod)
		assert.False(t, ok)
	})

	t.Run("terminated without marker", func(t *testing.T) {
		pod := restorePodFor("job-a", "all good")
		_, ok := restorePartialFromPod(pod)
		assert.False(t, ok)
	})
}

func TestRestoreMetricStatus(t *testing.T) {
	plain := restoreJobNamed("test-cluster-restore-20260101020000")
	assert.Equal(t, "success", restoreMetricStatus(plain, backupStatusSuccess))
	assert.Equal(t, "failed", restoreMetricStatus(plain, backupStatusFailed))

	partial := restoreJobNamed("test-cluster-restore-20260101020000")
	partial.Annotations = map[string]string{util.AnnotationRestorePartial: "stats"}
	assert.Equal(t, restoreStatusPartial, restoreMetricStatus(partial, backupStatusSuccess))
	// A FAILED job is never reported partial, even with the annotation.
	assert.Equal(t, "failed", restoreMetricStatus(partial, backupStatusFailed))
}

func TestReconcileRestorePartialAnnotations_PatchesAndEmitsOnce(t *testing.T) {
	cluster := newTestCluster()
	job := restoreJobNamed("test-cluster-restore-20260101020000")
	pod := restorePodFor(job.Name, "data restored\nGPRESTORE_PARTIAL=stats")

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job, pod).Build()
	recorder := record.NewFakeRecorder(20)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	jobs := []batchv1.Job{*job}
	r.reconcileRestorePartialAnnotations(context.Background(), cluster, jobs)

	// Annotation patched on the live object.
	updated := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: job.Name, Namespace: "default"}, updated))
	assert.Equal(t, "stats", updated.Annotations[util.AnnotationRestorePartial])

	// In-memory slice also carries the annotation so the same reconcile pass
	// reports the partial metric.
	assert.Equal(t, "stats", jobs[0].Annotations[util.AnnotationRestorePartial])

	// Warning Event emitted exactly once.
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonRestorePartial)
		assert.Contains(t, ev, "Warning")
	default:
		t.Fatal("expected RestorePartial warning event")
	}

	// Second pass with the (now annotated) job: idempotent, no second event.
	r.reconcileRestorePartialAnnotations(context.Background(), cluster,
		[]batchv1.Job{*updated})
	select {
	case ev := <-recorder.Events:
		t.Fatalf("no second event expected for an already annotated job, got %q", ev)
	default:
	}
}

func TestReconcileRestorePartialAnnotations_SkipsNonCandidates(t *testing.T) {
	cluster := newTestCluster()

	backupJob := restoreJobNamed("test-cluster-backup-20260101020000")
	backupJob.Labels[util.LabelBackupOperation] = util.BackupOperationBackup

	runningRestore := restoreJobNamed("test-cluster-restore-20260101030000")
	runningRestore.Status.Succeeded = 0

	cleanRestore := restoreJobNamed("test-cluster-restore-20260101040000")
	cleanPod := restorePodFor(cleanRestore.Name, "restore completed cleanly")

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, backupJob, runningRestore, cleanRestore, cleanPod).Build()
	recorder := record.NewFakeRecorder(20)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	r.reconcileRestorePartialAnnotations(context.Background(), cluster,
		[]batchv1.Job{*backupJob, *runningRestore, *cleanRestore})

	for _, name := range []string{backupJob.Name, runningRestore.Name, cleanRestore.Name} {
		got := &batchv1.Job{}
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Name: name, Namespace: "default"}, got))
		assert.NotContains(t, got.Annotations, util.AnnotationRestorePartial,
			"job %s must not be annotated", name)
	}
	select {
	case ev := <-recorder.Events:
		t.Fatalf("no event expected, got %q", ev)
	default:
	}
}

func TestReconcileRestorePartialAnnotations_PodListErrorSkips(t *testing.T) {
	cluster := newTestCluster()
	job := restoreJobNamed("test-cluster-restore-20260101020000")

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c clientpkg.WithWatch,
				list clientpkg.ObjectList, opts ...clientpkg.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return apierrors.NewInternalError(assert.AnError)
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
	recorder := record.NewFakeRecorder(20)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Non-fatal: a pod List failure must not annotate or emit events.
	r.reconcileRestorePartialAnnotations(context.Background(), cluster, []batchv1.Job{*job})

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: job.Name, Namespace: "default"}, got))
	assert.NotContains(t, got.Annotations, util.AnnotationRestorePartial)
}

func TestReconcileRestorePartialAnnotations_PatchErrorLoggedAndSkipped(t *testing.T) {
	cluster := newTestCluster()
	job := restoreJobNamed("test-cluster-restore-20260101020000")
	pod := restorePodFor(job.Name, "GPRESTORE_PARTIAL=stats")

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c clientpkg.WithWatch,
				obj clientpkg.Object, patch clientpkg.Patch,
				opts ...clientpkg.PatchOption) error {
				return apierrors.NewInternalError(assert.AnError)
			},
		}).Build()
	recorder := record.NewFakeRecorder(20)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Non-fatal: the patch failure is logged; NO event is emitted (the event
	// fires only after a successful patch so it stays exactly-once).
	r.reconcileRestorePartialAnnotations(context.Background(), cluster, []batchv1.Job{*job})
	select {
	case ev := <-recorder.Events:
		t.Fatalf("no event expected when the annotation patch fails, got %q", ev)
	default:
	}
}

func TestParseRestorePartialMessage_DetailTokenRunes(t *testing.T) {
	// Mixed-token detail exercises letters, digits, dash and underscore.
	detail, ok := parseRestorePartialMessage("GPRESTORE_PARTIAL=stats-2_X end")
	require.True(t, ok)
	assert.Equal(t, "stats-2_X", detail)
}

func TestJobRestorePartial(t *testing.T) {
	job := restoreJobNamed("r1")
	assert.False(t, jobRestorePartial(job))
	job.Annotations = map[string]string{util.AnnotationRestorePartial: "stats"}
	assert.True(t, jobRestorePartial(job))
}
