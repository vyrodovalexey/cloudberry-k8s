package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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

// ---------------------------------------------------------------------------
// Scenario 104 — controller pre-load health-check Event tests (W5).
//
// Catalog IDs covered here (the -R / reconcile-provable cases):
//   104-EVENT-R          : failed-init Job → ONE DataLoadingHealthCheckFailed Warning.
//   104-EVENT-R (de-dup) : second reconcile of the unchanged Failed Job → no new event.
//   104-EVENT-R-mainfail : Job failed in the MAIN container → NO HC event.
//   success              : a Succeeded Job → NO HC event.
//   104-BLOCK-R          : a Failed Job → job_status=3 + errors_total incremented.
//
// They reuse the existing dataload reconcile harness (dlCluster, dlReconciler,
// dlPxfJob, dataLoadMetricsRecorder) and mirror the de-dup Event pattern of the
// backup-failure-event tests (TestEmitBackupFailureEvent_Scenario77).
// ---------------------------------------------------------------------------

// countWarningHealthCheckFailed counts drained events that are Warning events
// carrying the DataLoadingHealthCheckFailed reason.
func countWarningHealthCheckFailed(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonDataLoadingHealthCheckFailed) {
			n++
		}
	}
	return n
}

// dlRecorderReconciler builds a dataload reconciler whose recorder is the given
// FakeRecorder (the harness's dlReconciler hardcodes its own recorder, so the
// HC-event tests construct the reconciler directly to capture events).
func dlRecorderReconciler(
	t *testing.T,
	cluster *cbv1alpha1.CloudberryCluster,
	rec metrics.Recorder,
	recorder *record.FakeRecorder,
) *AdminReconciler {
	t.Helper()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	return NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, rec, nil)
}

// markDataLoadJobFailed marks the named dataload Job Failed in the fake client.
func markDataLoadJobFailed(t *testing.T, r *AdminReconciler, cluster *cbv1alpha1.CloudberryCluster, jobName string) {
	t.Helper()
	ctx := context.Background()
	start := metav1.NewTime(time.Now().Add(-60 * time.Second))
	completion := metav1.NewTime(time.Now())
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Failed = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &completion
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(t, r.client.Status().Update(ctx, job))
}

// injectFailedInitPod creates a Job pod whose dataload-healthcheck INIT container
// terminated with a non-zero exit code (the HC-attributable failure).
func injectFailedInitPod(t *testing.T, r *AdminReconciler, cluster *cbv1alpha1.CloudberryCluster, jobName string) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{batchJobNameLabel: jobName},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: dataLoadHealthCheckInitName, Image: "img"}},
			Containers:     []corev1.Container{{Name: "dataload", Image: "img"}},
		},
	}
	require.NoError(t, r.client.Create(ctx, pod))
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{
			Name: dataLoadHealthCheckInitName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
			},
		},
	}
	require.NoError(t, r.client.Status().Update(ctx, pod))
}

// injectFailedMainPod creates a Job pod whose dataload-healthcheck INIT container
// SUCCEEDED (exit 0) but the MAIN dataload container terminated non-zero — a real
// load error that must NOT be attributed to the health checks.
func injectFailedMainPod(t *testing.T, r *AdminReconciler, cluster *cbv1alpha1.CloudberryCluster, jobName string) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{batchJobNameLabel: jobName},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: dataLoadHealthCheckInitName, Image: "img"}},
			Containers:     []corev1.Container{{Name: "dataload", Image: "img"}},
		},
	}
	require.NoError(t, r.client.Create(ctx, pod))
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{
			Name: dataLoadHealthCheckInitName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
			},
		},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "dataload",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Reason: "Error"},
			},
		},
	}
	require.NoError(t, r.client.Status().Update(ctx, pod))
}

// TestDataLoadHealthCheckEvent_FailedInitEmitsWarning (104-EVENT-R) asserts a
// dataload Job observed Failed WITH a failed dataload-healthcheck init container
// emits exactly ONE DataLoadingHealthCheckFailed Warning Event.
func TestDataLoadHealthCheckEvent_FailedInitEmitsWarning(t *testing.T) {
	cluster := dlCluster("dl-hc-event", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	markDataLoadJobFailed(t, r, cluster, jobName)
	injectFailedInitPod(t, r, cluster, jobName)

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningHealthCheckFailed(events),
		"failed-init Job must emit exactly one HC Warning: %v", events)
	// The Event names the job + the init container.
	var hc string
	for _, e := range events {
		if strings.Contains(e, cbv1alpha1.EventReasonDataLoadingHealthCheckFailed) {
			hc = e
		}
	}
	require.NotEmpty(t, hc)
	assert.Contains(t, hc, "loader")
	assert.Contains(t, hc, "dataload-healthcheck")
}

// TestDataLoadHealthCheckEvent_DeDup (104-EVENT-R) asserts a SECOND reconcile of
// the same unchanged Failed Job emits NO additional HC event (de-dup on the
// transition into Failed).
func TestDataLoadHealthCheckEvent_DeDup(t *testing.T) {
	cluster := dlCluster("dl-hc-dedup", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	markDataLoadJobFailed(t, r, cluster, jobName)
	injectFailedInitPod(t, r, cluster, jobName)

	// First failing observation: exactly one HC event.
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	first := drainEvents(recorder)
	require.Equal(t, 1, countWarningHealthCheckFailed(first),
		"first failing reconcile must emit one HC event: %v", first)

	// Second observation of the same Failed Job: de-duplicated, no new event.
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	second := drainEvents(recorder)
	assert.Equal(t, 0, countWarningHealthCheckFailed(second),
		"second reconcile of unchanged Failed Job must not re-emit: %v", second)
}

// TestDataLoadHealthCheckEvent_MainContainerFailureNoEvent (104-EVENT-R-mainfail)
// asserts a Job that failed in the MAIN container (init succeeded) emits NO
// DataLoadingHealthCheckFailed event (honest attribution).
func TestDataLoadHealthCheckEvent_MainContainerFailureNoEvent(t *testing.T) {
	cluster := dlCluster("dl-hc-mainfail", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	markDataLoadJobFailed(t, r, cluster, jobName)
	injectFailedMainPod(t, r, cluster, jobName)

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningHealthCheckFailed(events),
		"a main-container failure must not be attributed to the health checks: %v", events)
	// But the failure IS surfaced via status + errors_total (generic handling).
	require.Len(t, cluster.Status.DataLoading.Jobs, 1)
	assert.Equal(t, "Failed", cluster.Status.DataLoading.Jobs[0].LastStatus)
	assert.Equal(t, 1, rec.errors["loader"])
}

// TestDataLoadHealthCheckEvent_FailedNoPodNoEvent asserts a Failed Job with NO
// derivable init-container status (no pod) emits NO HC event (honest fallback).
func TestDataLoadHealthCheckEvent_FailedNoPodNoEvent(t *testing.T) {
	cluster := dlCluster("dl-hc-nopod", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	markDataLoadJobFailed(t, r, cluster, jobName) // no pod injected

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningHealthCheckFailed(events),
		"no derivable init failure must stay silent: %v", events)
	// Still recorded as Failed via the generic harvest.
	assert.Equal(t, "Failed", cluster.Status.DataLoading.Jobs[0].LastStatus)
	assert.Equal(t, 1, rec.errors["loader"])
}

// TestDataLoadHealthCheckEvent_SucceededNoEvent asserts a Succeeded dataload Job
// emits NO HC event.
func TestDataLoadHealthCheckEvent_SucceededNoEvent(t *testing.T) {
	cluster := dlCluster("dl-hc-ok", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	jobName := util.DataLoadJobName(cluster.Name, "loader")

	start := metav1.NewTime(time.Now().Add(-30 * time.Second))
	done := metav1.NewTime(time.Now())
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(t, r.client.Status().Update(ctx, job))
	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_ROWS=42")

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningHealthCheckFailed(events),
		"a succeeded dataload Job must not emit an HC Warning: %v", events)
	assert.Equal(t, "Succeeded", cluster.Status.DataLoading.Jobs[0].LastStatus)
}

// TestDataLoadHealthCheckEvent_BlockMetrics (104-BLOCK-R) asserts the failed-init
// Job is harvested as job_status=3 (Failed) AND errors_total is incremented (the
// existing honest harvest the HC failure rides on).
func TestDataLoadHealthCheckEvent_BlockMetrics(t *testing.T) {
	cluster := dlCluster("dl-hc-block", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	recorder := record.NewFakeRecorder(20)
	r := dlRecorderReconciler(t, cluster, rec, recorder)
	ctx := context.Background()

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	markDataLoadJobFailed(t, r, cluster, jobName)
	injectFailedInitPod(t, r, cluster, jobName)

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	assert.Equal(t, 3.0, rec.status["loader"], "status code must be 3 (Failed)")
	assert.Equal(t, 1, rec.errors["loader"], "errors_total must be incremented once")
	assert.Zero(t, rec.rows["loader"], "no rows harvested on a failed load")
	require.Len(t, cluster.Status.DataLoading.Jobs, 1)
	assert.Equal(t, "Failed", cluster.Status.DataLoading.Jobs[0].LastStatus)
}

// TestDataLoadHealthCheckInitFailed_ListErrorIsSilent asserts a pod-List failure
// during attribution is non-fatal and returns false (the controller stays silent
// rather than mis-attributing). Drives the List-error branch directly.
func TestDataLoadHealthCheckInitFailed_ListErrorIsSilent(t *testing.T) {
	scheme := newTestScheme()
	cluster := dlCluster("dl-hc-listerr", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("list boom")
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: util.DataLoadJobName(cluster.Name, "loader"), Namespace: cluster.Namespace,
	}}
	assert.False(t, r.dataLoadHealthCheckInitFailed(context.Background(), cluster, job),
		"a List error must be non-fatal and return false")
}

// TestPodInitContainerFailed covers the init-container-failure predicate
// directly: terminated non-zero => true; exit 0 / wrong name / not terminated
// => false (edge/boundary coverage).
func TestPodInitContainerFailed(t *testing.T) {
	mk := func(name string, st corev1.ContainerState) *corev1.Pod {
		return &corev1.Pod{Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{Name: name, State: st}},
		}}
	}
	term := func(code int32) corev1.ContainerState {
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code}}
	}

	assert.True(t, podInitContainerFailed(
		mk(dataLoadHealthCheckInitName, term(1)), dataLoadHealthCheckInitName))
	assert.False(t, podInitContainerFailed(
		mk(dataLoadHealthCheckInitName, term(0)), dataLoadHealthCheckInitName),
		"exit 0 is not a failure")
	assert.False(t, podInitContainerFailed(
		mk("other-init", term(1)), dataLoadHealthCheckInitName),
		"a different init container must not match")
	assert.False(t, podInitContainerFailed(
		mk(dataLoadHealthCheckInitName, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}),
		dataLoadHealthCheckInitName),
		"a still-running init container is not a failure")
	assert.False(t, podInitContainerFailed(&corev1.Pod{}, dataLoadHealthCheckInitName),
		"a pod with no init statuses is not a failure")
}
