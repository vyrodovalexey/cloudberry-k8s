package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// errAsCause is a tiny error constructor for the terminal-failure tests.
func errAsCause(msg string) error { return errors.New(msg) }

// redistProgressRecorder captures SetRedistributionProgress calls (the coarse
// data-redistribution gauge the expanding-phase handler flips while the gpexpand
// Job runs / on success) on top of the wave345 scale metrics.
type redistProgressRecorder struct {
	wave345Recorder
	progress []float64
}

func (r *redistProgressRecorder) SetRedistributionProgress(_, _ string, p float64) {
	r.progress = append(r.progress, p)
}

// nilGpexpandBuilder embeds the real builder but returns a nil gpexpand Job so
// startGpexpandJob's "builder returned nil" defensive branch is exercised.
type nilGpexpandBuilder struct {
	builder.ResourceBuilder
}

func (nilGpexpandBuilder) BuildGpexpandJob(
	_ *cbv1alpha1.CloudberryCluster, _, _ int32, _ string) *batchv1.Job {
	return nil
}

// scaleStateAnnotation marshals a scaleStateData into its annotation JSON.
func scaleStateAnnotation(t *testing.T, s scaleStateData) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}

// gpexpandJob builds a gpexpand Job with the deterministic name and the given
// status so the expanding-phase handler observes a crafted terminal state.
func gpexpandJob(cluster string, namespace string, oldCount, newCount int32,
	status batchv1.JobStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GpexpandJobName(cluster, oldCount, newCount),
			Namespace: namespace,
		},
		Status: status,
	}
}

// jobFailedStatus returns a Job status with a terminal Failed condition.
func jobFailedStatus() batchv1.JobStatus {
	return batchv1.JobStatus{
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
		},
	}
}

// TestProcessScaleOutExpanding_CreatesJob proves the first pass of the expanding
// phase creates the gpexpand Job (deterministic name), records ExpandJobName on
// the scale state, emits a GpexpandStarted event and requeues on the poll
// interval.
func TestProcessScaleOutExpanding_CreatesJob(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterGpexpandPoll, result.RequeueAfter)

	// The gpexpand Job exists with the deterministic name.
	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.GpexpandJobName(cluster.Name, 2, 3),
		Namespace: cluster.Namespace,
	}, job))

	// ExpandJobName is recorded on the scale state.
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, job.Name, got.ExpandJobName)

	var started bool
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "GpexpandStarted") {
			started = true
		}
	}
	assert.True(t, started, "a GpexpandStarted event must be emitted")
}

// TestProcessScaleOutExpanding_JobSucceeded proves a Succeeded gpexpand Job
// advances the scale-out to the completed phase (requeues immediately to run the
// completion step) and emits a SegmentsExpanded event.
func TestProcessScaleOutExpanding_JobSucceeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, scalePhaseCompleted, got.Phase)

	var expanded bool
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "SegmentsExpanded") {
			expanded = true
		}
	}
	assert.True(t, expanded, "a SegmentsExpanded event must be emitted")
}

// TestProcessScaleOutExpanding_JobFailedBelowBound proves a terminally-failed
// gpexpand Job below the attempt bound increments the attempt counter, deletes
// the failed Job (so a fresh resumable one is created next pass), clears the
// tracked name and requeues.
func TestProcessScaleOutExpanding_JobFailedBelowBound(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, int32(1), got.RedistributeAttempts)
	assert.Empty(t, got.ExpandJobName, "the failed Job name is cleared so a fresh one is created")

	// The failed Job was deleted.
	assert.True(t, apierrors.IsNotFound(k8sClient.Get(context.Background(), client.ObjectKey{
		Name: jobName, Namespace: cluster.Namespace,
	}, &batchv1.Job{})))
}

// TestProcessScaleOutExpanding_JobFailedAtBound proves that at the attempt bound
// a failed gpexpand Job fails the scale-out terminally: the cluster is marked
// Failed, failedSegments is recorded, the scale-out-failed metric and a
// ScaleOutFailed Warning event are emitted, and the scale-state annotation is
// cleared so the operator STOPS retrying.
func TestProcessScaleOutExpanding_JobFailedAtBound(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName,
		RedistributeAttempts: maxRedistributeAttempts - 1,
	}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	metricsRec := &wave345Recorder{}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		metricsRec, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "terminal failure must not requeue")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	assert.Equal(t, cbv1alpha1.ClusterPhaseFailed, updated.Status.Phase)
	require.Len(t, updated.Status.FailedSegments, 1)
	assert.Equal(t, int32(2), updated.Status.FailedSegments[0].ContentID)
	assert.NotContains(t, updated.Annotations, annotationScaleState)
	assert.Contains(t, metricsRec.scaleOps, "scale-out-failed")

	var warned bool
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, cbv1alpha1.EventReasonScaleOutFailed) {
			warned = true
		}
	}
	assert.True(t, warned, "a ScaleOutFailed warning event must be emitted")
}

// TestProcessScaleOutExpanding_WedgeDoesNotConsumeAttempt proves the D3/D8
// interaction: a coordinator wedge (57P03) surfaced by the failed gpexpand Job's
// pod termination message is routed to recovery (coordinator pod deleted) and
// must NOT increment the attempt counter.
func TestProcessScaleOutExpanding_WedgeDoesNotConsumeAttempt(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	coordPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.CoordinatorPodName(cluster.Name),
			Namespace: cluster.Namespace,
		},
	}
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName,
		RedistributeAttempts: 1,
	}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())
	// A gpexpand Job pod whose terminated container reports the coordinator
	// shutting-down (57P03) state so the failure is classified as a wedge.
	jobPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-abcde",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Message: "FATAL: the database system is shutting down (SQLSTATE 57P03)",
				}},
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job, coordPod, jobPod).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterCoordinatorRecovery, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	assert.NotEqual(t, cbv1alpha1.ClusterPhaseFailed, updated.Status.Phase)
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, int32(1), got.RedistributeAttempts,
		"a coordinator wedge must NOT consume a gpexpand attempt")
}

// TestFailScaleOutRedistribution_NilMetrics covers the r.metrics == nil guard in
// failScaleOutRedistribution: with no metrics recorder the terminal-failure path
// still marks the cluster Failed, records failedSegments, and clears the
// scale-state annotation without panicking.
func TestFailScaleOutRedistribution_NilMetrics(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		nil, nil, nil)

	state := &scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 1, NewCount: 3,
		RedistributeAttempts: maxRedistributeAttempts,
	}
	result, err := r.failScaleOutRedistribution(context.Background(), cluster, state,
		errAsCause("permanent failure"))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	assert.Equal(t, cbv1alpha1.ClusterPhaseFailed, updated.Status.Phase)
	require.Len(t, updated.Status.FailedSegments, 2)
	assert.NotContains(t, updated.Annotations, annotationScaleState)
	assert.NotContains(t, updated.Annotations, util.AnnotationScaleStarted)
}

// TestFailScaleOutRedistribution_ErrorPathsLogged drives the defensive
// error-logging branches of failScaleOutRedistribution: a failing Status().Update
// and failing annotation removals are logged (not returned) so the terminal
// failure still completes without requeue.
func TestFailScaleOutRedistribution_ErrorPathsLogged(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{
		annotationScaleState:        "{}",
		util.AnnotationScaleStarted: "true",
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ ...client.SubResourceUpdateOption) error {
				return errAsCause("status update forbidden")
			},
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return errAsCause("patch forbidden")
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	metricsRec := &wave345Recorder{}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		metricsRec, nil, nil)

	state := &scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3,
		RedistributeAttempts: maxRedistributeAttempts,
	}
	result, err := r.failScaleOutRedistribution(context.Background(), cluster, state,
		errAsCause("permanent failure"))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
	assert.Contains(t, metricsRec.scaleOps, "scale-out-failed")

	var warned bool
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, cbv1alpha1.EventReasonScaleOutFailed) {
			warned = true
		}
	}
	assert.True(t, warned, "a ScaleOutFailed warning event must still be emitted")
}

// TestPersistScaleState_PatchError covers the error return of persistScaleState:
// when the annotation patch fails the wrapped patch error is surfaced.
func TestPersistScaleState_PatchError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return errAsCause("patch forbidden")
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, RedistributeAttempts: 2}
	err := r.persistScaleState(context.Background(), cluster, state)
	require.Error(t, err)
}

// TestProcessScaleOutExpanding_JobRunning proves an active (not-yet-terminal)
// gpexpand Job self-loops: the handler reports the coarse in-progress
// redistribution gauge and requeues on the poll interval WITHOUT advancing the
// scale phase (covers processScaleOutExpanding's default branch +
// setRedistributionProgress non-nil path).
func TestProcessScaleOutExpanding_JobRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, batchv1.JobStatus{Active: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	metricsRec := &redistProgressRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		metricsRec, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterGpexpandPoll, result.RequeueAfter)

	// The coarse in-progress gauge is reported (< 1.0) and the phase is unchanged.
	require.NotEmpty(t, metricsRec.progress)
	assert.InDelta(t, gpexpandInProgressRatio, metricsRec.progress[len(metricsRec.progress)-1], 1e-9)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, scalePhaseExpanding, got.Phase, "an active job must not advance the phase")
}

// TestProcessScaleOutExpanding_JobSucceeded_ReportsProgressComplete asserts a
// Succeeded gpexpand Job flips the redistribution gauge to 1.0 (completion) via
// the non-nil metrics recorder.
func TestProcessScaleOutExpanding_JobSucceeded_ReportsProgressComplete(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	metricsRec := &redistProgressRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		metricsRec, nil, nil)

	_, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	require.NotEmpty(t, metricsRec.progress)
	assert.InDelta(t, 1.0, metricsRec.progress[len(metricsRec.progress)-1], 1e-9)
}

// TestProcessScaleOutExpanding_JobNotFoundRecreates proves that when the tracked
// gpexpand Job has disappeared (deleted externally) the handler clears the stale
// name and recreates the (resumable) Job under the same deterministic name,
// re-persisting ExpandJobName (covers the IsNotFound recreate branch).
func TestProcessScaleOutExpanding_JobNotFoundRecreates(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	// ExpandJobName is set but NO Job object exists in the fake client.
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterGpexpandPoll, result.RequeueAfter)

	// The Job is (re)created under the same deterministic name.
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name: jobName, Namespace: cluster.Namespace,
	}, &batchv1.Job{}))

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, jobName, got.ExpandJobName)
}

// TestProcessScaleOutExpanding_JobGetError proves a transport-level Get failure
// (not NotFound) is surfaced as an error with an error requeue (covers the
// getErr != nil branch of processScaleOutExpanding).
func TestProcessScaleOutExpanding_JobGetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return errAsCause("get gpexpand job forbidden")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.processScaleOutExpanding(context.Background(), cluster, &state)
	require.Error(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// TestStartGpexpandJob_NilBuilder proves that when the builder returns a nil Job
// the scale-out fails terminally (defensive branch) rather than creating a nil
// object.
func TestStartGpexpandJob_NilBuilder(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	metricsRec := &wave345Recorder{}
	r := NewClusterReconciler(k8sClient, scheme, rec,
		nilGpexpandBuilder{ResourceBuilder: builder.NewBuilder()}, metricsRec, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "a nil-built Job terminally fails the scale-out")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	assert.Equal(t, cbv1alpha1.ClusterPhaseFailed, updated.Status.Phase)
}

// TestStartGpexpandJob_CreateError proves a non-AlreadyExists Job create failure
// requeues on the error interval and does NOT persist a Job name (covers the
// create-error branch of startGpexpandJob).
func TestStartGpexpandJob_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return errAsCause("create gpexpand job forbidden")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
	assert.Empty(t, state.ExpandJobName, "no Job name is recorded when create fails")
}

// TestStartGpexpandJob_AlreadyExistsTolerated proves an in-flight (Running) Job
// whose script MATCHES the freshly-built one is adopted (NOT deleted): the
// AlreadyExists create is tolerated, the Job name is persisted and the handler
// requeues on the poll interval. This guards DEFECT B's "keep AlreadyExists
// tolerance for the in-flight case" requirement.
func TestStartGpexpandJob_AlreadyExistsTolerated(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}
	// Pre-create the REAL in-flight Job (matching script hash) so it is adopted,
	// not treated as stale drift. Mark it Active (still running).
	existing := builder.NewBuilder().BuildGpexpandJob(cluster, 2, 3, "true")
	existing.Status = batchv1.JobStatus{Active: 1}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, existing).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterGpexpandPoll, result.RequeueAfter)
	assert.Equal(t, util.GpexpandJobName(cluster.Name, 2, 3), state.ExpandJobName,
		"AlreadyExists is tolerated and the running Job name is still adopted")

	// The in-flight Job must NOT have been deleted.
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name: existing.Name, Namespace: cluster.Namespace,
	}, &batchv1.Job{}), "an in-flight matching gpexpand Job must not be deleted")
}

// TestStartGpexpandJob_StaleFailedJobRecreated proves DEFECT B: a pre-existing
// gpexpand Job of the same deterministic name that has terminally FAILED (left by
// a prior operator generation) is DELETED and NOT adopted; the handler requeues
// so a fresh Job is created after the stale one is gone.
func TestStartGpexpandJob_StaleFailedJobRecreated(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}
	// A stale FAILED Job with the deterministic name (old operator version).
	existing := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, existing).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"the stale failed Job is deleted and the handler requeues to recreate")

	// The stale Job was deleted (fake client has no finalizers to block it).
	assert.True(t, apierrors.IsNotFound(k8sClient.Get(context.Background(), client.ObjectKey{
		Name: existing.Name, Namespace: cluster.Namespace,
	}, &batchv1.Job{})), "the stale failed gpexpand Job must be deleted")

	var recreated bool
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "GpexpandJobRecreated") {
			recreated = true
		}
	}
	assert.True(t, recreated, "a GpexpandJobRecreated event must be emitted")
}

// TestStartGpexpandJob_ScriptDriftRecreated proves DEFECT B's operator-upgrade
// case: a pre-existing Job whose script-hash annotation DIFFERS from the freshly
// built Job (an upgraded operator changed the gpexpand script) is deleted and
// recreated even if it is still Active — the immutable pod template would
// otherwise carry the OLD script forever.
func TestStartGpexpandJob_ScriptDriftRecreated(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}
	// A Job with a STALE script-hash annotation (drift), still Active.
	existing := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, batchv1.JobStatus{Active: 1})
	existing.Annotations = map[string]string{
		util.AnnotationGpexpandScriptHash: "stale-hash-from-old-operator",
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, existing).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
	assert.True(t, apierrors.IsNotFound(k8sClient.Get(context.Background(), client.ObjectKey{
		Name: existing.Name, Namespace: cluster.Namespace,
	}, &batchv1.Job{})), "the drifted gpexpand Job must be deleted")
}

// TestStartGpexpandJob_PersistError proves a failing scale-state persist after a
// successful Job create requeues on the error interval (covers the persistErr
// branch of startGpexpandJob).
func TestStartGpexpandJob_PersistError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{util.AnnotationScaleStarted: "true"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return errAsCause("patch forbidden")
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	state := &scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3}
	result, err := r.startGpexpandJob(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// TestHandleGpexpandJobFailure_DeleteAndPersistErrorsLogged drives the defensive
// error-logging branches of handleGpexpandJobFailure below the attempt bound: a
// failing Job Delete and a failing scale-state Patch are logged (not returned),
// so the handler still requeues on the error interval.
func TestHandleGpexpandJobFailure_DeleteAndPersistErrorsLogged(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName}
	cluster.Annotations = map[string]string{annotationScaleState: scaleStateAnnotation(t, state)}
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.DeleteOption) error {
				return errAsCause("delete forbidden")
			},
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return errAsCause("patch forbidden")
			},
		}).Build()
	rec := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, nil)

	result, err := r.handleGpexpandJobFailure(context.Background(), cluster, &state, job)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
	// The attempt counter still advances despite the logged (non-fatal) errors.
	assert.Equal(t, int32(1), state.RedistributeAttempts)
	assert.Empty(t, state.ExpandJobName)
}

// TestGpexpandFailureMessage covers the classification helper's branches: a
// terminated container message is surfaced verbatim; a List error yields the
// "pod message unavailable" fallback; and no pod/message yields the generic
// fallback.
func TestGpexpandFailureMessage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3, jobFailedStatus())

	t.Run("terminated message surfaced", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName + "-xyz",
				Namespace: cluster.Namespace,
				Labels:    map[string]string{"job-name": jobName},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Message: "  EXPAND_RESULT=incomplete:3  ",
					}},
				}},
			},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(1),
			builder.NewBuilder(), &wave345Recorder{}, nil, nil)
		msg := gpexpandFailureMessage(context.Background(), r, cluster, job)
		assert.Equal(t, "EXPAND_RESULT=incomplete:3", msg)
	})

	t.Run("list error fallback", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList,
					_ ...client.ListOption) error {
					return errAsCause("list forbidden")
				},
			}).Build()
		r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(1),
			builder.NewBuilder(), &wave345Recorder{}, nil, nil)
		msg := gpexpandFailureMessage(context.Background(), r, cluster, job)
		assert.Contains(t, msg, "pod message unavailable")
	})

	t.Run("no pod message fallback", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(1),
			builder.NewBuilder(), &wave345Recorder{}, nil, nil)
		msg := gpexpandFailureMessage(context.Background(), r, cluster, job)
		assert.Contains(t, msg, "no terminated container message")
	})
}

// TestSetRedistributionProgress_NilMetrics covers the r.metrics == nil guard of
// setRedistributionProgress: with no recorder the call is a no-op (no panic).
func TestSetRedistributionProgress_NilMetrics(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(1),
		builder.NewBuilder(), nil, nil)
	assert.NotPanics(t, func() { r.setRedistributionProgress(cluster, 0.5) })
}
