package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// scalingStateCluster builds a Scaling-phase cluster carrying a scaling-sts
// scale-state annotation (oldCount->newCount) so checkScaleOutPhases routes to
// the scaling-sts -> expanding transition guarded by the coordinator-stability
// gate.
func scalingStateCluster(t *testing.T, oldCount, newCount int32) (
	*cbv1alpha1.CloudberryCluster, scaleStateData,
) {
	t.Helper()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = newCount
	state := scaleStateData{
		Phase: scalePhaseScalingSTS, OldCount: oldCount, NewCount: newCount,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}
	return cluster, state
}

// reloadScaleState reads back the persisted scale-state annotation phase.
func reloadScaleState(
	t *testing.T, c client.Client, cluster *cbv1alpha1.CloudberryCluster,
) scaleStateData {
	t.Helper()
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, c.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &got))
	return got
}

// TestScaleOut_CoordinatorRolling_DoesNotCreateGpexpandJob is the primary
// regression for the scale-out timing bug: while the coordinator StatefulSet is
// mid-rollout (currentRevision != updateRevision, replicas not yet updated/ready)
// the scaling-sts phase MUST hold (requeue) and the gpexpand Job MUST NOT be
// created — otherwise gpexpand connects to a restarting coordinator and dies with
// "the database system is shutting down" -> "refusing to expand: existing
// segment(s) down".
func TestScaleOut_CoordinatorRolling_DoesNotCreateGpexpandJob(t *testing.T) {
	scheme := newTestScheme()
	cluster, state := scalingStateCluster(t, 2, 3)

	// Segments provisioned (existing Ready, new pod Running) — the segment gate
	// passes — but the coordinator is mid-rollout.
	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	coordSts := rollingCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a rolling coordinator must keep the scaling-sts phase requeuing")

	// Phase must NOT have advanced.
	assert.Equal(t, scalePhaseScalingSTS, reloadScaleState(t, k8sClient, cluster).Phase,
		"phase must NOT advance while the coordinator is mid-rollout")

	// The gpexpand Job must NOT exist yet.
	job := &batchv1.Job{}
	getErr := k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.GpexpandJobName(cluster.Name, 2, 3),
		Namespace: cluster.Namespace,
	}, job)
	assert.Error(t, getErr,
		"no gpexpand Job may be created while the coordinator is not stable")
}

// TestScaleOut_CoordinatorPodNotReady_DoesNotCreateGpexpandJob proves that even
// when the coordinator STATEFULSET reports fully rolled, a coordinator POD that
// is not Ready (e.g. still restarting its container) holds the scaling-sts phase.
func TestScaleOut_CoordinatorPodNotReady_DoesNotCreateGpexpandJob(t *testing.T) {
	scheme := newTestScheme()
	cluster, state := scalingStateCluster(t, 2, 3)

	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	// Coordinator pod Running but NOT Ready.
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)
	coordPod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a not-Ready coordinator pod must keep the scaling-sts phase requeuing")
	assert.Equal(t, scalePhaseScalingSTS, reloadScaleState(t, k8sClient, cluster).Phase,
		"phase must NOT advance while the coordinator pod is not Ready")
}

// TestScaleOut_CoordinatorTerminating_DoesNotCreateGpexpandJob proves a
// terminating coordinator pod (DeletionTimestamp set — the exact state during a
// rollout-triggered restart) holds the scaling-sts phase.
func TestScaleOut_CoordinatorTerminating_DoesNotCreateGpexpandJob(t *testing.T) {
	scheme := newTestScheme()
	cluster, state := scalingStateCluster(t, 2, 3)

	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)
	now := metav1.Now()
	coordPod.DeletionTimestamp = &now
	coordPod.Finalizers = []string{"cloudberry.test/hold"} // required with DeletionTimestamp

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a terminating coordinator pod must keep the scaling-sts phase requeuing")
	assert.Equal(t, scalePhaseScalingSTS, reloadScaleState(t, k8sClient, cluster).Phase,
		"phase must NOT advance while the coordinator pod is terminating")
}

// TestScaleOut_CoordinatorStable_CreatesGpexpandJob proves the positive path:
// with the coordinator fully stable (STS rolled + pod Running/Ready), existing
// segments Ready, and the new segment pod Running, the scaling-sts phase advances
// to expanding and the expanding phase creates the gpexpand Job.
func TestScaleOut_CoordinatorStable_CreatesGpexpandJob(t *testing.T) {
	scheme := newTestScheme()
	cluster, state := scalingStateCluster(t, 2, 3)

	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	// Pass 1: scaling-sts -> expanding (coordinator stable + segments provisioned).
	_, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	after := reloadScaleState(t, k8sClient, cluster)
	require.Equal(t, scalePhaseExpanding, after.Phase,
		"a stable coordinator advances scaling-sts -> expanding")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))

	// Pass 2: expanding -> create the gpexpand Job.
	_, err = r.checkScaleOutPhases(context.Background(), updated,
		scaleStateAnnotation(t, after))
	require.NoError(t, err)

	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.GpexpandJobName(cluster.Name, 2, 3),
		Namespace: cluster.Namespace,
	}, job), "the gpexpand Job is created once the coordinator is stable")
}

// TestScaleOut_StandbyRolling_DoesNotCreateGpexpandJob proves the gate also
// covers the STANDBY when enabled: a mid-rollout standby holds the scaling-sts
// phase even when the coordinator itself is stable.
func TestScaleOut_StandbyRolling_DoesNotCreateGpexpandJob(t *testing.T) {
	scheme := newTestScheme()
	cluster, state := scalingStateCluster(t, 2, 3)
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)

	// Standby STS mid-rollout.
	one := int32(1)
	standbySts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       util.StandbyName(cluster.Name),
			Namespace:  cluster.Namespace,
			Generation: 1,
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &one},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1, Replicas: 1, ReadyReplicas: 0, UpdatedReplicas: 0,
			CurrentRevision: "rev-1", UpdateRevision: "rev-2",
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod, standbySts).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a rolling standby must keep the scaling-sts phase requeuing")
	assert.Equal(t, scalePhaseScalingSTS, reloadScaleState(t, k8sClient, cluster).Phase,
		"phase must NOT advance while the standby is mid-rollout")
}

// TestApplyScaleOutStatefulSets_DoesNotTouchCoordinator proves constraint #2:
// the scale-out env injection / StatefulSet application only writes the SEGMENT
// StatefulSet(s) and never modifies the COORDINATOR StatefulSet template, so the
// scale-out itself never triggers a coordinator rollout.
func TestApplyScaleOutStatefulSets_DoesNotTouchCoordinator(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 3

	// Pre-existing coordinator STS with a distinctive template annotation that
	// must survive the scale-out untouched (no coordinator rollout).
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordSts.Spec.Template.Annotations = map[string]string{
		"sentinel": "unchanged",
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, coordSts).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	// Capture the coordinator's resourceVersion AFTER the fake client assigns it,
	// so a later comparison detects any write (roll) performed by the scale-out.
	before := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, before))
	origResourceVersion := before.ResourceVersion

	state := scaleStateData{Phase: scalePhaseScalingSTS, OldCount: 2, NewCount: 3}
	stateJSON, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, r.applyScaleOutStatefulSets(
		context.Background(), cluster, 2, string(stateJSON)))

	// The coordinator StatefulSet must be byte-for-byte unchanged: same template
	// annotation and same resourceVersion (i.e. it was never Update()d).
	after := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, after))
	assert.Equal(t, "unchanged", after.Spec.Template.Annotations["sentinel"],
		"scale-out must not modify the coordinator pod template")
	assert.Equal(t, origResourceVersion, after.ResourceVersion,
		"scale-out must not write (roll) the coordinator StatefulSet")

	// The SEGMENT StatefulSet, by contrast, must now carry the expansion env.
	segSts := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, segSts))
	assert.True(t, segmentTemplateHasExpansionBaseCount(segSts),
		"scale-out must inject CLOUDBERRY_EXPANSION_BASE_COUNT into the segment template")
}
