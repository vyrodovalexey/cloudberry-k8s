package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// segRegSpyClient wraps the controller-package mockDBClient and records whether
// the DEPRECATED hand-registration methods were invoked. The gpexpand scale-out
// path MUST NOT call these (gpexpand's -i phase owns segment registration and
// physical init); a call here is a regression of the seeding-ordering defect.
type segRegSpyClient struct {
	*mockDBClient
	registerCalls atomic.Int32
	seedCalls     atomic.Int32
}

func (s *segRegSpyClient) RegisterNewSegments(
	_ context.Context, _ db.SegmentRegistrationOptions) error {
	s.registerCalls.Add(1)
	return nil
}

func (s *segRegSpyClient) SeedNewSegmentCatalog(
	_ context.Context, _ db.SegmentRegistrationOptions) (int, error) {
	s.seedCalls.Add(1)
	return 0, nil
}

// readyStatefulSet returns a Ready StatefulSet (replicas met) so the scaling-sts
// phase advances to expanding.
func readyStatefulSet(name, namespace string, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			CurrentReplicas: replicas,
			UpdatedReplicas: replicas,
		},
	}
}

// scaleOutStatefulSet returns a StatefulSet at the NEW replica count but with
// only the PRE-scale count Ready (ReadyReplicas = oldCount). This mirrors the
// live scale-out state: existing segments Ready, new gpexpand-managed segment(s)
// NOT Ready (empty datadir). The relaxed scale-out gate requires the pre-scale
// count Ready + the new pods merely Running.
func scaleOutStatefulSet(name, namespace string, newCount, oldCount int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: &newCount},
		Status: appsv1.StatefulSetStatus{
			Replicas:        newCount,
			ReadyReplicas:   oldCount,
			CurrentReplicas: newCount,
			UpdatedReplicas: newCount,
		},
	}
}

// stableCoordinatorSts returns a fully-rolled, single-replica coordinator
// StatefulSet (observedGeneration == generation, ready == updated == replicas,
// currentRevision == updateRevision) so the coordinator-stability gate passes.
func stableCoordinatorSts(clusterName, namespace string) *appsv1.StatefulSet {
	one := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       util.CoordinatorName(clusterName),
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &one},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
			CurrentRevision:    "rev-1",
			UpdateRevision:     "rev-1",
		},
	}
}

// rollingCoordinatorSts returns a coordinator StatefulSet that is mid-rollout
// (currentRevision != updateRevision and not all replicas updated/ready) so the
// coordinator-stability gate must WAIT.
func rollingCoordinatorSts(clusterName, namespace string) *appsv1.StatefulSet {
	sts := stableCoordinatorSts(clusterName, namespace)
	sts.Status.ReadyReplicas = 0
	sts.Status.UpdatedReplicas = 0
	sts.Status.CurrentRevision = "rev-1"
	sts.Status.UpdateRevision = "rev-2"
	return sts
}

// readyCoordinatorPod returns the ordinal-0 coordinator pod in the Running phase
// with a PodReady=True condition and no DeletionTimestamp — a stable coordinator
// pod that satisfies the coordinator-stability gate.
func readyCoordinatorPod(clusterName, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-0", util.CoordinatorName(clusterName)),
			Namespace: namespace,
			Labels:    util.CommonLabels(clusterName, util.ComponentCoordinator),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

// runningSegmentPod returns a segment pod (matching the component's CommonLabels)
// in the Running phase but WITHOUT any Ready container condition — i.e. Running
// but not Ready, which is exactly what the relaxed scale-out gate accepts for a
// new gpexpand-managed segment.
func runningSegmentPod(clusterName, component string, ordinal int32) *corev1.Pod {
	stsName := util.SegmentPrimaryName(clusterName)
	if component == util.ComponentSegmentMirror {
		stsName = util.SegmentMirrorName(clusterName)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", stsName, ordinal),
			Namespace: "default",
			Labels:    util.CommonLabels(clusterName, component),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// TestScaleOut_GpexpandPath_DoesNotRegisterSegments proves the corrected
// scale-out ordering: the scaling-sts phase (pods Ready) advances to the
// gpexpand-backed expanding phase which CREATES the gpexpand Job, and the
// controller NEVER hand-registers segments via the db.Client
// (RegisterNewSegments / SeedNewSegmentCatalog). gpexpand's -i phase owns
// segment registration + physical init, so pre-registration would make gpexpand
// see the new segments as down/unknown and refuse to expand.
func TestScaleOut_GpexpandPath_DoesNotRegisterSegments(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	// The desired (post-scale) count must match the Ready STS replicas so the
	// scaling-sts phase advances.
	cluster.Spec.Segments.Count = 3

	// scaling-sts phase with all segment StatefulSets Ready at the NEW count.
	state := scaleStateData{Phase: scalePhaseScalingSTS, OldCount: 2, NewCount: 3}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	// New segment state: STS at newCount, only oldCount Ready; new pod (ordinal 2)
	// merely Running. The relaxed scale-out gate must still advance to expanding.
	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	// The coordinator-stability gate requires a fully-rolled coordinator STS +
	// Running/Ready coordinator pod before the gpexpand phase is entered.
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)

	spy := &segRegSpyClient{mockDBClient: &mockDBClient{}}
	factory := &mockDBClientFactory{client: spy}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, factory)

	// Pass 1: scaling-sts -> expanding (pods Ready). advanceScalePhase requeues.
	_, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)

	// Reload the cluster + state (the phase advanced to expanding).
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var afterSTS scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &afterSTS))
	require.Equal(t, scalePhaseExpanding, afterSTS.Phase,
		"scaling-sts must advance to the gpexpand-backed expanding phase")

	// Pass 2: expanding -> create the gpexpand Job.
	_, err = r.checkScaleOutPhases(context.Background(), updated,
		scaleStateAnnotation(t, afterSTS))
	require.NoError(t, err)

	// The gpexpand Job exists — gpexpand (not the operator) registers segments.
	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.GpexpandJobName(cluster.Name, 2, 3),
		Namespace: cluster.Namespace,
	}, job), "the expanding phase must create the gpexpand Job")

	// The DEPRECATED hand-registration methods must NEVER be called.
	assert.Equal(t, int32(0), spy.registerCalls.Load(),
		"scale-out gpexpand path must NOT call RegisterNewSegments")
	assert.Equal(t, int32(0), spy.seedCalls.Load(),
		"scale-out gpexpand path must NOT call SeedNewSegmentCatalog")
}

// TestScaleOut_PhaseOrdering_PodsThenJobThenRunning walks the full corrected
// phase sequence: scaling-sts (pods Ready) -> expanding (gpexpand Job created) ->
// (Job Succeeded) -> completed -> Running, asserting each transition and that no
// segment hand-registration ever occurs.
func TestScaleOut_PhaseOrdering_PodsThenJobThenRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Status.SegmentsTotal = 2
	cluster.Spec.Segments.Count = 3

	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName,
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	// A Succeeded gpexpand Job (deterministic name), already tracked via
	// ExpandJobName, so the expanding phase observes it and advances -> completed.
	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3,
		batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)

	spy := &segRegSpyClient{mockDBClient: &mockDBClient{}}
	factory := &mockDBClientFactory{client: spy}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, factory)

	// expanding (Job Succeeded) -> completed.
	_, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var afterExpand scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &afterExpand))
	assert.Equal(t, scalePhaseCompleted, afterExpand.Phase,
		"a Succeeded gpexpand Job advances expanding -> completed")

	// completed -> Running (annotation cleared, phase Running).
	_, err = r.checkScaleOutPhases(context.Background(), updated,
		scaleStateAnnotation(t, afterExpand))
	require.NoError(t, err)

	final := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), final))
	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, final.Status.Phase,
		"completed phase returns the cluster to Running")
	assert.NotContains(t, final.Annotations, annotationScaleState,
		"the scale-state annotation is cleared on completion")

	assert.Equal(t, int32(0), spy.registerCalls.Load(),
		"no hand-registration across the full scale-out sequence")
	assert.Equal(t, int32(0), spy.seedCalls.Load(),
		"no coordinator-dispatch seeding across the full scale-out sequence")
}
