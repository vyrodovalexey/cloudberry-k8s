package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// scaleOutAnnotatedCluster builds a cluster carrying an in-progress scale-out
// state annotation at the given phase so scaleOutInProgress() reports true and
// the scale-window quiescence guard suppresses coordinator/standby/segment
// StatefulSet reconciliation.
func scaleOutAnnotatedCluster(t *testing.T, phase string) *cbv1alpha1.CloudberryCluster {
	t.Helper()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 3
	raw, err := json.Marshal(scaleStateData{Phase: phase, OldCount: 2, NewCount: 3})
	require.NoError(t, err)
	cluster.Annotations = map[string]string{annotationScaleState: string(raw)}
	return cluster
}

// TestScaleOutInProgress_Phases asserts the scaleOutInProgress predicate is true
// for every non-terminal scale-out phase and false otherwise, so quiescence is
// scoped exactly to the gpexpand window.
func TestScaleOutInProgress_Phases(t *testing.T) {
	cases := []struct {
		name string
		anno map[string]string
		want bool
	}{
		{"scaling-sts", map[string]string{annotationScaleState: `{"phase":"scaling-sts"}`}, true},
		{"expanding", map[string]string{annotationScaleState: `{"phase":"expanding"}`}, true},
		{"legacy registering", map[string]string{annotationScaleState: `{"phase":"registering"}`}, true},
		{"legacy redistributing", map[string]string{annotationScaleState: `{"phase":"redistributing"}`}, true},
		{"completed", map[string]string{annotationScaleState: `{"phase":"completed"}`}, false},
		{"no annotation", nil, false},
		{"malformed", map[string]string{annotationScaleState: `not-json`}, false},
		{"empty value", map[string]string{annotationScaleState: ``}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Annotations = tc.anno
			assert.Equal(t, tc.want, scaleOutInProgress(cluster))
		})
	}
}

// TestReconcileStatefulSets_ScaleOut_SuppressesCoordinatorRoll is the primary
// regression for the post-gate coordinator restart: while a scale-out is in
// flight (scale-state annotation at the expanding phase — i.e. the gpexpand Job
// has been created), reconcileStatefulSets MUST NOT re-apply/roll the
// coordinator, standby or existing-segment StatefulSets. We assert the
// pre-existing coordinator StatefulSet is byte-for-byte unchanged (same
// resourceVersion, same distinctive template annotation).
func TestReconcileStatefulSets_ScaleOut_SuppressesCoordinatorRoll(t *testing.T) {
	scheme := newTestScheme()
	cluster := scaleOutAnnotatedCluster(t, scalePhaseExpanding)

	// Pre-existing coordinator STS with a sentinel template annotation that must
	// survive untouched (no roll). A build-based reconcile would overwrite the
	// whole template (dropping the sentinel) and bump the resourceVersion.
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordSts.Spec.Template.Annotations = map[string]string{"sentinel": "unchanged"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, coordSts).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	before := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, before))
	origRV := before.ResourceVersion

	require.NoError(t, r.reconcileStatefulSets(context.Background(), cluster))

	after := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, after))
	assert.Equal(t, "unchanged", after.Spec.Template.Annotations["sentinel"],
		"scale-out quiescence must not modify the coordinator pod template")
	assert.Equal(t, origRV, after.ResourceVersion,
		"scale-out quiescence must not write (roll) the coordinator StatefulSet")
}

// TestReconcileStatefulSets_ScaleOut_SuppressesStandbyRoll proves the standby
// StatefulSet is equally quiescent during a scale-out.
func TestReconcileStatefulSets_ScaleOut_SuppressesStandbyRoll(t *testing.T) {
	scheme := newTestScheme()
	cluster := scaleOutAnnotatedCluster(t, scalePhaseExpanding)
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	one := int32(1)
	standbySts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.StandbyName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			Template: newTemplateWithSentinel(),
		},
	}
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, coordSts, standbySts).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	before := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.StandbyName(cluster.Name),
		Namespace: cluster.Namespace,
	}, before))
	origRV := before.ResourceVersion

	require.NoError(t, r.reconcileStatefulSets(context.Background(), cluster))

	after := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.StandbyName(cluster.Name),
		Namespace: cluster.Namespace,
	}, after))
	assert.Equal(t, origRV, after.ResourceVersion,
		"scale-out quiescence must not write (roll) the standby StatefulSet")
}

// TestReconcileStatefulSets_ScaleOut_SuppressesExistingSegmentRoll proves the
// EXISTING segment StatefulSet is not re-applied during scale-out: the segment
// scale-up was already applied in applyScaleOutStatefulSets before the gate, so
// no further write to the segment STS may happen between the gate and gpexpand
// completion.
func TestReconcileStatefulSets_ScaleOut_SuppressesExistingSegmentRoll(t *testing.T) {
	scheme := newTestScheme()
	cluster := scaleOutAnnotatedCluster(t, scalePhaseExpanding)

	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	segSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(3),
			Template: newTemplateWithSentinel(),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, coordSts, segSts).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	before := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, before))
	origRV := before.ResourceVersion

	require.NoError(t, r.reconcileStatefulSets(context.Background(), cluster))

	after := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, after))
	assert.Equal(t, origRV, after.ResourceVersion,
		"scale-out quiescence must not write (roll) the existing segment StatefulSet")
	assert.Equal(t, "unchanged", after.Spec.Template.Annotations["sentinel"],
		"scale-out quiescence must not modify the existing segment pod template")
}

// TestReconcileCoordinator_Idempotent_NoRollAcrossNoOpReconciles proves the
// coordinator StatefulSet spec is byte-stable across two consecutive no-op
// reconciles: after the first reconcile creates it, a second reconcile with an
// UNCHANGED spec must NOT bump the resourceVersion (no rollout-triggering diff).
func TestReconcileCoordinator_Idempotent_NoRollAcrossNoOpReconciles(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	// Pass 1: create the coordinator StatefulSet.
	require.NoError(t, r.reconcileCoordinator(context.Background(), cluster))
	first := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, first))
	rv1 := first.ResourceVersion

	// Pass 2: a no-op reconcile must NOT write the StatefulSet again.
	require.NoError(t, r.reconcileCoordinator(context.Background(), cluster))
	second := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, second))

	assert.Equal(t, rv1, second.ResourceVersion,
		"a no-op coordinator reconcile must not roll the StatefulSet")
	assert.True(t, equality.Semantic.DeepEqual(first.Spec.Template, second.Spec.Template),
		"the coordinator pod template must be byte-stable across no-op reconciles")
}

// TestReconcileCoordinator_NormalReconcile_StillApplies proves normal (non-scale)
// coordinator reconcile is UNCHANGED: it creates the StatefulSet when absent.
func TestReconcileCoordinator_NormalReconcile_StillApplies(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	// reconcileStatefulSets (no scale-out annotation) MUST create the coordinator.
	require.NoError(t, r.reconcileStatefulSets(context.Background(), cluster))
	sts := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, sts), "normal reconcile must create the coordinator StatefulSet")
}

// newTemplateWithSentinel returns a pod template carrying a sentinel annotation
// used to detect an unwanted overwrite of the template during quiescence.
func newTemplateWithSentinel() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"sentinel": "unchanged"}},
	}
}
