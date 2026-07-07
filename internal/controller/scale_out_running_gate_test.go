package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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

// pendingSegmentPod returns a segment pod that exists but is NOT yet Running
// (Pending) — the state of a just-created new pod whose container has not
// started. The relaxed scale-out gate must NOT advance while it is Pending.
func pendingSegmentPod(clusterName, component string, ordinal int32) *corev1.Pod {
	p := runningSegmentPod(clusterName, component, ordinal)
	p.Status.Phase = corev1.PodPending
	return p
}

// TestScaleOut_ScalingSTS_AdvancesOnNewPodsRunning_NotReady is the BUG-2
// regression test: during scale-out the scaling-sts -> expanding transition MUST
// proceed once the NEW segment pod(s) are RUNNING (not necessarily DB/PXF Ready),
// because a gpexpand-managed new segment has an empty datadir and can never reach
// full readiness until gpexpand initializes it. The StatefulSet here reports only
// the PRE-scale count Ready (ReadyReplicas=2 of 3) — the OLD full-readiness gate
// would deadlock; the new gate advances.
func TestScaleOut_ScalingSTS_AdvancesOnNewPodsRunning_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 3

	state := scaleStateData{
		Phase: scalePhaseScalingSTS, OldCount: 2, NewCount: 3,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	// STS at newCount=3 but only oldCount=2 Ready (new pod NOT Ready).
	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	// The new pod (ordinal 2) is RUNNING but not Ready.
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)
	// A stable coordinator (fully rolled STS + Running/Ready pod) is required by
	// the coordinator-stability gate before the phase advances to expanding.
	coordSts := stableCoordinatorSts(cluster.Name, cluster.Namespace)
	coordPod := readyCoordinatorPod(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod, coordSts, coordPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	_, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, scalePhaseExpanding, got.Phase,
		"scale-out must advance to expanding once the NEW pods are Running (not Ready)")
}

// TestScaleOut_ScalingSTS_WaitsWhenNewPodNotRunning proves the gate still WAITS
// when the new pod is only Pending (container not started): gpexpand needs the
// pod Running + sshd up, so a Pending pod must not advance the phase.
func TestScaleOut_ScalingSTS_WaitsWhenNewPodNotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 3

	state := scaleStateData{
		Phase: scalePhaseScalingSTS, OldCount: 2, NewCount: 3,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 2)
	pendingPod := pendingSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, pendingPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a Pending (not Running) new pod must keep the scaling-sts phase requeuing")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var got scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &got))
	assert.Equal(t, scalePhaseScalingSTS, got.Phase,
		"phase must NOT advance while the new pod is not Running")
}

// TestScaleOut_ScalingSTS_WaitsWhenExistingSegmentNotReady proves the gate still
// requires the PRE-existing segments to be Ready: a degraded existing member
// (ReadyReplicas < oldCount) blocks the expansion, so gpexpand's "all segments
// up" precondition holds. This preserves the non-scale readiness expectation for
// existing members.
func TestScaleOut_ScalingSTS_WaitsWhenExistingSegmentNotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 3

	state := scaleStateData{
		Phase: scalePhaseScalingSTS, OldCount: 2, NewCount: 3,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	// Only 1 of the 2 EXISTING segments Ready (degraded) even though the new pod
	// is Running: the gate must still wait.
	primarySts := scaleOutStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 3, 1)
	newPod := runningSegmentPod(cluster.Name, util.ComponentSegmentPrimary, 2)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, primarySts, newPod).
		WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter,
		"a degraded existing segment must keep the scaling-sts phase requeuing")
}

// TestAllSegmentStatefulSetsReady_NonScaleUnchanged proves the ORIGINAL,
// non-scale readiness gating (allSegmentStatefulSetsReady) is unchanged: it still
// requires FULL readiness (ReadyReplicas >= desired) and is independent of the
// relaxed scale-out gate. A partially-ready StatefulSet is NOT ready here.
func TestAllSegmentStatefulSetsReady_NonScaleUnchanged(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 3

	// Full readiness -> ready.
	full := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: util.SegmentPrimaryName(cluster.Name), Namespace: cluster.Namespace,
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
		Status: appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3},
	}
	clientFull := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, full).Build()
	rFull := NewClusterReconciler(clientFull, scheme, record.NewFakeRecorder(5),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	assert.True(t, rFull.allSegmentStatefulSetsReady(context.Background(), cluster),
		"non-scale readiness must remain true only at FULL readiness")

	// Partial readiness -> NOT ready (unchanged strict gate).
	partial := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: util.SegmentPrimaryName(cluster.Name), Namespace: cluster.Namespace,
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
		Status: appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 2},
	}
	clientPartial := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, partial).Build()
	rPartial := NewClusterReconciler(clientPartial, scheme, record.NewFakeRecorder(5),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	assert.False(t, rPartial.allSegmentStatefulSetsReady(context.Background(), cluster),
		"non-scale readiness must stay STRICT (ReadyReplicas >= desired)")
}

func int32Ptr(v int32) *int32 { return &v }
