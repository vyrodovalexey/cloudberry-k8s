package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Cluster Controller: Scale-Out Multi-Phase Tests
// ============================================================================

func TestClusterReconciler_CheckScaleOutPhases_ScalingSTS_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 2 // Not all ready

	state := scaleStateData{
		Phase:     scalePhaseScalingSTS,
		OldCount:  2,
		NewCount:  4,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleOutPhases_ScalingSTS_Ready(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	state := scaleStateData{
		Phase:     scalePhaseScalingSTS,
		OldCount:  2,
		NewCount:  4,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter) // Advances to next phase
}

func TestClusterReconciler_CheckScaleOutPhases_Registering(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{
		Phase:     scalePhaseRegistering,
		OldCount:  2,
		NewCount:  4,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	// No dbFactory → registerNewSegments is a no-op
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleOutPhases_Redistributing(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{
		Phase:     scalePhaseRedistributing,
		OldCount:  2,
		NewCount:  4,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleOutPhases_Completed(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{
		Phase:     scalePhaseCompleted,
		OldCount:  2,
		NewCount:  4,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleOutPhases_InvalidJSON(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, "invalid-json")
	require.NoError(t, err)
	// Invalid JSON falls back to simple check; no STS exists = at scale = completes
	assert.True(t, result.Requeue || result.RequeueAfter > 0)
}

func TestClusterReconciler_CheckScaleOutPhases_UnknownPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{Phase: "unknown-phase", OldCount: 2, NewCount: 4}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: Scale-In Multi-Phase Tests
// ============================================================================

func TestClusterReconciler_CheckScaleInPhases_Redistributing(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleInStateData{
		Phase:     scaleInPhaseRedistributing,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	// No dbFactory → redistributeBeforeScaleIn is a no-op
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_Deregistering(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleInStateData{
		Phase:     scaleInPhaseDeregistering,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_DeregisteringWithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	state := scaleInStateData{
		Phase:     scaleInPhaseDeregistering,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_ScalingMirrors(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.Replicas = 2
	mirrorSts.Status.ReadyReplicas = 2

	state := scaleInStateData{
		Phase:     scaleInPhaseScalingMirrors,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_ScalingPrimaries(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.Replicas = 2
	primarySts.Status.ReadyReplicas = 2

	state := scaleInStateData{
		Phase:     scaleInPhaseScalingPrimaries,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_Cleanup(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	state := scaleInStateData{
		Phase:     scaleInPhaseCleanup,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_Completed(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleInStateData{
		Phase:     scaleInPhaseCompleted,
		OldCount:  4,
		NewCount:  2,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleInPhases_InvalidJSON(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, "invalid-json")
	require.NoError(t, err)
	// Invalid JSON falls back to simple check; no STS exists = at scale = completes
	assert.True(t, result.Requeue || result.RequeueAfter > 0)
}

func TestClusterReconciler_CheckScaleInPhases_UnknownPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleInStateData{Phase: "unknown-phase", OldCount: 4, NewCount: 2}
	stateJSON, _ := json.Marshal(state)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleInPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: DB-backed scale operations
// ============================================================================

func TestClusterReconciler_RegisterNewSegments_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := scaleStateData{OldCount: 2, NewCount: 4}
	err := r.registerNewSegments(context.Background(), cluster, state)
	require.NoError(t, err)
}

func TestClusterReconciler_RedistributeData_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	err := r.redistributeData(context.Background(), cluster)
	require.NoError(t, err)
}

func TestClusterReconciler_RedistributeBeforeScaleIn_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := scaleInStateData{OldCount: 4, NewCount: 2}
	err := r.redistributeBeforeScaleIn(context.Background(), cluster, state)
	require.NoError(t, err)
}

func TestClusterReconciler_DeregisterSegments_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := scaleInStateData{OldCount: 4, NewCount: 2}
	err := r.deregisterSegments(context.Background(), cluster, state)
	require.NoError(t, err)
}

func TestClusterReconciler_RedistributeBeforeScaleIn_DBError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := scaleInStateData{OldCount: 4, NewCount: 2}
	err := r.redistributeBeforeScaleIn(context.Background(), cluster, state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating db client")
}

// ============================================================================
// Cluster Controller: performGracefulShutdown
// ============================================================================

func TestClusterReconciler_PerformGracefulShutdown_SmartStop(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	// Should not panic
	r.performGracefulShutdown(context.Background(), cluster, util.ActionStop)
}

func TestClusterReconciler_PerformGracefulShutdown_FastStop(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	r.performGracefulShutdown(context.Background(), cluster, util.ActionStopFast)
}

func TestClusterReconciler_PerformGracefulShutdown_NilFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	// Should not panic with nil factory
	r.performGracefulShutdown(context.Background(), cluster, util.ActionStop)
}

func TestClusterReconciler_PerformGracefulShutdown_DBClientError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	// Should not panic, just log warning
	r.performGracefulShutdown(context.Background(), cluster, util.ActionStop)
}

// ============================================================================
// Cluster Controller: handleStop modes
// ============================================================================

func TestClusterReconciler_HandleStop_StopImmediate(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.handleStop(context.Background(), cluster, util.ActionStopImmediate)
	require.NoError(t, err)
}

func TestClusterReconciler_HandleStop_StopFast(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.handleStop(context.Background(), cluster, util.ActionStopFast)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: Storage Expansion
// ============================================================================

func TestClusterReconciler_ExpandPVCIfNeeded_Expansion(t *testing.T) {
	scheme := newTestScheme()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-pvc-0",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	// No StorageClass → expansion allowed by default
	changed, err := r.expandPVCIfNeeded(context.Background(), "default", "data-test-pvc-0", "20Gi")
	require.NoError(t, err)
	assert.True(t, changed)
}

func TestClusterReconciler_ExpandPVCIfNeeded_NoExpansionNeeded(t *testing.T) {
	scheme := newTestScheme()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-pvc-0",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("20Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	changed, err := r.expandPVCIfNeeded(context.Background(), "default", "data-test-pvc-0", "20Gi")
	require.NoError(t, err)
	assert.False(t, changed)
}

func TestClusterReconciler_ExpandStandbyPVC_WithStorage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Storage: &cbv1alpha1.StorageSpec{Size: "20Gi"},
	}

	// Create the standby PVC with smaller size
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.StandbyName(cluster.Name)),
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	changed, err := r.expandStandbyPVC(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, changed)
}

func TestClusterReconciler_ExpandStandbyPVC_NoStorage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Storage: nil, // No storage spec
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	changed, err := r.expandStandbyPVC(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, changed)
}

// ============================================================================
// Cluster Controller: preserveMirrorReplicasIfNeeded
// ============================================================================

func TestClusterReconciler_PreserveMirrorReplicasIfNeeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	existingReplicas := int32(4)
	mirrorSts.Spec.Replicas = &existingReplicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Build a desired mirror STS with fewer replicas
	desiredMirror, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	desiredReplicas := int32(2)
	desiredMirror.Spec.Replicas = &desiredReplicas

	r.preserveMirrorReplicasIfNeeded(context.Background(), desiredMirror, nil)

	// Should preserve the existing replicas (4, not 2)
	assert.Equal(t, int32(4), *desiredMirror.Spec.Replicas)
}

// ============================================================================
// Cluster Controller: getDesiredReplicas
// ============================================================================

func TestClusterReconciler_GetDesiredReplicas(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	tests := []struct {
		name     string
		stsName  string
		expected int32
	}{
		{"coordinator", util.CoordinatorName(cluster.Name), 1},
		{"standby", util.StandbyName(cluster.Name), 1},
		{"primary", util.SegmentPrimaryName(cluster.Name), 4},
		{"mirror", util.SegmentMirrorName(cluster.Name), 4},
		{"unknown", "unknown-sts", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas := r.getDesiredReplicas(cluster, tt.stsName)
			assert.Equal(t, tt.expected, replicas)
		})
	}
}

// ============================================================================
// Cluster Controller: handleGenerationUnchanged
// ============================================================================

func TestClusterReconciler_HandleGenerationUnchanged_ScaleStateAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Annotations = map[string]string{
		annotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":4}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	res := r.handleGenerationUnchanged(context.Background(), cluster)
	assert.True(t, res.handled)
	assert.Equal(t, requeueAfterImmediate, res.result.RequeueAfter)
}

func TestClusterReconciler_HandleGenerationUnchanged_ScaleInStateAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Annotations = map[string]string{
		annotationScaleInState: `{"phase":"redistributing","oldCount":4,"newCount":2}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	res := r.handleGenerationUnchanged(context.Background(), cluster)
	assert.True(t, res.handled)
	assert.Equal(t, requeueAfterImmediate, res.result.RequeueAfter)
}

func TestClusterReconciler_HandleGenerationUnchanged_ConfirmScaleIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Annotations = map[string]string{
		util.AnnotationConfirmScaleIn: annotationValueTrue,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	res := r.handleGenerationUnchanged(context.Background(), cluster)
	assert.False(t, res.handled) // Should NOT be handled, forces reconciliation
}

// ============================================================================
// Cluster Controller: handleAction error retention
// ============================================================================

func TestClusterReconciler_HandleAction_ErrorRetainsAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStop,
	}

	// Make status patch fail to simulate action error
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		// Don't register status subresource so status update fails
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.handleAction(context.Background(), cluster, util.ActionStop)
	// Error expected because status update fails
	require.Error(t, err)

	// Annotation should still be present (not removed on error)
	updated := &cbv1alpha1.CloudberryCluster{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, getErr)
	_, exists := updated.Annotations[util.AnnotationAction]
	assert.True(t, exists, "annotation should be retained on error")
}

// ============================================================================
// Cluster Controller: handleLifecyclePhase - Updating with mirroring
// ============================================================================

func TestClusterReconciler_HandleLifecyclePhase_UpdatingWithMirroringState(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Annotations = map[string]string{
		util.AnnotationMirroringState: `{"phase":"creating-sts"}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, handled, _ := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	_ = result
}

// ============================================================================
// Cluster Controller: handleScaleOut with mirroring
// ============================================================================

func TestClusterReconciler_HandleScaleOut_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(2)
	primarySts.Spec.Replicas = &replicas
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	mirrorSts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, mirrorSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleOut(context.Background(), cluster, 2, 4)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: upgradePhase
// ============================================================================

func TestClusterReconciler_ContinueUpgrade_WithValidState(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.ReadyReplicas = cluster.Spec.Segments.Count
	primarySts.Status.UpdatedReplicas = cluster.Spec.Segments.Count

	state := map[string]interface{}{
		"previousImage":   "old:7.6",
		"previousVersion": "7.6",
		"phase":           "coordinator",
		"phaseStartedAt":  time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)
	cluster.Annotations = map[string]string{
		util.AnnotationUpgrade: string(stateJSON),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.continueUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	_ = result
}

// ============================================================================
// HA Controller: isTableExcluded, splitSchemaTable, analyzeAndFilterSkew
// ============================================================================

func TestIsTableExcluded(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		patterns []string
		expected bool
	}{
		{"exact match", "public.users", []string{"public.users"}, true},
		{"no match", "public.users", []string{"public.orders"}, false},
		{"glob match table", "public.temp_data", []string{"temp_*"}, true},
		{"glob match schema.table", "public.audit_log", []string{"public.audit_*"}, true},
		{"empty patterns", "public.users", nil, false},
		{"empty patterns slice", "public.users", []string{}, false},
		{"wildcard all", "anything", []string{"*"}, true},
		{"no schema", "users", []string{"users"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTableExcluded(tt.table, tt.patterns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitSchemaTable(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"schema.table", "public.users", []string{"public", "users"}},
		{"no schema", "users", []string{"users"}},
		{"empty", "", []string{""}},
		{"multiple dots", "schema.table.extra", []string{"schema", "table.extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitSchemaTable(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHAReconciler_AnalyzeAndFilterSkew(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClient{}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	allTables, skewedTables := r.analyzeAndFilterSkew(
		context.Background(), dbClient, cluster, 10, nil)

	// mockDBClient returns empty skew results
	assert.Empty(t, allTables)
	assert.Empty(t, skewedTables)
}

func TestHAReconciler_AnalyzeAndFilterSkew_WithExclusions(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	// Create a mock that returns skew data
	dbClient := &mockDBClientWithSkew{
		mockDBClient: &mockDBClient{},
		skewResults: map[string][]db.TableSkewInfo{
			"postgres": {
				{Database: "postgres", Schema: "public", Table: "users", SkewCoefficient: 15.0, DistributionKey: "id"},
				{Database: "postgres", Schema: "public", Table: "temp_data", SkewCoefficient: 25.0, DistributionKey: "id"},
			},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	allTables, skewedTables := r.analyzeAndFilterSkew(
		context.Background(), dbClient, cluster, 10, []string{"temp_*"})

	assert.Len(t, allTables, 2)
	assert.Len(t, skewedTables, 1) // temp_data excluded
	assert.Equal(t, "users", skewedTables[0].Table)
}

// ============================================================================
// HA Controller: executeRebalanceViaDB
// ============================================================================

func TestHAReconciler_ExecuteRebalanceViaDB_NoSkewedTables(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.executeRebalanceViaDB(context.Background(), cluster, 10, 2, nil)
	require.NoError(t, err)
}

func TestHAReconciler_ExecuteRebalanceViaDB_WithSkewedTables(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithSkew{
		mockDBClient: &mockDBClient{},
		skewResults: map[string][]db.TableSkewInfo{
			"postgres": {
				{Database: "postgres", Schema: "public", Table: "users", SkewCoefficient: 15.0, DistributionKey: "id"},
			},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.executeRebalanceViaDB(context.Background(), cluster, 10, 2, nil)
	require.NoError(t, err)
}

func TestHAReconciler_ExecuteRebalanceViaDB_DBClientError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.executeRebalanceViaDB(context.Background(), cluster, 10, 2, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating db client")
}

// ============================================================================
// HA Controller: handleRebalance with DB factory
// ============================================================================

func TestHAReconciler_HandleRebalance_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	result, err := r.handleRebalance(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_HandleRebalance_WithRebalanceConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Rebalance = &cbv1alpha1.RebalanceSpec{
		Parallelism:   4,
		SkewThreshold: 20,
		ExcludeTables: []string{"temp_*"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	result, err := r.handleRebalance(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Admin Controller: executeMaintenanceViaDB
// ============================================================================

func TestAdminReconciler_ExecuteMaintenanceViaDB_AllOps(t *testing.T) {
	tests := []struct {
		name      string
		operation string
	}{
		{"vacuum", util.MaintenanceVacuum},
		{"vacuum-analyze", util.MaintenanceVacuumAnalyze},
		{"vacuum-full", util.MaintenanceVacuumFull},
		{"analyze", util.MaintenanceAnalyze},
		{"reindex", util.MaintenanceReindex},
		{"log-rotate", util.MaintenanceLogRotate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()
			recorder := record.NewFakeRecorder(10)
			b := builder.NewBuilder()
			m := &metrics.NoopRecorder{}

			dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
			r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

			err := r.executeMaintenanceViaDB(context.Background(), cluster, tt.operation)
			require.NoError(t, err)
		})
	}
}

func TestAdminReconciler_ExecuteMaintenanceViaDB_UnsupportedOp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.executeMaintenanceViaDB(context.Background(), cluster, "unsupported")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported maintenance operation")
}

func TestAdminReconciler_ExecuteMaintenanceViaDB_DBClientError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.executeMaintenanceViaDB(context.Background(), cluster, util.MaintenanceVacuum)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating database client")
}

// ============================================================================
// Admin Controller: handleMaintenance with DB factory
// ============================================================================

func TestAdminReconciler_HandleMaintenance_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_HandleMaintenance_LogRotate(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceLogRotate)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Admin Controller: completePendingReload
// ============================================================================

func TestAdminReconciler_CompletePendingReload_NoPending(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	_, handled := r.completePendingReload(context.Background(), cluster)
	assert.False(t, handled)
}

func TestAdminReconciler_CompletePendingReload_WaitingForPropagation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: time.Now().UTC().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled)
	assert.NotZero(t, result.RequeueAfter) // Waiting for propagation
}

func TestAdminReconciler_CompletePendingReload_Ready(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Set timestamp in the past (>30s ago)
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: time.Now().Add(-60 * time.Second).UTC().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	_, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled)
}

func TestAdminReconciler_CompletePendingReload_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: time.Now().Add(-60 * time.Second).UTC().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	_, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled)
}

func TestAdminReconciler_CompletePendingReload_InvalidTimestamp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: "invalid-timestamp",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	_, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled) // Should still handle (execute reload now)
}

func TestAdminReconciler_CompletePendingReload_DBFactoryError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: time.Now().Add(-60 * time.Second).UTC().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	_, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled) // Should still handle even with DB error
}

// ============================================================================
// Admin Controller: computeFullConfigHash
// ============================================================================

func TestAdminReconciler_ComputeFullConfigHash(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	config := &cbv1alpha1.ConfigSpec{
		Parameters:            map[string]string{"max_connections": "200"},
		CoordinatorParameters: map[string]string{"work_mem": "64MB"},
		DatabaseParameters: map[string]map[string]string{
			"mydb": {"search_path": "public"},
		},
		RoleParameters: map[string]map[string]string{
			"analyst": {"statement_timeout": "30s"},
		},
	}

	hash1 := r.computeFullConfigHash(config)
	assert.NotEmpty(t, hash1)

	// Same config should produce same hash
	hash2 := r.computeFullConfigHash(config)
	assert.Equal(t, hash1, hash2)

	// Different config should produce different hash
	config2 := &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "300"},
	}
	hash3 := r.computeFullConfigHash(config2)
	assert.NotEqual(t, hash1, hash3)
}

// ============================================================================
// Admin Controller: applyCoordinatorParameters, applyDatabaseParameters, applyRoleParameters
// ============================================================================

func TestAdminReconciler_ApplyCoordinatorParameters(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		CoordinatorParameters: map[string]string{
			"work_mem": "64MB",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	mockClient := &mockDBClient{}
	err := r.applyCoordinatorParameters(context.Background(), cluster, mockClient)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyCoordinatorParameters_Empty(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		CoordinatorParameters: map[string]string{},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyCoordinatorParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyCoordinatorParameters_NilDBClient(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		CoordinatorParameters: map[string]string{"work_mem": "64MB"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyCoordinatorParameters(context.Background(), cluster, nil)
	require.NoError(t, err) // Should return nil when no shared DB client
}

func TestAdminReconciler_ApplyDatabaseParameters(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		DatabaseParameters: map[string]map[string]string{
			"mydb": {"search_path": "public,analytics"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	mockClient := &mockDBClient{}
	err := r.applyDatabaseParameters(context.Background(), cluster, mockClient)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyDatabaseParameters_Empty(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		DatabaseParameters: map[string]map[string]string{},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyDatabaseParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyDatabaseParameters_NilDBClient(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		DatabaseParameters: map[string]map[string]string{
			"mydb": {"search_path": "public"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyDatabaseParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyRoleParameters(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		RoleParameters: map[string]map[string]string{
			"analyst": {"statement_timeout": "30s"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	mockClient := &mockDBClient{}
	err := r.applyRoleParameters(context.Background(), cluster, mockClient)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyRoleParameters_Empty(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		RoleParameters: map[string]map[string]string{},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyRoleParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyRoleParameters_NilDBClient(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		RoleParameters: map[string]map[string]string{
			"analyst": {"statement_timeout": "30s"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyRoleParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyCoordinatorParameters_NilSharedClient(t *testing.T) {
	// When sharedClient is nil but parameters exist, should return nil (skip).
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		CoordinatorParameters: map[string]string{"work_mem": "64MB"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyCoordinatorParameters(context.Background(), cluster, nil)
	require.NoError(t, err)
}

// ============================================================================
// Admin Controller: continueRollingRestart edge cases
// ============================================================================

func TestAdminReconciler_ContinueRollingRestart_SkipPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// No mirroring, no standby → mirrors and standby phases should be skipped

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordReplicas := int32(1)
	coordSts.Spec.Replicas = &coordReplicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1
	coordSts.Status.CurrentRevision = "rev-2"
	coordSts.Status.UpdateRevision = "rev-2"

	state := `{"phase":"mirrors","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts).
		WithStatusSubresource(cluster, coordSts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ContinueRollingRestart_STSNotFound(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	// mirrors phase but no mirror STS exists
	state := `{"phase":"mirrors","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Admin Controller: Reconcile with pending reload
// ============================================================================

func TestAdminReconciler_Reconcile_WithPendingReload(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
	cluster.Annotations = map[string]string{
		util.AnnotationPendingReload: time.Now().Add(-60 * time.Second).UTC().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	_ = result
}

// ============================================================================
// mockDBClientWithSkew - extends mockDBClient with configurable AnalyzeSkew
// ============================================================================

type mockDBClientWithSkew struct {
	*mockDBClient
	skewResults map[string][]db.TableSkewInfo
}

func (m *mockDBClientWithSkew) AnalyzeSkew(_ context.Context, database string) ([]db.TableSkewInfo, error) {
	if results, ok := m.skewResults[database]; ok {
		return results, nil
	}
	return []db.TableSkewInfo{}, nil
}

func (m *mockDBClientWithSkew) ListUserDatabases(_ context.Context) ([]string, error) {
	var dbs []string
	for db := range m.skewResults {
		dbs = append(dbs, db)
	}
	if len(dbs) == 0 {
		return []string{"postgres"}, nil
	}
	return dbs, nil
}

// ============================================================================
// Cluster Controller: reconcileStorageExpansion full flow
// ============================================================================

func TestClusterReconciler_ReconcileStorageExpansion_WithExpansion(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Coordinator.Storage.Size = "20Gi"
	cluster.Spec.Segments.Storage.Size = "30Gi"

	// Create coordinator PVC with smaller size
	coordPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name)),
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordPVC).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	err := r.reconcileStorageExpansion(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: detectAndHandleScale
// ============================================================================

func TestClusterReconciler_DetectAndHandleScale_NilReplicas(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	existingSts := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: nil,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	handled, err := r.detectAndHandleScale(context.Background(), cluster, existingSts, nil)
	assert.False(t, handled)
	require.NoError(t, err)
}

func TestClusterReconciler_DetectAndHandleScale_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	handled, err := r.detectAndHandleScale(context.Background(), cluster, nil, fmt.Errorf("not found"))
	assert.False(t, handled)
	require.NoError(t, err)
}

func TestClusterReconciler_DetectAndHandleScale_ZeroReplicas(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4

	replicas := int32(0)
	existingSts := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	// Zero replicas = restart/initial creation, not scale
	handled, err := r.detectAndHandleScale(context.Background(), cluster, existingSts, nil)
	assert.False(t, handled)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: StorageClass annotation fallback
// ============================================================================

func TestClusterReconciler_StorageClassSupportsExpansion_AnnotationFallback(t *testing.T) {
	scheme := newTestScheme()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-class": "nonexistent-sc",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	supported, reason := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.False(t, supported)
	assert.Contains(t, reason, "not found")
}

func TestClusterReconciler_StorageClassSupportsExpansion_NilExpansion(t *testing.T) {
	scheme := newTestScheme()
	scName := "standard"
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "standard"},
		AllowVolumeExpansion: nil, // nil = not supported
	}
	pvc := &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sc).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	supported, reason := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.False(t, supported)
	assert.Contains(t, reason, "allowVolumeExpansion=false")
}
