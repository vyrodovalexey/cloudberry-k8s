package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Cluster Controller: upgradePhase with various phases
// ============================================================================

func TestClusterReconciler_ContinueUpgrade_PrimariesPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.ReadyReplicas = cluster.Spec.Segments.Count
	primarySts.Status.UpdatedReplicas = cluster.Spec.Segments.Count

	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1

	state := map[string]interface{}{
		"previousImage":   "old:7.6",
		"previousVersion": "7.6",
		"phase":           "primaries",
		"phaseStartedAt":  time.Now().Format(time.RFC3339),
	}
	stateJSON, _ := json.Marshal(state)
	cluster.Annotations = map[string]string{
		util.AnnotationUpgrade: string(stateJSON),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, coordSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.continueUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	_ = result
}

func TestClusterReconciler_ContinueUpgrade_VerifyPhase(t *testing.T) {
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
		"phase":           "verify",
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
// Cluster Controller: expandSegmentPVCs with error
// ============================================================================

func TestClusterReconciler_ExpandSegmentPVCs_PrimaryExpansion(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 1
	cluster.Spec.Segments.Storage.Size = "30Gi"

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.SegmentPrimaryName(cluster.Name)),
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
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	expanded := r.expandSegmentPVCs(context.Background(), cluster)
	assert.True(t, expanded)
}

// ============================================================================
// Cluster Controller: reconcileStorageExpansion with standby
// ============================================================================

func TestClusterReconciler_ReconcileStorageExpansion_WithStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Storage: &cbv1alpha1.StorageSpec{Size: "20Gi"},
	}

	standbyPVC := &corev1.PersistentVolumeClaim{
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
		WithObjects(cluster, standbyPVC).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	err := r.reconcileStorageExpansion(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: handleScaleOut with STS build error
// ============================================================================

func TestClusterReconciler_HandleScaleOut_BuildError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Storage.Size = "invalid-size" // Will cause build error

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	err := r.handleScaleOut(context.Background(), cluster, 2, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "building primary segment StatefulSet")
}

// ============================================================================
// Cluster Controller: cleanupOrphanedPVCs with delete error
// ============================================================================

func TestClusterReconciler_CleanupOrphanedPVCs_WithPVCs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 2

	// Create PVCs for primary segments 2 and 3
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-2", util.SanitizeK8sName(fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentPrimary))),
			Namespace: "default",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc2).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	r.cleanupOrphanedPVCs(context.Background(), cluster, 2)

	// Verify PVC was deleted
	pvc := &corev1.PersistentVolumeClaim{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      pvc2.Name,
		Namespace: "default",
	}, pvc)
	assert.Error(t, err) // Should be not found
}

// ============================================================================
// Cluster Controller: handleStop with STS that need scaling
// ============================================================================

func TestClusterReconciler_HandleStop_WithExistingSTS(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.Replicas = 1

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.Replicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster, coordSts, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleStop(context.Background(), cluster, util.ActionStop)
	require.NoError(t, err)
	// STS still running, should requeue
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: updateObservedGeneration with blocked scale-in
// ============================================================================

func TestClusterReconciler_UpdateObservedGeneration_BlockedScaleIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 3
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 2 // Desired: 2

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4) // Current: 4 (scale-in pending)
	primarySts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	r.updateObservedGeneration(context.Background(), cluster)
	// Should NOT advance ObservedGeneration due to blocked scale-in
	assert.NotEqual(t, int64(3), cluster.Status.ObservedGeneration)
}

func TestClusterReconciler_UpdateObservedGeneration_WithScaleInState(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 3
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 2
	cluster.Annotations = map[string]string{
		annotationScaleInState: `{"phase":"redistributing"}`,
	}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	r.updateObservedGeneration(context.Background(), cluster)
	// Should advance because scale-in state annotation is present
	assert.Equal(t, int64(3), cluster.Status.ObservedGeneration)
}

// ============================================================================
// Cluster Controller: handleStart with standby
// ============================================================================

func TestClusterReconciler_HandleStart_RestrictedWithStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(0)
	coordSts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleStart(context.Background(), cluster, util.ActionStartRestricted)
	require.NoError(t, err)
	_ = result
}

// ============================================================================
// Cluster Controller: completeRestart
// ============================================================================

func TestClusterReconciler_CompleteRestart(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	cluster.Annotations = map[string]string{
		util.AnnotationRestartPending: "true",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.completeRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: expandSegmentPVCs with mirroring expansion
// ============================================================================

func TestClusterReconciler_ExpandSegmentPVCs_MirrorExpansion(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 1
	cluster.Spec.Segments.Storage.Size = "30Gi"
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	mirrorPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.SegmentMirrorName(cluster.Name)),
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
		WithObjects(cluster, mirrorPVC).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	expanded := r.expandSegmentPVCs(context.Background(), cluster)
	assert.True(t, expanded)
}

// ============================================================================
// Cluster Controller: allStatefulSetsAtScale with STS not at scale
// ============================================================================

func TestClusterReconciler_AllStatefulSetsAtScale_NotAtScale(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.Replicas = 1 // Still running

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts).
		WithStatusSubresource(cluster, coordSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.False(t, r.allStatefulSetsAtScale(context.Background(), cluster, 0))
}

// ============================================================================
// Admin Controller: handleMaintenance with existing job (already exists)
// ============================================================================

func TestAdminReconciler_HandleMaintenance_JobAlreadyExists(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: isStatefulSetAtScale with get error
// ============================================================================

func TestClusterReconciler_IsStatefulSetAtScale_GetError(t *testing.T) {
	scheme := newTestScheme()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	// Non-existent STS returns true (at scale)
	ready, err := r.isStatefulSetAtScale(context.Background(), "default", "nonexistent", 0)
	require.NoError(t, err)
	assert.True(t, ready)
}

// ============================================================================
// Cluster Controller: handleScaleOut with STS that already exists
// ============================================================================

func TestClusterReconciler_HandleScaleOut_ExistingSTS(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 6

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleOut(context.Background(), cluster, 4, 6)
	require.NoError(t, err)
}

// ============================================================================
// Admin Controller: reconcileConfig with restart-required param change
// ============================================================================

func TestAdminReconciler_ReconcileConfig_RestartRequiredChange(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"work_mem": "64MB"},
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

	// First reconcile sets the hash
	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)

	// Change to a restart-required param
	cluster.Spec.Config.Parameters["shared_buffers"] = "256MB"

	b2 := builder.NewBuilder()
	primarySts, _ := b2.BuildSegmentPrimaryStatefulSet(cluster)

	// Need to create the STS for triggerRollingRestart
	err = k8sClient.Create(context.Background(), primarySts)
	require.NoError(t, err)

	err = r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}
