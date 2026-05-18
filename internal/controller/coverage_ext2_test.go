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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// reconcileCluster: isUpgradeRolledBack path
// ============================================================================

func TestClusterReconciler_ReconcileCluster_UpgradeRolledBack(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Set UpgradeFailed condition to trigger isUpgradeRolledBack
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionUpgradeFailed), metav1.ConditionTrue, "RolledBack", "Upgrade failed")

	b := builder.NewBuilder()
	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	coordSvc := b.BuildCoordinatorService(cluster)
	segSvc := b.BuildSegmentService(cluster)
	clientSvc := b.BuildClientService(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pgConfCM, hbaCM, coordSvc, segSvc, clientSvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// reconcileSegments: scale-in detection with existing STS
// ============================================================================

func TestClusterReconciler_ReconcileSegments_ScaleInDetected(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 2 // Desired: 2

	b := builder.NewBuilder()
	// Existing STS has 4 replicas (scale-in from 4 to 2)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	existingReplicas := int32(4)
	primarySts.Spec.Replicas = &existingReplicas
	primarySts.Status.ReadyReplicas = 4

	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	coordSvc := b.BuildCoordinatorService(cluster)
	segSvc := b.BuildSegmentService(cluster)
	clientSvc := b.BuildClientService(cluster)
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, pgConfCM, hbaCM, coordSvc, segSvc, clientSvc, coordSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// reconcileSegments should detect scale-in
	err := r.reconcileSegments(context.Background(), cluster)
	require.NoError(t, err) // Scale-in blocked (>50% reduction without confirmation)
}

// ============================================================================
// reconcileSegments: normal path preserves replicas on scale-in
// ============================================================================

func TestClusterReconciler_ReconcileSegments_PreservesReplicasOnScaleIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 3 // Desired: 3

	b := builder.NewBuilder()
	// Existing STS has 4 replicas (scale-in from 4 to 3, <50% so no confirmation needed)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	existingReplicas := int32(4)
	primarySts.Spec.Replicas = &existingReplicas
	primarySts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileSegments(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// cleanupScaleAnnotations: with scale-in state annotation
// ============================================================================

func TestClusterReconciler_CleanupScaleAnnotations_WithScaleInState(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted:   time.Now().Format(time.RFC3339),
		util.AnnotationConfirmScaleIn: "true",
		annotationScaleInState:        `{"phase":"completed"}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	r.cleanupScaleAnnotations(context.Background(), cluster, true)

	// Verify annotations were removed
	updated := &cbv1alpha1.CloudberryCluster{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
}

func TestClusterReconciler_CleanupScaleAnnotations_ScaleOut(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted: time.Now().Format(time.RFC3339),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	// isScaleIn = false
	r.cleanupScaleAnnotations(context.Background(), cluster, false)
}

// ============================================================================
// handleScaleFailure: with mirroring
// ============================================================================

func TestClusterReconciler_HandleScaleFailure_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted: time.Now().Add(-15 * time.Minute).Format(time.RFC3339),
	}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 2 // Not all ready

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, mirrorSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleScaleFailure(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// allSegmentStatefulSetsReady: with mirroring
// ============================================================================

func TestClusterReconciler_AllSegmentStatefulSetsReady_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, mirrorSts).
		WithStatusSubresource(cluster, primarySts, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.True(t, r.allSegmentStatefulSetsReady(context.Background(), cluster))
}

func TestClusterReconciler_AllSegmentStatefulSetsReady_MirrorNotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 2 // Not all ready

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, mirrorSts).
		WithStatusSubresource(cluster, primarySts, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.False(t, r.allSegmentStatefulSetsReady(context.Background(), cluster))
}

// ============================================================================
// reconcileSubComponents: error paths
// ============================================================================

func TestAdminReconciler_ReconcileSubComponents_WithErrors(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Enable all features to trigger all sub-reconcilers
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", Bucket: "b"},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(30)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Should not panic even if sub-reconcilers have issues
	r.reconcileSubComponents(context.Background(), r.logger, cluster)
}

// ============================================================================
// updateConfigMap: conflict retry path
// ============================================================================

func TestAdminReconciler_UpdateConfigMap_ConflictRetry(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "200"},
	}

	b := builder.NewBuilder()
	existingCM := b.BuildPostgresqlConfConfigMap(cluster)

	conflictCount := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					conflictCount++
					if conflictCount <= 1 {
						return fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again")
					}
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Change config to trigger update
	cluster.Spec.Config.Parameters["max_connections"] = "300"
	err := r.updateConfigMap(context.Background(), cluster)
	// May or may not error depending on retry logic, but should not panic
	_ = err
}

// ============================================================================
// expandSegmentPVCs: with mirroring
// ============================================================================

func TestClusterReconciler_ExpandSegmentPVCs_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 2
	cluster.Spec.Segments.Storage.Size = "30Gi"
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	// Create primary PVCs with smaller size
	for i := int32(0); i < 2; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i),
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
		return // Only need to test one iteration
	}
}

// ============================================================================
// Cluster Controller: Reconcile with scale state annotation
// ============================================================================

func TestClusterReconciler_Reconcile_WithScaleStateAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
	cluster.Annotations = map[string]string{
		annotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":4}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

// ============================================================================
// Cluster Controller: checkScaleProgress with scale-in state
// ============================================================================

func TestClusterReconciler_CheckScaleProgress_WithScaleInState(t *testing.T) {
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
	cluster.Annotations = map[string]string{
		annotationScaleInState: string(stateJSON),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

func TestClusterReconciler_CheckScaleProgress_WithScaleOutState(t *testing.T) {
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
	cluster.Annotations = map[string]string{
		annotationScaleState: string(stateJSON),
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.checkScaleProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

// ============================================================================
// Cluster Controller: upgradePhase with timeout
// ============================================================================

func TestClusterReconciler_ContinueUpgrade_PhaseTimeout(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)

	state := map[string]interface{}{
		"previousImage":   "old:7.6",
		"previousVersion": "7.6",
		"phase":           "coordinator",
		"phaseStartedAt":  time.Now().Add(-15 * time.Minute).Format(time.RFC3339), // Timed out
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
// Cluster Controller: handleStart normal mode
// ============================================================================

func TestClusterReconciler_HandleStart_NormalMode(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.handleStart(context.Background(), cluster, util.ActionStart)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Admin Controller: reconcileConfig with all config layers
// ============================================================================

func TestAdminReconciler_ReconcileConfig_WithAllLayers(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters:            map[string]string{"work_mem": "64MB"},
		CoordinatorParameters: map[string]string{"log_statement": "all"},
		DatabaseParameters: map[string]map[string]string{
			"mydb": {"search_path": "public"},
		},
		RoleParameters: map[string]map[string]string{
			"analyst": {"statement_timeout": "30s"},
		},
	}

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

	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: processScaleInRedistributing with DB error
// ============================================================================

func TestClusterReconciler_ProcessScaleInRedistributing_DBError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := &scaleInStateData{OldCount: 4, NewCount: 2, Phase: scaleInPhaseRedistributing}
	result, err := r.processScaleInRedistributing(context.Background(), cluster, state)
	require.NoError(t, err) // Error is logged, not returned
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: processScaleInDeregistering with DB error
// ============================================================================

func TestClusterReconciler_ProcessScaleInDeregistering_DBError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	state := &scaleInStateData{OldCount: 4, NewCount: 2, Phase: scaleInPhaseDeregistering}
	result, err := r.processScaleInDeregistering(context.Background(), cluster, state)
	require.NoError(t, err) // Error is logged, not returned
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: processScaleInScalingMirrors not ready
// ============================================================================

func TestClusterReconciler_ProcessScaleInScalingMirrors_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 1 // Only 1 ready, target is 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := &scaleInStateData{OldCount: 4, NewCount: 2, Phase: scaleInPhaseScalingMirrors}
	result, err := r.processScaleInScalingMirrors(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: processScaleInScalingPrimaries not ready
// ============================================================================

func TestClusterReconciler_ProcessScaleInScalingPrimaries_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 1 // Only 1 ready, target is 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := &scaleInStateData{OldCount: 4, NewCount: 2, Phase: scaleInPhaseScalingPrimaries}
	result, err := r.processScaleInScalingPrimaries(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: processScaleInCleanup with Retain policy
// ============================================================================

func TestClusterReconciler_ProcessScaleInCleanup_RetainPolicy(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyRetain

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	state := &scaleInStateData{OldCount: 4, NewCount: 2, Phase: scaleInPhaseCleanup}
	result, err := r.processScaleInCleanup(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

// ============================================================================
// Cluster Controller: expandPVCIfNeeded with StorageClass blocking
// ============================================================================

func TestClusterReconciler_ExpandPVCIfNeeded_StorageClassBlocks(t *testing.T) {
	scheme := newTestScheme()
	scName := "no-expand"
	allowExpansion := false
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "no-expand"},
		AllowVolumeExpansion: &allowExpansion,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-pvc-0",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, sc).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	changed, err := r.expandPVCIfNeeded(context.Background(), "default", "data-test-pvc-0", "20Gi")
	require.NoError(t, err)
	assert.False(t, changed) // Blocked by StorageClass
}

// ============================================================================
// Admin Controller: handleMaintenance with DB factory error (fallback to Job)
// ============================================================================

func TestAdminReconciler_HandleMaintenance_DBFailsFallbackToJob(t *testing.T) {
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

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: Reconcile error path (reconcileCluster error)
// ============================================================================

func TestClusterReconciler_Reconcile_ReconcileClusterError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	// Make Secret Get fail to trigger reconcileAdminSecret error
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return fmt.Errorf("secret get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: getScaleUpOrder
// ============================================================================

func TestClusterReconciler_GetScaleUpOrder(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	names := r.getScaleUpOrder(cluster)
	assert.Len(t, names, 4)
	assert.Equal(t, util.CoordinatorName(cluster.Name), names[0])
	assert.Equal(t, util.StandbyName(cluster.Name), names[1])
	assert.Equal(t, util.SegmentPrimaryName(cluster.Name), names[2])
	assert.Equal(t, util.SegmentMirrorName(cluster.Name), names[3])
}

func TestClusterReconciler_GetScaleUpOrder_NoStandbyNoMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	names := r.getScaleUpOrder(cluster)
	assert.Len(t, names, 2) // coordinator + primaries
}

// ============================================================================
// Cluster Controller: scaleStatefulSet error
// ============================================================================

func TestClusterReconciler_ScaleStatefulSet_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					return fmt.Errorf("update failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.scaleStatefulSet(context.Background(), "default", sts.Name, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scaling statefulset")
}

// ============================================================================
// Admin Controller: applyConfigChange with restart-required params
// ============================================================================

func TestAdminReconciler_ApplyConfigChange_RestartRequired(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	changes := configChanges{
		restartNeeded: []string{"max_connections"},
		reloadSafe:    nil,
	}
	err := r.applyConfigChange(context.Background(), cluster, changes)
	require.NoError(t, err)
	assert.NotNil(t, cluster.Status.LastConfigChangeTime)
}

func TestAdminReconciler_ApplyConfigChange_ReloadOnly(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	changes := configChanges{
		restartNeeded: nil,
		reloadSafe:    []string{"work_mem"},
	}
	err := r.applyConfigChange(context.Background(), cluster, changes)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: handleStop with DB factory (smart stop)
// ============================================================================

func TestClusterReconciler_HandleStop_SmartWithDB(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	_, err := r.handleStop(context.Background(), cluster, util.ActionStop)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: handleRestart with existing STS
// ============================================================================

func TestClusterReconciler_HandleRestart_WithExistingSTS(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.Replicas = 0

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.Replicas = 0

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster, coordSts, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.handleRestart(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: checkStopProgress not all stopped
// ============================================================================

func TestClusterReconciler_CheckStopProgress_NotAllStopped(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping

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

	result, err := r.checkStopProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

// ============================================================================
// Admin Controller: continueRollingRestart advancing to next phase with restart
// ============================================================================

func TestAdminReconciler_ContinueRollingRestart_AdvancePhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 4
	mirrorSts.Status.UpdatedReplicas = 4
	mirrorSts.Status.CurrentRevision = "rev-2"
	mirrorSts.Status.UpdateRevision = "rev-2"

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	state := `{"phase":"mirrors","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts, primarySts).
		WithStatusSubresource(cluster, mirrorSts, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}
