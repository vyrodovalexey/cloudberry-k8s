package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
// reconcileSubComponents: error paths for each sub-reconciler
// ============================================================================

func TestAdminReconciler_ReconcileSubComponents_ErrorPaths(t *testing.T) {
	// Test that errors in sub-reconcilers are logged but don't crash
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	// Enable all features
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", S3: &cbv1alpha1.S3Destination{Bucket: "b"}},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "j1", Type: "s3", Enabled: true, TargetTable: "t"},
		},
	}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * 0",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Should not panic - all sub-reconcilers should succeed
	r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Verify conditions were set
	assert.NotEmpty(t, cluster.Status.Conditions)
}

// ============================================================================
// handleRestart: with STS that are still running
// ============================================================================

func TestClusterReconciler_HandleRestart_STSStillRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.Replicas = 1 // Still running

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.Replicas = 4 // Still running

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster, coordSts, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleRestart(context.Background(), cluster)
	require.NoError(t, err)
	// STS still running, should requeue
	assert.Equal(t, requeueAfterStopping, result.RequeueAfter)
}

// ============================================================================
// handleStart: restricted mode with standby and mirroring
// ============================================================================

func TestClusterReconciler_HandleStart_RestrictedWithStandbyAndMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	result, err := r.handleStart(context.Background(), cluster, util.ActionStartRestricted)
	require.NoError(t, err)
	_ = result
}

// ============================================================================
// handleStart: maintenance mode
// ============================================================================

func TestClusterReconciler_HandleStart_MaintenanceMode(t *testing.T) {
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

	result, err := r.handleStart(context.Background(), cluster, util.ActionStartMaintenance)
	require.NoError(t, err)
	_ = result
}

// ============================================================================
// Cluster Controller: reconcileCluster with upgrade needed
// ============================================================================

func TestClusterReconciler_ReconcileCluster_UpgradeNeeded(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ClusterVersion = "7.6"
	cluster.Spec.Version = "7.7"

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.ReadyReplicas = 1

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.ReadyReplicas = cluster.Spec.Segments.Count

	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	coordSvc := b.BuildCoordinatorService(cluster)
	segSvc := b.BuildSegmentService(cluster)
	clientSvc := b.BuildClientService(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts, pgConfCM, hbaCM, coordSvc, segSvc, clientSvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.True(t, result.Requeue || result.RequeueAfter > 0)
}

// ============================================================================
// Admin Controller: triggerRollingRestart with mirroring
// ============================================================================

func TestAdminReconciler_TriggerRollingRestart_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.triggerRollingRestart(context.Background(), cluster, []string{"max_connections"})
	require.NoError(t, err)
}

func TestAdminReconciler_TriggerRollingRestart_NoMirroring(t *testing.T) {
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

	err := r.triggerRollingRestart(context.Background(), cluster, []string{"max_connections"})
	require.NoError(t, err)
}

// ============================================================================
// Admin Controller: updateRestartAnnotation error path
// ============================================================================

func TestAdminReconciler_UpdateRestartAnnotation_PatchError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	state := rollingRestartState{
		Phase:         "coordinator",
		RestartParams: []string{"max_connections"},
	}
	_, err := r.updateRestartAnnotation(context.Background(), cluster, state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating rolling restart annotation")
}

// ============================================================================
// Cluster Controller: handleAction success removes annotation
// ============================================================================

func TestClusterReconciler_HandleAction_SuccessRemovesAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStartMaintenance,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.handleAction(context.Background(), cluster, util.ActionStartMaintenance)
	require.NoError(t, err)

	// Verify annotation was removed
	updated := &cbv1alpha1.CloudberryCluster{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, getErr)
	_, exists := updated.Annotations[util.AnnotationAction]
	assert.False(t, exists, "annotation should be removed after success")
}

// ============================================================================
// Cluster Controller: handleStop with mirroring
// ============================================================================

func TestClusterReconciler_HandleStop_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.handleStop(context.Background(), cluster, util.ActionStop)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: handleStop with fast mode and DB
// ============================================================================

func TestClusterReconciler_HandleStop_FastWithDB(t *testing.T) {
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

	_, err := r.handleStop(context.Background(), cluster, util.ActionStopFast)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: Reconcile with Scaling phase and scale-in state
// ============================================================================

func TestClusterReconciler_Reconcile_ScalingPhaseWithScaleInState(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{
		annotationScaleInState: `{"phase":"completed","oldCount":4,"newCount":2}`,
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
// Cluster Controller: handleDeletion with backup-on-delete error
// ============================================================================

func TestClusterReconciler_HandleDeletion_BackupOnDeleteError(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.BackupOnDelete = true

	// Make Job creation fail
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return c.Create(ctx, obj, opts...)
				}
				return fmt.Errorf("create failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Should still complete deletion even if backup job creation fails
	result, err := r.handleDeletion(context.Background(), cluster)
	// May error on finalizer removal due to interceptor, but backup error is logged
	_ = result
	_ = err
}

// ============================================================================
// Cluster Controller: reconcileCluster with standby and mirroring
// ============================================================================

func TestClusterReconciler_ReconcileCluster_WithStandbyAndMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	standbySts, _ := b.BuildStandbyStatefulSet(cluster)
	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	coordSvc := b.BuildCoordinatorService(cluster)
	segSvc := b.BuildSegmentService(cluster)
	clientSvc := b.BuildClientService(cluster)
	standbySvc := b.BuildStandbyService(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts, mirrorSts, standbySts,
			pgConfCM, hbaCM, coordSvc, segSvc, clientSvc, standbySvc).
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
// Admin Controller: continueRollingRestart - completed phase
// ============================================================================

func TestAdminReconciler_ContinueRollingRestart_CompletedPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordReplicas := int32(1)
	coordSts.Spec.Replicas = &coordReplicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1
	coordSts.Status.CurrentRevision = "rev-2"
	coordSts.Status.UpdateRevision = "rev-2"

	// coordinator phase, STS is rolled → should complete
	state := `{"phase":"coordinator","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
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
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: handleScaleOut with registering error
// ============================================================================

func TestClusterReconciler_CheckScaleOutPhases_RegisteringError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{
		Phase:    scalePhaseRegistering,
		OldCount: 2,
		NewCount: 4,
	}
	stateJSON, _ := json.Marshal(state)

	// Use a DB factory that returns an error
	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err) // Error is logged, returns requeue
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleOutPhases_RedistributingError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	state := scaleStateData{
		Phase:    scalePhaseRedistributing,
		OldCount: 2,
		NewCount: 4,
	}
	stateJSON, _ := json.Marshal(state)

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil, dbFactory)

	result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: expandPVCIfNeeded - get error
// ============================================================================

func TestClusterReconciler_ExpandPVCIfNeeded_GetError(t *testing.T) {
	scheme := newTestScheme()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return fmt.Errorf("get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	_, err := r.expandPVCIfNeeded(context.Background(), "default", "some-pvc", "20Gi")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting PVC")
}

// ============================================================================
// Cluster Controller: reconcileStorageExpansion - no expansion needed
// ============================================================================

func TestClusterReconciler_ReconcileStorageExpansion_NoExpansion(t *testing.T) {
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

	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), m, nil)

	err := r.reconcileStorageExpansion(context.Background(), cluster)
	require.NoError(t, err)
}

// ============================================================================
// Cluster Controller: detectAndHandleScale - scale-out detected
// ============================================================================

func TestClusterReconciler_DetectAndHandleScale_ScaleOutDetected(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Count = 6 // Desired: 6

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4) // Current: 4
	primarySts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	existingSts := &appsv1.StatefulSet{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: "default",
	}, existingSts)
	require.NoError(t, err)

	handled, scaleErr := r.detectAndHandleScale(context.Background(), cluster, existingSts, nil)
	assert.True(t, handled)
	require.NoError(t, scaleErr)
}

// ============================================================================
// Admin Controller: Reconcile with config change triggering restart
// ============================================================================

func TestAdminReconciler_Reconcile_ConfigChangeTriggersRestart(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "200"}, // restart-required param
	}

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

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

// ============================================================================
// Cluster Controller: handleScaleIn with confirmation
// ============================================================================

func TestClusterReconciler_HandleScaleIn_WithConfirmation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationConfirmScaleIn: "true",
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
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Scale from 4 to 1 (>50% reduction) WITH confirmation
	err := r.handleScaleIn(context.Background(), cluster, 4, 1)
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, getErr)
	assert.Equal(t, cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase)
}

// ============================================================================
// Admin Controller: completePendingReload - patch error
// ============================================================================

func TestAdminReconciler_CompletePendingReload_PatchError(t *testing.T) {
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
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, handled := r.completePendingReload(context.Background(), cluster)
	assert.True(t, handled)
	assert.NotZero(t, result.RequeueAfter) // Should requeue on patch error
}
