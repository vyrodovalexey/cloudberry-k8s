package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	return scheme
}

func newTestCluster() *cbv1alpha1.CloudberryCluster {
	replicas := int32(1)
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cluster",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Replicas: &replicas,
				Storage:  cbv1alpha1.StorageSpec{Size: "10Gi"},
				Port:     5432,
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:   4,
				Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
			},
			DeletionPolicy: cbv1alpha1.DeletionPolicyRetain,
		},
	}
}

func TestNewClusterReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)
	require.NotNil(t, r)
}

func TestClusterReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_Reconcile_AddFinalizer(t *testing.T) {
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

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.True(t, result.Requeue)

	// Verify finalizer was added
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Contains(t, updated.Finalizers, util.FinalizerName)
}

func TestClusterReconciler_Reconcile_SetInitialPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.True(t, result.Requeue)

	// Verify phase was set to Pending
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhasePending, updated.Status.Phase)
}

func TestClusterReconciler_Reconcile_FullReconciliation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify resources were created
	coordSts := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.CoordinatorName("test-cluster"),
		Namespace: "default",
	}, coordSts)
	require.NoError(t, err)

	// Verify services were created
	coordSvc := &corev1.Service{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.CoordinatorServiceName("test-cluster"),
		Namespace: "default",
	}, coordSvc)
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleAction(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRestart,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleDeletion(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_Reconcile_UnknownAction(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: "unknown-action",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_Reconcile_WithStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify standby StatefulSet was created
	standbySts := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.StandbyName("test-cluster"),
		Namespace: "default",
	}, standbySts)
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleAction_Start(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStart,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleAction_Stop(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStop,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleAction_StartRestricted(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStartRestricted,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleAction_StartMaintenance(t *testing.T) {
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
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_HandleDeletion_DeletePolicy(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_Reconcile_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify mirror StatefulSet was created
	mirrorSts := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.SegmentMirrorName("test-cluster"),
		Namespace: "default",
	}, mirrorSts)
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_SecondReconcileUpdatesResources(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// First reconcile creates resources.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	// Second reconcile should update existing resources (covers update paths).
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
}

func TestClusterReconciler_DeletePVCs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}

	// Create PVCs that belong to the cluster.
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-cluster-0", Namespace: "default",
			Labels: map[string]string{util.LabelCluster: "test-cluster"},
		},
	}
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-cluster-1", Namespace: "default",
			Labels: map[string]string{util.LabelCluster: "test-cluster"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc1, pvc2).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.deletePVCs(context.Background(), cluster)
	require.NoError(t, err)

	// Verify PVCs were deleted.
	pvcList := &corev1.PersistentVolumeClaimList{}
	err = k8sClient.List(context.Background(), pvcList)
	require.NoError(t, err)
	assert.Empty(t, pvcList.Items)
}

func TestClusterReconciler_Reconcile_UpdateExistingResources(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()

	// Pre-create resources with different specs to trigger update paths.
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordSts.Spec.Template.Spec.Containers = nil // Make it different to trigger update.
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Template.Spec.Containers = nil // Make it different.
	coordSvc := b.BuildCoordinatorService(cluster)
	coordSvc.Spec.Ports = nil // Make it different.
	standbySvc := b.BuildStandbyService(cluster)
	standbySvc.Spec.Ports = nil
	segSvc := b.BuildSegmentService(cluster)
	segSvc.Spec.Ports = nil
	clientSvc := b.BuildClientService(cluster)
	clientSvc.Spec.Ports = nil
	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	pgConfCM.Data = map[string]string{"old": "data"} // Make it different.
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	hbaCM.Data = map[string]string{"old": "hba"} // Make it different.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts, coordSvc, standbySvc, segSvc, clientSvc, pgConfCM, hbaCM).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// This reconcile should hit the update paths for all resources.
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_Reconcile_StandbyDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: false}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_Reconcile_MirroringDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_Reconcile_WithMirroringStatusUpdate(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true, Layout: cbv1alpha1.MirroringLayoutGroup,
	}
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	b := builder.NewBuilder()

	// Pre-create mirror StatefulSet with ready replicas.
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 4

	// Pre-create standby StatefulSet with ready replicas.
	standbySts, _ := b.BuildStandbyStatefulSet(cluster)
	standbyReplicas := int32(1)
	standbySts.Spec.Replicas = &standbyReplicas
	standbySts.Status.ReadyReplicas = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts, standbySts).
		WithStatusSubresource(cluster, mirrorSts, standbySts).
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

func TestClusterReconciler_Reconcile_RunningWithAllReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	b := builder.NewBuilder()

	// Pre-create coordinator StatefulSet with ready replicas.
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordSts.Status.ReadyReplicas = 1

	// Pre-create primary segment StatefulSet with ready replicas.
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster, coordSts, primarySts).
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

func TestClusterReconciler_Reconcile_WithStandbyAndMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true, Layout: cbv1alpha1.MirroringLayoutSpread,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify all StatefulSets were created.
	for _, name := range []string{
		util.CoordinatorName("test-cluster"),
		util.StandbyName("test-cluster"),
		util.SegmentPrimaryName("test-cluster"),
		util.SegmentMirrorName("test-cluster"),
	} {
		sts := &appsv1.StatefulSet{}
		err = k8sClient.Get(context.Background(), types.NamespacedName{
			Name: name, Namespace: "default",
		}, sts)
		require.NoError(t, err, "StatefulSet %s should exist", name)
	}
}

func TestClusterReconciler_Reconcile_ObservedGenerationSkip(t *testing.T) {
	// When ObservedGeneration matches Generation and phase is Running,
	// reconciliation should be skipped.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestClusterReconciler_Reconcile_ObservedGenerationNotSkippedWhenNotRunning(t *testing.T) {
	// When ObservedGeneration matches but phase is NOT Running, should NOT skip.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Status.ObservedGeneration = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	// Should proceed with reconciliation (not skip), and succeed.
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_HandleDeletion_NoFinalizer(t *testing.T) {
	// Test handleDeletion directly when cluster has no finalizer.
	scheme := newTestScheme()
	cluster := newTestCluster()
	// No finalizer set.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleDeletion(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_HandleDeletion_AlreadyDeletingPhase(t *testing.T) {
	// When cluster is already in Deleting phase, should skip updatePhase.
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseDeleting

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_HandleDeletion_DeletePolicyWithPVCs(t *testing.T) {
	// Deletion with Delete policy and existing PVCs.
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-cluster-0", Namespace: "default",
			Labels: map[string]string{util.LabelCluster: "test-cluster"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)

	// Verify PVCs were deleted.
	pvcList := &corev1.PersistentVolumeClaimList{}
	err = k8sClient.List(context.Background(), pvcList)
	require.NoError(t, err)
	assert.Empty(t, pvcList.Items)
}

func TestClusterReconciler_UpdateStatus_DeletingPhasePreserved(t *testing.T) {
	// When phase is Deleting, updateStatus should not change it.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseDeleting

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseDeleting, cluster.Status.Phase)
}

func TestClusterReconciler_UpdateStatus_PendingToInitializing(t *testing.T) {
	// When phase is Pending and components are not ready, should transition to Initializing.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseInitializing, cluster.Status.Phase)
}

func TestClusterReconciler_ReconcileCluster_ConfigMapErrors(t *testing.T) {
	// Test that reconcileCluster returns error when configmap reconciliation fails.
	// We use an invalid storage size to make the builder return nil for coordinator STS.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Coordinator.Storage.Size = "invalid-size"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// This should fail at reconcileCoordinator because builder returns nil.
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconciling coordinator")
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestClusterReconciler_ReconcileSegments_BuilderReturnsNil(t *testing.T) {
	// Test that reconcileSegments returns error when builder returns nil for primary STS.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Storage.Size = "invalid-size"

	// Pre-create coordinator resources so we get past that step.
	b := builder.NewBuilder()
	validCluster := newTestCluster()
	validCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	coordSts, _ := b.BuildCoordinatorStatefulSet(validCluster)
	pgConfCM := b.BuildPostgresqlConfConfigMap(validCluster)
	hbaCM := b.BuildPgHBAConfConfigMap(validCluster)
	coordSvc := b.BuildCoordinatorService(validCluster)
	segSvc := b.BuildSegmentService(validCluster)
	clientSvc := b.BuildClientService(validCluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, pgConfCM, hbaCM, coordSvc, segSvc, clientSvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconciling segments")
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestClusterReconciler_RecordReconcileResult(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	cluster := newTestCluster()
	// Should not panic.
	r.recordReconcileResult(cluster, time.Now(), "success")
	r.recordReconcileResult(cluster, time.Now(), "error")
}

func TestClusterReconciler_UpdateStatus_MirroringInSync(t *testing.T) {
	// Test mirroring status when all mirror replicas are ready.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.MirroringInSync, cluster.Status.MirroringStatus)
}

func TestClusterReconciler_UpdateStatus_MirroringDegraded(t *testing.T) {
	// Test mirroring status when not all mirror replicas are ready.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	mirrorSts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 2 // Not all ready

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorSts).
		WithStatusSubresource(cluster, mirrorSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.MirroringDegraded, cluster.Status.MirroringStatus)
}

func TestClusterReconciler_UpdateStatus_MirroringNotConfigured(t *testing.T) {
	// Test mirroring status when mirroring is not configured.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.MirroringNotConfigured, cluster.Status.MirroringStatus)
}

func TestClusterReconciler_UpdateStatus_StandbyReadiness(t *testing.T) {
	// Test standby readiness check in updateStatus.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	b := builder.NewBuilder()
	standbySts, _ := b.BuildStandbyStatefulSet(cluster)
	standbySts.Status.ReadyReplicas = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, standbySts).
		WithStatusSubresource(cluster, standbySts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatus(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, cluster.Status.StandbyReady)
}

func TestClusterReconciler_Reconcile_StandbyServiceCreatedWhenEnabled(t *testing.T) {
	// Verify standby service is created when standby is enabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	// Verify standby service was created.
	standbySvc := &corev1.Service{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.StandbyServiceName("test-cluster"),
		Namespace: "default",
	}, standbySvc)
	require.NoError(t, err)
}

func TestClusterReconciler_Reconcile_StandbyServiceNotCreatedWhenDisabled(t *testing.T) {
	// Verify standby service is NOT created when standby is disabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	// No standby spec

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	// Verify standby service was NOT created (only 3 services: coord, segment, client).
	svcList := &corev1.ServiceList{}
	err = k8sClient.List(context.Background(), svcList)
	require.NoError(t, err)
	assert.Equal(t, 3, len(svcList.Items))
}

func TestClusterReconciler_Reconcile_GetError(t *testing.T) {
	// Test that a non-NotFound Get error is returned.
	scheme := newTestScheme()
	cluster := newTestCluster()

	getErr := fmt.Errorf("connection refused")
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return getErr
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching cluster")
}

func TestClusterReconciler_CreateOrUpdateStatefulSet_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	createCallCount := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					createCallCount++
					return fmt.Errorf("create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired, _ := b.BuildCoordinatorStatefulSet(cluster)
	err := r.createOrUpdateStatefulSet(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating statefulset")
}

func TestClusterReconciler_CreateOrUpdateStatefulSet_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					return fmt.Errorf("get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired, _ := b.BuildCoordinatorStatefulSet(cluster)
	err := r.createOrUpdateStatefulSet(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting statefulset")
}

func TestClusterReconciler_CreateOrUpdateStatefulSet_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()

	// Pre-create with different spec to trigger update.
	existing, _ := b.BuildCoordinatorStatefulSet(cluster)
	existing.Spec.Template.Spec.Containers = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
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

	desired, _ := b.BuildCoordinatorStatefulSet(cluster)
	err := r.createOrUpdateStatefulSet(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating statefulset")
}

func TestClusterReconciler_CreateOrUpdateConfigMap_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildPostgresqlConfConfigMap(cluster)
	err := r.createOrUpdateConfigMap(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating configmap")
}

func TestClusterReconciler_CreateOrUpdateConfigMap_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildPostgresqlConfConfigMap(cluster)
	err := r.createOrUpdateConfigMap(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting configmap")
}

func TestClusterReconciler_CreateOrUpdateConfigMap_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()

	// Pre-create with different data to trigger update.
	existing := b.BuildPostgresqlConfConfigMap(cluster)
	existing.Data = map[string]string{"old": "data"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("update failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildPostgresqlConfConfigMap(cluster)
	err := r.createOrUpdateConfigMap(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating configmap")
}

func TestClusterReconciler_CreateOrUpdateService_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return fmt.Errorf("create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildCoordinatorService(cluster)
	err := r.createOrUpdateService(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating service")
}

func TestClusterReconciler_CreateOrUpdateService_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return fmt.Errorf("get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildCoordinatorService(cluster)
	err := r.createOrUpdateService(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting service")
}

func TestClusterReconciler_CreateOrUpdateService_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()

	// Pre-create with different ports to trigger update.
	existing := b.BuildCoordinatorService(cluster)
	existing.Spec.Ports = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return fmt.Errorf("update failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	desired := b.BuildCoordinatorService(cluster)
	err := r.createOrUpdateService(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating service")
}

func TestClusterReconciler_DeletePVCs_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return fmt.Errorf("list failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.deletePVCs(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing PVCs")
}

func TestClusterReconciler_DeletePVCs_DeleteError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-cluster-0", Namespace: "default",
			Labels: map[string]string{util.LabelCluster: "test-cluster"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				return fmt.Errorf("delete failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.deletePVCs(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleting PVC")
}

func TestClusterReconciler_ReconcileCluster_ServiceError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)

	createCallCount := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pgConfCM, hbaCM).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					createCallCount++
					return fmt.Errorf("service create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconciling services")
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestClusterReconciler_ReconcileCluster_ConfigMapError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("configmap create failed")
				}
				return c.Create(ctx, obj, opts...)
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
	assert.Contains(t, err.Error(), "reconciling configmaps")
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestClusterReconciler_ReconcileAdminSecret_CreatesSecret(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileAdminSecret(context.Background(), cluster)
	require.NoError(t, err)

	// Verify secret was created.
	secret := &corev1.Secret{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.AdminPasswordSecretName("test-cluster"),
		Namespace: "default",
	}, secret)
	require.NoError(t, err)
	assert.NotEmpty(t, secret.Data["password"])
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)
}

func TestClusterReconciler_ReconcileAdminSecret_ExistingSecretNotOverwritten(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	// Pre-create the secret with a known password.
	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte("user-provided-password"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingSecret).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileAdminSecret(context.Background(), cluster)
	require.NoError(t, err)

	// Verify secret was NOT overwritten.
	secret := &corev1.Secret{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.AdminPasswordSecretName("test-cluster"),
		Namespace: "default",
	}, secret)
	require.NoError(t, err)
	assert.Equal(t, []byte("user-provided-password"), secret.Data["password"])
}

func TestClusterReconciler_ReconcileAdminSecret_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return fmt.Errorf("secret create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileAdminSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating admin password secret")
}

func TestClusterReconciler_ReconcileAdminSecret_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

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

	err := r.reconcileAdminSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting admin password secret")
}

func TestClusterReconciler_FullReconciliation_CreatesAdminSecret(t *testing.T) {
	// Verify that full reconciliation creates the admin secret.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	// Verify admin secret was created as part of reconciliation.
	secret := &corev1.Secret{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.AdminPasswordSecretName("test-cluster"),
		Namespace: "default",
	}, secret)
	require.NoError(t, err)
	assert.NotEmpty(t, secret.Data["password"])
}

func TestClusterReconciler_ReconcileCluster_StandbyError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Resources: &cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "invalid-cpu"},
		},
	}

	b := builder.NewBuilder()
	// Pre-create coordinator and segment resources so we get past those steps.
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	pgConfCM := b.BuildPostgresqlConfConfigMap(cluster)
	hbaCM := b.BuildPgHBAConfConfigMap(cluster)
	coordSvc := b.BuildCoordinatorService(cluster)
	segSvc := b.BuildSegmentService(cluster)
	clientSvc := b.BuildClientService(cluster)
	standbySvc := b.BuildStandbyService(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts, pgConfCM, hbaCM, coordSvc, segSvc, clientSvc, standbySvc).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// The standby builder will return an error because of invalid resources.
	// reconcileStandby now propagates the error instead of silently returning nil.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "building standby StatefulSet")
}

// ============================================================================
// Scale Operations Tests
// ============================================================================

func TestClusterReconciler_HandleScaleOut_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(2)
	primarySts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleOut(context.Background(), cluster, 2, 4)
	require.NoError(t, err)

	// Verify phase changed to Scaling.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase)
}

func TestClusterReconciler_HandleScaleOut_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleOut(context.Background(), cluster, 2, 4)
	require.NoError(t, err) // Should not error, just skip.

	// Phase should remain Stopped.
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, cluster.Status.Phase)
}

func TestClusterReconciler_HandleScaleIn_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

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

	err := r.handleScaleIn(context.Background(), cluster, 4, 3)
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase)
}

func TestClusterReconciler_HandleScaleIn_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleIn(context.Background(), cluster, 4, 2)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, cluster.Status.Phase)
}

func TestClusterReconciler_HandleScaleIn_RequiresConfirmation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Scale from 4 to 1 (>50% reduction) without confirmation.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.handleScaleIn(context.Background(), cluster, 4, 1)
	require.NoError(t, err) // Should not error, just skip.
	// Phase should remain Running (blocked).
	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, cluster.Status.Phase)
}

func TestClusterReconciler_HandleScaleIn_WithMirroring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
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

	err := r.handleScaleIn(context.Background(), cluster, 4, 3)
	require.NoError(t, err)
}

func TestClusterReconciler_CheckScaleProgress_Complete(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 4
	cluster.Status.SegmentsTotal = 2 // Was 2, scaling to 4.

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

func TestClusterReconciler_CheckScaleProgress_InProgress(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 4

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 2 // Not all ready yet.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_CheckScaleProgress_Timeout(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 4
	// Set scale started time to well in the past.
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted: time.Now().Add(-15 * time.Minute).Format(time.RFC3339),
	}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 2 // Not all ready.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkScaleProgress(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_CompleteScaleOperation_ScaleOut(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 6
	cluster.Status.SegmentsTotal = 4 // Was 4, now 6 (scale-out).

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}
	b := builder.NewBuilder()

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.completeScaleOperation(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

func TestClusterReconciler_CompleteScaleOperation_ScaleIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Spec.Segments.Count = 2
	cluster.Status.SegmentsTotal = 4 // Was 4, now 2 (scale-in).
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}
	b := builder.NewBuilder()

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.completeScaleOperation(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

func TestClusterReconciler_FinaliseScaleIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 2
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyRetain

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}
	b := builder.NewBuilder()

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Should not panic.
	r.finaliseScaleIn(context.Background(), cluster)
}

func TestClusterReconciler_CleanupScaleAnnotations(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted:   time.Now().Format(time.RFC3339),
		util.AnnotationConfirmScaleIn: "true",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}
	b := builder.NewBuilder()

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Should not panic.
	r.cleanupScaleAnnotations(context.Background(), cluster, true)
}

func TestClusterReconciler_HandleScaleFailure(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted: time.Now().Add(-15 * time.Minute).Format(time.RFC3339),
	}

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleScaleFailure(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify failed segments were recorded.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.NotEmpty(t, updated.Status.FailedSegments)
}

func TestClusterReconciler_AllSegmentStatefulSetsReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.True(t, r.allSegmentStatefulSetsReady(context.Background(), cluster))
}

func TestClusterReconciler_AllSegmentStatefulSetsReady_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 4

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.False(t, r.allSegmentStatefulSetsReady(context.Background(), cluster))
}

func TestClusterReconciler_ScaleStatefulSet_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.scaleStatefulSet(context.Background(), "default", sts.Name, 0)
	require.NoError(t, err)

	// Verify replicas were updated.
	updated := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: sts.Name, Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, int32(0), *updated.Spec.Replicas)
}

func TestClusterReconciler_ScaleStatefulSet_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.scaleStatefulSet(context.Background(), "default", "nonexistent", 0)
	require.NoError(t, err) // Not found is not an error.
}

func TestClusterReconciler_ScaleStatefulSet_AlreadyAtScale(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.scaleStatefulSet(context.Background(), "default", sts.Name, 1)
	require.NoError(t, err) // Already at scale, no-op.
}

func TestClusterReconciler_IsStatefulSetAtScale(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	ready, err := r.isStatefulSetAtScale(context.Background(), "default", sts.Name, 1)
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestClusterReconciler_IsStatefulSetAtScale_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	ready, err := r.isStatefulSetAtScale(context.Background(), "default", "nonexistent", 1)
	require.NoError(t, err)
	assert.True(t, ready) // Not found = at scale.
}

func TestClusterReconciler_IsStatefulSetAtScale_Zero(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(0)
	sts.Spec.Replicas = &replicas
	sts.Status.Replicas = 0

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	ready, err := r.isStatefulSetAtScale(context.Background(), "default", sts.Name, 0)
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestClusterReconciler_CleanupOrphanedPVCs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 2

	// Create PVCs for segments 2 and 3 (orphaned after scale-in to 2).
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-2", util.SanitizeK8sName(fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentPrimary))),
			Namespace: "default",
		},
	}
	pvc3 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-3", util.SanitizeK8sName(fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentPrimary))),
			Namespace: "default",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc2, pvc3).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Should not panic.
	r.cleanupOrphanedPVCs(context.Background(), cluster, 2)
}

// ============================================================================
// Upgrade Operations Tests
// ============================================================================

func TestClusterReconciler_HandleUpgrade_Start(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
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

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, result.Requeue || result.RequeueAfter > 0)
}

func TestClusterReconciler_HandleUpgrade_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.ClusterVersion = "7.6"
	cluster.Spec.Version = "7.7"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_ContinueUpgrade_NoAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	// No upgrade annotation.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.continueUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_ContinueUpgrade_InvalidJSON(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Annotations = map[string]string{
		util.AnnotationUpgrade: "invalid-json",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.continueUpgrade(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_GetCurrentImage_FromStatefulSet(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	image := r.getCurrentImage(context.Background(), cluster)
	assert.NotEmpty(t, image)
}

func TestClusterReconciler_GetCurrentImage_Fallback(t *testing.T) {
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

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	image := r.getCurrentImage(context.Background(), cluster)
	assert.Equal(t, cluster.Spec.Image, image)
}

func TestClusterReconciler_UpdateStatefulSetImage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatefulSetImage(context.Background(), "default", sts.Name, "cloudberrydb/cloudberry:7.8")
	require.NoError(t, err)

	updated := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: sts.Name, Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, "cloudberrydb/cloudberry:7.8", updated.Spec.Template.Spec.Containers[0].Image)
}

func TestClusterReconciler_UpdateStatefulSetImage_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.updateStatefulSetImage(context.Background(), "default", "nonexistent", "image:new")
	require.NoError(t, err) // Not found is not an error.
}

func TestClusterReconciler_UpdateStatefulSetImage_AlreadyMatches(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Use the same image that's already set.
	err := r.updateStatefulSetImage(context.Background(), "default", sts.Name, cluster.Spec.Image)
	require.NoError(t, err)
}

func TestClusterReconciler_IsStatefulSetReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.True(t, r.isStatefulSetReady(context.Background(), "default", sts.Name))
}

func TestClusterReconciler_IsStatefulSetReady_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 0

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.False(t, r.isStatefulSetReady(context.Background(), "default", sts.Name))
}

func TestClusterReconciler_IsStatefulSetReady_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	assert.True(t, r.isStatefulSetReady(context.Background(), "default", "nonexistent"))
}

func TestClusterReconciler_GetAllStatefulSetNames(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	names := r.getAllStatefulSetNames(cluster)
	assert.Len(t, names, 4) // mirrors, primaries, standby, coordinator
}

func TestClusterReconciler_CompleteUpgrade(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Spec.Version = "7.8"
	cluster.Annotations = map[string]string{
		util.AnnotationUpgrade: `{"previousImage":"old:7.7","previousVersion":"7.7","phase":"verify"}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := upgradeStateData{
		PreviousImage:   "old:7.7",
		PreviousVersion: "7.7",
	}
	result, err := r.completeUpgrade(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase)
	assert.Equal(t, "7.8", updated.Status.ClusterVersion)
}

func TestClusterReconciler_RollbackUpgrade(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Annotations = map[string]string{
		util.AnnotationUpgrade: `{"previousImage":"old:7.7","previousVersion":"7.7","phase":"primaries"}`,
	}

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := upgradeStateData{
		PreviousImage:   "old:7.7",
		PreviousVersion: "7.7",
	}
	result, err := r.rollbackUpgrade(context.Background(), cluster, state, "phase timed out")
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase)
	assert.Equal(t, "7.7", updated.Status.ClusterVersion)
}

// ============================================================================
// Storage Expansion Tests
// ============================================================================

func TestClusterReconciler_StorageClassSupportsExpansion_True(t *testing.T) {
	scheme := newTestScheme()
	allowExpansion := true
	scName := "standard"
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "standard"},
		AllowVolumeExpansion: &allowExpansion,
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
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	supported, reason := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.True(t, supported)
	assert.Empty(t, reason)
}

func TestClusterReconciler_StorageClassSupportsExpansion_False(t *testing.T) {
	scheme := newTestScheme()
	allowExpansion := false
	scName := "standard"
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "standard"},
		AllowVolumeExpansion: &allowExpansion,
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
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	supported, reason := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.False(t, supported)
	assert.Contains(t, reason, "allowVolumeExpansion=false")
}

func TestClusterReconciler_StorageClassSupportsExpansion_NotFound(t *testing.T) {
	scheme := newTestScheme()
	scName := "nonexistent"
	pvc := &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	supported, reason := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.False(t, supported)
	assert.Contains(t, reason, "not found")
}

func TestClusterReconciler_StorageClassSupportsExpansion_NoStorageClass(t *testing.T) {
	scheme := newTestScheme()
	pvc := &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	supported, _ := r.storageClassSupportsExpansion(context.Background(), pvc)
	assert.True(t, supported) // No SC = allow attempt.
}

func TestClusterReconciler_ExpandPVCIfNeeded_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	changed, err := r.expandPVCIfNeeded(context.Background(), "default", "nonexistent-pvc", "20Gi")
	require.NoError(t, err)
	assert.False(t, changed)
}

func TestClusterReconciler_ExpandStandbyPVC_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	// No standby configured.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	changed, err := r.expandStandbyPVC(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, changed)
}

// ============================================================================
// Lifecycle Phase Tests
// ============================================================================

func TestClusterReconciler_HandleLifecyclePhase_Stopped(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_HandleLifecyclePhase_Stopping(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	// Should either requeue or complete.
	_ = result
}

func TestClusterReconciler_HandleLifecyclePhase_Restricted(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRestricted

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestClusterReconciler_HandleLifecyclePhase_Maintenance(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseMaintenance

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestClusterReconciler_HandleLifecyclePhase_Scaling(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	_ = result
}

func TestClusterReconciler_HandleLifecyclePhase_Updating(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.True(t, handled)
	_ = result
}

func TestClusterReconciler_HandleLifecyclePhase_WithAction(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionStart,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, handled := r.handleLifecyclePhase(context.Background(), cluster)
	assert.False(t, handled) // Action pending, should not handle lifecycle.
}

func TestClusterReconciler_CheckStopProgress_AllStopped(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	// No StatefulSets exist = all at scale 0.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.checkStopProgress(context.Background(), cluster)
	require.NoError(t, err)

	// Should transition to Stopped.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, updated.Status.Phase)
	_ = result
}

func TestClusterReconciler_CheckStopProgress_RestartPending(t *testing.T) {
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
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.checkStopProgress(context.Background(), cluster)
	require.NoError(t, err)
}

func TestClusterReconciler_HandleStop_WithStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.handleStop(context.Background(), cluster, util.ActionStop)
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	// Should be Stopped (all STS not found = at scale 0).
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, updated.Status.Phase)
}

func TestClusterReconciler_HandleRestart_AllStopped(t *testing.T) {
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

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.handleRestart(context.Background(), cluster)
	require.NoError(t, err)
}

func TestClusterReconciler_HandleDeletion_BackupOnDelete(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.BackupOnDelete = true

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.handleDeletion(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestClusterReconciler_IsUpgradeNeeded(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *cbv1alpha1.CloudberryCluster
		expected bool
	}{
		{
			name: "upgrade annotation present",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Annotations = map[string]string{util.AnnotationUpgrade: "{}"}
				return c
			}(),
			expected: true,
		},
		{
			name: "version changed",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Status.ClusterVersion = "7.6"
				c.Spec.Version = "7.7"
				return c
			}(),
			expected: true,
		},
		{
			name: "no upgrade needed",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Status.ClusterVersion = "7.7"
				c.Spec.Version = "7.7"
				return c
			}(),
			expected: false,
		},
		{
			name: "empty cluster version",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Status.ClusterVersion = ""
				return c
			}(),
			expected: false,
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, r.isUpgradeNeeded(tt.cluster))
		})
	}
}

func TestClusterReconciler_VerifyUpgrade_AllReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Spec.Version = "7.8"

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.ReadyReplicas = 1

	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	primarySts.Spec.Replicas = &cluster.Spec.Segments.Count
	primarySts.Status.ReadyReplicas = cluster.Spec.Segments.Count

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts, primarySts).
		WithStatusSubresource(cluster, coordSts, primarySts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := upgradeStateData{PreviousVersion: "7.7", PreviousImage: "old:7.7"}
	result, err := r.verifyUpgrade(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestClusterReconciler_VerifyUpgrade_CoordNotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating

	b := builder.NewBuilder()
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	coordSts.Spec.Replicas = &replicas
	coordSts.Status.ReadyReplicas = 0

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, coordSts).
		WithStatusSubresource(cluster, coordSts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	state := upgradeStateData{PreviousVersion: "7.7"}
	result, err := r.verifyUpgrade(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}
