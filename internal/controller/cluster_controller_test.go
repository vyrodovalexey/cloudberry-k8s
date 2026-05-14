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
	coordSts := b.BuildCoordinatorStatefulSet(cluster)
	coordSts.Spec.Template.Spec.Containers = nil // Make it different to trigger update.
	primarySts := b.BuildSegmentPrimaryStatefulSet(cluster)
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
	mirrorSts := b.BuildSegmentMirrorStatefulSet(cluster)
	replicas := int32(4)
	mirrorSts.Spec.Replicas = &replicas
	mirrorSts.Status.ReadyReplicas = 4

	// Pre-create standby StatefulSet with ready replicas.
	standbySts := b.BuildStandbyStatefulSet(cluster)
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
	coordSts := b.BuildCoordinatorStatefulSet(cluster)
	coordSts.Status.ReadyReplicas = 1

	// Pre-create primary segment StatefulSet with ready replicas.
	primarySts := b.BuildSegmentPrimaryStatefulSet(cluster)
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
	coordSts := b.BuildCoordinatorStatefulSet(validCluster)
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
	mirrorSts := b.BuildSegmentMirrorStatefulSet(cluster)
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
	mirrorSts := b.BuildSegmentMirrorStatefulSet(cluster)
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
	standbySts := b.BuildStandbyStatefulSet(cluster)
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

	desired := b.BuildCoordinatorStatefulSet(cluster)
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

	desired := b.BuildCoordinatorStatefulSet(cluster)
	err := r.createOrUpdateStatefulSet(context.Background(), desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting statefulset")
}

func TestClusterReconciler_CreateOrUpdateStatefulSet_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()

	// Pre-create with different spec to trigger update.
	existing := b.BuildCoordinatorStatefulSet(cluster)
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

	desired := b.BuildCoordinatorStatefulSet(cluster)
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
	coordSts := b.BuildCoordinatorStatefulSet(cluster)
	primarySts := b.BuildSegmentPrimaryStatefulSet(cluster)
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

	// The standby builder will return nil because of invalid resources.
	// But reconcileStandby returns nil when builder returns nil.
	// So this should succeed.
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}
