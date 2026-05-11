package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func TestNewAdminReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)
	require.NotNil(t, r)
}

func TestAdminReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestAdminReconciler_Reconcile_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

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
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_Running_NoConfig(t *testing.T) {
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

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_WithConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{
			"max_connections": "200",
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

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_MaintenanceAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
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
	assert.NotZero(t, result.RequeueAfter)

	// Verify annotation was removed
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	_, exists := updated.Annotations[util.AnnotationMaintenance]
	assert.False(t, exists)
}

func TestAdminReconciler_ReconcileConfig_NoChange(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
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

	// First reconcile sets the hash
	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)

	// Second reconcile with same config should be a no-op
	err = r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileConfig_NilConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_HandleMaintenance(t *testing.T) {
	tests := []struct {
		name        string
		maintenance string
	}{
		{"vacuum", util.MaintenanceVacuum},
		{"vacuum-analyze", util.MaintenanceVacuumAnalyze},
		{"vacuum-full", util.MaintenanceVacuumFull},
		{"analyze", util.MaintenanceAnalyze},
		{"reindex", util.MaintenanceReindex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			cluster.Annotations = map[string]string{
				util.AnnotationMaintenance: tt.maintenance,
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

			result, err := r.handleMaintenance(context.Background(), cluster, tt.maintenance)
			require.NoError(t, err)
			assert.NotZero(t, result.RequeueAfter)
		})
	}
}
