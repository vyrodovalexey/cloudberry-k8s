package controller

// Regression tests for the deletion-ordering fix (code review B1 / task T1):
// Reconcile must route a Terminating cluster to handleDeletion BEFORE the
// action-annotation gate, the lifecycle phase short-circuit and the
// generation gate. Before the fix a cluster deleted in phase Stopped returned
// without removing the finalizer (stuck Terminating forever) and
// Restricted/Maintenance requeued endlessly without ever reaching
// handleDeletion.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestReconcile_DeleteClusterInLifecyclePhases proves deletion always wins:
// a Terminating cluster is fully deleted (finalizer removed, no requeue loop)
// regardless of the lifecycle phase it is in, a pending action annotation, or
// an up-to-date ObservedGeneration.
func TestReconcile_DeleteClusterInLifecyclePhases(t *testing.T) {
	tests := []struct {
		name        string
		phase       cbv1alpha1.ClusterPhase
		annotations map[string]string
	}{
		{name: "stopped", phase: cbv1alpha1.ClusterPhaseStopped},
		{name: "stopping", phase: cbv1alpha1.ClusterPhaseStopping},
		{name: "restricted", phase: cbv1alpha1.ClusterPhaseRestricted},
		{name: "maintenance", phase: cbv1alpha1.ClusterPhaseMaintenance},
		{name: "empty phase", phase: ""},
		{
			name:        "running with pending action annotation",
			phase:       cbv1alpha1.ClusterPhaseRunning,
			annotations: map[string]string{util.AnnotationAction: "stop"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			now := metav1.Now()
			cluster := newTestCluster()
			cluster.Finalizers = []string{util.FinalizerName}
			cluster.DeletionTimestamp = &now
			cluster.Status.Phase = tt.phase
			// The generation gate must not shadow deletion either.
			cluster.Status.ObservedGeneration = cluster.Generation
			cluster.Annotations = tt.annotations

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()
			r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
				builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: cluster.Name, Namespace: cluster.Namespace,
				},
			})
			require.NoError(t, err)
			assert.Zero(t, result.RequeueAfter,
				"deletion must complete without an endless requeue loop")

			got := &cbv1alpha1.CloudberryCluster{}
			getErr := k8sClient.Get(context.Background(),
				types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, got)
			assert.True(t, apierrors.IsNotFound(getErr),
				"finalizer must be removed so the object terminates; err=%v finalizers=%v",
				getErr, got.Finalizers)
		})
	}
}

// TestReconcile_DeleteClusterRecordsReconcileOutcome pins that the deferred
// reconcile outcome metric still fires on the hoisted early-return deletion
// path (the defer runs on every exit).
func TestReconcile_DeleteClusterRecordsReconcileOutcome(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err)

	require.Len(t, rec.reconcileCalls, 1, "reconcile outcome must be recorded exactly once")
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}
