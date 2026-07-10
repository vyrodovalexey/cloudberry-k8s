package controller

// Error-path tests for handleDeletion (handoff task T-8): updatePhase
// failure, deletePVCs failure and a finalizer-Update conflict must propagate
// the error, keep the finalizer (deletion NOT completed) and preserve the
// documented requeue semantics.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newDeletionErrorEnv builds a deleting cluster (finalizer + timestamp) with
// one labeled PVC behind the given interceptors.
func newDeletionErrorEnv(
	t *testing.T,
	mutate func(*cbv1alpha1.CloudberryCluster),
	funcs interceptor.Funcs,
) (*ClusterReconciler, client.Client, *record.FakeRecorder, *cbv1alpha1.CloudberryCluster) {
	t.Helper()
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	if mutate != nil {
		mutate(cluster)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{util.LabelCluster: cluster.Name},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(funcs).
		Build()
	events := record.NewFakeRecorder(50)
	r := NewClusterReconciler(k8sClient, scheme, events, builder.NewBuilder(), nil, nil)
	return r, k8sClient, events, cluster
}

// clusterStillHasFinalizer asserts deletion did NOT complete.
func clusterStillHasFinalizer(t *testing.T, c client.Client, key types.NamespacedName) {
	t.Helper()
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, c.Get(context.Background(), key, got),
		"the cluster must still exist after a failed deletion step")
	assert.Contains(t, got.Finalizers, util.FinalizerName,
		"the finalizer must be kept so deletion is retried")
}

func TestHandleDeletion_UpdatePhaseFailure(t *testing.T) {
	r, k8sClient, events, cluster := newDeletionErrorEnv(t,
		func(c *cbv1alpha1.CloudberryCluster) {
			c.Status.Phase = cbv1alpha1.ClusterPhaseRunning // forces the phase transition
		},
		interceptor.Funcs{
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
				obj client.Object, _ ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
					return fmt.Errorf("status update boom")
				}
				return fmt.Errorf("unexpected subresource update")
			},
		})

	result, err := r.handleDeletion(context.Background(), cluster)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating phase to Deleting")
	assert.Zero(t, result.RequeueAfter, "error propagation drives the retry, not a timed requeue")
	clusterStillHasFinalizer(t, k8sClient,
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})

	// The transition event must NOT fire when the phase update failed.
	assert.False(t, drainEventsContains(events, "Normal", "Cluster deletion initiated"),
		"no Deleting event may be emitted for an unpersisted phase")
}

func TestHandleDeletion_DeletePVCsFailure(t *testing.T) {
	r, k8sClient, events, cluster := newDeletionErrorEnv(t,
		func(c *cbv1alpha1.CloudberryCluster) {
			c.Status.Phase = cbv1alpha1.ClusterPhaseDeleting // skip the phase update
			c.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete
		},
		interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch,
				obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return fmt.Errorf("pvc delete boom")
				}
				return c.Delete(ctx, obj, opts...)
			},
		})

	result, err := r.handleDeletion(context.Background(), cluster)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleting PVC")
	assert.Equal(t, requeueAfterError, result.RequeueAfter,
		"a PVC deletion failure must requeue with the error backoff")
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	clusterStillHasFinalizer(t, k8sClient, key)

	// The PVC survives and no success events fire.
	pvcs := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, k8sClient.List(context.Background(), pvcs,
		client.InNamespace(cluster.Namespace)))
	assert.Len(t, pvcs.Items, 1, "the PVC must survive the failed delete")
	assert.False(t, drainEventsContains(events, "Normal", "All PVCs deleted"),
		"no PVCsDeleted event may be emitted when the deletion failed")
}

func TestHandleDeletion_PVCListFailure(t *testing.T) {
	r, k8sClient, _, cluster := newDeletionErrorEnv(t,
		func(c *cbv1alpha1.CloudberryCluster) {
			c.Status.Phase = cbv1alpha1.ClusterPhaseDeleting
			c.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete
		},
		interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch,
				list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PersistentVolumeClaimList); ok {
					return fmt.Errorf("pvc list boom")
				}
				return c.List(ctx, list, opts...)
			},
		})

	result, err := r.handleDeletion(context.Background(), cluster)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing PVCs")
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
	clusterStillHasFinalizer(t, k8sClient,
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})
}

func TestHandleDeletion_FinalizerUpdateConflict(t *testing.T) {
	r, k8sClient, _, cluster := newDeletionErrorEnv(t,
		func(c *cbv1alpha1.CloudberryCluster) {
			c.Status.Phase = cbv1alpha1.ClusterPhaseDeleting
			c.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyRetain // PVCs untouched
		},
		interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch,
				obj client.Object, _ ...client.UpdateOption) error {
				if _, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
					return apierrors.NewConflict(
						schema.GroupResource{Group: cbv1alpha1.GroupVersion.Group, Resource: "cloudberryclusters"},
						obj.GetName(), fmt.Errorf("the object has been modified"))
				}
				return fmt.Errorf("unexpected update")
			},
		})

	_, err := r.handleDeletion(context.Background(), cluster)

	require.Error(t, err, "a finalizer-removal conflict must surface for retry")
	assert.Contains(t, err.Error(), "removing finalizer")
	assert.True(t, apierrors.IsConflict(err),
		"the %%w-wrapped conflict must stay detectable for conflict-aware retries: %v", err)
	clusterStillHasFinalizer(t, k8sClient,
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})

	// The PVC is retained per policy even though the finalizer removal failed.
	pvcs := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, k8sClient.List(context.Background(), pvcs,
		client.InNamespace(cluster.Namespace)))
	assert.Len(t, pvcs.Items, 1)
}
