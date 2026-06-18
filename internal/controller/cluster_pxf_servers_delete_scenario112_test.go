package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// Scenario 112 — Disabled States (DIS.1/DIS.2) cluster-controller ConfigMap
// teardown. ensurePxfServersConfigMap now DELETES the rendered
// "<cluster>-pxf-servers" ConfigMap (best-effort, NotFound-tolerant) when the
// builder yields nil (pxf sidecar disabled), instead of the previous no-op that
// left a stale ConfigMap ORPHANED. These tests prove the new delete-when-disabled
// path: a pre-seeded stale CM is reclaimed; an absent CM is a clean no-op; and
// neither ever fails the reconcile.

// stalePxfServersConfigMap returns a "<cluster>-pxf-servers" ConfigMap as it
// would have been rendered while PXF was enabled (the orphan to reclaim).
func stalePxfServersConfigMap(clusterName, namespace string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      builder.PxfServersConfigMapName(clusterName),
			Namespace: namespace,
		},
		Data: map[string]string{"s3-a__s3-site.xml": "<configuration/>"},
	}
}

// 112-DIS1-U-CMDEL: with the PXF sidecar disabled (a default, non-PXF cluster
// renders a nil ConfigMap), a pre-seeded stale "<cluster>-pxf-servers" ConfigMap
// is DELETED by ensurePxfServersConfigMap.
func TestEnsurePxfServersConfigMap_Disabled_DeletesStaleConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster() // no DataLoading/PXF => builder yields nil
	stale := stalePxfServersConfigMap(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, stale).
		WithStatusSubresource(cluster).
		Build()

	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, &corev1.ConfigMap{})
	assert.True(t, apierrors.IsNotFound(err),
		"stale PXF servers ConfigMap must be deleted when the sidecar is disabled")
}

// 112-DIS2 stale-CM cleanup: dataLoading.enabled=true but pxf.enabled=false also
// yields a nil rendered ConfigMap (pxfSidecarEnabled=false), so a stale CM left
// over from a prior PXF-on state is reclaimed too.
func TestEnsurePxfServersConfigMap_PxfDisabledDLEnabled_DeletesStaleConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: false, Image: "cloudberry/pxf:2.1.0"},
	}
	stale := stalePxfServersConfigMap(cluster.Name, cluster.Namespace)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, stale).
		WithStatusSubresource(cluster).
		Build()

	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, &corev1.ConfigMap{})
	assert.True(t, apierrors.IsNotFound(err))
}

// 112-DIS1-U-CMDEL (NotFound-tolerance): with the sidecar disabled AND no
// existing ConfigMap, the delete-when-disabled path is a clean no-op — no error,
// no panic.
func TestEnsurePxfServersConfigMap_Disabled_NoConfigMapIsNoOp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster() // no DataLoading/PXF, no seeded CM

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))
	})
}

// 112-DIS1-U-CMDEL (direct): deletePxfServersConfigMap is best-effort and
// NotFound-tolerant — deleting an absent ConfigMap returns nil (no error).
func TestDeletePxfServersConfigMap_AbsentIsNoError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.deletePxfServersConfigMap(context.Background(), cluster))
}
