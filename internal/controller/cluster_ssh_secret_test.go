package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func newSSHSecretReconciler(t *testing.T, c client.Client) *ClusterReconciler {
	t.Helper()
	scheme := newTestScheme()
	return NewClusterReconciler(c, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
}

// TestReconcileClusterSSHSecret_CreatesWhenAbsent verifies the Secret is created
// with the three SSH data keys when it does not yet exist.
func TestReconcileClusterSSHSecret_CreatesWhenAbsent(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := newSSHSecretReconciler(t, k8sClient)

	require.NoError(t, r.reconcileClusterSSHSecret(context.Background(), cluster))

	secret := &corev1.Secret{}
	name := util.ClusterSSHSecretName(cluster.Name)
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret))

	assert.Equal(t, "test-cluster-ssh-keys", secret.Name)
	require.Contains(t, secret.Data, util.SSHPrivateKeyField)
	require.Contains(t, secret.Data, util.SSHPublicKeyField)
	require.Contains(t, secret.Data, util.SSHAuthorizedKeysField)
	assert.NotEmpty(t, secret.Data[util.SSHPrivateKeyField])
	assert.NotEmpty(t, secret.Data[util.SSHAuthorizedKeysField])
}

// TestReconcileClusterSSHSecret_Idempotent verifies a second reconcile is a
// no-op error-free and leaves the existing Secret (and its key material)
// unchanged.
func TestReconcileClusterSSHSecret_Idempotent(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := newSSHSecretReconciler(t, k8sClient)

	require.NoError(t, r.reconcileClusterSSHSecret(context.Background(), cluster))

	name := util.ClusterSSHSecretName(cluster.Name)
	first := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, first))

	// Second reconcile: must not error and must not rotate the key material.
	require.NoError(t, r.reconcileClusterSSHSecret(context.Background(), cluster))

	second := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, second))

	assert.Equal(t, first.Data[util.SSHPrivateKeyField], second.Data[util.SSHPrivateKeyField],
		"existing SSH identity must remain stable across reconciles")
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"the Secret must not be rewritten when already present")
}

// TestReconcileClusterSSHSecret_ToleratesAlreadyExists verifies that a Create
// returning AlreadyExists (a race with a concurrent reconcile) is tolerated.
func TestReconcileClusterSSHSecret_ToleratesAlreadyExists(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	gr := schema.GroupResource{Group: "", Resource: "secrets"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				if s, ok := obj.(*corev1.Secret); ok &&
					s.Name == util.ClusterSSHSecretName(cluster.Name) {
					return apierrors.NewAlreadyExists(gr, s.Name)
				}
				return nil
			},
		}).
		Build()
	r := newSSHSecretReconciler(t, k8sClient)

	// Get returns NotFound (secret absent), but Create races and returns
	// AlreadyExists — the reconcile must succeed without surfacing an error.
	require.NoError(t, r.reconcileClusterSSHSecret(context.Background(), cluster))
}

// TestReconcileClusterSSHSecret_GetError verifies a non-NotFound Get error is
// surfaced (wrapped).
func TestReconcileClusterSSHSecret_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok &&
					key.Name == util.ClusterSSHSecretName(cluster.Name) {
					return fmt.Errorf("api server unavailable")
				}
				return nil
			},
		}).
		Build()
	r := newSSHSecretReconciler(t, k8sClient)

	err := r.reconcileClusterSSHSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting cluster ssh secret")
}

// TestReconcileClusterSSHSecret_CreateError verifies a non-AlreadyExists Create
// error is surfaced (wrapped).
func TestReconcileClusterSSHSecret_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				if s, ok := obj.(*corev1.Secret); ok &&
					s.Name == util.ClusterSSHSecretName(cluster.Name) {
					return fmt.Errorf("etcd write failed")
				}
				return nil
			},
		}).
		Build()
	r := newSSHSecretReconciler(t, k8sClient)

	err := r.reconcileClusterSSHSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating cluster ssh secret")
}
