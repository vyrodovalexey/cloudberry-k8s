package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func TestNewAuthReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)
	require.NotNil(t, r)
}

func TestAuthReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestAuthReconciler_Reconcile_NotRunning(t *testing.T) {
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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_Running(t *testing.T) {
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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify HBA configmap was created
	cm := &corev1.ConfigMap{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PgHBAConfConfigMapName("test-cluster"),
		Namespace: "default",
	}, cm)
	require.NoError(t, err)
}

func TestAuthReconciler_Reconcile_Initializing(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_WithOIDC(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		OIDC: &cbv1alpha1.OIDCSpec{
			Enabled:   true,
			IssuerURL: "https://issuer.example.com",
			ClientID:  "client-id",
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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_ValidateOIDCConfig(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewAuthReconciler(k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	tests := []struct {
		name      string
		cluster   *cbv1alpha1.CloudberryCluster
		expectErr bool
	}{
		{
			name: "valid OIDC config",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
						ClientID:  "client-id",
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "missing issuer URL",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:  true,
						ClientID: "client-id",
					},
				}
				return c
			}(),
			expectErr: true,
		},
		{
			name: "missing client ID",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newTestCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
					},
				}
				return c
			}(),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.validateOIDCConfig(context.Background(), tt.cluster)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
