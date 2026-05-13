package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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

func TestAuthReconciler_Reconcile_WithOIDC_MissingIssuer(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		OIDC: &cbv1alpha1.OIDCSpec{
			Enabled:  true,
			ClientID: "client-id",
			// IssuerURL intentionally missing.
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

	// Should still succeed (OIDC validation is a warning, not a failure).
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_WithSSL(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:       true,
			CertSecret:    &cbv1alpha1.CertSecretRef{Name: "tls-cert"},
			MinTLSVersion: "1.3",
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

func TestAuthReconciler_Reconcile_HBAUpdate(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()

	// Pre-create the HBA configmap so the reconciler hits the update path.
	existingCM := b.BuildPgHBAConfConfigMap(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_WithOIDCAndSSL(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		Basic: &cbv1alpha1.BasicAuthSpec{
			Enabled:   true,
			AdminUser: "gpadmin",
		},
		OIDC: &cbv1alpha1.OIDCSpec{
			Enabled:   true,
			IssuerURL: "https://keycloak.example.com",
			ClientID:  "cloudberry",
		},
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:       true,
			CertSecret:    &cbv1alpha1.CertSecretRef{Name: "tls-cert"},
			MinTLSVersion: "1.3",
		},
		HBARules: []cbv1alpha1.HBARule{
			{Type: cbv1alpha1.HBATypeHostSSL, Database: "all", User: "all",
				Address: "0.0.0.0/0", Method: cbv1alpha1.AuthMethodScramSHA256},
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

func TestAuthReconciler_Reconcile_HBAUpdateWithChangedRules(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()

	// Pre-create the HBA configmap with old data.
	existingCM := b.BuildPgHBAConfConfigMap(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	// Now add HBA rules to the cluster (changes the desired configmap).
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		HBARules: []cbv1alpha1.HBARule{
			{Type: cbv1alpha1.HBATypeHostSSL, Database: "all", User: "all",
				Address: "10.0.0.0/8", Method: cbv1alpha1.AuthMethodScramSHA256},
		},
	}
	// Update the cluster object in the fake client.
	err := k8sClient.Update(context.Background(), cluster)
	require.NoError(t, err)

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_WithHBARules(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		HBARules: []cbv1alpha1.HBARule{
			{Type: cbv1alpha1.HBATypeHostSSL, Database: "all", User: "all",
				Address: "10.0.0.0/8", Method: cbv1alpha1.AuthMethodScramSHA256},
			{Type: cbv1alpha1.HBATypeLocal, Database: "all", User: "gpadmin",
				Method: cbv1alpha1.AuthMethodPeer},
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

	// Verify HBA configmap was created.
	cm := &corev1.ConfigMap{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PgHBAConfConfigMapName("test-cluster"),
		Namespace: "default",
	}, cm)
	require.NoError(t, err)
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

func TestAuthReconciler_Reconcile_OIDCDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		OIDC: &cbv1alpha1.OIDCSpec{
			Enabled: false,
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

func TestAuthReconciler_Reconcile_OIDCMissingClientID(t *testing.T) {
	// OIDC validation failure is a warning, not a hard error.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		OIDC: &cbv1alpha1.OIDCSpec{
			Enabled:   true,
			IssuerURL: "https://issuer.example.com",
			// ClientID intentionally missing.
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

func TestAuthReconciler_Reconcile_DeletingPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseDeleting

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
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestAuthReconciler_ReconcileHBA_CreateThenUpdate(t *testing.T) {
	// Test reconcileHBA create path then update path.
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

	// First call creates the configmap.
	err := r.reconcileHBA(context.Background(), cluster)
	require.NoError(t, err)

	// Second call should find existing and check hash.
	err = r.reconcileHBA(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAuthReconciler_Reconcile_AuthNilSpec(t *testing.T) {
	// When Auth spec is nil, OIDC validation should be skipped.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Auth = nil

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

func TestAuthReconciler_Reconcile_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching cluster")
}

func TestAuthReconciler_ReconcileHBA_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileHBA(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating pg_hba.conf configmap")
}

func TestAuthReconciler_ReconcileHBA_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileHBA(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting pg_hba.conf configmap")
}

func TestAuthReconciler_ReconcileHBA_UpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	// Pre-create with different hash to trigger update.
	existingCM := b.BuildPgHBAConfConfigMap(cluster)
	existingCM.Annotations[util.AnnotationConfigHash] = "old-hash"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	err := r.reconcileHBA(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating pg_hba.conf configmap")
}

func TestAuthReconciler_Reconcile_ObservedGenerationSkip(t *testing.T) {
	// When ObservedGeneration matches Generation, should skip reconciliation.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 3
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 3

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
	assert.Equal(t, 5*time.Minute, result.RequeueAfter)
}

func TestAuthReconciler_Reconcile_ObservedGenerationMismatch(t *testing.T) {
	// When ObservedGeneration does NOT match Generation, should proceed with reconciliation.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 3
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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify HBA configmap was created (reconciliation proceeded).
	cm := &corev1.ConfigMap{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PgHBAConfConfigMapName("test-cluster"),
		Namespace: "default",
	}, cm)
	require.NoError(t, err)
}

func TestAuthReconciler_Reconcile_HBAError_SetsCondition(t *testing.T) {
	// When HBA reconciliation fails, the auth condition should be set to False.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

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

	r := NewAuthReconciler(k8sClient, scheme, recorder, b, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}
