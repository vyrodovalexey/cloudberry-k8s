package controller

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func newAdminExporterReconciler(objs ...client.Object) (*AdminReconciler, client.Client) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
	return r, k8sClient
}

func TestAdminReconciler_ResolveExporterDSNPort(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0
	assert.Equal(t, int32(util.DefaultCoordinatorPort), resolveExporterDSNPort(cluster))

	cluster.Spec.Coordinator.Port = 7777
	assert.Equal(t, int32(7777), resolveExporterDSNPort(cluster))
}

func TestIsNodeExporterEnabled(t *testing.T) {
	assert.False(t, isNodeExporterEnabled(&cbv1alpha1.QueryMonitoringSpec{}))
	assert.False(t, isNodeExporterEnabled(&cbv1alpha1.QueryMonitoringSpec{
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{},
	}))
	assert.False(t, isNodeExporterEnabled(&cbv1alpha1.QueryMonitoringSpec{
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			NodeExporter: &cbv1alpha1.ExporterSpec{Enabled: false},
		},
	}))
	assert.True(t, isNodeExporterEnabled(&cbv1alpha1.QueryMonitoringSpec{
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			NodeExporter: &cbv1alpha1.ExporterSpec{Enabled: true},
		},
	}))
}

func TestAdminReconciler_ResolveExporterPassword(t *testing.T) {
	t.Run("existing secret", func(t *testing.T) {
		cluster := newTestCluster()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterCredentialsSecretName(cluster.Name),
				Namespace: cluster.Namespace,
			},
			Data: map[string][]byte{"password": []byte("stored-pw")},
		}
		r, _ := newAdminExporterReconciler(cluster, secret)

		pw, err := r.resolveExporterPassword(context.Background(), cluster)
		require.NoError(t, err)
		assert.Equal(t, "stored-pw", pw)
	})

	t.Run("missing secret generates password", func(t *testing.T) {
		cluster := newTestCluster()
		r, _ := newAdminExporterReconciler(cluster)

		pw, err := r.resolveExporterPassword(context.Background(), cluster)
		require.NoError(t, err)
		assert.NotEmpty(t, pw)
	})

	t.Run("get error propagated", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(
					_ context.Context, _ client.WithWatch, _ client.ObjectKey,
					obj client.Object, _ ...client.GetOption,
				) error {
					if _, ok := obj.(*corev1.Secret); ok {
						return fmt.Errorf("api error")
					}
					return nil
				},
			}).
			Build()
		r := NewAdminReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

		_, err := r.resolveExporterPassword(context.Background(), cluster)
		require.Error(t, err)
	})
}

func TestAdminReconciler_EnsureExporterCredentialsSecret(t *testing.T) {
	logger := slog.Default()

	t.Run("create when missing", func(t *testing.T) {
		cluster := newTestCluster()
		r, _ := newAdminExporterReconciler(cluster)
		err := r.ensureExporterCredentialsSecret(context.Background(), cluster, "pw", "dsn", logger)
		require.NoError(t, err)
	})

	t.Run("update when exists", func(t *testing.T) {
		cluster := newTestCluster()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterCredentialsSecretName(cluster.Name),
				Namespace: cluster.Namespace,
			},
			Data: map[string][]byte{"password": []byte("old")},
		}
		r, _ := newAdminExporterReconciler(cluster, secret)
		err := r.ensureExporterCredentialsSecret(context.Background(), cluster, "newpw", "dsn", logger)
		require.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(
					_ context.Context, _ client.WithWatch, _ client.ObjectKey,
					obj client.Object, _ ...client.GetOption,
				) error {
					if _, ok := obj.(*corev1.Secret); ok {
						return fmt.Errorf("api error")
					}
					return nil
				},
			}).
			Build()
		r := NewAdminReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		err := r.ensureExporterCredentialsSecret(context.Background(), cluster, "pw", "dsn", logger)
		require.Error(t, err)
	})
}

func TestAdminReconciler_EnsureExporterQueriesConfigMap(t *testing.T) {
	logger := slog.Default()

	t.Run("create when missing", func(t *testing.T) {
		cluster := newTestCluster()
		r, _ := newAdminExporterReconciler(cluster)
		err := r.ensureExporterQueriesConfigMap(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("update when exists", func(t *testing.T) {
		cluster := newTestCluster()
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterQueriesConfigMapName(cluster.Name),
				Namespace: cluster.Namespace,
			},
			Data: map[string]string{"queries.yaml": "old"},
		}
		r, _ := newAdminExporterReconciler(cluster, cm)
		err := r.ensureExporterQueriesConfigMap(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(
					_ context.Context, _ client.WithWatch, _ client.ObjectKey,
					obj client.Object, _ ...client.GetOption,
				) error {
					if _, ok := obj.(*corev1.ConfigMap); ok {
						return fmt.Errorf("api error")
					}
					return nil
				},
			}).
			Build()
		r := NewAdminReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		err := r.ensureExporterQueriesConfigMap(context.Background(), cluster, logger)
		require.Error(t, err)
	})
}

func TestAdminReconciler_EnsureExporterService(t *testing.T) {
	logger := slog.Default()

	t.Run("create when missing", func(t *testing.T) {
		cluster := newTestCluster()
		r, _ := newAdminExporterReconciler(cluster)
		err := r.ensureExporterService(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("update when exists", func(t *testing.T) {
		cluster := newTestCluster()
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterMetricsServiceName(cluster.Name),
				Namespace: cluster.Namespace,
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 1}},
			},
		}
		r, _ := newAdminExporterReconciler(cluster, svc)
		err := r.ensureExporterService(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("create error", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Create: func(
					_ context.Context, _ client.WithWatch, _ client.Object,
					_ ...client.CreateOption,
				) error {
					return fmt.Errorf("create failed")
				},
			}).
			Build()
		r := NewAdminReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		err := r.ensureExporterService(context.Background(), cluster, logger)
		require.Error(t, err)
	})
}

func TestAdminReconciler_EnsureExporterCoreResources(t *testing.T) {
	logger := slog.Default()
	cluster := newTestCluster()
	r, k8sClient := newAdminExporterReconciler(cluster)

	err := r.ensureExporterCoreResources(context.Background(), cluster, "pw", "dsn", logger)
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.ExporterCredentialsSecretName(cluster.Name),
		Namespace: cluster.Namespace,
	}, secret))
}

func nodeExporterCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled: true,
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			NodeExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "prom/node-exporter:latest",
			},
		},
	}
	return cluster
}

func TestAdminReconciler_EnsureNodeExporterDaemonSet(t *testing.T) {
	logger := slog.Default()

	t.Run("create when missing", func(t *testing.T) {
		cluster := nodeExporterCluster()
		r, _ := newAdminExporterReconciler(cluster)
		err := r.ensureNodeExporterDaemonSet(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("update when exists", func(t *testing.T) {
		cluster := nodeExporterCluster()
		ds := &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.NodeExporterDaemonSetName(cluster.Name),
				Namespace: cluster.Namespace,
			},
		}
		r, _ := newAdminExporterReconciler(cluster, ds)
		err := r.ensureNodeExporterDaemonSet(context.Background(), cluster, logger)
		require.NoError(t, err)
	})

	t.Run("create already exists ignored is error", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := nodeExporterCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Create: func(
					_ context.Context, _ client.WithWatch, obj client.Object,
					_ ...client.CreateOption,
				) error {
					return apierrors.NewAlreadyExists(
						schema.GroupResource{Resource: "daemonsets"}, obj.GetName())
				},
			}).
			Build()
		r := NewAdminReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
		err := r.ensureNodeExporterDaemonSet(context.Background(), cluster, logger)
		// AlreadyExists is not specially handled here -> propagated as error.
		require.Error(t, err)
	})
}

func TestAdminReconciler_LogExporterConfig(t *testing.T) {
	logger := slog.Default()
	r, _ := newAdminExporterReconciler()

	// nil spec is a no-op.
	r.logExporterConfig(logger, "x", nil)

	// spec with resources exercises the resources branch.
	r.logExporterConfig(logger, "postgres-exporter", &cbv1alpha1.ExporterSpec{
		Enabled: true,
		Image:   "img",
		Port:    9187,
		Resources: &cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
			Limits:   &cbv1alpha1.ResourceList{CPU: "200m", Memory: "256Mi"},
		},
	})
}

func TestAdminReconciler_LogQueryMonitoringExporters(t *testing.T) {
	logger := slog.Default()
	r, _ := newAdminExporterReconciler()

	t.Run("nil exporters is no-op", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
		r.logQueryMonitoringExporters(logger, cluster)
	})

	t.Run("full exporters config", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
			Enabled: true,
			Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
				PostgresExporter: &cbv1alpha1.ExporterSpec{Enabled: true},
				NodeExporter:     &cbv1alpha1.ExporterSpec{Enabled: true},
				ServiceMonitor: &cbv1alpha1.QueryServiceMonitorSpec{
					Enabled: true,
				},
				PrometheusRule: &cbv1alpha1.QueryPrometheusRuleSpec{
					Enabled:   true,
					Namespace: "monitoring",
				},
			},
		}
		r.logQueryMonitoringExporters(logger, cluster)
	})
}

func TestAdminReconciler_LogServiceMonitorAndPrometheusRuleConfig(t *testing.T) {
	logger := slog.Default()
	r, _ := newAdminExporterReconciler()

	// nil is a no-op.
	r.logServiceMonitorConfig(logger, nil, "default")
	r.logPrometheusRuleConfig(logger, nil, "default")

	// empty namespace falls back to default.
	r.logServiceMonitorConfig(logger, &cbv1alpha1.QueryServiceMonitorSpec{Enabled: true}, "default")
	r.logPrometheusRuleConfig(logger, &cbv1alpha1.QueryPrometheusRuleSpec{Enabled: true}, "default")

	// explicit namespace is used.
	r.logServiceMonitorConfig(logger,
		&cbv1alpha1.QueryServiceMonitorSpec{Enabled: true, Namespace: "mon"}, "default")
	r.logPrometheusRuleConfig(logger,
		&cbv1alpha1.QueryPrometheusRuleSpec{Enabled: true, Namespace: "mon"}, "default")
}
