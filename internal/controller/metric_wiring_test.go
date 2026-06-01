package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Shared metric-tracking recorder and DB wrappers for the wiring tests.
// ============================================================================

// pvcSizeCall captures a SetPVCSizeBytes invocation.
type pvcSizeCall struct {
	cluster   string
	namespace string
	component string
	bytes     float64
}

// maintenanceCall captures a RecordMaintenanceOperation invocation.
type maintenanceCall struct {
	cluster   string
	namespace string
	operation string
	result    string
}

// redistributionCall captures a SetRedistributionProgress invocation.
type redistributionCall struct {
	cluster   string
	namespace string
	progress  float64
}

// wiringRecorder wraps NoopRecorder and tracks the metric calls exercised by
// the controller metric-wiring code paths under test.
type wiringRecorder struct {
	metrics.NoopRecorder
	pvcSizeCalls        []pvcSizeCall
	maintenanceCalls    []maintenanceCall
	redistributionCalls []redistributionCall
}

func (w *wiringRecorder) SetPVCSizeBytes(cluster, namespace, component string, sizeBytes float64) {
	w.pvcSizeCalls = append(w.pvcSizeCalls, pvcSizeCall{
		cluster: cluster, namespace: namespace, component: component, bytes: sizeBytes,
	})
}

func (w *wiringRecorder) RecordMaintenanceOperation(cluster, namespace, operation, result string) {
	w.maintenanceCalls = append(w.maintenanceCalls, maintenanceCall{
		cluster: cluster, namespace: namespace, operation: operation, result: result,
	})
}

func (w *wiringRecorder) SetRedistributionProgress(cluster, namespace string, progress float64) {
	w.redistributionCalls = append(w.redistributionCalls, redistributionCall{
		cluster: cluster, namespace: namespace, progress: progress,
	})
}

// vacuumErrDBClient embeds the shared mockDBClient and overrides Vacuum to
// force a maintenance-via-DB failure, driving the Job-fallback path.
type vacuumErrDBClient struct {
	*mockDBClient
	vacuumErr error
}

func (m *vacuumErrDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error {
	return m.vacuumErr
}

// progressDBClient embeds the shared mockDBClient and overrides
// GetRedistributionProgress to return configurable progress/error values.
type progressDBClient struct {
	*mockDBClient
	progress    int32
	progressErr error
}

func (m *progressDBClient) GetRedistributionProgress(_ context.Context) (int32, error) {
	return m.progress, m.progressErr
}

// ============================================================================
// GAP-4: recordPVCSize
// ============================================================================

func TestClusterReconciler_RecordPVCSize_ParseError(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), rec, nil)

	// Act: an invalid quantity string triggers the early return.
	r.recordPVCSize(cluster, "coordinator", "not-a-quantity")

	// Assert: nothing recorded.
	assert.Empty(t, rec.pvcSizeCalls)
}

func TestClusterReconciler_RecordPVCSize_Success(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), rec, nil)

	// Act: a valid quantity records the byte value.
	r.recordPVCSize(cluster, "segment", "20Gi")

	// Assert
	require.Len(t, rec.pvcSizeCalls, 1)
	want := resource.MustParse("20Gi")
	assert.Equal(t, "test-cluster", rec.pvcSizeCalls[0].cluster)
	assert.Equal(t, "default", rec.pvcSizeCalls[0].namespace)
	assert.Equal(t, "segment", rec.pvcSizeCalls[0].component)
	assert.InDelta(t, float64(want.Value()), rec.pvcSizeCalls[0].bytes, 0.001)
}

// ============================================================================
// GAP-5: handleMaintenance Job-fallback path
// ============================================================================

func TestAdminReconciler_HandleMaintenance_DBFailureFallsBackToJob(t *testing.T) {
	// Arrange: DB-exec fails -> records "failed" then creates the Job and
	// records "started".
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &wiringRecorder{}
	dbClient := &vacuumErrDBClient{
		mockDBClient: &mockDBClient{},
		vacuumErr:    fmt.Errorf("vacuum failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	// Act
	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)

	// Assert
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
	require.Len(t, rec.maintenanceCalls, 2)
	assert.Equal(t, "failed", rec.maintenanceCalls[0].result)
	assert.Equal(t, "started", rec.maintenanceCalls[1].result)
	assert.Equal(t, util.MaintenanceVacuum, rec.maintenanceCalls[1].operation)
}

func TestAdminReconciler_HandleMaintenance_DBFailureJobAlreadyExists(t *testing.T) {
	// Arrange: Create returns AlreadyExists -> branch is handled gracefully and
	// "started" is still recorded.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption,
			) error {
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: "batch", Resource: "jobs"},
					obj.GetName(),
				)
			},
		}).
		Build()
	rec := &wiringRecorder{}
	dbClient := &vacuumErrDBClient{
		mockDBClient: &mockDBClient{},
		vacuumErr:    fmt.Errorf("vacuum failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	// Act
	result, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)

	// Assert: no error despite AlreadyExists; "failed" then "started" recorded.
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
	require.Len(t, rec.maintenanceCalls, 2)
	assert.Equal(t, "failed", rec.maintenanceCalls[0].result)
	assert.Equal(t, "started", rec.maintenanceCalls[1].result)
}

func TestAdminReconciler_HandleMaintenance_JobCreateError(t *testing.T) {
	// Arrange: Create returns a non-AlreadyExists error -> propagated.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.CreateOption,
			) error {
				return fmt.Errorf("api server unavailable")
			},
		}).
		Build()
	rec := &wiringRecorder{}
	dbClient := &vacuumErrDBClient{
		mockDBClient: &mockDBClient{},
		vacuumErr:    fmt.Errorf("vacuum failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	// Act
	_, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating maintenance job")
	// "failed" recorded on the DB path, but "started" never reached.
	require.Len(t, rec.maintenanceCalls, 1)
	assert.Equal(t, "failed", rec.maintenanceCalls[0].result)
}

// ============================================================================
// GAP-6: redistributeData progress branches
// ============================================================================

func TestClusterReconciler_RedistributeData_ProgressError(t *testing.T) {
	// Arrange: GetRedistributionProgress errors -> falls back to 1.0.
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbClient := &progressDBClient{
		mockDBClient: &mockDBClient{},
		progressErr:  fmt.Errorf("progress query failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil, dbFactory)

	// Act
	err := r.redistributeData(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	require.Len(t, rec.redistributionCalls, 1)
	assert.InDelta(t, 1.0, rec.redistributionCalls[0].progress, 0.001)
}

func TestClusterReconciler_RedistributeData_ProgressBelow100(t *testing.T) {
	// Arrange: progress 40% -> converted to 0.4.
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbClient := &progressDBClient{
		mockDBClient: &mockDBClient{},
		progress:     40,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil, dbFactory)

	// Act
	err := r.redistributeData(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	require.Len(t, rec.redistributionCalls, 1)
	assert.InDelta(t, 0.4, rec.redistributionCalls[0].progress, 0.001)
}

func TestClusterReconciler_RedistributeData_ProgressComplete(t *testing.T) {
	// Arrange: progress 100% -> 1.0 (existing happy path with assertion).
	scheme := newTestScheme()
	cluster := newTestCluster()
	dbClient := &progressDBClient{
		mockDBClient: &mockDBClient{},
		progress:     100,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil, dbFactory)

	// Act
	err := r.redistributeData(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	require.Len(t, rec.redistributionCalls, 1)
	assert.InDelta(t, 1.0, rec.redistributionCalls[0].progress, 0.001)
}

// ============================================================================
// GAP-7: Reconcile early-return SetSpanError paths
// ============================================================================

func TestClusterReconciler_Reconcile_HandleActionError_SetsSpanError(t *testing.T) {
	// Arrange: action present, but the status subresource is unregistered so
	// the action's status patch fails, driving the handleAction error branch.
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
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)

	// Act
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	// Assert
	require.Error(t, err)
}

func TestClusterReconciler_Reconcile_FinalizerUpdateError_SetsSpanError(t *testing.T) {
	// Arrange: no finalizer present so Reconcile tries to add it; Update fails.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Finalizers = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.UpdateOption,
			) error {
				return fmt.Errorf("conflict updating finalizer")
			},
		}).
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)

	// Act
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "adding finalizer")
}

// ============================================================================
// GAP-8: expandSegmentPVCs mirror-expansion error log path
// ============================================================================

func TestClusterReconciler_ExpandSegmentPVCs_MirrorExpansionError(t *testing.T) {
	// Arrange: a mirror PVC exists and must be expanded, but Update fails for
	// the mirror PVC, driving the "failed to expand mirror PVC" error log path.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 1
	cluster.Spec.Segments.Storage.Size = "30Gi"
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	mirrorPVCName := fmt.Sprintf("data-%s-0", util.SegmentMirrorName(cluster.Name))
	mirrorPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mirrorPVCName,
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("20Gi"),
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mirrorPVC).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.UpdateOption,
			) error {
				if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok &&
					pvc.Name == mirrorPVCName {
					return fmt.Errorf("update mirror pvc failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	rec := &wiringRecorder{}
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), rec, nil)

	// Act
	expanded := r.expandSegmentPVCs(context.Background(), cluster)

	// Assert: mirror expansion failed, so nothing was expanded.
	assert.False(t, expanded)
	assert.Empty(t, rec.pvcSizeCalls)
}

// ============================================================================
// Exporter prerequisite helpers (pre-existing untested code)
// ============================================================================

func newExporterTestReconciler(
	objs ...client.Object,
) (*ClusterReconciler, client.Client) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	return r, k8sClient
}

func TestClusterReconciler_BuildExporterDSN(t *testing.T) {
	r, _ := newExporterTestReconciler()

	t.Run("default port", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Coordinator.Port = 0
		dsn := r.buildExporterDSN(cluster, "p@ss/word")
		assert.Contains(t, dsn, "cloudberry_exporter:")
		// Password must be URL-escaped.
		assert.Contains(t, dsn, "p%40ss%2Fword")
		assert.Contains(t, dsn, fmt.Sprintf(":%d/postgres", util.DefaultCoordinatorPort))
	})

	t.Run("custom port", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Coordinator.Port = 6000
		dsn := r.buildExporterDSN(cluster, "pw")
		assert.Contains(t, dsn, ":6000/postgres")
	})
}

func TestClusterReconciler_ResolveExporterPrereqPassword(t *testing.T) {
	t.Run("existing secret with password", func(t *testing.T) {
		cluster := newTestCluster()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterCredentialsSecretName(cluster.Name),
				Namespace: cluster.Namespace,
			},
			Data: map[string][]byte{"password": []byte("existing-pw")},
		}
		r, _ := newExporterTestReconciler(cluster, secret)

		pw, missing, err := r.resolveExporterPrereqPassword(context.Background(), cluster)
		require.NoError(t, err)
		assert.Equal(t, "existing-pw", pw)
		assert.False(t, missing)
	})

	t.Run("secret missing generates password", func(t *testing.T) {
		cluster := newTestCluster()
		r, _ := newExporterTestReconciler(cluster)

		pw, missing, err := r.resolveExporterPrereqPassword(context.Background(), cluster)
		require.NoError(t, err)
		assert.NotEmpty(t, pw)
		assert.True(t, missing)
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
						return fmt.Errorf("api server error")
					}
					return nil
				},
			}).
			Build()
		r := NewClusterReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

		_, _, err := r.resolveExporterPrereqPassword(context.Background(), cluster)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting exporter credentials secret")
	})
}

func TestClusterReconciler_CreateExporterPrereqSecret(t *testing.T) {
	t.Run("creates secret", func(t *testing.T) {
		cluster := newTestCluster()
		r, k8sClient := newExporterTestReconciler(cluster)

		err := r.createExporterPrereqSecret(context.Background(), cluster, "pw", "dsn")
		require.NoError(t, err)

		got := &corev1.Secret{}
		getErr := k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      util.ExporterCredentialsSecretName(cluster.Name),
			Namespace: cluster.Namespace,
		}, got)
		require.NoError(t, getErr)
	})

	t.Run("already exists is ignored", func(t *testing.T) {
		scheme := newTestScheme()
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Create: func(
					_ context.Context, _ client.WithWatch, obj client.Object,
					_ ...client.CreateOption,
				) error {
					return apierrors.NewAlreadyExists(
						schema.GroupResource{Resource: "secrets"}, obj.GetName())
				},
			}).
			Build()
		r := NewClusterReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

		err := r.createExporterPrereqSecret(context.Background(), cluster, "pw", "dsn")
		require.NoError(t, err)
	})

	t.Run("create error propagated", func(t *testing.T) {
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
		r := NewClusterReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

		err := r.createExporterPrereqSecret(context.Background(), cluster, "pw", "dsn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "creating exporter credentials secret")
	})
}

func TestClusterReconciler_CreateExporterPrereqConfigMap(t *testing.T) {
	t.Run("creates configmap when missing", func(t *testing.T) {
		cluster := newTestCluster()
		r, k8sClient := newExporterTestReconciler(cluster)

		err := r.createExporterPrereqConfigMap(context.Background(), cluster)
		require.NoError(t, err)

		got := &corev1.ConfigMap{}
		getErr := k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      util.ExporterQueriesConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
		}, got)
		require.NoError(t, getErr)
	})

	t.Run("already exists is a no-op", func(t *testing.T) {
		cluster := newTestCluster()
		existing := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.ExporterQueriesConfigMapName(cluster.Name),
				Namespace: cluster.Namespace,
			},
		}
		r, _ := newExporterTestReconciler(cluster, existing)

		err := r.createExporterPrereqConfigMap(context.Background(), cluster)
		require.NoError(t, err)
	})
}

func TestClusterReconciler_EnsureExporterPrerequisites(t *testing.T) {
	t.Run("happy path creates secret and configmap", func(t *testing.T) {
		cluster := newTestCluster()
		r, k8sClient := newExporterTestReconciler(cluster)

		err := r.ensureExporterPrerequisites(context.Background(), cluster)
		require.NoError(t, err)

		secret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      util.ExporterCredentialsSecretName(cluster.Name),
			Namespace: cluster.Namespace,
		}, secret))
	})

	t.Run("password resolution error", func(t *testing.T) {
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
						return fmt.Errorf("api server error")
					}
					return nil
				},
			}).
			Build()
		r := NewClusterReconciler(k8sClient, scheme,
			record.NewFakeRecorder(20), builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

		err := r.ensureExporterPrerequisites(context.Background(), cluster)
		require.Error(t, err)
	})
}
