package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// ============================================================================
// startOrUpdateIdleDaemon Tests
// ============================================================================

func TestAdminReconciler_StartOrUpdateIdleDaemon_NoIdleRules(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled:   true,
		IdleRules: []cbv1alpha1.IdleSessionRule{}, // Empty rules
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

	// Should stop daemon (no rules).
	r.startOrUpdateIdleDaemon(context.Background(), cluster)
	// No panic expected.
}

func TestAdminReconciler_StartOrUpdateIdleDaemon_NoEnabledRules(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "rule1", Enabled: false, ResourceGroup: "grp", IdleTimeout: "5m"},
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

	// Should stop daemon (no enabled rules).
	r.startOrUpdateIdleDaemon(context.Background(), cluster)
}

func TestAdminReconciler_StartOrUpdateIdleDaemon_InvalidDuration(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "bad-rule", Enabled: true, ResourceGroup: "grp", IdleTimeout: "invalid"},
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

	// Should log error and return (invalid duration).
	r.startOrUpdateIdleDaemon(context.Background(), cluster)
}

func TestAdminReconciler_StartOrUpdateIdleDaemon_DaemonAlreadyRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "rule1", Enabled: true, ResourceGroup: "grp", IdleTimeout: "5m"},
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

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// First call: starts daemon.
	r.startOrUpdateIdleDaemon(context.Background(), cluster)

	// Second call: daemon already running, just updates rules.
	cluster.Spec.Workload.IdleRules = append(cluster.Spec.Workload.IdleRules,
		cbv1alpha1.IdleSessionRule{Name: "rule2", Enabled: true, ResourceGroup: "grp2", IdleTimeout: "10m"})
	r.startOrUpdateIdleDaemon(context.Background(), cluster)

	// Cleanup.
	r.stopIdleDaemon()
}

func TestAdminReconciler_StartOrUpdateIdleDaemon_DBClientCreationFailure(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "rule1", Enabled: true, ResourceGroup: "grp", IdleTimeout: "5m"},
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

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Should log error and return (DB client creation failure).
	r.startOrUpdateIdleDaemon(context.Background(), cluster)
}

// ============================================================================
// cleanupWorkload Tests
// ============================================================================

func TestAdminReconciler_CleanupWorkload_NilDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.cleanupWorkload(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_CleanupWorkload_DBClientCreationFailure(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.cleanupWorkload(context.Background(), cluster)
	require.NoError(t, err) // Errors are logged, not returned.
}

func TestAdminReconciler_CleanupWorkload_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	// Create the workload-rules ConfigMap so it can be deleted.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-workload-rules",
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{"idle-rules.json": "[]"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cm).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.cleanupWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify ConfigMap was deleted.
	getCM := &corev1.ConfigMap{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name: cluster.Name + "-workload-rules", Namespace: cluster.Namespace,
	}, getCM)
	assert.True(t, getErr != nil, "ConfigMap should be deleted")
}

// ============================================================================
// dropAllUserResourceGroups Tests
// ============================================================================

func TestAdminReconciler_DropAllUserResourceGroups_ListError(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Create a mock that returns error on ListResourceGroups.
	mockClient := &mockDBClientWithListGroups{
		mockDBClient: &mockDBClient{},
		listErr:      fmt.Errorf("list failed"),
	}

	// Should not panic, just log warning.
	r.dropAllUserResourceGroups(context.Background(), cluster, mockClient, r.logger)
}

func TestAdminReconciler_DropAllUserResourceGroups_DropError(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	mockClient := &mockDBClientWithListGroups{
		mockDBClient: &mockDBClient{},
		groups: []db.ResourceGroupInfo{
			{Name: "analytics"},
			{Name: "etl"},
		},
		dropErr: fmt.Errorf("drop failed"),
	}

	// Should not panic, just log warnings for each failed drop.
	r.dropAllUserResourceGroups(context.Background(), cluster, mockClient, r.logger)
}

func TestAdminReconciler_DropAllUserResourceGroups_Success(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	mockClient := &mockDBClientWithListGroups{
		mockDBClient: &mockDBClient{},
		groups: []db.ResourceGroupInfo{
			{Name: "analytics"},
		},
	}

	r.dropAllUserResourceGroups(context.Background(), cluster, mockClient, r.logger)
}

// ============================================================================
// deleteWorkloadRulesConfigMap Tests
// ============================================================================

func TestAdminReconciler_DeleteWorkloadRulesConfigMap_NotFound(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// ConfigMap doesn't exist — should be a no-op.
	err := r.deleteWorkloadRulesConfigMap(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdminReconciler_DeleteWorkloadRulesConfigMap_GetError(t *testing.T) {
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

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Should return an error.
	err := r.deleteWorkloadRulesConfigMap(context.Background(), cluster)
	if err == nil {
		t.Fatal("expected error from deleteWorkloadRulesConfigMap, got nil")
	}
}

func TestAdminReconciler_DeleteWorkloadRulesConfigMap_DeleteError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-workload-rules",
			Namespace: cluster.Namespace,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cm).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("delete failed")
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Should return an error.
	err := r.deleteWorkloadRulesConfigMap(context.Background(), cluster)
	if err == nil {
		t.Fatal("expected error from deleteWorkloadRulesConfigMap, got nil")
	}
}

// ============================================================================
// reconcileSubComponents error paths
// ============================================================================

func TestAdminReconciler_ReconcileSubComponents_AllFeaturesEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "idle-rule", Enabled: true, ResourceGroup: "analytics", IdleTimeout: "5m"},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", Bucket: "b"},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * 0",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Should not panic even with all features enabled.
	r.reconcileSubComponents(context.Background(), r.logger, cluster)
}

// ============================================================================
// idleDaemonDBClientFactory Tests
// ============================================================================

func TestIdleDaemonDBClientFactory_NewClient(t *testing.T) {
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}

	factory := &idleDaemonDBClientFactory{
		dbFactory: dbFactory,
		cluster:   cluster,
	}

	client, err := factory.NewClient(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestIdleDaemonDBClientFactory_NewClient_Error(t *testing.T) {
	cluster := newTestCluster()
	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	factory := &idleDaemonDBClientFactory{
		dbFactory: dbFactory,
		cluster:   cluster,
	}

	_, err := factory.NewClient(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// ============================================================================
// mockDBClientWithListGroups - extends mockDBClient with configurable ListResourceGroups
// ============================================================================

type mockDBClientWithListGroups struct {
	*mockDBClient
	groups  []db.ResourceGroupInfo
	listErr error
	dropErr error
}

func (m *mockDBClientWithListGroups) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return m.groups, m.listErr
}

func (m *mockDBClientWithListGroups) DropResourceGroup(_ context.Context, _ string) error {
	return m.dropErr
}
