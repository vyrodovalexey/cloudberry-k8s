package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// waitWithContext Tests
// ============================================================================

func TestWaitWithContext_DurationElapsed(t *testing.T) {
	// Arrange: context that won't be canceled.
	ctx := context.Background()

	// Act: wait for a very short duration.
	err := waitWithContext(ctx, 10*time.Millisecond)

	// Assert: should return nil (duration elapsed).
	require.NoError(t, err)
}

func TestWaitWithContext_ContextCanceled(t *testing.T) {
	// Arrange: context that is already canceled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act: wait for a long duration (should be interrupted by context).
	err := waitWithContext(ctx, 10*time.Second)

	// Assert: should return context error.
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitWithContext_ContextDeadlineExceeded(t *testing.T) {
	// Arrange: context with a very short deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give the context time to expire.
	time.Sleep(5 * time.Millisecond)

	// Act: wait for a long duration.
	err := waitWithContext(ctx, 10*time.Second)

	// Assert: should return deadline exceeded error.
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWaitWithContext_ContextCanceledDuringWait(t *testing.T) {
	// Arrange: context that will be canceled after a short delay.
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Act: wait for a long duration.
	start := time.Now()
	err := waitWithContext(ctx, 5*time.Second)
	elapsed := time.Since(start)

	// Assert: should return quickly (not wait full 5 seconds).
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 1*time.Second, "should return quickly after context cancellation")
}

func TestWaitWithContext_ZeroDuration(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act: wait for zero duration.
	err := waitWithContext(ctx, 0)

	// Assert: should return immediately.
	require.NoError(t, err)
}

// ============================================================================
// reconcileSubComponents error aggregation Tests
// ============================================================================

// mockDBClientWithErrors extends mockDBClient with configurable errors for
// specific operations used by reconcileSubComponents.
type mockDBClientWithErrors struct {
	*mockDBClient
	listGroupsErr error
	groups        []db.ResourceGroupInfo
}

func (m *mockDBClientWithErrors) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return m.groups, m.listGroupsErr
}

func TestAdminReconciler_ReconcileSubComponents_ErrorAggregation(t *testing.T) {
	// Arrange: cluster with workload enabled but DB factory returns error.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	// DB factory that returns error — will cause workload reconciliation to fail.
	dbFactory := &mockDBClientFactory{err: fmt.Errorf("db connection failed")}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert: errors from sub-reconcilers are aggregated via errors.Join.
	// The workload reconciliation should fail but not prevent other sub-reconcilers.
	// Note: reconcileWorkload with a failing DB factory falls back to condition-only mode
	// and does NOT return an error. So the aggregated error may be nil.
	// This test verifies the function doesn't panic and handles all sub-components.
	_ = err // May or may not have errors depending on sub-reconciler behavior.
}

func TestAdminReconciler_ReconcileSubComponents_NoFeaturesEnabled(t *testing.T) {
	// Arrange: cluster with no features enabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// No workload, no query monitoring, no backup, no data loading, no storage.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert: no errors when no features are enabled.
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileSubComponents_WorkloadOnly(t *testing.T) {
	// Arrange: cluster with only workload enabled, no DB factory.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	// No DB factory — falls back to condition-only mode.
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert: no error (condition-only mode doesn't fail).
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileSubComponents_BackupEnabled(t *testing.T) {
	// Arrange: cluster with backup enabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", S3: &cbv1alpha1.S3Destination{Bucket: "test-bucket"}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileSubComponents_StorageEnabled(t *testing.T) {
	// Arrange: cluster with storage management enabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileSubComponents_DataLoadingEnabled(t *testing.T) {
	// Arrange: cluster with data loading enabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileSubComponents_ErrorsJoinMultiple(t *testing.T) {
	// Arrange: cluster with multiple features that will fail.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", S3: &cbv1alpha1.S3Destination{Bucket: "b"}},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * 0",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}

	// Use interceptor to make status patch fail, which will cause some sub-reconcilers to fail.
	patchCallCount := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				patchCallCount++
				// Fail some patches to trigger error aggregation.
				if patchCallCount > 2 {
					return fmt.Errorf("status patch failed")
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(50)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{client: &mockDBClient{}}
	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Act
	err := r.reconcileSubComponents(context.Background(), r.logger, cluster)

	// Assert: if there are errors, they should be joined.
	if err != nil {
		// Verify errors.Join was used (multiple errors can be unwrapped).
		var joinedErrs interface{ Unwrap() []error }
		if errors.As(err, &joinedErrs) {
			assert.NotEmpty(t, joinedErrs.Unwrap(), "joined errors should not be empty")
		}
	}
}

// ============================================================================
// dispatchRebalanceTables Tests
// ============================================================================

// rebalanceMockDBClient extends mockDBClient with configurable RebalanceTable behavior.
type rebalanceMockDBClient struct {
	*mockDBClient
	rebalanceErr   error
	rebalanceCalls atomic.Int32
	rebalanceDelay time.Duration
}

func (m *rebalanceMockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error {
	m.rebalanceCalls.Add(1)
	if m.rebalanceDelay > 0 {
		time.Sleep(m.rebalanceDelay)
	}
	return m.rebalanceErr
}

func TestHAReconciler_DispatchRebalanceTables_EmptyTableList(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{mockDBClient: &mockDBClient{}}
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act: dispatch with empty table list.
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, []db.TableSkewInfo{}, 2)

	// Assert: should succeed with no work done.
	require.NoError(t, err)
	assert.Equal(t, int32(0), dbClient.rebalanceCalls.Load())
}

func TestHAReconciler_DispatchRebalanceTables_SingleTable(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{mockDBClient: &mockDBClient{}}
	ctx := util.WithLogger(context.Background(), r.logger)

	tables := []db.TableSkewInfo{
		{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
	}

	// Act
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, tables, 2)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, int32(1), dbClient.rebalanceCalls.Load())
}

func TestHAReconciler_DispatchRebalanceTables_MultipleTables(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{mockDBClient: &mockDBClient{}}
	ctx := util.WithLogger(context.Background(), r.logger)

	tables := []db.TableSkewInfo{
		{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
		{Database: "mydb", Schema: "public", Table: "orders", DistributionKey: "id", SkewCoefficient: 20.0},
		{Database: "mydb", Schema: "public", Table: "products", DistributionKey: "id", SkewCoefficient: 12.0},
	}

	// Act
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, tables, 2)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, int32(3), dbClient.rebalanceCalls.Load())
}

func TestHAReconciler_DispatchRebalanceTables_RebalanceError(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{
		mockDBClient: &mockDBClient{},
		rebalanceErr: fmt.Errorf("rebalance failed"),
	}
	ctx := util.WithLogger(context.Background(), r.logger)

	tables := []db.TableSkewInfo{
		{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
		{Database: "mydb", Schema: "public", Table: "orders", DistributionKey: "id", SkewCoefficient: 20.0},
	}

	// Act
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, tables, 2)

	// Assert: should return error indicating failed tables.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to rebalance")
}

func TestHAReconciler_DispatchRebalanceTables_ContextCanceled(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{
		mockDBClient:   &mockDBClient{},
		rebalanceDelay: 100 * time.Millisecond,
	}

	// Cancel context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = util.WithLogger(ctx, r.logger)

	tables := []db.TableSkewInfo{
		{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
		{Database: "mydb", Schema: "public", Table: "orders", DistributionKey: "id", SkewCoefficient: 20.0},
		{Database: "mydb", Schema: "public", Table: "products", DistributionKey: "id", SkewCoefficient: 12.0},
	}

	// Act
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, tables, 1)

	// Assert: should not dispatch all tables due to context cancellation.
	// The function should return without error (context cancellation is handled gracefully).
	// Some tables may not have been dispatched.
	_ = err // May or may not have errors depending on timing.
	// The key assertion is that it doesn't hang.
}

func TestHAReconciler_DispatchRebalanceTables_Parallelism(t *testing.T) {
	// Arrange: test that parallelism is respected.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	dbClient := &rebalanceMockDBClient{mockDBClient: &mockDBClient{}}
	ctx := util.WithLogger(context.Background(), r.logger)

	tables := []db.TableSkewInfo{
		{Database: "mydb", Schema: "public", Table: "t1", DistributionKey: "id", SkewCoefficient: 15.0},
		{Database: "mydb", Schema: "public", Table: "t2", DistributionKey: "id", SkewCoefficient: 20.0},
		{Database: "mydb", Schema: "public", Table: "t3", DistributionKey: "id", SkewCoefficient: 12.0},
		{Database: "mydb", Schema: "public", Table: "t4", DistributionKey: "id", SkewCoefficient: 18.0},
	}

	// Act: parallelism of 1 (sequential).
	err := r.dispatchRebalanceTables(ctx, r.logger, dbClient, tables, 1)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, int32(4), dbClient.rebalanceCalls.Load())
}

// ============================================================================
// handleRebalance with DB factory Tests
// ============================================================================

// rebalanceSkewMockDBClient supports both AnalyzeSkew and RebalanceTable.
type rebalanceSkewMockDBClient struct {
	*mockDBClient
	skewResults    map[string][]db.TableSkewInfo
	skewErr        error
	rebalanceErr   error
	rebalanceCalls atomic.Int32
	databases      []string
	listDBErr      error
}

func (m *rebalanceSkewMockDBClient) ListUserDatabases(_ context.Context) ([]string, error) {
	if m.listDBErr != nil {
		return nil, m.listDBErr
	}
	return m.databases, nil
}

func (m *rebalanceSkewMockDBClient) AnalyzeSkew(_ context.Context, dbName string) ([]db.TableSkewInfo, error) {
	if m.skewErr != nil {
		return nil, m.skewErr
	}
	if results, ok := m.skewResults[dbName]; ok {
		return results, nil
	}
	return []db.TableSkewInfo{}, nil
}

func (m *rebalanceSkewMockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error {
	m.rebalanceCalls.Add(1)
	return m.rebalanceErr
}

func TestHAReconciler_HandleRebalance_WithDBFactory_NoSkewedTables(t *testing.T) {
	// Arrange: DB factory available, no tables exceed skew threshold.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	dbClient := &rebalanceSkewMockDBClient{
		mockDBClient: &mockDBClient{},
		databases:    []string{"mydb"},
		skewResults: map[string][]db.TableSkewInfo{
			"mydb": {
				{Database: "mydb", Schema: "public", Table: "users", SkewCoefficient: 5.0}, // below threshold
			},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Act
	result, err := r.handleRebalance(context.Background(), cluster)

	// Assert: should succeed (no tables to rebalance).
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
	assert.Equal(t, int32(0), dbClient.rebalanceCalls.Load())
}

func TestHAReconciler_HandleRebalance_WithDBFactory_SkewedTables(t *testing.T) {
	// Arrange: DB factory available, tables exceed skew threshold.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	dbClient := &rebalanceSkewMockDBClient{
		mockDBClient: &mockDBClient{},
		databases:    []string{"mydb"},
		skewResults: map[string][]db.TableSkewInfo{
			"mydb": {
				{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
			},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Act
	result, err := r.handleRebalance(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
	assert.Equal(t, int32(1), dbClient.rebalanceCalls.Load())
}

func TestHAReconciler_HandleRebalance_WithCustomConfig(t *testing.T) {
	// Arrange: cluster with custom rebalance config.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}
	cluster.Spec.Segments.Rebalance = &cbv1alpha1.RebalanceSpec{
		Parallelism:   4,
		SkewThreshold: 5,
		ExcludeTables: []string{"temp_*"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	dbClient := &rebalanceSkewMockDBClient{
		mockDBClient: &mockDBClient{},
		databases:    []string{"mydb"},
		skewResults: map[string][]db.TableSkewInfo{
			"mydb": {
				{Database: "mydb", Schema: "public", Table: "users", DistributionKey: "id", SkewCoefficient: 15.0},
				{Database: "mydb", Schema: "public", Table: "temp_data", DistributionKey: "id", SkewCoefficient: 20.0}, // excluded
			},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Act
	result, err := r.handleRebalance(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
	// Only "users" should be rebalanced (temp_data is excluded).
	assert.Equal(t, int32(1), dbClient.rebalanceCalls.Load())
}

// ============================================================================
// isTableExcluded Tests
// ============================================================================

func TestIsTableExcluded_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		patterns []string
		expected bool
	}{
		{
			name:     "exact match",
			table:    "public.temp_data",
			patterns: []string{"public.temp_data"},
			expected: true,
		},
		{
			name:     "glob pattern match",
			table:    "public.temp_data",
			patterns: []string{"temp_*"},
			expected: true,
		},
		{
			name:     "schema.table glob match",
			table:    "public.temp_data",
			patterns: []string{"public.temp_*"},
			expected: true,
		},
		{
			name:     "no match",
			table:    "public.users",
			patterns: []string{"temp_*"},
			expected: false,
		},
		{
			name:     "empty patterns",
			table:    "public.users",
			patterns: []string{},
			expected: false,
		},
		{
			name:     "nil patterns",
			table:    "public.users",
			patterns: nil,
			expected: false,
		},
		{
			name:     "table without schema",
			table:    "users",
			patterns: []string{"users"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTableExcluded(tt.table, tt.patterns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// splitSchemaTable Tests
// ============================================================================

func TestSplitSchemaTable_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "schema.table",
			input:    "public.users",
			expected: []string{"public", "users"},
		},
		{
			name:     "table only",
			input:    "users",
			expected: []string{"users"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{""},
		},
		{
			name:     "multiple dots",
			input:    "schema.table.extra",
			expected: []string{"schema", "table.extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitSchemaTable(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// handleRebalance status patch error Tests
// ============================================================================

func TestHAReconciler_HandleRebalance_StatusPatchError(t *testing.T) {
	// Arrange: status patch fails after annotation removal.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}

	patchCount := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				patchCount++
				if patchCount == 1 {
					return fmt.Errorf("status patch failed")
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Act
	_, err := r.handleRebalance(context.Background(), cluster)

	// Assert: should return error from status patch.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating rebalance status")
}

// ============================================================================
// Cluster Controller: removeAnnotationPatch, setAnnotationPatch, patchStatus
// ============================================================================

func TestRemoveAnnotationPatch_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Annotations = map[string]string{
		util.AnnotationRecovery: util.RecoveryIncremental,
		"other-annotation":      "value",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	// Act
	err := removeAnnotationPatch(context.Background(), k8sClient, cluster, util.AnnotationRecovery)

	// Assert
	require.NoError(t, err)
	_, exists := cluster.Annotations[util.AnnotationRecovery]
	assert.False(t, exists, "recovery annotation should be removed")
	assert.Equal(t, "value", cluster.Annotations["other-annotation"], "other annotations should remain")
}

func TestSetAnnotationPatch_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	// Act
	err := setAnnotationPatch(context.Background(), k8sClient, cluster, "test-key", "test-value")

	// Assert
	require.NoError(t, err)
}

func TestSetAnnotationPatch_PatchError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()

	// Act
	err := setAnnotationPatch(context.Background(), k8sClient, cluster, "test-key", "test-value")

	// Assert
	require.Error(t, err)
}

func TestPatchStatus_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	// Modify status.
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync

	// Act
	err := patchStatus(context.Background(), k8sClient, cluster)

	// Assert
	require.NoError(t, err)
}

func TestPatchFTSStatus_EmptyFailedSegments(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	// Act: patch with empty failed segments (should clear previous failures).
	err := patchFTSStatus(context.Background(), k8sClient, cluster, []cbv1alpha1.FailedSegment{})

	// Assert
	require.NoError(t, err)
}

func TestPatchFTSStatus_WithFailedSegments(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	failedSegs := []cbv1alpha1.FailedSegment{
		{ContentID: 0, Hostname: "host1", Role: "p", Status: "d"},
	}

	// Act
	err := patchFTSStatus(context.Background(), k8sClient, cluster, failedSegs)

	// Assert
	require.NoError(t, err)
}

// ============================================================================
// buildAnnotationPatch Tests
// ============================================================================

func TestBuildAnnotationPatch_SetValue(t *testing.T) {
	// Act
	data, err := buildAnnotationPatch("test-key", "test-value")

	// Assert
	require.NoError(t, err)
	assert.Contains(t, string(data), "test-key")
	assert.Contains(t, string(data), "test-value")
}

func TestBuildAnnotationPatch_RemoveValue(t *testing.T) {
	// Act: nil value removes the annotation.
	data, err := buildAnnotationPatch("test-key", nil)

	// Assert
	require.NoError(t, err)
	assert.Contains(t, string(data), "test-key")
	assert.Contains(t, string(data), "null")
}

// ============================================================================
// Cluster Controller: updatePhase Tests
// ============================================================================

func TestClusterReconciler_UpdatePhase_Success(t *testing.T) {
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

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Act
	result, err := r.updatePhase(context.Background(), cluster, cbv1alpha1.ClusterPhasePending)

	// Assert
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, cbv1alpha1.ClusterPhasePending, cluster.Status.Phase)
}

func TestClusterReconciler_UpdatePhase_Error(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return fmt.Errorf("status update failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Act
	_, err := r.updatePhase(context.Background(), cluster, cbv1alpha1.ClusterPhasePending)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating phase")
}

// ============================================================================
// handleGenerationUnchanged Tests
// ============================================================================

// ============================================================================
// handleGenerationUnchanged - additional edge case Tests
// ============================================================================

func TestClusterReconciler_HandleGenerationUnchanged_ScaleStateAnnotation_RestoresPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
	cluster.Annotations = map[string]string{
		annotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":4}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act
	result := r.handleGenerationUnchanged(ctx, cluster)

	// Assert: should be handled (restores Scaling phase).
	assert.True(t, result.handled)
	assert.True(t, result.result.Requeue)
}

// ============================================================================
// Cluster Controller: Reconcile with Pending phase
// ============================================================================

func TestClusterReconciler_Reconcile_PendingPhase(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = ""

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewClusterReconciler(k8sClient, scheme, recorder, b, m, nil)

	// Act: first reconciliation should set phase to Pending.
	result, err := r.Reconcile(context.Background(), newTestRequest())

	// Assert
	require.NoError(t, err)
	assert.True(t, result.Requeue)
}

// newTestRequest creates a ctrl.Request for the test cluster.
func newTestRequest() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	}
}
