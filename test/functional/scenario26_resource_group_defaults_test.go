//go:build functional

package functional

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 26: Resource Group Default Values
// ============================================================================
//
// Verifies that when a resource group is created with only a name specified,
// the mutating webhook and reconciler apply correct default values:
//   - concurrency: 20 (set by mutating webhook)
//   - cpuMaxPercent: 100 (set by mutating webhook)
//   - cpuWeight: 100 (set by mutating webhook)
//   - memoryLimit: 0 (Go zero value = unlimited)
//   - minCost: 0 (Go zero value)
// ============================================================================

const (
	scenario26Namespace = "default"
)

// scenario26MockDBClient implements db.Client for resource group defaults tests.
// It tracks all resource group operations (create, alter, drop) with thread-safe
// recording so tests can verify the exact calls made by the reconciler.
type scenario26MockDBClient struct {
	testutil.MockDBClient

	mu sync.Mutex

	// createCalls records all CreateResourceGroup calls with their options.
	createCalls []db.ResourceGroupOptions
	// alterCalls records all AlterResourceGroup calls with their options.
	alterCalls []db.ResourceGroupOptions
	// dropCalls records all DropResourceGroup calls with the group names.
	dropCalls []string

	// listResourceGroups is the list returned by ListResourceGroups.
	listResourceGroups []db.ResourceGroupInfo
	// listResourceGroupsErr is the error returned by ListResourceGroups.
	listResourceGroupsErr error

	// usageMap maps group name to (cpu, mem) usage values.
	usageMap map[string][2]float64

	// usageCalls records all GetResourceGroupUsage calls with the group names.
	usageCalls []string
}

func newScenario26MockDBClient() *scenario26MockDBClient {
	m := &scenario26MockDBClient{
		usageMap: make(map[string][2]float64),
	}

	// Wire up the function-based mock methods to our tracking implementations.
	m.CreateResourceGroupFunc = m.trackCreateResourceGroup
	m.AlterResourceGroupFunc = m.trackAlterResourceGroup
	m.DropResourceGroupFunc = m.trackDropResourceGroup
	m.ListResourceGroupsFunc = m.trackListResourceGroups
	m.GetResourceGroupUsageFunc = m.trackGetResourceGroupUsage

	return m
}

func (m *scenario26MockDBClient) trackCreateResourceGroup(_ context.Context, opts db.ResourceGroupOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCalls = append(m.createCalls, opts)
	return nil
}

func (m *scenario26MockDBClient) trackAlterResourceGroup(_ context.Context, opts db.ResourceGroupOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alterCalls = append(m.alterCalls, opts)
	return nil
}

func (m *scenario26MockDBClient) trackDropResourceGroup(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropCalls = append(m.dropCalls, name)
	return nil
}

func (m *scenario26MockDBClient) trackListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listResourceGroupsErr != nil {
		return nil, m.listResourceGroupsErr
	}
	return m.listResourceGroups, nil
}

func (m *scenario26MockDBClient) trackGetResourceGroupUsage(_ context.Context, group string) (float64, float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usageCalls = append(m.usageCalls, group)
	if usage, ok := m.usageMap[group]; ok {
		return usage[0], usage[1], nil
	}
	return 0, 0, nil
}

func (m *scenario26MockDBClient) getCreateCalls() []db.ResourceGroupOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]db.ResourceGroupOptions, len(m.createCalls))
	copy(result, m.createCalls)
	return result
}

func (m *scenario26MockDBClient) getAlterCalls() []db.ResourceGroupOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]db.ResourceGroupOptions, len(m.alterCalls))
	copy(result, m.alterCalls)
	return result
}

func (m *scenario26MockDBClient) getDropCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.dropCalls))
	copy(result, m.dropCalls)
	return result
}

func (m *scenario26MockDBClient) getUsageCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.usageCalls))
	copy(result, m.usageCalls)
	return result
}

// scenario26MockDBFactory implements db.DBClientFactory for scenario 26 tests.
type scenario26MockDBFactory struct {
	client db.Client
	err    error
}

func (f *scenario26MockDBFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// ============================================================================
// Test Suite
// ============================================================================

// Scenario26ResourceGroupDefaultsSuite tests that resource groups created with
// only a name get correct default values applied by the mutating webhook and
// are properly reconciled by the admin controller.
type Scenario26ResourceGroupDefaultsSuite struct {
	suite.Suite
	ctx context.Context
}

func TestScenario26_ResourceGroupDefaults(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario26ResourceGroupDefaultsSuite))
}

func (s *Scenario26ResourceGroupDefaultsSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario26ResourceGroupDefaultsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// ============================================================================
// Test 26a: Defaults Applied - Name Only Resource Group Creates in DB
// ============================================================================

func (s *Scenario26ResourceGroupDefaultsSuite) TestScenario26a_DefaultsApplied_NameOnly() {
	// Arrange: cluster with workload enabled and a single resource group
	// with only name specified (all other fields zero).
	cluster := testutil.NewClusterBuilder("s26a-defaults-name-only", scenario26Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: "defaults-test",
			},
		},
	}

	// Simulate the mutating webhook applying defaults before reconciliation.
	// In a real cluster, the webhook runs before the object reaches the reconciler.
	defaulter := webhook.NewCloudberryClusterDefaulter()
	err := defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Verify webhook set the expected defaults on the spec.
	rg := cluster.Spec.Workload.ResourceGroups[0]
	assert.Equal(s.T(), int32(20), rg.Concurrency, "webhook should set concurrency=20")
	assert.Equal(s.T(), int32(100), rg.CPUMaxPercent, "webhook should set cpuMaxPercent=100")
	assert.Equal(s.T(), int32(100), rg.CPUWeight, "webhook should set cpuWeight=100")
	assert.Equal(s.T(), int32(0), rg.MemoryLimit, "memoryLimit should remain 0 (unlimited)")
	assert.Equal(s.T(), int32(0), rg.MinCost, "minCost should remain 0")

	mockClient := newScenario26MockDBClient()
	// No existing resource groups in DB — should be created.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}

	factory := &scenario26MockDBFactory{client: mockClient}
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify CreateResourceGroup was called with webhook-defaulted values.
	createCalls := mockClient.getCreateCalls()
	require.Len(s.T(), createCalls, 1, "expected 1 CreateResourceGroup call")

	opts := createCalls[0]
	assert.Equal(s.T(), "defaults-test", opts.Name)
	assert.Equal(s.T(), int32(20), opts.Concurrency, "concurrency should be 20 (webhook default)")
	assert.Equal(s.T(), int32(100), opts.CPUMaxPercent, "cpuMaxPercent should be 100 (webhook default)")
	assert.Equal(s.T(), int32(100), opts.CPUWeight, "cpuWeight should be 100 (webhook default)")
	assert.Equal(s.T(), int32(0), opts.MemoryLimit, "memoryLimit should be 0 (unlimited)")
	assert.Equal(s.T(), int32(0), opts.MinCost, "minCost should be 0")

	// Verify WorkloadConfigured condition is True.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}

// ============================================================================
// Test 26b: Mutating Webhook Sets Defaults on Resource Group
// ============================================================================

func (s *Scenario26ResourceGroupDefaultsSuite) TestScenario26b_MutatingWebhook_SetsDefaults() {
	// Arrange: create a CloudberryCluster with workload.resourceGroups
	// containing only name — all numeric fields are zero.
	cluster := testutil.NewClusterBuilder("s26b-webhook-defaults", scenario26Namespace).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: "defaults-test",
			},
		},
	}

	// Verify pre-condition: all fields are zero before webhook.
	rg := cluster.Spec.Workload.ResourceGroups[0]
	assert.Equal(s.T(), int32(0), rg.Concurrency, "concurrency should be 0 before webhook")
	assert.Equal(s.T(), int32(0), rg.CPUMaxPercent, "cpuMaxPercent should be 0 before webhook")
	assert.Equal(s.T(), int32(0), rg.CPUWeight, "cpuWeight should be 0 before webhook")
	assert.Equal(s.T(), int32(0), rg.MemoryLimit, "memoryLimit should be 0 before webhook")
	assert.Equal(s.T(), int32(0), rg.MinCost, "minCost should be 0 before webhook")

	// Act: call the mutating webhook's Default method.
	defaulter := webhook.NewCloudberryClusterDefaulter()
	err := defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Assert: verify the resource group fields are set to defaults.
	rg = cluster.Spec.Workload.ResourceGroups[0]
	assert.Equal(s.T(), "defaults-test", rg.Name, "name should be preserved")
	assert.Equal(s.T(), int32(20), rg.Concurrency, "concurrency should be defaulted to 20")
	assert.Equal(s.T(), int32(100), rg.CPUMaxPercent, "cpuMaxPercent should be defaulted to 100")
	assert.Equal(s.T(), int32(100), rg.CPUWeight, "cpuWeight should be defaulted to 100")
	assert.Equal(s.T(), int32(0), rg.MemoryLimit, "memoryLimit should remain 0 (unchanged, unlimited)")
	assert.Equal(s.T(), int32(0), rg.MinCost, "minCost should remain 0 (unchanged)")
}

// ============================================================================
// Test 26c: Defaults vs Explicit - No Alter Needed When Values Match
// ============================================================================

func (s *Scenario26ResourceGroupDefaultsSuite) TestScenario26c_DefaultsVsExplicit_NoAlterNeeded() {
	// Arrange: cluster with a resource group that has explicit values
	// matching the defaults (after webhook). DB returns the same values.
	cluster := testutil.NewClusterBuilder("s26c-no-alter", scenario26Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "defaults-test",
				Concurrency:   20,
				CPUMaxPercent: 100,
				CPUWeight:     100,
				MemoryLimit:   0,
				MinCost:       0,
			},
		},
	}

	mockClient := newScenario26MockDBClient()
	// DB returns the same resource group with matching default values.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{
		{
			Name:          "defaults-test",
			Concurrency:   20,
			CPUMaxPercent: 100,
			CPUWeight:     100,
			MemoryLimit:   0,
			MinCost:       0,
		},
	}

	factory := &scenario26MockDBFactory{client: mockClient}
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify NO AlterResourceGroup call was made (no changes needed).
	alterCalls := mockClient.getAlterCalls()
	assert.Empty(s.T(), alterCalls, "should not alter resource group when values match defaults")

	// Verify NO CreateResourceGroup call was made (group already exists).
	createCalls := mockClient.getCreateCalls()
	assert.Empty(s.T(), createCalls, "should not create resource group when it already exists")

	// Verify NO DropResourceGroup call was made.
	dropCalls := mockClient.getDropCalls()
	assert.Empty(s.T(), dropCalls, "should not drop any resource groups")

	// Verify WorkloadConfigured condition is True.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}

// ============================================================================
// Test 26d: Defaults Created in DB - Verify SQL Parameter Inclusion
// ============================================================================

func (s *Scenario26ResourceGroupDefaultsSuite) TestScenario26d_DefaultsCreatedInDB_VerifySQL() {
	// Arrange: cluster with defaults-only resource group (after webhook defaults applied).
	// This test verifies the exact ResourceGroupOptions passed to CreateResourceGroup,
	// which determines which parameters are included in the SQL CREATE statement.
	cluster := testutil.NewClusterBuilder("s26d-sql-params", scenario26Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: "defaults-test",
			},
		},
	}

	// Apply webhook defaults (simulates admission webhook).
	defaulter := webhook.NewCloudberryClusterDefaulter()
	err := defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)

	mockClient := newScenario26MockDBClient()
	// No existing groups in DB — will trigger create.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}

	factory := &scenario26MockDBFactory{client: mockClient}
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify CreateResourceGroup was called.
	createCalls := mockClient.getCreateCalls()
	require.Len(s.T(), createCalls, 1, "expected 1 CreateResourceGroup call")

	opts := createCalls[0]
	assert.Equal(s.T(), "defaults-test", opts.Name)

	// Concurrency=20 (>0) — will be included in SQL CREATE statement.
	assert.Equal(s.T(), int32(20), opts.Concurrency,
		"concurrency=20 should be passed to CreateResourceGroup (included in SQL)")

	// CPUMaxPercent=100 (>0) — will be included in SQL CREATE statement.
	assert.Equal(s.T(), int32(100), opts.CPUMaxPercent,
		"cpuMaxPercent=100 should be passed to CreateResourceGroup (included in SQL)")

	// CPUWeight=100 (>0) — will be included in SQL CREATE statement.
	assert.Equal(s.T(), int32(100), opts.CPUWeight,
		"cpuWeight=100 should be passed to CreateResourceGroup (included in SQL)")

	// MemoryLimit=0 — will NOT be included in SQL CREATE statement (uses DB default).
	assert.Equal(s.T(), int32(0), opts.MemoryLimit,
		"memoryLimit=0 should be passed to CreateResourceGroup (NOT included in SQL, uses DB default)")

	// MinCost=0 — will NOT be included in SQL CREATE statement (uses DB default).
	assert.Equal(s.T(), int32(0), opts.MinCost,
		"minCost=0 should be passed to CreateResourceGroup (NOT included in SQL, uses DB default)")

	// Verify no alter or drop calls.
	assert.Empty(s.T(), mockClient.getAlterCalls(), "should not alter any resource groups")
	assert.Empty(s.T(), mockClient.getDropCalls(), "should not drop any resource groups")
}

// ============================================================================
// Test 26e: Defaults Listed from DB Match Spec - No Changes Needed
// ============================================================================

func (s *Scenario26ResourceGroupDefaultsSuite) TestScenario26e_DefaultsListedFromDB_MatchesSpec() {
	// Arrange: DB returns a resource group with default values.
	// Cluster spec has defaults-only resource group (after webhook defaults).
	// Reconciler should detect no changes and skip create/alter.
	cluster := testutil.NewClusterBuilder("s26e-db-matches", scenario26Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: "defaults-test",
			},
		},
	}

	// Apply webhook defaults.
	defaulter := webhook.NewCloudberryClusterDefaulter()
	err := defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)

	mockClient := newScenario26MockDBClient()
	// DB returns a resource group with values matching the webhook defaults.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{
		{
			Name:          "defaults-test",
			Concurrency:   20,
			CPUMaxPercent: 100,
			CPUWeight:     100,
			MemoryLimit:   0,
			MinCost:       0,
		},
	}
	// Set usage data so we can verify usage queries are made.
	mockClient.usageMap["defaults-test"] = [2]float64{5.0, 10.0}

	factory := &scenario26MockDBFactory{client: mockClient}
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify NO create or alter calls (everything matches).
	createCalls := mockClient.getCreateCalls()
	assert.Empty(s.T(), createCalls, "should not create resource group when DB matches spec")

	alterCalls := mockClient.getAlterCalls()
	assert.Empty(s.T(), alterCalls, "should not alter resource group when DB matches spec")

	dropCalls := mockClient.getDropCalls()
	assert.Empty(s.T(), dropCalls, "should not drop any resource groups")

	// Verify resource group usage metrics were queried.
	usageCalls := mockClient.getUsageCalls()
	require.Len(s.T(), usageCalls, 1, "expected 1 GetResourceGroupUsage call")
	assert.Equal(s.T(), "defaults-test", usageCalls[0],
		"usage should be queried for defaults-test group")

	// Verify WorkloadConfigured condition is True.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}
