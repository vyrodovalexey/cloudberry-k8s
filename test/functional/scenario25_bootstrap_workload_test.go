//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 25: Bootstrap Workload Management with Mock DB
// ============================================================================

const (
	scenario25Namespace = "default"
)

// scenario25MockDBClient implements db.Client for workload bootstrap tests.
// It tracks all resource group operations (create, alter, drop) with thread-safe
// recording so tests can verify the exact calls made by the reconciler.
type scenario25MockDBClient struct {
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
}

func newScenario25MockDBClient() *scenario25MockDBClient {
	m := &scenario25MockDBClient{
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

func (m *scenario25MockDBClient) trackCreateResourceGroup(_ context.Context, opts db.ResourceGroupOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCalls = append(m.createCalls, opts)
	return nil
}

func (m *scenario25MockDBClient) trackAlterResourceGroup(_ context.Context, opts db.ResourceGroupOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alterCalls = append(m.alterCalls, opts)
	return nil
}

func (m *scenario25MockDBClient) trackDropResourceGroup(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropCalls = append(m.dropCalls, name)
	return nil
}

func (m *scenario25MockDBClient) trackListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listResourceGroupsErr != nil {
		return nil, m.listResourceGroupsErr
	}
	return m.listResourceGroups, nil
}

func (m *scenario25MockDBClient) trackGetResourceGroupUsage(_ context.Context, group string) (float64, float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if usage, ok := m.usageMap[group]; ok {
		return usage[0], usage[1], nil
	}
	return 0, 0, nil
}

func (m *scenario25MockDBClient) getCreateCalls() []db.ResourceGroupOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]db.ResourceGroupOptions, len(m.createCalls))
	copy(result, m.createCalls)
	return result
}

func (m *scenario25MockDBClient) getAlterCalls() []db.ResourceGroupOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]db.ResourceGroupOptions, len(m.alterCalls))
	copy(result, m.alterCalls)
	return result
}

func (m *scenario25MockDBClient) getDropCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.dropCalls))
	copy(result, m.dropCalls)
	return result
}

// scenario25MockDBFactory implements db.DBClientFactory for scenario 25 tests.
type scenario25MockDBFactory struct {
	client db.Client
	err    error
}

func (f *scenario25MockDBFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// ============================================================================
// Test Suite
// ============================================================================

// Scenario25BootstrapWorkloadSuite tests the full workload bootstrap flow
// with a mock DB client, verifying resource group CRUD operations, ConfigMap
// creation for workload/idle rules, and fallback behavior.
type Scenario25BootstrapWorkloadSuite struct {
	suite.Suite
	ctx context.Context
}

func TestScenario25_BootstrapWorkload(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario25BootstrapWorkloadSuite))
}

func (s *Scenario25BootstrapWorkloadSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario25BootstrapWorkloadSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// ============================================================================
// Test 25a: Bootstrap Resource Groups - Creates in DB
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25a_BootstrapResourceGroups_CreatesInDB() {
	// Arrange: cluster with analytics + etl resource groups (Scenario 25 spec).
	cluster := testutil.NewClusterBuilder("s25a-rg-create", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   30,
				MinCost:       500,
			},
			{
				Name:          "etl",
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
				MemoryLimit:   20,
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	// No existing resource groups in DB — both should be created.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// Verify both resource groups were created with correct parameters.
	createCalls := mockClient.getCreateCalls()
	require.Len(s.T(), createCalls, 2, "expected 2 CreateResourceGroup calls")

	// Build a map for order-independent assertion.
	createdByName := make(map[string]db.ResourceGroupOptions, len(createCalls))
	for _, call := range createCalls {
		createdByName[call.Name] = call
	}

	analyticsOpts, ok := createdByName["analytics"]
	require.True(s.T(), ok, "analytics resource group should be created")
	assert.Equal(s.T(), int32(10), analyticsOpts.Concurrency)
	assert.Equal(s.T(), int32(50), analyticsOpts.CPUMaxPercent)
	assert.Equal(s.T(), int32(100), analyticsOpts.CPUWeight)
	assert.Equal(s.T(), int32(30), analyticsOpts.MemoryLimit)
	assert.Equal(s.T(), int32(500), analyticsOpts.MinCost)

	etlOpts, ok := createdByName["etl"]
	require.True(s.T(), ok, "etl resource group should be created")
	assert.Equal(s.T(), int32(5), etlOpts.Concurrency)
	assert.Equal(s.T(), int32(30), etlOpts.CPUMaxPercent)
	assert.Equal(s.T(), int32(50), etlOpts.CPUWeight)
	assert.Equal(s.T(), int32(20), etlOpts.MemoryLimit)

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
// Test 25b: Bootstrap Workload Rules - Creates ConfigMap
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25b_BootstrapWorkloadRules_CreatesConfigMap() {
	// Arrange: cluster with workload rules (cancel-long-queries, move-heavy-queries).
	cluster := testutil.NewClusterBuilder("s25b-wl-rules", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics"},
			{Name: "etl"},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long-queries",
				Enabled:       true,
				ResourceGroup: "analytics",
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "3600",
				Priority:      1,
			},
			{
				Name:          "move-heavy-queries",
				Enabled:       true,
				QueryTag:      "heavy",
				Action:        "move",
				MoveTarget:    "etl",
				ThresholdType: "spill_size",
				Threshold:     "1073741824",
				Priority:      2,
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// Verify ConfigMap was created with rules.json.
	cmName := cluster.Name + "-workload-rules"
	cm, err := env.GetConfigMap(s.ctx, cmName, cluster.Namespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	// Verify labels.
	assert.Equal(s.T(), "cloudberry-operator", cm.Labels["app.kubernetes.io/managed-by"])
	assert.Equal(s.T(), "workload-rules", cm.Labels["app.kubernetes.io/component"])
	assert.Equal(s.T(), cluster.Name, cm.Labels["app.kubernetes.io/instance"])

	// Verify rules.json content.
	rulesJSON, ok := cm.Data["rules.json"]
	require.True(s.T(), ok, "ConfigMap should contain rules.json")

	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(rulesJSON), &rules)
	require.NoError(s.T(), err, "rules.json should be valid JSON")
	require.Len(s.T(), rules, 2, "should have 2 workload rules")

	// Verify rule details.
	rulesByName := make(map[string]cbv1alpha1.WorkloadRule, len(rules))
	for _, r := range rules {
		rulesByName[r.Name] = r
	}

	cancelRule, ok := rulesByName["cancel-long-queries"]
	require.True(s.T(), ok, "cancel-long-queries rule should exist")
	assert.Equal(s.T(), "cancel", cancelRule.Action)
	assert.Equal(s.T(), "running_time", cancelRule.ThresholdType)
	assert.Equal(s.T(), "3600", cancelRule.Threshold)
	assert.Equal(s.T(), "analytics", cancelRule.ResourceGroup)
	assert.Equal(s.T(), int32(1), cancelRule.Priority)

	moveRule, ok := rulesByName["move-heavy-queries"]
	require.True(s.T(), ok, "move-heavy-queries rule should exist")
	assert.Equal(s.T(), "move", moveRule.Action)
	assert.Equal(s.T(), "spill_size", moveRule.ThresholdType)
	assert.Equal(s.T(), "1073741824", moveRule.Threshold)
	assert.Equal(s.T(), "etl", moveRule.MoveTarget)
	assert.Equal(s.T(), int32(2), moveRule.Priority)
}

// ============================================================================
// Test 25c: Bootstrap Idle Rules - Creates ConfigMap
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25c_BootstrapIdleRules_CreatesConfigMap() {
	// Arrange: cluster with idle session rules (terminate-idle-analytics).
	cluster := testutil.NewClusterBuilder("s25c-idle-rules", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics"},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{
				Name:                 "terminate-idle-analytics",
				Enabled:              true,
				ResourceGroup:        "analytics",
				IdleTimeout:          "30m",
				ExcludeInTransaction: true,
				TerminateMessage:     "Session terminated due to inactivity",
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// Verify ConfigMap was created with idle-rules.json.
	cmName := cluster.Name + "-workload-rules"
	cm, err := env.GetConfigMap(s.ctx, cmName, cluster.Namespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	// Verify idle-rules.json content.
	idleRulesJSON, ok := cm.Data["idle-rules.json"]
	require.True(s.T(), ok, "ConfigMap should contain idle-rules.json")

	var idleRules []cbv1alpha1.IdleSessionRule
	err = json.Unmarshal([]byte(idleRulesJSON), &idleRules)
	require.NoError(s.T(), err, "idle-rules.json should be valid JSON")
	require.Len(s.T(), idleRules, 1, "should have 1 idle session rule")

	rule := idleRules[0]
	assert.Equal(s.T(), "terminate-idle-analytics", rule.Name)
	assert.True(s.T(), rule.Enabled)
	assert.Equal(s.T(), "analytics", rule.ResourceGroup)
	assert.Equal(s.T(), "30m", rule.IdleTimeout)
	assert.True(s.T(), rule.ExcludeInTransaction)
	assert.Equal(s.T(), "Session terminated due to inactivity", rule.TerminateMessage)
}

// ============================================================================
// Test 25d: Full Bootstrap - All Components
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25d_FullBootstrap_AllComponents() {
	// Arrange: cluster with the FULL Scenario 25 spec.
	cluster := testutil.NewClusterBuilder("s25d-full-bootstrap", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   30,
				MinCost:       500,
			},
			{
				Name:          "etl",
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
				MemoryLimit:   20,
			},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long-queries",
				Enabled:       true,
				ResourceGroup: "analytics",
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "3600",
				Priority:      1,
			},
			{
				Name:          "move-heavy-queries",
				Enabled:       true,
				QueryTag:      "heavy",
				Action:        "move",
				MoveTarget:    "etl",
				ThresholdType: "spill_size",
				Threshold:     "1073741824",
				Priority:      2,
			},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{
				Name:                 "terminate-idle-analytics",
				Enabled:              true,
				ResourceGroup:        "analytics",
				IdleTimeout:          "30m",
				ExcludeInTransaction: true,
				TerminateMessage:     "Session terminated due to inactivity",
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	mockClient.listResourceGroups = []db.ResourceGroupInfo{}
	mockClient.usageMap["analytics"] = [2]float64{25.0, 50.0}
	mockClient.usageMap["etl"] = [2]float64{10.0, 15.0}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// 1. Verify all resource groups created in DB.
	createCalls := mockClient.getCreateCalls()
	require.Len(s.T(), createCalls, 2, "expected 2 CreateResourceGroup calls")

	createdNames := make(map[string]bool, len(createCalls))
	for _, call := range createCalls {
		createdNames[call.Name] = true
	}
	assert.True(s.T(), createdNames["analytics"], "analytics group should be created")
	assert.True(s.T(), createdNames["etl"], "etl group should be created")

	// 2. Verify ConfigMap created with both rules.json and idle-rules.json.
	cmName := cluster.Name + "-workload-rules"
	cm, err := env.GetConfigMap(s.ctx, cmName, cluster.Namespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	_, hasRules := cm.Data["rules.json"]
	assert.True(s.T(), hasRules, "ConfigMap should contain rules.json")

	_, hasIdleRules := cm.Data["idle-rules.json"]
	assert.True(s.T(), hasIdleRules, "ConfigMap should contain idle-rules.json")

	// Verify rules.json has 2 rules.
	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(cm.Data["rules.json"]), &rules)
	require.NoError(s.T(), err)
	assert.Len(s.T(), rules, 2)

	// Verify idle-rules.json has 1 rule.
	var idleRules []cbv1alpha1.IdleSessionRule
	err = json.Unmarshal([]byte(cm.Data["idle-rules.json"]), &idleRules)
	require.NoError(s.T(), err)
	assert.Len(s.T(), idleRules, 1)

	// 3. Verify WorkloadConfigured condition.
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

	// 4. Verify events were emitted by checking the fake recorder channel.
	// The record.FakeRecorder buffers events in a channel; the WorkloadReconciled
	// event should have been emitted during reconciliation.
	// We already verified the condition above, which confirms the reconciler
	// completed the workload reconciliation path successfully.
}

// ============================================================================
// Test 25e: Resource Group Update - Alters in DB
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25e_ResourceGroupUpdate_AltersInDB() {
	// Arrange: cluster with analytics resource group.
	cluster := testutil.NewClusterBuilder("s25e-rg-update", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   30,
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	// Simulate that the group already exists in DB with DIFFERENT parameters.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{
		{
			Name:          "analytics",
			Concurrency:   5,  // was 5, now desired 10
			CPUMaxPercent: 30, // was 30, now desired 50
			CPUWeight:     50, // was 50, now desired 100
			MemoryLimit:   20, // was 20, now desired 30
		},
	}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// Verify AlterResourceGroup was called (not Create, since group already exists).
	createCalls := mockClient.getCreateCalls()
	assert.Empty(s.T(), createCalls, "should not create existing resource group")

	alterCalls := mockClient.getAlterCalls()
	require.Len(s.T(), alterCalls, 1, "expected 1 AlterResourceGroup call")

	alterOpts := alterCalls[0]
	assert.Equal(s.T(), "analytics", alterOpts.Name)
	assert.Equal(s.T(), int32(10), alterOpts.Concurrency)
	assert.Equal(s.T(), int32(50), alterOpts.CPUMaxPercent)
	assert.Equal(s.T(), int32(100), alterOpts.CPUWeight)
	assert.Equal(s.T(), int32(30), alterOpts.MemoryLimit)
}

// ============================================================================
// Test 25f: Resource Group Removal - Drops from DB
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25f_ResourceGroupRemoval_DropsFromDB() {
	// Arrange: cluster with only analytics group (etl was removed from spec).
	cluster := testutil.NewClusterBuilder("s25f-rg-remove", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				MemoryLimit:   30,
			},
		},
	}

	mockClient := newScenario25MockDBClient()
	// Simulate that both groups exist in DB, but only analytics is desired.
	mockClient.listResourceGroups = []db.ResourceGroupInfo{
		{
			Name:          "analytics",
			Concurrency:   10,
			CPUMaxPercent: 50,
			MemoryLimit:   30,
		},
		{
			Name:          "etl",
			Concurrency:   5,
			CPUMaxPercent: 30,
			MemoryLimit:   20,
		},
	}

	factory := &scenario25MockDBFactory{client: mockClient}
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

	// Verify DropResourceGroup was called for the removed group.
	dropCalls := mockClient.getDropCalls()
	require.Len(s.T(), dropCalls, 1, "expected 1 DropResourceGroup call")
	assert.Equal(s.T(), "etl", dropCalls[0], "etl group should be dropped")

	// Verify analytics was NOT altered (parameters match).
	alterCalls := mockClient.getAlterCalls()
	assert.Empty(s.T(), alterCalls, "analytics should not be altered when params match")

	// Verify analytics was NOT created (already exists).
	createCalls := mockClient.getCreateCalls()
	assert.Empty(s.T(), createCalls, "analytics should not be created when it already exists")
}

// ============================================================================
// Test 25g: DB Unavailable - Falls Back to Condition Only
// ============================================================================

func (s *Scenario25BootstrapWorkloadSuite) TestScenario25g_DBUnavailable_FallsBackToConditionOnly() {
	// Arrange: cluster with workload spec, but DB factory returns an error.
	cluster := testutil.NewClusterBuilder("s25g-db-unavail", scenario25Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				MemoryLimit:   30,
			},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long-queries",
				Enabled:       true,
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "3600",
			},
		},
	}

	// DB factory returns an error — simulates DB unavailability.
	factory := &scenario25MockDBFactory{
		err: fmt.Errorf("connection refused: database is not reachable"),
	}
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert: reconciliation succeeds (no error returned).
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify WorkloadConfigured condition is set with DBUnavailable reason.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "False", string(c.Status),
				"WorkloadConfigured should be False when DB is unavailable")
			assert.Equal(s.T(), "DBUnavailable", c.Reason,
				"reason should indicate DB unavailability")
			assert.Contains(s.T(), c.Message, "connection refused",
				"message should contain the error details")
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}
