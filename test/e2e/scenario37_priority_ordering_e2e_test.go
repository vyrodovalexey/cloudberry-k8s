//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 37: Rule Priority Ordering with Real Cloudberry Cluster
// ============================================================================
//
// Validates that the operator sorts workload rules by priority (lowest number
// first) before storing them in the ConfigMap. Rules with the same priority
// preserve their CRD spec order (stable sort).
//
// Specification reference: Section 10 "Rule Ordering" of
// specifications/09-workload-management-spec.md:
//   "Rules are evaluated in priority order (lowest number first). Rules with
//    the same priority are evaluated in the order they appear in the CRD spec."
//
// Sub-tests:
//   - 37a: Different priorities — rules stored sorted by priority (lowest first)
//   - 37b: Same priority — CRD spec order preserved as tiebreaker
// ============================================================================

// Scenario37PriorityOrderingE2ESuite tests rule priority ordering against a
// real Cloudberry cluster.
type Scenario37PriorityOrderingE2ESuite struct {
	E2ESuite

	// dbClient is the real database client connected to the Cloudberry coordinator.
	dbClient db.Client
	// portForwardCmd holds the kubectl port-forward process, if started.
	portForwardCmd *exec.Cmd
	// localPort is the local port used for the port-forward.
	localPort int
	// testSuffix is a unique suffix for resource group names to avoid conflicts.
	testSuffix string
}

func TestE2E_Scenario37(t *testing.T) {
	suite.Run(t, new(Scenario37PriorityOrderingE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario37PriorityOrderingE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 37 E2E suite setup", "testSuffix", s.testSuffix)

	// Determine connection parameters from environment or defaults.
	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	// If no password is provided, try to read it from the Kubernetes secret.
	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		// Auto port-forward: find a free local port and start kubectl port-forward.
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		err = s.portForwardCmd.Start()
		if err != nil {
			s.T().Skipf("skipping scenario 37 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward37()
			s.T().Skipf("skipping scenario 37 E2E: port-forward did not become ready within timeout")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	// Connect to the real Cloudberry cluster.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	cfg := db.Config{
		Host:     host,
		Port:     int32(port),
		Database: database,
		Username: user,
		Password: password,
		SSLMode:  "disable",
		MaxConns: 5,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
	}

	dbClient, err := db.NewClient(ctx, cfg, s.logger)
	if err != nil {
		s.cleanupPortForward37()
		s.T().Skipf("skipping scenario 37 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward37()
		s.T().Skipf("skipping scenario 37 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 37",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario37PriorityOrderingE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward37()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward37 terminates the kubectl port-forward process if running.
func (s *Scenario37PriorityOrderingE2ESuite) cleanupPortForward37() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		if err := s.portForwardCmd.Process.Kill(); err != nil {
			s.logger.Warn("failed to kill port-forward process", "error", err)
		}
		// Wait to reap the process and avoid zombies.
		_ = s.portForwardCmd.Wait()
		s.logger.Info("port-forward process terminated")
		s.portForwardCmd = nil
	}
}

// uniqueGroupName returns a unique resource group name for test isolation.
func (s *Scenario37PriorityOrderingE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s37e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario37PriorityOrderingE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// ============================================================================
// Test 37a: Different Priorities — Lowest Number First
// ============================================================================
//
// Steps:
//  1. Create resource groups in real DB (analytics + etl for move target)
//  2. Build CloudberryCluster with 3 rules in NON-priority order (p3, p1, p2)
//  3. Run reconciliation
//  4. Read the ConfigMap and parse rules.json
//  5. Verify: rules are sorted by priority (p1, p2, p3)
//  6. Verify: each rule has correct fields
//  7. Verify: evaluation order is log(1) → move(2) → cancel(3)
//  8. Cleanup

func (s *Scenario37PriorityOrderingE2ESuite) TestScenario37a_DifferentPriorities_LowestFirst() {
	s.logger.Info("starting test 37a: different priorities — lowest number first")

	analyticsName := s.uniqueGroupName("analytics_a")
	etlName := s.uniqueGroupName("etl_a")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- Step 1: Create resource groups in real DB ----
	s.logger.Info("step 1: creating resource groups in real DB",
		"analytics", analyticsName, "etl", etlName)

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	err = s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          etlName,
		Concurrency:   5,
		CPUMaxPercent: 30,
		CPUWeight:     50,
	})
	require.NoError(s.T(), err, "should create etl resource group")

	// ---- Step 2: Build CloudberryCluster with rules in NON-priority order ----
	s.logger.Info("step 2: building CloudberryCluster with rules in non-priority order (p3, p1, p2)")

	clusterName := "s37a-e2e-priority"
	clusterNamespace := "default"

	cluster := testutil.NewClusterBuilder(clusterName, clusterNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   -1,
				MinCost:       -1,
			},
			{
				Name:          etlName,
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
				MemoryLimit:   -1,
				MinCost:       -1,
			},
		},
		// Rules intentionally in non-priority order: p3, p1, p2.
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "rule_p3",
				Enabled:       true,
				ResourceGroup: analyticsName,
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "30",
				Priority:      3,
			},
			{
				Name:          "rule_p1",
				Enabled:       true,
				ResourceGroup: analyticsName,
				Action:        "log",
				ThresholdType: "running_time",
				Threshold:     "10",
				Priority:      1,
			},
			{
				Name:          "rule_p2",
				Enabled:       true,
				ResourceGroup: analyticsName,
				Action:        "move",
				MoveTarget:    etlName,
				ThresholdType: "running_time",
				Threshold:     "20",
				Priority:      2,
			},
		},
	}

	// ---- Step 3: Run reconciliation ----
	s.logger.Info("step 3: running reconciliation")

	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// ---- Step 4: Read ConfigMap and parse rules.json ----
	s.logger.Info("step 4: reading ConfigMap and parsing rules.json")

	cmName := clusterName + "-workload-rules"
	cm, err := env.GetConfigMap(ctx, cmName, clusterNamespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	rulesJSON, hasRules := cm.Data["rules.json"]
	require.True(s.T(), hasRules, "ConfigMap should contain rules.json")

	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(rulesJSON), &rules)
	require.NoError(s.T(), err, "rules.json should be valid JSON")
	require.Len(s.T(), rules, 3, "should have 3 workload rules")

	// ---- Step 5: Verify rules are sorted by priority ----
	s.logger.Info("step 5: verifying rules are sorted by priority (lowest first)")

	// Index 0: rule_p1 (priority=1, action=log)
	assert.Equal(s.T(), "rule_p1", rules[0].Name,
		"first rule should be rule_p1 (priority 1)")
	assert.Equal(s.T(), int32(1), rules[0].Priority,
		"first rule priority should be 1")
	assert.Equal(s.T(), "log", rules[0].Action,
		"first rule action should be log")

	// Index 1: rule_p2 (priority=2, action=move)
	assert.Equal(s.T(), "rule_p2", rules[1].Name,
		"second rule should be rule_p2 (priority 2)")
	assert.Equal(s.T(), int32(2), rules[1].Priority,
		"second rule priority should be 2")
	assert.Equal(s.T(), "move", rules[1].Action,
		"second rule action should be move")

	// Index 2: rule_p3 (priority=3, action=cancel)
	assert.Equal(s.T(), "rule_p3", rules[2].Name,
		"third rule should be rule_p3 (priority 3)")
	assert.Equal(s.T(), int32(3), rules[2].Priority,
		"third rule priority should be 3")
	assert.Equal(s.T(), "cancel", rules[2].Action,
		"third rule action should be cancel")

	// ---- Step 6: Verify each rule has correct fields ----
	s.logger.Info("step 6: verifying each rule has correct fields")

	// rule_p1 (log)
	assert.True(s.T(), rules[0].Enabled, "rule_p1 should be enabled")
	assert.Equal(s.T(), analyticsName, rules[0].ResourceGroup,
		"rule_p1 resource group should be analytics")
	assert.Equal(s.T(), "running_time", rules[0].ThresholdType,
		"rule_p1 threshold type should be running_time")
	assert.Equal(s.T(), "10", rules[0].Threshold,
		"rule_p1 threshold should be 10")

	// rule_p2 (move)
	assert.True(s.T(), rules[1].Enabled, "rule_p2 should be enabled")
	assert.Equal(s.T(), analyticsName, rules[1].ResourceGroup,
		"rule_p2 resource group should be analytics")
	assert.Equal(s.T(), etlName, rules[1].MoveTarget,
		"rule_p2 move target should be etl")
	assert.Equal(s.T(), "running_time", rules[1].ThresholdType,
		"rule_p2 threshold type should be running_time")
	assert.Equal(s.T(), "20", rules[1].Threshold,
		"rule_p2 threshold should be 20")

	// rule_p3 (cancel)
	assert.True(s.T(), rules[2].Enabled, "rule_p3 should be enabled")
	assert.Equal(s.T(), analyticsName, rules[2].ResourceGroup,
		"rule_p3 resource group should be analytics")
	assert.Equal(s.T(), "running_time", rules[2].ThresholdType,
		"rule_p3 threshold type should be running_time")
	assert.Equal(s.T(), "30", rules[2].Threshold,
		"rule_p3 threshold should be 30")

	// ---- Step 7: Verify evaluation order ----
	s.logger.Info("step 7: verifying evaluation order: log(1) -> move(2) -> cancel(3)")

	expectedOrder := []struct {
		name     string
		action   string
		priority int32
	}{
		{name: "rule_p1", action: "log", priority: 1},
		{name: "rule_p2", action: "move", priority: 2},
		{name: "rule_p3", action: "cancel", priority: 3},
	}

	for i, expected := range expectedOrder {
		assert.Equal(s.T(), expected.name, rules[i].Name,
			"evaluation order index %d: name mismatch", i)
		assert.Equal(s.T(), expected.action, rules[i].Action,
			"evaluation order index %d: action mismatch", i)
		assert.Equal(s.T(), expected.priority, rules[i].Priority,
			"evaluation order index %d: priority mismatch", i)
	}

	s.logger.Info("test 37a completed: rules sorted by priority verified",
		"order", "log(1) -> move(2) -> cancel(3)",
		"rulesCount", len(rules))
}

// ============================================================================
// Test 37b: Same Priority — CRD Spec Order Preserved
// ============================================================================
//
// Steps:
//  1. Create resource group in real DB
//  2. Build CloudberryCluster with 2 rules that have IDENTICAL priority
//  3. Run reconciliation
//  4. Read the ConfigMap and parse rules.json
//  5. Verify: both rules have priority=1
//  6. Verify: first_in_spec appears at index 0, second_in_spec at index 1
//  7. Verify: CRD spec order is preserved as tiebreaker
//  8. Cleanup

func (s *Scenario37PriorityOrderingE2ESuite) TestScenario37b_SamePriority_CRDSpecOrder() {
	s.logger.Info("starting test 37b: same priority — CRD spec order preserved")

	analyticsName := s.uniqueGroupName("analytics_b")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- Step 1: Create resource group in real DB ----
	s.logger.Info("step 1: creating resource group in real DB", "name", analyticsName)

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	// ---- Step 2: Build CloudberryCluster with 2 rules at same priority ----
	s.logger.Info("step 2: building CloudberryCluster with 2 rules at identical priority")

	clusterName := "s37b-e2e-same-priority"
	clusterNamespace := "default"

	cluster := testutil.NewClusterBuilder(clusterName, clusterNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   -1,
				MinCost:       -1,
			},
		},
		// Two rules with identical priority — CRD spec order must be preserved.
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "first_in_spec",
				Enabled:       true,
				ResourceGroup: analyticsName,
				Action:        "log",
				ThresholdType: "running_time",
				Threshold:     "5",
				Priority:      1,
			},
			{
				Name:          "second_in_spec",
				Enabled:       true,
				ResourceGroup: analyticsName,
				Action:        "log",
				ThresholdType: "running_time",
				Threshold:     "5",
				Priority:      1,
			},
		},
	}

	// ---- Step 3: Run reconciliation ----
	s.logger.Info("step 3: running reconciliation")

	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// ---- Step 4: Read ConfigMap and parse rules.json ----
	s.logger.Info("step 4: reading ConfigMap and parsing rules.json")

	cmName := clusterName + "-workload-rules"
	cm, err := env.GetConfigMap(ctx, cmName, clusterNamespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	rulesJSON, hasRules := cm.Data["rules.json"]
	require.True(s.T(), hasRules, "ConfigMap should contain rules.json")

	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(rulesJSON), &rules)
	require.NoError(s.T(), err, "rules.json should be valid JSON")
	require.Len(s.T(), rules, 2, "should have 2 workload rules")

	// ---- Step 5: Verify both rules have priority=1 ----
	s.logger.Info("step 5: verifying both rules have priority=1")

	assert.Equal(s.T(), int32(1), rules[0].Priority,
		"first rule priority should be 1")
	assert.Equal(s.T(), int32(1), rules[1].Priority,
		"second rule priority should be 1")

	// ---- Step 6: Verify CRD spec order is preserved ----
	s.logger.Info("step 6: verifying CRD spec order is preserved (first_in_spec before second_in_spec)")

	assert.Equal(s.T(), "first_in_spec", rules[0].Name,
		"first_in_spec should appear at index 0 (CRD spec order preserved)")
	assert.Equal(s.T(), "second_in_spec", rules[1].Name,
		"second_in_spec should appear at index 1 (CRD spec order preserved)")

	// ---- Step 7: Verify tiebreaker behavior ----
	s.logger.Info("step 7: verifying stable sort tiebreaker — CRD spec order is the tiebreaker")

	// Both rules have the same priority, action, threshold type, and threshold.
	// The only difference is the name. The stable sort must preserve the original
	// CRD spec order as the tiebreaker.
	assert.Equal(s.T(), "log", rules[0].Action,
		"first rule action should be log")
	assert.Equal(s.T(), "log", rules[1].Action,
		"second rule action should be log")
	assert.Equal(s.T(), "running_time", rules[0].ThresholdType,
		"first rule threshold type should be running_time")
	assert.Equal(s.T(), "running_time", rules[1].ThresholdType,
		"second rule threshold type should be running_time")
	assert.Equal(s.T(), "5", rules[0].Threshold,
		"first rule threshold should be 5")
	assert.Equal(s.T(), "5", rules[1].Threshold,
		"second rule threshold should be 5")
	assert.Equal(s.T(), analyticsName, rules[0].ResourceGroup,
		"first rule resource group should be analytics")
	assert.Equal(s.T(), analyticsName, rules[1].ResourceGroup,
		"second rule resource group should be analytics")

	s.logger.Info("test 37b completed: same priority CRD spec order preserved",
		"rule0", rules[0].Name, "rule1", rules[1].Name,
		"bothPriority", rules[0].Priority)
}
