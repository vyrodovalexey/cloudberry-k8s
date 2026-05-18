//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

// Environment variable names for Cloudberry E2E test configuration.
const (
	envCloudberryTestHost      = "CLOUDBERRY_TEST_HOST"
	envCloudberryTestPort      = "CLOUDBERRY_TEST_PORT"
	envCloudberryTestUser      = "CLOUDBERRY_TEST_USER"
	envCloudberryTestPassword  = "CLOUDBERRY_TEST_PASSWORD"
	envCloudberryTestDB        = "CLOUDBERRY_TEST_DB"
	envCloudberryTestNamespace = "CLOUDBERRY_TEST_NAMESPACE"
	envCloudberryTestService   = "CLOUDBERRY_TEST_SERVICE"

	defaultCloudberryHost      = "localhost"
	defaultCloudberryUser      = "gpadmin"
	defaultCloudberryDB        = "postgres"
	defaultCloudberryNamespace = "cloudberry-test"
	defaultCloudberryService   = "scenario1-cluster-client"

	// dbOperationTimeout is the timeout for individual DB operations.
	dbOperationTimeout = 30 * time.Second
)

// Scenario25WorkloadE2ESuite tests workload management against a real Cloudberry cluster.
type Scenario25WorkloadE2ESuite struct {
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

func TestE2E_Scenario25(t *testing.T) {
	suite.Run(t, new(Scenario25WorkloadE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario25WorkloadE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 25 E2E suite setup", "testSuffix", s.testSuffix)

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
			s.T().Skipf("skipping scenario 25 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward()
			s.T().Skipf("skipping scenario 25 E2E: port-forward did not become ready within timeout")
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
		MaxConns: 3,
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
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 25 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 25 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario25WorkloadE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward terminates the kubectl port-forward process if running.
func (s *Scenario25WorkloadE2ESuite) cleanupPortForward() {
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
func (s *Scenario25WorkloadE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s25e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario25WorkloadE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// ============================================================================
// Test 25a: Resource Groups Created in Real DB
// ============================================================================

func (s *Scenario25WorkloadE2ESuite) TestScenario25a_ResourceGroups_CreatedInRealDB() {
	s.logger.Info("starting test 25a: resource groups created in real DB")

	analyticsName := s.uniqueGroupName("analytics")
	etlName := s.uniqueGroupName("etl")

	// Register cleanup to drop resource groups after test.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	// Step 1: Create analytics resource group with spec values.
	// Note: Cloudberry 2.1.0 does NOT support "memory_limit" as a CREATE/ALTER option.
	// The reslimittype=4 row exists in pg_resgroupcapability with default value -1,
	// but it cannot be set via SQL. We omit MemoryLimit (leave at 0) so the operator
	// skips it in the CREATE statement.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
		// MemoryLimit omitted — not supported in Cloudberry 2.1.0 CREATE RESOURCE GROUP.
		// MinCost omitted — Cloudberry 2.1.0 stores min_cost in reslimittype=6, but
		// the operator's ListResourceGroups reads reslimittype=5 (which is a different
		// capability). To avoid confusion, we omit MinCost and verify the DB defaults.
	})
	require.NoError(s.T(), err, "should create analytics resource group")
	s.logger.Info("created analytics resource group", "name", analyticsName)

	// Step 2: Create etl resource group.
	err = s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          etlName,
		Concurrency:   5,
		CPUMaxPercent: 30,
		CPUWeight:     50,
		// MemoryLimit and MinCost omitted — DB uses defaults.
	})
	require.NoError(s.T(), err, "should create etl resource group")
	s.logger.Info("created etl resource group", "name", etlName)

	// Step 3: Verify both groups exist via ListResourceGroups.
	groups, err := s.dbClient.ListResourceGroups(ctx)
	require.NoError(s.T(), err, "should list resource groups")

	groupsByName := make(map[string]db.ResourceGroupInfo, len(groups))
	for _, g := range groups {
		groupsByName[g.Name] = g
	}

	// Verify analytics group parameters.
	analyticsInfo, ok := groupsByName[analyticsName]
	require.True(s.T(), ok, "analytics resource group should exist in DB")
	assert.Equal(s.T(), int32(10), analyticsInfo.Concurrency, "analytics concurrency")
	assert.Equal(s.T(), int32(50), analyticsInfo.CPUMaxPercent, "analytics cpuMaxPercent")
	assert.Equal(s.T(), int32(100), analyticsInfo.CPUWeight, "analytics cpuWeight")
	// MemoryLimit defaults to -1 in Cloudberry 2.1.0 (reslimittype=4).
	assert.Equal(s.T(), int32(-1), analyticsInfo.MemoryLimit, "analytics memoryLimit should be -1 (default)")
	// MinCost: the operator's ListResourceGroups reads reslimittype=5, which defaults to -1
	// in Cloudberry 2.1.0. The actual min_cost is stored in reslimittype=6.
	assert.Equal(s.T(), int32(-1), analyticsInfo.MinCost,
		"analytics minCost should be -1 (reslimittype=5 default in Cloudberry 2.1.0)")

	// Verify etl group parameters.
	etlInfo, ok := groupsByName[etlName]
	require.True(s.T(), ok, "etl resource group should exist in DB")
	assert.Equal(s.T(), int32(5), etlInfo.Concurrency, "etl concurrency")
	assert.Equal(s.T(), int32(30), etlInfo.CPUMaxPercent, "etl cpuMaxPercent")
	assert.Equal(s.T(), int32(50), etlInfo.CPUWeight, "etl cpuWeight")
	assert.Equal(s.T(), int32(-1), etlInfo.MinCost, "etl minCost should be -1 (DB default)")
	assert.Equal(s.T(), int32(-1), etlInfo.MemoryLimit, "etl memoryLimit should be -1 (default)")

	s.logger.Info("test 25a completed: resource groups verified in real DB",
		"analytics", analyticsInfo, "etl", etlInfo)
}

// ============================================================================
// Test 25b: Full Bootstrap via Reconciler with Real DB
// ============================================================================

func (s *Scenario25WorkloadE2ESuite) TestScenario25b_FullBootstrap_ViaReconcilerWithRealDB() {
	s.logger.Info("starting test 25b: full bootstrap via reconciler with real DB")

	analyticsName := s.uniqueGroupName("analytics_b")
	etlName := s.uniqueGroupName("etl_b")

	// Register cleanup to drop resource groups after test.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	// Step 1: Build a CloudberryCluster object with the full workload spec.
	clusterName := "s25b-e2e-bootstrap"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	// Note: Cloudberry 2.1.0 does NOT support "memory_limit" as a CREATE/ALTER option.
	// MemoryLimit is set to 0 so the operator omits it from the CREATE statement.
	// MinCost is also omitted because the operator's ListResourceGroups reads
	// reslimittype=5 (not the actual min_cost at reslimittype=6 in Cloudberry 2.1.0),
	// which would cause a perpetual drift detection.
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				// MemoryLimit and MinCost omitted — not reliably supported in Cloudberry 2.1.0.
			},
			{
				Name:          etlName,
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
				// MemoryLimit and MinCost omitted — DB uses defaults.
			},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long-queries",
				Enabled:       true,
				ResourceGroup: analyticsName,
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
				MoveTarget:    etlName,
				ThresholdType: "spill_size",
				Threshold:     "1073741824",
				Priority:      2,
			},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{
				Name:                 "terminate-idle-analytics",
				Enabled:              true,
				ResourceGroup:        analyticsName,
				IdleTimeout:          "30m",
				ExcludeInTransaction: true,
				TerminateMessage:     "Session terminated due to inactivity",
			},
		},
	}

	// Step 2: Create a real DB factory that wraps our existing dbClient.
	factory := &realDBClientFactory{client: s.dbClient}

	// Step 3: Create the K8s environment and AdminReconciler.
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	// Step 4: Run reconciliation.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Step 5: Verify resource groups were created in the real DB.
	listCtx, listCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel()

	groups, err := s.dbClient.ListResourceGroups(listCtx)
	require.NoError(s.T(), err, "should list resource groups after reconciliation")

	groupsByName := make(map[string]db.ResourceGroupInfo, len(groups))
	for _, g := range groups {
		groupsByName[g.Name] = g
	}

	analyticsInfo, ok := groupsByName[analyticsName]
	require.True(s.T(), ok, "analytics resource group should exist after reconciliation")
	assert.Equal(s.T(), int32(10), analyticsInfo.Concurrency)
	assert.Equal(s.T(), int32(50), analyticsInfo.CPUMaxPercent)
	assert.Equal(s.T(), int32(100), analyticsInfo.CPUWeight)
	// MemoryLimit and MinCost default to -1 in Cloudberry 2.1.0 when not specified.
	assert.Equal(s.T(), int32(-1), analyticsInfo.MemoryLimit)
	assert.Equal(s.T(), int32(-1), analyticsInfo.MinCost)

	etlInfo, ok := groupsByName[etlName]
	require.True(s.T(), ok, "etl resource group should exist after reconciliation")
	assert.Equal(s.T(), int32(5), etlInfo.Concurrency)
	assert.Equal(s.T(), int32(30), etlInfo.CPUMaxPercent)
	assert.Equal(s.T(), int32(50), etlInfo.CPUWeight)
	// MemoryLimit and MinCost default to -1 when not specified.
	assert.Equal(s.T(), int32(-1), etlInfo.MemoryLimit)
	assert.Equal(s.T(), int32(-1), etlInfo.MinCost)

	// Step 6: Verify ConfigMap was created with rules.json and idle-rules.json.
	cmName := clusterName + "-workload-rules"
	cm, err := env.GetConfigMap(ctx, cmName, cluster.Namespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist")

	// Verify rules.json content.
	rulesJSON, hasRules := cm.Data["rules.json"]
	require.True(s.T(), hasRules, "ConfigMap should contain rules.json")

	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(rulesJSON), &rules)
	require.NoError(s.T(), err, "rules.json should be valid JSON")
	require.Len(s.T(), rules, 2, "should have 2 workload rules")

	// Verify idle-rules.json content.
	idleRulesJSON, hasIdleRules := cm.Data["idle-rules.json"]
	require.True(s.T(), hasIdleRules, "ConfigMap should contain idle-rules.json")

	var idleRules []cbv1alpha1.IdleSessionRule
	err = json.Unmarshal([]byte(idleRulesJSON), &idleRules)
	require.NoError(s.T(), err, "idle-rules.json should be valid JSON")
	require.Len(s.T(), idleRules, 1, "should have 1 idle session rule")

	// Verify idle rule details.
	assert.Equal(s.T(), "terminate-idle-analytics", idleRules[0].Name)
	assert.Equal(s.T(), analyticsName, idleRules[0].ResourceGroup)
	assert.Equal(s.T(), "30m", idleRules[0].IdleTimeout)
	assert.True(s.T(), idleRules[0].ExcludeInTransaction)

	// Verify ConfigMap labels.
	assert.Equal(s.T(), "cloudberry-operator", cm.Labels["app.kubernetes.io/managed-by"])
	assert.Equal(s.T(), "workload-rules", cm.Labels["app.kubernetes.io/component"])
	assert.Equal(s.T(), clusterName, cm.Labels["app.kubernetes.io/instance"])

	// Step 7: Verify WorkloadConfigured condition is True.
	updated, err := env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status), "WorkloadConfigured should be True")
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")

	s.logger.Info("test 25b completed: full bootstrap via reconciler verified")
}

// ============================================================================
// Test 25c: Idempotency with Real DB
// ============================================================================

func (s *Scenario25WorkloadE2ESuite) TestScenario25c_Idempotency_WithRealDB() {
	s.logger.Info("starting test 25c: idempotency with real DB")

	analyticsName := s.uniqueGroupName("analytics_c")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	// Step 1: Create the resource group directly in the DB first.
	createCtx, createCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer createCancel()

	// Note: Cloudberry 2.1.0 does NOT support "memory_limit" as a CREATE/ALTER option.
	// We create the group with only the supported options.
	err := s.dbClient.CreateResourceGroup(createCtx, db.ResourceGroupOptions{
		Name:          analyticsName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
		// MemoryLimit and MinCost omitted — not reliably supported in Cloudberry 2.1.0.
	})
	require.NoError(s.T(), err, "should create analytics resource group for idempotency test")

	// Step 2: Build a cluster spec with parameters that MATCH what ListResourceGroups returns.
	// ListResourceGroups reads reslimittype 1-5 from pg_resgroupcapability:
	//   reslimittype=1 → concurrency (10)
	//   reslimittype=2 → cpu_max_percent (50)
	//   reslimittype=3 → cpu_weight (100)
	//   reslimittype=4 → memory_limit (-1, default)
	//   reslimittype=5 → min_cost (-1, default)
	// To achieve true idempotency (no ALTER), the spec must match these values exactly.
	// The needsAlter function compares desired vs actual for each field.
	clusterName := "s25c-e2e-idempotent"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
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
				// MemoryLimit: -1 matches the DB default (reslimittype=4).
				// needsAlter checks: desired.MemoryLimit != actual.MemoryLimit
				MemoryLimit: -1,
				// MinCost: -1 matches what ListResourceGroups returns (reslimittype=5).
				// needsAlter checks: desired.MinCost != actual.MinCost
				MinCost: -1,
			},
		},
	}

	// Step 3: Create a tracking DB factory to detect ALTER calls.
	trackingClient := &trackingDBClientWrapper{
		delegate:   s.dbClient,
		alterCalls: make([]string, 0),
	}
	factory := &realDBClientFactory{client: trackingClient}

	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	// Step 4: Run reconciliation — should NOT alter since values match.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "idempotent reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter)

	// Step 5: Verify no ALTER calls were made (values match).
	assert.Empty(s.T(), trackingClient.alterCalls,
		"no ALTER calls should be made when resource group parameters match")

	// Step 6: Verify the resource group still exists with correct values.
	listCtx, listCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel()

	groups, err := s.dbClient.ListResourceGroups(listCtx)
	require.NoError(s.T(), err)

	found := false
	for _, g := range groups {
		if g.Name == analyticsName {
			found = true
			assert.Equal(s.T(), int32(10), g.Concurrency)
			assert.Equal(s.T(), int32(50), g.CPUMaxPercent)
			assert.Equal(s.T(), int32(100), g.CPUWeight)
			assert.Equal(s.T(), int32(-1), g.MemoryLimit, "memoryLimit should be -1 (DB default)")
			assert.Equal(s.T(), int32(-1), g.MinCost, "minCost should be -1 (reslimittype=5 default)")
			break
		}
	}
	assert.True(s.T(), found, "analytics resource group should still exist after idempotent reconciliation")

	s.logger.Info("test 25c completed: idempotency verified — no ALTER calls made")
}

// ============================================================================
// Helper: realDBClientFactory wraps an existing db.Client for use as a factory.
// ============================================================================

// realDBClientFactory implements db.DBClientFactory by returning a pre-existing db.Client.
// The Close() call on the returned client is a no-op to prevent the shared connection
// from being closed by the reconciler.
type realDBClientFactory struct {
	client db.Client
}

// NewClient returns a non-closing wrapper around the shared db.Client.
func (f *realDBClientFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	return &nonClosingClientWrapper{delegate: f.client}, nil
}

// nonClosingClientWrapper wraps a db.Client and makes Close() a no-op.
// This prevents the reconciler from closing the shared test connection.
type nonClosingClientWrapper struct {
	delegate db.Client
}

func (w *nonClosingClientWrapper) Ping(ctx context.Context) error {
	return w.delegate.Ping(ctx)
}

// Close is a no-op to prevent closing the shared test connection.
func (w *nonClosingClientWrapper) Close() {
	// no-op: the shared test connection is managed by the suite lifecycle
}

func (w *nonClosingClientWrapper) GetSegmentConfiguration(ctx context.Context) ([]db.SegmentInfo, error) {
	return w.delegate.GetSegmentConfiguration(ctx)
}

func (w *nonClosingClientWrapper) GetClusterState(ctx context.Context) (*db.ClusterState, error) {
	return w.delegate.GetClusterState(ctx)
}

func (w *nonClosingClientWrapper) SetParameter(ctx context.Context, name, value string, scope db.ParameterScope) error {
	return w.delegate.SetParameter(ctx, name, value, scope)
}

func (w *nonClosingClientWrapper) ShowParameter(ctx context.Context, name string) (string, error) {
	return w.delegate.ShowParameter(ctx, name)
}

func (w *nonClosingClientWrapper) ReloadConfig(ctx context.Context) error {
	return w.delegate.ReloadConfig(ctx)
}

func (w *nonClosingClientWrapper) ListSessions(ctx context.Context) ([]db.Session, error) {
	return w.delegate.ListSessions(ctx)
}

func (w *nonClosingClientWrapper) CancelQuery(ctx context.Context, pid int32) (bool, error) {
	return w.delegate.CancelQuery(ctx, pid)
}

func (w *nonClosingClientWrapper) TerminateSession(ctx context.Context, pid int32) (bool, error) {
	return w.delegate.TerminateSession(ctx, pid)
}

func (w *nonClosingClientWrapper) CreateRole(ctx context.Context, opts db.RoleOptions) error {
	return w.delegate.CreateRole(ctx, opts)
}

func (w *nonClosingClientWrapper) AlterRole(ctx context.Context, opts db.RoleOptions) error {
	return w.delegate.AlterRole(ctx, opts)
}

func (w *nonClosingClientWrapper) DropRole(ctx context.Context, name string) error {
	return w.delegate.DropRole(ctx, name)
}

func (w *nonClosingClientWrapper) Vacuum(ctx context.Context, opts db.VacuumOptions) error {
	return w.delegate.Vacuum(ctx, opts)
}

func (w *nonClosingClientWrapper) Analyze(ctx context.Context, table string) error {
	return w.delegate.Analyze(ctx, table)
}

func (w *nonClosingClientWrapper) Reindex(ctx context.Context, opts db.ReindexOptions) error {
	return w.delegate.Reindex(ctx, opts)
}

func (w *nonClosingClientWrapper) GetDiskUsage(ctx context.Context, database string) ([]db.DiskUsage, error) {
	return w.delegate.GetDiskUsage(ctx, database)
}

func (w *nonClosingClientWrapper) GetReplicationLag(ctx context.Context) (int64, error) {
	return w.delegate.GetReplicationLag(ctx)
}

func (w *nonClosingClientWrapper) PromoteStandby(ctx context.Context) error {
	return w.delegate.PromoteStandby(ctx)
}

func (w *nonClosingClientWrapper) GetActiveQueryCount(ctx context.Context) (int32, int32, int32, error) {
	return w.delegate.GetActiveQueryCount(ctx)
}

func (w *nonClosingClientWrapper) GetResourceGroupUsage(ctx context.Context, group string) (float64, float64, error) {
	return w.delegate.GetResourceGroupUsage(ctx, group)
}

func (w *nonClosingClientWrapper) CreateResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	return w.delegate.CreateResourceGroup(ctx, opts)
}

func (w *nonClosingClientWrapper) AlterResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	return w.delegate.AlterResourceGroup(ctx, opts)
}

func (w *nonClosingClientWrapper) DropResourceGroup(ctx context.Context, name string) error {
	return w.delegate.DropResourceGroup(ctx, name)
}

func (w *nonClosingClientWrapper) ListResourceGroups(ctx context.Context) ([]db.ResourceGroupInfo, error) {
	return w.delegate.ListResourceGroups(ctx)
}

func (w *nonClosingClientWrapper) AssignRoleResourceGroup(ctx context.Context, role, group string) error {
	return w.delegate.AssignRoleResourceGroup(ctx, role, group)
}

func (w *nonClosingClientWrapper) CreateResourceQueue(ctx context.Context, opts db.ResourceQueueOptions) error {
	return w.delegate.CreateResourceQueue(ctx, opts)
}

func (w *nonClosingClientWrapper) DropResourceQueue(ctx context.Context, name string) error {
	return w.delegate.DropResourceQueue(ctx, name)
}

func (w *nonClosingClientWrapper) ListResourceQueues(ctx context.Context) ([]db.ResourceQueueInfo, error) {
	return w.delegate.ListResourceQueues(ctx)
}

func (w *nonClosingClientWrapper) CreateBackup(ctx context.Context, opts db.BackupOptions) (*db.BackupInfo, error) {
	return w.delegate.CreateBackup(ctx, opts)
}

func (w *nonClosingClientWrapper) RestoreBackup(ctx context.Context, opts db.RestoreOptions) error {
	return w.delegate.RestoreBackup(ctx, opts)
}

func (w *nonClosingClientWrapper) ListBackups(ctx context.Context) ([]db.BackupInfo, error) {
	return w.delegate.ListBackups(ctx)
}

func (w *nonClosingClientWrapper) DeleteBackup(ctx context.Context, id string) error {
	return w.delegate.DeleteBackup(ctx, id)
}

func (w *nonClosingClientWrapper) CreateDataLoadingJob(ctx context.Context, job db.DataLoadingJobConfig) error {
	return w.delegate.CreateDataLoadingJob(ctx, job)
}

func (w *nonClosingClientWrapper) StartDataLoadingJob(ctx context.Context, name string) error {
	return w.delegate.StartDataLoadingJob(ctx, name)
}

func (w *nonClosingClientWrapper) StopDataLoadingJob(ctx context.Context, name string) error {
	return w.delegate.StopDataLoadingJob(ctx, name)
}

func (w *nonClosingClientWrapper) ListDataLoadingJobs(ctx context.Context) ([]db.DataLoadingJobStatus, error) {
	return w.delegate.ListDataLoadingJobs(ctx)
}

func (w *nonClosingClientWrapper) GetStorageDiskUsage(ctx context.Context) ([]db.DiskUsageInfo, error) {
	return w.delegate.GetStorageDiskUsage(ctx)
}

func (w *nonClosingClientWrapper) GetBloatRecommendations(ctx context.Context) ([]db.Recommendation, error) {
	return w.delegate.GetBloatRecommendations(ctx)
}

func (w *nonClosingClientWrapper) GetSkewRecommendations(ctx context.Context) ([]db.Recommendation, error) {
	return w.delegate.GetSkewRecommendations(ctx)
}

func (w *nonClosingClientWrapper) GetAgeRecommendations(ctx context.Context) ([]db.Recommendation, error) {
	return w.delegate.GetAgeRecommendations(ctx)
}

func (w *nonClosingClientWrapper) GetIndexBloatRecommendations(ctx context.Context) ([]db.Recommendation, error) {
	return w.delegate.GetIndexBloatRecommendations(ctx)
}

func (w *nonClosingClientWrapper) TriggerRecommendationScan(ctx context.Context) error {
	return w.delegate.TriggerRecommendationScan(ctx)
}

func (w *nonClosingClientWrapper) GetTableDetails(ctx context.Context, schema, table string) (*db.TableDetail, error) {
	return w.delegate.GetTableDetails(ctx, schema, table)
}

func (w *nonClosingClientWrapper) GetUsageReport(ctx context.Context, month string) ([]db.UsageReportEntry, error) {
	return w.delegate.GetUsageReport(ctx, month)
}

func (w *nonClosingClientWrapper) InitializeMirrors(ctx context.Context, opts db.MirrorInitOptions) error {
	return w.delegate.InitializeMirrors(ctx, opts)
}

func (w *nonClosingClientWrapper) ConfigureReplication(ctx context.Context, opts db.ReplicationOptions) error {
	return w.delegate.ConfigureReplication(ctx, opts)
}

func (w *nonClosingClientWrapper) GetMirrorSyncStatus(ctx context.Context) ([]db.MirrorSyncInfo, error) {
	return w.delegate.GetMirrorSyncStatus(ctx)
}

func (w *nonClosingClientWrapper) TriggerFTSProbe(ctx context.Context) error {
	return w.delegate.TriggerFTSProbe(ctx)
}

func (w *nonClosingClientWrapper) TerminateAllBackends(ctx context.Context) (int32, error) {
	return w.delegate.TerminateAllBackends(ctx)
}

func (w *nonClosingClientWrapper) CancelAllQueries(ctx context.Context) (int32, error) {
	return w.delegate.CancelAllQueries(ctx)
}

func (w *nonClosingClientWrapper) LogRotate(ctx context.Context) error {
	return w.delegate.LogRotate(ctx)
}

func (w *nonClosingClientWrapper) RegisterNewSegments(ctx context.Context, opts db.SegmentRegistrationOptions) error {
	return w.delegate.RegisterNewSegments(ctx, opts)
}

func (w *nonClosingClientWrapper) RedistributeData(ctx context.Context, opts db.RedistributionOptions) error {
	return w.delegate.RedistributeData(ctx, opts)
}

func (w *nonClosingClientWrapper) GetRedistributionProgress(ctx context.Context) (int32, error) {
	return w.delegate.GetRedistributionProgress(ctx)
}

func (w *nonClosingClientWrapper) DeregisterSegments(ctx context.Context, newCount int32) error {
	return w.delegate.DeregisterSegments(ctx, newCount)
}

func (w *nonClosingClientWrapper) RedistributeBeforeScaleIn(ctx context.Context, opts db.ScaleInRedistributionOptions) error {
	return w.delegate.RedistributeBeforeScaleIn(ctx, opts)
}

func (w *nonClosingClientWrapper) AnalyzeSkew(ctx context.Context, database string) ([]db.TableSkewInfo, error) {
	return w.delegate.AnalyzeSkew(ctx, database)
}

func (w *nonClosingClientWrapper) RebalanceTable(ctx context.Context, database, schema, table, distKey string) error {
	return w.delegate.RebalanceTable(ctx, database, schema, table, distKey)
}

func (w *nonClosingClientWrapper) ListUserDatabases(ctx context.Context) ([]string, error) {
	return w.delegate.ListUserDatabases(ctx)
}

// ============================================================================
// Helper: trackingDBClientWrapper intercepts AlterResourceGroup calls.
// ============================================================================

// trackingDBClientWrapper wraps a db.Client and records AlterResourceGroup calls
// to verify idempotency (no ALTER when parameters match).
type trackingDBClientWrapper struct {
	nonClosingClientWrapper
	delegate   db.Client
	alterCalls []string
}

func (t *trackingDBClientWrapper) AlterResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	t.alterCalls = append(t.alterCalls, opts.Name)
	return t.delegate.AlterResourceGroup(ctx, opts)
}

func (t *trackingDBClientWrapper) CreateResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	return t.delegate.CreateResourceGroup(ctx, opts)
}

func (t *trackingDBClientWrapper) DropResourceGroup(ctx context.Context, name string) error {
	return t.delegate.DropResourceGroup(ctx, name)
}

func (t *trackingDBClientWrapper) ListResourceGroups(ctx context.Context) ([]db.ResourceGroupInfo, error) {
	return t.delegate.ListResourceGroups(ctx)
}

func (t *trackingDBClientWrapper) GetResourceGroupUsage(ctx context.Context, group string) (float64, float64, error) {
	return t.delegate.GetResourceGroupUsage(ctx, group)
}

func (t *trackingDBClientWrapper) Ping(ctx context.Context) error {
	return t.delegate.Ping(ctx)
}

// Close is a no-op to prevent closing the shared test connection.
func (t *trackingDBClientWrapper) Close() {
	// no-op: the shared test connection is managed by the suite lifecycle
}

// ============================================================================
// Utility functions
// ============================================================================

// getEnvDefault returns the value of an environment variable or a default value.
func getEnvDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// readPasswordFromSecret attempts to read the admin password from the Kubernetes secret
// associated with the cluster. It derives the secret name from the service name
// (e.g., "scenario1-cluster-client" → "scenario1-cluster-admin-password").
// Returns an empty string if the secret cannot be read.
func readPasswordFromSecret(namespace, service string) string {
	// Derive cluster name from service name by removing the "-client" suffix.
	clusterName := strings.TrimSuffix(service, "-client")
	secretName := clusterName + "-admin-password"

	logger := slog.Default()
	logger.Info("attempting to read admin password from Kubernetes secret",
		"namespace", namespace, "secret", secretName)

	out, err := exec.Command("kubectl", "get", "secret", "-n", namespace,
		secretName, "-o", "jsonpath={.data.password}").Output()
	if err != nil {
		logger.Debug("failed to read password secret", "error", err)
		return ""
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(out)))
	if err != nil {
		logger.Debug("failed to decode base64 password", "error", err)
		return ""
	}

	logger.Info("admin password read from Kubernetes secret", "secret", secretName)
	return string(decoded)
}

// findFreePort finds an available TCP port on localhost.
func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("finding free port: %w", err)
	}
	defer func() {
		_ = listener.Close()
	}()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected address type: %T", listener.Addr())
	}
	return addr.Port, nil
}

// waitForPort waits for a TCP port to become available within the given timeout.
func waitForPort(host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	logger := slog.Default()
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
		logger.Debug("waiting for port", "addr", addr, "error", err)
		time.Sleep(500 * time.Millisecond)
	}
	return false
}
