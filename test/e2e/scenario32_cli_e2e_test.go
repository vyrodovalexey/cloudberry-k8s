//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 32: All CLI Commands with Real Cloudberry Cluster
// ============================================================================
//
// Tests all `cloudberry-ctl workload` CLI commands against a real Cloudberry
// cluster by exercising the same API endpoints the CLI calls. The tests use
// an httptest server running the operator API backed by a real DB connection.
//
// Sub-tests:
//   32a — workload status
//   32b — resource-groups list
//   32c — resource-groups create
//   32d — rules list
//   32e — rules create from file
//   32f — rules import (upsert)
//   32g — rules export + round-trip
// ============================================================================

// Scenario32CLIE2ESuite tests all CLI workload commands against a real Cloudberry cluster.
type Scenario32CLIE2ESuite struct {
	E2ESuite

	// dbClient is the real database client connected to the Cloudberry coordinator.
	dbClient db.Client
	// portForwardCmd holds the kubectl port-forward process, if started.
	portForwardCmd *exec.Cmd
	// localPort is the local port used for the port-forward.
	localPort int
	// testSuffix is a unique suffix for resource names to avoid conflicts.
	testSuffix string
}

func TestE2E_Scenario32(t *testing.T) {
	suite.Run(t, new(Scenario32CLIE2ESuite))
}

// SetupSuite connects to the real Cloudberry cluster (same pattern as Scenario 25-31).
func (s *Scenario32CLIE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 32 E2E suite setup", "testSuffix", s.testSuffix)

	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port))
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		if err := s.portForwardCmd.Start(); err != nil {
			s.T().Skipf("skipping scenario 32 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward32()
			s.T().Skipf("skipping scenario 32 E2E: port-forward did not become ready within timeout")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	dbClient, err := db.NewClient(ctx, db.Config{
		Host: host, Port: int32(port), Database: database,
		Username: user, Password: password, SSLMode: "disable", MaxConns: 3,
		RetryOpts: util.RetryOptions{
			MaxRetries: 3, InitialBackoff: time.Second,
			MaxBackoff: 5 * time.Second, Multiplier: 2.0, JitterFraction: 0.1,
		},
	}, s.logger)
	if err != nil {
		s.cleanupPortForward32()
		s.T().Skipf("skipping scenario 32 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward32()
		s.T().Skipf("skipping scenario 32 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 32",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario32CLIE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward32()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward32 terminates the kubectl port-forward process if running.
func (s *Scenario32CLIE2ESuite) cleanupPortForward32() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		_ = s.portForwardCmd.Process.Kill()
		_ = s.portForwardCmd.Wait()
		s.logger.Info("port-forward process terminated")
		s.portForwardCmd = nil
	}
}

// uniqueGroupName returns a unique resource group name for test isolation.
func (s *Scenario32CLIE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s32e2e_%s_%s", base, s.testSuffix)
}

// uniqueRuleName returns a unique rule name for test isolation.
func (s *Scenario32CLIE2ESuite) uniqueRuleName(base string) string {
	return fmt.Sprintf("s32e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario32CLIE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// newScenario32APIServer creates an API server with a fake K8s client and real DB factory.
// Auth middleware is nil so all requests bypass authentication.
func (s *Scenario32CLIE2ESuite) newScenario32APIServer(
	cluster *cbv1alpha1.CloudberryCluster,
) *scenario32APITestServer {
	env := testutil.NewTestK8sEnv(cluster)
	factory := &realDBClientFactory{client: s.dbClient}

	apiServer := api.NewServer(env.Client, nil, factory, &metrics.NoopRecorder{}, s.logger, 0)
	return &scenario32APITestServer{
		server:  apiServer,
		handler: apiServer.Handler(),
		env:     env,
	}
}

type scenario32APITestServer struct {
	server  *api.Server
	handler http.Handler
	env     *testutil.TestK8sEnv
}

func (ts *scenario32APITestServer) close() {
	ts.server.Close()
}

// serveHTTP dispatches a request through the API handler with an admin identity
// injected into the context. When authMW is nil the auth middleware is skipped,
// but the permission middleware still checks for an identity in the context.
func (ts *scenario32APITestServer) serveHTTP(rec *httptest.ResponseRecorder, req *http.Request) {
	adminIdentity := &auth.Identity{
		Username:   "e2e-admin",
		Permission: auth.PermissionAdmin,
		AuthMethod: "test",
	}
	ctx := auth.ContextWithIdentity(req.Context(), adminIdentity)
	ts.handler.ServeHTTP(rec, req.WithContext(ctx))
}

// writeTempYAML writes content to a temp file and returns the path.
func (s *Scenario32CLIE2ESuite) writeTempYAML(content string) string {
	tmpDir := s.T().TempDir()
	path := filepath.Join(tmpDir, "rules.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(s.T(), err, "should write temp YAML file")
	return path
}

// ============================================================================
// Test 32a: Workload Status via API
// ============================================================================
//
// Tests the GET /clusters/{name}/workload endpoint which is the same endpoint
// the `cloudberry-ctl workload status` command calls.

func (s *Scenario32CLIE2ESuite) TestScenario32a_WorkloadStatus_CLI() {
	s.logger.Info("starting test 32a: workload status via API")

	analyticsName := s.uniqueGroupName("analytics_a")
	etlName := s.uniqueGroupName("etl_a")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	// Step 1: Create resource groups in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	err = s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: etlName, Concurrency: 5, CPUMaxPercent: 30, CPUWeight: 50,
	})
	require.NoError(s.T(), err, "should create etl resource group")

	// Step 2: Build cluster with workload spec.
	clusterName := "s32a-e2e-status"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50,
				CPUWeight: 100, MemoryLimit: -1, MinCost: -1,
			},
			{
				Name: etlName, Concurrency: 5, CPUMaxPercent: 30,
				CPUWeight: 50, MemoryLimit: -1, MinCost: -1,
			},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name: "cancel_long_queries", Enabled: true,
				ResourceGroup: analyticsName, Action: "cancel",
				ThresholdType: "running_time", Threshold: "3600", Priority: 1,
			},
			{
				Name: "log_heavy_queries", Enabled: true,
				ResourceGroup: etlName, Action: "log",
				ThresholdType: "cpu_time", Threshold: "120", Priority: 5,
			},
		},
	}

	// Step 3: Run reconciler to populate DB and ConfigMap.
	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer reconcileCancel()

	result, err := reconciler.Reconcile(reconcileCtx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Step 4: Create API server and call GET /workload.
	apiServer := api.NewServer(env.Client, nil, factory, &metrics.NoopRecorder{}, s.logger, 0)
	defer apiServer.Close()
	ts := &scenario32APITestServer{
		server:  apiServer,
		handler: apiServer.Handler(),
		env:     env,
	}

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload?namespace=default", nil)
	rec := httptest.NewRecorder()
	ts.serveHTTP(rec, req)

	require.Equal(s.T(), http.StatusOK, rec.Code, "GET /workload should succeed")

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))

	// Step 5: Verify response contains workload configuration.
	// The GET /workload endpoint returns the WorkloadSpec directly (not wrapped in a "workload" key).
	assert.Equal(s.T(), true, resp["enabled"], "workload should be enabled")

	// Verify resource groups are present.
	rgs, ok := resp["resourceGroups"].([]interface{})
	require.True(s.T(), ok, "response should contain resourceGroups array")
	assert.Len(s.T(), rgs, 2, "should have 2 resource groups")

	// Verify rules are present.
	rules, ok := resp["rules"].([]interface{})
	require.True(s.T(), ok, "response should contain rules array")
	assert.Len(s.T(), rules, 2, "should have 2 rules")

	s.logger.Info("test 32a completed: workload status verified via API")
}

// ============================================================================
// Test 32b: Resource-Groups List via API
// ============================================================================
//
// Tests the GET /clusters/{name}/workload/resource-groups endpoint which is
// the same endpoint the `cloudberry-ctl workload resource-groups list` command calls.

func (s *Scenario32CLIE2ESuite) TestScenario32b_ResourceGroupsList_CLI() {
	s.logger.Info("starting test 32b: resource-groups list via API")

	analyticsName := s.uniqueGroupName("analytics_b")
	etlName := s.uniqueGroupName("etl_b")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	// Step 1: Create resource groups in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	err = s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: etlName, Concurrency: 5, CPUMaxPercent: 30, CPUWeight: 50,
	})
	require.NoError(s.T(), err, "should create etl resource group")

	// Step 2: Build cluster and create API server.
	cluster := testutil.NewClusterBuilder("s32b-e2e-rglist", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 3: Call GET /workload/resource-groups.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/s32b-e2e-rglist/workload/resource-groups?namespace=default", nil)
	rec := httptest.NewRecorder()
	ts.serveHTTP(rec, req)

	require.Equal(s.T(), http.StatusOK, rec.Code, "GET /workload/resource-groups should succeed")

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))

	// Step 4: Verify response contains resource groups.
	total, ok := resp["total"].(float64)
	require.True(s.T(), ok, "response should contain 'total' number")
	assert.True(s.T(), total >= 2, "should have at least 2 resource groups from DB")

	rgs, ok := resp["resourceGroups"].([]interface{})
	require.True(s.T(), ok, "response should contain 'resourceGroups' array")

	// Verify our test groups are in the list.
	groupNames := make(map[string]bool)
	for _, rg := range rgs {
		rgMap, ok := rg.(map[string]interface{})
		if ok {
			if name, ok := rgMap["name"].(string); ok {
				groupNames[name] = true
			}
		}
	}
	assert.True(s.T(), groupNames[analyticsName],
		"analytics resource group should be in the list")
	assert.True(s.T(), groupNames[etlName],
		"etl resource group should be in the list")

	s.logger.Info("test 32b completed: resource-groups list verified via API",
		"total", total)
}

// ============================================================================
// Test 32c: Resource-Groups Create via API
// ============================================================================
//
// Tests the POST /clusters/{name}/workload/resource-groups endpoint which is
// the same endpoint the `cloudberry-ctl workload resource-groups create` command calls.

func (s *Scenario32CLIE2ESuite) TestScenario32c_ResourceGroupsCreate_CLI() {
	s.logger.Info("starting test 32c: resource-groups create via API")

	groupName := s.uniqueGroupName("cli_group_c")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(groupName)
	})

	// Step 1: Build cluster and create API server.
	cluster := testutil.NewClusterBuilder("s32c-e2e-rgcreate", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 2: POST to create resource group (same as CLI: workload resource-groups create --name X --concurrency 10).
	// Note: Cloudberry 2.1.0 requires cpu_max_percent when creating a resource group.
	body, _ := json.Marshal(map[string]interface{}{
		"name":          groupName,
		"concurrency":   10,
		"cpuMaxPercent": 50,
		"cpuWeight":     100,
	})
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/s32c-e2e-rgcreate/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	rec := httptest.NewRecorder()
	ts.serveHTTP(rec, req)

	require.Equal(s.T(), http.StatusCreated, rec.Code,
		"POST /workload/resource-groups should return 201")

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), "created", resp["status"])
	assert.Equal(s.T(), groupName, resp["name"])

	// Step 3: Verify group exists in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	groups, err := s.dbClient.ListResourceGroups(ctx)
	require.NoError(s.T(), err, "should list resource groups")

	var found *db.ResourceGroupInfo
	for i := range groups {
		if groups[i].Name == groupName {
			found = &groups[i]
			break
		}
	}
	require.NotNil(s.T(), found, "resource group %q should exist in DB", groupName)
	assert.Equal(s.T(), int32(10), found.Concurrency, "concurrency should be 10")

	// Step 4: Verify duplicate creation returns error.
	dupReq := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/s32c-e2e-rgcreate/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	dupRec := httptest.NewRecorder()
	ts.serveHTTP(dupRec, dupReq)

	assert.True(s.T(), dupRec.Code >= 400,
		"duplicate resource group creation should return error (got %d)", dupRec.Code)

	s.logger.Info("test 32c completed: resource-groups create verified via API",
		"group", groupName)
}

// ============================================================================
// Test 32d: Rules List via API
// ============================================================================
//
// Tests the GET /clusters/{name}/workload/rules endpoint which is the same
// endpoint the `cloudberry-ctl workload rules list` command calls.

func (s *Scenario32CLIE2ESuite) TestScenario32d_RulesList_CLI() {
	s.logger.Info("starting test 32d: rules list via API")

	analyticsName := s.uniqueGroupName("analytics_d")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	// Step 1: Create resource group in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	// Step 2: Build cluster with rules in the CRD spec.
	clusterName := "s32d-e2e-ruleslist"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name: "cancel_long_queries", Enabled: true,
				ResourceGroup: analyticsName, Action: "cancel",
				ThresholdType: "running_time", Threshold: "3600", Priority: 1,
			},
			{
				Name: "log_heavy_queries", Enabled: true,
				Action: "log", ThresholdType: "cpu_time",
				Threshold: "120", Priority: 5,
			},
		},
	}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 3: Call GET /workload/rules.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	rec := httptest.NewRecorder()
	ts.serveHTTP(rec, req)

	require.Equal(s.T(), http.StatusOK, rec.Code, "GET /workload/rules should succeed")

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))

	// Step 4: Verify response contains rules.
	assert.Equal(s.T(), float64(2), resp["total"], "should have 2 rules")

	rules, ok := resp["rules"].([]interface{})
	require.True(s.T(), ok, "response should contain 'rules' array")
	require.Len(s.T(), rules, 2, "should have 2 rules in array")

	// Verify rule attributes.
	rulesByName := make(map[string]map[string]interface{})
	for _, r := range rules {
		rMap, ok := r.(map[string]interface{})
		if ok {
			if name, ok := rMap["name"].(string); ok {
				rulesByName[name] = rMap
			}
		}
	}

	cancelRule, ok := rulesByName["cancel_long_queries"]
	require.True(s.T(), ok, "cancel_long_queries rule should exist")
	assert.Equal(s.T(), "cancel", cancelRule["action"])
	assert.Equal(s.T(), "running_time", cancelRule["thresholdType"])
	assert.Equal(s.T(), "3600", cancelRule["threshold"])
	assert.Equal(s.T(), float64(1), cancelRule["priority"])

	logRule, ok := rulesByName["log_heavy_queries"]
	require.True(s.T(), ok, "log_heavy_queries rule should exist")
	assert.Equal(s.T(), "log", logRule["action"])
	assert.Equal(s.T(), "cpu_time", logRule["thresholdType"])
	assert.Equal(s.T(), "120", logRule["threshold"])
	assert.Equal(s.T(), float64(5), logRule["priority"])

	s.logger.Info("test 32d completed: rules list verified via API")
}

// ============================================================================
// Test 32e: Rules Create from File via API
// ============================================================================
//
// Tests the POST /clusters/{name}/workload/rules endpoint which is the same
// endpoint the `cloudberry-ctl workload rules create -f rule.yaml` command calls.
// Also tests the YAML file reading utilities from internal/ctl/rules.go.

func (s *Scenario32CLIE2ESuite) TestScenario32e_RulesCreate_FromFile() {
	s.logger.Info("starting test 32e: rules create from file via API")

	analyticsName := s.uniqueGroupName("analytics_e")
	ruleName := s.uniqueRuleName("cli_rule_e")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	// Step 1: Create resource group in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	// Step 2: Write a rule YAML file (simulating what the CLI reads).
	ruleYAML := fmt.Sprintf(`name: %s
enabled: true
resourceGroup: %s
action: cancel
thresholdType: running_time
threshold: "3600"
priority: 5
`, ruleName, analyticsName)

	tmpPath := s.writeTempYAML(ruleYAML)

	// Step 3: Read the rule from file using ctl.ReadRuleFromFile (same as CLI).
	rule, err := ctl.ReadRuleFromFile(tmpPath)
	require.NoError(s.T(), err, "should read rule from YAML file")
	assert.Equal(s.T(), ruleName, rule.Name)
	assert.Equal(s.T(), "cancel", rule.Action)
	assert.Equal(s.T(), analyticsName, rule.ResourceGroup)

	// Step 4: Build cluster and create API server.
	clusterName := "s32e-e2e-rulecreate"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 5: POST the rule to the API (same as CLI: workload rules create -f rule.yaml).
	body, _ := json.Marshal(rule)
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
		bytes.NewReader(body))
	rec := httptest.NewRecorder()
	ts.serveHTTP(rec, req)

	require.Equal(s.T(), http.StatusCreated, rec.Code,
		"POST /workload/rules should return 201")

	var createResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&createResp))
	assert.Equal(s.T(), "created", createResp["status"])

	// Step 6: Verify the rule appears in the list.
	listReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	listRec := httptest.NewRecorder()
	ts.serveHTTP(listRec, listReq)

	require.Equal(s.T(), http.StatusOK, listRec.Code)
	var listResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(listRec.Body).Decode(&listResp))
	assert.Equal(s.T(), float64(1), listResp["total"], "should have 1 rule after create")

	rules, ok := listResp["rules"].([]interface{})
	require.True(s.T(), ok, "response should contain 'rules' array")
	require.Len(s.T(), rules, 1)

	ruleMap, ok := rules[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), ruleName, ruleMap["name"])
	assert.Equal(s.T(), "cancel", ruleMap["action"])
	assert.Equal(s.T(), "running_time", ruleMap["thresholdType"])
	assert.Equal(s.T(), "3600", ruleMap["threshold"])
	assert.Equal(s.T(), float64(5), ruleMap["priority"])

	// Step 7: Test --name override behavior.
	// Read the same file but override the name.
	overrideRule, err := ctl.ReadRuleFromFile(tmpPath)
	require.NoError(s.T(), err)
	overrideName := s.uniqueRuleName("override_e")
	overrideRule.Name = overrideName // --name flag overrides file name

	overrideBody, _ := json.Marshal(overrideRule)
	overrideReq := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
		bytes.NewReader(overrideBody))
	overrideRec := httptest.NewRecorder()
	ts.serveHTTP(overrideRec, overrideReq)

	require.Equal(s.T(), http.StatusCreated, overrideRec.Code,
		"POST with overridden name should succeed")

	// Verify both rules exist.
	listReq2 := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	listRec2 := httptest.NewRecorder()
	ts.serveHTTP(listRec2, listReq2)

	require.Equal(s.T(), http.StatusOK, listRec2.Code)
	var listResp2 map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(listRec2.Body).Decode(&listResp2))
	assert.Equal(s.T(), float64(2), listResp2["total"], "should have 2 rules after override create")

	// Step 8: Test duplicate rule creation returns error.
	dupReq := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
		bytes.NewReader(body))
	dupRec := httptest.NewRecorder()
	ts.serveHTTP(dupRec, dupReq)

	assert.Equal(s.T(), http.StatusBadRequest, dupRec.Code,
		"duplicate rule creation should return 400")

	var dupResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(dupRec.Body).Decode(&dupResp))
	errObj, ok := dupResp["error"].(map[string]interface{})
	require.True(s.T(), ok, "error response should contain 'error' object")
	assert.Equal(s.T(), "DUPLICATE_RULE", errObj["code"],
		"error code should be DUPLICATE_RULE")

	s.logger.Info("test 32e completed: rules create from file verified via API")
}

// ============================================================================
// Test 32f: Rules Import (Upsert) via API
// ============================================================================
//
// Tests the import workflow: POST to create new rules, PUT to update existing
// rules. This is the same logic the `cloudberry-ctl workload rules import -f`
// command uses.

func (s *Scenario32CLIE2ESuite) TestScenario32f_RulesImport_Upsert() {
	s.logger.Info("starting test 32f: rules import (upsert) via API")

	analyticsName := s.uniqueGroupName("analytics_f")
	existingRuleName := s.uniqueRuleName("existing_f")
	newRuleName := s.uniqueRuleName("import_new_f")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	// Step 1: Create resource group in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	// Step 2: Build cluster with one existing rule.
	clusterName := "s32f-e2e-import"
	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name: existingRuleName, Enabled: true,
				ResourceGroup: analyticsName, Action: "cancel",
				ThresholdType: "running_time", Threshold: "3600", Priority: 1,
			},
		},
	}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 3: Write a rules YAML file with 1 new rule + 1 that matches existing.
	rulesYAML := fmt.Sprintf(`- name: %s
  enabled: true
  resourceGroup: %s
  action: log
  thresholdType: cpu_time
  threshold: "120"
  priority: 10
- name: %s
  enabled: true
  resourceGroup: %s
  action: cancel
  thresholdType: running_time
  threshold: "7200"
  priority: 1
`, newRuleName, analyticsName, existingRuleName, analyticsName)

	tmpPath := s.writeTempYAML(rulesYAML)

	// Step 4: Read rules from file using ctl.ReadRulesFromFile (same as CLI).
	rules, err := ctl.ReadRulesFromFile(tmpPath)
	require.NoError(s.T(), err, "should read rules from YAML file")
	require.Len(s.T(), rules, 2, "should have 2 rules in file")

	// Step 5: Simulate the import upsert logic (same as CLI import command).
	var created, updated, failed int
	for i := range rules {
		rule := &rules[i]

		// Try POST (create).
		body, _ := json.Marshal(rule)
		createReq := httptest.NewRequest(http.MethodPost,
			"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
			bytes.NewReader(body))
		createRec := httptest.NewRecorder()
		ts.serveHTTP(createRec, createReq)

		if createRec.Code == http.StatusCreated {
			created++
			s.logger.Info("import: rule created", "name", rule.Name)
			continue
		}

		// Check if DUPLICATE_RULE — if so, try PUT (update).
		if createRec.Code == http.StatusBadRequest {
			var errResp map[string]interface{}
			if json.NewDecoder(createRec.Body).Decode(&errResp) == nil {
				if errObj, ok := errResp["error"].(map[string]interface{}); ok {
					if errObj["code"] == "DUPLICATE_RULE" {
						// PUT to update.
						updateBody, _ := json.Marshal(rule)
						updateReq := httptest.NewRequest(http.MethodPut,
							"/api/v1alpha1/clusters/"+clusterName+"/workload/rules/"+rule.Name+"?namespace=default",
							bytes.NewReader(updateBody))
						updateRec := httptest.NewRecorder()
						ts.serveHTTP(updateRec, updateReq)

						if updateRec.Code == http.StatusOK {
							updated++
							s.logger.Info("import: rule updated", "name", rule.Name)
							continue
						}
					}
				}
			}
		}

		failed++
		s.logger.Error("import: rule failed", "name", rule.Name, "status", createRec.Code)
	}

	// Step 6: Verify import summary.
	assert.Equal(s.T(), 1, created, "should have 1 rule created")
	assert.Equal(s.T(), 1, updated, "should have 1 rule updated")
	assert.Equal(s.T(), 0, failed, "should have 0 rules failed")

	// Step 7: Verify all rules are present in the list.
	listReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	listRec := httptest.NewRecorder()
	ts.serveHTTP(listRec, listReq)

	require.Equal(s.T(), http.StatusOK, listRec.Code)
	var listResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(listRec.Body).Decode(&listResp))
	assert.Equal(s.T(), float64(2), listResp["total"], "should have 2 rules after import")

	rulesArr, ok := listResp["rules"].([]interface{})
	require.True(s.T(), ok)

	rulesByName := make(map[string]map[string]interface{})
	for _, r := range rulesArr {
		rMap, ok := r.(map[string]interface{})
		if ok {
			if name, ok := rMap["name"].(string); ok {
				rulesByName[name] = rMap
			}
		}
	}

	// Verify the new rule was created.
	newRule, ok := rulesByName[newRuleName]
	require.True(s.T(), ok, "new rule should exist after import")
	assert.Equal(s.T(), "log", newRule["action"])
	assert.Equal(s.T(), "cpu_time", newRule["thresholdType"])
	assert.Equal(s.T(), "120", newRule["threshold"])

	// Verify the existing rule was updated (threshold changed from 3600 to 7200).
	existingRule, ok := rulesByName[existingRuleName]
	require.True(s.T(), ok, "existing rule should still exist after import")
	assert.Equal(s.T(), "cancel", existingRule["action"])
	assert.Equal(s.T(), "7200", existingRule["threshold"],
		"existing rule threshold should be updated to 7200")

	// Step 8: Test idempotent import — import same file again.
	var created2, updated2, failed2 int
	for i := range rules {
		rule := &rules[i]
		body, _ := json.Marshal(rule)
		createReq := httptest.NewRequest(http.MethodPost,
			"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
			bytes.NewReader(body))
		createRec := httptest.NewRecorder()
		ts.serveHTTP(createRec, createReq)

		if createRec.Code == http.StatusCreated {
			created2++
			continue
		}

		if createRec.Code == http.StatusBadRequest {
			var errResp map[string]interface{}
			if json.NewDecoder(createRec.Body).Decode(&errResp) == nil {
				if errObj, ok := errResp["error"].(map[string]interface{}); ok {
					if errObj["code"] == "DUPLICATE_RULE" {
						updateBody, _ := json.Marshal(rule)
						updateReq := httptest.NewRequest(http.MethodPut,
							"/api/v1alpha1/clusters/"+clusterName+"/workload/rules/"+rule.Name+"?namespace=default",
							bytes.NewReader(updateBody))
						updateRec := httptest.NewRecorder()
						ts.serveHTTP(updateRec, updateReq)
						if updateRec.Code == http.StatusOK {
							updated2++
							continue
						}
					}
				}
			}
		}
		failed2++
	}

	assert.Equal(s.T(), 0, created2, "second import should create 0 rules")
	assert.Equal(s.T(), 2, updated2, "second import should update 2 rules")
	assert.Equal(s.T(), 0, failed2, "second import should fail 0 rules")

	s.logger.Info("test 32f completed: rules import (upsert) verified via API",
		"created", created, "updated", updated, "failed", failed)
}

// ============================================================================
// Test 32g: Rules Export + Round-Trip via API
// ============================================================================
//
// Tests the export workflow: GET /workload/rules → convert to YAML → write file.
// Then verifies round-trip: export → delete all → import → verify identical.
// This is the same logic the `cloudberry-ctl workload rules export` command uses.

func (s *Scenario32CLIE2ESuite) TestScenario32g_RulesExport_RoundTrip() {
	s.logger.Info("starting test 32g: rules export + round-trip via API")

	analyticsName := s.uniqueGroupName("analytics_g")

	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
	})

	// Step 1: Create resource group in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name: analyticsName, Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	require.NoError(s.T(), err, "should create analytics resource group")

	// Step 2: Build cluster with 2 rules.
	clusterName := "s32g-e2e-export"
	rule1Name := s.uniqueRuleName("export_r1_g")
	rule2Name := s.uniqueRuleName("export_r2_g")

	cluster := testutil.NewClusterBuilder(clusterName, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name: rule1Name, Enabled: true,
				ResourceGroup: analyticsName, Action: "cancel",
				ThresholdType: "running_time", Threshold: "3600", Priority: 1,
			},
			{
				Name: rule2Name, Enabled: true,
				Action: "log", ThresholdType: "cpu_time",
				Threshold: "120", Priority: 5,
			},
		},
	}

	ts := s.newScenario32APIServer(cluster)
	defer ts.close()

	// Step 3: GET /workload/rules to fetch all rules (same as CLI export).
	listReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	listRec := httptest.NewRecorder()
	ts.serveHTTP(listRec, listReq)

	require.Equal(s.T(), http.StatusOK, listRec.Code)
	var listResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(listRec.Body).Decode(&listResp))
	assert.Equal(s.T(), float64(2), listResp["total"], "should have 2 rules")

	// Step 4: Convert API response to WorkloadRuleFile slice (same as CLI export).
	rulesRaw, ok := listResp["rules"].([]interface{})
	require.True(s.T(), ok, "response should contain 'rules' array")

	var exportedRules []ctl.WorkloadRuleFile
	for _, raw := range rulesRaw {
		jsonBytes, err := json.Marshal(raw)
		require.NoError(s.T(), err)
		var rule ctl.WorkloadRuleFile
		require.NoError(s.T(), json.Unmarshal(jsonBytes, &rule))
		exportedRules = append(exportedRules, rule)
	}
	require.Len(s.T(), exportedRules, 2, "should have 2 exported rules")

	// Step 5: Write exported rules to a YAML file using ctl.WriteRulesToFile.
	exportPath := filepath.Join(s.T().TempDir(), "exported-rules.yaml")
	err = ctl.WriteRulesToFile(exportPath, exportedRules)
	require.NoError(s.T(), err, "should write rules to YAML file")

	// Verify the file exists and is readable.
	_, err = os.Stat(exportPath)
	require.NoError(s.T(), err, "exported file should exist")

	// Step 6: Read the exported file back using ctl.ReadRulesFromFile.
	reimportedRules, err := ctl.ReadRulesFromFile(exportPath)
	require.NoError(s.T(), err, "should read exported rules back from file")
	require.Len(s.T(), reimportedRules, 2, "should have 2 rules after reading back")

	// Verify the rules match the originals.
	reimportedByName := make(map[string]ctl.WorkloadRuleFile)
	for _, r := range reimportedRules {
		reimportedByName[r.Name] = r
	}

	r1, ok := reimportedByName[rule1Name]
	require.True(s.T(), ok, "rule1 should exist in reimported rules")
	assert.Equal(s.T(), "cancel", r1.Action)
	assert.Equal(s.T(), "running_time", r1.ThresholdType)
	assert.Equal(s.T(), "3600", r1.Threshold)
	assert.Equal(s.T(), int32(1), r1.Priority)

	r2, ok := reimportedByName[rule2Name]
	require.True(s.T(), ok, "rule2 should exist in reimported rules")
	assert.Equal(s.T(), "log", r2.Action)
	assert.Equal(s.T(), "cpu_time", r2.ThresholdType)
	assert.Equal(s.T(), "120", r2.Threshold)
	assert.Equal(s.T(), int32(5), r2.Priority)

	// Step 7: Round-trip test — delete all rules, then import from exported file.
	// Delete rule 1.
	delReq1 := httptest.NewRequest(http.MethodDelete,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules/"+rule1Name+"?namespace=default", nil)
	delRec1 := httptest.NewRecorder()
	ts.serveHTTP(delRec1, delReq1)
	require.Equal(s.T(), http.StatusOK, delRec1.Code, "DELETE rule1 should succeed")

	// Delete rule 2.
	delReq2 := httptest.NewRequest(http.MethodDelete,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules/"+rule2Name+"?namespace=default", nil)
	delRec2 := httptest.NewRecorder()
	ts.serveHTTP(delRec2, delReq2)
	require.Equal(s.T(), http.StatusOK, delRec2.Code, "DELETE rule2 should succeed")

	// Verify no rules remain.
	emptyListReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	emptyListRec := httptest.NewRecorder()
	ts.serveHTTP(emptyListRec, emptyListReq)

	require.Equal(s.T(), http.StatusOK, emptyListRec.Code)
	var emptyResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(emptyListRec.Body).Decode(&emptyResp))
	assert.Equal(s.T(), float64(0), emptyResp["total"], "should have 0 rules after deletion")

	// Step 8: Import the exported rules back (POST each one).
	for i := range reimportedRules {
		body, _ := json.Marshal(&reimportedRules[i])
		importReq := httptest.NewRequest(http.MethodPost,
			"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default",
			bytes.NewReader(body))
		importRec := httptest.NewRecorder()
		ts.serveHTTP(importRec, importReq)

		require.Equal(s.T(), http.StatusCreated, importRec.Code,
			"POST to re-import rule %q should succeed", reimportedRules[i].Name)
	}

	// Step 9: Verify the re-imported rules match the original set.
	finalListReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	finalListRec := httptest.NewRecorder()
	ts.serveHTTP(finalListRec, finalListReq)

	require.Equal(s.T(), http.StatusOK, finalListRec.Code)
	var finalResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(finalListRec.Body).Decode(&finalResp))
	assert.Equal(s.T(), float64(2), finalResp["total"],
		"should have 2 rules after round-trip import")

	finalRules, ok := finalResp["rules"].([]interface{})
	require.True(s.T(), ok)

	finalRulesByName := make(map[string]map[string]interface{})
	for _, r := range finalRules {
		rMap, ok := r.(map[string]interface{})
		if ok {
			if name, ok := rMap["name"].(string); ok {
				finalRulesByName[name] = rMap
			}
		}
	}

	// Verify rule1 attributes preserved through round-trip.
	finalR1, ok := finalRulesByName[rule1Name]
	require.True(s.T(), ok, "rule1 should exist after round-trip")
	assert.Equal(s.T(), "cancel", finalR1["action"])
	assert.Equal(s.T(), "running_time", finalR1["thresholdType"])
	assert.Equal(s.T(), "3600", finalR1["threshold"])
	assert.Equal(s.T(), float64(1), finalR1["priority"])
	assert.Equal(s.T(), analyticsName, finalR1["resourceGroup"])

	// Verify rule2 attributes preserved through round-trip.
	finalR2, ok := finalRulesByName[rule2Name]
	require.True(s.T(), ok, "rule2 should exist after round-trip")
	assert.Equal(s.T(), "log", finalR2["action"])
	assert.Equal(s.T(), "cpu_time", finalR2["thresholdType"])
	assert.Equal(s.T(), "120", finalR2["threshold"])
	assert.Equal(s.T(), float64(5), finalR2["priority"])

	// Step 10: Test export when no rules exist.
	// Delete all rules first.
	for _, name := range []string{rule1Name, rule2Name} {
		delReq := httptest.NewRequest(http.MethodDelete,
			"/api/v1alpha1/clusters/"+clusterName+"/workload/rules/"+name+"?namespace=default", nil)
		delRec := httptest.NewRecorder()
		ts.serveHTTP(delRec, delReq)
		require.Equal(s.T(), http.StatusOK, delRec.Code)
	}

	// Export empty rule set.
	emptyExportReq := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+clusterName+"/workload/rules?namespace=default", nil)
	emptyExportRec := httptest.NewRecorder()
	ts.serveHTTP(emptyExportRec, emptyExportReq)

	require.Equal(s.T(), http.StatusOK, emptyExportRec.Code)
	var emptyExportResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(emptyExportRec.Body).Decode(&emptyExportResp))
	assert.Equal(s.T(), float64(0), emptyExportResp["total"],
		"should have 0 rules for empty export")

	// Write empty rules to file.
	emptyExportPath := filepath.Join(s.T().TempDir(), "empty-rules.yaml")
	err = ctl.WriteRulesToFile(emptyExportPath, []ctl.WorkloadRuleFile{})
	require.NoError(s.T(), err, "should write empty rules to YAML file")

	// Read back empty file.
	emptyRules, err := ctl.ReadRulesFromFile(emptyExportPath)
	require.NoError(s.T(), err, "should read empty rules file")
	assert.Empty(s.T(), emptyRules, "empty export should produce empty slice")

	s.logger.Info("test 32g completed: rules export + round-trip verified via API")
}
