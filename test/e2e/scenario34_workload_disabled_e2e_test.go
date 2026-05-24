//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/idle"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 34: Workload Management Disabled with Real Cloudberry Cluster
// ============================================================================
//
// Validates the workload disable/re-enable lifecycle against a real Cloudberry
// cluster. When spec.workload.enabled is set to false, the operator must:
//   - Drop all user-created resource groups from the database
//   - Delete the workload-rules ConfigMap
//   - Stop the idle session daemon
//   - Zero out resource group metrics
//   - Set WorkloadConfigured=False with reason WorkloadDisabled
//   - Emit a WorkloadDisabled event
//
// Sub-tests:
//   - 34a: Disable drops resource groups and deletes ConfigMap, re-enable recreates them
//   - 34b: Idle daemon stops on disable, restarts on re-enable
//   - 34c: Metrics zeroed on disable
//   - 34d: Idempotent disable (multiple reconciliations with nothing to clean up)
// ============================================================================

// Scenario34WorkloadDisabledE2ESuite tests workload disable/re-enable lifecycle
// against a real Cloudberry cluster.
type Scenario34WorkloadDisabledE2ESuite struct {
	E2ESuite

	// dbClient is the real database client connected to the Cloudberry coordinator.
	dbClient db.Client
	// portForwardCmd holds the kubectl port-forward process, if started.
	portForwardCmd *exec.Cmd
	// localPort is the local port used for the port-forward.
	localPort int
	// testSuffix is a unique suffix for resource group/role names to avoid conflicts.
	testSuffix string
	// connHost is the host used for DB connections.
	connHost string
	// connPort is the port used for DB connections.
	connPort int
	// connDatabase is the database used for DB connections.
	connDatabase string
	// connPassword is the admin password for DB connections.
	connPassword string
}

func TestE2E_Scenario34(t *testing.T) {
	suite.Run(t, new(Scenario34WorkloadDisabledE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario34WorkloadDisabledE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 34 E2E suite setup", "testSuffix", s.testSuffix)

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
			s.T().Skipf("skipping scenario 34 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward34()
			s.T().Skipf("skipping scenario 34 E2E: port-forward did not become ready within timeout")
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
		s.cleanupPortForward34()
		s.T().Skipf("skipping scenario 34 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward34()
		s.T().Skipf("skipping scenario 34 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	// Store connection parameters for creating test role connections.
	s.connHost = host
	s.connPort = port
	s.connDatabase = database
	s.connPassword = password

	s.logger.Info("connected to real Cloudberry cluster for scenario 34",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario34WorkloadDisabledE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward34()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward34 terminates the kubectl port-forward process if running.
func (s *Scenario34WorkloadDisabledE2ESuite) cleanupPortForward34() {
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
func (s *Scenario34WorkloadDisabledE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s34e2e_%s_%s", base, s.testSuffix)
}

// uniqueRoleName returns a unique role name for test isolation.
func (s *Scenario34WorkloadDisabledE2ESuite) uniqueRoleName(base string) string {
	return fmt.Sprintf("s34e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario34WorkloadDisabledE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// cleanupRole drops a role, ignoring errors (best-effort cleanup).
func (s *Scenario34WorkloadDisabledE2ESuite) cleanupRole(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropRole(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop role (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped role", "name", name)
	}
}

// openTestRoleConnection opens a raw pgx connection as the test role.
// Returns the connection and its backend PID.
func (s *Scenario34WorkloadDisabledE2ESuite) openTestRoleConnection(
	ctx context.Context, roleName, rolePassword string,
) (*pgx.Conn, int32, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		s.connHost, s.connPort, roleName, rolePassword, s.connDatabase)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, 0, fmt.Errorf("connecting as test role %s: %w", roleName, err)
	}

	// Get the backend PID for this connection.
	var pid int32
	err = conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		conn.Close(ctx)
		return nil, 0, fmt.Errorf("getting backend PID for test role %s: %w", roleName, err)
	}

	return conn, pid, nil
}

// waitForSessionTermination polls pg_stat_activity via the admin connection
// to check if the given PID has disappeared (been terminated).
// IMPORTANT: We do NOT ping the test connection because that would update
// query_start in pg_stat_activity, resetting the idle timer.
func (s *Scenario34WorkloadDisabledE2ESuite) waitForSessionTermination(
	pid int32, timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		sessions, err := s.dbClient.ListSessions(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("error listing sessions during termination wait", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		found := false
		for _, sess := range sessions {
			if sess.PID == pid {
				found = true
				s.logger.Info("session still alive, waiting for termination",
					"pid", pid, "state", sess.State,
					"queryStart", sess.QueryStart)
				break
			}
		}

		if !found {
			s.logger.Info("session terminated (no longer in pg_stat_activity)", "pid", pid)
			return true
		}

		time.Sleep(1 * time.Second)
	}
	return false
}

// waitForSessionSurvival verifies that a session remains alive for the given duration
// by checking pg_stat_activity via the admin connection.
// IMPORTANT: We do NOT ping the test connection because that would update
// query_start in pg_stat_activity, resetting the idle timer.
// Returns true if the session survived (was NOT terminated).
func (s *Scenario34WorkloadDisabledE2ESuite) waitForSessionSurvival(
	pid int32, duration time.Duration,
) bool {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		sessions, err := s.dbClient.ListSessions(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("error listing sessions during survival check", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		found := false
		for _, sess := range sessions {
			if sess.PID == pid {
				found = true
				break
			}
		}

		if !found {
			s.logger.Info("session unexpectedly terminated during survival check", "pid", pid)
			return false
		}

		time.Sleep(1 * time.Second)
	}
	return true
}

// ============================================================================
// Tracking metrics recorder for Scenario 34 E2E tests
// ============================================================================

// scenario34MetricsRecorder wraps NoopRecorder and records SetResourceGroupUsage calls.
type scenario34MetricsRecorder struct {
	metrics.NoopRecorder
	mu                 sync.Mutex
	resourceGroupUsage []resourceGroupUsageEvent
}

// resourceGroupUsageEvent records a single SetResourceGroupUsage call.
type resourceGroupUsageEvent struct {
	Cluster   string
	Namespace string
	Group     string
	CPU       float64
	Memory    float64
}

// SetResourceGroupUsage records a resource group usage event.
func (r *scenario34MetricsRecorder) SetResourceGroupUsage(cluster, namespace, group string, cpu, memory float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resourceGroupUsage = append(r.resourceGroupUsage, resourceGroupUsageEvent{
		Cluster:   cluster,
		Namespace: namespace,
		Group:     group,
		CPU:       cpu,
		Memory:    memory,
	})
}

// getUsageEvents returns a copy of all recorded usage events.
func (r *scenario34MetricsRecorder) getUsageEvents() []resourceGroupUsageEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]resourceGroupUsageEvent, len(r.resourceGroupUsage))
	copy(result, r.resourceGroupUsage)
	return result
}

// resetUsageEvents clears all recorded usage events.
func (r *scenario34MetricsRecorder) resetUsageEvents() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resourceGroupUsage = nil
}

// ============================================================================
// Test 34a: Disable Drops Resource Groups and Deletes ConfigMap, Re-enable Recreates
// ============================================================================
//
// This is the core test. It verifies the full disable/re-enable lifecycle:
//
// 1. Enable phase: Create cluster with workload enabled, 2 resource groups,
//    2 rules, 1 idle rule. Run reconciliation. Verify resource groups exist
//    in DB, ConfigMap created, WorkloadConfigured=True.
//
// 2. Disable phase: Set cluster.Spec.Workload.Enabled = false. Run reconciliation.
//    Verify: resource groups DROPPED from real DB, ConfigMap DELETED,
//    WorkloadConfigured=False with reason WorkloadDisabled.
//
// 3. Re-enable phase: Set cluster.Spec.Workload.Enabled = true (same groups/rules).
//    Run reconciliation. Verify: resource groups RE-CREATED in real DB,
//    ConfigMap RE-CREATED, WorkloadConfigured=True with reason WorkloadReconciled.

func (s *Scenario34WorkloadDisabledE2ESuite) TestScenario34a_DisableDropsResourceGroups_ReenableRecreates() {
	s.logger.Info("starting test 34a: disable drops resource groups and deletes ConfigMap, re-enable recreates")

	analyticsName := s.uniqueGroupName("analytics_a")
	etlName := s.uniqueGroupName("etl_a")

	// Register cleanup: always drop resource groups even if the test expects them to be dropped.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	clusterName := "s34a-e2e-disable"
	clusterNamespace := "default"

	// ---- Phase 1: Enable workload ----
	s.logger.Info("phase 1: enabling workload management")

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
			},
			{
				Name:          etlName,
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
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

	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "enable reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Verify resource groups exist in real DB.
	listCtx, listCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel()

	groups, err := s.dbClient.ListResourceGroups(listCtx)
	require.NoError(s.T(), err, "should list resource groups after enable")

	groupsByName := make(map[string]db.ResourceGroupInfo, len(groups))
	for _, g := range groups {
		groupsByName[g.Name] = g
	}

	_, analyticsExists := groupsByName[analyticsName]
	require.True(s.T(), analyticsExists, "analytics resource group should exist after enable")
	_, etlExists := groupsByName[etlName]
	require.True(s.T(), etlExists, "etl resource group should exist after enable")

	// Verify ConfigMap was created with rules.json and idle-rules.json.
	cmName := clusterName + "-workload-rules"
	cm, err := env.GetConfigMap(ctx, cmName, clusterNamespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist after enable")

	_, hasRules := cm.Data["rules.json"]
	require.True(s.T(), hasRules, "ConfigMap should contain rules.json")
	_, hasIdleRules := cm.Data["idle-rules.json"]
	require.True(s.T(), hasIdleRules, "ConfigMap should contain idle-rules.json")

	// Verify WorkloadConfigured condition is True.
	updated, err := env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status), "WorkloadConfigured should be True after enable")
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set after enable")

	s.logger.Info("phase 1 complete: workload enabled, resource groups and ConfigMap verified")

	// ---- Phase 2: Disable workload ----
	s.logger.Info("phase 2: disabling workload management")

	// Re-read the cluster from the fake client to get the latest version.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Update the spec to disable workload.
	updated.Spec.Workload.Enabled = false
	// Bump generation to trigger reconciliation.
	updated.Generation = 2
	err = env.Client.Update(ctx, updated)
	require.NoError(s.T(), err, "should update cluster to disable workload")

	result, err = reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "disable reconciliation should succeed")

	// Verify resource groups are DROPPED from real DB.
	listCtx2, listCancel2 := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel2()

	groups, err = s.dbClient.ListResourceGroups(listCtx2)
	require.NoError(s.T(), err, "should list resource groups after disable")

	groupsByName = make(map[string]db.ResourceGroupInfo, len(groups))
	for _, g := range groups {
		groupsByName[g.Name] = g
	}

	_, analyticsStillExists := groupsByName[analyticsName]
	assert.False(s.T(), analyticsStillExists,
		"analytics resource group should NOT exist after disable")
	_, etlStillExists := groupsByName[etlName]
	assert.False(s.T(), etlStillExists,
		"etl resource group should NOT exist after disable")

	// Verify ConfigMap is DELETED.
	_, cmErr := env.GetConfigMap(ctx, cmName, clusterNamespace)
	assert.True(s.T(), apierrors.IsNotFound(cmErr),
		"workload-rules ConfigMap should be deleted after disable, got: %v", cmErr)

	// Verify WorkloadConfigured condition is False with reason WorkloadDisabled.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound = false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "False", string(c.Status),
				"WorkloadConfigured should be False after disable")
			assert.Equal(s.T(), "WorkloadDisabled", c.Reason,
				"reason should be WorkloadDisabled")
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set after disable")

	s.logger.Info("phase 2 complete: workload disabled, resource groups dropped, ConfigMap deleted")

	// ---- Phase 3: Re-enable workload ----
	s.logger.Info("phase 3: re-enabling workload management")

	// Re-read the cluster from the fake client.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Re-enable workload with the same resource groups and rules.
	updated.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          analyticsName,
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
			},
			{
				Name:          etlName,
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
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
	// Bump generation again.
	updated.Generation = 3
	err = env.Client.Update(ctx, updated)
	require.NoError(s.T(), err, "should update cluster to re-enable workload")

	result, err = reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "re-enable reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after re-enable reconciliation")

	// Verify resource groups are RE-CREATED in real DB.
	listCtx3, listCancel3 := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer listCancel3()

	groups, err = s.dbClient.ListResourceGroups(listCtx3)
	require.NoError(s.T(), err, "should list resource groups after re-enable")

	groupsByName = make(map[string]db.ResourceGroupInfo, len(groups))
	for _, g := range groups {
		groupsByName[g.Name] = g
	}

	analyticsInfo, analyticsRecreated := groupsByName[analyticsName]
	require.True(s.T(), analyticsRecreated,
		"analytics resource group should be recreated after re-enable")
	assert.Equal(s.T(), int32(10), analyticsInfo.Concurrency, "analytics concurrency after re-enable")
	assert.Equal(s.T(), int32(50), analyticsInfo.CPUMaxPercent, "analytics cpuMaxPercent after re-enable")

	etlInfo, etlRecreated := groupsByName[etlName]
	require.True(s.T(), etlRecreated,
		"etl resource group should be recreated after re-enable")
	assert.Equal(s.T(), int32(5), etlInfo.Concurrency, "etl concurrency after re-enable")
	assert.Equal(s.T(), int32(30), etlInfo.CPUMaxPercent, "etl cpuMaxPercent after re-enable")

	// Verify ConfigMap is RE-CREATED with rules.json and idle-rules.json.
	cm, err = env.GetConfigMap(ctx, cmName, clusterNamespace)
	require.NoError(s.T(), err, "workload-rules ConfigMap should exist after re-enable")

	rulesJSON, hasRules := cm.Data["rules.json"]
	require.True(s.T(), hasRules, "ConfigMap should contain rules.json after re-enable")

	var rules []cbv1alpha1.WorkloadRule
	err = json.Unmarshal([]byte(rulesJSON), &rules)
	require.NoError(s.T(), err, "rules.json should be valid JSON")
	require.Len(s.T(), rules, 2, "should have 2 workload rules after re-enable")

	idleRulesJSON, hasIdleRules := cm.Data["idle-rules.json"]
	require.True(s.T(), hasIdleRules, "ConfigMap should contain idle-rules.json after re-enable")

	var idleRules []cbv1alpha1.IdleSessionRule
	err = json.Unmarshal([]byte(idleRulesJSON), &idleRules)
	require.NoError(s.T(), err, "idle-rules.json should be valid JSON")
	require.Len(s.T(), idleRules, 1, "should have 1 idle session rule after re-enable")

	// Verify WorkloadConfigured condition is True with reason WorkloadReconciled.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound = false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status),
				"WorkloadConfigured should be True after re-enable")
			assert.Equal(s.T(), "WorkloadReconciled", c.Reason,
				"reason should be WorkloadReconciled after re-enable")
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set after re-enable")

	s.logger.Info("test 34a completed: full disable/re-enable lifecycle verified",
		"analytics", analyticsName, "etl", etlName)
}

// ============================================================================
// Test 34b: Idle Daemon Stops on Disable, Restarts on Re-enable
// ============================================================================
//
// Steps:
//  1. Create resource group and test role in real DB
//  2. Create idle daemon with short scan interval (2s) and 10s timeout
//  3. Start the daemon, open a test connection as the test role
//  4. Verify the daemon terminates idle sessions (sanity check)
//  5. Stop the daemon (simulating what the controller does on disable)
//  6. Open a new test connection, leave idle for >15s
//  7. Verify: session NOT terminated (daemon was stopped)
//  8. Restart the daemon (simulating re-enable)
//  9. Leave idle for >12s
// 10. Verify: session IS terminated (daemon restarted)

func (s *Scenario34WorkloadDisabledE2ESuite) TestScenario34b_IdleDaemonStopsOnDisable() {
	s.logger.Info("starting test 34b: idle daemon stops on disable, restarts on re-enable")

	groupName := s.uniqueGroupName("idle_b")
	roleName := s.uniqueRoleName("role_b")
	rolePassword := "s34e2e_testpass_b"
	ruleName := "terminate-idle-34b"

	// Register cleanup: role must be dropped before resource group.
	s.T().Cleanup(func() {
		s.cleanupRole(roleName)
		s.cleanupResourceGroup(groupName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Step 1: Create resource group and test role in real DB.
	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          groupName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create resource group")
	s.logger.Info("created resource group", "name", groupName)

	err = s.dbClient.CreateRole(ctx, db.RoleOptions{
		Name:     roleName,
		Login:    true,
		Password: rolePassword,
	})
	require.NoError(s.T(), err, "should create test role")

	err = s.dbClient.AssignRoleResourceGroup(ctx, roleName, groupName)
	require.NoError(s.T(), err, "should assign role to resource group")
	s.logger.Info("created and assigned test role", "role", roleName, "group", groupName)

	// Step 2: Parse idle rules and create the daemon.
	crdRules := []cbv1alpha1.IdleSessionRule{
		{
			Name:                 ruleName,
			Enabled:              true,
			ResourceGroup:        groupName,
			IdleTimeout:          "10s",
			ExcludeInTransaction: true,
			TerminateMessage:     "Session terminated due to inactivity (34b)",
		},
	}

	parsedRules, err := idle.ParseIdleRules(crdRules)
	require.NoError(s.T(), err, "should parse idle rules")

	recorder34b := &scenario33MetricsRecorder{}
	daemon := idle.New(idle.Config{
		ClusterName:  "s34b-e2e-cluster",
		Namespace:    "default",
		ScanInterval: 2 * time.Second,
		DBClient:     s.dbClient,
		Metrics:      recorder34b,
		Logger:       s.logger,
	})
	daemon.UpdateRules(parsedRules)
	daemon.Start(ctx)

	s.logger.Info("idle daemon started for sanity check", "scanInterval", "2s", "idleTimeout", "10s")

	// Step 3: Open a test connection and verify the daemon terminates idle sessions.
	testConn1, pid1, err := s.openTestRoleConnection(ctx, roleName, rolePassword)
	require.NoError(s.T(), err, "should open test role connection for sanity check")
	s.logger.Info("opened test role connection for sanity check", "pid", pid1)

	// Execute a query to establish the session, then leave idle.
	var result int
	err = testConn1.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(s.T(), err, "should execute initial query")

	// Wait for the daemon to terminate the session (sanity check).
	terminated := s.waitForSessionTermination(pid1, 30*time.Second)
	assert.True(s.T(), terminated,
		"sanity check: idle session should be terminated by daemon")
	testConn1.Close(ctx)

	s.logger.Info("sanity check passed: daemon terminates idle sessions")

	// Step 4: Stop the daemon (simulating disable).
	daemon.Stop()
	s.logger.Info("idle daemon stopped (simulating workload disable)")

	// Step 5: Open a new test connection and leave idle for >15s.
	testConn2, pid2, err := s.openTestRoleConnection(ctx, roleName, rolePassword)
	require.NoError(s.T(), err, "should open test role connection after daemon stop")
	s.logger.Info("opened test role connection after daemon stop", "pid", pid2)

	// Execute a query to establish the session, then leave idle.
	err = testConn2.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(s.T(), err, "should execute initial query after daemon stop")

	// Step 6: Verify session is NOT terminated (daemon was stopped).
	// Wait 15s (beyond the 10s timeout + 2s scan interval + 3s buffer).
	survived := s.waitForSessionSurvival(pid2, 15*time.Second)
	assert.True(s.T(), survived,
		"session should NOT be terminated when daemon is stopped")

	s.logger.Info("session survived as expected (daemon stopped)", "pid", pid2)

	// Step 7: Restart the daemon (simulating re-enable).
	daemon2 := idle.New(idle.Config{
		ClusterName:  "s34b-e2e-cluster",
		Namespace:    "default",
		ScanInterval: 2 * time.Second,
		DBClient:     s.dbClient,
		Metrics:      recorder34b,
		Logger:       s.logger,
	})
	daemon2.UpdateRules(parsedRules)
	daemon2.Start(ctx)
	defer daemon2.Stop()

	s.logger.Info("idle daemon restarted (simulating workload re-enable)")

	// Step 8: The session (pid2) has been idle for >15s already.
	// The daemon should detect it and terminate it within a few scan cycles.
	terminated = s.waitForSessionTermination(pid2, 30*time.Second)
	assert.True(s.T(), terminated,
		"session should be terminated after daemon restart (was idle >15s)")

	// Close the test connection (may already be closed by termination).
	testConn2.Close(ctx)

	s.logger.Info("test 34b completed: idle daemon stops on disable, restarts on re-enable",
		"pid1", pid1, "pid2", pid2)
}

// ============================================================================
// Test 34c: Metrics Zeroed on Disable
// ============================================================================
//
// Steps:
//  1. Create cluster with workload enabled, 2 resource groups
//  2. Create a tracking metrics recorder that records SetResourceGroupUsage calls
//  3. Run reconciliation (enabled) → verify metrics were set for both groups
//  4. Disable: Set Enabled=false, run reconciliation
//  5. Verify: SetResourceGroupUsage was called with (0, 0) for each dropped group

func (s *Scenario34WorkloadDisabledE2ESuite) TestScenario34c_MetricsZeroedOnDisable() {
	s.logger.Info("starting test 34c: metrics zeroed on disable")

	analyticsName := s.uniqueGroupName("analytics_c")
	etlName := s.uniqueGroupName("etl_c")

	// Register cleanup.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(analyticsName)
		s.cleanupResourceGroup(etlName)
	})

	clusterName := "s34c-e2e-metrics"
	clusterNamespace := "default"

	// Step 1: Build cluster with workload enabled.
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
			},
			{
				Name:          etlName,
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
			},
		},
	}

	// Step 2: Create tracking metrics recorder.
	metricsRecorder := &scenario34MetricsRecorder{}
	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, metricsRecorder, s.logger,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Step 3: Run reconciliation (enabled).
	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "enable reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter)

	// Note: In Cloudberry 2.1.0 with gp_resource_manager=queue, GetResourceGroupUsage
	// fails because gp_resgroup_status doesn't exist. So SetResourceGroupUsage is NOT
	// called during the enable phase. The key verification is that metrics ARE zeroed
	// during the disable phase when resource groups are dropped.
	enableEvents := metricsRecorder.getUsageEvents()
	s.logger.Info("metrics events after enable (may be 0 in Cloudberry 2.1.0 queue mode)",
		"count", len(enableEvents))

	// Step 4: Disable workload.
	s.logger.Info("disabling workload for metrics test")
	metricsRecorder.resetUsageEvents()

	updated, err := env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	updated.Spec.Workload.Enabled = false
	updated.Generation = 2
	err = env.Client.Update(ctx, updated)
	require.NoError(s.T(), err, "should update cluster to disable workload")

	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "disable reconciliation should succeed")

	// Step 5: Verify SetResourceGroupUsage was called with (0, 0) for each dropped group.
	disableEvents := metricsRecorder.getUsageEvents()
	s.logger.Info("metrics events after disable", "count", len(disableEvents))

	analyticsZeroed := false
	etlZeroed := false
	for _, e := range disableEvents {
		if e.Group == analyticsName && e.CPU == 0 && e.Memory == 0 {
			analyticsZeroed = true
		}
		if e.Group == etlName && e.CPU == 0 && e.Memory == 0 {
			etlZeroed = true
		}
	}
	assert.True(s.T(), analyticsZeroed,
		"SetResourceGroupUsage should be called with (0, 0) for analytics group on disable")
	assert.True(s.T(), etlZeroed,
		"SetResourceGroupUsage should be called with (0, 0) for etl group on disable")

	s.logger.Info("test 34c completed: metrics zeroed on disable",
		"analytics", analyticsName, "etl", etlName,
		"disableEvents", len(disableEvents))
}

// ============================================================================
// Test 34d: Idempotent Disable (Multiple Reconciliations)
// ============================================================================
//
// Steps:
//  1. Create cluster with workload disabled from the start (no prior enable)
//  2. Run reconciliation → verify no errors, WorkloadConfigured=False/WorkloadDisabled
//  3. Run reconciliation again → verify same result, no errors
//  4. This tests that cleanup is safe when there's nothing to clean up

func (s *Scenario34WorkloadDisabledE2ESuite) TestScenario34d_IdempotentDisable() {
	s.logger.Info("starting test 34d: idempotent disable (multiple reconciliations)")

	clusterName := "s34d-e2e-idempotent"
	clusterNamespace := "default"

	// Step 1: Build cluster with workload disabled from the start.
	cluster := testutil.NewClusterBuilder(clusterName, clusterNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: false,
	}

	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, s.logger,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Step 2: First reconciliation — should succeed with no errors.
	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "first disable reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Verify WorkloadConfigured=False/WorkloadDisabled.
	updated, err := env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "False", string(c.Status),
				"WorkloadConfigured should be False")
			assert.Equal(s.T(), "WorkloadDisabled", c.Reason,
				"reason should be WorkloadDisabled")
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")

	// Verify ConfigMap does not exist (was never created).
	cmName := clusterName + "-workload-rules"
	_, cmErr := env.GetConfigMap(ctx, cmName, clusterNamespace)
	assert.True(s.T(), apierrors.IsNotFound(cmErr),
		"workload-rules ConfigMap should not exist (never created)")

	s.logger.Info("first disable reconciliation verified")

	// Step 3: Second reconciliation — should also succeed with no errors.
	// Re-read cluster to get latest version.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	// Bump generation to trigger reconciliation.
	updated.Generation = 2
	err = env.Client.Update(ctx, updated)
	require.NoError(s.T(), err)

	result, err = reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "second disable reconciliation should succeed (idempotent)")
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify same condition.
	updated, err = env.GetCluster(ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound = false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "False", string(c.Status),
				"WorkloadConfigured should still be False after second reconciliation")
			assert.Equal(s.T(), "WorkloadDisabled", c.Reason,
				"reason should still be WorkloadDisabled")
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should still be set")

	// Verify ConfigMap still does not exist.
	_, cmErr = env.GetConfigMap(ctx, cmName, clusterNamespace)
	assert.True(s.T(), apierrors.IsNotFound(cmErr),
		"workload-rules ConfigMap should still not exist after second reconciliation")

	s.logger.Info("test 34d completed: idempotent disable verified — no errors on repeated reconciliation")
}
