//go:build e2e

package e2e

import (
	"context"
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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/idle"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Scenario 33: Idle Session Rules with Real Cloudberry Cluster
// ============================================================================
//
// Validates the idle session enforcement daemon against a real Cloudberry cluster.
// Tests use SHORT timeouts (10s) instead of 30m for practical E2E testing.
// The daemon's scan interval is set to 2s for tests.
//
// Sub-tests:
//   - 33a: Idle session terminated after timeout
//   - 33b: In-transaction session excluded, then terminated after COMMIT
//   - 33c: Disabled rule does not terminate idle sessions
// ============================================================================

// Scenario33IdleSessionE2ESuite tests idle session enforcement against a real Cloudberry cluster.
type Scenario33IdleSessionE2ESuite struct {
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

func TestE2E_Scenario33(t *testing.T) {
	suite.Run(t, new(Scenario33IdleSessionE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario33IdleSessionE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 33 E2E suite setup", "testSuffix", s.testSuffix)

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
			s.T().Skipf("skipping scenario 33 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward33()
			s.T().Skipf("skipping scenario 33 E2E: port-forward did not become ready within timeout")
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
		s.cleanupPortForward33()
		s.T().Skipf("skipping scenario 33 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward33()
		s.T().Skipf("skipping scenario 33 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	// Store connection parameters for creating test role connections.
	s.connHost = host
	s.connPort = port
	s.connDatabase = database
	s.connPassword = password

	s.logger.Info("connected to real Cloudberry cluster for scenario 33",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario33IdleSessionE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward33()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward33 terminates the kubectl port-forward process if running.
func (s *Scenario33IdleSessionE2ESuite) cleanupPortForward33() {
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
func (s *Scenario33IdleSessionE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s33e2e_%s_%s", base, s.testSuffix)
}

// uniqueRoleName returns a unique role name for test isolation.
func (s *Scenario33IdleSessionE2ESuite) uniqueRoleName(base string) string {
	return fmt.Sprintf("s33e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario33IdleSessionE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("cleanup: failed to drop resource group (may not exist)", "name", name, "error", err)
	} else {
		s.logger.Info("cleanup: dropped resource group", "name", name)
	}
}

// cleanupRole drops a role, ignoring errors (best-effort cleanup).
func (s *Scenario33IdleSessionE2ESuite) cleanupRole(name string) {
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
func (s *Scenario33IdleSessionE2ESuite) openTestRoleConnection(
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
func (s *Scenario33IdleSessionE2ESuite) waitForSessionTermination(
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
func (s *Scenario33IdleSessionE2ESuite) waitForSessionSurvival(
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
// Tracking metrics recorder for E2E tests
// ============================================================================

// scenario33MetricsRecorder wraps NoopRecorder and records idle termination calls.
type scenario33MetricsRecorder struct {
	metrics.NoopRecorder
	mu               sync.Mutex
	idleTerminations []scenario33TerminationEvent
}

// scenario33TerminationEvent records a single idle session termination event.
type scenario33TerminationEvent struct {
	Cluster   string
	Namespace string
	Rule      string
}

// RecordIdleSessionTermination records an idle session termination event.
func (r *scenario33MetricsRecorder) RecordIdleSessionTermination(cluster, namespace, rule string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idleTerminations = append(r.idleTerminations, scenario33TerminationEvent{
		Cluster:   cluster,
		Namespace: namespace,
		Rule:      rule,
	})
}

// getTerminations returns a copy of all recorded termination events.
func (r *scenario33MetricsRecorder) getTerminations() []scenario33TerminationEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]scenario33TerminationEvent, len(r.idleTerminations))
	copy(result, r.idleTerminations)
	return result
}

// ============================================================================
// Test 33a: Idle Session Terminated After Timeout
// ============================================================================
//
// Precondition: Idle rule with 10s timeout, excludeInTransaction=true, custom message.
//
// Steps:
//  1. Create resource group in real DB
//  2. Create test role with LOGIN + password, assign to resource group
//  3. Create idle daemon with ScanInterval=2s, IdleTimeout=10s
//  4. Open a NEW database connection as the test role
//  5. Execute a simple query to establish the session, then leave idle
//  6. Wait for the daemon to terminate the session (~15s)
//  7. Verify: test connection is no longer valid
//  8. Verify: RecordIdleSessionTermination was called with correct rule name
//  9. Stop daemon, cleanup

func (s *Scenario33IdleSessionE2ESuite) TestScenario33a_IdleSessionTerminated() {
	s.logger.Info("starting test 33a: idle session terminated after timeout")

	groupName := s.uniqueGroupName("idle_a")
	roleName := s.uniqueRoleName("role_a")
	rolePassword := "s33e2e_testpass_a"
	ruleName := "terminate-idle-33a"

	// Register cleanup: role must be dropped before resource group.
	s.T().Cleanup(func() {
		s.cleanupRole(roleName)
		s.cleanupResourceGroup(groupName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Step 1: Create resource group in real DB.
	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          groupName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "should create resource group")
	s.logger.Info("created resource group", "name", groupName)

	// Step 2: Create test role with LOGIN + password, assign to resource group.
	err = s.dbClient.CreateRole(ctx, db.RoleOptions{
		Name:     roleName,
		Login:    true,
		Password: rolePassword,
	})
	require.NoError(s.T(), err, "should create test role")
	s.logger.Info("created test role", "name", roleName)

	err = s.dbClient.AssignRoleResourceGroup(ctx, roleName, groupName)
	require.NoError(s.T(), err, "should assign role to resource group")
	s.logger.Info("assigned role to resource group", "role", roleName, "group", groupName)

	// Step 3: Parse idle rules and create the daemon.
	crdRules := []cbv1alpha1.IdleSessionRule{
		{
			Name:                 ruleName,
			Enabled:              true,
			ResourceGroup:        groupName,
			IdleTimeout:          "10s",
			ExcludeInTransaction: true,
			TerminateMessage:     "Session terminated due to inactivity",
		},
	}

	parsedRules, err := idle.ParseIdleRules(crdRules)
	require.NoError(s.T(), err, "should parse idle rules")

	recorder := &scenario33MetricsRecorder{}
	daemon := idle.New(idle.Config{
		ClusterName:  "s33a-e2e-cluster",
		Namespace:    "default",
		ScanInterval: 2 * time.Second,
		DBClient:     s.dbClient,
		Metrics:      recorder,
		Logger:       s.logger,
	})
	daemon.UpdateRules(parsedRules)
	daemon.Start(ctx)
	defer daemon.Stop()

	s.logger.Info("idle daemon started", "scanInterval", "2s", "idleTimeout", "10s")

	// Step 4: Open a NEW database connection as the test role.
	testConn, pid, err := s.openTestRoleConnection(ctx, roleName, rolePassword)
	require.NoError(s.T(), err, "should open test role connection")
	s.logger.Info("opened test role connection", "role", roleName, "pid", pid)

	// Step 5: Execute a simple query to establish the session, then leave idle.
	var result int
	err = testConn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(s.T(), err, "should execute initial query")
	assert.Equal(s.T(), 1, result, "initial query should return 1")
	s.logger.Info("initial query executed, leaving session idle", "pid", pid)

	// Step 6: Wait for the daemon to terminate the session.
	// We check pg_stat_activity from the admin connection (NOT pinging the test conn,
	// which would reset query_start and the idle timer).
	// Timeout: 10s idle + 2s scan interval + 3s buffer = 15s.
	// Use generous timeout of 30s to avoid flakiness.
	terminated := s.waitForSessionTermination(pid, 30*time.Second)

	// Step 7: Verify the session was terminated.
	assert.True(s.T(), terminated,
		"idle session should be terminated after timeout (10s idle + 2s scan)")

	// Step 8: Verify the test connection is no longer valid.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	pingErr := testConn.Ping(pingCtx)
	pingCancel()
	if terminated {
		assert.Error(s.T(), pingErr, "test connection should fail after session termination")
	}

	// Step 9: Verify RecordIdleSessionTermination was called.
	terms := recorder.getTerminations()
	require.NotEmpty(s.T(), terms, "should have at least one termination event recorded")

	// Find the termination event for our rule.
	foundTermination := false
	for _, t := range terms {
		if t.Rule == ruleName {
			foundTermination = true
			assert.Equal(s.T(), "s33a-e2e-cluster", t.Cluster, "cluster name should match")
			assert.Equal(s.T(), "default", t.Namespace, "namespace should match")
			break
		}
	}
	assert.True(s.T(), foundTermination,
		"RecordIdleSessionTermination should be called with rule %q", ruleName)

	// Close the test connection (may already be closed by termination).
	testConn.Close(ctx)

	s.logger.Info("test 33a completed: idle session terminated after timeout",
		"pid", pid, "terminationEvents", len(terms))
}

// ============================================================================
// Test 33b: In-Transaction Session Excluded, Then Terminated After COMMIT
// ============================================================================
//
// Steps:
//  1. Create resource group and test role (same as 33a)
//  2. Create idle rule with excludeInTransaction=true, idleTimeout=10s
//  3. Start idle daemon with ScanInterval=2s
//  4. Open test connection as test role
//  5. Execute BEGIN to start a transaction
//  6. Leave idle for >12s (should NOT be terminated — in transaction)
//  7. Execute COMMIT to end the transaction
//  8. Leave idle for >12s (should now be terminated — state is "idle")
//  9. Verify: metric recorded exactly once (for the termination after COMMIT)

func (s *Scenario33IdleSessionE2ESuite) TestScenario33b_InTransactionExcluded_ThenTerminated() {
	s.logger.Info("starting test 33b: in-transaction session excluded, then terminated after COMMIT")

	groupName := s.uniqueGroupName("idle_b")
	roleName := s.uniqueRoleName("role_b")
	rolePassword := "s33e2e_testpass_b"
	ruleName := "terminate-idle-33b"

	// Register cleanup: role must be dropped before resource group.
	s.T().Cleanup(func() {
		s.cleanupRole(roleName)
		s.cleanupResourceGroup(groupName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Step 1: Create resource group and test role.
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
			TerminateMessage:     "Session terminated due to inactivity (33b)",
		},
	}

	parsedRules, err := idle.ParseIdleRules(crdRules)
	require.NoError(s.T(), err, "should parse idle rules")

	recorder := &scenario33MetricsRecorder{}
	daemon := idle.New(idle.Config{
		ClusterName:  "s33b-e2e-cluster",
		Namespace:    "default",
		ScanInterval: 2 * time.Second,
		DBClient:     s.dbClient,
		Metrics:      recorder,
		Logger:       s.logger,
	})
	daemon.UpdateRules(parsedRules)
	daemon.Start(ctx)
	defer daemon.Stop()

	s.logger.Info("idle daemon started for test 33b")

	// Step 3: Open test connection as test role.
	testConn, pid, err := s.openTestRoleConnection(ctx, roleName, rolePassword)
	require.NoError(s.T(), err, "should open test role connection")
	s.logger.Info("opened test role connection", "role", roleName, "pid", pid)

	// Step 4: Execute BEGIN to start a transaction.
	_, err = testConn.Exec(ctx, "BEGIN")
	require.NoError(s.T(), err, "should execute BEGIN")
	s.logger.Info("transaction started (BEGIN), leaving session idle in transaction", "pid", pid)

	// Step 5: Leave idle for >12s — session should NOT be terminated (in transaction).
	// Wait 15s to be safe (10s timeout + 2s scan + 3s buffer).
	// We check from the admin connection to avoid resetting the idle timer.
	survived := s.waitForSessionSurvival(pid, 15*time.Second)
	assert.True(s.T(), survived,
		"in-transaction session should NOT be terminated when excludeInTransaction=true")
	s.logger.Info("in-transaction session survived as expected", "pid", pid)

	// Verify no termination events yet.
	termsBeforeCommit := recorder.getTerminations()
	terminationsForRule := 0
	for _, t := range termsBeforeCommit {
		if t.Rule == ruleName {
			terminationsForRule++
		}
	}
	assert.Equal(s.T(), 0, terminationsForRule,
		"no termination events should be recorded while session is in transaction")

	// Step 6: Execute COMMIT to end the transaction.
	// This changes the session state from "idle in transaction" to "idle".
	_, err = testConn.Exec(ctx, "COMMIT")
	require.NoError(s.T(), err, "should execute COMMIT")
	s.logger.Info("transaction committed (COMMIT), session now idle", "pid", pid)

	// Step 7: Leave idle for >12s — session should now be terminated.
	// We check from the admin connection to avoid resetting the idle timer.
	terminated := s.waitForSessionTermination(pid, 30*time.Second)
	assert.True(s.T(), terminated,
		"session should be terminated after COMMIT (state changed to idle)")

	// Step 8: Verify metric recorded exactly once for the termination after COMMIT.
	termsAfterCommit := recorder.getTerminations()
	terminationsForRule = 0
	for _, t := range termsAfterCommit {
		if t.Rule == ruleName {
			terminationsForRule++
			assert.Equal(s.T(), "s33b-e2e-cluster", t.Cluster)
			assert.Equal(s.T(), "default", t.Namespace)
		}
	}
	assert.Equal(s.T(), 1, terminationsForRule,
		"exactly one termination event should be recorded (after COMMIT)")

	// Close the test connection (may already be closed by termination).
	testConn.Close(ctx)

	s.logger.Info("test 33b completed: in-transaction excluded, terminated after COMMIT",
		"pid", pid, "terminationEvents", terminationsForRule)
}

// ============================================================================
// Test 33c: Disabled Rule Does Not Terminate Idle Sessions
// ============================================================================
//
// Steps:
//  1. Create resource group and test role
//  2. Create idle rule with enabled=false, idleTimeout=10s
//  3. Start idle daemon with ScanInterval=2s
//  4. Open test connection as test role
//  5. Leave idle for >15s (well beyond timeout)
//  6. Verify: session is NOT terminated (rule is disabled)
//  7. Verify: no metrics recorded

func (s *Scenario33IdleSessionE2ESuite) TestScenario33c_DisabledRuleNoTermination() {
	s.logger.Info("starting test 33c: disabled rule does not terminate idle sessions")

	groupName := s.uniqueGroupName("idle_c")
	roleName := s.uniqueRoleName("role_c")
	rolePassword := "s33e2e_testpass_c"
	ruleName := "terminate-idle-33c-disabled"

	// Register cleanup: role must be dropped before resource group.
	s.T().Cleanup(func() {
		s.cleanupRole(roleName)
		s.cleanupResourceGroup(groupName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Step 1: Create resource group and test role.
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

	// Step 2: Parse idle rules with enabled=false.
	crdRules := []cbv1alpha1.IdleSessionRule{
		{
			Name:                 ruleName,
			Enabled:              false, // DISABLED
			ResourceGroup:        groupName,
			IdleTimeout:          "10s",
			ExcludeInTransaction: true,
			TerminateMessage:     "This should never be logged",
		},
	}

	parsedRules, err := idle.ParseIdleRules(crdRules)
	require.NoError(s.T(), err, "should parse idle rules")

	recorder := &scenario33MetricsRecorder{}
	daemon := idle.New(idle.Config{
		ClusterName:  "s33c-e2e-cluster",
		Namespace:    "default",
		ScanInterval: 2 * time.Second,
		DBClient:     s.dbClient,
		Metrics:      recorder,
		Logger:       s.logger,
	})
	daemon.UpdateRules(parsedRules)
	daemon.Start(ctx)
	defer daemon.Stop()

	s.logger.Info("idle daemon started with disabled rule for test 33c")

	// Step 3: Open test connection as test role.
	testConn, pid, err := s.openTestRoleConnection(ctx, roleName, rolePassword)
	require.NoError(s.T(), err, "should open test role connection")
	defer testConn.Close(ctx)
	s.logger.Info("opened test role connection", "role", roleName, "pid", pid)

	// Step 4: Execute a simple query to establish the session, then leave idle.
	var result int
	err = testConn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(s.T(), err, "should execute initial query")
	s.logger.Info("initial query executed, leaving session idle with disabled rule", "pid", pid)

	// Step 5: Leave idle for >15s (well beyond the 10s timeout).
	// The session should NOT be terminated because the rule is disabled.
	// We check from the admin connection to avoid resetting the idle timer.
	survived := s.waitForSessionSurvival(pid, 18*time.Second)
	assert.True(s.T(), survived,
		"session should NOT be terminated when rule is disabled")

	// Step 6: Verify the session is still alive by executing a query.
	err = testConn.QueryRow(ctx, "SELECT 1").Scan(&result)
	assert.NoError(s.T(), err, "session should still be alive after idle period with disabled rule")
	assert.Equal(s.T(), 1, result, "query should return 1")

	// Step 7: Verify no metrics recorded.
	terms := recorder.getTerminations()
	terminationsForRule := 0
	for _, t := range terms {
		if t.Rule == ruleName {
			terminationsForRule++
		}
	}
	assert.Equal(s.T(), 0, terminationsForRule,
		"no termination events should be recorded for disabled rule")

	s.logger.Info("test 33c completed: disabled rule did not terminate idle session",
		"pid", pid, "terminationEvents", terminationsForRule)
}
