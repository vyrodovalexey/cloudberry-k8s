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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 35: API Permission Negative Tests with Real Cloudberry Cluster
// ============================================================================
//
// This scenario tests the API permission model with negative cases:
//   - 35a: Basic role attempts POST to resource-groups → 403 Forbidden
//   - 35b: Operator role attempts DELETE on resource-groups → 403 Forbidden
//   - 35c: Unauthenticated request → 401 Unauthorized
//
// It uses InMemoryCredentialStore with real bcrypt-based BasicAuth,
// an httptest API server with real AuthMiddleware, and a real Cloudberry
// DB connection for verifying resource groups aren't actually deleted.
// ============================================================================

// Scenario35APIPermissionE2ESuite tests API permission enforcement with negative cases.
type Scenario35APIPermissionE2ESuite struct {
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

func TestE2E_Scenario35(t *testing.T) {
	suite.Run(t, new(Scenario35APIPermissionE2ESuite))
}

// SetupSuite initializes the E2E test environment and connects to the real Cloudberry cluster.
func (s *Scenario35APIPermissionE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	s.testSuffix = strconv.FormatInt(time.Now().UnixMilli(), 36)
	s.logger.Info("scenario 35 E2E suite setup", "testSuffix", s.testSuffix)

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

		s.logger.Info("starting kubectl port-forward for scenario 35",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		if err := s.portForwardCmd.Start(); err != nil {
			s.T().Skipf("skipping scenario 35 E2E: kubectl port-forward failed to start: %v", err)
			return
		}

		// Wait for port-forward to become ready.
		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward35()
			s.T().Skipf("skipping scenario 35 E2E: port-forward did not become ready within timeout")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established for scenario 35", "localPort", port)
	}

	// Connect to the real Cloudberry cluster.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	dbClient, err := db.NewClient(ctx, db.Config{
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
	}, s.logger)
	if err != nil {
		s.cleanupPortForward35()
		s.T().Skipf("skipping scenario 35 E2E: cannot connect to Cloudberry cluster: %v", err)
		return
	}
	s.dbClient = dbClient

	// Verify connectivity with a ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward35()
		s.T().Skipf("skipping scenario 35 E2E: ping to Cloudberry cluster failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 35",
		"host", host, "port", port, "user", user, "database", database)
}

// TearDownSuite cleans up the database connection and port-forward.
func (s *Scenario35APIPermissionE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("scenario 35: database connection closed")
	}
	s.cleanupPortForward35()
	s.E2ESuite.TearDownSuite()
}

// cleanupPortForward35 terminates the kubectl port-forward process if running.
func (s *Scenario35APIPermissionE2ESuite) cleanupPortForward35() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		_ = s.portForwardCmd.Process.Kill()
		_ = s.portForwardCmd.Wait()
		s.portForwardCmd = nil
		s.logger.Info("scenario 35: port-forward process terminated")
	}
}

// uniqueGroupName generates a unique resource group name for this test suite.
func (s *Scenario35APIPermissionE2ESuite) uniqueGroupName(base string) string {
	return fmt.Sprintf("s35e2e_%s_%s", base, s.testSuffix)
}

// cleanupResourceGroup drops a resource group, ignoring errors (best-effort cleanup).
func (s *Scenario35APIPermissionE2ESuite) cleanupResourceGroup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()
	if err := s.dbClient.DropResourceGroup(ctx, name); err != nil {
		s.logger.Warn("scenario 35 cleanup: drop resource group", "name", name, "error", err)
	} else {
		s.logger.Info("scenario 35 cleanup: dropped resource group", "name", name)
	}
}

// newAuthenticatedAPIServer creates an API server with real BasicAuth middleware.
// It sets up three users at different permission levels:
//   - viewer (PermissionBasic) — can only read
//   - operator (PermissionOperator) — can create/update but not delete resource groups
//   - admin (PermissionAdmin) — full access
func (s *Scenario35APIPermissionE2ESuite) newAuthenticatedAPIServer(
	cluster *cbv1alpha1.CloudberryCluster,
) (*api.Server, http.Handler, *testutil.TestK8sEnv) {
	// Create credential store with 3 users at different permission levels.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	factory := &realDBClientFactory{client: s.dbClient}
	env := testutil.NewTestK8sEnv(cluster)

	apiServer := api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, s.logger, 0)
	return apiServer, apiServer.Handler(), env
}

// ============================================================================
// Test 35a: Basic Role Attempts POST to Resource-Groups → 403
// ============================================================================

func (s *Scenario35APIPermissionE2ESuite) TestScenario35a_BasicRole_POST_ResourceGroups_Forbidden() {
	s.logger.Info("test 35a: basic role attempts POST to resource-groups → 403")

	groupName := s.uniqueGroupName("perm_a")

	// Cleanup any resource groups created during this test.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(groupName)
	})

	// Create a cluster with workload enabled.
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	apiServer, handler, _ := s.newAuthenticatedAPIServer(cluster)
	defer apiServer.Close()

	createBody, err := json.Marshal(map[string]interface{}{
		"name":          groupName,
		"concurrency":   10,
		"cpuMaxPercent": 50,
		"cpuWeight":     100,
	})
	require.NoError(s.T(), err)

	apiPath := "/api/v1alpha1/clusters/test-cluster/workload/resource-groups?namespace=default"

	// --- Viewer (Basic) should be DENIED (403) ---
	s.Run("viewer_denied_403", func() {
		req := httptest.NewRequest(http.MethodPost, apiPath, bytes.NewReader(createBody))
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("viewer", "viewerpass")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusForbidden, rec.Code,
			"viewer (Basic) should be denied POST to resource-groups")

		// Verify error response body.
		var errResp map[string]interface{}
		require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
		errObj, ok := errResp["error"].(map[string]interface{})
		require.True(s.T(), ok, "response should have 'error' object")
		assert.Equal(s.T(), "FORBIDDEN", errObj["code"])
		assert.Equal(s.T(), "insufficient permissions: requires Operator", errObj["message"])

		// Verify security headers are present on 403 responses.
		assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"),
			"X-Content-Type-Options should be set on 403 response")
		assert.Equal(s.T(), "DENY", rec.Header().Get("X-Frame-Options"),
			"X-Frame-Options should be set on 403 response")
		assert.Equal(s.T(), "no-store", rec.Header().Get("Cache-Control"),
			"Cache-Control should be set on 403 response")
	})

	// --- Operator should SUCCEED (201) ---
	s.Run("operator_allowed_201", func() {
		req := httptest.NewRequest(http.MethodPost, apiPath, bytes.NewReader(createBody))
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("operator", "operatorpass")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusCreated, rec.Code,
			"operator should be allowed POST to resource-groups")
	})

	// --- Admin should SUCCEED (201 or 500 if group already exists) ---
	s.Run("admin_allowed", func() {
		// Use a different group name to avoid conflict with the operator-created one.
		adminGroupName := s.uniqueGroupName("perm_a_admin")
		s.T().Cleanup(func() {
			s.cleanupResourceGroup(adminGroupName)
		})

		adminBody, marshalErr := json.Marshal(map[string]interface{}{
			"name":          adminGroupName,
			"concurrency":   10,
			"cpuMaxPercent": 50,
			"cpuWeight":     100,
		})
		require.NoError(s.T(), marshalErr)

		req := httptest.NewRequest(http.MethodPost, apiPath, bytes.NewReader(adminBody))
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("admin", "adminpass")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusCreated, rec.Code,
			"admin should be allowed POST to resource-groups")
	})

	s.logger.Info("test 35a completed: basic role POST permission verified")
}

// ============================================================================
// Test 35b: Operator Role Attempts DELETE on Resource-Groups → 403
// ============================================================================

func (s *Scenario35APIPermissionE2ESuite) TestScenario35b_OperatorRole_DELETE_ResourceGroups_Forbidden() {
	s.logger.Info("test 35b: operator role attempts DELETE on resource-groups → 403")

	groupName := s.uniqueGroupName("perm_b")

	// Cleanup: always try to drop the resource group.
	s.T().Cleanup(func() {
		s.cleanupResourceGroup(groupName)
	})

	// Create resource group in real DB.
	ctx, cancel := context.WithTimeout(context.Background(), dbOperationTimeout)
	defer cancel()

	err := s.dbClient.CreateResourceGroup(ctx, db.ResourceGroupOptions{
		Name:          groupName,
		Concurrency:   10,
		CPUMaxPercent: 50,
		CPUWeight:     100,
	})
	require.NoError(s.T(), err, "failed to create resource group in real DB")

	// Create a cluster with workload enabled.
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	apiServer, handler, _ := s.newAuthenticatedAPIServer(cluster)
	defer apiServer.Close()

	deletePath := fmt.Sprintf(
		"/api/v1alpha1/clusters/test-cluster/workload/resource-groups/%s?namespace=default",
		groupName,
	)

	// --- Operator should be DENIED (403) ---
	s.Run("operator_denied_403", func() {
		req := httptest.NewRequest(http.MethodDelete, deletePath, nil)
		req.SetBasicAuth("operator", "operatorpass")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusForbidden, rec.Code,
			"operator should be denied DELETE on resource-groups (requires Admin)")

		// Verify error response body.
		var errResp map[string]interface{}
		require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
		errObj, ok := errResp["error"].(map[string]interface{})
		require.True(s.T(), ok, "response should have 'error' object")
		assert.Equal(s.T(), "FORBIDDEN", errObj["code"])
		assert.Equal(s.T(), "insufficient permissions: requires Admin", errObj["message"])

		// Verify security headers are present on 403 responses.
		assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"),
			"X-Content-Type-Options should be set on 403 response")
	})

	// --- Verify resource group STILL EXISTS in real DB ---
	s.Run("resource_group_still_exists_after_denied_delete", func() {
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
		defer verifyCancel()

		groups, listErr := s.dbClient.ListResourceGroups(verifyCtx)
		require.NoError(s.T(), listErr, "failed to list resource groups")

		found := false
		for _, g := range groups {
			if g.Name == groupName {
				found = true
				break
			}
		}
		assert.True(s.T(), found,
			"resource group %q should still exist after denied DELETE", groupName)
	})

	// --- Admin should SUCCEED (200) ---
	s.Run("admin_allowed_200", func() {
		req := httptest.NewRequest(http.MethodDelete, deletePath, nil)
		req.SetBasicAuth("admin", "adminpass")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"admin should be allowed DELETE on resource-groups")

		var resp map[string]interface{}
		require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(s.T(), "deleted", resp["status"])
		assert.Equal(s.T(), groupName, resp["group"])
	})

	// --- Verify resource group NOW DELETED from real DB ---
	s.Run("resource_group_deleted_after_admin_delete", func() {
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), dbOperationTimeout)
		defer verifyCancel()

		groups, listErr := s.dbClient.ListResourceGroups(verifyCtx)
		require.NoError(s.T(), listErr, "failed to list resource groups")

		found := false
		for _, g := range groups {
			if g.Name == groupName {
				found = true
				break
			}
		}
		assert.False(s.T(), found,
			"resource group %q should be deleted after admin DELETE", groupName)
	})

	s.logger.Info("test 35b completed: operator DELETE permission verified")
}

// ============================================================================
// Test 35c: Unauthenticated Request → 401
// ============================================================================

func (s *Scenario35APIPermissionE2ESuite) TestScenario35c_Unauthenticated_Request_Unauthorized() {
	s.logger.Info("test 35c: unauthenticated request → 401")

	// Create a cluster with workload enabled.
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}

	apiServer, handler, _ := s.newAuthenticatedAPIServer(cluster)
	defer apiServer.Close()

	// --- Test unauthenticated requests to multiple endpoints ---
	unauthEndpoints := []struct {
		name   string
		method string
		path   string
	}{
		{
			"GET_workload",
			http.MethodGet,
			"/api/v1alpha1/clusters/test-cluster/workload?namespace=default",
		},
		{
			"POST_resource_groups",
			http.MethodPost,
			"/api/v1alpha1/clusters/test-cluster/workload/resource-groups?namespace=default",
		},
		{
			"DELETE_resource_group",
			http.MethodDelete,
			"/api/v1alpha1/clusters/test-cluster/workload/resource-groups/test_group?namespace=default",
		},
	}

	for _, ep := range unauthEndpoints {
		s.Run("no_auth_"+ep.name+"_401", func() {
			// Request WITHOUT any Authorization header.
			req := httptest.NewRequest(ep.method, ep.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
				"unauthenticated %s %s should return 401", ep.method, ep.path)

			// Verify error response body.
			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
			assert.Equal(s.T(), "missing Authorization header", errObj["message"])

			// Verify security headers are present on 401 responses.
			assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"),
				"X-Content-Type-Options should be set on 401 response")
			assert.Equal(s.T(), "DENY", rec.Header().Get("X-Frame-Options"),
				"X-Frame-Options should be set on 401 response")
			assert.Equal(s.T(), "no-store", rec.Header().Get("Cache-Control"),
				"Cache-Control should be set on 401 response")
		})
	}

	// --- Health endpoints should work WITHOUT auth (200 OK) ---
	healthEndpoints := []struct {
		name string
		path string
	}{
		{"healthz", "/healthz"},
		{"readyz", "/readyz"},
	}

	for _, ep := range healthEndpoints {
		s.Run("health_no_auth_"+ep.name+"_200", func() {
			req := httptest.NewRequest(http.MethodGet, ep.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), http.StatusOK, rec.Code,
				"%s should work without auth", ep.path)

			// Verify security headers are present even on health endpoints.
			assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"),
				"X-Content-Type-Options should be set on health endpoint")
		})
	}

	// --- Wrong credentials should return 401 ---
	s.Run("wrong_password_401", func() {
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1alpha1/clusters/test-cluster/workload?namespace=default", nil)
		req.SetBasicAuth("viewer", "wrongpassword")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"wrong password should return 401")

		var errResp map[string]interface{}
		require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
		errObj, ok := errResp["error"].(map[string]interface{})
		require.True(s.T(), ok, "response should have 'error' object")
		assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
		assert.Equal(s.T(), "authentication failed", errObj["message"])
	})

	// --- Unknown user should return 401 ---
	s.Run("unknown_user_401", func() {
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1alpha1/clusters/test-cluster/workload?namespace=default", nil)
		req.SetBasicAuth("nonexistent", "somepassword")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unknown user should return 401")

		var errResp map[string]interface{}
		require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
		errObj, ok := errResp["error"].(map[string]interface{})
		require.True(s.T(), ok, "response should have 'error' object")
		assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
		assert.Equal(s.T(), "authentication failed", errObj["message"])
	})

	s.logger.Info("test 35c completed: unauthenticated request handling verified")
}
