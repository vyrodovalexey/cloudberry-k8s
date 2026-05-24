//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 43: Full Permission Matrix Verification (E2E)
// ============================================================================
//
// End-to-end tests for the full API permission matrix, verifying the complete
// request lifecycle through the API server with all five permission levels.
// ============================================================================

// permissionLevelMap maps permission level strings to auth.PermissionLevel values.
var permissionLevelMap = map[string]auth.PermissionLevel{
	"Basic":         auth.PermissionBasic,
	"OperatorBasic": auth.PermissionOperatorBasic,
	"Operator":      auth.PermissionOperator,
	"Admin":         auth.PermissionAdmin,
}

// Scenario43PermissionMatrixE2ESuite tests the full API permission matrix end-to-end.
type Scenario43PermissionMatrixE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario43(t *testing.T) {
	suite.Run(t, new(Scenario43PermissionMatrixE2ESuite))
}

// scenario43E2EUser holds credentials and permission level for a test user.
type scenario43E2EUser struct {
	username   string
	password   string
	permission auth.PermissionLevel
}

// scenario43E2EUsers returns the five test users at different permission levels.
func scenario43E2EUsers() []scenario43E2EUser {
	return []scenario43E2EUser{
		{username: "admin-user", password: "admin-pass", permission: auth.PermissionAdmin},
		{username: "operator-user", password: "operator-pass", permission: auth.PermissionOperator},
		{username: "opbasic-user", password: "opbasic-pass", permission: auth.PermissionOperatorBasic},
		{username: "basic-user", password: "basic-pass", permission: auth.PermissionBasic},
		{username: "selfonly-user", password: "selfonly-pass", permission: auth.PermissionSelfOnly},
	}
}

// newPermissionMatrixServer creates an API server with all five users configured.
func (s *Scenario43PermissionMatrixE2ESuite) newPermissionMatrixServer() (*api.Server, http.Handler) {
	store := auth.NewInMemoryCredentialStore()
	for _, u := range scenario43E2EUsers() {
		store.SetCredentials(u.username, u.password, u.permission)
	}

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	cluster := testutil.NewClusterBuilder("test-cluster", s.namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	// Use a high rate limit to avoid rate limiting during permission matrix testing,
	// which sends many requests in rapid succession across all permission levels.
	const highRateLimit = 1000
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	return server, server.Handler()
}

// doE2ERequest creates and executes an HTTP request with the given credentials.
func doE2ERequest(handler http.Handler, method, path, username, password string) *httptest.ResponseRecorder {
	var body *bytes.Reader
	if method == http.MethodPost || method == http.MethodPut {
		body = bytes.NewReader([]byte("{}"))
	}

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, body)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}

	if username != "" {
		req.SetBasicAuth(username, password)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// --- 43a: Admin user can access all operations ---

// TestE2E_Scenario43a_Admin_AllOperationsSucceed verifies that an Admin user
// can access all endpoints without receiving 401 or 403.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43a_Admin_AllOperationsSucceed() {
	s.logger.Info("starting scenario 43a: admin all operations succeed")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	endpoints := []struct {
		method string
		path   string
	}{
		// Basic
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/segments"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/mirroring"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/standby"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/rebalance/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/queries"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/queries/active"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/backups"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/storage/pvcs"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/storage/disk-usage"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload/resource-groups"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload/rules"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload/resource-queues"},
		// OperatorBasic
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/sessions"},
		// Operator
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/stop"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/restart"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/maintenance/vacuum"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/maintenance/analyze"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/rebalance"},
		// Admin
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/standby/activate"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster/backups/backup-1"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/backups/backup-1/restore"},
	}

	for _, ep := range endpoints {
		s.Run(ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "admin-user", "admin-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"admin should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"admin should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	s.logger.Info("scenario 43a completed")
}

// --- 43b: Operator user — allowed and denied operations ---

// TestE2E_Scenario43b_Operator_AllowedAndDenied verifies that an Operator user
// can access operator-level endpoints but is denied admin-only endpoints.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43b_Operator_AllowedAndDenied() {
	s.logger.Info("starting scenario 43b: operator allowed and denied")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	// Operator-allowed endpoints.
	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/sessions"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/stop"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/restart"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/maintenance/vacuum"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/maintenance/analyze"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/rebalance"},
	}

	for _, ep := range allowed {
		s.Run("allowed_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "operator-user", "operator-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"operator should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"operator should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	// Admin-only endpoints should be denied.
	denied := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1alpha1/clusters"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/standby/activate"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster/backups/backup-1"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/backups/backup-1/restore"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster/workload/resource-groups/test_group"},
	}

	for _, ep := range denied {
		s.Run("denied_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "operator-user", "operator-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"operator should get 403 for admin-only %s %s", ep.method, ep.path)

			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), "FORBIDDEN", errObj["code"])
			assert.Contains(s.T(), errObj["message"], "insufficient permissions")
		})
	}

	s.logger.Info("scenario 43b completed")
}

// --- 43c: OperatorBasic user — allowed and denied operations ---

// TestE2E_Scenario43c_OperatorBasic_AllowedAndDenied verifies that an OperatorBasic
// user can view config/sessions but is denied operator operations.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43c_OperatorBasic_AllowedAndDenied() {
	s.logger.Info("starting scenario 43c: operator basic allowed and denied")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/segments"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/sessions"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload"},
	}

	for _, ep := range allowed {
		s.Run("allowed_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "opbasic-user", "opbasic-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"opbasic should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"opbasic should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	denied := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/stop"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/restart"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/maintenance/vacuum"},
		{http.MethodPost, "/api/v1alpha1/clusters"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
	}

	for _, ep := range denied {
		s.Run("denied_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "opbasic-user", "opbasic-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"opbasic should get 403 for %s %s", ep.method, ep.path)
		})
	}

	s.logger.Info("scenario 43c completed")
}

// --- 43d: Basic user — allowed and denied operations ---

// TestE2E_Scenario43d_Basic_AllowedAndDenied verifies that a Basic user can
// view cluster state but is denied config/sessions and operator operations.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43d_Basic_AllowedAndDenied() {
	s.logger.Info("starting scenario 43d: basic allowed and denied")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/segments"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/mirroring"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/standby"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/queries"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/backups"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/storage/pvcs"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/workload/resource-groups"},
	}

	for _, ep := range allowed {
		s.Run("allowed_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "basic-user", "basic-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"basic should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"basic should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	denied := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/sessions"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/stop"},
		{http.MethodPost, "/api/v1alpha1/clusters"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
	}

	for _, ep := range denied {
		s.Run("denied_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "basic-user", "basic-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"basic should get 403 for %s %s", ep.method, ep.path)
		})
	}

	s.logger.Info("scenario 43d completed")
}

// --- 43e: SelfOnly user — only health endpoints, everything else denied ---

// TestE2E_Scenario43e_SelfOnly_AllowedAndDenied verifies that a SelfOnly user
// can only access health endpoints; all API endpoints return 403.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43e_SelfOnly_AllowedAndDenied() {
	s.logger.Info("starting scenario 43e: selfonly allowed and denied")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	// Health endpoints work without auth.
	healthEndpoints := []string{"/healthz", "/readyz"}
	for _, path := range healthEndpoints {
		s.Run("health_"+path, func() {
			rec := doE2ERequest(handler, http.MethodGet, path, "selfonly-user", "selfonly-pass")
			assert.Equal(s.T(), http.StatusOK, rec.Code,
				"health endpoint %s should return 200", path)
		})
	}

	// All API endpoints should be denied.
	denied := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/sessions"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
	}

	for _, ep := range denied {
		s.Run("denied_"+ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "selfonly-user", "selfonly-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"selfonly should get 403 for %s %s", ep.method, ep.path)
		})
	}

	s.logger.Info("scenario 43e completed")
}

// --- Full Permission Matrix Cases catalog ---

// TestE2E_Scenario43_PermissionMatrixCases runs the full PermissionMatrixCases
// catalog, verifying each endpoint against all five permission levels.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43_PermissionMatrixCases() {
	s.logger.Info("starting scenario 43: permission matrix cases catalog")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	testCases := cases.PermissionMatrixCases()
	require.NotEmpty(s.T(), testCases, "PermissionMatrixCases should return test cases")

	users := scenario43E2EUsers()

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			requiredLevel, ok := permissionLevelMap[tc.RequiredLevel]
			require.True(s.T(), ok,
				"unknown required level %q in test case %s", tc.RequiredLevel, tc.Name)

			for _, u := range users {
				s.Run(u.username, func() {
					rec := doE2ERequest(handler, tc.Method, tc.Path, u.username, u.password)

					if u.permission >= requiredLevel {
						assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
							"user %s (perm=%s) should be allowed %s %s (requires %s)",
							u.username, u.permission.String(), tc.Method, tc.Path, tc.RequiredLevel)
						assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
							"user %s should not get 401 for %s %s",
							u.username, tc.Method, tc.Path)
					} else {
						assert.Equal(s.T(), http.StatusForbidden, rec.Code,
							"user %s (perm=%s) should be denied %s %s (requires %s)",
							u.username, u.permission.String(), tc.Method, tc.Path, tc.RequiredLevel)
					}
				})
			}
		})
	}

	s.logger.Info("scenario 43: permission matrix cases catalog completed")
}

// TestE2E_Scenario43_UnauthenticatedDenied verifies that unauthenticated
// requests to all API endpoints return 401.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43_UnauthenticatedDenied() {
	s.logger.Info("starting scenario 43: unauthenticated denied")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/start"},
		{http.MethodPost, "/api/v1alpha1/clusters"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
	}

	for _, ep := range endpoints {
		s.Run(ep.method+"_"+ep.path, func() {
			rec := doE2ERequest(handler, ep.method, ep.path, "", "")
			assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
				"unauthenticated request should get 401 for %s %s", ep.method, ep.path)

			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
		})
	}

	s.logger.Info("scenario 43: unauthenticated denied completed")
}

// TestE2E_Scenario43_SecurityHeadersOnForbidden verifies that security headers
// are present on 403 Forbidden responses.
func (s *Scenario43PermissionMatrixE2ESuite) TestE2E_Scenario43_SecurityHeadersOnForbidden() {
	s.logger.Info("starting scenario 43: security headers on forbidden")

	server, handler := s.newPermissionMatrixServer()
	defer server.Close()

	rec := doE2ERequest(handler, http.MethodPost, "/api/v1alpha1/clusters",
		"basic-user", "basic-pass")

	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
	assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"),
		"X-Content-Type-Options should be set on 403 response")
	assert.Equal(s.T(), "DENY", rec.Header().Get("X-Frame-Options"),
		"X-Frame-Options should be set on 403 response")
	assert.Equal(s.T(), "no-store", rec.Header().Get("Cache-Control"),
		"Cache-Control should be set on 403 response")
	assert.Contains(s.T(), rec.Header().Get("Strict-Transport-Security"), "max-age=31536000",
		"HSTS should be set on 403 response")

	s.logger.Info("scenario 43: security headers on forbidden completed")
}
