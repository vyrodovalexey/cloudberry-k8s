//go:build functional

package functional

import (
	"bytes"
	"encoding/json"
	"log/slog"
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
// Scenario 43: Full Permission Matrix Verification
// ============================================================================
//
// This scenario verifies the complete API permission matrix by testing every
// endpoint against all five permission levels:
//   - Admin: full access to all endpoints
//   - Operator: allowed cluster operations, denied admin-only operations
//   - OperatorBasic: allowed config/sessions viewing, denied operator operations
//   - Basic: allowed cluster state viewing, denied config/sessions
//   - SelfOnly: only health endpoints, everything else denied
//
// ============================================================================

// permissionLevelOrder maps permission level strings to their numeric order
// for comparison. Higher values include all lower-level permissions.
var permissionLevelOrder = map[string]auth.PermissionLevel{
	"Basic":         auth.PermissionBasic,
	"OperatorBasic": auth.PermissionOperatorBasic,
	"Operator":      auth.PermissionOperator,
	"Admin":         auth.PermissionAdmin,
}

// scenario43User holds credentials and permission level for a test user.
type scenario43User struct {
	username   string
	password   string
	permission auth.PermissionLevel
}

// scenario43Users returns the five test users at different permission levels.
func scenario43Users() []scenario43User {
	return []scenario43User{
		{username: "admin-user", password: "admin-pass", permission: auth.PermissionAdmin},
		{username: "operator-user", password: "operator-pass", permission: auth.PermissionOperator},
		{username: "opbasic-user", password: "opbasic-pass", permission: auth.PermissionOperatorBasic},
		{username: "basic-user", password: "basic-pass", permission: auth.PermissionBasic},
		{username: "selfonly-user", password: "selfonly-pass", permission: auth.PermissionSelfOnly},
	}
}

// Scenario43PermissionMatrixSuite tests the full API permission matrix.
type Scenario43PermissionMatrixSuite struct {
	suite.Suite
	store   *auth.InMemoryCredentialStore
	handler http.Handler
	server  *api.Server
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	users   []scenario43User
}

func TestFunctional_Scenario43(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario43PermissionMatrixSuite))
}

func (s *Scenario43PermissionMatrixSuite) SetupTest() {
	s.users = scenario43Users()

	// Create credential store with all five users.
	s.store = auth.NewInMemoryCredentialStore()
	for _, u := range s.users {
		s.store.SetCredentials(u.username, u.password, u.permission)
	}

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(s.store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	// Build a cluster with all features enabled so endpoints don't fail
	// due to missing spec fields.
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	// Use a high rate limit to avoid rate limiting during permission matrix testing,
	// which sends many requests in rapid succession across all permission levels.
	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario43PermissionMatrixSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an HTTP request with the given credentials.
func (s *Scenario43PermissionMatrixSuite) doRequest(method, path, username, password string) *httptest.ResponseRecorder {
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
	s.handler.ServeHTTP(rec, req)
	return rec
}

// --- 43a: Admin user can access all operations ---

// TestFunctional_Scenario43a_Admin_AllOperationsSucceed verifies that an Admin
// user can access all endpoints without receiving 401 or 403.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43a_Admin_AllOperationsSucceed() {
	// Representative endpoints from each permission level.
	endpoints := []struct {
		method string
		path   string
	}{
		// Basic
		{http.MethodGet, "/api/v1alpha1/clusters"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/status"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/segments"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/standby"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/queries"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/backups"},
		{http.MethodGet, "/api/v1alpha1/clusters/test-cluster/storage/pvcs"},
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
		// Admin
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/standby/activate"},
		{http.MethodDelete, "/api/v1alpha1/clusters/test-cluster/backups/backup-1"},
		{http.MethodPost, "/api/v1alpha1/clusters/test-cluster/backups/backup-1/restore"},
	}

	for _, ep := range endpoints {
		s.Run(ep.method+"_"+ep.path, func() {
			rec := s.doRequest(ep.method, ep.path, "admin-user", "admin-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"admin should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"admin should not get 403 for %s %s", ep.method, ep.path)
		})
	}
}

// --- 43b: Operator user — allowed and denied operations ---

// TestFunctional_Scenario43b_Operator_AllowedAndDenied verifies that an Operator
// user can access operator-level endpoints but is denied admin-only endpoints.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43b_Operator_AllowedAndDenied() {
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
			rec := s.doRequest(ep.method, ep.path, "operator-user", "operator-pass")
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
			rec := s.doRequest(ep.method, ep.path, "operator-user", "operator-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"operator should get 403 for admin-only %s %s", ep.method, ep.path)

			// Verify error response format.
			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), "FORBIDDEN", errObj["code"])
			assert.Contains(s.T(), errObj["message"], "insufficient permissions")
		})
	}
}

// --- 43c: OperatorBasic user — allowed and denied operations ---

// TestFunctional_Scenario43c_OperatorBasic_AllowedAndDenied verifies that an
// OperatorBasic user can view config/sessions but is denied operator operations.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43c_OperatorBasic_AllowedAndDenied() {
	// OperatorBasic-allowed endpoints (Basic + config/sessions).
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
			rec := s.doRequest(ep.method, ep.path, "opbasic-user", "opbasic-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"opbasic should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"opbasic should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	// Operator-level endpoints should be denied.
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
			rec := s.doRequest(ep.method, ep.path, "opbasic-user", "opbasic-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"opbasic should get 403 for %s %s", ep.method, ep.path)
		})
	}
}

// --- 43d: Basic user — allowed and denied operations ---

// TestFunctional_Scenario43d_Basic_AllowedAndDenied verifies that a Basic user
// can view cluster state but is denied config/sessions and operator operations.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43d_Basic_AllowedAndDenied() {
	// Basic-allowed endpoints (read-only cluster state).
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
			rec := s.doRequest(ep.method, ep.path, "basic-user", "basic-pass")
			assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
				"basic should not get 401 for %s %s", ep.method, ep.path)
			assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
				"basic should not get 403 for %s %s", ep.method, ep.path)
		})
	}

	// OperatorBasic and higher endpoints should be denied.
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
			rec := s.doRequest(ep.method, ep.path, "basic-user", "basic-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"basic should get 403 for %s %s", ep.method, ep.path)
		})
	}
}

// --- 43e: SelfOnly user — only health endpoints, everything else denied ---

// TestFunctional_Scenario43e_SelfOnly_AllowedAndDenied verifies that a SelfOnly
// user can only access health endpoints; all API endpoints return 403.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43e_SelfOnly_AllowedAndDenied() {
	// Health endpoints work without auth (no 401/403).
	healthEndpoints := []string{"/healthz", "/readyz"}
	for _, path := range healthEndpoints {
		s.Run("health_"+path, func() {
			rec := s.doRequest(http.MethodGet, path, "selfonly-user", "selfonly-pass")
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
			rec := s.doRequest(ep.method, ep.path, "selfonly-user", "selfonly-pass")
			assert.Equal(s.T(), http.StatusForbidden, rec.Code,
				"selfonly should get 403 for %s %s", ep.method, ep.path)
		})
	}
}

// --- 43: Permission Matrix Cases catalog ---

// TestFunctional_Scenario43_PermissionMatrixCases runs the full PermissionMatrixCases
// catalog, verifying each endpoint against all five permission levels.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43_PermissionMatrixCases() {
	testCases := cases.PermissionMatrixCases()
	require.NotEmpty(s.T(), testCases, "PermissionMatrixCases should return test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			requiredLevel, ok := permissionLevelOrder[tc.RequiredLevel]
			require.True(s.T(), ok,
				"unknown required level %q in test case %s", tc.RequiredLevel, tc.Name)

			for _, u := range s.users {
				s.Run(u.username, func() {
					rec := s.doRequest(tc.Method, tc.Path, u.username, u.password)

					if u.permission >= requiredLevel {
						// User has sufficient permission — should NOT get 403.
						assert.NotEqual(s.T(), http.StatusForbidden, rec.Code,
							"user %s (perm=%s) should be allowed %s %s (requires %s)",
							u.username, u.permission.String(), tc.Method, tc.Path, tc.RequiredLevel)
						assert.NotEqual(s.T(), http.StatusUnauthorized, rec.Code,
							"user %s should not get 401 for %s %s",
							u.username, tc.Method, tc.Path)
					} else {
						// User has insufficient permission — should get 403.
						assert.Equal(s.T(), http.StatusForbidden, rec.Code,
							"user %s (perm=%s) should be denied %s %s (requires %s)",
							u.username, u.permission.String(), tc.Method, tc.Path, tc.RequiredLevel)
					}
				})
			}
		})
	}
}

// TestFunctional_Scenario43_UnauthenticatedDenied verifies that unauthenticated
// requests to all API endpoints return 401.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43_UnauthenticatedDenied() {
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
			rec := s.doRequest(ep.method, ep.path, "", "")
			assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
				"unauthenticated request should get 401 for %s %s", ep.method, ep.path)

			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
		})
	}
}

// TestFunctional_Scenario43_HealthEndpointsNoAuth verifies that health endpoints
// work without any authentication.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43_HealthEndpointsNoAuth() {
	healthEndpoints := []string{"/healthz", "/readyz"}
	for _, path := range healthEndpoints {
		s.Run(path, func() {
			rec := s.doRequest(http.MethodGet, path, "", "")
			assert.Equal(s.T(), http.StatusOK, rec.Code,
				"health endpoint %s should work without auth", path)
		})
	}
}

// TestFunctional_Scenario43_ForbiddenResponseFormat verifies the JSON error
// format for 403 Forbidden responses.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43_ForbiddenResponseFormat() {
	// Basic user trying to access config (requires OperatorBasic).
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-cluster/config",
		"basic-user", "basic-pass")

	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
	assert.Contains(s.T(), rec.Header().Get("Content-Type"), "application/json",
		"403 response should be JSON")

	var errResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
	errObj, ok := errResp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should have 'error' object")
	assert.Equal(s.T(), "FORBIDDEN", errObj["code"])
	assert.Contains(s.T(), errObj["message"], "insufficient permissions")
	assert.Contains(s.T(), errObj["message"], "Operator Basic",
		"error message should indicate the required permission level")
}

// TestFunctional_Scenario43_SecurityHeadersOnForbidden verifies that security
// headers are present on 403 Forbidden responses.
func (s *Scenario43PermissionMatrixSuite) TestFunctional_Scenario43_SecurityHeadersOnForbidden() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters",
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
}
