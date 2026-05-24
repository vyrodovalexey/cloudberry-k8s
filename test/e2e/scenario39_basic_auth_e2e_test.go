//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 39: Basic Authentication Flow (E2E)
// ============================================================================
//
// End-to-end tests for the basic authentication flow, verifying the full
// request lifecycle through the API server with basic auth middleware.
// ============================================================================

// Scenario39BasicAuthE2ESuite tests the basic authentication flow end-to-end.
type Scenario39BasicAuthE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario39(t *testing.T) {
	suite.Run(t, new(Scenario39BasicAuthE2ESuite))
}

// newBasicAuthServer creates an API server with basic auth and the given users.
func (s *Scenario39BasicAuthE2ESuite) newBasicAuthServer(
	cluster *cbv1alpha1.CloudberryCluster,
	users map[string]struct {
		password   string
		permission auth.PermissionLevel
	},
) (*api.Server, http.Handler) {
	store := auth.NewInMemoryCredentialStore()
	for username, cred := range users {
		store.SetCredentials(username, cred.password, cred.permission)
	}

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, 0)
	return server, server.Handler()
}

// defaultUsers returns the default set of test users.
func defaultUsers() map[string]struct {
	password   string
	permission auth.PermissionLevel
} {
	return map[string]struct {
		password   string
		permission auth.PermissionLevel
	}{
		"admin":    {password: "admin-secret", permission: auth.PermissionAdmin},
		"operator": {password: "operator-pass", permission: auth.PermissionOperator},
		"opbasic":  {password: "opbasic-pass", permission: auth.PermissionOperatorBasic},
		"viewer":   {password: "viewer-pass", permission: auth.PermissionBasic},
		"reader":   {password: "reader-pass", permission: auth.PermissionSelfOnly},
	}
}

// TestE2E_Scenario39_AdminAuth_FullFlow verifies the full admin authentication
// flow: valid credentials → 200, invalid → 401, missing → 401.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_AdminAuth_FullFlow() {
	s.logger.Info("starting scenario 39: admin auth full flow")

	cluster := testutil.NewClusterBuilder("s39-admin-auth", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	server, handler := s.newBasicAuthServer(cluster, defaultUsers())
	defer server.Close()

	// Valid admin credentials should succeed.
	s.Run("valid_admin_credentials", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", "admin-secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"valid admin credentials should return 200")
	})

	// Invalid admin password should fail.
	s.Run("invalid_admin_password", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", "wrong-password")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"invalid admin password should return 401")
	})

	// Missing auth header should fail.
	s.Run("missing_auth_header", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"missing auth header should return 401")
	})

	s.logger.Info("scenario 39: admin auth full flow completed")
}

// TestE2E_Scenario39_PermissionLevels tests all 5 permission levels via Basic auth.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_PermissionLevels() {
	s.logger.Info("starting scenario 39: permission levels verification")

	cluster := testutil.NewClusterBuilder("s39-perm-levels", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("opbasic", "opbasicpass", auth.PermissionOperatorBasic)
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)
	store.SetCredentials("reader", "readerpass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	permissionTests := []struct {
		name               string
		username           string
		password           string
		expectedPermission auth.PermissionLevel
		expectedString     string
	}{
		{"admin_permission", "admin", "adminpass", auth.PermissionAdmin, "Admin"},
		{"operator_permission", "operator", "operatorpass", auth.PermissionOperator, "Operator"},
		{"operator_basic_permission", "opbasic", "opbasicpass", auth.PermissionOperatorBasic, "Operator Basic"},
		{"basic_permission", "viewer", "viewerpass", auth.PermissionBasic, "Basic"},
		{"self_only_permission", "reader", "readerpass", auth.PermissionSelfOnly, "Self Only"},
	}

	for _, tt := range permissionTests {
		s.Run(tt.name, func() {
			var capturedIdentity *auth.Identity
			handler := authMW.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedIdentity = auth.IdentityFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(tt.username, tt.password)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), http.StatusOK, rec.Code)
			require.NotNil(s.T(), capturedIdentity)
			assert.Equal(s.T(), tt.expectedPermission, capturedIdentity.Permission,
				"user %q should have %s permission", tt.username, tt.expectedString)
			assert.Equal(s.T(), tt.expectedString, capturedIdentity.Permission.String(),
				"permission string should match")
			assert.Equal(s.T(), "basic", capturedIdentity.AuthMethod,
				"AuthMethod should be 'basic'")
		})
	}

	_ = cluster // cluster used for context
	s.logger.Info("scenario 39: permission levels verification completed")
}

// TestE2E_Scenario39_SecurityHeaders verifies that security headers are present
// on auth responses.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_SecurityHeaders() {
	s.logger.Info("starting scenario 39: security headers verification")

	cluster := testutil.NewClusterBuilder("s39-sec-headers", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	server, handler := s.newBasicAuthServer(cluster, defaultUsers())
	defer server.Close()

	expectedHeaders := map[string]string{
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'self'",
		"Permissions-Policy":      "camera=(), microphone=()",
		"Referrer-Policy":         "strict-origin-when-cross-origin",
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
		"X-XSS-Protection":        "1; mode=block",
	}

	// Test security headers on successful auth response.
	s.Run("success_response_headers", func() {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		for header, expected := range expectedHeaders {
			assert.Equal(s.T(), expected, rec.Header().Get(header),
				"header %s should be set on success response", header)
		}
		assert.Contains(s.T(), rec.Header().Get("Strict-Transport-Security"), "max-age=31536000")
	})

	// Test security headers on auth failure response.
	s.Run("failure_response_headers", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		for header, expected := range expectedHeaders {
			assert.Equal(s.T(), expected, rec.Header().Get(header),
				"header %s should be set on failure response", header)
		}
		assert.Contains(s.T(), rec.Header().Get("Strict-Transport-Security"), "max-age=31536000")
	})

	s.logger.Info("scenario 39: security headers verification completed")
}

// TestE2E_Scenario39_ErrorResponseFormat verifies the JSON error format on 401.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_ErrorResponseFormat() {
	s.logger.Info("starting scenario 39: error response format verification")

	cluster := testutil.NewClusterBuilder("s39-error-format", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	server, handler := s.newBasicAuthServer(cluster, defaultUsers())
	defer server.Close()

	errorTests := []struct {
		name           string
		authHeader     string
		setBasicAuth   bool
		username       string
		password       string
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "missing_auth_header",
			expectedStatus: http.StatusUnauthorized,
			expectedCode:   "UNAUTHORIZED",
		},
		{
			name:           "wrong_password",
			setBasicAuth:   true,
			username:       "admin",
			password:       "wrong",
			expectedStatus: http.StatusUnauthorized,
			expectedCode:   "UNAUTHORIZED",
		},
		{
			name:           "unsupported_auth_type",
			authHeader:     "Digest username=test",
			expectedStatus: http.StatusUnauthorized,
			expectedCode:   "UNAUTHORIZED",
		},
	}

	for _, tt := range errorTests {
		s.Run(tt.name, func() {
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
			if tt.setBasicAuth {
				req.SetBasicAuth(tt.username, tt.password)
			} else if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), tt.expectedStatus, rec.Code)
			assert.Contains(s.T(), rec.Header().Get("Content-Type"), "application/json",
				"error response should be JSON")

			var errResp map[string]interface{}
			require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
			errObj, ok := errResp["error"].(map[string]interface{})
			require.True(s.T(), ok, "response should have 'error' object")
			assert.Equal(s.T(), tt.expectedCode, errObj["code"])
			assert.NotEmpty(s.T(), errObj["message"])
		})
	}

	s.logger.Info("scenario 39: error response format verification completed")
}

// TestE2E_Scenario39_ClusterCRWithBasicAuth verifies that a cluster CR with
// basic auth configuration is accepted and persisted correctly.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_ClusterCRWithBasicAuth() {
	s.logger.Info("starting scenario 39: cluster CR with basic auth")

	cluster := testutil.NewClusterBuilder("s39-cr-basic-auth", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	// Verify the cluster spec has basic auth configured.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.Basic, "basic auth spec should be set")
	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled,
		"basic auth should be enabled")
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser,
		"basic auth admin user should be 'gpadmin'")

	// Create the cluster in the fake K8s env and verify it persists.
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	retrieved, err := k8sEnv.GetCluster(context.Background(), cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	require.NotNil(s.T(), retrieved.Spec.Auth.Basic)
	assert.True(s.T(), retrieved.Spec.Auth.Basic.Enabled)
	assert.Equal(s.T(), "gpadmin", retrieved.Spec.Auth.Basic.AdminUser)

	// Verify the API server works with basic auth on this cluster.
	server, handler := s.newBasicAuthServer(cluster, defaultUsers())
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", "admin-secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.logger.Info("scenario 39: cluster CR with basic auth completed")
}

// TestE2E_Scenario39_BasicAuthFlowCases runs the BasicAuthFlowCases catalog.
func (s *Scenario39BasicAuthE2ESuite) TestE2E_Scenario39_BasicAuthFlowCases() {
	s.logger.Info("starting scenario 39: basic auth flow cases catalog")

	testCases := cases.BasicAuthFlowCases()
	require.NotEmpty(s.T(), testCases, "BasicAuthFlowCases should return test cases")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	store.SetCredentials("operator", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)
	store.SetCredentials("reader", "reader-pass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			// Handle special cases.
			if tc.Name == "39a_malformed_auth_header" {
				handler := authMW.Handler()(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Set("Authorization", "Basic not-valid-base64!!!")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			if tc.Name == "39a_missing_auth_header" {
				handler := authMW.Handler()(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			// Standard Basic auth cases.
			var capturedIdentity *auth.Identity
			handler := authMW.Handler()(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					capturedIdentity = auth.IdentityFromContext(r.Context())
					w.WriteHeader(http.StatusOK)
				}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(tc.Username, tc.Password)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if !tc.ExpectSuccess {
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
			require.NotNil(s.T(), capturedIdentity)
			assert.Equal(s.T(), tc.ExpectedAuthMethod, capturedIdentity.AuthMethod)
			assert.Equal(s.T(), tc.ExpectedPermission, capturedIdentity.Permission.String())
		})
	}

	s.logger.Info("scenario 39: basic auth flow cases catalog completed")
}
