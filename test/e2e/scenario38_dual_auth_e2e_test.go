//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
// Scenario 38: Dual-Mode Auth Infrastructure Bootstrap
// ============================================================================
//
// This scenario tests that when a CloudberryCluster is deployed with BOTH
// basic and OIDC auth enabled, the operator's auth middleware correctly routes
// requests to the appropriate provider based on the Authorization header, and
// both providers return correct Identity objects with proper AuthMethod and
// PermissionLevel.
// ============================================================================

// scenario38MockOIDCProvider implements auth.Provider for testing OIDC routing
// without requiring a real OIDC issuer.
type scenario38MockOIDCProvider struct{}

// Authenticate extracts the bearer token and returns a mock OIDC identity.
func (m *scenario38MockOIDCProvider) Authenticate(_ context.Context, r *http.Request) (*auth.Identity, error) {
	token := r.Header.Get("Authorization")
	if !strings.HasPrefix(token, "Bearer ") {
		return nil, fmt.Errorf("missing bearer token")
	}
	return &auth.Identity{
		Username:   "oidc-user",
		AuthMethod: "oidc",
		Permission: auth.PermissionOperator,
		Roles:      []string{"operator"},
	}, nil
}

// Type returns the provider type name.
func (m *scenario38MockOIDCProvider) Type() string { return "oidc" }

// Scenario38DualAuthE2ESuite tests the dual-mode auth infrastructure end-to-end.
type Scenario38DualAuthE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario38(t *testing.T) {
	suite.Run(t, new(Scenario38DualAuthE2ESuite))
}

// newDualAuthServer creates an API server with both basic and mock OIDC providers.
func (s *Scenario38DualAuthE2ESuite) newDualAuthServer(
	cluster *cbv1alpha1.CloudberryCluster,
) (*api.Server, http.Handler) {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("opbasic", "opbasicpass", auth.PermissionOperatorBasic)
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)
	store.SetCredentials("reader", "readerpass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)
	return server, server.Handler()
}

// TestE2E_Scenario38_DualAuth_BothProvidersSimultaneous verifies that the API server
// correctly routes Basic and Bearer requests to the appropriate providers.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_BothProvidersSimultaneous() {
	s.logger.Info("starting scenario 38: dual auth both providers simultaneous")

	cluster := testutil.NewClusterBuilder("s38-dual-auth", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/cloudberry", "cloudberry-operator").
		Build()

	server, handler := s.newDualAuthServer(cluster)
	defer server.Close()

	// Basic auth request should succeed and route to basic provider.
	s.Run("basic_auth_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", "adminpass")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"basic auth request should succeed")
	})

	// Bearer auth request should succeed and route to OIDC provider.
	s.Run("bearer_auth_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.Header.Set("Authorization", "Bearer mock-oidc-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"bearer auth request should succeed")
	})

	// No auth request should fail with 401.
	s.Run("no_auth_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"no auth request should return 401")
	})

	s.logger.Info("scenario 38: dual auth both providers simultaneous completed")
}

// TestE2E_Scenario38_DualAuth_BasicAuthIdentity verifies that the Identity
// returned by basic auth has the correct fields.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_BasicAuthIdentity() {
	s.logger.Info("starting scenario 38: basic auth identity verification")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})

	var capturedIdentity *auth.Identity
	handler := authMW.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "adminpass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	require.NotNil(s.T(), capturedIdentity, "identity should be set in context")
	assert.Equal(s.T(), "admin", capturedIdentity.Username,
		"Username should match the authenticated user")
	assert.Equal(s.T(), "basic", capturedIdentity.AuthMethod,
		"AuthMethod should be 'basic'")
	assert.Equal(s.T(), auth.PermissionAdmin, capturedIdentity.Permission,
		"Permission should be Admin")

	s.logger.Info("scenario 38: basic auth identity verification completed")
}

// TestE2E_Scenario38_DualAuth_OIDCAuthIdentity verifies that the Identity
// returned by OIDC auth (mock) has the correct fields.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_OIDCAuthIdentity() {
	s.logger.Info("starting scenario 38: OIDC auth identity verification")

	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})

	var capturedIdentity *auth.Identity
	handler := authMW.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer mock-oidc-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	require.NotNil(s.T(), capturedIdentity, "identity should be set in context")
	assert.Equal(s.T(), "oidc-user", capturedIdentity.Username,
		"Username should match the OIDC user")
	assert.Equal(s.T(), "oidc", capturedIdentity.AuthMethod,
		"AuthMethod should be 'oidc'")
	assert.Equal(s.T(), auth.PermissionOperator, capturedIdentity.Permission,
		"Permission should be Operator")
	assert.Contains(s.T(), capturedIdentity.Roles, "operator",
		"Roles should contain 'operator'")

	s.logger.Info("scenario 38: OIDC auth identity verification completed")
}

// TestE2E_Scenario38_DualAuth_PermissionMatrix tests all 5 permission levels
// via basic auth to verify the full permission hierarchy.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_PermissionMatrix() {
	s.logger.Info("starting scenario 38: permission matrix verification")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("opbasic", "opbasicpass", auth.PermissionOperatorBasic)
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)
	store.SetCredentials("reader", "readerpass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})

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

	s.logger.Info("scenario 38: permission matrix verification completed")
}

// TestE2E_Scenario38_DualAuth_ProviderInterfaceCompliance verifies that both
// providers correctly implement the auth.Provider interface.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_ProviderInterfaceCompliance() {
	s.logger.Info("starting scenario 38: provider interface compliance")

	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}

	// Verify both implement the Provider interface via type assertion.
	var basicIface auth.Provider = basicProvider
	var oidcIface auth.Provider = oidcProvider

	require.NotNil(s.T(), basicIface, "basic provider should implement auth.Provider")
	require.NotNil(s.T(), oidcIface, "OIDC provider should implement auth.Provider")

	// Verify Type() returns correct values.
	assert.Equal(s.T(), "basic", basicIface.Type(),
		"basic provider Type() should return 'basic'")
	assert.Equal(s.T(), "oidc", oidcIface.Type(),
		"OIDC provider Type() should return 'oidc'")

	// Verify Authenticate() returns correct AuthMethod.
	store.SetCredentials("testuser", "testpass", auth.PermissionBasic)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("testuser", "testpass")

	identity, err := basicIface.Authenticate(context.Background(), req)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), "basic", identity.AuthMethod)

	bearerReq := httptest.NewRequest(http.MethodGet, "/", nil)
	bearerReq.Header.Set("Authorization", "Bearer test-token")

	oidcIdentity, err := oidcIface.Authenticate(context.Background(), bearerReq)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), oidcIdentity)
	assert.Equal(s.T(), "oidc", oidcIdentity.AuthMethod)

	s.logger.Info("scenario 38: provider interface compliance completed")
}

// TestE2E_Scenario38_DualAuth_CRSpecReflected verifies that a cluster with
// dual auth CR spec has the auth configuration reflected correctly.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_CRSpecReflected() {
	s.logger.Info("starting scenario 38: CR spec reflection")

	cluster := testutil.NewClusterBuilder("s38-cr-spec", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/cloudberry", "cloudberry-operator").
		Build()

	// Verify the cluster spec has both auth methods configured.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.Basic, "basic auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.OIDC, "OIDC auth spec should be set")

	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled,
		"basic auth should be enabled")
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser,
		"basic auth admin user should be 'gpadmin'")

	assert.True(s.T(), cluster.Spec.Auth.OIDC.Enabled,
		"OIDC auth should be enabled")
	assert.Equal(s.T(), "http://keycloak:8090/realms/cloudberry", cluster.Spec.Auth.OIDC.IssuerURL,
		"OIDC issuer URL should match")
	assert.Equal(s.T(), "cloudberry-operator", cluster.Spec.Auth.OIDC.ClientID,
		"OIDC client ID should match")

	// Create the cluster in the fake K8s env and verify it persists.
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	retrieved, err := k8sEnv.GetCluster(context.Background(), cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	assert.True(s.T(), retrieved.Spec.Auth.Basic.Enabled)
	assert.True(s.T(), retrieved.Spec.Auth.OIDC.Enabled)

	// Verify the API server works with dual auth on this cluster.
	server, handler := s.newDualAuthServer(cluster)
	defer server.Close()

	// Basic auth should work.
	basicReq := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	basicReq.SetBasicAuth("admin", "adminpass")
	basicRec := httptest.NewRecorder()
	handler.ServeHTTP(basicRec, basicReq)
	assert.Equal(s.T(), http.StatusOK, basicRec.Code)

	// Bearer auth should work.
	bearerReq := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	bearerReq.Header.Set("Authorization", "Bearer mock-token")
	bearerRec := httptest.NewRecorder()
	handler.ServeHTTP(bearerRec, bearerReq)
	assert.Equal(s.T(), http.StatusOK, bearerRec.Code)

	s.logger.Info("scenario 38: CR spec reflection completed")
}

// TestE2E_Scenario38_DualAuth_ErrorResponseFormat verifies that error responses
// from the dual-auth middleware have the correct JSON format.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_ErrorResponseFormat() {
	s.logger.Info("starting scenario 38: error response format verification")

	cluster := testutil.NewClusterBuilder("s38-error-format", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	server, handler := s.newDualAuthServer(cluster)
	defer server.Close()

	errorTests := []struct {
		name           string
		authHeader     string
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "missing_auth_header",
			authHeader:     "",
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
			if tt.authHeader != "" {
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
		})
	}

	s.logger.Info("scenario 38: error response format verification completed")
}

// TestE2E_Scenario38_DualAuth_CasesCatalog runs the DualAuthCases from the
// test cases catalog to verify the middleware behavior matches expectations.
func (s *Scenario38DualAuthE2ESuite) TestE2E_Scenario38_DualAuth_CasesCatalog() {
	s.logger.Info("starting scenario 38: cases catalog verification")

	testCases := cases.DualAuthCases()
	require.NotEmpty(s.T(), testCases, "DualAuthCases should return test cases")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "oppass", auth.PermissionOperator)
	store.SetCredentials("viewer", "viewpass", auth.PermissionBasic)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	oidcProvider := &scenario38MockOIDCProvider{}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			// Provider type verification cases.
			switch tc.Name {
			case "basic_provider_type_returns_basic":
				assert.Equal(s.T(), tc.ExpectedAuthMethod, basicProvider.Type())
				return
			case "oidc_provider_type_returns_oidc":
				assert.Equal(s.T(), tc.ExpectedAuthMethod, oidcProvider.Type())
				return
			}

			// Skip cases without expected status code.
			if tc.ExpectStatusCode == 0 {
				return
			}

			authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})
			var capturedIdentity *auth.Identity
			handler := authMW.Handler()(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					capturedIdentity = auth.IdentityFromContext(r.Context())
					w.WriteHeader(http.StatusOK)
				}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.AuthHeader != "" {
				req.Header.Set("Authorization", tc.AuthHeader)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if !tc.ExpectSuccess {
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			if capturedIdentity != nil {
				assert.Equal(s.T(), tc.ExpectedAuthMethod, capturedIdentity.AuthMethod)
			}
		})
	}

	s.logger.Info("scenario 38: cases catalog verification completed")
}
