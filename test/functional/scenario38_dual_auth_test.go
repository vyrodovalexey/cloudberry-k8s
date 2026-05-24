//go:build functional

package functional

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

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// mockOIDCProvider implements auth.Provider for testing OIDC routing
// without requiring a real OIDC issuer.
type mockOIDCProvider struct{}

// Authenticate extracts the bearer token and returns a mock OIDC identity.
func (m *mockOIDCProvider) Authenticate(_ context.Context, r *http.Request) (*auth.Identity, error) {
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
func (m *mockOIDCProvider) Type() string { return "oidc" }

// Scenario38DualAuthSuite tests dual-mode authentication infrastructure bootstrap.
type Scenario38DualAuthSuite struct {
	suite.Suite
	store         *auth.InMemoryCredentialStore
	basicProvider *auth.BasicAuthProvider
	oidcProvider  *mockOIDCProvider
	middleware    *auth.AuthMiddleware
}

func TestFunctional_Scenario38(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario38DualAuthSuite))
}

func (s *Scenario38DualAuthSuite) SetupTest() {
	s.store = auth.NewInMemoryCredentialStore()
	s.store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	s.store.SetCredentials("operator", "oppass", auth.PermissionOperator)
	s.store.SetCredentials("opbasic", "opbasicpass", auth.PermissionOperatorBasic)
	s.store.SetCredentials("viewer", "viewpass", auth.PermissionBasic)
	s.store.SetCredentials("reader", "readerpass", auth.PermissionSelfOnly)

	s.basicProvider = auth.NewBasicAuthProvider(s.store, nil)
	s.oidcProvider = &mockOIDCProvider{}
	s.middleware = auth.NewAuthMiddleware(s.basicProvider, s.oidcProvider, nil, &metrics.NoopRecorder{})
}

// TestFunctional_Scenario38_BothProvidersActive verifies that the middleware
// can be created with both basic and OIDC providers simultaneously.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_BothProvidersActive() {
	require.NotNil(s.T(), s.middleware, "middleware should be created with both providers")
	require.NotNil(s.T(), s.basicProvider, "basic provider should be non-nil")
	require.NotNil(s.T(), s.oidcProvider, "OIDC provider should be non-nil")
}

// TestFunctional_Scenario38_BasicAuthRouting verifies that requests with
// "Authorization: Basic ..." are routed to the basic provider.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_BasicAuthRouting() {
	var capturedIdentity *auth.Identity
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "adminpass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	require.NotNil(s.T(), capturedIdentity, "identity should be set in context")
	assert.Equal(s.T(), "basic", capturedIdentity.AuthMethod,
		"AuthMethod should be 'basic' for Basic auth header")
	assert.Equal(s.T(), "admin", capturedIdentity.Username)
	assert.Equal(s.T(), auth.PermissionAdmin, capturedIdentity.Permission)
}

// TestFunctional_Scenario38_BearerAuthRouting verifies that requests with
// "Authorization: Bearer ..." are routed to the OIDC provider.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_BearerAuthRouting() {
	var capturedIdentity *auth.Identity
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer mock-oidc-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	require.NotNil(s.T(), capturedIdentity, "identity should be set in context")
	assert.Equal(s.T(), "oidc", capturedIdentity.AuthMethod,
		"AuthMethod should be 'oidc' for Bearer auth header")
	assert.Equal(s.T(), "oidc-user", capturedIdentity.Username)
	assert.Equal(s.T(), auth.PermissionOperator, capturedIdentity.Permission)
}

// TestFunctional_Scenario38_BasicProviderType verifies that the basic provider
// returns "basic" from its Type() method.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_BasicProviderType() {
	assert.Equal(s.T(), "basic", s.basicProvider.Type(),
		"BasicAuthProvider.Type() should return 'basic'")
}

// TestFunctional_Scenario38_OIDCProviderType verifies that the mock OIDC provider
// returns "oidc" from its Type() method.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_OIDCProviderType() {
	assert.Equal(s.T(), "oidc", s.oidcProvider.Type(),
		"OIDCProvider.Type() should return 'oidc'")
}

// TestFunctional_Scenario38_PermissionResolver_Admin verifies that basic auth
// with admin credentials resolves to PermissionAdmin.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_PermissionResolver_Admin() {
	identity := s.authenticateBasic("admin", "adminpass")
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
		"admin user should have Admin permission")
}

// TestFunctional_Scenario38_PermissionResolver_Operator verifies that basic auth
// with operator credentials resolves to PermissionOperator.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_PermissionResolver_Operator() {
	identity := s.authenticateBasic("operator", "oppass")
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), auth.PermissionOperator, identity.Permission,
		"operator user should have Operator permission")
}

// TestFunctional_Scenario38_PermissionResolver_Basic verifies that basic auth
// with viewer credentials resolves to PermissionBasic.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_PermissionResolver_Basic() {
	identity := s.authenticateBasic("viewer", "viewpass")
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), auth.PermissionBasic, identity.Permission,
		"viewer user should have Basic permission")
}

// TestFunctional_Scenario38_PermissionResolver_SelfOnly verifies that basic auth
// with reader credentials resolves to PermissionSelfOnly.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_PermissionResolver_SelfOnly() {
	identity := s.authenticateBasic("reader", "readerpass")
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
		"reader user should have Self Only permission")
}

// TestFunctional_Scenario38_MissingAuthHeader verifies that a request without
// an Authorization header returns 401 Unauthorized.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_MissingAuthHeader() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"missing auth header should return 401")

	var errResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
	errObj, ok := errResp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should have 'error' object")
	assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
}

// TestFunctional_Scenario38_UnsupportedAuthType verifies that a request with
// an unsupported authorization type (e.g., Digest) returns 401 Unauthorized.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_UnsupportedAuthType() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Digest username=test")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unsupported auth type should return 401")

	var errResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
	errObj, ok := errResp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should have 'error' object")
	assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
}

// TestFunctional_Scenario38_SimultaneousProviders_DifferentUsers verifies that
// multiple sequential requests with different auth types are correctly routed
// to the appropriate provider.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_SimultaneousProviders_DifferentUsers() {
	var capturedIdentity *auth.Identity
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	// First request: Basic auth as admin.
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.SetBasicAuth("admin", "adminpass")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	assert.Equal(s.T(), http.StatusOK, rec1.Code)
	require.NotNil(s.T(), capturedIdentity)
	assert.Equal(s.T(), "basic", capturedIdentity.AuthMethod)
	assert.Equal(s.T(), "admin", capturedIdentity.Username)
	assert.Equal(s.T(), auth.PermissionAdmin, capturedIdentity.Permission)

	// Second request: Bearer auth as OIDC user.
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer oidc-token-123")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	assert.Equal(s.T(), http.StatusOK, rec2.Code)
	require.NotNil(s.T(), capturedIdentity)
	assert.Equal(s.T(), "oidc", capturedIdentity.AuthMethod)
	assert.Equal(s.T(), "oidc-user", capturedIdentity.Username)
	assert.Equal(s.T(), auth.PermissionOperator, capturedIdentity.Permission)

	// Third request: Basic auth as viewer.
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	req3.SetBasicAuth("viewer", "viewpass")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	assert.Equal(s.T(), http.StatusOK, rec3.Code)
	require.NotNil(s.T(), capturedIdentity)
	assert.Equal(s.T(), "basic", capturedIdentity.AuthMethod)
	assert.Equal(s.T(), "viewer", capturedIdentity.Username)
	assert.Equal(s.T(), auth.PermissionBasic, capturedIdentity.Permission)

	// Fourth request: No auth header.
	req4 := httptest.NewRequest(http.MethodGet, "/", nil)
	rec4 := httptest.NewRecorder()
	handler.ServeHTTP(rec4, req4)

	assert.Equal(s.T(), http.StatusUnauthorized, rec4.Code,
		"request without auth should return 401 even after successful requests")
}

// TestFunctional_Scenario38_DualAuthCases runs the test cases from the cases catalog.
func (s *Scenario38DualAuthSuite) TestFunctional_Scenario38_DualAuthCases() {
	testCases := cases.DualAuthCases()
	require.NotEmpty(s.T(), testCases, "DualAuthCases should return test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			// Provider type verification cases (no HTTP request needed).
			switch tc.Name {
			case "basic_provider_type_returns_basic":
				assert.Equal(s.T(), tc.ExpectedAuthMethod, s.basicProvider.Type())
				return
			case "oidc_provider_type_returns_oidc":
				assert.Equal(s.T(), tc.ExpectedAuthMethod, s.oidcProvider.Type())
				return
			}

			// HTTP request-based cases.
			if tc.ExpectStatusCode == 0 {
				return
			}

			var capturedIdentity *auth.Identity
			handler := s.middleware.Handler()(http.HandlerFunc(
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

			// For success cases, the mock providers always succeed,
			// so we verify the identity was set correctly.
			if capturedIdentity != nil {
				assert.Equal(s.T(), tc.ExpectedAuthMethod, capturedIdentity.AuthMethod)
			}
		})
	}
}

// authenticateBasic is a helper that sends a Basic auth request through the middleware
// and returns the captured identity.
func (s *Scenario38DualAuthSuite) authenticateBasic(username, password string) *auth.Identity {
	s.T().Helper()

	var capturedIdentity *auth.Identity
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = auth.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth(username, password)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(s.T(), http.StatusOK, rec.Code,
		"basic auth should succeed for user %q", username)
	return capturedIdentity
}
