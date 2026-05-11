//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// AuthFlowSuite tests complete authentication flows.
type AuthFlowSuite struct {
	suite.Suite
	env      *testutil.TestEnv
	keycloak *testutil.KeycloakTestHelper
	ctx      context.Context
}

func TestIntegration_AuthFlow(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AuthFlowSuite))
}

func (s *AuthFlowSuite) SetupSuite() {
	s.env = testutil.NewTestEnv()
	s.keycloak = testutil.NewKeycloakTestHelper(
		s.env.KeycloakAddr,
		s.env.KeycloakAdmin,
		s.env.KeycloakAdminPassword,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !s.keycloak.IsAvailable(ctx) {
		s.T().Skip("Keycloak is not available, skipping auth flow tests")
	}
}

func (s *AuthFlowSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *AuthFlowSuite) TestIntegration_BasicAuth_ValidCredentials() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "secretpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := auth.IdentityFromContext(r.Context())
		require.NotNil(s.T(), identity)
		assert.Equal(s.T(), "gpadmin", identity.Username)
		assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("gpadmin", "secretpass")
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_BasicAuth_InvalidCredentials() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "secretpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("gpadmin", "wrongpassword")
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_BasicAuth_MissingHeader() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_BasicAuth_UnknownUser() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("unknownuser", "anypass")
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_BasicAuth_PermissionLevels() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "pass", auth.PermissionAdmin)
	store.SetCredentials("operator", "pass", auth.PermissionOperator)
	store.SetCredentials("basic", "pass", auth.PermissionBasic)
	store.SetCredentials("self", "pass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	tests := []struct {
		name           string
		username       string
		requiredLevel  auth.PermissionLevel
		expectedStatus int
	}{
		{"admin_can_access_admin", "admin", auth.PermissionAdmin, http.StatusOK},
		{"operator_cannot_access_admin", "operator", auth.PermissionAdmin, http.StatusForbidden},
		{"operator_can_access_operator", "operator", auth.PermissionOperator, http.StatusOK},
		{"basic_can_access_basic", "basic", auth.PermissionBasic, http.StatusOK},
		{"basic_cannot_access_operator", "basic", auth.PermissionOperator, http.StatusForbidden},
		{"self_can_access_self", "self", auth.PermissionSelfOnly, http.StatusOK},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			handler := middleware.Handler()(
				auth.RequirePermission(tt.requiredLevel)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				})),
			)

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.SetBasicAuth(tt.username, "pass")
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), tt.expectedStatus, rec.Code, "user=%s, required=%s", tt.username, tt.requiredLevel)
		})
	}
}

func (s *AuthFlowSuite) TestIntegration_BearerToken_NoOIDCProvider() {
	// Arrange - no OIDC provider configured
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert - should fail because OIDC is not configured
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_SecurityHeaders() {
	// Arrange
	handler := auth.SecurityHeaders()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	assert.Equal(s.T(), "no-store", rec.Header().Get("Cache-Control"))
	assert.Equal(s.T(), "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Equal(s.T(), "DENY", rec.Header().Get("X-Frame-Options"))
	assert.NotEmpty(s.T(), rec.Header().Get("Strict-Transport-Security"))
	assert.NotEmpty(s.T(), rec.Header().Get("Content-Security-Policy"))
}

func (s *AuthFlowSuite) TestIntegration_UnsupportedAuthType() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Digest username=test")
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *AuthFlowSuite) TestIntegration_MalformedBasicAuth() {
	// Arrange
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// Malformed base64
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("nocolon")))
	rec := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}
