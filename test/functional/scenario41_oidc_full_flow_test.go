//go:build functional

package functional

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 41: OIDC Full Flow with Keycloak
// ============================================================================
//
// This scenario tests the complete OIDC authentication flow including:
// - OIDC provider initialization with mock discovery
// - JWT verification (valid, invalid, expired tokens)
// - Role extraction from nested realm_access.roles claims
// - Role-to-permission mapping for all 5 permission levels
// - Standard claim extraction (sub, email, preferred_username)
// - Dual-mode auth (Basic + OIDC simultaneously)
// - All role match modes (exact, suffix, prefix, contains)
// - Cases catalog integration
// ============================================================================

// scenario41OIDCServer holds a mock OIDC server with RSA key for JWT signing.
type scenario41OIDCServer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	url    string
}

// newScenario41OIDCServer creates a mock OIDC discovery server with a real RSA key.
func newScenario41OIDCServer(t *testing.T) *scenario41OIDCServer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "generating RSA key")

	mux := http.NewServeMux()
	var serverURL string

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})

	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key"}]}`,
			n, e,
		)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)

	return &scenario41OIDCServer{
		server: server,
		key:    key,
		url:    serverURL,
	}
}

// signJWT41 creates a minimal RS256-signed JWT for testing.
func signJWT41(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
	t.Helper()

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))

	claimsJSON, err := json.Marshal(claims)
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signingInput := header + "." + payload
	hash := crypto.SHA256.New()
	hash.Write([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hash.Sum(nil))
	require.NoError(t, err)

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// scenario41RoleMapping returns the standard role mapping used in scenario 41.
func scenario41RoleMapping() map[string]string {
	return map[string]string{
		"admin":          "Admin",
		"operator":       "Operator",
		"operator-basic": "Operator Basic",
		"user":           "Basic",
		"reader":         "Self Only",
	}
}

// Scenario41OIDCFullFlowSuite tests the complete OIDC authentication flow.
type Scenario41OIDCFullFlowSuite struct {
	suite.Suite
	oidcServer *scenario41OIDCServer
	provider   *auth.OIDCProvider
}

func TestFunctional_Scenario41(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario41OIDCFullFlowSuite))
}

func (s *Scenario41OIDCFullFlowSuite) SetupSuite() {
	s.oidcServer = newScenario41OIDCServer(s.T())

	cfg := auth.OIDCConfig{
		IssuerURL:     s.oidcServer.url,
		ClientID:      "cloudberry-operator",
		ClientSecret:  "test-secret",
		Scopes:        []string{"openid", "profile", "email"},
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   scenario41RoleMapping(),
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err, "creating OIDC provider")
	require.NotNil(s.T(), provider, "OIDC provider should not be nil")
	s.provider = provider
}

// TestFunctional_Scenario41_OIDCProviderInit verifies that NewOIDCProvider
// correctly initializes with a mock OIDC discovery endpoint.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_OIDCProviderInit() {
	t := s.T()

	t.Run("successful_init_with_mock_discovery", func(t *testing.T) {
		require.NotNil(t, s.provider, "provider should be initialized")
		assert.Equal(t, "oidc", s.provider.Type(), "provider type should be 'oidc'")
	})

	t.Run("oauth2_config_populated", func(t *testing.T) {
		oauth2Cfg := s.provider.GetOAuth2Config()
		require.NotNil(t, oauth2Cfg, "OAuth2 config should not be nil")
		assert.Equal(t, "cloudberry-operator", oauth2Cfg.ClientID)
		assert.Equal(t, "test-secret", oauth2Cfg.ClientSecret)
		assert.Contains(t, oauth2Cfg.Scopes, "openid")
		assert.Contains(t, oauth2Cfg.Scopes, "profile")
		assert.Contains(t, oauth2Cfg.Scopes, "email")
	})

	t.Run("missing_issuer_url_fails", func(t *testing.T) {
		cfg := auth.OIDCConfig{ClientID: "test"}
		_, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "OIDC issuer URL is required")
	})

	t.Run("missing_client_id_fails", func(t *testing.T) {
		cfg := auth.OIDCConfig{IssuerURL: "https://example.com"}
		_, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "OIDC client ID is required")
	})

	t.Run("unreachable_issuer_fails", func(t *testing.T) {
		cfg := auth.OIDCConfig{
			IssuerURL: "https://unreachable.invalid/realms/test",
			ClientID:  "test",
		}
		_, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.Error(t, err)
	})
}

// TestFunctional_Scenario41_JWTVerification tests JWT verification with
// valid, invalid, and expired tokens.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_JWTVerification() {
	t := s.T()

	t.Run("valid_token_succeeds", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "user-valid",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user-valid", identity.Username)
		assert.Equal(t, "oidc", identity.AuthMethod)
	})

	t.Run("invalid_token_fails", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer invalid-jwt-token")

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "token verification failed")
	})

	t.Run("expired_token_fails", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "user-expired",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(-time.Hour).Unix()),
			"iat": float64(time.Now().Add(-2 * time.Hour).Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "token verification failed")
	})

	t.Run("wrong_audience_fails", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "user-wrong-aud",
			"aud": "wrong-client-id",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "token verification failed")
	})

	t.Run("missing_bearer_token_fails", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "missing or malformed Bearer token")
	})
}

// TestFunctional_Scenario41_RoleExtraction tests extracting roles from
// nested realm_access.roles claims.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_RoleExtraction() {
	t := s.T()

	t.Run("single_role_extracted", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"admin"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, []string{"admin"}, identity.Roles)
	})

	t.Run("multiple_roles_extracted", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "multi-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"user", "operator", "admin"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Len(t, identity.Roles, 3)
		assert.Contains(t, identity.Roles, "user")
		assert.Contains(t, identity.Roles, "operator")
		assert.Contains(t, identity.Roles, "admin")
	})

	t.Run("no_roles_returns_empty", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "no-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Empty(t, identity.Roles)
	})

	t.Run("missing_realm_access_returns_nil_roles", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss":   s.oidcServer.url,
			"sub":   "missing-realm-user",
			"aud":   "cloudberry-operator",
			"exp":   float64(time.Now().Add(time.Hour).Unix()),
			"iat":   float64(time.Now().Unix()),
			"other": "value",
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Nil(t, identity.Roles)
	})
}

// TestFunctional_Scenario41_RoleMapping_AllLevels tests that each of the 5
// role-to-permission mappings resolves correctly.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_RoleMapping_AllLevels() {
	t := s.T()

	roleMappingTests := []struct {
		name               string
		role               string
		expectedPermission auth.PermissionLevel
		expectedString     string
	}{
		{"admin_maps_to_admin", "admin", auth.PermissionAdmin, "Admin"},
		{"operator_maps_to_operator", "operator", auth.PermissionOperator, "Operator"},
		{"operator_basic_maps_to_operator_basic", "operator-basic", auth.PermissionOperatorBasic, "Operator Basic"},
		{"user_maps_to_basic", "user", auth.PermissionBasic, "Basic"},
		{"reader_maps_to_self_only", "reader", auth.PermissionSelfOnly, "Self Only"},
	}

	for _, tt := range roleMappingTests {
		t.Run(tt.name, func(t *testing.T) {
			claims := map[string]interface{}{
				"iss": s.oidcServer.url,
				"sub": tt.name + "-user",
				"aud": "cloudberry-operator",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{tt.role},
				},
			}
			token := signJWT41(t, s.oidcServer.key, claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, err := s.provider.Authenticate(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, identity)
			assert.Equal(t, tt.expectedPermission, identity.Permission,
				"role %q should map to %s", tt.role, tt.expectedString)
			assert.Equal(t, tt.expectedString, identity.Permission.String())
		})
	}

	t.Run("multiple_roles_highest_wins", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "multi-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"reader", "user", "admin"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, auth.PermissionAdmin, identity.Permission,
			"highest role (admin) should win when multiple roles present")
	})

	t.Run("unknown_role_defaults_to_self_only", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "unknown-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"unknown-role"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
			"unknown role should default to Self Only")
	})
}

// TestFunctional_Scenario41_ClaimExtraction tests extraction of standard
// OIDC claims: sub, email, preferred_username.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_ClaimExtraction() {
	t := s.T()

	t.Run("sub_claim_sets_username", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "user-id-12345",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user-id-12345", identity.Username)
	})

	t.Run("email_claim_extracted", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss":   s.oidcServer.url,
			"sub":   "user-email",
			"aud":   "cloudberry-operator",
			"email": "user@cloudberry.io",
			"exp":   float64(time.Now().Add(time.Hour).Unix()),
			"iat":   float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user@cloudberry.io", identity.Email)
	})

	t.Run("preferred_username_overrides_sub", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss":                s.oidcServer.url,
			"sub":                "user-id-67890",
			"aud":                "cloudberry-operator",
			"preferred_username": "jdoe",
			"email":              "jdoe@cloudberry.io",
			"exp":                float64(time.Now().Add(time.Hour).Unix()),
			"iat":                float64(time.Now().Unix()),
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "jdoe", identity.Username,
			"preferred_username should override sub for Username")
		assert.Equal(t, "jdoe@cloudberry.io", identity.Email)
	})

	t.Run("all_claims_together", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss":                s.oidcServer.url,
			"sub":                "sub-value",
			"aud":                "cloudberry-operator",
			"preferred_username": "preferred-user",
			"email":              "preferred@cloudberry.io",
			"exp":                float64(time.Now().Add(time.Hour).Unix()),
			"iat":                float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"operator"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, err := s.provider.Authenticate(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "preferred-user", identity.Username)
		assert.Equal(t, "preferred@cloudberry.io", identity.Email)
		assert.Equal(t, "oidc", identity.AuthMethod)
		assert.Equal(t, auth.PermissionOperator, identity.Permission)
		assert.Contains(t, identity.Roles, "operator")
	})
}

// TestFunctional_Scenario41_AllowLocalSignIn tests that Basic auth works
// alongside OIDC when allowLocalSignIn is true.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_AllowLocalSignIn() {
	t := s.T()

	// Set up basic auth provider.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "admin-password", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-password", auth.PermissionBasic)
	basicProvider := auth.NewBasicAuthProvider(store, nil)

	// Create middleware with both providers active (simulating allowLocalSignIn: true).
	middleware := auth.NewAuthMiddleware(basicProvider, s.provider, nil, &metrics.NoopRecorder{})

	t.Run("basic_auth_works_alongside_oidc", func(t *testing.T) {
		var capturedIdentity *auth.Identity
		handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedIdentity = auth.IdentityFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth("gpadmin", "admin-password")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, capturedIdentity)
		assert.Equal(t, "basic", capturedIdentity.AuthMethod)
		assert.Equal(t, "gpadmin", capturedIdentity.Username)
		assert.Equal(t, auth.PermissionAdmin, capturedIdentity.Permission)
	})

	t.Run("oidc_auth_works_alongside_basic", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "oidc-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"operator"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		var capturedIdentity *auth.Identity
		handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedIdentity = auth.IdentityFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, capturedIdentity)
		assert.Equal(t, "oidc", capturedIdentity.AuthMethod)
		assert.Equal(t, "oidc-user", capturedIdentity.Username)
		assert.Equal(t, auth.PermissionOperator, capturedIdentity.Permission)
	})

	t.Run("sequential_basic_then_oidc", func(t *testing.T) {
		var capturedIdentity *auth.Identity
		handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedIdentity = auth.IdentityFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		// First: basic auth.
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		req1.SetBasicAuth("viewer", "viewer-password")
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code)
		require.NotNil(t, capturedIdentity)
		assert.Equal(t, "basic", capturedIdentity.AuthMethod)
		assert.Equal(t, auth.PermissionBasic, capturedIdentity.Permission)

		// Second: OIDC auth.
		claims := map[string]interface{}{
			"iss": s.oidcServer.url,
			"sub": "oidc-admin",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"admin"},
			},
		}
		token := signJWT41(t, s.oidcServer.key, claims)

		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		req2.Header.Set("Authorization", "Bearer "+token)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code)
		require.NotNil(t, capturedIdentity)
		assert.Equal(t, "oidc", capturedIdentity.AuthMethod)
		assert.Equal(t, auth.PermissionAdmin, capturedIdentity.Permission)
	})

	t.Run("no_auth_header_returns_401", func(t *testing.T) {
		handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

// TestFunctional_Scenario41_MatchModes tests all role match modes:
// exact, suffix, prefix, contains.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_MatchModes() {
	t := s.T()

	matchModeTests := []struct {
		name               string
		matchMode          string
		idpRole            string
		tokenRole          string
		expectedPermission auth.PermissionLevel
		shouldMatch        bool
	}{
		{
			name:               "exact_match",
			matchMode:          "exact",
			idpRole:            "admin",
			tokenRole:          "admin",
			expectedPermission: auth.PermissionAdmin,
			shouldMatch:        true,
		},
		{
			name:        "exact_no_match",
			matchMode:   "exact",
			idpRole:     "admin",
			tokenRole:   "cloudberry-admin",
			shouldMatch: false,
		},
		{
			name:               "suffix_match",
			matchMode:          "suffix",
			idpRole:            "admin",
			tokenRole:          "cloudberry-admin",
			expectedPermission: auth.PermissionAdmin,
			shouldMatch:        true,
		},
		{
			name:        "suffix_no_match",
			matchMode:   "suffix",
			idpRole:     "admin",
			tokenRole:   "admin-role",
			shouldMatch: false,
		},
		{
			name:               "prefix_match",
			matchMode:          "prefix",
			idpRole:            "admin",
			tokenRole:          "admin-role",
			expectedPermission: auth.PermissionAdmin,
			shouldMatch:        true,
		},
		{
			name:        "prefix_no_match",
			matchMode:   "prefix",
			idpRole:     "admin",
			tokenRole:   "super-admin",
			shouldMatch: false,
		},
		{
			name:               "contains_match",
			matchMode:          "contains",
			idpRole:            "admin",
			tokenRole:          "super-admin-role",
			expectedPermission: auth.PermissionAdmin,
			shouldMatch:        true,
		},
		{
			name:        "contains_no_match",
			matchMode:   "contains",
			idpRole:     "admin",
			tokenRole:   "operator",
			shouldMatch: false,
		},
	}

	for _, tt := range matchModeTests {
		t.Run(tt.name, func(t *testing.T) {
			oidcServer := newScenario41OIDCServer(t)

			cfg := auth.OIDCConfig{
				IssuerURL:     oidcServer.url,
				ClientID:      "cloudberry-operator",
				RoleClaimPath: "realm_access.roles",
				RoleMatchMode: tt.matchMode,
				RoleMapping:   map[string]string{tt.idpRole: "Admin"},
			}

			provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
			require.NoError(t, err)

			claims := map[string]interface{}{
				"iss": oidcServer.url,
				"sub": "match-mode-user",
				"aud": "cloudberry-operator",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{tt.tokenRole},
				},
			}
			token := signJWT41(t, oidcServer.key, claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, authErr := provider.Authenticate(context.Background(), req)
			require.NoError(t, authErr)
			require.NotNil(t, identity)

			if tt.shouldMatch {
				assert.Equal(t, tt.expectedPermission, identity.Permission,
					"role %q with mode %q should match pattern %q",
					tt.tokenRole, tt.matchMode, tt.idpRole)
			} else {
				assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
					"role %q with mode %q should NOT match pattern %q",
					tt.tokenRole, tt.matchMode, tt.idpRole)
			}
		})
	}
}

// TestFunctional_Scenario41_OIDCFlowCases runs the OIDCFlowCases catalog
// to verify all user/role/permission combinations.
func (s *Scenario41OIDCFullFlowSuite) TestFunctional_Scenario41_OIDCFlowCases() {
	t := s.T()

	testCases := cases.OIDCFlowCases()
	require.NotEmpty(t, testCases, "OIDCFlowCases should return test cases")
	require.Len(t, testCases, 5, "OIDCFlowCases should return 5 test cases")

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Log(tc.Description)

			claims := map[string]interface{}{
				"iss":                s.oidcServer.url,
				"sub":                tc.Username + "-sub",
				"aud":                "cloudberry-operator",
				"preferred_username": tc.Username,
				"email":              tc.Username + "@cloudberry.io",
				"exp":                float64(time.Now().Add(time.Hour).Unix()),
				"iat":                float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{tc.Role},
				},
			}
			token := signJWT41(t, s.oidcServer.key, claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, err := s.provider.Authenticate(context.Background(), req)
			require.NoError(t, err, "authentication should succeed for user %q", tc.Username)
			require.NotNil(t, identity)

			assert.Equal(t, tc.Username, identity.Username,
				"username should match")
			assert.Equal(t, tc.ExpectedAuthMethod, identity.AuthMethod,
				"auth method should be %q", tc.ExpectedAuthMethod)
			assert.Equal(t, tc.ExpectedPermission, identity.Permission.String(),
				"permission should be %q for role %q", tc.ExpectedPermission, tc.Role)
		})
	}
}

// scenario41MockOIDCProvider implements auth.Provider for testing OIDC routing
// in the middleware without requiring a real OIDC issuer. It supports
// configurable per-token identity responses.
type scenario41MockOIDCProvider struct {
	identities map[string]*auth.Identity
}

// Authenticate extracts the bearer token and returns the corresponding mock identity.
func (m *scenario41MockOIDCProvider) Authenticate(_ context.Context, r *http.Request) (*auth.Identity, error) {
	token := r.Header.Get("Authorization")
	if !strings.HasPrefix(token, "Bearer ") {
		return nil, fmt.Errorf("missing bearer token")
	}
	rawToken := strings.TrimPrefix(token, "Bearer ")
	if identity, ok := m.identities[rawToken]; ok {
		return identity, nil
	}
	return nil, fmt.Errorf("unknown token")
}

// Type returns the provider type name.
func (m *scenario41MockOIDCProvider) Type() string { return "oidc" }
