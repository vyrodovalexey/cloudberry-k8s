//go:build e2e

package e2e

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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 41: OIDC Full Flow with Keycloak (E2E)
// ============================================================================
//
// End-to-end tests for the complete OIDC authentication flow including:
// - OIDC provider initialization
// - Per-user authentication with all 5 permission levels
// - Dual-mode auth (Basic + OIDC simultaneously)
// - Service account (client_credentials) flow
// - Cluster CR with OIDC config acceptance
// - Cases catalog integration
// ============================================================================

// scenario41E2EOIDCServer holds a mock OIDC server with RSA key for JWT signing.
type scenario41E2EOIDCServer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	url    string
}

// newScenario41E2EOIDCServer creates a mock OIDC discovery server with a real RSA key.
func newScenario41E2EOIDCServer(t *testing.T) *scenario41E2EOIDCServer {
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

	return &scenario41E2EOIDCServer{
		server: server,
		key:    key,
		url:    serverURL,
	}
}

// signJWT41E2E creates a minimal RS256-signed JWT for testing.
func signJWT41E2E(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
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

// scenario41E2ERoleMapping returns the standard role mapping used in scenario 41.
func scenario41E2ERoleMapping() map[string]string {
	return map[string]string{
		"admin":          "Admin",
		"operator":       "Operator",
		"operator-basic": "Operator Basic",
		"user":           "Basic",
		"reader":         "Self Only",
	}
}

// scenario41MockOIDCProvider implements auth.Provider for testing OIDC routing
// in the middleware without requiring a real OIDC issuer.
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

// Scenario41OIDCE2ESuite tests the OIDC full flow end-to-end.
type Scenario41OIDCE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario41(t *testing.T) {
	suite.Run(t, new(Scenario41OIDCE2ESuite))
}

// newScenario41Server creates an API server with both basic and OIDC providers.
func (s *Scenario41OIDCE2ESuite) newScenario41Server(
	cluster *cbv1alpha1.CloudberryCluster,
	oidcProvider auth.Provider,
) (*api.Server, http.Handler) {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "admin-password", auth.PermissionAdmin)
	store.SetCredentials("operator", "operator-password", auth.PermissionOperator)
	store.SetCredentials("opbasic", "opbasic-password", auth.PermissionOperatorBasic)
	store.SetCredentials("viewer", "viewer-password", auth.PermissionBasic)
	store.SetCredentials("reader", "reader-password", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, nil, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)
	return server, server.Handler()
}

// TestE2E_Scenario41_OIDCProviderInit tests OIDC provider initialization
// with a mock OIDC discovery endpoint.
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_OIDCProviderInit() {
	s.logger.Info("starting scenario 41: OIDC provider initialization")

	oidcServer := newScenario41E2EOIDCServer(s.T())

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.url,
		ClientID:      "cloudberry-operator",
		ClientSecret:  "test-secret",
		Scopes:        []string{"openid", "profile", "email"},
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   scenario41E2ERoleMapping(),
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err, "OIDC provider should initialize successfully")
	require.NotNil(s.T(), provider)
	assert.Equal(s.T(), "oidc", provider.Type())

	oauth2Cfg := provider.GetOAuth2Config()
	require.NotNil(s.T(), oauth2Cfg)
	assert.Equal(s.T(), "cloudberry-operator", oauth2Cfg.ClientID)
	assert.Equal(s.T(), "test-secret", oauth2Cfg.ClientSecret)

	s.logger.Info("scenario 41: OIDC provider initialization completed")
}

// TestE2E_Scenario41_PerUserAuth tests each of the 5 users with different
// roles through the real OIDC provider with mock OIDC server.
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_PerUserAuth() {
	s.logger.Info("starting scenario 41: per-user OIDC authentication")

	oidcServer := newScenario41E2EOIDCServer(s.T())

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.url,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   scenario41E2ERoleMapping(),
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	userTests := []struct {
		name               string
		username           string
		email              string
		role               string
		expectedPermission auth.PermissionLevel
		expectedString     string
	}{
		{"admin_user", "admin-user", "admin@cloudberry.io", "admin", auth.PermissionAdmin, "Admin"},
		{"operator_user", "operator-user", "operator@cloudberry.io", "operator", auth.PermissionOperator, "Operator"},
		{"opbasic_user", "opbasic-user", "opbasic@cloudberry.io", "operator-basic", auth.PermissionOperatorBasic, "Operator Basic"},
		{"basic_user", "basic-user", "basic@cloudberry.io", "user", auth.PermissionBasic, "Basic"},
		{"reader_user", "reader-user", "reader@cloudberry.io", "reader", auth.PermissionSelfOnly, "Self Only"},
	}

	for _, tt := range userTests {
		s.Run(tt.name, func() {
			claims := map[string]interface{}{
				"iss":                oidcServer.url,
				"sub":                tt.username + "-sub",
				"aud":                "cloudberry-operator",
				"preferred_username": tt.username,
				"email":              tt.email,
				"exp":                float64(time.Now().Add(time.Hour).Unix()),
				"iat":                float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{tt.role},
				},
			}
			token := signJWT41E2E(s.T(), oidcServer.key, claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, authErr := provider.Authenticate(context.Background(), req)
			require.NoError(s.T(), authErr, "auth should succeed for user %q", tt.username)
			require.NotNil(s.T(), identity)

			assert.Equal(s.T(), tt.username, identity.Username)
			assert.Equal(s.T(), tt.email, identity.Email)
			assert.Equal(s.T(), "oidc", identity.AuthMethod)
			assert.Equal(s.T(), tt.expectedPermission, identity.Permission,
				"user %q with role %q should have %s permission", tt.username, tt.role, tt.expectedString)
			assert.Equal(s.T(), tt.expectedString, identity.Permission.String())
		})
	}

	s.logger.Info("scenario 41: per-user OIDC authentication completed")
}

// TestE2E_Scenario41_AllowLocalSignIn tests that Basic auth works alongside
// OIDC when both providers are configured (allowLocalSignIn: true).
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_AllowLocalSignIn() {
	s.logger.Info("starting scenario 41: allow local sign-in with dual auth")

	cluster := testutil.NewClusterBuilder("s41-dual-auth", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/cloudberry", "cloudberry-operator").
		Build()

	// Use mock OIDC provider for the middleware.
	mockOIDC := &scenario41MockOIDCProvider{
		identities: map[string]*auth.Identity{
			"oidc-admin-token": {
				Username:   "oidc-admin",
				AuthMethod: "oidc",
				Permission: auth.PermissionAdmin,
				Roles:      []string{"admin"},
			},
			"oidc-operator-token": {
				Username:   "oidc-operator",
				AuthMethod: "oidc",
				Permission: auth.PermissionOperator,
				Roles:      []string{"operator"},
			},
		},
	}

	server, handler := s.newScenario41Server(cluster, mockOIDC)
	defer server.Close()

	// Basic auth should work.
	s.Run("basic_auth_succeeds", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("gpadmin", "admin-password")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"basic auth should succeed alongside OIDC")
	})

	// OIDC auth should work.
	s.Run("oidc_auth_succeeds", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.Header.Set("Authorization", "Bearer oidc-admin-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"OIDC auth should succeed alongside basic")
	})

	// No auth should fail.
	s.Run("no_auth_fails", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"no auth should return 401")
	})

	// Interleaved requests.
	s.Run("interleaved_basic_and_oidc", func() {
		// Basic auth.
		req1 := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req1.SetBasicAuth("viewer", "viewer-password")
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)
		assert.Equal(s.T(), http.StatusOK, rec1.Code)

		// OIDC auth.
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req2.Header.Set("Authorization", "Bearer oidc-operator-token")
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)
		assert.Equal(s.T(), http.StatusOK, rec2.Code)

		// Basic auth again.
		req3 := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req3.SetBasicAuth("gpadmin", "admin-password")
		rec3 := httptest.NewRecorder()
		handler.ServeHTTP(rec3, req3)
		assert.Equal(s.T(), http.StatusOK, rec3.Code)
	})

	s.logger.Info("scenario 41: allow local sign-in with dual auth completed")
}

// TestE2E_Scenario41_ServiceAccount tests the client_credentials flow
// by verifying that a token with service account claims is accepted.
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_ServiceAccount() {
	s.logger.Info("starting scenario 41: service account (client_credentials) flow")

	oidcServer := newScenario41E2EOIDCServer(s.T())

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.url,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   scenario41E2ERoleMapping(),
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Simulate a service account token (client_credentials grant).
	// In Keycloak, client_credentials tokens have the client_id as azp
	// and may not have preferred_username, so sub is used.
	claims := map[string]interface{}{
		"iss": oidcServer.url,
		"sub": "service-account-cloudberry-operator",
		"aud": "cloudberry-operator",
		"azp": "cloudberry-operator",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"admin"},
		},
	}
	token := signJWT41E2E(s.T(), oidcServer.key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, authErr := provider.Authenticate(context.Background(), req)
	require.NoError(s.T(), authErr, "service account token should be accepted")
	require.NotNil(s.T(), identity)

	assert.Equal(s.T(), "service-account-cloudberry-operator", identity.Username,
		"service account should use sub as username")
	assert.Equal(s.T(), "oidc", identity.AuthMethod)
	assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
		"service account with admin role should have Admin permission")

	s.logger.Info("scenario 41: service account flow completed")
}

// TestE2E_Scenario41_ClusterCRWithOIDC tests that a cluster CR with OIDC
// configuration is accepted and persisted correctly.
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_ClusterCRWithOIDC() {
	s.logger.Info("starting scenario 41: cluster CR with OIDC config")

	cluster := testutil.NewClusterBuilder("s41-oidc-cr", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/cloudberry", "cloudberry-operator").
		Build()

	// Verify the cluster spec has OIDC configured.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.OIDC, "OIDC spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.Basic, "basic spec should be set")

	assert.True(s.T(), cluster.Spec.Auth.OIDC.Enabled, "OIDC should be enabled")
	assert.Equal(s.T(), "http://keycloak:8090/realms/cloudberry", cluster.Spec.Auth.OIDC.IssuerURL)
	assert.Equal(s.T(), "cloudberry-operator", cluster.Spec.Auth.OIDC.ClientID)
	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled, "basic auth should be enabled")
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser)

	// Create the cluster in the fake K8s env and verify it persists.
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	retrieved, err := k8sEnv.GetCluster(context.Background(), cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	require.NotNil(s.T(), retrieved.Spec.Auth.OIDC)

	assert.True(s.T(), retrieved.Spec.Auth.OIDC.Enabled)
	assert.Equal(s.T(), "http://keycloak:8090/realms/cloudberry", retrieved.Spec.Auth.OIDC.IssuerURL)
	assert.Equal(s.T(), "cloudberry-operator", retrieved.Spec.Auth.OIDC.ClientID)
	assert.True(s.T(), retrieved.Spec.Auth.Basic.Enabled)

	// Verify the API server works with this cluster.
	mockOIDC := &scenario41MockOIDCProvider{
		identities: map[string]*auth.Identity{
			"test-token": {
				Username:   "oidc-user",
				AuthMethod: "oidc",
				Permission: auth.PermissionOperator,
				Roles:      []string{"operator"},
			},
		},
	}

	server, handler := s.newScenario41Server(cluster, mockOIDC)
	defer server.Close()

	// Verify both auth methods work.
	basicReq := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	basicReq.SetBasicAuth("gpadmin", "admin-password")
	basicRec := httptest.NewRecorder()
	handler.ServeHTTP(basicRec, basicReq)
	assert.Equal(s.T(), http.StatusOK, basicRec.Code, "basic auth should work with OIDC CR")

	bearerReq := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	bearerReq.Header.Set("Authorization", "Bearer test-token")
	bearerRec := httptest.NewRecorder()
	handler.ServeHTTP(bearerRec, bearerReq)
	assert.Equal(s.T(), http.StatusOK, bearerRec.Code, "OIDC auth should work with OIDC CR")

	s.logger.Info("scenario 41: cluster CR with OIDC config completed")
}

// TestE2E_Scenario41_OIDCFlowCases runs the OIDCFlowCases catalog end-to-end.
func (s *Scenario41OIDCE2ESuite) TestE2E_Scenario41_OIDCFlowCases() {
	s.logger.Info("starting scenario 41: OIDC flow cases catalog")

	oidcServer := newScenario41E2EOIDCServer(s.T())

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.url,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   scenario41E2ERoleMapping(),
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	testCases := cases.OIDCFlowCases()
	require.NotEmpty(s.T(), testCases, "OIDCFlowCases should return test cases")
	require.Len(s.T(), testCases, 5, "OIDCFlowCases should return 5 test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			claims := map[string]interface{}{
				"iss":                oidcServer.url,
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
			token := signJWT41E2E(s.T(), oidcServer.key, claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, authErr := provider.Authenticate(context.Background(), req)
			require.NoError(s.T(), authErr, "auth should succeed for user %q", tc.Username)
			require.NotNil(s.T(), identity)

			assert.Equal(s.T(), tc.Username, identity.Username)
			assert.Equal(s.T(), tc.ExpectedAuthMethod, identity.AuthMethod)
			assert.Equal(s.T(), tc.ExpectedPermission, identity.Permission.String(),
				"permission should be %q for role %q", tc.ExpectedPermission, tc.Role)
		})
	}

	s.logger.Info("scenario 41: OIDC flow cases catalog completed")
}
