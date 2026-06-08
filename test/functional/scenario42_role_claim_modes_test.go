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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 42: Role Claim Source and Match Modes
// ============================================================================
//
// This scenario tests:
// - 42a: roleClaimSource=id_token — roles extracted from ID token claims
// - 42b: roleClaimSource=userinfo — config field acceptance (known gap: no UserInfo call)
// - 42c: roleMatchMode=exact — exact string matching
// - 42d: roleMatchMode=suffix — suffix string matching
// - 42e: roleMatchMode=prefix — prefix string matching
// - 42f: roleMatchMode=contains — substring matching
// - Integration: resolvePermission with all match modes
// - Cases catalog: RoleClaimCases
// ============================================================================

// scenario42OIDCServer holds a mock OIDC server with RSA key for JWT signing.
type scenario42OIDCServer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	url    string
}

// newScenario42OIDCServer creates a mock OIDC discovery server with a real RSA key.
func newScenario42OIDCServer(t *testing.T) *scenario42OIDCServer {
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
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key-42"}]}`,
			n, e,
		)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)

	return &scenario42OIDCServer{
		server: server,
		key:    key,
		url:    serverURL,
	}
}

// signJWT42 creates a minimal RS256-signed JWT for testing.
func signJWT42(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
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

// newScenario42Provider creates an OIDCProvider with the given match mode and role mapping.
func newScenario42Provider(
	t *testing.T,
	oidcServer *scenario42OIDCServer,
	matchMode string,
	roleMapping map[string]string,
) *auth.OIDCProvider {
	t.Helper()

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.url,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: matchMode,
		RoleMapping:   roleMapping,
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err, "creating OIDC provider with matchMode=%q", matchMode)
	require.NotNil(t, provider)

	return provider
}

// authenticateWithRole creates a JWT with the given role and authenticates it.
func authenticateWithRole(
	t *testing.T,
	provider *auth.OIDCProvider,
	oidcServer *scenario42OIDCServer,
	role string,
) *auth.Identity {
	t.Helper()

	claims := map[string]interface{}{
		"iss": oidcServer.url,
		"sub": "scenario42-user",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
		"realm_access": map[string]interface{}{
			"roles": []interface{}{role},
		},
	}
	token := signJWT42(t, oidcServer.key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, err := provider.Authenticate(context.Background(), req)
	require.NoError(t, err, "authentication should succeed for role %q", role)
	require.NotNil(t, identity)

	return identity
}

// --- 42a: roleClaimSource=id_token ---

// TestFunctional_Scenario42a_IDToken_RolesFromClaims verifies that when
// roleClaimSource is set to "id_token" (the default), roles are extracted
// from the ID token claims via the configured RoleClaimPath.
func TestFunctional_Scenario42a_IDToken_RolesFromClaims(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)

	cfg := auth.OIDCConfig{
		IssuerURL:       oidcServer.url,
		ClientID:        "cloudberry-operator",
		RoleClaimPath:   "realm_access.roles",
		RoleClaimSource: "id_token",
		RoleMatchMode:   "exact",
		RoleMapping:     map[string]string{"admin": "Admin", "viewer": "Basic"},
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, provider)

	t.Run("admin_role_extracted_from_id_token", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": oidcServer.url,
			"sub": "admin-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"admin"},
			},
		}
		token := signJWT42(t, oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, authErr := provider.Authenticate(context.Background(), req)
		require.NoError(t, authErr)
		require.NotNil(t, identity)

		assert.Equal(t, []string{"admin"}, identity.Roles,
			"roles should be extracted from ID token claims")
		assert.Equal(t, auth.PermissionAdmin, identity.Permission,
			"admin role should map to Admin permission")
	})

	t.Run("multiple_roles_extracted_from_id_token", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": oidcServer.url,
			"sub": "multi-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"viewer", "admin"},
			},
		}
		token := signJWT42(t, oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, authErr := provider.Authenticate(context.Background(), req)
		require.NoError(t, authErr)
		require.NotNil(t, identity)

		assert.Len(t, identity.Roles, 2)
		assert.Contains(t, identity.Roles, "viewer")
		assert.Contains(t, identity.Roles, "admin")
		assert.Equal(t, auth.PermissionAdmin, identity.Permission,
			"highest role (admin) should win")
	})

	t.Run("no_roles_in_id_token_defaults_to_self_only", func(t *testing.T) {
		claims := map[string]interface{}{
			"iss": oidcServer.url,
			"sub": "no-role-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
		}
		token := signJWT42(t, oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, authErr := provider.Authenticate(context.Background(), req)
		require.NoError(t, authErr)
		require.NotNil(t, identity)

		assert.Empty(t, identity.Roles)
		assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
			"no roles should default to Self Only")
	})
}

// --- 42b: roleClaimSource=userinfo ---

// TestFunctional_Scenario42b_UserInfo_ConfigField verifies that OIDCConfig.RoleClaimSource
// can be set to "userinfo". Note: the actual UserInfo endpoint call is not implemented
// in the current code — this is a known gap. The Authenticate method always extracts
// roles from the ID token claims regardless of the RoleClaimSource setting.
func TestFunctional_Scenario42b_UserInfo_ConfigField(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)

	t.Run("userinfo_config_accepted", func(t *testing.T) {
		cfg := auth.OIDCConfig{
			IssuerURL:       oidcServer.url,
			ClientID:        "cloudberry-operator",
			RoleClaimPath:   "realm_access.roles",
			RoleClaimSource: "userinfo",
			RoleMatchMode:   "exact",
			RoleMapping:     map[string]string{"admin": "Admin"},
		}

		provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.NoError(t, err, "provider should accept roleClaimSource=userinfo")
		require.NotNil(t, provider)
		assert.Equal(t, "oidc", provider.Type())
	})

	t.Run("userinfo_source_still_reads_id_token_claims", func(t *testing.T) {
		// Known gap: even with roleClaimSource=userinfo, the current implementation
		// extracts roles from the ID token claims. This test documents the behavior.
		cfg := auth.OIDCConfig{
			IssuerURL:       oidcServer.url,
			ClientID:        "cloudberry-operator",
			RoleClaimPath:   "realm_access.roles",
			RoleClaimSource: "userinfo",
			RoleMatchMode:   "exact",
			RoleMapping:     map[string]string{"admin": "Admin"},
		}

		provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.NoError(t, err)

		claims := map[string]interface{}{
			"iss": oidcServer.url,
			"sub": "userinfo-user",
			"aud": "cloudberry-operator",
			"exp": float64(time.Now().Add(time.Hour).Unix()),
			"iat": float64(time.Now().Unix()),
			"realm_access": map[string]interface{}{
				"roles": []interface{}{"admin"},
			},
		}
		token := signJWT42(t, oidcServer.key, claims)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		identity, authErr := provider.Authenticate(context.Background(), req)
		require.NoError(t, authErr)
		require.NotNil(t, identity)

		// Even with userinfo source, roles come from ID token (known gap).
		assert.Equal(t, []string{"admin"}, identity.Roles,
			"roles are extracted from ID token claims even when source is userinfo (known gap)")
		assert.Equal(t, auth.PermissionAdmin, identity.Permission)
	})

	t.Run("default_source_is_id_token", func(t *testing.T) {
		cfg := auth.OIDCConfig{
			IssuerURL: oidcServer.url,
			ClientID:  "cloudberry-operator",
			// RoleClaimSource left empty — should default to "id_token".
		}

		provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
		require.NoError(t, err)
		require.NotNil(t, provider)
	})
}

// --- 42c: roleMatchMode=exact ---

// TestFunctional_Scenario42c_Exact_Match verifies that exact mode matches
// only when the role string is identical to the mapping key.
func TestFunctional_Scenario42c_Exact_Match(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "exact", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "admin")
	assert.Equal(t, auth.PermissionAdmin, identity.Permission,
		"exact mode: 'admin' should match 'admin'")
}

// TestFunctional_Scenario42c_Exact_NoMatch verifies that exact mode does not
// match when the role string differs from the mapping key.
func TestFunctional_Scenario42c_Exact_NoMatch(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "exact", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "super-admin")
	assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
		"exact mode: 'super-admin' should NOT match 'admin'")
}

// --- 42d: roleMatchMode=suffix ---

// TestFunctional_Scenario42d_Suffix_Match verifies that suffix mode matches
// when the role ends with the mapping key.
func TestFunctional_Scenario42d_Suffix_Match(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "suffix", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "org-admin")
	assert.Equal(t, auth.PermissionAdmin, identity.Permission,
		"suffix mode: 'org-admin' ends with 'admin' — should match")
}

// TestFunctional_Scenario42d_Suffix_NoMatch verifies that suffix mode does not
// match when the role does not end with the mapping key.
func TestFunctional_Scenario42d_Suffix_NoMatch(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "suffix", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "admin-team")
	assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
		"suffix mode: 'admin-team' does NOT end with 'admin' — should not match")
}

// --- 42e: roleMatchMode=prefix ---

// TestFunctional_Scenario42e_Prefix_Match verifies that prefix mode matches
// when the role starts with the mapping key.
func TestFunctional_Scenario42e_Prefix_Match(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "prefix", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "admin-team")
	assert.Equal(t, auth.PermissionAdmin, identity.Permission,
		"prefix mode: 'admin-team' starts with 'admin' — should match")
}

// TestFunctional_Scenario42e_Prefix_NoMatch verifies that prefix mode does not
// match when the role does not start with the mapping key.
func TestFunctional_Scenario42e_Prefix_NoMatch(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "prefix", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "org-admin")
	assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
		"prefix mode: 'org-admin' does NOT start with 'admin' — should not match")
}

// --- 42f: roleMatchMode=contains ---

// TestFunctional_Scenario42f_Contains_Match verifies that contains mode matches
// when the role contains the mapping key as a substring.
func TestFunctional_Scenario42f_Contains_Match(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "contains", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "super-admin-user")
	assert.Equal(t, auth.PermissionAdmin, identity.Permission,
		"contains mode: 'super-admin-user' contains 'admin' — should match")
}

// TestFunctional_Scenario42f_Contains_NoMatch verifies that contains mode does not
// match when the role does not contain the mapping key.
func TestFunctional_Scenario42f_Contains_NoMatch(t *testing.T) {
	t.Parallel()

	oidcServer := newScenario42OIDCServer(t)
	provider := newScenario42Provider(t, oidcServer, "contains", map[string]string{"admin": "Admin"})

	identity := authenticateWithRole(t, provider, oidcServer, "reader")
	assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
		"contains mode: 'reader' does NOT contain 'admin' — should not match")
}

// --- Integration: resolvePermission with all match modes ---

// TestFunctional_Scenario42_ResolvePermission_AllModes tests resolvePermission
// behavior through the Authenticate method with each match mode and various
// role combinations.
func TestFunctional_Scenario42_ResolvePermission_AllModes(t *testing.T) {
	t.Parallel()

	modeTests := []struct {
		name               string
		matchMode          string
		mappingKey         string
		userRole           string
		expectedPermission auth.PermissionLevel
		shouldMatch        bool
	}{
		// Exact mode.
		{"exact_admin_matches_admin", "exact", "admin", "admin", auth.PermissionAdmin, true},
		{"exact_admin_not_matches_super_admin", "exact", "admin", "super-admin", auth.PermissionSelfOnly, false},
		{"exact_operator_matches_operator", "exact", "operator", "operator", auth.PermissionOperator, true},

		// Suffix mode.
		{"suffix_admin_matches_org_admin", "suffix", "admin", "org-admin", auth.PermissionAdmin, true},
		{"suffix_admin_matches_cloudberry_admin", "suffix", "admin", "cloudberry-admin", auth.PermissionAdmin, true},
		{"suffix_admin_not_matches_admin_team", "suffix", "admin", "admin-team", auth.PermissionSelfOnly, false},

		// Prefix mode.
		{"prefix_admin_matches_admin_team", "prefix", "admin", "admin-team", auth.PermissionAdmin, true},
		{"prefix_admin_matches_admin_role", "prefix", "admin", "admin-role", auth.PermissionAdmin, true},
		{"prefix_admin_not_matches_super_admin", "prefix", "admin", "super-admin", auth.PermissionSelfOnly, false},

		// Contains mode.
		{"contains_admin_matches_super_admin_user", "contains", "admin", "super-admin-user", auth.PermissionAdmin, true},
		{"contains_admin_matches_admin", "contains", "admin", "admin", auth.PermissionAdmin, true},
		{"contains_admin_not_matches_reader", "contains", "admin", "reader", auth.PermissionSelfOnly, false},
	}

	for _, tt := range modeTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			oidcServer := newScenario42OIDCServer(t)
			permName := tt.expectedPermission.String()
			if !tt.shouldMatch {
				permName = "Admin" // mapping value doesn't matter for non-match
			}
			provider := newScenario42Provider(t, oidcServer, tt.matchMode,
				map[string]string{tt.mappingKey: permName})

			identity := authenticateWithRole(t, provider, oidcServer, tt.userRole)
			assert.Equal(t, tt.expectedPermission, identity.Permission,
				"matchMode=%q: role %q with pattern %q — shouldMatch=%v",
				tt.matchMode, tt.userRole, tt.mappingKey, tt.shouldMatch)
		})
	}
}

// --- Cases catalog ---

// TestFunctional_Scenario42_RoleClaimCases runs the RoleClaimCases catalog
// to verify all role claim source and match mode combinations.
func TestFunctional_Scenario42_RoleClaimCases(t *testing.T) {
	t.Parallel()

	testCases := cases.RoleClaimCases()
	require.NotEmpty(t, testCases, "RoleClaimCases should return test cases")
	require.Len(t, testCases, 10, "RoleClaimCases should return 10 test cases")

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			t.Log(tc.Description)

			oidcServer := newScenario42OIDCServer(t)
			provider := newScenario42Provider(t, oidcServer, tc.MatchMode,
				map[string]string{tc.MappingKey: "Admin"})

			identity := authenticateWithRole(t, provider, oidcServer, tc.UserRole)

			if tc.ExpectMatch {
				assert.Equal(t, auth.PermissionAdmin, identity.Permission,
					"case %q: role %q should match pattern %q in %s mode",
					tc.Name, tc.UserRole, tc.MappingKey, tc.MatchMode)
			} else {
				assert.Equal(t, auth.PermissionSelfOnly, identity.Permission,
					"case %q: role %q should NOT match pattern %q in %s mode",
					tc.Name, tc.UserRole, tc.MappingKey, tc.MatchMode)
			}
		})
	}
}
