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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 42: Role Claim Source and Match Modes (E2E)
// ============================================================================
//
// End-to-end tests for role claim source and match mode functionality:
// - Exact match with admin role
// - Exact match with non-matching role
// - Suffix match
// - Prefix match
// - Contains match
// - Cluster CR with role config accepted
// - Cases catalog integration
// ============================================================================

// scenario42E2EOIDCServer holds a mock OIDC server with RSA key for JWT signing.
type scenario42E2EOIDCServer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	url    string
}

// newScenario42E2EOIDCServer creates a mock OIDC discovery server with a real RSA key.
func newScenario42E2EOIDCServer(t *testing.T) *scenario42E2EOIDCServer {
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
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key-42e2e"}]}`,
			n, e,
		)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)

	return &scenario42E2EOIDCServer{
		server: server,
		key:    key,
		url:    serverURL,
	}
}

// signJWT42E2E creates a minimal RS256-signed JWT for testing.
func signJWT42E2E(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
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

// newScenario42E2EProvider creates an OIDCProvider with the given match mode and role mapping.
func newScenario42E2EProvider(
	t *testing.T,
	oidcServer *scenario42E2EOIDCServer,
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

// authenticateWithRole42E2E creates a JWT with the given role and authenticates it.
func authenticateWithRole42E2E(
	t *testing.T,
	provider *auth.OIDCProvider,
	oidcServer *scenario42E2EOIDCServer,
	role string,
) *auth.Identity {
	t.Helper()

	claims := map[string]interface{}{
		"iss": oidcServer.url,
		"sub": "scenario42-e2e-user",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
		"realm_access": map[string]interface{}{
			"roles": []interface{}{role},
		},
	}
	token := signJWT42E2E(t, oidcServer.key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, err := provider.Authenticate(context.Background(), req)
	require.NoError(t, err, "authentication should succeed for role %q", role)
	require.NotNil(t, identity)

	return identity
}

// Scenario42RoleClaimE2ESuite tests role claim source and match modes end-to-end.
type Scenario42RoleClaimE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario42(t *testing.T) {
	suite.Run(t, new(Scenario42RoleClaimE2ESuite))
}

// TestE2E_Scenario42_ExactMatch_AdminRole tests exact match with admin role.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_ExactMatch_AdminRole() {
	s.logger.Info("starting scenario 42: exact match with admin role")

	oidcServer := newScenario42E2EOIDCServer(s.T())
	provider := newScenario42E2EProvider(s.T(), oidcServer, "exact",
		map[string]string{"admin": "Admin"})

	identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "admin")
	assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
		"exact mode: 'admin' should match 'admin' and resolve to Admin permission")
	assert.Equal(s.T(), "oidc", identity.AuthMethod)

	s.logger.Info("scenario 42: exact match with admin role completed")
}

// TestE2E_Scenario42_ExactMatch_NoMatch tests exact match with non-matching role.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_ExactMatch_NoMatch() {
	s.logger.Info("starting scenario 42: exact match with non-matching role")

	oidcServer := newScenario42E2EOIDCServer(s.T())
	provider := newScenario42E2EProvider(s.T(), oidcServer, "exact",
		map[string]string{"admin": "Admin"})

	identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "super-admin")
	assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
		"exact mode: 'super-admin' should NOT match 'admin'")

	s.logger.Info("scenario 42: exact match with non-matching role completed")
}

// TestE2E_Scenario42_SuffixMatch tests suffix match mode.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_SuffixMatch() {
	s.logger.Info("starting scenario 42: suffix match mode")

	oidcServer := newScenario42E2EOIDCServer(s.T())
	provider := newScenario42E2EProvider(s.T(), oidcServer, "suffix",
		map[string]string{"admin": "Admin"})

	s.Run("suffix_match_org_admin", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "org-admin")
		assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
			"suffix mode: 'org-admin' ends with 'admin' — should match")
	})

	s.Run("suffix_no_match_admin_team", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "admin-team")
		assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
			"suffix mode: 'admin-team' does NOT end with 'admin' — should not match")
	})

	s.logger.Info("scenario 42: suffix match mode completed")
}

// TestE2E_Scenario42_PrefixMatch tests prefix match mode.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_PrefixMatch() {
	s.logger.Info("starting scenario 42: prefix match mode")

	oidcServer := newScenario42E2EOIDCServer(s.T())
	provider := newScenario42E2EProvider(s.T(), oidcServer, "prefix",
		map[string]string{"admin": "Admin"})

	s.Run("prefix_match_admin_team", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "admin-team")
		assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
			"prefix mode: 'admin-team' starts with 'admin' — should match")
	})

	s.Run("prefix_no_match_org_admin", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "org-admin")
		assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
			"prefix mode: 'org-admin' does NOT start with 'admin' — should not match")
	})

	s.logger.Info("scenario 42: prefix match mode completed")
}

// TestE2E_Scenario42_ContainsMatch tests contains match mode.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_ContainsMatch() {
	s.logger.Info("starting scenario 42: contains match mode")

	oidcServer := newScenario42E2EOIDCServer(s.T())
	provider := newScenario42E2EProvider(s.T(), oidcServer, "contains",
		map[string]string{"admin": "Admin"})

	s.Run("contains_match_super_admin_user", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "super-admin-user")
		assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
			"contains mode: 'super-admin-user' contains 'admin' — should match")
	})

	s.Run("contains_no_match_reader", func() {
		identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, "reader")
		assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
			"contains mode: 'reader' does NOT contain 'admin' — should not match")
	})

	s.logger.Info("scenario 42: contains match mode completed")
}

// TestE2E_Scenario42_ClusterCRWithRoleConfig tests that a cluster CR with
// role claim configuration is accepted and persisted correctly.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_ClusterCRWithRoleConfig() {
	s.logger.Info("starting scenario 42: cluster CR with role config")

	cluster := testutil.NewClusterBuilder("s42-role-config", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/test", "cloudberry-operator").
		Build()

	// Verify the cluster spec has OIDC configured.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.OIDC, "OIDC spec should be set")

	assert.True(s.T(), cluster.Spec.Auth.OIDC.Enabled, "OIDC should be enabled")
	assert.Equal(s.T(), "http://keycloak:8090/realms/test", cluster.Spec.Auth.OIDC.IssuerURL)
	assert.Equal(s.T(), "cloudberry-operator", cluster.Spec.Auth.OIDC.ClientID)

	// Create the cluster in the fake K8s env and verify it persists.
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	retrieved, err := k8sEnv.GetCluster(context.Background(), cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	require.NotNil(s.T(), retrieved.Spec.Auth.OIDC)

	assert.True(s.T(), retrieved.Spec.Auth.OIDC.Enabled)
	assert.Equal(s.T(), "http://keycloak:8090/realms/test", retrieved.Spec.Auth.OIDC.IssuerURL)
	assert.Equal(s.T(), "cloudberry-operator", retrieved.Spec.Auth.OIDC.ClientID)

	s.logger.Info("scenario 42: cluster CR with role config completed")
}

// TestE2E_Scenario42_RoleClaimCases runs the RoleClaimCases catalog end-to-end.
func (s *Scenario42RoleClaimE2ESuite) TestE2E_Scenario42_RoleClaimCases() {
	s.logger.Info("starting scenario 42: role claim cases catalog")

	testCases := cases.RoleClaimCases()
	require.NotEmpty(s.T(), testCases, "RoleClaimCases should return test cases")
	require.Len(s.T(), testCases, 10, "RoleClaimCases should return 10 test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			oidcServer := newScenario42E2EOIDCServer(s.T())
			provider := newScenario42E2EProvider(s.T(), oidcServer, tc.MatchMode,
				map[string]string{tc.MappingKey: "Admin"})

			identity := authenticateWithRole42E2E(s.T(), provider, oidcServer, tc.UserRole)

			if tc.ExpectMatch {
				assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
					"case %q: role %q should match pattern %q in %s mode",
					tc.Name, tc.UserRole, tc.MappingKey, tc.MatchMode)
			} else {
				assert.Equal(s.T(), auth.PermissionSelfOnly, identity.Permission,
					"case %q: role %q should NOT match pattern %q in %s mode",
					tc.Name, tc.UserRole, tc.MappingKey, tc.MatchMode)
			}
		})
	}

	s.logger.Info("scenario 42: role claim cases catalog completed")
}
