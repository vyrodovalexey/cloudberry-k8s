//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// KeycloakIntegrationSuite tests Keycloak integration with real Keycloak server.
type KeycloakIntegrationSuite struct {
	suite.Suite
	env      *testutil.TestEnv
	keycloak *testutil.KeycloakTestHelper
	ctx      context.Context
}

func TestIntegration_Keycloak(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(KeycloakIntegrationSuite))
}

func (s *KeycloakIntegrationSuite) SetupSuite() {
	s.env = testutil.NewTestEnv()
	s.keycloak = testutil.NewKeycloakTestHelper(
		s.env.KeycloakAddr,
		s.env.KeycloakAdmin,
		s.env.KeycloakAdminPassword,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !s.keycloak.IsAvailable(ctx) {
		s.T().Skip("Keycloak is not available, skipping integration tests")
	}
}

func (s *KeycloakIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_IsAvailable() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	available := s.keycloak.IsAvailable(ctx)
	assert.True(s.T(), available, "Keycloak should be available")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_AdminToken() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	token, err := s.keycloak.GetAdminToken(ctx)
	require.NoError(s.T(), err, "should obtain admin token")
	assert.NotEmpty(s.T(), token, "admin token should not be empty")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_BackendRealmExists() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	err := s.keycloak.CheckRealmExists(ctx, "backend-test")
	if err != nil {
		s.T().Skipf("backend-test realm not found (run setup-keycloak.sh first): %v", err)
	}
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_GatewayRealmExists() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	err := s.keycloak.CheckRealmExists(ctx, "gateway-test")
	if err != nil {
		s.T().Skipf("gateway-test realm not found (run setup-keycloak.sh first): %v", err)
	}
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_ClientCredentialsGrant() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Check realm exists first
	if err := s.keycloak.CheckRealmExists(ctx, "backend-test"); err != nil {
		s.T().Skipf("backend-test realm not found: %v", err)
	}

	tokenResp, err := s.keycloak.GetClientCredentialsToken(
		ctx, "backend-test", "gateway-backend", "gateway-backend-secret",
	)
	require.NoError(s.T(), err, "should obtain client credentials token")
	require.NotNil(s.T(), tokenResp)
	assert.NotEmpty(s.T(), tokenResp.AccessToken, "access token should not be empty")
	assert.Equal(s.T(), "Bearer", tokenResp.TokenType)
	assert.Greater(s.T(), tokenResp.ExpiresIn, 0, "token should have positive expiry")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_PasswordGrant() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Check realm exists first
	if err := s.keycloak.CheckRealmExists(ctx, "gateway-test"); err != nil {
		s.T().Skipf("gateway-test realm not found: %v", err)
	}

	tokenResp, err := s.keycloak.GetPasswordToken(
		ctx, "gateway-test", "gateway", "gateway-secret", "testuser", "testpass",
	)
	require.NoError(s.T(), err, "should obtain password grant token")
	require.NotNil(s.T(), tokenResp)
	assert.NotEmpty(s.T(), tokenResp.AccessToken, "access token should not be empty")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_PasswordGrant_InvalidCredentials() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "gateway-test"); err != nil {
		s.T().Skipf("gateway-test realm not found: %v", err)
	}

	_, err := s.keycloak.GetPasswordToken(
		ctx, "gateway-test", "gateway", "gateway-secret", "testuser", "wrongpassword",
	)
	assert.Error(s.T(), err, "should fail with invalid credentials")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_PasswordGrant_AdminUser() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "gateway-test"); err != nil {
		s.T().Skipf("gateway-test realm not found: %v", err)
	}

	tokenResp, err := s.keycloak.GetPasswordToken(
		ctx, "gateway-test", "gateway", "gateway-secret", "adminuser", "adminpass",
	)
	require.NoError(s.T(), err, "should obtain token for admin user")
	assert.NotEmpty(s.T(), tokenResp.AccessToken)
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_PasswordGrant_ReaderUser() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "gateway-test"); err != nil {
		s.T().Skipf("gateway-test realm not found: %v", err)
	}

	tokenResp, err := s.keycloak.GetPasswordToken(
		ctx, "gateway-test", "gateway", "gateway-secret", "reader", "readerpass",
	)
	require.NoError(s.T(), err, "should obtain token for reader user")
	assert.NotEmpty(s.T(), tokenResp.AccessToken)
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_OIDCDiscovery_BackendRealm() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "backend-test"); err != nil {
		s.T().Skipf("backend-test realm not found: %v", err)
	}

	discovery, err := s.keycloak.GetOIDCDiscovery(ctx, "backend-test")
	require.NoError(s.T(), err, "should get OIDC discovery document")
	require.NotNil(s.T(), discovery)

	// Verify standard OIDC discovery fields
	assert.NotEmpty(s.T(), discovery["issuer"], "issuer should be set")
	assert.NotEmpty(s.T(), discovery["authorization_endpoint"], "authorization_endpoint should be set")
	assert.NotEmpty(s.T(), discovery["token_endpoint"], "token_endpoint should be set")
	assert.NotEmpty(s.T(), discovery["jwks_uri"], "jwks_uri should be set")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_OIDCDiscovery_GatewayRealm() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "gateway-test"); err != nil {
		s.T().Skipf("gateway-test realm not found: %v", err)
	}

	discovery, err := s.keycloak.GetOIDCDiscovery(ctx, "gateway-test")
	require.NoError(s.T(), err, "should get OIDC discovery document")
	require.NotNil(s.T(), discovery)

	assert.NotEmpty(s.T(), discovery["issuer"])
	assert.NotEmpty(s.T(), discovery["token_endpoint"])
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_TokenIntrospection() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "backend-test"); err != nil {
		s.T().Skipf("backend-test realm not found: %v", err)
	}

	// Get a token first
	tokenResp, err := s.keycloak.GetClientCredentialsToken(
		ctx, "backend-test", "gateway-backend", "gateway-backend-secret",
	)
	require.NoError(s.T(), err)

	// Introspect the token
	introspection, err := s.keycloak.IntrospectToken(
		ctx, "backend-test", "gateway-backend", "gateway-backend-secret", tokenResp.AccessToken,
	)
	require.NoError(s.T(), err, "should introspect token")
	require.NotNil(s.T(), introspection)

	active, ok := introspection["active"].(bool)
	require.True(s.T(), ok, "active field should be a boolean")
	assert.True(s.T(), active, "token should be active")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_InvalidClientCredentials() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := s.keycloak.CheckRealmExists(ctx, "backend-test"); err != nil {
		s.T().Skipf("backend-test realm not found: %v", err)
	}

	_, err := s.keycloak.GetClientCredentialsToken(
		ctx, "backend-test", "gateway-backend", "wrong-secret",
	)
	assert.Error(s.T(), err, "should fail with invalid client secret")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_NonExistentRealm() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	err := s.keycloak.CheckRealmExists(ctx, "nonexistent-realm-12345")
	assert.Error(s.T(), err, "non-existent realm should return error")
}

func (s *KeycloakIntegrationSuite) TestIntegration_Keycloak_ContextCancellation() {
	ctx, cancel := context.WithCancel(s.ctx)
	cancel() // Cancel immediately

	_, err := s.keycloak.GetAdminToken(ctx)
	assert.Error(s.T(), err, "should fail with canceled context")
}
