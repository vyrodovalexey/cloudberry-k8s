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

// VaultIntegrationSuite tests Vault integration with real Vault server.
type VaultIntegrationSuite struct {
	suite.Suite
	env   *testutil.TestEnv
	vault *testutil.VaultTestHelper
	ctx   context.Context
}

func TestIntegration_Vault(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(VaultIntegrationSuite))
}

func (s *VaultIntegrationSuite) SetupSuite() {
	s.env = testutil.NewTestEnv()
	s.vault = testutil.NewVaultTestHelper(s.env.VaultAddr, s.env.VaultToken)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !s.vault.IsAvailable(ctx) {
		s.T().Skip("Vault is not available, skipping integration tests")
	}
}

func (s *VaultIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_IsAvailable() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	available := s.vault.IsAvailable(ctx)
	assert.True(s.T(), available, "Vault should be available")
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_KVWriteAndRead() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Write a test secret
	testPath := "secret/data/test/cloudberry-integration"
	testData := map[string]interface{}{
		"username": "testuser",
		"password": "testpass123",
		"database": "cloudberry",
	}

	err := s.vault.WriteSecret(ctx, testPath, testData)
	require.NoError(s.T(), err, "should write secret to Vault")

	// Read the secret back
	readData, err := s.vault.ReadSecret(ctx, testPath)
	require.NoError(s.T(), err, "should read secret from Vault")
	require.NotNil(s.T(), readData)

	assert.Equal(s.T(), "testuser", readData["username"])
	assert.Equal(s.T(), "testpass123", readData["password"])
	assert.Equal(s.T(), "cloudberry", readData["database"])
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_KVOverwrite() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	testPath := "secret/data/test/cloudberry-overwrite"

	// Write initial secret
	err := s.vault.WriteSecret(ctx, testPath, map[string]interface{}{
		"key": "value1",
	})
	require.NoError(s.T(), err)

	// Overwrite with new data
	err = s.vault.WriteSecret(ctx, testPath, map[string]interface{}{
		"key": "value2",
	})
	require.NoError(s.T(), err)

	// Read should return the latest version
	data, err := s.vault.ReadSecret(ctx, testPath)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "value2", data["key"])
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_KVReadNonExistent() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	_, err := s.vault.ReadSecret(ctx, "secret/data/test/nonexistent-path-12345")
	assert.Error(s.T(), err, "reading non-existent secret should fail")
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_PKIEngineCheck() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	err := s.vault.CheckPKIEngine(ctx, "pki")
	if err != nil {
		s.T().Skipf("PKI engine not configured: %v (run setup-vault.sh first)", err)
	}
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_PKICertIssuance() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Check if PKI is configured
	if err := s.vault.CheckPKIEngine(ctx, "pki"); err != nil {
		s.T().Skipf("PKI engine not configured: %v", err)
	}

	// Issue a test certificate
	cert, err := s.vault.IssueCertificate(ctx, "pki", "test-role", "test.cloudberry.local")
	if err != nil {
		s.T().Skipf("Cannot issue certificate (role may not exist): %v", err)
	}

	require.NotNil(s.T(), cert)
	assert.NotEmpty(s.T(), cert.Certificate, "certificate should not be empty")
	assert.NotEmpty(s.T(), cert.PrivateKey, "private key should not be empty")
	assert.NotEmpty(s.T(), cert.SerialNumber, "serial number should not be empty")
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_KVEngineCheck() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	err := s.vault.CheckKVEngine(ctx, "secret")
	require.NoError(s.T(), err, "KV engine should be mounted at 'secret'")
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_BackendCredentials() {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Check if backend credentials were stored by setup script
	data, err := s.vault.ReadSecret(ctx, "secret/data/backend-auth/basic")
	if err != nil {
		s.T().Skipf("Backend credentials not found (run setup-vault.sh first): %v", err)
	}

	require.NotNil(s.T(), data)
	assert.NotEmpty(s.T(), data["username"], "username should be set")
	assert.NotEmpty(s.T(), data["password"], "password should be set")
}

func (s *VaultIntegrationSuite) TestIntegration_Vault_ContextCancellation() {
	ctx, cancel := context.WithCancel(s.ctx)
	cancel() // Cancel immediately

	_, err := s.vault.ReadSecret(ctx, "secret/data/test/anything")
	assert.Error(s.T(), err, "should fail with canceled context")
}
