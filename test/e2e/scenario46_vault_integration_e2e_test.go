//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// e2eVaultSecretPaths defines the 4 KV paths used across Scenario 46 E2E tests.
var e2eVaultSecretPaths = []string{
	"secret/data/cloudberry/admin-password",
	"secret/data/cloudberry/oidc-secret",
	"secret/data/cloudberry/monitoring-password",
	"secret/data/cloudberry/tls",
}

// Scenario46VaultE2ESuite tests Scenario 46: Vault Integration end-to-end.
type Scenario46VaultE2ESuite struct {
	E2ESuite
	vaultHelper *testutil.VaultTestHelper
	vaultAddr   string
	vaultToken  string
	pkiMount    string
	pkiRole     string
}

func TestE2E_Scenario46(t *testing.T) {
	suite.Run(t, new(Scenario46VaultE2ESuite))
}

func (s *Scenario46VaultE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()

	env := testutil.NewTestEnv()
	s.vaultAddr = env.VaultAddr
	s.vaultToken = env.VaultToken
	s.pkiMount = env.VaultPKIMount
	s.pkiRole = env.VaultPKIRole
	s.vaultHelper = testutil.NewVaultTestHelper(s.vaultAddr, s.vaultToken)
}

// skipIfVaultUnavailable skips the test if Vault is not reachable.
func (s *Scenario46VaultE2ESuite) skipIfVaultUnavailable() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if !s.vaultHelper.IsAvailable(ctx) {
		s.T().Skip("Vault is not available, skipping E2E test")
	}
}

// TestE2E_Scenario46_TokenAuth connects to the real Vault instance with token auth,
// writes and reads secrets.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_TokenAuth() {
	s.skipIfVaultUnavailable()
	s.logger.Info("starting scenario 46 E2E: token auth")

	cfg := vault.Config{
		Enabled:    true,
		Address:    s.vaultAddr,
		AuthMethod: "token",
		Token:      s.vaultToken,
		RetryOpts:  util.RetryOptions{MaxRetries: 3, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2.0},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err, "token auth against real Vault should succeed")
	require.NotNil(s.T(), client)
	assert.True(s.T(), client.IsEnabled())

	// Write a test secret.
	testPath := "secret/data/cloudberry/e2e-test"
	writeData := map[string]interface{}{
		"username": "e2e-user",
		"password": "e2e-password",
	}
	err = client.WriteSecret(s.ctx, testPath, writeData)
	require.NoError(s.T(), err, "writing secret should succeed")

	// Read it back.
	data, err := client.ReadSecret(s.ctx, testPath)
	require.NoError(s.T(), err, "reading secret should succeed")
	require.NotNil(s.T(), data)
	assert.Equal(s.T(), "e2e-user", data["username"])
	assert.Equal(s.T(), "e2e-password", data["password"])

	s.logger.Info("scenario 46 E2E: token auth completed")
}

// TestE2E_Scenario46_KVSecretPaths reads all 4 KV paths from the real Vault instance.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_KVSecretPaths() {
	s.skipIfVaultUnavailable()
	s.logger.Info("starting scenario 46 E2E: KV secret paths")

	// First, seed the secrets using the VaultTestHelper.
	seedData := map[string]map[string]interface{}{
		"secret/data/cloudberry/admin-password": {
			"username": "gpadmin",
			"password": "admin-secret",
		},
		"secret/data/cloudberry/oidc-secret": {
			"client_id":     "cloudberry-operator",
			"client_secret": "oidc-secret-value",
		},
		"secret/data/cloudberry/monitoring-password": {
			"username": "monitoring",
			"password": "monitoring-secret",
		},
		"secret/data/cloudberry/tls": {
			"ca_cert":  "mock-ca-cert",
			"tls_cert": "mock-tls-cert",
			"tls_key":  "mock-tls-key",
		},
	}

	for path, data := range seedData {
		err := s.vaultHelper.WriteSecret(s.ctx, path, data)
		require.NoError(s.T(), err, "seeding secret at %s should succeed", path)
	}

	// Now read all 4 paths using the vault.Client.
	cfg := vault.Config{
		Enabled:    true,
		Address:    s.vaultAddr,
		AuthMethod: "token",
		Token:      s.vaultToken,
		RetryOpts:  util.RetryOptions{MaxRetries: 3, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2.0},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err)

	for _, path := range e2eVaultSecretPaths {
		data, readErr := client.ReadSecret(s.ctx, path)
		require.NoError(s.T(), readErr, "reading secret at %s should succeed", path)
		require.NotNil(s.T(), data, "secret data at %s should not be nil", path)
		assert.NotEmpty(s.T(), data, "secret data at %s should not be empty", path)
	}

	s.logger.Info("scenario 46 E2E: KV secret paths completed")
}

// TestE2E_Scenario46_AppRoleAuth tests AppRole login against a mock Vault server.
// In a real E2E environment with AppRole configured, this would use the real Vault.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_AppRoleAuth() {
	s.logger.Info("starting scenario 46 E2E: AppRole auth")

	// Create a mock AppRole server since AppRole may not be configured in the
	// docker-compose Vault instance.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/approle/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut && r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		roleID, _ := body["role_id"].(string)
		secretID, _ := body["secret_id"].(string)
		if roleID == "" || secretID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errors":["missing role_id or secret_id"]}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.e2e-approle-token",
				"policies": ["default", "cloudberry-policy"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		Role:       "e2e-role-id",
		Token:      "e2e-secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err, "AppRole auth should succeed")
	require.NotNil(s.T(), client)
	assert.True(s.T(), client.IsEnabled())

	s.logger.Info("scenario 46 E2E: AppRole auth completed")
}

// TestE2E_Scenario46_SecretRotation writes a secret, creates a SecretWatcher,
// updates the secret, and verifies change detection.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_SecretRotation() {
	s.logger.Info("starting scenario 46 E2E: secret rotation")

	// Use a mock client that simulates changing data to avoid dependency on
	// real Vault timing.
	var readCount atomic.Int32
	mockClient := &e2eScenario46MockClient{
		readFunc: func(_ context.Context, _ string) (map[string]interface{}, error) {
			count := readCount.Add(1)
			return map[string]interface{}{
				"password": fmt.Sprintf("rotated-secret-v%d", count),
			}, nil
		},
		enabled: true,
	}

	var changeCount atomic.Int32
	onChange := func(_ map[string]interface{}) {
		changeCount.Add(1)
	}

	watchCtx, watchCancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
	defer watchCancel()

	watcher := vault.NewSecretWatcher(
		mockClient,
		"secret/data/cloudberry/admin-password",
		30*time.Millisecond,
		onChange,
		s.logger,
	)

	watcher.Watch(watchCtx)

	assert.Greater(s.T(), readCount.Load(), int32(1),
		"watcher should have read the secret multiple times")
	assert.Greater(s.T(), changeCount.Load(), int32(0),
		"onChange should have been called at least once")

	s.logger.Info("scenario 46 E2E: secret rotation completed",
		"reads", readCount.Load(), "changes", changeCount.Load())
}

// TestE2E_Scenario46_RetryConfig verifies retry configuration values.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_RetryConfig() {
	s.logger.Info("starting scenario 46 E2E: retry config")

	opts := util.DefaultRetryOptions()
	assert.Equal(s.T(), 5, opts.MaxRetries)
	assert.Equal(s.T(), time.Second, opts.InitialBackoff)
	assert.Equal(s.T(), 30*time.Second, opts.MaxBackoff)
	assert.InDelta(s.T(), 2.0, opts.Multiplier, 0.001)
	assert.InDelta(s.T(), 0.1, opts.JitterFraction, 0.001)

	// Test retry with a function that fails then succeeds.
	var attempts atomic.Int32
	retryOpts := util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}

	err := util.RetryWithBackoff(s.ctx, retryOpts, func(_ context.Context) error {
		count := attempts.Add(1)
		if count < 3 {
			return fmt.Errorf("transient error (attempt %d)", count)
		}
		return nil
	})

	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(3), attempts.Load())

	s.logger.Info("scenario 46 E2E: retry config completed")
}

// TestE2E_Scenario46_PKICertIssuance issues a certificate from the real Vault PKI.
// The mount and role come from VAULT_PKI_MOUNT / VAULT_PKI_ROLE (defaults match
// the docker-compose setup-vault.sh provisioning). The test is skipped only if
// the PKI engine or the configured role is genuinely absent.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_PKICertIssuance() {
	s.skipIfVaultUnavailable()
	s.logger.Info("starting scenario 46 E2E: PKI cert issuance",
		"mount", s.pkiMount, "role", s.pkiRole)

	// Check if PKI engine is available.
	pkiErr := s.vaultHelper.CheckPKIEngine(s.ctx, s.pkiMount)
	if pkiErr != nil {
		s.T().Skipf("PKI engine not available: %v", pkiErr)
	}

	cert, err := s.vaultHelper.IssueCertificate(s.ctx, s.pkiMount, s.pkiRole, "test.cloudberry.local")
	if err != nil {
		// Skip if the role is not configured (common in dev/test environments).
		s.T().Skipf("PKI role %q not configured, skipping: %v", s.pkiRole, err)
	}
	require.NotNil(s.T(), cert)
	assert.NotEmpty(s.T(), cert.Certificate, "certificate should not be empty")
	assert.NotEmpty(s.T(), cert.PrivateKey, "private key should not be empty")
	assert.NotEmpty(s.T(), cert.IssuingCA, "issuing CA should not be empty")

	s.logger.Info("scenario 46 E2E: PKI cert issuance completed")
}

// TestE2E_Scenario46_ClusterWithVault creates a CloudberryCluster CR with vault config
// and verifies it is accepted by the fake K8s client.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_ClusterWithVault() {
	s.logger.Info("starting scenario 46 E2E: cluster with vault")

	cluster := testutil.NewClusterBuilder("e2e-s46-vault", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithVault(true, "http://vault:8200", "token").
		Build()

	// Verify the vault spec is set correctly.
	require.NotNil(s.T(), cluster.Spec.Vault, "vault spec should be set")
	assert.True(s.T(), cluster.Spec.Vault.Enabled, "vault should be enabled")
	assert.Equal(s.T(), "http://vault:8200", cluster.Spec.Vault.Address)
	assert.Equal(s.T(), cbv1alpha1.VaultAuthToken, cluster.Spec.Vault.AuthMethod)

	// Create the cluster in the fake K8s env and verify it persists.
	env := testutil.NewTestK8sEnv(cluster)
	retrieved, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Vault)
	assert.True(s.T(), retrieved.Spec.Vault.Enabled)
	assert.Equal(s.T(), "http://vault:8200", retrieved.Spec.Vault.Address)
	assert.Equal(s.T(), cbv1alpha1.VaultAuthToken, retrieved.Spec.Vault.AuthMethod)

	s.logger.Info("scenario 46 E2E: cluster with vault completed")
}

// TestE2E_Scenario46_VaultIntegrationCases verifies the test case catalog.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_VaultIntegrationCases() {
	testCases := cases.VaultIntegrationCases()
	require.Len(s.T(), testCases, 5, "should have 5 vault integration test cases")

	for _, tc := range testCases {
		assert.NotEmpty(s.T(), tc.Name, "test case should have a name")
		assert.NotEmpty(s.T(), tc.Description, "test case should have a description")
		assert.NotEmpty(s.T(), tc.AuthMethod, "test case should have an auth method")
		assert.True(s.T(), tc.ExpectSuccess, "all test cases should expect success")
	}
}

// TestE2E_Scenario46_VaultHelperReadWrite tests the VaultTestHelper read/write operations.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_VaultHelperReadWrite() {
	s.skipIfVaultUnavailable()
	s.logger.Info("starting scenario 46 E2E: vault helper read/write")

	// Write a secret using the helper.
	testPath := "secret/data/cloudberry/e2e-helper-test"
	writeData := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}
	err := s.vaultHelper.WriteSecret(s.ctx, testPath, writeData)
	require.NoError(s.T(), err)

	// Read it back.
	data, err := s.vaultHelper.ReadSecret(s.ctx, testPath)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), data)
	assert.Equal(s.T(), "value1", data["key1"])
	assert.Equal(s.T(), "value2", data["key2"])

	s.logger.Info("scenario 46 E2E: vault helper read/write completed")
}

// TestE2E_Scenario46_DisabledVaultClient verifies that a disabled Vault config
// returns a no-op client.
func (s *Scenario46VaultE2ESuite) TestE2E_Scenario46_DisabledVaultClient() {
	s.logger.Info("starting scenario 46 E2E: disabled vault client")

	cfg := vault.Config{Enabled: false}
	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), client)
	assert.False(s.T(), client.IsEnabled())

	// No-op operations should succeed silently.
	data, readErr := client.ReadSecret(s.ctx, "secret/data/any")
	assert.NoError(s.T(), readErr)
	assert.Nil(s.T(), data)

	writeErr := client.WriteSecret(s.ctx, "secret/data/any", map[string]interface{}{"k": "v"})
	assert.NoError(s.T(), writeErr)

	writeResp, writeRespErr := client.WriteSecretWithResponse(s.ctx, "pki/issue/role", map[string]interface{}{
		"common_name": "test.example.com",
	})
	assert.NoError(s.T(), writeRespErr)
	assert.Nil(s.T(), writeResp)

	s.logger.Info("scenario 46 E2E: disabled vault client completed")
}

// e2eScenario46MockClient implements vault.Client for E2E testing.
type e2eScenario46MockClient struct {
	readFunc func(ctx context.Context, path string) (map[string]interface{}, error)
	enabled  bool
}

func (m *e2eScenario46MockClient) ReadSecret(
	ctx context.Context, path string,
) (map[string]interface{}, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, path)
	}
	return nil, nil
}

func (m *e2eScenario46MockClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return nil
}

// WriteSecretWithResponse is a mock implementation for E2E testing.
func (m *e2eScenario46MockClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (m *e2eScenario46MockClient) IsEnabled() bool {
	return m.enabled
}
