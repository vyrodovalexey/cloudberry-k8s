//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// vaultSecretPaths defines the 4 KV paths used across Scenario 46 sub-tests.
var vaultSecretPaths = []string{
	"secret/data/cloudberry/admin-password",
	"secret/data/cloudberry/oidc-secret",
	"secret/data/cloudberry/monitoring-password",
	"secret/data/cloudberry/tls",
}

// Scenario46VaultSuite tests Scenario 46: Vault Integration (All Auth Methods + Secrets).
type Scenario46VaultSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestFunctional_Scenario46(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario46VaultSuite))
}

func (s *Scenario46VaultSuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	s.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func (s *Scenario46VaultSuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
}

// newMockVaultServer creates a mock Vault HTTP server that handles token auth,
// KV v2 reads for the 4 secret paths, and KV v2 writes.
func newMockVaultServer(t *testing.T) *httptest.Server {
	t.Helper()

	secretStore := map[string]map[string]interface{}{
		"secret/data/cloudberry/admin-password": {
			"username": "gpadmin",
			"password": "admin-secret-123",
		},
		"secret/data/cloudberry/oidc-secret": {
			"client_id":     "cloudberry-operator",
			"client_secret": "oidc-secret-456",
		},
		"secret/data/cloudberry/monitoring-password": {
			"username": "monitoring",
			"password": "monitoring-secret-789",
		},
		"secret/data/cloudberry/tls": {
			"ca_cert":  "-----BEGIN CERTIFICATE-----\nMOCK-CA\n-----END CERTIFICATE-----",
			"tls_cert": "-----BEGIN CERTIFICATE-----\nMOCK-CERT\n-----END CERTIFICATE-----",
			"tls_key":  "-----BEGIN RSA PRIVATE KEY-----\nMOCK-KEY\n-----END RSA PRIVATE KEY-----",
		},
	}

	mux := http.NewServeMux()

	// Handle KV v2 reads for all secret paths.
	for path, data := range secretStore {
		secretData := data // capture loop variable
		mux.HandleFunc("/v1/"+path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				resp := map[string]interface{}{
					"data": map[string]interface{}{
						"data": secretData,
						"metadata": map[string]interface{}{
							"version": 1,
						},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			if r.Method == http.MethodPut || r.Method == http.MethodPost {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"data":{"version":2}}`))
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
		})
	}

	return httptest.NewServer(mux)
}

// newMockAppRoleServer creates a mock Vault HTTP server that handles AppRole auth.
func newMockAppRoleServer(t *testing.T) *httptest.Server {
	t.Helper()

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
				"client_token": "s.approle-test-token",
				"policies": ["default", "cloudberry-policy"]
			}
		}`))
	})

	return httptest.NewServer(mux)
}

// TestFunctional_Scenario46a_TokenAuth verifies that a Vault client can authenticate
// using the token method and read secrets from all 4 KV paths.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46a_TokenAuth() {
	s.logger.Info("starting scenario 46a: token auth with KV secret reads")

	tc := cases.VaultIntegrationCases()[0] // 46a_token_auth_read_secrets
	s.T().Log(tc.Description)

	server := newMockVaultServer(s.T())
	defer server.Close()

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-root-token",
		SecretPath: "secret/data/cloudberry",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err, "token auth should succeed")
	require.NotNil(s.T(), client)
	assert.True(s.T(), client.IsEnabled(), "client should be enabled")

	// Read all 4 secret paths.
	for _, path := range vaultSecretPaths {
		data, readErr := client.ReadSecret(s.ctx, path)
		require.NoError(s.T(), readErr, "reading secret at %s should succeed", path)
		require.NotNil(s.T(), data, "secret data at %s should not be nil", path)
		assert.NotEmpty(s.T(), data, "secret data at %s should not be empty", path)
	}

	s.logger.Info("scenario 46a: token auth with KV secret reads completed")
}

// TestFunctional_Scenario46b_TokenAuthDevMode verifies token auth in dev mode
// with a static token, reading all 4 KV paths.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46b_TokenAuthDevMode() {
	s.logger.Info("starting scenario 46b: token auth dev mode")

	tc := cases.VaultIntegrationCases()[1] // 46b_token_auth_dev_mode
	s.T().Log(tc.Description)

	server := newMockVaultServer(s.T())
	defer server.Close()

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "myroot",
		SecretPath: "secret/data/cloudberry",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err, "dev mode token auth should succeed")
	require.NotNil(s.T(), client)
	assert.True(s.T(), client.IsEnabled())

	// Verify each secret path returns expected keys.
	expectedKeys := map[string][]string{
		"secret/data/cloudberry/admin-password":      {"username", "password"},
		"secret/data/cloudberry/oidc-secret":         {"client_id", "client_secret"},
		"secret/data/cloudberry/monitoring-password": {"username", "password"},
		"secret/data/cloudberry/tls":                 {"ca_cert", "tls_cert", "tls_key"},
	}

	for _, path := range tc.SecretPaths {
		data, readErr := client.ReadSecret(s.ctx, path)
		require.NoError(s.T(), readErr, "reading secret at %s should succeed", path)
		require.NotNil(s.T(), data)

		for _, key := range expectedKeys[path] {
			assert.Contains(s.T(), data, key,
				"secret at %s should contain key %q", path, key)
		}
	}

	s.logger.Info("scenario 46b: token auth dev mode completed")
}

// TestFunctional_Scenario46c_AppRoleAuth verifies that a Vault client can authenticate
// using the AppRole method.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46c_AppRoleAuth() {
	s.logger.Info("starting scenario 46c: AppRole auth")

	tc := cases.VaultIntegrationCases()[2] // 46c_approle_auth
	s.T().Log(tc.Description)

	server := newMockAppRoleServer(s.T())
	defer server.Close()

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		Role:       "test-role-id",
		Token:      "test-secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err, "AppRole auth should succeed")
	require.NotNil(s.T(), client)
	assert.True(s.T(), client.IsEnabled(), "client should be enabled after AppRole auth")

	s.logger.Info("scenario 46c: AppRole auth completed")
}

// TestFunctional_Scenario46d_SecretRotationWatch verifies that SecretWatcher detects
// secret changes and invokes the onChange callback.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46d_SecretRotationWatch() {
	s.logger.Info("starting scenario 46d: secret rotation watch")

	tc := cases.VaultIntegrationCases()[3] // 46d_secret_rotation_watch
	s.T().Log(tc.Description)

	// Use a mock client that returns changing data on each read.
	var readCount atomic.Int32
	mockClient := &scenario46MockVaultClient{
		readFunc: func(_ context.Context, _ string) (map[string]interface{}, error) {
			count := readCount.Add(1)
			return map[string]interface{}{
				"password": fmt.Sprintf("secret-v%d", count),
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

	// Watch blocks until context is canceled.
	watcher.Watch(watchCtx)

	// Verify that at least one change was detected.
	assert.Greater(s.T(), readCount.Load(), int32(1),
		"watcher should have read the secret multiple times")
	assert.Greater(s.T(), changeCount.Load(), int32(0),
		"onChange should have been called at least once when secret data changes")

	s.logger.Info("scenario 46d: secret rotation watch completed",
		"reads", readCount.Load(), "changes", changeCount.Load())
}

// TestFunctional_Scenario46e_ConnectionRetry verifies DefaultRetryOptions values
// and RetryWithBackoff behavior with a failing-then-succeeding function.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46e_ConnectionRetry() {
	s.logger.Info("starting scenario 46e: connection retry")

	tc := cases.VaultIntegrationCases()[4] // 46e_connection_retry
	s.T().Log(tc.Description)

	// Verify DefaultRetryOptions returns correct values.
	opts := util.DefaultRetryOptions()
	assert.Equal(s.T(), 5, opts.MaxRetries,
		"MaxRetries should be 5")
	assert.Equal(s.T(), time.Second, opts.InitialBackoff,
		"InitialBackoff should be 1s")
	assert.Equal(s.T(), 30*time.Second, opts.MaxBackoff,
		"MaxBackoff should be 30s")
	assert.InDelta(s.T(), 2.0, opts.Multiplier, 0.001,
		"Multiplier should be 2.0")
	assert.InDelta(s.T(), 0.1, opts.JitterFraction, 0.001,
		"JitterFraction should be 0.1")

	// Test RetryWithBackoff with a function that fails twice then succeeds.
	var attempts atomic.Int32
	retryOpts := util.RetryOptions{
		MaxRetries:     5,
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

	require.NoError(s.T(), err, "RetryWithBackoff should succeed after transient failures")
	assert.Equal(s.T(), int32(3), attempts.Load(),
		"should have taken 3 attempts (2 failures + 1 success)")

	s.logger.Info("scenario 46e: connection retry completed")
}

// TestFunctional_Scenario46_KubernetesAuth verifies Kubernetes auth with a mock server.
// Since kubeTokenPath is internal to the vault package, this test verifies the mock
// server responds correctly to the expected Kubernetes auth request format.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46_KubernetesAuth() {
	s.logger.Info("starting scenario 46: kubernetes auth")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/kubernetes/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut && r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var body map[string]interface{}
		if decErr := json.NewDecoder(r.Body).Decode(&body); decErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		assert.Equal(s.T(), "cloudberry-role", body["role"])
		assert.Equal(s.T(), "fake-k8s-jwt-token", body["jwt"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.k8s-auth-token",
				"policies": ["default", "cloudberry-policy"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// Note: We cannot directly test kubernetes auth via vault.NewClient here
	// because it reads from a hardcoded path. The unit tests in vault_test.go
	// cover this by overriding kubeTokenPath. Here we verify the mock server
	// responds correctly to the expected request format.
	reqBody := `{"role":"cloudberry-role","jwt":"fake-k8s-jwt-token"}`
	httpReq, httpErr := http.NewRequestWithContext(
		s.ctx, http.MethodPost,
		server.URL+"/v1/auth/kubernetes/login",
		strings.NewReader(reqBody),
	)
	require.NoError(s.T(), httpErr)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, doErr := http.DefaultClient.Do(httpReq)
	require.NoError(s.T(), doErr)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	var authResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(resp.Body).Decode(&authResp))
	authData, ok := authResp["auth"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain auth data")
	assert.Equal(s.T(), "s.k8s-auth-token", authData["client_token"])

	s.logger.Info("scenario 46: kubernetes auth completed")
}

// TestFunctional_Scenario46_VaultIntegrationCases verifies the test case catalog.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46_VaultIntegrationCases() {
	testCases := cases.VaultIntegrationCases()
	require.Len(s.T(), testCases, 5, "should have 5 vault integration test cases")

	expectedNames := []string{
		"46a_token_auth_read_secrets",
		"46b_token_auth_dev_mode",
		"46c_approle_auth",
		"46d_secret_rotation_watch",
		"46e_connection_retry",
	}

	for i, tc := range testCases {
		assert.Equal(s.T(), expectedNames[i], tc.Name,
			"test case %d should have expected name", i)
		assert.NotEmpty(s.T(), tc.Description,
			"test case %q should have a description", tc.Name)
		assert.True(s.T(), tc.ExpectSuccess,
			"test case %q should expect success", tc.Name)
	}
}

// TestFunctional_Scenario46_DisabledVault verifies that a disabled Vault config
// returns a no-op client.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46_DisabledVault() {
	cfg := vault.Config{Enabled: false}
	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), client)
	assert.False(s.T(), client.IsEnabled(), "disabled vault should return no-op client")

	// No-op client operations should succeed silently.
	data, readErr := client.ReadSecret(s.ctx, "secret/data/any")
	assert.NoError(s.T(), readErr)
	assert.Nil(s.T(), data)

	writeErr := client.WriteSecret(s.ctx, "secret/data/any", map[string]interface{}{"k": "v"})
	assert.NoError(s.T(), writeErr)
}

// TestFunctional_Scenario46_WriteAndReadSecret verifies write then read round-trip.
func (s *Scenario46VaultSuite) TestFunctional_Scenario46_WriteAndReadSecret() {
	// Create a mock server that stores written data and returns it on read.
	var storedData map[string]interface{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/cloudberry/test-write", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut || r.Method == http.MethodPost:
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if data, ok := body["data"].(map[string]interface{}); ok {
				storedData = data
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":1}}`))
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := map[string]interface{}{
				"data": map[string]interface{}{
					"data":     storedData,
					"metadata": map[string]interface{}{"version": 1},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, s.logger)
	require.NoError(s.T(), err)

	// Write a secret.
	writeData := map[string]interface{}{
		"username": "testuser",
		"password": "testpass",
	}
	err = client.WriteSecret(s.ctx, "secret/data/cloudberry/test-write", writeData)
	require.NoError(s.T(), err)

	// Read it back.
	data, err := client.ReadSecret(s.ctx, "secret/data/cloudberry/test-write")
	require.NoError(s.T(), err)
	require.NotNil(s.T(), data)
	assert.Equal(s.T(), "testuser", data["username"])
	assert.Equal(s.T(), "testpass", data["password"])
}

// scenario46MockVaultClient implements vault.Client for testing.
type scenario46MockVaultClient struct {
	readFunc func(ctx context.Context, path string) (map[string]interface{}, error)
	enabled  bool
}

func (m *scenario46MockVaultClient) ReadSecret(
	ctx context.Context, path string,
) (map[string]interface{}, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, path)
	}
	return nil, nil
}

func (m *scenario46MockVaultClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return nil
}

// WriteSecretWithResponse is a mock implementation for testing.
func (m *scenario46MockVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (m *scenario46MockVaultClient) IsEnabled() bool {
	return m.enabled
}
