package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func TestNewClient_Disabled(t *testing.T) {
	cfg := Config{Enabled: false}
	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.False(t, client.IsEnabled())
}

func TestNewClient_EnabledNoAddress(t *testing.T) {
	cfg := Config{Enabled: true, Address: ""}
	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "vault address is required")
}

func TestNoopClient(t *testing.T) {
	client := &noopClient{}

	t.Run("IsEnabled returns false", func(t *testing.T) {
		assert.False(t, client.IsEnabled())
	})

	t.Run("ReadSecret returns nil", func(t *testing.T) {
		data, err := client.ReadSecret(context.Background(), "secret/path")
		assert.NoError(t, err)
		assert.Nil(t, data)
	})

	t.Run("WriteSecret returns nil", func(t *testing.T) {
		err := client.WriteSecret(context.Background(), "secret/path", map[string]interface{}{"key": "value"})
		assert.NoError(t, err)
	})
}

func TestNoopClient_ImplementsInterface(t *testing.T) {
	var _ Client = &noopClient{}
}

func TestVaultClient_ImplementsInterface(t *testing.T) {
	var _ Client = &vaultClient{}
}

func TestNewSecretWatcher(t *testing.T) {
	client := &noopClient{}
	logger := slog.Default()
	onChange := func(data map[string]interface{}) {}

	watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
	require.NotNil(t, watcher)
	assert.Equal(t, "secret/path", watcher.path)
	assert.Equal(t, time.Minute, watcher.interval)
}

func TestSecretWatcher_Watch_ContextCancellation(t *testing.T) {
	client := &noopClient{}
	logger := slog.Default()
	onChange := func(data map[string]interface{}) {}

	watcher := NewSecretWatcher(client, "secret/path", 50*time.Millisecond, onChange, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		watcher.Watch(ctx)
		close(done)
	}()

	// Cancel after a short delay
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success - Watch returned
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after context cancellation")
	}
}

func TestSecretWatcher_CheckForChanges(t *testing.T) {
	t.Run("no data returns early", func(t *testing.T) {
		client := &noopClient{}
		logger := slog.Default()
		called := false
		onChange := func(data map[string]interface{}) {
			called = true
		}

		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
		watcher.checkForChanges(context.Background())
		assert.False(t, called)
	})

	t.Run("error reading secret", func(t *testing.T) {
		client := &mockVaultClient{
			readErr: fmt.Errorf("connection refused"),
		}
		logger := slog.Default()
		called := false
		onChange := func(data map[string]interface{}) {
			called = true
		}

		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
		watcher.checkForChanges(context.Background())
		assert.False(t, called)
	})

	t.Run("first read sets hash without calling onChange", func(t *testing.T) {
		client := &mockVaultClient{
			readData: map[string]interface{}{"key": "value"},
		}
		logger := slog.Default()
		called := false
		onChange := func(data map[string]interface{}) {
			called = true
		}

		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
		watcher.checkForChanges(context.Background())
		assert.False(t, called, "onChange should not be called on first read")
		assert.NotEmpty(t, watcher.lastHash)
	})

	t.Run("changed data calls onChange", func(t *testing.T) {
		client := &mockVaultClient{
			readData: map[string]interface{}{"key": "value2"},
		}
		logger := slog.Default()
		called := false
		onChange := func(data map[string]interface{}) {
			called = true
		}

		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
		watcher.lastHash = "old-hash" // Simulate previous read
		watcher.checkForChanges(context.Background())
		assert.True(t, called, "onChange should be called when data changes")
	})

	t.Run("same data does not call onChange", func(t *testing.T) {
		client := &mockVaultClient{
			readData: map[string]interface{}{"key": "value"},
		}
		logger := slog.Default()
		called := false
		onChange := func(data map[string]interface{}) {
			called = true
		}

		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, logger)
		// First read to set hash
		watcher.checkForChanges(context.Background())
		called = false
		// Second read with same data
		watcher.checkForChanges(context.Background())
		assert.False(t, called, "onChange should not be called when data hasn't changed")
	})
}

func TestConfig_RetryOpts(t *testing.T) {
	cfg := Config{
		Enabled:   false,
		RetryOpts: util.DefaultRetryOptions(),
	}
	assert.Equal(t, 5, cfg.RetryOpts.MaxRetries)
}

// mockVaultClient is a mock implementation of the Client interface for testing.
type mockVaultClient struct {
	readData map[string]interface{}
	readErr  error
	writeErr error
	enabled  bool
}

func (m *mockVaultClient) ReadSecret(_ context.Context, _ string) (map[string]interface{}, error) {
	return m.readData, m.readErr
}

func (m *mockVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return m.writeErr
}

// WriteSecretWithResponse is a mock implementation for testing.
func (m *mockVaultClient) WriteSecretWithResponse(_ context.Context, _ string, _ map[string]interface{}) (map[string]interface{}, error) {
	return m.readData, m.writeErr
}

func (m *mockVaultClient) IsEnabled() bool {
	return m.enabled
}

func TestNewClient_TokenAuth_MissingToken(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "token",
		Token:      "",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "vault token is required")
}

func TestNewClient_KubernetesAuth_NoServiceAccountToken(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "kubernetes",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	// This will fail because the service account token file doesn't exist.
	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestNewClient_AppRoleAuth_NoServer(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:1",
		AuthMethod: "approle",
		Role:       "role-id",
		Token:      "secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestNewClient_UnsupportedAuthMethod(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "unsupported",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "unsupported vault auth method")
}

func TestNewClient_MissingAddress(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Address: "",
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "vault address is required")
}

func TestNoopClient_ReadWriteSecret(t *testing.T) {
	client := &noopClient{}

	t.Run("ReadSecret returns nil data and no error", func(t *testing.T) {
		data, err := client.ReadSecret(context.Background(), "secret/data/test")
		assert.NoError(t, err)
		assert.Nil(t, data)
	})

	t.Run("WriteSecret returns no error", func(t *testing.T) {
		err := client.WriteSecret(context.Background(), "secret/data/test",
			map[string]interface{}{"username": "admin", "password": "secret"})
		assert.NoError(t, err)
	})

	t.Run("IsEnabled returns false", func(t *testing.T) {
		assert.False(t, client.IsEnabled())
	})
}

func TestMockVaultClient_ReadSecret(t *testing.T) {
	tests := []struct {
		name     string
		client   *mockVaultClient
		wantData map[string]interface{}
		wantErr  bool
	}{
		{
			name:     "successful read",
			client:   &mockVaultClient{readData: map[string]interface{}{"key": "value"}},
			wantData: map[string]interface{}{"key": "value"},
			wantErr:  false,
		},
		{
			name:     "read error",
			client:   &mockVaultClient{readErr: fmt.Errorf("not found")},
			wantData: nil,
			wantErr:  true,
		},
		{
			name:     "nil data",
			client:   &mockVaultClient{readData: nil},
			wantData: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.client.ReadSecret(context.Background(), "secret/path")
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantData, data)
		})
	}
}

func TestMockVaultClient_WriteSecret(t *testing.T) {
	tests := []struct {
		name    string
		client  *mockVaultClient
		wantErr bool
	}{
		{
			name:    "successful write",
			client:  &mockVaultClient{},
			wantErr: false,
		},
		{
			name:    "write error",
			client:  &mockVaultClient{writeErr: fmt.Errorf("permission denied")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.client.WriteSecret(context.Background(), "secret/path",
				map[string]interface{}{"key": "value"})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMockVaultClient_IsEnabled(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		client := &mockVaultClient{enabled: true}
		assert.True(t, client.IsEnabled())
	})

	t.Run("disabled", func(t *testing.T) {
		client := &mockVaultClient{enabled: false}
		assert.False(t, client.IsEnabled())
	})
}

func TestSecretWatcher_CheckForChanges_WithMockClient(t *testing.T) {
	t.Run("data changes trigger onChange", func(t *testing.T) {
		callCount := 0
		onChange := func(_ map[string]interface{}) {
			callCount++
		}

		client := &mockVaultClient{
			readData: map[string]interface{}{"key": "value1"},
		}
		watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, slog.Default())

		// First check sets hash.
		watcher.checkForChanges(context.Background())
		assert.Equal(t, 0, callCount)

		// Change data.
		client.readData = map[string]interface{}{"key": "value2"}
		watcher.checkForChanges(context.Background())
		assert.Equal(t, 1, callCount)
	})
}

func TestNewClient_DisabledReturnsNoopClient(t *testing.T) {
	cfg := Config{Enabled: false}
	client, err := NewClient(context.Background(), cfg, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.False(t, client.IsEnabled())

	// Verify it's a noopClient by testing behavior.
	data, err := client.ReadSecret(context.Background(), "any/path")
	assert.NoError(t, err)
	assert.Nil(t, data)

	err = client.WriteSecret(context.Background(), "any/path", map[string]interface{}{"k": "v"})
	assert.NoError(t, err)
}

func TestConfig_Fields(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "https://vault.example.com:8200",
		AuthMethod: "kubernetes",
		AuthPath:   "auth/kubernetes",
		Role:       "cloudberry",
		Token:      "s.token123",
		SecretPath: "secret/data/cloudberry",
		TLSCACert:  "/etc/vault/ca.pem",
		RetryOpts:  util.DefaultRetryOptions(),
	}

	assert.True(t, cfg.Enabled)
	assert.Equal(t, "https://vault.example.com:8200", cfg.Address)
	assert.Equal(t, "kubernetes", cfg.AuthMethod)
	assert.Equal(t, "cloudberry", cfg.Role)
	assert.Equal(t, 5, cfg.RetryOpts.MaxRetries)
}

func TestNewClient_TokenAuth_WithMockServer(t *testing.T) {
	// Create a mock Vault server that accepts token auth
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.True(t, client.IsEnabled())
}

func TestVaultClient_ReadSecret_WithMockServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/test", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"data": {
				"data": {
					"username": "admin",
					"password": "secret123"
				},
				"metadata": {
					"version": 1
				}
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/test")
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Equal(t, "admin", data["username"])
	assert.Equal(t, "secret123", data["password"])
}

func TestVaultClient_ReadSecret_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/missing")
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestVaultClient_WriteSecret_WithMockServer(t *testing.T) {
	var receivedData map[string]interface{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)
			receivedData = body
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":1}}`))
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	err = client.WriteSecret(context.Background(), "secret/data/test", map[string]interface{}{
		"username": "admin",
		"password": "new-secret",
	})
	require.NoError(t, err)

	// Verify KV v2 data wrapping
	require.NotNil(t, receivedData)
	dataField, ok := receivedData["data"]
	assert.True(t, ok, "data should be wrapped in 'data' key for KV v2")
	dataMap, ok := dataField.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "admin", dataMap["username"])
}

func TestVaultClient_ReadSecret_NonKVv2(t *testing.T) {
	// Test reading a secret that is NOT KV v2 (no nested "data" key)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/test", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"data": {
				"username": "admin",
				"password": "secret123"
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/test")
	require.NoError(t, err)
	require.NotNil(t, data)
	// When data doesn't have a nested "data" key, it returns the raw data
	assert.Equal(t, "admin", data["username"])
}

func TestVaultClient_AppRoleAuth_WithMockServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/approle/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.approle-token",
				"policies": ["default"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		Role:       "my-role-id",
		Token:      "my-secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.True(t, client.IsEnabled())
}

func TestVaultClient_AppRoleAuth_CustomPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/custom-approle/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.custom-token",
				"policies": ["default"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		AuthPath:   "auth/custom-approle",
		Role:       "my-role-id",
		Token:      "my-secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestVaultClient_AppRoleAuth_NoAuthData(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/approle/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		Role:       "my-role-id",
		Token:      "my-secret-id",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "no auth data")
}

func TestNewClient_InvalidTLSCACert(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "https://127.0.0.1:8200",
		AuthMethod: "token",
		Token:      "s.test",
		TLSCACert:  "/nonexistent/ca.pem",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "configuring vault TLS")
}

func TestVaultClient_WriteSecret_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	err = client.WriteSecret(context.Background(), "secret/data/test", map[string]interface{}{
		"key": "value",
	})
	assert.Error(t, err)
}

func TestVaultClient_IsEnabled(t *testing.T) {
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	assert.True(t, client.IsEnabled())
}

func TestVaultClient_ReadSecret_ContextCancelled(t *testing.T) {
	// unblock releases the handler after the client has timed out, replacing
	// the previous fixed 5s sleep: teardown is instant and deterministic.
	unblock := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/test", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	defer close(unblock)

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.ReadSecret(ctx, "secret/data/test")
	assert.Error(t, err)
}

func TestNewClient_KubernetesAuth_CustomPath(t *testing.T) {
	// This tests the kubernetes auth path resolution with custom auth path
	// It will fail because the service account token file doesn't exist
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "kubernetes",
		AuthPath:   "auth/custom-k8s",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestNewClient_DefaultRetryOpts(t *testing.T) {
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test",
		RetryOpts:  util.RetryOptions{MaxRetries: 0}, // Should use defaults
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestSecretWatcher_Watch_WithChanges(t *testing.T) {
	callCount := 0
	readCount := 0
	client := &changingMockClient{
		readFunc: func() (map[string]interface{}, error) {
			readCount++
			return map[string]interface{}{"key": fmt.Sprintf("value-%d", readCount)}, nil
		},
	}

	onChange := func(_ map[string]interface{}) {
		callCount++
	}

	watcher := NewSecretWatcher(client, "secret/path", 30*time.Millisecond, onChange, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	watcher.Watch(ctx)

	// Should have detected at least one change (first read sets hash, subsequent reads detect changes)
	assert.Greater(t, readCount, 1)
}

// changingMockClient returns different data on each read.
type changingMockClient struct {
	readFunc func() (map[string]interface{}, error)
}

func (c *changingMockClient) ReadSecret(_ context.Context, _ string) (map[string]interface{}, error) {
	return c.readFunc()
}

func (c *changingMockClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

// WriteSecretWithResponse is a mock implementation for testing.
func (c *changingMockClient) WriteSecretWithResponse(_ context.Context, _ string, _ map[string]interface{}) (map[string]interface{}, error) {
	return c.readFunc()
}

func (c *changingMockClient) IsEnabled() bool {
	return true
}

func TestSecretWatcher_Watch_ContextAlreadyCancelled(t *testing.T) {
	client := &noopClient{}
	onChange := func(_ map[string]interface{}) {}

	watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	done := make(chan struct{})
	go func() {
		watcher.Watch(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Success - Watch returned immediately.
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after context was already cancelled")
	}
}

func TestVaultClient_ReadSecret_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/error", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errors":["internal server error"]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/error")
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestVaultClient_WriteSecret_ContextCancelled(t *testing.T) {
	// unblock releases the handler after the client has timed out (see
	// TestVaultClient_ReadSecret_ContextCancelled): instant teardown.
	unblock := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	defer close(unblock)

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = client.WriteSecret(ctx, "secret/data/slow", map[string]interface{}{"key": "value"})
	assert.Error(t, err)
}

func TestNewClient_WithLogger(t *testing.T) {
	cfg := Config{Enabled: false}
	logger := slog.Default()
	client, err := NewClient(context.Background(), cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestVaultClient_ReadSecret_KVv2DataWrapping(t *testing.T) {
	// Test that KV v2 data is properly unwrapped from the "data" key.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/kvv2", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{
					"db_user":     "admin",
					"db_password": "secret",
				},
				"metadata": map[string]interface{}{
					"version": 3,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/kvv2")
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Equal(t, "admin", data["db_user"])
	assert.Equal(t, "secret", data["db_password"])
}

func TestVaultClient_WriteSecret_KVv2DataWrapping(t *testing.T) {
	// Verify that WriteSecret wraps data in "data" key for KV v2.
	var receivedBody map[string]interface{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/kvv2-write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":1}}`))
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	require.NoError(t, err)

	err = client.WriteSecret(context.Background(), "secret/data/kvv2-write", map[string]interface{}{
		"key1": "val1",
		"key2": "val2",
	})
	require.NoError(t, err)

	// Verify the data was wrapped.
	require.NotNil(t, receivedBody)
	dataField, ok := receivedBody["data"].(map[string]interface{})
	require.True(t, ok, "data should be wrapped in 'data' key")
	assert.Equal(t, "val1", dataField["key1"])
	assert.Equal(t, "val2", dataField["key2"])
}

func TestNewClient_NilLogger(t *testing.T) {
	// Test that NewClient works with nil logger.
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestVaultClient_ReadSecret_EmptyData(t *testing.T) {
	// Test reading a secret that returns empty data.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/empty", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Return a response with nil data (simulates deleted secret).
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/empty")
	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "secret not found")
}

func TestNewClient_KubernetesAuth_WithMockServer(t *testing.T) {
	// Test kubernetes auth with a mock server that provides a valid response.
	// The test will fail at reading the service account token file, which is expected.
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "kubernetes",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	assert.Error(t, err)
	assert.Nil(t, client)
	// Should fail because the service account token file doesn't exist.
	assert.Contains(t, err.Error(), "authenticating with vault")
}

func TestNewClient_TokenAuth_EmptyToken(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:8200",
		AuthMethod: "token",
		Token:      "",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "vault token is required")
}

func TestVaultClient_ReadSecret_RetryOnError(t *testing.T) {
	// Test that ReadSecret retries on transient errors.
	callCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/retry-test", func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		if callCount == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"data":{"key":"value"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 1.5},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.ReadSecret(context.Background(), "secret/data/retry-test")
	require.NoError(t, err)
	assert.Equal(t, "value", data["key"])
}

func TestSecretWatcher_Watch_DetectsChanges(t *testing.T) {
	// Test that Watch detects changes and calls onChange.
	readCount := 0
	changeCount := 0
	client := &changingMockClient{
		readFunc: func() (map[string]interface{}, error) {
			readCount++
			return map[string]interface{}{"version": fmt.Sprintf("%d", readCount)}, nil
		},
	}

	onChange := func(_ map[string]interface{}) {
		changeCount++
	}

	watcher := NewSecretWatcher(client, "secret/path", 20*time.Millisecond, onChange, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	watcher.Watch(ctx)

	// Should have detected at least one change.
	assert.Greater(t, readCount, 1)
	assert.Greater(t, changeCount, 0)
}

func TestSecretWatcher_CheckForChanges_MultipleChanges(t *testing.T) {
	callCount := 0
	var lastData map[string]interface{}
	onChange := func(data map[string]interface{}) {
		callCount++
		lastData = data
	}

	readCount := 0
	client := &changingMockClient{
		readFunc: func() (map[string]interface{}, error) {
			readCount++
			return map[string]interface{}{"version": fmt.Sprintf("%d", readCount)}, nil
		},
	}

	watcher := NewSecretWatcher(client, "secret/path", time.Minute, onChange, slog.Default())

	// First check sets hash.
	watcher.checkForChanges(context.Background())
	assert.Equal(t, 0, callCount)

	// Second check detects change.
	watcher.checkForChanges(context.Background())
	assert.Equal(t, 1, callCount)
	assert.NotNil(t, lastData)

	// Third check detects another change.
	watcher.checkForChanges(context.Background())
	assert.Equal(t, 2, callCount)
}

func TestNewClient_KubernetesAuth_SuccessfulLogin(t *testing.T) {
	// Create a temporary file to simulate the Kubernetes service account token.
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	err := os.WriteFile(tokenFile, []byte("fake-jwt-token"), 0o600)
	require.NoError(t, err)

	// Override the kubeTokenPath for this test.
	origPath := kubeTokenPath
	kubeTokenPath = tokenFile
	t.Cleanup(func() { kubeTokenPath = origPath })

	// Create a mock Vault server that handles Kubernetes auth login.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/kubernetes/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)

		// Verify the request contains the expected fields.
		assert.Equal(t, "test-role", body["role"])
		assert.Equal(t, "fake-jwt-token", body["jwt"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.k8s-token",
				"policies": ["default"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "kubernetes",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.True(t, client.IsEnabled())
}

func TestNewClient_KubernetesAuth_CustomAuthPath(t *testing.T) {
	// Create a temporary file to simulate the Kubernetes service account token.
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	err := os.WriteFile(tokenFile, []byte("fake-jwt-token"), 0o600)
	require.NoError(t, err)

	origPath := kubeTokenPath
	kubeTokenPath = tokenFile
	t.Cleanup(func() { kubeTokenPath = origPath })

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/custom-k8s/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"auth": {
				"client_token": "s.custom-k8s-token",
				"policies": ["default"]
			}
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "kubernetes",
		AuthPath:   "auth/custom-k8s",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.True(t, client.IsEnabled())
}

func TestNewClient_KubernetesAuth_NoAuthData(t *testing.T) {
	// Create a temporary file to simulate the Kubernetes service account token.
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	err := os.WriteFile(tokenFile, []byte("fake-jwt-token"), 0o600)
	require.NoError(t, err)

	origPath := kubeTokenPath
	kubeTokenPath = tokenFile
	t.Cleanup(func() { kubeTokenPath = origPath })

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/kubernetes/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Return response with no auth data.
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "kubernetes",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "no auth data")
}

func TestNoopClient_WriteSecretWithResponse(t *testing.T) {
	client := &noopClient{}
	data, err := client.WriteSecretWithResponse(context.Background(), "pki/issue/role", map[string]interface{}{
		"common_name": "test.example.com",
	})
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func TestVaultClient_WriteSecretWithResponse_WithMockServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pki/issue/test-role", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := map[string]interface{}{
				"data": map[string]interface{}{
					"certificate": "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----",
					"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----",
					"issuing_ca":  "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----",
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.WriteSecretWithResponse(context.Background(), "pki/issue/test-role", map[string]interface{}{
		"common_name": "test.example.com",
	})
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.NotEmpty(t, data["certificate"])
	assert.NotEmpty(t, data["private_key"])
	assert.NotEmpty(t, data["issuing_ca"])
}

func TestVaultClient_WriteSecretWithResponse_Error(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pki/issue/test-role", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.WriteSecretWithResponse(context.Background(), "pki/issue/test-role", map[string]interface{}{
		"common_name": "test.example.com",
	})
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestVaultClient_WriteSecretWithResponse_EmptyResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pki/issue/test-role", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, nil)
	require.NoError(t, err)

	data, err := client.WriteSecretWithResponse(context.Background(), "pki/issue/test-role", map[string]interface{}{
		"common_name": "test.example.com",
	})
	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "no data returned")
}

func TestNewClient_KubernetesAuth_ServerError(t *testing.T) {
	// Create a temporary file to simulate the Kubernetes service account token.
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	err := os.WriteFile(tokenFile, []byte("fake-jwt-token"), 0o600)
	require.NoError(t, err)

	origPath := kubeTokenPath
	kubeTokenPath = tokenFile
	t.Cleanup(func() { kubeTokenPath = origPath })

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/kubernetes/login", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "kubernetes",
		Role:       "test-role",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := NewClient(context.Background(), cfg, slog.Default())
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "authenticating with vault")
}
