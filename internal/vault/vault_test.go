package vault

import (
	"context"
	"fmt"
	"log/slog"
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
