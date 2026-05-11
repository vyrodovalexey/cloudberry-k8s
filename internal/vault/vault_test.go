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
