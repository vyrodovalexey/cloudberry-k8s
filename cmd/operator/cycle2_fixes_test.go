package main

// Cycle-2 fix tests (T14):
//   M-4: handleAPIServerExit — startup/runtime API failures cancel the run
//        context; clean shutdown (nil / http.ErrServerClosed) does not.
//   L-2: resolveWebhookPKIVaultClient — the shared admin Vault client is
//        reused for webhook PKI; a dedicated client (with Closer ownership)
//        is created only when no shared client exists.

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

func TestHandleAPIServerExit_ErrorLogsAndCancels(t *testing.T) {
	canceled := false
	handleAPIServerExit(fmt.Errorf("bind: address already in use"),
		func() { canceled = true }, testLogger())
	assert.True(t, canceled, "an API server failure must cancel the run context")
}

func TestHandleAPIServerExit_ServerClosedNoCancel(t *testing.T) {
	canceled := false
	handleAPIServerExit(fmt.Errorf("wrapped: %w", http.ErrServerClosed),
		func() { canceled = true }, testLogger())
	assert.False(t, canceled, "a clean shutdown (ErrServerClosed) must not cancel")
}

func TestHandleAPIServerExit_NilErrorNoCancel(t *testing.T) {
	canceled := false
	handleAPIServerExit(nil, func() { canceled = true }, testLogger())
	assert.False(t, canceled, "a nil error is a no-op")
}

// stubVaultClient is a minimal vault.Client for wiring tests.
type stubVaultClient struct{ enabled bool }

func (s *stubVaultClient) ReadSecret(context.Context, string) (map[string]interface{}, error) {
	return nil, nil
}
func (s *stubVaultClient) WriteSecret(context.Context, string, map[string]interface{}) error {
	return nil
}
func (s *stubVaultClient) WriteSecretWithResponse(
	context.Context, string, map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}
func (s *stubVaultClient) IsEnabled() bool { return s.enabled }

func TestResolveWebhookPKIVaultClient_ReusesSharedAdminClient(t *testing.T) {
	shared := &stubVaultClient{enabled: true}
	cfg := webhookCertsConfig()

	vc, closer, err := resolveWebhookPKIVaultClient(
		context.Background(), cfg, shared, testLogger(), &metrics.NoopRecorder{})

	require.NoError(t, err)
	assert.Same(t, vault.Client(shared), vc,
		"the single shared client instance must be reused (no second client)")
	assert.Nil(t, closer,
		"a reused shared client is owned by run(); setupWebhookCerts must not close it")
}

func TestResolveWebhookPKIVaultClient_NoSharedClient_CreatesDedicated(t *testing.T) {
	cfg := webhookCertsConfig()
	cfg.Vault.Address = "http://127.0.0.1:1" // never dialed for token auth
	cfg.Vault.AuthMethod = "token"
	cfg.Vault.Token = "s.test-token"

	vc, closer, err := resolveWebhookPKIVaultClient(
		context.Background(), cfg, nil, testLogger(), &metrics.NoopRecorder{})

	require.NoError(t, err)
	require.NotNil(t, vc)
	assert.True(t, vc.IsEnabled())
	require.NotNil(t, closer,
		"a dedicated client must hand its Closer to the caller (no watcher leak)")
	closer.Close()
}

func TestResolveWebhookPKIVaultClient_DisabledSharedClient_FallsThrough(t *testing.T) {
	// A disabled shared client (defensive: production passes nil when Vault
	// is off) must not be reused; with an empty address the dedicated-client
	// construction fails fast.
	cfg := webhookCertsConfig()
	cfg.Vault.Address = ""

	_, _, err := resolveWebhookPKIVaultClient(
		context.Background(), cfg, &stubVaultClient{enabled: false},
		testLogger(), &metrics.NoopRecorder{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault address is required")
}
