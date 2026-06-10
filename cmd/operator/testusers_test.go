package main

// Tests for A-1: hardcoded test credentials are gated behind the
// CLOUDBERRY_ENABLE_TEST_USERS opt-in (Config.EnableTestUsers, default off).

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
)

func TestSeedTestUsers_DisabledByDefault(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	cfg := &config.OperatorConfig{EnableTestUsers: false}

	seedTestUsers(cfg, credStore, slog.Default())

	for _, user := range []string{"basic_user", "opbasic_user", "operator_user"} {
		hash, err := credStore.GetPassword(context.Background(), user)
		require.NoError(t, err)
		assert.Empty(t, hash, "test user %q must NOT be registered when the gate is off", user)
	}
}

func TestSeedTestUsers_EnabledRegistersAllThree(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	cfg := &config.OperatorConfig{EnableTestUsers: true}

	seedTestUsers(cfg, credStore, slog.Default())

	tests := []struct {
		user string
		perm auth.PermissionLevel
	}{
		{"basic_user", auth.PermissionBasic},
		{"opbasic_user", auth.PermissionOperatorBasic},
		{"operator_user", auth.PermissionOperator},
	}
	for _, tc := range tests {
		hash, err := credStore.GetPassword(context.Background(), tc.user)
		require.NoError(t, err)
		assert.NotEmpty(t, hash, "test user %q must be registered when enabled", tc.user)
		perm, permErr := credStore.GetPermissionLevel(context.Background(), tc.user)
		require.NoError(t, permErr)
		assert.Equal(t, tc.perm, perm)
	}
}

// TestSeedTestUsers_AuthRejectedWhenDisabled verifies the negative path end
// to end: with the gate off, basic auth with the well-known test credentials
// is rejected by the basic auth provider built from the same store.
func TestSeedTestUsers_AuthRejectedWhenDisabled(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	seedTestUsers(&config.OperatorConfig{EnableTestUsers: false}, credStore, slog.Default())

	provider := auth.NewBasicAuthProvider(credStore, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("operator_user", "operator_pass")

	identity, err := provider.Authenticate(context.Background(), req)
	require.Error(t, err, "auth with seeded test credentials must fail when the gate is off")
	assert.Nil(t, identity)
}

func TestSeedTestUsers_AuthAcceptedWhenEnabled(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	seedTestUsers(&config.OperatorConfig{EnableTestUsers: true}, credStore, slog.Default())

	provider := auth.NewBasicAuthProvider(credStore, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("operator_user", "operator_pass")

	identity, err := provider.Authenticate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, identity)
	assert.Equal(t, auth.PermissionOperator, identity.Permission)
}
