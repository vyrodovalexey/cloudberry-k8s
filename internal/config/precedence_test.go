package config

// Tests for B-1: configuration precedence ENV > flag > config file > default,
// flag binding via NewLoaderWithFlags, and the EnableTestUsers gate (A-1).

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTempConfigFile writes a YAML config file and returns its path.
func writeTempConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestPrecedence_Defaults(t *testing.T) {
	cfg, err := NewLoaderWithFlags("", OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.MetricsAddress)
	assert.Equal(t, 9443, cfg.WebhookPort)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 30*time.Second, cfg.ReconcileInterval)
	assert.False(t, cfg.EnableTestUsers, "test users must be disabled by default")
}

func TestPrecedence_FlagOnly(t *testing.T) {
	flags := OperatorFlagSet()
	require.NoError(t, flags.Parse([]string{
		"--metrics-address=:9999",
		"--webhook-port=8443",
		"--log-level=debug",
	}))

	cfg, err := NewLoaderWithFlags("", flags).Load()
	require.NoError(t, err)

	assert.Equal(t, ":9999", cfg.MetricsAddress)
	assert.Equal(t, 8443, cfg.WebhookPort)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestPrecedence_EnvOnly(t *testing.T) {
	t.Setenv("CLOUDBERRY_METRICS_ADDRESS", ":7777")
	t.Setenv("CLOUDBERRY_WEBHOOK_PORT", "7443")
	t.Setenv("CLOUDBERRY_LOG_LEVEL", "warn")

	cfg, err := NewLoaderWithFlags("", OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, ":7777", cfg.MetricsAddress)
	assert.Equal(t, 7443, cfg.WebhookPort)
	assert.Equal(t, "warn", cfg.LogLevel)
}

func TestPrecedence_EnvBeatsExplicitFlag(t *testing.T) {
	// The documented contract: ENV wins even over an explicitly CHANGED flag.
	t.Setenv("CLOUDBERRY_METRICS_ADDRESS", ":7777")
	t.Setenv("CLOUDBERRY_WEBHOOK_PORT", "7443")
	t.Setenv("CLOUDBERRY_RECONCILE_INTERVAL", "45s")

	flags := OperatorFlagSet()
	require.NoError(t, flags.Parse([]string{
		"--metrics-address=:9999",
		"--webhook-port=8443",
		"--reconcile-interval=10s",
	}))

	cfg, err := NewLoaderWithFlags("", flags).Load()
	require.NoError(t, err)

	assert.Equal(t, ":7777", cfg.MetricsAddress, "ENV must beat an explicitly set flag")
	assert.Equal(t, 7443, cfg.WebhookPort, "ENV must beat an explicitly set flag")
	assert.Equal(t, 45*time.Second, cfg.ReconcileInterval, "ENV must beat an explicitly set flag")
}

func TestPrecedence_FlagBeatsConfigFile(t *testing.T) {
	configFile := writeTempConfigFile(t, "metrics-address: \":5555\"\nlog-level: error\n")

	flags := OperatorFlagSet()
	require.NoError(t, flags.Parse([]string{"--metrics-address=:9999"}))

	cfg, err := NewLoaderWithFlags(configFile, flags).Load()
	require.NoError(t, err)

	assert.Equal(t, ":9999", cfg.MetricsAddress, "explicit flag must beat the config file")
	assert.Equal(t, "error", cfg.LogLevel, "config file applies for keys without flag/env")
}

func TestPrecedence_ConfigFileBeatsDefault(t *testing.T) {
	configFile := writeTempConfigFile(t, "metrics-address: \":5555\"\n")

	cfg, err := NewLoaderWithFlags(configFile, OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, ":5555", cfg.MetricsAddress)
}

func TestPrecedence_UnchangedFlagDoesNotOverrideConfigFile(t *testing.T) {
	// A flag left at its default must not shadow a config-file value.
	configFile := writeTempConfigFile(t, "log-level: error\n")

	flags := OperatorFlagSet()
	require.NoError(t, flags.Parse(nil))

	cfg, err := NewLoaderWithFlags(configFile, flags).Load()
	require.NoError(t, err)

	assert.Equal(t, "error", cfg.LogLevel)
}

func TestVaultAppRole_Defaults(t *testing.T) {
	cfg, err := NewLoaderWithFlags("", OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Empty(t, cfg.Vault.RoleID, "vault.role-id must default to empty")
	assert.Empty(t, cfg.Vault.SecretID.Value(), "vault.secret-id must default to empty")
}

func TestVaultAppRole_EnvBinding(t *testing.T) {
	// The Helm chart emits CLOUDBERRY_VAULT_ROLE_ID / CLOUDBERRY_VAULT_SECRET_ID;
	// both must populate the nested vault config fields.
	t.Setenv("CLOUDBERRY_VAULT_ROLE_ID", "test-role-id")
	t.Setenv("CLOUDBERRY_VAULT_SECRET_ID", "test-secret-id")

	cfg, err := NewLoaderWithFlags("", OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, "test-role-id", cfg.Vault.RoleID)
	assert.Equal(t, "test-secret-id", cfg.Vault.SecretID.Value())
	assert.Equal(t, "[REDACTED]", cfg.Vault.SecretID.String(),
		"secret-id must be redacted in Stringer output")
}

func TestVaultAppRole_EnvBeatsConfigFile(t *testing.T) {
	// Documented precedence: ENV > config file. applyEnvOverrides must
	// re-resolve the AppRole keys like every other key.
	configFile := writeTempConfigFile(t,
		"vault:\n  role-id: file-role-id\n  secret-id: file-secret-id\n")
	t.Setenv("CLOUDBERRY_VAULT_ROLE_ID", "env-role-id")
	t.Setenv("CLOUDBERRY_VAULT_SECRET_ID", "env-secret-id")

	cfg, err := NewLoaderWithFlags(configFile, OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, "env-role-id", cfg.Vault.RoleID, "ENV must beat the config file")
	assert.Equal(t, "env-secret-id", cfg.Vault.SecretID.Value(), "ENV must beat the config file")
}

func TestVaultAppRole_ConfigFileBeatsDefault(t *testing.T) {
	configFile := writeTempConfigFile(t,
		"vault:\n  role-id: file-role-id\n  secret-id: file-secret-id\n")

	cfg, err := NewLoaderWithFlags(configFile, OperatorFlagSet()).Load()
	require.NoError(t, err)

	assert.Equal(t, "file-role-id", cfg.Vault.RoleID)
	assert.Equal(t, "file-secret-id", cfg.Vault.SecretID.Value())
}

func TestEnableTestUsers_EnvBinding(t *testing.T) {
	t.Setenv("CLOUDBERRY_ENABLE_TEST_USERS", "true")

	cfg, err := NewLoader("").Load()
	require.NoError(t, err)

	assert.True(t, cfg.EnableTestUsers)
}

func TestOperatorFlagSet_ContainsExpectedFlags(t *testing.T) {
	fs := OperatorFlagSet()
	for _, name := range []string{
		"api-address", "api-rate-limit", "metrics-address",
		"health-probe-address", "webhook-port", "webhook-enabled", "log-level",
		"log-format", "leader-election", "reconcile-interval",
		"operation-timeout", "namespace",
	} {
		assert.NotNil(t, fs.Lookup(name), "flag %q must be defined", name)
	}
}
