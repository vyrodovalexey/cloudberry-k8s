package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLoader(t *testing.T) {
	loader := NewLoader("")
	require.NotNil(t, loader)
}

func TestLoad_Defaults(t *testing.T) {
	// Clear any env vars that might interfere
	envVars := []string{
		"CLOUDBERRY_LISTEN_ADDRESS",
		"CLOUDBERRY_METRICS_ADDRESS",
		"CLOUDBERRY_HEALTH_PROBE_ADDRESS",
		"CLOUDBERRY_WEBHOOK_PORT",
		"CLOUDBERRY_LOG_LEVEL",
		"CLOUDBERRY_LOG_FORMAT",
		"CLOUDBERRY_LEADER_ELECTION",
		"CLOUDBERRY_VAULT_ENABLED",
		"CLOUDBERRY_VAULT_ADDRESS",
		"CLOUDBERRY_OIDC_ENABLED",
		"CLOUDBERRY_OIDC_ISSUER_URL",
		"CLOUDBERRY_OIDC_CLIENT_ID",
		"CLOUDBERRY_TELEMETRY_ENABLED",
		"CLOUDBERRY_TELEMETRY_OTLP_ENDPOINT",
	}
	for _, env := range envVars {
		t.Setenv(env, "")
		os.Unsetenv(env)
	}

	loader := NewLoader("")
	cfg, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":8443", cfg.ListenAddress)
	assert.Equal(t, ":8080", cfg.MetricsAddress)
	assert.Equal(t, ":8081", cfg.HealthProbeAddress)
	assert.Equal(t, 9443, cfg.WebhookPort)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.True(t, cfg.LeaderElection)
	assert.Equal(t, "", cfg.Namespace)

	// Vault defaults
	assert.False(t, cfg.Vault.Enabled)
	assert.Equal(t, "kubernetes", cfg.Vault.AuthMethod)
	assert.Equal(t, "secret/data/cloudberry", cfg.Vault.SecretPath)

	// OIDC defaults
	assert.False(t, cfg.OIDC.Enabled)

	// Telemetry defaults
	assert.False(t, cfg.Telemetry.Enabled)
	assert.Equal(t, "grpc", cfg.Telemetry.OTLPProtocol)
	assert.InDelta(t, 1.0, cfg.Telemetry.SamplingRate, 0.001)
	assert.Equal(t, "cloudberry-operator", cfg.Telemetry.ServiceName)
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("CLOUDBERRY_LOG_LEVEL", "debug")
	t.Setenv("CLOUDBERRY_LOG_FORMAT", "text")
	t.Setenv("CLOUDBERRY_WEBHOOK_PORT", "9444")

	loader := NewLoader("")
	cfg, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
	assert.Equal(t, 9444, cfg.WebhookPort)
}

func TestLoad_ConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	content := `
listen-address: ":9443"
log-level: "debug"
log-format: "text"
webhook-port: 9444
namespace: "test-ns"
`
	err := os.WriteFile(configFile, []byte(content), 0644)
	require.NoError(t, err)

	loader := NewLoader(configFile)
	cfg, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":9443", cfg.ListenAddress)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
	assert.Equal(t, 9444, cfg.WebhookPort)
	assert.Equal(t, "test-ns", cfg.Namespace)
}

func TestLoad_InvalidConfigFile(t *testing.T) {
	loader := NewLoader("/nonexistent/path/config.yaml")
	cfg, err := loader.Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)
}

func TestLoad_MalformedConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")

	// Write invalid YAML
	err := os.WriteFile(configFile, []byte("{{invalid yaml"), 0644)
	require.NoError(t, err)

	loader := NewLoader(configFile)
	cfg, err := loader.Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *OperatorConfig
		expectErr   bool
		errContains string
	}{
		{
			name: "valid config",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
			},
			expectErr: false,
		},
		{
			name: "invalid webhook port zero",
			cfg: &OperatorConfig{
				WebhookPort: 0,
				LogLevel:    "info",
				LogFormat:   "json",
			},
			expectErr:   true,
			errContains: "webhook-port",
		},
		{
			name: "invalid webhook port too high",
			cfg: &OperatorConfig{
				WebhookPort: 70000,
				LogLevel:    "info",
				LogFormat:   "json",
			},
			expectErr:   true,
			errContains: "webhook-port",
		},
		{
			name: "invalid log level",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "invalid",
				LogFormat:   "json",
			},
			expectErr:   true,
			errContains: "log-level",
		},
		{
			name: "invalid log format",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "xml",
			},
			expectErr:   true,
			errContains: "log-format",
		},
		{
			name: "vault enabled without address",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				Vault:       VaultConfig{Enabled: true, Address: ""},
			},
			expectErr:   true,
			errContains: "vault.address",
		},
		{
			name: "vault enabled with address",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				Vault:       VaultConfig{Enabled: true, Address: "https://vault.example.com"},
			},
			expectErr: false,
		},
		{
			name: "oidc enabled without issuer url",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				OIDC:        OIDCConfig{Enabled: true, IssuerURL: "", ClientID: "client"},
			},
			expectErr:   true,
			errContains: "oidc.issuer-url",
		},
		{
			name: "oidc enabled without client id",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				OIDC:        OIDCConfig{Enabled: true, IssuerURL: "https://issuer.example.com", ClientID: ""},
			},
			expectErr:   true,
			errContains: "oidc.client-id",
		},
		{
			name: "telemetry enabled without endpoint",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				Telemetry:   TelemetryConfig{Enabled: true, OTLPEndpoint: ""},
			},
			expectErr:   true,
			errContains: "telemetry.otlp-endpoint",
		},
		{
			name: "telemetry enabled with endpoint",
			cfg: &OperatorConfig{
				WebhookPort: 9443,
				LogLevel:    "info",
				LogFormat:   "json",
				Telemetry:   TelemetryConfig{Enabled: true, OTLPEndpoint: "localhost:4317"},
			},
			expectErr: false,
		},
		{
			name: "negative webhook port",
			cfg: &OperatorConfig{
				WebhookPort: -1,
				LogLevel:    "info",
				LogFormat:   "json",
			},
			expectErr:   true,
			errContains: "webhook-port",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(tt.cfg)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
