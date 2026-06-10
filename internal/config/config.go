// Package config provides configuration management for the cloudberry operator.
// It supports loading configuration from environment variables, command-line
// flags, and config files.
//
// Precedence (highest wins): ENV > flag > config file > default.
//
// Environment variables use the CLOUDBERRY_ prefix with '-' and '.' replaced
// by '_' (e.g. CLOUDBERRY_METRICS_ADDRESS, CLOUDBERRY_VAULT_TOKEN). The
// ENV-over-flag rule is enforced inside the Loader: after binding flags, any
// key that has an explicit environment value is re-resolved so the
// environment wins even over an explicitly set flag.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	envPrefix = "CLOUDBERRY"

	// Default values for operator configuration.
	defaultAPIAddress         = ":8090"
	defaultAPIRateLimit       = 10
	defaultMetricsAddress     = ":8080"
	defaultHealthProbeAddress = ":8081"
	defaultWebhookPort        = 9443
	defaultLogLevel           = "info"
	defaultLogFormat          = "json"
	defaultLeaderElection     = true
	defaultWebhookEnabled     = false
	defaultReconcileInterval  = 30 * time.Second
	// DefaultOperationTimeout is the default long-operation timeout. Exported
	// so cmd/operator can detect an explicitly configured override (the
	// reconcilers keep their per-operation hardcoded deadlines when the value
	// is left at this default — see B-2/M-2).
	DefaultOperationTimeout   = 5 * time.Minute
	defaultWebhookCertSource  = "self-signed"
	defaultWebhookCertSecret  = "cloudberry-operator-webhook-certs"
	defaultWebhookServiceName = "cloudberry-operator-webhook"
)

// OperatorConfig holds all operator configuration settings.
type OperatorConfig struct {
	// APIAddress is the address the REST API server listens on.
	APIAddress string `mapstructure:"api-address"`
	// APIRateLimit is the maximum number of API requests per minute per IP.
	// Set to 0 to disable rate limiting (useful for performance testing).
	APIRateLimit int `mapstructure:"api-rate-limit"`
	// MetricsAddress is the address the metrics server listens on.
	MetricsAddress string `mapstructure:"metrics-address"`
	// HealthProbeAddress is the address the health probe server listens on.
	HealthProbeAddress string `mapstructure:"health-probe-address"`
	// WebhookPort is the port the webhook server listens on.
	WebhookPort int `mapstructure:"webhook-port"`
	// WebhookEnabled controls whether admission webhooks are registered.
	// Disable in development environments where webhook certificates are not available.
	WebhookEnabled bool `mapstructure:"webhook-enabled"`
	// LogLevel is the logging level (debug, info, warn, error).
	LogLevel string `mapstructure:"log-level"`
	// LogFormat is the logging format (json, text).
	LogFormat string `mapstructure:"log-format"`
	// LeaderElection enables leader election for the operator.
	LeaderElection bool `mapstructure:"leader-election"`
	// ReconcileInterval is the default reconciliation interval.
	ReconcileInterval time.Duration `mapstructure:"reconcile-interval"`
	// OperationTimeout is the default timeout for operations.
	OperationTimeout time.Duration `mapstructure:"operation-timeout"`
	// Namespace is the namespace the operator watches (empty for all namespaces).
	Namespace string `mapstructure:"namespace"`
	// EnableTestUsers controls whether the well-known TEST users
	// (basic_user/opbasic_user/operator_user) are seeded into the API
	// credential store. It exists ONLY for e2e/access-control test suites and
	// MUST stay false (the default) in production: the test credentials are
	// publicly known. Bound to CLOUDBERRY_ENABLE_TEST_USERS.
	EnableTestUsers bool `mapstructure:"enable-test-users"`

	// WebhookCertSource is the certificate source for webhook TLS ("self-signed" or "vault-pki").
	WebhookCertSource string `mapstructure:"webhook-cert-source"`
	// WebhookCertSecretName is the name of the Secret to store webhook certs in.
	WebhookCertSecretName string `mapstructure:"webhook-cert-secret-name"`
	// WebhookServiceName is the webhook service name.
	WebhookServiceName string `mapstructure:"webhook-service-name"`
	// VaultPKIMountPath is the Vault PKI mount path (for vault-pki cert source).
	VaultPKIMountPath string `mapstructure:"vault-pki-mount-path"`
	// VaultPKIRole is the Vault PKI role name (for vault-pki cert source).
	VaultPKIRole string `mapstructure:"vault-pki-role"`

	// Vault holds Vault client configuration.
	Vault VaultConfig `mapstructure:"vault"`
	// OIDC holds OIDC provider configuration.
	OIDC OIDCConfig `mapstructure:"oidc"`
	// Telemetry holds telemetry configuration.
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
}

// RedactedString is a string type that redacts its value in fmt.Stringer output
// to prevent accidental logging of sensitive data.
type RedactedString string

// String returns a redacted placeholder instead of the actual value.
func (r RedactedString) String() string {
	return "[REDACTED]"
}

// Value returns the underlying string value for use in code that needs the actual value.
func (r RedactedString) Value() string {
	return string(r)
}

// VaultConfig holds Vault client configuration.
type VaultConfig struct {
	// Enabled controls whether Vault integration is active.
	Enabled bool `mapstructure:"enabled"`
	// Address is the Vault server address.
	Address string `mapstructure:"address"`
	// AuthMethod is the Vault authentication method (token, kubernetes, approle).
	AuthMethod string `mapstructure:"auth-method"`
	// AuthPath is the Vault auth mount path.
	AuthPath string `mapstructure:"auth-path"`
	// Role is the Vault role name.
	Role string `mapstructure:"role"`
	// Token is the Vault token (for token auth method).
	// Uses RedactedString to prevent accidental logging of the token value.
	Token RedactedString `mapstructure:"token"`
	// RoleID is the AppRole role_id (approle auth method). Bound to
	// CLOUDBERRY_VAULT_ROLE_ID. When empty, the vault client falls back to
	// Role for backward compatibility.
	RoleID string `mapstructure:"role-id"`
	// SecretID is the AppRole secret_id (approle auth method). Bound to
	// CLOUDBERRY_VAULT_SECRET_ID. When empty, the vault client falls back to
	// Token for backward compatibility. Uses RedactedString to prevent
	// accidental logging of the credential value.
	SecretID RedactedString `mapstructure:"secret-id"`
	// SecretPath is the base secret path.
	SecretPath string `mapstructure:"secret-path"`
}

// OIDCConfig holds OIDC provider configuration.
type OIDCConfig struct {
	// Enabled controls whether OIDC authentication is active.
	Enabled bool `mapstructure:"enabled"`
	// IssuerURL is the OIDC issuer URL.
	IssuerURL string `mapstructure:"issuer-url"`
	// ClientID is the OIDC client identifier.
	ClientID string `mapstructure:"client-id"`
	// ClientSecret is the OIDC client secret.
	// Uses RedactedString to prevent accidental logging of the secret value.
	ClientSecret RedactedString `mapstructure:"client-secret"`
	// RoleClaimPath is the JSON path to extract roles from the token (e.g. "realm_access.roles").
	RoleClaimPath string `mapstructure:"role-claim-path"`
	// RoleClaimSource defines where to extract role claims from ("id_token" or "userinfo").
	RoleClaimSource string `mapstructure:"role-claim-source"`
	// RoleMatchMode defines how to match IdP roles ("exact", "suffix", "prefix", "contains").
	RoleMatchMode string `mapstructure:"role-match-mode"`
	// RoleMapping maps IdP roles to permission level names.
	RoleMapping map[string]string `mapstructure:"role-mapping"`
}

// TelemetryConfig holds telemetry configuration.
type TelemetryConfig struct {
	// Enabled controls whether telemetry is active.
	Enabled bool `mapstructure:"enabled"`
	// OTLPEndpoint is the OTLP collector endpoint.
	OTLPEndpoint string `mapstructure:"otlp-endpoint"`
	// OTLPProtocol is the OTLP exporter protocol (grpc, http).
	OTLPProtocol string `mapstructure:"otlp-protocol"`
	// OTLPInsecure controls whether OTLP exporters use insecure (plaintext) connections.
	// When true, TLS is disabled for the OTLP exporter. Defaults to false (TLS enabled).
	OTLPInsecure bool `mapstructure:"otlp-insecure"`
	// SamplingRate is the trace sampling rate (0.0 to 1.0).
	SamplingRate float64 `mapstructure:"sampling-rate"`
	// ServiceName is the service name for traces.
	ServiceName string `mapstructure:"service-name"`
}

// Loader defines the interface for loading configuration.
type Loader interface {
	// Load reads configuration from all sources and returns the merged config.
	Load() (*OperatorConfig, error)
}

// viperLoader implements Loader using viper.
type viperLoader struct {
	v          *viper.Viper
	configFile string
	// flags is an optional command-line flag set bound into the loader's
	// viper instance (see NewLoaderWithFlags).
	flags *pflag.FlagSet
}

// NewLoader creates a new configuration loader.
func NewLoader(configFile string) Loader {
	return &viperLoader{
		v:          viper.New(),
		configFile: configFile,
	}
}

// NewLoaderWithFlags creates a configuration loader that also binds the given
// (already parsed) command-line flag set into its viper instance. The
// documented precedence is enforced by the loader: ENV > flag > config file >
// default.
func NewLoaderWithFlags(configFile string, flags *pflag.FlagSet) Loader {
	return &viperLoader{
		v:          viper.New(),
		configFile: configFile,
		flags:      flags,
	}
}

// Load reads configuration from environment variables, flags, and config file.
func (l *viperLoader) Load() (*OperatorConfig, error) {
	l.setDefaults()
	l.bindEnv()
	if l.flags != nil {
		BindFlags(l.v, l.flags)
	}

	if l.configFile != "" {
		l.v.SetConfigFile(l.configFile)
		if err := l.v.ReadInConfig(); err != nil {
			// Config file is optional; only return error if it was explicitly specified.
			var configNotFoundErr viper.ConfigFileNotFoundError
			if !errors.As(err, &configNotFoundErr) {
				return nil, fmt.Errorf("reading config file: %w", err)
			}
		}
	}

	// Enforce the documented ENV-over-flag precedence: viper's native order
	// puts an explicitly changed pflag ABOVE the environment, so any key with
	// an explicit environment value is re-resolved here (Set has the highest
	// viper priority). Centralizing the rule in the Loader keeps it
	// unit-testable and consistent for every consumer.
	l.applyEnvOverrides()

	cfg := &OperatorConfig{}
	if err := l.v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

// applyEnvOverrides pins every key that has an explicit environment value so
// ENV beats explicitly set flags (documented precedence: ENV > flag > config
// file > default).
func (l *viperLoader) applyEnvOverrides() {
	replacer := strings.NewReplacer("-", "_", ".", "_")
	for _, key := range l.v.AllKeys() {
		envName := envPrefix + "_" + strings.ToUpper(replacer.Replace(key))
		if value, ok := os.LookupEnv(envName); ok {
			l.v.Set(key, value)
		}
	}
}

// OperatorFlagSet returns the operator's command-line flag set with defaults
// matching the configuration defaults. The flag set is bound into the Loader
// via NewLoaderWithFlags so flag values participate in the documented
// precedence (ENV > flag > config file > default).
func OperatorFlagSet() *pflag.FlagSet {
	fs := pflag.NewFlagSet("cloudberry-operator", pflag.ContinueOnError)
	fs.String("api-address", defaultAPIAddress, "Address the REST API server listens on")
	fs.Int("api-rate-limit", defaultAPIRateLimit,
		"Maximum API requests per minute per IP (0 disables rate limiting)")
	fs.String("metrics-address", defaultMetricsAddress, "Address the metrics server listens on")
	fs.String("health-probe-address", defaultHealthProbeAddress,
		"Address the health probe server listens on")
	fs.Int("webhook-port", defaultWebhookPort, "Port the webhook server listens on")
	fs.Bool("webhook-enabled", defaultWebhookEnabled, "Enable admission webhooks")
	fs.String("log-level", defaultLogLevel, "Logging level (debug, info, warn, error)")
	fs.String("log-format", defaultLogFormat, "Logging format (json, text)")
	fs.Bool("leader-election", defaultLeaderElection, "Enable leader election")
	fs.Duration("reconcile-interval", defaultReconcileInterval, "Default reconciliation interval")
	fs.Duration("operation-timeout", DefaultOperationTimeout, "Default operation timeout")
	fs.String("namespace", "", "Namespace the operator watches (empty for all namespaces)")
	return fs
}

// setDefaults sets default values for all configuration options.
func (l *viperLoader) setDefaults() {
	l.v.SetDefault("api-address", defaultAPIAddress)
	l.v.SetDefault("api-rate-limit", defaultAPIRateLimit)
	l.v.SetDefault("metrics-address", defaultMetricsAddress)
	l.v.SetDefault("health-probe-address", defaultHealthProbeAddress)
	l.v.SetDefault("webhook-port", defaultWebhookPort)
	l.v.SetDefault("webhook-enabled", defaultWebhookEnabled)
	l.v.SetDefault("log-level", defaultLogLevel)
	l.v.SetDefault("log-format", defaultLogFormat)
	l.v.SetDefault("leader-election", defaultLeaderElection)
	l.v.SetDefault("reconcile-interval", defaultReconcileInterval)
	l.v.SetDefault("operation-timeout", DefaultOperationTimeout)
	l.v.SetDefault("namespace", "")
	l.v.SetDefault("enable-test-users", false)

	l.v.SetDefault("webhook-cert-source", defaultWebhookCertSource)
	l.v.SetDefault("webhook-cert-secret-name", defaultWebhookCertSecret)
	l.v.SetDefault("webhook-service-name", defaultWebhookServiceName)
	l.v.SetDefault("vault-pki-mount-path", "")
	l.v.SetDefault("vault-pki-role", "")

	l.v.SetDefault("vault.enabled", false)
	l.v.SetDefault("vault.address", "")
	l.v.SetDefault("vault.auth-method", "kubernetes")
	l.v.SetDefault("vault.auth-path", "auth/kubernetes")
	l.v.SetDefault("vault.role", "")
	l.v.SetDefault("vault.token", "")
	// Registering defaults for the AppRole credentials makes the keys visible
	// to AllKeys(), so applyEnvOverrides re-resolves CLOUDBERRY_VAULT_ROLE_ID
	// and CLOUDBERRY_VAULT_SECRET_ID with the documented ENV-over-flag
	// precedence.
	l.v.SetDefault("vault.role-id", "")
	l.v.SetDefault("vault.secret-id", "")
	l.v.SetDefault("vault.secret-path", "secret/data/cloudberry")

	l.v.SetDefault("oidc.enabled", false)
	l.v.SetDefault("oidc.issuer-url", "")
	l.v.SetDefault("oidc.client-id", "")
	l.v.SetDefault("oidc.client-secret", "")
	l.v.SetDefault("oidc.role-claim-path", "realm_access.roles")
	l.v.SetDefault("oidc.role-claim-source", "id_token")
	l.v.SetDefault("oidc.role-match-mode", "exact")

	l.v.SetDefault("telemetry.enabled", false)
	l.v.SetDefault("telemetry.otlp-protocol", "grpc")
	l.v.SetDefault("telemetry.otlp-insecure", false)
	l.v.SetDefault("telemetry.sampling-rate", 1.0)
	l.v.SetDefault("telemetry.service-name", "cloudberry-operator")
}

// bindEnv binds environment variables with the CLOUDBERRY_ prefix.
func (l *viperLoader) bindEnv() {
	l.v.SetEnvPrefix(envPrefix)
	l.v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	l.v.AutomaticEnv()
}

// BindFlags binds command-line flags to viper configuration.
func BindFlags(v *viper.Viper, flags *pflag.FlagSet) {
	flags.VisitAll(func(f *pflag.Flag) {
		// Bind the flag to viper, ignoring errors since flags are pre-validated.
		_ = v.BindPFlag(f.Name, f)
	})
}

// validateOIDC checks the OIDC configuration. Unsupported values are
// REJECTED instead of silently ignored (B-8/M-8): a typo like
// role-claim-source=userinfos would otherwise degrade every Bearer identity
// to Self Only without any signal.
func validateOIDC(oidc *OIDCConfig) error {
	if !oidc.Enabled {
		return nil
	}
	if oidc.IssuerURL == "" {
		return fmt.Errorf("oidc.issuer-url is required when OIDC is enabled")
	}
	if oidc.ClientID == "" {
		return fmt.Errorf("oidc.client-id is required when OIDC is enabled")
	}
	validRoleSources := map[string]bool{"": true, "id_token": true, "userinfo": true}
	if !validRoleSources[oidc.RoleClaimSource] {
		return fmt.Errorf(
			"oidc.role-claim-source must be one of id_token, userinfo; got %q",
			oidc.RoleClaimSource,
		)
	}
	validMatchModes := map[string]bool{
		"": true, "exact": true, "suffix": true, "prefix": true, "contains": true,
	}
	if !validMatchModes[oidc.RoleMatchMode] {
		return fmt.Errorf(
			"oidc.role-match-mode must be one of exact, suffix, prefix, contains; got %q",
			oidc.RoleMatchMode,
		)
	}
	return nil
}

// validate checks the configuration for required fields and valid values.
func validate(cfg *OperatorConfig) error {
	if cfg.WebhookPort <= 0 || cfg.WebhookPort > 65535 {
		return fmt.Errorf("webhook-port must be between 1 and 65535, got %d", cfg.WebhookPort)
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[strings.ToLower(cfg.LogLevel)] {
		return fmt.Errorf("log-level must be one of debug, info, warn, error; got %q", cfg.LogLevel)
	}

	validLogFormats := map[string]bool{"json": true, "text": true}
	if !validLogFormats[strings.ToLower(cfg.LogFormat)] {
		return fmt.Errorf("log-format must be one of json, text; got %q", cfg.LogFormat)
	}

	if cfg.Vault.Enabled && cfg.Vault.Address == "" {
		return fmt.Errorf("vault.address is required when vault is enabled")
	}

	if err := validateOIDC(&cfg.OIDC); err != nil {
		return err
	}

	if cfg.Telemetry.Enabled && cfg.Telemetry.OTLPEndpoint == "" {
		return fmt.Errorf("telemetry.otlp-endpoint is required when telemetry is enabled")
	}

	validCertSources := map[string]bool{"self-signed": true, "vault-pki": true}
	if cfg.WebhookCertSource != "" && !validCertSources[cfg.WebhookCertSource] {
		return fmt.Errorf(
			"webhook-cert-source must be one of self-signed, vault-pki; got %q",
			cfg.WebhookCertSource,
		)
	}

	return nil
}
