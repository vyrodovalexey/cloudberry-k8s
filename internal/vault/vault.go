// Package vault provides a HashiCorp Vault client with retry logic for the cloudberry operator.
package vault

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	vaultapi "github.com/hashicorp/vault/api"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	authMethodToken      = "token"
	authMethodKubernetes = "kubernetes"
	authMethodAppRole    = "approle"
)

// kubeTokenPath is the path to the Kubernetes service account token file.
// It is a variable so that tests can override it.
var kubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // not a credential

// Client defines the interface for Vault operations.
type Client interface {
	// ReadSecret reads a secret from Vault KV v2.
	ReadSecret(ctx context.Context, path string) (map[string]interface{}, error)
	// WriteSecret writes a secret to Vault KV v2.
	WriteSecret(ctx context.Context, path string, data map[string]interface{}) error
	// IsEnabled returns whether Vault integration is active.
	IsEnabled() bool
}

// Config holds Vault client configuration.
type Config struct {
	// Enabled controls whether Vault integration is active.
	Enabled bool
	// Address is the Vault server address.
	Address string
	// AuthMethod is the authentication method (token, kubernetes, approle).
	AuthMethod string
	// AuthPath is the auth mount path.
	AuthPath string
	// Role is the Vault role name.
	Role string
	// Token is the Vault token (for token auth).
	Token string
	// SecretPath is the base secret path.
	SecretPath string
	// TLSCACert is the path to the CA certificate for TLS.
	TLSCACert string
	// RetryOpts configures retry behavior.
	RetryOpts util.RetryOptions
}

// vaultClient implements Client using the Vault API.
type vaultClient struct {
	client    *vaultapi.Client
	config    Config
	retryOpts util.RetryOptions
	logger    *slog.Logger
}

// NewClient creates a new Vault client.
// If Vault is not enabled, returns a no-op client.
func NewClient(ctx context.Context, cfg Config, logger *slog.Logger) (Client, error) {
	if !cfg.Enabled {
		return &noopClient{}, nil
	}

	if cfg.Address == "" {
		return nil, fmt.Errorf("vault address is required when vault is enabled")
	}

	if logger == nil {
		logger = slog.Default()
	}

	retryOpts := cfg.RetryOpts
	if retryOpts.MaxRetries == 0 {
		retryOpts = util.DefaultRetryOptions()
	}

	vaultCfg := vaultapi.DefaultConfig()
	vaultCfg.Address = cfg.Address

	if cfg.TLSCACert != "" {
		tlsCfg := &vaultapi.TLSConfig{CACert: cfg.TLSCACert}
		if err := vaultCfg.ConfigureTLS(tlsCfg); err != nil {
			return nil, fmt.Errorf("configuring vault TLS: %w", err)
		}
	}

	client, err := vaultapi.NewClient(vaultCfg)
	if err != nil {
		return nil, fmt.Errorf("creating vault client: %w", err)
	}

	vc := &vaultClient{
		client:    client,
		config:    cfg,
		retryOpts: retryOpts,
		logger:    logger,
	}

	if err := vc.authenticate(ctx); err != nil {
		return nil, fmt.Errorf("authenticating with vault: %w", err)
	}

	return vc, nil
}

// authenticate performs Vault authentication based on the configured method.
func (v *vaultClient) authenticate(ctx context.Context) error {
	return util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		switch v.config.AuthMethod {
		case authMethodToken:
			return v.authenticateToken()
		case authMethodKubernetes:
			return v.authenticateKubernetes(ctx)
		case authMethodAppRole:
			return v.authenticateAppRole(ctx)
		default:
			return fmt.Errorf("unsupported vault auth method: %s", v.config.AuthMethod)
		}
	})
}

// authenticateToken sets the Vault token directly.
func (v *vaultClient) authenticateToken() error {
	if v.config.Token == "" {
		return fmt.Errorf("vault token is required for token auth method")
	}
	v.client.SetToken(v.config.Token)
	v.logger.Info("authenticated with vault using token method")
	return nil
}

// authenticateKubernetes authenticates using the Kubernetes service account token.
func (v *vaultClient) authenticateKubernetes(ctx context.Context) error {
	jwt, err := os.ReadFile(kubeTokenPath)
	if err != nil {
		return fmt.Errorf("reading kubernetes service account token: %w", err)
	}

	authPath := v.config.AuthPath
	if authPath == "" {
		authPath = "auth/kubernetes"
	}

	loginPath := fmt.Sprintf("%s/login", authPath)
	data := map[string]interface{}{
		"role": v.config.Role,
		"jwt":  string(jwt),
	}

	secret, err := v.client.Logical().WriteWithContext(ctx, loginPath, data)
	if err != nil {
		return fmt.Errorf("kubernetes auth login: %w", err)
	}

	if secret == nil || secret.Auth == nil {
		return fmt.Errorf("kubernetes auth login returned no auth data")
	}

	v.client.SetToken(secret.Auth.ClientToken)
	v.logger.Info("authenticated with vault using kubernetes method",
		"role", v.config.Role,
	)
	return nil
}

// authenticateAppRole authenticates using AppRole credentials.
func (v *vaultClient) authenticateAppRole(ctx context.Context) error {
	authPath := v.config.AuthPath
	if authPath == "" {
		authPath = "auth/approle"
	}

	loginPath := fmt.Sprintf("%s/login", authPath)
	data := map[string]interface{}{
		"role_id":   v.config.Role,
		"secret_id": v.config.Token,
	}

	secret, err := v.client.Logical().WriteWithContext(ctx, loginPath, data)
	if err != nil {
		return fmt.Errorf("approle auth login: %w", err)
	}

	if secret == nil || secret.Auth == nil {
		return fmt.Errorf("approle auth login returned no auth data")
	}

	v.client.SetToken(secret.Auth.ClientToken)
	v.logger.Info("authenticated with vault using approle method")
	return nil
}

// ReadSecret reads a secret from Vault KV v2.
func (v *vaultClient) ReadSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	var result map[string]interface{}

	err := util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		secret, readErr := v.client.Logical().ReadWithContext(ctx, path)
		if readErr != nil {
			return fmt.Errorf("reading secret at %s: %w", path, readErr)
		}

		if secret == nil || secret.Data == nil {
			return fmt.Errorf("secret not found at path %s", path)
		}

		// KV v2 wraps data in a "data" key.
		if data, ok := secret.Data["data"].(map[string]interface{}); ok {
			result = data
		} else {
			result = secret.Data
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	v.logger.Debug("read secret from vault", "path", path)
	return result, nil
}

// WriteSecret writes a secret to Vault KV v2.
func (v *vaultClient) WriteSecret(ctx context.Context, path string, data map[string]interface{}) error {
	err := util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		// KV v2 expects data wrapped in a "data" key.
		wrappedData := map[string]interface{}{
			"data": data,
		}
		_, writeErr := v.client.Logical().WriteWithContext(ctx, path, wrappedData)
		if writeErr != nil {
			return fmt.Errorf("writing secret at %s: %w", path, writeErr)
		}
		return nil
	})

	if err != nil {
		return err
	}

	v.logger.Debug("wrote secret to vault", "path", path)
	return nil
}

// IsEnabled returns whether Vault integration is active.
func (v *vaultClient) IsEnabled() bool {
	return true
}

// noopClient is a no-op Vault client used when Vault is disabled.
type noopClient struct{}

// ReadSecret returns nil when Vault is disabled.
func (n *noopClient) ReadSecret(_ context.Context, _ string) (map[string]interface{}, error) {
	return nil, nil
}

// WriteSecret is a no-op when Vault is disabled.
func (n *noopClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

// IsEnabled returns false for the no-op client.
func (n *noopClient) IsEnabled() bool {
	return false
}

// SecretWatcher watches Vault secrets for changes.
type SecretWatcher struct {
	client   Client
	path     string
	interval time.Duration
	lastHash string
	logger   *slog.Logger
	onChange func(data map[string]interface{})
}

// NewSecretWatcher creates a new SecretWatcher.
func NewSecretWatcher(
	client Client,
	path string,
	interval time.Duration,
	onChange func(data map[string]interface{}),
	logger *slog.Logger,
) *SecretWatcher {
	return &SecretWatcher{
		client:   client,
		path:     path,
		interval: interval,
		logger:   logger,
		onChange: onChange,
	}
}

// Watch starts watching for secret changes. It blocks until the context is canceled.
func (w *SecretWatcher) Watch(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.checkForChanges(ctx)
		}
	}
}

// checkForChanges reads the secret and calls onChange if it has changed.
func (w *SecretWatcher) checkForChanges(ctx context.Context) {
	data, err := w.client.ReadSecret(ctx, w.path)
	if err != nil {
		w.logger.Warn("failed to read vault secret for change detection",
			"path", w.path,
			"error", err,
		)
		return
	}

	if data == nil {
		return
	}

	// Convert data to a string map for hashing.
	strData := make(map[string]string, len(data))
	for k, v := range data {
		strData[k] = fmt.Sprintf("%v", v)
	}

	hash := util.ComputeHash(strData)
	if hash != w.lastHash && w.lastHash != "" {
		w.logger.Info("vault secret changed", "path", w.path)
		w.onChange(data)
	}
	w.lastHash = hash
}
