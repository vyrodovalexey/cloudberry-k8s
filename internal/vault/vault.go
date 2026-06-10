// Package vault provides a HashiCorp Vault client with retry logic for the cloudberry operator.
package vault

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	authMethodToken      = "token"
	authMethodKubernetes = "kubernetes"
	authMethodAppRole    = "approle"

	// vaultOpAuth, vaultOpRead, vaultOpWrite, vaultOpRenew, and vaultOpReauth
	// are the operation label values used for Vault operation metrics.
	vaultOpAuth   = "auth"
	vaultOpRead   = "read"
	vaultOpWrite  = "write"
	vaultOpRenew  = "renew"
	vaultOpReauth = "reauth"

	// metricResultSuccess and metricResultError are the result label values used
	// for Vault operation metrics.
	metricResultSuccess = "success"
	metricResultError   = "error"

	// vaultTracerName is the tracer name used for Vault operation spans.
	vaultTracerName = "vault-client"
)

// kubeTokenPath is the path to the Kubernetes service account token file.
// It is a variable so that tests can override it.
var kubeTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // not a credential

// newLifetimeWatcher constructs the token lifetime watcher. It is a variable
// so that tests can inject construction failures (the real constructor only
// fails on nil input, which the caller's guards make unreachable).
var newLifetimeWatcher = func(c *vaultapi.Client, secret *vaultapi.Secret) (*vaultapi.LifetimeWatcher, error) {
	return c.NewLifetimeWatcher(&vaultapi.LifetimeWatcherInput{Secret: secret})
}

// Client defines the interface for Vault operations.
type Client interface {
	// ReadSecret reads a secret from Vault KV v2.
	ReadSecret(ctx context.Context, path string) (map[string]interface{}, error)
	// WriteSecret writes a secret to Vault KV v2.
	WriteSecret(ctx context.Context, path string, data map[string]interface{}) error
	// WriteSecretWithResponse writes data to a Vault path and returns the response data.
	// This is used for operations like PKI certificate issuance that require a POST
	// and return response data.
	WriteSecretWithResponse(
		ctx context.Context, path string, data map[string]interface{},
	) (map[string]interface{}, error)
	// IsEnabled returns whether Vault integration is active.
	IsEnabled() bool
}

// Closer is implemented by Vault clients that own background goroutines
// (token lifetime watcher) and must be closed on operator shutdown. It is a
// separate interface so existing Client mocks remain compatible; callers
// type-assert: `if c, ok := vc.(vault.Closer); ok { c.Close() }`.
type Closer interface {
	// Close stops the background token lifetime watcher and waits for it to
	// terminate. Safe to call multiple times.
	Close()
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
	// RoleID is the AppRole role_id (approle auth method). When empty, Role
	// is used for backward compatibility with configurations that overloaded
	// the generic fields (L-12).
	RoleID string
	// SecretID is the AppRole secret_id (approle auth method). When empty,
	// Token is used for backward compatibility. Treat as a credential: never
	// log it.
	SecretID string
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
	// recorder records Vault operation metrics. It is optional and may be nil;
	// all metric recording is guarded with a nil check.
	recorder metrics.Recorder

	// authMu serializes (re-)authentication so concurrent operations that hit
	// an expired/revoked token trigger exactly ONE re-login (no stampede).
	authMu sync.Mutex
	// stateMu guards lastAuthSecret and authGeneration.
	stateMu sync.RWMutex
	// lastAuthSecret is the most recent login response (nil for token auth,
	// which performs no login call and has no lease to renew).
	lastAuthSecret *vaultapi.Secret
	// authGeneration increments on every successful login. Operations record
	// the generation before a request; on an auth error they only trigger a
	// re-login when the generation is unchanged (someone else may have
	// already re-authenticated).
	authGeneration uint64

	// watcherCancel stops the background token lifetime watcher.
	watcherCancel context.CancelFunc
	// watcherWG tracks the lifetime-watcher goroutine for Close().
	watcherWG sync.WaitGroup
	// closeOnce makes Close idempotent.
	closeOnce sync.Once
}

// SetRecorder sets an optional metrics recorder for Vault operations.
// It is safe to leave the recorder unset (nil); metric recording is then a no-op.
func (v *vaultClient) SetRecorder(recorder metrics.Recorder) {
	v.recorder = recorder
}

// recordVaultOp records a Vault operation metric and its duration when a recorder
// is configured. It is nil-safe.
func (v *vaultClient) recordVaultOp(operation string, start time.Time, err error) {
	if v.recorder == nil {
		return
	}
	result := metricResultSuccess
	if err != nil {
		result = metricResultError
	}
	v.recorder.RecordVaultOperation(operation, result)
	v.recorder.ObserveVaultOperationDuration(operation, time.Since(start))
}

// NewClient creates a new Vault client.
// If Vault is not enabled, returns a no-op client.
// An optional metrics recorder may be supplied to record Vault operation metrics;
// when omitted (or nil), metric recording is a no-op.
func NewClient(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	recorder ...metrics.Recorder,
) (Client, error) {
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
	if len(recorder) > 0 {
		vc.recorder = recorder[0]
	}

	if err := vc.authenticate(ctx); err != nil {
		return nil, fmt.Errorf("authenticating with vault: %w", err)
	}

	// Start the background token lifetime watcher so renewable login tokens
	// (kubernetes/approle) are renewed before expiry and re-authenticated when
	// renewal is no longer possible. The watcher lifecycle is bound to the
	// provided context AND to Close() (whichever happens first).
	watcherCtx, watcherCancel := context.WithCancel(ctx)
	vc.watcherCancel = watcherCancel
	vc.watcherWG.Add(1)
	go vc.runTokenLifecycle(watcherCtx)

	return vc, nil
}

// authenticate performs Vault authentication based on the configured method,
// retrying with exponential backoff.
func (v *vaultClient) authenticate(ctx context.Context) error {
	ctx, span := telemetry.StartSpan(ctx, vaultTracerName, "vault.authenticate",
		trace.WithAttributes(attribute.String("vault.operation", vaultOpAuth)))
	defer span.End()

	start := time.Now()
	err := util.RetryWithBackoff(ctx, v.retryOpts, v.loginOnce)
	v.recordVaultOp(vaultOpAuth, start, err)
	telemetry.SetSpanError(span, err)
	return err
}

// loginOnce performs a single authentication attempt (no retry) based on the
// configured method. On success the new auth secret and generation are stored.
func (v *vaultClient) loginOnce(ctx context.Context) error {
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
}

// storeAuthSecret records the latest login response and bumps the auth
// generation so in-flight operations know a re-login already happened.
func (v *vaultClient) storeAuthSecret(secret *vaultapi.Secret) {
	v.stateMu.Lock()
	defer v.stateMu.Unlock()
	v.lastAuthSecret = secret
	v.authGeneration++
}

// currentAuthState returns the latest auth secret and generation.
func (v *vaultClient) currentAuthState() (secret *vaultapi.Secret, generation uint64) {
	v.stateMu.RLock()
	defer v.stateMu.RUnlock()
	return v.lastAuthSecret, v.authGeneration
}

// isVaultAuthError reports whether the error indicates an authentication /
// authorization failure (expired or revoked token): HTTP 401/403 from Vault.
func isVaultAuthError(err error) bool {
	var respErr *vaultapi.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusUnauthorized ||
			respErr.StatusCode == http.StatusForbidden
	}
	// Fallback for wrapped errors that lost the typed response error.
	msg := err.Error()
	return strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "Code: 403") ||
		strings.Contains(msg, "Code: 401")
}

// maybeReauthOnAuthError performs a single re-login when opErr is a Vault
// auth error (401/403), so the caller's retry loop (RetryWithBackoff) can
// retry the operation with a fresh token. observedGen is the auth generation
// read BEFORE the failing request: when it no longer matches, another
// goroutine already re-authenticated and no additional login is issued —
// concurrent operations during a token expiry produce exactly one re-login.
func (v *vaultClient) maybeReauthOnAuthError(ctx context.Context, observedGen uint64, opErr error) {
	if opErr == nil || !isVaultAuthError(opErr) {
		return
	}

	v.authMu.Lock()
	defer v.authMu.Unlock()

	if _, gen := v.currentAuthState(); gen != observedGen {
		// A concurrent operation already re-authenticated.
		return
	}

	v.logger.Warn("vault operation failed with auth error, re-authenticating",
		"error", opErr)
	start := time.Now()
	loginErr := v.loginOnce(ctx)
	v.recordVaultOp(vaultOpReauth, start, loginErr)
	if loginErr != nil {
		v.logger.Error("vault re-authentication failed", "error", loginErr)
	}
}

// runTokenLifecycle renews the login token via vaultapi's LifetimeWatcher
// until it can no longer be renewed, then re-authenticates (with backoff) and
// starts a fresh watcher. It exits when the context is canceled, when Close
// is called, or when re-authentication exhausts its retry budget (reactive
// re-auth in Read/Write still recovers later in that case).
func (v *vaultClient) runTokenLifecycle(ctx context.Context) {
	defer v.watcherWG.Done()

	for {
		secret, _ := v.currentAuthState()
		if secret == nil || secret.Auth == nil || !secret.Auth.Renewable {
			// Nothing to renew: token auth performs no login (the token is
			// managed externally) and non-renewable leases cannot be extended.
			// Reactive re-auth in ReadSecret/WriteSecret covers expiry.
			v.logger.Debug("vault token is not renewable; lifetime watcher not started")
			return
		}

		watcher, err := newLifetimeWatcher(v.client, secret)
		if err != nil {
			v.logger.Error("failed to create vault token lifetime watcher", "error", err)
			return
		}

		go watcher.Start()
		if expired := v.watchRenewals(ctx, watcher); !expired {
			// Context canceled / Close called.
			return
		}

		// The token can no longer be renewed — re-authenticate with backoff.
		v.authMu.Lock()
		reauthErr := v.authenticate(ctx)
		v.authMu.Unlock()
		if reauthErr != nil {
			v.logger.Error("vault re-authentication after token expiry failed; "+
				"reactive re-auth on the next operation will retry", "error", reauthErr)
			return
		}
		v.logger.Info("vault token re-acquired after expiry")
	}
}

// watchRenewals consumes the watcher's renewal/done channels, recording
// renewal outcomes. It returns true when the token reached the end of its
// renewable lifetime (caller should re-authenticate) and false when the
// context was canceled.
func (v *vaultClient) watchRenewals(ctx context.Context, watcher *vaultapi.LifetimeWatcher) bool {
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case err := <-watcher.DoneCh():
			v.recordRenewalDone(err)
			return true
		case renewal := <-watcher.RenewCh():
			if v.recorder != nil {
				v.recorder.RecordVaultOperation(vaultOpRenew, metricResultSuccess)
			}
			v.logger.Debug("vault token renewed",
				"renewedAt", renewal.RenewedAt)
		}
	}
}

// recordRenewalDone logs and records the end of a renewal cycle. The watcher
// reports a nil error for expected end-of-life (e.g. an expired token falls
// back to the non-renewable path), and a non-nil error for hard failures.
func (v *vaultClient) recordRenewalDone(err error) {
	if err == nil {
		return
	}
	v.logger.Warn("vault token renewal stopped", "error", err)
	if v.recorder != nil {
		v.recorder.RecordVaultOperation(vaultOpRenew, metricResultError)
	}
}

// Close stops the background token lifetime watcher and waits for it to
// terminate. It is safe to call multiple times and implements vault.Closer.
func (v *vaultClient) Close() {
	v.closeOnce.Do(func() {
		if v.watcherCancel != nil {
			v.watcherCancel()
		}
		v.watcherWG.Wait()
	})
}

// authenticateToken sets the Vault token directly.
func (v *vaultClient) authenticateToken() error {
	if v.config.Token == "" {
		return fmt.Errorf("vault token is required for token auth method")
	}
	v.client.SetToken(v.config.Token)
	// No login response: the externally managed token has no lease to renew.
	v.storeAuthSecret(nil)
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
	v.storeAuthSecret(secret)
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

	// Prefer the dedicated AppRole credential fields; fall back to the
	// historically overloaded Role/Token fields so existing configurations
	// keep working (L-12).
	roleID := v.config.RoleID
	if roleID == "" {
		roleID = v.config.Role
	}
	secretID := v.config.SecretID
	if secretID == "" {
		secretID = v.config.Token
	}

	loginPath := fmt.Sprintf("%s/login", authPath)
	data := map[string]interface{}{
		"role_id":   roleID,
		"secret_id": secretID,
	}

	secret, err := v.client.Logical().WriteWithContext(ctx, loginPath, data)
	if err != nil {
		return fmt.Errorf("approle auth login: %w", err)
	}

	if secret == nil || secret.Auth == nil {
		return fmt.Errorf("approle auth login returned no auth data")
	}

	v.client.SetToken(secret.Auth.ClientToken)
	v.storeAuthSecret(secret)
	v.logger.Info("authenticated with vault using approle method")
	return nil
}

// ReadSecret reads a secret from Vault KV v2.
func (v *vaultClient) ReadSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	var result map[string]interface{}

	ctx, span := telemetry.StartSpan(ctx, vaultTracerName, "vault.ReadSecret",
		trace.WithAttributes(
			attribute.String("vault.operation", vaultOpRead),
			attribute.String("vault.path", path),
		))
	defer span.End()

	start := time.Now()
	err := util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		_, gen := v.currentAuthState()
		secret, readErr := v.client.Logical().ReadWithContext(ctx, path)
		if readErr != nil {
			// On 401/403 re-authenticate once (mutex-guarded, generation-
			// gated) so the next backoff retry uses a fresh token.
			v.maybeReauthOnAuthError(ctx, gen, readErr)
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
	v.recordVaultOp(vaultOpRead, start, err)
	telemetry.SetSpanError(span, err)

	if err != nil {
		return nil, err
	}

	v.logger.Debug("read secret from vault", "path", path)
	return result, nil
}

// WriteSecret writes a secret to Vault KV v2.
func (v *vaultClient) WriteSecret(ctx context.Context, path string, data map[string]interface{}) error {
	ctx, span := telemetry.StartSpan(ctx, vaultTracerName, "vault.WriteSecret",
		trace.WithAttributes(
			attribute.String("vault.operation", vaultOpWrite),
			attribute.String("vault.path", path),
		))
	defer span.End()

	start := time.Now()
	err := util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		// KV v2 expects data wrapped in a "data" key.
		wrappedData := map[string]interface{}{
			"data": data,
		}
		_, gen := v.currentAuthState()
		_, writeErr := v.client.Logical().WriteWithContext(ctx, path, wrappedData)
		if writeErr != nil {
			// On 401/403 re-authenticate once (mutex-guarded, generation-
			// gated) so the next backoff retry uses a fresh token.
			v.maybeReauthOnAuthError(ctx, gen, writeErr)
			return fmt.Errorf("writing secret at %s: %w", path, writeErr)
		}
		return nil
	})
	v.recordVaultOp(vaultOpWrite, start, err)
	telemetry.SetSpanError(span, err)

	if err != nil {
		return err
	}

	v.logger.Debug("wrote secret to vault", "path", path)
	return nil
}

// WriteSecretWithResponse writes data to a Vault path and returns the response data.
// This is used for operations like PKI certificate issuance that require a POST
// and return response data.
func (v *vaultClient) WriteSecretWithResponse(
	ctx context.Context, path string, data map[string]interface{},
) (map[string]interface{}, error) {
	var result map[string]interface{}

	ctx, span := telemetry.StartSpan(ctx, vaultTracerName, "vault.WriteSecretWithResponse",
		trace.WithAttributes(
			attribute.String("vault.operation", vaultOpWrite),
			attribute.String("vault.path", path),
		))
	defer span.End()

	start := time.Now()
	err := util.RetryWithBackoff(ctx, v.retryOpts, func(ctx context.Context) error {
		_, gen := v.currentAuthState()
		secret, writeErr := v.client.Logical().WriteWithContext(ctx, path, data)
		if writeErr != nil {
			// On 401/403 re-authenticate once (mutex-guarded, generation-
			// gated) so the next backoff retry uses a fresh token.
			v.maybeReauthOnAuthError(ctx, gen, writeErr)
			return fmt.Errorf("writing to %s: %w", path, writeErr)
		}
		if secret == nil || secret.Data == nil {
			return fmt.Errorf("no data returned from %s", path)
		}
		result = secret.Data
		return nil
	})
	v.recordVaultOp(vaultOpWrite, start, err)
	telemetry.SetSpanError(span, err)

	if err != nil {
		return nil, err
	}

	v.logger.Debug("wrote to vault with response", "path", path)
	return result, nil
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

// WriteSecretWithResponse is a no-op when Vault is disabled.
func (n *noopClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
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

	// Suppress the very first observed value: on the initial poll lastHash is
	// empty, so we record the baseline hash without invoking onChange. The
	// onChange callback only fires on subsequent polls when the hash actually
	// differs from the previously recorded value, avoiding a spurious "changed"
	// event when the watcher first populates its state.
	hash := util.ComputeHash(strData)
	if hash != w.lastHash && w.lastHash != "" {
		w.logger.Info("vault secret changed", "path", w.path)
		w.onChange(data)
	}
	w.lastHash = hash
}
