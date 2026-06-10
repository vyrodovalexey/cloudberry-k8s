package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// OIDCConfig holds OIDC provider configuration.
type OIDCConfig struct {
	// IssuerURL is the OIDC issuer URL.
	IssuerURL string
	// ClientID is the OIDC client identifier.
	ClientID string
	// ClientSecret is the OIDC client secret.
	ClientSecret string
	// Scopes are the OIDC scopes to request.
	Scopes []string
	// RoleClaimPath is the JSON path to extract roles from the token.
	RoleClaimPath string
	// RoleClaimSource defines where to extract role claims from (id_token or userinfo).
	RoleClaimSource string
	// RoleMatchMode defines how to match IdP roles (exact, suffix, prefix, contains).
	RoleMatchMode string
	// RoleMapping maps IdP roles to permission level names.
	RoleMapping map[string]string

	// NOTE: the former PKCE and AllowLocalSignIn fields were REMOVED (B-8/M-8):
	// PKCE is a client-side concern of the authorization-code flow (the ctl
	// CLI implements it) and the operator only verifies Bearer tokens;
	// AllowLocalSignIn was never read. Unsupported values for
	// RoleClaimSource/RoleMatchMode are rejected by config validation.
}

// Role claim source values supported by the provider.
const (
	// RoleClaimSourceIDToken extracts roles from the verified ID token claims.
	RoleClaimSourceIDToken = "id_token"
	// RoleClaimSourceUserinfo extracts roles from the userinfo endpoint
	// response (falling back to ID-token claims when userinfo is unavailable
	// or does not carry the configured claim path).
	RoleClaimSourceUserinfo = "userinfo"
)

// OIDCProvider implements Provider for OIDC/JWT authentication.
type OIDCProvider struct {
	provider   *gooidc.Provider
	verifier   *gooidc.IDTokenVerifier
	oauth2Cfg  *oauth2.Config
	config     OIDCConfig
	logger     *slog.Logger
	httpClient *http.Client
	// recorder is the optional metrics recorder for token-verification
	// latency. Nil-safe.
	recorder metrics.Recorder
}

// SetRecorder configures the optional metrics recorder (nil-safe).
func (p *OIDCProvider) SetRecorder(recorder metrics.Recorder) {
	p.recorder = recorder
}

// NewOIDCProvider creates a new OIDCProvider.
func NewOIDCProvider(ctx context.Context, cfg OIDCConfig, logger *slog.Logger) (*OIDCProvider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.IssuerURL == "" {
		return nil, fmt.Errorf("OIDC issuer URL is required")
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("OIDC client ID is required")
	}

	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{gooidc.ScopeOpenID, "profile", "email"}
	}
	if cfg.RoleClaimPath == "" {
		cfg.RoleClaimPath = "realm_access.roles"
	}
	if cfg.RoleClaimSource == "" {
		cfg.RoleClaimSource = RoleClaimSourceIDToken
	}
	if cfg.RoleMatchMode == "" {
		cfg.RoleMatchMode = "exact"
	}

	// maxOIDCRedirects limits the number of HTTP redirects the OIDC client
	// will follow to prevent redirect loops and forging attacks.
	const maxOIDCRedirects = 5

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= maxOIDCRedirects {
				return fmt.Errorf("OIDC HTTP client stopped after %d redirects", maxOIDCRedirects)
			}
			return nil
		},
	}
	oidcCtx := gooidc.ClientContext(ctx, httpClient)

	provider, err := gooidc.NewProvider(oidcCtx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("creating OIDC provider: %w", err)
	}

	verifier := provider.Verifier(&gooidc.Config{
		ClientID: cfg.ClientID,
	})

	oauth2Cfg := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Endpoint:     provider.Endpoint(),
		Scopes:       cfg.Scopes,
	}

	logger.Info("OIDC provider initialized",
		"issuer", cfg.IssuerURL,
		"clientID", cfg.ClientID,
	)

	return &OIDCProvider{
		provider:   provider,
		verifier:   verifier,
		oauth2Cfg:  oauth2Cfg,
		config:     cfg,
		logger:     logger,
		httpClient: httpClient,
	}, nil
}

// Authenticate validates a Bearer JWT token from the request.
func (p *OIDCProvider) Authenticate(ctx context.Context, r *http.Request) (*Identity, error) {
	rawToken := extractBearerToken(r)
	if rawToken == "" {
		return nil, util.NewAuthenticationError(AuthMethodOIDCName, "missing or malformed Bearer token")
	}

	oidcCtx := gooidc.ClientContext(ctx, p.httpClient)

	// Token verification performs network I/O (JWKS fetch) — traced as a
	// child span (D-5) and timed on the verify-duration histogram (C-8).
	// No token material or subject is recorded in span attributes.
	verifyCtx, verifySpan := telemetry.StartSpan(oidcCtx, authTracerName, "auth.oidc.verify")
	verifyStart := time.Now()
	idToken, err := p.verifier.Verify(verifyCtx, rawToken)
	if p.recorder != nil {
		p.recorder.ObserveAuthTokenVerifyDuration(time.Since(verifyStart))
	}
	if err != nil {
		telemetry.SetSpanError(verifySpan, err)
		verifySpan.End()
		return nil, util.NewAuthenticationError(AuthMethodOIDCName, fmt.Sprintf("token verification failed: %v", err))
	}
	verifySpan.End()

	var claims map[string]interface{}
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("extracting token claims: %w", err)
	}

	identity := &Identity{
		AuthMethod:  AuthMethodOIDCName,
		TokenExpiry: idToken.Expiry,
	}

	// Extract standard claims.
	if sub, ok := claims["sub"].(string); ok {
		identity.Username = sub
	}
	if email, ok := claims["email"].(string); ok {
		identity.Email = email
	}
	if preferredUsername, ok := claims["preferred_username"].(string); ok {
		identity.Username = preferredUsername
	}

	// Extract roles from the configured source (id_token or userinfo).
	roles := p.resolveRoles(oidcCtx, rawToken, claims)
	identity.Roles = roles

	// Map roles to permission level.
	identity.Permission = p.resolvePermission(roles)

	// Debug level (L-4): per-request identity details (username/email/roles)
	// are PII and high-volume; they must not land in production Info logs.
	p.logger.Debug("OIDC auth succeeded",
		"username", identity.Username,
		"email", identity.Email,
		"roles", roles,
		"permission", identity.Permission.String(),
	)

	return identity, nil
}

// resolveRoles extracts roles from the configured claim source. For
// RoleClaimSource=userinfo it queries the provider's userinfo endpoint with
// the bearer token and reads the configured claim path from the response;
// when the userinfo call fails or the claim path is absent there, it FALLS
// BACK to the verified ID-token claims (documented behavior: the ID token is
// already verified, so a degraded userinfo endpoint never locks users out).
func (p *OIDCProvider) resolveRoles(
	ctx context.Context,
	rawToken string,
	idTokenClaims map[string]interface{},
) []string {
	if p.config.RoleClaimSource != RoleClaimSourceUserinfo {
		return p.extractRoles(idTokenClaims)
	}

	userinfoClaims, err := p.fetchUserinfoClaims(ctx, rawToken)
	if err != nil {
		p.logger.Warn("userinfo request failed; falling back to ID-token role claims",
			"error", err)
		return p.extractRoles(idTokenClaims)
	}
	if roles := p.extractRoles(userinfoClaims); len(roles) > 0 {
		return roles
	}
	p.logger.Debug("userinfo response has no roles at the configured claim path; " +
		"falling back to ID-token role claims")
	return p.extractRoles(idTokenClaims)
}

// fetchUserinfoClaims calls the provider's userinfo endpoint with the bearer
// token and returns the response claims. Traced as a child span (network I/O).
func (p *OIDCProvider) fetchUserinfoClaims(
	ctx context.Context,
	rawToken string,
) (map[string]interface{}, error) {
	ctx, span := telemetry.StartSpan(ctx, authTracerName, "auth.oidc.userinfo")
	defer span.End()

	userinfo, err := p.provider.UserInfo(ctx, oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: rawToken,
		TokenType:   "Bearer",
	}))
	if err != nil {
		telemetry.SetSpanError(span, err)
		return nil, fmt.Errorf("querying userinfo endpoint: %w", err)
	}

	var claims map[string]interface{}
	if err := userinfo.Claims(&claims); err != nil {
		telemetry.SetSpanError(span, err)
		return nil, fmt.Errorf("extracting userinfo claims: %w", err)
	}
	return claims, nil
}

// Type returns the provider type name.
func (p *OIDCProvider) Type() string {
	return "oidc"
}

// extractBearerToken extracts the Bearer token from the Authorization header.
func extractBearerToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return ""
	}

	return strings.TrimPrefix(authHeader, bearerPrefix)
}

// extractRoles extracts roles from the token claims using the configured path.
func (p *OIDCProvider) extractRoles(claims map[string]interface{}) []string {
	parts := strings.Split(p.config.RoleClaimPath, ".")
	var current interface{} = claims

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
		default:
			return nil
		}
	}

	return interfaceToStringSlice(current)
}

// interfaceToStringSlice converts an interface{} to a string slice.
func interfaceToStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return val
	case string:
		// Try to parse as JSON array.
		var arr []string
		if err := json.Unmarshal([]byte(val), &arr); err == nil {
			return arr
		}
		return []string{val}
	default:
		return nil
	}
}

// resolvePermission maps IdP roles to the highest matching permission level.
func (p *OIDCProvider) resolvePermission(roles []string) PermissionLevel {
	highest := PermissionSelfOnly

	for _, role := range roles {
		for idpRole, permName := range p.config.RoleMapping {
			if p.matchRole(role, idpRole) {
				level := ParsePermissionLevel(permName)
				if level > highest {
					highest = level
				}
			}
		}
	}

	return highest
}

// matchRole checks if a role matches the IdP role based on the configured match mode.
func (p *OIDCProvider) matchRole(role, pattern string) bool {
	switch p.config.RoleMatchMode {
	case "suffix":
		return strings.HasSuffix(role, pattern)
	case "prefix":
		return strings.HasPrefix(role, pattern)
	case "contains":
		return strings.Contains(role, pattern)
	default:
		return role == pattern
	}
}

// GetOAuth2Config returns the OAuth2 configuration for authorization code flow.
func (p *OIDCProvider) GetOAuth2Config() *oauth2.Config {
	return p.oauth2Cfg
}

// defaultOIDCInitCooldown is the minimum interval between lazy OIDC discovery
// attempts, preventing a thundering herd of discovery requests against the
// IdP while it is unavailable.
const defaultOIDCInitCooldown = 30 * time.Second

// defaultOIDCDiscoveryTimeout bounds a single lazy discovery attempt so
// request-path callers waiting on the shared singleflight discovery are never
// blocked behind the HTTP client's full 30s dial budget (L-3).
const defaultOIDCDiscoveryTimeout = 10 * time.Second

// LazyOIDCProvider is an auth.Provider that initializes the underlying
// OIDCProvider lazily. When discovery fails at operator startup (e.g.
// Keycloak is briefly unavailable), Bearer auth is NOT permanently disabled:
// the first Bearer request after the IdP recovers re-runs discovery (subject
// to a cooldown) and authentication starts working without a pod restart.
// Concurrent first requests share a SINGLE in-flight discovery (singleflight,
// L-3): the mutex guards state only and is never held during the network
// call, and every waiter fails fast together when the bounded discovery
// (discoveryTimeout) errors or times out.
type LazyOIDCProvider struct {
	cfg    OIDCConfig
	logger *slog.Logger
	// recorder is the optional metrics recorder for discovery attempts and
	// token-verification latency (propagated to the inner provider). Nil-safe.
	recorder metrics.Recorder

	// sf collapses concurrent discovery attempts into one upstream call.
	sf singleflight.Group
	// discoveryTimeout bounds a single discovery attempt.
	discoveryTimeout time.Duration

	// mu guards provider/lastAttempt only; it is NOT held across network I/O.
	mu          sync.Mutex
	provider    *OIDCProvider
	lastAttempt time.Time
	cooldown    time.Duration
}

// NewLazyOIDCProvider creates a lazily initialized OIDC provider. No network
// I/O happens until Init or the first Authenticate call. An optional metrics
// recorder records discovery attempts (cloudberry_oidc_discovery_total) and
// Bearer verification latency.
func NewLazyOIDCProvider(
	cfg OIDCConfig,
	logger *slog.Logger,
	recorder ...metrics.Recorder,
) *LazyOIDCProvider {
	if logger == nil {
		logger = slog.Default()
	}
	p := &LazyOIDCProvider{
		cfg:              cfg,
		logger:           logger,
		cooldown:         defaultOIDCInitCooldown,
		discoveryTimeout: defaultOIDCDiscoveryTimeout,
	}
	if len(recorder) > 0 {
		p.recorder = recorder[0]
	}
	return p
}

// Init eagerly attempts provider initialization (used at startup, typically
// wrapped in util.RetryWithBackoff). It bypasses the lazy-path cooldown so a
// startup retry budget is fully under the caller's control.
func (p *LazyOIDCProvider) Init(ctx context.Context) error {
	_, err := p.getProvider(ctx, true)
	return err
}

// Initialized reports whether the underlying provider is ready.
func (p *LazyOIDCProvider) Initialized() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.provider != nil
}

// getProvider returns the initialized provider, performing discovery when
// needed. State checks happen under the mutex; the discovery network call
// itself runs OUTSIDE the lock inside a singleflight group, so concurrent
// callers share one bounded upstream attempt instead of serializing behind
// it (L-3). The cooldown (skipped when ignoreCooldown is set) prevents
// hammering an unavailable IdP.
func (p *LazyOIDCProvider) getProvider(ctx context.Context, ignoreCooldown bool) (*OIDCProvider, error) {
	p.mu.Lock()
	if p.provider != nil {
		provider := p.provider
		p.mu.Unlock()
		return provider, nil
	}
	if !ignoreCooldown && time.Since(p.lastAttempt) < p.cooldown {
		p.mu.Unlock()
		return nil, fmt.Errorf("OIDC provider initialization is cooling down after a recent failure")
	}
	p.mu.Unlock()

	v, err, _ := p.sf.Do("oidc-discovery", func() (interface{}, error) {
		return p.discoverProvider(ctx)
	})
	if err != nil {
		return nil, err
	}
	provider, ok := v.(*OIDCProvider)
	if !ok || provider == nil {
		return nil, fmt.Errorf("OIDC discovery returned no provider")
	}
	return provider, nil
}

// discoverProvider performs one bounded OIDC discovery attempt and publishes
// the result. It runs inside the singleflight group: exactly one goroutine
// executes it per burst of concurrent callers. A successful concurrent
// initialization is re-checked first so a duplicate discovery is never
// issued.
func (p *LazyOIDCProvider) discoverProvider(ctx context.Context) (*OIDCProvider, error) {
	p.mu.Lock()
	if p.provider != nil {
		provider := p.provider
		p.mu.Unlock()
		return provider, nil
	}
	p.lastAttempt = time.Now()
	p.mu.Unlock()

	discoveryCtx, cancel := context.WithTimeout(ctx, p.discoveryTimeout)
	defer cancel()

	provider, err := NewOIDCProvider(discoveryCtx, p.cfg, p.logger)
	p.recordDiscovery(err)
	if err != nil {
		p.logger.Warn("lazy OIDC provider initialization failed; will retry after cooldown",
			"error", err, "cooldown", p.cooldown)
		return nil, err
	}

	provider.SetRecorder(p.recorder)
	p.mu.Lock()
	p.provider = provider
	p.mu.Unlock()
	p.logger.Info("OIDC provider initialized lazily", "issuer", p.cfg.IssuerURL)
	return provider, nil
}

// recordDiscovery records an OIDC discovery attempt outcome on the
// cloudberry_oidc_discovery_total counter (nil-safe).
func (p *LazyOIDCProvider) recordDiscovery(err error) {
	if p.recorder == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "error"
	}
	p.recorder.RecordOIDCDiscovery(result)
}

// Authenticate validates a Bearer JWT token, lazily (re-)initializing the
// OIDC provider when discovery previously failed. During an IdP outage,
// Bearer requests fail with a 401 authentication error; basic auth is
// unaffected (separate provider in the middleware).
func (p *LazyOIDCProvider) Authenticate(ctx context.Context, r *http.Request) (*Identity, error) {
	provider, err := p.getProvider(ctx, false)
	if err != nil {
		return nil, util.NewAuthenticationError(AuthMethodOIDCName,
			fmt.Sprintf("OIDC provider unavailable: %v", err))
	}
	return provider.Authenticate(ctx, r)
}

// Type returns the provider type name.
func (p *LazyOIDCProvider) Type() string {
	return "oidc"
}
