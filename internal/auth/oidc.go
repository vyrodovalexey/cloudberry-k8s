package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

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
	// PKCE enables Proof Key for Code Exchange.
	PKCE bool
	// AllowLocalSignIn allows local sign-in when OIDC is enabled.
	AllowLocalSignIn bool
}

// OIDCProvider implements Provider for OIDC/JWT authentication.
type OIDCProvider struct {
	provider   *gooidc.Provider
	verifier   *gooidc.IDTokenVerifier
	oauth2Cfg  *oauth2.Config
	config     OIDCConfig
	logger     *slog.Logger
	httpClient *http.Client
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
		cfg.RoleClaimSource = "id_token"
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
	idToken, err := p.verifier.Verify(oidcCtx, rawToken)
	if err != nil {
		return nil, util.NewAuthenticationError(AuthMethodOIDCName, fmt.Sprintf("token verification failed: %v", err))
	}

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

	// Extract roles from the configured claim path.
	roles := p.extractRoles(claims)
	identity.Roles = roles

	// Map roles to permission level.
	identity.Permission = p.resolvePermission(roles)

	p.logger.Info("OIDC auth succeeded",
		"username", identity.Username,
		"email", identity.Email,
		"roles", roles,
		"permission", identity.Permission.String(),
	)

	return identity, nil
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
