package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// KeycloakTestHelper provides helpers for interacting with Keycloak in tests.
type KeycloakTestHelper struct {
	Addr          string
	AdminUser     string
	AdminPassword string
	HTTPClient    *http.Client
}

// NewKeycloakTestHelper creates a new KeycloakTestHelper.
func NewKeycloakTestHelper(addr, adminUser, adminPassword string) *KeycloakTestHelper {
	return &KeycloakTestHelper{
		Addr:          addr,
		AdminUser:     adminUser,
		AdminPassword: adminPassword,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// IsAvailable checks if Keycloak is available.
func (k *KeycloakTestHelper) IsAvailable(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.Addr+"/realms/master", nil)
	if err != nil {
		return false
	}
	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// TokenResponse represents an OIDC token response.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// GetClientCredentialsToken obtains a token using client_credentials grant.
func (k *KeycloakTestHelper) GetClientCredentialsToken(ctx context.Context, realm, clientID, clientSecret string) (*TokenResponse, error) {
	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", k.Addr, realm)

	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}

	return &tokenResp, nil
}

// GetPasswordToken obtains a token using password grant.
func (k *KeycloakTestHelper) GetPasswordToken(ctx context.Context, realm, clientID, clientSecret, username, password string) (*TokenResponse, error) {
	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", k.Addr, realm)

	data := url.Values{
		"grant_type":    {"password"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"username":      {username},
		"password":      {password},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}

	return &tokenResp, nil
}

// GetOIDCDiscovery retrieves the OIDC discovery document for a realm.
func (k *KeycloakTestHelper) GetOIDCDiscovery(ctx context.Context, realm string) (map[string]interface{}, error) {
	discoveryURL := fmt.Sprintf("%s/realms/%s/.well-known/openid-configuration", k.Addr, realm)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting discovery: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("discovery request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var discovery map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("decoding discovery response: %w", err)
	}

	return discovery, nil
}

// CheckRealmExists checks if a Keycloak realm exists.
func (k *KeycloakTestHelper) CheckRealmExists(ctx context.Context, realm string) error {
	realmURL := fmt.Sprintf("%s/realms/%s", k.Addr, realm)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realmURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("checking realm: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("realm %s not found (status %d)", realm, resp.StatusCode)
	}

	return nil
}

// GetAdminToken obtains an admin token for Keycloak admin API calls.
func (k *KeycloakTestHelper) GetAdminToken(ctx context.Context) (string, error) {
	tokenURL := fmt.Sprintf("%s/realms/master/protocol/openid-connect/token", k.Addr)

	data := url.Values{
		"grant_type": {"password"},
		"client_id":  {"admin-cli"},
		"username":   {k.AdminUser},
		"password":   {k.AdminPassword},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting admin token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("admin token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}

	return tokenResp.AccessToken, nil
}

// IntrospectToken introspects a token to check its validity.
func (k *KeycloakTestHelper) IntrospectToken(ctx context.Context, realm, clientID, clientSecret, token string) (map[string]interface{}, error) {
	introspectURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token/introspect", k.Addr, realm)

	data := url.Values{
		"token":         {token},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, introspectURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("introspecting token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("introspection failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding introspection response: %w", err)
	}

	return result, nil
}
