package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{
			name:     "valid bearer token",
			header:   "Bearer eyJhbGciOiJSUzI1NiJ9.test.signature",
			expected: "eyJhbGciOiJSUzI1NiJ9.test.signature",
		},
		{
			name:     "empty header",
			header:   "",
			expected: "",
		},
		{
			name:     "basic auth header",
			header:   "Basic dXNlcjpwYXNz",
			expected: "",
		},
		{
			name:     "bearer without token",
			header:   "Bearer ",
			expected: "",
		},
		{
			name:     "lowercase bearer",
			header:   "bearer token",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			result := extractBearerToken(req)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInterfaceToStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []string
	}{
		{
			name:     "interface slice",
			input:    []interface{}{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "string slice",
			input:    []string{"x", "y"},
			expected: []string{"x", "y"},
		},
		{
			name:     "single string",
			input:    "single",
			expected: []string{"single"},
		},
		{
			name:     "json array string",
			input:    `["a","b"]`,
			expected: []string{"a", "b"},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "integer input",
			input:    42,
			expected: nil,
		},
		{
			name:     "mixed interface slice",
			input:    []interface{}{"a", 42, "b"},
			expected: []string{"a", "b"},
		},
		{
			name:     "empty interface slice",
			input:    []interface{}{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interfaceToStringSlice(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOIDCProvider_ExtractRoles(t *testing.T) {
	tests := []struct {
		name          string
		roleClaimPath string
		claims        map[string]interface{}
		expected      []string
	}{
		{
			name:          "nested path",
			roleClaimPath: "realm_access.roles",
			claims: map[string]interface{}{
				"realm_access": map[string]interface{}{
					"roles": []interface{}{"admin", "user"},
				},
			},
			expected: []string{"admin", "user"},
		},
		{
			name:          "top-level path",
			roleClaimPath: "roles",
			claims: map[string]interface{}{
				"roles": []interface{}{"admin"},
			},
			expected: []string{"admin"},
		},
		{
			name:          "missing path",
			roleClaimPath: "realm_access.roles",
			claims: map[string]interface{}{
				"other": "value",
			},
			expected: nil,
		},
		{
			name:          "partial path",
			roleClaimPath: "realm_access.roles",
			claims: map[string]interface{}{
				"realm_access": "not-a-map",
			},
			expected: nil,
		},
		{
			name:          "empty claims",
			roleClaimPath: "roles",
			claims:        map[string]interface{}{},
			expected:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &OIDCProvider{
				config: OIDCConfig{
					RoleClaimPath: tt.roleClaimPath,
				},
			}
			result := provider.extractRoles(tt.claims)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOIDCProvider_MatchRole(t *testing.T) {
	tests := []struct {
		name      string
		matchMode string
		role      string
		pattern   string
		expected  bool
	}{
		{
			name:      "exact match",
			matchMode: "exact",
			role:      "admin",
			pattern:   "admin",
			expected:  true,
		},
		{
			name:      "exact no match",
			matchMode: "exact",
			role:      "admin",
			pattern:   "user",
			expected:  false,
		},
		{
			name:      "prefix match",
			matchMode: "prefix",
			role:      "admin-role",
			pattern:   "admin",
			expected:  true,
		},
		{
			name:      "prefix no match",
			matchMode: "prefix",
			role:      "user-role",
			pattern:   "admin",
			expected:  false,
		},
		{
			name:      "suffix match",
			matchMode: "suffix",
			role:      "cloudberry-admin",
			pattern:   "admin",
			expected:  true,
		},
		{
			name:      "suffix no match",
			matchMode: "suffix",
			role:      "admin-role",
			pattern:   "admin",
			expected:  false,
		},
		{
			name:      "contains match",
			matchMode: "contains",
			role:      "cloudberry-admin-role",
			pattern:   "admin",
			expected:  true,
		},
		{
			name:      "contains no match",
			matchMode: "contains",
			role:      "user-role",
			pattern:   "admin",
			expected:  false,
		},
		{
			name:      "default is exact",
			matchMode: "unknown",
			role:      "admin",
			pattern:   "admin",
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &OIDCProvider{
				config: OIDCConfig{
					RoleMatchMode: tt.matchMode,
				},
			}
			result := provider.matchRole(tt.role, tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewOIDCProvider_MissingIssuerURL(t *testing.T) {
	cfg := OIDCConfig{
		IssuerURL: "",
		ClientID:  "client-id",
	}
	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "OIDC issuer URL is required")
}

func TestNewOIDCProvider_MissingClientID(t *testing.T) {
	cfg := OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		ClientID:  "",
	}
	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "OIDC client ID is required")
}

func TestNewOIDCProvider_InvalidIssuer(t *testing.T) {
	cfg := OIDCConfig{
		IssuerURL: "https://invalid-issuer.example.com/nonexistent",
		ClientID:  "client-id",
	}
	// This should fail because the issuer URL is not reachable.
	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	assert.Error(t, err)
	assert.Nil(t, provider)
}

func TestOIDCProvider_Type(t *testing.T) {
	provider := &OIDCProvider{}
	assert.Equal(t, "oidc", provider.Type())
}

func TestOIDCProvider_Authenticate_MissingToken(t *testing.T) {
	provider := &OIDCProvider{
		config: OIDCConfig{},
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	identity, err := provider.Authenticate(context.Background(), req)
	assert.Error(t, err)
	assert.Nil(t, identity)
	assert.Contains(t, err.Error(), "missing or malformed Bearer token")
}

func TestOIDCProvider_Authenticate_InvalidToken(t *testing.T) {
	// Create a mock OIDC server that returns discovery document
	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	server := httptest.NewServer(mux)
	serverURL = server.URL
	defer server.Close()

	cfg := OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Test with an invalid token
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	identity, err := provider.Authenticate(context.Background(), req)
	assert.Error(t, err)
	assert.Nil(t, identity)
	assert.Contains(t, err.Error(), "token verification failed")
}

func TestOIDCProvider_GetOAuth2Config(t *testing.T) {
	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	server := httptest.NewServer(mux)
	serverURL = server.URL
	defer server.Close()

	cfg := OIDCConfig{
		IssuerURL:    server.URL,
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Scopes:       []string{"openid", "profile"},
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	oauth2Cfg := provider.GetOAuth2Config()
	require.NotNil(t, oauth2Cfg)
	assert.Equal(t, "test-client", oauth2Cfg.ClientID)
	assert.Equal(t, "test-secret", oauth2Cfg.ClientSecret)
	assert.Contains(t, oauth2Cfg.Scopes, "openid")
}

func TestNewOIDCProvider_DefaultValues(t *testing.T) {
	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	server := httptest.NewServer(mux)
	serverURL = server.URL
	defer server.Close()

	cfg := OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
		// Leave all optional fields empty to test defaults
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Verify defaults were applied
	assert.Equal(t, "realm_access.roles", provider.config.RoleClaimPath)
	assert.Equal(t, "id_token", provider.config.RoleClaimSource)
	assert.Equal(t, "exact", provider.config.RoleMatchMode)
	assert.Len(t, provider.config.Scopes, 3) // openid, profile, email
}

// signJWT creates a minimal RS256-signed JWT for testing.
func signJWT(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
	t.Helper()

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))

	claimsJSON, err := json.Marshal(claims)
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signingInput := header + "." + payload
	hash := crypto.SHA256.New()
	hash.Write([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hash.Sum(nil))
	require.NoError(t, err)

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// setupOIDCTestServer creates a mock OIDC server with a real RSA key for JWT verification.
func setupOIDCTestServer(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	var serverURL string

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})

	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
		_, _ = w.Write([]byte(fmt.Sprintf(`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key"}]}`, n, e)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)
	return server
}

func TestOIDCProvider_Authenticate_ValidToken(t *testing.T) {
	// Generate RSA key for signing.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	server := setupOIDCTestServer(t, key)

	cfg := OIDCConfig{
		IssuerURL:     server.URL,
		ClientID:      "test-client",
		RoleClaimPath: "realm_access.roles",
		RoleMapping:   map[string]string{"admin": "Admin", "viewer": "Basic"},
		RoleMatchMode: "exact",
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, provider)

	tests := []struct {
		name           string
		claims         map[string]interface{}
		wantUsername   string
		wantEmail      string
		wantPermission PermissionLevel
	}{
		{
			name: "token with sub and email",
			claims: map[string]interface{}{
				"iss":   server.URL,
				"sub":   "user-123",
				"aud":   "test-client",
				"email": "user@example.com",
				"exp":   float64(time.Now().Add(time.Hour).Unix()),
				"iat":   float64(time.Now().Unix()),
			},
			wantUsername:   "user-123",
			wantEmail:      "user@example.com",
			wantPermission: PermissionSelfOnly,
		},
		{
			name: "token with preferred_username overrides sub",
			claims: map[string]interface{}{
				"iss":                server.URL,
				"sub":                "user-123",
				"aud":                "test-client",
				"preferred_username": "jdoe",
				"email":              "jdoe@example.com",
				"exp":                float64(time.Now().Add(time.Hour).Unix()),
				"iat":                float64(time.Now().Unix()),
			},
			wantUsername:   "jdoe",
			wantEmail:      "jdoe@example.com",
			wantPermission: PermissionSelfOnly,
		},
		{
			name: "token with admin role",
			claims: map[string]interface{}{
				"iss": server.URL,
				"sub": "admin-user",
				"aud": "test-client",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{"admin"},
				},
			},
			wantUsername:   "admin-user",
			wantPermission: PermissionAdmin,
		},
		{
			name: "token with viewer role",
			claims: map[string]interface{}{
				"iss": server.URL,
				"sub": "viewer-user",
				"aud": "test-client",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{"viewer"},
				},
			},
			wantUsername:   "viewer-user",
			wantPermission: PermissionBasic,
		},
		{
			name: "token with multiple roles - highest wins",
			claims: map[string]interface{}{
				"iss": server.URL,
				"sub": "multi-role-user",
				"aud": "test-client",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
				"realm_access": map[string]interface{}{
					"roles": []interface{}{"viewer", "admin"},
				},
			},
			wantUsername:   "multi-role-user",
			wantPermission: PermissionAdmin,
		},
		{
			name: "token with no roles",
			claims: map[string]interface{}{
				"iss": server.URL,
				"sub": "no-role-user",
				"aud": "test-client",
				"exp": float64(time.Now().Add(time.Hour).Unix()),
				"iat": float64(time.Now().Unix()),
			},
			wantUsername:   "no-role-user",
			wantPermission: PermissionSelfOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := signJWT(t, key, tt.claims)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer "+token)

			identity, authErr := provider.Authenticate(context.Background(), req)
			require.NoError(t, authErr)
			require.NotNil(t, identity)

			assert.Equal(t, tt.wantUsername, identity.Username)
			if tt.wantEmail != "" {
				assert.Equal(t, tt.wantEmail, identity.Email)
			}
			assert.Equal(t, tt.wantPermission, identity.Permission)
			assert.Equal(t, AuthMethodOIDCName, identity.AuthMethod)
		})
	}
}

func TestOIDCProvider_Authenticate_ExpiredToken(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	server := setupOIDCTestServer(t, key)

	cfg := OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Create an expired token.
	claims := map[string]interface{}{
		"iss": server.URL,
		"sub": "user-123",
		"aud": "test-client",
		"exp": float64(time.Now().Add(-time.Hour).Unix()),
		"iat": float64(time.Now().Add(-2 * time.Hour).Unix()),
	}
	token := signJWT(t, key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, authErr := provider.Authenticate(context.Background(), req)
	assert.Error(t, authErr)
	assert.Nil(t, identity)
	assert.Contains(t, authErr.Error(), "token verification failed")
}

func TestOIDCProvider_Authenticate_WrongAudience(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	server := setupOIDCTestServer(t, key)

	cfg := OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Create a token with wrong audience.
	claims := map[string]interface{}{
		"iss": server.URL,
		"sub": "user-123",
		"aud": "wrong-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT(t, key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, authErr := provider.Authenticate(context.Background(), req)
	assert.Error(t, authErr)
	assert.Nil(t, identity)
	assert.Contains(t, authErr.Error(), "token verification failed")
}

func TestExtractBearerToken_BearerWithOnlySpaces(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer    ")
	result := extractBearerToken(req)
	assert.Equal(t, "   ", result) // Trims only the "Bearer " prefix.
}

func TestInterfaceToStringSlice_InvalidJSONString(t *testing.T) {
	result := interfaceToStringSlice("not-json")
	assert.Equal(t, []string{"not-json"}, result)
}

func TestInterfaceToStringSlice_EmptyString(t *testing.T) {
	result := interfaceToStringSlice("")
	assert.Equal(t, []string{""}, result)
}

func TestOIDCProvider_ExtractRoles_DeepNestedPath(t *testing.T) {
	provider := &OIDCProvider{
		config: OIDCConfig{
			RoleClaimPath: "a.b.c",
		},
	}
	claims := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": []interface{}{"role1", "role2"},
			},
		},
	}
	result := provider.extractRoles(claims)
	assert.Equal(t, []string{"role1", "role2"}, result)
}

func TestNewOIDCProvider_CustomScopes(t *testing.T) {
	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	server := httptest.NewServer(mux)
	serverURL = server.URL
	defer server.Close()

	cfg := OIDCConfig{
		IssuerURL:     server.URL,
		ClientID:      "test-client",
		Scopes:        []string{"openid", "custom-scope"},
		RoleClaimPath: "custom.roles",
		RoleMatchMode: "prefix",
	}

	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, provider)

	assert.Equal(t, "custom.roles", provider.config.RoleClaimPath)
	assert.Equal(t, "prefix", provider.config.RoleMatchMode)
	assert.Equal(t, []string{"openid", "custom-scope"}, provider.config.Scopes)
}

func TestOIDCProvider_MatchRole_EmptyStrings(t *testing.T) {
	provider := &OIDCProvider{
		config: OIDCConfig{RoleMatchMode: "exact"},
	}
	assert.True(t, provider.matchRole("", ""))
	assert.False(t, provider.matchRole("admin", ""))

	provider.config.RoleMatchMode = "prefix"
	assert.True(t, provider.matchRole("admin", ""))

	provider.config.RoleMatchMode = "suffix"
	assert.True(t, provider.matchRole("admin", ""))

	provider.config.RoleMatchMode = "contains"
	assert.True(t, provider.matchRole("admin", ""))
}

// Verify that the redirect limit works.
func TestNewOIDCProvider_RedirectLimit(t *testing.T) {
	redirectCount := 0
	// Use a mutable variable so the handler can reference the server URL
	// without relying on user-controlled request data for the redirect target.
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCount++
		if strings.Contains(r.URL.Path, "openid-configuration") {
			target := serverURL + r.URL.Path + "?r=" + fmt.Sprint(redirectCount)
			http.Redirect(w, r, target, http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	serverURL = server.URL

	cfg := OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}

	_, err := NewOIDCProvider(context.Background(), cfg, nil)
	assert.Error(t, err)
}

func TestOIDCProvider_ResolvePermission(t *testing.T) {
	tests := []struct {
		name        string
		roles       []string
		roleMapping map[string]string
		matchMode   string
		expected    PermissionLevel
	}{
		{
			name:        "admin role",
			roles:       []string{"admin"},
			roleMapping: map[string]string{"admin": "Admin"},
			matchMode:   "exact",
			expected:    PermissionAdmin,
		},
		{
			name:        "multiple roles - highest wins",
			roles:       []string{"viewer", "admin"},
			roleMapping: map[string]string{"viewer": "Basic", "admin": "Admin"},
			matchMode:   "exact",
			expected:    PermissionAdmin,
		},
		{
			name:        "no matching roles",
			roles:       []string{"unknown"},
			roleMapping: map[string]string{"admin": "Admin"},
			matchMode:   "exact",
			expected:    PermissionSelfOnly,
		},
		{
			name:        "empty roles",
			roles:       []string{},
			roleMapping: map[string]string{"admin": "Admin"},
			matchMode:   "exact",
			expected:    PermissionSelfOnly,
		},
		{
			name:        "nil roles",
			roles:       nil,
			roleMapping: map[string]string{"admin": "Admin"},
			matchMode:   "exact",
			expected:    PermissionSelfOnly,
		},
		{
			name:        "operator basic role",
			roles:       []string{"operator-basic"},
			roleMapping: map[string]string{"operator-basic": "Operator Basic"},
			matchMode:   "exact",
			expected:    PermissionOperatorBasic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &OIDCProvider{
				config: OIDCConfig{
					RoleMapping:   tt.roleMapping,
					RoleMatchMode: tt.matchMode,
				},
			}
			result := provider.resolvePermission(tt.roles)
			assert.Equal(t, tt.expected, result)
		})
	}
}
