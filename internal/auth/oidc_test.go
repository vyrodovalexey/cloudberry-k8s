package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
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
