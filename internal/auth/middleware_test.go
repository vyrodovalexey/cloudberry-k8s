package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// mockProvider implements Provider for testing.
type mockProvider struct {
	identity *Identity
	err      error
	typeName string
}

func (m *mockProvider) Authenticate(_ context.Context, _ *http.Request) (*Identity, error) {
	return m.identity, m.err
}

func (m *mockProvider) Type() string {
	return m.typeName
}

func TestNewAuthMiddleware(t *testing.T) {
	mw := NewAuthMiddleware(nil, nil, nil, nil)
	require.NotNil(t, mw)
}

func TestAuthMiddleware_Handler_MissingAuthHeader(t *testing.T) {
	mw := NewAuthMiddleware(nil, nil, nil, nil)
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	var resp errorResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "UNAUTHORIZED", resp.Error.Code)
}

func TestAuthMiddleware_Handler_BasicAuth(t *testing.T) {
	basicProvider := &mockProvider{
		identity: &Identity{
			Username:   "admin",
			Permission: PermissionAdmin,
			AuthMethod: "basic",
		},
		typeName: "basic",
	}

	recorder := &metrics.NoopRecorder{}
	mw := NewAuthMiddleware(basicProvider, nil, nil, recorder)

	var capturedIdentity *Identity
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "password")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, capturedIdentity)
	assert.Equal(t, "admin", capturedIdentity.Username)
}

func TestAuthMiddleware_Handler_BearerAuth(t *testing.T) {
	oidcProvider := &mockProvider{
		identity: &Identity{
			Username:   "oidc-user",
			Permission: PermissionOperator,
			AuthMethod: "oidc",
		},
		typeName: "oidc",
	}

	recorder := &metrics.NoopRecorder{}
	mw := NewAuthMiddleware(nil, oidcProvider, nil, recorder)

	var capturedIdentity *Identity
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedIdentity = IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, capturedIdentity)
	assert.Equal(t, "oidc-user", capturedIdentity.Username)
}

func TestAuthMiddleware_Handler_BasicNotConfigured(t *testing.T) {
	mw := NewAuthMiddleware(nil, nil, nil, nil)
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("user", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuthMiddleware_Handler_OIDCNotConfigured(t *testing.T) {
	mw := NewAuthMiddleware(nil, nil, nil, nil)
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuthMiddleware_Handler_UnsupportedAuthType(t *testing.T) {
	mw := NewAuthMiddleware(nil, nil, nil, nil)
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Digest username=test")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuthMiddleware_Handler_AuthFailure(t *testing.T) {
	basicProvider := &mockProvider{
		identity: nil,
		err:      fmt.Errorf("authentication failed"),
		typeName: "basic",
	}

	recorder := &metrics.NoopRecorder{}
	mw := NewAuthMiddleware(basicProvider, nil, nil, recorder)
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("user", "wrong")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestRequirePermission(t *testing.T) {
	tests := []struct {
		name           string
		identity       *Identity
		requiredLevel  PermissionLevel
		expectedStatus int
	}{
		{
			name: "sufficient permission",
			identity: &Identity{
				Username:   "admin",
				Permission: PermissionAdmin,
			},
			requiredLevel:  PermissionOperator,
			expectedStatus: http.StatusOK,
		},
		{
			name: "exact permission",
			identity: &Identity{
				Username:   "operator",
				Permission: PermissionOperator,
			},
			requiredLevel:  PermissionOperator,
			expectedStatus: http.StatusOK,
		},
		{
			name: "insufficient permission",
			identity: &Identity{
				Username:   "viewer",
				Permission: PermissionBasic,
			},
			requiredLevel:  PermissionAdmin,
			expectedStatus: http.StatusForbidden,
		},
		{
			name:           "no identity",
			identity:       nil,
			requiredLevel:  PermissionBasic,
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := RequirePermission(tt.requiredLevel)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.identity != nil {
				ctx := ContextWithIdentity(req.Context(), tt.identity)
				req = req.WithContext(ctx)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}
}

func TestSecurityHeaders(t *testing.T) {
	handler := SecurityHeaders()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Equal(t, "default-src 'self'", rec.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
	assert.Equal(t, "1; mode=block", rec.Header().Get("X-XSS-Protection"))
	assert.Contains(t, rec.Header().Get("Strict-Transport-Security"), "max-age=31536000")
	assert.Equal(t, "strict-origin-when-cross-origin", rec.Header().Get("Referrer-Policy"))
	assert.Equal(t, "camera=(), microphone=()", rec.Header().Get("Permissions-Policy"))
}

func TestWriteErrorResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErrorResponse(rec, http.StatusBadRequest, "BAD_REQUEST", "invalid input")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp errorResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "BAD_REQUEST", resp.Error.Code)
	assert.Equal(t, "invalid input", resp.Error.Message)
}

func TestContextWithIdentity_IdentityFromContext(t *testing.T) {
	t.Run("set and get identity", func(t *testing.T) {
		identity := &Identity{
			Username:   "test-user",
			Permission: PermissionAdmin,
		}
		ctx := ContextWithIdentity(context.Background(), identity)
		retrieved := IdentityFromContext(ctx)
		require.NotNil(t, retrieved)
		assert.Equal(t, "test-user", retrieved.Username)
		assert.Equal(t, PermissionAdmin, retrieved.Permission)
	})

	t.Run("no identity in context", func(t *testing.T) {
		retrieved := IdentityFromContext(context.Background())
		assert.Nil(t, retrieved)
	})
}

func TestPermissionLevel_String(t *testing.T) {
	tests := []struct {
		level    PermissionLevel
		expected string
	}{
		{PermissionSelfOnly, "Self Only"},
		{PermissionBasic, "Basic"},
		{PermissionOperatorBasic, "Operator Basic"},
		{PermissionOperator, "Operator"},
		{PermissionAdmin, "Admin"},
		{PermissionLevel(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.level.String())
		})
	}
}

func TestParsePermissionLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected PermissionLevel
	}{
		{"Self Only", PermissionSelfOnly},
		{"Basic", PermissionBasic},
		{"Operator Basic", PermissionOperatorBasic},
		{"Operator", PermissionOperator},
		{"Admin", PermissionAdmin},
		{"unknown", PermissionSelfOnly},
		{"", PermissionSelfOnly},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParsePermissionLevel(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
