package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBasicAuthProvider(t *testing.T) {
	store := NewInMemoryCredentialStore()
	provider := NewBasicAuthProvider(store, nil)
	require.NotNil(t, provider)
	assert.Equal(t, "basic", provider.Type())
}

func TestNewBasicAuthProvider_NilLogger(t *testing.T) {
	store := NewInMemoryCredentialStore()
	provider := NewBasicAuthProvider(store, nil)
	require.NotNil(t, provider)
	assert.NotNil(t, provider.logger)
}

func TestBasicAuthProvider_Authenticate(t *testing.T) {
	tests := []struct {
		name        string
		setupStore  func(store *InMemoryCredentialStore)
		username    string
		password    string
		setAuth     bool
		expectErr   bool
		errContains string
		expectPerm  PermissionLevel
	}{
		{
			name: "valid credentials",
			setupStore: func(store *InMemoryCredentialStore) {
				store.SetCredentials("admin", "secret123", PermissionAdmin)
			},
			username:   "admin",
			password:   "secret123",
			setAuth:    true,
			expectErr:  false,
			expectPerm: PermissionAdmin,
		},
		{
			name: "invalid password",
			setupStore: func(store *InMemoryCredentialStore) {
				store.SetCredentials("admin", "secret123", PermissionAdmin)
			},
			username:    "admin",
			password:    "wrong",
			setAuth:     true,
			expectErr:   true,
			errContains: "invalid credentials",
		},
		{
			name:        "user not found",
			setupStore:  func(store *InMemoryCredentialStore) {},
			username:    "unknown",
			password:    "password",
			setAuth:     true,
			expectErr:   true,
			errContains: "invalid credentials",
		},
		{
			name:        "missing auth header",
			setupStore:  func(store *InMemoryCredentialStore) {},
			username:    "",
			password:    "",
			setAuth:     false,
			expectErr:   true,
			errContains: "missing or malformed",
		},
		{
			name: "basic permission level",
			setupStore: func(store *InMemoryCredentialStore) {
				store.SetCredentials("viewer", "pass", PermissionBasic)
			},
			username:   "viewer",
			password:   "pass",
			setAuth:    true,
			expectErr:  false,
			expectPerm: PermissionBasic,
		},
		{
			name: "operator permission level",
			setupStore: func(store *InMemoryCredentialStore) {
				store.SetCredentials("operator", "pass", PermissionOperator)
			},
			username:   "operator",
			password:   "pass",
			setAuth:    true,
			expectErr:  false,
			expectPerm: PermissionOperator,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewInMemoryCredentialStore()
			tt.setupStore(store)
			provider := NewBasicAuthProvider(store, nil)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.setAuth {
				req.SetBasicAuth(tt.username, tt.password)
			}

			identity, err := provider.Authenticate(context.Background(), req)

			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, identity)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, identity)
				assert.Equal(t, tt.username, identity.Username)
				assert.Equal(t, tt.expectPerm, identity.Permission)
				assert.Equal(t, AuthMethodBasicName, identity.AuthMethod)
			}
		})
	}
}

func TestInMemoryCredentialStore(t *testing.T) {
	store := NewInMemoryCredentialStore()
	require.NotNil(t, store)

	t.Run("set and get credentials", func(t *testing.T) {
		store.SetCredentials("user1", "pass1", PermissionAdmin)

		password, err := store.GetPassword(context.Background(), "user1")
		require.NoError(t, err)
		assert.Equal(t, "pass1", password)

		level, err := store.GetPermissionLevel(context.Background(), "user1")
		require.NoError(t, err)
		assert.Equal(t, PermissionAdmin, level)
	})

	t.Run("get non-existent user password", func(t *testing.T) {
		password, err := store.GetPassword(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Empty(t, password)
	})

	t.Run("get non-existent user permission", func(t *testing.T) {
		level, err := store.GetPermissionLevel(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Equal(t, PermissionSelfOnly, level)
	})

	t.Run("update credentials", func(t *testing.T) {
		store.SetCredentials("user2", "pass2", PermissionBasic)
		store.SetCredentials("user2", "newpass", PermissionOperator)

		password, err := store.GetPassword(context.Background(), "user2")
		require.NoError(t, err)
		assert.Equal(t, "newpass", password)

		level, err := store.GetPermissionLevel(context.Background(), "user2")
		require.NoError(t, err)
		assert.Equal(t, PermissionOperator, level)
	})
}
