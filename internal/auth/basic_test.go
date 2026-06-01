package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
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

		storedHash, err := store.GetPassword(context.Background(), "user1")
		require.NoError(t, err)
		// The stored value should be a bcrypt hash, not the raw password.
		assert.NotEqual(t, "pass1", storedHash)
		assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("pass1")))

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

		storedHash, err := store.GetPassword(context.Background(), "user2")
		require.NoError(t, err)
		// The stored value should be a bcrypt hash of the new password.
		assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("newpass")))
		assert.Error(t, bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("pass2")))

		level, err := store.GetPermissionLevel(context.Background(), "user2")
		require.NoError(t, err)
		assert.Equal(t, PermissionOperator, level)
	})
}

func TestBasicAuthProvider_Authenticate_EmptyCredentials(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{"empty username and password", "", ""},
		{"empty username", "", "somepass"},
		{"empty password", "someuser", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewInMemoryCredentialStore()
			provider := NewBasicAuthProvider(store, nil)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(tt.username, tt.password)

			identity, err := provider.Authenticate(context.Background(), req)
			require.Error(t, err)
			assert.Nil(t, identity)
			assert.Contains(t, err.Error(), "invalid credentials")
		})
	}
}

func TestBasicAuthProvider_Authenticate_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name     string
		password string
	}{
		{"password with special chars", "p@$$w0rd!#%^&*()"},
		{"password with unicode", "\u00e9\u00e8\u00ea\u00eb"},
		{"password with spaces", "my secret password"},
		{"password with quotes", `pass"word'test`},
		{"password with newlines", "pass\nword"},
		{"very long password", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewInMemoryCredentialStore()
			store.SetCredentials("user", tt.password, PermissionAdmin)
			provider := NewBasicAuthProvider(store, nil)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth("user", tt.password)

			identity, err := provider.Authenticate(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, identity)
			assert.Equal(t, "user", identity.Username)
			assert.Equal(t, PermissionAdmin, identity.Permission)
		})
	}
}

// errorCredentialStore is a credential store that returns errors.
type errorCredentialStore struct {
	getPasswordErr   error
	getPermissionErr error
	password         string
	permission       PermissionLevel
}

func (s *errorCredentialStore) GetPassword(_ context.Context, _ string) (string, error) {
	return s.password, s.getPasswordErr
}

func (s *errorCredentialStore) GetPermissionLevel(_ context.Context, _ string) (PermissionLevel, error) {
	return s.permission, s.getPermissionErr
}

func TestBasicAuthProvider_Authenticate_GetPasswordError(t *testing.T) {
	store := &errorCredentialStore{
		getPasswordErr: fmt.Errorf("database connection failed"),
	}
	provider := NewBasicAuthProvider(store, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "pass")

	identity, err := provider.Authenticate(context.Background(), req)
	require.Error(t, err)
	assert.Nil(t, identity)
	assert.Contains(t, err.Error(), "retrieving credentials")
}

func TestBasicAuthProvider_Authenticate_GetPermissionError(t *testing.T) {
	// Need a valid bcrypt hash for the password
	hash, err := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.DefaultCost)
	require.NoError(t, err)

	store := &errorCredentialStore{
		password:         string(hash),
		getPermissionErr: fmt.Errorf("permission lookup failed"),
	}
	provider := NewBasicAuthProvider(store, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "pass")

	identity, authErr := provider.Authenticate(context.Background(), req)
	require.Error(t, authErr)
	assert.Nil(t, identity)
	assert.Contains(t, authErr.Error(), "retrieving permission level")
}

func TestBasicAuthProvider_Authenticate_AllPermissionLevels(t *testing.T) {
	levels := []struct {
		name  string
		level PermissionLevel
	}{
		{"SelfOnly", PermissionSelfOnly},
		{"Basic", PermissionBasic},
		{"OperatorBasic", PermissionOperatorBasic},
		{"Operator", PermissionOperator},
		{"Admin", PermissionAdmin},
	}

	for _, tt := range levels {
		t.Run(tt.name, func(t *testing.T) {
			store := NewInMemoryCredentialStore()
			store.SetCredentials("user", "pass", tt.level)
			provider := NewBasicAuthProvider(store, nil)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth("user", "pass")

			identity, err := provider.Authenticate(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, identity)
			assert.Equal(t, tt.level, identity.Permission)
		})
	}
}

func TestNewInMemoryCredentialStoreWithCost(t *testing.T) {
	tests := []struct {
		name     string
		cost     int
		wantCost int
	}{
		{"min cost", bcrypt.MinCost, bcrypt.MinCost},
		{"valid mid cost", 6, 6},
		{"below min falls back to default", bcrypt.MinCost - 1, bcrypt.DefaultCost},
		{"above max falls back to default", bcrypt.MaxCost + 1, bcrypt.DefaultCost},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewInMemoryCredentialStoreWithCost(tt.cost)
			require.NotNil(t, store)
			assert.Equal(t, tt.wantCost, store.cost)

			// The configured cost must be used when hashing.
			store.SetCredentials("user", "secret123", PermissionAdmin)
			hash, err := store.GetPassword(context.Background(), "user")
			require.NoError(t, err)
			gotCost, costErr := bcrypt.Cost([]byte(hash))
			require.NoError(t, costErr)
			assert.Equal(t, tt.wantCost, gotCost)
		})
	}
}
