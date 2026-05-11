package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// AuthMethodBasicName is the name for basic authentication.
	AuthMethodBasicName = "basic"
	// AuthMethodOIDCName is the name for OIDC authentication.
	AuthMethodOIDCName = "oidc"
)

// dummyHash is generated at init time to prevent timing attacks when the user is not found.
var dummyHash []byte

func init() {
	h := sha256.Sum256([]byte("cloudberry-dummy-hash-init-value"))
	dummyHash = h[:]
}

// CredentialStore defines the interface for retrieving user credentials.
type CredentialStore interface {
	// GetPassword returns the password hash for the given username.
	// Returns empty string if user not found.
	GetPassword(ctx context.Context, username string) (string, error)
	// GetPermissionLevel returns the permission level for the given username.
	GetPermissionLevel(ctx context.Context, username string) (PermissionLevel, error)
}

// BasicAuthProvider implements Provider for HTTP Basic authentication.
type BasicAuthProvider struct {
	store  CredentialStore
	logger *slog.Logger
}

// NewBasicAuthProvider creates a new BasicAuthProvider.
func NewBasicAuthProvider(store CredentialStore, logger *slog.Logger) *BasicAuthProvider {
	if logger == nil {
		logger = slog.Default()
	}
	return &BasicAuthProvider{
		store:  store,
		logger: logger,
	}
}

// Authenticate validates Basic auth credentials from the request.
func (p *BasicAuthProvider) Authenticate(ctx context.Context, r *http.Request) (*Identity, error) {
	username, password, ok := r.BasicAuth()
	if !ok {
		return nil, util.NewAuthenticationError(AuthMethodBasicName, "missing or malformed Authorization header")
	}

	storedPassword, err := p.store.GetPassword(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("retrieving credentials: %w", err)
	}

	if storedPassword == "" {
		// User not found; perform dummy comparison to prevent timing attacks.
		_ = subtle.ConstantTimeCompare(dummyHash, dummyHash)
		return nil, util.NewAuthenticationError(AuthMethodBasicName, "invalid credentials")
	}

	expectedHash := sha256.Sum256([]byte(storedPassword))
	actualHash := sha256.Sum256([]byte(password))

	if subtle.ConstantTimeCompare(expectedHash[:], actualHash[:]) != 1 {
		p.logger.Warn("basic auth failed", "username", username)
		return nil, util.NewAuthenticationError(AuthMethodBasicName, "invalid credentials")
	}

	permission, err := p.store.GetPermissionLevel(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("retrieving permission level: %w", err)
	}

	p.logger.Info("basic auth succeeded", "username", username, "permission", permission.String())

	return &Identity{
		Username:   username,
		Permission: permission,
		AuthMethod: AuthMethodBasicName,
	}, nil
}

// Type returns the provider type name.
func (p *BasicAuthProvider) Type() string {
	return "basic"
}

// InMemoryCredentialStore is a simple in-memory credential store for testing and static configuration.
type InMemoryCredentialStore struct {
	mu          sync.RWMutex
	credentials map[string]string
	permissions map[string]PermissionLevel
}

// NewInMemoryCredentialStore creates a new InMemoryCredentialStore.
func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]string),
		permissions: make(map[string]PermissionLevel),
	}
}

// SetCredentials sets the password and permission level for a user.
func (s *InMemoryCredentialStore) SetCredentials(username, password string, permission PermissionLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credentials[username] = password
	s.permissions[username] = permission
}

// GetPassword returns the password for the given username.
func (s *InMemoryCredentialStore) GetPassword(_ context.Context, username string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.credentials[username], nil
}

// GetPermissionLevel returns the permission level for the given username.
func (s *InMemoryCredentialStore) GetPermissionLevel(_ context.Context, username string) (PermissionLevel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	level, ok := s.permissions[username]
	if !ok {
		return PermissionSelfOnly, nil
	}
	return level, nil
}
