package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"golang.org/x/crypto/bcrypt"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// AuthMethodBasicName is the name for basic authentication.
	AuthMethodBasicName = "basic"
	// AuthMethodOIDCName is the name for OIDC authentication.
	AuthMethodOIDCName = "oidc"
)

// dummyHash is a valid bcrypt hash generated at init time to prevent timing attacks
// when the user is not found. Using a bcrypt comparison against this hash ensures
// constant-time behavior regardless of whether the user exists.
var dummyHash []byte

func init() {
	// Generate a bcrypt hash from random bytes at init time.
	// The value is never matched against real credentials; it exists solely
	// so that a bcrypt comparison is performed even when the user is not found,
	// ensuring constant-time behavior to prevent timing attacks.
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		panic(fmt.Sprintf("failed to generate random bytes for dummy hash: %v", err))
	}
	h, err := bcrypt.GenerateFromPassword(randomBytes, bcrypt.DefaultCost)
	if err != nil {
		panic(fmt.Sprintf("failed to generate dummy bcrypt hash: %v", err))
	}
	dummyHash = h
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

	storedHash, err := p.store.GetPassword(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("retrieving credentials: %w", err)
	}

	if storedHash == "" {
		// User not found; compare against dummy hash to prevent timing attacks.
		// bcrypt.CompareHashAndPassword provides constant-time comparison internally.
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
		return nil, util.NewAuthenticationError(AuthMethodBasicName, "invalid credentials")
	}

	if bcryptErr := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)); bcryptErr != nil {
		p.logger.Warn("basic auth failed", "username", username)
		return nil, util.NewAuthenticationError(AuthMethodBasicName, "invalid credentials")
	}

	permission, err := p.store.GetPermissionLevel(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("retrieving permission level: %w", err)
	}

	p.logger.Info("basic auth succeeded",
		"username", username,
		"method", AuthMethodBasicName,
		"source_ip", r.RemoteAddr,
		"permission", permission.String(),
	)

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
	cost        int // bcrypt cost; defaults to bcrypt.DefaultCost
}

// NewInMemoryCredentialStore creates a new InMemoryCredentialStore.
func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]string),
		permissions: make(map[string]PermissionLevel),
		cost:        bcrypt.DefaultCost,
	}
}

// NewInMemoryCredentialStoreWithCost creates a new InMemoryCredentialStore with a custom bcrypt cost.
// Use bcrypt.MinCost in tests to speed up password hashing.
func NewInMemoryCredentialStoreWithCost(cost int) *InMemoryCredentialStore {
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		cost = bcrypt.DefaultCost
	}
	return &InMemoryCredentialStore{
		credentials: make(map[string]string),
		permissions: make(map[string]PermissionLevel),
		cost:        cost,
	}
}

// SetCredentials hashes the password with bcrypt and stores the hash and permission level for a user.
func (s *InMemoryCredentialStore) SetCredentials(username, password string, permission PermissionLevel) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.cost)
	if err != nil {
		// bcrypt only fails if password exceeds 72 bytes or cost is invalid;
		// log and store empty to prevent silent authentication bypass.
		slog.Error("failed to hash password for user", "username", username, "error", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credentials[username] = string(hash)
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
