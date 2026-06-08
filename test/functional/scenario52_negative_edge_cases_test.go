//go:build functional

package functional

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 52: Negative / Edge Cases
// ============================================================================
//
// This scenario tests negative and edge cases across 8 sub-scenarios:
//   52a — JWT with wrong issuer
//   52b — JWT with wrong audience
//   52c — Expired JWT
//   52d — JWT with future iat (behavioral/documentation test)
//   52e — Token refresh failure (expired token rejected)
//   52f — Vault connection retry (RetryWithBackoff)
//   52g — Invalid OIDC configuration / Basic auth fallback
//   52h — Missing admin secret (empty credential store)
// ============================================================================

// signJWT52 creates a minimal RS256-signed JWT with the given claims.
func signJWT52(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
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

// setupOIDCServer52 creates a mock OIDC server with discovery and JWKS endpoints.
func setupOIDCServer52(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	var serverURL string

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/protocol/openid-connect/certs"
		}`, serverURL, serverURL, serverURL, serverURL)))
	})

	mux.HandleFunc("/protocol/openid-connect/certs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key-52"}]}`,
			n, e,
		)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)

	return server
}

// Scenario52NegativeEdgeCaseSuite tests negative and edge cases.
type Scenario52NegativeEdgeCaseSuite struct {
	suite.Suite
}

func TestFunctional_Scenario52(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario52NegativeEdgeCaseSuite))
}

// --- 52a: JWT with wrong issuer ---

// TestFunctional_Scenario52a_JWTWrongIssuer verifies that a JWT signed with
// the correct key but containing a wrong issuer claim is rejected with 401.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52a_JWTWrongIssuer() {
	t := s.T()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create mock OIDC server A.
	serverA := setupOIDCServer52(t, key)

	// Create OIDC provider configured with server A's issuer.
	cfg := auth.OIDCConfig{
		IssuerURL:     serverA.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Sign JWT with a different issuer (but using server A's key).
	claims := map[string]interface{}{
		"iss": "http://different-issuer/realms/other",
		"sub": "user-wrong-issuer",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52(t, key, claims)

	// Create API server with this OIDC provider.
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	// Send Bearer token to an authenticated endpoint → expect 401.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong issuer should be rejected with 401")
}

// --- 52b: JWT with wrong audience ---

// TestFunctional_Scenario52b_JWTWrongAudience verifies that a JWT with the
// correct issuer and key but wrong audience claim is rejected with 401.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52b_JWTWrongAudience() {
	t := s.T()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	oidcServer := setupOIDCServer52(t, key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "correct-client",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Sign JWT with wrong audience.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-wrong-aud",
		"aud": "wrong-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52(t, key, claims)

	// Create API server.
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong audience should be rejected with 401")
}

// --- 52c: Expired JWT ---

// TestFunctional_Scenario52c_JWTExpired verifies that an expired JWT is
// rejected with 401.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52c_JWTExpired() {
	t := s.T()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	oidcServer := setupOIDCServer52(t, key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Sign JWT with exp in the past.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-expired",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(-1 * time.Hour).Unix()),
		"iat": float64(time.Now().Add(-2 * time.Hour).Unix()),
	}
	token := signJWT52(t, key, claims)

	// Create API server.
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"expired JWT should be rejected with 401")
}

// --- 52d: JWT with future iat ---

// TestFunctional_Scenario52d_JWTFutureIAT documents the behavior of gooidc
// when a JWT has a future iat (issued-at) claim. The gooidc library does NOT
// validate iat, so the token is accepted. This is a behavioral/documentation test.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52d_JWTFutureIAT() {
	t := s.T()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	oidcServer := setupOIDCServer52(t, key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Sign JWT with iat in the future but valid exp.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-future-iat",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(2 * time.Hour).Unix()),
		"iat": float64(time.Now().Add(1 * time.Hour).Unix()),
	}
	token := signJWT52(t, key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, authErr := provider.Authenticate(context.Background(), req)

	// Document behavior: gooidc does NOT reject future iat.
	// The token IS accepted because gooidc only validates: signature, issuer, audience, expiry.
	// It does NOT validate the iat (issued-at) claim.
	t.Log("Behavioral test: gooidc does NOT validate iat (issued-at) claims")
	t.Log("A JWT with future iat is accepted as long as signature, issuer, audience, and expiry are valid")

	if authErr == nil {
		// Token was accepted — this is the expected gooidc behavior.
		require.NotNil(t, identity)
		assert.Equal(t, "user-future-iat", identity.Username,
			"gooidc should accept token with future iat")
		assert.Equal(t, "oidc", identity.AuthMethod)
		t.Log("CONFIRMED: gooidc accepts JWT with future iat (no iat validation)")
	} else {
		// If a future version of gooidc starts validating iat, this branch documents that change.
		t.Logf("NOTE: gooidc rejected future iat token: %v", authErr)
		t.Log("This indicates gooidc behavior has changed to validate iat claims")
	}
}

// --- 52e: Token refresh failure ---

// TestFunctional_Scenario52e_TokenRefreshFailure verifies that an expired
// access token (simulating a failed refresh) is rejected with 401 and the
// error response contains "authentication failed".
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52e_TokenRefreshFailure() {
	t := s.T()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	oidcServer := setupOIDCServer52(t, key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	// Sign JWT with exp in the past (simulating expired access token after refresh failure).
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-refresh-failed",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(-30 * time.Minute).Unix()),
		"iat": float64(time.Now().Add(-1 * time.Hour).Unix()),
	}
	token := signJWT52(t, key, claims)

	// Create API server with OIDC provider.
	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"expired token (refresh failure) should return 401")

	// Verify error response contains "authentication failed".
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "authentication failed",
		"error response should contain 'authentication failed'")
}

// --- 52f: Vault connection retry ---

// TestFunctional_Scenario52f_VaultConnectionRetry verifies that RetryWithBackoff
// retries a failing function the correct number of times.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52f_VaultConnectionRetry() {
	t := s.T()

	var attempts atomic.Int32
	failCount := 3

	opts := util.RetryOptions{
		MaxRetries:     5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.0,
	}

	fn := func(_ context.Context) error {
		current := attempts.Add(1)
		if int(current) <= failCount {
			return fmt.Errorf("vault connection refused (attempt %d)", current)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := util.RetryWithBackoff(ctx, opts, fn)
	require.NoError(t, err, "should succeed after retries")
	assert.Equal(t, int32(failCount+1), attempts.Load(),
		"should have attempted %d times (3 failures + 1 success)", failCount+1)
}

// TestFunctional_Scenario52f_VaultRetryExhausted verifies that RetryWithBackoff
// returns ErrRetryExhausted when all retry attempts fail.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52f_VaultRetryExhausted() {
	t := s.T()

	var attempts atomic.Int32

	opts := util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.0,
	}

	fn := func(_ context.Context) error {
		attempts.Add(1)
		return fmt.Errorf("vault unreachable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := util.RetryWithBackoff(ctx, opts, fn)
	require.Error(t, err, "should return error when all retries exhausted")
	assert.True(t, errors.Is(err, util.ErrRetryExhausted),
		"error should wrap ErrRetryExhausted, got: %v", err)
	assert.Contains(t, err.Error(), "vault unreachable",
		"error should contain the last error message")

	// MaxRetries=3 means 1 initial attempt + 3 retries = 4 total attempts.
	assert.Equal(t, int32(4), attempts.Load(),
		"should have attempted 4 times (1 initial + 3 retries)")
}

// TestFunctional_Scenario52f_VaultRetryRecovery verifies that RetryWithBackoff
// succeeds when the function fails 3 times then succeeds on the 4th attempt.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52f_VaultRetryRecovery() {
	t := s.T()

	var attempts atomic.Int32
	recoverAttempt := int32(4)

	opts := util.RetryOptions{
		MaxRetries:     5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.0,
	}

	fn := func(_ context.Context) error {
		current := attempts.Add(1)
		if current < recoverAttempt {
			return fmt.Errorf("vault connection timeout (attempt %d)", current)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := util.RetryWithBackoff(ctx, opts, fn)
	require.NoError(t, err, "should succeed after recovery on attempt %d", recoverAttempt)
	assert.Equal(t, recoverAttempt, attempts.Load(),
		"should have attempted exactly %d times", recoverAttempt)
}

// TestFunctional_Scenario52f_VaultRetryContextCancellation verifies that
// RetryWithBackoff stops retrying when the context is cancelled.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52f_VaultRetryContextCancellation() {
	t := s.T()

	var attempts atomic.Int32

	opts := util.RetryOptions{
		MaxRetries:     10,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.0,
	}

	fn := func(_ context.Context) error {
		attempts.Add(1)
		return fmt.Errorf("vault unreachable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := util.RetryWithBackoff(ctx, opts, fn)
	require.Error(t, err, "should return error when context is cancelled")
	assert.Contains(t, err.Error(), "context",
		"error should mention context cancellation")

	// With 100ms initial backoff, we should get at most a few attempts before timeout.
	finalAttempts := attempts.Load()
	assert.Less(t, finalAttempts, int32(10),
		"should have stopped retrying before exhausting all attempts (got %d)", finalAttempts)
	assert.Greater(t, finalAttempts, int32(0),
		"should have made at least one attempt")
}

// --- 52g: Invalid OIDC configuration ---

// TestFunctional_Scenario52g_InvalidOIDCConfig verifies that NewOIDCProvider
// returns an error when the issuer URL is unreachable.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52g_InvalidOIDCConfig() {
	t := s.T()

	cfg := auth.OIDCConfig{
		IssuerURL: "http://unreachable.invalid:9999/realms/test",
		ClientID:  "cloudberry-operator",
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.Error(t, err, "NewOIDCProvider should fail with unreachable issuer")
	assert.Nil(t, provider, "provider should be nil on error")
	t.Logf("Expected error from unreachable OIDC issuer: %v", err)
}

// TestFunctional_Scenario52g_BasicAuthFallback verifies that when OIDC is not
// available (nil provider), Basic auth still works and Bearer tokens are rejected.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52g_BasicAuthFallback() {
	t := s.T()

	// Create API server with valid Basic auth but nil OIDC provider.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "admin-password", auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// nil OIDC provider — simulating failed OIDC initialization.
	middleware := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	t.Run("basic_auth_works_without_oidc", func(t *testing.T) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
		require.NoError(t, err)
		req.SetBasicAuth("gpadmin", "admin-password")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"Basic auth should work when OIDC is not available")
	})

	t.Run("bearer_token_rejected_without_oidc", func(t *testing.T) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer some-token")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
			"Bearer token should be rejected when OIDC is not available")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, strings.ToLower(string(body)), "oidc",
			"error response should mention OIDC not being configured")
	})
}

// --- 52h: Missing admin secret (empty credential store) ---

// TestFunctional_Scenario52h_MissingAdminSecret verifies that Basic auth fails
// with 401 when the credential store is empty (no admin password configured).
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52h_MissingAdminSecret() {
	t := s.T()

	// Create BasicAuthProvider with empty InMemoryCredentialStore.
	emptyStore := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(emptyStore, nil)

	// Verify direct authentication fails.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "any-password")

	identity, err := basicProvider.Authenticate(context.Background(), req)
	require.Error(t, err, "authentication should fail with empty credential store")
	assert.Nil(t, identity, "identity should be nil when credentials are missing")
	assert.Contains(t, err.Error(), "invalid credentials",
		"error should indicate invalid credentials")
}

// TestFunctional_Scenario52h_UnknownUser verifies that Basic auth fails with
// 401 when an unknown user attempts to authenticate via the API server.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52h_UnknownUser() {
	t := s.T()

	// Create API server with empty credential store.
	emptyStore := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(emptyStore, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(t, err)
	req.SetBasicAuth("unknown-user", "some-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"unknown user should receive 401")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "authentication failed",
		"error response should contain 'authentication failed'")
}

// --- Coverage test ---

// TestFunctional_Scenario52_NegativeEdgeCaseCases_Coverage verifies that the
// NegativeEdgeCaseCases catalog returns the expected 8 cases with correct categories.
func (s *Scenario52NegativeEdgeCaseSuite) TestFunctional_Scenario52_NegativeEdgeCaseCases_Coverage() {
	t := s.T()

	testCases := cases.NegativeEdgeCaseCases()
	require.Len(t, testCases, 8, "NegativeEdgeCaseCases should return 8 test cases")

	// Count categories.
	categoryCounts := make(map[string]int)
	for _, tc := range testCases {
		categoryCounts[tc.Category]++
		t.Logf("Case: %s (sub-scenario: %s, category: %s)", tc.Name, tc.SubScenario, tc.Category)
	}

	assert.Equal(t, 5, categoryCounts["jwt"],
		"should have 5 jwt category cases")
	assert.Equal(t, 1, categoryCounts["vault"],
		"should have 1 vault category case")
	assert.Equal(t, 1, categoryCounts["config"],
		"should have 1 config category case")
	assert.Equal(t, 1, categoryCounts["auth"],
		"should have 1 auth category case")

	// Verify sub-scenarios are present.
	subScenarios := make(map[string]bool)
	for _, tc := range testCases {
		subScenarios[tc.SubScenario] = true
	}
	for _, expected := range []string{"52a", "52b", "52c", "52d", "52e", "52f", "52g", "52h"} {
		assert.True(t, subScenarios[expected],
			"sub-scenario %s should be present in test cases", expected)
	}
}
