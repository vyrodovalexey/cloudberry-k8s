//go:build e2e

package e2e

import (
	"bytes"
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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 52 E2E: Negative / Edge Cases
// ============================================================================
//
// End-to-end tests for negative and edge cases across 8 sub-scenarios:
//   52a - JWT with wrong issuer
//   52b - JWT with wrong audience
//   52c - Expired JWT
//   52d - JWT with future iat (behavioral test)
//   52e - Token refresh failure (expired token rejected)
//   52f - Vault connection retry (RetryWithBackoff)
//   52g - Invalid OIDC configuration / Basic auth fallback
//   52h - Missing admin secret (empty credential store)
//
// Two suites:
//   - Scenario52NegativeEdgeCaseE2ESuite - mock-based (fake K8s client)
//   - Scenario52RealClusterE2ESuite - real Cloudberry cluster via port-forward
// ============================================================================

// signJWT52E2E creates a minimal RS256-signed JWT with the given claims.
func signJWT52E2E(t *testing.T, key *rsa.PrivateKey, claims map[string]interface{}) string {
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

// setupOIDCServer52E2E creates a mock OIDC server with discovery and JWKS endpoints.
func setupOIDCServer52E2E(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
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
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":"%s","e":"%s","kid":"test-key-52e2e"}]}`,
			n, e,
		)))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)

	return server
}

// ============================================================================
// Suite 1: Scenario52NegativeEdgeCaseE2ESuite (mock-based)
// ============================================================================

// Scenario52NegativeEdgeCaseE2ESuite tests Scenario 52: Negative/Edge Cases end-to-end.
type Scenario52NegativeEdgeCaseE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario52(t *testing.T) {
	suite.Run(t, new(Scenario52NegativeEdgeCaseE2ESuite))
}

// --- 52a: JWT with wrong issuer ---

// TestE2E_Scenario52a_JWTWrongIssuer verifies that a JWT signed with the
// correct key but containing a wrong issuer claim is rejected with 401.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52a_JWTWrongIssuer() {
	s.logger.Info("starting scenario 52a E2E: JWT with wrong issuer")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Sign JWT with a different issuer (but using the correct key).
	claims := map[string]interface{}{
		"iss": "http://different-issuer/realms/other",
		"sub": "user-wrong-issuer",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

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

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong issuer should be rejected with 401")

	s.logger.Info("scenario 52a E2E: JWT with wrong issuer completed")
}

// --- 52b: JWT with wrong audience ---

// TestE2E_Scenario52b_JWTWrongAudience verifies that a JWT with the correct
// issuer and key but wrong audience claim is rejected with 401.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52b_JWTWrongAudience() {
	s.logger.Info("starting scenario 52b E2E: JWT with wrong audience")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "correct-client",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Sign JWT with wrong audience.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-wrong-aud",
		"aud": "wrong-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong audience should be rejected with 401")

	s.logger.Info("scenario 52b E2E: JWT with wrong audience completed")
}

// --- 52c: Expired JWT ---

// TestE2E_Scenario52c_JWTExpired verifies that an expired JWT is rejected with 401.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52c_JWTExpired() {
	s.logger.Info("starting scenario 52c E2E: expired JWT")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Sign JWT with exp in the past.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-expired",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(-1 * time.Hour).Unix()),
		"iat": float64(time.Now().Add(-2 * time.Hour).Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"expired JWT should be rejected with 401")

	s.logger.Info("scenario 52c E2E: expired JWT completed")
}

// --- 52d: JWT with future iat ---

// TestE2E_Scenario52d_JWTFutureIAT documents the behavior of gooidc when a
// JWT has a future iat (issued-at) claim. The gooidc library does NOT validate
// iat, so the token is accepted. This is a behavioral/documentation test.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52d_JWTFutureIAT() {
	s.logger.Info("starting scenario 52d E2E: JWT with future iat")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Sign JWT with iat in the future but valid exp.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-future-iat",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(2 * time.Hour).Unix()),
		"iat": float64(time.Now().Add(1 * time.Hour).Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, authErr := provider.Authenticate(context.Background(), req)

	// Document behavior: gooidc does NOT reject future iat.
	s.T().Log("Behavioral test: gooidc does NOT validate iat (issued-at) claims")
	s.T().Log("A JWT with future iat is accepted as long as signature, issuer, audience, and expiry are valid")

	if authErr == nil {
		// Token was accepted - this is the expected gooidc behavior.
		require.NotNil(s.T(), identity)
		assert.Equal(s.T(), "user-future-iat", identity.Username,
			"gooidc should accept token with future iat")
		assert.Equal(s.T(), "oidc", identity.AuthMethod)
		s.T().Log("CONFIRMED: gooidc accepts JWT with future iat (no iat validation)")
	} else {
		// If a future version of gooidc starts validating iat, this branch documents that change.
		s.T().Logf("NOTE: gooidc rejected future iat token: %v", authErr)
		s.T().Log("This indicates gooidc behavior has changed to validate iat claims")
	}

	s.logger.Info("scenario 52d E2E: JWT with future iat completed")
}

// --- 52e: Token refresh failure ---

// TestE2E_Scenario52e_TokenRefreshFailure verifies that an expired access
// token (simulating a failed refresh) is rejected with 401 and the error
// response contains "authentication failed".
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52e_TokenRefreshFailure() {
	s.logger.Info("starting scenario 52e E2E: token refresh failure")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	cfg := auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
	}
	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(s.T(), err)

	// Sign JWT with exp in the past (simulating expired access token after refresh failure).
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-refresh-failed",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(-30 * time.Minute).Unix()),
		"iat": float64(time.Now().Add(-1 * time.Hour).Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	store := auth.NewInMemoryCredentialStore()
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	middleware := auth.NewAuthMiddleware(basicProvider, provider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"expired token (refresh failure) should return 401")

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), string(body), "authentication failed",
		"error response should contain 'authentication failed'")

	s.logger.Info("scenario 52e E2E: token refresh failure completed")
}

// --- 52f: Vault connection retry ---

// TestE2E_Scenario52f_VaultRetryExhausted verifies that RetryWithBackoff
// returns ErrRetryExhausted when all retry attempts fail.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52f_VaultRetryExhausted() {
	s.logger.Info("starting scenario 52f E2E: vault retry exhausted")

	var attempts atomic.Int32

	opts := util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.0,
	}

	fn := func(_ context.Context) error {
		attempts.Add(1)
		return fmt.Errorf("always fails")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := util.RetryWithBackoff(ctx, opts, fn)
	require.Error(s.T(), err, "should return error when all retries exhausted")
	assert.True(s.T(), errors.Is(err, util.ErrRetryExhausted),
		"error should wrap ErrRetryExhausted, got: %v", err)

	// MaxRetries=3 means 1 initial attempt + 3 retries = 4 total attempts.
	assert.Equal(s.T(), int32(4), attempts.Load(),
		"should have attempted 4 times (1 initial + 3 retries)")

	s.logger.Info("scenario 52f E2E: vault retry exhausted completed")
}

// TestE2E_Scenario52f_VaultRetryRecovery verifies that RetryWithBackoff
// succeeds when the function fails 3 times then succeeds on the 4th attempt.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52f_VaultRetryRecovery() {
	s.logger.Info("starting scenario 52f E2E: vault retry recovery")

	var attempts atomic.Int32
	recoverAttempt := int32(4)

	opts := util.RetryOptions{
		MaxRetries:     5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
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
	require.NoError(s.T(), err, "should succeed after recovery on attempt %d", recoverAttempt)
	assert.Equal(s.T(), recoverAttempt, attempts.Load(),
		"should have attempted exactly %d times", recoverAttempt)

	s.logger.Info("scenario 52f E2E: vault retry recovery completed")
}

// --- 52g: Invalid OIDC configuration ---

// TestE2E_Scenario52g_InvalidOIDCConfig verifies that NewOIDCProvider returns
// an error when the issuer URL is unreachable.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52g_InvalidOIDCConfig() {
	s.logger.Info("starting scenario 52g E2E: invalid OIDC config")

	cfg := auth.OIDCConfig{
		IssuerURL: "http://unreachable.invalid:9999/realms/test",
		ClientID:  "cloudberry-operator",
	}

	provider, err := auth.NewOIDCProvider(context.Background(), cfg, nil)
	require.Error(s.T(), err, "NewOIDCProvider should fail with unreachable issuer")
	assert.Nil(s.T(), provider, "provider should be nil on error")
	s.T().Logf("Expected error from unreachable OIDC issuer: %v", err)

	s.logger.Info("scenario 52g E2E: invalid OIDC config completed")
}

// TestE2E_Scenario52g_BasicAuthFallback verifies that when OIDC is not
// available (nil provider), Basic auth still works and Bearer tokens are rejected.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52g_BasicAuthFallback() {
	s.logger.Info("starting scenario 52g E2E: basic auth fallback")

	// Create API server with valid Basic auth but nil OIDC provider.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("gpadmin", "admin-password", auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// nil OIDC provider - simulating failed OIDC initialization.
	middleware := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, middleware, nil, &metrics.NoopRecorder{}, logger, 0)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	s.T().Run("basic_auth_works_without_oidc", func(t *testing.T) {
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
		require.NoError(t, err)
		req.SetBasicAuth("gpadmin", "admin-password")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"Basic auth should work when OIDC is not available")
	})

	s.T().Run("bearer_token_rejected_without_oidc", func(t *testing.T) {
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
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

	s.logger.Info("scenario 52g E2E: basic auth fallback completed")
}

// --- 52h: Missing admin secret ---

// TestE2E_Scenario52h_MissingAdminSecret verifies that Basic auth fails with
// 401 when the credential store is empty (no admin password configured).
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52h_MissingAdminSecret() {
	s.logger.Info("starting scenario 52h E2E: missing admin secret")

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

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "any-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"empty credential store should cause 401")

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), string(body), "authentication failed",
		"error response should contain 'authentication failed'")

	s.logger.Info("scenario 52h E2E: missing admin secret completed")
}

// --- Coverage test ---

// TestE2E_Scenario52_NegativeEdgeCaseCases_Coverage verifies that the
// NegativeEdgeCaseCases catalog returns the expected 8 cases with correct categories.
func (s *Scenario52NegativeEdgeCaseE2ESuite) TestE2E_Scenario52_NegativeEdgeCaseCases_Coverage() {
	s.logger.Info("starting scenario 52 E2E: negative edge case cases coverage")

	testCases := cases.NegativeEdgeCaseCases()
	require.Len(s.T(), testCases, 8, "NegativeEdgeCaseCases should return 8 test cases")

	// Count categories.
	categoryCounts := make(map[string]int)
	for _, tc := range testCases {
		categoryCounts[tc.Category]++
		s.T().Logf("Case: %s (sub-scenario: %s, category: %s)", tc.Name, tc.SubScenario, tc.Category)
	}

	assert.Equal(s.T(), 5, categoryCounts["jwt"],
		"should have 5 jwt category cases")
	assert.Equal(s.T(), 1, categoryCounts["vault"],
		"should have 1 vault category case")
	assert.Equal(s.T(), 1, categoryCounts["config"],
		"should have 1 config category case")
	assert.Equal(s.T(), 1, categoryCounts["auth"],
		"should have 1 auth category case")

	// Verify sub-scenarios are present.
	subScenarios := make(map[string]bool)
	for _, tc := range testCases {
		subScenarios[tc.SubScenario] = true
	}
	for _, expected := range []string{"52a", "52b", "52c", "52d", "52e", "52f", "52g", "52h"} {
		assert.True(s.T(), subScenarios[expected],
			"sub-scenario %s should be present in test cases", expected)
	}

	s.logger.Info("scenario 52 E2E: negative edge case cases coverage completed")
}

// ============================================================================
// Suite 2: Scenario52RealClusterE2ESuite (real cluster)
// ============================================================================
//
// These tests connect to the real Cloudberry cluster running in Kubernetes
// to verify negative/edge case behavior on an API server backed by a real
// database connection.
// ============================================================================

// Scenario52RealClusterE2ESuite tests Scenario 52 against the real Cloudberry cluster.
type Scenario52RealClusterE2ESuite struct {
	E2ESuite

	dbClient       db.Client
	portForwardCmd *exec.Cmd
	localPort      int
}

func TestE2E_Scenario52_RealCluster(t *testing.T) {
	suite.Run(t, new(Scenario52RealClusterE2ESuite))
}

func (s *Scenario52RealClusterE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()
	s.logger.Info("scenario 52 real cluster E2E suite setup")

	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		err = s.portForwardCmd.Start()
		if err != nil {
			s.T().Skipf("skipping scenario 52 real cluster E2E: kubectl port-forward failed: %v", err)
			return
		}

		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward()
			s.T().Skipf("skipping scenario 52 real cluster E2E: port-forward not ready")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := db.Config{
		Host:     host,
		Port:     int32(port),
		Database: database,
		Username: user,
		Password: password,
		SSLMode:  "disable",
		MaxConns: 3,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
	}

	dbClient, err := db.NewClient(ctx, cfg, s.logger)
	if err != nil {
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 52 real cluster E2E: cannot connect: %v", err)
		return
	}
	s.dbClient = dbClient

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 52 real cluster E2E: ping failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 52",
		"host", host, "port", port)
}

func (s *Scenario52RealClusterE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward()
	s.E2ESuite.TearDownSuite()
}

func (s *Scenario52RealClusterE2ESuite) cleanupPortForward() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		_ = s.portForwardCmd.Process.Kill()
		_ = s.portForwardCmd.Wait()
		s.portForwardCmd = nil
	}
}

// newRealClusterOIDCServer creates an API server backed by the real DB client
// with both basic auth and an optional OIDC provider for real cluster E2E tests.
func (s *Scenario52RealClusterE2ESuite) newRealClusterOIDCServer(
	oidcProvider auth.Provider,
) (*httptest.Server, *api.Server, *bytes.Buffer) {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)

	ts := httptest.NewServer(server.Handler())
	return ts, server, logBuf
}

// --- Real Cluster E2E Tests ---

// TestE2E_Scenario52a_RealCluster_JWTWrongIssuer verifies that a JWT with
// wrong issuer is rejected with 401 on an API server backed by a real DB.
func (s *Scenario52RealClusterE2ESuite) TestE2E_Scenario52a_RealCluster_JWTWrongIssuer() {
	s.logger.Info("starting scenario 52a real cluster: JWT with wrong issuer")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	ctx := context.Background()
	oidcProvider, err := auth.NewOIDCProvider(ctx, auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "test-client",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   map[string]string{"admin": "Admin"},
	}, nil)
	require.NoError(s.T(), err)

	ts, server, _ := s.newRealClusterOIDCServer(oidcProvider)
	defer ts.Close()
	defer server.Close()

	// Sign JWT with wrong issuer.
	claims := map[string]interface{}{
		"iss": "http://different-issuer/realms/other",
		"sub": "user-wrong-issuer",
		"aud": "test-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong issuer should be rejected with 401 on real cluster")

	s.logger.Info("scenario 52a real cluster: JWT with wrong issuer completed")
}

// TestE2E_Scenario52b_RealCluster_JWTWrongAudience verifies that a JWT with
// wrong audience is rejected with 401 on an API server backed by a real DB.
func (s *Scenario52RealClusterE2ESuite) TestE2E_Scenario52b_RealCluster_JWTWrongAudience() {
	s.logger.Info("starting scenario 52b real cluster: JWT with wrong audience")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	ctx := context.Background()
	oidcProvider, err := auth.NewOIDCProvider(ctx, auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "correct-client",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   map[string]string{"admin": "Admin"},
	}, nil)
	require.NoError(s.T(), err)

	ts, server, _ := s.newRealClusterOIDCServer(oidcProvider)
	defer ts.Close()
	defer server.Close()

	// Sign JWT with wrong audience.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-wrong-aud",
		"aud": "wrong-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"JWT with wrong audience should be rejected with 401 on real cluster")

	s.logger.Info("scenario 52b real cluster: JWT with wrong audience completed")
}

// TestE2E_Scenario52c_RealCluster_JWTExpired verifies that an expired JWT is
// rejected with 401 on an API server backed by a real DB.
func (s *Scenario52RealClusterE2ESuite) TestE2E_Scenario52c_RealCluster_JWTExpired() {
	s.logger.Info("starting scenario 52c real cluster: expired JWT")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(s.T(), err)

	oidcServer := setupOIDCServer52E2E(s.T(), key)

	ctx := context.Background()
	oidcProvider, err := auth.NewOIDCProvider(ctx, auth.OIDCConfig{
		IssuerURL:     oidcServer.URL,
		ClientID:      "cloudberry-operator",
		RoleClaimPath: "realm_access.roles",
		RoleMatchMode: "exact",
		RoleMapping:   map[string]string{"admin": "Admin"},
	}, nil)
	require.NoError(s.T(), err)

	ts, server, _ := s.newRealClusterOIDCServer(oidcProvider)
	defer ts.Close()
	defer server.Close()

	// Sign JWT with exp in the past.
	claims := map[string]interface{}{
		"iss": oidcServer.URL,
		"sub": "user-expired",
		"aud": "cloudberry-operator",
		"exp": float64(time.Now().Add(-1 * time.Hour).Unix()),
		"iat": float64(time.Now().Add(-2 * time.Hour).Unix()),
	}
	token := signJWT52E2E(s.T(), key, claims)

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"expired JWT should be rejected with 401 on real cluster")

	s.logger.Info("scenario 52c real cluster: expired JWT completed")
}

// TestE2E_Scenario52g_RealCluster_BasicAuthFallback verifies that Basic auth
// works and Bearer tokens are rejected when OIDC is nil, on a real-DB-backed server.
func (s *Scenario52RealClusterE2ESuite) TestE2E_Scenario52g_RealCluster_BasicAuthFallback() {
	s.logger.Info("starting scenario 52g real cluster: basic auth fallback")

	// Create server with nil OIDC provider.
	ts, server, _ := s.newRealClusterOIDCServer(nil)
	defer ts.Close()
	defer server.Close()

	s.T().Run("basic_auth_works", func(t *testing.T) {
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
		require.NoError(t, err)
		req.SetBasicAuth("admin", "admin-secret")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"Basic auth should work when OIDC is not available on real cluster")
	})

	s.T().Run("bearer_rejected", func(t *testing.T) {
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer some-token")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
			"Bearer token should be rejected when OIDC is nil on real cluster")
	})

	s.logger.Info("scenario 52g real cluster: basic auth fallback completed")
}

// TestE2E_Scenario52h_RealCluster_EmptyCredentialStore verifies that an empty
// credential store causes 401 on a real-DB-backed server.
func (s *Scenario52RealClusterE2ESuite) TestE2E_Scenario52h_RealCluster_EmptyCredentialStore() {
	s.logger.Info("starting scenario 52h real cluster: empty credential store")

	// Create server with empty credential store (override newRealClusterOIDCServer pattern).
	emptyStore := auth.NewInMemoryCredentialStore()

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(emptyStore, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "any-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode,
		"empty credential store should cause 401 on real cluster")

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), string(body), "authentication failed",
		"error response should contain 'authentication failed'")

	s.logger.Info("scenario 52h real cluster: empty credential store completed")
}
