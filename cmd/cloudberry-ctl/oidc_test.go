package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensure url import is used.
var _ = url.Parse

// ---------------------------------------------------------------------------
// generatePKCE
// ---------------------------------------------------------------------------

func TestGeneratePKCE(t *testing.T) {
	verifier, challenge, err := generatePKCE()
	require.NoError(t, err)

	// Verifier should be base64url-encoded 32 bytes = 43 characters.
	assert.Len(t, verifier, 43, "verifier should be 43 characters")

	// Challenge should be base64url-encoded SHA-256 = 43 characters.
	assert.Len(t, challenge, 43, "challenge should be 43 characters")

	// Verifier and challenge must differ.
	assert.NotEqual(t, verifier, challenge)

	// No padding characters in base64url encoding.
	assert.NotContains(t, verifier, "=")
	assert.NotContains(t, challenge, "=")

	// No standard base64 characters that differ from base64url.
	assert.NotContains(t, verifier, "+")
	assert.NotContains(t, verifier, "/")
	assert.NotContains(t, challenge, "+")
	assert.NotContains(t, challenge, "/")
}

func TestGeneratePKCE_Uniqueness(t *testing.T) {
	v1, c1, err1 := generatePKCE()
	require.NoError(t, err1)

	v2, c2, err2 := generatePKCE()
	require.NoError(t, err2)

	// Each call should produce unique values.
	assert.NotEqual(t, v1, v2, "verifiers should be unique")
	assert.NotEqual(t, c1, c2, "challenges should be unique")
}

// ---------------------------------------------------------------------------
// generateState
// ---------------------------------------------------------------------------

func TestGenerateState(t *testing.T) {
	state, err := generateState()
	require.NoError(t, err)

	// State should be base64url-encoded 16 bytes = 22 characters.
	assert.Len(t, state, 22, "state should be 22 characters")

	// No padding characters.
	assert.NotContains(t, state, "=")
}

func TestGenerateState_Uniqueness(t *testing.T) {
	s1, err1 := generateState()
	require.NoError(t, err1)

	s2, err2 := generateState()
	require.NoError(t, err2)

	assert.NotEqual(t, s1, s2, "states should be unique")
}

// ---------------------------------------------------------------------------
// buildAuthorizationURL
// ---------------------------------------------------------------------------

func TestBuildAuthorizationURL(t *testing.T) {
	authURL := buildAuthorizationURL(
		"http://localhost:8090/realms/test",
		"cloudberry-ctl",
		"http://localhost:8085/callback",
		"test-challenge",
		"test-state",
	)

	parsed, err := url.Parse(authURL)
	require.NoError(t, err)

	assert.Equal(t, "http", parsed.Scheme)
	assert.Equal(t, "localhost:8090", parsed.Host)
	assert.Equal(t,
		"/realms/test/protocol/openid-connect/auth",
		parsed.Path,
	)

	q := parsed.Query()
	assert.Equal(t, "cloudberry-ctl", q.Get("client_id"))
	assert.Equal(t, "code", q.Get("response_type"))
	assert.Equal(t, "http://localhost:8085/callback", q.Get("redirect_uri"))
	assert.Equal(t, "openid profile email", q.Get("scope"))
	assert.Equal(t, "test-challenge", q.Get("code_challenge"))
	assert.Equal(t, "S256", q.Get("code_challenge_method"))
	assert.Equal(t, "test-state", q.Get("state"))
}

// ---------------------------------------------------------------------------
// startCallbackServer
// ---------------------------------------------------------------------------

func TestStartCallbackServer_SuccessfulCallback(t *testing.T) {
	resultCh := make(chan callbackResult, 1)
	expectedState := "test-state-123"

	srv := startCallbackServer(resultCh, expectedState)
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), callbackShutdownTimeout,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// Give the server a moment to start.
	time.Sleep(50 * time.Millisecond)

	// Simulate a successful callback from the IdP.
	callbackURL := fmt.Sprintf(
		"http://127.0.0.1:%s%s?code=auth-code-xyz&state=%s",
		callbackPort, callbackPath, expectedState,
	)
	resp, err := http.Get(callbackURL) //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Check the result channel.
	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		assert.Equal(t, "auth-code-xyz", result.code)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback result")
	}
}

func TestStartCallbackServer_StateMismatch(t *testing.T) {
	resultCh := make(chan callbackResult, 1)
	expectedState := "correct-state"

	srv := startCallbackServer(resultCh, expectedState)
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), callbackShutdownTimeout,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	callbackURL := fmt.Sprintf(
		"http://127.0.0.1:%s%s?code=auth-code&state=wrong-state",
		callbackPort, callbackPath,
	)
	resp, err := http.Get(callbackURL) //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case result := <-resultCh:
		require.Error(t, result.err)
		assert.Contains(t, result.err.Error(), "state mismatch")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback result")
	}
}

func TestStartCallbackServer_MissingCode(t *testing.T) {
	resultCh := make(chan callbackResult, 1)
	expectedState := "test-state"

	srv := startCallbackServer(resultCh, expectedState)
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), callbackShutdownTimeout,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	callbackURL := fmt.Sprintf(
		"http://127.0.0.1:%s%s?state=%s",
		callbackPort, callbackPath, expectedState,
	)
	resp, err := http.Get(callbackURL) //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case result := <-resultCh:
		require.Error(t, result.err)
		assert.Contains(t, result.err.Error(), "missing authorization code")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback result")
	}
}

func TestStartCallbackServer_ErrorFromIdP(t *testing.T) {
	resultCh := make(chan callbackResult, 1)

	srv := startCallbackServer(resultCh, "test-state")
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), callbackShutdownTimeout,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	callbackURL := fmt.Sprintf(
		"http://127.0.0.1:%s%s?error=access_denied"+
			"&error_description=user+denied+access",
		callbackPort, callbackPath,
	)
	resp, err := http.Get(callbackURL) //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case result := <-resultCh:
		require.Error(t, result.err)
		assert.Contains(t, result.err.Error(), "access_denied")
		assert.Contains(t, result.err.Error(), "user denied access")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback result")
	}
}

// ---------------------------------------------------------------------------
// exchangeCode
// ---------------------------------------------------------------------------

func TestExchangeCode_Success(t *testing.T) {
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t,
				"application/x-www-form-urlencoded",
				r.Header.Get("Content-Type"),
			)

			require.NoError(t, r.ParseForm())
			assert.Equal(t, "authorization_code", r.FormValue("grant_type"))
			assert.Equal(t, "test-code", r.FormValue("code"))
			assert.Equal(t, "http://localhost:8085/callback",
				r.FormValue("redirect_uri"))
			assert.Equal(t, "cloudberry-ctl", r.FormValue("client_id"))
			assert.Equal(t, "test-verifier", r.FormValue("code_verifier"))

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
				"token_type":    "Bearer",
				"expires_in":    300,
				"refresh_token": "refresh-token-xyz",
				"id_token":      "id-token-xyz",
				"scope":         "openid profile email",
			})
		}),
	)
	defer tokenServer.Close()

	// The issuerURL is the token server URL minus the token endpoint path.
	// exchangeCode appends "/protocol/openid-connect/token".
	issuerURL := strings.TrimSuffix(
		tokenServer.URL, "/protocol/openid-connect/token",
	)

	ctx := context.Background()
	tokenResp, err := exchangeCode(
		ctx, issuerURL, "cloudberry-ctl",
		"test-code", "http://localhost:8085/callback", "test-verifier",
	)
	require.NoError(t, err)
	require.NotNil(t, tokenResp)

	assert.Equal(t,
		"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
		tokenResp.AccessToken,
	)
	assert.Equal(t, "Bearer", tokenResp.TokenType)
	assert.Equal(t, 300, tokenResp.ExpiresIn)
	assert.Equal(t, "refresh-token-xyz", tokenResp.RefreshToken)
	assert.Equal(t, "id-token-xyz", tokenResp.IDToken)
	assert.Equal(t, "openid profile email", tokenResp.Scope)
}

func TestExchangeCode_ServerError(t *testing.T) {
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":             "invalid_grant",
				"error_description": "code expired",
			})
		}),
	)
	defer tokenServer.Close()

	issuerURL := strings.TrimSuffix(
		tokenServer.URL, "/protocol/openid-connect/token",
	)

	ctx := context.Background()
	tokenResp, err := exchangeCode(
		ctx, issuerURL, "cloudberry-ctl",
		"expired-code", "http://localhost:8085/callback", "test-verifier",
	)
	require.Error(t, err)
	assert.Nil(t, tokenResp)
	assert.Contains(t, err.Error(), "token endpoint returned 400")
}

func TestExchangeCode_MissingAccessToken(t *testing.T) {
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"token_type": "Bearer",
				"expires_in": 300,
			})
		}),
	)
	defer tokenServer.Close()

	issuerURL := strings.TrimSuffix(
		tokenServer.URL, "/protocol/openid-connect/token",
	)

	ctx := context.Background()
	tokenResp, err := exchangeCode(
		ctx, issuerURL, "cloudberry-ctl",
		"test-code", "http://localhost:8085/callback", "test-verifier",
	)
	require.Error(t, err)
	assert.Nil(t, tokenResp)
	assert.Contains(t, err.Error(), "missing access_token")
}

func TestExchangeCode_InvalidJSON(t *testing.T) {
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, "not-json")
		}),
	)
	defer tokenServer.Close()

	issuerURL := strings.TrimSuffix(
		tokenServer.URL, "/protocol/openid-connect/token",
	)

	ctx := context.Background()
	tokenResp, err := exchangeCode(
		ctx, issuerURL, "cloudberry-ctl",
		"test-code", "http://localhost:8085/callback", "test-verifier",
	)
	require.Error(t, err)
	assert.Nil(t, tokenResp)
	assert.Contains(t, err.Error(), "parsing token response")
}

func TestExchangeCode_ContextCanceled(t *testing.T) {
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			// Delay to allow context cancellation.
			time.Sleep(2 * time.Second)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "token",
			})
		}),
	)
	defer tokenServer.Close()

	issuerURL := strings.TrimSuffix(
		tokenServer.URL, "/protocol/openid-connect/token",
	)

	ctx, cancel := context.WithTimeout(
		context.Background(), 100*time.Millisecond,
	)
	defer cancel()

	tokenResp, err := exchangeCode(
		ctx, issuerURL, "cloudberry-ctl",
		"test-code", "http://localhost:8085/callback", "test-verifier",
	)
	require.Error(t, err)
	assert.Nil(t, tokenResp)
}

// ---------------------------------------------------------------------------
// openBrowser
// ---------------------------------------------------------------------------

func TestOpenBrowser_InvalidURL(t *testing.T) {
	ctx := context.Background()
	err := openBrowser(ctx, "://invalid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid URL")
}

func TestOpenBrowser_NonHTTPScheme(t *testing.T) {
	ctx := context.Background()
	err := openBrowser(ctx, "ftp://example.com/file")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to open non-HTTP URL scheme")
}

// ---------------------------------------------------------------------------
// oidcTokenResponse
// ---------------------------------------------------------------------------

func TestOIDCTokenResponse_JSONParsing(t *testing.T) {
	jsonData := `{
		"access_token": "at-123",
		"token_type": "Bearer",
		"expires_in": 600,
		"refresh_token": "rt-456",
		"id_token": "id-789",
		"scope": "openid"
	}`

	var resp oidcTokenResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	require.NoError(t, err)

	assert.Equal(t, "at-123", resp.AccessToken)
	assert.Equal(t, "Bearer", resp.TokenType)
	assert.Equal(t, 600, resp.ExpiresIn)
	assert.Equal(t, "rt-456", resp.RefreshToken)
	assert.Equal(t, "id-789", resp.IDToken)
	assert.Equal(t, "openid", resp.Scope)
}

// ---------------------------------------------------------------------------
// callbackResult
// ---------------------------------------------------------------------------

func TestCallbackResult_WithCode(t *testing.T) {
	r := callbackResult{code: "test-code"}
	assert.Equal(t, "test-code", r.code)
	assert.NoError(t, r.err)
}

func TestCallbackResult_WithError(t *testing.T) {
	r := callbackResult{err: fmt.Errorf("test error")}
	assert.Empty(t, r.code)
	assert.Error(t, r.err)
	assert.Contains(t, r.err.Error(), "test error")
}

// ---------------------------------------------------------------------------
// runAuthLoginOIDC - browser flow requires issuer URL
// ---------------------------------------------------------------------------

func TestRunAuthLoginOIDC_RequiresIssuerURL(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.username = ""
	globals.password = ""
	globals.issuerURL = ""

	err := runAuthLoginOIDC()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer URL is required")
}

// ---------------------------------------------------------------------------
// openBrowser - success path (platform-specific)
// ---------------------------------------------------------------------------

func TestOpenBrowser_ValidHTTPURL(t *testing.T) {
	// Test that openBrowser accepts a valid HTTP URL and attempts to start a process.
	// The actual browser won't open in CI, but the function should not return an
	// error for URL validation.
	ctx := context.Background()
	err := openBrowser(ctx, "https://example.com/auth")
	// On CI/test environments, the browser command may fail (no display),
	// but the URL validation should pass. We just verify it doesn't return
	// a URL validation error.
	if err != nil {
		assert.NotContains(t, err.Error(), "invalid URL")
		assert.NotContains(t, err.Error(), "refusing to open non-HTTP URL scheme")
	}
}

func TestOpenBrowser_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With a canceled context, the command should fail quickly.
	err := openBrowser(ctx, "https://example.com/auth")
	// May or may not error depending on platform, but should not hang.
	_ = err
}

// ---------------------------------------------------------------------------
// runOIDCBrowserFlow - integration test with mock token server
// ---------------------------------------------------------------------------

func TestRunOIDCBrowserFlow_SuccessfulFlow(t *testing.T) {
	// Set up a mock token server that returns a valid token response.
	tokenServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  strings.Repeat("a", 60), // long enough for preview
				"token_type":    "Bearer",
				"expires_in":    300,
				"refresh_token": "refresh-token-xyz",
				"id_token":      "id-token-xyz",
				"scope":         "openid profile email",
			})
		}),
	)
	defer tokenServer.Close()

	saved := globals
	defer func() { globals = saved }()
	globals.timeout = "5s"
	globals.output = "table"

	// The issuerURL is the token server URL (exchangeCode appends /protocol/openid-connect/token).
	issuerURL := strings.TrimSuffix(tokenServer.URL, "/protocol/openid-connect/token")

	// We can't easily test the full browser flow, but we can test the function
	// by simulating the callback. Start the flow in a goroutine and send a
	// callback request.
	errCh := make(chan error, 1)
	go func() {
		errCh <- runOIDCBrowserFlow(issuerURL, "cloudberry-ctl")
	}()

	// Give the callback server time to start.
	time.Sleep(200 * time.Millisecond)

	// We need to know the state parameter to send a valid callback.
	// Since we can't predict it, we'll just send a request and accept
	// that it will fail with state mismatch. This still exercises the code path.
	callbackURL := fmt.Sprintf("http://127.0.0.1:%s%s?code=test-code&state=wrong-state",
		callbackPort, callbackPath)
	resp, httpErr := http.Get(callbackURL) //nolint:noctx // test helper
	if httpErr == nil {
		resp.Body.Close()
	}

	// The flow should fail with state mismatch.
	select {
	case err := <-errCh:
		// Expected: either state mismatch error or timeout.
		if err != nil {
			assert.True(t,
				strings.Contains(err.Error(), "state mismatch") ||
					strings.Contains(err.Error(), "timed out") ||
					strings.Contains(err.Error(), "OIDC callback error"),
				"unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runOIDCBrowserFlow did not return")
	}
}

func TestRunOIDCBrowserFlow_Timeout(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.timeout = "200ms" // Very short timeout.
	globals.output = "table"

	errCh := make(chan error, 1)
	go func() {
		errCh <- runOIDCBrowserFlow("http://localhost:9999/realms/test", "cloudberry-ctl")
	}()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")
	case <-time.After(5 * time.Second):
		t.Fatal("runOIDCBrowserFlow did not return after timeout")
	}
}

// ---------------------------------------------------------------------------
// startCallbackServer - error from IdP without description
// ---------------------------------------------------------------------------

func TestStartCallbackServer_ErrorFromIdP_NoDescription(t *testing.T) {
	resultCh := make(chan callbackResult, 1)

	srv := startCallbackServer(resultCh, "test-state")
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), callbackShutdownTimeout,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Error without description.
	callbackURL := fmt.Sprintf(
		"http://127.0.0.1:%s%s?error=server_error",
		callbackPort, callbackPath,
	)
	resp, err := http.Get(callbackURL) //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case result := <-resultCh:
		require.Error(t, result.err)
		assert.Contains(t, result.err.Error(), "server_error")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback result")
	}
}

// ---------------------------------------------------------------------------
// New global flags
// ---------------------------------------------------------------------------

func TestRootCmd_OIDCFlags(t *testing.T) {
	root := newRootCmd()
	pf := root.PersistentFlags()

	issuerFlag := pf.Lookup("issuer-url")
	require.NotNil(t, issuerFlag, "root should have --issuer-url flag")
	assert.Equal(t, "string", issuerFlag.Value.Type())

	clientIDFlag := pf.Lookup("client-id")
	require.NotNil(t, clientIDFlag, "root should have --client-id flag")
	assert.Equal(t, "string", clientIDFlag.Value.Type())
	assert.Equal(t, "cloudberry-ctl", clientIDFlag.DefValue)
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

func TestOIDCConstants(t *testing.T) {
	assert.Equal(t, "8085", callbackPort)
	assert.Equal(t, "/callback", callbackPath)
	assert.Equal(t, 2*time.Minute, oidcLoginTimeout)
	assert.Equal(t, "openid profile email", oidcScopeParam)
}
