package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// OIDC PKCE constants.
const (
	// pkceVerifierLength is the length of the PKCE code verifier in bytes
	// before base64url encoding. 32 bytes produces a 43-character verifier.
	pkceVerifierLength = 32

	// stateLength is the length of the random state parameter in bytes.
	stateLength = 16

	// callbackPort is the local port for the OIDC callback server.
	callbackPort = "8085"

	// callbackPath is the path the IdP redirects to after authentication.
	callbackPath = "/callback"

	// oidcLoginTimeout is the maximum time to wait for the user to complete
	// the browser-based login flow.
	oidcLoginTimeout = 2 * time.Minute

	// callbackShutdownTimeout is the grace period for the callback server
	// to finish serving the response before shutting down.
	callbackShutdownTimeout = 3 * time.Second

	// maxTokenResponseSize is the maximum allowed token response body size (1 MiB).
	maxTokenResponseSize = 1 << 20

	// oidcScopeParam is the default OIDC scope parameter value.
	oidcScopeParam = "openid profile email"
)

// generatePKCE creates a PKCE code verifier and its corresponding S256
// code challenge. The verifier is a cryptographically random base64url
// string (43 characters for 32 random bytes). The challenge is the
// base64url-encoded SHA-256 hash of the verifier.
func generatePKCE() (verifier, challenge string, err error) {
	buf := make([]byte, pkceVerifierLength)
	if _, err = rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generating PKCE verifier: %w", err)
	}

	verifier = base64.RawURLEncoding.EncodeToString(buf)

	h := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(h[:])

	return verifier, challenge, nil
}

// generateState creates a cryptographically random state parameter for
// CSRF protection during the OIDC authorization flow.
func generateState() (string, error) {
	buf := make([]byte, stateLength)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating state parameter: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// buildAuthorizationURL constructs the OIDC authorization endpoint URL
// with all required parameters for the Authorization Code flow with PKCE.
func buildAuthorizationURL(issuerURL, clientID, redirectURI, challenge, state string) string {
	params := url.Values{
		"client_id":             {clientID},
		"response_type":         {"code"},
		"redirect_uri":          {redirectURI},
		"scope":                 {oidcScopeParam},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
		"state":                 {state},
	}
	return issuerURL + "/protocol/openid-connect/auth?" + params.Encode()
}

// callbackResult holds the result received from the OIDC callback.
type callbackResult struct {
	code string
	err  error
}

// startCallbackServer starts a local HTTP server that listens for the OIDC
// authorization callback. It validates the state parameter and sends the
// authorization code (or error) on the result channel. The server binds to
// localhost only to prevent external access.
func startCallbackServer(resultCh chan<- callbackResult, expectedState string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(callbackPath, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		// Check for error response from the IdP.
		if errCode := query.Get("error"); errCode != "" {
			errDesc := query.Get("error_description")
			msg := fmt.Sprintf("authorization failed: %s", errCode)
			if errDesc != "" {
				msg += " - " + errDesc
			}
			http.Error(w, msg, http.StatusBadRequest)
			resultCh <- callbackResult{err: fmt.Errorf("%s", msg)}
			return
		}

		// Validate state to prevent CSRF attacks.
		state := query.Get("state")
		if state != expectedState {
			http.Error(w, "invalid state parameter", http.StatusBadRequest)
			resultCh <- callbackResult{err: fmt.Errorf("state mismatch: expected %q, got %q", expectedState, state)}
			return
		}

		code := query.Get("code")
		if code == "" {
			http.Error(w, "missing authorization code", http.StatusBadRequest)
			resultCh <- callbackResult{err: fmt.Errorf("missing authorization code in callback")}
			return
		}

		// Respond to the browser with a success page.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<html><body><h2>Login successful!</h2>"+
			"<p>You can close this browser tab and return to the terminal.</p></body></html>")

		resultCh <- callbackResult{code: code}
	})

	srv := &http.Server{
		Addr:              net.JoinHostPort("127.0.0.1", callbackPort),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("callback server error", "error", err)
			resultCh <- callbackResult{err: fmt.Errorf("callback server failed: %w", err)}
		}
	}()

	return srv
}

// oidcTokenResponse represents the relevant fields from an OIDC token response.
type oidcTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	Scope        string `json:"scope"`
}

// exchangeCode exchanges an authorization code for tokens at the OIDC token
// endpoint. It sends the PKCE code_verifier to prove possession of the
// original challenge.
func exchangeCode(
	ctx context.Context,
	issuerURL, clientID, code, redirectURI, verifier string,
) (*oidcTokenResponse, error) {
	tokenURL := issuerURL + "/protocol/openid-connect/token"

	data := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"client_id":     {clientID},
		"code_verifier": {verifier},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{
		Timeout: 30 * time.Second,
		// Prevent open redirect attacks by disabling automatic redirects.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseSize))
	if err != nil {
		return nil, fmt.Errorf("reading token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp oidcTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("parsing token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}

	return &tokenResp, nil
}

// openBrowser attempts to open the given URL in the user's default browser.
// It is best-effort; if it fails, the user can manually copy the URL.
// The URL is validated before being passed to the subprocess to prevent
// command injection (gosec G204).
func openBrowser(ctx context.Context, targetURL string) error {
	// Validate that the target is a well-formed HTTP(S) URL to prevent
	// arbitrary command injection via the subprocess.
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("refusing to open non-HTTP URL scheme %q", parsed.Scheme)
	}

	// Reconstruct the URL from the parsed form to ensure it is canonical.
	safeURL := parsed.String()

	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		//nolint:gosec // URL validated above
		cmd = exec.CommandContext(ctx, "open", safeURL)
	case "linux":
		//nolint:gosec // URL validated above
		cmd = exec.CommandContext(ctx, "xdg-open", safeURL)
	case "windows":
		//nolint:gosec // URL validated above
		cmd = exec.CommandContext(
			ctx, "rundll32", "url.dll,FileProtocolHandler", safeURL,
		)
	default:
		return fmt.Errorf(
			"unsupported platform %q for opening browser", runtime.GOOS,
		)
	}

	return cmd.Start()
}

// runOIDCBrowserFlow performs the full OIDC Authorization Code flow with PKCE:
//  1. Generates PKCE verifier and challenge
//  2. Starts a local callback server on localhost:8085
//  3. Opens the browser to the authorization endpoint
//  4. Waits for the callback with the authorization code
//  5. Exchanges the code for tokens
//  6. Displays the result
func runOIDCBrowserFlow(issuerURL, clientID string) error {
	redirectURI := "http://localhost:" + callbackPort + callbackPath

	// Generate PKCE parameters.
	verifier, challenge, err := generatePKCE()
	if err != nil {
		return err
	}

	// Generate random state for CSRF protection.
	state, err := generateState()
	if err != nil {
		return err
	}

	// Build the authorization URL.
	authURL := buildAuthorizationURL(issuerURL, clientID, redirectURI, challenge, state)

	// Start the local callback server.
	resultCh := make(chan callbackResult, 1)
	srv := startCallbackServer(resultCh, state)

	// Ensure the callback server is shut down when we're done.
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), callbackShutdownTimeout)
		defer shutdownCancel()
		if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Warn("callback server shutdown error", "error", shutdownErr)
		}
	}()

	// Wait for the callback, timeout, or context cancellation.
	ctx, cancel := cmdContext()
	defer cancel()

	// Use the shorter of cmdContext timeout and oidcLoginTimeout.
	loginCtx, loginCancel := context.WithTimeout(ctx, oidcLoginTimeout)
	defer loginCancel()

	// Attempt to open the browser; fall back to printing the URL.
	slog.Info("opening browser for OIDC login", "url", authURL)
	fmt.Fprintf(os.Stdout,
		"Open this URL in your browser to log in:\n%s\n\nWaiting for callback...\n",
		authURL,
	)

	if browserErr := openBrowser(loginCtx, authURL); browserErr != nil {
		slog.Warn("could not open browser automatically", "error", browserErr)
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			return fmt.Errorf("OIDC callback error: %w", result.err)
		}

		// Exchange the authorization code for tokens.
		tokenResp, exchangeErr := exchangeCode(loginCtx, issuerURL, clientID, result.code, redirectURI, verifier)
		if exchangeErr != nil {
			return fmt.Errorf("token exchange failed: %w", exchangeErr)
		}

		// Display token info.
		tokenPreviewLen := 50
		if len(tokenResp.AccessToken) < tokenPreviewLen {
			tokenPreviewLen = len(tokenResp.AccessToken)
		}

		f := newFormatter()
		f.FormatMessage("Login successful (method=oidc)")
		f.FormatMessage(fmt.Sprintf("Access token: %s...", tokenResp.AccessToken[:tokenPreviewLen]))
		if tokenResp.ExpiresIn > 0 {
			f.FormatMessage(fmt.Sprintf("Token expires in: %d seconds", tokenResp.ExpiresIn))
		}

		return nil

	case <-loginCtx.Done():
		return fmt.Errorf("login timed out waiting for browser callback")
	}
}
