package auth

// Tests for B-7: OIDC discovery retry with backoff at startup and lazy
// re-initialization (with cooldown) on the first Bearer request.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// flakyDiscoveryServer serves the OIDC discovery document, failing the first
// failCount requests with HTTP 500 and counting every attempt.
type flakyDiscoveryServer struct {
	server    *httptest.Server
	attempts  atomic.Int64
	failUntil atomic.Int64 // attempts <= failUntil receive 500
}

func newFlakyDiscoveryServer(t *testing.T, failCount int64) *flakyDiscoveryServer {
	f := &flakyDiscoveryServer{}
	f.failUntil.Store(failCount)

	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		n := f.attempts.Add(1)
		if n <= f.failUntil.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{
			"issuer": "%s",
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys"
		}`, serverURL, serverURL, serverURL, serverURL)
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	f.server = httptest.NewServer(mux)
	serverURL = f.server.URL
	t.Cleanup(f.server.Close)
	return f
}

func lazyTestRetryOpts() util.RetryOptions {
	return util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
}

func TestLazyOIDCProvider_StartupRetry_TransientFailureThenSuccess(t *testing.T) {
	f := newFlakyDiscoveryServer(t, 2) // first 2 discovery attempts fail

	lazy := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: f.server.URL,
		ClientID:  "test-client",
	}, slog.Default())

	err := util.RetryWithBackoff(context.Background(), lazyTestRetryOpts(),
		func(ctx context.Context) error { return lazy.Init(ctx) })

	require.NoError(t, err, "startup retry must ride out transient discovery failures")
	assert.True(t, lazy.Initialized())
	assert.GreaterOrEqual(t, f.attempts.Load(), int64(3),
		"at least 3 discovery attempts (2 failures + 1 success) expected")
}

func TestLazyOIDCProvider_LazyReinitAfterStartupBudgetExhausted(t *testing.T) {
	// Discovery fails far beyond the startup budget...
	f := newFlakyDiscoveryServer(t, 100)

	lazy := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: f.server.URL,
		ClientID:  "test-client",
	}, slog.Default())
	lazy.cooldown = 10 * time.Millisecond // keep the test fast

	startupErr := util.RetryWithBackoff(context.Background(), lazyTestRetryOpts(),
		func(ctx context.Context) error { return lazy.Init(ctx) })
	require.Error(t, startupErr, "startup budget must be exhausted")
	require.False(t, lazy.Initialized())

	// During the outage, Bearer requests are rejected with an auth error
	// (mapped to 401 by the middleware); basic auth is a separate provider
	// and remains unaffected.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	_, authErr := lazy.Authenticate(context.Background(), req)
	require.Error(t, authErr)
	var authError *util.AuthenticationError
	assert.ErrorAs(t, authErr, &authError, "outage must surface as a 401 authentication error")

	// The endpoint recovers...
	f.failUntil.Store(0)
	time.Sleep(15 * time.Millisecond) // let the cooldown elapse

	// ...and the next Bearer request re-initializes the provider WITHOUT a
	// pod restart. (Token verification itself then fails — empty JWKS — but
	// the provider is initialized, which is what this test asserts.)
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer some-token")
	_, _ = lazy.Authenticate(context.Background(), req2)

	assert.True(t, lazy.Initialized(),
		"provider must initialize lazily once the IdP recovers")
}

func TestLazyOIDCProvider_CooldownPreventsHammering(t *testing.T) {
	f := newFlakyDiscoveryServer(t, 1000) // always failing

	lazy := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: f.server.URL,
		ClientID:  "test-client",
	}, slog.Default())
	lazy.cooldown = time.Hour // effectively forever for this test

	// First Bearer request performs one discovery attempt...
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer tok")
	_, err1 := lazy.Authenticate(context.Background(), req)
	require.Error(t, err1)
	after1 := f.attempts.Load()

	// ...subsequent requests during the cooldown must NOT hit the IdP again.
	for i := 0; i < 5; i++ {
		_, err := lazy.Authenticate(context.Background(), req)
		require.Error(t, err)
	}
	assert.Equal(t, after1, f.attempts.Load(),
		"cooldown must prevent additional discovery attempts")
}

func TestLazyOIDCProvider_ConcurrentFirstRequests_SingleDiscovery(t *testing.T) {
	f := newFlakyDiscoveryServer(t, 0) // healthy IdP

	lazy := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: f.server.URL,
		ClientID:  "test-client",
	}, slog.Default())

	const requests = 8
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer tok")
			// Token verification fails (empty JWKS) — only discovery counts.
			_, _ = lazy.Authenticate(context.Background(), req)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), f.attempts.Load(),
		"concurrent first Bearer requests must trigger a single in-flight discovery")
	assert.True(t, lazy.Initialized())
}

func TestLazyOIDCProvider_Type(t *testing.T) {
	lazy := NewLazyOIDCProvider(OIDCConfig{IssuerURL: "https://x", ClientID: "c"}, nil)
	assert.Equal(t, "oidc", lazy.Type())
}
