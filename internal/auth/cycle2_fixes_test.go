package auth

// Cycle-2 fix tests (T14):
//   L-3: LazyOIDC discovery runs in a singleflight with a bounded timeout —
//        one upstream call per burst, the state mutex is never held across
//        the network call, and waiters fail fast together.
//   L-4: per-request identity details are logged at Debug, never Info.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
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
)

// slowDiscoveryServer serves the OIDC discovery document after a configurable
// delay, counting upstream discovery attempts.
type slowDiscoveryServer struct {
	server   *httptest.Server
	attempts atomic.Int64
	delay    atomic.Int64 // nanoseconds
}

func newSlowDiscoveryServer(t *testing.T, delay time.Duration) *slowDiscoveryServer {
	t.Helper()
	s := &slowDiscoveryServer{}
	s.delay.Store(int64(delay))

	mux := http.NewServeMux()
	var serverURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		s.attempts.Add(1)
		select {
		case <-time.After(time.Duration(s.delay.Load())):
		case <-r.Context().Done():
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
	s.server = httptest.NewServer(mux)
	serverURL = s.server.URL
	t.Cleanup(s.server.Close)
	return s
}

func TestLazyOIDC_ConcurrentDiscovery_SingleUpstreamCallAndBoundedWait(t *testing.T) {
	srv := newSlowDiscoveryServer(t, 5*time.Second) // far beyond the timeout
	p := NewLazyOIDCProvider(OIDCConfig{IssuerURL: srv.server.URL, ClientID: "c"}, testDiscardLogger())
	p.discoveryTimeout = 200 * time.Millisecond

	const callers = 8
	start := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = p.getProvider(context.Background(), true)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i, err := range errs {
		assert.Error(t, err, "caller %d must fail fast with the shared discovery error", i)
	}
	assert.Equal(t, int64(1), srv.attempts.Load(),
		"a burst of concurrent callers must trigger exactly ONE upstream discovery")
	assert.Less(t, elapsed, 2*time.Second,
		"no caller may be blocked past the bounded discovery timeout (10s default, 200ms here)")
}

func TestLazyOIDC_MutexNotHeldDuringDiscovery(t *testing.T) {
	srv := newSlowDiscoveryServer(t, 1*time.Second)
	p := NewLazyOIDCProvider(OIDCConfig{IssuerURL: srv.server.URL, ClientID: "c"}, testDiscardLogger())
	p.discoveryTimeout = 2 * time.Second

	discoveryStarted := make(chan struct{})
	go func() {
		close(discoveryStarted)
		_, _ = p.getProvider(context.Background(), true)
	}()
	<-discoveryStarted
	// Give the goroutine time to enter the network call.
	require.Eventually(t, func() bool { return srv.attempts.Load() >= 1 },
		2*time.Second, 5*time.Millisecond)

	// State accessors must answer immediately while discovery is in flight:
	// the mutex is no longer held across the network call.
	done := make(chan bool, 1)
	go func() { done <- p.Initialized() }()
	select {
	case initialized := <-done:
		assert.False(t, initialized)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Initialized() blocked while discovery was in flight — mutex held during network call")
	}
}

func TestLazyOIDC_DiscoveryRecoversAfterTimeout(t *testing.T) {
	srv := newSlowDiscoveryServer(t, 5*time.Second)
	p := NewLazyOIDCProvider(OIDCConfig{IssuerURL: srv.server.URL, ClientID: "c"}, testDiscardLogger())
	p.discoveryTimeout = 150 * time.Millisecond
	p.cooldown = 0 // retry immediately for the test

	_, err := p.getProvider(context.Background(), false)
	require.Error(t, err, "the slow IdP must time out")

	// IdP recovers: discovery now answers instantly.
	srv.delay.Store(0)
	provider, err := p.getProvider(context.Background(), false)
	require.NoError(t, err, "discovery must succeed after the IdP recovers")
	require.NotNil(t, provider)
	assert.True(t, p.Initialized())

	// Subsequent calls take the fast path (no further upstream attempts).
	attempts := srv.attempts.Load()
	_, err = p.getProvider(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, attempts, srv.attempts.Load())
}

// ----------------------------------------------------------------------------
// L-4: identity details at Debug, never Info
// ----------------------------------------------------------------------------

// testDiscardLogger returns a logger that drops everything.
func testDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

func TestOIDCAuthenticate_IdentityNotLoggedAtInfo(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := setupOIDCTestServer(t, key)

	var infoBuf bytes.Buffer
	infoLogger := slog.New(slog.NewTextHandler(&infoBuf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	provider, err := NewOIDCProvider(context.Background(), OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}, infoLogger)
	require.NoError(t, err)

	token := signJWT(t, key, map[string]interface{}{
		"iss":   server.URL,
		"sub":   "pii-user-123",
		"aud":   "test-client",
		"email": "pii@example.com",
		"exp":   float64(time.Now().Add(time.Hour).Unix()),
		"iat":   float64(time.Now().Unix()),
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	identity, err := provider.Authenticate(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "pii-user-123", identity.Username)

	logged := infoBuf.String()
	assert.NotContains(t, logged, "pii-user-123",
		"the username must not appear in Info-level logs")
	assert.NotContains(t, logged, "pii@example.com",
		"the email must not appear in Info-level logs")
}

func TestOIDCAuthenticate_IdentityLoggedAtDebug(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := setupOIDCTestServer(t, key)

	var debugBuf bytes.Buffer
	debugLogger := slog.New(slog.NewTextHandler(&debugBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	provider, err := NewOIDCProvider(context.Background(), OIDCConfig{
		IssuerURL: server.URL,
		ClientID:  "test-client",
	}, debugLogger)
	require.NoError(t, err)

	token := signJWT(t, key, map[string]interface{}{
		"iss": server.URL,
		"sub": "debug-user",
		"aud": "test-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	_, err = provider.Authenticate(context.Background(), req)
	require.NoError(t, err)

	assert.Contains(t, debugBuf.String(), "debug-user",
		"identity details remain available for troubleshooting at Debug level")
}
