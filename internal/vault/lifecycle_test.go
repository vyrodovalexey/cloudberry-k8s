package vault

// Tests for A-6: Vault token lifecycle management — background renewal via
// vaultapi.NewLifetimeWatcher and reactive re-authentication on 401/403.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// fastRetryOpts keeps the reactive-reauth tests quick.
func fastRetryOpts() util.RetryOptions {
	return util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
}

// fakeVaultServer is a minimal Vault HTTP fake supporting approle login,
// token renew-self and a single KV read path with per-token behavior.
type fakeVaultServer struct {
	t *testing.T

	mu          sync.Mutex
	loginCount  atomic.Int64
	renewCount  atomic.Int64
	readCount   atomic.Int64
	renewable   bool
	leaseSecs   int
	loginErrSeq []int // HTTP status per login call (0 → 200), beyond → 200
	// readForbiddenTokens: tokens that receive 403 on read.
	readForbiddenTokens map[string]bool
	// renewForbiddenTokens: tokens whose renew-self receives 403 (simulating
	// an expired/revoked token so the lifetime watcher's DoneCh fires).
	renewForbiddenTokens map[string]bool

	server *httptest.Server
}

func newFakeVaultServer(t *testing.T, renewable bool, leaseSecs int) *fakeVaultServer {
	f := &fakeVaultServer{
		t:                    t,
		renewable:            renewable,
		leaseSecs:            leaseSecs,
		readForbiddenTokens:  map[string]bool{},
		renewForbiddenTokens: map[string]bool{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/approle/login", f.handleLogin)
	mux.HandleFunc("/v1/auth/token/renew-self", f.handleRenew)
	mux.HandleFunc("/v1/secret/data/test", f.handleRead)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeVaultServer) tokenForLogin(n int64) string {
	return fmt.Sprintf("token-%d", n)
}

func (f *fakeVaultServer) handleLogin(w http.ResponseWriter, _ *http.Request) {
	n := f.loginCount.Add(1)

	f.mu.Lock()
	var status int
	if len(f.loginErrSeq) > 0 {
		status = f.loginErrSeq[0]
		f.loginErrSeq = f.loginErrSeq[1:]
	}
	f.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"errors":["login refused"]}`))
		return
	}

	resp := map[string]interface{}{
		"auth": map[string]interface{}{
			"client_token":   f.tokenForLogin(n),
			"renewable":      f.renewable,
			"lease_duration": f.leaseSecs,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (f *fakeVaultServer) handleRenew(w http.ResponseWriter, r *http.Request) {
	f.renewCount.Add(1)
	token := r.Header.Get("X-Vault-Token")

	f.mu.Lock()
	forbidden := f.renewForbiddenTokens[token]
	f.mu.Unlock()
	if forbidden {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
		return
	}

	resp := map[string]interface{}{
		"auth": map[string]interface{}{
			"client_token":   f.tokenForLogin(f.loginCount.Load()),
			"renewable":      f.renewable,
			"lease_duration": f.leaseSecs,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (f *fakeVaultServer) handleRead(w http.ResponseWriter, r *http.Request) {
	f.readCount.Add(1)
	token := r.Header.Get("X-Vault-Token")

	f.mu.Lock()
	forbidden := f.readForbiddenTokens[token]
	f.mu.Unlock()

	if forbidden {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
		return
	}
	resp := map[string]interface{}{
		"data": map[string]interface{}{
			"data": map[string]interface{}{"key": "value"},
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func newLifecycleTestClient(t *testing.T, f *fakeVaultServer) Client {
	cfg := Config{
		Enabled:    true,
		Address:    f.server.URL,
		AuthMethod: authMethodAppRole,
		Role:       "role-id",
		Token:      "secret-id",
		RetryOpts:  fastRetryOpts(),
	}
	// A (noop) recorder is supplied so the metric-recording branches of the
	// renewal/reauth paths are exercised.
	client, err := NewClient(context.Background(), cfg, nil, &metrics.NoopRecorder{})
	require.NoError(t, err)
	t.Cleanup(func() {
		if closer, ok := client.(Closer); ok {
			closer.Close()
		}
	})
	return client
}

// ----------------------------------------------------------------------------
// Background renewal (LifetimeWatcher)
// ----------------------------------------------------------------------------

func TestTokenLifecycle_RenewCalledBeforeExpiry(t *testing.T) {
	// A renewable token with a short lease: the lifetime watcher must hit the
	// renew-self endpoint before the TTL elapses.
	f := newFakeVaultServer(t, true, 2 /* seconds */)
	_ = newLifecycleTestClient(t, f)

	require.Eventually(t, func() bool {
		return f.renewCount.Load() >= 1
	}, 5*time.Second, 25*time.Millisecond,
		"lifetime watcher must renew the token before TTL expiry")
}

func TestTokenLifecycle_CloseTerminatesWatcher(t *testing.T) {
	// Long lease → watcher is sleeping; Close must return promptly anyway.
	f := newFakeVaultServer(t, true, 600)
	client := newLifecycleTestClient(t, f)

	closer, ok := client.(Closer)
	require.True(t, ok, "the live client must implement vault.Closer")

	done := make(chan struct{})
	go func() {
		closer.Close()
		closer.Close() // idempotent
		close(done)
	}()

	select {
	case <-done:
		// Watcher goroutine terminated.
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not terminate the lifetime watcher goroutine")
	}
}

func TestTokenLifecycle_ExpiredTokenReauthenticatesAndResumesRenewal(t *testing.T) {
	// The first token's renewal fails (expired/revoked) → the watcher's
	// DoneCh fires → the lifecycle re-authenticates and starts a fresh
	// watcher that successfully renews the NEW token.
	f := newFakeVaultServer(t, true, 2)
	f.mu.Lock()
	f.renewForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	_ = newLifecycleTestClient(t, f)

	// Re-login after the failed renewal...
	require.Eventually(t, func() bool {
		return f.loginCount.Load() >= 2
	}, 10*time.Second, 25*time.Millisecond,
		"token expiry must trigger re-authentication")

	// ...and the new token gets renewed by the restarted watcher.
	require.Eventually(t, func() bool {
		return f.renewCount.Load() >= 2
	}, 10*time.Second, 25*time.Millisecond,
		"the fresh token must be renewed by a new lifetime watcher")
}

func TestTokenLifecycle_ReauthAfterExpiryFails_WatcherExits(t *testing.T) {
	// Renewal of the first token fails AND every re-login fails: the
	// lifecycle goroutine must give up cleanly (reactive re-auth on the next
	// operation still recovers later).
	f := newFakeVaultServer(t, true, 2)
	f.mu.Lock()
	f.renewForbiddenTokens[f.tokenForLogin(1)] = true
	// Initial login (during NewClient) succeeds, all re-login attempts fail.
	f.loginErrSeq = []int{0, 403, 403, 403, 403, 403, 403, 403, 403}
	f.mu.Unlock()

	_ = newLifecycleTestClient(t, f)

	// The watcher re-auth budget is exhausted (1 initial + up to 4 attempts).
	require.Eventually(t, func() bool {
		return f.loginCount.Load() >= 5
	}, 10*time.Second, 25*time.Millisecond)

	// No further renewals happen after the watcher exits.
	stable := f.renewCount.Load()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, stable, f.renewCount.Load(),
		"watcher must exit after re-auth budget exhaustion")
}

func TestTokenLifecycle_NonRenewableToken_NoWatcher(t *testing.T) {
	// Non-renewable login: the watcher exits immediately and no renew calls
	// are ever issued.
	f := newFakeVaultServer(t, false, 1)
	_ = newLifecycleTestClient(t, f)

	time.Sleep(150 * time.Millisecond)
	assert.Zero(t, f.renewCount.Load(),
		"non-renewable tokens must not be renewed")
}

// ----------------------------------------------------------------------------
// Reactive re-authentication on 401/403
// ----------------------------------------------------------------------------

func TestReadSecret_403TriggersSingleReauthAndSucceeds(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	client := newLifecycleTestClient(t, f)
	require.Equal(t, int64(1), f.loginCount.Load())

	// The initial token is now "revoked": reads with it return 403; the
	// re-login token works.
	f.mu.Lock()
	f.readForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	data, err := client.ReadSecret(context.Background(), "secret/data/test")

	require.NoError(t, err, "read must succeed after re-authentication")
	assert.Equal(t, "value", data["key"])
	assert.Equal(t, int64(2), f.loginCount.Load(),
		"exactly one re-login must be issued (initial + reauth)")
}

func TestReadSecret_ReauthFailure_SurfacesWrappedError(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	client := newLifecycleTestClient(t, f)

	// All reads 403 with every token; every re-login attempt fails too.
	f.mu.Lock()
	for i := int64(1); i <= 10; i++ {
		f.readForbiddenTokens[f.tokenForLogin(i)] = true
	}
	// 403 (not 5xx) so the vault API client's internal retryablehttp does not
	// add its own retries on top of ours, keeping the test fast.
	f.loginErrSeq = []int{403, 403, 403, 403, 403, 403, 403, 403}
	f.mu.Unlock()

	_, err := client.ReadSecret(context.Background(), "secret/data/test")

	require.Error(t, err, "exhausted backoff budget must surface an error")
	assert.Contains(t, err.Error(), "reading secret at secret/data/test")
	assert.ErrorIs(t, err, util.ErrRetryExhausted)
}

func TestReadSecret_ConcurrentReadsDuringExpiry_SingleReauth(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	client := newLifecycleTestClient(t, f)
	require.Equal(t, int64(1), f.loginCount.Load())

	// Token 1 is expired; token 2 (from the single expected re-login) works.
	f.mu.Lock()
	f.readForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	const readers = 5
	var wg sync.WaitGroup
	errs := make([]error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = client.ReadSecret(context.Background(), "secret/data/test")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "reader %d must eventually succeed", i)
	}
	assert.Equal(t, int64(2), f.loginCount.Load(),
		"concurrent expired reads must trigger exactly one re-login (no stampede)")
}

func TestWriteSecret_403TriggersReauth(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)

	// Reuse the read handler's token gating for the write path by adding a
	// write handler with identical 403 semantics.
	writeCount := atomic.Int64{}
	f.server.Config.Handler.(*http.ServeMux).HandleFunc("/v1/secret/data/writable",
		func(w http.ResponseWriter, r *http.Request) {
			writeCount.Add(1)
			token := r.Header.Get("X-Vault-Token")
			f.mu.Lock()
			forbidden := f.readForbiddenTokens[token]
			f.mu.Unlock()
			if forbidden {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})

	client := newLifecycleTestClient(t, f)
	f.mu.Lock()
	f.readForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	err := client.WriteSecret(context.Background(), "secret/data/writable",
		map[string]interface{}{"k": "v"})

	require.NoError(t, err)
	assert.Equal(t, int64(2), f.loginCount.Load())
	assert.GreaterOrEqual(t, writeCount.Load(), int64(2))
}

// ----------------------------------------------------------------------------
// NewClient construction errors
// ----------------------------------------------------------------------------

func TestNewClient_VaultAPIClientError(t *testing.T) {
	// vaultapi.DefaultConfig reads VAULT_* env vars; an invalid rate limit
	// makes vaultapi.NewClient fail, exercising the construction-error branch.
	t.Setenv("VAULT_RATE_LIMIT", "not-a-number")

	cfg := Config{
		Enabled:    true,
		Address:    "http://127.0.0.1:1",
		AuthMethod: authMethodToken,
		Token:      "tok",
	}
	_, err := NewClient(context.Background(), cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating vault client")
}

func TestTokenLifecycle_WatcherConstructionError_ExitsCleanly(t *testing.T) {
	// Inject a watcher-construction failure: the lifecycle goroutine must
	// log and exit instead of crashing or spinning.
	orig := newLifetimeWatcher
	newLifetimeWatcher = func(*vaultapi.Client, *vaultapi.Secret) (*vaultapi.LifetimeWatcher, error) {
		return nil, fmt.Errorf("injected watcher construction failure")
	}
	t.Cleanup(func() { newLifetimeWatcher = orig })

	f := newFakeVaultServer(t, true, 600)
	client := newLifecycleTestClient(t, f)

	closer, ok := client.(Closer)
	require.True(t, ok)

	done := make(chan struct{})
	go func() {
		closer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle goroutine did not exit after watcher construction failure")
	}
	assert.Zero(t, f.renewCount.Load())
}

// ----------------------------------------------------------------------------
// recordRenewalDone
// ----------------------------------------------------------------------------

func TestRecordRenewalDone(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	client := newLifecycleTestClient(t, f)
	vc, ok := client.(*vaultClient)
	require.True(t, ok)

	// nil error (expected end-of-life) and hard failure are both safe, with
	// and without a recorder.
	vc.recordRenewalDone(nil)
	vc.recordRenewalDone(fmt.Errorf("renewal exploded"))
	vc.recorder = nil
	vc.recordRenewalDone(fmt.Errorf("renewal exploded without recorder"))
}

// ----------------------------------------------------------------------------
// isVaultAuthError discrimination
// ----------------------------------------------------------------------------

func TestIsVaultAuthError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil-safe wrapped 403 text", fmt.Errorf("op failed: permission denied"), true},
		{"code 403 text", fmt.Errorf("Error making API request. Code: 403"), true},
		{"code 401 text", fmt.Errorf("Code: 401. Errors:"), true},
		{"plain network error", fmt.Errorf("connection refused"), false},
		{"500 error", fmt.Errorf("Code: 500. Errors: internal"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isVaultAuthError(tc.err))
		})
	}
}
