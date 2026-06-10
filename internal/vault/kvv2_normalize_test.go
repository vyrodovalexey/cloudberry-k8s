package vault

// Tests for H-1a (KV-v2 request-path normalization in ReadSecret) and L-1
// (generation-gated lifecycle re-auth after a reactive re-login).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func TestKVV2FallbackPath(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		want   string
		wantOK bool
	}{
		{"logical kv2 path", "secret/test", "secret/data/test", true},
		{"explicit data path untouched", "secret/data/test", "", false},
		{"multi-segment secret path", "secret/a/b/c", "secret/data/a/b/c", true},
		{"single segment", "secret", "", false},
		{"empty", "", "", false},
		{"bare data suffix", "secret/data", "", false},
		{"leading slash trimmed", "/secret/test", "secret/data/test", true},
		{"trailing slash trimmed", "secret/test/", "secret/data/test", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := kvV2FallbackPath(tc.path)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// kvNormalizeServer is a fake Vault server that serves the KV-v2 data
// endpoint only, answers Vault-style empty 404s on the logical path, and
// records every request path so tests can assert the exact retry behavior.
type kvNormalizeServer struct {
	server *httptest.Server

	mu    sync.Mutex
	paths []string
}

func newKVNormalizeServer(t *testing.T) *kvNormalizeServer {
	t.Helper()
	s := &kvNormalizeServer{}

	kvv2OK := func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{"username": "admin", "password": "pw"},
			},
		})
	}
	kv1OK := func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"username": "legacy"},
		})
	}
	notFound := func(w http.ResponseWriter, _ *http.Request) {
		// Vault-style 404: empty body → the API client reports (nil, nil).
		w.WriteHeader(http.StatusNotFound)
	}
	forbidden := func(w http.ResponseWriter, _ *http.Request) {
		// Vault-style 403, as produced by a least-privilege policy that does
		// not cover the requested request path.
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	}

	mux := http.NewServeMux()
	// KV-v2 mount "secret": only the data endpoint serves content.
	mux.HandleFunc("/v1/secret/data/cloudberry/backup-s3", kvv2OK)
	mux.HandleFunc("/v1/secret/data/multi/seg/ment", kvv2OK)
	mux.HandleFunc("/v1/kv1/legacy", kv1OK)
	// Least-privilege policy simulation ("secret/data/lp/*" only): the
	// verbatim logical path is DENIED while the data path serves content.
	mux.HandleFunc("/v1/secret/lp/backup-s3", forbidden)
	mux.HandleFunc("/v1/secret/data/lp/backup-s3", kvv2OK)
	// Both the verbatim and the normalized path are denied (genuine
	// permission problem, not a path-shape artifact).
	mux.HandleFunc("/v1/secret/lp/denied-both", forbidden)
	mux.HandleFunc("/v1/secret/data/lp/denied-both", forbidden)
	// Verbatim denied + fallback readable-but-missing: the policy covers the
	// data path, the secret simply does not exist there.
	mux.HandleFunc("/v1/secret/lp/missing", forbidden)
	mux.HandleFunc("/v1/secret/data/lp/missing", notFound)
	// Everything else (logical KV-v2 paths, genuinely missing secrets,
	// would-be double-injected data/data/ paths) is a Vault-style 404.
	mux.HandleFunc("/v1/", notFound)

	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(s.server.Close)
	return s
}

// countPath returns how many requests hit the given request path.
func (s *kvNormalizeServer) countPath(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, p := range s.paths {
		if p == path {
			n++
		}
	}
	return n
}

func newKVNormalizeClient(t *testing.T, s *kvNormalizeServer) Client {
	t.Helper()
	client, err := NewClient(context.Background(), Config{
		Enabled:    true,
		Address:    s.server.URL,
		AuthMethod: authMethodToken,
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closer, ok := client.(Closer); ok {
			closer.Close()
		}
	})
	return client
}

func TestReadSecret_KVv2LogicalPath_NormalizedRetrySucceeds(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	data, err := client.ReadSecret(context.Background(), "secret/cloudberry/backup-s3")

	require.NoError(t, err)
	assert.Equal(t, "admin", data["username"])
	assert.Equal(t, 1, s.countPath("/v1/secret/cloudberry/backup-s3"),
		"the verbatim logical path is tried first")
	assert.Equal(t, 1, s.countPath("/v1/secret/data/cloudberry/backup-s3"),
		"exactly one normalized KV-v2 retry")
}

func TestReadSecret_ExplicitDataPath_NoDoubleInjection(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	data, err := client.ReadSecret(context.Background(), "secret/data/cloudberry/backup-s3")

	require.NoError(t, err)
	assert.Equal(t, "admin", data["username"])
	assert.Zero(t, s.countPath("/v1/secret/data/data/cloudberry/backup-s3"),
		"a path already containing data/ must never be double-injected")
}

func TestReadSecret_KV1Path_Verbatim(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	data, err := client.ReadSecret(context.Background(), "kv1/legacy")

	require.NoError(t, err)
	assert.Equal(t, "legacy", data["username"])
	assert.Zero(t, s.countPath("/v1/kv1/data/legacy"),
		"a KV-v1 path that resolves verbatim must not trigger the fallback")
}

func TestReadSecret_MultiSegmentSecretPath_Normalized(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	data, err := client.ReadSecret(context.Background(), "secret/multi/seg/ment")

	require.NoError(t, err)
	assert.Equal(t, "admin", data["username"])
	assert.Equal(t, 1, s.countPath("/v1/secret/data/multi/seg/ment"),
		"data/ is injected after the mount segment, preserving the full secret path")
}

func TestReadSecret_GenuinelyMissing_NotFoundWithSingleFallbackPerAttempt(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	_, err := client.ReadSecret(context.Background(), "secret/missing")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret not found at path secret/missing",
		"the error names the ORIGINAL path the caller configured")
	// MaxRetries=1 → 2 backoff attempts; each issues exactly one verbatim
	// read and exactly one normalized fallback read.
	assert.Equal(t, 2, s.countPath("/v1/secret/missing"))
	assert.Equal(t, 2, s.countPath("/v1/secret/data/missing"),
		"exactly one KV-v2 fallback retry per backoff attempt")
}

// ----------------------------------------------------------------------------
// H-1b: KV-v2 normalization on 403 (least-privilege "<mount>/data/*" policies)
// ----------------------------------------------------------------------------

// TestReadSecret_403OnVerbatim_FallbackSucceeds pins the H-1b fix: a 403 on
// the verbatim logical path under a least-privilege policy must trigger the
// normalized KV-v2 retry instead of short-circuiting into a failure, with
// exactly one fallback attempt.
func TestReadSecret_403OnVerbatim_FallbackSucceeds(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	data, err := client.ReadSecret(context.Background(), "secret/lp/backup-s3")

	require.NoError(t, err,
		"a path-shape 403 must fall back to the KV-v2 data path and succeed")
	assert.Equal(t, "admin", data["username"])
	assert.Equal(t, 1, s.countPath("/v1/secret/lp/backup-s3"),
		"the verbatim logical path is tried first")
	assert.Equal(t, 1, s.countPath("/v1/secret/data/lp/backup-s3"),
		"exactly one normalized KV-v2 retry")
}

// TestReadSecret_403OnBothPaths_SurfacesPermissionError verifies that when
// BOTH the verbatim and the normalized path are denied, the permission error
// is surfaced (naming both request paths) with exactly one fallback attempt
// per backoff attempt.
func TestReadSecret_403OnBothPaths_SurfacesPermissionError(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	_, err := client.ReadSecret(context.Background(), "secret/lp/denied-both")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
	assert.Contains(t, err.Error(), "reading secret at secret/lp/denied-both",
		"the error names the ORIGINAL path the caller configured")
	// MaxRetries=1 → 2 backoff attempts; each issues exactly one verbatim
	// read and exactly one normalized fallback read.
	assert.Equal(t, 2, s.countPath("/v1/secret/lp/denied-both"))
	assert.Equal(t, 2, s.countPath("/v1/secret/data/lp/denied-both"),
		"exactly one KV-v2 fallback attempt per backoff attempt")
}

// TestReadSecret_403OnVerbatim_FallbackNotFound verifies that a verbatim 403
// combined with a readable-but-missing data path reports not-found (the token
// works; the secret does not exist) instead of a permission error.
func TestReadSecret_403OnVerbatim_FallbackNotFound(t *testing.T) {
	s := newKVNormalizeServer(t)
	client := newKVNormalizeClient(t, s)

	_, err := client.ReadSecret(context.Background(), "secret/lp/missing")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret not found at path secret/lp/missing")
}

// TestReadSecret_403PathShape_NoReauthStorm verifies (with the login-counting
// fake Vault) that a path-shape 403 resolved by the KV-v2 fallback issues NO
// re-authentication: the 403 was not a token problem.
func TestReadSecret_403PathShape_NoReauthStorm(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	mux, ok := f.server.Config.Handler.(*http.ServeMux)
	require.True(t, ok)
	// Least-privilege simulation: the verbatim logical path is always denied;
	// the data path serves content for every token.
	mux.HandleFunc("/v1/secret/lp", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	})
	mux.HandleFunc("/v1/secret/data/lp", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{"key": "value"},
			},
		})
	})
	client := newLifecycleTestClient(t, f)
	require.Equal(t, int64(1), f.loginCount.Load())

	data, err := client.ReadSecret(context.Background(), "secret/lp")

	require.NoError(t, err)
	assert.Equal(t, "value", data["key"])
	assert.Equal(t, int64(1), f.loginCount.Load(),
		"a path-shape 403 resolved by the fallback must NOT re-authenticate")
}

// TestReadSecret_403OnBoth_ReauthsOnceThenFallbackSucceeds verifies the
// combined semantics: an expired token 403s on BOTH paths → exactly one
// generation-gated re-login is issued, and the next backoff attempt succeeds
// via the KV-v2 fallback with the fresh token (the verbatim path stays denied
// by the least-privilege policy).
func TestReadSecret_403OnBoth_ReauthsOnceThenFallbackSucceeds(t *testing.T) {
	f := newFakeVaultServer(t, false, 3600)
	mux, ok := f.server.Config.Handler.(*http.ServeMux)
	require.True(t, ok)
	// The verbatim logical path is ALWAYS denied (policy shape).
	mux.HandleFunc("/v1/secret/lp", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	})
	// The data path is token-gated like the stock read handler: token-1 is
	// revoked, the re-login token works.
	mux.HandleFunc("/v1/secret/data/lp", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Vault-Token")
		f.mu.Lock()
		forbidden := f.readForbiddenTokens[token]
		f.mu.Unlock()
		if forbidden {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{"key": "value"},
			},
		})
	})
	client := newLifecycleTestClient(t, f)
	require.Equal(t, int64(1), f.loginCount.Load())
	f.mu.Lock()
	f.readForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	data, err := client.ReadSecret(context.Background(), "secret/lp")

	require.NoError(t, err, "read must succeed after re-authentication")
	assert.Equal(t, "value", data["key"])
	assert.Equal(t, int64(2), f.loginCount.Load(),
		"403 on BOTH paths is a genuine auth failure: exactly one re-login")
}

// TestTokenLifecycle_ReactiveReauthSkipsRedundantLifecycleLogin verifies the
// L-1 generation gate: when a reactive re-auth (403 storm on reads) already
// re-acquired a token while the lifetime watcher was still bound to the old
// one, the lifecycle loop must NOT issue another login — it restarts the
// watcher on the current token instead. Total logins: initial + ONE reactive.
func TestTokenLifecycle_ReactiveReauthSkipsRedundantLifecycleLogin(t *testing.T) {
	// Renewable 2s lease: the watcher renews well within the test window.
	f := newFakeVaultServer(t, true, 2)
	// token-1: renew-self fails (watcher will report end-of-life) AND reads
	// fail 403 (the reactive path re-authenticates first).
	f.mu.Lock()
	f.renewForbiddenTokens[f.tokenForLogin(1)] = true
	f.readForbiddenTokens[f.tokenForLogin(1)] = true
	f.mu.Unlock()

	client := newLifecycleTestClient(t, f)
	require.Equal(t, int64(1), f.loginCount.Load())

	// Storm of reads with the revoked token → exactly one reactive re-login
	// (generation-gated), after which reads succeed with token-2.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.ReadSecret(context.Background(), "secret/data/test")
		}()
	}
	wg.Wait()
	require.Equal(t, int64(2), f.loginCount.Load(),
		"the read storm must trigger exactly one reactive re-login")

	// The watcher for token-1 eventually fails its renewal and reports
	// expiry; the lifecycle loop must SKIP the redundant login (generation
	// changed) and renew token-2 with a fresh watcher instead.
	require.Eventually(t, func() bool {
		return f.renewCount.Load() >= 2
	}, 10*time.Second, 25*time.Millisecond,
		"a fresh watcher must renew the reactively acquired token")

	assert.Equal(t, int64(2), f.loginCount.Load(),
		"the lifecycle loop must not re-login after the reactive re-auth (generation gate)")
}
