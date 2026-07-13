package vault

// Tests for the DEV-5 SecretWatcher staleness metrics (handoff gaps
// UT-17..UT-20): the per-path last-success stamp, the error-streak counter,
// the success-stamp-before-nil-data ordering contract, and the nil-recorder
// safety of the variadic NewSecretWatcher parameter.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// watchRecorder captures the SecretWatcher staleness-metric invocations. It
// embeds metrics.NoopRecorder so only the watch methods are overridden; all
// counters are mutex-guarded (race-safe under -race).
type watchRecorder struct {
	*metrics.NoopRecorder

	mu sync.Mutex

	successCalls int
	lastPath     string
	lastTS       float64

	errorCalls    int
	lastErrorPath string
}

func newWatchRecorder() *watchRecorder {
	return &watchRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (w *watchRecorder) SetVaultWatchLastSuccess(path string, ts float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.successCalls++
	w.lastPath = path
	w.lastTS = ts
}

func (w *watchRecorder) IncVaultWatchError(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errorCalls++
	w.lastErrorPath = path
}

func (w *watchRecorder) snapshot() (successCalls int, lastPath string, lastTS float64, errorCalls int, lastErrorPath string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.successCalls, w.lastPath, w.lastTS, w.errorCalls, w.lastErrorPath
}

// stubVaultClient is a minimal vault.Client stub whose ReadSecret returns the
// configured data/error pair. It records onChange-independent read counts.
type stubVaultClient struct {
	data map[string]interface{}
	err  error
}

func (s *stubVaultClient) ReadSecret(_ context.Context, _ string) (map[string]interface{}, error) {
	return s.data, s.err
}

func (s *stubVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

func (s *stubVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (s *stubVaultClient) IsEnabled() bool { return true }

// ---------------------------------------------------------------------------
// UT-17: successful poll stamps the per-path last-success gauge
// ---------------------------------------------------------------------------

func TestSecretWatcher_SuccessfulPoll_StampsLastSuccess(t *testing.T) {
	// Arrange: a real Vault-shaped httptest server so the recorder is wired
	// through the full NewClient -> ReadSecret -> checkForChanges path (this
	// also covers the NewSecretWatcher variadic-recorder assignment branch).
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/watched", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"data":{"key":"value"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rec := newWatchRecorder()
	client := newRecordingVaultClient(t, rec, server.URL)
	watcher := NewSecretWatcher(client, "secret/data/watched", time.Minute,
		func(map[string]interface{}) {}, slog.Default(), rec)

	// Act.
	before := time.Now().Unix()
	watcher.checkForChanges(context.Background())
	after := time.Now().Unix()

	// Assert: exactly one success stamp with the watched path and a
	// timestamp taken between the surrounding readings; no error increments.
	successCalls, lastPath, lastTS, errorCalls, _ := rec.snapshot()
	assert.Equal(t, 1, successCalls, "one successful poll must stamp exactly once")
	assert.Equal(t, "secret/data/watched", lastPath)
	assert.GreaterOrEqual(t, lastTS, float64(before))
	assert.LessOrEqual(t, lastTS, float64(after))
	assert.Zero(t, errorCalls, "a successful poll must not count as a watch error")
}

// ---------------------------------------------------------------------------
// UT-18: error streaks increment the error counter and keep the stamp stale
// ---------------------------------------------------------------------------

func TestSecretWatcher_ErrorStreak_CountsErrorsAndKeepsStampStale(t *testing.T) {
	// Arrange: the secret path 404s on every poll.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rec := newWatchRecorder()
	client := newRecordingVaultClient(t, rec, server.URL)
	watcher := NewSecretWatcher(client, "secret/data/missing", time.Minute,
		func(map[string]interface{}) {}, slog.Default(), rec)

	// Act: three consecutive failing polls.
	const polls = 3
	for range polls {
		watcher.checkForChanges(context.Background())
	}

	// Assert: one error increment per failing poll on the watched path, and
	// the last-success gauge is NEVER stamped (stays stale for alerting).
	successCalls, _, _, errorCalls, lastErrorPath := rec.snapshot()
	assert.Equal(t, polls, errorCalls, "every failing poll must increment the error counter")
	assert.Equal(t, "secret/data/missing", lastErrorPath)
	assert.Zero(t, successCalls,
		"failing polls must not stamp last-success (staleness contract, M-3/G-2)")
}

// ---------------------------------------------------------------------------
// UT-19: success stamped BEFORE the data==nil early return
// ---------------------------------------------------------------------------

func TestSecretWatcher_NilDataSuccess_StampsBeforeEarlyReturn(t *testing.T) {
	// Arrange: a client that succeeds but returns no data (empty secret).
	rec := newWatchRecorder()
	changed := false
	watcher := NewSecretWatcher(&stubVaultClient{data: nil, err: nil},
		"secret/data/empty", time.Minute,
		func(map[string]interface{}) { changed = true }, slog.Default(), rec)

	// Act.
	watcher.checkForChanges(context.Background())

	// Assert: a successful poll of an empty secret IS a successful poll —
	// stamped before the early return; onChange must not fire and the hash
	// baseline must stay untouched.
	successCalls, lastPath, _, errorCalls, _ := rec.snapshot()
	assert.Equal(t, 1, successCalls,
		"nil-data success must stamp last-success BEFORE the early return (DEV-5 ordering)")
	assert.Equal(t, "secret/data/empty", lastPath)
	assert.Zero(t, errorCalls)
	assert.False(t, changed, "onChange must not fire for nil data")
	assert.Empty(t, watcher.lastHash, "the hash baseline must not be touched on nil data")
}

// ---------------------------------------------------------------------------
// UT-20: variadic recorder edge cases (omitted / explicit nil)
// ---------------------------------------------------------------------------

func TestSecretWatcher_RecorderOmittedOrNil_IsNilSafe(t *testing.T) {
	successClient := &stubVaultClient{data: map[string]interface{}{"k": "v"}}
	errorClient := &stubVaultClient{err: fmt.Errorf("vault unreachable")}

	tests := []struct {
		name       string
		construct  func(c Client, onChange func(map[string]interface{})) *SecretWatcher
		client     Client
		wantChange bool
	}{
		{
			name: "recorder omitted, successful poll",
			construct: func(c Client, onChange func(map[string]interface{})) *SecretWatcher {
				return NewSecretWatcher(c, "secret/data/x", time.Minute, onChange, slog.Default())
			},
			client: successClient,
		},
		{
			name: "recorder omitted, failing poll",
			construct: func(c Client, onChange func(map[string]interface{})) *SecretWatcher {
				return NewSecretWatcher(c, "secret/data/x", time.Minute, onChange, slog.Default())
			},
			client: errorClient,
		},
		{
			name: "explicit nil recorder, successful poll",
			construct: func(c Client, onChange func(map[string]interface{})) *SecretWatcher {
				return NewSecretWatcher(c, "secret/data/x", time.Minute, onChange, slog.Default(), nil)
			},
			client: successClient,
		},
		{
			name: "explicit nil recorder, failing poll",
			construct: func(c Client, onChange func(map[string]interface{})) *SecretWatcher {
				return NewSecretWatcher(c, "secret/data/x", time.Minute, onChange, slog.Default(), nil)
			},
			client: errorClient,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := 0
			watcher := tt.construct(tt.client, func(map[string]interface{}) { changed++ })
			require.Nil(t, watcher.recorder, "omitted/nil recorder must stay nil")

			// Act twice: first poll records the baseline hash (no onChange),
			// second poll sees the same value (no onChange). Must not panic
			// on either the success or the error path with a nil recorder.
			watcher.checkForChanges(context.Background())
			watcher.checkForChanges(context.Background())

			assert.Zero(t, changed, "unchanged secret must not fire onChange")
		})
	}
}
