package vault

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// findSpan returns the first ended span with the given name, or nil.
func findSpan(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// TestSecretWatcher_CheckForChanges_Span asserts the C-01 contract: the
// vault.watch.check span is created on every checkForChanges invocation, carries
// codes.Error when ReadSecret fails, and — crucially — does NOT double-count the
// vault read/error metric (the metric is emitted once inside ReadSecret; the
// watcher must add zero extra ops). T-C.
func TestSecretWatcher_CheckForChanges_Span(t *testing.T) {
	t.Run("success path: span present, no error status", func(t *testing.T) {
		sr, restore := telemetry.InstallSpanRecorder()
		defer restore()

		mux := http.NewServeMux()
		mux.HandleFunc("/v1/secret/data/ok", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"data":{"key":"value"}}}`))
		})
		server := httptest.NewServer(mux)
		defer server.Close()

		rec := newCapturingRecorder()
		client := newRecordingVaultClient(t, rec, server.URL)

		watcher := NewSecretWatcher(client, "secret/data/ok", time.Minute,
			func(map[string]interface{}) {}, slog.Default())
		watcher.checkForChanges(context.Background())

		span := findSpan(sr.Ended(), "vault.watch.check")
		require.NotNil(t, span, "vault.watch.check span must be created")
		assert.NotEqual(t, codes.Error, span.Status().Code,
			"successful check must not mark the span errored")
	})

	t.Run("read error: span carries error status and read/error metric fires exactly once", func(t *testing.T) {
		sr, restore := telemetry.InstallSpanRecorder()
		defer restore()

		mux := http.NewServeMux()
		mux.HandleFunc("/v1/secret/data/missing", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
		server := httptest.NewServer(mux)
		defer server.Close()

		rec := newCapturingRecorder()
		client := newRecordingVaultClient(t, rec, server.URL)

		// Baseline: NewClient's token auth already recorded one op. Capture it so
		// we can prove checkForChanges adds exactly one (the read/error from
		// ReadSecret) and no second, double-counted op.
		rec.mu.Lock()
		opsBefore := rec.ops
		rec.mu.Unlock()

		watcher := NewSecretWatcher(client, "secret/data/missing", time.Minute,
			func(map[string]interface{}) {}, slog.Default())
		watcher.checkForChanges(context.Background())

		span := findSpan(sr.Ended(), "vault.watch.check")
		require.NotNil(t, span, "vault.watch.check span must be created on read error")
		assert.Equal(t, codes.Error, span.Status().Code,
			"read failure must mark the span errored")

		// Double-count guard: exactly ONE additional vault op (the read/error
		// emitted inside ReadSecret); checkForChanges must NOT record another.
		rec.mu.Lock()
		opsAfter := rec.ops
		lastOp := rec.lastOp
		lastResult := rec.lastResult
		rec.mu.Unlock()
		assert.Equal(t, 1, opsAfter-opsBefore,
			"failed check must record the vault read/error metric exactly once (no double-count)")
		assert.Equal(t, vaultOpRead, lastOp)
		assert.Equal(t, metricResultError, lastResult)
	})
}

// capturingRecorder captures Vault operation metric invocations. It embeds
// metrics.NoopRecorder so only the relevant methods are overridden.
type capturingRecorder struct {
	*metrics.NoopRecorder

	mu sync.Mutex

	ops          int
	lastOp       string
	lastResult   string
	durations    int
	lastDuration time.Duration
}

func newCapturingRecorder() *capturingRecorder {
	return &capturingRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (c *capturingRecorder) RecordVaultOperation(operation, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ops++
	c.lastOp = operation
	c.lastResult = result
}

func (c *capturingRecorder) ObserveVaultOperationDuration(_ string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.durations++
	c.lastDuration = d
}

// ============================================================================
// SetRecorder
// ============================================================================

func TestVaultClient_SetRecorder(t *testing.T) {
	vc := &vaultClient{}
	require.Nil(t, vc.recorder)

	rec := newCapturingRecorder()
	vc.SetRecorder(rec)
	assert.NotNil(t, vc.recorder)

	// Setting nil is also valid.
	vc.SetRecorder(nil)
	assert.Nil(t, vc.recorder)
}

// ============================================================================
// recordVaultOp
// ============================================================================

func TestRecordVaultOp_NilRecorder(t *testing.T) {
	vc := &vaultClient{}
	// No recorder: must be a no-op and not panic.
	vc.recordVaultOp(vaultOpRead, time.Now(), nil)
}

func TestRecordVaultOp_Success(t *testing.T) {
	rec := newCapturingRecorder()
	vc := &vaultClient{recorder: rec}

	vc.recordVaultOp(vaultOpWrite, time.Now().Add(-time.Millisecond), nil)

	assert.Equal(t, 1, rec.ops)
	assert.Equal(t, vaultOpWrite, rec.lastOp)
	assert.Equal(t, metricResultSuccess, rec.lastResult)
	assert.Equal(t, 1, rec.durations)
	assert.Positive(t, rec.lastDuration)
}

func TestRecordVaultOp_Error(t *testing.T) {
	rec := newCapturingRecorder()
	vc := &vaultClient{recorder: rec}

	vc.recordVaultOp(vaultOpRead, time.Now(), assertErr)

	assert.Equal(t, 1, rec.ops)
	assert.Equal(t, vaultOpRead, rec.lastOp)
	assert.Equal(t, metricResultError, rec.lastResult)
	assert.Equal(t, 1, rec.durations)
}

var assertErr = &opError{}

type opError struct{}

func (*opError) Error() string { return "vault op failed" }

// ============================================================================
// End-to-end metric recording via NewClient + ReadSecret/WriteSecret.
// ============================================================================

func newRecordingVaultClient(t *testing.T, rec metrics.Recorder, serverURL string) Client {
	t.Helper()
	cfg := Config{
		Enabled:    true,
		Address:    serverURL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}
	client, err := NewClient(context.Background(), cfg, nil, rec)
	require.NoError(t, err)
	return client
}

func TestVaultClient_RecordsMetrics_OnRead(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/ok", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"data":{"key":"value"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rec := newCapturingRecorder()
	client := newRecordingVaultClient(t, rec, server.URL)

	_, err := client.ReadSecret(context.Background(), "secret/data/ok")
	require.NoError(t, err)

	// Auth on NewClient records one op; the read records another.
	assert.GreaterOrEqual(t, rec.ops, 2)
	assert.Equal(t, vaultOpRead, rec.lastOp)
	assert.Equal(t, metricResultSuccess, rec.lastResult)
}

func TestVaultClient_RecordsMetrics_OnReadError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rec := newCapturingRecorder()
	client := newRecordingVaultClient(t, rec, server.URL)

	_, err := client.ReadSecret(context.Background(), "secret/data/missing")
	require.Error(t, err)

	assert.Equal(t, vaultOpRead, rec.lastOp)
	assert.Equal(t, metricResultError, rec.lastResult)
}

func TestVaultClient_RecordsMetrics_OnWrite(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/w", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":1}}`))
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rec := newCapturingRecorder()
	client := newRecordingVaultClient(t, rec, server.URL)

	err := client.WriteSecret(context.Background(), "secret/data/w", map[string]interface{}{"k": "v"})
	require.NoError(t, err)

	assert.Equal(t, vaultOpWrite, rec.lastOp)
	assert.Equal(t, metricResultSuccess, rec.lastResult)
}

func TestVaultClient_RecordsMetrics_OnAuth(t *testing.T) {
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	rec := newCapturingRecorder()
	_ = newRecordingVaultClient(t, rec, server.URL)

	// Token auth succeeds during NewClient and records an auth op.
	assert.GreaterOrEqual(t, rec.ops, 1)
	assert.Equal(t, vaultOpAuth, rec.lastOp)
	assert.Equal(t, metricResultSuccess, rec.lastResult)
}
