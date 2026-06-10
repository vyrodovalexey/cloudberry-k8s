package idle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// idleObsRecorder captures the idle daemon health metric calls (C-3).
type idleObsRecorder struct {
	metrics.NoopRecorder
	mu           sync.Mutex
	upStates     []bool
	scanFailures int
	reconnects   []string
	terminations int
}

func (r *idleObsRecorder) SetIdleDaemonUp(_, _ string, up bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upStates = append(r.upStates, up)
}

func (r *idleObsRecorder) RecordIdleScanFailure(_, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scanFailures++
}

func (r *idleObsRecorder) RecordIdleReconnectAttempt(_, _, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconnects = append(r.reconnects, result)
}

func (r *idleObsRecorder) RecordIdleSessionTermination(_, _, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminations++
}

func (r *idleObsRecorder) snapshot() ([]bool, int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.upStates...), r.scanFailures,
		append([]string(nil), r.reconnects...)
}

// failingFactory fails N times before succeeding.
type failingFactory struct {
	mu        sync.Mutex
	failures  int
	succeeded bool
	client    db.Client
}

func (f *failingFactory) NewClient(_ context.Context) (db.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return nil, errors.New("connection refused")
	}
	f.succeeded = true
	return f.client, nil
}

// TestIdleDaemonUpGauge verifies up=1 on start and up=0 after Stop (C-3).
func TestIdleDaemonUpGauge(t *testing.T) {
	rec := &idleObsRecorder{}
	d := New(Config{
		ClusterName:  "c1",
		Namespace:    "ns1",
		ScanInterval: 10 * time.Millisecond,
		DBClient:     &mockDBClient{},
		Metrics:      rec,
	})
	d.Start(context.Background())
	// Allow the loop to start.
	require.Eventually(t, func() bool {
		states, _, _ := rec.snapshot()
		return len(states) >= 1 && states[0]
	}, time.Second, 5*time.Millisecond)

	d.Stop()
	states, _, _ := rec.snapshot()
	require.NotEmpty(t, states)
	assert.True(t, states[0], "up=1 after start")
	assert.False(t, states[len(states)-1], "up=0 after Stop")
}

// TestIdleScanFailureMetricAndReconnect verifies scan failures increment the
// failure counter and three consecutive failures trigger a reconnect whose
// attempts are recorded per result (C-3 + L-10).
func TestIdleScanFailureMetricAndReconnect(t *testing.T) {
	rec := &idleObsRecorder{}
	failing := &mockDBClient{listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
		return nil, errors.New("conn lost")
	}}
	factory := &failingFactory{failures: 1, client: &mockDBClient{}}
	d := New(Config{
		ClusterName:     "c1",
		Namespace:       "ns1",
		DBClient:        failing,
		DBClientFactory: factory,
		Metrics:         rec,
	})
	d.UpdateRules([]IdleRule{{Name: "r", Enabled: true, IdleTimeout: time.Minute}})

	// Three failing cycles trigger the reconnect (1 failed + 1 successful attempt).
	for i := 0; i < 3; i++ {
		d.runScanCycle(context.Background())
	}

	_, failures, reconnects := rec.snapshot()
	assert.Equal(t, 3, failures)
	assert.Equal(t, []string{"error", "success"}, reconnects)
	assert.True(t, factory.succeeded)
}

// TestIdleScanSpans verifies D-3: one idle.scan span per cycle with the
// session counts and one idle.terminate child per termination.
func TestIdleScanSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	rec := &idleObsRecorder{}
	client := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{Session: db.Session{PID: 1, State: "idle", QueryStart: time.Now().Add(-time.Hour)}},
				{Session: db.Session{PID: 2, State: "idle", QueryStart: time.Now().Add(-time.Hour)}},
				{Session: db.Session{PID: 3, State: "active", QueryStart: time.Now()}},
			}, nil
		},
		terminateSessionFn: func(_ context.Context, _ int32) (bool, error) { return true, nil },
	}
	d := New(Config{
		ClusterName: "c1",
		Namespace:   "ns1",
		DBClient:    client,
		Metrics:     rec,
	})
	d.UpdateRules([]IdleRule{{Name: "kill-idle", Enabled: true, IdleTimeout: time.Minute}})

	d.runScanCycle(context.Background())

	spans := sr.Ended()
	var scanSpan sdktrace.ReadOnlySpan
	var terminates []sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "idle.scan":
			scanSpan = s
		case "idle.terminate":
			terminates = append(terminates, s)
		}
	}
	require.NotNil(t, scanSpan, "idle.scan span missing")
	require.Len(t, terminates, 2, "one idle.terminate child per termination")

	attrs := map[string]int64{}
	for _, a := range scanSpan.Attributes() {
		if a.Value.Type().String() == "INT64" {
			attrs[string(a.Key)] = a.Value.AsInt64()
		}
	}
	assert.Equal(t, int64(3), attrs["idle.sessions_evaluated"])
	assert.Equal(t, int64(2), attrs["idle.sessions_terminated"])

	// Terminate spans are children of the scan span and carry the rule.
	for _, ts := range terminates {
		assert.Equal(t, scanSpan.SpanContext().SpanID(), ts.Parent().SpanID())
	}

	// E-5 PII gate: idle-daemon spans must not carry session/query content.
	telemetry.AssertNoPII(t, spans)
}

// TestIdleScanSpanErrorStatus verifies a failed scan marks the span as error.
func TestIdleScanSpanErrorStatus(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	d := New(Config{
		ClusterName: "c1",
		Namespace:   "ns1",
		DBClient: &mockDBClient{listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return nil, errors.New("boom")
		}},
		Metrics: &idleObsRecorder{},
	})
	d.UpdateRules([]IdleRule{{Name: "r", Enabled: true, IdleTimeout: time.Minute}})

	d.runScanCycle(context.Background())

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "idle.scan" {
			found = true
			assert.Equal(t, "Error", s.Status().Code.String())
		}
	}
	assert.True(t, found)
}
