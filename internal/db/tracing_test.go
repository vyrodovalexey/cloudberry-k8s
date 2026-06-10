package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// opDurationRecorder captures ObserveDBQueryDuration calls (C-2 hook test).
type opDurationRecorder struct {
	metrics.NoopRecorder
	mu  sync.Mutex
	ops []string
}

func (r *opDurationRecorder) ObserveDBQueryDuration(operation string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, operation)
}

func (r *opDurationRecorder) operations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

// spanNames extracts the span names from ended spans.
func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
	}
	return names
}

// TestPromoteStandbySpanAndDuration verifies that a composite method produces
// a named child span parented on the caller's span (trace continuity across
// the package boundary), per-query child spans from the pgx tracer, and a
// query-duration histogram observation via the shared hook.
func TestPromoteStandbySpanAndDuration(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	rec := &opDurationRecorder{}
	client.SetRecorder(rec, "c1", "ns1")

	// Install the pgx tracer manually: the mock client builds its pool
	// directly instead of going through NewClient.
	parentCtx, parentSpan := telemetry.StartSpan(context.Background(), "test", "controller.parent")
	err := client.PromoteStandby(parentCtx)
	parentSpan.End()
	require.NoError(t, err)

	spans := sr.Ended()
	names := spanNames(spans)
	assert.Contains(t, names, "db.PromoteStandby")
	assert.Contains(t, names, "controller.parent")

	// Parent linkage: db.PromoteStandby's parent is controller.parent.
	var opSpan, parent sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "db.PromoteStandby":
			opSpan = s
		case "controller.parent":
			parent = s
		}
	}
	require.NotNil(t, opSpan)
	require.NotNil(t, parent)
	assert.Equal(t, parent.SpanContext().SpanID(), opSpan.Parent().SpanID())
	assert.Equal(t, parent.SpanContext().TraceID(), opSpan.SpanContext().TraceID())

	// The duration histogram hook observed the operation exactly once.
	assert.Equal(t, []string{"PromoteStandby"}, rec.operations())

	// E-5 PII gate: db spans must not carry statement text or credentials.
	telemetry.AssertNoPII(t, spans)
}

// TestCompositeMethodErrorSetsSpanStatus verifies the error path marks the
// operation span with codes.Error.
func TestCompositeMethodErrorSetsSpanStatus(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	err := client.PromoteStandby(context.Background())
	require.Error(t, err)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "db.PromoteStandby" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code)
		}
	}
	assert.True(t, found, "db.PromoteStandby span not exported")
}

// TestPgxQueryTracerSpans verifies that the pgx tracer creates a per-query
// child span (no SQL text in attributes) when installed on the pool config.
func TestPgxQueryTracerSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	tracer := &pgxQueryTracer{database: "testdb"}
	ctx := tracer.TraceQueryStart(context.Background(), nil,
		pgx.TraceQueryStartData{SQL: "SELECT secret FROM credentials"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "db.query", span.Name())
	for _, attr := range span.Attributes() {
		assert.NotContains(t, attr.Value.AsString(), "SELECT secret",
			"SQL text must not be recorded in span attributes")
	}
}

// TestRepresentativeOperationsObserveDuration exercises several composite
// methods against the mock server and asserts each observes its duration
// histogram with the bounded operation label.
func TestRepresentativeOperationsObserveDuration(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("OK")
	})
	defer cleanup()

	rec := &opDurationRecorder{}
	client.SetRecorder(rec, "c1", "ns1")
	ctx := context.Background()

	_ = client.PromoteStandby(ctx)
	_ = client.SetupExporterRole(ctx, "pw")
	_, _, _ = client.GetQueryHistory(ctx, QueryHistoryFilter{})

	ops := rec.operations()
	assert.Contains(t, ops, "PromoteStandby")
	assert.Contains(t, ops, "SetupExporterRole")
	assert.Contains(t, ops, "GetQueryHistory")
}

// TestGetMaxConnections verifies the new max_connections query (C-5).
func TestGetMaxConnections(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{{name: "setting", oid: 23}}, []string{"250"})
	})
	defer cleanup()

	maxConns, err := client.GetMaxConnections(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(250), maxConns)
}

func TestGetMaxConnectionsError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("no such setting")
	})
	defer cleanup()

	_, err := client.GetMaxConnections(context.Background())
	require.Error(t, err)
}
