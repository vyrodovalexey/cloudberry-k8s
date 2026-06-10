package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// dbTracerName is the tracer name used for all database client spans.
const dbTracerName = "db-client"

// attrDBSystem is the OTel semconv db.system attribute, constant for this client.
var attrDBSystem = attribute.String("db.system", "postgresql") //nolint:gochecknoglobals // immutable attribute

// pgxQueryTracer implements pgx.QueryTracer (and the batch variant), creating
// a child span for every SQL statement executed through the pool. The SQL
// statement text is intentionally NOT recorded as a span attribute: statements
// may embed identifiers derived from user input and history exports can be
// large. Only low-cardinality metadata (db.system, database name) is attached.
// It is an in-house tracer (instead of the otelpgx dependency) per the
// project's prefer-no-new-deps rule; spans are no-ops when telemetry is
// disabled (global no-op tracer).
type pgxQueryTracer struct {
	// database is the connected database name, attached as db.name.
	database string
}

// TraceQueryStart starts the per-statement span. pgx propagates the returned
// context to TraceQueryEnd.
func (t *pgxQueryTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	ctx, _ = telemetry.StartSpan(ctx, dbTracerName, "db.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrDBSystem, attribute.String("db.name", t.database)),
	)
	return ctx
}

// TraceQueryEnd ends the per-statement span, recording the error status.
func (t *pgxQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	if data.Err != nil {
		telemetry.SetSpanError(span, data.Err)
	}
	span.End()
}

// startOperation opens a named child span for a composite (multi-statement /
// long-running) client method and returns the span context plus a completion
// callback. The callback records the operation duration on the
// cloudberry_db_query_duration_seconds histogram (operation = method name,
// bounded set), marks the span on error, and ends it. This single hook point
// feeds both the OTel spans (D-2) and the Prometheus query-duration metrics
// (C-2). It is nil-safe for the optional metrics recorder and a no-op for
// spans when telemetry is disabled.
func (c *pgxClient) startOperation(
	ctx context.Context,
	operation string,
) (spanCtx context.Context, end func(err error)) {
	start := time.Now()
	ctx, span := telemetry.StartSpan(ctx, dbTracerName, "db."+operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrDBSystem, attribute.String("db.operation", operation)),
	)
	return ctx, func(err error) {
		if c.recorder != nil {
			c.recorder.ObserveDBQueryDuration(operation, time.Since(start))
		}
		if err != nil {
			telemetry.SetSpanError(span, err)
		}
		span.End()
	}
}
