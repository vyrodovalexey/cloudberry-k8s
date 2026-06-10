package controller

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// startControllerSpan opens a child span for a multi-step controller
// operation, named "controller.<operation>", parented on the current span in
// ctx (the per-request Reconcile root). It returns the span context and a
// completion callback that records the error status (when non-nil) and ends
// the span. Attribute values must come from bounded enums (phase names,
// operation kinds) — never resource-derived free-form strings — to keep span
// cardinality low. No-op when telemetry is disabled.
func startControllerSpan(
	ctx context.Context,
	tracerName, operation string,
	attrs ...attribute.KeyValue,
) (spanCtx context.Context, end func(err error)) {
	ctx, span := telemetry.StartSpan(ctx, tracerName, "controller."+operation,
		trace.WithAttributes(attrs...),
	)
	return ctx, func(err error) {
		if err != nil {
			telemetry.SetSpanError(span, err)
		}
		span.End()
	}
}
