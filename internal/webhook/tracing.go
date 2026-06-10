package webhook

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// webhookTracerName is the tracer name for admission webhook spans.
const webhookTracerName = "admission-webhook"

// startAdmissionSpan opens a span around an admission handler (D-5),
// complementing the existing cloudberry_webhook_admission_total metrics.
// spanName is "webhook.validate" or "webhook.mutate"; operation comes from
// the bounded admission-operation enum. The returned completion callback
// records the allowed attribute (false + error status on a denial/error) and
// ends the span. Only the resource KIND and operation are attached — never
// resource names or spec contents. No-op when telemetry is disabled.
func startAdmissionSpan(
	ctx context.Context,
	spanName, operation string,
) (spanCtx context.Context, end func(err error)) {
	ctx, span := telemetry.StartSpan(ctx, webhookTracerName, spanName,
		trace.WithAttributes(
			attribute.String("webhook.kind", "CloudberryCluster"),
			attribute.String("webhook.operation", operation),
		),
	)
	return ctx, func(err error) {
		span.SetAttributes(attribute.Bool("webhook.allowed", err == nil))
		if err != nil {
			telemetry.SetSpanError(span, err)
		}
		span.End()
	}
}
