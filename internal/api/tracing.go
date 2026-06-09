package api

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// apiTracerName is the tracer name used for all REST API spans.
const apiTracerName = "api-server"

// statusRecorder wraps http.ResponseWriter to capture the HTTP status code so
// the tracing middleware can mark the span as an error on non-2xx responses.
// It defaults to 200 because net/http treats a handler that writes a body
// without calling WriteHeader as a 200 OK.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the status code before delegating to the wrapped writer.
func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Write ensures a status is recorded for handlers that write a body without an
// explicit WriteHeader call (implicit 200 OK).
func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// tracingMiddleware opens a server span for every API request, named by the
// matched method+route pattern, and records the response status. On a non-2xx
// response the span is marked as an error. The span context is propagated into
// the request so handler-level child spans nest underneath it. Tracing is a
// no-op when telemetry is disabled (the global no-op tracer is used).
func (s *Server) tracingMiddleware(next http.Handler) http.Handler {
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract any inbound trace context so cross-service traces continue.
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		spanName := r.Method + " " + r.URL.Path
		ctx, span := telemetry.StartSpan(ctx, apiTracerName, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			),
		)
		defer span.End()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetAttributes(attribute.Int("http.status_code", rec.status))
		if rec.status >= http.StatusBadRequest {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}
	})
}
