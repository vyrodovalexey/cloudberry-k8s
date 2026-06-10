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

// Unwrap returns the wrapped ResponseWriter so http.NewResponseController can
// reach the underlying writer's optional interfaces (Flusher, deadline
// control) through this wrapper. Required for streaming endpoints (log
// follow mode, CSV exports).
func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// Flush delegates to the wrapped writer when it supports http.Flusher so
// streaming handlers that type-assert on http.Flusher keep working through
// this wrapper. It is a safe no-op when the underlying writer does not
// support flushing (nothing to flush in that case).
func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// tracingMiddleware opens a server span for every API request and records
// the response status. On a non-2xx response the span is marked as an error.
// The span context is propagated into the request so handler-level child
// spans nest underneath it. Tracing is a no-op when telemetry is disabled
// (the global no-op tracer is used).
//
// Span naming (D-1/M-11): the span starts with a provisional low-cardinality
// name and is RENAMED after the handler runs to "METHOD <route template>"
// (e.g. "GET /api/v1alpha1/clusters/{name}") — never the raw URL path, whose
// cluster names/PIDs would explode span-name cardinality. The raw path stays
// available on the http.target attribute. Unmatched requests get the
// "METHOD unmatched" fallback.
func (s *Server) tracingMiddleware(next http.Handler) http.Handler {
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract any inbound trace context so cross-service traces continue.
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		ctx, span := telemetry.StartSpan(ctx, apiTracerName, r.Method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			),
		)
		defer span.End()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetName(r.Method + " " + s.routePattern(r))
		span.SetAttributes(attribute.Int("http.status_code", rec.status))
		if rec.status >= http.StatusBadRequest {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}
	})
}
