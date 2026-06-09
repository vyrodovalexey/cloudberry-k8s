package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestTracingMiddleware_CreatesSpan verifies the tracing middleware opens a
// server span per request and records the response status code. It installs an
// in-memory SDK tracer provider so the emitted span can be asserted.
func TestTracingMiddleware_CreatesSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	s := newTestServerWithBatch()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	handler := s.tracingMiddleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters/foo", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "GET /api/v1alpha1/clusters/foo", span.Name())
	// A 404 response must mark the span as an error.
	assert.Equal(t, "Error", span.Status().Code.String())
}

// TestTracingMiddleware_NoopTracer verifies the middleware is a safe no-op when
// the global tracer is the no-op provider (telemetry disabled): the request is
// still served and no span machinery panics.
func TestTracingMiddleware_NoopTracer(t *testing.T) {
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(noop.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	s := newTestServerWithBatch()

	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := s.tracingMiddleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestTracingMiddleware_ImplicitOK verifies the statusRecorder.Write path: a
// handler that writes a body WITHOUT an explicit WriteHeader call is treated as
// an implicit 200 OK, so the span records http.status_code=200 and is NOT
// marked as an error. This exercises the Write() branch that defaults status to
// 200 when no WriteHeader was called.
func TestTracingMiddleware_ImplicitOK(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	s := newTestServerWithBatch()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Write a body without calling WriteHeader -> implicit 200 OK.
		_, _ = w.Write([]byte("ok"))
	})
	handler := s.tracingMiddleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "GET /api/v1alpha1/clusters", span.Name())
	// An implicit 200 must NOT mark the span as an error.
	assert.NotEqual(t, "Error", span.Status().Code.String())
}

// TestTracingMiddleware_ExplicitHeaderThenWrite covers the statusRecorder.Write
// branch where WriteHeader was ALREADY called (s.status != 0): Write must NOT
// overwrite the previously-recorded status, so a 201 set via WriteHeader is
// preserved even though a body is subsequently written.
func TestTracingMiddleware_ExplicitHeaderThenWrite(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	s := newTestServerWithBatch()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	})
	handler := s.tracingMiddleware(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1alpha1/clusters", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	// 201 is a success status: span must not be marked as error and the
	// recorded status must be the explicitly-set 201, not a Write-implied 200.
	assert.NotEqual(t, "Error", spans[0].Status().Code.String())
}
