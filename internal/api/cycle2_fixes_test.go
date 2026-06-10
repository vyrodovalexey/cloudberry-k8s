package api

// Cycle-2 fix tests (T14, M-3): the metrics middleware decrements the
// in-flight gauge and records the request via defer, so a panicking handler
// can no longer leak the gauge upward or drop the request sample.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsMiddleware_PanickingHandler_GaugeRestoredAndRequestRecorded(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec)

	handler := s.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		panic("handler exploded")
	}))

	req := httptest.NewRequest(http.MethodGet, "/no/such/route", nil)
	w := httptest.NewRecorder()
	func() {
		// net/http's connection goroutine recovers handler panics in
		// production; the test harness recovers here so the deferred metric
		// bookkeeping that ran BEFORE the panic propagated can be asserted.
		defer func() {
			require.NotNil(t, recover(), "the panic must propagate through the middleware")
		}()
		handler.ServeHTTP(w, req)
	}()

	assert.Equal(t, 0.0, rec.inFlight,
		"the in-flight gauge must return to zero on the panic path")
	require.Len(t, rec.apiRequests, 1,
		"the request must be recorded exactly once despite the panic")
	assert.Equal(t, "500", rec.apiRequests[0].code,
		"the status written before the panic is recorded")
}

func TestMetricsMiddleware_StatusCodeRecordedPostDefer(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec)

	handler := s.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/no/such/route", nil))

	require.Len(t, rec.apiRequests, 1)
	assert.Equal(t, "418", rec.apiRequests[0].code)
	assert.Equal(t, 0.0, rec.inFlight)
	assert.GreaterOrEqual(t, rec.maxInFlight, 1.0,
		"the gauge must have been raised while the handler ran")
}

func TestMetricsMiddleware_ImplicitOKWithoutWriteHeader(t *testing.T) {
	// Guards the statusRecorder simplification (L-8): a handler writing a
	// body without WriteHeader still records the implicit 200.
	rec := &obsRecorder{}
	s := newObsServer(rec)

	handler := s.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/no/such/route", nil))

	require.Len(t, rec.apiRequests, 1)
	assert.Equal(t, "200", rec.apiRequests[0].code)
}
