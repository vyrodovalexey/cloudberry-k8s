package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// routeUnmatched is the route label/span-name fallback for requests that do
// not match any registered mux pattern (404s, bad methods). Using a single
// constant keeps the metric/span cardinality bounded.
const routeUnmatched = "unmatched"

// routePattern resolves the route TEMPLATE that the server mux matched for
// the request (e.g. "/api/v1alpha1/clusters/{name}"), never the raw URL path,
// keeping label cardinality bounded. ServeMux.Handler performs the same match
// the dispatcher does, so the result is exact; an empty pattern (no match)
// returns routeUnmatched. The Go 1.22 mux pattern includes the method prefix
// ("GET /path/{x}") — it is stripped so the route label carries only the
// path template (the method is a separate label).
func (s *Server) routePattern(r *http.Request) string {
	_, pattern := s.mux.Handler(r)
	if pattern == "" {
		return routeUnmatched
	}
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

// metricsMiddleware records the generic HTTP server metrics for every API
// request (C-1/M-10):
//
//   - cloudberry_api_requests_total{route,method,code}
//   - cloudberry_api_request_duration_seconds{route,method}
//   - cloudberry_api_requests_in_flight
//
// It reuses the statusRecorder installed by the tracing middleware when
// present (single wrapper per request) and is a no-op when the server has no
// metrics recorder. The /metrics endpoint is served by the controller-runtime
// metrics server on a separate listener, so it never passes through here.
func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.metrics == nil {
			next.ServeHTTP(w, r)
			return
		}

		rec, ok := w.(*statusRecorder)
		if !ok {
			rec = &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		}

		s.metrics.AddAPIRequestsInFlight(1)
		start := time.Now()
		next.ServeHTTP(rec, r)
		duration := time.Since(start)
		s.metrics.AddAPIRequestsInFlight(-1)

		s.metrics.RecordAPIRequest(
			s.routePattern(r),
			r.Method,
			strconv.Itoa(rec.status),
			duration,
		)
	})
}
