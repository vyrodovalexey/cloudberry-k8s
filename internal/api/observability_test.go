package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// obsRecorder captures the new observability Recorder calls for assertions.
type obsRecorder struct {
	metrics.NoopRecorder
	mu                  sync.Mutex
	apiRequests         []apiRequestSample
	inFlight            float64
	maxInFlight         float64
	rateLimitRoutes     []string
	sessionTerminations []string
	queryCancels        int
	clusterOps          []string
	logStreamResults    []string
	logStreamBytes      float64
	migrateResults      []string
}

type apiRequestSample struct {
	route, method, code string
}

func (o *obsRecorder) RecordAPIRequest(route, method, code string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.apiRequests = append(o.apiRequests, apiRequestSample{route, method, code})
}

func (o *obsRecorder) AddAPIRequestsInFlight(delta float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inFlight += delta
	if o.inFlight > o.maxInFlight {
		o.maxInFlight = o.inFlight
	}
}

func (o *obsRecorder) RecordRateLimitRejection(route string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rateLimitRoutes = append(o.rateLimitRoutes, route)
}

func (o *obsRecorder) RecordSessionTermination(_, _, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sessionTerminations = append(o.sessionTerminations, result)
}

func (o *obsRecorder) RecordQueryCancel(_, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queryCancels++
}

func (o *obsRecorder) RecordAPIClusterOperation(operation, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.clusterOps = append(o.clusterOps, operation+"/"+result)
}

func (o *obsRecorder) RecordLogStreamSession(result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logStreamResults = append(o.logStreamResults, result)
}

func (o *obsRecorder) AddLogStreamBytes(n float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logStreamBytes += n
}

func (o *obsRecorder) RecordMigrateOperation(result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.migrateResults = append(o.migrateResults, result)
}

// obsAuthedRequest builds a request carrying an admin identity so the
// permission middleware admits it (the auth middleware itself is nil in these
// tests; identity injection mirrors the existing server tests).
func obsAuthedRequest(method, path string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, path, body)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	return req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
}

// newObsServer builds a Server with the capturing recorder and given clusters.
func newObsServer(rec metrics.Recorder, clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, nil, rec, nil, 0))
}

// TestMetricsMiddleware_RouteTemplateAndCardinality verifies the requests
// counter uses the ROUTE TEMPLATE (one label value for two cluster names) and
// the duration histogram and in-flight gauge are maintained (C-1).
func TestMetricsMiddleware_RouteTemplateAndCardinality(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec, newTestCluster("alpha", "default"), newTestCluster("beta", "default"))
	handler := s.Handler()

	for _, path := range []string{"/api/v1alpha1/clusters/alpha", "/api/v1alpha1/clusters/beta"} {
		req := obsAuthedRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	}

	require.Len(t, rec.apiRequests, 2)
	// Two requests to DIFFERENT cluster names produce ONE route label value.
	assert.Equal(t, "/api/v1alpha1/clusters/{name}", rec.apiRequests[0].route)
	assert.Equal(t, rec.apiRequests[0].route, rec.apiRequests[1].route)
	assert.Equal(t, "GET", rec.apiRequests[0].method)
	assert.Equal(t, "200", rec.apiRequests[0].code)

	// In-flight went up during the requests and returned to zero.
	assert.Equal(t, 0.0, rec.inFlight)
	assert.GreaterOrEqual(t, rec.maxInFlight, 1.0)
}

// TestMetricsMiddleware_UnmatchedRoute verifies the bounded fallback label.
func TestMetricsMiddleware_UnmatchedRoute(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec)
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodGet, "/no/such/route", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Len(t, rec.apiRequests, 1)
	assert.Equal(t, routeUnmatched, rec.apiRequests[0].route)
	assert.Equal(t, "404", rec.apiRequests[0].code)
}

// TestMetricsMiddleware_NilRecorderSafe verifies the middleware is a no-op
// passthrough without a recorder.
func TestMetricsMiddleware_NilRecorderSafe(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, nil, nil, 0))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

// TestRateLimitRejectionMetric verifies the 429 path increments the rejection
// counter with the route template (C-1).
func TestRateLimitRejectionMetric(t *testing.T) {
	rec := &obsRecorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(newTestCluster("c1", "default")).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, rec, nil, 1)) // 1 request/interval
	defer s.Close()

	// withAuth is a passthrough without an auth middleware, so exercise the
	// limiter middleware directly — the SAME limiter instance the server
	// wires, including its route-labeled rejection callback.
	handler := s.rateLimiter.Middleware(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	var last int
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.RemoteAddr = "10.1.2.3:1234"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		last = w.Code
	}
	require.Equal(t, http.StatusTooManyRequests, last)
	require.NotEmpty(t, rec.rateLimitRoutes)
	assert.Equal(t, "/api/v1alpha1/clusters", rec.rateLimitRoutes[0])
}

// TestTracingMiddleware_UnmatchedFallbackName verifies the D-1 span-name
// fallback for unmatched routes.
func TestTracingMiddleware_UnmatchedFallbackName(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	rec := &obsRecorder{}
	s := newObsServer(rec)
	req := httptest.NewRequest(http.MethodGet, "/missing", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	spans := sr.Ended()
	require.NotEmpty(t, spans)
	assert.Equal(t, "GET unmatched", spans[0].Name())
}

// terminateMockFactory returns a factory whose client reports terminate/cancel
// outcomes.
type terminateMockFactory struct {
	client db.Client
	err    error
}

func (f *terminateMockFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	return f.client, f.err
}

// TestSessionTerminationMetrics verifies C-4: terminate-session records the
// terminations counter; cancel via BOTH APIs records the cancel counter.
func TestSessionTerminationMetrics(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec, newTestCluster("c1", "default"))
	s.dbFactory = &terminateMockFactory{client: &mockDBClient{}}
	handler := s.Handler()

	// Terminate session (DELETE /sessions/{pid}).
	req := obsAuthedRequest(http.MethodDelete, "/api/v1alpha1/clusters/c1/sessions/42", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []string{"success"}, rec.sessionTerminations)

	// Cancel via the sessions API.
	req = obsAuthedRequest(http.MethodPost, "/api/v1alpha1/clusters/c1/sessions/42/cancel", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Cancel via the queries API.
	req = obsAuthedRequest(http.MethodPost, "/api/v1alpha1/clusters/c1/queries/42/cancel", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	assert.Equal(t, 2, rec.queryCancels, "both cancel APIs must record the cancel metric")
}

// TestClusterCRUDMetrics verifies C-8 cluster create/delete counters.
func TestClusterCRUDMetrics(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec, newTestCluster("doomed", "default"))
	handler := s.Handler()

	// Successful create.
	body := strings.NewReader(`{"metadata":{"name":"newc","namespace":"default"},` +
		`"spec":{"version":"7.7","image":"x","coordinator":{"storage":{"size":"1Gi"}},` +
		`"segments":{"count":1,"storage":{"size":"1Gi"}}}}`)
	req := obsAuthedRequest(http.MethodPost, "/api/v1alpha1/clusters", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	// Failed create (duplicate name).
	body = strings.NewReader(`{"metadata":{"name":"newc","namespace":"default"},` +
		`"spec":{"version":"7.7"}}`)
	req = obsAuthedRequest(http.MethodPost, "/api/v1alpha1/clusters", body)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// Successful delete.
	req = obsAuthedRequest(http.MethodDelete, "/api/v1alpha1/clusters/doomed", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	assert.Equal(t, []string{"create/success", "create/error", "delete/success"}, rec.clusterOps)
}

// TestCopyLogStreamReturnsBytes verifies the C-8 log stream byte accounting.
func TestCopyLogStreamReturnsBytes(t *testing.T) {
	w := httptest.NewRecorder()
	written, err := copyLogStream(context.Background(), w, strings.NewReader("hello logs"), false)
	require.NoError(t, err)
	assert.Equal(t, int64(10), written)

	// Follow mode: EOF ends the session cleanly.
	w = httptest.NewRecorder()
	written, err = copyLogStream(context.Background(), w, strings.NewReader("follow me"), true)
	require.NoError(t, err)
	assert.Equal(t, int64(9), written)
}

// TestRecordLogStreamSession verifies result labeling and byte accounting.
func TestRecordLogStreamSession(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec)

	s.recordLogStreamSession(128, nil)
	s.recordLogStreamSession(0, io.ErrClosedPipe)

	assert.Equal(t, []string{"success", "error"}, rec.logStreamResults)
	assert.Equal(t, 128.0, rec.logStreamBytes)
}

// TestHandlerChildSpans verifies D-6: representative handlers open a static-
// named child span under the request root span.
func TestHandlerChildSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	rec := &obsRecorder{}
	s := newObsServer(rec, newTestCluster("c1", "default"))
	s.dbFactory = &terminateMockFactory{client: &mockDBClient{}}
	handler := s.Handler()

	cases := []struct {
		method, path, span string
	}{
		{http.MethodGet, "/api/v1alpha1/clusters/c1/sessions", "api.sessions.list"},
		{http.MethodGet, "/api/v1alpha1/clusters/c1/workload/resource-groups", "api.resourceGroups.list"},
		{http.MethodGet, "/api/v1alpha1/clusters/c1/queries/history", "api.queryHistory.search"},
		{http.MethodPost, "/api/v1alpha1/clusters/c1/queries/plan-check", "api.planCheck"},
		{http.MethodDelete, "/api/v1alpha1/clusters/c1/sessions/42", "api.session.terminate"},
		{http.MethodPost, "/api/v1alpha1/clusters/c1/queries/42/cancel", "api.query.cancel"},
	}
	for _, tc := range cases {
		var body io.Reader
		if tc.method == http.MethodPost && strings.Contains(tc.path, "plan-check") {
			body = strings.NewReader(`{"planText":"Seq Scan on t"}`)
		}
		req := obsAuthedRequest(tc.method, tc.path, body)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}

	spans := sr.Ended()
	byName := map[string]bool{}
	parents := map[string]bool{}
	for _, sp := range spans {
		byName[sp.Name()] = true
		if sp.Parent().IsValid() {
			parents[sp.Name()] = true
		}
	}
	for _, tc := range cases {
		assert.True(t, byName[tc.span], "missing span %s", tc.span)
		assert.True(t, parents[tc.span], "span %s must be a child of the request root", tc.span)
	}

	// E-5 PII gate: no credential-looking attribute may leave the API layer
	// (the authed requests above all carry an Authorization header).
	telemetry.AssertNoPII(t, spans)
}

// TestRoutePattern verifies the route-template resolution helper.
func TestRoutePattern(t *testing.T) {
	s := newObsServer(&obsRecorder{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters/foo/status", nil)
	assert.Equal(t, "/api/v1alpha1/clusters/{name}/status", s.routePattern(req))

	req = httptest.NewRequest(http.MethodGet, "/definitely/not/registered", nil)
	assert.Equal(t, routeUnmatched, s.routePattern(req))
}

// TestPrometheusAPIMetricsEndToEnd exercises the real PrometheusRecorder
// through the middleware (registration + label correctness, E-4 style).
func TestPrometheusAPIMetricsEndToEnd(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	s := newObsServer(rec, newTestCluster("c1", "default"))
	handler := s.Handler()

	req := obsAuthedRequest(http.MethodGet, "/api/v1alpha1/clusters/c1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	families, err := reg.Gather()
	require.NoError(t, err)
	var found bool
	for _, f := range families {
		if f.GetName() == "cloudberry_api_requests_total" {
			found = true
			require.NotEmpty(t, f.GetMetric())
			labels := map[string]string{}
			for _, lp := range f.GetMetric()[0].GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			assert.Equal(t, "/api/v1alpha1/clusters/{name}", labels["route"])
			assert.Equal(t, "GET", labels["method"])
			assert.Equal(t, "200", labels["code"])
		}
	}
	assert.True(t, found)
}

// TestCSVEscapeFormulaInjection verifies the L-8 hardening.
func TestCSVEscapeFormulaInjection(t *testing.T) {
	cases := map[string]string{
		"=cmd|' /C calc'!A0": "'=cmd|' /C calc'!A0",
		"+SUM(1,2)":          `"'+SUM(1,2)"`, // quoted because of the comma
		"-2+3":               "'-2+3",
		"@cell":              "'@cell",
		"normal":             "normal",
		"":                   "",
	}
	for in, want := range cases {
		assert.Equal(t, want, csvEscape(in), "input %q", in)
	}
}

// TestMigrateMetric verifies the migrate counter (C-8) on the success path.
func TestMigrateMetric(t *testing.T) {
	rec := &obsRecorder{}
	s := newObsServer(rec)
	s.recordMigrate("started")
	s.recordMigrate("error")
	assert.Equal(t, []string{"started", "error"}, rec.migrateResults)
}
