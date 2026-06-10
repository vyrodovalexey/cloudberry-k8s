package api

// E-4 / C-2: duplicate-registration safety. The operator can host multiple
// API server instances over one process lifetime (restarts in tests, future
// multi-listener setups); all Prometheus families are owned by the Recorder,
// so constructing additional Servers against the SAME recorder/registry must
// never panic with duplicate registration, and both servers' traffic must
// aggregate into the same families.

import (
	"net/http"
	"net/http/httptest"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// counterValueSum sums all samples of a counter family in the registry.
func counterValueSum(t *testing.T, reg *prometheus.Registry, family string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				total += c.GetValue()
			}
		}
	}
	return total
}

func TestTwoServersOneRecorder_NoDuplicateRegistrationPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)

	var s1, s2 *Server
	require.NotPanics(t, func() {
		s1 = trackServer(NewServer(ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, nil, recorder, nil, 0))
		s2 = trackServer(NewServer(ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, nil, recorder, nil, 0))
	}, "two coexisting servers over one recorder must not re-register families")

	// Drive a request through EACH server's full middleware chain.
	for _, s := range []*Server{s1, s2} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		s.Handler().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	// Both servers' requests aggregate into the shared family.
	assert.Equal(t, 2.0, counterValueSum(t, reg, "cloudberry_api_requests_total"),
		"both servers must record into the same cloudberry_api_requests_total family")

	// Closing one server must not break the other's metrics pipeline.
	s1.Close()
	rec := httptest.NewRecorder()
	s2.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 3.0, counterValueSum(t, reg, "cloudberry_api_requests_total"))
}

// TestTwoRecordersTwoRegistries_Coexist proves per-registry isolation: a
// second PrometheusRecorder on a FRESH registry (the supported multi-recorder
// configuration) must construct without panicking and count independently.
func TestTwoRecordersTwoRegistries_Coexist(t *testing.T) {
	regA := prometheus.NewRegistry()
	regB := prometheus.NewRegistry()

	var recA, recB *metrics.PrometheusRecorder
	require.NotPanics(t, func() {
		recA = metrics.NewPrometheusRecorder(regA)
		recB = metrics.NewPrometheusRecorder(regB)
	})

	sA := trackServer(NewServer(ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, nil, recA, nil, 0))
	sB := trackServer(NewServer(ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, nil, recB, nil, 0))

	rec := httptest.NewRecorder()
	sA.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 1.0, counterValueSum(t, regA, "cloudberry_api_requests_total"))
	assert.Zero(t, counterValueSum(t, regB, "cloudberry_api_requests_total"),
		"registries must stay isolated")

	rec = httptest.NewRecorder()
	sB.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1.0, counterValueSum(t, regB, "cloudberry_api_requests_total"))
}

// metricFamilyCount is a small sanity helper ensuring the gathered family
// exists at all (guards against silently renamed families making the sums
// above vacuous zeros). Used via the assertion below.
func TestAPIRequestsTotalFamilyExists(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	s := trackServer(NewServer(ctrlfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, nil, recorder, nil, 0))

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var found *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "cloudberry_api_requests_total" {
			found = mf
		}
	}
	require.NotNil(t, found, "cloudberry_api_requests_total family missing")
	require.NotEmpty(t, found.GetMetric())
}
