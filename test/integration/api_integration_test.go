//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// APIIntegrationSuite tests REST API endpoints with real auth.
type APIIntegrationSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	server *api.Server
	ctx    context.Context
}

func TestIntegration_API(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(APIIntegrationSuite))
}

func (s *APIIntegrationSuite) SetupTest() {
	s.ctx = context.Background()

	// Create a cluster for API tests
	cluster := testutil.NewClusterBuilder("test-api-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithConfig(map[string]string{
			"shared_buffers": "256MB",
		}).
		Build()

	s.env = testutil.NewTestK8sEnv(cluster)

	// Create auth middleware with basic auth
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	s.server = api.NewServer(s.env.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)
}

func (s *APIIntegrationSuite) doRequest(method, path string, body string, username, password string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	if username != "" {
		req.SetBasicAuth(username, password)
	}
	rec := httptest.NewRecorder()
	s.server.Handler().ServeHTTP(rec, req)
	return rec
}

func (s *APIIntegrationSuite) TestIntegration_API_Healthz() {
	rec := s.doRequest(http.MethodGet, "/healthz", "", "", "")
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]string
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "ok", resp["status"])
}

func (s *APIIntegrationSuite) TestIntegration_API_Readyz() {
	rec := s.doRequest(http.MethodGet, "/readyz", "", "", "")
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_ListClusters_Authenticated() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), resp["items"])
}

func (s *APIIntegrationSuite) TestIntegration_API_ListClusters_Unauthenticated() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters", "", "", "")
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetCluster() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetCluster_NotFound() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/nonexistent?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetClusterStatus() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/status?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "test-api-cluster", resp["name"])
}

func (s *APIIntegrationSuite) TestIntegration_API_GetConfig() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/config?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetConfig_InsufficientPermissions() {
	// viewer has PermissionBasic, config requires PermissionOperatorBasic
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/config?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_StartCluster() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/start?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_StopCluster() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/stop?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_RestartCluster() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/restart?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_StartCluster_InsufficientPermissions() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/start?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_DeleteCluster_RequiresAdmin() {
	rec := s.doRequest(http.MethodDelete, "/api/v1alpha1/clusters/test-api-cluster?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetSegments() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/segments?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), resp["segmentsReady"])
	assert.NotNil(s.T(), resp["segmentsTotal"])
}

func (s *APIIntegrationSuite) TestIntegration_API_GetMirroring() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/mirroring?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_GetStandby() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/standby?namespace=default", "", "viewer", "viewerpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), true, resp["enabled"])
}

func (s *APIIntegrationSuite) TestIntegration_API_VacuumMaintenance() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/maintenance/vacuum?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_AnalyzeMaintenance() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/maintenance/analyze?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_ReindexMaintenance() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/maintenance/reindex?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_SecurityHeaders_Present() {
	rec := s.doRequest(http.MethodGet, "/healthz", "", "", "")
	assert.NotEmpty(s.T(), rec.Header().Get("X-Content-Type-Options"))
	assert.NotEmpty(s.T(), rec.Header().Get("X-Frame-Options"))
}

func (s *APIIntegrationSuite) TestIntegration_API_ListSessions() {
	rec := s.doRequest(http.MethodGet, "/api/v1alpha1/clusters/test-api-cluster/sessions?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

func (s *APIIntegrationSuite) TestIntegration_API_Rebalance() {
	rec := s.doRequest(http.MethodPost, "/api/v1alpha1/clusters/test-api-cluster/rebalance?namespace=default", "", "operator", "operatorpass")
	assert.Equal(s.T(), http.StatusAccepted, rec.Code)
}

// newRecordingServer builds an API server wired to a REAL PrometheusRecorder
// backed by an isolated registry, returning both so a test can drive the public
// HTTP surface and then gather the resulting metric families. It mirrors
// SetupTest's wiring exactly (same fake client, same basic-auth store) but
// swaps the NoopRecorder for an observable one so the request-side metric
// emission is verified end-to-end through the real handler chain.
func (s *APIIntegrationSuite) newRecordingServer() (*api.Server, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})

	server := api.NewServer(s.env.Client, authMW, nil, recorder, nil, 0)
	return server, reg
}

// counterValueWithLabels returns the value of the sample of the named counter
// family carrying exactly the given label set, and whether such a sample was
// found. Used to assert a specific {operation,result} / {kind,operation,result}
// series is emitted (bounded-cardinality contract).
func counterValueWithLabels(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if labelsMatch(m, want) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// labelsMatch reports whether the metric's label set is a superset of want.
func labelsMatch(m *dto.Metric, want map[string]string) bool {
	got := make(map[string]string, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// TestIntegration_API_LifecycleRequestMetric verifies that a successful cluster
// lifecycle action requested through the REST API increments the new
// cloudberry_api_cluster_lifecycle_requests_total{operation,result} counter
// (request-side complement of the controller-side lifecycle counters). This
// black-boxes the wiring in server.recordLifecycleRequest through the real
// handler chain + a real PrometheusRecorder, complementing the direct-call unit
// coverage in internal/metrics.
func (s *APIIntegrationSuite) TestIntegration_API_LifecycleRequestMetric() {
	server, reg := s.newRecordingServer()

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/test-api-cluster/restart?namespace=default", nil)
	req.SetBasicAuth("operator", "operatorpass")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	require.Equal(s.T(), http.StatusAccepted, rec.Code,
		"restart request should be accepted")

	v, found := counterValueWithLabels(s.T(), reg,
		"cloudberry_api_cluster_lifecycle_requests_total",
		map[string]string{"operation": "restart", "result": "accepted"})
	assert.True(s.T(), found,
		"cloudberry_api_cluster_lifecycle_requests_total{operation=restart,result=accepted} must be emitted")
	assert.InDelta(s.T(), 1.0, v, 0.001,
		"a single accepted restart must increment the lifecycle-request counter by 1")
}

// TestIntegration_API_LifecycleMaintenanceMetric verifies the same lifecycle
// counter is also emitted for a maintenance action (vacuum) requested via the
// API, exercising the setMaintenanceAnnotation -> recordLifecycleRequest path
// with a distinct bounded operation label.
func (s *APIIntegrationSuite) TestIntegration_API_LifecycleMaintenanceMetric() {
	server, reg := s.newRecordingServer()

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1alpha1/clusters/test-api-cluster/maintenance/vacuum?namespace=default", nil)
	req.SetBasicAuth("operator", "operatorpass")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	require.Equal(s.T(), http.StatusAccepted, rec.Code,
		"vacuum maintenance request should be accepted")

	v, found := counterValueWithLabels(s.T(), reg,
		"cloudberry_api_cluster_lifecycle_requests_total",
		map[string]string{"operation": "vacuum", "result": "accepted"})
	assert.True(s.T(), found,
		"cloudberry_api_cluster_lifecycle_requests_total{operation=vacuum,result=accepted} must be emitted")
	assert.InDelta(s.T(), 1.0, v, 0.001,
		"a single accepted vacuum must increment the lifecycle-request counter by 1")
}
