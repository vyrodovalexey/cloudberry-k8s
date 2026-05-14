//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	s.server = api.NewServer(s.env.Client, authMW, nil, &metrics.NoopRecorder{}, nil)
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
