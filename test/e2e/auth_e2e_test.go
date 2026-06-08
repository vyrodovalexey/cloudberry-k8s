//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// AuthE2ESuite tests the full authentication flow end-to-end.
type AuthE2ESuite struct {
	E2ESuite
}

func TestE2E_Auth(t *testing.T) {
	suite.Run(t, new(AuthE2ESuite))
}

func (s *AuthE2ESuite) TestE2E_AuthFlow_BasicAuth_FullJourney() {
	// This test exercises the complete basic auth flow:
	// 1. Configure auth with multiple users
	// 2. Access API with different permission levels
	// 3. Verify access control works correctly

	s.logger.Info("starting basic auth full journey E2E test")

	// Create a cluster
	cluster := testutil.NewClusterBuilder("e2e-auth", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Setup auth with multiple users at different permission levels
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)
	store.SetCredentials("operator", "operatorpass", auth.PermissionOperator)
	store.SetCredentials("opbasic", "opbasicpass", auth.PermissionOperatorBasic)
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)
	store.SetCredentials("selfonly", "selfpass", auth.PermissionSelfOnly)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)

	// Test matrix: endpoint -> required permission -> user -> expected result
	tests := []struct {
		name           string
		method         string
		path           string
		username       string
		password       string
		expectedStatus int
	}{
		// Health endpoints (no auth required)
		{"healthz_no_auth", http.MethodGet, "/healthz", "", "", http.StatusOK},
		{"readyz_no_auth", http.MethodGet, "/readyz", "", "", http.StatusOK},

		// Cluster listing (requires Basic)
		{"list_clusters_admin", http.MethodGet, "/api/v1alpha1/clusters", "admin", "adminpass", http.StatusOK},
		{"list_clusters_viewer", http.MethodGet, "/api/v1alpha1/clusters", "viewer", "viewerpass", http.StatusOK},
		{"list_clusters_no_auth", http.MethodGet, "/api/v1alpha1/clusters", "", "", http.StatusUnauthorized},

		// Cluster operations (requires Operator)
		{"start_cluster_operator", http.MethodPost, "/api/v1alpha1/clusters/e2e-auth/start?namespace=" + s.namespace, "operator", "operatorpass", http.StatusAccepted},
		{"start_cluster_viewer_denied", http.MethodPost, "/api/v1alpha1/clusters/e2e-auth/start?namespace=" + s.namespace, "viewer", "viewerpass", http.StatusForbidden},

		// Admin operations (requires Admin)
		{"delete_cluster_admin", http.MethodDelete, "/api/v1alpha1/clusters/e2e-auth?namespace=" + s.namespace, "admin", "adminpass", http.StatusOK},
		{"delete_cluster_operator_denied", http.MethodDelete, "/api/v1alpha1/clusters/e2e-auth?namespace=" + s.namespace, "operator", "operatorpass", http.StatusForbidden},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.username != "" {
				req.SetBasicAuth(tt.username, tt.password)
			}
			rec := httptest.NewRecorder()

			server.Handler().ServeHTTP(rec, req)

			assert.Equal(s.T(), tt.expectedStatus, rec.Code,
				"test=%s method=%s path=%s user=%s", tt.name, tt.method, tt.path, tt.username)
		})
	}

	s.logger.Info("basic auth full journey E2E test completed")
}

func (s *AuthE2ESuite) TestE2E_AuthFlow_PermissionEscalation_Prevention() {
	s.logger.Info("starting permission escalation prevention E2E test")

	cluster := testutil.NewClusterBuilder("e2e-auth-escalation", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("viewer", "viewerpass", auth.PermissionBasic)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)

	// A viewer should NOT be able to perform any write operations
	writeEndpoints := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1alpha1/clusters/e2e-auth-escalation/start?namespace=" + s.namespace},
		{http.MethodPost, "/api/v1alpha1/clusters/e2e-auth-escalation/stop?namespace=" + s.namespace},
		{http.MethodPost, "/api/v1alpha1/clusters/e2e-auth-escalation/restart?namespace=" + s.namespace},
		{http.MethodPost, "/api/v1alpha1/clusters/e2e-auth-escalation/maintenance/vacuum?namespace=" + s.namespace},
		{http.MethodDelete, "/api/v1alpha1/clusters/e2e-auth-escalation?namespace=" + s.namespace},
	}

	for _, ep := range writeEndpoints {
		req := httptest.NewRequest(ep.method, ep.path, nil)
		req.SetBasicAuth("viewer", "viewerpass")
		rec := httptest.NewRecorder()

		server.Handler().ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusForbidden, rec.Code,
			"viewer should not access %s %s", ep.method, ep.path)
	}

	s.logger.Info("permission escalation prevention E2E test completed")
}

func (s *AuthE2ESuite) TestE2E_AuthFlow_SecurityHeaders_AllEndpoints() {
	s.logger.Info("starting security headers E2E test")

	cluster := testutil.NewClusterBuilder("e2e-auth-headers", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)

	endpoints := []string{
		"/healthz",
		"/readyz",
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		rec := httptest.NewRecorder()

		server.Handler().ServeHTTP(rec, req)

		assert.NotEmpty(s.T(), rec.Header().Get("X-Content-Type-Options"),
			"X-Content-Type-Options should be set for %s", ep)
		assert.NotEmpty(s.T(), rec.Header().Get("X-Frame-Options"),
			"X-Frame-Options should be set for %s", ep)
		assert.NotEmpty(s.T(), rec.Header().Get("Cache-Control"),
			"Cache-Control should be set for %s", ep)
	}

	s.logger.Info("security headers E2E test completed")
}

func (s *AuthE2ESuite) TestE2E_AuthFlow_APIResponseFormat() {
	s.logger.Info("starting API response format E2E test")

	cluster := testutil.NewClusterBuilder("e2e-auth-format", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "adminpass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, nil)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, nil, 0)

	// Test that error responses have proper JSON format
	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
	assert.Contains(s.T(), rec.Header().Get("Content-Type"), "application/json")

	var errResp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&errResp)
	require.NoError(s.T(), err, "error response should be valid JSON")
	assert.NotNil(s.T(), errResp["error"], "error response should have 'error' field")

	s.logger.Info("API response format E2E test completed")
}
