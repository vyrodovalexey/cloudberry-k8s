//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 51 E2E: Security Headers
// ============================================================================
//
// End-to-end tests verifying that ALL 8 security headers are present with
// exact values on every API response. The SecurityHeaders middleware is
// applied globally as the outermost middleware in server.Handler().
//
// Two suites:
//   - Scenario51SecurityHeadersE2ESuite — mock-based (fake K8s client)
//   - Scenario51RealClusterE2ESuite — real Cloudberry cluster via port-forward
// ============================================================================

// Scenario51SecurityHeadersE2ESuite tests Scenario 51: Security Headers end-to-end.
type Scenario51SecurityHeadersE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario51(t *testing.T) {
	suite.Run(t, new(Scenario51SecurityHeadersE2ESuite))
}

// setupE2EServer creates an API server with basic auth middleware and a fake
// K8s client for mock-based E2E tests. Returns the httptest.Server and a
// cleanup function.
func (s *Scenario51SecurityHeadersE2ESuite) setupE2EServer() (*httptest.Server, func()) {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, logger, 0)

	ts := httptest.NewServer(server.Handler())
	cleanup := func() {
		ts.Close()
		server.Close()
	}
	return ts, cleanup
}

// assertSecurityHeaders verifies that all 8 security headers are present
// with exact expected values on the given HTTP response.
func (s *Scenario51SecurityHeadersE2ESuite) assertSecurityHeaders(resp *http.Response) {
	s.T().Helper()
	for _, tc := range cases.SecurityHeaderCases() {
		actual := resp.Header.Get(tc.Header)
		assert.Equal(s.T(), tc.ExpectedValue, actual,
			"header %s should be %q, got %q", tc.Header, tc.ExpectedValue, actual)
	}
}

// --- Mock-based E2E Tests ---

// TestE2E_Scenario51_AllHeaders_HealthEndpoint verifies that all 8 security
// headers are present on GET /healthz (no auth required).
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_AllHeaders_HealthEndpoint() {
	s.logger.Info("starting scenario 51 E2E: health endpoint security headers")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/healthz", nil)
	require.NoError(s.T(), err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 E2E: health endpoint security headers completed")
}

// TestE2E_Scenario51_AllHeaders_AuthenticatedGET verifies that all 8 security
// headers are present on GET /api/v1alpha1/clusters with admin auth.
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_AllHeaders_AuthenticatedGET() {
	s.logger.Info("starting scenario 51 E2E: authenticated GET security headers")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 E2E: authenticated GET security headers completed")
}

// TestE2E_Scenario51_AllHeaders_UnauthorizedResponse verifies that all 8
// security headers are present on a 401 Unauthorized response (no auth header).
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_AllHeaders_UnauthorizedResponse() {
	s.logger.Info("starting scenario 51 E2E: unauthorized response security headers")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 E2E: unauthorized response security headers completed")
}

// TestE2E_Scenario51_AllHeaders_ForbiddenResponse verifies that all 8 security
// headers are present on a 403 Forbidden response (viewer tries POST).
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_AllHeaders_ForbiddenResponse() {
	s.logger.Info("starting scenario 51 E2E: forbidden response security headers")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	body := `{"metadata":{"name":"test-s51","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("viewer", "viewer-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 E2E: forbidden response security headers completed")
}

// TestE2E_Scenario51_AllHeaders_ErrorResponse verifies that all 8 security
// headers are present on a 404 Not Found response.
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_AllHeaders_ErrorResponse() {
	s.logger.Info("starting scenario 51 E2E: error response security headers")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/nonexistent", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusNotFound, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 E2E: error response security headers completed")
}

// TestE2E_Scenario51_HeadersConsistentAcrossEndpoints verifies that the SAME
// set of security headers appears on ALL responses regardless of endpoint.
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_HeadersConsistentAcrossEndpoints() {
	s.logger.Info("starting scenario 51 E2E: headers consistent across endpoints")

	ts, cleanup := s.setupE2EServer()
	defer cleanup()

	type endpointResult struct {
		name    string
		headers map[string]string
	}

	expectedHeaders := cases.SecurityHeaderCases()
	results := make([]endpointResult, 0, 4)

	// 1. GET /healthz (no auth, 200)
	req1, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/healthz", nil)
	require.NoError(s.T(), err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(s.T(), err)
	defer resp1.Body.Close()
	h1 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h1[tc.Header] = resp1.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /healthz", headers: h1})

	// 2. GET /readyz (no auth, 200)
	req2, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/readyz", nil)
	require.NoError(s.T(), err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(s.T(), err)
	defer resp2.Body.Close()
	h2 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h2[tc.Header] = resp2.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /readyz", headers: h2})

	// 3. GET /api/v1alpha1/clusters (authenticated, 200)
	req3, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req3.SetBasicAuth("admin", "admin-secret")
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(s.T(), err)
	defer resp3.Body.Close()
	h3 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h3[tc.Header] = resp3.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /api/v1alpha1/clusters", headers: h3})

	// 4. GET /api/v1alpha1/clusters (no auth, 401)
	req4, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	resp4, err := http.DefaultClient.Do(req4)
	require.NoError(s.T(), err)
	defer resp4.Body.Close()
	h4 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h4[tc.Header] = resp4.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /api/v1alpha1/clusters (401)", headers: h4})

	// Compare all results: every endpoint should have the same header values.
	baseline := results[0]
	for i := 1; i < len(results); i++ {
		for _, tc := range expectedHeaders {
			assert.Equal(s.T(), baseline.headers[tc.Header], results[i].headers[tc.Header],
				"header %s should be consistent between %s and %s",
				tc.Header, baseline.name, results[i].name)
		}
	}

	// Also verify all headers match expected values.
	for _, result := range results {
		for _, tc := range expectedHeaders {
			assert.Equal(s.T(), tc.ExpectedValue, result.headers[tc.Header],
				"header %s on %s should be %q", tc.Header, result.name, tc.ExpectedValue)
		}
	}

	s.logger.Info("scenario 51 E2E: headers consistent across endpoints completed")
}

// TestE2E_Scenario51_SecurityHeaderCases_Coverage verifies that
// cases.SecurityHeaderCases() returns exactly 8 test cases with valid fields.
func (s *Scenario51SecurityHeadersE2ESuite) TestE2E_Scenario51_SecurityHeaderCases_Coverage() {
	s.logger.Info("starting scenario 51 E2E: security header cases coverage")

	headerCases := cases.SecurityHeaderCases()
	require.Len(s.T(), headerCases, 8, "should have exactly 8 security header cases")

	for _, tc := range headerCases {
		assert.NotEmpty(s.T(), tc.Name, "case name should not be empty")
		assert.NotEmpty(s.T(), tc.Header, "header name should not be empty")
		assert.NotEmpty(s.T(), tc.ExpectedValue, "expected value should not be empty")
		assert.NotEmpty(s.T(), tc.Description, "description should not be empty")
	}

	s.logger.Info("scenario 51 E2E: security header cases coverage completed")
}

// ============================================================================
// Scenario 51 Real Cluster E2E: Security Headers with Real Cloudberry
// ============================================================================
//
// These tests connect to the real Cloudberry cluster running in Kubernetes
// to verify that security headers are present on responses from an API server
// backed by a real database connection.
// ============================================================================

// Scenario51RealClusterE2ESuite tests Scenario 51 against the real Cloudberry cluster.
type Scenario51RealClusterE2ESuite struct {
	E2ESuite

	dbClient       db.Client
	portForwardCmd *exec.Cmd
	localPort      int
}

func TestE2E_Scenario51_RealCluster(t *testing.T) {
	suite.Run(t, new(Scenario51RealClusterE2ESuite))
}

func (s *Scenario51RealClusterE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()
	s.logger.Info("scenario 51 real cluster E2E suite setup")

	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		err = s.portForwardCmd.Start()
		if err != nil {
			s.T().Skipf("skipping scenario 51 real cluster E2E: kubectl port-forward failed: %v", err)
			return
		}

		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward()
			s.T().Skipf("skipping scenario 51 real cluster E2E: port-forward not ready")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := db.Config{
		Host:     host,
		Port:     int32(port),
		Database: database,
		Username: user,
		Password: password,
		SSLMode:  "disable",
		MaxConns: 3,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
	}

	dbClient, err := db.NewClient(ctx, cfg, s.logger)
	if err != nil {
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 51 real cluster E2E: cannot connect: %v", err)
		return
	}
	s.dbClient = dbClient

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 51 real cluster E2E: ping failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 51",
		"host", host, "port", port)
}

func (s *Scenario51RealClusterE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward()
	s.E2ESuite.TearDownSuite()
}

func (s *Scenario51RealClusterE2ESuite) cleanupPortForward() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		_ = s.portForwardCmd.Process.Kill()
		_ = s.portForwardCmd.Wait()
		s.portForwardCmd = nil
	}
}

// newRealClusterServer creates an API server backed by the real DB client
// with basic auth middleware for real cluster E2E tests.
func (s *Scenario51RealClusterE2ESuite) newRealClusterServer() (*httptest.Server, *api.Server) {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)

	ts := httptest.NewServer(server.Handler())
	return ts, server
}

// assertSecurityHeaders verifies that all 8 security headers are present
// with exact expected values on the given HTTP response.
func (s *Scenario51RealClusterE2ESuite) assertSecurityHeaders(resp *http.Response) {
	s.T().Helper()
	for _, tc := range cases.SecurityHeaderCases() {
		actual := resp.Header.Get(tc.Header)
		assert.Equal(s.T(), tc.ExpectedValue, actual,
			"header %s should be %q, got %q", tc.Header, tc.ExpectedValue, actual)
	}
}

// --- Real Cluster E2E Tests ---

// TestE2E_Scenario51_RealCluster_HealthEndpoint verifies that all 8 security
// headers are present on GET /healthz on an API server backed by a real DB.
func (s *Scenario51RealClusterE2ESuite) TestE2E_Scenario51_RealCluster_HealthEndpoint() {
	s.logger.Info("starting scenario 51 real cluster: health endpoint security headers")

	ts, server := s.newRealClusterServer()
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/healthz", nil)
	require.NoError(s.T(), err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 real cluster: health endpoint security headers completed")
}

// TestE2E_Scenario51_RealCluster_AuthenticatedGET verifies that all 8 security
// headers are present on GET /api/v1alpha1/clusters with admin auth on a
// real-DB-backed server.
func (s *Scenario51RealClusterE2ESuite) TestE2E_Scenario51_RealCluster_AuthenticatedGET() {
	s.logger.Info("starting scenario 51 real cluster: authenticated GET security headers")

	ts, server := s.newRealClusterServer()
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 real cluster: authenticated GET security headers completed")
}

// TestE2E_Scenario51_RealCluster_AuthFailure verifies that all 8 security
// headers are present on a 401 response when using wrong credentials on a
// real-DB-backed server.
func (s *Scenario51RealClusterE2ESuite) TestE2E_Scenario51_RealCluster_AuthFailure() {
	s.logger.Info("starting scenario 51 real cluster: auth failure security headers")

	ts, server := s.newRealClusterServer()
	defer ts.Close()
	defer server.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "wrong-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 real cluster: auth failure security headers completed")
}

// TestE2E_Scenario51_RealCluster_PermissionDenied verifies that all 8 security
// headers are present on a 403 response when a viewer tries to POST on a
// real-DB-backed server.
func (s *Scenario51RealClusterE2ESuite) TestE2E_Scenario51_RealCluster_PermissionDenied() {
	s.logger.Info("starting scenario 51 real cluster: permission denied security headers")

	ts, server := s.newRealClusterServer()
	defer ts.Close()
	defer server.Close()

	body := `{"metadata":{"name":"test-s51-rc","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("viewer", "viewer-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
	s.assertSecurityHeaders(resp)

	s.logger.Info("scenario 51 real cluster: permission denied security headers completed")
}

// TestE2E_Scenario51_RealCluster_MultipleEndpoints verifies that all 8 security
// headers are present and identical across multiple endpoints on a real-DB-backed
// server: GET /healthz, GET /readyz, GET /api/v1alpha1/clusters (authenticated),
// and GET /api/v1alpha1/clusters (unauthenticated, 401).
func (s *Scenario51RealClusterE2ESuite) TestE2E_Scenario51_RealCluster_MultipleEndpoints() {
	s.logger.Info("starting scenario 51 real cluster: multiple endpoints security headers")

	ts, server := s.newRealClusterServer()
	defer ts.Close()
	defer server.Close()

	type endpointResult struct {
		name    string
		headers map[string]string
	}

	expectedHeaders := cases.SecurityHeaderCases()
	results := make([]endpointResult, 0, 4)

	// 1. GET /healthz (no auth, 200)
	req1, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/healthz", nil)
	require.NoError(s.T(), err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(s.T(), err)
	defer resp1.Body.Close()
	assert.Equal(s.T(), http.StatusOK, resp1.StatusCode)
	h1 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h1[tc.Header] = resp1.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /healthz", headers: h1})

	// 2. GET /readyz (no auth, 200)
	req2, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/readyz", nil)
	require.NoError(s.T(), err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(s.T(), err)
	defer resp2.Body.Close()
	assert.Equal(s.T(), http.StatusOK, resp2.StatusCode)
	h2 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h2[tc.Header] = resp2.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /readyz", headers: h2})

	// 3. GET /api/v1alpha1/clusters (authenticated, 200)
	req3, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req3.SetBasicAuth("admin", "admin-secret")
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(s.T(), err)
	defer resp3.Body.Close()
	assert.Equal(s.T(), http.StatusOK, resp3.StatusCode)
	h3 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h3[tc.Header] = resp3.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /api/v1alpha1/clusters", headers: h3})

	// 4. GET /api/v1alpha1/clusters (no auth, 401)
	req4, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	resp4, err := http.DefaultClient.Do(req4)
	require.NoError(s.T(), err)
	defer resp4.Body.Close()
	assert.Equal(s.T(), http.StatusUnauthorized, resp4.StatusCode)
	h4 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h4[tc.Header] = resp4.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /api/v1alpha1/clusters (401)", headers: h4})

	// Compare all results: every endpoint should have the same header values.
	baseline := results[0]
	for i := 1; i < len(results); i++ {
		for _, tc := range expectedHeaders {
			assert.Equal(s.T(), baseline.headers[tc.Header], results[i].headers[tc.Header],
				"header %s should be consistent between %s and %s",
				tc.Header, baseline.name, results[i].name)
		}
	}

	// Also verify all headers match expected values.
	for _, result := range results {
		for _, tc := range expectedHeaders {
			assert.Equal(s.T(), tc.ExpectedValue, result.headers[tc.Header],
				"header %s on %s should be %q", tc.Header, result.name, tc.ExpectedValue)
		}
	}

	s.logger.Info("scenario 51 real cluster: multiple endpoints security headers completed")
}
