//go:build functional

package functional

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 51: Security Headers
// ============================================================================
//
// This scenario verifies that ALL 8 security headers are present with exact
// values on every API response, regardless of endpoint, HTTP method, or
// response status code. The SecurityHeaders middleware is applied as the
// outermost middleware wrapping the entire mux in server.Handler().
// ============================================================================

// Scenario51SecurityHeadersSuite tests Scenario 51: Security Headers.
type Scenario51SecurityHeadersSuite struct {
	suite.Suite
}

func TestFunctional_Scenario51(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario51SecurityHeadersSuite))
}

// setupServer creates a test server with basic auth middleware and returns
// the httptest.Server and a cleanup function.
func (s *Scenario51SecurityHeadersSuite) setupServer() (*httptest.Server, func()) {
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
func (s *Scenario51SecurityHeadersSuite) assertSecurityHeaders(resp *http.Response) {
	s.T().Helper()
	for _, tc := range cases.SecurityHeaderCases() {
		actual := resp.Header.Get(tc.Header)
		assert.Equal(s.T(), tc.ExpectedValue, actual,
			"header %s should be %q, got %q", tc.Header, tc.ExpectedValue, actual)
	}
}

// TestFunctional_Scenario51_AllHeaders_HealthEndpoint verifies that all 8
// security headers are present on GET /healthz (no auth required).
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_HealthEndpoint() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_AuthenticatedGET verifies that all 8
// security headers are present on GET /api/v1alpha1/clusters with admin auth.
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_AuthenticatedGET() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_AuthenticatedPOST verifies that all 8
// security headers are present on POST /api/v1alpha1/clusters with admin auth
// and a valid cluster body.
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_AuthenticatedPOST() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	body := `{"metadata":{"name":"test-s51","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Status may be 201 Created or other; we only care about headers.
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_UnauthorizedResponse verifies that all 8
// security headers are present on a 401 Unauthorized response (no auth header).
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_UnauthorizedResponse() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	resp, err := http.Get(ts.URL + "/api/v1alpha1/clusters")
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_ForbiddenResponse verifies that all 8
// security headers are present on a 403 Forbidden response (viewer tries POST).
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_ForbiddenResponse() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	body := `{"metadata":{"name":"test-s51-forbidden","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("viewer", "viewer-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_NotFoundResponse verifies that all 8
// security headers are present on a 404 Not Found response.
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_NotFoundResponse() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1alpha1/nonexistent", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusNotFound, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_AllHeaders_ReadyzEndpoint verifies that all 8
// security headers are present on GET /readyz (no auth required).
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_AllHeaders_ReadyzEndpoint() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	resp, err := http.Get(ts.URL + "/readyz")
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
	s.assertSecurityHeaders(resp)
}

// TestFunctional_Scenario51_SecurityHeaderCases_Coverage verifies that
// cases.SecurityHeaderCases() returns exactly 8 test cases.
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_SecurityHeaderCases_Coverage() {
	headerCases := cases.SecurityHeaderCases()
	require.Len(s.T(), headerCases, 8, "should have exactly 8 security header cases")

	// Verify each case has non-empty fields.
	for _, tc := range headerCases {
		assert.NotEmpty(s.T(), tc.Name, "case name should not be empty")
		assert.NotEmpty(s.T(), tc.Header, "header name should not be empty")
		assert.NotEmpty(s.T(), tc.ExpectedValue, "expected value should not be empty")
		assert.NotEmpty(s.T(), tc.Description, "description should not be empty")
	}
}

// TestFunctional_Scenario51_HeadersConsistentAcrossEndpoints verifies that the
// SAME set of security headers appears on ALL responses regardless of endpoint.
func (s *Scenario51SecurityHeadersSuite) TestFunctional_Scenario51_HeadersConsistentAcrossEndpoints() {
	ts, cleanup := s.setupServer()
	defer cleanup()

	// Collect header values from multiple endpoints.
	type endpointResult struct {
		name    string
		headers map[string]string
	}

	expectedHeaders := cases.SecurityHeaderCases()
	results := make([]endpointResult, 0, 4)

	// 1. GET /healthz (no auth, 200)
	resp1, err := http.Get(ts.URL + "/healthz")
	require.NoError(s.T(), err)
	defer resp1.Body.Close()
	h1 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h1[tc.Header] = resp1.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /healthz", headers: h1})

	// 2. GET /readyz (no auth, 200)
	resp2, err := http.Get(ts.URL + "/readyz")
	require.NoError(s.T(), err)
	defer resp2.Body.Close()
	h2 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h2[tc.Header] = resp2.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "GET /readyz", headers: h2})

	// 3. GET /api/v1alpha1/clusters (authenticated, 200)
	req3, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
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

	// 4. POST /api/v1alpha1/clusters with bad body (authenticated, error response)
	req4, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader("invalid-json"))
	require.NoError(s.T(), err)
	req4.Header.Set("Content-Type", "application/json")
	req4.SetBasicAuth("admin", "admin-secret")
	resp4, err := http.DefaultClient.Do(req4)
	require.NoError(s.T(), err)
	defer resp4.Body.Close()
	h4 := make(map[string]string)
	for _, tc := range expectedHeaders {
		h4[tc.Header] = resp4.Header.Get(tc.Header)
	}
	results = append(results, endpointResult{name: "POST /api/v1alpha1/clusters (error)", headers: h4})

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
}
