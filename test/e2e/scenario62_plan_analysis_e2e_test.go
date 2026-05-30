//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/planchecker"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 62: Plan Analysis API (E2E)
// ============================================================================
//
// This E2E scenario tests the full user journeys for the Plan Analysis API:
//   1. User submits plans with various issues and verifies detection and
//      recommendations, then submits a clean plan and verifies no issues.
//   2. Unauthenticated user is rejected, authenticated user can access the
//      endpoint.
//   3. User handles error conditions: empty plan, malformed JSON, non-existent
//      cluster.
//
// The plan-check endpoint is pure text analysis — no DB connection required.
//
// ============================================================================

const (
	scenario62E2ECluster = "e2e-plan-cluster"
	scenario62E2EUser    = "admin"
	scenario62E2EPass    = "admin-pass"
	scenario62E2EPrefix  = "/api/v1alpha1"
)

// e2ePlanCheckPath returns the path for E2E plan-check API calls.
func e2ePlanCheckPath() string {
	return scenario62E2EPrefix + "/clusters/" + scenario62E2ECluster + "/queries/plan-check?namespace=default"
}

// Scenario62PlanAnalysisE2ESuite tests the Plan Analysis API user journeys.
type Scenario62PlanAnalysisE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario62(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario62PlanAnalysisE2ESuite))
}

func (s *Scenario62PlanAnalysisE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 60*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario62E2ECluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Plan-check endpoint does not need DB, but the server requires a factory.
	mockClient := &testutil.MockDBClient{}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario62E2EUser, scenario62E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario62PlanAnalysisE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario62PlanAnalysisE2ESuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario62E2EUser, scenario62E2EPass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodePlanCheckResult decodes the response body into a PlanCheckResult.
func (s *Scenario62PlanAnalysisE2ESuite) decodePlanCheckResult(rec *httptest.ResponseRecorder) *planchecker.PlanCheckResult {
	var result planchecker.PlanCheckResult
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&result))
	return &result
}

// decodeJSON decodes the response body into a map.
func (s *Scenario62PlanAnalysisE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// postPlanCheck sends an authenticated plan-check request with the given plan text.
func (s *Scenario62PlanAnalysisE2ESuite) postPlanCheck(planText string) *httptest.ResponseRecorder {
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: planText})
	require.NoError(s.T(), err)
	return s.doRequest(http.MethodPost, e2ePlanCheckPath(), body)
}

// e2eIssueCategories extracts the set of categories from a PlanCheckResult.
func e2eIssueCategories(result *planchecker.PlanCheckResult) map[string]bool {
	cats := make(map[string]bool)
	for _, issue := range result.Issues {
		cats[issue.Category] = true
	}
	return cats
}

// ============================================================================
// E2E Journey: Plan Check (62)
// ============================================================================

// TestE2E_Scenario62_PlanCheckJourney tests the complete user journey for
// plan analysis: submit plans with various issues, verify detection and
// recommendations, then submit a clean plan.
func (s *Scenario62PlanAnalysisE2ESuite) TestE2E_Scenario62_PlanCheckJourney() {
	// Step 1: User submits plan with sequential scan issue.
	s.T().Log("Step 1: Submit plan with sequential scan issue")
	rec := s.postPlanCheck(cases.SamplePlanText("seq_scan"))
	require.Equal(s.T(), http.StatusOK, rec.Code, "seq scan plan check should succeed")

	result := s.decodePlanCheckResult(rec)
	cats := e2eIssueCategories(result)
	assert.True(s.T(), cats["sequential_scan"],
		"should detect sequential_scan issue")

	// Verify recommendation mentions index creation.
	for _, issue := range result.Issues {
		if issue.Category == "sequential_scan" {
			assert.Contains(s.T(), issue.Recommendation, "index",
				"recommendation should mention index creation")
			break
		}
	}

	// Step 2: User submits plan with row estimate mismatch.
	s.T().Log("Step 2: Submit plan with row estimate mismatch")
	rec = s.postPlanCheck(cases.SamplePlanText("row_mismatch"))
	require.Equal(s.T(), http.StatusOK, rec.Code, "row mismatch plan check should succeed")

	result = s.decodePlanCheckResult(rec)
	cats = e2eIssueCategories(result)
	assert.True(s.T(), cats["row_estimate_mismatch"],
		"should detect row_estimate_mismatch issue")

	// Verify recommendation mentions ANALYZE.
	for _, issue := range result.Issues {
		if issue.Category == "row_estimate_mismatch" {
			assert.Contains(s.T(), issue.Recommendation, "ANALYZE",
				"recommendation should mention ANALYZE")
			break
		}
	}

	// Step 3: User submits plan with sort spill to disk.
	s.T().Log("Step 3: Submit plan with sort spill to disk")
	rec = s.postPlanCheck(cases.SamplePlanText("sort_spill"))
	require.Equal(s.T(), http.StatusOK, rec.Code, "sort spill plan check should succeed")

	result = s.decodePlanCheckResult(rec)
	cats = e2eIssueCategories(result)
	assert.True(s.T(), cats["sort_spill"],
		"should detect sort_spill issue")

	// Verify recommendation mentions work_mem.
	for _, issue := range result.Issues {
		if issue.Category == "sort_spill" {
			assert.Contains(s.T(), issue.Recommendation, "work_mem",
				"recommendation should mention work_mem")
			break
		}
	}

	// Step 4: User submits comprehensive plan with all issues.
	s.T().Log("Step 4: Submit comprehensive plan with all issues")
	rec = s.postPlanCheck(cases.SamplePlanText("full"))
	require.Equal(s.T(), http.StatusOK, rec.Code, "full plan check should succeed")

	result = s.decodePlanCheckResult(rec)
	cats = e2eIssueCategories(result)
	assert.GreaterOrEqual(s.T(), len(result.Issues), 3,
		"full plan should have at least 3 issues")
	assert.True(s.T(), cats["sequential_scan"],
		"full plan should detect sequential_scan")
	assert.True(s.T(), cats["row_estimate_mismatch"],
		"full plan should detect row_estimate_mismatch")
	assert.True(s.T(), cats["sort_spill"],
		"full plan should detect sort_spill")

	// Verify summary counts issues correctly.
	assert.NotEmpty(s.T(), result.Summary,
		"summary should not be empty")
	assert.Contains(s.T(), result.Summary, "Found",
		"summary should contain 'Found'")
	assert.Greater(s.T(), result.TotalNodes, 0,
		"totalNodes should be > 0")
	assert.Greater(s.T(), result.ExecutionTime, float64(0),
		"executionTime should be extracted from plan footer")

	// Step 5: User submits clean plan (no issues).
	s.T().Log("Step 5: Submit clean plan with no issues")
	rec = s.postPlanCheck(cases.SamplePlanText("clean"))
	require.Equal(s.T(), http.StatusOK, rec.Code, "clean plan check should succeed")

	result = s.decodePlanCheckResult(rec)
	assert.Empty(s.T(), result.Issues,
		"clean plan should have no issues")
	assert.Contains(s.T(), result.Summary, "No performance issues",
		"summary should indicate no issues found")
}

// ============================================================================
// E2E Journey: Authentication (62)
// ============================================================================

// TestE2E_Scenario62_AuthenticationJourney verifies that unauthenticated users
// are rejected and authenticated users can access the plan-check endpoint.
func (s *Scenario62PlanAnalysisE2ESuite) TestE2E_Scenario62_AuthenticationJourney() {
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	// Step 1: Unauthenticated POST returns 401.
	s.T().Log("Step 1: Unauthenticated POST returns 401")
	req := httptest.NewRequest(http.MethodPost, e2ePlanCheckPath(), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated plan-check should return 401")

	// Step 2: Authenticated POST succeeds with 200.
	s.T().Log("Step 2: Authenticated POST succeeds with 200")
	rec = s.postPlanCheck(cases.SamplePlanText("clean"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"authenticated plan-check should return 200")
}

// ============================================================================
// E2E Journey: Error Handling (62)
// ============================================================================

// TestE2E_Scenario62_ErrorHandlingJourney verifies that the API handles various
// error conditions gracefully: empty plan, malformed JSON, non-existent cluster.
func (s *Scenario62PlanAnalysisE2ESuite) TestE2E_Scenario62_ErrorHandlingJourney() {
	// Step 1: Empty plan text returns 400.
	s.T().Log("Step 1: Empty plan text returns 400")
	emptyBody, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: ""})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, e2ePlanCheckPath(), emptyBody)
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"empty plan text should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")

	// Step 2: Malformed JSON body returns 400.
	s.T().Log("Step 2: Malformed JSON body returns 400")
	rec = s.doRequest(http.MethodPost, e2ePlanCheckPath(), []byte(`{not valid json`))
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"malformed JSON should return 400")

	resp = s.decodeJSON(rec)
	errObj, ok = resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST for malformed JSON")

	// Step 3: Non-existent cluster returns 404.
	s.T().Log("Step 3: Non-existent cluster returns 404")
	nonExistentPath := scenario62E2EPrefix + "/clusters/nonexistent-cluster/queries/plan-check?namespace=default"
	validBody, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, nonExistentPath, validBody)
	require.Equal(s.T(), http.StatusNotFound, rec.Code,
		"non-existent cluster should return 404")

	resp = s.decodeJSON(rec)
	errObj, ok = resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "CLUSTER_NOT_FOUND", errObj["code"],
		"error code should be CLUSTER_NOT_FOUND")
}
