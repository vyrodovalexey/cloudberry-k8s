//go:build functional

package functional

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

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
// Scenario 62: Plan Analysis API
// ============================================================================
//
// This scenario verifies the Plan Analysis API endpoint:
//   - 62a: Sequential scan detection
//   - 62b: Row estimate mismatch detection
//   - 62c: Sort spill to disk detection
//   - 62d: All issues combined
//   - 62e: Clean plan with no issues
//   - 62f: Empty plan text (error case)
//   - Authentication: unauthenticated requests rejected
//   - Recommendations: actionable recommendations in output
//
// The plan-check endpoint is pure text analysis — no DB connection required.
//
// ============================================================================

const (
	scenario62APIPrefix = "/api/v1alpha1"
	scenario62Cluster   = "test-cluster"
	scenario62User      = "admin"
	scenario62Pass      = "admin-pass"
)

// planCheckBasePath returns the base path for plan-check API calls.
func planCheckBasePath() string {
	return scenario62APIPrefix + "/clusters/" + scenario62Cluster + "/queries/plan-check?namespace=default"
}

// Scenario62PlanAnalysisSuite tests the Plan Analysis API.
type Scenario62PlanAnalysisSuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario62(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario62PlanAnalysisSuite))
}

func (s *Scenario62PlanAnalysisSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario62Cluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Plan-check endpoint does not need DB, but the server requires a factory.
	mockClient := &testutil.MockDBClient{}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario62User, scenario62Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario62PlanAnalysisSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an HTTP request with basic auth credentials.
func (s *Scenario62PlanAnalysisSuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario62User, scenario62Pass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth credentials.
func (s *Scenario62PlanAnalysisSuite) doRequestNoAuth(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodePlanCheckResult decodes the response body into a PlanCheckResult.
func (s *Scenario62PlanAnalysisSuite) decodePlanCheckResult(rec *httptest.ResponseRecorder) *planchecker.PlanCheckResult {
	var result planchecker.PlanCheckResult
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&result))
	return &result
}

// decodeJSON decodes the response body into a map.
func (s *Scenario62PlanAnalysisSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// postPlanCheck sends a plan-check request with the given plan text.
func (s *Scenario62PlanAnalysisSuite) postPlanCheck(planText string) *httptest.ResponseRecorder {
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: planText})
	require.NoError(s.T(), err)
	return s.doRequest(http.MethodPost, planCheckBasePath(), body)
}

// issueCategories extracts the set of categories from a PlanCheckResult.
func issueCategories(result *planchecker.PlanCheckResult) map[string]bool {
	cats := make(map[string]bool)
	for _, issue := range result.Issues {
		cats[issue.Category] = true
	}
	return cats
}

// ============================================================================
// 62a: Sequential Scan Detection
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_SeqScan verifies that a plan with a
// sequential scan on a large table is flagged with a sequential_scan issue.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_SeqScan() {
	rec := s.postPlanCheck(cases.SamplePlanText("seq_scan"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST plan-check with seq scan plan should return 200 OK")

	result := s.decodePlanCheckResult(rec)
	cats := issueCategories(result)

	assert.True(s.T(), cats["sequential_scan"],
		"should detect sequential_scan issue")
	assert.GreaterOrEqual(s.T(), len(result.Issues), 1,
		"should have at least 1 issue")

	// Verify the sequential scan issue has a recommendation mentioning index.
	for _, issue := range result.Issues {
		if issue.Category == "sequential_scan" {
			assert.Contains(s.T(), issue.Recommendation, "index",
				"sequential_scan recommendation should mention index creation")
			assert.Equal(s.T(), "warning", issue.Severity,
				"sequential_scan should be a warning")
			assert.NotEmpty(s.T(), issue.Relation,
				"sequential_scan issue should have a relation name")
		}
	}
}

// ============================================================================
// 62b: Row Estimate Mismatch Detection
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_RowMismatch verifies that a plan with a
// row estimate mismatch is flagged with a row_estimate_mismatch issue.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_RowMismatch() {
	rec := s.postPlanCheck(cases.SamplePlanText("row_mismatch"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST plan-check with row mismatch plan should return 200 OK")

	result := s.decodePlanCheckResult(rec)
	cats := issueCategories(result)

	assert.True(s.T(), cats["row_estimate_mismatch"],
		"should detect row_estimate_mismatch issue")

	// Verify the mismatch issue has a recommendation mentioning ANALYZE.
	for _, issue := range result.Issues {
		if issue.Category == "row_estimate_mismatch" {
			assert.Contains(s.T(), issue.Recommendation, "ANALYZE",
				"row_estimate_mismatch recommendation should mention ANALYZE")
			assert.Equal(s.T(), "warning", issue.Severity,
				"row_estimate_mismatch should be a warning")
		}
	}
}

// ============================================================================
// 62c: Sort Spill to Disk Detection
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_SortSpill verifies that a plan with a
// sort spill to disk is flagged with a sort_spill issue.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_SortSpill() {
	rec := s.postPlanCheck(cases.SamplePlanText("sort_spill"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST plan-check with sort spill plan should return 200 OK")

	result := s.decodePlanCheckResult(rec)
	cats := issueCategories(result)

	assert.True(s.T(), cats["sort_spill"],
		"should detect sort_spill issue")

	// Verify the sort spill issue has a recommendation mentioning work_mem.
	for _, issue := range result.Issues {
		if issue.Category == "sort_spill" {
			assert.Contains(s.T(), issue.Recommendation, "work_mem",
				"sort_spill recommendation should mention work_mem")
			assert.Equal(s.T(), "warning", issue.Severity,
				"sort_spill should be a warning")
		}
	}
}

// ============================================================================
// 62d: All Issues Combined
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_AllIssues verifies that a comprehensive
// plan with all 3 issue types flags all of them.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_AllIssues() {
	rec := s.postPlanCheck(cases.SamplePlanText("full"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST plan-check with full plan should return 200 OK")

	result := s.decodePlanCheckResult(rec)
	cats := issueCategories(result)

	assert.GreaterOrEqual(s.T(), len(result.Issues), 3,
		"full plan should have at least 3 issues")
	assert.True(s.T(), cats["sequential_scan"],
		"full plan should detect sequential_scan")
	assert.True(s.T(), cats["row_estimate_mismatch"],
		"full plan should detect row_estimate_mismatch")
	assert.True(s.T(), cats["sort_spill"],
		"full plan should detect sort_spill")

	// Verify summary and metadata.
	assert.NotEmpty(s.T(), result.Summary,
		"summary should not be empty")
	assert.Greater(s.T(), result.TotalNodes, 0,
		"totalNodes should be > 0")
	assert.Greater(s.T(), result.ExecutionTime, float64(0),
		"executionTime should be extracted from plan footer")

	// Verify each issue has non-empty description and recommendation.
	for i, issue := range result.Issues {
		assert.NotEmpty(s.T(), issue.Description,
			"issue %d should have a description", i)
		assert.NotEmpty(s.T(), issue.Recommendation,
			"issue %d should have a recommendation", i)
		assert.NotEmpty(s.T(), issue.Severity,
			"issue %d should have a severity", i)
		assert.NotEmpty(s.T(), issue.Category,
			"issue %d should have a category", i)
		assert.NotEmpty(s.T(), issue.NodeType,
			"issue %d should have a nodeType", i)
	}
}

// ============================================================================
// 62e: Clean Plan — No Issues
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_NoIssues verifies that an optimized plan
// with no performance issues returns an empty issues array.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_NoIssues() {
	rec := s.postPlanCheck(cases.SamplePlanText("clean"))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST plan-check with clean plan should return 200 OK")

	result := s.decodePlanCheckResult(rec)

	assert.Empty(s.T(), result.Issues,
		"clean plan should have no issues")
	assert.Contains(s.T(), result.Summary, "No performance issues",
		"summary should indicate no issues found")
	assert.Greater(s.T(), result.TotalNodes, 0,
		"totalNodes should be > 0 even for clean plan")
}

// ============================================================================
// 62f: Empty Plan Text — Error
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_EmptyPlan verifies that submitting an
// empty plan text returns 400 Bad Request.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_EmptyPlan() {
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: ""})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, planCheckBasePath(), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"POST plan-check with empty planText should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}

// TestFunctional_Scenario62_PlanCheck_MissingPlanText verifies that submitting
// a request without planText field returns 400 Bad Request.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_MissingPlanText() {
	body, err := json.Marshal(map[string]string{})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, planCheckBasePath(), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"POST plan-check with missing planText should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}

// ============================================================================
// Cross-cutting: Authentication
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_Unauthenticated verifies that
// unauthenticated requests to the plan-check endpoint are rejected with 401.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_Unauthenticated() {
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec := s.doRequestNoAuth(http.MethodPost, planCheckBasePath(), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated POST plan-check should return 401")
}

// ============================================================================
// Recommendations Verification
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_Recommendations verifies that each
// detected issue contains actionable recommendations with specific details.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_Recommendations() {
	rec := s.postPlanCheck(cases.SamplePlanText("full"))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	result := s.decodePlanCheckResult(rec)
	require.GreaterOrEqual(s.T(), len(result.Issues), 3,
		"full plan should have at least 3 issues for recommendation verification")

	// Track which recommendation types we found.
	foundIndexRec := false
	foundAnalyzeRec := false
	foundWorkMemRec := false

	for _, issue := range result.Issues {
		switch issue.Category {
		case "sequential_scan":
			if assert.Contains(s.T(), issue.Recommendation, "index",
				"sequential_scan recommendation should mention index") {
				foundIndexRec = true
			}
		case "row_estimate_mismatch":
			if assert.Contains(s.T(), issue.Recommendation, "ANALYZE",
				"row_estimate_mismatch recommendation should mention ANALYZE") {
				foundAnalyzeRec = true
			}
		case "sort_spill":
			if assert.Contains(s.T(), issue.Recommendation, "work_mem",
				"sort_spill recommendation should mention work_mem") {
				foundWorkMemRec = true
			}
		}
	}

	assert.True(s.T(), foundIndexRec,
		"should have found index recommendation for sequential scan")
	assert.True(s.T(), foundAnalyzeRec,
		"should have found ANALYZE recommendation for row estimate mismatch")
	assert.True(s.T(), foundWorkMemRec,
		"should have found work_mem recommendation for sort spill")
}

// ============================================================================
// Non-existent Cluster
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_ClusterNotFound verifies that a plan-check
// request for a non-existent cluster returns 404.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_ClusterNotFound() {
	nonExistentPath := scenario62APIPrefix + "/clusters/nonexistent-cluster/queries/plan-check?namespace=default"
	body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, nonExistentPath, body)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code,
		"plan-check for non-existent cluster should return 404")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "CLUSTER_NOT_FOUND", errObj["code"],
		"error code should be CLUSTER_NOT_FOUND")
}

// ============================================================================
// Response Structure Validation
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_ResponseStructure verifies that the
// response has the correct JSON structure with all expected fields.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_ResponseStructure() {
	rec := s.postPlanCheck(cases.SamplePlanText("full"))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)

	// Verify top-level fields.
	assert.NotNil(s.T(), resp["issues"], "response should have 'issues' array")
	assert.NotNil(s.T(), resp["summary"], "response should have 'summary' string")
	assert.NotNil(s.T(), resp["totalNodes"], "response should have 'totalNodes' integer")

	// Verify issues array structure.
	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok, "issues should be an array")
	require.GreaterOrEqual(s.T(), len(issues), 1, "should have at least 1 issue")

	// Verify each issue has required fields.
	for i, item := range issues {
		issue, ok := item.(map[string]interface{})
		require.True(s.T(), ok, "each issue should be an object")

		assert.NotNil(s.T(), issue["severity"],
			"issue %d should have 'severity'", i)
		assert.NotNil(s.T(), issue["category"],
			"issue %d should have 'category'", i)
		assert.NotNil(s.T(), issue["nodeType"],
			"issue %d should have 'nodeType'", i)
		assert.NotNil(s.T(), issue["description"],
			"issue %d should have 'description'", i)
		assert.NotNil(s.T(), issue["recommendation"],
			"issue %d should have 'recommendation'", i)
		assert.NotNil(s.T(), issue["details"],
			"issue %d should have 'details'", i)
	}
}

// ============================================================================
// Data-Driven Tests from Test Cases Catalog
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_DataDriven runs all plan check test cases
// from the test cases catalog.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_DataDriven() {
	for _, tc := range cases.PlanCheckCases() {
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)

			if tc.ExpectError {
				body, err := json.Marshal(planchecker.PlanCheckRequest{PlanText: tc.PlanText})
				require.NoError(s.T(), err)

				rec := s.doRequest(http.MethodPost, planCheckBasePath(), body)
				assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
					"error case should return 400")
				return
			}

			rec := s.postPlanCheck(tc.PlanText)
			require.Equal(s.T(), http.StatusOK, rec.Code,
				"valid plan should return 200")

			result := s.decodePlanCheckResult(rec)

			// Verify minimum issue count.
			if tc.ExpectedIssueCount >= 0 {
				assert.GreaterOrEqual(s.T(), len(result.Issues), tc.ExpectedIssueCount,
					"should have at least %d issues", tc.ExpectedIssueCount)
			}

			// Verify expected categories are present.
			if len(tc.ExpectedCategories) > 0 {
				cats := issueCategories(result)
				for _, expectedCat := range tc.ExpectedCategories {
					assert.True(s.T(), cats[expectedCat],
						"should detect category %q", expectedCat)
				}
			}
		})
	}
}

// ============================================================================
// Invalid JSON Body
// ============================================================================

// TestFunctional_Scenario62_PlanCheck_InvalidJSON verifies that a malformed
// JSON body returns 400 Bad Request.
func (s *Scenario62PlanAnalysisSuite) TestFunctional_Scenario62_PlanCheck_InvalidJSON() {
	rec := s.doRequest(http.MethodPost, planCheckBasePath(), []byte(`{invalid json`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"malformed JSON should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}
