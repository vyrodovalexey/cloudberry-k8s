//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 63: All REST API Endpoints (E2E)
// ============================================================================
//
// This E2E scenario tests realistic user journeys across all 13 REST API
// endpoints from the Query Monitoring & Session Management specification:
//
//   Journey 1: Query Monitoring (63a → 63b → 63c → 63d → 63e)
//   Journey 2: Query History (63f → 63g → 63h)
//   Journey 3: Plan Analysis (63i)
//   Journey 4: Exporter Health (63j)
//   Journey 5: Session Management (63k → 63l → 63m)
//   Journey 6: Authentication Boundary (all endpoints → 401)
//   Journey 7: Error Handling (invalid PIDs, missing bodies, etc.)
//
// ============================================================================

const (
	scenario63E2ECluster   = "e2e-api-cluster"
	scenario63E2ENamespace = "default"
	scenario63E2EUser      = "admin"
	scenario63E2EPass      = "admin-pass"
	scenario63E2EPrefix    = "/api/v1alpha1"
	scenario63E2ERateLimit = 1000
)

// e2eClusterPath returns the base path for E2E cluster API calls.
func e2eClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario63E2EPrefix, scenario63E2ECluster, endpoint, scenario63E2ENamespace)
}

// Scenario63AllRESTAPIEndpointsE2ESuite tests all API endpoints via user journeys.
type Scenario63AllRESTAPIEndpointsE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario63(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario63AllRESTAPIEndpointsE2ESuite))
}

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 120*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario63E2ECluster, scenario63E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()

	cluster.Status.ActiveQueries = 10
	cluster.Status.QueuedQueries = 3
	cluster.Status.BlockedQueries = 1

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Track cancel calls for journey verification.
	cancelledPIDs := make(map[int32]bool)

	mockClient := &testutil.MockDBClient{
		CancelQueryFunc: func(_ context.Context, pid int32) (bool, error) {
			cancelledPIDs[pid] = true
			return true, nil
		},
		TerminateSessionFunc: func(_ context.Context, pid int32) (bool, error) {
			return true, nil
		},
		MoveQueryToResourceGroupFunc: func(_ context.Context, pid int32, targetGroup string) error {
			return nil
		},
		GetQueryDetailFunc: func(_ context.Context, pid int32) (*db.QueryDetail, error) {
			if pid == 9999 {
				return nil, fmt.Errorf("query with PID %d not found", pid)
			}
			return &db.QueryDetail{
				PID:      pid,
				Username: "gpadmin",
				Database: "testdb",
				State:    "active",
				Query:    "SELECT * FROM orders WHERE region = 'US'",
				Duration: "00:00:02.5",
				Locks: []db.LockInfo{
					{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "orders"},
				},
				TablesAccessed: []string{"public.orders"},
			}, nil
		},
		GetQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			entries := []db.QueryHistoryEntry{
				{
					QueryID:       "q-e2e-001",
					QueryText:     "SELECT * FROM orders WHERE region = 'US'",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    2500.0,
					State:         "completed",
					ResourceGroup: "analytics",
					QueryStart:    time.Now().Add(-2 * time.Hour),
					CPUTimeMs:     1200.0,
					MemoryBytes:   52428800,
					SpillBytes:    0,
				},
				{
					QueryID:       "q-e2e-002",
					QueryText:     "INSERT INTO audit_log VALUES (1, 'test')",
					Username:      "etl_user",
					DatabaseName:  "warehouse",
					DurationMs:    150.0,
					State:         "completed",
					ResourceGroup: "etl",
					QueryStart:    time.Now().Add(-1 * time.Hour),
					CPUTimeMs:     50.0,
					MemoryBytes:   1048576,
					SpillBytes:    0,
				},
				{
					QueryID:       "q-e2e-003",
					QueryText:     "SELECT count(*) FROM large_table",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    8500.0,
					State:         "completed",
					ResourceGroup: "analytics",
					QueryStart:    time.Now().Add(-30 * time.Minute),
					CPUTimeMs:     5000.0,
					MemoryBytes:   104857600,
					SpillBytes:    8388608,
				},
			}

			// Apply basic filtering for journey tests.
			if filter.Username != "" {
				var filtered []db.QueryHistoryEntry
				for _, e := range entries {
					if e.Username == filter.Username {
						filtered = append(filtered, e)
					}
				}
				return filtered, len(filtered), nil
			}

			return entries, len(entries), nil
		},
		GetQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			if queryID == "q-e2e-001" {
				return &db.QueryHistoryEntry{
					QueryID:       "q-e2e-001",
					QueryText:     "SELECT * FROM orders WHERE region = 'US'",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    2500.0,
					State:         "completed",
					ResourceGroup: "analytics",
					ExplainPlan:   "Seq Scan on orders (cost=0.00..5000.00 rows=100000 width=100)",
					QueryStart:    time.Now().Add(-2 * time.Hour),
					CPUTimeMs:     1200.0,
					MemoryBytes:   52428800,
				}, nil
			}
			return nil, fmt.Errorf("query %s not found", queryID)
		},
		ExportQueryHistoryCSVFunc: func(_ context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id", "query", "username", "database", "duration_ms", "state", "resource_group"})
			_ = csvWriter.Write([]string{"q-e2e-001", "SELECT * FROM orders", "analyst", "mydb", "2500.0", "completed", "analytics"})
			_ = csvWriter.Write([]string{"q-e2e-002", "INSERT INTO audit_log", "etl_user", "warehouse", "150.0", "completed", "etl"})
			csvWriter.Flush()
			return nil
		},
		ListSessionsWithResourceGroupFunc: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:           1001,
						Username:      "gpadmin",
						Database:      "testdb",
						State:         "active",
						Query:         "SELECT * FROM orders",
						QueryStart:    time.Now(),
						WaitEventType: "",
					},
					ResourceGroup: "default_group",
				},
				{
					Session: db.Session{
						PID:           1002,
						Username:      "analyst",
						Database:      "mydb",
						State:         "active",
						Query:         "SELECT count(*) FROM large_table",
						QueryStart:    time.Now(),
						WaitEventType: "",
					},
					ResourceGroup: "analytics",
				},
				{
					Session: db.Session{
						PID:           1003,
						Username:      "etl_user",
						Database:      "warehouse",
						State:         "idle",
						Query:         "",
						QueryStart:    time.Now().Add(-10 * time.Minute),
						WaitEventType: "",
					},
					ResourceGroup: "etl",
				},
			}, nil
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario63E2EUser, scenario63E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario63E2ERateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario63AllRESTAPIEndpointsE2ESuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario63E2EUser, scenario63E2EPass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth.
func (s *Scenario63AllRESTAPIEndpointsE2ESuite) doRequestNoAuth(method, path string, body []byte) *httptest.ResponseRecorder {
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

// decodeJSON decodes the response body into a map.
func (s *Scenario63AllRESTAPIEndpointsE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// ============================================================================
// Journey 1: Query Monitoring (63a → 63b → 63c → 63d → 63e)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_QueryMonitoringJourney() {
	// Step 1: List all queries — verify monitoring overview.
	s.T().Log("Step 1: GET /queries — monitoring overview")
	rec := s.doRequest(http.MethodGet, e2eClusterPath("/queries"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	activeCount := resp["activeQueries"].(float64)
	assert.Equal(s.T(), float64(10), activeCount, "should have 10 active queries")
	assert.Equal(s.T(), float64(3), resp["queuedQueries"])
	assert.Equal(s.T(), float64(1), resp["blockedQueries"])

	// Step 2: Get active query counts — verify numbers.
	s.T().Log("Step 2: GET /queries/active — active counts")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/active"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])
	assert.Equal(s.T(), float64(3), resp["queuedQueries"])
	assert.Equal(s.T(), float64(1), resp["blockedQueries"])

	// Step 3: Get detail for a specific query — verify locks and tables.
	s.T().Log("Step 3: GET /queries/1001 — query detail")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/1001"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1001), resp["pid"])
	assert.Equal(s.T(), "active", resp["state"])
	assert.NotNil(s.T(), resp["locks"], "should have locks")
	assert.NotNil(s.T(), resp["tablesAccessed"], "should have tablesAccessed")

	locks, ok := resp["locks"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(locks), 1, "should have at least 1 lock")

	tables, ok := resp["tablesAccessed"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(tables), 1, "should have at least 1 table")

	// Step 4: Cancel the query — verify canceled=true.
	s.T().Log("Step 4: POST /queries/1001/cancel — cancel query")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/1001/cancel"),
		[]byte(`{"reason":"e2e test cancellation"}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1001), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "e2e test cancellation", resp["reason"])

	// Step 5: Move a different query to a resource group — verify moved.
	s.T().Log("Step 5: POST /queries/1002/move — move query")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/1002/move"),
		[]byte(`{"targetGroup":"etl_group"}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1002), resp["pid"])
	assert.Equal(s.T(), "etl_group", resp["targetGroup"])
	assert.Equal(s.T(), "moved", resp["status"])
}

// ============================================================================
// Journey 2: Query History (63f → 63g → 63h)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_QueryHistoryJourney() {
	// Step 1: Search history — get results.
	s.T().Log("Step 1: GET /queries/history — browse history")
	rec := s.doRequest(http.MethodGet, e2eClusterPath("/queries/history"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(items), "should have 3 history entries")
	assert.Equal(s.T(), float64(3), resp["total"])

	// Step 2: Get detail for a specific historical query — verify plan.
	s.T().Log("Step 2: GET /queries/history/q-e2e-001 — history detail")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/history/q-e2e-001"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "q-e2e-001", resp["queryId"])
	assert.NotEmpty(s.T(), resp["queryText"], "should have query text")
	assert.NotEmpty(s.T(), resp["explainPlan"], "should have execution plan")
	assert.Equal(s.T(), "analyst", resp["username"])
	assert.Equal(s.T(), "mydb", resp["databaseName"])

	// Step 3: Export filtered history as CSV — verify format.
	s.T().Log("Step 3: POST /queries/history/export — export CSV")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/history/export"), []byte(`{}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
	csvBody := rec.Body.String()
	assert.Contains(s.T(), csvBody, "query_id", "CSV should have header")
	assert.Contains(s.T(), csvBody, "q-e2e-001", "CSV should contain first entry")
	assert.Contains(s.T(), csvBody, "q-e2e-002", "CSV should contain second entry")

	// Verify CSV is parseable.
	reader := csv.NewReader(bytes.NewBufferString(csvBody))
	records, err := reader.ReadAll()
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), len(records), 2, "CSV should have header + at least 1 data row")
}

// ============================================================================
// Journey 3: Plan Analysis (63i)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_PlanAnalysisJourney() {
	// Step 1: Submit plan with sequential scan — verify issues detected.
	s.T().Log("Step 1: Submit plan with sequential scan")
	body, err := json.Marshal(map[string]string{"planText": cases.SamplePlanText("seq_scan")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, e2eClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 1, "should detect at least 1 issue")

	// Verify sequential scan issue has recommendation.
	foundSeqScan := false
	for _, issue := range issues {
		issueMap, ok := issue.(map[string]interface{})
		require.True(s.T(), ok)
		if issueMap["category"] == "sequential_scan" {
			foundSeqScan = true
			assert.Contains(s.T(), issueMap["recommendation"].(string), "index",
				"recommendation should mention index")
		}
	}
	assert.True(s.T(), foundSeqScan, "should detect sequential_scan issue")

	// Step 2: Submit comprehensive plan — verify all issue types.
	s.T().Log("Step 2: Submit comprehensive plan with all issues")
	body, err = json.Marshal(map[string]string{"planText": cases.SamplePlanText("full")})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	issues, ok = resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 3, "full plan should have at least 3 issues")

	categories := make(map[string]bool)
	for _, issue := range issues {
		issueMap := issue.(map[string]interface{})
		categories[issueMap["category"].(string)] = true
	}
	assert.True(s.T(), categories["sequential_scan"], "should detect sequential_scan")
	assert.True(s.T(), categories["row_estimate_mismatch"], "should detect row_estimate_mismatch")
	assert.True(s.T(), categories["sort_spill"], "should detect sort_spill")

	// Step 3: Submit clean plan — verify no issues.
	s.T().Log("Step 3: Submit clean plan")
	body, err = json.Marshal(map[string]string{"planText": cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	issues, ok = resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), issues, "clean plan should have no issues")
	assert.Contains(s.T(), resp["summary"].(string), "No performance issues")
}

// ============================================================================
// Journey 4: Exporter Health (63j)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_ExporterHealthJourney() {
	// Step 1: Check exporter health — verify all exporters listed.
	s.T().Log("Step 1: GET /metrics/exporters — check exporter health")
	rec := s.doRequest(http.MethodGet, e2eClusterPath("/metrics/exporters"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	exporters, ok := resp["exporters"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(exporters), "should have 3 exporters")
	assert.Equal(s.T(), float64(3), resp["total"])

	// Verify exporter names and ports.
	exporterNames := make(map[string]float64)
	for _, exp := range exporters {
		expMap := exp.(map[string]interface{})
		name := expMap["name"].(string)
		port := expMap["port"].(float64)
		exporterNames[name] = port

		// All exporters should have status (up/down/unknown).
		status := expMap["status"].(string)
		assert.Contains(s.T(), []string{"up", "down", "unknown"}, status,
			"exporter %s status should be up/down/unknown", name)

		// Verify endpoint format.
		endpoint := expMap["endpoint"].(string)
		assert.Contains(s.T(), endpoint, "/metrics",
			"exporter %s endpoint should contain /metrics", name)
	}

	assert.Equal(s.T(), float64(9187), exporterNames["postgres-exporter"])
	assert.Equal(s.T(), float64(9188), exporterNames["cloudberry-query-exporter"])
	assert.Equal(s.T(), float64(9100), exporterNames["node-exporter"])
}

// ============================================================================
// Journey 5: Session Management (63k → 63l → 63m)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_SessionManagementJourney() {
	// Step 1: List sessions — verify session list.
	s.T().Log("Step 1: GET /sessions — list sessions")
	rec := s.doRequest(http.MethodGet, e2eClusterPath("/sessions"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(sessions), "should have 3 sessions")

	// Verify session structure.
	for _, sess := range sessions {
		sessMap := sess.(map[string]interface{})
		assert.NotNil(s.T(), sessMap["pid"], "session should have pid")
		assert.NotEmpty(s.T(), sessMap["username"], "session should have username")
		assert.NotEmpty(s.T(), sessMap["database"], "session should have database")
		assert.NotEmpty(s.T(), sessMap["state"], "session should have state")
	}

	// Step 2: Filter sessions by status.
	s.T().Log("Step 2: GET /sessions?status=running — filter active sessions")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/sessions")+"&status=running", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	activeSessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(activeSessions), "should have 2 active sessions")

	// Step 3: Cancel a session's query — verify canceled.
	s.T().Log("Step 3: POST /sessions/1001/cancel — cancel session query")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/sessions/1001/cancel"), []byte(`{}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1001), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])

	// Step 4: Terminate a session — verify terminated.
	s.T().Log("Step 4: DELETE /sessions/1003 — terminate session")
	rec = s.doRequest(http.MethodDelete, e2eClusterPath("/sessions/1003"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1003), resp["pid"])
	assert.Equal(s.T(), true, resp["terminated"])
}

// ============================================================================
// Journey 6: Authentication Boundary (all endpoints → 401)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_AuthenticationBoundary() {
	// All endpoints should return 401 without authentication.
	endpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodGet, "/queries", nil},
		{http.MethodGet, "/queries/active", nil},
		{http.MethodGet, "/queries/1001", nil},
		{http.MethodPost, "/queries/1001/cancel", []byte(`{}`)},
		{http.MethodPost, "/queries/1001/move", []byte(`{"targetGroup":"etl"}`)},
		{http.MethodGet, "/queries/history", nil},
		{http.MethodGet, "/queries/history/q-e2e-001", nil},
		{http.MethodPost, "/queries/history/export", []byte(`{}`)},
		{http.MethodPost, "/queries/plan-check", []byte(`{"planText":"test"}`)},
		{http.MethodGet, "/metrics/exporters", nil},
		{http.MethodGet, "/sessions", nil},
		{http.MethodPost, "/sessions/1001/cancel", []byte(`{}`)},
		{http.MethodDelete, "/sessions/1001", nil},
	}

	for _, ep := range endpoints {
		s.T().Logf("Verifying 401 for unauthenticated %s %s", ep.method, ep.path)
		rec := s.doRequestNoAuth(ep.method, e2eClusterPath(ep.path), ep.body)
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated %s %s should return 401", ep.method, ep.path)
	}
}

// ============================================================================
// Journey 7: Error Handling (invalid PIDs, missing bodies, etc.)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_ErrorHandlingJourney() {
	// Step 1: Invalid PIDs return 400.
	s.T().Log("Step 1: Invalid PIDs return 400")
	invalidPIDPaths := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodGet, "/queries/abc", nil},
		{http.MethodGet, "/queries/-1", nil},
		{http.MethodPost, "/queries/abc/cancel", []byte(`{}`)},
		{http.MethodPost, "/queries/-1/cancel", []byte(`{}`)},
		{http.MethodPost, "/queries/abc/move", []byte(`{"targetGroup":"etl"}`)},
		{http.MethodPost, "/queries/-1/move", []byte(`{"targetGroup":"etl"}`)},
		{http.MethodPost, "/sessions/abc/cancel", []byte(`{}`)},
		{http.MethodPost, "/sessions/-1/cancel", []byte(`{}`)},
		{http.MethodDelete, "/sessions/abc", nil},
		{http.MethodDelete, "/sessions/-1", nil},
	}

	for _, ep := range invalidPIDPaths {
		rec := s.doRequest(ep.method, e2eClusterPath(ep.path), ep.body)
		assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
			"%s %s should return 400 for invalid PID", ep.method, ep.path)
	}

	// Step 2: Missing request body for move returns 400.
	s.T().Log("Step 2: Missing body for move returns 400")
	rec := s.doRequest(http.MethodPost, e2eClusterPath("/queries/1001/move"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 3: Empty targetGroup for move returns 400.
	s.T().Log("Step 3: Empty targetGroup returns 400")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/1001/move"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 4: SQL injection in targetGroup returns 400.
	s.T().Log("Step 4: SQL injection in targetGroup returns 400")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/1001/move"),
		[]byte(`{"targetGroup":"DROP TABLE;--"}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 5: Non-existent query returns 404.
	s.T().Log("Step 5: Non-existent query PID returns 404")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/9999"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	// Step 6: Non-existent history query returns 404.
	s.T().Log("Step 6: Non-existent history query returns 404")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/history/q-nonexistent"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	// Step 7: Empty plan text returns 400.
	s.T().Log("Step 7: Empty plan text returns 400")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/plan-check"),
		[]byte(`{"planText":""}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 8: Invalid JSON body returns 400.
	s.T().Log("Step 8: Invalid JSON body returns 400")
	rec = s.doRequest(http.MethodPost, e2eClusterPath("/queries/plan-check"),
		[]byte(`{not valid json`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 9: Non-existent cluster returns 404 for all endpoints.
	s.T().Log("Step 9: Non-existent cluster returns 404")
	nonExistentPath := fmt.Sprintf("%s/clusters/nonexistent-cluster/queries?namespace=%s",
		scenario63E2EPrefix, scenario63E2ENamespace)
	rec = s.doRequest(http.MethodGet, nonExistentPath, nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "CLUSTER_NOT_FOUND", errObj["code"])

	// Step 10: Invalid limit parameter returns 400.
	s.T().Log("Step 10: Invalid limit parameter returns 400")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/history?limit=-1"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

// ============================================================================
// Journey: Monitoring Overview (63a → 63b → 63j → 63k)
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsE2ESuite) TestE2E_Scenario63_MonitoringOverviewJourney() {
	// Step 1: Get query monitoring status.
	s.T().Log("Step 1: GET /queries — monitoring status")
	rec := s.doRequest(http.MethodGet, e2eClusterPath("/queries"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])

	// Step 2: Get active query counts.
	s.T().Log("Step 2: GET /queries/active — active counts")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/queries/active"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])

	// Step 3: Check exporter health.
	s.T().Log("Step 3: GET /metrics/exporters — exporter health")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/metrics/exporters"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	exporters, ok := resp["exporters"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(exporters))

	// Step 4: List sessions.
	s.T().Log("Step 4: GET /sessions — session list")
	rec = s.doRequest(http.MethodGet, e2eClusterPath("/sessions"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(sessions))
}
