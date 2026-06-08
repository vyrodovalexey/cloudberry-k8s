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
// Scenario 64: All CLI Commands (E2E)
// ============================================================================
//
// This E2E scenario tests realistic user journeys across all CLI commands:
//
//   Journey 1: Query Listing (list all → list by status)
//   Journey 2: Query Operations (detail → cancel → move)
//   Journey 3: History (list by time → list by user/db → export csv)
//   Journey 4: Plan Check
//   Journey 5: Active Query Export
//
// ============================================================================

const (
	scenario64E2ECluster   = "e2e-cli-cluster"
	scenario64E2ENamespace = "default"
	scenario64E2EUser      = "admin"
	scenario64E2EPass      = "admin-pass"
	scenario64E2EPrefix    = "/api/v1alpha1"
	scenario64E2ERateLimit = 1000
)

// e2e64ClusterPath returns the base path for E2E cluster API calls.
func e2e64ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario64E2EPrefix, scenario64E2ECluster, endpoint, scenario64E2ENamespace)
}

// Scenario64AllCLICommandsE2ESuite tests all CLI commands via user journeys.
type Scenario64AllCLICommandsE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario64(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario64AllCLICommandsE2ESuite))
}

func (s *Scenario64AllCLICommandsE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 120*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario64E2ECluster, scenario64E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()

	cluster.Status.ActiveQueries = 8
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	mockClient := &testutil.MockDBClient{
		CancelQueryFunc: func(_ context.Context, pid int32) (bool, error) {
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
					QueryID:       "q-e2e64-001",
					QueryText:     "SELECT * FROM orders WHERE region = 'US'",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    2500.0,
					State:         "completed",
					ResourceGroup: "analytics",
					QueryStart:    time.Now().Add(-2 * time.Hour),
					CPUTimeMs:     1200.0,
					MemoryBytes:   52428800,
				},
				{
					QueryID:       "q-e2e64-002",
					QueryText:     "INSERT INTO audit_log VALUES (1, 'test')",
					Username:      "etl_user",
					DatabaseName:  "warehouse",
					DurationMs:    150.0,
					State:         "completed",
					ResourceGroup: "etl",
					QueryStart:    time.Now().Add(-1 * time.Hour),
					CPUTimeMs:     50.0,
					MemoryBytes:   1048576,
				},
				{
					QueryID:       "q-e2e64-003",
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

			if filter.Username != "" {
				var filtered []db.QueryHistoryEntry
				for _, e := range entries {
					if e.Username == filter.Username {
						filtered = append(filtered, e)
					}
				}
				return filtered, len(filtered), nil
			}
			if filter.Database != "" {
				var filtered []db.QueryHistoryEntry
				for _, e := range entries {
					if e.DatabaseName == filter.Database {
						filtered = append(filtered, e)
					}
				}
				return filtered, len(filtered), nil
			}

			return entries, len(entries), nil
		},
		GetQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			if queryID == "q-e2e64-001" {
				return &db.QueryHistoryEntry{
					QueryID:       "q-e2e64-001",
					QueryText:     "SELECT * FROM orders WHERE region = 'US'",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    2500.0,
					State:         "completed",
					ResourceGroup: "analytics",
					ExplainPlan:   "Seq Scan on orders (cost=0.00..5000.00 rows=100000 width=100)",
					QueryStart:    time.Now().Add(-2 * time.Hour),
				}, nil
			}
			return nil, fmt.Errorf("query %s not found", queryID)
		},
		ExportQueryHistoryCSVFunc: func(_ context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id", "query", "username", "database", "duration_ms", "state", "resource_group"})
			_ = csvWriter.Write([]string{"q-e2e64-001", "SELECT * FROM orders", "analyst", "mydb", "2500.0", "completed", "analytics"})
			_ = csvWriter.Write([]string{"q-e2e64-002", "INSERT INTO audit_log", "etl_user", "warehouse", "150.0", "completed", "etl"})
			csvWriter.Flush()
			return nil
		},
		ListSessionsWithResourceGroupFunc: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:           2001,
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
						PID:           2002,
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
						PID:           2003,
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
	store.SetCredentials(scenario64E2EUser, scenario64E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario64E2ERateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario64AllCLICommandsE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario64AllCLICommandsE2ESuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario64E2EUser, scenario64E2EPass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth.
func (s *Scenario64AllCLICommandsE2ESuite) doRequestNoAuth(method, path string, body []byte) *httptest.ResponseRecorder {
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
func (s *Scenario64AllCLICommandsE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// ============================================================================
// Journey 1: Query Listing (list all → list by status)
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_QueryListingJourney() {
	// Step 1: List all active queries (sessions).
	s.T().Log("Step 1: GET /sessions — list all queries")
	rec := s.doRequest(http.MethodGet, e2e64ClusterPath("/sessions"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(sessions), "should have 3 sessions")

	// Step 2: Filter by status=running.
	s.T().Log("Step 2: GET /sessions?status=running — filter active sessions")
	rec = s.doRequest(http.MethodGet, e2e64ClusterPath("/sessions")+"&status=running", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	activeSessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(activeSessions), "should have 2 active sessions")

	// Step 3: Filter by status=idle.
	s.T().Log("Step 3: GET /sessions?status=idle — filter idle sessions")
	rec = s.doRequest(http.MethodGet, e2e64ClusterPath("/sessions")+"&status=idle", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	idleSessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(idleSessions), "should have 1 idle session")
}

// ============================================================================
// Journey 2: Query Operations (detail → cancel → move)
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_QueryOperationsJourney() {
	// Step 1: Get query detail.
	s.T().Log("Step 1: GET /queries/2001 — query detail")
	rec := s.doRequest(http.MethodGet, e2e64ClusterPath("/queries/2001"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(2001), resp["pid"])
	assert.Equal(s.T(), "active", resp["state"])
	assert.NotNil(s.T(), resp["locks"])
	assert.NotNil(s.T(), resp["tablesAccessed"])

	// Step 2: Cancel the query with reason.
	s.T().Log("Step 2: POST /queries/2001/cancel — cancel query")
	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/2001/cancel"),
		[]byte(`{"reason":"e2e test cancellation"}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(2001), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "e2e test cancellation", resp["reason"])

	// Step 3: Move a different query to a resource group.
	s.T().Log("Step 3: POST /queries/2002/move — move query")
	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/2002/move"),
		[]byte(`{"targetGroup":"etl_group"}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(2002), resp["pid"])
	assert.Equal(s.T(), "etl_group", resp["targetGroup"])
	assert.Equal(s.T(), "moved", resp["status"])
}

// ============================================================================
// Journey 3: History (list by time → list by user/db → export csv)
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_HistoryJourney() {
	// Step 1: Browse history with time filter.
	s.T().Log("Step 1: GET /queries/history?since=24h — history by time")
	rec := s.doRequest(http.MethodGet, e2e64ClusterPath("/queries/history")+"&since=24h", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(items), "should have 3 history entries")

	// Step 2: Filter by user.
	s.T().Log("Step 2: GET /queries/history?user=analyst — filter by user")
	rec = s.doRequest(http.MethodGet, e2e64ClusterPath("/queries/history")+"&user=analyst", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, ok = resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(items), "should have 2 analyst entries")

	// Step 3: Filter by database.
	s.T().Log("Step 3: GET /queries/history?database=warehouse — filter by database")
	rec = s.doRequest(http.MethodGet, e2e64ClusterPath("/queries/history")+"&database=warehouse", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, ok = resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(items), "should have 1 warehouse entry")

	// Step 4: Export history as CSV.
	s.T().Log("Step 4: POST /queries/history/export — export CSV")
	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/history/export"), []byte(`{}`))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
	csvBody := rec.Body.String()
	assert.Contains(s.T(), csvBody, "query_id", "CSV should have header")
	assert.Contains(s.T(), csvBody, "q-e2e64-001", "CSV should contain first entry")
	assert.Contains(s.T(), csvBody, "q-e2e64-002", "CSV should contain second entry")

	// Verify CSV is parseable.
	reader := csv.NewReader(bytes.NewBufferString(csvBody))
	records, err := reader.ReadAll()
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), len(records), 2, "CSV should have header + at least 1 data row")
}

// ============================================================================
// Journey 4: Plan Check
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_PlanCheckJourney() {
	// Step 1: Submit plan with sequential scan.
	s.T().Log("Step 1: Submit plan with sequential scan")
	body, err := json.Marshal(map[string]string{"planText": cases.SamplePlanText("seq_scan")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 1, "should detect at least 1 issue")

	// Verify sequential scan issue.
	foundSeqScan := false
	for _, issue := range issues {
		issueMap, ok := issue.(map[string]interface{})
		require.True(s.T(), ok)
		if issueMap["category"] == "sequential_scan" {
			foundSeqScan = true
			assert.Contains(s.T(), issueMap["recommendation"].(string), "index")
		}
	}
	assert.True(s.T(), foundSeqScan, "should detect sequential_scan issue")

	// Step 2: Submit comprehensive plan.
	s.T().Log("Step 2: Submit comprehensive plan with all issues")
	body, err = json.Marshal(map[string]string{"planText": cases.SamplePlanText("full")})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	issues, ok = resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 3, "full plan should have at least 3 issues")

	// Step 3: Submit clean plan.
	s.T().Log("Step 3: Submit clean plan")
	body, err = json.Marshal(map[string]string{"planText": cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/plan-check"), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	issues, ok = resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), issues, "clean plan should have no issues")
	assert.Contains(s.T(), resp["summary"].(string), "No performance issues")
}

// ============================================================================
// Journey 5: Active Query Export
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_ActiveQueryExportJourney() {
	// Step 1: Export active queries as CSV.
	s.T().Log("Step 1: POST /queries/export — export active queries")
	rec := s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/export"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	// Verify CSV headers.
	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
	contentDisp := rec.Header().Get("Content-Disposition")
	assert.Contains(s.T(), contentDisp, "active-queries.csv")

	// Verify CSV content.
	csvBody := rec.Body.String()
	assert.Contains(s.T(), csvBody, "pid,username,database,state", "CSV should have header")
	assert.Contains(s.T(), csvBody, "2001", "CSV should contain PID 2001")
	assert.Contains(s.T(), csvBody, "2002", "CSV should contain PID 2002")
	assert.Contains(s.T(), csvBody, "2003", "CSV should contain PID 2003")
	assert.Contains(s.T(), csvBody, "gpadmin", "CSV should contain gpadmin user")
	assert.Contains(s.T(), csvBody, "analyst", "CSV should contain analyst user")
}

// ============================================================================
// Journey 6: Authentication Boundary (all CLI endpoints → 401)
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_AuthenticationBoundary() {
	endpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodGet, "/sessions", nil},
		{http.MethodGet, "/queries/2001", nil},
		{http.MethodPost, "/queries/2001/cancel", []byte(`{}`)},
		{http.MethodPost, "/queries/2001/move", []byte(`{"targetGroup":"etl"}`)},
		{http.MethodGet, "/queries/history", nil},
		{http.MethodPost, "/queries/history/export", []byte(`{}`)},
		{http.MethodPost, "/queries/plan-check", []byte(`{"planText":"test"}`)},
		{http.MethodPost, "/queries/export", nil},
	}

	for _, ep := range endpoints {
		s.T().Logf("Verifying 401 for unauthenticated %s %s", ep.method, ep.path)
		rec := s.doRequestNoAuth(ep.method, e2e64ClusterPath(ep.path), ep.body)
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated %s %s should return 401", ep.method, ep.path)
	}
}

// ============================================================================
// Journey 7: Error Handling
// ============================================================================

func (s *Scenario64AllCLICommandsE2ESuite) TestE2E_Scenario64_ErrorHandlingJourney() {
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
	}

	for _, ep := range invalidPIDPaths {
		rec := s.doRequest(ep.method, e2e64ClusterPath(ep.path), ep.body)
		assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
			"%s %s should return 400 for invalid PID", ep.method, ep.path)
	}

	// Step 2: Missing targetGroup for move returns 400.
	s.T().Log("Step 2: Missing targetGroup returns 400")
	rec := s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/2001/move"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 3: SQL injection in targetGroup returns 400.
	s.T().Log("Step 3: SQL injection in targetGroup returns 400")
	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/2001/move"),
		[]byte(`{"targetGroup":"DROP TABLE;--"}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 4: Non-existent query returns 404.
	s.T().Log("Step 4: Non-existent query PID returns 404")
	rec = s.doRequest(http.MethodGet, e2e64ClusterPath("/queries/9999"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	// Step 5: Empty plan text returns 400.
	s.T().Log("Step 5: Empty plan text returns 400")
	rec = s.doRequest(http.MethodPost, e2e64ClusterPath("/queries/plan-check"),
		[]byte(`{"planText":""}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	// Step 6: Non-existent cluster returns 404.
	s.T().Log("Step 6: Non-existent cluster returns 404")
	nonExistentPath := fmt.Sprintf("%s/clusters/nonexistent-cluster/sessions?namespace=%s",
		scenario64E2EPrefix, scenario64E2ENamespace)
	rec = s.doRequest(http.MethodGet, nonExistentPath, nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "CLUSTER_NOT_FOUND", errObj["code"])
}
