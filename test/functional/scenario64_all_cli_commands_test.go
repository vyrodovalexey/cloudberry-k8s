//go:build functional

package functional

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
// Scenario 64: All CLI Commands
// ============================================================================
//
// This scenario verifies all 9 CLI sub-scenarios by testing the underlying
// API endpoints that the CLI commands call:
//   - 64a: queries list — GET /sessions with optional status filter
//   - 64b: queries detail — GET /queries/{pid}
//   - 64c: queries cancel — POST /queries/{pid}/cancel with reason
//   - 64d: queries move — POST /queries/{pid}/move with targetGroup
//   - 64e: queries history --last 24h — GET /queries/history?since=24h
//   - 64f: queries history --user --database — GET /queries/history?user=X&database=Y
//   - 64g: queries plan-check — POST /queries/plan-check
//   - 64h: queries export --format csv — POST /queries/export (CSV response)
//   - 64i: queries history --export csv — POST /queries/history/export
//
// ============================================================================

const (
	scenario64APIPrefix = "/api/v1alpha1"
	scenario64Cluster   = "cli-test-cluster"
	scenario64Namespace = "default"
	scenario64User      = "admin"
	scenario64Pass      = "admin-pass"
	scenario64RateLimit = 1000
)

// scenario64ClusterPath returns the base path for a cluster endpoint.
func scenario64ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario64APIPrefix, scenario64Cluster, endpoint, scenario64Namespace)
}

// scenario64NonExistentClusterPath returns a path for a non-existent cluster.
func scenario64NonExistentClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/nonexistent-cluster%s?namespace=%s",
		scenario64APIPrefix, endpoint, scenario64Namespace)
}

// Scenario64AllCLICommandsSuite tests all CLI command API endpoints.
type Scenario64AllCLICommandsSuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario64(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario64AllCLICommandsSuite))
}

func (s *Scenario64AllCLICommandsSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario64Cluster, scenario64Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()

	cluster.Status.ActiveQueries = 5
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
				Query:    "SELECT * FROM orders",
				Duration: "00:00:01.5005",
				Locks: []db.LockInfo{
					{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "orders"},
				},
				TablesAccessed: []string{"public.orders"},
			}, nil
		},
		GetQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			entries := []db.QueryHistoryEntry{
				{
					QueryID:       "q-cli-001",
					QueryText:     "SELECT * FROM orders",
					Username:      "analyst",
					DatabaseName:  "mydb",
					DurationMs:    1500.5,
					State:         "completed",
					ResourceGroup: "analytics",
					QueryStart:    time.Now().Add(-1 * time.Hour),
				},
				{
					QueryID:       "q-cli-002",
					QueryText:     "INSERT INTO logs VALUES (1)",
					Username:      "etl_user",
					DatabaseName:  "warehouse",
					DurationMs:    250.0,
					State:         "completed",
					ResourceGroup: "etl",
					QueryStart:    time.Now().Add(-30 * time.Minute),
				},
			}

			// Apply basic filtering for test verification.
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
			if queryID == "q-cli-001" {
				return &db.QueryHistoryEntry{
					QueryID:      "q-cli-001",
					QueryText:    "SELECT * FROM orders",
					Username:     "analyst",
					DatabaseName: "mydb",
					DurationMs:   1500.5,
					State:        "completed",
					ExplainPlan:  "Seq Scan on orders (cost=0.00..100.00 rows=1000 width=100)",
					QueryStart:   time.Now().Add(-1 * time.Hour),
				}, nil
			}
			return nil, fmt.Errorf("query %s not found", queryID)
		},
		ExportQueryHistoryCSVFunc: func(_ context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id", "query", "username", "database", "duration_ms", "state"})
			_ = csvWriter.Write([]string{"q-cli-001", "SELECT * FROM orders", "analyst", "mydb", "1500.5", "completed"})
			csvWriter.Flush()
			return nil
		},
		ListSessionsWithResourceGroupFunc: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:           1234,
						Username:      "gpadmin",
						Database:      "testdb",
						State:         "active",
						Query:         "SELECT 1",
						QueryStart:    time.Now(),
						WaitEventType: "",
					},
					ResourceGroup: "default_group",
				},
				{
					Session: db.Session{
						PID:           5678,
						Username:      "analyst",
						Database:      "mydb",
						State:         "idle",
						Query:         "",
						QueryStart:    time.Now().Add(-5 * time.Minute),
						WaitEventType: "",
					},
					ResourceGroup: "analytics",
				},
			}, nil
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario64User, scenario64Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario64RateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario64AllCLICommandsSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario64AllCLICommandsSuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario64User, scenario64Pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth.
func (s *Scenario64AllCLICommandsSuite) doRequestNoAuth(method, path string, body []byte) *httptest.ResponseRecorder {
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
func (s *Scenario64AllCLICommandsSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// ============================================================================
// 64a: queries list — GET /sessions with optional status filter
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64a_QueriesList() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /sessions should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["sessions"], "should have sessions")
	assert.NotNil(s.T(), resp["total"], "should have total")

	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(sessions), "should have 2 sessions")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64a_QueriesList_WithStatusFilter() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/sessions")+"&status=running", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(sessions), "running filter should return 1 session")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64a_QueriesList_IdleFilter() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/sessions")+"&status=idle", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(sessions), "idle filter should return 1 session")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64a_QueriesList_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario64NonExistentClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64a_QueriesList_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario64ClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64b: queries detail — GET /queries/{pid}
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64b_QueryDetail() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/1234"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries/{pid} should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), "active", resp["state"])
	assert.NotNil(s.T(), resp["locks"], "should have locks")
	assert.NotNil(s.T(), resp["tablesAccessed"], "should have tablesAccessed")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64b_QueryDetail_InvalidPID() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/abc"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64b_QueryDetail_NotFound() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/9999"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64b_QueryDetail_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario64ClusterPath("/queries/1234"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64c: queries cancel — POST /queries/{pid}/cancel with reason
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "canceled", resp["status"])
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel_WithReason() {
	body := []byte(`{"reason":"query taking too long"}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/1234/cancel"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "query taking too long", resp["reason"])
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel_InvalidPID() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/abc/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel_NegativePID() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/-5/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario64NonExistentClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64c_QueryCancel_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario64ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64d: queries move — POST /queries/{pid}/move with targetGroup
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), "etl_group", resp["targetGroup"])
	assert.Equal(s.T(), "moved", resp["status"])
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove_MissingTargetGroup() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/1234/move"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove_InvalidTargetGroup() {
	body := []byte(`{"targetGroup":"DROP TABLE;--"}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove_InvalidPID() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/abc/move"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove_ClusterNotFound() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario64NonExistentClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64d_QueryMove_Unauthenticated() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequestNoAuth(http.MethodPost, scenario64ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64e: queries history --last 24h — GET /queries/history?since=24h
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64e_QueryHistoryLast24h() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/history")+"&since=24h", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["items"], "should have items")
	assert.NotNil(s.T(), resp["total"], "should have total")
	assert.NotNil(s.T(), resp["limit"], "should have limit")
	assert.NotNil(s.T(), resp["offset"], "should have offset")

	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(items), "should have 2 history entries")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64e_QueryHistoryLast24h_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario64NonExistentClusterPath("/queries/history")+"&since=24h", nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64e_QueryHistoryLast24h_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario64ClusterPath("/queries/history")+"&since=24h", nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64f: queries history --user --database — GET /queries/history?user=X&database=Y
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64f_QueryHistoryUserFilter() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/history")+"&user=analyst", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(items), "user filter should return 1 entry")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64f_QueryHistoryDatabaseFilter() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/history")+"&database=warehouse", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 1, len(items), "database filter should return 1 entry")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64f_QueryHistoryCombinedFilters() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/history")+"&user=analyst&database=mydb", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(items), 1, "combined filters should return at least 1 entry")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64f_QueryHistoryInvalidLimit() {
	rec := s.doRequest(http.MethodGet, scenario64ClusterPath("/queries/history?limit=-1"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

// ============================================================================
// 64g: queries plan-check — POST /queries/plan-check
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64g_PlanCheck() {
	body, err := json.Marshal(map[string]string{"planText": cases.SamplePlanText("seq_scan")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["issues"], "should have issues")
	assert.NotNil(s.T(), resp["summary"], "should have summary")
	assert.NotNil(s.T(), resp["totalNodes"], "should have totalNodes")

	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 1, "should detect at least 1 issue")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64g_PlanCheck_CleanPlan() {
	body, err := json.Marshal(map[string]string{"planText": cases.SamplePlanText("clean")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), issues, "clean plan should have no issues")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64g_PlanCheck_EmptyPlan() {
	body := []byte(`{"planText":""}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64g_PlanCheck_Unauthenticated() {
	body, _ := json.Marshal(map[string]string{"planText": cases.SamplePlanText("clean")})
	rec := s.doRequestNoAuth(http.MethodPost, scenario64ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64h: queries export --format csv — POST /queries/export (CSV response)
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64h_ExportActiveQueries() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/export"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	// Verify Content-Type is CSV.
	contentType := rec.Header().Get("Content-Type")
	assert.Equal(s.T(), "text/csv", contentType, "Content-Type should be text/csv")

	// Verify Content-Disposition header.
	contentDisp := rec.Header().Get("Content-Disposition")
	assert.Contains(s.T(), contentDisp, "active-queries.csv", "should have CSV filename")

	// Verify CSV content has header and data.
	body := rec.Body.String()
	assert.Contains(s.T(), body, "pid,username,database,state", "CSV should have header row")
	assert.Contains(s.T(), body, "1234", "CSV should contain PID 1234")
	assert.Contains(s.T(), body, "5678", "CSV should contain PID 5678")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64h_ExportActiveQueries_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario64NonExistentClusterPath("/queries/export"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64h_ExportActiveQueries_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario64ClusterPath("/queries/export"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 64i: queries history --export csv — POST /queries/history/export
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64i_ExportQueryHistory() {
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	// Verify Content-Type is CSV.
	contentType := rec.Header().Get("Content-Type")
	assert.Equal(s.T(), "text/csv", contentType, "Content-Type should be text/csv")

	// Verify Content-Disposition header.
	contentDisp := rec.Header().Get("Content-Disposition")
	assert.Contains(s.T(), contentDisp, "query-history.csv", "should have CSV filename")

	// Verify CSV content.
	body := rec.Body.String()
	assert.Contains(s.T(), body, "query_id", "CSV should have header row")
	assert.Contains(s.T(), body, "q-cli-001", "CSV should contain data")
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64i_ExportQueryHistory_WithFilters() {
	body := []byte(`{"user":"analyst"}`)
	rec := s.doRequest(http.MethodPost, scenario64ClusterPath("/queries/history/export"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64i_ExportQueryHistory_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario64NonExistentClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64i_ExportQueryHistory_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario64ClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// Data-Driven Tests from Test Cases Catalog
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64_DataDriven() {
	for _, tc := range cases.CLICommandCases() {
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)

			var path string
			if tc.UseNonExistentCluster {
				path = scenario64NonExistentClusterPath(tc.Path)
			} else {
				path = scenario64ClusterPath(tc.Path)
			}

			var body []byte
			if tc.Body != "" {
				body = []byte(tc.Body)
			}

			rec := s.doRequest(tc.Method, path, body)
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"expected status %d for %s", tc.ExpectedStatus, tc.Name)

			// Verify expected keys in response for successful requests.
			if tc.ExpectedStatus == http.StatusOK && len(tc.ExpectedKeys) > 0 {
				if tc.ContentType == "text/csv" {
					assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
					return
				}
				resp := s.decodeJSON(rec)
				for _, key := range tc.ExpectedKeys {
					assert.NotNil(s.T(), resp[key],
						"response should have key %q for %s", key, tc.Name)
				}
			}
		})
	}
}

// ============================================================================
// Response Content-Type Validation
// ============================================================================

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64_ContentType_JSON() {
	endpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodGet, "/sessions", nil},
		{http.MethodGet, "/queries/1234", nil},
		{http.MethodGet, "/queries/history", nil},
	}

	for _, ep := range endpoints {
		rec := s.doRequest(ep.method, scenario64ClusterPath(ep.path), ep.body)
		if rec.Code == http.StatusOK {
			ct := rec.Header().Get("Content-Type")
			assert.Contains(s.T(), ct, "application/json",
				"%s %s should return application/json", ep.method, ep.path)
		}
	}
}

func (s *Scenario64AllCLICommandsSuite) TestFunctional_Scenario64_ContentType_CSV() {
	csvEndpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodPost, "/queries/export", nil},
		{http.MethodPost, "/queries/history/export", []byte(`{}`)},
	}

	for _, ep := range csvEndpoints {
		rec := s.doRequest(ep.method, scenario64ClusterPath(ep.path), ep.body)
		if rec.Code == http.StatusOK {
			ct := rec.Header().Get("Content-Type")
			assert.Equal(s.T(), "text/csv", ct,
				"%s %s should return text/csv", ep.method, ep.path)
		}
	}
}
