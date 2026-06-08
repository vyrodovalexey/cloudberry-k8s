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
	"strings"
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
// Scenario 63: All REST API Endpoints
// ============================================================================
//
// This scenario verifies all 13 REST API endpoints from the Query Monitoring
// & Session Management specification:
//   - 63a: GET /queries — query monitoring overview
//   - 63b: GET /queries/active — active query counts
//   - 63c: GET /queries/{pid} — query detail
//   - 63d: POST /queries/{pid}/cancel — cancel query
//   - 63e: POST /queries/{pid}/move — move query to resource group
//   - 63f: GET /queries/history — query history with pagination
//   - 63g: GET /queries/history/{qid} — query history detail
//   - 63h: POST /queries/history/export — export query history as CSV
//   - 63i: POST /queries/plan-check — plan analysis
//   - 63j: GET /metrics/exporters — exporter health
//   - 63k: GET /sessions — list sessions
//   - 63l: POST /sessions/{pid}/cancel — cancel session query
//   - 63m: DELETE /sessions/{pid} — terminate session
//
// Each endpoint is tested for:
//   - Happy path (200 OK with expected response structure)
//   - Unauthenticated access (401)
//   - Cluster not found (404)
//   - Invalid input (400)
//
// ============================================================================

const (
	scenario63APIPrefix = "/api/v1alpha1"
	scenario63Cluster   = "test-cluster"
	scenario63Namespace = "default"
	scenario63User      = "admin"
	scenario63Pass      = "admin-pass"
	scenario63RateLimit = 1000
)

// scenario63ClusterPath returns the base path for a cluster endpoint.
func scenario63ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario63APIPrefix, scenario63Cluster, endpoint, scenario63Namespace)
}

// scenario63NonExistentClusterPath returns a path for a non-existent cluster.
func scenario63NonExistentClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/nonexistent-cluster%s?namespace=%s",
		scenario63APIPrefix, endpoint, scenario63Namespace)
}

// Scenario63AllRESTAPIEndpointsSuite tests all 13 REST API endpoints.
type Scenario63AllRESTAPIEndpointsSuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario63(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario63AllRESTAPIEndpointsSuite))
}

func (s *Scenario63AllRESTAPIEndpointsSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario63Cluster, scenario63Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()

	// Set query counts on status for 63a/63b.
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
					QueryID:      "q-1234-5678",
					QueryText:    "SELECT * FROM orders",
					Username:     "analyst",
					DatabaseName: "mydb",
					DurationMs:   1500.5,
					State:        "completed",
					QueryStart:   time.Now().Add(-1 * time.Hour),
				},
				{
					QueryID:      "q-2345-6789",
					QueryText:    "INSERT INTO logs VALUES (1)",
					Username:     "etl_user",
					DatabaseName: "warehouse",
					DurationMs:   250.0,
					State:        "completed",
					QueryStart:   time.Now().Add(-30 * time.Minute),
				},
			}
			return entries, len(entries), nil
		},
		GetQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			if queryID == "q-1234-5678" {
				return &db.QueryHistoryEntry{
					QueryID:      "q-1234-5678",
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
			_ = csvWriter.Write([]string{"q-1234-5678", "SELECT * FROM orders", "analyst", "mydb", "1500.5", "completed"})
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
	store.SetCredentials(scenario63User, scenario63Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario63RateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario63AllRESTAPIEndpointsSuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario63User, scenario63Pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth.
func (s *Scenario63AllRESTAPIEndpointsSuite) doRequestNoAuth(method, path string, body []byte) *httptest.ResponseRecorder {
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
func (s *Scenario63AllRESTAPIEndpointsSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// ============================================================================
// 63a: GET /queries — Query Monitoring Overview
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63a_ListQueries() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["activeQueries"], "should have activeQueries")
	assert.NotNil(s.T(), resp["queuedQueries"], "should have queuedQueries")
	assert.NotNil(s.T(), resp["blockedQueries"], "should have blockedQueries")

	// Verify counts match what we set on the cluster status.
	assert.Equal(s.T(), float64(5), resp["activeQueries"])
	assert.Equal(s.T(), float64(2), resp["queuedQueries"])
	assert.Equal(s.T(), float64(1), resp["blockedQueries"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63a_ListQueries_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code, "GET /queries for non-existent cluster should return 404")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "CLUSTER_NOT_FOUND", errObj["code"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63a_ListQueries_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code, "unauthenticated GET /queries should return 401")
}

// ============================================================================
// 63b: GET /queries/active — Active Query Counts
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63b_ActiveQueries() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries/active should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["activeQueries"], "should have activeQueries")
	assert.NotNil(s.T(), resp["queuedQueries"], "should have queuedQueries")
	assert.NotNil(s.T(), resp["blockedQueries"], "should have blockedQueries")

	// Verify integer counts.
	active, ok := resp["activeQueries"].(float64)
	require.True(s.T(), ok, "activeQueries should be a number")
	assert.Equal(s.T(), float64(5), active)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63b_ActiveQueries_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63b_ActiveQueries_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63c: GET /queries/{pid} — Query Detail
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63c_QueryDetail() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/1234"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries/{pid} should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["pid"], "should have pid")
	assert.NotNil(s.T(), resp["username"], "should have username")
	assert.NotNil(s.T(), resp["database"], "should have database")
	assert.NotNil(s.T(), resp["state"], "should have state")
	assert.NotNil(s.T(), resp["query"], "should have query")
	assert.NotNil(s.T(), resp["duration"], "should have duration")
	assert.NotNil(s.T(), resp["locks"], "should have locks")
	assert.NotNil(s.T(), resp["tablesAccessed"], "should have tablesAccessed")

	// Verify PID matches.
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), "active", resp["state"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63c_QueryDetail_InvalidPID() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/abc"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "GET /queries/abc should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63c_QueryDetail_NegativePID() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/-1"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "GET /queries/-1 should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63c_QueryDetail_NotFound() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/9999"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code, "GET /queries/9999 should return 404")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63c_QueryDetail_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/queries/1234"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63d: POST /queries/{pid}/cancel — Cancel Query
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code, "POST /queries/{pid}/cancel should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "canceled", resp["status"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery_WithReason() {
	body := []byte(`{"reason":"query taking too long"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/cancel"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.Equal(s.T(), "query taking too long", resp["reason"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery_InvalidPID() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/abc/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery_NegativePID() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/-5/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario63NonExistentClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63d_CancelQuery_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario63ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63e: POST /queries/{pid}/move — Move Query
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "POST /queries/{pid}/move should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), "etl_group", resp["targetGroup"])
	assert.Equal(s.T(), "moved", resp["status"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_MissingTargetGroup() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/move"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "missing targetGroup should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_InvalidTargetGroup() {
	body := []byte(`{"targetGroup":"DROP TABLE;--"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "SQL injection targetGroup should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_EmptyBody() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/1234/move"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "nil body should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_InvalidPID() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/abc/move"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_NegativePID() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/-1/move"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_ClusterNotFound() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequest(http.MethodPost, scenario63NonExistentClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63e_MoveQuery_Unauthenticated() {
	body := []byte(`{"targetGroup":"etl_group"}`)
	rec := s.doRequestNoAuth(http.MethodPost, scenario63ClusterPath("/queries/1234/move"), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63f: GET /queries/history — Query History
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63f_QueryHistory() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/history"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries/history should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["items"], "should have items")
	assert.NotNil(s.T(), resp["total"], "should have total")
	assert.NotNil(s.T(), resp["limit"], "should have limit")
	assert.NotNil(s.T(), resp["offset"], "should have offset")

	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(items), "should have 2 history entries")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63f_QueryHistory_WithFilters() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/history")+"&user=analyst&limit=10", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["items"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63f_QueryHistory_InvalidLimit() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/history?limit=-1"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "invalid limit should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63f_QueryHistory_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/queries/history"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63f_QueryHistory_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/queries/history"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63g: GET /queries/history/{qid} — Query History Detail
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63g_QueryHistoryDetail() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/history/q-1234-5678"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /queries/history/{qid} should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "q-1234-5678", resp["queryId"])
	assert.NotNil(s.T(), resp["queryText"], "should have query text")
	assert.NotNil(s.T(), resp["explainPlan"], "should have plan")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63g_QueryHistoryDetail_NotFound() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/queries/history/q-nonexistent"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code, "unknown query ID should return 404")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63g_QueryHistoryDetail_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/queries/history/q-1234-5678"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63g_QueryHistoryDetail_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/queries/history/q-1234-5678"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63h: POST /queries/history/export — Export Query History
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63h_ExportQueryHistory() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code, "POST /queries/history/export should return 200")

	// Verify Content-Type is CSV.
	contentType := rec.Header().Get("Content-Type")
	assert.Equal(s.T(), "text/csv", contentType, "Content-Type should be text/csv")

	// Verify Content-Disposition header.
	contentDisp := rec.Header().Get("Content-Disposition")
	assert.Contains(s.T(), contentDisp, "query-history.csv", "should have CSV filename")

	// Verify CSV content.
	body := rec.Body.String()
	assert.Contains(s.T(), body, "query_id", "CSV should have header row")
	assert.Contains(s.T(), body, "q-1234-5678", "CSV should contain data")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63h_ExportQueryHistory_WithFilters() {
	body := []byte(`{"user":"analyst"}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/history/export"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"))
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63h_ExportQueryHistory_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario63NonExistentClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63h_ExportQueryHistory_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario63ClusterPath("/queries/history/export"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63i: POST /queries/plan-check — Plan Analysis
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63i_PlanCheck() {
	body, err := json.Marshal(map[string]string{"planText": cases.SamplePlanText("seq_scan")})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "POST /queries/plan-check should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["issues"], "should have issues")
	assert.NotNil(s.T(), resp["summary"], "should have summary")
	assert.NotNil(s.T(), resp["totalNodes"], "should have totalNodes")

	issues, ok := resp["issues"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(issues), 1, "should detect at least 1 issue")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63i_PlanCheck_EmptyPlan() {
	body := []byte(`{"planText":""}`)
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code, "empty plan should return 400")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63i_PlanCheck_Unauthenticated() {
	body, _ := json.Marshal(map[string]string{"planText": cases.SamplePlanText("clean")})
	rec := s.doRequestNoAuth(http.MethodPost, scenario63ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63j: GET /metrics/exporters — Exporter Health
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63j_ExporterHealth() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /metrics/exporters should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["exporters"], "should have exporters")
	assert.NotNil(s.T(), resp["total"], "should have total")

	exporters, ok := resp["exporters"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 3, len(exporters), "should have 3 exporters configured")

	// Verify each exporter has expected fields.
	for i, exp := range exporters {
		expMap, ok := exp.(map[string]interface{})
		require.True(s.T(), ok, "exporter %d should be an object", i)
		assert.NotEmpty(s.T(), expMap["name"], "exporter %d should have name", i)
		assert.NotNil(s.T(), expMap["port"], "exporter %d should have port", i)
		assert.NotEmpty(s.T(), expMap["status"], "exporter %d should have status", i)
		assert.NotNil(s.T(), expMap["containerReady"], "exporter %d should have containerReady", i)
		assert.NotEmpty(s.T(), expMap["endpoint"], "exporter %d should have endpoint", i)
	}
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63j_ExporterHealth_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63j_ExporterHealth_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63k: GET /sessions — List Sessions
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63k_ListSessions() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "GET /sessions should return 200")

	resp := s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["sessions"], "should have sessions")
	assert.NotNil(s.T(), resp["total"], "should have total")

	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), 2, len(sessions), "should have 2 sessions")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63k_ListSessions_WithStatusFilter() {
	rec := s.doRequest(http.MethodGet, scenario63ClusterPath("/sessions")+"&status=running", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	// Only the "active" session should match "running" filter.
	assert.Equal(s.T(), 1, len(sessions), "running filter should return 1 session")
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63k_ListSessions_ClusterNotFound() {
	rec := s.doRequest(http.MethodGet, scenario63NonExistentClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63k_ListSessions_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodGet, scenario63ClusterPath("/sessions"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63l: POST /sessions/{pid}/cancel — Cancel Session Query
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63l_CancelSession() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/sessions/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code, "POST /sessions/{pid}/cancel should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63l_CancelSession_InvalidPID() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/sessions/abc/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63l_CancelSession_NegativePID() {
	rec := s.doRequest(http.MethodPost, scenario63ClusterPath("/sessions/-1/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63l_CancelSession_ClusterNotFound() {
	rec := s.doRequest(http.MethodPost, scenario63NonExistentClusterPath("/sessions/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63l_CancelSession_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodPost, scenario63ClusterPath("/sessions/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// 63m: DELETE /sessions/{pid} — Terminate Session
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63m_TerminateSession() {
	rec := s.doRequest(http.MethodDelete, scenario63ClusterPath("/sessions/1234"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code, "DELETE /sessions/{pid} should return 200")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["terminated"])
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63m_TerminateSession_InvalidPID() {
	rec := s.doRequest(http.MethodDelete, scenario63ClusterPath("/sessions/abc"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63m_TerminateSession_NegativePID() {
	rec := s.doRequest(http.MethodDelete, scenario63ClusterPath("/sessions/-1"), nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63m_TerminateSession_ClusterNotFound() {
	rec := s.doRequest(http.MethodDelete, scenario63NonExistentClusterPath("/sessions/1234"), nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63m_TerminateSession_Unauthenticated() {
	rec := s.doRequestNoAuth(http.MethodDelete, scenario63ClusterPath("/sessions/1234"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// Data-Driven Tests from Test Cases Catalog
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63_DataDriven() {
	for _, tc := range cases.APIEndpointCases() {
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)

			// Skip cluster-not-found cases (they use a different cluster name).
			if strings.Contains(tc.Name, "cluster_not_found") {
				path := scenario63NonExistentClusterPath(tc.Path)
				var body []byte
				if tc.Body != "" {
					body = []byte(tc.Body)
				}
				rec := s.doRequest(tc.Method, path, body)
				assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
					"expected status %d for %s", tc.ExpectedStatus, tc.Name)
				return
			}

			// Skip "no_config" cases — they need a cluster without exporters.
			if strings.Contains(tc.Name, "no_config") {
				return
			}

			path := scenario63ClusterPath(tc.Path)
			var body []byte
			if tc.Body != "" {
				body = []byte(tc.Body)
			}

			rec := s.doRequest(tc.Method, path, body)
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"expected status %d for %s", tc.ExpectedStatus, tc.Name)

			// Verify expected keys in response for successful requests.
			if tc.ExpectedStatus == http.StatusOK && len(tc.ExpectedKeys) > 0 {
				// CSV responses don't have JSON keys.
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
// DB Unavailable Graceful Degradation
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63_DBUnavailable_Sessions() {
	// Create a server without DB factory.
	cluster := testutil.NewClusterBuilder(scenario63Cluster, scenario63Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario63User, scenario63Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	// Pass nil for dbFactory.
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario63RateLimit)
	defer server.Close()
	handler := server.Handler()

	// Sessions should return empty list with message.
	req := httptest.NewRequest(http.MethodGet, scenario63ClusterPath("/sessions"), nil)
	req.SetBasicAuth(scenario63User, scenario63Pass)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), float64(0), resp["total"])
	assert.NotEmpty(s.T(), resp["message"])

	// Cancel query should return canceled=false with message.
	req = httptest.NewRequest(http.MethodPost, scenario63ClusterPath("/sessions/1234/cancel"), bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(scenario63User, scenario63Pass)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = map[string]interface{}{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), false, resp["canceled"])
	assert.NotEmpty(s.T(), resp["message"])

	// Terminate session should return terminated=false with message.
	req = httptest.NewRequest(http.MethodDelete, scenario63ClusterPath("/sessions/1234"), nil)
	req.SetBasicAuth(scenario63User, scenario63Pass)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = map[string]interface{}{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), false, resp["terminated"])
	assert.NotEmpty(s.T(), resp["message"])
}

// ============================================================================
// Response Content-Type Validation
// ============================================================================

func (s *Scenario63AllRESTAPIEndpointsSuite) TestFunctional_Scenario63_ContentType_JSON() {
	endpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{http.MethodGet, "/queries", nil},
		{http.MethodGet, "/queries/active", nil},
		{http.MethodGet, "/queries/1234", nil},
		{http.MethodGet, "/sessions", nil},
		{http.MethodGet, "/metrics/exporters", nil},
	}

	for _, ep := range endpoints {
		rec := s.doRequest(ep.method, scenario63ClusterPath(ep.path), ep.body)
		if rec.Code == http.StatusOK {
			ct := rec.Header().Get("Content-Type")
			assert.Contains(s.T(), ct, "application/json",
				"%s %s should return application/json", ep.method, ep.path)
		}
	}
}
