//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 60: Query Detail API
// ============================================================================
//
// This scenario verifies the Query Detail API endpoint:
//   - Fetching execution metrics for a specific query by PID
//   - Returning lock information held or awaited by the query
//   - Returning tables accessed by the query
//   - Returning 404 when the query is not found
//   - Returning 400 for invalid PID values
//
// ============================================================================

const (
	scenario60APIPrefix = "/api/v1alpha1"
	scenario60Cluster   = "test-cluster"
	scenario60User      = "admin"
	scenario60Pass      = "admin-pass"
)

// Scenario60QueryDetailsSuite tests the Query Detail API.
type Scenario60QueryDetailsSuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario60(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario60QueryDetailsSuite))
}

func (s *Scenario60QueryDetailsSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario60Cluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Create mock DB client with a GetQueryDetail function that returns
	// detailed query information for PID 1234 and an error for all others.
	mockClient := &testutil.MockDBClient{
		GetQueryDetailFunc: func(_ context.Context, pid int32) (*db.QueryDetail, error) {
			if pid == 1234 {
				return &db.QueryDetail{
					PID:           1234,
					Username:      "analyst",
					Database:      "mydb",
					State:         "active",
					Query:         "SELECT * FROM large_table JOIN dim_table ON ...",
					QueryStart:    time.Now().Add(-30 * time.Second),
					Duration:      "00:00:30",
					WaitEventType: "",
					BackendType:   "client backend",
					Locks: []db.LockInfo{
						{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "large_table"},
						{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "dim_table"},
					},
					TablesAccessed: []string{"public.large_table", "public.dim_table"},
				}, nil
			}
			return nil, fmt.Errorf("query not found")
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario60User, scenario60Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario60QueryDetailsSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an HTTP request with basic auth credentials.
func (s *Scenario60QueryDetailsSuite) doRequest(method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.SetBasicAuth(scenario60User, scenario60Pass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario60QueryDetailsSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// --- 60a: Query detail found with all fields ---

// TestFunctional_Scenario60_QueryDetail_Found verifies that the query detail
// endpoint returns 200 with full execution metrics when the query exists.
func (s *Scenario60QueryDetailsSuite) TestFunctional_Scenario60_QueryDetail_Found() {
	path := scenario60APIPrefix + "/clusters/" + scenario60Cluster + "/queries/1234?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"GET /queries/1234 should return 200 OK")

	resp := s.decodeJSON(rec)

	// Verify execution metrics.
	assert.Equal(s.T(), float64(1234), resp["pid"],
		"response should contain the correct PID")
	assert.Equal(s.T(), "active", resp["state"],
		"response should contain the query state")
	assert.Equal(s.T(), "SELECT * FROM large_table JOIN dim_table ON ...", resp["query"],
		"response should contain the query text")
	assert.Equal(s.T(), "00:00:30", resp["duration"],
		"response should contain the query duration")
	assert.Equal(s.T(), "client backend", resp["backendType"],
		"response should contain the backend type")
	assert.Equal(s.T(), "analyst", resp["username"],
		"response should contain the username")
	assert.Equal(s.T(), "mydb", resp["database"],
		"response should contain the database name")
}

// --- 60b: Query detail with locks ---

// TestFunctional_Scenario60_QueryDetail_WithLocks verifies that the query detail
// endpoint returns lock information when the query holds or awaits locks.
func (s *Scenario60QueryDetailsSuite) TestFunctional_Scenario60_QueryDetail_WithLocks() {
	path := scenario60APIPrefix + "/clusters/" + scenario60Cluster + "/queries/1234?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)

	// Verify locks array is present and contains expected entries.
	locksRaw, ok := resp["locks"].([]interface{})
	require.True(s.T(), ok, "response should contain 'locks' array")
	require.Len(s.T(), locksRaw, 2, "should have 2 lock entries")

	// Verify first lock entry.
	lock0 := locksRaw[0].(map[string]interface{})
	assert.Equal(s.T(), "relation", lock0["lockType"],
		"first lock should have lockType 'relation'")
	assert.Equal(s.T(), "AccessShareLock", lock0["mode"],
		"first lock should have mode 'AccessShareLock'")
	assert.Equal(s.T(), true, lock0["granted"],
		"first lock should be granted")
	assert.Equal(s.T(), "large_table", lock0["relation"],
		"first lock should be on 'large_table'")

	// Verify second lock entry.
	lock1 := locksRaw[1].(map[string]interface{})
	assert.Equal(s.T(), "relation", lock1["lockType"],
		"second lock should have lockType 'relation'")
	assert.Equal(s.T(), "AccessShareLock", lock1["mode"],
		"second lock should have mode 'AccessShareLock'")
	assert.Equal(s.T(), true, lock1["granted"],
		"second lock should be granted")
	assert.Equal(s.T(), "dim_table", lock1["relation"],
		"second lock should be on 'dim_table'")
}

// --- 60c: Query detail with tables accessed ---

// TestFunctional_Scenario60_QueryDetail_WithTables verifies that the query detail
// endpoint returns the list of tables accessed by the query.
func (s *Scenario60QueryDetailsSuite) TestFunctional_Scenario60_QueryDetail_WithTables() {
	path := scenario60APIPrefix + "/clusters/" + scenario60Cluster + "/queries/1234?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)

	// Verify tablesAccessed array is present and contains expected entries.
	tablesRaw, ok := resp["tablesAccessed"].([]interface{})
	require.True(s.T(), ok, "response should contain 'tablesAccessed' array")
	require.Len(s.T(), tablesRaw, 2, "should have 2 tables accessed")

	tables := make([]string, len(tablesRaw))
	for i, t := range tablesRaw {
		tables[i] = t.(string)
	}
	assert.Contains(s.T(), tables, "public.large_table",
		"tablesAccessed should contain 'public.large_table'")
	assert.Contains(s.T(), tables, "public.dim_table",
		"tablesAccessed should contain 'public.dim_table'")
}

// --- 60d: Query detail not found ---

// TestFunctional_Scenario60_QueryDetail_NotFound verifies that the query detail
// endpoint returns 404 when the query PID does not exist.
func (s *Scenario60QueryDetailsSuite) TestFunctional_Scenario60_QueryDetail_NotFound() {
	path := scenario60APIPrefix + "/clusters/" + scenario60Cluster + "/queries/9999?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code,
		"GET /queries/9999 should return 404 Not Found")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "QUERY_NOT_FOUND", errObj["code"],
		"error code should be QUERY_NOT_FOUND")
}

// --- 60e: Invalid PID ---

// TestFunctional_Scenario60_QueryDetail_InvalidPID verifies that the query detail
// endpoint returns 400 when the PID is not a valid integer.
func (s *Scenario60QueryDetailsSuite) TestFunctional_Scenario60_QueryDetail_InvalidPID() {
	path := scenario60APIPrefix + "/clusters/" + scenario60Cluster + "/queries/abc?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"GET /queries/abc should return 400 Bad Request")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}
