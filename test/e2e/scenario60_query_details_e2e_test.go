//go:build e2e

package e2e

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
// Scenario 60: Query Detail API (E2E)
// ============================================================================
//
// This E2E scenario tests the full user journey for the Query Detail API:
//   1. User fetches query details for an active query and sees execution metrics
//   2. User inspects lock information held by the query
//   3. User inspects tables accessed by the query
//   4. User attempts to fetch details for a non-existent query and gets 404
//   5. User attempts to fetch details with an invalid PID and gets 400
//   6. Unauthenticated request is rejected with 401
//
// The test uses a mock DB client to simulate query detail responses and verifies
// the complete request/response cycle through the API server with auth.
//
// ============================================================================

const (
	scenario60E2ECluster = "e2e-cluster"
	scenario60E2EUser    = "admin"
	scenario60E2EPass    = "admin-pass"
	scenario60E2EPrefix  = "/api/v1alpha1"
)

// Scenario60QueryDetailsE2ESuite tests the Query Detail API user journey.
type Scenario60QueryDetailsE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario60(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario60QueryDetailsE2ESuite))
}

func (s *Scenario60QueryDetailsE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 60*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario60E2ECluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Create mock DB client simulating realistic query detail responses.
	mockClient := &testutil.MockDBClient{
		GetQueryDetailFunc: func(_ context.Context, pid int32) (*db.QueryDetail, error) {
			if pid == 5001 {
				return &db.QueryDetail{
					PID:           5001,
					Username:      "analyst",
					Database:      "warehouse",
					State:         "active",
					Query:         "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 1000",
					QueryStart:    time.Now().Add(-45 * time.Second),
					Duration:      "00:00:45",
					WaitEventType: "",
					WaitEvent:     "",
					BackendType:   "client backend",
					Locks: []db.LockInfo{
						{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "orders"},
						{LockType: "relation", Mode: "AccessShareLock", Granted: true, Relation: "customers"},
					},
					TablesAccessed: []string{"public.orders", "public.customers"},
				}, nil
			}
			if pid == 5002 {
				return &db.QueryDetail{
					PID:           5002,
					Username:      "etl_service",
					Database:      "warehouse",
					State:         "active",
					Query:         "UPDATE inventory SET qty = qty - 1 WHERE product_id = 42",
					QueryStart:    time.Now().Add(-2 * time.Minute),
					Duration:      "00:02:00",
					WaitEventType: "Lock",
					WaitEvent:     "transactionid",
					BackendType:   "client backend",
					Locks: []db.LockInfo{
						{LockType: "relation", Mode: "RowExclusiveLock", Granted: true, Relation: "inventory"},
						{LockType: "transactionid", Mode: "ShareLock", Granted: false, Relation: ""},
					},
					TablesAccessed: []string{"public.inventory"},
				}, nil
			}
			return nil, fmt.Errorf("query not found")
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario60E2EUser, scenario60E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario60QueryDetailsE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario60QueryDetailsE2ESuite) doRequest(method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.SetBasicAuth(scenario60E2EUser, scenario60E2EPass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario60QueryDetailsE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// --- E2E Journey: Full Query Detail workflow ---

// TestE2E_Scenario60_FullJourney tests the complete user journey:
// fetch query details -> inspect locks -> inspect tables -> handle not found -> handle invalid PID.
func (s *Scenario60QueryDetailsE2ESuite) TestE2E_Scenario60_FullJourney() {
	basePath := scenario60E2EPrefix + "/clusters/" + scenario60E2ECluster + "/queries"

	// Step 1: User fetches details for an active SELECT query.
	s.T().Log("Step 1: Fetch query details for PID 5001")
	rec := s.doRequest(http.MethodGet, basePath+"/5001?namespace=default")
	require.Equal(s.T(), http.StatusOK, rec.Code, "fetching query detail should succeed")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(5001), resp["pid"], "should return correct PID")
	assert.Equal(s.T(), "active", resp["state"], "query should be active")
	assert.Equal(s.T(), "analyst", resp["username"], "should return correct username")
	assert.Equal(s.T(), "warehouse", resp["database"], "should return correct database")
	assert.Equal(s.T(), "client backend", resp["backendType"], "should return backend type")
	assert.Equal(s.T(), "00:00:45", resp["duration"], "should return correct duration")

	// Step 2: User inspects lock information for the query.
	s.T().Log("Step 2: Inspect lock information")
	locks, ok := resp["locks"].([]interface{})
	require.True(s.T(), ok, "response should contain 'locks' array")
	require.Len(s.T(), locks, 2, "should have 2 lock entries")

	lock0 := locks[0].(map[string]interface{})
	assert.Equal(s.T(), "relation", lock0["lockType"])
	assert.Equal(s.T(), "AccessShareLock", lock0["mode"])
	assert.Equal(s.T(), true, lock0["granted"])
	assert.Equal(s.T(), "orders", lock0["relation"])

	// Step 3: User inspects tables accessed by the query.
	s.T().Log("Step 3: Inspect tables accessed")
	tables, ok := resp["tablesAccessed"].([]interface{})
	require.True(s.T(), ok, "response should contain 'tablesAccessed' array")
	require.Len(s.T(), tables, 2, "should have 2 tables accessed")
	assert.Equal(s.T(), "public.orders", tables[0])
	assert.Equal(s.T(), "public.customers", tables[1])

	// Step 4: User tries to fetch details for a non-existent query.
	s.T().Log("Step 4: Fetch non-existent query")
	rec = s.doRequest(http.MethodGet, basePath+"/9999?namespace=default")
	require.Equal(s.T(), http.StatusNotFound, rec.Code,
		"non-existent query should return 404")

	resp = s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "QUERY_NOT_FOUND", errObj["code"],
		"error code should be QUERY_NOT_FOUND")

	// Step 5: User provides an invalid PID.
	s.T().Log("Step 5: Fetch with invalid PID")
	rec = s.doRequest(http.MethodGet, basePath+"/abc?namespace=default")
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"invalid PID should return 400")

	resp = s.decodeJSON(rec)
	errObj, ok = resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}

// TestE2E_Scenario60_BlockedQueryWithWaitingLock tests fetching details for a
// query that is blocked waiting on a lock, verifying the wait event and
// non-granted lock information.
func (s *Scenario60QueryDetailsE2ESuite) TestE2E_Scenario60_BlockedQueryWithWaitingLock() {
	path := scenario60E2EPrefix + "/clusters/" + scenario60E2ECluster + "/queries/5002?namespace=default"

	rec := s.doRequest(http.MethodGet, path)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(5002), resp["pid"])
	assert.Equal(s.T(), "active", resp["state"])
	assert.Equal(s.T(), "Lock", resp["waitEventType"],
		"blocked query should have Lock wait event type")
	assert.Equal(s.T(), "transactionid", resp["waitEvent"],
		"blocked query should have transactionid wait event")

	// Verify locks include a non-granted lock.
	locks, ok := resp["locks"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), locks, 2)

	// Find the non-granted lock.
	var foundWaitingLock bool
	for _, l := range locks {
		lockMap := l.(map[string]interface{})
		if lockMap["granted"] == false {
			foundWaitingLock = true
			assert.Equal(s.T(), "transactionid", lockMap["lockType"],
				"waiting lock should be of type transactionid")
			assert.Equal(s.T(), "ShareLock", lockMap["mode"],
				"waiting lock should have ShareLock mode")
		}
	}
	assert.True(s.T(), foundWaitingLock,
		"should find at least one non-granted (waiting) lock")

	// Verify tables accessed.
	tables, ok := resp["tablesAccessed"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), tables, 1)
	assert.Equal(s.T(), "public.inventory", tables[0])
}

// TestE2E_Scenario60_UnauthenticatedDenied verifies that unauthenticated
// requests to the query detail endpoint are rejected with 401.
func (s *Scenario60QueryDetailsE2ESuite) TestE2E_Scenario60_UnauthenticatedDenied() {
	path := scenario60E2EPrefix + "/clusters/" + scenario60E2ECluster + "/queries/5001?namespace=default"

	req := httptest.NewRequest(http.MethodGet, path, nil)
	// No basic auth set.
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated request should be rejected with 401")
}
