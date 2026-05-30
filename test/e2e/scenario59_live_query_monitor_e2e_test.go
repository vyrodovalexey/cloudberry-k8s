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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 59: Live Query Monitor API (E2E)
// ============================================================================
//
// This E2E scenario tests the full user journey for the Live Query Monitor:
//   1. User lists all sessions and sees the full set
//   2. User filters sessions by status, database, user, and resource group
//   3. User cancels a long-running query with a reason
//   4. User reassigns a role to a different resource group
//
// The test uses a mock DB client to simulate database sessions and verifies
// the complete request/response cycle through the API server with auth.
//
// ============================================================================

const (
	scenario59E2ECluster = "e2e-cluster"
	scenario59E2EUser    = "admin"
	scenario59E2EPass    = "admin-pass"
	scenario59E2EPrefix  = "/api/v1alpha1"
)

// Scenario59LiveQueryMonitorE2ESuite tests the Live Query Monitor user journey.
type Scenario59LiveQueryMonitorE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario59(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario59LiveQueryMonitorE2ESuite))
}

func (s *Scenario59LiveQueryMonitorE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 60*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario59E2ECluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Create mock DB client simulating a realistic set of database sessions.
	mockClient := &testutil.MockDBClient{
		ListSessionsWithResourceGroupFunc: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:           200,
						Username:      "analyst",
						Database:      "warehouse",
						Application:   "dbeaver",
						ClientAddress: "10.0.1.10",
						State:         "active",
						Query:         "SELECT * FROM sales WHERE year=2026",
						QueryStart:    time.Now().Add(-2 * time.Minute),
						Duration:      "00:02:00",
					},
					ResourceGroup: "analytics",
				},
				{
					Session: db.Session{
						PID:           201,
						Username:      "etl_service",
						Database:      "warehouse",
						Application:   "airflow",
						ClientAddress: "10.0.1.20",
						State:         "active",
						WaitEventType: "Lock",
						Query:         "UPDATE inventory SET qty=qty-1 WHERE id=42",
						QueryStart:    time.Now().Add(-5 * time.Minute),
						Duration:      "00:05:00",
					},
					ResourceGroup: "etl",
				},
				{
					Session: db.Session{
						PID:           202,
						Username:      "gpadmin",
						Database:      "postgres",
						Application:   "psql",
						ClientAddress: "127.0.0.1",
						State:         "idle",
						Query:         "",
						QueryStart:    time.Now().Add(-30 * time.Minute),
						Duration:      "00:30:00",
					},
					ResourceGroup: "admin_group",
				},
				{
					Session: db.Session{
						PID:           203,
						Username:      "analyst",
						Database:      "reporting",
						Application:   "tableau",
						ClientAddress: "10.0.1.30",
						State:         "idle in transaction",
						Query:         "BEGIN",
						QueryStart:    time.Now().Add(-1 * time.Minute),
						Duration:      "00:01:00",
					},
					ResourceGroup: "analytics",
				},
			}, nil
		},
		CancelQueryFunc: func(_ context.Context, _ int32) (bool, error) {
			return true, nil
		},
		AssignRoleResourceGroupFunc: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario59E2EUser, scenario59E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario59LiveQueryMonitorE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario59LiveQueryMonitorE2ESuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario59E2EUser, scenario59E2EPass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario59LiveQueryMonitorE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// --- E2E Journey: Full Live Query Monitor workflow ---

// TestE2E_Scenario59_FullJourney tests the complete user journey:
// list all sessions -> filter by status -> cancel a query -> reassign resource group.
func (s *Scenario59LiveQueryMonitorE2ESuite) TestE2E_Scenario59_FullJourney() {
	basePath := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/sessions?namespace=default"

	// Step 1: User lists all sessions to get an overview.
	s.T().Log("Step 1: List all sessions")
	rec := s.doRequest(http.MethodGet, basePath, nil)
	require.Equal(s.T(), http.StatusOK, rec.Code, "listing all sessions should succeed")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(4), resp["total"], "should see all 4 sessions")

	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok, "sessions should be an array")
	assert.Len(s.T(), sessions, 4)

	// Step 2: User filters to find blocked sessions (Lock-waiting).
	s.T().Log("Step 2: Filter blocked sessions")
	rec = s.doRequest(http.MethodGet, basePath+"&status=blocked", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code, "filtering blocked sessions should succeed")

	resp = s.decodeJSON(rec)
	blockedSessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok)
	require.NotEmpty(s.T(), blockedSessions, "should find at least one blocked session")

	// Verify the blocked session is PID 201 with Lock wait.
	blockedSession := blockedSessions[0].(map[string]interface{})
	assert.Equal(s.T(), float64(201), blockedSession["pid"],
		"blocked session should be PID 201")
	assert.Equal(s.T(), "Lock", blockedSession["waitEventType"],
		"blocked session should have Lock wait event type")

	// Step 3: User cancels the blocked query with a reason.
	s.T().Log("Step 3: Cancel blocked query with reason")
	cancelPath := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/sessions/201/cancel?namespace=default"
	cancelBody, err := json.Marshal(map[string]string{"reason": "blocking other transactions"})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, cancelPath, cancelBody)
	require.Equal(s.T(), http.StatusOK, rec.Code, "canceling query should succeed")

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(201), resp["pid"], "canceled PID should be 201")
	assert.Equal(s.T(), true, resp["canceled"], "query should be canceled")
	assert.Equal(s.T(), "blocking other transactions", resp["reason"],
		"cancel reason should be included in response")

	// Step 4: User reassigns the etl_service role to a different resource group.
	s.T().Log("Step 4: Reassign resource group")
	assignPath := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/workload/resource-groups/analytics/assign?namespace=default"
	assignBody, err := json.Marshal(map[string]string{"role": "etl_service"})
	require.NoError(s.T(), err)

	rec = s.doRequest(http.MethodPost, assignPath, assignBody)
	require.Equal(s.T(), http.StatusOK, rec.Code, "resource group assignment should succeed")

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "analytics", resp["group"],
		"response should contain the target resource group")
	assert.Equal(s.T(), "etl_service", resp["role"],
		"response should contain the assigned role")
	assert.Equal(s.T(), "assigned", resp["status"],
		"assignment status should be 'assigned'")
}

// TestE2E_Scenario59_FilterCombinations tests that multiple filters can be
// combined to narrow down session results.
func (s *Scenario59LiveQueryMonitorE2ESuite) TestE2E_Scenario59_FilterCombinations() {
	basePath := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/sessions?namespace=default"

	s.Run("database_and_status", func() {
		// Filter: database=warehouse AND status=running (active).
		// Expected: PID 200 (active in warehouse). PID 201 is also in warehouse
		// but is blocked (active+Lock), which still matches "running" (state=active).
		rec := s.doRequest(http.MethodGet, basePath+"&database=warehouse&status=running", nil)
		require.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions, ok := resp["sessions"].([]interface{})
		require.True(s.T(), ok)
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "warehouse", sessMap["database"])
			assert.Equal(s.T(), "active", sessMap["state"])
		}
	})

	s.Run("user_and_resource_group", func() {
		// Filter: user=analyst AND resource_group=analytics.
		// Expected: PIDs 200 and 203 (both analyst in analytics group).
		rec := s.doRequest(http.MethodGet, basePath+"&user=analyst&resource_group=analytics", nil)
		require.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions, ok := resp["sessions"].([]interface{})
		require.True(s.T(), ok)
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "analyst", sessMap["username"])
			assert.Equal(s.T(), "analytics", sessMap["resourceGroup"])
		}
	})

	s.Run("since_filter_excludes_old_sessions", func() {
		// Filter: since=10m should exclude PID 202 (started 30m ago).
		rec := s.doRequest(http.MethodGet, basePath+"&since=10m", nil)
		require.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions, ok := resp["sessions"].([]interface{})
		require.True(s.T(), ok)
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.NotEqual(s.T(), float64(202), sessMap["pid"],
				"since=10m should exclude PID 202 which started 30 minutes ago")
		}
	})
}

// TestE2E_Scenario59_CancelWithoutReason tests that canceling a query
// without providing a reason still works and does not include a reason field.
func (s *Scenario59LiveQueryMonitorE2ESuite) TestE2E_Scenario59_CancelWithoutReason() {
	cancelPath := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/sessions/200/cancel?namespace=default"

	rec := s.doRequest(http.MethodPost, cancelPath, nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(200), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	_, hasReason := resp["reason"]
	assert.False(s.T(), hasReason,
		"response should not include 'reason' field when not provided")
}

// TestE2E_Scenario59_UnauthenticatedDenied verifies that unauthenticated
// requests to session endpoints are rejected with 401.
func (s *Scenario59LiveQueryMonitorE2ESuite) TestE2E_Scenario59_UnauthenticatedDenied() {
	path := scenario59E2EPrefix + "/clusters/" + scenario59E2ECluster + "/sessions?namespace=default"

	req := httptest.NewRequest(http.MethodGet, path, nil)
	// No basic auth set.
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated request should be rejected with 401")
}
