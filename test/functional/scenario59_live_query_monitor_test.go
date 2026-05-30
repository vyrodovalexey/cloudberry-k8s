//go:build functional

package functional

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
// Scenario 59: Live Query Monitor API
// ============================================================================
//
// This scenario verifies the Live Query Monitor API endpoints:
//   - Session listing with status, database, user, resource_group, and since filters
//   - Cancel query with optional reason
//   - Resource group assignment
//   - Advanced search combining multiple filters
//
// ============================================================================

const (
	scenario59APIPrefix = "/api/v1alpha1"
	scenario59Cluster   = "test-cluster"
	scenario59User      = "admin"
	scenario59Pass      = "admin-pass"
)

// Scenario59LiveQueryMonitorSuite tests the Live Query Monitor API.
type Scenario59LiveQueryMonitorSuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario59(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario59LiveQueryMonitorSuite))
}

func (s *Scenario59LiveQueryMonitorSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario59Cluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	// Create mock DB client with test sessions covering different states,
	// databases, users, and resource groups.
	mockClient := &testutil.MockDBClient{
		ListSessionsWithResourceGroupFunc: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        100,
						Username:   "analyst",
						Database:   "mydb",
						State:      "active",
						Query:      "SELECT 1",
						QueryStart: time.Now(),
					},
					ResourceGroup: "analytics",
				},
				{
					Session: db.Session{
						PID:        101,
						Username:   "gpadmin",
						Database:   "postgres",
						State:      "idle",
						Query:      "",
						QueryStart: time.Now().Add(-10 * time.Minute),
					},
					ResourceGroup: "admin_group",
				},
				{
					Session: db.Session{
						PID:        102,
						Username:   "etl_user",
						Database:   "mydb",
						State:      "idle in transaction",
						Query:      "BEGIN",
						QueryStart: time.Now(),
					},
					ResourceGroup: "etl",
				},
				{
					Session: db.Session{
						PID:           103,
						Username:      "analyst",
						Database:      "analytics_db",
						State:         "active",
						WaitEventType: "Lock",
						Query:         "UPDATE t SET x=1",
						QueryStart:    time.Now(),
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
	store.SetCredentials(scenario59User, scenario59Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario59LiveQueryMonitorSuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an HTTP request with basic auth credentials.
func (s *Scenario59LiveQueryMonitorSuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario59User, scenario59Pass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario59LiveQueryMonitorSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// sessionsFromResp extracts the sessions array from a response map.
func (s *Scenario59LiveQueryMonitorSuite) sessionsFromResp(resp map[string]interface{}) []interface{} {
	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok, "response should contain 'sessions' array")
	return sessions
}

// --- 59a: List sessions by status filter ---

// TestFunctional_Scenario59a_ListByStatus verifies that the sessions endpoint
// correctly filters sessions by status (running, blocked, idle, and unfiltered).
func (s *Scenario59LiveQueryMonitorSuite) TestFunctional_Scenario59a_ListByStatus() {
	basePath := scenario59APIPrefix + "/clusters/" + scenario59Cluster + "/sessions?namespace=default"

	s.Run("all_sessions_no_filter", func() {
		rec := s.doRequest(http.MethodGet, basePath, nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(4), resp["total"],
			"unfiltered request should return all 4 sessions")
		sessions := s.sessionsFromResp(resp)
		assert.Len(s.T(), sessions, 4)
	})

	s.Run("status_running", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&status=running", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		// "running" maps to state="active" — PIDs 100 and 103 are active.
		sessions := s.sessionsFromResp(resp)
		assert.Equal(s.T(), float64(len(sessions)), resp["total"])
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "active", sessMap["state"],
				"status=running should only return active sessions")
		}
	})

	s.Run("status_blocked", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&status=blocked", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		// "blocked" maps to waitEventType="Lock" — only PID 103.
		sessions := s.sessionsFromResp(resp)
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "Lock", sessMap["waitEventType"],
				"status=blocked should only return Lock-waiting sessions")
		}
	})

	s.Run("status_idle", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&status=idle", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		// "idle" maps to state="idle" — only PID 101.
		sessions := s.sessionsFromResp(resp)
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "idle", sessMap["state"],
				"status=idle should only return idle sessions")
		}
	})
}

// --- 59b: Cancel query with reason ---

// TestFunctional_Scenario59b_CancelWithReason verifies that the cancel query
// endpoint accepts an optional reason and includes it in the response.
func (s *Scenario59LiveQueryMonitorSuite) TestFunctional_Scenario59b_CancelWithReason() {
	cancelPath := scenario59APIPrefix + "/clusters/" + scenario59Cluster + "/sessions/1234/cancel?namespace=default"

	s.Run("cancel_with_reason", func() {
		body, err := json.Marshal(map[string]string{"reason": "user requested"})
		require.NoError(s.T(), err)

		rec := s.doRequest(http.MethodPost, cancelPath, body)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(1234), resp["pid"],
			"response should contain the canceled PID")
		assert.Equal(s.T(), true, resp["canceled"],
			"canceled should be true")
		assert.Equal(s.T(), "user requested", resp["reason"],
			"response should include the reason when provided")
	})

	s.Run("cancel_without_reason", func() {
		rec := s.doRequest(http.MethodPost, cancelPath, nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(1234), resp["pid"],
			"response should contain the canceled PID")
		assert.Equal(s.T(), true, resp["canceled"],
			"canceled should be true")
		_, hasReason := resp["reason"]
		assert.False(s.T(), hasReason,
			"response should not include 'reason' field when not provided")
	})
}

// --- 59c: Resource group reassign ---

// TestFunctional_Scenario59c_ResourceGroupReassign verifies that the resource
// group assignment endpoint correctly assigns a role to a resource group.
func (s *Scenario59LiveQueryMonitorSuite) TestFunctional_Scenario59c_ResourceGroupReassign() {
	assignPath := scenario59APIPrefix + "/clusters/" + scenario59Cluster + "/workload/resource-groups/etl/assign?namespace=default"

	body, err := json.Marshal(map[string]string{"role": "analyst"})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, assignPath, body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "etl", resp["group"],
		"response should contain the resource group name")
	assert.Equal(s.T(), "analyst", resp["role"],
		"response should contain the assigned role")
	assert.Equal(s.T(), "assigned", resp["status"],
		"response status should be 'assigned'")
}

// --- 59d: Advanced search with multiple filters ---

// TestFunctional_Scenario59d_AdvancedSearch verifies that the sessions endpoint
// correctly filters by database, user, resource_group, and since parameters.
func (s *Scenario59LiveQueryMonitorSuite) TestFunctional_Scenario59d_AdvancedSearch() {
	basePath := scenario59APIPrefix + "/clusters/" + scenario59Cluster + "/sessions?namespace=default"

	s.Run("filter_by_database", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&database=mydb", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions := s.sessionsFromResp(resp)
		// PIDs 100 and 102 are in "mydb".
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "mydb", sessMap["database"],
				"database=mydb should only return sessions from mydb")
		}
		assert.Equal(s.T(), float64(len(sessions)), resp["total"])
	})

	s.Run("filter_by_user", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&user=analyst", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions := s.sessionsFromResp(resp)
		// PIDs 100 and 103 belong to "analyst".
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "analyst", sessMap["username"],
				"user=analyst should only return sessions from analyst")
		}
		assert.Equal(s.T(), float64(len(sessions)), resp["total"])
	})

	s.Run("filter_by_resource_group", func() {
		rec := s.doRequest(http.MethodGet, basePath+"&resource_group=analytics", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions := s.sessionsFromResp(resp)
		// PIDs 100 and 103 are in "analytics" resource group.
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.Equal(s.T(), "analytics", sessMap["resourceGroup"],
				"resource_group=analytics should only return sessions in analytics group")
		}
		assert.Equal(s.T(), float64(len(sessions)), resp["total"])
	})

	s.Run("filter_by_since", func() {
		// "since=5m" should return sessions started in the last 5 minutes.
		// PIDs 100, 102, 103 have QueryStart=time.Now() (within 5m).
		// PID 101 has QueryStart=time.Now()-10m (outside 5m window).
		rec := s.doRequest(http.MethodGet, basePath+"&since=5m", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		sessions := s.sessionsFromResp(resp)
		assert.Equal(s.T(), float64(len(sessions)), resp["total"])

		// Verify PID 101 (idle, started 10m ago) is NOT in the results.
		for _, sess := range sessions {
			sessMap := sess.(map[string]interface{})
			assert.NotEqual(s.T(), float64(101), sessMap["pid"],
				"since=5m should exclude PID 101 which started 10 minutes ago")
		}
	})
}
