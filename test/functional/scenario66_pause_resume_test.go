//go:build functional

package functional

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 66: Pause/Resume Monitor
// ============================================================================
//
// This scenario verifies the pause/resume monitor functionality:
//   - 66a: Full pause/resume lifecycle — initial state, pause, stale data,
//          resume, fresh data, idempotent operations, auth boundaries
//
// ============================================================================

const (
	scenario66APIPrefix = "/api/v1alpha1"
	scenario66Cluster   = "pause-resume-cluster"
	scenario66Namespace = "default"
	scenario66RateLimit = 1000
)

// scenario66ClusterPath returns the base path for a cluster endpoint.
func scenario66ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario66APIPrefix, scenario66Cluster, endpoint, scenario66Namespace)
}

// scenario66NonExistentClusterPath returns a path for a non-existent cluster.
func scenario66NonExistentClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario66APIPrefix, "nonexistent-cluster", endpoint, scenario66Namespace)
}

// Scenario66PauseResumeSuite tests pause/resume monitor functionality.
type Scenario66PauseResumeSuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
}

func TestFunctional_Scenario66(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario66PauseResumeSuite))
}

// buildServer creates an API server with the given cluster and credential store.
func (s *Scenario66PauseResumeSuite) buildServer(
	cluster *cbv1alpha1.CloudberryCluster,
	store *auth.InMemoryCredentialStore,
) (*api.Server, http.Handler) {
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario66RateLimit)
	return server, server.Handler()
}

// doRequestWithAuth creates and executes an authenticated HTTP request.
func (s *Scenario66PauseResumeSuite) doRequestWithAuth(
	handler http.Handler, method, path, user, pass string, body []byte,
) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth.
func (s *Scenario66PauseResumeSuite) doRequestNoAuth(
	handler http.Handler, method, path string, body []byte,
) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario66PauseResumeSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func (s *Scenario66PauseResumeSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newClusterAndStore creates a test cluster and credential store for scenario 66.
func (s *Scenario66PauseResumeSuite) newClusterAndStore() (*cbv1alpha1.CloudberryCluster, *auth.InMemoryCredentialStore) {
	cluster := testutil.NewClusterBuilder(scenario66Cluster, scenario66Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("admin-user", "admin-pass", auth.PermissionAdmin)

	return cluster, store
}

// ============================================================================
// 66a: Pause and Resume lifecycle
// ============================================================================

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_InitialState() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 1: Initial state — GET /queries/monitor/state should return paused=false, stale=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), false, resp["paused"])
	assert.Equal(s.T(), false, resp["stale"])
	assert.Nil(s.T(), resp["pausedAt"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_PauseMonitor() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 2: Pause — POST /queries/monitor/pause should return status=paused")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "paused", resp["status"])
	assert.NotEmpty(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), "Query monitor paused", resp["message"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_VerifyPausedState() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause first.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 3: Verify paused state — GET /queries/monitor/state should return paused=true, stale=true")
	rec = s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["paused"])
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_StaleActiveQueries() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause first.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 4: Stale data — GET /queries/active should return stale=true with pausedAt")
	rec = s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(5), resp["activeQueries"])
	assert.Equal(s.T(), float64(2), resp["queuedQueries"])
	assert.Equal(s.T(), float64(1), resp["blockedQueries"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_StaleQueries() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause first.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 5: Stale data on /queries — GET /queries should return stale=true")
	rec = s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(5), resp["activeQueries"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_ResumeMonitor() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause first.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 7: Resume — POST /queries/monitor/resume should return status=resumed")
	rec = s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "resumed", resp["status"])
	assert.Equal(s.T(), "Query monitor resumed", resp["message"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_VerifyResumedState() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause then resume.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	rec = s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 8: Verify resumed — GET /queries/monitor/state should return paused=false, stale=false")
	rec = s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), false, resp["paused"])
	assert.Equal(s.T(), false, resp["stale"])
	assert.Nil(s.T(), resp["pausedAt"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_FreshDataAfterResume() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Pause then resume.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	rec = s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 9: Fresh data — GET /queries/active should return data without stale flag")
	rec = s.doRequestWithAuth(handler, http.MethodGet, scenario66ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Nil(s.T(), resp["stale"])
	assert.Nil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(5), resp["activeQueries"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_IdempotentPause() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 10: Idempotent pause — pausing twice should both succeed")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	rec = s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "paused", resp["status"])
	assert.Contains(s.T(), resp["message"], "already paused")
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_ResumeWithoutPause() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 11: Resume without pause — should succeed")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "resumed", resp["status"])
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_UnauthenticatedPause() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 12: Unauthenticated — POST pause without auth should return 401")
	rec := s.doRequestNoAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_BasicUserPauseForbidden() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 13: Permission — Basic user POST pause should return 403")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66ClusterPath("/queries/monitor/pause"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)
}

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_ClusterNotFound() {
	cluster, store := s.newClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("Step 14: Cluster not found — POST pause for non-existent cluster should return 404")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario66NonExistentClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

// ============================================================================
// Data-driven tests using PauseResumeCases
// ============================================================================

func (s *Scenario66PauseResumeSuite) TestFunctional_Scenario66a_DataDrivenCases() {
	for _, tc := range cases.PauseResumeCases() {
		if tc.SubScenario != "66a" {
			continue
		}
		// Skip lifecycle-dependent cases (handled by individual tests above).
		if tc.Step == "verify_paused" || tc.Step == "stale_active" || tc.Step == "stale_queries" ||
			tc.Step == "resume" || tc.Step == "verify_resumed" || tc.Step == "fresh_active" ||
			tc.Step == "idempotent_pause" || tc.Step == "resume_no_pause" {
			continue
		}
		s.Run(tc.Name, func() {
			cluster, store := s.newClusterAndStore()
			server, handler := s.buildServer(cluster, store)
			defer server.Close()

			s.T().Log("Test case:", tc.Description)

			var rec *httptest.ResponseRecorder
			if tc.Step == "not_found" {
				if tc.AuthUser != "" {
					rec = s.doRequestWithAuth(handler, tc.Method, scenario66NonExistentClusterPath(tc.Path),
						tc.AuthUser, tc.AuthPass, nil)
				} else {
					rec = s.doRequestNoAuth(handler, tc.Method, scenario66NonExistentClusterPath(tc.Path), nil)
				}
			} else if tc.AuthUser != "" {
				rec = s.doRequestWithAuth(handler, tc.Method, scenario66ClusterPath(tc.Path),
					tc.AuthUser, tc.AuthPass, nil)
			} else {
				rec = s.doRequestNoAuth(handler, tc.Method, scenario66ClusterPath(tc.Path), nil)
			}
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"%s %s as %s should return %d", tc.Method, tc.Path, tc.AuthUser, tc.ExpectedStatus)
		})
	}
}
