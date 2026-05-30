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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 66: Pause/Resume Monitor (E2E)
// ============================================================================
//
// Journey-style tests:
//   - Journey 1: Full pause/resume cycle — state -> pause -> verify stale -> resume -> verify fresh
//   - Journey 2: Stale data verification — pause -> check /queries/active has stale -> check /queries has stale
//   - Journey 3: Auth boundary — unauth pause -> 401, basic pause -> 403, operator pause -> 200
//   - Journey 4: Error handling — pause non-existent cluster -> 404
//
// ============================================================================

const (
	scenario66E2ECluster   = "e2e-pause-resume"
	scenario66E2ENamespace = "default"
	scenario66E2EPrefix    = "/api/v1alpha1"
	scenario66E2ERateLimit = 1000
)

func e2e66ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario66E2EPrefix, scenario66E2ECluster, endpoint, scenario66E2ENamespace)
}

func e2e66NonExistentClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario66E2EPrefix, "nonexistent-cluster", endpoint, scenario66E2ENamespace)
}

// Scenario66PauseResumeE2ESuite tests pause/resume monitor via user journeys.
type Scenario66PauseResumeE2ESuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func TestE2E_Scenario66(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario66PauseResumeE2ESuite))
}

func (s *Scenario66PauseResumeE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 120*time.Second)
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (s *Scenario66PauseResumeE2ESuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Scenario66PauseResumeE2ESuite) doRequestWithAuth(
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

func (s *Scenario66PauseResumeE2ESuite) doRequestNoAuth(
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

func (s *Scenario66PauseResumeE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// buildE2EServer creates a fully configured server for E2E testing.
func (s *Scenario66PauseResumeE2ESuite) buildE2EServer() (*api.Server, http.Handler) {
	cluster := testutil.NewClusterBuilder(scenario66E2ECluster, scenario66E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 8
	cluster.Status.QueuedQueries = 3
	cluster.Status.BlockedQueries = 1
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("admin-user", "admin-pass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario66E2ERateLimit)
	return server, server.Handler()
}

// ============================================================================
// Journey 1: Full Pause/Resume Cycle
// ============================================================================

func (s *Scenario66PauseResumeE2ESuite) TestE2E_Scenario66_FullPauseResumeCycle() {
	server, handler := s.buildE2EServer()
	defer server.Close()

	// Step 1: Verify initial state is not paused.
	s.T().Log("Step 1: GET /queries/monitor/state -> paused=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), false, resp["paused"])
	assert.Equal(s.T(), false, resp["stale"])

	// Step 2: Pause the monitor.
	s.T().Log("Step 2: POST /queries/monitor/pause -> status=paused")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "paused", resp["status"])
	assert.NotEmpty(s.T(), resp["pausedAt"])

	// Step 3: Verify state is paused.
	s.T().Log("Step 3: GET /queries/monitor/state -> paused=true, stale=true")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["paused"])
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])

	// Step 4: Verify active queries return stale data.
	s.T().Log("Step 4: GET /queries/active -> stale=true, data preserved")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(8), resp["activeQueries"])

	// Step 5: Resume the monitor.
	s.T().Log("Step 5: POST /queries/monitor/resume -> status=resumed")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "resumed", resp["status"])

	// Step 6: Verify state is resumed.
	s.T().Log("Step 6: GET /queries/monitor/state -> paused=false, stale=false")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), false, resp["paused"])
	assert.Equal(s.T(), false, resp["stale"])

	// Step 7: Verify active queries return fresh data.
	s.T().Log("Step 7: GET /queries/active -> no stale flag, fresh data")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Nil(s.T(), resp["stale"])
	assert.Nil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(8), resp["activeQueries"])
}

// ============================================================================
// Journey 2: Stale Data Verification
// ============================================================================

func (s *Scenario66PauseResumeE2ESuite) TestE2E_Scenario66_StaleDataVerification() {
	server, handler := s.buildE2EServer()
	defer server.Close()

	// Step 1: Pause the monitor.
	s.T().Log("Step 1: Pause monitor")
	rec := s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	// Step 2: Check /queries/active has stale flag.
	s.T().Log("Step 2: GET /queries/active -> stale=true, pausedAt set")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
	assert.Equal(s.T(), float64(8), resp["activeQueries"])
	assert.Equal(s.T(), float64(3), resp["queuedQueries"])
	assert.Equal(s.T(), float64(1), resp["blockedQueries"])

	// Step 3: Check /queries has stale flag.
	s.T().Log("Step 3: GET /queries -> stale=true, pausedAt set")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["stale"])
	assert.NotNil(s.T(), resp["pausedAt"])
}

// ============================================================================
// Journey 3: Auth Boundary
// ============================================================================

func (s *Scenario66PauseResumeE2ESuite) TestE2E_Scenario66_AuthBoundary() {
	server, handler := s.buildE2EServer()
	defer server.Close()

	// Step 1: Unauthenticated pause -> 401.
	s.T().Log("Step 1: Unauthenticated POST /queries/monitor/pause -> 401")
	rec := s.doRequestNoAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/pause"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	// Step 2: Basic user pause -> 403.
	s.T().Log("Step 2: Basic user POST /queries/monitor/pause -> 403")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/pause"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// Step 3: Operator user pause -> 200.
	s.T().Log("Step 3: Operator user POST /queries/monitor/pause -> 200")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	// Step 4: Basic user can read state (Basic permission).
	s.T().Log("Step 4: Basic user GET /queries/monitor/state -> 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66ClusterPath("/queries/monitor/state"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), true, resp["paused"])

	// Step 5: Basic user cannot resume (needs Operator).
	s.T().Log("Step 5: Basic user POST /queries/monitor/resume -> 403")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/resume"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// Step 6: Operator user can resume.
	s.T().Log("Step 6: Operator user POST /queries/monitor/resume -> 200")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66ClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

// ============================================================================
// Journey 4: Error Handling
// ============================================================================

func (s *Scenario66PauseResumeE2ESuite) TestE2E_Scenario66_ErrorHandling() {
	server, handler := s.buildE2EServer()
	defer server.Close()

	// Step 1: Pause non-existent cluster -> 404.
	s.T().Log("Step 1: POST /queries/monitor/pause for non-existent cluster -> 404")
	rec := s.doRequestWithAuth(handler, http.MethodPost, e2e66NonExistentClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	// Step 2: Resume non-existent cluster -> 404.
	s.T().Log("Step 2: POST /queries/monitor/resume for non-existent cluster -> 404")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e66NonExistentClusterPath("/queries/monitor/resume"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)

	// Step 3: Get state for non-existent cluster -> 404.
	s.T().Log("Step 3: GET /queries/monitor/state for non-existent cluster -> 404")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e66NonExistentClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}
