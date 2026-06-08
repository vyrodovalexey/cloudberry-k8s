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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 67: Monitoring Disabled and planCollection Disabled (E2E)
// ============================================================================
//
// Journey-style tests:
//   - Journey 1: Monitoring disable/enable cycle — enable -> verify data ->
//                disable -> verify disabled response -> re-enable -> verify data
//   - Journey 2: planCollection disable behavior — verify exporter args with
//                planCollection=true vs planCollection=false
//   - Journey 3: Error handling — monitoring disabled endpoints return correct
//                response format, non-monitoring endpoints unaffected
//
// ============================================================================

const (
	scenario67E2ECluster   = "e2e-monitoring-disabled"
	scenario67E2ENamespace = "default"
	scenario67E2EPrefix    = "/api/v1alpha1"
	scenario67E2ERateLimit = 1000
)

func e2e67ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario67E2EPrefix, scenario67E2ECluster, endpoint, scenario67E2ENamespace)
}

// Scenario67MonitoringDisabledE2ESuite tests monitoring disabled via user journeys.
type Scenario67MonitoringDisabledE2ESuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func TestE2E_Scenario67(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario67MonitoringDisabledE2ESuite))
}

func (s *Scenario67MonitoringDisabledE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 120*time.Second)
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (s *Scenario67MonitoringDisabledE2ESuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Scenario67MonitoringDisabledE2ESuite) doRequestWithAuth(
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

func (s *Scenario67MonitoringDisabledE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// assertMonitoringDisabledResponse verifies the standard monitoring disabled response.
func (s *Scenario67MonitoringDisabledE2ESuite) assertMonitoringDisabledResponse(resp map[string]interface{}) {
	s.T().Helper()
	assert.Equal(s.T(), false, resp["monitoringEnabled"],
		"monitoringEnabled should be false")
	assert.Equal(s.T(), "query monitoring is not enabled for this cluster", resp["message"],
		"message should indicate monitoring is disabled")
}

// buildE2EServerWithMonitoring creates a server with monitoring enabled or disabled.
func (s *Scenario67MonitoringDisabledE2ESuite) buildE2EServerWithMonitoring(enabled bool) (*api.Server, http.Handler) {
	cluster := testutil.NewClusterBuilder(scenario67E2ECluster, scenario67E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 10
	cluster.Status.QueuedQueries = 4
	cluster.Status.BlockedQueries = 2
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: enabled}

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("admin-user", "admin-pass", auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario67E2ERateLimit)
	return server, server.Handler()
}

// ============================================================================
// Journey 1: Monitoring Disable/Enable Cycle
// ============================================================================

func (s *Scenario67MonitoringDisabledE2ESuite) TestE2E_Scenario67_MonitoringDisableEnableCycle() {
	// Step 1: Start with monitoring enabled — verify data is returned.
	s.T().Log("Step 1: Monitoring enabled — GET /queries/active returns data")
	serverEnabled, handlerEnabled := s.buildE2EServerWithMonitoring(true)

	rec := s.doRequestWithAuth(handlerEnabled, http.MethodGet, e2e67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])
	assert.Equal(s.T(), float64(4), resp["queuedQueries"])
	assert.Equal(s.T(), float64(2), resp["blockedQueries"])
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"monitoringEnabled should not be present when monitoring is enabled")
	serverEnabled.Close()

	// Step 2: Disable monitoring — verify disabled response.
	s.T().Log("Step 2: Monitoring disabled — GET /queries/active returns monitoringEnabled=false")
	serverDisabled, handlerDisabled := s.buildE2EServerWithMonitoring(false)

	rec = s.doRequestWithAuth(handlerDisabled, http.MethodGet, e2e67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)

	// Step 3: Verify all monitoring endpoints return disabled response.
	s.T().Log("Step 3: All monitoring endpoints return monitoringEnabled=false")
	monitoringEndpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/queries"},
		{http.MethodGet, "/queries/active"},
		{http.MethodGet, "/queries/history"},
		{http.MethodGet, "/metrics/exporters"},
		{http.MethodGet, "/queries/monitor/state"},
		{http.MethodPost, "/queries/monitor/pause"},
	}

	for _, ep := range monitoringEndpoints {
		rec = s.doRequestWithAuth(handlerDisabled, ep.method, e2e67ClusterPath(ep.path),
			"operator-user", "operator-pass", nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"%s %s should return 200", ep.method, ep.path)
		resp = s.decodeJSON(rec)
		s.assertMonitoringDisabledResponse(resp)
	}
	serverDisabled.Close()

	// Step 4: Re-enable monitoring — verify data is returned again.
	s.T().Log("Step 4: Re-enable monitoring — GET /queries/active returns data again")
	serverReEnabled, handlerReEnabled := s.buildE2EServerWithMonitoring(true)
	defer serverReEnabled.Close()

	rec = s.doRequestWithAuth(handlerReEnabled, http.MethodGet, e2e67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])
	assert.Nil(s.T(), resp["monitoringEnabled"])
}

// ============================================================================
// Journey 2: planCollection Disable Behavior
// ============================================================================

func (s *Scenario67MonitoringDisabledE2ESuite) TestE2E_Scenario67_PlanCollectionDisableBehavior() {
	// Step 1: Build exporter with planCollection=true — verify --plan-collection arg.
	s.T().Log("Step 1: planCollection=true — exporter args include --plan-collection")
	clusterWithPlan := testutil.NewClusterBuilder(scenario67E2ECluster, scenario67E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	clusterWithPlan.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		PlanCollection:   true,
		HistoryRetention: "30d",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containersWithPlan := b.BuildExporterSidecarContainers(clusterWithPlan)
	require.NotEmpty(s.T(), containersWithPlan)

	hasPlanArg := false
	hasRetentionArg := false
	for _, c := range containersWithPlan {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--plan-collection" {
					hasPlanArg = true
				}
				if arg == "--history-retention=30d" {
					hasRetentionArg = true
				}
			}
		}
	}
	assert.True(s.T(), hasPlanArg, "--plan-collection should be present")
	assert.True(s.T(), hasRetentionArg, "--history-retention=30d should be present")

	// Step 2: Build exporter with planCollection=false — verify no --plan-collection arg.
	s.T().Log("Step 2: planCollection=false — exporter args do NOT include --plan-collection")
	clusterNoPlan := testutil.NewClusterBuilder(scenario67E2ECluster, scenario67E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	clusterNoPlan.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		PlanCollection:   false,
		HistoryRetention: "30d",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	containersNoPlan := b.BuildExporterSidecarContainers(clusterNoPlan)
	require.NotEmpty(s.T(), containersNoPlan)

	for _, c := range containersNoPlan {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				assert.NotEqual(s.T(), "--plan-collection", arg,
					"--plan-collection should NOT be present when planCollection=false")
			}
		}
	}

	// Step 3: Verify other args are still present.
	s.T().Log("Step 3: Other exporter args still present when planCollection=false")
	hasListenAddr := false
	hasSamplingInterval := false
	hasSlowQueryThreshold := false
	for _, c := range containersNoPlan {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--listen-address=:9188" {
					hasListenAddr = true
				}
				if arg == "--sampling-interval=5s" {
					hasSamplingInterval = true
				}
				if arg == "--slow-query-threshold=1000ms" {
					hasSlowQueryThreshold = true
				}
			}
		}
	}
	assert.True(s.T(), hasListenAddr, "--listen-address should be present")
	assert.True(s.T(), hasSamplingInterval, "--sampling-interval should be present")
	assert.True(s.T(), hasSlowQueryThreshold, "--slow-query-threshold should be present")
}

// ============================================================================
// Journey 3: Error Handling — Non-monitoring endpoints unaffected
// ============================================================================

func (s *Scenario67MonitoringDisabledE2ESuite) TestE2E_Scenario67_ErrorHandling() {
	serverDisabled, handlerDisabled := s.buildE2EServerWithMonitoring(false)
	defer serverDisabled.Close()

	// Step 1: Non-monitoring endpoints still work.
	s.T().Log("Step 1: GET /sessions still works when monitoring is disabled")
	rec := s.doRequestWithAuth(handlerDisabled, http.MethodGet, e2e67ClusterPath("/sessions"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"sessions endpoint should not have monitoringEnabled field")
	assert.NotNil(s.T(), resp["sessions"],
		"sessions endpoint should return sessions field")

	// Step 2: Plan-check still works.
	s.T().Log("Step 2: POST /queries/plan-check still works when monitoring is disabled")
	planBody := []byte(`{"planText":"Index Scan using idx_orders_id on orders  (cost=0.29..8.31 rows=1 width=100) (actual time=0.020..0.025 rows=1 loops=1)\n  Index Cond: (id = 42)\nPlanning Time: 0.100 ms\nExecution Time: 0.050 ms"}`)
	rec = s.doRequestWithAuth(handlerDisabled, http.MethodPost, e2e67ClusterPath("/queries/plan-check"),
		"operator-user", "operator-pass", planBody)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"plan-check should not have monitoringEnabled field")

	// Step 3: Cluster info endpoints still work.
	s.T().Log("Step 3: GET /clusters/{name}/status still works when monitoring is disabled")
	statusPath := fmt.Sprintf("%s/clusters/%s/status?namespace=%s",
		scenario67E2EPrefix, scenario67E2ECluster, scenario67E2ENamespace)
	rec = s.doRequestWithAuth(handlerDisabled, http.MethodGet, statusPath,
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["status"],
		"cluster status endpoint should return status field")

	// Step 4: Monitoring disabled response has correct format.
	s.T().Log("Step 4: Monitoring disabled response has exactly 2 fields")
	rec = s.doRequestWithAuth(handlerDisabled, http.MethodGet, e2e67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Len(s.T(), resp, 2,
		"monitoring disabled response should have exactly 2 fields (monitoringEnabled, message)")
	assert.Equal(s.T(), false, resp["monitoringEnabled"])
	assert.Equal(s.T(), "query monitoring is not enabled for this cluster", resp["message"])

	// Step 5: Verify Content-Type is application/json.
	s.T().Log("Step 5: Monitoring disabled response has correct Content-Type")
	assert.Contains(s.T(), rec.Header().Get("Content-Type"), "application/json",
		"Content-Type should be application/json")
}
