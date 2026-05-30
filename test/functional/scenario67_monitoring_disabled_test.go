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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 67: Monitoring Disabled and planCollection Disabled
// ============================================================================
//
// This scenario verifies behavior when query monitoring is disabled and when
// planCollection is disabled:
//   - 67a: queryMonitoring.enabled=false — monitoring endpoints return
//          {"monitoringEnabled": false, ...}, non-monitoring endpoints still work
//   - 67b: planCollection=false — exporter args do NOT include --plan-collection
//
// ============================================================================

const (
	scenario67APIPrefix = "/api/v1alpha1"
	scenario67Cluster   = "monitoring-disabled-cluster"
	scenario67Namespace = "default"
	scenario67RateLimit = 1000
)

// scenario67ClusterPath returns the base path for a cluster endpoint.
func scenario67ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario67APIPrefix, scenario67Cluster, endpoint, scenario67Namespace)
}

// Scenario67MonitoringDisabledSuite tests monitoring disabled functionality.
type Scenario67MonitoringDisabledSuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
}

func TestFunctional_Scenario67(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario67MonitoringDisabledSuite))
}

func (s *Scenario67MonitoringDisabledSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// buildServer creates an API server with the given cluster and credential store.
func (s *Scenario67MonitoringDisabledSuite) buildServer(
	cluster *cbv1alpha1.CloudberryCluster,
	store *auth.InMemoryCredentialStore,
) (*api.Server, http.Handler) {
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario67RateLimit)
	return server, server.Handler()
}

// doRequestWithAuth creates and executes an authenticated HTTP request.
func (s *Scenario67MonitoringDisabledSuite) doRequestWithAuth(
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

// decodeJSON decodes the response body into a map.
func (s *Scenario67MonitoringDisabledSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// newMonitoringDisabledClusterAndStore creates a cluster with monitoring disabled.
func (s *Scenario67MonitoringDisabledSuite) newMonitoringDisabledClusterAndStore() (
	*cbv1alpha1.CloudberryCluster, *auth.InMemoryCredentialStore,
) {
	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 3
	cluster.Status.QueuedQueries = 1
	cluster.Status.BlockedQueries = 0
	// Monitoring explicitly disabled.
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: false}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("admin-user", "admin-pass", auth.PermissionAdmin)

	return cluster, store
}

// newMonitoringEnabledClusterAndStore creates a cluster with monitoring enabled.
func (s *Scenario67MonitoringDisabledSuite) newMonitoringEnabledClusterAndStore() (
	*cbv1alpha1.CloudberryCluster, *auth.InMemoryCredentialStore,
) {
	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
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

// assertMonitoringDisabledResponse verifies the standard monitoring disabled response.
func (s *Scenario67MonitoringDisabledSuite) assertMonitoringDisabledResponse(resp map[string]interface{}) {
	s.T().Helper()
	assert.Equal(s.T(), false, resp["monitoringEnabled"],
		"monitoringEnabled should be false")
	assert.Equal(s.T(), "query monitoring is not enabled for this cluster", resp["message"],
		"message should indicate monitoring is disabled")
}

// ============================================================================
// 67a: queryMonitoring.enabled=false — monitoring endpoints return disabled
// ============================================================================

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_QueriesReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /queries with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_ActiveQueriesReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /queries/active with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_QueryHistoryReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /queries/history with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries/history"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_ExporterHealthReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /metrics/exporters with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/metrics/exporters"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_MonitorStateReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /queries/monitor/state with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries/monitor/state"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_PauseReturnsMonitoringDisabled() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: POST /queries/monitor/pause with monitoring disabled returns monitoringEnabled=false")
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario67ClusterPath("/queries/monitor/pause"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_SessionsStillWork() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: GET /sessions still works when monitoring is disabled")
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/sessions"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	// Sessions endpoint should NOT return monitoringEnabled=false.
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"sessions endpoint should not have monitoringEnabled field")
	assert.NotNil(s.T(), resp["sessions"],
		"sessions endpoint should return sessions field")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_PlanCheckStillWorks() {
	cluster, store := s.newMonitoringDisabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	s.T().Log("67a: POST /queries/plan-check still works when monitoring is disabled")
	planBody := []byte(`{"planText":"Index Scan using idx_orders_id on orders  (cost=0.29..8.31 rows=1 width=100) (actual time=0.020..0.025 rows=1 loops=1)\n  Index Cond: (id = 42)\nPlanning Time: 0.100 ms\nExecution Time: 0.050 ms"}`)
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario67ClusterPath("/queries/plan-check"),
		"operator-user", "operator-pass", planBody)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	// Plan-check should NOT return monitoringEnabled=false.
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"plan-check endpoint should not have monitoringEnabled field")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_EnableDisableToggle() {
	s.T().Log("67a: Enable monitoring -> verify data -> disable -> verify disabled -> re-enable -> verify data")

	// Step 1: Start with monitoring enabled.
	cluster, store := s.newMonitoringEnabledClusterAndStore()
	server, handler := s.buildServer(cluster, store)

	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(5), resp["activeQueries"],
		"with monitoring enabled, activeQueries should be returned")
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"with monitoring enabled, monitoringEnabled field should not be present")
	server.Close()

	// Step 2: Disable monitoring.
	clusterDisabled, storeDisabled := s.newMonitoringDisabledClusterAndStore()
	serverDisabled, handlerDisabled := s.buildServer(clusterDisabled, storeDisabled)

	rec = s.doRequestWithAuth(handlerDisabled, http.MethodGet, scenario67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
	serverDisabled.Close()

	// Step 3: Re-enable monitoring.
	clusterReEnabled, storeReEnabled := s.newMonitoringEnabledClusterAndStore()
	serverReEnabled, handlerReEnabled := s.buildServer(clusterReEnabled, storeReEnabled)
	defer serverReEnabled.Close()

	rec = s.doRequestWithAuth(handlerReEnabled, http.MethodGet, scenario67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(5), resp["activeQueries"],
		"after re-enabling, activeQueries should be returned again")
	assert.Nil(s.T(), resp["monitoringEnabled"],
		"after re-enabling, monitoringEnabled field should not be present")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_NilQueryMonitoring() {
	s.T().Log("67a: Cluster with nil queryMonitoring spec should return monitoring disabled")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	// QueryMonitoring is nil (not set at all).
	cluster.Spec.QueryMonitoring = nil

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario67ClusterPath("/queries/active"),
		"operator-user", "operator-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	s.assertMonitoringDisabledResponse(resp)
}

// ============================================================================
// 67b: planCollection=false — exporter args verification
// ============================================================================

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_PlanCollectionEnabled() {
	s.T().Log("67b: With planCollection=true, exporter args include --plan-collection")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:        true,
		PlanCollection: true,
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containers := b.BuildExporterSidecarContainers(cluster)
	require.NotEmpty(s.T(), containers, "should have at least one exporter container")

	found := false
	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--plan-collection" {
					found = true
					break
				}
			}
		}
	}
	assert.True(s.T(), found, "exporter args should include --plan-collection when planCollection=true")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_PlanCollectionDisabled() {
	s.T().Log("67b: With planCollection=false, exporter args do NOT include --plan-collection")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:        true,
		PlanCollection: false,
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containers := b.BuildExporterSidecarContainers(cluster)
	require.NotEmpty(s.T(), containers, "should have at least one exporter container")

	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				assert.NotEqual(s.T(), "--plan-collection", arg,
					"exporter args should NOT include --plan-collection when planCollection=false")
			}
		}
	}
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_HistoryRetentionArg() {
	s.T().Log("67b: With historyRetention set, exporter args include --history-retention")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		PlanCollection:   false,
		HistoryRetention: "90d",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containers := b.BuildExporterSidecarContainers(cluster)
	require.NotEmpty(s.T(), containers, "should have at least one exporter container")

	found := false
	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--history-retention=90d" {
					found = true
					break
				}
			}
		}
	}
	assert.True(s.T(), found, "exporter args should include --history-retention=90d")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_DefaultSamplingInterval() {
	s.T().Log("67b: Default sampling interval is used when not specified")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:        true,
		PlanCollection: false,
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containers := b.BuildExporterSidecarContainers(cluster)
	require.NotEmpty(s.T(), containers, "should have at least one exporter container")

	found := false
	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--sampling-interval=5s" {
					found = true
					break
				}
			}
		}
	}
	assert.True(s.T(), found, "exporter args should include default --sampling-interval=5s")
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_CustomSlowQueryThreshold() {
	s.T().Log("67b: Custom slow query threshold is passed to exporter")

	cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
		WithFinalizer().
		WithStatusReady().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		PlanCollection:     false,
		SlowQueryThreshold: "2000ms",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
			},
		},
	}

	b := &builder.DefaultBuilder{}
	containers := b.BuildExporterSidecarContainers(cluster)
	require.NotEmpty(s.T(), containers, "should have at least one exporter container")

	found := false
	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			for _, arg := range c.Args {
				if arg == "--slow-query-threshold=2000ms" {
					found = true
					break
				}
			}
		}
	}
	assert.True(s.T(), found, "exporter args should include --slow-query-threshold=2000ms")
}

// ============================================================================
// Data-driven tests using MonitoringDisabledCases
// ============================================================================

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67a_DataDrivenCases() {
	for _, tc := range cases.MonitoringDisabledCases() {
		if tc.SubScenario != "67a" {
			continue
		}
		// Skip cases that need special setup (plan-check needs body, sessions needs no monitoring check).
		if tc.Step == "plan_check_ok" || tc.Step == "sessions_ok" {
			continue
		}
		s.Run(tc.Name, func() {
			cluster, store := s.newMonitoringDisabledClusterAndStore()
			server, handler := s.buildServer(cluster, store)
			defer server.Close()

			s.T().Log("Test case:", tc.Description)

			rec := s.doRequestWithAuth(handler, tc.Method, scenario67ClusterPath(tc.Path),
				tc.AuthUser, tc.AuthPass, nil)
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"%s %s should return %d", tc.Method, tc.Path, tc.ExpectedStatus)

			if tc.ExpectMonOff {
				resp := s.decodeJSON(rec)
				s.assertMonitoringDisabledResponse(resp)
			}
		})
	}
}

func (s *Scenario67MonitoringDisabledSuite) TestFunctional_Scenario67b_DataDrivenCases() {
	for _, tc := range cases.MonitoringDisabledCases() {
		if tc.SubScenario != "67b" {
			continue
		}
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)

			planCollection := tc.ExpectPlanArg
			cluster := testutil.NewClusterBuilder(scenario67Cluster, scenario67Namespace).
				WithFinalizer().
				WithStatusReady().
				Build()
			cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
				Enabled:          true,
				PlanCollection:   planCollection,
				HistoryRetention: "30d",
				Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
					CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
						Enabled: true,
						Image:   "cloudberry-query-exporter:1.0.0",
					},
				},
			}

			b := &builder.DefaultBuilder{}
			containers := b.BuildExporterSidecarContainers(cluster)
			require.NotEmpty(s.T(), containers, "should have at least one exporter container")

			hasPlanArg := false
			for _, c := range containers {
				if c.Name == "cloudberry-query-exporter" {
					for _, arg := range c.Args {
						if arg == "--plan-collection" {
							hasPlanArg = true
							break
						}
					}
				}
			}

			if tc.Step == "plan_enabled" {
				assert.True(s.T(), hasPlanArg,
					"--plan-collection should be present when planCollection=true")
			} else if tc.Step == "plan_disabled" {
				assert.False(s.T(), hasPlanArg,
					"--plan-collection should NOT be present when planCollection=false")
			}
		})
	}
}
