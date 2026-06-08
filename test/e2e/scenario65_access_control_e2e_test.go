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
// Scenario 65: Access Control and Guest Access (E2E)
// ============================================================================
//
// Journey-style tests:
//   - Journey 1: Guest access disabled (65a) — all unauthenticated → 401
//   - Journey 2: Guest access enabled (65b) — guest GET on guest endpoints → 200
//   - Journey 3: Permission escalation (65c) — Basic → OperatorBasic → Operator
//   - Journey 4: Mixed auth — guest + authenticated in same cluster
//
// ============================================================================

const (
	scenario65E2ECluster   = "e2e-access-ctrl"
	scenario65E2ENamespace = "default"
	scenario65E2EPrefix    = "/api/v1alpha1"
	scenario65E2ERateLimit = 1000
)

func e2e65ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario65E2EPrefix, scenario65E2ECluster, endpoint, scenario65E2ENamespace)
}

// Scenario65AccessControlE2ESuite tests access control via user journeys.
type Scenario65AccessControlE2ESuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func TestE2E_Scenario65(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario65AccessControlE2ESuite))
}

func (s *Scenario65AccessControlE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 120*time.Second)
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (s *Scenario65AccessControlE2ESuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Scenario65AccessControlE2ESuite) doRequestWithAuth(
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

func (s *Scenario65AccessControlE2ESuite) doRequestNoAuth(
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

func (s *Scenario65AccessControlE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// ============================================================================
// Journey 1: Guest Access Disabled (65a)
// ============================================================================

func (s *Scenario65AccessControlE2ESuite) TestE2E_Scenario65a_GuestDisabledJourney() {
	cluster := testutil.NewClusterBuilder(scenario65E2ECluster, scenario65E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()
	cluster.Status.ActiveQueries = 10

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario65E2ERateLimit)
	defer server.Close()
	handler := server.Handler()

	// Step 1: Verify all guest-capable endpoints reject unauthenticated requests.
	s.T().Log("Step 1: Verify unauthenticated GET /queries/active → 401")
	rec := s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	s.T().Log("Step 2: Verify unauthenticated GET /metrics/exporters → 401")
	rec = s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	s.T().Log("Step 3: Verify unauthenticated GET /queries → 401")
	rec = s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	s.T().Log("Step 4: Verify unauthenticated POST /queries/1234/cancel → 401")
	rec = s.doRequestNoAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	// Step 5: Verify authenticated requests still work.
	s.T().Log("Step 5: Verify authenticated GET /queries/active → 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"),
		"admin", "admin-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(10), resp["activeQueries"])
}

// ============================================================================
// Journey 2: Guest Access Enabled (65b)
// ============================================================================

func (s *Scenario65AccessControlE2ESuite) TestE2E_Scenario65b_GuestEnabledJourney() {
	cluster := testutil.NewClusterBuilder(scenario65E2ECluster, scenario65E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 8
	cluster.Status.QueuedQueries = 3
	cluster.Status.BlockedQueries = 1

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario65E2ERateLimit)
	defer server.Close()
	handler := server.Handler()

	// Step 1: Guest can read active queries (Basic permission).
	s.T().Log("Step 1: Guest GET /queries/active → 200")
	rec := s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(8), resp["activeQueries"])

	// Step 2: Guest can read exporter health (Basic permission).
	s.T().Log("Step 2: Guest GET /metrics/exporters → 200")
	rec = s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/metrics/exporters"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp = s.decodeJSON(rec)
	assert.NotNil(s.T(), resp["exporters"])

	// Step 3: Guest cannot read queries overview (requires OperatorBasic).
	s.T().Log("Step 3: Guest GET /queries → 403")
	rec = s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// Step 4: Guest cannot POST (write operations always require auth).
	s.T().Log("Step 4: Guest POST /queries/1234/cancel → 401")
	rec = s.doRequestNoAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	// Step 5: Guest cannot POST plan-check (POST always requires auth).
	s.T().Log("Step 5: Guest POST /queries/plan-check → 401")
	rec = s.doRequestNoAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/plan-check"),
		[]byte(`{"planText":"test"}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}

// ============================================================================
// Journey 3: Permission Escalation (65c)
// ============================================================================

func (s *Scenario65AccessControlE2ESuite) TestE2E_Scenario65c_PermissionEscalationJourney() {
	cluster := testutil.NewClusterBuilder(scenario65E2ECluster, scenario65E2ENamespace).
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
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("opbasic-user", "opbasic-pass", auth.PermissionOperatorBasic)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	mockClient := &testutil.MockDBClient{
		CancelQueryFunc: func(_ context.Context, pid int32) (bool, error) {
			return true, nil
		},
		MoveQueryToResourceGroupFunc: func(_ context.Context, pid int32, targetGroup string) error {
			return nil
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	server := api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario65E2ERateLimit)
	defer server.Close()
	handler := server.Handler()

	// --- Basic user ---
	s.T().Log("Step 1: Basic user GET /queries/active → 200")
	rec := s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 2: Basic user GET /queries → 403 (requires OperatorBasic)")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"),
		"basic-user", "basic-pass", nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// --- OperatorBasic user ---
	s.T().Log("Step 3: OperatorBasic user GET /queries → 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"),
		"opbasic-user", "opbasic-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 4: OperatorBasic user GET /queries/active → 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"),
		"opbasic-user", "opbasic-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 5: OperatorBasic user POST /queries/1234/cancel → 403 (requires Operator)")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/cancel"),
		"opbasic-user", "opbasic-pass", []byte(`{}`))
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// --- Operator user ---
	s.T().Log("Step 6: Operator user POST /queries/1234/cancel → 200")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/cancel"),
		"operator-user", "operator-pass", []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.T().Log("Step 7: Operator user POST /queries/1234/move → 200")
	rec = s.doRequestWithAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/move"),
		"operator-user", "operator-pass", []byte(`{"targetGroup":"etl_group"}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code)
}

// ============================================================================
// Journey 4: Mixed Auth — Guest + Authenticated in Same Cluster
// ============================================================================

func (s *Scenario65AccessControlE2ESuite) TestE2E_Scenario65_MixedAuthJourney() {
	cluster := testutil.NewClusterBuilder(scenario65E2ECluster, scenario65E2ENamespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
		}).
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 5

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario65E2ERateLimit)
	defer server.Close()
	handler := server.Handler()

	// Step 1: Guest reads active queries.
	s.T().Log("Step 1: Guest GET /queries/active → 200")
	rec := s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	// Step 2: Authenticated viewer reads the same endpoint.
	s.T().Log("Step 2: Viewer GET /queries/active → 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries/active"),
		"viewer", "viewer-pass", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	// Step 3: Guest cannot access OperatorBasic endpoint.
	s.T().Log("Step 3: Guest GET /queries → 403")
	rec = s.doRequestNoAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code)

	// Step 4: Admin can access OperatorBasic endpoint.
	s.T().Log("Step 4: Admin GET /queries → 200")
	rec = s.doRequestWithAuth(handler, http.MethodGet, e2e65ClusterPath("/queries"),
		"admin", "admin-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	// Step 5: Guest cannot POST.
	s.T().Log("Step 5: Guest POST /queries/1234/cancel → 401")
	rec = s.doRequestNoAuth(handler, http.MethodPost, e2e65ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
}
