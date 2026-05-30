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
// Scenario 65: Access Control and Guest Access
// ============================================================================
//
// This scenario verifies access control and guest access behavior:
//   - 65a: guestAccess=false (default) — all unauthenticated requests → 401
//   - 65b: guestAccess=true — unauthenticated GET on guest endpoints → 200,
//          POST always → 401, higher-permission GET → 403
//   - 65c: Permission enforcement — Basic, OperatorBasic, Operator users
//          get correct access/denial based on endpoint permission requirements
//
// ============================================================================

const (
	scenario65APIPrefix = "/api/v1alpha1"
	scenario65Cluster   = "access-ctrl-cluster"
	scenario65Namespace = "default"
	scenario65RateLimit = 1000
)

// scenario65ClusterPath returns the base path for a cluster endpoint.
func scenario65ClusterPath(endpoint string) string {
	return fmt.Sprintf("%s/clusters/%s%s?namespace=%s",
		scenario65APIPrefix, scenario65Cluster, endpoint, scenario65Namespace)
}

// Scenario65AccessControlSuite tests access control and guest access.
type Scenario65AccessControlSuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
}

func TestFunctional_Scenario65(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario65AccessControlSuite))
}

// buildServer creates an API server with the given cluster and credential store.
func (s *Scenario65AccessControlSuite) buildServer(
	cluster *cbv1alpha1.CloudberryCluster,
	store *auth.InMemoryCredentialStore,
) (*api.Server, http.Handler) {
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, scenario65RateLimit)
	return server, server.Handler()
}

// doRequestWithAuth creates and executes an authenticated HTTP request.
func (s *Scenario65AccessControlSuite) doRequestWithAuth(
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
func (s *Scenario65AccessControlSuite) doRequestNoAuth(
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
func (s *Scenario65AccessControlSuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func (s *Scenario65AccessControlSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// ============================================================================
// 65a: guestAccess=false (default) — all unauthenticated requests → 401
// ============================================================================

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65a_GuestAccessDisabled() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()
	// guestAccess is false by default (QueryMonitoring.GuestAccess not set).
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	for _, tc := range cases.AccessControlCases() {
		if tc.SubScenario != "65a" {
			continue
		}
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)
			rec := s.doRequestNoAuth(handler, tc.Method, scenario65ClusterPath(tc.Path), nil)
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"unauthenticated %s %s should return %d", tc.Method, tc.Path, tc.ExpectedStatus)
		})
	}
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65a_ActiveQueries_Unauthenticated() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated GET /queries/active should return 401 when guestAccess=false")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65a_ExporterHealth_Unauthenticated() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
		}).
		Build()

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated GET /metrics/exporters should return 401 when guestAccess=false")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65a_CancelQuery_Unauthenticated() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodPost, scenario65ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated POST /queries/{pid}/cancel should return 401")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65a_ListQueries_Unauthenticated() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated GET /queries should return 401 when guestAccess=false")
}

// ============================================================================
// 65b: guestAccess=true — guest identity with PermissionBasic
// ============================================================================

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestAccessEnabled() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringExporters(&cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9187},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9188},
			NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9100},
		}).
		Build()
	// Enable guest access.
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	for _, tc := range cases.AccessControlCases() {
		if tc.SubScenario != "65b" {
			continue
		}
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)
			var rec *httptest.ResponseRecorder
			if tc.AuthUser != "" {
				rec = s.doRequestWithAuth(handler, tc.Method, scenario65ClusterPath(tc.Path),
					tc.AuthUser, tc.AuthPass, nil)
			} else {
				rec = s.doRequestNoAuth(handler, tc.Method, scenario65ClusterPath(tc.Path), nil)
			}
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"%s %s (auth=%s) should return %d", tc.Method, tc.Path, tc.AuthUser, tc.ExpectedStatus)
		})
	}
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestGET_ActiveQueries_200() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.Enabled = true
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/queries/active"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"guest GET /queries/active should return 200 when guestAccess=true")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(5), resp["activeQueries"])
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestGET_ExporterHealth_200() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
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

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/metrics/exporters"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"guest GET /metrics/exporters should return 200 when guestAccess=true")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestGET_Queries_403() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// GET /queries requires OperatorBasic, guest has Basic → 403
	rec := s.doRequestNoAuth(handler, http.MethodGet, scenario65ClusterPath("/queries"), nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code,
		"guest GET /queries should return 403 (requires OperatorBasic, guest has Basic)")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestPOST_Cancel_401() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// POST always requires auth regardless of guestAccess.
	rec := s.doRequestNoAuth(handler, http.MethodPost, scenario65ClusterPath("/queries/1234/cancel"), []byte(`{}`))
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"guest POST /queries/{pid}/cancel should return 401 (write ops always require auth)")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_GuestPOST_PlanCheck_401() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.GuestAccess = true

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	body := []byte(`{"planText":"Seq Scan on t (cost=0..1 rows=1 width=1)"}`)
	rec := s.doRequestNoAuth(handler, http.MethodPost, scenario65ClusterPath("/queries/plan-check"), body)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"guest POST /queries/plan-check should return 401 (POST always requires auth)")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65b_AuthenticatedStillWorks() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	if cluster.Spec.QueryMonitoring == nil {
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{}
	}
	cluster.Spec.QueryMonitoring.Enabled = true
	cluster.Spec.QueryMonitoring.GuestAccess = true
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// Authenticated request should still work when guestAccess=true.
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario65ClusterPath("/queries/active"),
		"admin", "admin-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"authenticated GET /queries/active should return 200 when guestAccess=true")
}

// ============================================================================
// 65c: Permission enforcement — different permission levels
// ============================================================================

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_PermissionEnforcement() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
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

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("basic-user", "basic-pass", auth.PermissionBasic)
	store.SetCredentials("opbasic-user", "opbasic-pass", auth.PermissionOperatorBasic)
	store.SetCredentials("operator-user", "operator-pass", auth.PermissionOperator)
	store.SetCredentials("admin-user", "admin-pass", auth.PermissionAdmin)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	for _, tc := range cases.AccessControlCases() {
		if tc.SubScenario != "65c" {
			continue
		}
		s.Run(tc.Name, func() {
			s.T().Log("Test case:", tc.Description)
			var body []byte
			if tc.Body != "" {
				body = []byte(tc.Body)
			}
			rec := s.doRequestWithAuth(handler, tc.Method, scenario65ClusterPath(tc.Path),
				tc.AuthUser, tc.AuthPass, body)
			assert.Equal(s.T(), tc.ExpectedStatus, rec.Code,
				"%s %s as %s should return %d", tc.Method, tc.Path, tc.AuthUser, tc.ExpectedStatus)
		})
	}
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_BasicUser_ActiveQueries_200() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario65ClusterPath("/queries/active"),
		"viewer", "viewer-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"Basic user GET /queries/active should return 200")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_BasicUser_Queries_403() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// GET /queries requires OperatorBasic.
	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario65ClusterPath("/queries"),
		"viewer", "viewer-pass", nil)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code,
		"Basic user GET /queries should return 403 (requires OperatorBasic)")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_OperatorBasicUser_Queries_200() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()
	cluster.Status.ActiveQueries = 5

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("opbasic", "opbasic-pass", auth.PermissionOperatorBasic)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	rec := s.doRequestWithAuth(handler, http.MethodGet, scenario65ClusterPath("/queries"),
		"opbasic", "opbasic-pass", nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"OperatorBasic user GET /queries should return 200")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_OperatorBasicUser_Cancel_403() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("opbasic", "opbasic-pass", auth.PermissionOperatorBasic)

	server, handler := s.buildServer(cluster, store)
	defer server.Close()

	// POST /queries/{pid}/cancel requires Operator.
	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario65ClusterPath("/queries/1234/cancel"),
		"opbasic", "opbasic-pass", []byte(`{}`))
	assert.Equal(s.T(), http.StatusForbidden, rec.Code,
		"OperatorBasic user POST /queries/{pid}/cancel should return 403 (requires Operator)")
}

func (s *Scenario65AccessControlSuite) TestFunctional_Scenario65c_OperatorUser_Cancel_200() {
	cluster := testutil.NewClusterBuilder(scenario65Cluster, scenario65Namespace).
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials("operator", "operator-pass", auth.PermissionOperator)

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	mockClient := &testutil.MockDBClient{
		CancelQueryFunc: func(_ context.Context, pid int32) (bool, error) {
			return true, nil
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	server := api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, scenario65RateLimit)
	defer server.Close()
	handler := server.Handler()

	rec := s.doRequestWithAuth(handler, http.MethodPost, scenario65ClusterPath("/queries/1234/cancel"),
		"operator", "operator-pass", []byte(`{}`))
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"Operator user POST /queries/{pid}/cancel should return 200")
}
