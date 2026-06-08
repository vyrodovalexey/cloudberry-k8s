//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 50: Auditing (All Categories)
// ============================================================================
//
// This scenario tests auditing across three categories:
// 50a — Connection auditing config (log_connections, log_disconnections)
// 50b — Statement auditing config (log_statement, log_min_duration_statement, log_duration)
// 50c — Operator audit log format (basic auth success/failure, permission denied, JSON format)
// ============================================================================

// Scenario50AuditSuite tests Scenario 50: Auditing (All Categories).
type Scenario50AuditSuite struct {
	suite.Suite
	builder *builder.DefaultBuilder
	ctx     context.Context
}

func TestFunctional_Scenario50(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario50AuditSuite))
}

func (s *Scenario50AuditSuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// buildAuditCluster creates a cluster with the given audit config parameters.
func buildAuditCluster(name string, params map[string]string) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(name, "default").
		WithSegments(2).
		WithConfig(params).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// getPostgresqlConf builds the postgresql.conf ConfigMap and returns its content.
func (s *Scenario50AuditSuite) getPostgresqlConf(
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	s.T().Helper()

	cm := s.builder.BuildPostgresqlConfConfigMap(cluster)
	content, ok := cm.Data["postgresql.conf"]
	require.True(s.T(), ok, "postgresql.conf key must exist in ConfigMap")
	return content
}

// --- 50a Tests: Connection Auditing Config ---

// TestFunctional_Scenario50a_ConnectionAudit_ConfigMap verifies that postgresql.conf
// contains log_connections = 'on' and log_disconnections = 'on' when configured.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50a_ConnectionAudit_ConfigMap() {
	tc := cases.AuditCases()[0] // 50a_connection_audit_logging
	require.Equal(s.T(), "connection", tc.Category)

	cluster := buildAuditCluster("s50a-conn-audit", tc.ConfigParams)
	content := s.getPostgresqlConf(cluster)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain connection audit setting: %s", expected)
	}
}

// TestFunctional_Scenario50a_ConnectionAudit_HashAnnotation verifies that the
// ConfigMap has a config hash annotation for change detection.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50a_ConnectionAudit_HashAnnotation() {
	params := map[string]string{
		"log_connections":    "on",
		"log_disconnections": "on",
	}
	cluster := buildAuditCluster("s50a-hash", params)
	cm := s.builder.BuildPostgresqlConfConfigMap(cluster)

	require.NotNil(s.T(), cm.Annotations, "ConfigMap should have annotations")
	hash, ok := cm.Annotations[util.AnnotationConfigHash]
	assert.True(s.T(), ok, "ConfigMap should have %s annotation", util.AnnotationConfigHash)
	assert.NotEmpty(s.T(), hash, "config-hash annotation should not be empty")
}

// TestFunctional_Scenario50a_ConnectionAudit_NoParams verifies that when no
// audit parameters are set, they do not appear in postgresql.conf.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50a_ConnectionAudit_NoParams() {
	cluster := testutil.NewClusterBuilder("s50a-no-params", "default").
		WithSegments(2).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	content := s.getPostgresqlConf(cluster)

	assert.NotContains(s.T(), content, "log_connections",
		"postgresql.conf should not contain log_connections when not configured")
	assert.NotContains(s.T(), content, "log_disconnections",
		"postgresql.conf should not contain log_disconnections when not configured")
}

// --- 50b Tests: Statement Auditing Config ---

// TestFunctional_Scenario50b_StatementAudit_DDL verifies that postgresql.conf
// contains log_statement = 'ddl' when configured.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50b_StatementAudit_DDL() {
	tc := cases.AuditCases()[1] // 50b_statement_audit_ddl
	require.Equal(s.T(), "statement", tc.Category)

	cluster := buildAuditCluster("s50b-ddl", tc.ConfigParams)
	content := s.getPostgresqlConf(cluster)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain statement audit setting: %s", expected)
	}
}

// TestFunctional_Scenario50b_StatementAudit_Duration verifies that postgresql.conf
// contains log_min_duration_statement = '1000' and log_duration = 'on'.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50b_StatementAudit_Duration() {
	tc := cases.AuditCases()[2] // 50b_statement_audit_duration
	require.Equal(s.T(), "statement", tc.Category)

	cluster := buildAuditCluster("s50b-duration", tc.ConfigParams)
	content := s.getPostgresqlConf(cluster)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain duration audit setting: %s", expected)
	}
}

// TestFunctional_Scenario50b_StatementAudit_AllParams verifies that all statement
// audit parameters appear together in postgresql.conf.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50b_StatementAudit_AllParams() {
	tc := cases.AuditCases()[3] // 50b_statement_audit_all_params
	require.Equal(s.T(), "statement", tc.Category)

	cluster := buildAuditCluster("s50b-all", tc.ConfigParams)
	content := s.getPostgresqlConf(cluster)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain statement audit setting: %s", expected)
	}
}

// TestFunctional_Scenario50b_StatementAudit_ParametersSorted verifies that
// user-defined parameters are rendered in sorted order.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50b_StatementAudit_ParametersSorted() {
	params := map[string]string{
		"log_statement":              "ddl",
		"log_min_duration_statement": "1000",
		"log_duration":               "on",
	}
	cluster := buildAuditCluster("s50b-sorted", params)
	content := s.getPostgresqlConf(cluster)

	// Parameters should be sorted alphabetically.
	idxDuration := strings.Index(content, "log_duration")
	idxMinDuration := strings.Index(content, "log_min_duration_statement")
	idxStatement := strings.Index(content, "log_statement")

	require.Greater(s.T(), idxDuration, 0, "log_duration should be present")
	require.Greater(s.T(), idxMinDuration, 0, "log_min_duration_statement should be present")
	require.Greater(s.T(), idxStatement, 0, "log_statement should be present")

	assert.Less(s.T(), idxDuration, idxMinDuration,
		"log_duration should appear before log_min_duration_statement (alphabetical order)")
	assert.Less(s.T(), idxMinDuration, idxStatement,
		"log_min_duration_statement should appear before log_statement (alphabetical order)")
}

// TestFunctional_Scenario50b_StatementAudit_FullScenarioConfig verifies the
// complete scenario50 example configuration.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50b_StatementAudit_FullScenarioConfig() {
	params := map[string]string{
		"log_connections":            "on",
		"log_disconnections":         "on",
		"log_statement":              "ddl",
		"log_min_duration_statement": "1000",
		"log_duration":               "on",
	}
	cluster := buildAuditCluster("s50b-full", params)
	content := s.getPostgresqlConf(cluster)

	expectedLines := []string{
		"log_connections = 'on'",
		"log_disconnections = 'on'",
		"log_duration = 'on'",
		"log_min_duration_statement = '1000'",
		"log_statement = 'ddl'",
	}

	for _, expected := range expectedLines {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain audit setting: %s", expected)
	}

	// Verify the header is present.
	assert.Contains(s.T(), content, "# User-defined parameters",
		"postgresql.conf should contain user-defined parameters section header")
}

// --- 50c Tests: Operator Audit Log Format ---

// setupOperatorAuditServer creates an API server with basic auth middleware
// and a log buffer for capturing structured audit logs.
func setupOperatorAuditServer(t *testing.T) (
	*api.Server, *bytes.Buffer, *auth.InMemoryCredentialStore,
) {
	t.Helper()

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, logger, 0)

	return server, logBuf, store
}

// TestFunctional_Scenario50c_OperatorAudit_BasicAuthSuccess verifies that
// successful basic auth produces a structured log entry with username and permission.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_BasicAuthSuccess() {
	server, logBuf, _ := setupOperatorAuditServer(s.T())
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/healthz", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Health endpoint does not require auth, so try an authenticated endpoint.
	req2, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req2.SetBasicAuth("admin", "admin-secret")

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(s.T(), err)
	defer resp2.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp2.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "basic auth succeeded",
		"log should contain 'basic auth succeeded' message")
	assert.Contains(s.T(), logOutput, "admin",
		"log should contain the username")
	assert.Contains(s.T(), logOutput, "Admin",
		"log should contain the permission level")
}

// TestFunctional_Scenario50c_OperatorAudit_BasicAuthFailure verifies that
// failed basic auth produces a structured log entry with method, error, and remote_addr.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_BasicAuthFailure() {
	server, logBuf, _ := setupOperatorAuditServer(s.T())
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "wrong-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "authentication failed",
		"log should contain 'authentication failed' message")
	assert.Contains(s.T(), logOutput, "basic",
		"log should contain the auth method")
}

// TestFunctional_Scenario50c_OperatorAudit_PermissionDenied verifies that
// a user with insufficient permissions receives a 403 Forbidden response.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_PermissionDenied() {
	server, logBuf, _ := setupOperatorAuditServer(s.T())
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Viewer (Basic permission) tries to create a cluster (requires Admin).
	body := `{"metadata":{"name":"test","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("viewer", "viewer-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusForbidden, resp.StatusCode,
		"viewer should get 403 when trying to create a cluster")

	// Verify the response body contains the FORBIDDEN error.
	var errResp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	err = json.NewDecoder(resp.Body).Decode(&errResp)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "FORBIDDEN", errResp.Error.Code)
	assert.Contains(s.T(), errResp.Error.Message, "insufficient permissions")

	// Verify permission denied is logged.
	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "permission denied",
		"log should contain 'permission denied' message")
	assert.Contains(s.T(), logOutput, "viewer",
		"permission denied log should contain the username")
	assert.Contains(s.T(), logOutput, "path",
		"permission denied log should contain the path field")
}

// TestFunctional_Scenario50c_OperatorAudit_JSONFormat verifies that all audit
// log entries are valid JSON with required fields.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_JSONFormat() {
	server, logBuf, _ := setupOperatorAuditServer(s.T())
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Generate a successful auth log entry.
	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Generate a failed auth log entry.
	req2, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req2.SetBasicAuth("admin", "bad-password")

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(s.T(), err)
	defer resp2.Body.Close()

	// Parse each line of the log buffer as JSON.
	logOutput := logBuf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	require.NotEmpty(s.T(), lines, "log output should contain at least one line")

	for _, line := range lines {
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		err := json.Unmarshal([]byte(line), &entry)
		assert.NoError(s.T(), err, "each log line should be valid JSON: %s", line)

		// Every structured log entry should have at minimum a level and msg field.
		_, hasLevel := entry["level"]
		assert.True(s.T(), hasLevel, "JSON log entry should have 'level' field: %s", line)
		_, hasMsg := entry["msg"]
		assert.True(s.T(), hasMsg, "JSON log entry should have 'msg' field: %s", line)
	}

	// Verify that we have both success and failure log entries.
	assert.Contains(s.T(), logOutput, "basic auth succeeded",
		"log should contain success audit entry")
	assert.Contains(s.T(), logOutput, "authentication failed",
		"log should contain failure audit entry")
}

// TestFunctional_Scenario50c_OperatorAudit_SuccessLogFields verifies that
// the success audit log entry contains all required structured fields.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_SuccessLogFields() {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "admin-secret")

	identity, err := basicProvider.Authenticate(s.ctx, req)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), identity)

	logOutput := logBuf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")

	// Find the success log entry.
	var found bool
	for _, line := range lines {
		var entry map[string]interface{}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		msg, _ := entry["msg"].(string)
		if msg == "basic auth succeeded" {
			found = true
			assert.Contains(s.T(), line, "username",
				"success log should contain 'username' field")
			assert.Contains(s.T(), line, "permission",
				"success log should contain 'permission' field")
			assert.Contains(s.T(), line, "admin",
				"success log should contain the actual username value")
			assert.Contains(s.T(), line, "Admin",
				"success log should contain the actual permission value")
			assert.Contains(s.T(), line, "method",
				"success log should contain 'method' field")
			assert.Contains(s.T(), line, "source_ip",
				"success log should contain 'source_ip' field")
			break
		}
	}
	assert.True(s.T(), found, "should find 'basic auth succeeded' log entry")
}

// TestFunctional_Scenario50c_OperatorAudit_FailureLogFields verifies that
// the failure audit log entry contains method, error, and remote_addr fields.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_FailureLogFields() {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	middleware := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	handler := middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "wrong-password")
	req.RemoteAddr = "192.168.1.100:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	logOutput := logBuf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")

	// Find the failure log entry from the middleware.
	var found bool
	for _, line := range lines {
		var entry map[string]interface{}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		msg, _ := entry["msg"].(string)
		if msg == "authentication failed" {
			found = true
			assert.Contains(s.T(), line, "method",
				"failure log should contain 'method' field")
			assert.Contains(s.T(), line, "error",
				"failure log should contain 'error' field")
			assert.Contains(s.T(), line, "remote_addr",
				"failure log should contain 'remote_addr' field")
			assert.Contains(s.T(), line, "basic",
				"failure log should contain the auth method value")
			assert.Contains(s.T(), line, "192.168.1.100",
				"failure log should contain the remote address")
			break
		}
	}
	assert.True(s.T(), found, "should find 'authentication failed' log entry")
}

// TestFunctional_Scenario50c_OperatorAudit_ConfigChange verifies that
// config changes are logged with the username and cluster name.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_ConfigChange() {
	// Create server with log capture
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()

	// Pre-create a cluster in the fake k8s client
	cluster := testutil.NewClusterBuilder("audit-config-test", "default").
		WithSegments(2).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	err := k8sEnv.Client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Update config
	configBody := `{"parameters":{"log_connections":"on","log_disconnections":"on"}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPut,
		ts.URL+"/api/v1alpha1/clusters/audit-config-test/config",
		strings.NewReader(configBody))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "config changed",
		"log should contain 'config changed' message")
	assert.Contains(s.T(), logOutput, "admin",
		"config change log should contain the username")
	assert.Contains(s.T(), logOutput, "audit-config-test",
		"config change log should contain the cluster name")
}

// TestFunctional_Scenario50c_OperatorAudit_RoleAssignment verifies that
// role assignment requests are logged with user context.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50c_OperatorAudit_RoleAssignment() {
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()

	// Pre-create a cluster
	cluster := testutil.NewClusterBuilder("audit-role-test", "default").
		WithSegments(2).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	err := k8sEnv.Client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Server without dbFactory — role assignment will return "pending"
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	body := `{"role":"analyst"}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost,
		ts.URL+"/api/v1alpha1/clusters/audit-role-test/workload/resource-groups/default_group/assign",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	// Verify the warning log for role assignment without DB
	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "assign resource group requested",
		"log should contain role assignment warning when DB not available")
	assert.Contains(s.T(), logOutput, "analyst",
		"role assignment log should contain the role name")
}

// TestFunctional_Scenario50_AuditCases_Coverage verifies that all audit test
// cases from the catalog are covered.
func (s *Scenario50AuditSuite) TestFunctional_Scenario50_AuditCases_Coverage() {
	auditCases := cases.AuditCases()
	require.Len(s.T(), auditCases, 11, "should have 11 audit test cases")

	// Verify categories.
	categories := make(map[string]int)
	for _, tc := range auditCases {
		categories[tc.Category]++
	}
	assert.Equal(s.T(), 1, categories["connection"], "should have 1 connection audit case")
	assert.Equal(s.T(), 3, categories["statement"], "should have 3 statement audit cases")
	assert.Equal(s.T(), 7, categories["operator"], "should have 7 operator audit cases")
}
