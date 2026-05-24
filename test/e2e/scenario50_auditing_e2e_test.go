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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 50 E2E: Auditing (All Categories)
// ============================================================================
//
// End-to-end tests for auditing across three categories:
// 50a — Connection auditing config
// 50b — Statement auditing config
// 50c — Operator audit log format
// ============================================================================

// Scenario50AuditE2ESuite tests Scenario 50: Auditing end-to-end.
type Scenario50AuditE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario50(t *testing.T) {
	suite.Run(t, new(Scenario50AuditE2ESuite))
}

// buildE2EAuditCluster creates a cluster with audit config parameters for E2E tests.
func (s *Scenario50AuditE2ESuite) buildE2EAuditCluster(
	name string, params map[string]string,
) *testutil.ClusterBuilder {
	return testutil.NewClusterBuilder(name, s.namespace).
		WithSegments(2).
		WithConfig(params).
		WithFinalizer().
		WithPhase("Running").
		WithPendingGeneration()
}

// getPostgresqlConf builds the postgresql.conf ConfigMap and returns its content.
func (s *Scenario50AuditE2ESuite) getPostgresqlConf(
	b *testutil.ClusterBuilder,
) string {
	s.T().Helper()

	cluster := b.Build()
	bldr := builder.NewBuilder()
	cm := bldr.BuildPostgresqlConfConfigMap(cluster)
	content, ok := cm.Data["postgresql.conf"]
	require.True(s.T(), ok, "postgresql.conf key must exist in ConfigMap")
	return content
}

// setupE2EOperatorAuditServer creates an API server with basic auth middleware
// and a log buffer for capturing structured audit logs in E2E tests.
func (s *Scenario50AuditE2ESuite) setupE2EOperatorAuditServer() (
	*api.Server, *bytes.Buffer, *auth.InMemoryCredentialStore,
) {
	s.T().Helper()

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

// --- 50a E2E Tests: Connection Auditing Config ---

// TestE2E_Scenario50a_ConnectionAudit_ConfigMap verifies that postgresql.conf
// contains connection audit settings end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50a_ConnectionAudit_ConfigMap() {
	s.logger.Info("starting scenario 50a E2E: connection audit config")

	tc := cases.AuditCases()[0] // 50a_connection_audit_logging
	require.Equal(s.T(), "connection", tc.Category)

	b := s.buildE2EAuditCluster("e2e-s50a-conn", tc.ConfigParams)
	content := s.getPostgresqlConf(b)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain connection audit setting: %s", expected)
	}

	s.logger.Info("scenario 50a E2E: connection audit config completed")
}

// TestE2E_Scenario50a_ConnectionAudit_HashAnnotation verifies that the
// ConfigMap has a config hash annotation for change detection.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50a_ConnectionAudit_HashAnnotation() {
	s.logger.Info("starting scenario 50a E2E: hash annotation")

	params := map[string]string{
		"log_connections":    "on",
		"log_disconnections": "on",
	}
	cluster := testutil.NewClusterBuilder("e2e-s50a-hash", s.namespace).
		WithSegments(2).
		WithConfig(params).
		WithFinalizer().
		WithPhase("Running").
		WithPendingGeneration().
		Build()

	bldr := builder.NewBuilder()
	cm := bldr.BuildPostgresqlConfConfigMap(cluster)

	require.NotNil(s.T(), cm.Annotations, "ConfigMap should have annotations")
	hash, ok := cm.Annotations[util.AnnotationConfigHash]
	assert.True(s.T(), ok, "ConfigMap should have %s annotation", util.AnnotationConfigHash)
	assert.NotEmpty(s.T(), hash, "config-hash annotation should not be empty")

	s.logger.Info("scenario 50a E2E: hash annotation completed")
}

// --- 50b E2E Tests: Statement Auditing Config ---

// TestE2E_Scenario50b_StatementAudit_DDL verifies that postgresql.conf
// contains log_statement = 'ddl' end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50b_StatementAudit_DDL() {
	s.logger.Info("starting scenario 50b E2E: statement audit DDL")

	tc := cases.AuditCases()[1] // 50b_statement_audit_ddl
	require.Equal(s.T(), "statement", tc.Category)

	b := s.buildE2EAuditCluster("e2e-s50b-ddl", tc.ConfigParams)
	content := s.getPostgresqlConf(b)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain statement audit setting: %s", expected)
	}

	s.logger.Info("scenario 50b E2E: statement audit DDL completed")
}

// TestE2E_Scenario50b_StatementAudit_Duration verifies that postgresql.conf
// contains log_min_duration_statement and log_duration settings end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50b_StatementAudit_Duration() {
	s.logger.Info("starting scenario 50b E2E: statement audit duration")

	tc := cases.AuditCases()[2] // 50b_statement_audit_duration
	require.Equal(s.T(), "statement", tc.Category)

	b := s.buildE2EAuditCluster("e2e-s50b-dur", tc.ConfigParams)
	content := s.getPostgresqlConf(b)

	for _, expected := range tc.ExpectInConf {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain duration audit setting: %s", expected)
	}

	s.logger.Info("scenario 50b E2E: statement audit duration completed")
}

// TestE2E_Scenario50b_StatementAudit_FullScenarioConfig verifies the complete
// scenario50 example configuration end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50b_StatementAudit_FullScenarioConfig() {
	s.logger.Info("starting scenario 50b E2E: full scenario config")

	params := map[string]string{
		"log_connections":            "on",
		"log_disconnections":         "on",
		"log_statement":              "ddl",
		"log_min_duration_statement": "1000",
		"log_duration":               "on",
	}
	b := s.buildE2EAuditCluster("e2e-s50b-full", params)
	content := s.getPostgresqlConf(b)

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

	s.logger.Info("scenario 50b E2E: full scenario config completed")
}

// --- 50c E2E Tests: Operator Audit Log Format ---

// TestE2E_Scenario50c_OperatorAudit_BasicAuthSuccess verifies that successful
// basic auth produces a structured log entry with username and permission.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_BasicAuthSuccess() {
	s.logger.Info("starting scenario 50c E2E: basic auth success audit")

	server, logBuf, _ := s.setupE2EOperatorAuditServer()
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "basic auth succeeded",
		"log should contain 'basic auth succeeded' message")
	assert.Contains(s.T(), logOutput, "admin",
		"log should contain the username")

	s.logger.Info("scenario 50c E2E: basic auth success audit completed")
}

// TestE2E_Scenario50c_OperatorAudit_BasicAuthFailure verifies that failed
// basic auth produces a structured log entry with method and error.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_BasicAuthFailure() {
	s.logger.Info("starting scenario 50c E2E: basic auth failure audit")

	server, logBuf, _ := s.setupE2EOperatorAuditServer()
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

	s.logger.Info("scenario 50c E2E: basic auth failure audit completed")
}

// TestE2E_Scenario50c_OperatorAudit_PermissionDenied verifies that a user
// with insufficient permissions receives a 403 Forbidden response.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_PermissionDenied() {
	s.logger.Info("starting scenario 50c E2E: permission denied audit")

	server, logBuf, _ := s.setupE2EOperatorAuditServer()
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

	s.logger.Info("scenario 50c E2E: permission denied audit completed")
}

// TestE2E_Scenario50c_OperatorAudit_JSONFormat verifies that all audit log
// entries are valid JSON with required fields.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_JSONFormat() {
	s.logger.Info("starting scenario 50c E2E: JSON format audit")

	server, logBuf, _ := s.setupE2EOperatorAuditServer()
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

		_, hasLevel := entry["level"]
		assert.True(s.T(), hasLevel, "JSON log entry should have 'level' field: %s", line)
		_, hasMsg := entry["msg"]
		assert.True(s.T(), hasMsg, "JSON log entry should have 'msg' field: %s", line)
	}

	assert.Contains(s.T(), logOutput, "basic auth succeeded",
		"log should contain success audit entry")
	assert.Contains(s.T(), logOutput, "authentication failed",
		"log should contain failure audit entry")

	s.logger.Info("scenario 50c E2E: JSON format audit completed")
}

// TestE2E_Scenario50c_OperatorAudit_SuccessLogFields verifies that the success
// audit log entry contains all required structured fields end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_SuccessLogFields() {
	s.logger.Info("starting scenario 50c E2E: success log fields")

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
			assert.Contains(s.T(), line, "method",
				"success log should contain 'method' field")
			assert.Contains(s.T(), line, "source_ip",
				"success log should contain 'source_ip' field")
			break
		}
	}
	assert.True(s.T(), found, "should find 'basic auth succeeded' log entry")

	s.logger.Info("scenario 50c E2E: success log fields completed")
}

// TestE2E_Scenario50c_OperatorAudit_FailureLogFields verifies that the failure
// audit log entry contains method, error, and remote_addr fields end-to-end.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_FailureLogFields() {
	s.logger.Info("starting scenario 50c E2E: failure log fields")

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
	req.RemoteAddr = "10.0.0.50:54321"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)

	logOutput := logBuf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")

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
			assert.Contains(s.T(), line, "10.0.0.50",
				"failure log should contain the remote address")
			break
		}
	}
	assert.True(s.T(), found, "should find 'authentication failed' log entry")

	s.logger.Info("scenario 50c E2E: failure log fields completed")
}

// TestE2E_Scenario50c_OperatorAudit_ConfigChange verifies that a config
// change produces an audit log entry with cluster name, username, and method.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_ConfigChange() {
	s.logger.Info("starting scenario 50c E2E: config change audit")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()

	// Pre-create a cluster in the fake k8s client
	cluster := testutil.NewClusterBuilder("e2e-audit-config", s.namespace).
		WithSegments(2).
		WithFinalizer().
		WithPhase("Running").
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
		ts.URL+"/api/v1alpha1/clusters/e2e-audit-config/config",
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
	assert.Contains(s.T(), logOutput, "e2e-audit-config",
		"config change log should contain the cluster name")

	s.logger.Info("scenario 50c E2E: config change audit completed")
}

// TestE2E_Scenario50c_OperatorAudit_RoleAssignment verifies that a role
// assignment produces an audit log entry with role name and username.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50c_OperatorAudit_RoleAssignment() {
	s.logger.Info("starting scenario 50c E2E: role assignment audit")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()

	// Pre-create a cluster
	cluster := testutil.NewClusterBuilder("e2e-audit-role", s.namespace).
		WithSegments(2).
		WithFinalizer().
		WithPhase("Running").
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
		ts.URL+"/api/v1alpha1/clusters/e2e-audit-role/workload/resource-groups/default_group/assign",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "assign resource group requested",
		"log should contain role assignment warning when DB not available")
	assert.Contains(s.T(), logOutput, "analyst",
		"role assignment log should contain the role name")

	s.logger.Info("scenario 50c E2E: role assignment audit completed")
}

// TestE2E_Scenario50_AuditCases_Coverage verifies that all audit test cases
// from the catalog are covered.
func (s *Scenario50AuditE2ESuite) TestE2E_Scenario50_AuditCases_Coverage() {
	s.logger.Info("starting scenario 50 E2E: audit cases coverage")

	auditCases := cases.AuditCases()
	require.Len(s.T(), auditCases, 11, "should have 11 audit test cases")

	categories := make(map[string]int)
	for _, tc := range auditCases {
		categories[tc.Category]++
	}
	assert.Equal(s.T(), 1, categories["connection"], "should have 1 connection audit case")
	assert.Equal(s.T(), 3, categories["statement"], "should have 3 statement audit cases")
	assert.Equal(s.T(), 7, categories["operator"], "should have 7 operator audit cases")

	s.logger.Info("scenario 50 E2E: audit cases coverage completed")
}

// ============================================================================
// Scenario 50 Real Cluster E2E: Auditing Verification with Real Cloudberry
// ============================================================================
//
// These tests connect to the real Cloudberry cluster running in Kubernetes
// to verify that audit-related configuration parameters are properly applied
// and that the operator API server produces correct audit logs when backed
// by a real database connection.
// ============================================================================

// Scenario50RealClusterE2ESuite tests Scenario 50 against the real Cloudberry cluster.
type Scenario50RealClusterE2ESuite struct {
	E2ESuite

	dbClient       db.Client
	portForwardCmd *exec.Cmd
	localPort      int
}

func TestE2E_Scenario50_RealCluster(t *testing.T) {
	suite.Run(t, new(Scenario50RealClusterE2ESuite))
}

func (s *Scenario50RealClusterE2ESuite) SetupSuite() {
	s.E2ESuite.SetupSuite()
	s.logger.Info("scenario 50 real cluster E2E suite setup")

	host := getEnvDefault(envCloudberryTestHost, defaultCloudberryHost)
	portStr := os.Getenv(envCloudberryTestPort)
	user := getEnvDefault(envCloudberryTestUser, defaultCloudberryUser)
	password := os.Getenv(envCloudberryTestPassword)
	database := getEnvDefault(envCloudberryTestDB, defaultCloudberryDB)
	namespace := getEnvDefault(envCloudberryTestNamespace, defaultCloudberryNamespace)
	service := getEnvDefault(envCloudberryTestService, defaultCloudberryService)

	if password == "" {
		password = readPasswordFromSecret(namespace, service)
	}

	var port int
	if portStr != "" {
		var parseErr error
		port, parseErr = strconv.Atoi(portStr)
		require.NoError(s.T(), parseErr, "CLOUDBERRY_TEST_PORT must be a valid integer")
	} else {
		freePort, err := findFreePort()
		require.NoError(s.T(), err, "failed to find a free local port")
		port = freePort

		s.logger.Info("starting kubectl port-forward",
			"namespace", namespace, "service", service, "localPort", port)

		s.portForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", namespace,
			fmt.Sprintf("svc/%s", service),
			fmt.Sprintf("%d:5432", port),
		)
		s.portForwardCmd.Stdout = os.Stdout
		s.portForwardCmd.Stderr = os.Stderr

		err = s.portForwardCmd.Start()
		if err != nil {
			s.T().Skipf("skipping scenario 50 real cluster E2E: kubectl port-forward failed: %v", err)
			return
		}

		if !waitForPort(host, port, 15*time.Second) {
			s.cleanupPortForward()
			s.T().Skipf("skipping scenario 50 real cluster E2E: port-forward not ready")
			return
		}
		s.localPort = port
		s.logger.Info("port-forward established", "localPort", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := db.Config{
		Host:     host,
		Port:     int32(port),
		Database: database,
		Username: user,
		Password: password,
		SSLMode:  "disable",
		MaxConns: 3,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
	}

	dbClient, err := db.NewClient(ctx, cfg, s.logger)
	if err != nil {
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 50 real cluster E2E: cannot connect: %v", err)
		return
	}
	s.dbClient = dbClient

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := s.dbClient.Ping(pingCtx); err != nil {
		s.dbClient.Close()
		s.cleanupPortForward()
		s.T().Skipf("skipping scenario 50 real cluster E2E: ping failed: %v", err)
		return
	}

	s.logger.Info("connected to real Cloudberry cluster for scenario 50",
		"host", host, "port", port)
}

func (s *Scenario50RealClusterE2ESuite) TearDownSuite() {
	if s.dbClient != nil {
		s.dbClient.Close()
		s.logger.Info("database connection closed")
	}
	s.cleanupPortForward()
	s.E2ESuite.TearDownSuite()
}

func (s *Scenario50RealClusterE2ESuite) cleanupPortForward() {
	if s.portForwardCmd != nil && s.portForwardCmd.Process != nil {
		_ = s.portForwardCmd.Process.Kill()
		_ = s.portForwardCmd.Wait()
		s.portForwardCmd = nil
	}
}

// --- 50a Real Cluster: Connection Auditing ---

// TestE2E_Scenario50a_RealCluster_ConnectionAuditParams verifies that
// connection audit parameters can be queried on the real cluster.
// The scenario1-cluster may or may not have log_connections set,
// but we verify the parameter is queryable and returns a valid value.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50a_RealCluster_ConnectionAuditParams() {
	s.logger.Info("starting scenario 50a real cluster: connection audit params")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify log_connections is a valid GUC parameter.
	val, err := s.dbClient.ShowParameter(ctx, "log_connections")
	require.NoError(s.T(), err, "SHOW log_connections should succeed")
	assert.Contains(s.T(), []string{"on", "off"}, val,
		"log_connections should be 'on' or 'off', got: %s", val)
	s.logger.Info("log_connections value", "value", val)

	// Verify log_disconnections is a valid GUC parameter.
	val2, err := s.dbClient.ShowParameter(ctx, "log_disconnections")
	require.NoError(s.T(), err, "SHOW log_disconnections should succeed")
	assert.Contains(s.T(), []string{"on", "off"}, val2,
		"log_disconnections should be 'on' or 'off', got: %s", val2)
	s.logger.Info("log_disconnections value", "value", val2)

	s.logger.Info("scenario 50a real cluster: connection audit params completed")
}

// --- 50b Real Cluster: Statement Auditing ---

// TestE2E_Scenario50b_RealCluster_StatementAuditParams verifies that
// statement audit parameters can be queried on the real cluster.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50b_RealCluster_StatementAuditParams() {
	s.logger.Info("starting scenario 50b real cluster: statement audit params")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify log_statement is a valid GUC parameter.
	val, err := s.dbClient.ShowParameter(ctx, "log_statement")
	require.NoError(s.T(), err, "SHOW log_statement should succeed")
	validValues := []string{"none", "ddl", "mod", "all"}
	assert.Contains(s.T(), validValues, val,
		"log_statement should be one of %v, got: %s", validValues, val)
	s.logger.Info("log_statement value", "value", val)

	// Verify log_min_duration_statement is queryable.
	// The scenario1-cluster has coordinatorParameters.log_min_duration_statement = "500".
	val2, err := s.dbClient.ShowParameter(ctx, "log_min_duration_statement")
	require.NoError(s.T(), err, "SHOW log_min_duration_statement should succeed")
	assert.NotEmpty(s.T(), val2, "log_min_duration_statement should have a value")
	s.logger.Info("log_min_duration_statement value", "value", val2)

	// Verify log_duration is a valid GUC parameter.
	val3, err := s.dbClient.ShowParameter(ctx, "log_duration")
	require.NoError(s.T(), err, "SHOW log_duration should succeed")
	assert.Contains(s.T(), []string{"on", "off"}, val3,
		"log_duration should be 'on' or 'off', got: %s", val3)
	s.logger.Info("log_duration value", "value", val3)

	s.logger.Info("scenario 50b real cluster: statement audit params completed")
}

// TestE2E_Scenario50b_RealCluster_LogMinDurationStatement verifies that
// the scenario1-cluster has log_min_duration_statement set to 500ms
// as configured in the cluster spec.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50b_RealCluster_LogMinDurationStatement() {
	s.logger.Info("starting scenario 50b real cluster: log_min_duration_statement verification")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	val, err := s.dbClient.ShowParameter(ctx, "log_min_duration_statement")
	require.NoError(s.T(), err, "SHOW log_min_duration_statement should succeed")

	// The scenario1-cluster has coordinatorParameters.log_min_duration_statement = "500".
	// PostgreSQL/Cloudberry returns this as "500ms".
	assert.Contains(s.T(), val, "500",
		"log_min_duration_statement should contain '500' (configured as 500ms), got: %s", val)

	s.logger.Info("log_min_duration_statement verified", "value", val)
	s.logger.Info("scenario 50b real cluster: log_min_duration_statement verification completed")
}

// --- 50c Real Cluster: Operator Audit with Real DB ---

// TestE2E_Scenario50c_RealCluster_ConfigChangeAudit verifies that changing
// config via the API server backed by a real DB produces audit logs.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50c_RealCluster_ConfigChangeAudit() {
	s.logger.Info("starting scenario 50c real cluster: config change audit with real DB")

	// Create an API server with real DB factory and auth middleware.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	// Create a cluster object in the fake K8s client.
	k8sEnv := testutil.NewTestK8sEnv()
	cluster := testutil.NewClusterBuilder("real-audit-config", s.namespace).
		WithSegments(2).
		WithFinalizer().
		WithPhase("Running").
		WithPendingGeneration().
		Build()
	err := k8sEnv.Client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Use real DB factory wrapping the actual DB client.
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Update config via API.
	configBody := `{"parameters":{"log_connections":"on","log_disconnections":"on"}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPut,
		ts.URL+"/api/v1alpha1/clusters/real-audit-config/config",
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
		"log should contain 'config changed' audit entry")
	assert.Contains(s.T(), logOutput, "admin",
		"config change log should contain the username")
	assert.Contains(s.T(), logOutput, "basic",
		"config change log should contain the auth method")

	s.logger.Info("scenario 50c real cluster: config change audit completed")
}

// TestE2E_Scenario50c_RealCluster_PermissionDeniedAudit verifies that
// permission denied events are logged when using a real DB-backed API server.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50c_RealCluster_PermissionDeniedAudit() {
	s.logger.Info("starting scenario 50c real cluster: permission denied audit with real DB")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	cluster := testutil.NewClusterBuilder("real-audit-perm", s.namespace).
		WithSegments(2).
		WithFinalizer().
		WithPhase("Running").
		WithPendingGeneration().
		Build()
	err := k8sEnv.Client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Viewer tries to create a cluster (requires Admin).
	body := `{"metadata":{"name":"test","namespace":"default"},"spec":{"version":"2.1.0","image":"test:latest","coordinator":{"storage":{"size":"5Gi"}},"segments":{"count":2,"storage":{"size":"5Gi"}}}}`
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost,
		ts.URL+"/api/v1alpha1/clusters",
		strings.NewReader(body))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("viewer", "viewer-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusForbidden, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "permission denied",
		"log should contain 'permission denied' audit entry")
	assert.Contains(s.T(), logOutput, "viewer",
		"permission denied log should contain the username")

	s.logger.Info("scenario 50c real cluster: permission denied audit completed")
}

// TestE2E_Scenario50c_RealCluster_AuthSuccessAudit verifies that
// successful authentication is logged with all required fields
// when using a real DB-backed API server.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50c_RealCluster_AuthSuccessAudit() {
	s.logger.Info("starting scenario 50c real cluster: auth success audit with real DB")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet,
		ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "admin-secret")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "basic auth succeeded",
		"log should contain 'basic auth succeeded' audit entry")
	assert.Contains(s.T(), logOutput, "admin",
		"success log should contain the username")
	assert.Contains(s.T(), logOutput, "method",
		"success log should contain 'method' field")
	assert.Contains(s.T(), logOutput, "source_ip",
		"success log should contain 'source_ip' field")
	assert.Contains(s.T(), logOutput, "permission",
		"success log should contain 'permission' field")

	// Verify JSON format.
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		err := json.Unmarshal([]byte(line), &entry)
		assert.NoError(s.T(), err, "each log line should be valid JSON: %s", line)
		_, hasTime := entry["time"]
		assert.True(s.T(), hasTime, "JSON log entry should have 'time' (timestamp) field")
	}

	s.logger.Info("scenario 50c real cluster: auth success audit completed")
}

// TestE2E_Scenario50c_RealCluster_AuthFailureAudit verifies that
// failed authentication is logged when using a real DB-backed API server.
func (s *Scenario50RealClusterE2ESuite) TestE2E_Scenario50c_RealCluster_AuthFailureAudit() {
	s.logger.Info("starting scenario 50c real cluster: auth failure audit with real DB")

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	basicProvider := auth.NewBasicAuthProvider(store, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv()
	factory := &realDBClientFactory{client: s.dbClient}
	server := api.NewServer(k8sEnv.Client, authMW, factory, &metrics.NoopRecorder{}, logger, 0)
	defer server.Close()

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet,
		ts.URL+"/api/v1alpha1/clusters", nil)
	require.NoError(s.T(), err)
	req.SetBasicAuth("admin", "wrong-password")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)

	logOutput := logBuf.String()
	assert.Contains(s.T(), logOutput, "authentication failed",
		"log should contain 'authentication failed' audit entry")
	assert.Contains(s.T(), logOutput, "basic",
		"failure log should contain the auth method")

	s.logger.Info("scenario 50c real cluster: auth failure audit completed")
}
