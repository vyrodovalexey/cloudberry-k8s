//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

const (
	scenario5ClusterName = "scenario5-cluster"
	scenario5Namespace   = "default"
	scenario5APIPrefix   = "/api/v1alpha1"
)

// scenario5MockDBClient implements db.Client for session management tests.
type scenario5MockDBClient struct {
	sessions        []db.Session
	cancelResult    bool
	terminateResult bool
	listErr         error
	cancelErr       error
	terminateErr    error
	closed          bool
}

func (m *scenario5MockDBClient) Ping(_ context.Context) error { return nil }
func (m *scenario5MockDBClient) Close()                       { m.closed = true }
func (m *scenario5MockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetClusterState(_ context.Context) (*db.ClusterState, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) SetParameter(_ context.Context, _, _ string, _ db.ParameterScope) error {
	return nil
}
func (m *scenario5MockDBClient) ShowParameter(_ context.Context, _ string) (string, error) {
	return "", nil
}
func (m *scenario5MockDBClient) ReloadConfig(_ context.Context) error { return nil }
func (m *scenario5MockDBClient) ListSessions(_ context.Context) ([]db.Session, error) {
	return m.sessions, m.listErr
}
func (m *scenario5MockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error) {
	return m.cancelResult, m.cancelErr
}
func (m *scenario5MockDBClient) TerminateSession(_ context.Context, _ int32) (bool, error) {
	return m.terminateResult, m.terminateErr
}
func (m *scenario5MockDBClient) CreateRole(_ context.Context, _ db.RoleOptions) error { return nil }
func (m *scenario5MockDBClient) AlterRole(_ context.Context, _ db.RoleOptions) error  { return nil }
func (m *scenario5MockDBClient) DropRole(_ context.Context, _ string) error           { return nil }
func (m *scenario5MockDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error   { return nil }
func (m *scenario5MockDBClient) Analyze(_ context.Context, _ string) error            { return nil }
func (m *scenario5MockDBClient) Reindex(_ context.Context, _ db.ReindexOptions) error { return nil }
func (m *scenario5MockDBClient) GetDiskUsage(_ context.Context, _ string) ([]db.DiskUsage, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetReplicationLag(_ context.Context) (int64, error) { return 0, nil }
func (m *scenario5MockDBClient) PromoteStandby(_ context.Context) error             { return nil }
func (m *scenario5MockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return 0, 0, 0, nil
}
func (m *scenario5MockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}
func (m *scenario5MockDBClient) CreateResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *scenario5MockDBClient) AlterResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *scenario5MockDBClient) DropResourceGroup(_ context.Context, _ string) error { return nil }
func (m *scenario5MockDBClient) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) AssignRoleResourceGroup(_ context.Context, _, _ string) error {
	return nil
}
func (m *scenario5MockDBClient) CreateBackup(_ context.Context, _ db.BackupOptions) (*db.BackupInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error {
	return nil
}
func (m *scenario5MockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) DeleteBackup(_ context.Context, _ string) error { return nil }
func (m *scenario5MockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}
func (m *scenario5MockDBClient) StartDataLoadingJob(_ context.Context, _ string) error { return nil }
func (m *scenario5MockDBClient) StopDataLoadingJob(_ context.Context, _ string) error  { return nil }
func (m *scenario5MockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) TriggerRecommendationScan(_ context.Context) error { return nil }
func (m *scenario5MockDBClient) GetTableDetails(_ context.Context, _, _ string) (*db.TableDetail, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) InitializeMirrors(_ context.Context, _ db.MirrorInitOptions) error {
	return nil
}
func (m *scenario5MockDBClient) ConfigureReplication(_ context.Context, _ db.ReplicationOptions) error {
	return nil
}
func (m *scenario5MockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) TriggerFTSProbe(_ context.Context) error { return nil }
func (m *scenario5MockDBClient) TerminateAllBackends(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *scenario5MockDBClient) CancelAllQueries(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *scenario5MockDBClient) LogRotate(_ context.Context) error { return nil }
func (m *scenario5MockDBClient) CreateResourceQueue(_ context.Context, _ db.ResourceQueueOptions) error {
	return nil
}
func (m *scenario5MockDBClient) DropResourceQueue(_ context.Context, _ string) error { return nil }
func (m *scenario5MockDBClient) ListResourceQueues(_ context.Context) ([]db.ResourceQueueInfo, error) {
	return nil, nil
}
func (m *scenario5MockDBClient) RegisterNewSegments(_ context.Context, _ db.SegmentRegistrationOptions) error {
	return nil
}
func (m *scenario5MockDBClient) RedistributeData(_ context.Context, _ db.RedistributionOptions) error {
	return nil
}
func (m *scenario5MockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) {
	return 100, nil
}
func (m *scenario5MockDBClient) DeregisterSegments(_ context.Context, _ int32) error {
	return nil
}
func (m *scenario5MockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ db.ScaleInRedistributionOptions) error {
	return nil
}

// scenario5MockDBFactory implements db.DBClientFactory for testing.
type scenario5MockDBFactory struct {
	client db.Client
	err    error
}

func (f *scenario5MockDBFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// newScenario5Cluster creates a test cluster for scenario 5.
func newScenario5Cluster() *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scenario5ClusterName,
			Namespace: scenario5Namespace,
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Storage: cbv1alpha1.StorageSpec{Size: "10Gi"},
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:   4,
				Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
			},
		},
		Status: cbv1alpha1.CloudberryClusterStatus{
			Phase:         cbv1alpha1.ClusterPhaseRunning,
			SegmentsReady: 4,
			SegmentsTotal: 4,
		},
	}
}

// newScenario5Server creates an API server with a mock DB factory for testing.
func newScenario5Server(cluster *cbv1alpha1.CloudberryCluster, dbFactory db.DBClientFactory) *api.Server {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)

	var objs []runtime.Object
	if cluster != nil {
		objs = append(objs, cluster)
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return api.NewServer(k8sClient, nil, dbFactory, &metrics.NoopRecorder{}, nil)
}

// scenario5AuthRequest adds an admin identity to the request context so that
// the permission middleware allows the request through.
func scenario5AuthRequest(req *http.Request) *http.Request {
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	return req.WithContext(ctx)
}

// Scenario5SessionManagementSuite tests session management operations
// (list sessions, cancel query, terminate session) via the API server handlers.
type Scenario5SessionManagementSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestScenario5_SessionManagement(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario5SessionManagementSuite))
}

func (s *Scenario5SessionManagementSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *Scenario5SessionManagementSuite) TearDownTest() {
	s.cancel()
}

// TestScenario5_ListSessions verifies that the list sessions endpoint returns
// sessions from the database via the mock DB client.
func (s *Scenario5SessionManagementSuite) TestScenario5_ListSessions() {
	queryStart := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	mockClient := &scenario5MockDBClient{
		sessions: []db.Session{
			{
				PID:           1234,
				Username:      "gpadmin",
				Application:   "psql",
				ClientAddress: "10.0.0.1",
				State:         "active",
				Query:         "SELECT * FROM orders",
				QueryStart:    queryStart,
				Duration:      "00:05:30",
			},
			{
				PID:           5678,
				Username:      "appuser",
				Application:   "pgbench",
				ClientAddress: "10.0.0.2",
				State:         "idle",
				Query:         "INSERT INTO logs VALUES ($1)",
				QueryStart:    queryStart.Add(-10 * time.Minute),
				Duration:      "00:15:30",
			},
		},
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodGet,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), float64(2), resp["total"])

	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok, "sessions should be an array")
	require.Len(s.T(), sessions, 2)

	// Verify first session fields.
	session0, ok := sessions[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1234), session0["pid"])
	assert.Equal(s.T(), "gpadmin", session0["username"])
	assert.Equal(s.T(), "psql", session0["application"])
	assert.Equal(s.T(), "10.0.0.1", session0["clientAddress"])
	assert.Equal(s.T(), "active", session0["state"])
	assert.Equal(s.T(), "SELECT * FROM orders", session0["query"])
	assert.Equal(s.T(), "00:05:30", session0["duration"])

	// Verify second session fields.
	session1, ok := sessions[1].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(5678), session1["pid"])
	assert.Equal(s.T(), "appuser", session1["username"])

	// Verify the mock client was closed after the request.
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario5_CancelQuery verifies that the cancel query endpoint calls
// pg_cancel_backend and returns the result.
func (s *Scenario5SessionManagementSuite) TestScenario5_CancelQuery() {
	mockClient := &scenario5MockDBClient{
		cancelResult: true,
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/1234/cancel?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "1234")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), float64(1234), resp["pid"])
	assert.Equal(s.T(), true, resp["canceled"])
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario5_CancelQuery_InvalidPID verifies that the cancel query endpoint
// returns a 400 error for invalid PID values.
func (s *Scenario5SessionManagementSuite) TestScenario5_CancelQuery_InvalidPID() {
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, nil)

	// Test with PID=0.
	req := scenario5AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/0/cancel?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "0")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])
	assert.Equal(s.T(), "PID must be a positive integer", errObj["message"])
}

// TestScenario5_TerminateSession verifies that the terminate session endpoint
// calls pg_terminate_backend and returns the result.
func (s *Scenario5SessionManagementSuite) TestScenario5_TerminateSession() {
	mockClient := &scenario5MockDBClient{
		terminateResult: true,
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/5678?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "5678")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), float64(5678), resp["pid"])
	assert.Equal(s.T(), true, resp["terminated"])
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario5_TerminateSession_InvalidPID verifies that the terminate session
// endpoint returns a 400 error for negative PID values.
func (s *Scenario5SessionManagementSuite) TestScenario5_TerminateSession_InvalidPID() {
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, nil)

	// Test with negative PID.
	req := scenario5AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/-5?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "-5")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])
	assert.Equal(s.T(), "PID must be a positive integer", errObj["message"])
}

// TestScenario5_NoDBFactory verifies that the API server gracefully handles
// the case when no DBClientFactory is configured.
func (s *Scenario5SessionManagementSuite) TestScenario5_NoDBFactory() {
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, nil)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodGet,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), float64(0), resp["total"])
	assert.Equal(s.T(), "database connection not available", resp["message"])

	sessions, ok := resp["sessions"].([]interface{})
	require.True(s.T(), ok, "sessions should be an array")
	assert.Empty(s.T(), sessions, "sessions should be empty when no db factory")
}

// TestScenario5_DBUnavailable verifies that the API server returns a 503 error
// when the database connection cannot be established.
func (s *Scenario5SessionManagementSuite) TestScenario5_DBUnavailable() {
	factory := &scenario5MockDBFactory{
		err: fmt.Errorf("connection refused"),
	}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodGet,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusServiceUnavailable, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "DB_UNAVAILABLE", errObj["code"])
}

// TestScenario5_ListSessionsError verifies that the API server returns a 500 error
// when the database query fails.
func (s *Scenario5SessionManagementSuite) TestScenario5_ListSessionsError() {
	mockClient := &scenario5MockDBClient{
		listErr: fmt.Errorf("query failed"),
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodGet,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INTERNAL_ERROR", errObj["code"])
}

// TestScenario5_CancelQueryError verifies that the API server returns a 500 error
// when the cancel query operation fails.
func (s *Scenario5SessionManagementSuite) TestScenario5_CancelQueryError() {
	mockClient := &scenario5MockDBClient{
		cancelErr: fmt.Errorf("cancel failed"),
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/1234/cancel?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "1234")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)
}

// TestScenario5_TerminateSessionError verifies that the API server returns a 500 error
// when the terminate session operation fails.
func (s *Scenario5SessionManagementSuite) TestScenario5_TerminateSessionError() {
	mockClient := &scenario5MockDBClient{
		terminateErr: fmt.Errorf("terminate failed"),
	}
	factory := &scenario5MockDBFactory{client: mockClient}
	cluster := newScenario5Cluster()
	srv := newScenario5Server(cluster, factory)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario5APIPrefix+"/clusters/"+scenario5ClusterName+"/sessions/5678?namespace="+scenario5Namespace, nil))
	req.SetPathValue("name", scenario5ClusterName)
	req.SetPathValue("pid", "5678")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)
}

// TestScenario5_CancelQuery_ClusterNotFound verifies that the cancel query endpoint
// returns a 404 error when the cluster does not exist.
func (s *Scenario5SessionManagementSuite) TestScenario5_CancelQuery_ClusterNotFound() {
	srv := newScenario5Server(nil, nil)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario5APIPrefix+"/clusters/nonexistent/sessions/1234/cancel?namespace=default", nil))
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("pid", "1234")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

// TestScenario5_TerminateSession_ClusterNotFound verifies that the terminate session
// endpoint returns a 404 error when the cluster does not exist.
func (s *Scenario5SessionManagementSuite) TestScenario5_TerminateSession_ClusterNotFound() {
	srv := newScenario5Server(nil, nil)

	req := scenario5AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario5APIPrefix+"/clusters/nonexistent/sessions/5678?namespace=default", nil))
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("pid", "5678")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}
