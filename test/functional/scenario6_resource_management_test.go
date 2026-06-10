//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	scenario6ClusterName = "scenario6-cluster"
	scenario6Namespace   = "default"
	scenario6APIPrefix   = "/api/v1alpha1"
)

// scenario6MockDBClient implements db.Client for resource management tests.
type scenario6MockDBClient struct {
	// Resource group fields.
	createResourceGroupOpts db.ResourceGroupOptions
	createResourceGroupErr  error
	dropResourceGroupName   string
	dropResourceGroupErr    error
	listResourceGroups      []db.ResourceGroupInfo
	listResourceGroupsErr   error
	assignRole              string
	assignGroup             string
	assignErr               error
	closed                  bool
}

func (m *scenario6MockDBClient) Ping(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) Close()                       { m.closed = true }
func (m *scenario6MockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetClusterState(_ context.Context) (*db.ClusterState, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) SetParameter(_ context.Context, _, _ string, _ db.ParameterScope) error {
	return nil
}
func (m *scenario6MockDBClient) ShowParameter(_ context.Context, _ string) (string, error) {
	return "", nil
}
func (m *scenario6MockDBClient) ReloadConfig(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) ListSessions(_ context.Context) ([]db.Session, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error) {
	return false, nil
}
func (m *scenario6MockDBClient) TerminateSession(_ context.Context, _ int32) (bool, error) {
	return false, nil
}
func (m *scenario6MockDBClient) CreateRole(_ context.Context, _ db.RoleOptions) error { return nil }
func (m *scenario6MockDBClient) AlterRole(_ context.Context, _ db.RoleOptions) error  { return nil }
func (m *scenario6MockDBClient) DropRole(_ context.Context, _ string) error           { return nil }
func (m *scenario6MockDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error   { return nil }
func (m *scenario6MockDBClient) Analyze(_ context.Context, _ string) error            { return nil }
func (m *scenario6MockDBClient) Reindex(_ context.Context, _ db.ReindexOptions) error { return nil }
func (m *scenario6MockDBClient) GetDiskUsage(_ context.Context, _ string) ([]db.DiskUsage, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetReplicationLag(_ context.Context) (int64, error) { return 0, nil }
func (m *scenario6MockDBClient) PromoteStandby(_ context.Context) error             { return nil }
func (m *scenario6MockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return 0, 0, 0, nil
}
func (m *scenario6MockDBClient) GetMaxConnections(_ context.Context) (int32, error) {
	return 100, nil
}
func (m *scenario6MockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}
func (m *scenario6MockDBClient) CreateResourceGroup(_ context.Context, opts db.ResourceGroupOptions) error {
	m.createResourceGroupOpts = opts
	return m.createResourceGroupErr
}
func (m *scenario6MockDBClient) AlterResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *scenario6MockDBClient) DropResourceGroup(_ context.Context, name string) error {
	m.dropResourceGroupName = name
	return m.dropResourceGroupErr
}
func (m *scenario6MockDBClient) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return m.listResourceGroups, m.listResourceGroupsErr
}
func (m *scenario6MockDBClient) AssignRoleResourceGroup(_ context.Context, role, group string) error {
	m.assignRole = role
	m.assignGroup = group
	return m.assignErr
}
func (m *scenario6MockDBClient) CreateBackup(_ context.Context, _ db.BackupOptions) (*db.BackupInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error {
	return nil
}
func (m *scenario6MockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) DeleteBackup(_ context.Context, _ string) error { return nil }
func (m *scenario6MockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}
func (m *scenario6MockDBClient) StartDataLoadingJob(_ context.Context, _ string) error { return nil }
func (m *scenario6MockDBClient) StopDataLoadingJob(_ context.Context, _ string) error  { return nil }
func (m *scenario6MockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) TriggerRecommendationScan(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) GetTableDetails(_ context.Context, _, _ string) (*db.TableDetail, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) InitializeMirrors(_ context.Context, _ db.MirrorInitOptions) error {
	return nil
}
func (m *scenario6MockDBClient) ConfigureReplication(_ context.Context, _ db.ReplicationOptions) error {
	return nil
}
func (m *scenario6MockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) TriggerFTSProbe(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) TerminateAllBackends(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *scenario6MockDBClient) CancelAllQueries(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *scenario6MockDBClient) LogRotate(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) CreateResourceQueue(_ context.Context, _ db.ResourceQueueOptions) error {
	return nil
}
func (m *scenario6MockDBClient) DropResourceQueue(_ context.Context, _ string) error { return nil }
func (m *scenario6MockDBClient) ListResourceQueues(_ context.Context) ([]db.ResourceQueueInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) RegisterNewSegments(_ context.Context, _ db.SegmentRegistrationOptions) error {
	return nil
}
func (m *scenario6MockDBClient) RedistributeData(_ context.Context, _ db.RedistributionOptions) error {
	return nil
}
func (m *scenario6MockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) {
	return 100, nil
}
func (m *scenario6MockDBClient) DeregisterSegments(_ context.Context, _ int32) error {
	return nil
}
func (m *scenario6MockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ db.ScaleInRedistributionOptions) error {
	return nil
}
func (m *scenario6MockDBClient) AnalyzeSkew(_ context.Context, _ string) ([]db.TableSkewInfo, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error {
	return nil
}
func (m *scenario6MockDBClient) ListSessionsWithResourceGroup(_ context.Context) ([]db.SessionWithGroup, error) {
	return nil, nil
}
func (m *scenario6MockDBClient) ListUserDatabases(_ context.Context) ([]string, error) {
	return nil, nil
}

// SetupExporterRole implements db.Client.
func (m *scenario6MockDBClient) SetupExporterRole(_ context.Context, _ string) error {
	return nil
}

// GetQueryDetail implements db.Client.
func (m *scenario6MockDBClient) GetQueryDetail(_ context.Context, pid int32) (*db.QueryDetail, error) {
	return &db.QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}

func (m *scenario6MockDBClient) EnsureQueryHistoryTable(_ context.Context) error { return nil }
func (m *scenario6MockDBClient) InsertQueryHistory(_ context.Context, _ *db.QueryHistoryEntry) error {
	return nil
}
func (m *scenario6MockDBClient) GetQueryHistory(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	return []db.QueryHistoryEntry{}, 0, nil
}
func (m *scenario6MockDBClient) GetQueryHistoryDetail(_ context.Context, _ string) (*db.QueryHistoryEntry, error) {
	return nil, fmt.Errorf("not found")
}
func (m *scenario6MockDBClient) ExportQueryHistoryCSV(_ context.Context, _ db.QueryHistoryFilter, _ io.Writer) error {
	return nil
}
func (m *scenario6MockDBClient) CleanupQueryHistory(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *scenario6MockDBClient) MoveQueryToResourceGroup(_ context.Context, _ int32, _ string) error {
	return nil
}

// scenario6MockDBFactory implements db.DBClientFactory for testing.
type scenario6MockDBFactory struct {
	client db.Client
	err    error
}

func (f *scenario6MockDBFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// newScenario6Cluster creates a test cluster for scenario 6.
func newScenario6Cluster() *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scenario6ClusterName,
			Namespace: scenario6Namespace,
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "2.1.0",
			Image:   "cloudberrydb/cloudberry:2.1.0",
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

// newScenario6Server creates an API server with a mock DB factory for testing.
func newScenario6Server(cluster *cbv1alpha1.CloudberryCluster, dbFactory db.DBClientFactory) *api.Server {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)

	var objs []runtime.Object
	if cluster != nil {
		objs = append(objs, cluster)
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return api.NewServer(k8sClient, nil, dbFactory, &metrics.NoopRecorder{}, nil, 0)
}

// scenario6AuthRequest adds an admin identity to the request context so that
// the permission middleware allows the request through.
func scenario6AuthRequest(req *http.Request) *http.Request {
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	return req.WithContext(ctx)
}

// Scenario6ResourceManagementSuite tests resource group management operations
// (create, assign, list, delete resource groups) via the API server handlers.
type Scenario6ResourceManagementSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestScenario6_ResourceManagement(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario6ResourceManagementSuite))
}

func (s *Scenario6ResourceManagementSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *Scenario6ResourceManagementSuite) TearDownTest() {
	s.cancel()
}

// TestScenario6a_CreateResourceGroup verifies that the create resource group
// endpoint calls db.Client.CreateResourceGroup with the correct options.
func (s *Scenario6ResourceManagementSuite) TestScenario6a_CreateResourceGroup() {
	mockClient := &scenario6MockDBClient{}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"name":          "analytics",
		"concurrency":   10,
		"cpuMaxPercent": 50,
		"memoryLimit":   30,
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusCreated, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "analytics", resp["name"])
	assert.Equal(s.T(), float64(10), resp["concurrency"])
	assert.Equal(s.T(), float64(50), resp["cpuMaxPercent"])
	assert.Equal(s.T(), float64(30), resp["memoryLimit"])
	assert.Equal(s.T(), "created", resp["status"])

	// Verify the mock was called with correct options.
	assert.Equal(s.T(), "analytics", mockClient.createResourceGroupOpts.Name)
	assert.Equal(s.T(), int32(10), mockClient.createResourceGroupOpts.Concurrency)
	assert.Equal(s.T(), int32(50), mockClient.createResourceGroupOpts.CPUMaxPercent)
	assert.Equal(s.T(), int32(30), mockClient.createResourceGroupOpts.MemoryLimit)
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario6a_AssignResourceGroup verifies that the assign resource group
// endpoint calls db.Client.AssignRoleResourceGroup with the correct parameters.
func (s *Scenario6ResourceManagementSuite) TestScenario6a_AssignResourceGroup() {
	mockClient := &scenario6MockDBClient{}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"role": "analyst",
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups/analytics/assign?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "analytics", resp["group"])
	assert.Equal(s.T(), "analyst", resp["role"])
	assert.Equal(s.T(), "assigned", resp["status"])

	// Verify the mock was called with correct parameters.
	assert.Equal(s.T(), "analyst", mockClient.assignRole)
	assert.Equal(s.T(), "analytics", mockClient.assignGroup)
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario6a_ListResourceGroups verifies that the list resource groups
// endpoint returns groups from the database via the mock DB client.
func (s *Scenario6ResourceManagementSuite) TestScenario6a_ListResourceGroups() {
	mockClient := &scenario6MockDBClient{
		listResourceGroups: []db.ResourceGroupInfo{
			{
				Name:          "default_group",
				Concurrency:   20,
				CPUMaxPercent: 100,
				CPUUsage:      0.45,
				MemoryUsage:   0.30,
			},
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUUsage:      0.20,
				MemoryUsage:   0.15,
			},
		},
	}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodGet,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups?namespace="+scenario6Namespace, nil))
	req.SetPathValue("name", scenario6ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), float64(2), resp["total"])

	groups, ok := resp["resourceGroups"].([]interface{})
	require.True(s.T(), ok, "resourceGroups should be an array")
	require.Len(s.T(), groups, 2)

	group0, ok := groups[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "default_group", group0["name"])

	group1, ok := groups[1].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "analytics", group1["name"])

	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario6a_DeleteResourceGroup verifies that the delete resource group
// endpoint calls db.Client.DropResourceGroup with the correct name.
func (s *Scenario6ResourceManagementSuite) TestScenario6a_DeleteResourceGroup() {
	mockClient := &scenario6MockDBClient{}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups/analytics?namespace="+scenario6Namespace, nil))
	req.SetPathValue("name", scenario6ClusterName)
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "analytics", resp["group"])
	assert.Equal(s.T(), "deleted", resp["status"])

	// Verify the mock was called with correct name.
	assert.Equal(s.T(), "analytics", mockClient.dropResourceGroupName)
	assert.True(s.T(), mockClient.closed, "db client should be closed after request")
}

// TestScenario6_CreateResourceGroup_NoDBFactory verifies that the API server
// gracefully handles the case when no DBClientFactory is configured.
func (s *Scenario6ResourceManagementSuite) TestScenario6_CreateResourceGroup_NoDBFactory() {
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, nil)

	body, err := json.Marshal(map[string]interface{}{
		"name":          "analytics",
		"concurrency":   10,
		"cpuMaxPercent": 50,
		"memoryLimit":   30,
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusCreated, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "analytics", resp["name"])
	assert.Contains(s.T(), resp["message"], "pending")
}

// TestScenario6_CreateResourceGroup_InvalidBody verifies that the API server
// returns a 400 error when the resource group name is empty.
func (s *Scenario6ResourceManagementSuite) TestScenario6_CreateResourceGroup_InvalidBody() {
	mockClient := &scenario6MockDBClient{}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"name":        "",
		"concurrency": 10,
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])
	assert.Contains(s.T(), errObj["message"], "name is required")
}

// TestScenario6_CreateResourceGroup_DBError verifies that the API server
// returns a 500 error when the database operation fails.
func (s *Scenario6ResourceManagementSuite) TestScenario6_CreateResourceGroup_DBError() {
	mockClient := &scenario6MockDBClient{
		createResourceGroupErr: fmt.Errorf("resource group already exists"),
	}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"name":          "analytics",
		"concurrency":   10,
		"cpuMaxPercent": 50,
		"memoryLimit":   30,
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INTERNAL_ERROR", errObj["code"])
}

// TestScenario6_DeleteResourceGroup_ClusterNotFound verifies that the delete
// resource group endpoint returns a 404 error when the cluster does not exist.
func (s *Scenario6ResourceManagementSuite) TestScenario6_DeleteResourceGroup_ClusterNotFound() {
	srv := newScenario6Server(nil, nil)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario6APIPrefix+"/clusters/nonexistent/workload/resource-groups/analytics?namespace=default", nil))
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

// TestScenario6_AssignResourceGroup_InvalidBody verifies that the assign
// resource group endpoint returns a 400 error when the role is empty.
func (s *Scenario6ResourceManagementSuite) TestScenario6_AssignResourceGroup_InvalidBody() {
	mockClient := &scenario6MockDBClient{}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"role": "",
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups/analytics/assign?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)

	var resp map[string]interface{}
	err = json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(s.T(), err)

	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])
	assert.Contains(s.T(), errObj["message"], "role is required")
}

// TestScenario6_AssignResourceGroup_DBError verifies that the assign resource
// group endpoint returns a 500 error when the database operation fails.
func (s *Scenario6ResourceManagementSuite) TestScenario6_AssignResourceGroup_DBError() {
	mockClient := &scenario6MockDBClient{
		assignErr: fmt.Errorf("role not found"),
	}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	body, err := json.Marshal(map[string]interface{}{
		"role": "nonexistent",
	})
	require.NoError(s.T(), err)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodPost,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups/analytics/assign?namespace="+scenario6Namespace,
		bytes.NewReader(body)))
	req.SetPathValue("name", scenario6ClusterName)
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)
}

// TestScenario6_DeleteResourceGroup_DBError verifies that the delete resource
// group endpoint returns a 500 error when the database operation fails.
func (s *Scenario6ResourceManagementSuite) TestScenario6_DeleteResourceGroup_DBError() {
	mockClient := &scenario6MockDBClient{
		dropResourceGroupErr: fmt.Errorf("resource group in use"),
	}
	factory := &scenario6MockDBFactory{client: mockClient}
	cluster := newScenario6Cluster()
	srv := newScenario6Server(cluster, factory)

	req := scenario6AuthRequest(httptest.NewRequest(http.MethodDelete,
		scenario6APIPrefix+"/clusters/"+scenario6ClusterName+"/workload/resource-groups/analytics?namespace="+scenario6Namespace, nil))
	req.SetPathValue("name", scenario6ClusterName)
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusInternalServerError, rec.Code)
}
