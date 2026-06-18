package testutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// TestK8sEnv provides a test Kubernetes environment with a fake client.
type TestK8sEnv struct {
	Client   client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Builder  builder.ResourceBuilder
	Metrics  metrics.Recorder
	Logger   *slog.Logger
}

// NewTestK8sEnv creates a new test Kubernetes environment.
func NewTestK8sEnv(initObjects ...client.Object) *TestK8sEnv {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	// Build the fake client with status subresource for CloudberryCluster.
	fakeClientBuilder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{})

	if len(initObjects) > 0 {
		fakeClientBuilder = fakeClientBuilder.WithObjects(initObjects...)
	}

	return &TestK8sEnv{
		Client:   fakeClientBuilder.Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Builder:  builder.NewBuilder(),
		Metrics:  &metrics.NoopRecorder{},
		Logger:   slog.Default(),
	}
}

// GetCluster retrieves a CloudberryCluster from the fake client.
func (e *TestK8sEnv) GetCluster(ctx context.Context, name, namespace string) (*cbv1alpha1.CloudberryCluster, error) {
	cluster := &cbv1alpha1.CloudberryCluster{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, cluster)
	if err != nil {
		return nil, fmt.Errorf("getting cluster %s/%s: %w", namespace, name, err)
	}
	return cluster, nil
}

// CreateCluster creates a CloudberryCluster in the fake client.
func (e *TestK8sEnv) CreateCluster(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	return e.Client.Create(ctx, cluster)
}

// UpdateClusterStatus updates the status of a CloudberryCluster.
func (e *TestK8sEnv) UpdateClusterStatus(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	return e.Client.Status().Update(ctx, cluster)
}

// CreateNamespace creates a namespace in the fake client.
func (e *TestK8sEnv) CreateNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	return e.Client.Create(ctx, ns)
}

// GetStatefulSet retrieves a StatefulSet from the fake client.
func (e *TestK8sEnv) GetStatefulSet(ctx context.Context, name, namespace string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts)
	if err != nil {
		return nil, fmt.Errorf("getting statefulset %s/%s: %w", namespace, name, err)
	}
	return sts, nil
}

// GetService retrieves a Service from the fake client.
func (e *TestK8sEnv) GetService(ctx context.Context, name, namespace string) (*corev1.Service, error) {
	svc := &corev1.Service{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, svc)
	if err != nil {
		return nil, fmt.Errorf("getting service %s/%s: %w", namespace, name, err)
	}
	return svc, nil
}

// GetJob retrieves a Job from the fake client.
func (e *TestK8sEnv) GetJob(ctx context.Context, name, namespace string) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, job)
	if err != nil {
		return nil, fmt.Errorf("getting job %s/%s: %w", namespace, name, err)
	}
	return job, nil
}

// GetConfigMap retrieves a ConfigMap from the fake client.
func (e *TestK8sEnv) GetConfigMap(ctx context.Context, name, namespace string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, cm)
	if err != nil {
		return nil, fmt.Errorf("getting configmap %s/%s: %w", namespace, name, err)
	}
	return cm, nil
}

// GetSecret retrieves a Secret from the fake client.
func (e *TestK8sEnv) GetSecret(ctx context.Context, name, namespace string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	err := e.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret)
	if err != nil {
		return nil, fmt.Errorf("getting secret %s/%s: %w", namespace, name, err)
	}
	return secret, nil
}

// MockDBClient implements db.Client for testing.
type MockDBClient struct {
	PingFunc                          func(ctx context.Context) error
	GetSegmentConfigurationFunc       func(ctx context.Context) ([]db.SegmentInfo, error)
	SetParameterFunc                  func(ctx context.Context, name, value string, scope db.ParameterScope) error
	ShowParameterFunc                 func(ctx context.Context, name string) (string, error)
	ReloadConfigFunc                  func(ctx context.Context) error
	ListSessionsFunc                  func(ctx context.Context) ([]db.Session, error)
	CancelQueryFunc                   func(ctx context.Context, pid int32) (bool, error)
	TerminateSessionFunc              func(ctx context.Context, pid int32) (bool, error)
	CreateRoleFunc                    func(ctx context.Context, opts db.RoleOptions) error
	AlterRoleFunc                     func(ctx context.Context, opts db.RoleOptions) error
	DropRoleFunc                      func(ctx context.Context, name string) error
	VacuumFunc                        func(ctx context.Context, opts db.VacuumOptions) error
	AnalyzeFunc                       func(ctx context.Context, table string) error
	ReindexFunc                       func(ctx context.Context, opts db.ReindexOptions) error
	GetDiskUsageFunc                  func(ctx context.Context, database string) ([]db.DiskUsage, error)
	GetReplicationLagFunc             func(ctx context.Context) (int64, error)
	PromoteStandbyFunc                func(ctx context.Context) error
	GetMaxConnectionsFunc             func(ctx context.Context) (int32, error)
	GetActiveQueryCountFunc           func(ctx context.Context) (int32, int32, int32, error)
	GetResourceGroupUsageFunc         func(ctx context.Context, group string) (float64, float64, error)
	CreateResourceGroupFunc           func(ctx context.Context, opts db.ResourceGroupOptions) error
	AlterResourceGroupFunc            func(ctx context.Context, opts db.ResourceGroupOptions) error
	DropResourceGroupFunc             func(ctx context.Context, name string) error
	ListResourceGroupsFunc            func(ctx context.Context) ([]db.ResourceGroupInfo, error)
	AssignRoleResourceGroupFunc       func(ctx context.Context, role, group string) error
	CreateResourceQueueFunc           func(ctx context.Context, opts db.ResourceQueueOptions) error
	DropResourceQueueFunc             func(ctx context.Context, name string) error
	ListResourceQueuesFunc            func(ctx context.Context) ([]db.ResourceQueueInfo, error)
	InitializeMirrorsFunc             func(ctx context.Context, opts db.MirrorInitOptions) error
	ConfigureReplicationFunc          func(ctx context.Context, opts db.ReplicationOptions) error
	GetMirrorSyncStatusFunc           func(ctx context.Context) ([]db.MirrorSyncInfo, error)
	TriggerFTSProbeFunc               func(ctx context.Context) error
	TerminateAllBackendsFunc          func(ctx context.Context) (int32, error)
	CancelAllQueriesFunc              func(ctx context.Context) (int32, error)
	LogRotateFunc                     func(ctx context.Context) error
	RegisterNewSegmentsFunc           func(ctx context.Context, opts db.SegmentRegistrationOptions) error
	RedistributeDataFunc              func(ctx context.Context, opts db.RedistributionOptions) error
	GetRedistributionProgressFunc     func(ctx context.Context) (int32, error)
	DeregisterSegmentsFunc            func(ctx context.Context, newCount int32) error
	RedistributeBeforeScaleInFunc     func(ctx context.Context, opts db.ScaleInRedistributionOptions) error
	AnalyzeSkewFunc                   func(ctx context.Context, database string) ([]db.TableSkewInfo, error)
	ListSessionsWithResourceGroupFunc func(ctx context.Context) ([]db.SessionWithGroup, error)
	RebalanceTableFunc                func(ctx context.Context, database, schema, table, distKey string) error
	SetupExporterRoleFunc             func(ctx context.Context, password string) error
	SetupPXFExtensionsFunc            func(ctx context.Context) (int, error)
	EnsureDataLoaderRoleFunc          func(ctx context.Context, roleName string) error
	ListPXFExtensionsFunc             func(ctx context.Context) ([]string, error)
	ListExternalTablesFunc            func(ctx context.Context) ([]db.ExternalTableInfo, error)
	ReadPXFSourceSampleFunc           func(ctx context.Context, server, profile, resource string, limit int) (*db.PXFSourceSample, error)
	GetQueryDetailFunc                func(ctx context.Context, pid int32) (*db.QueryDetail, error)
	EnsureQueryHistoryTableFunc       func(ctx context.Context) error
	InsertQueryHistoryFunc            func(ctx context.Context, entry *db.QueryHistoryEntry) error
	GetQueryHistoryFunc               func(ctx context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error)
	GetQueryHistoryDetailFunc         func(ctx context.Context, queryID string) (*db.QueryHistoryEntry, error)
	ExportQueryHistoryCSVFunc         func(ctx context.Context, filter db.QueryHistoryFilter, w io.Writer) error
	CleanupQueryHistoryFunc           func(ctx context.Context, retention time.Duration) (int64, error)
	MoveQueryToResourceGroupFunc      func(ctx context.Context, pid int32, targetGroup string) error
	Closed                            bool
}

// Ping implements db.Client.
func (m *MockDBClient) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// Close implements db.Client.
func (m *MockDBClient) Close() {
	m.Closed = true
}

// GetSegmentConfiguration implements db.Client.
func (m *MockDBClient) GetSegmentConfiguration(ctx context.Context) ([]db.SegmentInfo, error) {
	if m.GetSegmentConfigurationFunc != nil {
		return m.GetSegmentConfigurationFunc(ctx)
	}
	return DefaultSegmentConfiguration(), nil
}

// SetParameter implements db.Client.
func (m *MockDBClient) SetParameter(ctx context.Context, name, value string, scope db.ParameterScope) error {
	if m.SetParameterFunc != nil {
		return m.SetParameterFunc(ctx, name, value, scope)
	}
	return nil
}

// ShowParameter implements db.Client.
func (m *MockDBClient) ShowParameter(ctx context.Context, name string) (string, error) {
	if m.ShowParameterFunc != nil {
		return m.ShowParameterFunc(ctx, name)
	}
	return "default_value", nil
}

// ReloadConfig implements db.Client.
func (m *MockDBClient) ReloadConfig(ctx context.Context) error {
	if m.ReloadConfigFunc != nil {
		return m.ReloadConfigFunc(ctx)
	}
	return nil
}

// ListSessions implements db.Client.
func (m *MockDBClient) ListSessions(ctx context.Context) ([]db.Session, error) {
	if m.ListSessionsFunc != nil {
		return m.ListSessionsFunc(ctx)
	}
	return []db.Session{
		{
			PID:        1234,
			Username:   "gpadmin",
			State:      "active",
			Query:      "SELECT 1",
			QueryStart: time.Now(),
		},
	}, nil
}

// CancelQuery implements db.Client.
func (m *MockDBClient) CancelQuery(ctx context.Context, pid int32) (bool, error) {
	if m.CancelQueryFunc != nil {
		return m.CancelQueryFunc(ctx, pid)
	}
	return true, nil
}

// TerminateSession implements db.Client.
func (m *MockDBClient) TerminateSession(ctx context.Context, pid int32) (bool, error) {
	if m.TerminateSessionFunc != nil {
		return m.TerminateSessionFunc(ctx, pid)
	}
	return true, nil
}

// CreateRole implements db.Client.
func (m *MockDBClient) CreateRole(ctx context.Context, opts db.RoleOptions) error {
	if m.CreateRoleFunc != nil {
		return m.CreateRoleFunc(ctx, opts)
	}
	return nil
}

// AlterRole implements db.Client.
func (m *MockDBClient) AlterRole(ctx context.Context, opts db.RoleOptions) error {
	if m.AlterRoleFunc != nil {
		return m.AlterRoleFunc(ctx, opts)
	}
	return nil
}

// DropRole implements db.Client.
func (m *MockDBClient) DropRole(ctx context.Context, name string) error {
	if m.DropRoleFunc != nil {
		return m.DropRoleFunc(ctx, name)
	}
	return nil
}

// Vacuum implements db.Client.
func (m *MockDBClient) Vacuum(ctx context.Context, opts db.VacuumOptions) error {
	if m.VacuumFunc != nil {
		return m.VacuumFunc(ctx, opts)
	}
	return nil
}

// Analyze implements db.Client.
func (m *MockDBClient) Analyze(ctx context.Context, table string) error {
	if m.AnalyzeFunc != nil {
		return m.AnalyzeFunc(ctx, table)
	}
	return nil
}

// Reindex implements db.Client.
func (m *MockDBClient) Reindex(ctx context.Context, opts db.ReindexOptions) error {
	if m.ReindexFunc != nil {
		return m.ReindexFunc(ctx, opts)
	}
	return nil
}

// GetDiskUsage implements db.Client.
func (m *MockDBClient) GetDiskUsage(ctx context.Context, database string) ([]db.DiskUsage, error) {
	if m.GetDiskUsageFunc != nil {
		return m.GetDiskUsageFunc(ctx, database)
	}
	return []db.DiskUsage{
		{Database: "postgres", SizeBytes: 1024 * 1024, SizeHuman: "1 MB"},
	}, nil
}

// GetReplicationLag implements db.Client.
func (m *MockDBClient) GetReplicationLag(ctx context.Context) (int64, error) {
	if m.GetReplicationLagFunc != nil {
		return m.GetReplicationLagFunc(ctx)
	}
	return 0, nil
}

// PromoteStandby implements db.Client.
func (m *MockDBClient) PromoteStandby(ctx context.Context) error {
	if m.PromoteStandbyFunc != nil {
		return m.PromoteStandbyFunc(ctx)
	}
	return nil
}

// GetMaxConnections implements db.Client.
func (m *MockDBClient) GetMaxConnections(ctx context.Context) (int32, error) {
	if m.GetMaxConnectionsFunc != nil {
		return m.GetMaxConnectionsFunc(ctx)
	}
	return 100, nil
}

// GetActiveQueryCount implements db.Client.
func (m *MockDBClient) GetActiveQueryCount(ctx context.Context) (int32, int32, int32, error) {
	if m.GetActiveQueryCountFunc != nil {
		return m.GetActiveQueryCountFunc(ctx)
	}
	return 5, 0, 0, nil
}

// GetResourceGroupUsage implements db.Client.
func (m *MockDBClient) GetResourceGroupUsage(ctx context.Context, group string) (float64, float64, error) {
	if m.GetResourceGroupUsageFunc != nil {
		return m.GetResourceGroupUsageFunc(ctx, group)
	}
	return 25.0, 50.0, nil
}

// CreateResourceGroup implements db.Client.
func (m *MockDBClient) CreateResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	if m.CreateResourceGroupFunc != nil {
		return m.CreateResourceGroupFunc(ctx, opts)
	}
	return nil
}

// AlterResourceGroup implements db.Client.
func (m *MockDBClient) AlterResourceGroup(ctx context.Context, opts db.ResourceGroupOptions) error {
	if m.AlterResourceGroupFunc != nil {
		return m.AlterResourceGroupFunc(ctx, opts)
	}
	return nil
}

// DropResourceGroup implements db.Client.
func (m *MockDBClient) DropResourceGroup(ctx context.Context, name string) error {
	if m.DropResourceGroupFunc != nil {
		return m.DropResourceGroupFunc(ctx, name)
	}
	return nil
}

// ListResourceGroups implements db.Client.
func (m *MockDBClient) ListResourceGroups(ctx context.Context) ([]db.ResourceGroupInfo, error) {
	if m.ListResourceGroupsFunc != nil {
		return m.ListResourceGroupsFunc(ctx)
	}
	return []db.ResourceGroupInfo{
		{Name: "default_group", Concurrency: 20, CPUUsage: 10.0, MemoryUsage: 30.0},
	}, nil
}

// AssignRoleResourceGroup implements db.Client.
func (m *MockDBClient) AssignRoleResourceGroup(ctx context.Context, role, group string) error {
	if m.AssignRoleResourceGroupFunc != nil {
		return m.AssignRoleResourceGroupFunc(ctx, role, group)
	}
	return nil
}

// CreateResourceQueue implements db.Client.
func (m *MockDBClient) CreateResourceQueue(ctx context.Context, opts db.ResourceQueueOptions) error {
	if m.CreateResourceQueueFunc != nil {
		return m.CreateResourceQueueFunc(ctx, opts)
	}
	return nil
}

// DropResourceQueue implements db.Client.
func (m *MockDBClient) DropResourceQueue(ctx context.Context, name string) error {
	if m.DropResourceQueueFunc != nil {
		return m.DropResourceQueueFunc(ctx, name)
	}
	return nil
}

// ListResourceQueues implements db.Client.
func (m *MockDBClient) ListResourceQueues(ctx context.Context) ([]db.ResourceQueueInfo, error) {
	if m.ListResourceQueuesFunc != nil {
		return m.ListResourceQueuesFunc(ctx)
	}
	return []db.ResourceQueueInfo{
		{Name: "pg_default", ActiveStatements: 20, MemoryLimit: "-1", Priority: "MEDIUM"},
	}, nil
}

// CreateBackup implements db.Client.
func (m *MockDBClient) CreateBackup(_ context.Context, opts db.BackupOptions) (*db.BackupInfo, error) {
	return &db.BackupInfo{
		ID:     "backup-test-1",
		Type:   opts.Type,
		Status: "InProgress",
	}, nil
}

// RestoreBackup implements db.Client.
func (m *MockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error {
	return nil
}

// ListBackups implements db.Client.
func (m *MockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error) {
	return []db.BackupInfo{}, nil
}

// DeleteBackup implements db.Client.
func (m *MockDBClient) DeleteBackup(_ context.Context, _ string) error {
	return nil
}

// CreateDataLoadingJob implements db.Client.
func (m *MockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}

// StartDataLoadingJob implements db.Client.
func (m *MockDBClient) StartDataLoadingJob(_ context.Context, _ string) error {
	return nil
}

// StopDataLoadingJob implements db.Client.
func (m *MockDBClient) StopDataLoadingJob(_ context.Context, _ string) error {
	return nil
}

// ListDataLoadingJobs implements db.Client.
func (m *MockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return []db.DataLoadingJobStatus{}, nil
}

// GetStorageDiskUsage implements db.Client.
func (m *MockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return []db.DiskUsageInfo{}, nil
}

// GetBloatRecommendations implements db.Client.
func (m *MockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return []db.Recommendation{}, nil
}

// GetSkewRecommendations implements db.Client.
func (m *MockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return []db.Recommendation{}, nil
}

// GetAgeRecommendations implements db.Client.
func (m *MockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return []db.Recommendation{}, nil
}

// GetIndexBloatRecommendations implements db.Client.
func (m *MockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return []db.Recommendation{}, nil
}

// TriggerRecommendationScan implements db.Client.
func (m *MockDBClient) TriggerRecommendationScan(_ context.Context) error {
	return nil
}

// GetTableDetails implements db.Client.
func (m *MockDBClient) GetTableDetails(_ context.Context, schema, table string) (*db.TableDetail, error) {
	return &db.TableDetail{Schema: schema, Table: table}, nil
}

// GetUsageReport implements db.Client.
func (m *MockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return []db.UsageReportEntry{}, nil
}

// InitializeMirrors implements db.Client.
func (m *MockDBClient) InitializeMirrors(ctx context.Context, opts db.MirrorInitOptions) error {
	if m.InitializeMirrorsFunc != nil {
		return m.InitializeMirrorsFunc(ctx, opts)
	}
	return nil
}

// ConfigureReplication implements db.Client.
func (m *MockDBClient) ConfigureReplication(ctx context.Context, opts db.ReplicationOptions) error {
	if m.ConfigureReplicationFunc != nil {
		return m.ConfigureReplicationFunc(ctx, opts)
	}
	return nil
}

// GetMirrorSyncStatus implements db.Client.
func (m *MockDBClient) GetMirrorSyncStatus(ctx context.Context) ([]db.MirrorSyncInfo, error) {
	if m.GetMirrorSyncStatusFunc != nil {
		return m.GetMirrorSyncStatusFunc(ctx)
	}
	return []db.MirrorSyncInfo{}, nil
}

// TriggerFTSProbe implements db.Client.
func (m *MockDBClient) TriggerFTSProbe(ctx context.Context) error {
	if m.TriggerFTSProbeFunc != nil {
		return m.TriggerFTSProbeFunc(ctx)
	}
	return nil
}

// TerminateAllBackends implements db.Client.
func (m *MockDBClient) TerminateAllBackends(ctx context.Context) (int32, error) {
	if m.TerminateAllBackendsFunc != nil {
		return m.TerminateAllBackendsFunc(ctx)
	}
	return 0, nil
}

// CancelAllQueries implements db.Client.
func (m *MockDBClient) CancelAllQueries(ctx context.Context) (int32, error) {
	if m.CancelAllQueriesFunc != nil {
		return m.CancelAllQueriesFunc(ctx)
	}
	return 0, nil
}

// LogRotate implements db.Client.
func (m *MockDBClient) LogRotate(ctx context.Context) error {
	if m.LogRotateFunc != nil {
		return m.LogRotateFunc(ctx)
	}
	return nil
}

// RegisterNewSegments implements db.Client.
func (m *MockDBClient) RegisterNewSegments(ctx context.Context, opts db.SegmentRegistrationOptions) error {
	if m.RegisterNewSegmentsFunc != nil {
		return m.RegisterNewSegmentsFunc(ctx, opts)
	}
	return nil
}

// RedistributeData implements db.Client.
func (m *MockDBClient) RedistributeData(ctx context.Context, opts db.RedistributionOptions) error {
	if m.RedistributeDataFunc != nil {
		return m.RedistributeDataFunc(ctx, opts)
	}
	return nil
}

// GetRedistributionProgress implements db.Client.
func (m *MockDBClient) GetRedistributionProgress(ctx context.Context) (int32, error) {
	if m.GetRedistributionProgressFunc != nil {
		return m.GetRedistributionProgressFunc(ctx)
	}
	return 100, nil
}

// DeregisterSegments implements db.Client.
func (m *MockDBClient) DeregisterSegments(ctx context.Context, newCount int32) error {
	if m.DeregisterSegmentsFunc != nil {
		return m.DeregisterSegmentsFunc(ctx, newCount)
	}
	return nil
}

// RedistributeBeforeScaleIn implements db.Client.
func (m *MockDBClient) RedistributeBeforeScaleIn(ctx context.Context, opts db.ScaleInRedistributionOptions) error {
	if m.RedistributeBeforeScaleInFunc != nil {
		return m.RedistributeBeforeScaleInFunc(ctx, opts)
	}
	return nil
}

// AnalyzeSkew implements db.Client.
func (m *MockDBClient) AnalyzeSkew(ctx context.Context, database string) ([]db.TableSkewInfo, error) {
	if m.AnalyzeSkewFunc != nil {
		return m.AnalyzeSkewFunc(ctx, database)
	}
	return []db.TableSkewInfo{}, nil
}

// RebalanceTable implements db.Client.
func (m *MockDBClient) RebalanceTable(ctx context.Context, database, schema, table, distKey string) error {
	if m.RebalanceTableFunc != nil {
		return m.RebalanceTableFunc(ctx, database, schema, table, distKey)
	}
	return nil
}

// ListUserDatabases returns a list of user databases.
// ListSessionsWithResourceGroup implements db.Client.
func (m *MockDBClient) ListSessionsWithResourceGroup(ctx context.Context) ([]db.SessionWithGroup, error) {
	if m.ListSessionsWithResourceGroupFunc != nil {
		return m.ListSessionsWithResourceGroupFunc(ctx)
	}
	return nil, nil
}

func (m *MockDBClient) ListUserDatabases(_ context.Context) ([]string, error) {
	return []string{"mydb"}, nil
}

// SetupExporterRole implements db.Client.
func (m *MockDBClient) SetupExporterRole(ctx context.Context, password string) error {
	if m.SetupExporterRoleFunc != nil {
		return m.SetupExporterRoleFunc(ctx, password)
	}
	return nil
}

// SetupPXFExtensions implements db.Client. It returns the number of extensions
// installed (default 2 — both pxf and pxf_fdw) so the default mock marks PXF
// ready; override SetupPXFExtensionsFunc to exercise the 0-installed retry path.
func (m *MockDBClient) SetupPXFExtensions(ctx context.Context) (int, error) {
	if m.SetupPXFExtensionsFunc != nil {
		return m.SetupPXFExtensionsFunc(ctx)
	}
	return 2, nil
}

// EnsureDataLoaderRole implements db.Client. It returns nil by default (the
// SE.6 minimal-privilege role setup is best-effort/non-fatal). Override
// EnsureDataLoaderRoleFunc to exercise specific behavior.
func (m *MockDBClient) EnsureDataLoaderRole(ctx context.Context, roleName string) error {
	if m.EnsureDataLoaderRoleFunc != nil {
		return m.EnsureDataLoaderRoleFunc(ctx, roleName)
	}
	return nil
}

// ListPXFExtensions implements db.Client. It returns nil by default (a reachable
// DB with no PXF extensions present — the honest default on the stub image);
// override ListPXFExtensionsFunc to exercise the observed/unobservable paths.
func (m *MockDBClient) ListPXFExtensions(ctx context.Context) ([]string, error) {
	if m.ListPXFExtensionsFunc != nil {
		return m.ListPXFExtensionsFunc(ctx)
	}
	return nil, nil
}

// ListExternalTables implements db.Client. It returns nil by default (a
// reachable DB with no external/foreign tables — the honest default); override
// ListExternalTablesFunc to exercise the observed/unobservable paths.
func (m *MockDBClient) ListExternalTables(ctx context.Context) ([]db.ExternalTableInfo, error) {
	if m.ListExternalTablesFunc != nil {
		return m.ListExternalTablesFunc(ctx)
	}
	return nil, nil
}

// ReadPXFSourceSample implements db.Client. It returns (nil, nil) by default;
// override ReadPXFSourceSampleFunc to seed real rows for the happy path or an
// error for the honest ABSENT (source unreachable) path.
func (m *MockDBClient) ReadPXFSourceSample(
	ctx context.Context, server, profile, resource string, limit int,
) (*db.PXFSourceSample, error) {
	if m.ReadPXFSourceSampleFunc != nil {
		return m.ReadPXFSourceSampleFunc(ctx, server, profile, resource, limit)
	}
	return nil, nil
}

// GetQueryDetail implements db.Client.
func (m *MockDBClient) GetQueryDetail(ctx context.Context, pid int32) (*db.QueryDetail, error) {
	if m.GetQueryDetailFunc != nil {
		return m.GetQueryDetailFunc(ctx, pid)
	}
	return &db.QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}

// EnsureQueryHistoryTable implements db.Client.
func (m *MockDBClient) EnsureQueryHistoryTable(ctx context.Context) error {
	if m.EnsureQueryHistoryTableFunc != nil {
		return m.EnsureQueryHistoryTableFunc(ctx)
	}
	return nil
}

// InsertQueryHistory implements db.Client.
func (m *MockDBClient) InsertQueryHistory(ctx context.Context, entry *db.QueryHistoryEntry) error {
	if m.InsertQueryHistoryFunc != nil {
		return m.InsertQueryHistoryFunc(ctx, entry)
	}
	return nil
}

// GetQueryHistory implements db.Client.
func (m *MockDBClient) GetQueryHistory(ctx context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	if m.GetQueryHistoryFunc != nil {
		return m.GetQueryHistoryFunc(ctx, filter)
	}
	return []db.QueryHistoryEntry{}, 0, nil
}

// GetQueryHistoryDetail implements db.Client.
func (m *MockDBClient) GetQueryHistoryDetail(ctx context.Context, queryID string) (*db.QueryHistoryEntry, error) {
	if m.GetQueryHistoryDetailFunc != nil {
		return m.GetQueryHistoryDetailFunc(ctx, queryID)
	}
	return nil, fmt.Errorf("query %s not found", queryID)
}

// ExportQueryHistoryCSV implements db.Client.
func (m *MockDBClient) ExportQueryHistoryCSV(ctx context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
	if m.ExportQueryHistoryCSVFunc != nil {
		return m.ExportQueryHistoryCSVFunc(ctx, filter, w)
	}
	return nil
}

// CleanupQueryHistory implements db.Client.
func (m *MockDBClient) CleanupQueryHistory(ctx context.Context, retention time.Duration) (int64, error) {
	if m.CleanupQueryHistoryFunc != nil {
		return m.CleanupQueryHistoryFunc(ctx, retention)
	}
	return 0, nil
}

// MoveQueryToResourceGroup implements db.Client.
func (m *MockDBClient) MoveQueryToResourceGroup(ctx context.Context, pid int32, targetGroup string) error {
	if m.MoveQueryToResourceGroupFunc != nil {
		return m.MoveQueryToResourceGroupFunc(ctx, pid, targetGroup)
	}
	return nil
}

// MockDBClientFactory implements db.DBClientFactory for testing.
type MockDBClientFactory struct {
	Client *MockDBClient
	Err    error
}

// NewClient implements db.DBClientFactory.
func (f *MockDBClientFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Client, nil
}

// DefaultSegmentConfiguration returns a default segment configuration for testing.
func DefaultSegmentConfiguration() []db.SegmentInfo {
	return []db.SegmentInfo{
		{ContentID: -1, DBID: 1, Role: "p", PreferredRole: "p", Mode: "n", Status: "u", Hostname: "coordinator", Address: "coordinator", Port: 5432, DataDirectory: "/data/coordinator"},
		{ContentID: 0, DBID: 2, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 6000, DataDirectory: "/data/primary/gpseg0"},
		{ContentID: 0, DBID: 5, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7000, DataDirectory: "/data/mirror/gpseg0"},
		{ContentID: 1, DBID: 3, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 6001, DataDirectory: "/data/primary/gpseg1"},
		{ContentID: 1, DBID: 6, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7001, DataDirectory: "/data/mirror/gpseg1"},
		{ContentID: 2, DBID: 4, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6000, DataDirectory: "/data/primary/gpseg2"},
		{ContentID: 2, DBID: 7, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7000, DataDirectory: "/data/mirror/gpseg2"},
		{ContentID: 3, DBID: 8, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6001, DataDirectory: "/data/primary/gpseg3"},
		{ContentID: 3, DBID: 9, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7001, DataDirectory: "/data/mirror/gpseg3"},
	}
}

// DegradedSegmentConfiguration returns a segment configuration with a failed segment.
func DegradedSegmentConfiguration() []db.SegmentInfo {
	segments := DefaultSegmentConfiguration()
	// Mark segment 1 mirror as down.
	segments[4].Status = "d"
	return segments
}
