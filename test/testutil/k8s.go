package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
	PingFunc                    func(ctx context.Context) error
	GetSegmentConfigurationFunc func(ctx context.Context) ([]db.SegmentInfo, error)
	GetClusterStateFunc         func(ctx context.Context) (*db.ClusterState, error)
	SetParameterFunc            func(ctx context.Context, name, value string, scope db.ParameterScope) error
	ShowParameterFunc           func(ctx context.Context, name string) (string, error)
	ReloadConfigFunc            func(ctx context.Context) error
	ListSessionsFunc            func(ctx context.Context) ([]db.Session, error)
	CancelQueryFunc             func(ctx context.Context, pid int32) (bool, error)
	TerminateSessionFunc        func(ctx context.Context, pid int32) (bool, error)
	CreateRoleFunc              func(ctx context.Context, opts db.RoleOptions) error
	AlterRoleFunc               func(ctx context.Context, opts db.RoleOptions) error
	DropRoleFunc                func(ctx context.Context, name string) error
	VacuumFunc                  func(ctx context.Context, opts db.VacuumOptions) error
	AnalyzeFunc                 func(ctx context.Context, table string) error
	ReindexFunc                 func(ctx context.Context, opts db.ReindexOptions) error
	GetDiskUsageFunc            func(ctx context.Context, database string) ([]db.DiskUsage, error)
	GetReplicationLagFunc       func(ctx context.Context) (int64, error)
	PromoteStandbyFunc          func(ctx context.Context) error
	GetActiveQueryCountFunc     func(ctx context.Context) (int32, int32, int32, error)
	GetResourceGroupUsageFunc   func(ctx context.Context, group string) (float64, float64, error)
	CreateResourceGroupFunc     func(ctx context.Context, opts db.ResourceGroupOptions) error
	AlterResourceGroupFunc      func(ctx context.Context, opts db.ResourceGroupOptions) error
	DropResourceGroupFunc       func(ctx context.Context, name string) error
	ListResourceGroupsFunc      func(ctx context.Context) ([]db.ResourceGroupInfo, error)
	AssignRoleResourceGroupFunc func(ctx context.Context, role, group string) error
	Closed                      bool
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

// GetClusterState implements db.Client.
func (m *MockDBClient) GetClusterState(ctx context.Context) (*db.ClusterState, error) {
	if m.GetClusterStateFunc != nil {
		return m.GetClusterStateFunc(ctx)
	}
	return &db.ClusterState{
		IsUp:              true,
		Version:           "7.7",
		SegmentsUp:        4,
		SegmentsDown:      0,
		SegmentsTotal:     4,
		MirroringInSync:   true,
		ActiveConnections: 5,
		MaxConnections:    200,
	}, nil
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

// MockDBClientFactory implements controller.DBClientFactory for testing.
type MockDBClientFactory struct {
	Client *MockDBClient
	Err    error
}

// NewClient implements controller.DBClientFactory.
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
