package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// mockDBClientFactory implements DBClientFactory for testing.
type mockDBClientFactory struct {
	client db.Client
	err    error
}

func (f *mockDBClientFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	return f.client, f.err
}

// mockDBClient implements db.Client for testing.
type mockDBClient struct {
	segments       []db.SegmentInfo
	segErr         error
	clusterState   *db.ClusterState
	stateErr       error
	replicationLag int64
	repLagErr      error
}

func (m *mockDBClient) Ping(_ context.Context) error { return nil }
func (m *mockDBClient) Close()                       {}
func (m *mockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return m.segments, m.segErr
}
func (m *mockDBClient) GetClusterState(_ context.Context) (*db.ClusterState, error) {
	return m.clusterState, m.stateErr
}
func (m *mockDBClient) SetParameter(_ context.Context, _, _ string, _ db.ParameterScope) error {
	return nil
}
func (m *mockDBClient) ShowParameter(_ context.Context, _ string) (string, error) { return "", nil }
func (m *mockDBClient) ReloadConfig(_ context.Context) error                      { return nil }
func (m *mockDBClient) ListSessions(_ context.Context) ([]db.Session, error)      { return nil, nil }
func (m *mockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error)      { return true, nil }
func (m *mockDBClient) TerminateSession(_ context.Context, _ int32) (bool, error) { return true, nil }
func (m *mockDBClient) CreateRole(_ context.Context, _ db.RoleOptions) error      { return nil }
func (m *mockDBClient) AlterRole(_ context.Context, _ db.RoleOptions) error       { return nil }
func (m *mockDBClient) DropRole(_ context.Context, _ string) error                { return nil }
func (m *mockDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error        { return nil }
func (m *mockDBClient) Analyze(_ context.Context, _ string) error                 { return nil }
func (m *mockDBClient) Reindex(_ context.Context, _ db.ReindexOptions) error      { return nil }
func (m *mockDBClient) GetDiskUsage(_ context.Context, _ string) ([]db.DiskUsage, error) {
	return nil, nil
}
func (m *mockDBClient) GetReplicationLag(_ context.Context) (int64, error) {
	return m.replicationLag, m.repLagErr
}
func (m *mockDBClient) PromoteStandby(_ context.Context) error { return nil }
func (m *mockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return 0, 0, 0, nil
}
func (m *mockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}
func (m *mockDBClient) CreateResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *mockDBClient) AlterResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *mockDBClient) DropResourceGroup(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return nil, nil
}
func (m *mockDBClient) CreateBackup(_ context.Context, opts db.BackupOptions) (*db.BackupInfo, error) {
	return &db.BackupInfo{ID: "test", Type: opts.Type, Status: "InProgress"}, nil
}
func (m *mockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error { return nil }
func (m *mockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error) {
	return nil, nil
}
func (m *mockDBClient) DeleteBackup(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}
func (m *mockDBClient) StartDataLoadingJob(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) StopDataLoadingJob(_ context.Context, _ string) error  { return nil }
func (m *mockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return nil, nil
}
func (m *mockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return nil, nil
}
func (m *mockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerRecommendationScan(_ context.Context) error { return nil }
func (m *mockDBClient) GetTableDetails(_ context.Context, _, _ string) (*db.TableDetail, error) {
	return nil, nil
}
func (m *mockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return nil, nil
}

func TestNewHAReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)
	require.NotNil(t, r)
}

func TestHAReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestHAReconciler_Reconcile_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_Reconcile_Running(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}
	dbFactory := &mockDBClientFactory{
		client: &mockDBClient{},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_Reconcile_RecoveryAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationRecovery: util.RecoveryIncremental,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_Reconcile_RebalanceAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionRebalance,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_Reconcile_ActivateStandbyAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionActivateStandby,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_ProbeInterval(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, &metrics.NoopRecorder{}, nil)

	t.Run("default interval", func(t *testing.T) {
		cluster := newTestCluster()
		interval := r.probeInterval(cluster)
		assert.Equal(t, 60, int(interval.Seconds()))
	})

	t.Run("custom interval", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeInterval: 30}
		interval := r.probeInterval(cluster)
		assert.Equal(t, 30, int(interval.Seconds()))
	})
}

func TestHAReconciler_RunFTSProbe_AllHealthy(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: -1, Status: "u", Role: "p"}, // coordinator, should be skipped
			{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
			{ContentID: 1, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 1, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, m, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.MirroringInSync, cluster.Status.MirroringStatus)
	assert.Empty(t, cluster.Status.FailedSegments)
}

func TestHAReconciler_RunFTSProbe_DegradedSegments(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 0, Status: "d", Role: "m", Hostname: "host2"}, // down
			{ContentID: 1, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 1, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, m, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.MirroringDegraded, cluster.Status.MirroringStatus)
	assert.Len(t, cluster.Status.FailedSegments, 1)
}

func TestHAReconciler_MonitorStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClient{
		replicationLag: 1024,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, m, nil)

	err := r.monitorStandby(context.Background(), cluster)
	require.NoError(t, err)

	cond := findCondition(cluster.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
