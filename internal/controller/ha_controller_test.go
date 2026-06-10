package controller

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// mockDBClientFactory implements db.DBClientFactory for testing.
type mockDBClientFactory struct {
	client db.Client
	err    error
}

func (f *mockDBClientFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	return f.client, f.err
}

// mockDBClient implements db.Client for testing.
type mockDBClient struct {
	segments           []db.SegmentInfo
	segErr             error
	clusterState       *db.ClusterState
	stateErr           error
	replicationLag     int64
	repLagErr          error
	triggerFTSProbeErr error
	maxConnections     int32
	maxConnectionsErr  error
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
func (m *mockDBClient) GetMaxConnections(_ context.Context) (int32, error) {
	return m.maxConnections, m.maxConnectionsErr
}
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
func (m *mockDBClient) AssignRoleResourceGroup(_ context.Context, _, _ string) error { return nil }
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
func (m *mockDBClient) InitializeMirrors(_ context.Context, _ db.MirrorInitOptions) error {
	return nil
}
func (m *mockDBClient) ConfigureReplication(_ context.Context, _ db.ReplicationOptions) error {
	return nil
}
func (m *mockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerFTSProbe(_ context.Context) error {
	return m.triggerFTSProbeErr
}
func (m *mockDBClient) TerminateAllBackends(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *mockDBClient) CancelAllQueries(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *mockDBClient) LogRotate(_ context.Context) error { return nil }
func (m *mockDBClient) CreateResourceQueue(_ context.Context, _ db.ResourceQueueOptions) error {
	return nil
}
func (m *mockDBClient) DropResourceQueue(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) ListResourceQueues(_ context.Context) ([]db.ResourceQueueInfo, error) {
	return nil, nil
}
func (m *mockDBClient) RegisterNewSegments(_ context.Context, _ db.SegmentRegistrationOptions) error {
	return nil
}
func (m *mockDBClient) RedistributeData(_ context.Context, _ db.RedistributionOptions) error {
	return nil
}
func (m *mockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) {
	return 100, nil
}
func (m *mockDBClient) DeregisterSegments(_ context.Context, _ int32) error {
	return nil
}
func (m *mockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ db.ScaleInRedistributionOptions) error {
	return nil
}
func (m *mockDBClient) AnalyzeSkew(_ context.Context, _ string) ([]db.TableSkewInfo, error) {
	return []db.TableSkewInfo{}, nil
}
func (m *mockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error {
	return nil
}
func (m *mockDBClient) ListSessionsWithResourceGroup(_ context.Context) ([]db.SessionWithGroup, error) {
	return nil, nil
}
func (m *mockDBClient) ListUserDatabases(_ context.Context) ([]string, error) {
	return []string{"postgres", "mydb"}, nil
}
func (m *mockDBClient) SetupExporterRole(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) GetQueryDetail(_ context.Context, pid int32) (*db.QueryDetail, error) {
	return &db.QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}
func (m *mockDBClient) EnsureQueryHistoryTable(_ context.Context) error { return nil }
func (m *mockDBClient) InsertQueryHistory(_ context.Context, _ *db.QueryHistoryEntry) error {
	return nil
}
func (m *mockDBClient) GetQueryHistory(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	return []db.QueryHistoryEntry{}, 0, nil
}
func (m *mockDBClient) GetQueryHistoryDetail(_ context.Context, _ string) (*db.QueryHistoryEntry, error) {
	return nil, fmt.Errorf("not found")
}
func (m *mockDBClient) ExportQueryHistoryCSV(_ context.Context, _ db.QueryHistoryFilter, _ io.Writer) error {
	return nil
}
func (m *mockDBClient) CleanupQueryHistory(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockDBClient) MoveQueryToResourceGroup(_ context.Context, _ int32, _ string) error {
	return nil
}

func TestNewHAReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)
	require.NotNil(t, r)
}

func TestHAReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_ProbeInterval(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

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

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.monitorStandby(context.Background(), cluster)
	require.NoError(t, err)

	cond := findCondition(cluster.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
}

func TestHAReconciler_RunFTSProbe_DBClientError(t *testing.T) {
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

	dbFactory := &mockDBClientFactory{
		err: fmt.Errorf("connection refused"),
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating db client")
}

// TestHAReconciler_RunFTSProbe_RetriesRecordMetricOnce verifies the FTS probe
// metric is recorded EXACTLY ONCE per probe with the terminal result, even when
// the segment-config query fails on every retry attempt. This guards the
// over-counting regression where RecordFTSProbe was invoked inside the retry
// loop (up to FTSProbeRetries times), inflating fts_probe_total{result="failure"}
// and the duration histogram.
func TestHAReconciler_RunFTSProbe_RetriesRecordMetricOnce(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// 5 retries with a short timeout: GetSegmentConfiguration fails on every
	// attempt, so the probe exhausts all retries and terminates with "failure".
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 5, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &mockDBClient{segErr: fmt.Errorf("segment query failed")}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.Error(t, err)

	// Despite 5 failed retry attempts, the metric must be recorded exactly once
	// with the terminal "failure" result.
	require.Len(t, tracker.ftsProbeResults, 1,
		"fts_probe metric must be recorded exactly once per probe, not per retry")
	assert.Equal(t, "failure", tracker.ftsProbeResults[0])
}

// TestHAReconciler_RunFTSProbe_SuccessRecordsMetricOnce verifies the success
// path also records the probe metric exactly once.
func TestHAReconciler_RunFTSProbe_SuccessRecordsMetricOnce(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 5, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.NoError(t, err)

	require.Len(t, tracker.ftsProbeResults, 1,
		"fts_probe metric must be recorded exactly once per probe")
	assert.Equal(t, "success", tracker.ftsProbeResults[0])
}

func TestHAReconciler_MonitorStandby_DBClientError(t *testing.T) {
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

	dbFactory := &mockDBClientFactory{
		err: fmt.Errorf("connection refused"),
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.monitorStandby(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating db client")
}

func TestHAReconciler_MonitorStandby_ReplicationLagError(t *testing.T) {
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
		repLagErr: fmt.Errorf("standby not available"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.monitorStandby(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting replication lag")

	cond := findCondition(cluster.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
}

func TestHAReconciler_Reconcile_RunningWithMirroringAndStandby(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

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
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		},
		replicationLag: 512,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_ProbeInterval_CustomHA(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeInterval: 120}
	interval := r.probeInterval(cluster)
	assert.Equal(t, 120, int(interval.Seconds()))
}

func TestHAReconciler_RunFTSProbe_SegmentConfigError(t *testing.T) {
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
		segErr: fmt.Errorf("query failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting segment configuration")
}

func TestHAReconciler_RunFTSProbe_NilDBFactory(t *testing.T) {
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

	// Create reconciler with nil dbFactory.
	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	err := r.runFTSProbe(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database client factory is not configured")
}

func TestHAReconciler_MonitorStandby_NilDBFactory(t *testing.T) {
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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	err := r.monitorStandby(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database client factory is not configured")
}

func TestHAReconciler_Reconcile_ObservedGenerationSteadyState(t *testing.T) {
	// Regression: ObservedGeneration == Generation (steady state) and no
	// annotations must NOT skip periodic health checks. For a Running cluster
	// the reconcile still runs and requeues at the probe interval. With no
	// mirroring/standby enabled, runHealthChecks is a no-op but is still invoked.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	// Requeue uses the probe interval (default 60s), proving the reconcile did
	// not take the old early-return path (which used requeueAfterDefault).
	assert.Equal(t, r.probeInterval(cluster), result.RequeueAfter)
}

func TestHAReconciler_Reconcile_ObservedGenerationNotSkippedWithRecoveryAnnotation(t *testing.T) {
	// When ObservedGeneration matches but recovery annotation is present, should NOT skip.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify annotation was removed.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	_, exists := updated.Annotations[util.AnnotationRecovery]
	assert.False(t, exists)
}

func TestHAReconciler_Reconcile_ObservedGenerationNotSkippedWithActionAnnotation(t *testing.T) {
	// When ObservedGeneration matches but action annotation is present, should NOT skip.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestHAReconciler_RunHealthChecks_MirroringEnabled(t *testing.T) {
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
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Should not panic.
	r.runHealthChecks(context.Background(), cluster, r.logger)
}

func TestHAReconciler_RunHealthChecks_StandbyEnabled(t *testing.T) {
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
		replicationLag: 100,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Should not panic.
	r.runHealthChecks(context.Background(), cluster, r.logger)
}

func TestHAReconciler_RunHealthChecks_BothDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Neither mirroring nor standby enabled.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Should not panic and should be a no-op.
	r.runHealthChecks(context.Background(), cluster, r.logger)
}

func TestHAReconciler_RunHealthChecks_FTSProbeError(t *testing.T) {
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

	// nil dbFactory will cause FTS probe to fail.
	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Should not panic even when FTS probe fails.
	r.runHealthChecks(context.Background(), cluster, r.logger)
}

func TestHAReconciler_RunHealthChecks_StandbyMonitorError(t *testing.T) {
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

	// nil dbFactory will cause standby monitoring to fail.
	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Should not panic even when standby monitoring fails.
	r.runHealthChecks(context.Background(), cluster, r.logger)
}

func TestHAReconciler_HandleAnnotations_NoAnnotations(t *testing.T) {
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

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, handled, err := r.handleAnnotations(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, handled)
}

func TestHAReconciler_HandleAnnotations_UnknownAction(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationAction: "unknown-action",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, handled, err := r.handleAnnotations(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, handled)
}

func TestHAReconciler_Reconcile_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching cluster")
}

func TestHAReconciler_HandleRecovery_PatchError(t *testing.T) {
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
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, err := r.handleRecovery(context.Background(), cluster, util.RecoveryIncremental)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removing recovery annotation")
}

func TestHAReconciler_HandleRebalance_PatchError(t *testing.T) {
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
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, err := r.handleRebalance(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removing rebalance annotation")
}

func TestHAReconciler_HandleStandbyActivation_PatchError(t *testing.T) {
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
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	_, err := r.handleStandbyActivation(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removing standby activation annotation")
}

// ============================================================================
// Replication Lag Reporting Tests
// ============================================================================

func TestHAReconciler_ReportMirrorReplicationLag_Success(t *testing.T) {
	// Arrange: DB client returns sync status with replication lag.
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

	dbClient := &mockDBClientWithSyncStatus{
		mockDBClient: &mockDBClient{
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p"},
				{ContentID: 0, Status: "u", Role: "m"},
			},
		},
		syncStatus: []db.MirrorSyncInfo{
			{ContentID: 0, IsSynced: true, ReplicationLag: 512, State: "streaming"},
			{ContentID: 1, IsSynced: true, ReplicationLag: 1024, State: "streaming"},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Act: Call reportMirrorReplicationLag directly.
	r.reportMirrorReplicationLag(context.Background(), cluster, dbClient)

	// Assert: Should not panic. Metrics are recorded via NoopRecorder.
}

func TestHAReconciler_ReportMirrorReplicationLag_DBError(t *testing.T) {
	// Arrange: DB client returns error on GetMirrorSyncStatus.
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

	dbClient := &mockDBClientWithSyncStatus{
		mockDBClient: &mockDBClient{},
		syncErr:      fmt.Errorf("query failed"),
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Act: Should not panic even with error.
	r.reportMirrorReplicationLag(context.Background(), cluster, dbClient)
}

func TestHAReconciler_ReportMirrorReplicationLag_EmptyStatus(t *testing.T) {
	// Arrange: DB client returns empty sync status.
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

	dbClient := &mockDBClientWithSyncStatus{
		mockDBClient: &mockDBClient{},
		syncStatus:   []db.MirrorSyncInfo{},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, m, nil)

	// Act: Should not panic with empty status.
	r.reportMirrorReplicationLag(context.Background(), cluster, dbClient)
}

func TestHAReconciler_RunFTSProbe_ReportsReplicationLag(t *testing.T) {
	// Arrange: FTS probe with mirroring enabled should call reportMirrorReplicationLag.
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

	dbClient := &mockDBClientWithSyncStatus{
		mockDBClient: &mockDBClient{
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
				{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
			},
		},
		syncStatus: []db.MirrorSyncInfo{
			{ContentID: 0, IsSynced: true, ReplicationLag: 256, State: "streaming"},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert: Should succeed and report lag.
	require.NoError(t, err)
}

// mockDBClientWithSyncStatus extends mockDBClient with configurable GetMirrorSyncStatus.
type mockDBClientWithSyncStatus struct {
	*mockDBClient
	syncStatus []db.MirrorSyncInfo
	syncErr    error
}

func (m *mockDBClientWithSyncStatus) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return m.syncStatus, m.syncErr
}

// statefulMockDBClient supports call-counting for GetSegmentConfiguration and TriggerFTSProbe.
// This enables testing retry logic and post-failover re-reads where successive calls
// must return different results.
type statefulMockDBClient struct {
	mockDBClient
	segmentCallCount      int
	segmentResults        []segmentCallResult
	triggerFTSCallCount   int
	triggerFTSProbeErrors []error
	syncStatus            []db.MirrorSyncInfo
	syncErr               error
}

type segmentCallResult struct {
	segments []db.SegmentInfo
	err      error
}

func (m *statefulMockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	idx := m.segmentCallCount
	m.segmentCallCount++
	if idx < len(m.segmentResults) {
		return m.segmentResults[idx].segments, m.segmentResults[idx].err
	}
	// Fall back to last result if we exceed the configured results.
	if len(m.segmentResults) > 0 {
		last := m.segmentResults[len(m.segmentResults)-1]
		return last.segments, last.err
	}
	return nil, fmt.Errorf("no segment results configured")
}

func (m *statefulMockDBClient) TriggerFTSProbe(_ context.Context) error {
	idx := m.triggerFTSCallCount
	m.triggerFTSCallCount++
	if idx < len(m.triggerFTSProbeErrors) {
		return m.triggerFTSProbeErrors[idx]
	}
	return nil
}

func (m *statefulMockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return m.syncStatus, m.syncErr
}
func (m *statefulMockDBClient) TerminateAllBackends(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *statefulMockDBClient) CancelAllQueries(_ context.Context) (int32, error) {
	return 0, nil
}
func (m *statefulMockDBClient) LogRotate(_ context.Context) error { return nil }

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// trackingMetricsRecorder wraps NoopRecorder and tracks specific metric calls.
type trackingMetricsRecorder struct {
	metrics.NoopRecorder
	ftsFailoverCount     int
	segmentStatusCalls   []segmentStatusCall
	segmentsFailedCalls  []float64
	ftsProbeResults      []string
	mirroringInSyncCalls []bool
}

type segmentStatusCall struct {
	segment string
	up      bool
}

func (t *trackingMetricsRecorder) RecordFTSFailover(_, _ string) {
	t.ftsFailoverCount++
}

func (t *trackingMetricsRecorder) SetSegmentStatus(_, _, segment string, up bool) {
	t.segmentStatusCalls = append(t.segmentStatusCalls, segmentStatusCall{segment: segment, up: up})
}

func (t *trackingMetricsRecorder) SetSegmentsFailed(_, _ string, count float64) {
	t.segmentsFailedCalls = append(t.segmentsFailedCalls, count)
}

func (t *trackingMetricsRecorder) RecordFTSProbe(_, _, result string, _ time.Duration) {
	t.ftsProbeResults = append(t.ftsProbeResults, result)
}

func (t *trackingMetricsRecorder) SetMirroringInSync(_, _ string, inSync bool) {
	t.mirroringInSyncCalls = append(t.mirroringInSyncCalls, inSync)
}

// ============================================================================
// Detection Phase Tests: probeTimeout, probeRetries
// ============================================================================

func TestHAReconciler_ProbeTimeout_Default(t *testing.T) {
	// Arrange: cluster with no HA spec.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)
	cluster := newTestCluster()

	// Act
	timeout := r.probeTimeout(cluster)

	// Assert: default is 20 seconds.
	assert.Equal(t, 20, int(timeout.Seconds()))
}

func TestHAReconciler_ProbeTimeout_Custom(t *testing.T) {
	// Arrange: cluster with custom timeout.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)
	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeTimeout: 10}

	// Act
	timeout := r.probeTimeout(cluster)

	// Assert
	assert.Equal(t, 10, int(timeout.Seconds()))
}

func TestHAReconciler_ProbeRetries_Default(t *testing.T) {
	// Arrange: cluster with no HA spec.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)
	cluster := newTestCluster()

	// Act
	retries := r.probeRetries(cluster)

	// Assert: default is 5.
	assert.Equal(t, 5, retries)
}

func TestHAReconciler_ProbeRetries_Custom(t *testing.T) {
	// Arrange: cluster with custom retries.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)
	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 3}

	// Act
	retries := r.probeRetries(cluster)

	// Assert
	assert.Equal(t, 3, retries)
}

// ============================================================================
// Detection Phase Tests: probeSegmentConfigWithRetries
// ============================================================================

func TestHAReconciler_ProbeSegmentConfigWithRetries_SucceedsFirstAttempt(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 3, FTSProbeTimeout: 5}

	expectedSegments := []db.SegmentInfo{
		{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
	}
	dbClient := &mockDBClient{segments: expectedSegments}
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act
	segments, err := r.probeSegmentConfigWithRetries(ctx, cluster, dbClient)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, expectedSegments, segments)
	// The retry helper never records the FTS probe metric (the caller records the
	// terminal outcome exactly once), so nothing is recorded here.
	assert.Empty(t, tracker.ftsProbeResults)
}

func TestHAReconciler_ProbeSegmentConfigWithRetries_SucceedsAfterRetry(t *testing.T) {
	// Arrange: first call fails, second succeeds.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 3, FTSProbeTimeout: 5}

	expectedSegments := []db.SegmentInfo{
		{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
	}
	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{err: fmt.Errorf("connection timeout")},
			{segments: expectedSegments},
		},
	}
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act
	segments, err := r.probeSegmentConfigWithRetries(ctx, cluster, dbClient)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, expectedSegments, segments)
	assert.Equal(t, 2, dbClient.segmentCallCount)
	// The retry helper no longer records a per-attempt failure metric; the caller
	// (runFTSProbe) records the single terminal outcome instead.
	assert.Empty(t, tracker.ftsProbeResults)
}

func TestHAReconciler_ProbeSegmentConfigWithRetries_AllRetriesExhausted(t *testing.T) {
	// Arrange: all attempts fail.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 3, FTSProbeTimeout: 5}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{err: fmt.Errorf("fail 1")},
			{err: fmt.Errorf("fail 2")},
			{err: fmt.Errorf("fail 3")},
		},
	}
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act
	segments, err := r.probeSegmentConfigWithRetries(ctx, cluster, dbClient)

	// Assert
	require.Error(t, err)
	assert.Nil(t, segments)
	assert.Contains(t, err.Error(), "getting segment configuration after 3 retries")
	assert.Contains(t, err.Error(), "fail 3")
	assert.Equal(t, 3, dbClient.segmentCallCount)
	// The retry helper records NO per-attempt metrics (previously it recorded one
	// per attempt, over-counting). The terminal failure is recorded once by the
	// caller (covered by TestHAReconciler_RunFTSProbe_RetriesRecordMetricOnce).
	assert.Empty(t, tracker.ftsProbeResults)
}

func TestHAReconciler_ProbeSegmentConfigWithRetries_ContextCancelled(t *testing.T) {
	// Arrange: context is cancelled before probe.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 3, FTSProbeTimeout: 5}

	dbClient := &mockDBClient{
		segErr: context.Canceled,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.
	ctx = util.WithLogger(ctx, r.logger)

	// Act
	segments, err := r.probeSegmentConfigWithRetries(ctx, cluster, dbClient)

	// Assert: should fail (context cancelled propagates through timeout context).
	require.Error(t, err)
	assert.Nil(t, segments)
}

// ============================================================================
// Segment Analysis Tests: analyzeSegments
// ============================================================================

func TestHAReconciler_AnalyzeSegments_AllHealthy(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	segments := []db.SegmentInfo{
		{ContentID: -1, Status: "u", Role: "p"}, // coordinator
		{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
		{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		{ContentID: 1, Status: "u", Role: "p", Hostname: "host1"},
		{ContentID: 1, Status: "u", Role: "m", Hostname: "host2"},
	}

	// Act
	result := r.analyzeSegments(cluster, segments)

	// Assert
	assert.True(t, result.allHealthy)
	assert.Empty(t, result.failedSegments)
	assert.Empty(t, result.failedPrimaries)
	// Segment status metrics set for non-coordinator segments (4 segments).
	assert.Len(t, tracker.segmentStatusCalls, 4)
	for _, call := range tracker.segmentStatusCalls {
		assert.True(t, call.up)
	}
}

func TestHAReconciler_AnalyzeSegments_PrimaryDown(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	segments := []db.SegmentInfo{
		{ContentID: 0, Status: "d", Role: "p", Hostname: "host1", DBID: 2},
		{ContentID: 0, Status: "u", Role: "m", Hostname: "host2", DBID: 5},
		{ContentID: 1, Status: "u", Role: "p", Hostname: "host1", DBID: 3},
		{ContentID: 1, Status: "u", Role: "m", Hostname: "host2", DBID: 6},
	}

	// Act
	result := r.analyzeSegments(cluster, segments)

	// Assert
	assert.False(t, result.allHealthy)
	assert.Len(t, result.failedSegments, 1)
	assert.Equal(t, int32(0), result.failedSegments[0].ContentID)
	assert.Equal(t, "p", result.failedSegments[0].Role)
	assert.Len(t, result.failedPrimaries, 1)
	assert.Equal(t, int32(0), result.failedPrimaries[0].ContentID)
}

func TestHAReconciler_AnalyzeSegments_MirrorDown(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	segments := []db.SegmentInfo{
		{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
		{ContentID: 0, Status: "d", Role: "m", Hostname: "host2"},
	}

	// Act
	result := r.analyzeSegments(cluster, segments)

	// Assert
	assert.False(t, result.allHealthy)
	assert.Len(t, result.failedSegments, 1)
	assert.Equal(t, "m", result.failedSegments[0].Role)
	// Mirror down should NOT be in failedPrimaries.
	assert.Empty(t, result.failedPrimaries)
}

func TestHAReconciler_AnalyzeSegments_MultipleFailed(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	segments := []db.SegmentInfo{
		{ContentID: 0, Status: "d", Role: "p", Hostname: "host1", DBID: 2},
		{ContentID: 0, Status: "u", Role: "m", Hostname: "host2", DBID: 5},
		{ContentID: 1, Status: "d", Role: "p", Hostname: "host1", DBID: 3},
		{ContentID: 1, Status: "d", Role: "m", Hostname: "host2", DBID: 6},
	}

	// Act
	result := r.analyzeSegments(cluster, segments)

	// Assert
	assert.False(t, result.allHealthy)
	assert.Len(t, result.failedSegments, 3)
	assert.Len(t, result.failedPrimaries, 2)
}

func TestHAReconciler_AnalyzeSegments_SkipsCoordinator(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	segments := []db.SegmentInfo{
		{ContentID: -1, Status: "d", Role: "p", Hostname: "coordinator"}, // coordinator down
		{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
	}

	// Act
	result := r.analyzeSegments(cluster, segments)

	// Assert: coordinator (contentID=-1) should be skipped.
	assert.True(t, result.allHealthy)
	assert.Empty(t, result.failedSegments)
	assert.Empty(t, result.failedPrimaries)
	// Only one segment status call (for contentID=0).
	assert.Len(t, tracker.segmentStatusCalls, 1)
}

// ============================================================================
// Failover Phase Tests: handleFailover
// ============================================================================

func TestHAReconciler_HandleFailover_PrimaryDown_MirrorPromoted(t *testing.T) {
	// Arrange: primary is down, after TriggerFTSProbe the mirror is promoted.
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
	tracker := &trackingMetricsRecorder{}

	// First call: initial probe (already done before handleFailover).
	// handleFailover calls TriggerFTSProbe then GetSegmentConfiguration.
	// After failover: mirror (DBID=5) is now primary for contentID=0.
	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"}, // mirror promoted
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"}, // old primary now mirror/down
				{ContentID: 1, DBID: 3, Role: "p", Status: "u", Hostname: "host1"},
				{ContentID: 1, DBID: 6, Role: "m", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, dbClient.triggerFTSCallCount)
	assert.Equal(t, 1, dbClient.segmentCallCount)
	assert.Equal(t, 1, tracker.ftsFailoverCount)

	// Verify event was emitted.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, cbv1alpha1.EventReasonSegmentFailover)
		assert.Contains(t, event, "Segment failover completed")
		assert.Contains(t, event, "host2") // new primary
	default:
		t.Fatal("expected SegmentFailover event")
	}
}

func TestHAReconciler_HandleFailover_MultiplePrimariesDown(t *testing.T) {
	// Arrange: two primaries are down.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"}, // mirror promoted
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"},
				{ContentID: 1, DBID: 6, Role: "p", Status: "u", Hostname: "host2"}, // mirror promoted
				{ContentID: 1, DBID: 3, Role: "m", Status: "d", Hostname: "host1"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
		{ContentID: 1, DBID: 3, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.ftsFailoverCount) // One failover event, not per-segment.

	// Two events should be emitted (one per failed primary).
	eventCount := 0
	for {
		select {
		case event := <-recorder.Events:
			assert.Contains(t, event, cbv1alpha1.EventReasonSegmentFailover)
			eventCount++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 2, eventCount)

	// Segment status set to false for both failed primaries.
	downCalls := 0
	for _, call := range tracker.segmentStatusCalls {
		if !call.up {
			downCalls++
		}
	}
	assert.Equal(t, 2, downCalls)
}

func TestHAReconciler_HandleFailover_TriggerFTSProbeError(t *testing.T) {
	// Arrange: TriggerFTSProbe fails but handleFailover continues.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		triggerFTSProbeErrors: []error{fmt.Errorf("FTS probe scan failed")},
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				// After failed trigger, mirror not promoted.
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert: should still succeed (trigger error is logged, not returned).
	require.NoError(t, err)
	assert.Equal(t, 1, dbClient.triggerFTSCallCount)
	assert.Equal(t, 1, tracker.ftsFailoverCount)

	// Event should still be emitted.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, cbv1alpha1.EventReasonSegmentFailover)
	default:
		t.Fatal("expected SegmentFailover event even when trigger fails")
	}
}

func TestHAReconciler_HandleFailover_ReReadError(t *testing.T) {
	// Arrange: TriggerFTSProbe succeeds but re-read fails.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{err: fmt.Errorf("connection lost during re-read")},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert: should return error.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "re-reading segment configuration after failover")

	// Events should still be emitted for originally detected failures.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, cbv1alpha1.EventReasonSegmentFailover)
		assert.Contains(t, event, "Primary segment failover detected")
	default:
		t.Fatal("expected SegmentFailover event even on re-read error")
	}

	// Failover metric should still be recorded.
	assert.Equal(t, 1, tracker.ftsFailoverCount)
}

func TestHAReconciler_HandleFailover_AllHealthy_NoOp(t *testing.T) {
	// Arrange: no failed primaries passed to handleFailover.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "u", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	// Act: empty failedPrimaries.
	err := r.handleFailover(ctx, cluster, dbClient, []db.SegmentInfo{})

	// Assert: still calls TriggerFTSProbe and re-reads (function doesn't short-circuit).
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.ftsFailoverCount)
}

func TestHAReconciler_HandleFailover_MirrorNotPromoted(t *testing.T) {
	// Arrange: after trigger, mirror is still mirror (not promoted).
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				// Primary still down, mirror not promoted.
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)

	// Event should indicate mirror promotion pending.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "mirror promotion pending")
	default:
		t.Fatal("expected event about mirror promotion pending")
	}
}

// ============================================================================
// Integration Tests: runFTSProbe -> handleFailover flow
// ============================================================================

func TestHAReconciler_RunFTSProbe_TriggersFailover_WhenPrimaryDown(t *testing.T) {
	// Arrange: full flow — probe detects primary down, triggers failover.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 1, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	// Call 1 (probeSegmentConfigWithRetries): primary down.
	// Call 2 (handleFailover re-read): mirror promoted.
	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"}, // promoted
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"},
			}},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.ftsFailoverCount)
	assert.Equal(t, cbv1alpha1.MirroringDegraded, cluster.Status.MirroringStatus)
	assert.NotEmpty(t, cluster.Status.FailedSegments)
}

func TestHAReconciler_RunFTSProbe_NoFailover_WhenMirroringDisabled(t *testing.T) {
	// Arrange: primary down but mirroring is disabled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Mirroring NOT enabled.
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 1, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
			{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
		},
	}

	// Note: runFTSProbe is only called when mirroring is enabled (from runHealthChecks).
	// But we test the internal logic: even if called directly, mirroring check in runFTSProbe
	// prevents handleFailover from being called.
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert: no failover triggered.
	require.NoError(t, err)
	assert.Equal(t, 0, tracker.ftsFailoverCount)
}

func TestHAReconciler_RunFTSProbe_NoFailover_WhenAllHealthy(t *testing.T) {
	// Arrange: all segments healthy.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 1, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, DBID: 2, Role: "p", Status: "u", Hostname: "host1"},
			{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 0, tracker.ftsFailoverCount)
	assert.Equal(t, cbv1alpha1.MirroringInSync, cluster.Status.MirroringStatus)
}

func TestHAReconciler_RunFTSProbe_WithRetries_ThenFailover(t *testing.T) {
	// Arrange: first probe attempt fails, second succeeds with primary down, triggers failover.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 2, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	// Call 1 (retry 1): fails.
	// Call 2 (retry 2): succeeds with primary down.
	// Call 3 (handleFailover re-read): mirror promoted.
	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{err: fmt.Errorf("connection timeout")},
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"},
			}},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.ftsFailoverCount)
	assert.Equal(t, 3, dbClient.segmentCallCount) // 2 retries + 1 re-read
}

// ============================================================================
// Metrics and Events Tests
// ============================================================================

func TestHAReconciler_HandleFailover_EmitsSegmentFailoverEvent(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)

	// Verify event content.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "SegmentFailover")
		assert.Contains(t, event, "contentID=0")
	default:
		t.Fatal("expected SegmentFailover event")
	}
}

func TestHAReconciler_HandleFailover_RecordsFTSFailoverMetric(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.ftsFailoverCount)
}

func TestHAReconciler_HandleFailover_UpdatesSegmentStatusMetric(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
				{ContentID: 1, DBID: 6, Role: "p", Status: "u", Hostname: "host2"},
			}},
		},
	}

	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
	ctx := util.WithLogger(context.Background(), r.logger)

	failedPrimaries := []db.SegmentInfo{
		{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
		{ContentID: 1, DBID: 3, Role: "p", Status: "d", Hostname: "host1"},
	}

	// Act
	err := r.handleFailover(ctx, cluster, dbClient, failedPrimaries)

	// Assert
	require.NoError(t, err)
	// SetSegmentStatus should be called with up=false for each failed primary.
	assert.Len(t, tracker.segmentStatusCalls, 2)
	for _, call := range tracker.segmentStatusCalls {
		assert.False(t, call.up)
	}
}

func TestHAReconciler_HandleFailover_UpdatesFailedSegmentsStatus(t *testing.T) {
	// Arrange: full flow through runFTSProbe to verify status update.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.HA = &cbv1alpha1.HASpec{FTSProbeRetries: 1, FTSProbeTimeout: 5}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}

	dbClient := &statefulMockDBClient{
		segmentResults: []segmentCallResult{
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
				{ContentID: 0, DBID: 5, Role: "m", Status: "u", Hostname: "host2"},
			}},
			{segments: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
				{ContentID: 0, DBID: 2, Role: "m", Status: "d", Hostname: "host1"},
			}},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, tracker, nil)

	// Act
	err := r.runFTSProbe(context.Background(), cluster)

	// Assert
	require.NoError(t, err)

	// Verify cluster status was updated.
	assert.Equal(t, cbv1alpha1.MirroringDegraded, cluster.Status.MirroringStatus)
	assert.NotEmpty(t, cluster.Status.FailedSegments)

	// Verify SetSegmentsFailed was called.
	assert.NotEmpty(t, tracker.segmentsFailedCalls)
	assert.Equal(t, float64(1), tracker.segmentsFailedCalls[0])

	// Verify SetMirroringInSync was called with false.
	assert.NotEmpty(t, tracker.mirroringInSyncCalls)
	assert.False(t, tracker.mirroringInSyncCalls[0])
}

// ============================================================================
// updateFTSProbeStatus Tests
// ============================================================================

func TestHAReconciler_UpdateFTSProbeStatus_AllHealthy(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)

	cluster := newTestCluster()
	analysis := segmentAnalysisResult{
		allHealthy:     true,
		failedSegments: nil,
	}

	// Act
	r.updateFTSProbeStatus(cluster, analysis)

	// Assert
	assert.Equal(t, cbv1alpha1.MirroringInSync, cluster.Status.MirroringStatus)
	assert.Empty(t, cluster.Status.FailedSegments)
	assert.NotEmpty(t, tracker.mirroringInSyncCalls)
	assert.True(t, tracker.mirroringInSyncCalls[0])
}

func TestHAReconciler_UpdateFTSProbeStatus_Degraded(t *testing.T) {
	// Arrange
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	tracker := &trackingMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)

	cluster := newTestCluster()
	failedSegs := []cbv1alpha1.FailedSegment{
		{ContentID: 0, Hostname: "host1", Role: "p", Status: "d"},
	}
	analysis := segmentAnalysisResult{
		allHealthy:     false,
		failedSegments: failedSegs,
	}

	// Act
	r.updateFTSProbeStatus(cluster, analysis)

	// Assert
	assert.Equal(t, cbv1alpha1.MirroringDegraded, cluster.Status.MirroringStatus)
	assert.Equal(t, failedSegs, cluster.Status.FailedSegments)
	assert.NotEmpty(t, tracker.mirroringInSyncCalls)
	assert.False(t, tracker.mirroringInSyncCalls[0])
	assert.NotEmpty(t, tracker.segmentsFailedCalls)
	assert.Equal(t, float64(1), tracker.segmentsFailedCalls[0])

	// Verify MirroringDegraded event.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "MirroringDegraded")
		assert.Contains(t, event, "1 segments are down")
	default:
		t.Fatal("expected MirroringDegraded event")
	}
}

// ============================================================================
// Table-Driven Tests for probeTimeout and probeRetries edge cases
// ============================================================================

func TestHAReconciler_ProbeTimeout_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		haSpec   *cbv1alpha1.HASpec
		expected int
	}{
		{
			name:     "nil HA spec uses default",
			haSpec:   nil,
			expected: 20,
		},
		{
			name:     "zero timeout uses default",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeTimeout: 0},
			expected: 20,
		},
		{
			name:     "custom timeout",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeTimeout: 10},
			expected: 10,
		},
		{
			name:     "large timeout",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeTimeout: 120},
			expected: 120,
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.HA = tt.haSpec
			timeout := r.probeTimeout(cluster)
			assert.Equal(t, tt.expected, int(timeout.Seconds()))
		})
	}
}

func TestHAReconciler_ProbeRetries_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		haSpec   *cbv1alpha1.HASpec
		expected int
	}{
		{
			name:     "nil HA spec uses default",
			haSpec:   nil,
			expected: 5,
		},
		{
			name:     "zero retries uses default",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeRetries: 0},
			expected: 5,
		},
		{
			name:     "custom retries",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeRetries: 3},
			expected: 3,
		},
		{
			name:     "single retry",
			haSpec:   &cbv1alpha1.HASpec{FTSProbeRetries: 1},
			expected: 1,
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, &metrics.NoopRecorder{}, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.HA = tt.haSpec
			retries := r.probeRetries(cluster)
			assert.Equal(t, tt.expected, retries)
		})
	}
}

// ============================================================================
// Table-Driven Tests for analyzeSegments
// ============================================================================

func TestHAReconciler_AnalyzeSegments_TableDriven(t *testing.T) {
	tests := []struct {
		name                string
		segments            []db.SegmentInfo
		expectHealthy       bool
		expectFailedCount   int
		expectPrimaryFailed int
	}{
		{
			name:                "empty segments",
			segments:            []db.SegmentInfo{},
			expectHealthy:       true,
			expectFailedCount:   0,
			expectPrimaryFailed: 0,
		},
		{
			name: "only coordinator",
			segments: []db.SegmentInfo{
				{ContentID: -1, Status: "u", Role: "p"},
			},
			expectHealthy:       true,
			expectFailedCount:   0,
			expectPrimaryFailed: 0,
		},
		{
			name: "single healthy primary",
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
			},
			expectHealthy:       true,
			expectFailedCount:   0,
			expectPrimaryFailed: 0,
		},
		{
			name: "single down primary",
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "d", Role: "p", Hostname: "host1"},
			},
			expectHealthy:       false,
			expectFailedCount:   1,
			expectPrimaryFailed: 1,
		},
		{
			name: "single down mirror",
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
				{ContentID: 0, Status: "d", Role: "m", Hostname: "host2"},
			},
			expectHealthy:       false,
			expectFailedCount:   1,
			expectPrimaryFailed: 0,
		},
		{
			name: "mixed healthy and failed",
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
				{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
				{ContentID: 1, Status: "d", Role: "p", Hostname: "host1"},
				{ContentID: 1, Status: "u", Role: "m", Hostname: "host2"},
			},
			expectHealthy:       false,
			expectFailedCount:   1,
			expectPrimaryFailed: 1,
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &trackingMetricsRecorder{}
			r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(10), nil, nil, tracker, nil)
			cluster := newTestCluster()

			result := r.analyzeSegments(cluster, tt.segments)

			assert.Equal(t, tt.expectHealthy, result.allHealthy)
			assert.Len(t, result.failedSegments, tt.expectFailedCount)
			assert.Len(t, result.failedPrimaries, tt.expectPrimaryFailed)
		})
	}
}

// ============================================================================
// Table-Driven Tests for handleFailover scenarios
// ============================================================================

func TestHAReconciler_HandleFailover_TableDriven(t *testing.T) {
	tests := []struct {
		name                string
		failedPrimaries     []db.SegmentInfo
		postFailoverSegs    []db.SegmentInfo
		postFailoverErr     error
		triggerErr          error
		expectError         bool
		expectErrorContains string
		expectFailoverCount int
		expectEventCount    int
	}{
		{
			name: "single primary promoted",
			failedPrimaries: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
			},
			postFailoverSegs: []db.SegmentInfo{
				{ContentID: 0, DBID: 5, Role: "p", Status: "u", Hostname: "host2"},
			},
			expectError:         false,
			expectFailoverCount: 1,
			expectEventCount:    1,
		},
		{
			name: "trigger error continues",
			failedPrimaries: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
			},
			postFailoverSegs: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
			},
			triggerErr:          fmt.Errorf("trigger failed"),
			expectError:         false,
			expectFailoverCount: 1,
			expectEventCount:    1,
		},
		{
			name: "re-read error returns error",
			failedPrimaries: []db.SegmentInfo{
				{ContentID: 0, DBID: 2, Role: "p", Status: "d", Hostname: "host1"},
			},
			postFailoverErr:     fmt.Errorf("re-read failed"),
			expectError:         true,
			expectErrorContains: "re-reading segment configuration",
			expectFailoverCount: 1,
			expectEventCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()
			recorder := record.NewFakeRecorder(10)
			tracker := &trackingMetricsRecorder{}

			var triggerErrors []error
			if tt.triggerErr != nil {
				triggerErrors = []error{tt.triggerErr}
			}

			dbClient := &statefulMockDBClient{
				triggerFTSProbeErrors: triggerErrors,
				segmentResults: []segmentCallResult{
					{segments: tt.postFailoverSegs, err: tt.postFailoverErr},
				},
			}

			r := NewHAReconciler(k8sClient, scheme, recorder, nil, nil, tracker, nil)
			ctx := util.WithLogger(context.Background(), r.logger)

			err := r.handleFailover(ctx, cluster, dbClient, tt.failedPrimaries)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectErrorContains != "" {
					assert.Contains(t, err.Error(), tt.expectErrorContains)
				}
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectFailoverCount, tracker.ftsFailoverCount)

			eventCount := 0
			for {
				select {
				case <-recorder.Events:
					eventCount++
				default:
					goto checkEvents
				}
			}
		checkEvents:
			assert.Equal(t, tt.expectEventCount, eventCount)
		})
	}
}
