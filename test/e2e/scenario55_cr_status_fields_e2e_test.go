//go:build e2e

package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 55: CR Status Fields Updated from Database (E2E)
// ============================================================================
//
// This scenario verifies that the CR status fields (activeQueries,
// queuedQueries, blockedQueries) are correctly updated from the database
// during reconciliation. The reconcileQueryMonitoring method calls
// GetActiveQueryCount() via the DB client and writes results to
// cluster.Status, which is then patched to the API server.
//
// ============================================================================

// Scenario55CRStatusFieldsE2ESuite tests that CR status fields are updated
// from the database during reconciliation.
type Scenario55CRStatusFieldsE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario55(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario55CRStatusFieldsE2ESuite))
}

func (s *Scenario55CRStatusFieldsE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario55CRStatusFieldsE2ESuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario55E2EMockDBFactory creates a MockDBClientFactory that returns specific
// query counts when GetActiveQueryCount is called.
func scenario55E2EMockDBFactory(active, queued, blocked int32) *testutil.MockDBClientFactory {
	return &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetActiveQueryCountFunc: func(ctx context.Context) (int32, int32, int32, error) {
				return active, queued, blocked, nil
			},
		},
	}
}

// scenario55E2ECluster returns a cluster with QueryMonitoring enabled and
// a pending generation to trigger reconciliation.
func scenario55E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		HistoryRetention: "30d",
		SamplingInterval: 5,
	}
	return cluster
}

// --- 55.1: ActiveQueries status field is updated ---

// TestE2E_Scenario55_ActiveQueries_Updated verifies that after
// reconciliation with a mock DB returning active=10, the cluster status
// field ActiveQueries is set to 10.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_ActiveQueries_Updated() {
	// Arrange
	cluster := scenario55E2ECluster("test-s55e2e-active")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55E2EMockDBFactory(10, 0, 0)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), mockFactory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster")
	assert.Equal(s.T(), int32(10), updated.Status.ActiveQueries,
		"ActiveQueries should be updated to 10")
}

// --- 55.2: QueuedQueries status field is updated ---

// TestE2E_Scenario55_QueuedQueries_Updated verifies that after
// reconciliation with a mock DB returning queued=3, the cluster status
// field QueuedQueries is set to 3.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_QueuedQueries_Updated() {
	// Arrange
	cluster := scenario55E2ECluster("test-s55e2e-queued")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55E2EMockDBFactory(5, 3, 0)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), mockFactory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster")
	assert.Equal(s.T(), int32(3), updated.Status.QueuedQueries,
		"QueuedQueries should be updated to 3")
}

// --- 55.3: BlockedQueries status field is updated ---

// TestE2E_Scenario55_BlockedQueries_Updated verifies that after
// reconciliation with a mock DB returning blocked=2, the cluster status
// field BlockedQueries is set to 2.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_BlockedQueries_Updated() {
	// Arrange
	cluster := scenario55E2ECluster("test-s55e2e-blocked")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55E2EMockDBFactory(5, 0, 2)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), mockFactory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster")
	assert.Equal(s.T(), int32(2), updated.Status.BlockedQueries,
		"BlockedQueries should be updated to 2")
}

// --- 55.4: All query count status fields are updated together ---

// TestE2E_Scenario55_AllQueryCounts_Updated verifies that after
// reconciliation with a mock DB returning active=10, queued=3, blocked=1,
// all three status fields are correctly set.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_AllQueryCounts_Updated() {
	// Arrange
	cluster := scenario55E2ECluster("test-s55e2e-all")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55E2EMockDBFactory(10, 3, 1)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), mockFactory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster")
	assert.Equal(s.T(), int32(10), updated.Status.ActiveQueries,
		"ActiveQueries should be updated to 10")
	assert.Equal(s.T(), int32(3), updated.Status.QueuedQueries,
		"QueuedQueries should be updated to 3")
	assert.Equal(s.T(), int32(1), updated.Status.BlockedQueries,
		"BlockedQueries should be updated to 1")
}

// --- 55.5: Prometheus metrics reflect status values ---

// TestE2E_Scenario55_MetricsReflectStatus verifies that after
// reconciliation, the Prometheus metrics recorder receives the correct
// query count values matching the DB response.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_MetricsReflectStatus() {
	// Arrange
	cluster := scenario55E2ECluster("test-s55e2e-metrics")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55E2EMockDBFactory(7, 2, 1)
	metricsRecorder := &scenario55E2EMetricsRecorder{}

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), mockFactory, metricsRecorder, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	calls := metricsRecorder.getCalls()

	// Verify SetActiveQueries was called with the correct value.
	activeCalls := scenario55E2EFilterCalls(calls, "SetActiveQueries")
	require.NotEmpty(s.T(), activeCalls,
		"SetActiveQueries should have been called")
	assert.Equal(s.T(), float64(7), activeCalls[len(activeCalls)-1].args["count"],
		"SetActiveQueries should be called with count=7")

	// Verify SetQueuedQueries was called with the correct value.
	queuedCalls := scenario55E2EFilterCalls(calls, "SetQueuedQueries")
	require.NotEmpty(s.T(), queuedCalls,
		"SetQueuedQueries should have been called")
	assert.Equal(s.T(), float64(2), queuedCalls[len(queuedCalls)-1].args["count"],
		"SetQueuedQueries should be called with count=2")

	// Verify SetBlockedQueries was called with the correct value.
	blockedCalls := scenario55E2EFilterCalls(calls, "SetBlockedQueries")
	require.NotEmpty(s.T(), blockedCalls,
		"SetBlockedQueries should have been called")
	assert.Equal(s.T(), float64(1), blockedCalls[len(blockedCalls)-1].args["count"],
		"SetBlockedQueries should be called with count=1")
}

// --- 55.6: Status updates on each reconcile ---

// TestE2E_Scenario55_StatusUpdatesOnEachReconcile verifies that
// when the DB returns different values on subsequent reconciliations,
// the status fields are updated accordingly.
func (s *Scenario55CRStatusFieldsE2ESuite) TestE2E_Scenario55_StatusUpdatesOnEachReconcile() {
	// Arrange: first reconcile with active=5
	cluster := scenario55E2ECluster("test-s55e2e-update")
	env := testutil.NewTestK8sEnv(cluster)

	var mu sync.Mutex
	currentActive := int32(5)
	dynamicFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetActiveQueryCountFunc: func(ctx context.Context) (int32, int32, int32, error) {
				mu.Lock()
				defer mu.Unlock()
				return currentActive, 0, 0, nil
			},
		},
	}

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), dynamicFactory, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act: first reconcile
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "first reconcile should succeed")

	// Assert: first reconcile result
	updated, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster after first reconcile")
	assert.Equal(s.T(), int32(5), updated.Status.ActiveQueries,
		"ActiveQueries should be 5 after first reconcile")

	// Arrange: change mock to return active=15 and bump generation
	mu.Lock()
	currentActive = 15
	mu.Unlock()

	// Bump generation to trigger a new reconciliation cycle.
	updated.Generation = 2
	updated.Status.ObservedGeneration = 1
	err = env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err, "updating cluster generation should succeed")

	// Act: second reconcile
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "second reconcile should succeed")

	// Assert: second reconcile result
	updated2, getErr := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), getErr, "should be able to re-read cluster after second reconcile")
	assert.Equal(s.T(), int32(15), updated2.Status.ActiveQueries,
		"ActiveQueries should be updated to 15 after second reconcile")
}

// ============================================================================
// Scenario 55 E2E: Capturing Metrics Recorder
// ============================================================================

// scenario55E2EMetricsCall records a single metrics method invocation.
type scenario55E2EMetricsCall struct {
	method string
	args   map[string]interface{}
}

// scenario55E2EMetricsRecorder captures metrics calls for verification.
type scenario55E2EMetricsRecorder struct {
	mu    sync.Mutex
	calls []scenario55E2EMetricsCall
}

func (m *scenario55E2EMetricsRecorder) record(method string, args map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, scenario55E2EMetricsCall{method: method, args: args})
}

func (m *scenario55E2EMetricsRecorder) getCalls() []scenario55E2EMetricsCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]scenario55E2EMetricsCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// scenario55E2EFilterCalls returns all calls matching the given method name.
func scenario55E2EFilterCalls(calls []scenario55E2EMetricsCall, method string) []scenario55E2EMetricsCall {
	var result []scenario55E2EMetricsCall
	for _, c := range calls {
		if c.method == method {
			result = append(result, c)
		}
	}
	return result
}

// Implement the metrics.Recorder interface.
var _ metrics.Recorder = (*scenario55E2EMetricsRecorder)(nil)

func (m *scenario55E2EMetricsRecorder) RecordReconcile(_, _, _ string, _ time.Duration) {}
func (m *scenario55E2EMetricsRecorder) UpdateClusterInfo(_, _, _, _ string, _ float64)  {}
func (m *scenario55E2EMetricsRecorder) SetCoordinatorUp(_, _ string, _ bool)            {}
func (m *scenario55E2EMetricsRecorder) SetStandbyUp(_, _ string, _ bool)                {}
func (m *scenario55E2EMetricsRecorder) SetSegmentsReady(_, _ string, _ float64)         {}
func (m *scenario55E2EMetricsRecorder) SetSegmentsTotal(_, _ string, _ float64)         {}
func (m *scenario55E2EMetricsRecorder) SetSegmentsFailed(_, _ string, _ float64)        {}
func (m *scenario55E2EMetricsRecorder) SetMirroringInSync(_, _ string, _ bool)          {}
func (m *scenario55E2EMetricsRecorder) RecordFTSProbe(_, _, _ string, _ time.Duration)  {}
func (m *scenario55E2EMetricsRecorder) RecordFTSFailover(_, _ string)                   {}
func (m *scenario55E2EMetricsRecorder) SetSegmentStatus(_, _, _ string, _ bool)         {}
func (m *scenario55E2EMetricsRecorder) SetReplicationLag(_, _, _ string, _ float64)     {}
func (m *scenario55E2EMetricsRecorder) SetStandbyReplicationLag(_, _ string, _ float64) {}
func (m *scenario55E2EMetricsRecorder) RecordConfigReload(_, _ string)                  {}
func (m *scenario55E2EMetricsRecorder) SetConnectionsActive(_, _ string, _ float64)     {}
func (m *scenario55E2EMetricsRecorder) SetConnectionsMax(_, _ string, _ float64)        {}
func (m *scenario55E2EMetricsRecorder) SetDiskUsageBytes(_, _, _ string, _ float64)     {}
func (m *scenario55E2EMetricsRecorder) RecordAuthAttempt(_, _ string)                   {}

func (m *scenario55E2EMetricsRecorder) SetActiveQueries(cluster, namespace string, count float64) {
	m.record("SetActiveQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55E2EMetricsRecorder) SetQueuedQueries(cluster, namespace string, count float64) {
	m.record("SetQueuedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55E2EMetricsRecorder) SetBlockedQueries(cluster, namespace string, count float64) {
	m.record("SetBlockedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55E2EMetricsRecorder) RecordWorkloadRuleAction(_, _, _, _ string)            {}
func (m *scenario55E2EMetricsRecorder) SetResourceGroupUsage(_, _, _ string, _, _ float64)    {}
func (m *scenario55E2EMetricsRecorder) RecordIdleSessionTermination(_, _, _ string)           {}
func (m *scenario55E2EMetricsRecorder) RecordSlowQuery(_, _ string)                           {}
func (m *scenario55E2EMetricsRecorder) RecordBackup(_, _, _, _ string)                        {}
func (m *scenario55E2EMetricsRecorder) ObserveBackupDuration(_, _, _ string, _ time.Duration) {}
func (m *scenario55E2EMetricsRecorder) SetBackupSizeBytes(_, _, _ string, _ float64)          {}
func (m *scenario55E2EMetricsRecorder) SetBackupLastSuccessTimestamp(_, _ string, _ float64)  {}
func (m *scenario55E2EMetricsRecorder) SetBackupLastStatus(_, _ string, _ float64)            {}
func (m *scenario55E2EMetricsRecorder) ObserveRestoreDuration(_, _ string, _ time.Duration)   {}
func (m *scenario55E2EMetricsRecorder) RecordBackupRetentionDeleted(_, _ string, _ int)       {}
func (m *scenario55E2EMetricsRecorder) SetBackupJobStatus(_, _, _, _ string, _ float64)       {}
func (m *scenario55E2EMetricsRecorder) RecordRestore(_, _, _ string)                          {}
func (m *scenario55E2EMetricsRecorder) SetDataLoadingJobsActive(_, _ string, _ float64)       {}
func (m *scenario55E2EMetricsRecorder) RecordDataLoadingRows(_, _, _, _ string, _ float64)    {}
func (m *scenario55E2EMetricsRecorder) SetDiskUsagePercent(_, _ string, _ float64)            {}
func (m *scenario55E2EMetricsRecorder) SetRecommendationsTotal(_, _, _ string, _ float64)     {}
func (m *scenario55E2EMetricsRecorder) ObserveRecommendationScanDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55E2EMetricsRecorder) SetTableBloatRatio(_, _, _ string, _ float64)     {}
func (m *scenario55E2EMetricsRecorder) RecordScaleOperation(_, _, _ string)              {}
func (m *scenario55E2EMetricsRecorder) SetRedistributionProgress(_, _ string, _ float64) {}
func (m *scenario55E2EMetricsRecorder) SetDataSkewCoefficient(_, _ string, _ float64)    {}
func (m *scenario55E2EMetricsRecorder) SetPVCSizeBytes(_, _, _ string, _ float64)        {}
func (m *scenario55E2EMetricsRecorder) RecordMirroringOperation(_, _, _ string)          {}
func (m *scenario55E2EMetricsRecorder) RecordMaintenanceOperation(_, _, _, _ string)     {}
func (m *scenario55E2EMetricsRecorder) RecordPasswordRotation()                          {}
func (m *scenario55E2EMetricsRecorder) RecordQueryHistoryInsert(_, _ string)             {}
func (m *scenario55E2EMetricsRecorder) ObserveQueryHistorySearchDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55E2EMetricsRecorder) RecordQueryHistoryExport(_, _, _ string)                 {}
func (m *scenario55E2EMetricsRecorder) RecordQueryHistoryRetentionCleanup(_, _ string, _ int64) {}
func (m *scenario55E2EMetricsRecorder) SetQueryHistorySizeBytes(_, _ string, _ float64)         {}
func (m *scenario55E2EMetricsRecorder) RecordPlanCheck(_, _ string)                             {}
func (m *scenario55E2EMetricsRecorder) RecordPlanCheckIssue(_, _, _, _ string)                  {}
func (m *scenario55E2EMetricsRecorder) ObservePlanCheckDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55E2EMetricsRecorder) RecordQueryCancel(_, _ string)              {}
func (m *scenario55E2EMetricsRecorder) RecordQueryMove(_, _ string)                {}
func (m *scenario55E2EMetricsRecorder) RecordExporterHealthCheck(_, _ string)      {}
func (m *scenario55E2EMetricsRecorder) RecordActiveQueryExport()                   {}
func (m *scenario55E2EMetricsRecorder) RecordGuestAccess(_, _ string, _ bool)      {}
func (m *scenario55E2EMetricsRecorder) RecordMonitorPause(_, _ string)             {}
func (m *scenario55E2EMetricsRecorder) RecordMonitorResume(_, _ string)            {}
func (m *scenario55E2EMetricsRecorder) RecordMonitoringDisabledAccess(_, _ string) {}
func (m *scenario55E2EMetricsRecorder) RecordCertRotation(_, _, _ string)          {}
func (m *scenario55E2EMetricsRecorder) SetCertExpirySeconds(_ string, _ float64)   {}
func (m *scenario55E2EMetricsRecorder) RecordVaultOperation(_, _ string)           {}
func (m *scenario55E2EMetricsRecorder) ObserveVaultOperationDuration(_ string, _ time.Duration) {
}
func (m *scenario55E2EMetricsRecorder) RecordWebhookAdmission(_, _, _ string)     {}
func (m *scenario55E2EMetricsRecorder) RecordUpgradeOperation(_, _, _ string)     {}
func (m *scenario55E2EMetricsRecorder) RecordRollingRestart(_, _, _ string)       {}
func (m *scenario55E2EMetricsRecorder) RecordRecoveryOperation(_, _, _, _ string) {}
