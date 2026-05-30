//go:build functional

package functional

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
// Scenario 55: CR Status Fields Updated from Database
// ============================================================================
//
// This scenario verifies that the CR status fields (activeQueries,
// queuedQueries, blockedQueries) are correctly updated from the database
// during reconciliation. The reconcileQueryMonitoring method calls
// GetActiveQueryCount() via the DB client and writes results to
// cluster.Status, which is then patched to the API server.
//
// ============================================================================

// Scenario55CRStatusFieldsSuite tests that CR status fields are updated
// from the database during reconciliation.
type Scenario55CRStatusFieldsSuite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario55(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario55CRStatusFieldsSuite))
}

func (s *Scenario55CRStatusFieldsSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario55CRStatusFieldsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario55MockDBFactory creates a MockDBClientFactory that returns specific
// query counts when GetActiveQueryCount is called.
func scenario55MockDBFactory(active, queued, blocked int32) *testutil.MockDBClientFactory {
	return &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetActiveQueryCountFunc: func(ctx context.Context) (int32, int32, int32, error) {
				return active, queued, blocked, nil
			},
		},
	}
}

// scenario55Cluster returns a cluster with QueryMonitoring enabled and
// a pending generation to trigger reconciliation.
func scenario55Cluster(name string) *cbv1alpha1.CloudberryCluster {
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

// TestFunctional_Scenario55_ActiveQueries_Updated verifies that after
// reconciliation with a mock DB returning active=10, the cluster status
// field ActiveQueries is set to 10.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_ActiveQueries_Updated() {
	// Arrange
	cluster := scenario55Cluster("test-s55-active")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55MockDBFactory(10, 0, 0)

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

// TestFunctional_Scenario55_QueuedQueries_Updated verifies that after
// reconciliation with a mock DB returning queued=3, the cluster status
// field QueuedQueries is set to 3.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_QueuedQueries_Updated() {
	// Arrange
	cluster := scenario55Cluster("test-s55-queued")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55MockDBFactory(5, 3, 0)

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

// TestFunctional_Scenario55_BlockedQueries_Updated verifies that after
// reconciliation with a mock DB returning blocked=2, the cluster status
// field BlockedQueries is set to 2.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_BlockedQueries_Updated() {
	// Arrange
	cluster := scenario55Cluster("test-s55-blocked")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55MockDBFactory(5, 0, 2)

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

// TestFunctional_Scenario55_AllQueryCounts_Updated verifies that after
// reconciliation with a mock DB returning active=10, queued=3, blocked=1,
// all three status fields are correctly set.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_AllQueryCounts_Updated() {
	// Arrange
	cluster := scenario55Cluster("test-s55-all")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55MockDBFactory(10, 3, 1)

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

// TestFunctional_Scenario55_MetricsReflectStatus verifies that after
// reconciliation, the Prometheus metrics recorder receives the correct
// query count values matching the DB response.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_MetricsReflectStatus() {
	// Arrange
	cluster := scenario55Cluster("test-s55-metrics")
	env := testutil.NewTestK8sEnv(cluster)
	mockFactory := scenario55MockDBFactory(7, 2, 1)
	metricsRecorder := &scenario55MetricsRecorder{}

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
	activeCalls := scenario55FilterCalls(calls, "SetActiveQueries")
	require.NotEmpty(s.T(), activeCalls,
		"SetActiveQueries should have been called")
	assert.Equal(s.T(), float64(7), activeCalls[len(activeCalls)-1].args["count"],
		"SetActiveQueries should be called with count=7")

	// Verify SetQueuedQueries was called with the correct value.
	queuedCalls := scenario55FilterCalls(calls, "SetQueuedQueries")
	require.NotEmpty(s.T(), queuedCalls,
		"SetQueuedQueries should have been called")
	assert.Equal(s.T(), float64(2), queuedCalls[len(queuedCalls)-1].args["count"],
		"SetQueuedQueries should be called with count=2")

	// Verify SetBlockedQueries was called with the correct value.
	blockedCalls := scenario55FilterCalls(calls, "SetBlockedQueries")
	require.NotEmpty(s.T(), blockedCalls,
		"SetBlockedQueries should have been called")
	assert.Equal(s.T(), float64(1), blockedCalls[len(blockedCalls)-1].args["count"],
		"SetBlockedQueries should be called with count=1")
}

// --- 55.6: Status updates on each reconcile ---

// TestFunctional_Scenario55_StatusUpdatesOnEachReconcile verifies that
// when the DB returns different values on subsequent reconciliations,
// the status fields are updated accordingly.
func (s *Scenario55CRStatusFieldsSuite) TestFunctional_Scenario55_StatusUpdatesOnEachReconcile() {
	// Arrange: first reconcile with active=5
	cluster := scenario55Cluster("test-s55-update")
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
// Scenario 55: Capturing Metrics Recorder
// ============================================================================

// scenario55MetricsCall records a single metrics method invocation.
type scenario55MetricsCall struct {
	method string
	args   map[string]interface{}
}

// scenario55MetricsRecorder captures metrics calls for verification.
type scenario55MetricsRecorder struct {
	mu    sync.Mutex
	calls []scenario55MetricsCall
}

func (m *scenario55MetricsRecorder) record(method string, args map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, scenario55MetricsCall{method: method, args: args})
}

func (m *scenario55MetricsRecorder) getCalls() []scenario55MetricsCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]scenario55MetricsCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// scenario55FilterCalls returns all calls matching the given method name.
func scenario55FilterCalls(calls []scenario55MetricsCall, method string) []scenario55MetricsCall {
	var result []scenario55MetricsCall
	for _, c := range calls {
		if c.method == method {
			result = append(result, c)
		}
	}
	return result
}

// Implement the metrics.Recorder interface.
var _ metrics.Recorder = (*scenario55MetricsRecorder)(nil)

func (m *scenario55MetricsRecorder) RecordReconcile(_, _, _ string, _ time.Duration) {}
func (m *scenario55MetricsRecorder) UpdateClusterInfo(_, _, _, _ string, _ float64)  {}
func (m *scenario55MetricsRecorder) SetCoordinatorUp(_, _ string, _ bool)            {}
func (m *scenario55MetricsRecorder) SetStandbyUp(_, _ string, _ bool)                {}
func (m *scenario55MetricsRecorder) SetSegmentsReady(_, _ string, _ float64)         {}
func (m *scenario55MetricsRecorder) SetSegmentsTotal(_, _ string, _ float64)         {}
func (m *scenario55MetricsRecorder) SetSegmentsFailed(_, _ string, _ float64)        {}
func (m *scenario55MetricsRecorder) SetMirroringInSync(_, _ string, _ bool)          {}
func (m *scenario55MetricsRecorder) RecordFTSProbe(_, _, _ string, _ time.Duration)  {}
func (m *scenario55MetricsRecorder) RecordFTSFailover(_, _ string)                   {}
func (m *scenario55MetricsRecorder) SetSegmentStatus(_, _, _ string, _ bool)         {}
func (m *scenario55MetricsRecorder) SetReplicationLag(_, _, _ string, _ float64)     {}
func (m *scenario55MetricsRecorder) SetStandbyReplicationLag(_, _ string, _ float64) {}
func (m *scenario55MetricsRecorder) RecordConfigReload(_, _ string)                  {}
func (m *scenario55MetricsRecorder) SetConnectionsActive(_, _ string, _ float64)     {}
func (m *scenario55MetricsRecorder) SetConnectionsMax(_, _ string, _ float64)        {}
func (m *scenario55MetricsRecorder) SetDiskUsageBytes(_, _, _ string, _ float64)     {}
func (m *scenario55MetricsRecorder) RecordAuthAttempt(_, _ string)                   {}

func (m *scenario55MetricsRecorder) SetActiveQueries(cluster, namespace string, count float64) {
	m.record("SetActiveQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55MetricsRecorder) SetQueuedQueries(cluster, namespace string, count float64) {
	m.record("SetQueuedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55MetricsRecorder) SetBlockedQueries(cluster, namespace string, count float64) {
	m.record("SetBlockedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *scenario55MetricsRecorder) RecordWorkloadRuleAction(_, _, _, _ string)         {}
func (m *scenario55MetricsRecorder) SetResourceGroupUsage(_, _, _ string, _, _ float64) {}
func (m *scenario55MetricsRecorder) RecordIdleSessionTermination(_, _, _ string)        {}
func (m *scenario55MetricsRecorder) RecordSlowQuery(_, _ string)                        {}
func (m *scenario55MetricsRecorder) RecordBackup(_, _, _, _ string)                     {}
func (m *scenario55MetricsRecorder) ObserveBackupDuration(_, _ string, _ time.Duration) {}
func (m *scenario55MetricsRecorder) SetBackupSizeBytes(_, _ string, _ float64)          {}
func (m *scenario55MetricsRecorder) RecordRestore(_, _, _ string)                       {}
func (m *scenario55MetricsRecorder) SetDataLoadingJobsActive(_, _ string, _ float64)    {}
func (m *scenario55MetricsRecorder) RecordDataLoadingRows(_, _, _, _ string, _ float64) {}
func (m *scenario55MetricsRecorder) SetDiskUsagePercent(_, _ string, _ float64)         {}
func (m *scenario55MetricsRecorder) SetRecommendationsTotal(_, _, _ string, _ float64)  {}
func (m *scenario55MetricsRecorder) ObserveRecommendationScanDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55MetricsRecorder) SetTableBloatRatio(_, _, _ string, _ float64)     {}
func (m *scenario55MetricsRecorder) RecordScaleOperation(_, _, _ string)              {}
func (m *scenario55MetricsRecorder) SetRedistributionProgress(_, _ string, _ float64) {}
func (m *scenario55MetricsRecorder) SetDataSkewCoefficient(_, _ string, _ float64)    {}
func (m *scenario55MetricsRecorder) SetPVCSizeBytes(_, _, _ string, _ float64)        {}
func (m *scenario55MetricsRecorder) RecordMirroringOperation(_, _, _ string)          {}
func (m *scenario55MetricsRecorder) RecordMaintenanceOperation(_, _, _ string)        {}
func (m *scenario55MetricsRecorder) RecordPasswordRotation()                          {}
func (m *scenario55MetricsRecorder) RecordQueryHistoryInsert(_, _ string)             {}
func (m *scenario55MetricsRecorder) ObserveQueryHistorySearchDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55MetricsRecorder) RecordQueryHistoryExport(_, _, _ string)                 {}
func (m *scenario55MetricsRecorder) RecordQueryHistoryRetentionCleanup(_, _ string, _ int64) {}
func (m *scenario55MetricsRecorder) SetQueryHistorySizeBytes(_, _ string, _ float64)         {}
func (m *scenario55MetricsRecorder) RecordPlanCheck(_, _ string)                             {}
func (m *scenario55MetricsRecorder) RecordPlanCheckIssue(_, _, _, _ string)                  {}
func (m *scenario55MetricsRecorder) ObservePlanCheckDuration(_, _ string, _ time.Duration) {
}
func (m *scenario55MetricsRecorder) RecordQueryCancel(_, _ string)              {}
func (m *scenario55MetricsRecorder) RecordQueryMove(_, _ string)                {}
func (m *scenario55MetricsRecorder) RecordExporterHealthCheck(_, _ string)      {}
func (m *scenario55MetricsRecorder) RecordActiveQueryExport()                   {}
func (m *scenario55MetricsRecorder) RecordGuestAccess(_, _ string, _ bool)      {}
func (m *scenario55MetricsRecorder) RecordMonitorPause(_, _ string)             {}
func (m *scenario55MetricsRecorder) RecordMonitorResume(_, _ string)            {}
func (m *scenario55MetricsRecorder) RecordMonitoringDisabledAccess(_, _ string) {}
func (m *scenario55MetricsRecorder) RecordCertRotation(_, _, _ string)          {}
func (m *scenario55MetricsRecorder) SetCertExpirySeconds(_ string, _ float64)   {}
func (m *scenario55MetricsRecorder) RecordVaultOperation(_, _ string)           {}
func (m *scenario55MetricsRecorder) ObserveVaultOperationDuration(_ string, _ time.Duration) {
}
func (m *scenario55MetricsRecorder) RecordWebhookAdmission(_, _, _ string)     {}
func (m *scenario55MetricsRecorder) RecordUpgradeOperation(_, _, _ string)     {}
func (m *scenario55MetricsRecorder) RecordRollingRestart(_, _, _ string)       {}
func (m *scenario55MetricsRecorder) RecordRecoveryOperation(_, _, _, _ string) {}
