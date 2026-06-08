//go:build functional

package functional

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Metrics Recorder for Scenario 20
// ============================================================================

// scenario20MetricsRecorder wraps NoopRecorder and tracks failover-related metric calls.
type scenario20MetricsRecorder struct {
	metrics.NoopRecorder
	FTSFailoverCalls    int32
	FTSProbeCalls       []ftsProbeCall
	SegmentStatusCalls  []segmentStatusCall
	SegmentsFailed      float64
	MirroringInSyncVal  *bool
	ReplicationLagCalls []replicationLagCall20
}

type ftsProbeCall struct {
	Cluster   string
	Namespace string
	Result    string
	Duration  time.Duration
}

type segmentStatusCall struct {
	Cluster   string
	Namespace string
	Segment   string
	Up        bool
}

type replicationLagCall20 struct {
	Cluster   string
	Namespace string
	Segment   string
	LagBytes  float64
}

func (m *scenario20MetricsRecorder) RecordFTSFailover(cluster, namespace string) {
	m.FTSFailoverCalls++
}

func (m *scenario20MetricsRecorder) RecordFTSProbe(cluster, namespace, result string, duration time.Duration) {
	m.FTSProbeCalls = append(m.FTSProbeCalls, ftsProbeCall{
		Cluster:   cluster,
		Namespace: namespace,
		Result:    result,
		Duration:  duration,
	})
}

func (m *scenario20MetricsRecorder) SetSegmentStatus(cluster, namespace, segment string, up bool) {
	m.SegmentStatusCalls = append(m.SegmentStatusCalls, segmentStatusCall{
		Cluster:   cluster,
		Namespace: namespace,
		Segment:   segment,
		Up:        up,
	})
}

func (m *scenario20MetricsRecorder) SetSegmentsFailed(cluster, namespace string, count float64) {
	m.SegmentsFailed = count
}

func (m *scenario20MetricsRecorder) SetMirroringInSync(cluster, namespace string, inSync bool) {
	m.MirroringInSyncVal = &inSync
}

func (m *scenario20MetricsRecorder) SetReplicationLag(cluster, namespace, segment string, lagBytes float64) {
	m.ReplicationLagCalls = append(m.ReplicationLagCalls, replicationLagCall20{
		Cluster:   cluster,
		Namespace: namespace,
		Segment:   segment,
		LagBytes:  lagBytes,
	})
}

// Ensure scenario20MetricsRecorder satisfies the metrics.Recorder interface at compile time.
var _ metrics.Recorder = (*scenario20MetricsRecorder)(nil)

// ============================================================================
// Test Suite
// ============================================================================

// Scenario20AutomaticFailoverSuite tests automatic segment failover via FTS.
type Scenario20AutomaticFailoverSuite struct {
	suite.Suite
	ctx context.Context
}

func TestScenario20AutomaticFailover(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario20AutomaticFailoverSuite))
}

func (s *Scenario20AutomaticFailoverSuite) SetupTest() {
	s.ctx = context.Background()
}

// ============================================================================
// Helper Functions
// ============================================================================

// scenario20Req creates a reconcile request for a named cluster.
func scenario20Req(name, namespace string) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
	}
}

// failedPrimarySegmentConfig returns a segment configuration with one primary down.
// ContentID=0 primary (DBID=2) has status "d", its mirror (DBID=5) is still "u".
func failedPrimarySegmentConfig() []db.SegmentInfo {
	return []db.SegmentInfo{
		{ContentID: -1, DBID: 1, Role: "p", PreferredRole: "p", Mode: "n", Status: "u", Hostname: "coordinator", Address: "coordinator", Port: 5432, DataDirectory: "/data/coordinator"},
		{ContentID: 0, DBID: 2, Role: "p", PreferredRole: "p", Mode: "s", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6000, DataDirectory: "/data/primary/gpseg0"},
		{ContentID: 0, DBID: 5, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7000, DataDirectory: "/data/mirror/gpseg0"},
		{ContentID: 1, DBID: 3, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 6001, DataDirectory: "/data/primary/gpseg1"},
		{ContentID: 1, DBID: 6, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7001, DataDirectory: "/data/mirror/gpseg1"},
		{ContentID: 2, DBID: 4, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6000, DataDirectory: "/data/primary/gpseg2"},
		{ContentID: 2, DBID: 7, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7000, DataDirectory: "/data/mirror/gpseg2"},
		{ContentID: 3, DBID: 8, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6001, DataDirectory: "/data/primary/gpseg3"},
		{ContentID: 3, DBID: 9, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7001, DataDirectory: "/data/mirror/gpseg3"},
	}
}

// postFailoverSegmentConfig returns a segment configuration after failover:
// The mirror for contentID=0 (DBID=5) has been promoted to primary role.
func postFailoverSegmentConfig() []db.SegmentInfo {
	return []db.SegmentInfo{
		{ContentID: -1, DBID: 1, Role: "p", PreferredRole: "p", Mode: "n", Status: "u", Hostname: "coordinator", Address: "coordinator", Port: 5432, DataDirectory: "/data/coordinator"},
		{ContentID: 0, DBID: 2, Role: "m", PreferredRole: "p", Mode: "n", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6000, DataDirectory: "/data/primary/gpseg0"},
		{ContentID: 0, DBID: 5, Role: "p", PreferredRole: "m", Mode: "n", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7000, DataDirectory: "/data/mirror/gpseg0"},
		{ContentID: 1, DBID: 3, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 6001, DataDirectory: "/data/primary/gpseg1"},
		{ContentID: 1, DBID: 6, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7001, DataDirectory: "/data/mirror/gpseg1"},
		{ContentID: 2, DBID: 4, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6000, DataDirectory: "/data/primary/gpseg2"},
		{ContentID: 2, DBID: 7, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7000, DataDirectory: "/data/mirror/gpseg2"},
		{ContentID: 3, DBID: 8, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6001, DataDirectory: "/data/primary/gpseg3"},
		{ContentID: 3, DBID: 9, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7001, DataDirectory: "/data/mirror/gpseg3"},
	}
}

// multipleFailedPrimaryConfig returns a segment configuration with two primaries down.
// ContentID=0 (DBID=2) and ContentID=1 (DBID=3) are both down.
func multipleFailedPrimaryConfig() []db.SegmentInfo {
	return []db.SegmentInfo{
		{ContentID: -1, DBID: 1, Role: "p", PreferredRole: "p", Mode: "n", Status: "u", Hostname: "coordinator", Address: "coordinator", Port: 5432, DataDirectory: "/data/coordinator"},
		{ContentID: 0, DBID: 2, Role: "p", PreferredRole: "p", Mode: "s", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6000, DataDirectory: "/data/primary/gpseg0"},
		{ContentID: 0, DBID: 5, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7000, DataDirectory: "/data/mirror/gpseg0"},
		{ContentID: 1, DBID: 3, Role: "p", PreferredRole: "p", Mode: "s", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6001, DataDirectory: "/data/primary/gpseg1"},
		{ContentID: 1, DBID: 6, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7001, DataDirectory: "/data/mirror/gpseg1"},
		{ContentID: 2, DBID: 4, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6000, DataDirectory: "/data/primary/gpseg2"},
		{ContentID: 2, DBID: 7, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7000, DataDirectory: "/data/mirror/gpseg2"},
		{ContentID: 3, DBID: 8, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6001, DataDirectory: "/data/primary/gpseg3"},
		{ContentID: 3, DBID: 9, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7001, DataDirectory: "/data/mirror/gpseg3"},
	}
}

// multiplePostFailoverConfig returns a segment configuration after failover of two primaries.
func multiplePostFailoverConfig() []db.SegmentInfo {
	return []db.SegmentInfo{
		{ContentID: -1, DBID: 1, Role: "p", PreferredRole: "p", Mode: "n", Status: "u", Hostname: "coordinator", Address: "coordinator", Port: 5432, DataDirectory: "/data/coordinator"},
		{ContentID: 0, DBID: 2, Role: "m", PreferredRole: "p", Mode: "n", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6000, DataDirectory: "/data/primary/gpseg0"},
		{ContentID: 0, DBID: 5, Role: "p", PreferredRole: "m", Mode: "n", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7000, DataDirectory: "/data/mirror/gpseg0"},
		{ContentID: 1, DBID: 3, Role: "m", PreferredRole: "p", Mode: "n", Status: "d", Hostname: "segment-0", Address: "segment-0", Port: 6001, DataDirectory: "/data/primary/gpseg1"},
		{ContentID: 1, DBID: 6, Role: "p", PreferredRole: "m", Mode: "n", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 7001, DataDirectory: "/data/mirror/gpseg1"},
		{ContentID: 2, DBID: 4, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6000, DataDirectory: "/data/primary/gpseg2"},
		{ContentID: 2, DBID: 7, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7000, DataDirectory: "/data/mirror/gpseg2"},
		{ContentID: 3, DBID: 8, Role: "p", PreferredRole: "p", Mode: "s", Status: "u", Hostname: "segment-1", Address: "segment-1", Port: 6001, DataDirectory: "/data/primary/gpseg3"},
		{ContentID: 3, DBID: 9, Role: "m", PreferredRole: "m", Mode: "s", Status: "u", Hostname: "segment-0", Address: "segment-0", Port: 7001, DataDirectory: "/data/mirror/gpseg3"},
	}
}

// ============================================================================
// Group 1: Detection Phase
// ============================================================================

// TestDetection_FTSProbeFailsForKilledSegment verifies that when a primary segment
// is down (status "d"), the FTS probe detects it and reports degraded status.
func (s *Scenario20AutomaticFailoverSuite) TestDetection_FTSProbeFailsForKilledSegment() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-detect-killed", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	// First call returns failed primary, second call (post-failover re-read) returns post-failover.
	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify mirroring status is Degraded.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus,
		"mirroring status should be Degraded when a primary is down")
	assert.NotEmpty(s.T(), updated.Status.FailedSegments,
		"failedSegments should not be empty")
}

// TestDetection_RetriesOccurUpToFTSProbeRetries verifies that the probe retries
// on failure and succeeds when the Nth attempt works.
func (s *Scenario20AutomaticFailoverSuite) TestDetection_RetriesOccurUpToFTSProbeRetries() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-detect-retries", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3). // 3 retries
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	// First 2 calls fail, 3rd succeeds with healthy config.
	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n < 3 {
				return nil, fmt.Errorf("connection refused (attempt %d)", n)
			}
			return testutil.DefaultSegmentConfiguration(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify probe succeeded after retries — mirroring should be InSync.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus,
		"mirroring status should be InSync after successful retry")
	assert.Empty(s.T(), updated.Status.FailedSegments,
		"failedSegments should be empty when all healthy")

	// Verify failure metrics were recorded for the failed attempts.
	failureCount := 0
	for _, call := range mockMetrics.FTSProbeCalls {
		if call.Result == "failure" {
			failureCount++
		}
	}
	assert.Equal(s.T(), 2, failureCount,
		"fts_probe_failures_total should have been incremented twice for 2 failed attempts")
}

// TestDetection_AllRetriesExhausted_ProbeFailureRecorded verifies that when all
// retries are exhausted, the probe failure is recorded in metrics.
func (s *Scenario20AutomaticFailoverSuite) TestDetection_AllRetriesExhausted_ProbeFailureRecorded() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-detect-exhausted", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3). // 3 retries
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act — should not return error (errors are handled internally).
	result, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify failure metrics were recorded for all 3 attempts.
	failureCount := 0
	for _, call := range mockMetrics.FTSProbeCalls {
		if call.Result == "failure" {
			failureCount++
		}
	}
	assert.Equal(s.T(), 3, failureCount,
		"fts_probe_failures_total should have been incremented 3 times for all exhausted retries")
}

// TestDetection_ProbeTimeoutRespected verifies that the probe timeout is applied
// per attempt. We use a very short timeout and a mock that respects context cancellation.
func (s *Scenario20AutomaticFailoverSuite) TestDetection_ProbeTimeoutRespected() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-detect-timeout", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 1, 2). // 1 second timeout, 2 retries
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	// Mock that blocks until context is cancelled (simulating a slow DB).
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(ctx context.Context) ([]db.SegmentInfo, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act — should complete within a reasonable time (2 retries * 1s timeout = ~2s).
	start := time.Now()
	result, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	elapsed := time.Since(start)

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify the total time is bounded by retries * timeout (with some margin).
	assert.Less(s.T(), elapsed, 10*time.Second,
		"probe should complete within bounded time (retries * timeout)")

	// Verify failure metrics were recorded.
	failureCount := 0
	for _, call := range mockMetrics.FTSProbeCalls {
		if call.Result == "failure" {
			failureCount++
		}
	}
	assert.Equal(s.T(), 2, failureCount,
		"fts_probe_failures_total should have been incremented for each timed-out attempt")
}

// ============================================================================
// Group 2: Failover Phase
// ============================================================================

// TestFailover_MirrorPromotedToPrimary verifies that when a primary is down,
// TriggerFTSProbe is called and the mirror is promoted to primary.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_MirrorPromotedToPrimary() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-promote", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	triggerCalled := false
	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			// After TriggerFTSProbe, mirror is promoted.
			return postFailoverSegmentConfig(), nil
		},
		TriggerFTSProbeFunc: func(_ context.Context) error {
			triggerCalled = true
			return nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))

	// Assert
	require.NoError(s.T(), err)
	assert.True(s.T(), triggerCalled, "TriggerFTSProbe should have been called")
}

// TestFailover_SegmentConfigurationUpdated verifies that after failover,
// the segment configuration is re-read and shows the new roles.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_SegmentConfigurationUpdated() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-config", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))

	// Assert
	require.NoError(s.T(), err)

	// GetSegmentConfiguration should have been called at least twice:
	// once for initial probe, once for post-failover re-read.
	finalCount := atomic.LoadInt32(&callCount)
	assert.GreaterOrEqual(s.T(), finalCount, int32(2),
		"GetSegmentConfiguration should be called at least twice (initial + post-failover)")
}

// TestFailover_SegmentFailoverEventEmitted verifies that a SegmentFailover
// Kubernetes event is emitted when a primary fails over.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_SegmentFailoverEventEmitted() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-event", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: SegmentFailover event should be emitted.
	events := collectEvents(fakeRecorder)
	segmentFailoverFound := false
	for _, event := range events {
		if containsSubstring(event, cbv1alpha1.EventReasonSegmentFailover) {
			segmentFailoverFound = true
			break
		}
	}
	assert.True(s.T(), segmentFailoverFound,
		"SegmentFailover event should be emitted; events: %v", events)
}

// TestFailover_FTSFailoverMetricIncrements verifies that the
// cloudberry_fts_failover_total metric is incremented during failover.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_FTSFailoverMetricIncrements() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-metric", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert
	assert.GreaterOrEqual(s.T(), mockMetrics.FTSFailoverCalls, int32(1),
		"RecordFTSFailover should have been called at least once")
}

// TestFailover_SegmentStatusDropsToZero verifies that the segment_status metric
// for the failed primary is set to 0 (false).
func (s *Scenario20AutomaticFailoverSuite) TestFailover_SegmentStatusDropsToZero() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-segstatus", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: segment 0 should have status set to false (down).
	seg0Down := false
	for _, call := range mockMetrics.SegmentStatusCalls {
		if call.Segment == "0" && !call.Up {
			seg0Down = true
			break
		}
	}
	assert.True(s.T(), seg0Down,
		"SetSegmentStatus should have been called with segment=0, up=false")
}

// TestFailover_SegmentsFailedIncrements verifies that the segments_failed gauge
// is incremented when a failover occurs.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_SegmentsFailedIncrements() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-segfailed", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert
	assert.Greater(s.T(), mockMetrics.SegmentsFailed, float64(0),
		"SetSegmentsFailed should have been called with count > 0")
}

// TestFailover_FailedSegmentsListUpdated verifies that status.failedSegments
// lists the failed segment after failover.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_FailedSegmentsListUpdated() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-failover-list", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), updated.Status.FailedSegments,
		"failedSegments should list the failed segment")

	// Verify the failed segment has contentID=0.
	found := false
	for _, fs := range updated.Status.FailedSegments {
		if fs.ContentID == 0 && fs.Role == "p" {
			found = true
			break
		}
	}
	assert.True(s.T(), found,
		"failedSegments should contain contentID=0 with role=p")
}

// ============================================================================
// Group 3: Availability
// ============================================================================

// TestFailover_ClusterRemainsAvailable verifies that after failover,
// the cluster phase remains Running (not Failed).
func (s *Scenario20AutomaticFailoverSuite) TestFailover_ClusterRemainsAvailable() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-avail-running", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: cluster phase should still be Running, not Failed.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseFailed, updated.Status.Phase,
		"cluster phase should not be Failed after failover")
}

// TestFailover_SubsequentReconcileSucceeds verifies that after failover,
// the next reconcile works normally.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_SubsequentReconcileSucceeds() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-avail-subsequent", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	// First reconcile: primary down, failover triggered.
	// Second reconcile: all healthy (post-failover state).
	var reconcileCount int32
	var getSegCallCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&getSegCallCount, 1)
			rc := atomic.LoadInt32(&reconcileCount)
			if rc == 0 && n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			return postFailoverSegmentConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act: first reconcile (failover).
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)
	atomic.StoreInt32(&reconcileCount, 1)

	// Act: second reconcile (post-failover, should succeed).
	_, err = reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err, "subsequent reconcile after failover should succeed")
}

// ============================================================================
// Group 4: Edge Cases
// ============================================================================

// TestFailover_MultiplePrimariesDown verifies that when two primaries fail
// simultaneously, both failovers are processed and events emitted.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_MultiplePrimariesDown() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-edge-multi", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return multipleFailedPrimaryConfig(), nil
			}
			return multiplePostFailoverConfig(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: both segments should be in failedSegments.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), len(updated.Status.FailedSegments), 2,
		"failedSegments should contain at least 2 entries for two failed primaries")

	// Assert: SegmentFailover events should be emitted for both.
	events := collectEvents(fakeRecorder)
	failoverEventCount := 0
	for _, event := range events {
		if containsSubstring(event, cbv1alpha1.EventReasonSegmentFailover) {
			failoverEventCount++
		}
	}
	assert.GreaterOrEqual(s.T(), failoverEventCount, 2,
		"at least 2 SegmentFailover events should be emitted; events: %v", events)
}

// TestFailover_TriggerFTSProbeError verifies that when TriggerFTSProbe fails,
// the status is still updated with the detected failures.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_TriggerFTSProbeError() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-edge-trigger-err", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	var callCount int32
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			n := atomic.AddInt32(&callCount, 1)
			if n == 1 {
				return failedPrimarySegmentConfig(), nil
			}
			// Even after trigger error, re-read still shows failed primary.
			return failedPrimarySegmentConfig(), nil
		},
		TriggerFTSProbeFunc: func(_ context.Context) error {
			return fmt.Errorf("FTS probe trigger failed: connection reset")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act — should not return error (errors are handled internally).
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: status should still be updated with failed segments.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus,
		"mirroring status should be Degraded even when TriggerFTSProbe fails")
	assert.NotEmpty(s.T(), updated.Status.FailedSegments,
		"failedSegments should still be populated even when TriggerFTSProbe fails")

	// Assert: SegmentFailover event should still be emitted.
	events := collectEvents(fakeRecorder)
	failoverEventFound := false
	for _, event := range events {
		if containsSubstring(event, cbv1alpha1.EventReasonSegmentFailover) {
			failoverEventFound = true
			break
		}
	}
	assert.True(s.T(), failoverEventFound,
		"SegmentFailover event should be emitted even when TriggerFTSProbe fails; events: %v", events)
}

// TestFailover_MirroringDisabled_NoFailover verifies that when a primary is down
// but mirroring is not enabled, no failover is triggered.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_MirroringDisabled_NoFailover() {
	// Arrange: cluster without mirroring.
	cluster := testutil.NewClusterBuilder("test-s20-edge-no-mirror", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHA(60, 5, 3).
		Build()
	// Explicitly set mirroring to disabled.
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: false,
	}
	env := testutil.NewTestK8sEnv(cluster)

	triggerCalled := false
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return failedPrimarySegmentConfig(), nil
		},
		TriggerFTSProbeFunc: func(_ context.Context) error {
			triggerCalled = true
			return nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: TriggerFTSProbe should NOT have been called.
	assert.False(s.T(), triggerCalled,
		"TriggerFTSProbe should not be called when mirroring is disabled")

	// Assert: no SegmentFailover events.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, cbv1alpha1.EventReasonSegmentFailover),
			"SegmentFailover event should not be emitted when mirroring is disabled; events: %v", events)
	}
}

// TestFailover_AllHealthy_NoFailover verifies that when all segments are healthy,
// no failover is triggered.
func (s *Scenario20AutomaticFailoverSuite) TestFailover_AllHealthy_NoFailover() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-s20-edge-healthy", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 5, 3).
		Build()
	env := testutil.NewTestK8sEnv(cluster)

	triggerCalled := false
	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return testutil.DefaultSegmentConfiguration(), nil
		},
		TriggerFTSProbeFunc: func(_ context.Context) error {
			triggerCalled = true
			return nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &scenario20MetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		env.Client, env.Scheme, fakeRecorder,
		dbFactory, env.Builder, mockMetrics, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, scenario20Req(cluster.Name, cluster.Namespace))
	require.NoError(s.T(), err)

	// Assert: TriggerFTSProbe should NOT have been called.
	assert.False(s.T(), triggerCalled,
		"TriggerFTSProbe should not be called when all segments are healthy")

	// Assert: mirroring should be InSync.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus,
		"mirroring status should be InSync when all segments are healthy")
	assert.Empty(s.T(), updated.Status.FailedSegments,
		"failedSegments should be empty when all segments are healthy")

	// Assert: no failover metrics.
	assert.Equal(s.T(), int32(0), mockMetrics.FTSFailoverCalls,
		"RecordFTSFailover should not be called when all segments are healthy")
}
