package controller

// Tests for the code-review remediation wave in the HA controller:
//   A-2: dispatchRebalanceTables semaphore-leak/deadlock fix.
//   A-7: handleStandbyActivation actually promotes; handleRecovery reports
//        honestly (result="noop", RecoveryNotImplemented event).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// recoveryCall captures a RecordRecoveryOperation invocation.
type recoveryCall struct {
	cluster      string
	namespace    string
	recoveryType string
	result       string
}

// recoveryMetricsRecorder wraps NoopRecorder and tracks recovery-operation
// metric calls for the A-7 assertions.
type recoveryMetricsRecorder struct {
	metrics.NoopRecorder
	mu    sync.Mutex
	calls []recoveryCall
}

func (r *recoveryMetricsRecorder) RecordRecoveryOperation(cluster, namespace, recoveryType, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recoveryCall{
		cluster: cluster, namespace: namespace,
		recoveryType: recoveryType, result: result,
	})
}

func (r *recoveryMetricsRecorder) results() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.calls))
	for _, c := range r.calls {
		out = append(out, c.result)
	}
	return out
}

// rebalanceTrackingDBClient embeds the shared mockDBClient and tracks
// RebalanceTable invocations, optionally blocking until released and failing
// for configured tables.
type rebalanceTrackingDBClient struct {
	*mockDBClient
	calls      atomic.Int64
	inFlight   atomic.Int64
	maxFlight  atomic.Int64
	failTables map[string]bool
	blockCh    chan struct{} // when non-nil, workers block until closed
}

func (m *rebalanceTrackingDBClient) RebalanceTable(ctx context.Context, _, schema, table, _ string) error {
	m.calls.Add(1)
	cur := m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	for {
		prev := m.maxFlight.Load()
		if cur <= prev || m.maxFlight.CompareAndSwap(prev, cur) {
			break
		}
	}
	if m.blockCh != nil {
		select {
		case <-m.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.failTables[schema+"."+table] {
		return fmt.Errorf("rebalance failed for %s.%s", schema, table)
	}
	return nil
}

func newHATestReconciler(m metrics.Recorder) *HAReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	return NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(20), nil, nil, m, nil)
}

func makeSkewTables(n int) []db.TableSkewInfo {
	tables := make([]db.TableSkewInfo, 0, n)
	for i := 0; i < n; i++ {
		tables = append(tables, db.TableSkewInfo{
			Database: "postgres",
			Schema:   "public",
			Table:    fmt.Sprintf("t%d", i),
		})
	}
	return tables
}

// ----------------------------------------------------------------------------
// A-2: dispatchRebalanceTables
// ----------------------------------------------------------------------------

func TestDispatchRebalanceTables_HappyPath(t *testing.T) {
	r := newHATestReconciler(&metrics.NoopRecorder{})
	dbClient := &rebalanceTrackingDBClient{mockDBClient: &mockDBClient{}}

	err := r.dispatchRebalanceTables(
		context.Background(), r.logger, dbClient, makeSkewTables(5), 2)

	require.NoError(t, err)
	assert.Equal(t, int64(5), dbClient.calls.Load(), "every table must be rebalanced once")
	assert.LessOrEqual(t, dbClient.maxFlight.Load(), int64(2),
		"concurrency must be bounded by parallelism")
}

func TestDispatchRebalanceTables_PartialFailure(t *testing.T) {
	r := newHATestReconciler(&metrics.NoopRecorder{})
	dbClient := &rebalanceTrackingDBClient{
		mockDBClient: &mockDBClient{},
		failTables:   map[string]bool{"public.t1": true, "public.t3": true},
	}

	err := r.dispatchRebalanceTables(
		context.Background(), r.logger, dbClient, makeSkewTables(5), 2)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 of 5 tables failed to rebalance")
	assert.Equal(t, int64(5), dbClient.calls.Load(),
		"individual failures must not block other tables")
}

// TestDispatchRebalanceTables_CancelDuringInterTableDelay reproduces the H-1
// deadlock scenario: more tables than the concurrency limit, with the context
// canceled while workers hold all slots and the dispatcher sits in the
// inter-table delay / slot acquisition. The function must return promptly
// (completion-channel assertion) with no goroutine wedged on the semaphore.
func TestDispatchRebalanceTables_CancelDuringInterTableDelay(t *testing.T) {
	r := newHATestReconciler(&metrics.NoopRecorder{})
	blockCh := make(chan struct{})
	dbClient := &rebalanceTrackingDBClient{
		mockDBClient: &mockDBClient{},
		blockCh:      blockCh, // workers block until ctx cancel
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// 8 tables with parallelism 2: the dispatcher will fill both slots
		// and then wait (delay + Acquire) for more.
		done <- r.dispatchRebalanceTables(ctx, r.logger, dbClient, makeSkewTables(8), 2)
	}()

	// Let the first workers start and occupy the slots, then cancel while
	// the dispatcher is between tables.
	require.Eventually(t, func() bool { return dbClient.inFlight.Load() == 2 },
		2*time.Second, 5*time.Millisecond, "two workers must be in flight")
	cancel()

	select {
	case <-done:
		// Returned promptly — no semaphore-slot leak / completion deadlock.
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchRebalanceTables deadlocked after context cancellation")
	}

	assert.LessOrEqual(t, dbClient.calls.Load(), int64(8))
}

// ----------------------------------------------------------------------------
// A-7a: handleStandbyActivation
// ----------------------------------------------------------------------------

// promoteTrackingDBClient counts PromoteStandby calls and can fail them.
type promoteTrackingDBClient struct {
	*mockDBClient
	promoteCalls atomic.Int64
	promoteErr   error
}

func (m *promoteTrackingDBClient) PromoteStandby(_ context.Context) error {
	m.promoteCalls.Add(1)
	return m.promoteErr
}

func newStandbyCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Annotations = map[string]string{
		util.AnnotationAction: util.ActionActivateStandby,
	}
	return cluster
}

func newStandbyReconciler(
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	m metrics.Recorder,
	recorder record.EventRecorder,
) *HAReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	return NewHAReconciler(k8sClient, scheme, recorder,
		&mockDBClientFactory{client: dbClient}, nil, m, nil)
}

func TestHandleStandbyActivation_PromotesExactlyOnce(t *testing.T) {
	cluster := newStandbyCluster()
	dbClient := &promoteTrackingDBClient{mockDBClient: &mockDBClient{}}
	metricsRec := &recoveryMetricsRecorder{}
	events := record.NewFakeRecorder(20)
	r := newStandbyReconciler(cluster, dbClient, metricsRec, events)

	_, err := r.handleStandbyActivation(context.Background(), cluster)

	require.NoError(t, err)
	assert.Equal(t, int64(1), dbClient.promoteCalls.Load(),
		"PromoteStandby must be invoked exactly once")
	assert.Equal(t, []string{"completed"}, metricsRec.results())

	// Condition set.
	cond := findCondition(cluster.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(t, cond)
	assert.Equal(t, "StandbyPromoted", cond.Reason)

	// "completed" event present.
	assert.True(t, fakeRecorderHasEvent(events, "CoordinatorFailover completed"),
		"completion event must be emitted")
}

func TestHandleStandbyActivation_PromoteFailure(t *testing.T) {
	cluster := newStandbyCluster()
	dbClient := &promoteTrackingDBClient{
		mockDBClient: &mockDBClient{},
		promoteErr:   fmt.Errorf("standby unreachable"),
	}
	metricsRec := &recoveryMetricsRecorder{}
	events := record.NewFakeRecorder(20)
	r := newStandbyReconciler(cluster, dbClient, metricsRec, events)

	_, err := r.handleStandbyActivation(context.Background(), cluster)

	require.Error(t, err, "promotion failure must surface for controller-runtime retry")
	assert.Contains(t, err.Error(), "promoting standby coordinator")
	assert.Equal(t, int64(1), dbClient.promoteCalls.Load())
	assert.Equal(t, []string{"error"}, metricsRec.results())

	cond := findCondition(cluster.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(t, cond)
	assert.Equal(t, "StandbyPromotionFailed", cond.Reason)
}

func TestHandleStandbyActivation_NoStandbyConfigured_Skips(t *testing.T) {
	cluster := newStandbyCluster()
	cluster.Spec.Standby = nil
	dbClient := &promoteTrackingDBClient{mockDBClient: &mockDBClient{}}
	metricsRec := &recoveryMetricsRecorder{}
	events := record.NewFakeRecorder(20)
	r := newStandbyReconciler(cluster, dbClient, metricsRec, events)

	_, err := r.handleStandbyActivation(context.Background(), cluster)

	require.NoError(t, err)
	assert.Zero(t, dbClient.promoteCalls.Load(),
		"PromoteStandby must NOT be called without an enabled standby")
	assert.Empty(t, metricsRec.results(), "no completion metric for skipped work")
	assert.True(t, fakeRecorderHasEvent(events, "skipped"))
}

func TestHandleStandbyActivation_DBClientError(t *testing.T) {
	cluster := newStandbyCluster()
	metricsRec := &recoveryMetricsRecorder{}
	events := record.NewFakeRecorder(20)
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewHAReconciler(k8sClient, scheme, events,
		&mockDBClientFactory{err: fmt.Errorf("connect refused")}, nil, metricsRec, nil)

	_, err := r.handleStandbyActivation(context.Background(), cluster)

	require.Error(t, err)
	assert.Equal(t, []string{"error"}, metricsRec.results())
}

// ----------------------------------------------------------------------------
// A-7b: handleRecovery honest reporting
// ----------------------------------------------------------------------------

func TestHandleRecovery_RecordsNoopNotCompleted(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationRecovery: util.RecoveryFull,
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	metricsRec := &recoveryMetricsRecorder{}
	events := record.NewFakeRecorder(20)
	r := NewHAReconciler(k8sClient, scheme, events, nil, nil, metricsRec, nil)

	_, err := r.handleRecovery(context.Background(), cluster, util.RecoveryFull)

	require.NoError(t, err)
	results := metricsRec.results()
	assert.Equal(t, []string{"noop"}, results,
		"only result=noop may be recorded for unexecuted recovery work")
	assert.NotContains(t, results, "completed")
	assert.NotContains(t, results, "started")
	assert.True(t, fakeRecorderHasEvent(events, "RecoveryNotImplemented"),
		"explicit not-implemented event must be emitted")
}

// fakeRecorderHasEvent drains the fake recorder's channel looking for a
// substring match. Non-blocking: returns false when no matching event exists.
func fakeRecorderHasEvent(rec *record.FakeRecorder, substr string) bool {
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, substr) {
				return true
			}
		default:
			return false
		}
	}
}
