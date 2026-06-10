package controller

// E-3 adversarial regression tests:
//   H-1: randomized cancel-timing stress for dispatchRebalanceTables (the
//        original semaphore-leak window was timing dependent — 100 random
//        timings under -race).
//   H-5: deletion-backup replay across 5+ public Reconcile passes with
//        controller restarts simulated by fresh reconciler instances.

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestDispatchRebalanceTables_RandomCancelStress cancels the dispatch at 100
// randomized points (from "immediately" to "mid-worker") and asserts, for
// every timing, that the dispatcher returns promptly and no worker goroutine
// or semaphore slot is leaked. Run under -race this covers the H-1 window
// deterministic tests cannot: cancellation racing slot acquisition, the
// inter-table delay, and in-flight workers.
func TestDispatchRebalanceTables_RandomCancelStress(t *testing.T) {
	r := newHATestReconciler(&metrics.NoopRecorder{})

	const iterations = 100
	for i := 0; i < iterations; i++ {
		blockCh := make(chan struct{})
		dbClient := &rebalanceTrackingDBClient{
			mockDBClient: &mockDBClient{},
			blockCh:      blockCh, // workers block until ctx cancel
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- r.dispatchRebalanceTables(ctx, r.logger, dbClient, makeSkewTables(8), 2)
		}()

		// Random cancel timing in [0, 3ms): hits before the first acquire,
		// during worker execution, and inside the inter-table delay.
		delay := time.Duration(rand.IntN(3_000_000)) //nolint:gosec // test jitter
		time.Sleep(delay)
		cancel()

		select {
		case <-done:
			// Returned — now prove no worker is still holding a slot.
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d (cancel after %v): dispatchRebalanceTables deadlocked", i, delay)
		}
		assert.Zero(t, dbClient.inFlight.Load(),
			"iteration %d: in-flight workers must drain to zero after return", i)
	}
}

// deletionReconcileRequest builds the controller-runtime request for the test cluster.
func deletionReconcileRequest(env *deletionTestEnv) ctrl.Request {
	return ctrl.Request{NamespacedName: env.key}
}

// freshReconciler simulates a controller restart: a brand-new reconciler
// instance (no in-memory state) against the same backing store.
func freshReconciler(env *deletionTestEnv) *ClusterReconciler {
	return NewClusterReconciler(env.client, newTestScheme(), record.NewFakeRecorder(50),
		builder.NewBuilder(), env.metrics, nil)
}

// TestDeletionBackup_ReplayAcrossRestarts_FivePasses drives the deletion
// state machine through the PUBLIC Reconcile entrypoint for 5+ passes,
// constructing a fresh reconciler every other pass (simulated controller
// restart). The state machine must be fully recoverable from the cluster's
// annotations: PVCs and the finalizer survive every intermediate pass and
// exactly one terminal metric is recorded.
func TestDeletionBackup_ReplayAcrossRestarts_FivePasses(t *testing.T) {
	env := newDeletionTestEnv(t, nil)
	ctx := context.Background()

	reconciler := env.r
	// Passes 1-4: Job not terminal → requeue, nothing deleted. A fresh
	// reconciler replaces the active one every second pass.
	for pass := 1; pass <= 4; pass++ {
		if pass%2 == 1 && pass > 1 {
			reconciler = freshReconciler(env) // simulated restart
		}

		result, err := reconciler.Reconcile(ctx, deletionReconcileRequest(env))
		require.NoError(t, err, "pass %d", pass)
		assert.Equal(t, requeueAfterDeletionBackup, result.RequeueAfter,
			"pass %d must requeue while the backup Job is active", pass)
		assert.Equal(t, 1, env.pvcCount(t), "pass %d: PVCs must survive", pass)

		cluster := env.getCluster(t)
		assert.NotEmpty(t, cluster.Annotations[util.AnnotationDeletionBackupJob],
			"pass %d: tracking annotation must persist across restarts", pass)
	}

	// The backup Job reaches success between passes.
	job := env.trackedJob(t)
	job.Status.Succeeded = 1
	require.NoError(t, env.client.Status().Update(ctx, job))

	// Pass 5 runs on yet another fresh reconciler and must complete the
	// deletion purely from persisted state.
	result, err := freshReconciler(env).Reconcile(ctx, deletionReconcileRequest(env))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
	assert.True(t, env.clusterGone(), "finalizer must be removed on the final pass")
	assert.Equal(t, 0, env.pvcCount(t), "PVCs must be deleted on the final pass")
	assert.Equal(t, []string{"completed"}, env.metrics.onDeleteResults(),
		"exactly ONE terminal metric across all passes and restarts")

	// Pass 6: reconciling the now-deleted object is a clean no-op.
	result, err = freshReconciler(env).Reconcile(ctx, deletionReconcileRequest(env))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
}

// TestDeletionBackup_ReplayAcrossRestarts_JobFails ensures a restart between
// Job failure and the next pass still converges (terminal failure observed by
// a reconciler that did not create the Job).
func TestDeletionBackup_ReplayAcrossRestarts_JobFails(t *testing.T) {
	env := newDeletionTestEnv(t, nil)
	ctx := context.Background()

	_, err := env.r.Reconcile(ctx, deletionReconcileRequest(env))
	require.NoError(t, err)

	job := env.trackedJob(t)
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: "True", Reason: "BackoffLimitExceeded"},
	}
	require.NoError(t, env.client.Status().Update(ctx, job))

	// Restarted controller observes the failure and proceeds with deletion.
	_, err = freshReconciler(env).Reconcile(ctx, deletionReconcileRequest(env))
	require.NoError(t, err)
	assert.True(t, env.clusterGone(), "failed backup must not wedge deletion after a restart")
	assert.Equal(t, []string{"failed"}, env.metrics.onDeleteResults())
}
