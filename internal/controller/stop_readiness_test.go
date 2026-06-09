package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// stoppedReadinessCluster returns a cluster that is "running" with non-zero
// readiness status persisted, so a subsequent Stop transition must clear those
// fields. No StatefulSets are seeded, so allStatefulSetsAtScale(0) returns true
// and the stop paths reach their terminal persist branch immediately.
func stoppedReadinessCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	c.Status.CoordinatorReady = true
	c.Status.StandbyReady = true
	c.Status.SegmentsReady = 4
	c.Status.SegmentsTotal = 4
	return c
}

// readPersistedStatus re-reads the cluster from the fake client so the assertions
// observe the PERSISTED status (not the in-memory copy mutated by the reconciler).
func readPersistedStatus(
	t *testing.T,
	r *ClusterReconciler,
	c *cbv1alpha1.CloudberryCluster,
) *cbv1alpha1.CloudberryCluster {
	t.Helper()
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, r.client.Get(context.Background(), types.NamespacedName{
		Name: c.Name, Namespace: c.Namespace,
	}, got))
	return got
}

// TestCheckStopProgress_PersistsClearedReadiness asserts that when a cluster
// finishes stopping, the persisted status has coordinatorReady=false,
// standbyReady=false and segmentsReady=0. This guards the omitempty+MergePatch
// regression where zero values were silently dropped from the patch, leaving a
// Stopped cluster advertising coordinatorReady=true / segmentsReady=N.
func TestCheckStopProgress_PersistsClearedReadiness(t *testing.T) {
	scheme := newTestScheme()
	cluster := stoppedReadinessCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewClusterReconciler(
		k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil,
	)

	_, err := r.checkStopProgress(context.Background(), cluster)
	require.NoError(t, err)

	got := readPersistedStatus(t, r, cluster)
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, got.Status.Phase, "phase must be Stopped")
	assert.False(t, got.Status.CoordinatorReady, "coordinatorReady must be persisted as false")
	assert.False(t, got.Status.StandbyReady, "standbyReady must be persisted as false")
	assert.Equal(t, int32(0), got.Status.SegmentsReady, "segmentsReady must be persisted as 0")
	// Fields that are NOT cleared on stop must survive the explicit patch.
	assert.Equal(t, int32(4), got.Status.SegmentsTotal, "segmentsTotal must not be clobbered")
}

// TestHandleStop_PersistsClearedReadiness drives the full handleStop path (with a
// nil dbFactory so the DB-level graceful shutdown is skipped and no StatefulSets
// to scale) and asserts the persisted readiness fields are cleared.
func TestHandleStop_PersistsClearedReadiness(t *testing.T) {
	scheme := newTestScheme()
	cluster := stoppedReadinessCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewClusterReconciler(
		k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil,
	)

	// stop-immediate skips DB shutdown; no StatefulSets means scale-down completes
	// immediately and handleStop reaches the terminal Stopped persist branch.
	_, err := r.handleStop(context.Background(), cluster, "stop-immediate")
	require.NoError(t, err)

	got := readPersistedStatus(t, r, cluster)
	assert.Equal(t, cbv1alpha1.ClusterPhaseStopped, got.Status.Phase)
	assert.False(t, got.Status.CoordinatorReady, "coordinatorReady must be persisted as false")
	assert.False(t, got.Status.StandbyReady, "standbyReady must be persisted as false")
	assert.Equal(t, int32(0), got.Status.SegmentsReady, "segmentsReady must be persisted as 0")
}

// TestCompleteRestart_PersistsClearedReadiness drives the restart call site of
// patchClearReadinessStatus: completeRestart resets the phase to Initializing and
// must persist coordinatorReady=false / standbyReady=false / segmentsReady=0 via
// the explicit-value MergePatch, even though the cluster previously advertised
// non-zero readiness. This guards the same omitempty regression on the restart
// path that TestCheckStopProgress_* guards on the stop path.
func TestCompleteRestart_PersistsClearedReadiness(t *testing.T) {
	scheme := newTestScheme()
	cluster := stoppedReadinessCluster()
	// Restart-pending annotation drives checkStopProgress into completeRestart.
	cluster.Annotations = map[string]string{util.AnnotationRestartPending: "true"}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewClusterReconciler(
		k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil,
	)

	_, err := r.completeRestart(context.Background(), cluster)
	require.NoError(t, err)

	got := readPersistedStatus(t, r, cluster)
	assert.Equal(t, cbv1alpha1.ClusterPhaseInitializing, got.Status.Phase,
		"restart resets phase to Initializing")
	assert.False(t, got.Status.CoordinatorReady, "coordinatorReady must be persisted as false")
	assert.False(t, got.Status.StandbyReady, "standbyReady must be persisted as false")
	assert.Equal(t, int32(0), got.Status.SegmentsReady, "segmentsReady must be persisted as 0")
	// Unrelated field must survive the explicit readiness-clear patch.
	assert.Equal(t, int32(4), got.Status.SegmentsTotal, "segmentsTotal must not be clobbered")
	// The restart-pending annotation must be removed as part of completing restart.
	_, pending := got.Annotations[util.AnnotationRestartPending]
	assert.False(t, pending, "restart-pending annotation must be cleared")
}

// TestCheckStopProgress_RestartPending_RoutesToCompleteRestart verifies that when
// a restart is pending, checkStopProgress completes the restart (phase ->
// Initializing) instead of transitioning to Stopped, exercising the
// patchClearReadinessStatus restart call site through the stop-progress entry.
func TestCheckStopProgress_RestartPending_RoutesToCompleteRestart(t *testing.T) {
	scheme := newTestScheme()
	cluster := stoppedReadinessCluster()
	cluster.Annotations = map[string]string{util.AnnotationRestartPending: "true"}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewClusterReconciler(
		k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil,
	)

	_, err := r.checkStopProgress(context.Background(), cluster)
	require.NoError(t, err)

	got := readPersistedStatus(t, r, cluster)
	assert.Equal(t, cbv1alpha1.ClusterPhaseInitializing, got.Status.Phase,
		"restart-pending must route to completeRestart, not Stopped")
	assert.False(t, got.Status.CoordinatorReady)
	assert.Equal(t, int32(0), got.Status.SegmentsReady)
}

// TestPatchClearReadinessStatus_BypassesOmitempty is a direct unit test of the
// helper: it proves the explicit-value MergePatch clears omitempty zero values
// that a generic patchStatus (json.Marshal of the struct) would drop.
func TestPatchClearReadinessStatus_BypassesOmitempty(t *testing.T) {
	scheme := newTestScheme()
	cluster := stoppedReadinessCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewClusterReconciler(
		k8sClient, scheme, record.NewFakeRecorder(10), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil,
	)

	// Sanity: a generic patchStatus does NOT clear the readiness fields because
	// json.Marshal drops the omitempty zero values.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.CoordinatorReady = false
	cluster.Status.SegmentsReady = 0
	require.NoError(t, patchStatus(context.Background(), r.client, cluster))
	stale := readPersistedStatus(t, r, cluster)
	assert.True(t, stale.Status.CoordinatorReady,
		"generic patchStatus must leave the previously-persisted true (regression baseline)")
	assert.Equal(t, int32(4), stale.Status.SegmentsReady,
		"generic patchStatus must leave the previously-persisted count")

	// The dedicated helper clears them via an explicit-value MergePatch.
	require.NoError(t, patchClearReadinessStatus(context.Background(), r.client, cluster))
	got := readPersistedStatus(t, r, cluster)
	assert.False(t, got.Status.CoordinatorReady)
	assert.False(t, got.Status.StandbyReady)
	assert.Equal(t, int32(0), got.Status.SegmentsReady)
}
