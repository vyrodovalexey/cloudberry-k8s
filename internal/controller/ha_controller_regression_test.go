package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// TestHAReconciler_Reconcile_SteadyStateStillRunsFTSProbe is a focused regression
// test for the HA reconcile bug fix.
//
// Previously, HAReconciler.Reconcile contained an early-return that SKIPPED the
// periodic health checks (FTS probe + standby monitoring) whenever the cluster
// was in steady state — i.e. Status.ObservedGeneration == metadata.Generation —
// and no recovery/action annotation was present. That meant the mirroring status
// and failed-segment list were never refreshed on periodic requeues, so a segment
// that went down between spec changes would never be detected.
//
// The fix removed that early-return. Now, for a Running cluster, runHealthChecks
// (and therefore runFTSProbe when mirroring is enabled) runs on EVERY reconcile/
// requeue regardless of generation equality. This test locks in that behavior:
// even in steady state with no annotations, calling Reconcile must (re)compute the
// mirroring status and failed segments from the DB segment configuration.
func TestHAReconciler_Reconcile_SteadyStateStillRunsFTSProbe(t *testing.T) {
	tests := []struct {
		name            string
		segments        []db.SegmentInfo
		wantStatus      cbv1alpha1.MirroringStatus
		wantFailedCount int
		wantFailedID    int32
	}{
		{
			name: "all segments up -> InSync, no failed segments",
			segments: []db.SegmentInfo{
				{ContentID: -1, Status: "u", Role: "p"}, // coordinator, skipped
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
				{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
				{ContentID: 1, Status: "u", Role: "p", Hostname: "host3"},
				{ContentID: 1, Status: "u", Role: "m", Hostname: "host4"},
			},
			wantStatus:      cbv1alpha1.MirroringInSync,
			wantFailedCount: 0,
		},
		{
			name: "a segment down -> Degraded, failed segment recorded",
			segments: []db.SegmentInfo{
				{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
				{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
				{ContentID: 1, Status: "u", Role: "p", Hostname: "host3"},
				{ContentID: 1, Status: "d", Role: "m", Hostname: "host4"}, // mirror down
			},
			wantStatus:      cbv1alpha1.MirroringDegraded,
			wantFailedCount: 1,
			wantFailedID:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: a Running cluster in steady state (ObservedGeneration ==
			// Generation) with mirroring enabled and NO recovery/action annotations.
			scheme := newTestScheme()
			cluster := newTestCluster()
			cluster.Generation = 7
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			cluster.Status.ObservedGeneration = 7 // steady state
			cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()
			recorder := record.NewFakeRecorder(10)
			m := &metrics.NoopRecorder{}

			dbClient := &mockDBClient{segments: tc.segments}
			dbFactory := &mockDBClientFactory{client: dbClient}

			r := NewHAReconciler(k8sClient, scheme, recorder, dbFactory, nil, m, nil)

			// Act: reconcile in steady state.
			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
			})

			// Assert: reconcile succeeded and requeued at the probe interval,
			// proving the periodic health-check path ran (not the old skip path).
			require.NoError(t, err)
			assert.Equal(t, r.probeInterval(cluster), result.RequeueAfter)

			// Assert: the FTS probe ran and (re)computed mirroring status from the
			// DB segment configuration even though we were in steady state. The
			// recomputed status is persisted to the cluster status subresource via
			// the FTS status patch, so we read it back from the API server.
			updated := &cbv1alpha1.CloudberryCluster{}
			require.NoError(t, k8sClient.Get(context.Background(),
				types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated))
			assert.Equal(t, tc.wantStatus, updated.Status.MirroringStatus)
			require.Len(t, updated.Status.FailedSegments, tc.wantFailedCount)
			if tc.wantFailedCount > 0 {
				assert.Equal(t, tc.wantFailedID, updated.Status.FailedSegments[0].ContentID)
			}
		})
	}
}

// TestHAReconciler_Reconcile_SteadyStateInSyncThenDegraded verifies the
// transition detection the bug fix enables: a steady-state cluster reported as
// healthy must flip to Degraded on a subsequent periodic reconcile once a segment
// goes down — without any spec change (Generation/ObservedGeneration unchanged).
func TestHAReconciler_Reconcile_SteadyStateInSyncThenDegraded(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 3
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 3 // steady state, never changes here
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	// First reconcile: all healthy -> InSync.
	healthyClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, Status: "u", Role: "p", Hostname: "host1"},
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	r := NewHAReconciler(k8sClient, scheme, recorder, &mockDBClientFactory{client: healthyClient}, nil, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated))
	assert.Equal(t, cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus)
	assert.Empty(t, updated.Status.FailedSegments)

	// Second reconcile (periodic, still steady state): a primary goes down.
	// The fix guarantees the FTS probe runs again and detects the degradation.
	degradedClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, Status: "d", Role: "p", Hostname: "host1"}, // primary down
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	r2 := NewHAReconciler(k8sClient, scheme, recorder, &mockDBClientFactory{client: degradedClient}, nil, m, nil)

	_, err = r2.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated))
	assert.Equal(t, cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus)
	require.Len(t, updated.Status.FailedSegments, 1)
	assert.Equal(t, int32(0), updated.Status.FailedSegments[0].ContentID)
}

// TestHAReconciler_Reconcile_ArbitraryAnnotationDoesNotSkipProbe verifies that an
// arbitrary, non-recovery/non-action annotation does not cause the FTS probe to be
// skipped. handleAnnotations only handles the recovery and action annotations; any
// other annotation must fall through to the periodic health checks.
func TestHAReconciler_Reconcile_ArbitraryAnnotationDoesNotSkipProbe(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 4
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 4 // steady state
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Annotations = map[string]string{
		"avsoft.io/force-reconcile": "true",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClient{
		segments: []db.SegmentInfo{
			{ContentID: 0, Status: "d", Role: "p", Hostname: "host1"}, // down
			{ContentID: 0, Status: "u", Role: "m", Hostname: "host2"},
		},
	}
	r := NewHAReconciler(k8sClient, scheme, recorder, &mockDBClientFactory{client: dbClient}, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)

	// Requeued at probe interval (not requeueAfterDefault) -> probe path taken.
	assert.Equal(t, r.probeInterval(cluster), result.RequeueAfter)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated))
	assert.Equal(t, cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus)
	require.Len(t, updated.Status.FailedSegments, 1)
	assert.Equal(t, int32(0), updated.Status.FailedSegments[0].ContentID)
}
