package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// coordinatorPod builds the coordinator pod (ordinal 0) object for the cluster
// under test, matching util.CoordinatorPodName so recoverWedgedCoordinator's
// Get/Delete resolves it.
func coordinatorPod(clusterName, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.CoordinatorPodName(clusterName),
			Namespace: namespace,
		},
	}
}

// wedgeError constructs the SQLSTATE 57P03 "cannot connect now" error the way
// pgx surfaces a wedged coordinator, wrapped as the scale path would wrap it.
func wedgeError() error {
	return fmt.Errorf("registering new segments: %w",
		&pgconn.PgError{Code: "57P03", Message: "the database system is shutting down"})
}

// TestRecoverWedgedCoordinator covers the D3 self-heal: when a scale phase fails
// with SQLSTATE 57P03 the operator deletes the coordinator pod, emits a Warning
// event, and requeues so the phase is retried once a fresh pod accepts
// connections. Non-57P03 causes are not handled; not-found and delete-error
// paths are covered too.
func TestRecoverWedgedCoordinator(t *testing.T) {
	scheme := newTestScheme()

	t.Run("non-wedge error is not handled", func(t *testing.T) {
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, coordinatorPod(cluster.Name, cluster.Namespace)).Build()
		rec := record.NewFakeRecorder(10)
		r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
			&metrics.NoopRecorder{}, nil, nil)

		result, handled := r.recoverWedgedCoordinator(
			context.Background(), cluster, scalePhaseRegistering, errors.New("connection refused"))
		assert.False(t, handled, "a non-57P03 error must not be handled")
		assert.Zero(t, result.RequeueAfter)
		assert.Empty(t, drainEvents(rec), "no event should be emitted for a non-wedge error")

		// The coordinator pod must still exist (was not deleted).
		pod := &corev1.Pod{}
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Name: util.CoordinatorPodName(cluster.Name), Namespace: cluster.Namespace}, pod)
		require.NoError(t, err)
	})

	t.Run("wedge detected deletes pod, emits event, requeues", func(t *testing.T) {
		cluster := newTestCluster()
		pod := coordinatorPod(cluster.Name, cluster.Namespace)
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, pod).Build()
		rec := record.NewFakeRecorder(10)
		r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
			&metrics.NoopRecorder{}, nil, nil)

		result, handled := r.recoverWedgedCoordinator(
			context.Background(), cluster, scalePhaseRedistributing, wedgeError())
		require.True(t, handled, "a 57P03 error must be handled")
		assert.Equal(t, requeueAfterCoordinatorRecovery, result.RequeueAfter)

		// The coordinator pod must have been deleted for the STS to recreate it.
		got := &corev1.Pod{}
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Name: util.CoordinatorPodName(cluster.Name), Namespace: cluster.Namespace}, got)
		assert.True(t, apierrors.IsNotFound(err), "coordinator pod must be deleted, got err=%v", err)

		// A Warning CoordinatorRecovered event must be emitted.
		events := drainEvents(rec)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "CoordinatorRecovered")
		assert.Contains(t, events[0], "redistributing")
	})

	t.Run("pod already absent (not found) just requeues", func(t *testing.T) {
		cluster := newTestCluster()
		// No coordinator pod object → Get returns NotFound.
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster).Build()
		rec := record.NewFakeRecorder(10)
		r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
			&metrics.NoopRecorder{}, nil, nil)

		result, handled := r.recoverWedgedCoordinator(
			context.Background(), cluster, scalePhaseRegistering, wedgeError())
		require.True(t, handled)
		assert.Equal(t, requeueAfterCoordinatorRecovery, result.RequeueAfter)
		// No event: nothing was deleted, we just await recreation.
		assert.Empty(t, drainEvents(rec))
	})

	t.Run("get error (non-not-found) requeues with error backoff", func(t *testing.T) {
		cluster := newTestCluster()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey,
					obj client.Object, _ ...client.GetOption) error {
					if _, ok := obj.(*corev1.Pod); ok {
						return errors.New("apiserver unavailable")
					}
					return nil
				},
			}).Build()
		rec := record.NewFakeRecorder(10)
		r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
			&metrics.NoopRecorder{}, nil, nil)

		result, handled := r.recoverWedgedCoordinator(
			context.Background(), cluster, scalePhaseRegistering, wedgeError())
		require.True(t, handled)
		assert.Equal(t, requeueAfterError, result.RequeueAfter)
		assert.Empty(t, drainEvents(rec))
	})

	t.Run("delete error requeues with error backoff", func(t *testing.T) {
		cluster := newTestCluster()
		pod := coordinatorPod(cluster.Name, cluster.Namespace)
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, pod).
			WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(_ context.Context, _ client.WithWatch, obj client.Object,
					_ ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Pod); ok {
						return errors.New("delete forbidden")
					}
					return nil
				},
			}).Build()
		rec := record.NewFakeRecorder(10)
		r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
			&metrics.NoopRecorder{}, nil, nil)

		result, handled := r.recoverWedgedCoordinator(
			context.Background(), cluster, scalePhaseRedistributing, wedgeError())
		require.True(t, handled)
		assert.Equal(t, requeueAfterError, result.RequeueAfter)
		// No success event on a delete failure.
		assert.Empty(t, drainEvents(rec))
	})
}

// TestCheckScaleOutPhases_WedgeRecoveryWiring proves the D3 wiring for the
// gpexpand-backed expanding phase: when the gpexpand Job fails and its pod's
// terminated container reports the coordinator-wedged (57P03) state,
// checkScaleOutPhases routes through recoverWedgedCoordinator — deleting the
// coordinator pod and requeueing with the coordinator-recovery backoff — rather
// than the generic requeueAfterError path. The two LEGACY phase names
// (registering/redistributing) are also routed to the expanding handler.
func TestCheckScaleOutPhases_WedgeRecoveryWiring(t *testing.T) {
	scheme := newTestScheme()

	phases := []struct {
		name  string
		phase string
	}{
		{name: "expanding phase wedge", phase: scalePhaseExpanding},
		{name: "legacy registering phase wedge", phase: scalePhaseRegistering},
		{name: "legacy redistributing phase wedge", phase: scalePhaseRedistributing},
	}

	for _, tc := range phases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
			pod := coordinatorPod(cluster.Name, cluster.Namespace)

			jobName := util.GpexpandJobName(cluster.Name, 2, 4)
			state := scaleStateData{Phase: tc.phase, OldCount: 2, NewCount: 4, ExpandJobName: jobName}
			stateJSON, err := json.Marshal(state)
			require.NoError(t, err)

			// A failed gpexpand Job whose pod reports the 57P03 wedge state.
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: cluster.Namespace},
				Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
				}},
			}
			jobPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName + "-xyz",
					Namespace: cluster.Namespace,
					Labels:    map[string]string{"job-name": jobName},
				},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Message: "the database system is shutting down (SQLSTATE 57P03)",
					}},
				}}},
			}

			k8sClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(cluster, pod, job, jobPod).
				WithStatusSubresource(cluster).Build()
			rec := record.NewFakeRecorder(10)
			r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
				&metrics.NoopRecorder{}, nil)

			result, err := r.checkScaleOutPhases(context.Background(), cluster, string(stateJSON))
			require.NoError(t, err)
			// Wedge recovery backoff (not the generic requeueAfterError).
			assert.Equal(t, requeueAfterCoordinatorRecovery, result.RequeueAfter)

			// Coordinator pod must have been deleted for self-heal.
			got := &corev1.Pod{}
			getErr := k8sClient.Get(context.Background(),
				client.ObjectKey{Name: util.CoordinatorPodName(cluster.Name), Namespace: cluster.Namespace}, got)
			assert.True(t, apierrors.IsNotFound(getErr),
				"coordinator pod must be deleted on wedge recovery, got err=%v", getErr)

			// A CoordinatorRecovered warning event must be present.
			var found bool
			for _, e := range drainEvents(rec) {
				if strings.Contains(e, "CoordinatorRecovered") {
					found = true
				}
			}
			assert.True(t, found, "a CoordinatorRecovered event must be emitted")
		})
	}
}
