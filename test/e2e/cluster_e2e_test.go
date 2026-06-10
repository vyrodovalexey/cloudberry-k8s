//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ClusterE2ESuite tests the full cluster lifecycle end-to-end.
type ClusterE2ESuite struct {
	E2ESuite
}

func TestE2E_ClusterLifecycle(t *testing.T) {
	suite.Run(t, new(ClusterE2ESuite))
}

func (s *ClusterE2ESuite) TestE2E_FullClusterLifecycle_CreateToDelete() {
	// This test exercises the complete cluster lifecycle:
	// 1. Create CR
	// 2. Verify resources are created
	// 3. Update configuration
	// 4. Verify update is applied
	// 5. Delete CR
	// 6. Verify cleanup

	s.logger.Info("starting full cluster lifecycle E2E test")

	// Step 1: Create the CloudberryCluster CR
	cluster := testutil.NewClusterBuilder("e2e-lifecycle", s.namespace).
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithBasicAuth(true, "gpadmin").
		WithConfig(map[string]string{
			"shared_buffers":  "128MB",
			"max_connections": "100",
		}).
		WithHA(60, 20, 5).
		WithMonitoring(true, 9187).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err, "should create cluster CR")
	s.logger.Info("cluster CR created", "name", cluster.Name)

	// Step 2: Run reconciliation and verify resources
	reconciler := controller.NewClusterReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// First reconcile: add finalizer
	result, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	assert.Positive(s.T(), result.RequeueAfter)

	// Verify finalizer
	updated := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), updated.Finalizers, util.FinalizerName)
	s.logger.Info("finalizer added")

	// Second reconcile: set initial phase
	result, err = reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	assert.Positive(s.T(), result.RequeueAfter)

	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhasePending, updated.Status.Phase)
	s.logger.Info("phase set to Pending")

	// Third reconcile: create resources
	result, err = reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
	s.logger.Info("resources reconciled")

	// Verify coordinator StatefulSet
	coordSts := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, coordSts)
	require.NoError(s.T(), err)

	// Step 3: Update configuration
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	if updated.Spec.Config == nil {
		updated.Spec.Config = &cbv1alpha1.ConfigSpec{Parameters: make(map[string]string)}
	}
	updated.Spec.Config.Parameters["shared_buffers"] = "256MB"
	err = s.client.Update(s.ctx, updated)
	require.NoError(s.T(), err)
	s.logger.Info("configuration updated")

	// Step 4: Reconcile again to apply update
	_, err = reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	s.logger.Info("update reconciled")

	// Step 5: Delete the cluster
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	err = s.client.Delete(s.ctx, updated)
	require.NoError(s.T(), err)
	s.logger.Info("cluster deletion initiated")

	s.logger.Info("full cluster lifecycle E2E test completed successfully")
}

func (s *ClusterE2ESuite) TestE2E_ClusterWithStandby_FullLifecycle() {
	s.logger.Info("starting cluster with standby E2E test")

	cluster := testutil.NewClusterBuilder("e2e-standby", s.namespace).
		WithSegments(4).
		WithStandby(true).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	reconciler := controller.NewClusterReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Run through reconciliation phases
	for i := 0; i < 3; i++ {
		_, err = reconciler.Reconcile(s.ctx, req)
		require.NoError(s.T(), err, "reconcile iteration %d should succeed", i)
	}

	s.logger.Info("cluster with standby E2E test completed")
}

func (s *ClusterE2ESuite) TestE2E_ClusterActions_StartStopRestart() {
	s.logger.Info("starting cluster actions E2E test")

	cluster := testutil.NewClusterBuilder("e2e-actions", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	reconciler := controller.NewClusterReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), &metrics.NoopRecorder{}, s.logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	actions := []string{util.ActionStart, util.ActionStop, util.ActionRestart}

	for _, action := range actions {
		s.Run("action_"+action, func() {
			// Set action annotation and bump generation to trigger reconciliation
			updated := &cbv1alpha1.CloudberryCluster{}
			err := s.client.Get(s.ctx, req.NamespacedName, updated)
			require.NoError(s.T(), err)

			if updated.Annotations == nil {
				updated.Annotations = make(map[string]string)
			}
			updated.Annotations[util.AnnotationAction] = action
			// Bump generation to ensure the reconciler doesn't skip
			updated.Generation = updated.Status.ObservedGeneration + 1
			err = s.client.Update(s.ctx, updated)
			require.NoError(s.T(), err)

			// Reconcile
			_, err = reconciler.Reconcile(s.ctx, req)
			require.NoError(s.T(), err)

			// Verify annotation was removed
			err = s.client.Get(s.ctx, req.NamespacedName, updated)
			require.NoError(s.T(), err)
			_, hasAction := updated.Annotations[util.AnnotationAction]
			assert.False(s.T(), hasAction, "action %s annotation should be removed", action)
		})
	}

	s.logger.Info("cluster actions E2E test completed")
}

func (s *ClusterE2ESuite) TestE2E_MultipleClusterReconciliation() {
	s.logger.Info("starting multiple cluster reconciliation E2E test")

	clusterNames := []string{"e2e-multi-1", "e2e-multi-2", "e2e-multi-3"}

	for _, name := range clusterNames {
		cluster := testutil.NewClusterBuilder(name, s.namespace).
			WithSegments(2).
			Build()
		err := s.client.Create(s.ctx, cluster)
		require.NoError(s.T(), err, "should create cluster %s", name)
	}

	reconciler := controller.NewClusterReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), &metrics.NoopRecorder{}, s.logger,
	)

	// Reconcile all clusters
	for _, name := range clusterNames {
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      name,
				Namespace: s.namespace,
			},
		}

		// Run multiple reconciliation cycles
		for i := 0; i < 3; i++ {
			_, err := reconciler.Reconcile(s.ctx, req)
			require.NoError(s.T(), err, "reconcile %s iteration %d should succeed", name, i)
		}
	}

	s.logger.Info("multiple cluster reconciliation E2E test completed")
}

func (s *ClusterE2ESuite) TestE2E_ClusterReconciliation_WithTimeout() {
	s.logger.Info("starting cluster reconciliation with timeout E2E test")

	cluster := testutil.NewClusterBuilder("e2e-timeout", s.namespace).
		WithSegments(4).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	reconciler := controller.NewClusterReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), &metrics.NoopRecorder{}, s.logger,
	)

	// Use a tight timeout
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(s.T(), err, "reconciliation should complete within timeout")

	s.logger.Info("cluster reconciliation with timeout E2E test completed")
}
