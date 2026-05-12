//go:build e2e

package e2e

import (
	"context"
	"testing"

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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// HAE2ESuite tests HA operations end-to-end.
type HAE2ESuite struct {
	E2ESuite
}

func TestE2E_HA(t *testing.T) {
	suite.Run(t, new(HAE2ESuite))
}

func (s *HAE2ESuite) TestE2E_HA_MirroringLifecycle() {
	// Full mirroring lifecycle:
	// 1. Create cluster with mirroring enabled
	// 2. Verify mirroring status is InSync
	// 3. Simulate segment failure
	// 4. Verify mirroring status is Degraded
	// 5. Simulate recovery
	// 6. Verify mirroring status returns to InSync

	s.logger.Info("starting mirroring lifecycle E2E test")

	cluster := testutil.NewClusterBuilder("e2e-ha-mirror", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 20, 5).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Step 1: All segments healthy
	healthyDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return testutil.DefaultSegmentConfiguration(), nil
		},
		GetReplicationLagFunc: func(_ context.Context) (int64, error) {
			return 0, nil
		},
	}
	healthyFactory := &testutil.MockDBClientFactory{Client: healthyDB}

	haReconciler := controller.NewHAReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		healthyFactory, &metrics.NoopRecorder{}, s.logger,
	)

	_, err = haReconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify InSync
	updated := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus)
	s.logger.Info("mirroring verified as InSync")

	// Step 2: Simulate segment failure
	degradedDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return testutil.DegradedSegmentConfiguration(), nil
		},
	}
	degradedFactory := &testutil.MockDBClientFactory{Client: degradedDB}

	haReconcilerDegraded := controller.NewHAReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		degradedFactory, &metrics.NoopRecorder{}, s.logger,
	)

	_, err = haReconcilerDegraded.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify Degraded
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus)
	assert.NotEmpty(s.T(), updated.Status.FailedSegments)
	s.logger.Info("mirroring verified as Degraded", "failedSegments", len(updated.Status.FailedSegments))

	// Step 3: Trigger recovery
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[util.AnnotationRecovery] = util.RecoveryIncremental
	err = s.client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	_, err = haReconcilerDegraded.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify recovery annotation was removed
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	_, hasRecovery := updated.Annotations[util.AnnotationRecovery]
	assert.False(s.T(), hasRecovery, "recovery annotation should be removed")
	s.logger.Info("recovery triggered and annotation removed")

	// Step 4: Simulate recovery complete (all healthy again)
	_, err = haReconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus)
	assert.Empty(s.T(), updated.Status.FailedSegments)
	s.logger.Info("mirroring recovered to InSync")

	s.logger.Info("mirroring lifecycle E2E test completed successfully")
}

func (s *HAE2ESuite) TestE2E_HA_StandbyMonitoring() {
	s.logger.Info("starting standby monitoring E2E test")

	cluster := testutil.NewClusterBuilder("e2e-ha-standby", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithStandby(true).
		WithHA(60, 20, 5).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Standby with low replication lag
	mockDB := &testutil.MockDBClient{
		GetReplicationLagFunc: func(_ context.Context) (int64, error) {
			return 512, nil // 512 bytes lag
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	haReconciler := controller.NewHAReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		dbFactory, &metrics.NoopRecorder{}, s.logger,
	)

	_, err = haReconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify standby condition
	updated := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)

	standbyCond := util.FindCondition(updated.Status.Conditions, string(cbv1alpha1.ConditionStandbyReady))
	require.NotNil(s.T(), standbyCond, "StandbyReady condition should be set")
	assert.Equal(s.T(), "True", string(standbyCond.Status))

	s.logger.Info("standby monitoring E2E test completed")
}

func (s *HAE2ESuite) TestE2E_HA_StandbyActivation() {
	s.logger.Info("starting standby activation E2E test")

	cluster := testutil.NewClusterBuilder("e2e-ha-activate", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStandby(true).
		WithAnnotation(util.AnnotationAction, util.ActionActivateStandby).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	haReconciler := controller.NewHAReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		dbFactory, &metrics.NoopRecorder{}, s.logger,
	)

	_, err = haReconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	_, hasAction := updated.Annotations[util.AnnotationAction]
	assert.False(s.T(), hasAction, "activate-standby annotation should be removed")

	s.logger.Info("standby activation E2E test completed")
}

func (s *HAE2ESuite) TestE2E_HA_RebalanceOperation() {
	s.logger.Info("starting rebalance operation E2E test")

	cluster := testutil.NewClusterBuilder("e2e-ha-rebalance", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithAnnotation(util.AnnotationAction, util.ActionRebalance).
		Build()

	err := s.client.Create(s.ctx, cluster)
	require.NoError(s.T(), err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	haReconciler := controller.NewHAReconciler(
		s.client, s.scheme, record.NewFakeRecorder(100),
		dbFactory, &metrics.NoopRecorder{}, s.logger,
	)

	_, err = haReconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated := &cbv1alpha1.CloudberryCluster{}
	err = s.client.Get(s.ctx, req.NamespacedName, updated)
	require.NoError(s.T(), err)
	_, hasAction := updated.Annotations[util.AnnotationAction]
	assert.False(s.T(), hasAction, "rebalance annotation should be removed")

	s.logger.Info("rebalance operation E2E test completed")
}

func (s *HAE2ESuite) TestE2E_HA_RecoveryTypes() {
	s.logger.Info("starting recovery types E2E test")

	recoveryTypes := []string{
		util.RecoveryIncremental,
		util.RecoveryFull,
		util.RecoveryDifferential,
	}

	for _, recoveryType := range recoveryTypes {
		s.Run("recovery_"+recoveryType, func() {
			clusterName := "e2e-ha-recovery-" + recoveryType
			cluster := testutil.NewClusterBuilder(clusterName, s.namespace).
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				WithAnnotation(util.AnnotationRecovery, recoveryType).
				Build()

			err := s.client.Create(s.ctx, cluster)
			require.NoError(s.T(), err)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      clusterName,
					Namespace: s.namespace,
				},
			}

			mockDB := &testutil.MockDBClient{}
			dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

			haReconciler := controller.NewHAReconciler(
				s.client, s.scheme, record.NewFakeRecorder(100),
				dbFactory, &metrics.NoopRecorder{}, s.logger,
			)

			_, err = haReconciler.Reconcile(s.ctx, req)
			require.NoError(s.T(), err)

			// Verify annotation was removed
			updated := &cbv1alpha1.CloudberryCluster{}
			err = s.client.Get(s.ctx, req.NamespacedName, updated)
			require.NoError(s.T(), err)
			_, hasRecovery := updated.Annotations[util.AnnotationRecovery]
			assert.False(s.T(), hasRecovery, "recovery annotation should be removed for type %s", recoveryType)
		})
	}

	s.logger.Info("recovery types E2E test completed")
}
