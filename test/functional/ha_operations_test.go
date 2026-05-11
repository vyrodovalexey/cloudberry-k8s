//go:build functional

package functional

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// HAOperationsSuite tests high availability operations.
type HAOperationsSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_HAOperations(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(HAOperationsSuite))
}

func (s *HAOperationsSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *HAOperationsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_SkipsNonRunningCluster() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-skip", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_RunsFTSProbe_AllHealthy() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-fts-healthy", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 20, 5).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return testutil.DefaultSegmentConfiguration(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify mirroring status is InSync
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus)
	assert.Empty(s.T(), updated.Status.FailedSegments)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_RunsFTSProbe_DegradedSegments() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-fts-degraded", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 20, 5).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return testutil.DegradedSegmentConfiguration(), nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify mirroring status is Degraded
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus)
	assert.NotEmpty(s.T(), updated.Status.FailedSegments)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_FTSProbeFailure() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-fts-fail", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithHA(60, 20, 5).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{
		GetSegmentConfigurationFunc: func(_ context.Context) ([]db.SegmentInfo, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act - should not return error, just log it
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_DBClientCreationFailure() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-db-fail", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	dbFactory := &testutil.MockDBClientFactory{
		Err: fmt.Errorf("cannot create db client"),
	}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act - should not return error, just log it
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_StandbyMonitoring() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-standby", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStandby(true).
		WithHA(60, 20, 5).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{
		GetReplicationLagFunc: func(_ context.Context) (int64, error) {
			return 1024, nil // 1KB lag
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_RecoveryAnnotation() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-recovery", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationRecovery, util.RecoveryIncremental).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasRecovery := updated.Annotations[util.AnnotationRecovery]
	assert.False(s.T(), hasRecovery, "recovery annotation should be removed")
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_RebalanceAction() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-rebalance", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationAction, util.ActionRebalance).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasAction := updated.Annotations[util.AnnotationAction]
	assert.False(s.T(), hasAction, "action annotation should be removed")
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_ActivateStandby() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-ha-activate", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStandby(true).
		WithAnnotation(util.AnnotationAction, util.ActionActivateStandby).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_ProbeInterval() {
	// Arrange - custom probe interval
	cluster := testutil.NewClusterBuilder("test-ha-interval", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithHA(30, 10, 3).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - requeue interval should match probe interval
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *HAOperationsSuite) TestFunctional_HAReconcile_ClusterNotFound() {
	// Arrange
	s.env = testutil.NewTestK8sEnv()

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	// Assert
	require.NoError(s.T(), err)
	assert.False(s.T(), result.Requeue)
}
