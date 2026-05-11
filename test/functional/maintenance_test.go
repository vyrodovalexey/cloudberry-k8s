//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// MaintenanceSuite tests maintenance operations.
type MaintenanceSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Maintenance(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(MaintenanceSuite))
}

func (s *MaintenanceSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *MaintenanceSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_VacuumTrigger() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-vacuum", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceVacuum).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify annotation was removed
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasMaintenance := updated.Annotations[util.AnnotationMaintenance]
	assert.False(s.T(), hasMaintenance, "maintenance annotation should be removed after processing")
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_VacuumAnalyzeTrigger() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-vacuum-analyze", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceVacuumAnalyze).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_VacuumFullTrigger() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-vacuum-full", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceVacuumFull).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_AnalyzeTrigger() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-analyze", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceAnalyze).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_ReindexTrigger() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-reindex", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceReindex).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *MaintenanceSuite) TestFunctional_Maintenance_SkipsNonRunningCluster() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-maint-skip", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseInitializing).
		WithAnnotation(util.AnnotationMaintenance, util.MaintenanceVacuum).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should skip non-running cluster
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}
