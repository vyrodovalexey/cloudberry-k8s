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

// ConfigManagementSuite tests configuration management operations.
type ConfigManagementSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_ConfigManagement(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ConfigManagementSuite))
}

func (s *ConfigManagementSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *ConfigManagementSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *ConfigManagementSuite) TestFunctional_ConfigReconcile_AppliesParameters() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-config", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithConfig(map[string]string{
			"shared_buffers":  "256MB",
			"work_mem":        "64MB",
			"max_connections": "200",
		}).
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

	// Verify ConfigMap was created with the parameters
	cm, err := s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "shared_buffers")
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "work_mem")
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "max_connections")
}

func (s *ConfigManagementSuite) TestFunctional_ConfigReconcile_DetectsChanges() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-config-change", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithConfig(map[string]string{
			"shared_buffers": "128MB",
		}).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act - first reconcile
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Update config
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["shared_buffers"] = "256MB"
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	// Act - second reconcile should detect change
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Verify the ConfigMap was updated
	cm, err := s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "256MB")
}

func (s *ConfigManagementSuite) TestFunctional_ConfigReconcile_SkipsNonRunningCluster() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-config-skip", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		WithConfig(map[string]string{
			"shared_buffers": "128MB",
		}).
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

	// Assert - should skip and requeue
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *ConfigManagementSuite) TestFunctional_ConfigReconcile_NilConfig() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-config-nil", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	// Explicitly set Config to nil
	cluster.Spec.Config = nil
	s.env = testutil.NewTestK8sEnv(cluster)

	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should succeed with no config to apply
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *ConfigManagementSuite) TestFunctional_ConfigReconcile_EmptyParameters() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-config-empty", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithConfig(map[string]string{}).
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

func (s *ConfigManagementSuite) TestFunctional_MaintenanceAnnotation_Vacuum() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-vacuum", "default").
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
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasMaintenance := updated.Annotations[util.AnnotationMaintenance]
	assert.False(s.T(), hasMaintenance, "maintenance annotation should be removed")
}

func (s *ConfigManagementSuite) TestFunctional_MaintenanceAnnotation_Analyze() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-analyze", "default").
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
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *ConfigManagementSuite) TestFunctional_MaintenanceAnnotation_Reindex() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-reindex", "default").
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
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}
