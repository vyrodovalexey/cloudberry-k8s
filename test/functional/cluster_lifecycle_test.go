//go:build functional

package functional

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ClusterLifecycleSuite tests the cluster lifecycle operations.
type ClusterLifecycleSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ClusterLifecycleSuite))
}

func (s *ClusterLifecycleSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *ClusterLifecycleSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *ClusterLifecycleSuite) TestFunctional_CreateCluster_AddsFinalizer() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-create", "default").Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - first reconcile adds finalizer
	require.NoError(s.T(), err)
	assert.True(s.T(), result.Requeue)

	// Verify finalizer was added
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), updated.Finalizers, util.FinalizerName)
}

func (s *ClusterLifecycleSuite) TestFunctional_CreateCluster_SetsPhase() {
	// Arrange - cluster with finalizer already set
	cluster := testutil.NewClusterBuilder("test-phase", "default").
		WithFinalizer().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should set phase to Pending and requeue
	require.NoError(s.T(), err)
	assert.True(s.T(), result.Requeue)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhasePending, updated.Status.Phase)
}

func (s *ClusterLifecycleSuite) TestFunctional_CreateCluster_CreatesResources() {
	// Arrange - cluster with finalizer and Pending phase
	cluster := testutil.NewClusterBuilder("test-resources", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify ConfigMaps were created
	_, err = s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "postgresql.conf ConfigMap should be created")

	_, err = s.env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "pg_hba.conf ConfigMap should be created")

	// Verify Services were created
	_, err = s.env.GetService(s.ctx, util.CoordinatorServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "coordinator service should be created")

	_, err = s.env.GetService(s.ctx, util.ClientServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "client service should be created")

	// Verify StatefulSets were created
	_, err = s.env.GetStatefulSet(s.ctx, util.CoordinatorName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "coordinator StatefulSet should be created")

	_, err = s.env.GetStatefulSet(s.ctx, util.SegmentPrimaryName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "primary segment StatefulSet should be created")
}

func (s *ClusterLifecycleSuite) TestFunctional_ClusterWithMirroring_CreatesMirrorStatefulSet() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-mirror", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	_, err = s.env.GetStatefulSet(s.ctx, util.SegmentMirrorName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "mirror segment StatefulSet should be created")
}

func (s *ClusterLifecycleSuite) TestFunctional_ClusterWithStandby_CreatesStandbyStatefulSet() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-standby", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		WithStandby(true).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	_, err = s.env.GetStatefulSet(s.ctx, util.StandbyName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "standby StatefulSet should be created")
}

func (s *ClusterLifecycleSuite) TestFunctional_ActionStart_TriggersReconcile() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-start", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		WithAnnotation(util.AnnotationAction, util.ActionStart).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	// Verify annotation was removed
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasAction := updated.Annotations[util.AnnotationAction]
	assert.False(s.T(), hasAction, "action annotation should be removed after processing")
}

func (s *ClusterLifecycleSuite) TestFunctional_ActionStop_TriggersReconcile() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-stop", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationAction, util.ActionStop).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *ClusterLifecycleSuite) TestFunctional_ActionRestart_TriggersReconcile() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-restart", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationAction, util.ActionRestart).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *ClusterLifecycleSuite) TestFunctional_DeleteCluster_HandlesGracefully() {
	// Arrange
	now := metav1.Now()
	cluster := testutil.NewClusterBuilder("test-delete", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.DeletionTimestamp = &now
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *ClusterLifecycleSuite) TestFunctional_ClusterNotFound_ReturnsNoError() {
	// Arrange
	s.env = testutil.NewTestK8sEnv()

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	// Assert - not found should not return error
	require.NoError(s.T(), err)
	assert.False(s.T(), result.Requeue)
}

func (s *ClusterLifecycleSuite) TestFunctional_UnknownAction_IsIgnored() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-unknown-action", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithAnnotation(util.AnnotationAction, "unknown-action").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
}

func (s *ClusterLifecycleSuite) TestFunctional_ReconcileTimeout_RespectsContext() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-timeout", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Create a context with a reasonable timeout
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	// Act
	_, err := reconciler.Reconcile(ctx, s.reqFor(cluster))

	// Assert - should complete within timeout
	require.NoError(s.T(), err)
}
