//go:build functional

package functional

import (
	"context"
	"testing"

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

// AuthConfigSuite tests authentication configuration reconciliation.
type AuthConfigSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_AuthConfig(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AuthConfigSuite))
}

func (s *AuthConfigSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *AuthConfigSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_CreatesHBAConfigMap() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-hba", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(testutil.DefaultHBARules()).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify HBA ConfigMap was created
	cm, err := s.env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["pg_hba.conf"], "gpadmin")
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_CustomHBARules() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-custom-hba", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(testutil.CustomHBARules()).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)

	cm, err := s.env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["pg_hba.conf"], "hostssl")
	assert.Contains(s.T(), cm.Data["pg_hba.conf"], "mydb")
	assert.Contains(s.T(), cm.Data["pg_hba.conf"], "appuser")
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_SkipsNonRunningCluster() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-skip", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhasePending).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should skip and requeue
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_InitializingCluster() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-init", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseInitializing).
		WithPendingGeneration().
		WithHBARules(testutil.DefaultHBARules()).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should process initializing clusters
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_OIDCValidation_Valid() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-oidc-valid", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithOIDC(true, "http://keycloak:8090/realms/test", "cloudberry-client").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify auth condition is set
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	cond := util.FindCondition(updated.Status.Conditions, string(cbv1alpha1.ConditionAuthConfigured))
	require.NotNil(s.T(), cond)
	assert.Equal(s.T(), metav1.ConditionTrue, cond.Status)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_OIDCValidation_MissingIssuer() {
	// Arrange - OIDC enabled but missing issuer URL
	cluster := testutil.NewClusterBuilder("test-auth-oidc-no-issuer", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithOIDC(true, "", "cloudberry-client").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act - should still succeed (OIDC validation is a warning, not a failure)
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_OIDCValidation_MissingClientID() {
	// Arrange - OIDC enabled but missing client ID
	cluster := testutil.NewClusterBuilder("test-auth-oidc-no-client", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithOIDC(true, "http://keycloak:8090/realms/test", "").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_OIDCDisabled() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-auth-oidc-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithOIDC(false, "", "").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert - should succeed without OIDC validation
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_UpdatesHBAConfigMap() {
	// Arrange - create initial HBA config with pending generation to trigger reconciliation
	cluster := testutil.NewClusterBuilder("test-auth-update-hba", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(testutil.DefaultHBARules()).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// First reconcile - creates the HBA ConfigMap
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Update HBA rules and bump generation to trigger re-reconciliation
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Auth.HBARules = testutil.CustomHBARules()
	updated.Generation = updated.Status.ObservedGeneration + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	// Act - second reconcile should update
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Verify the ConfigMap was updated
	cm, err := s.env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["pg_hba.conf"], "hostssl")
}

func (s *AuthConfigSuite) TestFunctional_AuthReconcile_ClusterNotFound() {
	// Arrange
	s.env = testutil.NewTestK8sEnv()

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	// Assert
	require.NoError(s.T(), err)
	assert.False(s.T(), result.Requeue)
}
