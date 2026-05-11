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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// WorkloadManagementSuite tests workload management operations.
type WorkloadManagementSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_WorkloadManagement(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkloadManagementSuite))
}

func (s *WorkloadManagementSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *WorkloadManagementSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *WorkloadManagementSuite) TestFunctional_ResourceGroupCreation_ReconcilesSucessfully() {
	// Arrange: cluster with resource groups.
	cluster := testutil.NewClusterBuilder("test-wl-rg", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name:          "analytics",
				Concurrency:   10,
				CPUMaxPercent: 50,
				CPUWeight:     100,
				MemoryLimit:   30,
				MinCost:       500,
			},
			{
				Name:          "etl",
				Concurrency:   5,
				CPUMaxPercent: 30,
				CPUWeight:     50,
				MemoryLimit:   20,
			},
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}

func (s *WorkloadManagementSuite) TestFunctional_WorkloadRuleCancelAction_Applied() {
	// Arrange: cluster with cancel rule.
	cluster := testutil.NewClusterBuilder("test-wl-cancel", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long-queries",
				Enabled:       true,
				ResourceGroup: "analytics",
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "3600",
				Priority:      1,
			},
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *WorkloadManagementSuite) TestFunctional_WorkloadRuleMoveAction_Applied() {
	// Arrange: cluster with move rule.
	cluster := testutil.NewClusterBuilder("test-wl-move", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics"},
			{Name: "etl"},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "move-heavy-queries",
				Enabled:       true,
				QueryTag:      "heavy",
				Action:        "move",
				MoveTarget:    "etl",
				ThresholdType: "spill_size",
				Threshold:     "1073741824",
				Priority:      2,
			},
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *WorkloadManagementSuite) TestFunctional_WorkloadRuleLogAction_Applied() {
	// Arrange: cluster with log rule.
	cluster := testutil.NewClusterBuilder("test-wl-log", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "log-cpu-skew",
				Enabled:       true,
				Action:        "log",
				ThresholdType: "cpu_skew",
				Threshold:     "2.0",
				Priority:      3,
			},
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)
}

func (s *WorkloadManagementSuite) TestFunctional_IdleSessionRules_Applied() {
	// Arrange: cluster with idle session rules.
	cluster := testutil.NewClusterBuilder("test-wl-idle", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics"},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{
				Name:                 "terminate-idle-analytics",
				Enabled:              true,
				ResourceGroup:        "analytics",
				IdleTimeout:          "30m",
				ExcludeInTransaction: true,
				TerminateMessage:     "Session terminated due to inactivity",
			},
		},
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			conditionFound = true
			break
		}
	}
	assert.True(s.T(), conditionFound, "WorkloadConfigured condition should be set")
}

func (s *WorkloadManagementSuite) TestFunctional_WorkloadDisabled_SkipsReconcile() {
	// Arrange: cluster without workload management.
	cluster := testutil.NewClusterBuilder("test-wl-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	for _, c := range updated.Status.Conditions {
		assert.NotEqual(s.T(), "WorkloadConfigured", c.Type,
			"WorkloadConfigured condition should not be set when workload is disabled")
	}
}
