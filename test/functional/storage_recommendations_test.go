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

// StorageRecommendationsSuite tests storage management and recommendations.
type StorageRecommendationsSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_StorageRecommendations(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(StorageRecommendationsSuite))
}

func (s *StorageRecommendationsSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *StorageRecommendationsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *StorageRecommendationsSuite) TestFunctional_StorageMonitoringEnabled_ReconcilesSucessfully() {
	// Arrange: cluster with storage monitoring enabled.
	cluster := testutil.NewClusterBuilder("test-storage-mon", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
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

	// Verify StorageConfigured condition is set.
	found := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "StorageConfigured" {
			found = true
			assert.Equal(s.T(), "True", string(c.Status))
			break
		}
	}
	assert.True(s.T(), found, "StorageConfigured condition should be set")
}

func (s *StorageRecommendationsSuite) TestFunctional_RecommendationScanConfigApplied() {
	// Arrange: cluster with recommendation scan configured.
	cluster := testutil.NewClusterBuilder("test-rec-scan", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       50,
			AgeThreshold:        500000000,
			IndexBloatThreshold: 30,
			ScanDuration:        "2h",
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
	assert.NotNil(s.T(), updated.Spec.Storage.RecommendationScan)
	assert.True(s.T(), updated.Spec.Storage.RecommendationScan.Enabled)
}

func (s *StorageRecommendationsSuite) TestFunctional_UsageReportEnabled() {
	// Arrange: cluster with usage report enabled.
	cluster := testutil.NewClusterBuilder("test-usage-report", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport: &cbv1alpha1.UsageReportSpec{
			Enabled: true,
			Monthly: true,
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
	assert.NotNil(s.T(), updated.Spec.Storage.UsageReport)
	assert.True(s.T(), updated.Spec.Storage.UsageReport.Enabled)
}

func (s *StorageRecommendationsSuite) TestFunctional_DisabledStorage_SkipsReconcile() {
	// Arrange: cluster without storage management.
	cluster := testutil.NewClusterBuilder("test-storage-disabled", "default").
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

	// Verify StorageConfigured condition is NOT set.
	for _, c := range updated.Status.Conditions {
		assert.NotEqual(s.T(), "StorageConfigured", c.Type,
			"StorageConfigured condition should not be set when storage is disabled")
	}
}

func (s *StorageRecommendationsSuite) TestFunctional_DiskUsageStatusReporting() {
	// Arrange: cluster with storage monitoring and pre-set disk usage.
	cluster := testutil.NewClusterBuilder("test-disk-status", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
	}
	cluster.Status.DiskUsagePercent = 65
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
	assert.Equal(s.T(), int32(65), updated.Status.DiskUsagePercent)
}
