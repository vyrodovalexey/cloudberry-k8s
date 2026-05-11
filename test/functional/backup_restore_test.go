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

// BackupRestoreSuite tests backup and restore operations.
type BackupRestoreSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_BackupRestore(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(BackupRestoreSuite))
}

func (s *BackupRestoreSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *BackupRestoreSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *BackupRestoreSuite) TestFunctional_BackupS3Destination_ReconcilesSucessfully() {
	// Arrange: cluster with S3 backup destination.
	cluster := testutil.NewClusterBuilder("test-backup-s3", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Schedule:    "0 2 * * *",
		Compression: 6,
		Parallelism: 2,
		Destination: cbv1alpha1.BackupDestination{
			Type:     "s3",
			Bucket:   "cloudberry-backups",
			Endpoint: "http://minio:9000",
			Region:   "us-east-1",
			Path:     "/backups",
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

	// Verify backup condition was set.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	conditionFound := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "BackupConfigured" {
			conditionFound = true
			assert.Equal(s.T(), "True", string(c.Status))
			break
		}
	}
	assert.True(s.T(), conditionFound, "BackupConfigured condition should be set")
}

func (s *BackupRestoreSuite) TestFunctional_BackupIncremental_ConfigApplied() {
	// Arrange: cluster with incremental backup enabled.
	cluster := testutil.NewClusterBuilder("test-backup-incr", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Incremental: true,
		Schedule:    "0 */6 * * *",
		Destination: cbv1alpha1.BackupDestination{
			Type:   "s3",
			Bucket: "cloudberry-backups",
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

func (s *BackupRestoreSuite) TestFunctional_BackupRetentionPolicy_Applied() {
	// Arrange: cluster with retention policy.
	cluster := testutil.NewClusterBuilder("test-backup-retention", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        5,
			IncrementalCount: 20,
			MaxAge:           "90d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type:   "s3",
			Bucket: "cloudberry-backups",
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

	// Verify the cluster spec still has the retention policy.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(5), updated.Spec.Backup.Retention.FullCount)
	assert.Equal(s.T(), "90d", updated.Spec.Backup.Retention.MaxAge)
}

func (s *BackupRestoreSuite) TestFunctional_BackupDisabled_SkipsReconcile() {
	// Arrange: cluster without backup.
	cluster := testutil.NewClusterBuilder("test-backup-disabled", "default").
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

	// Verify no backup condition was set.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	for _, c := range updated.Status.Conditions {
		assert.NotEqual(s.T(), "BackupConfigured", c.Type,
			"BackupConfigured condition should not be set when backup is disabled")
	}
}

func (s *BackupRestoreSuite) TestFunctional_BackupLocalDestination_ReconcilesSucessfully() {
	// Arrange: cluster with local backup destination.
	cluster := testutil.NewClusterBuilder("test-backup-local", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Compression: 3,
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			Path: "/backups",
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
