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

// DataLoadingSuite tests data loading operations.
type DataLoadingSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_DataLoading(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DataLoadingSuite))
}

func (s *DataLoadingSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *DataLoadingSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *DataLoadingSuite) TestFunctional_DataLoadingS3Job_ReconcilesSucessfully() {
	// Arrange: cluster with a PXF s3 data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-s3", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake", Type: "s3",
					Config:            map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
					CredentialSecrets: []cbv1alpha1.SecretReference{{Name: "s3-creds"}},
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:     "s3-csv-loader",
				Type:     "pxf",
				Enabled:  true,
				Schedule: "*/30 * * * *",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:text",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
				},
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
	assert.Equal(s.T(), int32(1), updated.Status.DataLoadingJobs)
	require.NotNil(s.T(), updated.Status.DataLoading)
	assert.Equal(s.T(), "Configured", updated.Status.DataLoading.Phase)
	assert.Equal(s.T(), int32(1), updated.Status.DataLoading.ConfiguredJobs)
	assert.Equal(s.T(), int32(1), updated.Status.DataLoading.ActiveJobs)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	assert.Equal(s.T(),
		cbv1alpha1.DataLoadingJobStatus{Name: "s3-csv-loader", Enabled: true},
		updated.Status.DataLoading.Jobs[0])
}

func (s *DataLoadingSuite) TestFunctional_DataLoadingKafkaJob_ReconcilesSucessfully() {
	// Arrange: cluster with a PXF jdbc data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-jdbc", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "mysql-oltp", Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/db",
					},
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:    "jdbc-sync",
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "mysql-oltp",
					Profile:     "jdbc",
					Resource:    "production.orders",
					TargetTable: "public.stream_data",
				},
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
	assert.Equal(s.T(), int32(1), updated.Status.DataLoadingJobs)
}

func (s *DataLoadingSuite) TestFunctional_DataLoadingRabbitMQJob_ReconcilesSucessfully() {
	// Arrange: cluster with a gpload data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-gpload", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:    "csv-load",
				Type:    "gpload",
				Enabled: true,
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.queue_data",
					Format:      "csv",
					FilePaths:   []string{"/data/incoming/*.csv"},
				},
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
	assert.Equal(s.T(), int32(1), updated.Status.DataLoadingJobs)
}

func (s *DataLoadingSuite) TestFunctional_DataLoadingMixedJobs_CountsActiveCorrectly() {
	// Arrange: cluster with mixed enabled/disabled jobs.
	cluster := testutil.NewClusterBuilder("test-dl-mixed", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "active-pxf-1", Type: "pxf", Enabled: true},
			{Name: "inactive-pxf-1", Type: "pxf", Enabled: false},
			{Name: "active-gpload", Type: "gpload", Enabled: true},
			{Name: "inactive-pxf-2", Type: "pxf", Enabled: false},
			{Name: "active-pxf-2", Type: "pxf", Enabled: true},
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
	assert.Equal(s.T(), int32(3), updated.Status.DataLoadingJobs)

	// The lightweight DataLoading status reports counts, phase, and per-job state.
	require.NotNil(s.T(), updated.Status.DataLoading)
	assert.Equal(s.T(), "Configured", updated.Status.DataLoading.Phase)
	assert.Equal(s.T(), int32(5), updated.Status.DataLoading.ConfiguredJobs)
	assert.Equal(s.T(), int32(3), updated.Status.DataLoading.ActiveJobs)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 5)
	assert.Equal(s.T(),
		cbv1alpha1.DataLoadingJobStatus{Name: "active-pxf-1", Enabled: true},
		updated.Status.DataLoading.Jobs[0])
	assert.Equal(s.T(),
		cbv1alpha1.DataLoadingJobStatus{Name: "inactive-pxf-1", Enabled: false},
		updated.Status.DataLoading.Jobs[1])
}

func (s *DataLoadingSuite) TestFunctional_DataLoadingDisabled_SkipsReconcile() {
	// Arrange: cluster without data loading.
	cluster := testutil.NewClusterBuilder("test-dl-disabled", "default").
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
	assert.Equal(s.T(), int32(0), updated.Status.DataLoadingJobs)
	// Disabled/absent data loading is a no-op: lightweight status stays unset.
	assert.Nil(s.T(), updated.Status.DataLoading)
}
