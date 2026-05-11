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
	// Arrange: cluster with S3 data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-s3", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:        "s3-csv-loader",
				Type:        "s3",
				Enabled:     true,
				Schedule:    "*/30 * * * *",
				TargetTable: "public.events",
				S3Source: &cbv1alpha1.S3SourceSpec{
					Bucket:   "data-lake",
					Path:     "/events/",
					Endpoint: "http://minio:9000",
					Format:   "csv",
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

func (s *DataLoadingSuite) TestFunctional_DataLoadingKafkaJob_ReconcilesSucessfully() {
	// Arrange: cluster with Kafka data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-kafka", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:        "kafka-consumer",
				Type:        "kafka",
				Enabled:     true,
				TargetTable: "public.stream_data",
				KafkaSource: &cbv1alpha1.KafkaSourceSpec{
					Brokers:     []string{"kafka:9092"},
					Topic:       "cloudberry-data",
					GroupID:     "cloudberry-loader",
					Format:      "json",
					StartOffset: "earliest",
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
	// Arrange: cluster with RabbitMQ data loading job.
	cluster := testutil.NewClusterBuilder("test-dl-rmq", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:        "rabbitmq-consumer",
				Type:        "rabbitmq",
				Enabled:     true,
				TargetTable: "public.queue_data",
				RabbitMQSource: &cbv1alpha1.RabbitMQSourceSpec{
					Host:  "rabbitmq",
					Port:  5672,
					VHost: "cloudberry",
					Queue: "data-queue",
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
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		StreamingServer: &cbv1alpha1.StreamingServerSpec{
			Host: "streaming.example.com",
			Port: 5432,
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "active-s3", Type: "s3", Enabled: true, TargetTable: "public.t1"},
			{Name: "inactive-kafka", Type: "kafka", Enabled: false, TargetTable: "public.t2"},
			{Name: "active-rmq", Type: "rabbitmq", Enabled: true, TargetTable: "public.t3"},
			{Name: "inactive-s3", Type: "s3", Enabled: false, TargetTable: "public.t4"},
			{Name: "active-kafka", Type: "kafka", Enabled: true, TargetTable: "public.t5"},
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
}
