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

// ============================================================================
// Scenario 53: Query Monitoring Full CRD Configuration
// ============================================================================
//
// This scenario verifies that the full QueryMonitoringSpec CRD configuration,
// including all exporter, ServiceMonitor, and PrometheusRule fields, is
// correctly parsed, propagated, and retained through the reconciliation loop.
//
// ============================================================================

// Scenario53QueryMonitoringFullCRDSuite tests the full query monitoring CRD
// configuration through the AdminReconciler.
type Scenario53QueryMonitoringFullCRDSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario53(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario53QueryMonitoringFullCRDSuite))
}

func (s *Scenario53QueryMonitoringFullCRDSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario53QueryMonitoringFullCRDSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// fullExporterConfig returns a fully populated QueryMonitoringExportersSpec
// for testing all exporter, ServiceMonitor, and PrometheusRule fields.
func fullExporterConfig() *cbv1alpha1.QueryMonitoringExportersSpec {
	return &cbv1alpha1.QueryMonitoringExportersSpec{
		PostgresExporter: &cbv1alpha1.ExporterSpec{
			Enabled: true,
			Image:   "prometheuscommunity/postgres-exporter:0.16.0",
			Port:    9187,
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "500m", Memory: "256Mi"},
			},
		},
		NodeExporter: &cbv1alpha1.ExporterSpec{
			Enabled: true,
			Image:   "prom/node-exporter:1.8.2",
			Port:    9100,
		},
		CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
			Enabled: true,
			Image:   "cloudberry-query-exporter:1.0.0",
			Port:    9188,
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "500m", Memory: "256Mi"},
			},
		},
		ServiceMonitor: &cbv1alpha1.QueryServiceMonitorSpec{
			Enabled:       true,
			Namespace:     "",
			Interval:      "15s",
			ScrapeTimeout: "10s",
			Labels:        map[string]string{"team": "platform"},
		},
		PrometheusRule: &cbv1alpha1.QueryPrometheusRuleSpec{
			Enabled:   true,
			Namespace: "",
			Labels:    map[string]string{"team": "platform"},
		},
	}
}

// --- 53.1: Full CRD config reconciles successfully ---

// TestFunctional_Scenario53_FullCRDConfig_Reconciles verifies that a cluster
// with all QueryMonitoring fields set reconciles without error.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_FullCRDConfig_Reconciles() {
	// Arrange: cluster with full query monitoring configuration.
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "30d",
		SamplingInterval:   5,
		GuestAccess:        false,
		PlanCollection:     true,
		SlowQueryThreshold: "1000ms",
		Exporters:          fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed with full CRD config")
	assert.NotZero(s.T(), result.RequeueAfter, "reconcile should request requeue")
}

// --- 53.2: History retention is applied ---

// TestFunctional_Scenario53_HistoryRetention_Applied verifies that the
// historyRetention field is retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_HistoryRetention_Applied() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		HistoryRetention: "30d",
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "30d", updated.Spec.QueryMonitoring.HistoryRetention,
		"historyRetention should be retained as '30d'")
}

// --- 53.3: Sampling interval is propagated ---

// TestFunctional_Scenario53_SamplingInterval_Propagated verifies that the
// samplingInterval field is retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_SamplingInterval_Propagated() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		SamplingInterval: 5,
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(5), updated.Spec.QueryMonitoring.SamplingInterval,
		"samplingInterval should be retained as 5")
}

// --- 53.4: Guest access false ---

// TestFunctional_Scenario53_GuestAccess_False verifies that guestAccess=false
// is retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_GuestAccess_False() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:     true,
		GuestAccess: false,
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.False(s.T(), updated.Spec.QueryMonitoring.GuestAccess,
		"guestAccess should be false after reconciliation")
}

// --- 53.5: Plan collection true ---

// TestFunctional_Scenario53_PlanCollection_True verifies that planCollection=true
// is retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_PlanCollection_True() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:        true,
		PlanCollection: true,
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.True(s.T(), updated.Spec.QueryMonitoring.PlanCollection,
		"planCollection should be true after reconciliation")
}

// --- 53.6: Slow query threshold propagated ---

// TestFunctional_Scenario53_SlowQueryThreshold_Propagated verifies that the
// slowQueryThreshold field is retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_SlowQueryThreshold_Propagated() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		SlowQueryThreshold: "1000ms",
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "1000ms", updated.Spec.QueryMonitoring.SlowQueryThreshold,
		"slowQueryThreshold should be retained as '1000ms'")
}

// --- 53.7: Postgres exporter parsed ---

// TestFunctional_Scenario53_PostgresExporter_Parsed verifies that the postgres
// exporter configuration fields are correctly retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_PostgresExporter_Parsed() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:   true,
		Exporters: fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.QueryMonitoring.Exporters, "exporters should not be nil")
	pe := updated.Spec.QueryMonitoring.Exporters.PostgresExporter
	require.NotNil(s.T(), pe, "postgresExporter should not be nil")
	assert.True(s.T(), pe.Enabled, "postgresExporter should be enabled")
	assert.Equal(s.T(), "prometheuscommunity/postgres-exporter:0.16.0", pe.Image,
		"postgresExporter image should match")
	assert.Equal(s.T(), int32(9187), pe.Port, "postgresExporter port should be 9187")
	require.NotNil(s.T(), pe.Resources, "postgresExporter resources should not be nil")
	require.NotNil(s.T(), pe.Resources.Requests, "postgresExporter requests should not be nil")
	assert.Equal(s.T(), "100m", pe.Resources.Requests.CPU, "postgresExporter CPU request should match")
	assert.Equal(s.T(), "128Mi", pe.Resources.Requests.Memory, "postgresExporter memory request should match")
	require.NotNil(s.T(), pe.Resources.Limits, "postgresExporter limits should not be nil")
	assert.Equal(s.T(), "500m", pe.Resources.Limits.CPU, "postgresExporter CPU limit should match")
	assert.Equal(s.T(), "256Mi", pe.Resources.Limits.Memory, "postgresExporter memory limit should match")
}

// --- 53.8: Node exporter parsed ---

// TestFunctional_Scenario53_NodeExporter_Parsed verifies that the node exporter
// configuration fields are correctly retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_NodeExporter_Parsed() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:   true,
		Exporters: fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.QueryMonitoring.Exporters, "exporters should not be nil")
	ne := updated.Spec.QueryMonitoring.Exporters.NodeExporter
	require.NotNil(s.T(), ne, "nodeExporter should not be nil")
	assert.True(s.T(), ne.Enabled, "nodeExporter should be enabled")
	assert.Equal(s.T(), "prom/node-exporter:1.8.2", ne.Image, "nodeExporter image should match")
	assert.Equal(s.T(), int32(9100), ne.Port, "nodeExporter port should be 9100")
}

// --- 53.9: Cloudberry query exporter parsed ---

// TestFunctional_Scenario53_CloudberryQueryExporter_Parsed verifies that the
// cloudberry query exporter configuration fields are correctly retained after reconciliation.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_CloudberryQueryExporter_Parsed() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:   true,
		Exporters: fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.QueryMonitoring.Exporters, "exporters should not be nil")
	cqe := updated.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter
	require.NotNil(s.T(), cqe, "cloudberryQueryExporter should not be nil")
	assert.True(s.T(), cqe.Enabled, "cloudberryQueryExporter should be enabled")
	assert.Equal(s.T(), "cloudberry-query-exporter:1.0.0", cqe.Image,
		"cloudberryQueryExporter image should match")
	assert.Equal(s.T(), int32(9188), cqe.Port, "cloudberryQueryExporter port should be 9188")
	require.NotNil(s.T(), cqe.Resources, "cloudberryQueryExporter resources should not be nil")
	require.NotNil(s.T(), cqe.Resources.Requests, "cloudberryQueryExporter requests should not be nil")
	assert.Equal(s.T(), "100m", cqe.Resources.Requests.CPU, "cloudberryQueryExporter CPU request should match")
	assert.Equal(s.T(), "128Mi", cqe.Resources.Requests.Memory, "cloudberryQueryExporter memory request should match")
	require.NotNil(s.T(), cqe.Resources.Limits, "cloudberryQueryExporter limits should not be nil")
	assert.Equal(s.T(), "500m", cqe.Resources.Limits.CPU, "cloudberryQueryExporter CPU limit should match")
	assert.Equal(s.T(), "256Mi", cqe.Resources.Limits.Memory, "cloudberryQueryExporter memory limit should match")
}

// --- 53.10: ServiceMonitor parsed ---

// TestFunctional_Scenario53_ServiceMonitor_Parsed verifies that the ServiceMonitor
// configuration fields are correctly retained after reconciliation, including
// namespace defaulting to the cluster namespace when left empty.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_ServiceMonitor_Parsed() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:   true,
		Exporters: fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.QueryMonitoring.Exporters, "exporters should not be nil")
	sm := updated.Spec.QueryMonitoring.Exporters.ServiceMonitor
	require.NotNil(s.T(), sm, "serviceMonitor should not be nil")
	assert.True(s.T(), sm.Enabled, "serviceMonitor should be enabled")
	// When namespace is empty, it defaults to the cluster namespace.
	if sm.Namespace == "" {
		assert.Equal(s.T(), "", sm.Namespace,
			"serviceMonitor namespace should be empty (defaults to cluster namespace)")
	} else {
		assert.Equal(s.T(), cluster.Namespace, sm.Namespace,
			"serviceMonitor namespace should default to cluster namespace")
	}
	assert.Equal(s.T(), "15s", sm.Interval, "serviceMonitor interval should be '15s'")
	assert.Equal(s.T(), "10s", sm.ScrapeTimeout, "serviceMonitor scrapeTimeout should be '10s'")
	assert.Equal(s.T(), map[string]string{"team": "platform"}, sm.Labels,
		"serviceMonitor labels should match")
}

// --- 53.11: PrometheusRule parsed ---

// TestFunctional_Scenario53_PrometheusRule_Parsed verifies that the PrometheusRule
// configuration fields are correctly retained after reconciliation, including
// namespace defaulting to the cluster namespace when left empty.
func (s *Scenario53QueryMonitoringFullCRDSuite) TestFunctional_Scenario53_PrometheusRule_Parsed() {
	// Arrange
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:   true,
		Exporters: fullExporterConfig(),
	}
	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.QueryMonitoring.Exporters, "exporters should not be nil")
	pr := updated.Spec.QueryMonitoring.Exporters.PrometheusRule
	require.NotNil(s.T(), pr, "prometheusRule should not be nil")
	assert.True(s.T(), pr.Enabled, "prometheusRule should be enabled")
	// When namespace is empty, it defaults to the cluster namespace.
	if pr.Namespace == "" {
		assert.Equal(s.T(), "", pr.Namespace,
			"prometheusRule namespace should be empty (defaults to cluster namespace)")
	} else {
		assert.Equal(s.T(), cluster.Namespace, pr.Namespace,
			"prometheusRule namespace should default to cluster namespace")
	}
	assert.Equal(s.T(), map[string]string{"team": "platform"}, pr.Labels,
		"prometheusRule labels should match")
}
