//go:build functional

package functional

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 57: Query Exporter Metrics
// ============================================================================
//
// This scenario verifies that the cloudberry-query-exporter sidecar container
// is properly configured and that the collectors.go file exists with the
// expected metric definitions for sub-scenarios 57a-57g:
//   - 57a: Query activity metrics (sidecar container args, env, port)
//   - 57b: Resource group collector (resgroup_running_queries, resgroup_queued_queries, etc.)
//   - 57c: Resource group I/O collector (resgroup_io_read_bytes_per_sec, etc.)
//   - 57d: Spill file collector (spill_files_active, spill_files_bytes, etc.)
//   - 57e: Segment health collector (segments_total, segments_up, segments_down, etc.)
//   - 57f: Distributed transaction collector (distributed_transactions_active, etc.)
//   - 57g: Skew collector (table_skew_coefficient, gp_skew_coefficients)
//
// Note: Prometheus metric names in collectors.go use the Namespace field
// ("cloudberry") separately from the Name field. The full metric name
// (e.g. "cloudberry_resgroup_running_queries") is constructed at runtime.
// Tests verify the Name field values as they appear in the source code.
//
// ============================================================================

// Scenario57QueryExporterMetricsSuite tests the cloudberry-query-exporter
// sidecar container configuration and collectors.go metric definitions.
type Scenario57QueryExporterMetricsSuite struct {
	suite.Suite
}

func TestFunctional_Scenario57(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario57QueryExporterMetricsSuite))
}

// scenario57Cluster returns a cluster with query monitoring and cloudberry-query-exporter
// enabled for testing sidecar container configuration.
func scenario57Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		SamplingInterval:   5,
		SlowQueryThreshold: "1000ms",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
				Port:    9188,
			},
		},
	}
	return cluster
}

// --- 57a: Query Activity Metrics ---

// TestFunctional_Scenario57a_QueryActivityMetrics verifies that the
// cloudberry-query-exporter sidecar container is properly configured with
// the correct args (listen-address, sampling-interval, slow-query-threshold),
// environment variable (DATA_SOURCE_NAME), and container name.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57a_QueryActivityMetrics() {
	cluster := scenario57Cluster()
	b := builder.NewBuilder()
	containers := b.BuildExporterSidecarContainers(cluster)

	var found bool
	for _, c := range containers {
		if c.Name == "cloudberry-query-exporter" {
			found = true
			assert.Contains(s.T(), c.Args, "--listen-address=:9188")
			assert.Contains(s.T(), c.Args, "--sampling-interval=5s")
			assert.Contains(s.T(), c.Args, "--slow-query-threshold=1000ms")
			require.Len(s.T(), c.Env, 1)
			assert.Equal(s.T(), "DATA_SOURCE_NAME", c.Env[0].Name)
		}
	}
	assert.True(s.T(), found, "cloudberry-query-exporter container should exist")
}

// --- 57b: Resource Group Collector ---

// TestFunctional_Scenario57b_ResourceGroupCollector verifies that collectors.go
// contains resource group metric definitions: resgroup_running_queries,
// resgroup_queued_queries, resgroup_cpu_usage_percent,
// resgroup_memory_usage_bytes (Namespace "cloudberry" is set separately),
// and the gp_toolkit.gp_resgroup_status SQL query.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57b_ResourceGroupCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	assert.Contains(s.T(), src, "resgroup_running_queries",
		"collectors.go should define resgroup_running_queries metric")
	assert.Contains(s.T(), src, "resgroup_queued_queries",
		"collectors.go should define resgroup_queued_queries metric")
	assert.Contains(s.T(), src, "resgroup_cpu_usage_percent",
		"collectors.go should define resgroup_cpu_usage_percent metric")
	assert.Contains(s.T(), src, "resgroup_memory_usage_bytes",
		"collectors.go should define resgroup_memory_usage_bytes metric")
	assert.Contains(s.T(), src, "gp_toolkit.gp_resgroup_status",
		"collectors.go should query gp_toolkit.gp_resgroup_status")
}

// --- 57c: Resource Group I/O Collector ---

// TestFunctional_Scenario57c_ResourceGroupIOCollector verifies that collectors.go
// contains resource group I/O metric definitions: resgroup_io_read_bytes_per_sec,
// resgroup_io_write_bytes_per_sec, and the gp_toolkit.gp_resgroup_iostats_per_host query.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57c_ResourceGroupIOCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	assert.Contains(s.T(), src, "resgroup_io_read_bytes_per_sec",
		"collectors.go should define resgroup_io_read_bytes_per_sec metric")
	assert.Contains(s.T(), src, "resgroup_io_write_bytes_per_sec",
		"collectors.go should define resgroup_io_write_bytes_per_sec metric")
	assert.Contains(s.T(), src, "gp_toolkit.gp_resgroup_iostats_per_host",
		"collectors.go should query gp_toolkit.gp_resgroup_iostats_per_host")
}

// --- 57d: Spill File Collector ---

// TestFunctional_Scenario57d_SpillFileCollector verifies that collectors.go
// contains spill file metric definitions: spill_files_active,
// spill_files_bytes, and the gp_toolkit.gp_workfile_usage_per_query query.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57d_SpillFileCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	assert.Contains(s.T(), src, "spill_files_active",
		"collectors.go should define spill_files_active metric")
	assert.Contains(s.T(), src, "spill_files_bytes",
		"collectors.go should define spill_files_bytes metric")
	assert.Contains(s.T(), src, "gp_toolkit.gp_workfile_usage_per_query",
		"collectors.go should query gp_toolkit.gp_workfile_usage_per_query")
}

// --- 57e: Segment Health Collector ---

// TestFunctional_Scenario57e_SegmentHealthCollector verifies that collectors.go
// contains segment health metric definitions: segments_total, segments_up,
// segments_down, segments_not_synced, segments_not_preferred_role, and
// cluster_uptime_seconds (Namespace "cloudberry" is set separately).
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57e_SegmentHealthCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	for _, metric := range []string{
		"segments_total", "segments_up",
		"segments_down", "segments_not_synced",
		"segments_not_preferred_role", "cluster_uptime_seconds",
	} {
		assert.True(s.T(), strings.Contains(src, metric), "missing metric: %s", metric)
	}
}

// --- 57f: Distributed Transaction Collector ---

// TestFunctional_Scenario57f_DistributedTransactionCollector verifies that collectors.go
// contains distributed transaction metric definitions: distributed_transactions_active,
// distributed_transactions_committed_total, distributed_transactions_aborted_total,
// oldest_transaction_age_seconds, and the gp_distributed_xacts query.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57f_DistributedTransactionCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	assert.Contains(s.T(), src, "distributed_transactions_active",
		"collectors.go should define distributed_transactions_active metric")
	assert.Contains(s.T(), src, "distributed_transactions_committed_total",
		"collectors.go should define distributed_transactions_committed_total metric")
	assert.Contains(s.T(), src, "distributed_transactions_aborted_total",
		"collectors.go should define distributed_transactions_aborted_total metric")
	assert.Contains(s.T(), src, "oldest_transaction_age_seconds",
		"collectors.go should define oldest_transaction_age_seconds metric")
	assert.Contains(s.T(), src, "gp_distributed_xacts",
		"collectors.go should query gp_distributed_xacts")
}

// --- 57g: Skew Collector ---

// TestFunctional_Scenario57g_SkewCollector verifies that collectors.go
// contains data distribution skew metric definitions: table_skew_coefficient,
// and the gp_toolkit.gp_skew_coefficients query.
func (s *Scenario57QueryExporterMetricsSuite) TestFunctional_Scenario57g_SkewCollector() {
	content, err := os.ReadFile("../../cmd/cloudberry-query-exporter/collectors.go")
	require.NoError(s.T(), err)
	src := string(content)
	assert.Contains(s.T(), src, "table_skew_coefficient",
		"collectors.go should define table_skew_coefficient metric")
	assert.Contains(s.T(), src, "gp_toolkit.gp_skew_coefficients",
		"collectors.go should query gp_toolkit.gp_skew_coefficients")
}
