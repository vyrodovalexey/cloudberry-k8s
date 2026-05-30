//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 56: Exporter Queries ConfigMap Content (E2E)
// ============================================================================
//
// This scenario verifies that the postgres-exporter custom queries ConfigMap
// contains all required query definitions for sub-scenarios 56a-56g:
//   - 56a: Connection metrics (cloudberry_connections, cloudberry_connections_max)
//   - 56b: Database statistics (cloudberry_database_stats with 13 metrics)
//   - 56c: Segment health (cloudberry_segment_status, cloudberry_segments_down)
//   - 56d: Lock metrics (cloudberry_locks)
//   - 56e: Table statistics (cloudberry_table_stats with LIMIT 100)
//   - 56f: WAL metrics (cloudberry_wal)
//   - 56g: Replication metrics (cloudberry_replication)
//
// ============================================================================

// Scenario56ExporterQueriesE2ESuite tests the exporter queries ConfigMap content
// through the builder.
type Scenario56ExporterQueriesE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario56(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario56ExporterQueriesE2ESuite))
}

func (s *Scenario56ExporterQueriesE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario56E2ECluster returns a cluster with query monitoring enabled for testing
// exporter queries ConfigMap content.
func scenario56E2ECluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	return cluster
}

// --- 56a: Connection Metrics ---

// TestE2E_Scenario56a_ConnectionMetrics verifies that queries.yaml
// contains cloudberry_connections with labels datname, usename, state,
// application_name AND cloudberry_connections_max with max_connections gauge.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56a_ConnectionMetrics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56a: Connection metrics — cloudberry_connections
	assert.Contains(s.T(), queries, "cloudberry_connections:",
		"queries.yaml should contain cloudberry_connections query definition")
	assert.Contains(s.T(), queries, "datname",
		"cloudberry_connections should have datname label")
	assert.Contains(s.T(), queries, "usename",
		"cloudberry_connections should have usename label")
	assert.Contains(s.T(), queries, "state",
		"cloudberry_connections should have state label")
	assert.Contains(s.T(), queries, "application_name",
		"cloudberry_connections should have application_name label")

	// 56a: Connection metrics — cloudberry_connections_max
	assert.Contains(s.T(), queries, "cloudberry_connections_max:",
		"queries.yaml should contain cloudberry_connections_max query definition")
	assert.Contains(s.T(), queries, "max_connections",
		"cloudberry_connections_max should have max_connections gauge")
}

// --- 56b: Database Statistics ---

// TestE2E_Scenario56b_DatabaseStatistics verifies that queries.yaml
// contains cloudberry_database_stats with all 13 metrics: numbackends,
// xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched,
// tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes,
// deadlocks.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56b_DatabaseStatistics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56b: Database statistics
	assert.Contains(s.T(), queries, "cloudberry_database_stats:",
		"queries.yaml should contain cloudberry_database_stats query definition")

	expectedMetrics := []string{
		"numbackends",
		"xact_commit",
		"xact_rollback",
		"blks_read",
		"blks_hit",
		"tup_returned",
		"tup_fetched",
		"tup_inserted",
		"tup_updated",
		"tup_deleted",
		"conflicts",
		"temp_files",
		"temp_bytes",
		"deadlocks",
	}
	for _, metric := range expectedMetrics {
		assert.Contains(s.T(), queries, metric,
			"cloudberry_database_stats should contain metric %s", metric)
	}
}

// --- 56c: Segment Health ---

// TestE2E_Scenario56c_SegmentHealth verifies that queries.yaml contains
// cloudberry_segment_status with labels hostname, role, preferred_role, status,
// mode AND cloudberry_segments_down with down_count gauge.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56c_SegmentHealth() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56c: Segment health — cloudberry_segment_status
	assert.Contains(s.T(), queries, "cloudberry_segment_status:",
		"queries.yaml should contain cloudberry_segment_status query definition")
	assert.Contains(s.T(), queries, "hostname",
		"cloudberry_segment_status should have hostname label")
	assert.Contains(s.T(), queries, "role",
		"cloudberry_segment_status should have role label")
	assert.Contains(s.T(), queries, "preferred_role",
		"cloudberry_segment_status should have preferred_role label")
	assert.Contains(s.T(), queries, "status",
		"cloudberry_segment_status should have status label")
	assert.Contains(s.T(), queries, "mode",
		"cloudberry_segment_status should have mode label")

	// 56c: Segment health — cloudberry_segments_down
	assert.Contains(s.T(), queries, "cloudberry_segments_down:",
		"queries.yaml should contain cloudberry_segments_down query definition")
	assert.Contains(s.T(), queries, "down_count",
		"cloudberry_segments_down should have down_count gauge")
}

// --- 56d: Lock Metrics ---

// TestE2E_Scenario56d_LockMetrics verifies that queries.yaml contains
// cloudberry_locks with labels mode, locktype, state (granted/waiting).
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56d_LockMetrics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56d: Lock metrics
	assert.Contains(s.T(), queries, "cloudberry_locks:",
		"queries.yaml should contain cloudberry_locks query definition")
	assert.Contains(s.T(), queries, "mode",
		"cloudberry_locks should have mode label")
	assert.Contains(s.T(), queries, "locktype",
		"cloudberry_locks should have locktype label")
	assert.Contains(s.T(), queries, "granted",
		"cloudberry_locks should reference granted state")
	assert.Contains(s.T(), queries, "waiting",
		"cloudberry_locks should reference waiting state")
}

// --- 56e: Table Statistics ---

// TestE2E_Scenario56e_TableStatistics verifies that queries.yaml contains
// cloudberry_table_stats with labels schemaname, relname and metrics seq_scan,
// seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del,
// n_live_tup, n_dead_tup. Also verifies LIMIT 100 in query.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56e_TableStatistics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56e: Table statistics
	assert.Contains(s.T(), queries, "cloudberry_table_stats:",
		"queries.yaml should contain cloudberry_table_stats query definition")

	// Labels
	assert.Contains(s.T(), queries, "schemaname",
		"cloudberry_table_stats should have schemaname label")
	assert.Contains(s.T(), queries, "relname",
		"cloudberry_table_stats should have relname label")

	// Metrics
	expectedMetrics := []string{
		"seq_scan",
		"seq_tup_read",
		"idx_scan",
		"idx_tup_fetch",
		"n_tup_ins",
		"n_tup_upd",
		"n_tup_del",
		"n_live_tup",
		"n_dead_tup",
	}
	for _, metric := range expectedMetrics {
		assert.Contains(s.T(), queries, metric,
			"cloudberry_table_stats should contain metric %s", metric)
	}

	// LIMIT 100
	assert.Contains(s.T(), queries, "LIMIT 100",
		"cloudberry_table_stats query should contain LIMIT 100")
}

// --- 56f: WAL Metrics ---

// TestE2E_Scenario56f_WALMetrics verifies that queries.yaml contains
// cloudberry_wal with wal_bytes_total counter.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56f_WALMetrics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56f: WAL metrics
	assert.Contains(s.T(), queries, "cloudberry_wal:",
		"queries.yaml should contain cloudberry_wal query definition")
	assert.Contains(s.T(), queries, "wal_bytes_total",
		"cloudberry_wal should have wal_bytes_total counter")
}

// --- 56g: Replication Metrics ---

// TestE2E_Scenario56g_ReplicationMetrics verifies that queries.yaml
// contains cloudberry_replication with labels client_addr, application_name,
// state and metrics write_lag_bytes, flush_lag_bytes, replay_lag_bytes.
func (s *Scenario56ExporterQueriesE2ESuite) TestE2E_Scenario56g_ReplicationMetrics() {
	// Arrange
	cluster := scenario56E2ECluster()
	b := builder.NewBuilder()

	// Act
	cm := b.BuildExporterQueriesConfigMap(cluster)

	// Assert
	require.NotNil(s.T(), cm, "ConfigMap should not be nil")
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(s.T(), queries, "queries.yaml should not be empty")

	// 56g: Replication metrics
	assert.Contains(s.T(), queries, "cloudberry_replication:",
		"queries.yaml should contain cloudberry_replication query definition")

	// Labels
	assert.Contains(s.T(), queries, "client_addr",
		"cloudberry_replication should have client_addr label")
	assert.Contains(s.T(), queries, "application_name",
		"cloudberry_replication should have application_name label")
	assert.Contains(s.T(), queries, "state",
		"cloudberry_replication should have state label")

	// Metrics
	assert.Contains(s.T(), queries, "write_lag_bytes",
		"cloudberry_replication should have write_lag_bytes metric")
	assert.Contains(s.T(), queries, "flush_lag_bytes",
		"cloudberry_replication should have flush_lag_bytes metric")
	assert.Contains(s.T(), queries, "replay_lag_bytes",
		"cloudberry_replication should have replay_lag_bytes metric")
}
