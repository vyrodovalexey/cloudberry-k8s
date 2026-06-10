package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordAPIRequest verifies counter + histogram recording with the route
// template label.
func TestRecordAPIRequest(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordAPIRequest("/api/v1alpha1/clusters/{name}", "GET", "200", 50*time.Millisecond)
	r.RecordAPIRequest("/api/v1alpha1/clusters/{name}", "GET", "200", 70*time.Millisecond)
	r.RecordAPIRequest("/api/v1alpha1/clusters/{name}", "GET", "404", 10*time.Millisecond)

	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_api_requests_total", map[string]string{
		"route": "/api/v1alpha1/clusters/{name}", "method": "GET", "code": "200",
	}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_api_requests_total", map[string]string{
		"route": "/api/v1alpha1/clusters/{name}", "method": "GET", "code": "404",
	}))
	// Histogram observed 3 samples for the route/method pair.
	assert.Equal(t, 3.0, valueWithLabels(t, reg, "cloudberry_api_request_duration_seconds",
		map[string]string{"route": "/api/v1alpha1/clusters/{name}", "method": "GET"}))
}

func TestAPIRequestsInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.AddAPIRequestsInFlight(1)
	r.AddAPIRequestsInFlight(1)
	assert.Equal(t, 2.0, testutil.ToFloat64(r.apiRequestsInFlight))
	r.AddAPIRequestsInFlight(-1)
	r.AddAPIRequestsInFlight(-1)
	assert.Equal(t, 0.0, testutil.ToFloat64(r.apiRequestsInFlight))
}

func TestRecordRateLimitRejection(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordRateLimitRejection("/api/v1alpha1/clusters")
	r.RecordRateLimitRejection("/api/v1alpha1/clusters")

	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_api_rate_limit_rejections_total",
		map[string]string{"route": "/api/v1alpha1/clusters"}))
}

func TestRecordDBConnect(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordDBConnect("c1", "ns1", "success", 100*time.Millisecond)
	r.RecordDBConnect("c1", "ns1", "error", 5*time.Second)

	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_db_connect_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "success"}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_db_connect_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "error"}))
	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_db_connect_duration_seconds",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
}

func TestObserveDBQueryDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.ObserveDBQueryDuration("RebalanceTable", time.Second)
	r.ObserveDBQueryDuration("RebalanceTable", 2*time.Second)
	r.ObserveDBQueryDuration("GetQueryHistory", 10*time.Millisecond)

	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_db_query_duration_seconds",
		map[string]string{"operation": "RebalanceTable"}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_db_query_duration_seconds",
		map[string]string{"operation": "GetQueryHistory"}))
}

// TestRegisterDBPoolStats verifies pool stats are sampled on scrape, that two
// clients coexist with distinct labels and that unregistering removes the
// series without a duplicate-registration panic.
func TestRegisterDBPoolStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	unreg1 := r.RegisterDBPoolStats("c1", "ns1", func() (float64, float64, float64) {
		return 3, 2, 10
	})
	unreg2 := r.RegisterDBPoolStats("c2", "ns2", func() (float64, float64, float64) {
		return 1, 0, 5
	})

	assert.Equal(t, 3.0, valueWithLabels(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_db_pool_idle_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
	assert.Equal(t, 10.0, valueWithLabels(t, reg, "cloudberry_db_pool_max_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
	assert.Equal(t, 5.0, valueWithLabels(t, reg, "cloudberry_db_pool_max_conns",
		map[string]string{"cluster": "c2", "namespace": "ns2"}))

	unreg1()
	unreg1() // double-unregister is safe
	assert.False(t, metricExists(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
	assert.True(t, metricExists(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c2", "namespace": "ns2"}))
	unreg2()
}

// TestRegisterDBPoolStatsStaleUnregister verifies that a superseded client's
// late unregister does not remove the replacement's provider.
func TestRegisterDBPoolStatsStaleUnregister(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	unregOld := r.RegisterDBPoolStats("c1", "ns1", func() (float64, float64, float64) {
		return 1, 1, 1
	})
	unregNew := r.RegisterDBPoolStats("c1", "ns1", func() (float64, float64, float64) {
		return 7, 7, 7
	})

	// The replacement provider is live.
	assert.Equal(t, 7.0, valueWithLabels(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))

	// Stale unregister from the old client must be a no-op.
	unregOld()
	assert.Equal(t, 7.0, valueWithLabels(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))

	unregNew()
	assert.False(t, metricExists(t, reg, "cloudberry_db_pool_acquired_conns",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
}

func TestIdleDaemonHealthMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.SetIdleDaemonUp("c1", "ns1", true)
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_idle_daemon_up",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))
	r.SetIdleDaemonUp("c1", "ns1", false)
	assert.Equal(t, 0.0, valueWithLabels(t, reg, "cloudberry_idle_daemon_up",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))

	r.RecordIdleScanFailure("c1", "ns1")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_idle_scan_failures_total",
		map[string]string{"cluster": "c1", "namespace": "ns1"}))

	r.RecordIdleReconnectAttempt("c1", "ns1", "error")
	r.RecordIdleReconnectAttempt("c1", "ns1", "success")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_idle_reconnect_attempts_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "success"}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_idle_reconnect_attempts_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "error"}))
}

func TestRecordSessionTermination(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordSessionTermination("c1", "ns1", "success")
	r.RecordSessionTermination("c1", "ns1", "error")

	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_session_terminations_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "success"}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_session_terminations_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "error"}))
}

func TestControllerOperationMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordStorageExpansion("c1", "ns1", "success")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_storage_expansions_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "success"}))

	r.RecordBackupOnDelete("c1", "ns1", "failed")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_backup_on_delete_total",
		map[string]string{"cluster": "c1", "namespace": "ns1", "result": "failed"}))

	r.ObserveScalePhaseDuration("out", "registering", 30*time.Second)
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_scale_phase_duration_seconds",
		map[string]string{"direction": "out", "phase": "registering"}))
}

func TestAPIBusinessMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordMigrateOperation("started")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_migrate_operations_total",
		map[string]string{"result": "started"}))

	r.RecordAPIClusterOperation("create", "success")
	r.RecordAPIClusterOperation("delete", "error")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_api_cluster_operations_total",
		map[string]string{"operation": "create", "result": "success"}))
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_api_cluster_operations_total",
		map[string]string{"operation": "delete", "result": "error"}))

	r.RecordLogStreamSession("success")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_log_stream_sessions_total",
		map[string]string{"result": "success"}))

	r.AddLogStreamBytes(1024)
	r.AddLogStreamBytes(512)
	assert.Equal(t, 1536.0, testutil.ToFloat64(r.logStreamBytesTotal))

	r.RecordOIDCDiscovery("error")
	r.RecordOIDCDiscovery("success")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_oidc_discovery_total",
		map[string]string{"result": "success"}))

	r.ObserveAuthTokenVerifyDuration(20 * time.Millisecond)
	count := testutil.CollectAndCount(r.authTokenVerifyDuration,
		"cloudberry_auth_token_verify_duration_seconds")
	require.Equal(t, 1, count)
}

// TestNoopRecorderNewMethods exercises every new no-op method (compile-time
// interface assertion lives in metrics_test.go) so wiring later is covered.
func TestNoopRecorderNewMethods(t *testing.T) {
	n := NewNoopRecorder()
	n.RecordAPIRequest("/r", "GET", "200", time.Second)
	n.AddAPIRequestsInFlight(1)
	n.RecordRateLimitRejection("/r")
	n.RecordDBConnect("c", "ns", "success", time.Second)
	n.ObserveDBQueryDuration("Ping", time.Second)
	unreg := n.RegisterDBPoolStats("c", "ns", func() (float64, float64, float64) { return 0, 0, 0 })
	require.NotNil(t, unreg)
	unreg()
	n.SetIdleDaemonUp("c", "ns", true)
	n.RecordIdleScanFailure("c", "ns")
	n.RecordIdleReconnectAttempt("c", "ns", "success")
	n.RecordSessionTermination("c", "ns", "success")
	n.RecordStorageExpansion("c", "ns", "success")
	n.RecordBackupOnDelete("c", "ns", "completed")
	n.ObserveScalePhaseDuration("out", "registering", time.Second)
	n.RecordMigrateOperation("started")
	n.RecordAPIClusterOperation("create", "success")
	n.RecordLogStreamSession("success")
	n.AddLogStreamBytes(1)
	n.RecordOIDCDiscovery("success")
	n.ObserveAuthTokenVerifyDuration(time.Second)
}
