package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// valueWithLabels gathers the value of a metric family from the registry,
// matching on the metric name and the provided label key/value pairs. It fails
// the test when the metric is not found.
func valueWithLabels(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := findMetricValue(t, reg, name, labels)
	require.True(t, ok, "metric %s with labels %v not found", name, labels)
	return v
}

// metricExists reports whether at least one sample for the named metric family
// with the given labels exists in the registry.
func metricExists(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) bool {
	t.Helper()
	_, ok := findMetricValue(t, reg, name, labels)
	return ok
}

// findMetricValue returns the scalar value of the first matching sample for the
// metric family, supporting gauges, counters and histograms (sample count).
func findMetricValue(
	t *testing.T, reg *prometheus.Registry, name string, labels map[string]string,
) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			switch {
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue(), true
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue(), true
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount()), true
			}
		}
	}
	return 0, false
}

// labelsMatch reports whether the gathered label pairs contain all wanted pairs.
func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(pairs))
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func TestNewPrometheusRecorder(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	require.NotNil(t, recorder)

	// Verify metrics are registered - record something to ensure metrics exist
	recorder.RecordReconcile("test", "default", "success", time.Second)
	families, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, families)
}

func TestRecordReconcile(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		namespace string
		result    string
		duration  time.Duration
	}{
		{
			name:      "success reconcile",
			cluster:   "test-cluster",
			namespace: "default",
			result:    "success",
			duration:  100 * time.Millisecond,
		},
		{
			name:      "error reconcile",
			cluster:   "test-cluster",
			namespace: "default",
			result:    "error",
			duration:  500 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			// Should not panic
			recorder.RecordReconcile(tt.cluster, tt.namespace, tt.result, tt.duration)
		})
	}
}

func TestUpdateClusterInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.UpdateClusterInfo("test-cluster", "default", "7.7", "Running", 4)

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_cluster_info" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_cluster_info metric should be registered")
}

func TestSetCoordinatorUp(t *testing.T) {
	tests := []struct {
		name string
		up   bool
	}{
		{"coordinator up", true},
		{"coordinator down", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.SetCoordinatorUp("test", "default", tt.up)
		})
	}
}

func TestSetStandbyUp(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetStandbyUp("test", "default", true)
	recorder.SetStandbyUp("test", "default", false)
}

func TestSetSegmentsReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetSegmentsReady("test", "default", 4)
}

func TestSetSegmentsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetSegmentsTotal("test", "default", 4)
}

func TestSetSegmentsFailed(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetSegmentsFailed("test", "default", 1)
}

func TestSetMirroringInSync(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetMirroringInSync("test", "default", true)
	recorder.SetMirroringInSync("test", "default", false)
}

func TestRecordFTSProbe(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"success probe", "success"},
		{"failure probe", "failure"},
		{"degraded probe", "degraded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordFTSProbe("test", "default", tt.result, 100*time.Millisecond)
		})
	}
}

func TestRecordFTSFailover(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordFTSFailover("test", "default")
}

func TestSetSegmentStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetSegmentStatus("test", "default", "0", true)
	recorder.SetSegmentStatus("test", "default", "1", false)
}

func TestSetReplicationLag(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetReplicationLag("test", "default", "0", 1024)
}

func TestSetStandbyReplicationLag(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetStandbyReplicationLag("test", "default", 2048)
}

func TestRecordConfigReload(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordConfigReload("test", "default")
}

func TestSetConnectionsActive(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetConnectionsActive("test", "default", 10)
}

func TestSetConnectionsMax(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetConnectionsMax("test", "default", 100)
}

func TestSetDiskUsageBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDiskUsageBytes("test", "default", "mydb", 1073741824)
}

func TestRecordAuthAttempt(t *testing.T) {
	tests := []struct {
		name   string
		method string
		result string
	}{
		{"basic success", "basic", "success"},
		{"basic failure", "basic", "failure"},
		{"oidc success", "oidc", "success"},
		{"oidc failure", "oidc", "failure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordAuthAttempt(tt.method, tt.result)
		})
	}
}

func TestBoolToFloat64(t *testing.T) {
	assert.Equal(t, 1.0, boolToFloat64(true))
	assert.Equal(t, 0.0, boolToFloat64(false))
}

func TestNoopRecorder(t *testing.T) {
	recorder := &NoopRecorder{}

	// All methods should not panic — this verifies the Recorder interface
	// contract is maintained even when the no-op implementation changes.
	recorder.RecordReconcile("c", "n", "s", time.Second)
	recorder.UpdateClusterInfo("c", "n", "v", "p", 1)
	recorder.SetCoordinatorUp("c", "n", true)
	recorder.SetStandbyUp("c", "n", true)
	recorder.SetSegmentsReady("c", "n", 1)
	recorder.SetSegmentsTotal("c", "n", 1)
	recorder.SetSegmentsFailed("c", "n", 0)
	recorder.SetMirroringInSync("c", "n", true)
	recorder.RecordFTSProbe("c", "n", "s", time.Second)
	recorder.RecordFTSFailover("c", "n")
	recorder.SetSegmentStatus("c", "n", "0", true)
	recorder.SetReplicationLag("c", "n", "0", 0)
	recorder.SetStandbyReplicationLag("c", "n", 0)
	recorder.RecordConfigReload("c", "n")
	recorder.SetConnectionsActive("c", "n", 0)
	recorder.SetConnectionsMax("c", "n", 0)
	recorder.SetDiskUsageBytes("c", "n", "db", 0)
	recorder.RecordAuthAttempt("basic", "success")
	recorder.SetActiveQueries("c", "n", 5)
	recorder.SetQueuedQueries("c", "n", 2)
	recorder.SetBlockedQueries("c", "n", 1)
	recorder.RecordWorkloadRuleAction("c", "n", "cancel-long", "cancel")
	recorder.SetResourceGroupUsage("c", "n", "analytics", 45.5, 60.2)
	recorder.RecordIdleSessionTermination("c", "n", "idle-30m")
	recorder.RecordSlowQuery("c", "n")
	recorder.RecordBackup("c", "n", "full", "success")
	recorder.ObserveBackupDuration("c", "n", "full", time.Second)
	recorder.SetBackupSizeBytes("c", "n", "20260519020000", 1024)
	recorder.SetBackupLastSuccessTimestamp("c", "n", 1700000000)
	recorder.SetBackupLastStatus("c", "n", 0)
	recorder.ObserveRestoreDuration("c", "n", time.Second)
	recorder.RecordBackupRetentionDeleted("c", "n", 2)
	recorder.SetBackupJobStatus("c", "n", "job1", "backup", 2)
	recorder.RecordRestore("c", "n", "success")
	recorder.RecordRestoreValidation("c", "n", "success")
	recorder.SetDataLoadingJobsActive("c", "n", 1)
	recorder.SetPXFServersConfigured("c", "n", 5)
	recorder.RecordDataLoadingRows("c", "n", "job1", "s3", 100)
	recorder.SetDataLoadingJobStatus("c", "n", "job1", 2)
	recorder.SetDataLoadingJobLastSuccess("c", "n", "job1", 1700000000)
	recorder.ObserveDataLoadingJobDuration("c", "n", "job1", time.Second)
	recorder.RecordDataLoadingErrors("c", "n", "job1")
	recorder.SetDiskUsagePercent("c", "n", 50)
	recorder.SetRecommendationsTotal("c", "n", "bloat", 3)
	recorder.ObserveRecommendationScanDuration("c", "n", time.Second)
	recorder.SetTableBloatRatio("c", "n", "public.t", 0.1)
	recorder.RecordScaleOperation("c", "n", "scale-out")
	recorder.SetRedistributionProgress("c", "n", 0.75)
	recorder.SetDataSkewCoefficient("c", "n", 0.15)
	recorder.SetPVCSizeBytes("c", "n", "coordinator", 10737418240)
	recorder.RecordMirroringOperation("c", "n", "enable")
	recorder.RecordMirroringOperation("c", "n", "disable")
	recorder.RecordMaintenanceOperation("c", "n", "vacuum", "success")
	recorder.RecordPasswordRotation()
	recorder.RecordQueryHistoryInsert("c", "n")
	recorder.ObserveQueryHistorySearchDuration("c", "n", time.Second)
	recorder.RecordQueryHistoryExport("c", "n", "csv")
	recorder.RecordQueryHistoryRetentionCleanup("c", "n", 10)
	recorder.SetQueryHistorySizeBytes("c", "n", 1024)
}

func TestRecordBackup(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordBackup("test", "default", "full", "success")
	recorder.RecordBackup("test", "default", "incremental", "failed")
}

func TestObserveBackupDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveBackupDuration("test", "default", "full", 30*time.Second)

	got := valueWithLabels(t, reg, "cloudberry_backup_duration_seconds",
		map[string]string{"cluster": "test", "namespace": "default", "type": "full"})
	assert.InDelta(t, 1.0, got, 0.001)
}

func TestSetBackupSizeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBackupSizeBytes("test", "default", "20260519020000", 1073741824)

	got := valueWithLabels(t, reg, "cloudberry_backup_size_bytes",
		map[string]string{"cluster": "test", "namespace": "default", "timestamp": "20260519020000"})
	assert.InDelta(t, 1073741824.0, got, 0.5)
}

func TestSetBackupLastSuccessTimestamp(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBackupLastSuccessTimestamp("test", "default", 1700000000)

	got := valueWithLabels(t, reg, "cloudberry_backup_last_success_timestamp",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.InDelta(t, 1700000000.0, got, 0.5)
}

func TestSetBackupLastStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBackupLastStatus("test", "default", 1)

	got := valueWithLabels(t, reg, "cloudberry_backup_last_status",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.InDelta(t, 1.0, got, 0.001)
}

func TestObserveRestoreDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveRestoreDuration("test", "default", 30*time.Second)

	assert.True(t, metricExists(t, reg, "cloudberry_restore_duration_seconds",
		map[string]string{"cluster": "test", "namespace": "default"}))
}

func TestRecordBackupRetentionDeleted(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordBackupRetentionDeleted("test", "default", 3)
	recorder.RecordBackupRetentionDeleted("test", "default", 2)

	got := valueWithLabels(t, reg, "cloudberry_backup_retention_deleted_total",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.InDelta(t, 5.0, got, 0.001)
}

func TestSetBackupJobStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBackupJobStatus("test", "default", "job1", "backup", 2)

	got := valueWithLabels(t, reg, "cloudberry_backup_job_status",
		map[string]string{"cluster": "test", "namespace": "default", "job_name": "job1", "operation": "backup"})
	assert.InDelta(t, 2.0, got, 0.001)
}

func TestRecordRestore(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordRestore("test", "default", "success")
}

// TestRecordRestoreValidation_Scenario80 verifies the post-restore validation
// counter increments per {result} label and is registered under the expected
// metric name.
func TestRecordRestoreValidation_Scenario80(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.RecordRestoreValidation("test", "default", "success")
	recorder.RecordRestoreValidation("test", "default", "success")
	recorder.RecordRestoreValidation("test", "default", "failed")

	gotSuccess := valueWithLabels(t, reg, "cloudberry_restore_validation_total",
		map[string]string{"cluster": "test", "namespace": "default", "result": "success"})
	assert.InDelta(t, 2.0, gotSuccess, 0.001)

	gotFailed := valueWithLabels(t, reg, "cloudberry_restore_validation_total",
		map[string]string{"cluster": "test", "namespace": "default", "result": "failed"})
	assert.InDelta(t, 1.0, gotFailed, 0.001)
}

// TestNoopRecorder_RecordRestoreValidation_Scenario80 verifies the Noop
// implementation is a safe no-op (no panic).
func TestNoopRecorder_RecordRestoreValidation_Scenario80(t *testing.T) {
	var r Recorder = &NoopRecorder{}
	assert.NotPanics(t, func() {
		r.RecordRestoreValidation("c", "n", "success")
		r.RecordRestoreValidation("c", "n", "failed")
	})
}

func TestSetDataLoadingJobsActive(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDataLoadingJobsActive("test", "default", 3)
}

func TestSetPXFServersConfigured(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetPXFServersConfigured("test", "default", 5)

	got := valueWithLabels(t, reg, "cloudberry_pxf_servers_configured",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.Equal(t, 5.0, got)

	// The gauge is a SET (last-wins) gauge: re-setting overwrites, not adds.
	recorder.SetPXFServersConfigured("test", "default", 2)
	got = valueWithLabels(t, reg, "cloudberry_pxf_servers_configured",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.Equal(t, 2.0, got)
}

// TestIncPXFServersChanged covers Scenario 106 (106-MX-B1): the
// cloudberry_pxf_servers_changed_total COUNTER increments by exactly 1 per call,
// is monotonic (accumulates), and carries the {cluster,namespace} labels. The
// honesty invariant (fire only on a real diff) is enforced at the CALL SITE
// (controller/api) — the recorder itself just increments — so here we pin that
// the increment is by 1 and label-scoped.
func TestIncPXFServersChanged(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	const name = "cloudberry_pxf_servers_changed_total"
	labels := map[string]string{"cluster": "test", "namespace": "default"}

	// Before any call the series is absent (a counter only appears once touched):
	// the honest "never fired" state is no sample at all.
	assert.False(t, metricExists(t, reg, name, labels),
		"counter must not exist before the first real diff")

	recorder.IncPXFServersChanged("test", "default")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, name, labels))

	// Monotonic accumulation: a second real diff brings the total to 2.
	recorder.IncPXFServersChanged("test", "default")
	assert.Equal(t, 2.0, valueWithLabels(t, reg, name, labels))

	// A DIFFERENT cluster/namespace is an independent series (bounded labels).
	other := map[string]string{"cluster": "other", "namespace": "ns2"}
	recorder.IncPXFServersChanged("other", "ns2")
	assert.Equal(t, 1.0, valueWithLabels(t, reg, name, other))
	// The original series is unaffected.
	assert.Equal(t, 2.0, valueWithLabels(t, reg, name, labels))
}

// TestNoopRecorder_IncPXFServersChanged covers 106-MX-B4: the NoopRecorder
// implements the new method without panicking (interface satisfied), so test /
// non-metric setups never crash on the servers-changed signal.
func TestNoopRecorder_IncPXFServersChanged(t *testing.T) {
	var r Recorder = &NoopRecorder{}
	assert.NotPanics(t, func() {
		r.IncPXFServersChanged("c", "n")
	})
}

// TestSetPXFStatus covers 105-MX-B1: the observed PXF status gauge maps
// Stopped→0, Running→1, Error→2 via the metrics test registry. The controller
// only sets the gauge when the status is OBSERVABLE; this test pins the encoded
// values the controller publishes for each observed status.
func TestSetPXFStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	labels := map[string]string{"cluster": "test", "namespace": "default"}

	// Stopped → 0.
	recorder.SetPXFStatus("test", "default", 0)
	assert.Equal(t, 0.0, valueWithLabels(t, reg, "cloudberry_pxf_status", labels))

	// Running → 1 (last-wins overwrite).
	recorder.SetPXFStatus("test", "default", 1)
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_pxf_status", labels))

	// Error → 2.
	recorder.SetPXFStatus("test", "default", 2)
	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_pxf_status", labels))
}

// TestSetPXFStatus_UnobservableNotEmitted covers 105-MX-B1 honesty: when the
// controller never calls SetPXFStatus (the status was UNOBSERVABLE/absent), the
// cloudberry_pxf_status series must NOT exist — the gauge never claims a state
// that was not observed.
func TestSetPXFStatus_UnobservableNotEmitted(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewPrometheusRecorder(reg)

	assert.False(t, metricExists(t, reg, "cloudberry_pxf_status",
		map[string]string{"cluster": "test", "namespace": "default"}),
		"unobservable status must not emit a cloudberry_pxf_status sample")
}

// TestSetPXFExtensionsInstalled covers 105-MX-B2: the extensions-installed gauge
// equals the count of observed extensions (e.g. 2 for {pxf,pxf_fdw}).
func TestSetPXFExtensionsInstalled(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	labels := map[string]string{"cluster": "test", "namespace": "default"}

	recorder.SetPXFExtensionsInstalled("test", "default", 2)
	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_pxf_extensions_installed", labels))

	// Last-wins: a subsequent observation of only one extension overwrites.
	recorder.SetPXFExtensionsInstalled("test", "default", 1)
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_pxf_extensions_installed", labels))
}

// TestSetPXFExtensionsInstalled_UnreachableNotEmitted covers 105-MX-B2 honesty:
// when the DB is unreachable the controller never calls
// SetPXFExtensionsInstalled, so the gauge must NOT be present (it must never
// synthesize a 0 for an unobservable probe).
func TestSetPXFExtensionsInstalled_UnreachableNotEmitted(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewPrometheusRecorder(reg)

	assert.False(t, metricExists(t, reg, "cloudberry_pxf_extensions_installed",
		map[string]string{"cluster": "test", "namespace": "default"}),
		"unreachable DB must not emit a cloudberry_pxf_extensions_installed sample")
}

// TestNoopRecorder_PXFObservability covers 105-MX-B1/B2: the NoopRecorder
// implements both new methods without panicking (interface satisfied).
func TestNoopRecorder_PXFObservability(t *testing.T) {
	n := &NoopRecorder{}
	assert.NotPanics(t, func() {
		n.SetPXFStatus("c", "n", 1)
		n.SetPXFExtensionsInstalled("c", "n", 2)
	})
}

func TestRecordDataLoadingRows(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordDataLoadingRows("test", "default", "s3-loader", "s3", 1000)

	got := valueWithLabels(t, reg, "cloudberry_data_loading_rows_total",
		map[string]string{
			"cluster": "test", "namespace": "default",
			"job": "s3-loader", "source_type": "s3",
		})
	assert.Equal(t, 1000.0, got)

	// Counter accumulates across calls (DATALOAD_ROWS marker harvests add up).
	recorder.RecordDataLoadingRows("test", "default", "s3-loader", "s3", 500)
	got = valueWithLabels(t, reg, "cloudberry_data_loading_rows_total",
		map[string]string{
			"cluster": "test", "namespace": "default",
			"job": "s3-loader", "source_type": "s3",
		})
	assert.Equal(t, 1500.0, got)
}

func TestSetDataLoadingJobStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDataLoadingJobStatus("test", "default", "loader", 1)

	labels := map[string]string{"cluster": "test", "namespace": "default", "job": "loader"}
	assert.Equal(t, 1.0, valueWithLabels(t, reg, "cloudberry_data_loading_job_status", labels))

	// SET (last-wins) gauge: 1=running -> 2=success overwrites.
	recorder.SetDataLoadingJobStatus("test", "default", "loader", 2)
	assert.Equal(t, 2.0, valueWithLabels(t, reg, "cloudberry_data_loading_job_status", labels))
}

func TestSetDataLoadingJobLastSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDataLoadingJobLastSuccess("test", "default", "loader", 1700000000)

	got := valueWithLabels(t, reg, "cloudberry_data_loading_job_last_success_timestamp",
		map[string]string{"cluster": "test", "namespace": "default", "job": "loader"})
	assert.Equal(t, 1700000000.0, got)
}

func TestObserveDataLoadingJobDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveDataLoadingJobDuration("test", "default", "loader", 90*time.Second)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_data_loading_job_duration_seconds" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			h := f.GetMetric()[0].GetHistogram()
			assert.Equal(t, uint64(1), h.GetSampleCount())
			assert.InDelta(t, 90.0, h.GetSampleSum(), 0.001)
		}
	}
	assert.True(t, found, "duration histogram family must be registered")
}

func TestRecordDataLoadingErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordDataLoadingErrors("test", "default", "loader")
	recorder.RecordDataLoadingErrors("test", "default", "loader")

	got := valueWithLabels(t, reg, "cloudberry_data_loading_errors_total",
		map[string]string{"cluster": "test", "namespace": "default", "job": "loader"})
	assert.Equal(t, 2.0, got)
}

func TestSetDiskUsagePercent(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDiskUsagePercent("test", "default", 75.5)
}

func TestSetRecommendationsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetRecommendationsTotal("test", "default", "bloat", 5)
	recorder.SetRecommendationsTotal("test", "default", "skew", 3)
	recorder.SetRecommendationsTotal("test", "default", "age", 1)
	recorder.SetRecommendationsTotal("test", "default", "index_bloat", 2)
}

func TestObserveRecommendationScanDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveRecommendationScanDuration("test", "default", 45*time.Second)
}

func TestSetTableBloatRatio(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetTableBloatRatio("test", "default", "public.orders", 0.25)
}

func TestSetPVCSizeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetPVCSizeBytes("test", "default", "coordinator", 10737418240)

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_pvc_size_bytes" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_pvc_size_bytes metric should be registered")
}

func TestSetActiveQueries(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetActiveQueries("test", "default", 15)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_active_queries" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 15.0, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_active_queries metric should be registered")
}

func TestSetQueuedQueries(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetQueuedQueries("test", "default", 5)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_queued_queries" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 5.0, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_queued_queries metric should be registered")
}

func TestSetBlockedQueries(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBlockedQueries("test", "default", 3)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_blocked_queries" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 3.0, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_blocked_queries metric should be registered")
}

func TestRecordWorkloadRuleAction(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordWorkloadRuleAction("test", "default", "cancel-long", "cancel")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_workload_rule_actions_total" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_workload_rule_actions_total metric should be registered")
}

func TestSetResourceGroupUsage(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetResourceGroupUsage("test", "default", "analytics", 45.5, 60.2)

	families, err := reg.Gather()
	require.NoError(t, err)
	cpuFound := false
	memFound := false
	for _, f := range families {
		if f.GetName() == "cloudberry_resource_group_cpu_usage" {
			cpuFound = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 45.5, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
		}
		if f.GetName() == "cloudberry_resource_group_memory_usage" {
			memFound = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 60.2, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
		}
	}
	assert.True(t, cpuFound, "cloudberry_resource_group_cpu_usage metric should be registered")
	assert.True(t, memFound, "cloudberry_resource_group_memory_usage metric should be registered")
}

func TestRecordIdleSessionTermination(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordIdleSessionTermination("test", "default", "idle-30m")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_idle_session_terminations_total" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_idle_session_terminations_total metric should be registered")
}

func TestRecordSlowQuery(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordSlowQuery("test", "default")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_slow_queries_total" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_slow_queries_total metric should be registered")
}

func TestRecordScaleOperation(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordScaleOperation("test", "default", "scale-out")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_scale_operations_total" {
			found = true
			break
		}
	}
	assert.True(t, found, "cloudberry_scale_operations_total metric should be registered")
}

func TestSetRedistributionProgress(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetRedistributionProgress("test", "default", 0.75)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_redistribution_progress" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 0.75, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_redistribution_progress metric should be registered")
}

func TestSetDataSkewCoefficient(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDataSkewCoefficient("test", "default", 0.15)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_data_skew_coefficient" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 0.15, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_data_skew_coefficient metric should be registered")
}

func TestNoopRecorder_ImplementsInterface(t *testing.T) {
	var _ Recorder = &NoopRecorder{}
}

func TestNoopRecorder_AllMethods(t *testing.T) {
	r := NewNoopRecorder()
	require.NotNil(t, r)

	// Call every method — none should panic.
	r.RecordReconcile("cluster", "ns", "success", time.Second)
	r.UpdateClusterInfo("cluster", "ns", "7.7", "Running", 4)
	r.SetCoordinatorUp("cluster", "ns", true)
	r.SetStandbyUp("cluster", "ns", false)
	r.SetSegmentsReady("cluster", "ns", 4)
	r.SetSegmentsTotal("cluster", "ns", 4)
	r.SetSegmentsFailed("cluster", "ns", 0)
	r.SetMirroringInSync("cluster", "ns", true)
	r.RecordFTSProbe("cluster", "ns", "success", 100*time.Millisecond)
	r.RecordFTSFailover("cluster", "ns")
	r.SetSegmentStatus("cluster", "ns", "seg-0", true)
	r.SetReplicationLag("cluster", "ns", "seg-0", 1024)
	r.SetStandbyReplicationLag("cluster", "ns", 2048)
	r.RecordConfigReload("cluster", "ns")
	r.SetConnectionsActive("cluster", "ns", 10)
	r.SetConnectionsMax("cluster", "ns", 100)
	r.SetDiskUsageBytes("cluster", "ns", "postgres", 1073741824)
	r.RecordAuthAttempt("basic", "success")
	r.SetActiveQueries("cluster", "ns", 5)
	r.SetQueuedQueries("cluster", "ns", 2)
	r.SetBlockedQueries("cluster", "ns", 1)
	r.RecordWorkloadRuleAction("cluster", "ns", "cancel-long", "cancel")
	r.SetResourceGroupUsage("cluster", "ns", "analytics", 45.5, 60.2)
	r.RecordIdleSessionTermination("cluster", "ns", "idle-30m")
	r.RecordSlowQuery("cluster", "ns")
	r.RecordBackup("cluster", "ns", "full", "success")
	r.ObserveBackupDuration("cluster", "ns", "full", 30*time.Second)
	r.SetBackupSizeBytes("cluster", "ns", "20260519020000", 1073741824)
	r.SetBackupLastSuccessTimestamp("cluster", "ns", 1700000000)
	r.SetBackupLastStatus("cluster", "ns", 0)
	r.ObserveRestoreDuration("cluster", "ns", 30*time.Second)
	r.RecordBackupRetentionDeleted("cluster", "ns", 2)
	r.SetBackupJobStatus("cluster", "ns", "job1", "backup", 2)
	r.RecordRestore("cluster", "ns", "success")
	r.RecordRestoreValidation("cluster", "ns", "failed")
	r.SetDataLoadingJobsActive("cluster", "ns", 3)
	r.SetPXFServersConfigured("cluster", "ns", 5)
	r.IncPXFServersChanged("cluster", "ns")
	r.RecordDataLoadingRows("cluster", "ns", "s3-loader", "s3", 1000)
	r.RecordGpfdistReconcile("cluster", "ns", "deployment", "success")
	r.RecordPXFExtensionSetup("cluster", "ns", "installed")
	r.SetDiskUsagePercent("cluster", "ns", 75.5)
	r.SetRecommendationsTotal("cluster", "ns", "bloat", 5)
	r.ObserveRecommendationScanDuration("cluster", "ns", 45*time.Second)
	r.SetTableBloatRatio("cluster", "ns", "public.orders", 0.25)
	r.RecordScaleOperation("cluster", "ns", "scale-out")
	r.SetRedistributionProgress("cluster", "ns", 0.75)
	r.SetDataSkewCoefficient("cluster", "ns", 0.15)
	r.SetPVCSizeBytes("cluster", "ns", "coordinator", 10737418240)
	r.RecordMirroringOperation("cluster", "ns", "enable")
	r.RecordMaintenanceOperation("cluster", "ns", "vacuum", "success")
	r.RecordPasswordRotation()
	r.RecordActiveQueryExport()
	r.RecordMonitorPause("cluster", "ns")
	r.RecordMonitorResume("cluster", "ns")
}

func TestPrometheusRecorder_ImplementsInterface(t *testing.T) {
	var _ Recorder = &PrometheusRecorder{}
}

// ============================================================================
// Mirroring Operation Metrics Tests
// ============================================================================

func TestRecordMirroringOperation_Enable(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordMirroringOperation("test", "default", "enable")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_mirroring_operations_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 1.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_mirroring_operations_total metric should be registered")
}

func TestRecordMirroringOperation_Disable(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordMirroringOperation("test", "default", "disable")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_mirroring_operations_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 1.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_mirroring_operations_total metric should be registered")
}

func TestNoopRecorder_RecordMirroringOperation(t *testing.T) {
	recorder := &NoopRecorder{}
	// Should not panic.
	recorder.RecordMirroringOperation("c", "n", "enable")
	recorder.RecordMirroringOperation("c", "n", "disable")
}

func TestRecordMirroringOperation_MultipleIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordMirroringOperation("test", "default", "enable")
	recorder.RecordMirroringOperation("test", "default", "enable")
	recorder.RecordMirroringOperation("test", "default", "disable")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_mirroring_operations_total" {
			found = true
			// Should have 2 metric series (enable and disable).
			assert.Len(t, f.GetMetric(), 2)
			break
		}
	}
	assert.True(t, found)
}

// ============================================================================
// Query History Metrics Tests
// ============================================================================

func TestQueryHistoryMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	// Record all query history metrics to ensure they are registered.
	recorder.RecordQueryHistoryInsert("test", "default")
	recorder.ObserveQueryHistorySearchDuration("test", "default", 100*time.Millisecond)
	recorder.RecordQueryHistoryExport("test", "default", "csv")
	recorder.RecordQueryHistoryRetentionCleanup("test", "default", 50)
	recorder.SetQueryHistorySizeBytes("test", "default", 1073741824)

	families, err := reg.Gather()
	require.NoError(t, err)

	expectedMetrics := []string{
		"cloudberry_query_history_total",
		"cloudberry_query_history_search_duration_seconds",
		"cloudberry_query_history_export_total",
		"cloudberry_query_history_retention_deleted_total",
		"cloudberry_query_history_size_bytes",
	}

	for _, metricName := range expectedMetrics {
		found := false
		for _, f := range families {
			if f.GetName() == metricName {
				found = true
				break
			}
		}
		assert.True(t, found, "metric %s should be registered", metricName)
	}
}

func TestRecordQueryHistoryInsert(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordQueryHistoryInsert("test", "default")
	recorder.RecordQueryHistoryInsert("test", "default")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 2.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_query_history_total metric should be registered")
}

func TestObserveQueryHistorySearchDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveQueryHistorySearchDuration("test", "default", 250*time.Millisecond)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_search_duration_seconds" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.Equal(t, uint64(1), f.GetMetric()[0].GetHistogram().GetSampleCount())
			break
		}
	}
	assert.True(t, found, "cloudberry_query_history_search_duration_seconds metric should be registered")
}

func TestRecordQueryHistoryExport(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordQueryHistoryExport("test", "default", "csv")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_export_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 1.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_query_history_export_total metric should be registered")
}

func TestRecordQueryHistoryRetentionCleanup(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordQueryHistoryRetentionCleanup("test", "default", 100)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_retention_deleted_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 100.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_query_history_retention_deleted_total metric should be registered")
}

func TestSetQueryHistorySizeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetQueryHistorySizeBytes("test", "default", 5368709120)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_size_bytes" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 5368709120.0, f.GetMetric()[0].GetGauge().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_query_history_size_bytes metric should be registered")
}

func TestNoopRecorder_QueryHistoryMethods(t *testing.T) {
	recorder := &NoopRecorder{}

	// All query history methods should not panic.
	recorder.RecordQueryHistoryInsert("c", "n")
	recorder.ObserveQueryHistorySearchDuration("c", "n", time.Second)
	recorder.RecordQueryHistoryExport("c", "n", "csv")
	recorder.RecordQueryHistoryRetentionCleanup("c", "n", 50)
	recorder.SetQueryHistorySizeBytes("c", "n", 1024)
}

func TestQueryHistorySearchDuration_Buckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	// Record multiple observations at different durations.
	recorder.ObserveQueryHistorySearchDuration("test", "default", 10*time.Millisecond)
	recorder.ObserveQueryHistorySearchDuration("test", "default", 100*time.Millisecond)
	recorder.ObserveQueryHistorySearchDuration("test", "default", 1*time.Second)
	recorder.ObserveQueryHistorySearchDuration("test", "default", 5*time.Second)

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_search_duration_seconds" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.Equal(t, uint64(4), f.GetMetric()[0].GetHistogram().GetSampleCount())
			break
		}
	}
	assert.True(t, found)
}

func TestQueryHistoryExport_MultipleFormats(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordQueryHistoryExport("test", "default", "csv")
	recorder.RecordQueryHistoryExport("test", "default", "csv")
	recorder.RecordQueryHistoryExport("test", "default", "json")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_query_history_export_total" {
			found = true
			// Should have 2 metric series (csv and json).
			assert.Len(t, f.GetMetric(), 2)
			break
		}
	}
	assert.True(t, found)
}

// ============================================================================
// Monitor Pause/Resume Metrics Tests
// ============================================================================

func TestRecordMonitorPause(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordMonitorPause("test", "default")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_monitor_pause_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 1.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_monitor_pause_total metric should be registered")
}

func TestRecordMonitorResume(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordMonitorResume("test", "default")

	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "cloudberry_monitor_resume_total" {
			found = true
			require.Len(t, f.GetMetric(), 1)
			assert.InDelta(t, 1.0, f.GetMetric()[0].GetCounter().GetValue(), 0.001)
			break
		}
	}
	assert.True(t, found, "cloudberry_monitor_resume_total metric should be registered")
}

func TestNoopRecorder_MonitorPauseResume(t *testing.T) {
	recorder := &NoopRecorder{}
	// Should not panic.
	recorder.RecordMonitorPause("c", "n")
	recorder.RecordMonitorResume("c", "n")
}

// counterValue returns the value of the first metric series in the named family.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			require.NotEmpty(t, f.GetMetric())
			return f.GetMetric()[0].GetCounter().GetValue(), true
		}
	}
	return 0, false
}

// gaugeValue returns the value of the first metric series in the named family.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			require.NotEmpty(t, f.GetMetric())
			return f.GetMetric()[0].GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// histogramSampleCount returns the sample count of the first metric series in the named family.
func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string) (uint64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			require.NotEmpty(t, f.GetMetric())
			return f.GetMetric()[0].GetHistogram().GetSampleCount(), true
		}
	}
	return 0, false
}

// ============================================================================
// Security Metrics Tests (cert rotation, vault operations)
// ============================================================================

func TestRecordCertRotation(t *testing.T) {
	tests := []struct {
		name      string
		component string
		source    string
		result    string
	}{
		{"vault success", "webhook", "vault-pki", "success"},
		{"vault error", "webhook", "vault-pki", "error"},
		{"self-signed success", "webhook", "self-signed", "success"},
		{"self-signed error", "webhook", "self-signed", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordCertRotation(tt.component, tt.source, tt.result)

			v, found := counterValue(t, reg, "cloudberry_cert_rotation_total")
			assert.True(t, found, "cloudberry_cert_rotation_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordClusterCertIssuance(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"issuance success", "success"},
		{"issuance error", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordClusterCertIssuance("test-cluster", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_cluster_cert_issuance_total")
			assert.True(t, found, "cloudberry_cluster_cert_issuance_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordClusterCertIssuance_MultipleIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordClusterCertIssuance("test-cluster", "default", "success")
	recorder.RecordClusterCertIssuance("test-cluster", "default", "success")

	v, found := counterValue(t, reg, "cloudberry_cluster_cert_issuance_total")
	require.True(t, found)
	assert.InDelta(t, 2.0, v, 0.001)
}

func TestRecordCertRotation_MultipleIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordCertRotation("webhook", "self-signed", "success")
	recorder.RecordCertRotation("webhook", "self-signed", "success")

	v, found := counterValue(t, reg, "cloudberry_cert_rotation_total")
	require.True(t, found)
	assert.InDelta(t, 2.0, v, 0.001)
}

func TestSetCertExpirySeconds(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetCertExpirySeconds("webhook", 86400)

	v, found := gaugeValue(t, reg, "cloudberry_cert_expiry_seconds")
	assert.True(t, found, "cloudberry_cert_expiry_seconds should be registered")
	assert.InDelta(t, 86400.0, v, 0.001)
}

func TestRecordVaultOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		result    string
	}{
		{"auth success", "auth", "success"},
		{"auth error", "auth", "error"},
		{"read success", "read", "success"},
		{"write error", "write", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordVaultOperation(tt.operation, tt.result)

			v, found := counterValue(t, reg, "cloudberry_vault_operations_total")
			assert.True(t, found, "cloudberry_vault_operations_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestObserveVaultOperationDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.ObserveVaultOperationDuration("read", 250*time.Millisecond)
	recorder.ObserveVaultOperationDuration("read", 500*time.Millisecond)

	count, found := histogramSampleCount(t, reg, "cloudberry_vault_operation_duration_seconds")
	assert.True(t, found, "cloudberry_vault_operation_duration_seconds should be registered")
	assert.Equal(t, uint64(2), count)
}

// ============================================================================
// Webhook Admission Metrics Tests
// ============================================================================

func TestRecordWebhookAdmission(t *testing.T) {
	tests := []struct {
		name      string
		webhook   string
		operation string
		result    string
	}{
		{"validating allowed", "validating", "create", "allowed"},
		{"validating denied", "validating", "update", "denied"},
		{"validating error", "validating", "delete", "error"},
		{"mutating allowed", "mutating", "create", "allowed"},
		{"mutating denied", "mutating", "update", "denied"},
		{"mutating error", "mutating", "create", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordWebhookAdmission(tt.webhook, tt.operation, tt.result)

			v, found := counterValue(t, reg, "cloudberry_webhook_admission_total")
			assert.True(t, found, "cloudberry_webhook_admission_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

// ============================================================================
// Lifecycle Metrics Tests (upgrade, rolling restart, recovery)
// ============================================================================

func TestRecordUpgradeOperation(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"started", "started"},
		{"completed", "completed"},
		{"rollback", "rollback"},
		{"failed", "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordUpgradeOperation("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_upgrade_operations_total")
			assert.True(t, found, "cloudberry_upgrade_operations_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordRollingRestart(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"started", "started"},
		{"completed", "completed"},
		{"failed", "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordRollingRestart("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_rolling_restart_total")
			assert.True(t, found, "cloudberry_rolling_restart_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordPXFRestart(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"started", "started"},
		{"failed", "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordPXFRestart("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_pxf_restart_total")
			assert.True(t, found, "cloudberry_pxf_restart_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordPXFRestart_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordPXFRestart("cluster-a", "ns-a", "started")
	recorder.RecordPXFRestart("cluster-a", "ns-a", "started")
	recorder.RecordPXFRestart("cluster-b", "ns-b", "failed")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		startedValue float64
		failedValue  float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_pxf_restart_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "started":
				startedValue = m.GetCounter().GetValue()
			case "failed":
				failedValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, startedValue, 0.001)
	assert.InDelta(t, 1.0, failedValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["started"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["started"]["namespace"])
	assert.Equal(t, "cluster-b", seenLabels["failed"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["failed"]["namespace"])
}

// TestNoopRecorderPXFRestart ensures the no-op implementation does not panic.
func TestNoopRecorderPXFRestart(t *testing.T) {
	var r NoopRecorder
	assert.NotPanics(t, func() { r.RecordPXFRestart("c", "n", "started") })
}

func TestRecordRecoveryOperation(t *testing.T) {
	tests := []struct {
		name         string
		recoveryType string
		result       string
	}{
		{"incremental started", "incremental", "started"},
		{"full completed", "full", "completed"},
		{"differential failed", "differential", "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordRecoveryOperation("test", "default", tt.recoveryType, tt.result)

			v, found := counterValue(t, reg, "cloudberry_recovery_operations_total")
			assert.True(t, found, "cloudberry_recovery_operations_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

// TestNewMetricsRegistration verifies all new collectors are registered.
func TestNewMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.RecordCertRotation("webhook", "self-signed", "success")
	recorder.SetCertExpirySeconds("webhook", 3600)
	recorder.RecordVaultOperation("read", "success")
	recorder.ObserveVaultOperationDuration("read", time.Second)
	recorder.RecordWebhookAdmission("validating", "create", "allowed")
	recorder.RecordUpgradeOperation("test", "default", "started")
	recorder.RecordRollingRestart("test", "default", "started")
	recorder.RecordPXFRestart("test", "default", "started")
	recorder.RecordRecoveryOperation("test", "default", "full", "started")

	families, err := reg.Gather()
	require.NoError(t, err)

	expectedMetrics := []string{
		"cloudberry_cert_rotation_total",
		"cloudberry_cert_expiry_seconds",
		"cloudberry_vault_operations_total",
		"cloudberry_vault_operation_duration_seconds",
		"cloudberry_webhook_admission_total",
		"cloudberry_upgrade_operations_total",
		"cloudberry_rolling_restart_total",
		"cloudberry_pxf_restart_total",
		"cloudberry_recovery_operations_total",
	}

	for _, metricName := range expectedMetrics {
		found := false
		for _, f := range families {
			if f.GetName() == metricName {
				found = true
				break
			}
		}
		assert.True(t, found, "metric %s should be registered", metricName)
	}
}

// TestNoopRecorder_NewMethods exercises all newly added NoopRecorder methods.
func TestNoopRecorder_NewMethods(t *testing.T) {
	r := NewNoopRecorder()
	require.NotNil(t, r)

	// None of these should panic; they are intentional no-ops.
	r.RecordCertRotation("webhook", "self-signed", "success")
	r.SetCertExpirySeconds("webhook", 3600)
	r.RecordClusterCertIssuance("c", "n", "success")
	r.RecordVaultOperation("read", "success")
	r.ObserveVaultOperationDuration("read", time.Second)
	r.RecordWebhookAdmission("validating", "create", "allowed")
	r.RecordUpgradeOperation("c", "n", "started")
	r.RecordRollingRestart("c", "n", "started")
	r.RecordPXFRestart("c", "n", "started")
	r.RecordRecoveryOperation("c", "n", "full", "started")

	// Observability V4 API metric families (W2-B3, W2-B4, W2-B6): these
	// NoopRecorder methods must be safe no-ops on a nil/disabled recorder.
	r.RecordAPILifecycleRequest("start", "accepted")
	r.RecordAPIWorkloadOperation("resource_group", "create", "success")
	r.RecordPXFSync("c", "ns", "success")
}

// ============================================================================
// Maintenance Operation Metrics Tests
// ============================================================================

// TestRecordMaintenanceOperation verifies the maintenance_operations_total
// counter is incremented with the expected cluster/namespace/operation/result
// labels and value.
func TestRecordMaintenanceOperation(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		namespace string
		operation string
		result    string
	}{
		{"vacuum success", "test-cluster", "default", "vacuum", "success"},
		{"reindex failed", "prod-cluster", "prod", "reindex", "failed"},
		{"analyze started", "stg-cluster", "staging", "analyze", "started"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)

			recorder.RecordMaintenanceOperation(tt.cluster, tt.namespace, tt.operation, tt.result)

			families, err := reg.Gather()
			require.NoError(t, err)

			found := false
			for _, f := range families {
				if f.GetName() != "cloudberry_maintenance_operations_total" {
					continue
				}
				found = true
				require.Len(t, f.GetMetric(), 1)
				metric := f.GetMetric()[0]

				labels := map[string]string{}
				for _, l := range metric.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				assert.Equal(t, tt.cluster, labels["cluster"])
				assert.Equal(t, tt.namespace, labels["namespace"])
				assert.Equal(t, tt.operation, labels["operation"])
				assert.Equal(t, tt.result, labels["result"])

				assert.InDelta(t, 1.0, metric.GetCounter().GetValue(), 0.001)
			}
			assert.True(t, found,
				"cloudberry_maintenance_operations_total metric should be registered")
		})
	}
}

// TestRecordMaintenanceOperation_MultipleIncrements verifies repeated calls with
// the same labels accumulate on the counter.
func TestRecordMaintenanceOperation_MultipleIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.RecordMaintenanceOperation("c", "n", "vacuum", "success")
	recorder.RecordMaintenanceOperation("c", "n", "vacuum", "success")
	recorder.RecordMaintenanceOperation("c", "n", "vacuum", "failed")

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, f := range families {
		if f.GetName() != "cloudberry_maintenance_operations_total" {
			continue
		}
		found = true
		// Two distinct result label sets => two metric series.
		assert.Len(t, f.GetMetric(), 2)
	}
	assert.True(t, found,
		"cloudberry_maintenance_operations_total metric should be registered")
}

// ============================================================================
// API Lifecycle / Workload / PXF-sync Metrics Tests (W2-B3, W2-B4, W2-B6)
// ============================================================================

func TestRecordAPILifecycleRequest(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		result    string
	}{
		{"start accepted", "start", "accepted"},
		{"restart error", "restart", "error"},
		{"vacuum accepted", "vacuum", "accepted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordAPILifecycleRequest(tt.operation, tt.result)

			v, found := counterValue(t, reg, "cloudberry_api_cluster_lifecycle_requests_total")
			assert.True(t, found,
				"cloudberry_api_cluster_lifecycle_requests_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordAPILifecycleRequest_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordAPILifecycleRequest("start", "accepted")
	recorder.RecordAPILifecycleRequest("start", "accepted")
	recorder.RecordAPILifecycleRequest("restart", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		acceptedValue float64
		errorValue    float64
		seenLabels    = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_api_cluster_lifecycle_requests_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "accepted":
				acceptedValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, acceptedValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "start", seenLabels["accepted"]["operation"])
	assert.Equal(t, "restart", seenLabels["error"]["operation"])
}

func TestRecordAPIWorkloadOperation(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		operation string
		result    string
	}{
		{"rg create success", "resource_group", "create", "success"},
		{"rq delete error", "resource_queue", "delete", "error"},
		{"rule update success", "rule", "update", "success"},
		{"rg assign success", "resource_group", "assign", "success"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordAPIWorkloadOperation(tt.kind, tt.operation, tt.result)

			v, found := counterValue(t, reg, "cloudberry_api_workload_operations_total")
			assert.True(t, found,
				"cloudberry_api_workload_operations_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordAPIWorkloadOperation_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordAPIWorkloadOperation("resource_group", "create", "success")
	recorder.RecordAPIWorkloadOperation("resource_group", "create", "success")
	recorder.RecordAPIWorkloadOperation("rule", "delete", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		successValue float64
		errorValue   float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_api_workload_operations_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "success":
				successValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, successValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "resource_group", seenLabels["success"]["kind"])
	assert.Equal(t, "create", seenLabels["success"]["operation"])
	assert.Equal(t, "rule", seenLabels["error"]["kind"])
	assert.Equal(t, "delete", seenLabels["error"]["operation"])
}

func TestRecordPXFSync(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"success", "success"},
		{"error", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordPXFSync("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_pxf_sync_total")
			assert.True(t, found, "cloudberry_pxf_sync_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordPXFSync_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordPXFSync("cluster-a", "ns-a", "success")
	recorder.RecordPXFSync("cluster-a", "ns-a", "success")
	recorder.RecordPXFSync("cluster-b", "ns-b", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		successValue float64
		errorValue   float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_pxf_sync_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "success":
				successValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, successValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["success"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["success"]["namespace"])
	assert.Equal(t, "cluster-b", seenLabels["error"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["error"]["namespace"])
}

func TestRecordGpfdistReconcile(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		result    string
	}{
		{"pvc-success", "pvc", "success"},
		{"deployment-success", "deployment", "success"},
		{"service-error", "service", "error"},
		{"delete-success", "delete", "success"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordGpfdistReconcile("test", "default", tt.operation, tt.result)

			v, found := counterValue(t, reg, "cloudberry_gpfdist_reconcile_total")
			assert.True(t, found, "cloudberry_gpfdist_reconcile_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordGpfdistReconcile_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordGpfdistReconcile("cluster-a", "ns-a", "deployment", "success")
	recorder.RecordGpfdistReconcile("cluster-a", "ns-a", "deployment", "success")
	recorder.RecordGpfdistReconcile("cluster-b", "ns-b", "delete", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		successValue float64
		errorValue   float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_gpfdist_reconcile_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "success":
				successValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, successValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["success"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["success"]["namespace"])
	assert.Equal(t, "deployment", seenLabels["success"]["operation"])
	assert.Equal(t, "cluster-b", seenLabels["error"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["error"]["namespace"])
	assert.Equal(t, "delete", seenLabels["error"]["operation"])
}

func TestRecordPXFExtensionSetup(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"installed", "installed"},
		{"absent", "absent"},
		{"error", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordPXFExtensionSetup("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_pxf_extension_setup_total")
			assert.True(t, found, "cloudberry_pxf_extension_setup_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

func TestRecordPXFExtensionSetup_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordPXFExtensionSetup("cluster-a", "ns-a", "installed")
	recorder.RecordPXFExtensionSetup("cluster-a", "ns-a", "installed")
	recorder.RecordPXFExtensionSetup("cluster-b", "ns-b", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		installedValue float64
		errorValue     float64
		seenLabels     = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_pxf_extension_setup_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "installed":
				installedValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, installedValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["installed"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["installed"]["namespace"])
	assert.Equal(t, "cluster-b", seenLabels["error"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["error"]["namespace"])
}

// TestRecordDataLoaderRoleSetup verifies the PrometheusRecorder increments the
// cloudberry_dataloader_role_setup_total counter for each bounded result label
// (T-B1, GATE-CRITICAL: this is the only new 0.0%-covered statement).
func TestRecordDataLoaderRoleSetup(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"success", "success"},
		{"error", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordDataLoaderRoleSetup("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_dataloader_role_setup_total")
			assert.True(t, found, "cloudberry_dataloader_role_setup_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

// TestRecordDataLoaderRoleSetup_Labels verifies the counter is partitioned by
// the {cluster,namespace,result} label set and that NoopRecorder is a safe
// no-op that registers nothing.
func TestRecordDataLoaderRoleSetup_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordDataLoaderRoleSetup("cluster-a", "ns-a", "success")
	recorder.RecordDataLoaderRoleSetup("cluster-a", "ns-a", "success")
	recorder.RecordDataLoaderRoleSetup("cluster-b", "ns-b", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		successValue float64
		errorValue   float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_dataloader_role_setup_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "success":
				successValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, successValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["success"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["success"]["namespace"])
	assert.Equal(t, "cluster-b", seenLabels["error"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["error"]["namespace"])

	// NoopRecorder must be a safe no-op: it neither panics nor registers anything.
	noopReg := prometheus.NewRegistry()
	noop := &NoopRecorder{}
	assert.NotPanics(t, func() {
		noop.RecordDataLoaderRoleSetup("c", "ns", "success")
	})
	_, found := counterValue(t, noopReg, "cloudberry_dataloader_role_setup_total")
	assert.False(t, found, "NoopRecorder must not register the dataloader counter")
}

// TestRecordExporterRoleSetup verifies the PrometheusRecorder increments the
// cloudberry_exporter_role_setup_total counter for each bounded result label
// (T-1, GATE-CRITICAL: this is the only new 0.0%-covered statement).
func TestRecordExporterRoleSetup(t *testing.T) {
	tests := []struct {
		name   string
		result string
	}{
		{"success", "success"},
		{"error", "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)
			recorder.RecordExporterRoleSetup("test", "default", tt.result)

			v, found := counterValue(t, reg, "cloudberry_exporter_role_setup_total")
			assert.True(t, found, "cloudberry_exporter_role_setup_total should be registered")
			assert.InDelta(t, 1.0, v, 0.001)
		})
	}
}

// TestRecordExporterRoleSetup_Labels verifies the counter is partitioned by
// the {cluster,namespace,result} label set and that NoopRecorder is a safe
// no-op that registers nothing.
func TestRecordExporterRoleSetup_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordExporterRoleSetup("cluster-a", "ns-a", "success")
	recorder.RecordExporterRoleSetup("cluster-a", "ns-a", "success")
	recorder.RecordExporterRoleSetup("cluster-b", "ns-b", "error")

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		successValue float64
		errorValue   float64
		seenLabels   = map[string]map[string]string{}
	)
	for _, f := range families {
		if f.GetName() != "cloudberry_exporter_role_setup_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			seenLabels[labels["result"]] = labels
			switch labels["result"] {
			case "success":
				successValue = m.GetCounter().GetValue()
			case "error":
				errorValue = m.GetCounter().GetValue()
			}
		}
	}

	assert.InDelta(t, 2.0, successValue, 0.001)
	assert.InDelta(t, 1.0, errorValue, 0.001)
	assert.Equal(t, "cluster-a", seenLabels["success"]["cluster"])
	assert.Equal(t, "ns-a", seenLabels["success"]["namespace"])
	assert.Equal(t, "cluster-b", seenLabels["error"]["cluster"])
	assert.Equal(t, "ns-b", seenLabels["error"]["namespace"])

	// NoopRecorder must be a safe no-op: it neither panics nor registers anything.
	noopReg := prometheus.NewRegistry()
	noop := &NoopRecorder{}
	assert.NotPanics(t, func() {
		noop.RecordExporterRoleSetup("c", "ns", "success")
	})
	_, found := counterValue(t, noopReg, "cloudberry_exporter_role_setup_total")
	assert.False(t, found, "NoopRecorder must not register the exporter counter")
}
