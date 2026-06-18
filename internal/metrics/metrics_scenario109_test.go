package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Scenario 109 — All Prometheus Metrics (M.1-M.16)
//
// HONESTY is the point of this scenario: a metric value is emitted ONLY from a
// real source, or it stays honestly ABSENT (not fabricated) when no real source
// exists. These tests lock in both the emit-from-real-source behavior (M.1,
// M.10) and the no-fabrication decision (M.4/M.5/M.7/M.15/M.16 + synthetic M.6).
// ============================================================================

// familyExists reports whether the named metric family is present in the
// registry's gathered output (regardless of labels/samples).
func familyExists(t *testing.T, reg *prometheus.Registry, name string) bool {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return true
		}
	}
	return false
}

// TestSetPXFServiceUp_Scenario109 covers 109-M1-U: SetPXFServiceUp writes a
// per-segment-host pxf_service_up gauge with the correct {cluster,namespace,
// segment_host} labels and the 1/0 value from real readiness. Distinct hosts are
// independent series, and the gauge is last-wins (a host flipping to 0 overwrites
// its prior 1 — exactly what killing a segment's pxf container drives).
func TestSetPXFServiceUp_Scenario109(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	const name = "cloudberry_pxf_service_up"
	seg0 := map[string]string{"cluster": "test", "namespace": "default", "segment_host": "seg-0"}
	seg1 := map[string]string{"cluster": "test", "namespace": "default", "segment_host": "seg-1"}

	// seg-0 pxf Ready → 1; seg-1 pxf NOT Ready → 0.
	recorder.SetPXFServiceUp("test", "default", "seg-0", 1)
	recorder.SetPXFServiceUp("test", "default", "seg-1", 0)

	assert.Equal(t, 1.0, valueWithLabels(t, reg, name, seg0))
	assert.Equal(t, 0.0, valueWithLabels(t, reg, name, seg1))

	// Last-wins: seg-0's pxf container goes down → its series flips to 0 without
	// touching seg-1 (honest per-segment disaggregation).
	recorder.SetPXFServiceUp("test", "default", "seg-0", 0)
	assert.Equal(t, 0.0, valueWithLabels(t, reg, name, seg0))
	assert.Equal(t, 0.0, valueWithLabels(t, reg, name, seg1))
}

// TestSetPXFServiceUp_UnobservedNotEmitted covers 109-M1-U honesty: with no
// SetPXFServiceUp call (no observed segment pods), the pxf_service_up series for
// a host must NOT exist — the gauge never fabricates a host that was not observed.
func TestSetPXFServiceUp_UnobservedNotEmitted(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewPrometheusRecorder(reg)

	assert.False(t, metricExists(t, reg, "cloudberry_pxf_service_up",
		map[string]string{"cluster": "test", "namespace": "default", "segment_host": "seg-0"}),
		"an unobserved segment host must not emit a pxf_service_up sample")
}

// TestRecordDataLoadingBytes_Scenario109 covers 109-M10-U: RecordDataLoadingBytes
// increments data_loading_bytes_total with the {cluster,namespace,job,source_type}
// labels, and the counter accumulates across harvests (each DATALOAD_BYTES marker
// adds up). Distinct source_type values are independent series.
func TestRecordDataLoadingBytes_Scenario109(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	const name = "cloudberry_data_loading_bytes_total"
	labels := map[string]string{
		"cluster": "test", "namespace": "default",
		"job": "gpload-csv", "source_type": "gpfdist",
	}

	recorder.RecordDataLoadingBytes("test", "default", "gpload-csv", "gpfdist", 12345)
	assert.Equal(t, 12345.0, valueWithLabels(t, reg, name, labels))

	// Counter accumulates: a second harvest adds to the total.
	recorder.RecordDataLoadingBytes("test", "default", "gpload-csv", "gpfdist", 55)
	assert.Equal(t, 12400.0, valueWithLabels(t, reg, name, labels))

	// A different source_type is an independent series.
	s3 := map[string]string{
		"cluster": "test", "namespace": "default",
		"job": "s3-loader", "source_type": "s3",
	}
	recorder.RecordDataLoadingBytes("test", "default", "s3-loader", "s3", 999)
	assert.Equal(t, 999.0, valueWithLabels(t, reg, name, s3))
	// The original series is unaffected.
	assert.Equal(t, 12400.0, valueWithLabels(t, reg, name, labels))
}

// TestRecordDataLoadingBytes_UnharvestedNotEmitted covers 109-M10-ABSENT honesty:
// when no DATALOAD_BYTES marker is harvested the controller never calls
// RecordDataLoadingBytes, so the series must NOT exist for that job — bytes are
// never synthesized.
func TestRecordDataLoadingBytes_UnharvestedNotEmitted(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewPrometheusRecorder(reg)

	assert.False(t, metricExists(t, reg, "cloudberry_data_loading_bytes_total",
		map[string]string{
			"cluster": "test", "namespace": "default",
			"job": "remote-loader", "source_type": "gpfdist",
		}),
		"a load with no harvested DATALOAD_BYTES marker must not emit a bytes sample")
}

// TestNoopRecorder_Scenario109 covers the NoopRecorder contract for the new
// M.1/M.10 recorders: both are safe no-ops (no panic), so test/non-metric setups
// never crash on the new signals.
func TestNoopRecorder_Scenario109(t *testing.T) {
	var r Recorder = &NoopRecorder{}
	assert.NotPanics(t, func() {
		r.SetPXFServiceUp("c", "n", "seg-0", 1)
		r.SetPXFServiceUp("c", "n", "seg-1", 0)
		r.RecordDataLoadingBytes("c", "n", "job", "s3", 1024)
	})
}

// TestHonesty_AbsentMetricsNeverRegistered is the 109-HONESTY regression lock
// (109-M4/M5/M7/M15/M16-ABSENT + synthetic M.6): it gathers the operator's
// registry — after touching EVERY metric the recorder can emit — and asserts the
// deliberately-absent PXF/gpfdist metric families are NOT registered. This is a
// PASSING test that locks in the no-fabrication decision against future drift: if
// someone later registers any of these synthetic families it FAILS loudly.
//
// The absent families (no honest source in PXF 2.1.0 / gpfdist):
//   - M.4  cloudberry_pxf_bytes_transferred_total
//   - M.5  cloudberry_pxf_records_total      (substituted by data_loading_rows_total)
//   - M.7  cloudberry_pxf_active_connections (tomcat.threads.busy is a mislabeled proxy)
//   - M.15 cloudberry_gpfdist_connections_active (no scrapable endpoint)
//   - M.16 cloudberry_gpfdist_bytes_served_total (log file only)
//   - M.6  cloudberry_pxf_errors_total       (folded into data_loading_errors_total)
func TestHonesty_AbsentMetricsNeverRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	// Touch the M.1/M.10 (and the honest substitutes) so the registry is "warm":
	// the absent-metric assertion is meaningful even after real emissions.
	recorder.SetPXFServiceUp("test", "default", "seg-0", 1)
	recorder.RecordDataLoadingBytes("test", "default", "loader", "s3", 4096)
	recorder.RecordDataLoadingRows("test", "default", "loader", "s3", 100) // M.5 substitute
	recorder.RecordDataLoadingErrors("test", "default", "loader")          // M.6 honest fold

	absent := []string{
		"cloudberry_pxf_bytes_transferred_total", // M.4
		"cloudberry_pxf_records_total",           // M.5
		"cloudberry_pxf_active_connections",      // M.7
		"cloudberry_gpfdist_connections_active",  // M.15
		"cloudberry_gpfdist_bytes_served_total",  // M.16
		"cloudberry_pxf_errors_total",            // synthetic M.6 (must NOT exist)
	}
	for _, name := range absent {
		assert.False(t, familyExists(t, reg, name),
			"HONESTY: %s must NOT be registered (no honest source — never fabricate)", name)
	}

	// And the honest substitutes ARE present (M.5 → rows_total, M.6 → errors_total),
	// proving the absence is a deliberate fold, not a coverage gap.
	assert.True(t, familyExists(t, reg, "cloudberry_data_loading_rows_total"),
		"M.5 record throughput must be observable via the honest data_loading_rows_total")
	assert.True(t, familyExists(t, reg, "cloudberry_data_loading_errors_total"),
		"M.6 load errors must be observable via the honest data_loading_errors_total")
}
