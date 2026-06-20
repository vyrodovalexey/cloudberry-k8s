package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario 118 — Scan Scheduling and Duration Limit (metrics layer).
//
// Direct unit coverage of the two recommendation-scan metrics the controller's
// C.10 / M.3 path drives on the real PrometheusRecorder:
//   - cloudberry_recommendation_scan_truncated_total (counter, 118b);
//   - cloudberry_recommendation_scan_duration_seconds (histogram, M.3).
// Plus the NoopRecorder no-op surface (used as the default test recorder).

// TestScenario118_IncRecommendationScanTruncated proves the truncated-scan
// counter is registered under {cluster,namespace} and increments per call.
func TestScenario118_IncRecommendationScanTruncated(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.IncRecommendationScanTruncated("test", "default")
	recorder.IncRecommendationScanTruncated("test", "default")

	v := valueWithLabels(t, reg, "cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test", "namespace": "default"})
	assert.InDelta(t, 2.0, v, 0.001,
		"the truncated counter must increment once per call")
}

// TestScenario118_ObserveRecommendationScanDuration proves the M.3 histogram is
// registered and records one sample per observation (capped or not).
func TestScenario118_ObserveRecommendationScanDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.ObserveRecommendationScanDuration("test", "default", 10*time.Millisecond)

	cnt, found := histogramSampleCount(t, reg, "cloudberry_recommendation_scan_duration_seconds")
	require.True(t, found, "the scan-duration histogram must be registered")
	assert.Equal(t, uint64(1), cnt)
}

// TestScenario118_NoopRecorder_ScanMetrics proves the NoopRecorder surface for
// the Scenario 118 metrics is a safe no-op (no panic, no registration).
func TestScenario118_NoopRecorder_ScanMetrics(t *testing.T) {
	recorder := &NoopRecorder{}
	require.NotPanics(t, func() {
		recorder.IncRecommendationScanTruncated("c", "n")
		recorder.ObserveRecommendationScanDuration("c", "n", time.Second)
	})
}
