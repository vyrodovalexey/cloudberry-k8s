package metrics

// Tests for cloudberry_steady_state_refresh_errors_total (task T12/E1): the
// counter is registered with the expected name/labels, increments per
// component, and the NoopRecorder surface stays panic-free.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestRecordSteadyStateRefreshError(t *testing.T) {
	tests := []struct {
		name       string
		component  string
		increments int
	}{
		{name: "backup component", component: "backup", increments: 1},
		{name: "dataloading component", component: "dataloading", increments: 2},
		{name: "storage component", component: "storage", increments: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recorder := NewPrometheusRecorder(reg)

			for i := 0; i < tt.increments; i++ {
				recorder.RecordSteadyStateRefreshError("test-cluster", "default", tt.component)
			}

			got := valueWithLabels(t, reg, "cloudberry_steady_state_refresh_errors_total",
				map[string]string{
					"cluster":   "test-cluster",
					"namespace": "default",
					"component": tt.component,
				})
			assert.InDelta(t, float64(tt.increments), got, 0.0001)
		})
	}
}

func TestRecordSteadyStateRefreshError_LabelsAreIndependent(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)

	recorder.RecordSteadyStateRefreshError("a", "ns1", "backup")
	recorder.RecordSteadyStateRefreshError("b", "ns2", "storage")

	assert.InDelta(t, 1.0, valueWithLabels(t, reg,
		"cloudberry_steady_state_refresh_errors_total",
		map[string]string{"cluster": "a", "namespace": "ns1", "component": "backup"}), 0.0001)
	assert.InDelta(t, 1.0, valueWithLabels(t, reg,
		"cloudberry_steady_state_refresh_errors_total",
		map[string]string{"cluster": "b", "namespace": "ns2", "component": "storage"}), 0.0001)
	assert.False(t, metricExists(t, reg, "cloudberry_steady_state_refresh_errors_total",
		map[string]string{"cluster": "a", "namespace": "ns1", "component": "storage"}),
		"series must not leak across label sets")
}

func TestNoopRecorder_RecordSteadyStateRefreshError(t *testing.T) {
	n := &NoopRecorder{}
	assert.NotPanics(t, func() {
		n.RecordSteadyStateRefreshError("c", "ns", "backup")
	})
}
