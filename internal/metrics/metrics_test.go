package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// All methods should not panic
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
}

func TestNoopRecorder_ImplementsInterface(t *testing.T) {
	var _ Recorder = &NoopRecorder{}
}

func TestPrometheusRecorder_ImplementsInterface(t *testing.T) {
	var _ Recorder = &PrometheusRecorder{}
}
