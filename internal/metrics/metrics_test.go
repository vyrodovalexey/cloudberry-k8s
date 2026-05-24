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
	recorder.ObserveBackupDuration("c", "n", time.Second)
	recorder.SetBackupSizeBytes("c", "n", 1024)
	recorder.RecordRestore("c", "n", "success")
	recorder.SetDataLoadingJobsActive("c", "n", 1)
	recorder.RecordDataLoadingRows("c", "n", "job1", "s3", 100)
	recorder.SetDiskUsagePercent("c", "n", 50)
	recorder.SetRecommendationsTotal("c", "n", "bloat", 3)
	recorder.ObserveRecommendationScanDuration("c", "n", time.Second)
	recorder.SetTableBloatRatio("c", "n", "public.t", 0.1)
	recorder.RecordScaleOperation("c", "n", "scale-out")
	recorder.SetRedistributionProgress("c", "n", 0.75)
	recorder.SetDataSkewCoefficient("c", "n", 0.15)
	recorder.SetPVCSizeBytes("c", "n", "coordinator", 10737418240)
	recorder.RecordMirroringOperation("c", "n", "enable")
	recorder.RecordMaintenanceOperation("c", "n", "vacuum")
	recorder.RecordPasswordRotation()
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
	recorder.ObserveBackupDuration("test", "default", 30*time.Second)
}

func TestSetBackupSizeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetBackupSizeBytes("test", "default", 1073741824)
}

func TestRecordRestore(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordRestore("test", "default", "success")
}

func TestSetDataLoadingJobsActive(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.SetDataLoadingJobsActive("test", "default", 3)
}

func TestRecordDataLoadingRows(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewPrometheusRecorder(reg)
	recorder.RecordDataLoadingRows("test", "default", "s3-loader", "s3", 1000)
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
	r.ObserveBackupDuration("cluster", "ns", 30*time.Second)
	r.SetBackupSizeBytes("cluster", "ns", 1073741824)
	r.RecordRestore("cluster", "ns", "success")
	r.SetDataLoadingJobsActive("cluster", "ns", 3)
	r.RecordDataLoadingRows("cluster", "ns", "s3-loader", "s3", 1000)
	r.SetDiskUsagePercent("cluster", "ns", 75.5)
	r.SetRecommendationsTotal("cluster", "ns", "bloat", 5)
	r.ObserveRecommendationScanDuration("cluster", "ns", 45*time.Second)
	r.SetTableBloatRatio("cluster", "ns", "public.orders", 0.25)
	r.RecordScaleOperation("cluster", "ns", "scale-out")
	r.SetRedistributionProgress("cluster", "ns", 0.75)
	r.SetDataSkewCoefficient("cluster", "ns", 0.15)
	r.SetPVCSizeBytes("cluster", "ns", "coordinator", 10737418240)
	r.RecordMirroringOperation("cluster", "ns", "enable")
	r.RecordMaintenanceOperation("cluster", "ns", "vacuum")
	r.RecordPasswordRotation()
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
