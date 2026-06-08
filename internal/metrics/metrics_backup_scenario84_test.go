package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario 84 — Prometheus Metrics / gpbackup_exporter.
//
// These tests verify (no production code change) that every one of the 9
// backup-lifecycle metric recorders on PrometheusRecorder increments/sets the
// right metric family with the right label set and a queryable value, using a
// real prometheus.Registry round-trip (Gather). They explicitly guard GAP-A:
// cloudberry_backup_total / cloudberry_restore_total expose the outcome label
// `result` (success|failed), NOT `status`.
//
// The 9 metrics asserted here (namespace "cloudberry"):
//  1. backup_total{cluster,namespace,type,result}            -> RecordBackup
//  2. backup_duration_seconds{cluster,namespace,type} (hist) -> ObserveBackupDuration
//  3. backup_size_bytes{cluster,namespace,timestamp}         -> SetBackupSizeBytes
//  4. backup_last_success_timestamp{cluster,namespace}       -> SetBackupLastSuccessTimestamp
//  5. backup_last_status{cluster,namespace} (0/1/2)          -> SetBackupLastStatus
//  6. restore_total{cluster,namespace,result}                -> RecordRestore
//  7. restore_duration_seconds{cluster,namespace} (hist)     -> ObserveRestoreDuration
//  8. backup_retention_deleted_total{cluster,namespace}      -> RecordBackupRetentionDeleted
//  9. backup_job_status{cluster,namespace,job_name,operation} -> SetBackupJobStatus

const (
	s84Cluster = "scenario84-s3"
	s84NS      = "cloudberry-test"
)

// labelKeysForMetric gathers the sorted-or-not set of label keys present on the
// first series of the named metric family. It is used to assert the label
// CONTRACT (e.g. that backup_total carries `result` and never `status`).
func labelKeysForMetric(t *testing.T, reg *prometheus.Registry, name string) (map[string]struct{}, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		if len(f.GetMetric()) == 0 {
			return nil, false
		}
		keys := map[string]struct{}{}
		for _, l := range f.GetMetric()[0].GetLabel() {
			keys[l.GetName()] = struct{}{}
		}
		return keys, true
	}
	return nil, false
}

// TestScenario84_M1_BackupTotal_FullAndIncremental asserts M1: backup_total is a
// counter labelled {cluster,namespace,type,result}; full+success and
// incremental+success each increment to 1 on their own series, and a failed
// full increments the {type=full,result=failed} series independently.
func TestScenario84_M1_BackupTotal_FullAndIncremental(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.RecordBackup(s84Cluster, s84NS, "full", "success")
	rec.RecordBackup(s84Cluster, s84NS, "incremental", "success")
	rec.RecordBackup(s84Cluster, s84NS, "full", "failed")

	full := valueWithLabels(t, reg, "cloudberry_backup_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "type": "full", "result": "success",
	})
	assert.InDelta(t, 1.0, full, 0.001)

	incr := valueWithLabels(t, reg, "cloudberry_backup_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "type": "incremental", "result": "success",
	})
	assert.InDelta(t, 1.0, incr, 0.001)

	failed := valueWithLabels(t, reg, "cloudberry_backup_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "type": "full", "result": "failed",
	})
	assert.InDelta(t, 1.0, failed, 0.001)
}

// TestScenario84_M1_BackupTotal_LabelContract guards GAP-A: the implemented
// outcome label is `result` (NOT `status`). Renaming it would break dashboards
// built across Scenarios 76-83, so this test pins the contract.
func TestScenario84_M1_BackupTotal_LabelContract(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)
	rec.RecordBackup(s84Cluster, s84NS, "full", "success")

	keys, ok := labelKeysForMetric(t, reg, "cloudberry_backup_total")
	require.True(t, ok, "cloudberry_backup_total must have at least one series")
	assert.Contains(t, keys, "result", "backup_total must expose the `result` label")
	assert.NotContains(t, keys, "status", "backup_total must NOT expose a `status` label")
	assert.Contains(t, keys, "type")
	assert.Contains(t, keys, "cluster")
	assert.Contains(t, keys, "namespace")
}

// TestScenario84_M2_BackupDuration_PerType asserts M2: backup_duration_seconds
// is a histogram populated per `type` — the _count for full and incremental are
// each >= 1 after one observation per type.
func TestScenario84_M2_BackupDuration_PerType(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.ObserveBackupDuration(s84Cluster, s84NS, "full", 42*time.Second)
	rec.ObserveBackupDuration(s84Cluster, s84NS, "incremental", 7*time.Second)

	fullCount := valueWithLabels(t, reg, "cloudberry_backup_duration_seconds", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "type": "full",
	})
	assert.GreaterOrEqual(t, fullCount, 1.0)

	incrCount := valueWithLabels(t, reg, "cloudberry_backup_duration_seconds", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "type": "incremental",
	})
	assert.GreaterOrEqual(t, incrCount, 1.0)
}

// TestScenario84_M3_BackupSizeBytes asserts M3: backup_size_bytes is a gauge
// labelled by {cluster,namespace,timestamp} set to the annotated byte count.
func TestScenario84_M3_BackupSizeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.SetBackupSizeBytes(s84Cluster, s84NS, "20260608205700", 104857600)

	got := valueWithLabels(t, reg, "cloudberry_backup_size_bytes", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "timestamp": "20260608205700",
	})
	assert.InDelta(t, 104857600.0, got, 0.5)
	assert.Positive(t, got)
}

// TestScenario84_M4_BackupLastSuccessTimestamp asserts M4: the last-success
// timestamp gauge is set to the provided unix value.
func TestScenario84_M4_BackupLastSuccessTimestamp(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	now := float64(time.Now().Unix())
	rec.SetBackupLastSuccessTimestamp(s84Cluster, s84NS, now)

	got := valueWithLabels(t, reg, "cloudberry_backup_last_success_timestamp", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS,
	})
	assert.InDelta(t, now, got, 0.5)
}

// TestScenario84_M5_BackupLastStatus_AllCodes asserts M5: the last-status gauge
// reflects 0 (success), 1 (failed) and 2 (in-progress) across the three calls
// (the gauge holds the most recent value for the {cluster,namespace} series).
func TestScenario84_M5_BackupLastStatus_AllCodes(t *testing.T) {
	tests := []struct {
		name string
		code float64
	}{
		{"success", 0},
		{"failed", 1},
		{"in-progress", 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			rec := NewPrometheusRecorder(reg)
			rec.SetBackupLastStatus(s84Cluster, s84NS, tc.code)

			got := valueWithLabels(t, reg, "cloudberry_backup_last_status", map[string]string{
				"cluster": s84Cluster, "namespace": s84NS,
			})
			assert.InDelta(t, tc.code, got, 0.001)
		})
	}
}

// TestScenario84_M6_RestoreTotal asserts M6: restore_total is a counter labelled
// {cluster,namespace,result}; a success and a failed land on independent series.
func TestScenario84_M6_RestoreTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.RecordRestore(s84Cluster, s84NS, "success")
	rec.RecordRestore(s84Cluster, s84NS, "failed")

	success := valueWithLabels(t, reg, "cloudberry_restore_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "result": "success",
	})
	assert.InDelta(t, 1.0, success, 0.001)

	failed := valueWithLabels(t, reg, "cloudberry_restore_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS, "result": "failed",
	})
	assert.InDelta(t, 1.0, failed, 0.001)
}

// TestScenario84_M6_RestoreTotal_LabelContract guards GAP-A for restore_total:
// the outcome label is `result`, never `status`.
func TestScenario84_M6_RestoreTotal_LabelContract(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)
	rec.RecordRestore(s84Cluster, s84NS, "success")

	keys, ok := labelKeysForMetric(t, reg, "cloudberry_restore_total")
	require.True(t, ok, "cloudberry_restore_total must have at least one series")
	assert.Contains(t, keys, "result", "restore_total must expose the `result` label")
	assert.NotContains(t, keys, "status", "restore_total must NOT expose a `status` label")
}

// TestScenario84_M7_RestoreDuration asserts M7: restore_duration_seconds is a
// histogram with _count >= 1 after an observation.
func TestScenario84_M7_RestoreDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.ObserveRestoreDuration(s84Cluster, s84NS, 30*time.Second)

	count := valueWithLabels(t, reg, "cloudberry_restore_duration_seconds", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS,
	})
	assert.GreaterOrEqual(t, count, 1.0)
}

// TestScenario84_M8_RetentionDeleted asserts M8: backup_retention_deleted_total
// increments by N on each call (here 3 then 2 => 5).
func TestScenario84_M8_RetentionDeleted(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.RecordBackupRetentionDeleted(s84Cluster, s84NS, 3)
	rec.RecordBackupRetentionDeleted(s84Cluster, s84NS, 2)

	got := valueWithLabels(t, reg, "cloudberry_backup_retention_deleted_total", map[string]string{
		"cluster": s84Cluster, "namespace": s84NS,
	})
	assert.InDelta(t, 5.0, got, 0.001)
}

// TestScenario84_M9_BackupJobStatus_AllCodes asserts M9: backup_job_status is a
// gauge labelled {cluster,namespace,job,operation} set to 0/1/2/3 for the four
// lifecycle codes across the three operation kinds.
func TestScenario84_M9_BackupJobStatus_AllCodes(t *testing.T) {
	tests := []struct {
		name      string
		job       string
		operation string
		code      float64
	}{
		{"backup pending", "scenario84-s3-backup-1", "backup", 0},
		{"backup running", "scenario84-s3-backup-2", "backup", 1},
		{"backup succeeded", "scenario84-s3-backup-3", "backup", 2},
		{"backup failed", "scenario84-s3-backup-4", "backup", 3},
		{"restore succeeded", "scenario84-s3-restore-1", "restore", 2},
		{"cleanup succeeded", "scenario84-s3-cleanup-1", "cleanup", 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			rec := NewPrometheusRecorder(reg)
			rec.SetBackupJobStatus(s84Cluster, s84NS, tc.job, tc.operation, tc.code)

			got := valueWithLabels(t, reg, "cloudberry_backup_job_status", map[string]string{
				"cluster": s84Cluster, "namespace": s84NS,
				"job_name": tc.job, "operation": tc.operation,
			})
			assert.InDelta(t, tc.code, got, 0.001)
		})
	}
}

// TestScenario84_AllNineMetrics_RegistryRoundTrip is the consolidated GAP-B
// guard: after invoking all 9 recorders once, every one of the 9 metric
// families is present in the registry with the Scenario-84 label set and a
// queryable value. This proves the full backup lifecycle populates all metrics.
func TestScenario84_AllNineMetrics_RegistryRoundTrip(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	// Drive each of the 9 recorders with Scenario-84 sample values.
	rec.RecordBackup(s84Cluster, s84NS, "full", "success")                           // M1
	rec.ObserveBackupDuration(s84Cluster, s84NS, "full", 60*time.Second)             // M2
	rec.SetBackupSizeBytes(s84Cluster, s84NS, "20260608205700", 104857600)           // M3
	rec.SetBackupLastSuccessTimestamp(s84Cluster, s84NS, 1.7e9)                      // M4
	rec.SetBackupLastStatus(s84Cluster, s84NS, 0)                                    // M5
	rec.RecordRestore(s84Cluster, s84NS, "success")                                  // M6
	rec.ObserveRestoreDuration(s84Cluster, s84NS, 15*time.Second)                    // M7
	rec.RecordBackupRetentionDeleted(s84Cluster, s84NS, 3)                           // M8
	rec.SetBackupJobStatus(s84Cluster, s84NS, "scenario84-s3-backup-1", "backup", 2) // M9

	type check struct {
		name   string
		labels map[string]string
	}
	base := map[string]string{"cluster": s84Cluster, "namespace": s84NS}
	withType := func(t string) map[string]string {
		m := map[string]string{"cluster": s84Cluster, "namespace": s84NS, "type": t}
		return m
	}
	checks := []check{
		{"cloudberry_backup_total", map[string]string{
			"cluster": s84Cluster, "namespace": s84NS, "type": "full", "result": "success",
		}},
		{"cloudberry_backup_duration_seconds", withType("full")},
		{"cloudberry_backup_size_bytes", map[string]string{
			"cluster": s84Cluster, "namespace": s84NS, "timestamp": "20260608205700",
		}},
		{"cloudberry_backup_last_success_timestamp", base},
		{"cloudberry_backup_last_status", base},
		{"cloudberry_restore_total", map[string]string{
			"cluster": s84Cluster, "namespace": s84NS, "result": "success",
		}},
		{"cloudberry_restore_duration_seconds", base},
		{"cloudberry_backup_retention_deleted_total", base},
		{"cloudberry_backup_job_status", map[string]string{
			"cluster": s84Cluster, "namespace": s84NS,
			"job_name": "scenario84-s3-backup-1", "operation": "backup",
		}},
	}
	require.Len(t, checks, 9, "Scenario 84 defines exactly 9 backup-lifecycle metrics")

	for _, c := range checks {
		assert.True(t, metricExists(t, reg, c.name, c.labels),
			"Scenario-84 metric %s with labels %v must be present", c.name, c.labels)
	}
}
