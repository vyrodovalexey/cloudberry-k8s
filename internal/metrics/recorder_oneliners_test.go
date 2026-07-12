package metrics

// Registry-level tests for the previously-untested one-line PrometheusRecorder
// methods (handoff task T-10, section 6 list of the review report): each method
// must hit the correct metric family with the correct labels and value, and no
// other series may appear as a side effect.

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestPrometheusRecorder_OneLineMethods(t *testing.T) {
	const (
		cluster   = "test-cluster"
		namespace = "default"
	)

	tests := []struct {
		name   string
		record func(r *PrometheusRecorder)
		family string
		labels map[string]string
		want   float64
		absent map[string]string // optional: labels that must NOT exist on the family
	}{
		{
			name:   "SetRecommendationScanCronJob provisioned",
			record: func(r *PrometheusRecorder) { r.SetRecommendationScanCronJob(cluster, namespace, 1) },
			family: "cloudberry_recommendation_scan_cronjob",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		{
			name:   "SetRecommendationScanCronJob removed",
			record: func(r *PrometheusRecorder) { r.SetRecommendationScanCronJob(cluster, namespace, 0) },
			family: "cloudberry_recommendation_scan_cronjob",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   0,
		},
		{
			name: "RecordPasswordRotation increments label-less counter",
			record: func(r *PrometheusRecorder) {
				r.RecordPasswordRotation()
				r.RecordPasswordRotation()
			},
			family: "cloudberry_password_rotation_total",
			labels: nil,
			want:   2,
		},
		{
			name:   "RecordPlanCheck",
			record: func(r *PrometheusRecorder) { r.RecordPlanCheck(cluster, namespace) },
			family: "cloudberry_plan_check_total",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		{
			name: "RecordPlanCheckIssue carries severity and category",
			record: func(r *PrometheusRecorder) {
				r.RecordPlanCheckIssue(cluster, namespace, "warning", "seq_scan")
			},
			family: "cloudberry_plan_check_issues_total",
			labels: map[string]string{
				"cluster": cluster, "namespace": namespace,
				"severity": "warning", "category": "seq_scan",
			},
			want:   1,
			absent: map[string]string{"severity": "critical"},
		},
		{
			name: "ObservePlanCheckDuration observes one sample",
			record: func(r *PrometheusRecorder) {
				r.ObservePlanCheckDuration(cluster, namespace, 150*time.Millisecond)
			},
			family: "cloudberry_plan_check_duration_seconds",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1, // histogram sample count
		},
		{
			name:   "RecordQueryCancel",
			record: func(r *PrometheusRecorder) { r.RecordQueryCancel(cluster, namespace) },
			family: "cloudberry_query_cancel_total",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		{
			name:   "RecordQueryMove",
			record: func(r *PrometheusRecorder) { r.RecordQueryMove(cluster, namespace) },
			family: "cloudberry_query_move_total",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		{
			name:   "RecordExporterHealthCheck",
			record: func(r *PrometheusRecorder) { r.RecordExporterHealthCheck(cluster, namespace) },
			family: "cloudberry_exporter_health_check_total",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		{
			name:   "RecordActiveQueryExport increments label-less counter",
			record: func(r *PrometheusRecorder) { r.RecordActiveQueryExport() },
			family: "cloudberry_active_query_export_total",
			labels: nil,
			want:   1,
		},
		{
			name:   "RecordGuestAccess allowed",
			record: func(r *PrometheusRecorder) { r.RecordGuestAccess(cluster, namespace, true) },
			family: "cloudberry_guest_access_total",
			labels: map[string]string{
				"cluster": cluster, "namespace": namespace, "result": "allowed",
			},
			want:   1,
			absent: map[string]string{"result": "denied"},
		},
		{
			name:   "RecordGuestAccess denied",
			record: func(r *PrometheusRecorder) { r.RecordGuestAccess(cluster, namespace, false) },
			family: "cloudberry_guest_access_total",
			labels: map[string]string{
				"cluster": cluster, "namespace": namespace, "result": "denied",
			},
			want:   1,
			absent: map[string]string{"result": "allowed"},
		},
		{
			name:   "RecordMonitoringDisabledAccess",
			record: func(r *PrometheusRecorder) { r.RecordMonitoringDisabledAccess(cluster, namespace) },
			family: "cloudberry_monitoring_disabled_access_total",
			labels: map[string]string{"cluster": cluster, "namespace": namespace},
			want:   1,
		},
		// DEV-3 cert/vault-watch observability methods (UT-16).
		{
			name:   "RecordCABundleInjection success",
			record: func(r *PrometheusRecorder) { r.RecordCABundleInjection(ResultSuccess) },
			family: "cloudberry_webhook_ca_bundle_injection_total",
			labels: map[string]string{"result": ResultSuccess},
			want:   1,
			absent: map[string]string{"result": ResultError},
		},
		{
			name:   "RecordCABundleInjection error",
			record: func(r *PrometheusRecorder) { r.RecordCABundleInjection(ResultError) },
			family: "cloudberry_webhook_ca_bundle_injection_total",
			labels: map[string]string{"result": ResultError},
			want:   1,
			absent: map[string]string{"result": ResultSuccess},
		},
		{
			name: "IncCertRotationCheckError carries the component label",
			record: func(r *PrometheusRecorder) {
				r.IncCertRotationCheckError("webhook")
				r.IncCertRotationCheckError("webhook")
			},
			family: "cloudberry_cert_rotation_check_errors_total",
			labels: map[string]string{"component": "webhook"},
			want:   2,
			absent: map[string]string{"component": "cluster"},
		},
		{
			name: "SetVaultWatchLastSuccess sets the gauge to the timestamp",
			record: func(r *PrometheusRecorder) {
				r.SetVaultWatchLastSuccess("secret/data/app", 1767225600)
			},
			family: "cloudberry_vault_watch_last_success_timestamp",
			labels: map[string]string{"path": "secret/data/app"},
			want:   1767225600,
			absent: map[string]string{"path": "secret/data/other"},
		},
		{
			name: "SetVaultWatchLastSuccess overwrites (gauge, not counter)",
			record: func(r *PrometheusRecorder) {
				r.SetVaultWatchLastSuccess("secret/data/app", 1767225600)
				r.SetVaultWatchLastSuccess("secret/data/app", 1767225700)
			},
			family: "cloudberry_vault_watch_last_success_timestamp",
			labels: map[string]string{"path": "secret/data/app"},
			want:   1767225700,
		},
		{
			name:   "IncVaultWatchError carries the path label",
			record: func(r *PrometheusRecorder) { r.IncVaultWatchError("secret/data/app") },
			family: "cloudberry_vault_watch_errors_total",
			labels: map[string]string{"path": "secret/data/app"},
			want:   1,
			absent: map[string]string{"path": "secret/data/other"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: a fresh registry per case keeps the series isolated.
			reg := prometheus.NewRegistry()
			rec := NewPrometheusRecorder(reg)

			// Act.
			tt.record(rec)

			// Assert: exact family, labels and value.
			assert.InDelta(t, tt.want, valueWithLabels(t, reg, tt.family, tt.labels), 0.0001)
			if tt.absent != nil {
				assert.False(t, metricExists(t, reg, tt.family, tt.absent),
					"labels %v must not leak onto family %s", tt.absent, tt.family)
			}
		})
	}
}

// TestPrometheusRecorder_OneLineMethods_NoCrossFamilyLeak pins that recording
// one family does not create series on a sibling family (honest metrics: the
// registry only carries what was recorded).
func TestPrometheusRecorder_OneLineMethods_NoCrossFamilyLeak(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.RecordQueryCancel("c", "ns")

	assert.True(t, metricExists(t, reg, "cloudberry_query_cancel_total",
		map[string]string{"cluster": "c", "namespace": "ns"}))
	assert.False(t, metricExists(t, reg, "cloudberry_query_move_total",
		map[string]string{"cluster": "c", "namespace": "ns"}),
		"RecordQueryCancel must not touch the query_move family")
	assert.False(t, metricExists(t, reg, "cloudberry_plan_check_total",
		map[string]string{"cluster": "c", "namespace": "ns"}),
		"RecordQueryCancel must not touch the plan_check family")
}

// TestPrometheusRecorder_CertVaultWatchMethods_NoCrossFamilyLeak pins that the
// DEV-3 cert/vault-watch families stay independent of each other (UT-16): a
// label-name typo or a copy-paste of the wrong vector would surface here.
func TestPrometheusRecorder_CertVaultWatchMethods_NoCrossFamilyLeak(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	rec.IncVaultWatchError("secret/data/app")

	assert.True(t, metricExists(t, reg, "cloudberry_vault_watch_errors_total",
		map[string]string{"path": "secret/data/app"}))
	assert.False(t, metricExists(t, reg, "cloudberry_vault_watch_last_success_timestamp",
		map[string]string{"path": "secret/data/app"}),
		"IncVaultWatchError must not touch the last-success gauge")
	assert.False(t, metricExists(t, reg, "cloudberry_webhook_ca_bundle_injection_total", nil),
		"IncVaultWatchError must not touch the CA-bundle-injection family")
	assert.False(t, metricExists(t, reg, "cloudberry_cert_rotation_check_errors_total", nil),
		"IncVaultWatchError must not touch the rotation-check-errors family")
}

// TestNoopRecorder_CertVaultWatchMethods pins the NoopRecorder delegation
// contract for the DEV-3 methods: callable through the Recorder interface
// without panicking (nil-safe no-ops), matching the wider noop tests (UT-16).
func TestNoopRecorder_CertVaultWatchMethods(t *testing.T) {
	var r Recorder = &NoopRecorder{}

	r.RecordCABundleInjection(ResultSuccess)
	r.RecordCABundleInjection(ResultError)
	r.IncCertRotationCheckError("webhook")
	r.SetVaultWatchLastSuccess("secret/data/app", 1767225600)
	r.IncVaultWatchError("secret/data/app")
}
