package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExporterAndBackupNames exercises the exporter, monitoring, and backup
// related name builders. Each is a deterministic SanitizeK8sName wrapper, so we
// assert the expected suffix and that the result is a valid (sanitized,
// lowercase, <=63 char) Kubernetes name.
func TestExporterAndBackupNames(t *testing.T) {
	t.Parallel()

	const cluster = "My_Cluster"

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ExporterCredentialsSecretName", ExporterCredentialsSecretName(cluster), "my-cluster-exporter-credentials"},
		{"ExporterQueriesConfigMapName", ExporterQueriesConfigMapName(cluster), "my-cluster-exporter-queries"},
		{"ExporterMetricsServiceName", ExporterMetricsServiceName(cluster), "my-cluster-exporter-metrics"},
		{"NodeExporterDaemonSetName", NodeExporterDaemonSetName(cluster), "my-cluster-node-exporter"},
		{"QueryMetricsServiceMonitorName", QueryMetricsServiceMonitorName(cluster), "my-cluster-query-metrics"},
		{"QueryAlertsPrometheusRuleName", QueryAlertsPrometheusRuleName(cluster), "my-cluster-query-alerts"},
		{"BackupS3ConfigMapName", BackupS3ConfigMapName(cluster), "my-cluster-backup-s3-config"},
		{"BackupS3VaultCredentialsSecretName", BackupS3VaultCredentialsSecretName(cluster), "my-cluster-backup-s3-vault-creds"},
		{"BackupCronJobName", BackupCronJobName(cluster), "my-cluster-backup-schedule"},
		{"BackupServiceAccountName", BackupServiceAccountName(cluster), "cloudberry-backup-sa"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
			assert.LessOrEqual(t, len(tc.got), 63)
			assert.Equal(t, strings.ToLower(tc.got), tc.got)
		})
	}
}

// TestTimestampedBackupNames exercises the timestamp-suffixed backup/restore
// Job name builders.
func TestTimestampedBackupNames(t *testing.T) {
	t.Parallel()

	const (
		cluster = "prod"
		ts      = "20250101000000"
	)

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"BackupJobName", BackupJobName(cluster, ts), "prod-backup-20250101000000"},
		{"RestoreJobName", RestoreJobName(cluster, ts), "prod-restore-20250101000000"},
		{"RetentionCleanupJobName", RetentionCleanupJobName(cluster, ts), "prod-cleanup-20250101000000"},
		{"PostRestoreValidationJobName", PostRestoreValidationJobName(cluster, ts), "prod-validate-20250101000000"},
		{"MigrateBackupJobName", MigrateBackupJobName(cluster, ts), "prod-migrate-backup-20250101000000"},
		{"MigrateRestoreJobName", MigrateRestoreJobName(cluster, ts), "prod-migrate-restore-20250101000000"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
			assert.LessOrEqual(t, len(tc.got), 63)
		})
	}
}
