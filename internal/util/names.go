package util

import "fmt"

// CoordinatorName returns the coordinator StatefulSet name.
func CoordinatorName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-coordinator", cluster))
}

// StandbyName returns the standby coordinator StatefulSet name.
func StandbyName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-coordinator-standby", cluster))
}

// SegmentPrimaryName returns the primary segment StatefulSet name.
func SegmentPrimaryName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-segment-primary", cluster))
}

// SegmentMirrorName returns the mirror segment StatefulSet name.
func SegmentMirrorName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-segment-mirror", cluster))
}

// CoordinatorServiceName returns the coordinator headless service name.
// Short name ensures pod FQDN (<pod>.<svc>) stays within Cloudberry's 64-char hostname limit.
func CoordinatorServiceName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-coord-hl", cluster))
}

// StandbyServiceName returns the standby headless service name.
func StandbyServiceName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-standby-hl", cluster))
}

// SegmentServiceName returns the segment headless service name.
// Short name ensures pod FQDN (<pod>.<svc>) stays within Cloudberry's 64-char hostname limit.
func SegmentServiceName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-seg-hl", cluster))
}

// ClientServiceName returns the client-facing service name.
func ClientServiceName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-client", cluster))
}

// PostgresqlConfConfigMapName returns the postgresql.conf ConfigMap name.
func PostgresqlConfConfigMapName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-postgresql-conf", cluster))
}

// PgHBAConfConfigMapName returns the pg_hba.conf ConfigMap name.
func PgHBAConfConfigMapName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-pg-hba-conf", cluster))
}

// AdminPasswordSecretName returns the admin password secret name.
func AdminPasswordSecretName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-admin-password", cluster))
}

// RecoveryJobName returns the recovery job name with a timestamp suffix.
func RecoveryJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-recovery-%s", cluster, timestamp))
}

// MaintenanceJobName returns the maintenance job name with a timestamp suffix.
func MaintenanceJobName(cluster, operation, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-%s-%s", cluster, operation, timestamp))
}

// ExporterCredentialsSecretName returns the exporter credentials secret name.
func ExporterCredentialsSecretName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-exporter-credentials", cluster))
}

// ExporterQueriesConfigMapName returns the exporter queries ConfigMap name.
func ExporterQueriesConfigMapName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-exporter-queries", cluster))
}

// ExporterMetricsServiceName returns the exporter metrics service name.
func ExporterMetricsServiceName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-exporter-metrics", cluster))
}

// NodeExporterDaemonSetName returns the node exporter DaemonSet name.
func NodeExporterDaemonSetName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-node-exporter", cluster))
}

// QueryMetricsServiceMonitorName returns the query metrics ServiceMonitor name.
func QueryMetricsServiceMonitorName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-query-metrics", cluster))
}

// QueryAlertsPrometheusRuleName returns the query alerts PrometheusRule name.
func QueryAlertsPrometheusRuleName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-query-alerts", cluster))
}

// BackupS3ConfigMapName returns the backup S3 plugin config ConfigMap name.
func BackupS3ConfigMapName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-backup-s3-config", cluster))
}

// BackupCronJobName returns the scheduled backup CronJob name.
func BackupCronJobName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-backup-schedule", cluster))
}

// BackupJobName returns the on-demand backup Job name with a timestamp suffix.
func BackupJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-backup-%s", cluster, timestamp))
}

// RestoreJobName returns the restore Job name with a timestamp suffix.
func RestoreJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-restore-%s", cluster, timestamp))
}

// RetentionCleanupJobName returns the retention cleanup Job name with a timestamp suffix.
func RetentionCleanupJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-cleanup-%s", cluster, timestamp))
}

// PostRestoreValidationJobName returns the post-restore validation Job name.
func PostRestoreValidationJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-validate-%s", cluster, timestamp))
}

// MigrateBackupJobName returns the migration backup Job name on the source cluster.
func MigrateBackupJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-migrate-backup-%s", cluster, timestamp))
}

// MigrateRestoreJobName returns the migration restore Job name on the target cluster.
func MigrateRestoreJobName(cluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-migrate-restore-%s", cluster, timestamp))
}

// BackupServiceAccountName returns the ServiceAccount name used by backup/restore Jobs.
func BackupServiceAccountName(_ string) string {
	return "cloudberry-backup-sa"
}

// CommonLabels returns the standard labels for a cluster resource.
func CommonLabels(cluster, component string) map[string]string {
	return map[string]string{
		LabelManagedBy: LabelManagedByValue,
		LabelCluster:   cluster,
		LabelComponent: component,
	}
}
