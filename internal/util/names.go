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

// PxfNetworkPolicyName returns the name of the PXF cluster NetworkPolicy (SE.5)
// that confines the PXF port on the segment-primary pods.
func PxfNetworkPolicyName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-pxf", cluster))
}

// CoordinatorPodName returns the name of the active coordinator pod (ordinal 0
// of the coordinator StatefulSet). gpbackup/gprestore are MPP orchestrators that
// must run inside the coordinator pod (segment -1) so they can write the
// catalog-derived coordinator data directory + history DB and dispatch to every
// segment over SSH; the backup/restore Jobs `kubectl exec` into this pod (the
// coordinator-exec model, spec 11 §MPP Dispatch and the Coordinator-Exec Data
// Cycle).
func CoordinatorPodName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-coordinator-0", cluster))
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

// BackupS3VaultCredentialsSecretName returns the name of the Kubernetes Secret
// that the operator materializes from Vault-sourced S3 credentials. The Job spec
// always references a Secret (never embeds plaintext), so Vault credentials are
// projected into this Secret before the backup/restore Jobs run.
func BackupS3VaultCredentialsSecretName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-backup-s3-vault-creds", cluster))
}

// BackupCronJobName returns the scheduled backup CronJob name.
func BackupCronJobName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-backup-schedule", cluster))
}

// RecommendationScanCronJobName returns the scheduled storage recommendation-scan
// CronJob name ("<cluster>-recommendation-scan"). It is the single source of
// truth for the recommendation-scan workload name shared by the builder (create)
// and the controller (ensure/GC), mirroring BackupCronJobName (spec 13 §C.5).
func RecommendationScanCronJobName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-recommendation-scan", cluster))
}

// DataLoadJobName returns the deterministic data-loading Job/CronJob name for a
// named job ("<cluster>-dataload-<job>"). It is the single source of truth for
// the data-loading workload name shared by the builder (create) and the
// controller (get-or-create/idempotency).
func DataLoadJobName(cluster, job string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-dataload-%s", cluster, job))
}

// GpfdistServiceName2 returns the gpfdist Service name ("<cluster>-gpfdist-svc")
// that fronts the gpfdist Deployment pods. It is distinct from the Deployment
// name ("<cluster>-gpfdist", returned by builder.GpfdistServiceName) and is the
// host a gpload control file's gpfdist:// FILE entry targets.
func GpfdistServiceName2(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-gpfdist-svc", cluster))
}

// GpfdistDataPVCName returns the gpfdist data PVC name
// ("<cluster>-gpfdist-data-pvc"). The per-cluster name (vs. the spec's bare
// "gpfdist-data-pvc" literal) avoids collisions when two clusters share a
// namespace and lets the operator ownerRef-GC it.
func GpfdistDataPVCName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-gpfdist-data-pvc", cluster))
}

// GploadControlFileConfigMapName returns the per-job gpload control-file
// ConfigMap name ("<cluster>-gpload-<job>"). The ConfigMap carries the rendered
// gpload YAML control file that the gpload Job/CronJob mounts at /etc/gpload.
func GploadControlFileConfigMapName(cluster, job string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-gpload-%s", cluster, job))
}

// ClusterSSHSecretName returns the name of the cluster-wide gpadmin SSH keypair
// Secret. The operator generates ONE ed25519 keypair per cluster and stores it
// here; every cluster pod (coordinator, standby, segment primaries/mirrors) and
// the backup/restore Jobs mount it so passwordless SSH works cluster-wide.
// gpbackup/gprestore (MPP tools) dispatch over SSH to each segment, so a SHARED
// identity is required for the coordinator to reach the segments.
func ClusterSSHSecretName(cluster string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-ssh-keys", cluster))
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

// MigrationJobName returns the name of the SINGLE coordinated cross-cluster
// migration Job (spec 11 §Cross-Cluster Migration). The Job runs the whole
// migration sequence — gpbackup on the source coordinator, gprestore on the
// target coordinator and post-restore validation — inside one Job so the REAL
// gpbackup-generated timestamp can be captured and fed verbatim to gprestore
// (gpbackup chooses its own timestamp at runtime and offers no flag to pin it,
// so the operator's pre-chosen timestamp cannot be used for the restore). The
// operator-chosen timestamp is still used only to NAME the Job. It is suffixed
// with the source cluster so concurrent migrations never collide.
func MigrationJobName(sourceCluster, timestamp string) string {
	return SanitizeK8sName(fmt.Sprintf("%s-migration-%s", sourceCluster, timestamp))
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
