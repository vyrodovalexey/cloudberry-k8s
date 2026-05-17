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

// CommonLabels returns the standard labels for a cluster resource.
func CommonLabels(cluster, component string) map[string]string {
	return map[string]string{
		LabelManagedBy: LabelManagedByValue,
		LabelCluster:   cluster,
		LabelComponent: component,
	}
}
