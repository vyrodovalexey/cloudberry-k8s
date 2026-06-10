// Package util provides shared utilities, constants, and error types for the cloudberry operator.
package util

const (
	// APIGroup is the API group for Cloudberry resources.
	APIGroup = "avsoft.io"
	// APIVersion is the API version for Cloudberry resources.
	APIVersion = "v1alpha1"

	// FinalizerName is the finalizer used by the operator.
	FinalizerName = "avsoft.io/finalizer"

	// LabelManagedBy is the standard managed-by label key.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// LabelManagedByValue is the value for the managed-by label.
	LabelManagedByValue = "cloudberry-operator"
	// LabelPartOf is the standard part-of label key.
	LabelPartOf = "app.kubernetes.io/part-of"
	// LabelPartOfValue is the value for the part-of label.
	LabelPartOfValue = "cloudberry-operator"
	// LabelCluster is the label key for the cluster name.
	LabelCluster = "avsoft.io/cluster"
	// LabelComponent is the label key for the component type.
	LabelComponent = "avsoft.io/component"
	// LabelOperation is the label key for the operation type.
	LabelOperation = "avsoft.io/operation"
	// LabelBackupOperation is the label key for the backup operation type.
	LabelBackupOperation = "avsoft.io/backup-operation"
	// LabelBackupType is the label key for the effective backup type
	// (full|incremental) of a backup Job. It records the type of the backup that
	// actually ran so status can be derived from the Job rather than the spec.
	LabelBackupType = "avsoft.io/backup-type"

	// BackupOperationBackup is the backup-operation label value for a backup Job.
	BackupOperationBackup = "backup"
	// BackupOperationRestore is the backup-operation label value for a restore Job.
	BackupOperationRestore = "restore"
	// BackupOperationCleanup is the backup-operation label value for a retention cleanup Job.
	BackupOperationCleanup = "cleanup"
	// BackupOperationValidate is the backup-operation label value for a post-restore validation Job.
	BackupOperationValidate = "validate"
	// BackupOperationMigrate is the backup-operation label value for a cross-cluster migration Job.
	BackupOperationMigrate = "migrate"

	// BackupTypeFull is the full backup type.
	BackupTypeFull = "full"
	// BackupTypeIncremental is the incremental backup type.
	BackupTypeIncremental = "incremental"

	// ComponentBackup is the component label value for backup resources.
	ComponentBackup = "backup"

	// DefaultS3AccessKeyField is the canonical Secret/Vault key for the S3 access key id.
	DefaultS3AccessKeyField = "aws_access_key_id" //nolint:gosec // field name, not a credential
	// DefaultS3SecretKeyField is the canonical Secret/Vault key for the S3 secret access key.
	DefaultS3SecretKeyField = "aws_secret_access_key" //nolint:gosec // field name, not a credential

	// ComponentCoordinator is the component label value for the coordinator.
	ComponentCoordinator = "coordinator"
	// ComponentStandby is the component label value for the standby coordinator.
	ComponentStandby = "standby"
	// ComponentSegmentPrimary is the component label value for primary segments.
	ComponentSegmentPrimary = "segment-primary"
	// ComponentSegmentMirror is the component label value for mirror segments.
	ComponentSegmentMirror = "segment-mirror"
	// ComponentExporter is the component label value for monitoring exporters.
	ComponentExporter = "exporter"
	// ComponentNodeExporter is the component label value for node exporters.
	ComponentNodeExporter = "node-exporter"

	// AnnotationAction is the annotation key for cluster actions.
	AnnotationAction = "avsoft.io/action"
	// AnnotationMaintenance is the annotation key for maintenance operations.
	AnnotationMaintenance = "avsoft.io/maintenance"
	// AnnotationRecovery is the annotation key for recovery operations.
	AnnotationRecovery = "avsoft.io/recovery"
	// AnnotationConfigHash is the annotation key for configuration hash.
	AnnotationConfigHash = "avsoft.io/config-hash"
	// AnnotationRollingRestart tracks rolling restart progress.
	AnnotationRollingRestart = "avsoft.io/rolling-restart"
	// AnnotationRestartTrigger triggers a pod restart when changed.
	AnnotationRestartTrigger = "avsoft.io/restart-trigger"
	// AnnotationRestartPending indicates a full cluster restart is in progress.
	AnnotationRestartPending = "avsoft.io/restart-pending"
	// AnnotationConfirmScaleIn confirms a scale-in operation of more than 50%.
	AnnotationConfirmScaleIn = "avsoft.io/confirm-scale-in"
	// AnnotationScaleStarted tracks when a scale operation started.
	AnnotationScaleStarted = "avsoft.io/scale-started"
	// AnnotationUpgrade tracks in-progress upgrade state as JSON.
	AnnotationUpgrade = "avsoft.io/upgrade"
	// AnnotationMirroringState tracks in-progress mirroring enable/disable state as JSON.
	AnnotationMirroringState = "avsoft.io/mirroring-state"
	// AnnotationFailoverState tracks in-progress failover state.
	AnnotationFailoverState = "avsoft.io/failover-state"
	// AnnotationPendingReload tracks a pending pg_reload_conf() call.
	// The value is the RFC3339 timestamp when the ConfigMap was updated.
	// The operator waits for ConfigMap volume propagation before calling reload.
	AnnotationPendingReload = "avsoft.io/pending-reload"
	// AnnotationExporterRoleReady indicates the exporter DB role has been created.
	// When absent or not "true", the admin-controller will retry role setup on each cycle.
	AnnotationExporterRoleReady = "avsoft.io/exporter-role-ready"
	// AnnotationBackupRetentionDeleted records the number of backups removed by a
	// retention cleanup Job, used to drive the cloudberry_backup_retention_deleted_total counter.
	AnnotationBackupRetentionDeleted = "avsoft.io/backup-retention-deleted"
	// AnnotationRestorePartial marks a SUCCEEDED restore Job whose statistics
	// restore step failed (gprestore exit code 2 — known upstream gpbackup bug:
	// invalid bigint in statistics.sql) while the data restore succeeded. The
	// admin controller patches it from the restore pod's termination message
	// ("GPRESTORE_PARTIAL=stats") and reports the restore as
	// success-with-warning (RestorePartial Event, metric result "partial").
	AnnotationRestorePartial = "avsoft.io/restore-partial"
	// AnnotationBackupSizeBytes records the size in bytes of a completed backup Job,
	// used to drive the cloudberry_backup_size_bytes gauge.
	AnnotationBackupSizeBytes = "avsoft.io/backup-size-bytes"
	// AnnotationExpectedRowCounts carries the expected per-table row counts
	// (a JSON object of fully-qualified table -> count) captured from the gpbackup
	// history metadata of the restored timestamp. When present on a restore Job
	// the operator wires it into the post-restore validation Job so the row-count
	// compare can flag discrepancies; absent it falls back to a best-effort probe.
	AnnotationExpectedRowCounts = "avsoft.io/expected-row-counts"
	// AnnotationValidationRecorded marks a terminal validation Job whose outcome
	// has already been recorded (metric + de-duplicated Warning Event), so
	// periodic reconciles of the same finished Job do not double-count or emit an
	// event storm. Its value is the recorded result ("success" or "failed").
	AnnotationValidationRecorded = "avsoft.io/validation-recorded"
	// AnnotationDeletionBackupJob tracks the name of the backup Job created by
	// the deletion flow (spec.backupOnDelete=true with deletionPolicy=Delete).
	// While present and the Job is non-terminal, PVC deletion and finalizer
	// removal are deferred so the backup runs against intact volumes.
	AnnotationDeletionBackupJob = "avsoft.io/deletion-backup-job"
	// BackupTimestampLayout is the shared Go time layout used to stamp
	// backup/maintenance/rebalance Job names (YYYYMMDD-HHMMSS).
	BackupTimestampLayout = "20060102-150405"

	// GpbackupTimestampLayout is the gpbackup-style YYYYMMDDHHMMSS (14-digit)
	// timestamp layout shared by the API and the admin controller (L-2).
	GpbackupTimestampLayout = "20060102150405"

	// AnnotationRebalanceJob tracks the name of the fallback rebalance Job so
	// the HA controller observes it to a TERMINAL state across reconciles
	// before recording rebalance completion (no fire-and-forget success).
	AnnotationRebalanceJob = "avsoft.io/rebalance-job"
	// AnnotationDeletionBackupDeadline is the RFC3339 deadline for the
	// deletion backup Job. Past this deadline the deletion proceeds even if
	// the Job has not finished, so cluster/namespace deletion can never be
	// wedged forever by a stuck backup.
	AnnotationDeletionBackupDeadline = "avsoft.io/deletion-backup-deadline"
	// AnnotationTLSCertChecksum is the pod-template annotation carrying a
	// checksum of the cluster TLS certificate Secret data. Stamping it on the
	// StatefulSet pod templates makes the cluster pods roll exactly once when
	// the certificate rotates (so PostgreSQL serves the renewed certificate),
	// while staying stable across no-op reconciles (L-5).
	AnnotationTLSCertChecksum = "avsoft.io/tls-cert-checksum"

	// ActionStart triggers a cluster start.
	ActionStart = "start"
	// ActionStartRestricted triggers a restricted cluster start.
	ActionStartRestricted = "start-restricted"
	// ActionStartMaintenance triggers a maintenance mode start.
	ActionStartMaintenance = "start-maintenance"
	// ActionStop triggers a cluster stop.
	ActionStop = "stop"
	// ActionStopFast triggers a fast cluster stop (rolls back transactions).
	ActionStopFast = "stop-fast"
	// ActionStopImmediate triggers an immediate cluster stop (aborts connections).
	ActionStopImmediate = "stop-immediate"
	// ActionRestart triggers a cluster restart.
	ActionRestart = "restart"
	// ActionRebalance triggers segment rebalancing.
	ActionRebalance = "rebalance"
	// ActionActivateStandby triggers standby activation.
	ActionActivateStandby = "activate-standby"

	// MaintenanceVacuum triggers a vacuum operation.
	MaintenanceVacuum = "vacuum"
	// MaintenanceVacuumAnalyze triggers a vacuum with analyze.
	MaintenanceVacuumAnalyze = "vacuum-analyze"
	// MaintenanceVacuumFull triggers a full vacuum.
	MaintenanceVacuumFull = "vacuum-full"
	// MaintenanceAnalyze triggers an analyze operation.
	MaintenanceAnalyze = "analyze"
	// MaintenanceReindex triggers a reindex operation.
	MaintenanceReindex = "reindex"
	// MaintenanceRedistribute triggers a data redistribution operation.
	MaintenanceRedistribute = "redistribute"
	// MaintenanceRebalance triggers a segment rebalance operation.
	MaintenanceRebalance = "rebalance"
	// MaintenanceLogRotate triggers a log file rotation.
	MaintenanceLogRotate = "log-rotate"
	// MaintenanceBackupOnDelete triggers a backup before cluster deletion.
	MaintenanceBackupOnDelete = "backup-on-delete"

	// RecoveryIncremental triggers incremental recovery.
	RecoveryIncremental = "incremental"
	// RecoveryFull triggers full recovery.
	RecoveryFull = "full"
	// RecoveryDifferential triggers differential recovery.
	RecoveryDifferential = "differential"

	// DefaultAdminUser is the default admin username.
	DefaultAdminUser = "gpadmin"
	// DefaultCoordinatorPort is the default coordinator port.
	DefaultCoordinatorPort = 5432
	// DefaultMetricsPort is the default Prometheus metrics port.
	DefaultMetricsPort = 9187
	// DefaultVersion is the default Cloudberry DB version.
	DefaultVersion = "2.1.0"
	// DefaultImage is the default Cloudberry DB container image.
	DefaultImage = "cloudberrydb/cloudberry:2.1.0"
	// DefaultBackupImage is the default backup toolchain container image used
	// for backup/restore Jobs when spec.backup.image is unset. A backup-capable
	// image MUST contain kubectl (the Jobs `kubectl exec` gpbackup/gprestore
	// into the coordinator pod — the coordinator-exec model) and the
	// gpbackup/gprestore/gpbackup_s3_plugin toolchain.
	DefaultBackupImage = "cloudberry-backup:2.1.0"

	// OperatorNamespace is the default operator namespace.
	OperatorNamespace = "cloudberry-system"
	// TestNamespace is the default test namespace.
	TestNamespace = "cloudberry-test"

	// OperatorAdminPasswordSecretName is the name of the Kubernetes Secret
	// used to persist the auto-generated API admin password across pod restarts.
	OperatorAdminPasswordSecretName = "cloudberry-operator-admin-password"

	// PasswordSecretKey is the key within the admin password Secret.
	PasswordSecretKey = "password"

	// SSHPrivateKeyField is the Secret data key holding the shared gpadmin
	// ed25519 private key.
	SSHPrivateKeyField = "id_ed25519"
	// SSHPublicKeyField is the Secret data key holding the shared gpadmin
	// ed25519 public key.
	SSHPublicKeyField = "id_ed25519.pub" //nolint:gosec // field name, not a credential
	// SSHAuthorizedKeysField is the Secret data key holding the authorized_keys
	// file (the single shared public key) installed into every pod.
	SSHAuthorizedKeysField = "authorized_keys"
)
