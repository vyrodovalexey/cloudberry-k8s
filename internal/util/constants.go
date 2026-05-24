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

	// ComponentCoordinator is the component label value for the coordinator.
	ComponentCoordinator = "coordinator"
	// ComponentStandby is the component label value for the standby coordinator.
	ComponentStandby = "standby"
	// ComponentSegmentPrimary is the component label value for primary segments.
	ComponentSegmentPrimary = "segment-primary"
	// ComponentSegmentMirror is the component label value for mirror segments.
	ComponentSegmentMirror = "segment-mirror"

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

	// OperatorNamespace is the default operator namespace.
	OperatorNamespace = "cloudberry-system"
	// TestNamespace is the default test namespace.
	TestNamespace = "cloudberry-test"

	// OperatorAdminPasswordSecretName is the name of the Kubernetes Secret
	// used to persist the auto-generated API admin password across pod restarts.
	OperatorAdminPasswordSecretName = "cloudberry-operator-admin-password"

	// PasswordSecretKey is the key within the admin password Secret.
	PasswordSecretKey = "password"
)
