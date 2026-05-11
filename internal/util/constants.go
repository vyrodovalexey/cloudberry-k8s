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

	// ActionStart triggers a cluster start.
	ActionStart = "start"
	// ActionStartRestricted triggers a restricted cluster start.
	ActionStartRestricted = "start-restricted"
	// ActionStartMaintenance triggers a maintenance mode start.
	ActionStartMaintenance = "start-maintenance"
	// ActionStop triggers a cluster stop.
	ActionStop = "stop"
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
	DefaultVersion = "7.7"
	// DefaultImage is the default Cloudberry DB container image.
	DefaultImage = "cloudberrydb/cloudberry:7.7"

	// OperatorNamespace is the default operator namespace.
	OperatorNamespace = "cloudberry-system"
	// TestNamespace is the default test namespace.
	TestNamespace = "cloudberry-test"
)
