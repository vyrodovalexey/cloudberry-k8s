// Package cases defines test case data structures and test case catalogs for the cloudberry-k8s project.
package cases

import (
	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// TestCase represents a generic test case.
type TestCase struct {
	Name        string
	Description string
	Input       interface{}
	Expected    interface{}
	ShouldFail  bool
	ErrorMsg    string
}

// WebhookValidationCase represents a webhook validation test case.
type WebhookValidationCase struct {
	Name           string
	Cluster        *cbv1alpha1.CloudberryCluster
	ExpectError    bool
	ErrorSubstring string
	ExpectWarnings bool
}

// ClusterLifecycleCase represents a cluster lifecycle test case.
type ClusterLifecycleCase struct {
	Name          string
	Action        string
	InitialPhase  cbv1alpha1.ClusterPhase
	ExpectedPhase cbv1alpha1.ClusterPhase
	Annotations   map[string]string
	ExpectError   bool
}

// ConfigManagementCase represents a configuration management test case.
type ConfigManagementCase struct {
	Name           string
	Parameters     map[string]string
	ExpectReload   bool
	ExpectError    bool
	ErrorSubstring string
}

// HBATestCase represents an HBA rule test case.
type HBATestCase struct {
	Name        string
	Rules       []cbv1alpha1.HBARule
	ExpectError bool
}

// HAOperationCase represents an HA operation test case.
type HAOperationCase struct {
	Name             string
	Action           string
	MirroringEnabled bool
	StandbyEnabled   bool
	SegmentsHealthy  bool
	ExpectEvent      string
}

// MaintenanceCase represents a maintenance operation test case.
type MaintenanceCase struct {
	Name        string
	Operation   string
	ExpectEvent string
	ExpectError bool
}

// AuthFlowCase represents an authentication flow test case.
type AuthFlowCase struct {
	Name           string
	AuthMethod     string
	Username       string
	Password       string
	Token          string
	ExpectSuccess  bool
	ExpectedStatus int
}

// VaultOperationCase represents a Vault operation test case.
type VaultOperationCase struct {
	Name        string
	Operation   string
	Path        string
	Data        map[string]interface{}
	ExpectError bool
}

// ScalingCase represents a scaling operation test case.
type ScalingCase struct {
	Name            string
	InitialSegments int32
	TargetSegments  int32
	ExpectPhase     cbv1alpha1.ClusterPhase
	ExpectEvent     string
	RequireConfirm  bool
	ExpectError     bool
}

// BackupRestoreCase represents a backup/restore test case.
type BackupRestoreCase struct {
	Name        string
	BackupType  string
	Destination string
	Compression int32
	Parallelism int32
	Incremental bool
	ExpectError bool
}

// DataLoadingCase represents a data loading test case.
type DataLoadingCase struct {
	Name        string
	SourceType  string
	TargetTable string
	Enabled     bool
	ExpectError bool
}

// WorkloadCase represents a workload management test case.
type WorkloadCase struct {
	Name          string
	GroupName     string
	Concurrency   int32
	CPUMaxPercent int32
	ExpectError   bool
}

// StorageCase represents a storage management test case.
type StorageCase struct {
	Name           string
	DiskMonitoring bool
	ScanEnabled    bool
	BloatThreshold int32
	SkewThreshold  int32
	ExpectError    bool
}

// --- Test Case Catalogs ---

// ClusterLifecycleCases returns the standard cluster lifecycle test cases.
func ClusterLifecycleCases() []ClusterLifecycleCase {
	return []ClusterLifecycleCase{
		{
			Name:          "start_from_stopped",
			Action:        "start",
			InitialPhase:  cbv1alpha1.ClusterPhaseStopped,
			ExpectedPhase: cbv1alpha1.ClusterPhaseRunning,
		},
		{
			Name:          "stop_from_running",
			Action:        "stop",
			InitialPhase:  cbv1alpha1.ClusterPhaseRunning,
			ExpectedPhase: cbv1alpha1.ClusterPhaseStopping,
		},
		{
			Name:          "stop_fast_from_running",
			Action:        "stop-fast",
			InitialPhase:  cbv1alpha1.ClusterPhaseRunning,
			ExpectedPhase: cbv1alpha1.ClusterPhaseStopping,
		},
		{
			Name:          "stop_immediate_from_running",
			Action:        "stop-immediate",
			InitialPhase:  cbv1alpha1.ClusterPhaseRunning,
			ExpectedPhase: cbv1alpha1.ClusterPhaseStopping,
		},
		{
			Name:          "restart_from_running",
			Action:        "restart",
			InitialPhase:  cbv1alpha1.ClusterPhaseRunning,
			ExpectedPhase: cbv1alpha1.ClusterPhaseStopping,
		},
		{
			Name:          "start_restricted_from_stopped",
			Action:        "start-restricted",
			InitialPhase:  cbv1alpha1.ClusterPhaseStopped,
			ExpectedPhase: cbv1alpha1.ClusterPhaseRestricted,
		},
		{
			Name:          "start_maintenance_from_stopped",
			Action:        "start-maintenance",
			InitialPhase:  cbv1alpha1.ClusterPhaseStopped,
			ExpectedPhase: cbv1alpha1.ClusterPhaseMaintenance,
		},
	}
}

// MaintenanceCases returns the standard maintenance operation test cases.
func MaintenanceCases() []MaintenanceCase {
	return []MaintenanceCase{
		{
			Name:        "vacuum",
			Operation:   "vacuum",
			ExpectEvent: "MaintenanceStarted",
		},
		{
			Name:        "vacuum_analyze",
			Operation:   "vacuum-analyze",
			ExpectEvent: "MaintenanceStarted",
		},
		{
			Name:        "vacuum_full",
			Operation:   "vacuum-full",
			ExpectEvent: "MaintenanceStarted",
		},
		{
			Name:        "analyze",
			Operation:   "analyze",
			ExpectEvent: "MaintenanceStarted",
		},
		{
			Name:        "reindex",
			Operation:   "reindex",
			ExpectEvent: "MaintenanceStarted",
		},
		{
			Name:        "unknown_operation",
			Operation:   "unknown-operation",
			ExpectEvent: "MaintenanceUnknown",
			ExpectError: false,
		},
	}
}

// ConfigManagementCases returns the standard configuration management test cases.
func ConfigManagementCases() []ConfigManagementCase {
	return []ConfigManagementCase{
		{
			Name:         "reload_safe_log_min_messages",
			Parameters:   map[string]string{"log_min_messages": "WARNING"},
			ExpectReload: true,
		},
		{
			Name:         "reload_safe_work_mem",
			Parameters:   map[string]string{"work_mem": "128MB"},
			ExpectReload: true,
		},
		{
			Name:         "restart_required_shared_buffers",
			Parameters:   map[string]string{"shared_buffers": "4GB"},
			ExpectReload: false,
		},
		{
			Name:         "restart_required_max_connections",
			Parameters:   map[string]string{"max_connections": "300"},
			ExpectReload: false,
		},
		{
			Name:         "mixed_reload_and_restart",
			Parameters:   map[string]string{"work_mem": "128MB", "shared_buffers": "4GB"},
			ExpectReload: false,
		},
	}
}

// HAOperationCases returns the standard HA operation test cases.
func HAOperationCases() []HAOperationCase {
	return []HAOperationCase{
		{
			Name:             "healthy_cluster_with_mirroring",
			Action:           "probe",
			MirroringEnabled: true,
			StandbyEnabled:   true,
			SegmentsHealthy:  true,
			ExpectEvent:      "",
		},
		{
			Name:             "degraded_segment_triggers_failover",
			Action:           "probe",
			MirroringEnabled: true,
			StandbyEnabled:   true,
			SegmentsHealthy:  false,
			ExpectEvent:      "SegmentDown",
		},
		{
			Name:             "activate_standby",
			Action:           "activate-standby",
			MirroringEnabled: true,
			StandbyEnabled:   true,
			SegmentsHealthy:  true,
			ExpectEvent:      "StandbyActivated",
		},
		{
			Name:             "rebalance_segments",
			Action:           "rebalance",
			MirroringEnabled: true,
			StandbyEnabled:   false,
			SegmentsHealthy:  true,
			ExpectEvent:      "RebalanceStarted",
		},
		{
			Name:             "incremental_recovery",
			Action:           "recovery-incremental",
			MirroringEnabled: true,
			StandbyEnabled:   false,
			SegmentsHealthy:  false,
			ExpectEvent:      "RecoveryStarted",
		},
	}
}

// AuthFlowCases returns the standard authentication flow test cases.
func AuthFlowCases() []AuthFlowCase {
	return []AuthFlowCase{
		{
			Name:           "valid_admin_basic_auth",
			AuthMethod:     "basic",
			Username:       "gpadmin",
			Password:       "admin-password",
			ExpectSuccess:  true,
			ExpectedStatus: 200,
		},
		{
			Name:           "invalid_password_basic_auth",
			AuthMethod:     "basic",
			Username:       "gpadmin",
			Password:       "wrong-password",
			ExpectSuccess:  false,
			ExpectedStatus: 401,
		},
		{
			Name:           "missing_auth_header",
			AuthMethod:     "",
			ExpectSuccess:  false,
			ExpectedStatus: 401,
		},
		{
			Name:           "valid_oidc_token",
			AuthMethod:     "oidc",
			Token:          "valid-token",
			ExpectSuccess:  true,
			ExpectedStatus: 200,
		},
		{
			Name:           "expired_oidc_token",
			AuthMethod:     "oidc",
			Token:          "expired-token",
			ExpectSuccess:  false,
			ExpectedStatus: 401,
		},
	}
}

// VaultOperationCases returns the standard Vault operation test cases.
func VaultOperationCases() []VaultOperationCase {
	return []VaultOperationCase{
		{
			Name:      "write_and_read_secret",
			Operation: "write",
			Path:      "secret/data/test/credentials",
			Data:      map[string]interface{}{"username": "admin", "password": "secret"},
		},
		{
			Name:        "read_nonexistent_secret",
			Operation:   "read",
			Path:        "secret/data/nonexistent",
			ExpectError: true,
		},
		{
			Name:      "overwrite_secret",
			Operation: "write",
			Path:      "secret/data/test/overwrite",
			Data:      map[string]interface{}{"key": "new-value"},
		},
		{
			Name:      "issue_pki_certificate",
			Operation: "pki-issue",
			Path:      "pki/issue/cloudberry",
			Data:      map[string]interface{}{"common_name": "test.cloudberry.local"},
		},
	}
}

// ScalingCases returns the standard scaling test cases.
func ScalingCases() []ScalingCase {
	return []ScalingCase{
		{
			Name:            "scale_out_4_to_8",
			InitialSegments: 4,
			TargetSegments:  8,
			ExpectPhase:     cbv1alpha1.ClusterPhaseScaling,
			ExpectEvent:     "ScaleOutStarted",
		},
		{
			Name:            "scale_in_8_to_4",
			InitialSegments: 8,
			TargetSegments:  4,
			ExpectPhase:     cbv1alpha1.ClusterPhaseScaling,
			ExpectEvent:     "ScaleInStarted",
		},
		{
			Name:            "scale_in_large_requires_confirm",
			InitialSegments: 8,
			TargetSegments:  2,
			RequireConfirm:  true,
			ExpectPhase:     cbv1alpha1.ClusterPhaseRunning,
		},
		{
			Name:            "no_change",
			InitialSegments: 4,
			TargetSegments:  4,
			ExpectPhase:     cbv1alpha1.ClusterPhaseRunning,
		},
	}
}

// BackupRestoreCases returns the standard backup/restore test cases.
func BackupRestoreCases() []BackupRestoreCase {
	return []BackupRestoreCase{
		{
			Name:        "full_backup_s3",
			BackupType:  "full",
			Destination: "s3",
			Compression: 6,
			Parallelism: 4,
		},
		{
			Name:        "incremental_backup_s3",
			BackupType:  "incremental",
			Destination: "s3",
			Incremental: true,
		},
		{
			Name:        "full_backup_local",
			BackupType:  "full",
			Destination: "local",
		},
	}
}

// DataLoadingCases returns the standard data loading test cases.
func DataLoadingCases() []DataLoadingCase {
	return []DataLoadingCase{
		{
			Name:        "s3_csv_load",
			SourceType:  "s3",
			TargetTable: "public.test_data",
			Enabled:     true,
		},
		{
			Name:        "kafka_json_load",
			SourceType:  "kafka",
			TargetTable: "public.kafka_data",
			Enabled:     true,
		},
		{
			Name:        "rabbitmq_json_load",
			SourceType:  "rabbitmq",
			TargetTable: "public.rabbitmq_data",
			Enabled:     true,
		},
		{
			Name:        "disabled_job",
			SourceType:  "s3",
			TargetTable: "public.disabled_data",
			Enabled:     false,
		},
	}
}
