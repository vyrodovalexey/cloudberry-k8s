// Package cases defines test case data structures and test case catalogs for the cloudberry-k8s project.
package cases

import (
	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
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

// BackupRestoreCase represents a backup/restore test case backed by the
// gpbackup/gprestore toolchain (spec 11). Capability flags describe which part
// of the backup feature the case exercises so suites can select subsets.
type BackupRestoreCase struct {
	Name        string
	BackupType  string
	Destination string
	Compression int32
	Parallelism int32
	Incremental bool
	// Capability is the spec-11 capability under test, one of:
	// "scheduled", "on-demand", "restore", "retention", "migrate",
	// "pre-backup-check", "post-restore-validation".
	Capability string
	// Scheduled indicates the case exercises the backup CronJob.
	Scheduled   bool
	ExpectError bool
}

// Scenario71BackupConfigCase represents a single Scenario 71 "Enable Backup with
// Full S3 Configuration" variant. Both variants share the same full S3 config
// (bucket, endpoint, folder, encryption, forcePathStyle, multipart) and differ
// only in the credential source: a Kubernetes Secret vs a Vault path.
type Scenario71BackupConfigCase struct {
	// Name is a short test name.
	Name string
	// CredentialSource is "secret" (Kubernetes Secret) or "vault" (Vault path).
	CredentialSource string
	// CredentialRef is the Secret name the backup/restore Job env references:
	// the user Secret for the "secret" variant, or the operator-materialized
	// "<cluster>-backup-s3-vault-creds" Secret for the "vault" variant.
	CredentialRef string
	// Bucket is the configured S3 bucket.
	Bucket string
	// Endpoint is the configured S3-compatible endpoint.
	Endpoint string
	// Folder is the configured S3 folder prefix.
	Folder string
	// Encryption is the configured S3 plugin encryption (on|off).
	Encryption string
	// ForcePathStyle is the configured path-style addressing flag.
	ForcePathStyle bool
	// Multipart indicates the full multipart tuning block is configured.
	Multipart bool
	// Description explains the variant.
	Description string
}

// Scenario71BackupConfigCases returns the two Scenario 71 full-S3 backup config
// variants (Kubernetes Secret vs Vault credentials). Both verify the same S3
// config fields (bucket, endpoint, folder, encryption, forcePathStyle, multipart)
// and differ only in the credential source / referenced Secret.
func Scenario71BackupConfigCases() []Scenario71BackupConfigCase {
	return []Scenario71BackupConfigCase{
		{
			Name:             "71a_secret_credentials",
			CredentialSource: "secret",
			CredentialRef:    "backup-s3-credentials",
			Bucket:           "cloudberry-backups",
			Endpoint:         "http://minio:9000",
			Folder:           "/backups",
			Encryption:       "on",
			ForcePathStyle:   true,
			Multipart:        true,
			Description: "full S3 config with credentials from a Kubernetes Secret; " +
				"Job env AWS_* reference the 'backup-s3-credentials' Secret",
		},
		{
			Name:             "71b_vault_credentials",
			CredentialSource: "vault",
			// The Job never references the Vault path directly; it references the
			// operator-materialized Secret (BackupS3VaultCredentialsSecretName).
			CredentialRef:  "<cluster>-backup-s3-vault-creds",
			Bucket:         "cloudberry-backups",
			Endpoint:       "http://minio:9000",
			Folder:         "/backups",
			Encryption:     "on",
			ForcePathStyle: true,
			Multipart:      true,
			Description: "full S3 config with credentials from Vault (vaultSecret); " +
				"the operator materializes '<cluster>-backup-s3-vault-creds' and " +
				"Job env AWS_* reference that Secret instead of 'backup-s3-credentials'",
		},
	}
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

// HBADefaultRuleCase represents a test case for verifying default HBA rule generation.
type HBADefaultRuleCase struct {
	Name          string
	HBARules      []cbv1alpha1.HBARule // nil or empty = use defaults
	ExpectedLines []string             // expected lines in generated pg_hba.conf
	ExcludedLines []string             // lines that should NOT appear
	Description   string
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
			Capability:  "on-demand",
		},
		{
			Name:        "incremental_backup_s3",
			BackupType:  "incremental",
			Destination: "s3",
			Incremental: true,
			Capability:  "on-demand",
		},
		{
			Name:        "full_backup_local",
			BackupType:  "full",
			Destination: "local",
			Capability:  "on-demand",
		},
		{
			Name:        "scheduled_backup_cronjob_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "scheduled",
			Scheduled:   true,
		},
		{
			Name:        "restore_from_timestamp_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "restore",
		},
		{
			Name:        "retention_cleanup_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "retention",
		},
		{
			Name:        "migrate_between_clusters_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "migrate",
		},
		{
			Name:        "pre_backup_health_check_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "pre-backup-check",
		},
		{
			Name:        "post_restore_validation_s3",
			BackupType:  "full",
			Destination: "s3",
			Capability:  "post-restore-validation",
		},
		{
			Name:        "incremental_backup_from_full_s3",
			BackupType:  "incremental",
			Destination: "s3",
			Incremental: true,
			Capability:  "on-demand",
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

// IOLimitFormatCase represents a test case for FormatIOLimits.
type IOLimitFormatCase struct {
	Name     string
	Limits   []db.IOLimitOption
	Expected string
}

// IOLimitFormatCases returns the standard FormatIOLimits test cases.
func IOLimitFormatCases() []IOLimitFormatCase {
	return []IOLimitFormatCase{
		{
			Name:     "empty_limits",
			Limits:   nil,
			Expected: "",
		},
		{
			Name: "wildcard_only",
			Limits: []db.IOLimitOption{
				{
					Tablespace:       "*",
					ReadBytesPerSec:  104857600,
					WriteBytesPerSec: 52428800,
					ReadIOPS:         1000,
					WriteIOPS:        500,
				},
			},
			Expected: "*:rbps=104857600:wbps=52428800:riops=1000:wiops=500",
		},
		{
			Name: "named_tablespace_only",
			Limits: []db.IOLimitOption{
				{
					Tablespace:       "fast_storage",
					ReadBytesPerSec:  209715200,
					WriteBytesPerSec: 104857600,
					ReadIOPS:         5000,
					WriteIOPS:        2500,
				},
			},
			Expected: "fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500",
		},
		{
			Name: "named_and_wildcard",
			Limits: []db.IOLimitOption{
				{
					Tablespace:       "fast_storage",
					ReadBytesPerSec:  209715200,
					WriteBytesPerSec: 104857600,
					ReadIOPS:         5000,
					WriteIOPS:        2500,
				},
				{
					Tablespace:       "*",
					ReadBytesPerSec:  52428800,
					WriteBytesPerSec: 26214400,
					ReadIOPS:         500,
					WriteIOPS:        250,
				},
			},
			Expected: "fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250",
		},
		{
			Name: "zero_values",
			Limits: []db.IOLimitOption{
				{
					Tablespace:       "*",
					ReadBytesPerSec:  0,
					WriteBytesPerSec: 0,
					ReadIOPS:         0,
					WriteIOPS:        0,
				},
			},
			Expected: "*:rbps=0:wbps=0:riops=0:wiops=0",
		},
	}
}

// IOLimitReconcileCase represents a test case for I/O limit reconciliation.
type IOLimitReconcileCase struct {
	Name           string
	ResourceGroup  string
	IOLimits       []cbv1alpha1.TablespaceIOLimitSpec
	ExpectedFormat string
	ExpectAlter    bool
}

// IOLimitReconcileCases returns the standard I/O limit reconciliation test cases.
func IOLimitReconcileCases() []IOLimitReconcileCase {
	return []IOLimitReconcileCase{
		{
			Name:          "wildcard_io_limits",
			ResourceGroup: "analytics",
			IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
				{
					Tablespace:       "*",
					ReadBytesPerSec:  104857600,
					WriteBytesPerSec: 52428800,
					ReadIOPS:         1000,
					WriteIOPS:        500,
				},
			},
			ExpectedFormat: "*:rbps=104857600:wbps=52428800:riops=1000:wiops=500",
			ExpectAlter:    true,
		},
		{
			Name:          "named_and_wildcard_io_limits",
			ResourceGroup: "analytics",
			IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
				{
					Tablespace:       "fast_storage",
					ReadBytesPerSec:  209715200,
					WriteBytesPerSec: 104857600,
					ReadIOPS:         5000,
					WriteIOPS:        2500,
				},
				{
					Tablespace:       "*",
					ReadBytesPerSec:  52428800,
					WriteBytesPerSec: 26214400,
					ReadIOPS:         500,
					WriteIOPS:        250,
				},
			},
			ExpectedFormat: "fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250",
			ExpectAlter:    true,
		},
		{
			Name:           "no_io_limits",
			ResourceGroup:  "analytics",
			IOLimits:       nil,
			ExpectedFormat: "",
			ExpectAlter:    false,
		},
	}
}

// HBADefaultRuleCases returns the test cases for verifying default HBA rule generation.
func HBADefaultRuleCases() []HBADefaultRuleCase {
	// defaultExpectedLines are the five lines that the operator must produce
	// when no custom hbaRules are specified.
	defaultExpectedLines := []string{
		"local\tall\tgpadmin\ttrust",
		"local\tall\tall\tscram-sha-256",
		"host\tall\tgpadmin\t127.0.0.1/32\ttrust",
		"host\tall\tall\t0.0.0.0/0\tscram-sha-256",
		"host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
	}

	return []HBADefaultRuleCase{
		{
			Name:          "no_hba_rules_generates_defaults",
			HBARules:      nil,
			ExpectedLines: defaultExpectedLines,
			Description:   "nil HBARules should produce all 5 default pg_hba.conf lines",
		},
		{
			Name:          "empty_hba_rules_generates_defaults",
			HBARules:      []cbv1alpha1.HBARule{},
			ExpectedLines: defaultExpectedLines,
			Description:   "empty slice HBARules should produce all 5 default pg_hba.conf lines",
		},
		{
			Name: "custom_rules_override_defaults",
			HBARules: []cbv1alpha1.HBARule{
				{
					Type:     cbv1alpha1.HBATypeHostSSL,
					Database: "mydb",
					User:     "appuser",
					Address:  "10.0.0.0/8",
					Method:   cbv1alpha1.AuthMethodScramSHA256,
				},
				{
					Type:     cbv1alpha1.HBATypeHost,
					Database: "all",
					User:     "all",
					Address:  "0.0.0.0/0",
					Method:   cbv1alpha1.AuthMethodReject,
				},
			},
			ExpectedLines: []string{
				"hostssl\tmydb\tappuser\t10.0.0.0/8\tscram-sha-256",
				"host\tall\tall\t0.0.0.0/0\treject",
			},
			ExcludedLines: []string{
				"local\tall\tgpadmin\ttrust",
			},
			Description: "custom rules should replace defaults entirely",
		},
		{
			Name:     "verify_default_rule_order",
			HBARules: nil,
			ExpectedLines: []string{
				"local\tall\tgpadmin\ttrust",
				"local\tall\tall\tscram-sha-256",
				"host\tall\tgpadmin\t127.0.0.1/32\ttrust",
				"host\tall\tall\t0.0.0.0/0\tscram-sha-256",
				"host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
			},
			Description: "local rules must come before host rules in the default set",
		},
		{
			Name:     "verify_replication_rule_present",
			HBARules: nil,
			ExpectedLines: []string{
				"host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
			},
			Description: "replication rule must be present in the default set",
		},
	}
}

// DualAuthCase represents a test case for dual-mode authentication verification.
// It validates that when both basic and OIDC auth are enabled, the middleware
// correctly routes requests to the appropriate provider based on the Authorization header.
type DualAuthCase struct {
	Name               string
	AuthHeader         string // "Basic ..." or "Bearer ..."
	ExpectedAuthMethod string // "basic" or "oidc"
	ExpectedPermission string // "Admin", "Operator", "Operator Basic", "Basic", "Self Only"
	ExpectSuccess      bool
	ExpectStatusCode   int
	Description        string
}

// VaultIntegrationCase represents a test case for Vault integration verification.
type VaultIntegrationCase struct {
	Name          string
	AuthMethod    string // "token", "kubernetes", "approle"
	SecretPaths   []string
	ExpectSuccess bool
	Description   string
}

// VaultIntegrationCases returns the standard Vault integration test cases.
func VaultIntegrationCases() []VaultIntegrationCase {
	return []VaultIntegrationCase{
		{
			Name:       "46a_token_auth_read_secrets",
			AuthMethod: "token",
			SecretPaths: []string{
				"secret/data/cloudberry/admin-password",
				"secret/data/cloudberry/oidc-secret",
				"secret/data/cloudberry/monitoring-password",
				"secret/data/cloudberry/tls",
			},
			ExpectSuccess: true,
			Description:   "Token auth should authenticate and read all 4 KV secret paths",
		},
		{
			Name:       "46b_token_auth_dev_mode",
			AuthMethod: "token",
			SecretPaths: []string{
				"secret/data/cloudberry/admin-password",
				"secret/data/cloudberry/oidc-secret",
				"secret/data/cloudberry/monitoring-password",
				"secret/data/cloudberry/tls",
			},
			ExpectSuccess: true,
			Description:   "Token auth in dev mode with static token should authenticate and read secrets",
		},
		{
			Name:          "46c_approle_auth",
			AuthMethod:    "approle",
			SecretPaths:   []string{"secret/data/cloudberry/admin-password"},
			ExpectSuccess: true,
			Description:   "AppRole auth should authenticate successfully via role_id and secret_id",
		},
		{
			Name:          "46d_secret_rotation_watch",
			AuthMethod:    "token",
			SecretPaths:   []string{"secret/data/cloudberry/admin-password"},
			ExpectSuccess: true,
			Description:   "SecretWatcher should detect secret changes and invoke onChange callback",
		},
		{
			Name:          "46e_connection_retry",
			AuthMethod:    "token",
			SecretPaths:   nil,
			ExpectSuccess: true,
			Description:   "RetryWithBackoff should retry failing operations with exponential backoff",
		},
	}
}

// DualAuthCases returns the standard dual-mode authentication test cases.
func DualAuthCases() []DualAuthCase {
	return []DualAuthCase{
		{
			Name:               "basic_auth_routes_to_basic_provider",
			AuthHeader:         "Basic dGVzdDpwYXNz", // test:pass
			ExpectedAuthMethod: "basic",
			ExpectedPermission: "Admin",
			ExpectSuccess:      true,
			ExpectStatusCode:   200,
			Description:        "Basic auth header should route to the basic provider",
		},
		{
			Name:               "bearer_auth_routes_to_oidc_provider",
			AuthHeader:         "Bearer eyJhbGciOiJSUzI1NiJ9.test.sig",
			ExpectedAuthMethod: "oidc",
			ExpectedPermission: "Operator",
			ExpectSuccess:      true,
			ExpectStatusCode:   200,
			Description:        "Bearer auth header should route to the OIDC provider",
		},
		{
			Name:             "missing_auth_header_returns_401",
			AuthHeader:       "",
			ExpectSuccess:    false,
			ExpectStatusCode: 401,
			Description:      "Missing Authorization header should return 401 Unauthorized",
		},
		{
			Name:             "unsupported_auth_type_returns_401",
			AuthHeader:       "Digest username=test",
			ExpectSuccess:    false,
			ExpectStatusCode: 401,
			Description:      "Unsupported authorization type should return 401 Unauthorized",
		},
		{
			Name:               "basic_provider_type_returns_basic",
			ExpectedAuthMethod: "basic",
			Description:        "BasicAuthProvider.Type() should return 'basic'",
		},
		{
			Name:               "oidc_provider_type_returns_oidc",
			ExpectedAuthMethod: "oidc",
			Description:        "OIDCProvider.Type() should return 'oidc'",
		},
		{
			Name:               "basic_admin_gets_admin_permission",
			AuthHeader:         "Basic YWRtaW46YWRtaW5wYXNz", // admin:adminpass
			ExpectedAuthMethod: "basic",
			ExpectedPermission: "Admin",
			ExpectSuccess:      true,
			ExpectStatusCode:   200,
			Description:        "Basic auth admin user should get Admin permission level",
		},
		{
			Name:               "basic_operator_gets_operator_permission",
			AuthHeader:         "Basic b3BlcmF0b3I6b3BwYXNz", // operator:oppass
			ExpectedAuthMethod: "basic",
			ExpectedPermission: "Operator",
			ExpectSuccess:      true,
			ExpectStatusCode:   200,
			Description:        "Basic auth operator user should get Operator permission level",
		},
		{
			Name:               "basic_viewer_gets_basic_permission",
			AuthHeader:         "Basic dmlld2VyOnZpZXdwYXNz", // viewer:viewpass
			ExpectedAuthMethod: "basic",
			ExpectedPermission: "Basic",
			ExpectSuccess:      true,
			ExpectStatusCode:   200,
			Description:        "Basic auth viewer user should get Basic permission level",
		},
	}
}

// SSLConfigCase represents a test case for SSL/TLS configuration verification.
type SSLConfigCase struct {
	Name              string
	SSLEnabled        bool
	CertSecretName    string
	MinTLSVersion     string
	ExpectedConfLines []string // expected lines in postgresql.conf
	ExpectTLSVolume   bool
	Description       string
}

// SSLConfigCases returns the standard SSL/TLS configuration test cases.
func SSLConfigCases() []SSLConfigCase {
	return []SSLConfigCase{
		{
			Name:           "47a_ssl_enabled_with_k8s_secret",
			SSLEnabled:     true,
			CertSecretName: "cloudberry-tls",
			MinTLSVersion:  "1.2",
			ExpectedConfLines: []string{
				"ssl = on",
				"ssl_cert_file = '/tls/tls.crt'",
				"ssl_key_file = '/tls/tls.key'",
				"ssl_ca_file = '/tls/ca.crt'",
				"ssl_min_protocol_version = 'TLSv1.2'",
			},
			ExpectTLSVolume: true,
			Description:     "SSL enabled with K8s secret should produce all SSL settings and TLS volume",
		},
		{
			Name:           "47a_ssl_enabled_tls13",
			SSLEnabled:     true,
			CertSecretName: "cloudberry-tls",
			MinTLSVersion:  "1.3",
			ExpectedConfLines: []string{
				"ssl = on",
				"ssl_min_protocol_version = 'TLSv1.3'",
			},
			ExpectTLSVolume: true,
			Description:     "SSL enabled with minTLSVersion 1.3 should produce TLSv1.3 in config",
		},
		{
			Name:              "47a_ssl_disabled",
			SSLEnabled:        false,
			CertSecretName:    "",
			MinTLSVersion:     "",
			ExpectedConfLines: nil,
			ExpectTLSVolume:   false,
			Description:       "SSL disabled should produce no SSL settings and no TLS volume",
		},
		{
			Name:           "47b_ssl_enabled_vault_pki",
			SSLEnabled:     true,
			CertSecretName: "cloudberry-vault-tls",
			MinTLSVersion:  "1.2",
			ExpectedConfLines: []string{
				"ssl = on",
				"ssl_cert_file = '/tls/tls.crt'",
				"ssl_key_file = '/tls/tls.key'",
				"ssl_ca_file = '/tls/ca.crt'",
				"ssl_min_protocol_version = 'TLSv1.2'",
			},
			ExpectTLSVolume: true,
			Description:     "SSL enabled with Vault PKI cert secret should produce all SSL settings and TLS volume",
		},
	}
}

// WebhookCertCase represents a test case for webhook certificate management verification.
type WebhookCertCase struct {
	Name           string
	CertSource     string // "vault-pki" or "self-signed"
	ExpectCABundle bool
	ExpectSecret   bool
	Description    string
}

// WebhookCertCases returns the standard webhook certificate management test cases.
func WebhookCertCases() []WebhookCertCase {
	return []WebhookCertCase{
		{
			Name:           "48a_vault_pki_cert_source",
			CertSource:     "vault-pki",
			ExpectCABundle: true,
			ExpectSecret:   true,
			Description:    "Vault PKI cert source should issue certificates via Vault and store them in a K8s Secret",
		},
		{
			Name:           "48b_self_signed_cert_source",
			CertSource:     "self-signed",
			ExpectCABundle: true,
			ExpectSecret:   true,
			Description:    "Self-signed cert source should generate certificates locally and store them in a K8s Secret",
		},
		{
			Name:           "48_cert_rotation_near_expiry",
			CertSource:     "self-signed",
			ExpectCABundle: true,
			ExpectSecret:   true,
			Description:    "Certificates past 2/3 of their lifetime should be detected as needing rotation",
		},
	}
}

// Scenario69ValidationCase represents a single Scenario 69 webhook backup
// validation negative test case: an otherwise-valid backup spec with exactly one
// offending field, expected to be rejected with a descriptive error mentioning
// the field path.
type Scenario69ValidationCase struct {
	// ID is the spec rule id (e.g. "69a").
	ID string
	// Name is a short test name.
	Name string
	// OffendingField describes the single field mutated to an invalid value.
	OffendingField string
	// ErrorSubstrings are substrings expected in the rejection error.
	ErrorSubstrings []string
	// Description explains the rule.
	Description string
}

// Scenario69ValidationCases returns the Scenario 69 backup webhook validation
// negative cases 69a-69j. Rule 69c is Vault-aware: S3 credentials may come from
// credentialSecret.name OR vaultSecret.path, and rejection only occurs when
// NEITHER source is provided. Rule 69d rejects compressionLevel outside 1-9,
// including an explicit 0 (the mutating defaulter sets 1 for an omitted value,
// so a 0 reaching the validator is an explicit invalid level).
func Scenario69ValidationCases() []Scenario69ValidationCase {
	return []Scenario69ValidationCase{
		{
			ID: "69a", Name: "missing_destination_type",
			OffendingField:  "backup.destination.type",
			ErrorSubstrings: []string{"destination.type"},
			Description:     "destination.type empty must be rejected when backup is enabled",
		},
		{
			ID: "69b", Name: "missing_s3_bucket",
			OffendingField:  "backup.destination.s3.bucket",
			ErrorSubstrings: []string{"bucket"},
			Description:     "s3 destination with no bucket must be rejected",
		},
		{
			ID: "69c", Name: "missing_credential_and_vault",
			OffendingField:  "backup.destination.s3.credentialSecret / vaultSecret",
			ErrorSubstrings: []string{"credentialSecret", "vaultSecret"},
			Description:     "s3 with neither credentialSecret.name nor vaultSecret.path must be rejected",
		},
		{
			ID: "69d", Name: "compression_level_too_high",
			OffendingField:  "backup.gpbackup.compressionLevel",
			ErrorSubstrings: []string{"compressionLevel"},
			Description:     "compressionLevel=10 must be rejected (valid range 1-9)",
		},
		{
			ID: "69d", Name: "compression_level_zero",
			OffendingField:  "backup.gpbackup.compressionLevel",
			ErrorSubstrings: []string{"compressionLevel"},
			Description:     "compressionLevel=0 must be rejected as an explicit invalid level",
		},
		{
			ID: "69e", Name: "invalid_compression_type",
			OffendingField:  "backup.gpbackup.compressionType",
			ErrorSubstrings: []string{"compressionType"},
			Description:     "compressionType=lz4 must be rejected (only gzip or zstd)",
		},
		{
			ID: "69f", Name: "copy_queue_size_without_single_data_file",
			OffendingField:  "backup.gpbackup.copyQueueSize",
			ErrorSubstrings: []string{"copyQueueSize"},
			Description:     "copyQueueSize without singleDataFile must be rejected",
		},
		{
			ID: "69g", Name: "jobs_combined_with_single_data_file",
			OffendingField:  "backup.gpbackup.jobs",
			ErrorSubstrings: []string{"jobs cannot be combined"},
			Description:     "jobs>1 combined with singleDataFile must be rejected",
		},
		{
			ID: "69h", Name: "incremental_without_leaf_partition_data",
			OffendingField:  "backup.gpbackup.incremental",
			ErrorSubstrings: []string{"leafPartitionData"},
			Description:     "incremental without leafPartitionData must be rejected",
		},
		{
			ID: "69i", Name: "invalid_cron_schedule",
			OffendingField:  "backup.schedule",
			ErrorSubstrings: []string{"cron"},
			Description:     "a non-cron schedule must be rejected",
		},
		{
			ID: "69j", Name: "missing_image",
			OffendingField:  "backup.image",
			ErrorSubstrings: []string{"backup.image"},
			Description:     "empty backup.image must be rejected when backup is enabled",
		},
	}
}

// RoleClaimCase represents a test case for role claim source and match mode verification.
type RoleClaimCase struct {
	Name        string
	MatchMode   string // "exact", "suffix", "prefix", "contains"
	MappingKey  string // the key in roleMapping (e.g., "admin")
	UserRole    string // the role the user actually has
	ExpectMatch bool   // whether the role should match
	Description string
}

// RoleClaimCases returns the standard role claim source and match mode test cases.
// Cases 42a-42f cover id_token source, userinfo config, and all four match modes.
func RoleClaimCases() []RoleClaimCase {
	return []RoleClaimCase{
		{
			Name:        "42a_id_token_roles_from_claims",
			MatchMode:   "exact",
			MappingKey:  "admin",
			UserRole:    "admin",
			ExpectMatch: true,
			Description: "roleClaimSource=id_token: roles extracted from ID token claims should match exactly",
		},
		{
			Name:        "42b_userinfo_config_field",
			MatchMode:   "exact",
			MappingKey:  "admin",
			UserRole:    "admin",
			ExpectMatch: true,
			Description: "roleClaimSource=userinfo: config field can be set (actual UserInfo call not implemented)",
		},
		{
			Name:        "42c_exact_match",
			MatchMode:   "exact",
			MappingKey:  "admin",
			UserRole:    "admin",
			ExpectMatch: true,
			Description: "exact mode: 'admin' matches 'admin'",
		},
		{
			Name:        "42c_exact_no_match",
			MatchMode:   "exact",
			MappingKey:  "admin",
			UserRole:    "super-admin",
			ExpectMatch: false,
			Description: "exact mode: 'super-admin' does NOT match 'admin'",
		},
		{
			Name:        "42d_suffix_match",
			MatchMode:   "suffix",
			MappingKey:  "admin",
			UserRole:    "org-admin",
			ExpectMatch: true,
			Description: "suffix mode: 'org-admin' ends with 'admin'",
		},
		{
			Name:        "42d_suffix_no_match",
			MatchMode:   "suffix",
			MappingKey:  "admin",
			UserRole:    "admin-team",
			ExpectMatch: false,
			Description: "suffix mode: 'admin-team' does NOT end with 'admin'",
		},
		{
			Name:        "42e_prefix_match",
			MatchMode:   "prefix",
			MappingKey:  "admin",
			UserRole:    "admin-team",
			ExpectMatch: true,
			Description: "prefix mode: 'admin-team' starts with 'admin'",
		},
		{
			Name:        "42e_prefix_no_match",
			MatchMode:   "prefix",
			MappingKey:  "admin",
			UserRole:    "org-admin",
			ExpectMatch: false,
			Description: "prefix mode: 'org-admin' does NOT start with 'admin'",
		},
		{
			Name:        "42f_contains_match",
			MatchMode:   "contains",
			MappingKey:  "admin",
			UserRole:    "super-admin-user",
			ExpectMatch: true,
			Description: "contains mode: 'super-admin-user' contains 'admin'",
		},
		{
			Name:        "42f_contains_no_match",
			MatchMode:   "contains",
			MappingKey:  "admin",
			UserRole:    "reader",
			ExpectMatch: false,
			Description: "contains mode: 'reader' does NOT contain 'admin'",
		},
	}
}

// BasicAuthFlowCase represents a test case for basic authentication flow verification.
type BasicAuthFlowCase struct {
	Name               string
	Username           string
	Password           string
	ExpectSuccess      bool
	ExpectedPermission string // "Admin", "Operator", etc.
	ExpectedAuthMethod string // "basic"
	ExpectStatusCode   int
	Description        string
}

// BasicAuthFlowCases returns the standard basic authentication flow test cases.
// Cases 39a cover admin user validation and 39b cover DB role validation (current behavior).
func BasicAuthFlowCases() []BasicAuthFlowCase {
	return []BasicAuthFlowCase{
		{
			Name:               "39a_admin_correct_password",
			Username:           "admin",
			Password:           "admin-secret",
			ExpectSuccess:      true,
			ExpectedPermission: "Admin",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   200,
			Description:        "Valid admin credentials should authenticate successfully with Admin permission",
		},
		{
			Name:               "39a_admin_wrong_password",
			Username:           "admin",
			Password:           "wrong-password",
			ExpectSuccess:      false,
			ExpectedPermission: "",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   401,
			Description:        "Wrong admin password should return 401 Unauthorized",
		},
		{
			Name:               "39a_missing_auth_header",
			Username:           "",
			Password:           "",
			ExpectSuccess:      false,
			ExpectedPermission: "",
			ExpectedAuthMethod: "",
			ExpectStatusCode:   401,
			Description:        "Missing Authorization header should return 401 Unauthorized",
		},
		{
			Name:               "39a_malformed_auth_header",
			Username:           "",
			Password:           "",
			ExpectSuccess:      false,
			ExpectedPermission: "",
			ExpectedAuthMethod: "",
			ExpectStatusCode:   401,
			Description:        "Malformed Basic auth header should return 401 Unauthorized",
		},
		{
			Name:               "39b_unknown_user",
			Username:           "unknown-user",
			Password:           "some-password",
			ExpectSuccess:      false,
			ExpectedPermission: "",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   401,
			Description:        "Unknown user should return 401 Unauthorized (DB role validation not implemented)",
		},
		{
			Name:               "39b_operator_user",
			Username:           "operator",
			Password:           "operator-pass",
			ExpectSuccess:      true,
			ExpectedPermission: "Operator",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   200,
			Description:        "Operator user with correct password should authenticate with Operator permission",
		},
		{
			Name:               "39b_viewer_user",
			Username:           "viewer",
			Password:           "viewer-pass",
			ExpectSuccess:      true,
			ExpectedPermission: "Basic",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   200,
			Description:        "Viewer user with correct password should authenticate with Basic permission",
		},
		{
			Name:               "39b_reader_user",
			Username:           "reader",
			Password:           "reader-pass",
			ExpectSuccess:      true,
			ExpectedPermission: "Self Only",
			ExpectedAuthMethod: "basic",
			ExpectStatusCode:   200,
			Description:        "Reader user with correct password should authenticate with Self Only permission",
		},
	}
}

// OIDCFlowCase represents a test case for OIDC full flow verification.
type OIDCFlowCase struct {
	Name               string
	Username           string
	Role               string // Keycloak realm role
	ExpectedPermission string // Expected PermissionLevel string
	ExpectedAuthMethod string // "oidc"
	Description        string
}

// PermissionMatrixCase represents a test case for full permission matrix verification.
// Each case maps an HTTP method + path to the minimum required permission level.
type PermissionMatrixCase struct {
	Name          string
	Method        string
	Path          string
	RequiredLevel string // "Basic", "OperatorBasic", "Operator", "Admin"
	Description   string
}

// PermissionMatrixCases returns representative test cases for each permission level
// covering the full API permission matrix.
func PermissionMatrixCases() []PermissionMatrixCase {
	return []PermissionMatrixCase{
		// --- Basic (read-only cluster state) ---
		{
			Name:          "get_clusters",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters",
			RequiredLevel: "Basic",
			Description:   "List clusters requires Basic permission",
		},
		{
			Name:          "get_cluster",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster",
			RequiredLevel: "Basic",
			Description:   "Get cluster requires Basic permission",
		},
		{
			Name:          "get_cluster_status",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/status",
			RequiredLevel: "Basic",
			Description:   "Get cluster status requires Basic permission",
		},
		{
			Name:          "get_scale_status",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/scale/status",
			RequiredLevel: "Basic",
			Description:   "Get scale status requires Basic permission",
		},
		{
			Name:          "get_segments",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/segments",
			RequiredLevel: "Basic",
			Description:   "List segments requires Basic permission",
		},
		{
			Name:          "get_mirroring",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/mirroring",
			RequiredLevel: "Basic",
			Description:   "Get mirroring status requires Basic permission",
		},
		{
			Name:          "get_standby",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/standby",
			RequiredLevel: "Basic",
			Description:   "Get standby status requires Basic permission",
		},
		{
			Name:          "get_rebalance_status",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/rebalance/status",
			RequiredLevel: "Basic",
			Description:   "Get rebalance status requires Basic permission",
		},
		{
			Name:          "get_query_monitoring",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/queries",
			RequiredLevel: "OperatorBasic",
			Description:   "Get query monitoring requires Operator Basic permission",
		},
		{
			Name:          "get_active_queries",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/queries/active",
			RequiredLevel: "Basic",
			Description:   "Get active queries requires Basic permission",
		},
		{
			Name:          "get_backups",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/backups",
			RequiredLevel: "Basic",
			Description:   "List backups requires Basic permission",
		},
		{
			Name:          "get_backup",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/backups/backup-1",
			RequiredLevel: "Basic",
			Description:   "Get backup requires Basic permission",
		},
		{
			Name:          "get_pvcs",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/pvcs",
			RequiredLevel: "Basic",
			Description:   "List PVCs requires Basic permission",
		},
		{
			Name:          "get_disk_usage",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/disk-usage",
			RequiredLevel: "Basic",
			Description:   "Get disk usage requires Basic permission",
		},
		{
			Name:          "get_tables",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/tables",
			RequiredLevel: "Basic",
			Description:   "List tables requires Basic permission",
		},
		{
			Name:          "get_table_detail",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/tables/public/test_table",
			RequiredLevel: "Basic",
			Description:   "Get table detail requires Basic permission",
		},
		{
			Name:          "get_recommendations",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/recommendations",
			RequiredLevel: "Basic",
			Description:   "List recommendations requires Basic permission",
		},
		{
			Name:          "get_usage_report",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/usage-report",
			RequiredLevel: "Basic",
			Description:   "Get usage report requires Basic permission",
		},
		{
			Name:          "get_data_loading_jobs",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs",
			RequiredLevel: "Basic",
			Description:   "List data loading jobs requires Basic permission",
		},
		{
			Name:          "get_data_loading_job",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1",
			RequiredLevel: "Basic",
			Description:   "Get data loading job requires Basic permission",
		},
		{
			Name:          "get_workload",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload",
			RequiredLevel: "Basic",
			Description:   "Get workload requires Basic permission",
		},
		{
			Name:          "get_resource_groups",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-groups",
			RequiredLevel: "Basic",
			Description:   "List resource groups requires Basic permission",
		},
		{
			Name:          "get_workload_rules",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/rules",
			RequiredLevel: "Basic",
			Description:   "List workload rules requires Basic permission",
		},
		{
			Name:          "get_resource_queues",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-queues",
			RequiredLevel: "Basic",
			Description:   "List resource queues requires Basic permission",
		},

		// --- OperatorBasic (config + sessions viewing) ---
		{
			Name:          "get_config",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/config",
			RequiredLevel: "OperatorBasic",
			Description:   "Get config requires OperatorBasic permission",
		},
		{
			Name:          "get_sessions",
			Method:        "GET",
			Path:          "/api/v1alpha1/clusters/test-cluster/sessions",
			RequiredLevel: "OperatorBasic",
			Description:   "List sessions requires OperatorBasic permission",
		},

		// --- Operator (cluster operations, mutations) ---
		{
			Name:          "post_start",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/start",
			RequiredLevel: "Operator",
			Description:   "Start cluster requires Operator permission",
		},
		{
			Name:          "post_stop",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/stop",
			RequiredLevel: "Operator",
			Description:   "Stop cluster requires Operator permission",
		},
		{
			Name:          "post_restart",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/restart",
			RequiredLevel: "Operator",
			Description:   "Restart cluster requires Operator permission",
		},
		{
			Name:          "post_reload_config",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/reload",
			RequiredLevel: "Operator",
			Description:   "Reload config requires Operator permission",
		},
		{
			Name:          "put_config",
			Method:        "PUT",
			Path:          "/api/v1alpha1/clusters/test-cluster/config",
			RequiredLevel: "Operator",
			Description:   "Update config requires Operator permission",
		},
		{
			Name:          "post_cancel_query",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/sessions/1234/cancel",
			RequiredLevel: "Operator",
			Description:   "Cancel query requires Operator permission",
		},
		{
			Name:          "delete_terminate_session",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/sessions/1234",
			RequiredLevel: "Operator",
			Description:   "Terminate session requires Operator permission",
		},
		{
			Name:          "post_vacuum",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/maintenance/vacuum",
			RequiredLevel: "Operator",
			Description:   "Vacuum requires Operator permission",
		},
		{
			Name:          "post_analyze",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/maintenance/analyze",
			RequiredLevel: "Operator",
			Description:   "Analyze requires Operator permission",
		},
		{
			Name:          "post_reindex",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/maintenance/reindex",
			RequiredLevel: "Operator",
			Description:   "Reindex requires Operator permission",
		},
		{
			Name:          "post_recovery",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/recovery",
			RequiredLevel: "Operator",
			Description:   "Recovery requires Operator permission",
		},
		{
			Name:          "post_rebalance",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/rebalance",
			RequiredLevel: "Operator",
			Description:   "Rebalance requires Operator permission",
		},
		{
			Name:          "post_create_backup",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/backups",
			RequiredLevel: "Operator",
			Description:   "Create backup requires Operator permission",
		},
		{
			Name:          "post_create_data_loading_job",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs",
			RequiredLevel: "Operator",
			Description:   "Create data loading job requires Operator permission",
		},
		{
			Name:          "put_update_data_loading_job",
			Method:        "PUT",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1",
			RequiredLevel: "Operator",
			Description:   "Update data loading job requires Operator permission",
		},
		{
			Name:          "post_start_data_loading_job",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1/start",
			RequiredLevel: "Operator",
			Description:   "Start data loading job requires Operator permission",
		},
		{
			Name:          "post_stop_data_loading_job",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1/stop",
			RequiredLevel: "Operator",
			Description:   "Stop data loading job requires Operator permission",
		},
		{
			Name:          "post_create_resource_group",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-groups",
			RequiredLevel: "Operator",
			Description:   "Create resource group requires Operator permission",
		},
		{
			Name:          "put_update_resource_group",
			Method:        "PUT",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-groups/test_group",
			RequiredLevel: "Operator",
			Description:   "Update resource group requires Operator permission",
		},
		{
			Name:          "post_assign_resource_group",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-groups/test_group/assign",
			RequiredLevel: "Operator",
			Description:   "Assign resource group requires Operator permission",
		},
		{
			Name:          "post_create_workload_rule",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/rules",
			RequiredLevel: "Operator",
			Description:   "Create workload rule requires Operator permission",
		},
		{
			Name:          "put_update_workload_rule",
			Method:        "PUT",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/rules/test_rule",
			RequiredLevel: "Operator",
			Description:   "Update workload rule requires Operator permission",
		},
		{
			Name:          "delete_workload_rule",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/rules/test_rule",
			RequiredLevel: "Operator",
			Description:   "Delete workload rule requires Operator permission",
		},
		{
			Name:          "post_create_resource_queue",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-queues",
			RequiredLevel: "Operator",
			Description:   "Create resource queue requires Operator permission",
		},
		{
			Name:          "delete_resource_queue",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-queues/test_queue",
			RequiredLevel: "Operator",
			Description:   "Delete resource queue requires Operator permission",
		},
		{
			Name:          "post_trigger_recommendation_scan",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/storage/recommendations/scan",
			RequiredLevel: "Operator",
			Description:   "Trigger recommendation scan requires Operator permission",
		},

		// --- Admin (destructive / high-impact operations) ---
		{
			Name:          "post_create_cluster",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters",
			RequiredLevel: "Admin",
			Description:   "Create cluster requires Admin permission",
		},
		{
			Name:          "delete_cluster",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster",
			RequiredLevel: "Admin",
			Description:   "Delete cluster requires Admin permission",
		},
		{
			Name:          "post_activate_standby",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/standby/activate",
			RequiredLevel: "Admin",
			Description:   "Activate standby requires Admin permission",
		},
		{
			Name:          "delete_backup",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/backups/backup-1",
			RequiredLevel: "Admin",
			Description:   "Delete backup requires Admin permission",
		},
		{
			Name:          "post_restore_backup",
			Method:        "POST",
			Path:          "/api/v1alpha1/clusters/test-cluster/backups/backup-1/restore",
			RequiredLevel: "Admin",
			Description:   "Restore backup requires Admin permission",
		},
		{
			Name:          "delete_data_loading_job",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/job1",
			RequiredLevel: "Admin",
			Description:   "Delete data loading job requires Admin permission",
		},
		{
			Name:          "delete_resource_group",
			Method:        "DELETE",
			Path:          "/api/v1alpha1/clusters/test-cluster/workload/resource-groups/test_group",
			RequiredLevel: "Admin",
			Description:   "Delete resource group requires Admin permission",
		},
	}
}

// HBACustomRuleCase represents a test case for custom HBA rule verification.
type HBACustomRuleCase struct {
	Name              string
	Rules             []cbv1alpha1.HBARule
	ExpectedLines     []string
	ExpectedCount     int
	HasHashAnnotation bool
	Description       string
}

// HBACustomRuleCases returns the test cases for verifying custom HBA rule generation.
func HBACustomRuleCases() []HBACustomRuleCase {
	// scenario44CustomRules are the four custom rules from the scenario44 example.
	scenario44CustomRules := []cbv1alpha1.HBARule{
		{
			Type:     cbv1alpha1.HBATypeLocal,
			Database: "all",
			User:     "gpadmin",
			Method:   cbv1alpha1.AuthMethodTrust,
		},
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "10.0.0.0/8",
			Method:   cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type:     cbv1alpha1.HBATypeHostSSL,
			Database: "all",
			User:     "all",
			Address:  "192.168.0.0/16",
			Method:   cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "0.0.0.0/0",
			Method:   cbv1alpha1.AuthMethodReject,
		},
	}

	return []HBACustomRuleCase{
		{
			Name:  "all_four_custom_rules_present",
			Rules: scenario44CustomRules,
			ExpectedLines: []string{
				"local\tall\tgpadmin\ttrust",
				"host\tall\tall\t10.0.0.0/8\tscram-sha-256",
				"hostssl\tall\tall\t192.168.0.0/16\tscram-sha-256",
				"host\tall\tall\t0.0.0.0/0\treject",
			},
			ExpectedCount:     4,
			HasHashAnnotation: true,
			Description:       "all 4 custom rules should be present in the generated pg_hba.conf",
		},
		{
			Name:  "custom_rules_preserve_order",
			Rules: scenario44CustomRules,
			ExpectedLines: []string{
				"local\tall\tgpadmin\ttrust",
				"host\tall\tall\t10.0.0.0/8\tscram-sha-256",
				"hostssl\tall\tall\t192.168.0.0/16\tscram-sha-256",
				"host\tall\tall\t0.0.0.0/0\treject",
			},
			ExpectedCount:     4,
			HasHashAnnotation: true,
			Description:       "custom rules must appear in the same order as specified in the CRD",
		},
		{
			Name:  "custom_rules_exclude_defaults",
			Rules: scenario44CustomRules,
			ExpectedLines: []string{
				"local\tall\tgpadmin\ttrust",
			},
			ExpectedCount:     4,
			HasHashAnnotation: true,
			Description:       "default rules must NOT be present when custom rules are specified",
		},
		{
			Name: "single_reject_rule",
			Rules: []cbv1alpha1.HBARule{
				{
					Type:     cbv1alpha1.HBATypeHost,
					Database: "all",
					User:     "all",
					Address:  "0.0.0.0/0",
					Method:   cbv1alpha1.AuthMethodReject,
				},
			},
			ExpectedLines: []string{
				"host\tall\tall\t0.0.0.0/0\treject",
			},
			ExpectedCount:     1,
			HasHashAnnotation: true,
			Description:       "a single reject rule should produce exactly one rule line",
		},
		{
			Name: "rule_with_options",
			Rules: []cbv1alpha1.HBARule{
				{
					Type:     cbv1alpha1.HBATypeHost,
					Database: "all",
					User:     "all",
					Address:  "0.0.0.0/0",
					Method:   cbv1alpha1.AuthMethodLDAP,
					Options:  "ldapserver=ldap.example.com ldapbasedn=\"dc=example,dc=com\"",
				},
			},
			ExpectedLines: []string{
				"host\tall\tall\t0.0.0.0/0\tldap\tldapserver=ldap.example.com ldapbasedn=\"dc=example,dc=com\"",
			},
			ExpectedCount:     1,
			HasHashAnnotation: true,
			Description:       "HBA rule with options should include options in the formatted output",
		},
	}
}

// PasswordRotationCase represents a test case for password rotation verification.
type PasswordRotationCase struct {
	Name           string
	OldPassword    string
	NewPassword    string
	ExpectOldFails bool
	ExpectNewWorks bool
	Description    string
}

// PasswordRotationCases returns the standard password rotation test cases.
func PasswordRotationCases() []PasswordRotationCase {
	return []PasswordRotationCase{
		{
			Name:           "40a_rotate_admin_password",
			OldPassword:    "old-admin-secret",
			NewPassword:    "new-admin-secret-2024",
			ExpectOldFails: true,
			ExpectNewWorks: true,
			Description:    "After rotating the admin password, the old password should fail and the new password should work",
		},
		{
			Name:           "40b_rotate_to_complex_password",
			OldPassword:    "simple",
			NewPassword:    "C0mpl3x!P@ssw0rd#2024",
			ExpectOldFails: true,
			ExpectNewWorks: true,
			Description:    "Rotation to a complex password should work correctly",
		},
		{
			Name:           "40c_rotate_same_password",
			OldPassword:    "same-password",
			NewPassword:    "same-password",
			ExpectOldFails: false,
			ExpectNewWorks: true,
			Description:    "Rotating to the same password should still authenticate successfully",
		},
		{
			Name:           "40d_rotate_operator_password",
			OldPassword:    "operator-old",
			NewPassword:    "operator-new-rotated",
			ExpectOldFails: true,
			ExpectNewWorks: true,
			Description:    "Operator user password rotation should invalidate old and accept new credentials",
		},
		{
			Name:           "40e_rotate_to_long_password",
			OldPassword:    "short",
			NewPassword:    "a-very-long-password-that-is-still-within-bcrypt-72-byte-limit-ok",
			ExpectOldFails: true,
			ExpectNewWorks: true,
			Description:    "Rotation to a long password within bcrypt limits should work",
		},
	}
}

// CTLAuthCase represents a test case for cloudberry-ctl auth command verification.
type CTLAuthCase struct {
	Name        string
	Command     string // "login", "status", "logout"
	AuthMethod  string // "basic", "oidc"
	ExpectError bool
	Description string
}

// CTLAuthCases returns the standard cloudberry-ctl auth command test cases.
func CTLAuthCases() []CTLAuthCase {
	return []CTLAuthCase{
		{
			Name:        "49a_oidc_login_browser_not_implemented",
			Command:     "login",
			AuthMethod:  "oidc",
			ExpectError: true,
			Description: "OIDC login without credentials should return not-implemented for browser flow",
		},
		{
			Name:        "49b_basic_login_valid_credentials",
			Command:     "login",
			AuthMethod:  "basic",
			ExpectError: false,
			Description: "Basic login with valid credentials should succeed",
		},
		{
			Name:        "49b_basic_login_invalid_password",
			Command:     "login",
			AuthMethod:  "basic",
			ExpectError: true,
			Description: "Basic login with invalid password should fail",
		},
		{
			Name:        "49c_auth_status_authenticated",
			Command:     "status",
			AuthMethod:  "basic",
			ExpectError: false,
			Description: "Auth status with valid credentials should show authenticated",
		},
		{
			Name:        "49c_auth_status_unauthenticated",
			Command:     "status",
			AuthMethod:  "basic",
			ExpectError: false,
			Description: "Auth status with invalid credentials should show unauthenticated (no error)",
		},
		{
			Name:        "49d_logout",
			Command:     "logout",
			AuthMethod:  "",
			ExpectError: false,
			Description: "Logout should always succeed and remind user to unset env vars",
		},
	}
}

// AuditCase represents a test case for auditing verification.
type AuditCase struct {
	Name         string
	Category     string // "connection", "statement", "operator"
	ConfigParams map[string]string
	ExpectInConf []string
	Description  string
}

// AuditCases returns the standard auditing test cases for scenarios 50a-50c.
func AuditCases() []AuditCase {
	return []AuditCase{
		{
			Name:     "50a_connection_audit_logging",
			Category: "connection",
			ConfigParams: map[string]string{
				"log_connections":    "on",
				"log_disconnections": "on",
			},
			ExpectInConf: []string{
				"log_connections = 'on'",
				"log_disconnections = 'on'",
			},
			Description: "Connection auditing: log_connections and log_disconnections should appear in postgresql.conf",
		},
		{
			Name:     "50b_statement_audit_ddl",
			Category: "statement",
			ConfigParams: map[string]string{
				"log_statement": "ddl",
			},
			ExpectInConf: []string{
				"log_statement = 'ddl'",
			},
			Description: "Statement auditing: log_statement=ddl should appear in postgresql.conf",
		},
		{
			Name:     "50b_statement_audit_duration",
			Category: "statement",
			ConfigParams: map[string]string{
				"log_min_duration_statement": "1000",
				"log_duration":               "on",
			},
			ExpectInConf: []string{
				"log_duration = 'on'",
				"log_min_duration_statement = '1000'",
			},
			Description: "Statement auditing: log_duration and log_min_duration_statement should appear in postgresql.conf",
		},
		{
			Name:     "50b_statement_audit_all_params",
			Category: "statement",
			ConfigParams: map[string]string{
				"log_statement":              "ddl",
				"log_min_duration_statement": "1000",
				"log_duration":               "on",
			},
			ExpectInConf: []string{
				"log_statement = 'ddl'",
				"log_duration = 'on'",
				"log_min_duration_statement = '1000'",
			},
			Description: "Statement auditing: all statement audit params should appear in postgresql.conf",
		},
		{
			Name:        "50c_operator_audit_basic_auth_success",
			Category:    "operator",
			Description: "Operator audit: basic auth success log should contain username, method, source_ip, and permission",
		},
		{
			Name:        "50c_operator_audit_basic_auth_failure",
			Category:    "operator",
			Description: "Operator audit: authentication failure log should contain method, error, and remote_addr",
		},
		{
			Name:        "50c_operator_audit_permission_denied",
			Category:    "operator",
			Description: "Operator audit: permission denied should be logged with username, method, source_ip, and required permission",
		},
		{
			Name:        "50c_operator_audit_json_format",
			Category:    "operator",
			Description: "Operator audit: all audit entries should be valid JSON with level, msg, and timestamp fields",
		},
		{
			Name:        "50c_operator_audit_config_change",
			Category:    "operator",
			Description: "Operator audit: config change should be logged with cluster, username, method, and source_ip",
		},
		{
			Name:        "50c_operator_audit_role_management",
			Category:    "operator",
			Description: "Operator audit: role assignment should be logged with cluster, role, username, method, and source_ip",
		},
		{
			Name:        "50c_operator_audit_success_all_fields",
			Category:    "operator",
			Description: "Operator audit: success log should contain method, source_ip, permission, and timestamp (via slog time field)",
		},
	}
}

// OIDCFlowCases returns the standard OIDC full flow test cases.
// Each case represents a user with a specific Keycloak realm role and the
// expected permission level after role mapping.
func OIDCFlowCases() []OIDCFlowCase {
	return []OIDCFlowCase{
		{
			Name:               "admin_role_maps_to_admin_permission",
			Username:           "admin-user",
			Role:               "admin",
			ExpectedPermission: "Admin",
			ExpectedAuthMethod: "oidc",
			Description:        "User with 'admin' realm role should map to Admin permission",
		},
		{
			Name:               "operator_role_maps_to_operator_permission",
			Username:           "operator-user",
			Role:               "operator",
			ExpectedPermission: "Operator",
			ExpectedAuthMethod: "oidc",
			Description:        "User with 'operator' realm role should map to Operator permission",
		},
		{
			Name:               "operator_basic_role_maps_to_operator_basic_permission",
			Username:           "opbasic-user",
			Role:               "operator-basic",
			ExpectedPermission: "Operator Basic",
			ExpectedAuthMethod: "oidc",
			Description:        "User with 'operator-basic' realm role should map to Operator Basic permission",
		},
		{
			Name:               "user_role_maps_to_basic_permission",
			Username:           "basic-user",
			Role:               "user",
			ExpectedPermission: "Basic",
			ExpectedAuthMethod: "oidc",
			Description:        "User with 'user' realm role should map to Basic permission",
		},
		{
			Name:               "reader_role_maps_to_self_only_permission",
			Username:           "reader-user",
			Role:               "reader",
			ExpectedPermission: "Self Only",
			ExpectedAuthMethod: "oidc",
			Description:        "User with 'reader' realm role should map to Self Only permission",
		},
	}
}

// SecurityHeaderCase represents a test case for security header verification.
type SecurityHeaderCase struct {
	Name          string
	Header        string
	ExpectedValue string
	Description   string
}

// SecurityHeaderCases returns the standard security header test cases for Scenario 51.
func SecurityHeaderCases() []SecurityHeaderCase {
	return []SecurityHeaderCase{
		{
			Name:          "cache_control",
			Header:        "Cache-Control",
			ExpectedValue: "no-store",
			Description:   "Cache-Control should prevent caching of API responses",
		},
		{
			Name:          "content_security_policy",
			Header:        "Content-Security-Policy",
			ExpectedValue: "default-src 'self'",
			Description:   "Content-Security-Policy should restrict resource loading to same origin",
		},
		{
			Name:          "permissions_policy",
			Header:        "Permissions-Policy",
			ExpectedValue: "camera=(), microphone=()",
			Description:   "Permissions-Policy should disable camera and microphone access",
		},
		{
			Name:          "referrer_policy",
			Header:        "Referrer-Policy",
			ExpectedValue: "strict-origin-when-cross-origin",
			Description:   "Referrer-Policy should limit referrer information sent cross-origin",
		},
		{
			Name:          "strict_transport_security",
			Header:        "Strict-Transport-Security",
			ExpectedValue: "max-age=31536000; includeSubDomains",
			Description:   "HSTS should enforce HTTPS with one-year max-age and include subdomains",
		},
		{
			Name:          "x_content_type_options",
			Header:        "X-Content-Type-Options",
			ExpectedValue: "nosniff",
			Description:   "X-Content-Type-Options should prevent MIME type sniffing",
		},
		{
			Name:          "x_frame_options",
			Header:        "X-Frame-Options",
			ExpectedValue: "DENY",
			Description:   "X-Frame-Options should prevent framing of API responses",
		},
		{
			Name:          "x_xss_protection",
			Header:        "X-XSS-Protection",
			ExpectedValue: "1; mode=block",
			Description:   "X-XSS-Protection should enable browser XSS filtering in block mode",
		},
	}
}

// NegativeEdgeCaseCase represents a test case for negative/edge case verification.
type NegativeEdgeCaseCase struct {
	Name           string
	SubScenario    string // "52a", "52b", "52c", "52d", "52e", "52f", "52g", "52h"
	Category       string // "jwt", "vault", "config", "auth"
	ExpectedStatus int    // expected HTTP status code (401, 500, etc.)
	Description    string
}

// NegativeEdgeCaseCases returns the standard negative/edge case test cases for Scenario 52.
func NegativeEdgeCaseCases() []NegativeEdgeCaseCase {
	return []NegativeEdgeCaseCase{
		{
			Name:           "52a_jwt_wrong_issuer",
			SubScenario:    "52a",
			Category:       "jwt",
			ExpectedStatus: 401,
			Description:    "JWT with wrong issuer should be rejected with 401",
		},
		{
			Name:           "52b_jwt_wrong_audience",
			SubScenario:    "52b",
			Category:       "jwt",
			ExpectedStatus: 401,
			Description:    "JWT with wrong audience should be rejected with 401",
		},
		{
			Name:           "52c_jwt_expired",
			SubScenario:    "52c",
			Category:       "jwt",
			ExpectedStatus: 401,
			Description:    "Expired JWT should be rejected with 401",
		},
		{
			Name:           "52d_jwt_future_iat",
			SubScenario:    "52d",
			Category:       "jwt",
			ExpectedStatus: 401,
			Description:    "JWT with future iat should be rejected with 401",
		},
		{
			Name:           "52e_token_refresh_failure",
			SubScenario:    "52e",
			Category:       "jwt",
			ExpectedStatus: 401,
			Description:    "Expired token without refresh should result in 401",
		},
		{
			Name:           "52f_vault_connection_retry",
			SubScenario:    "52f",
			Category:       "vault",
			ExpectedStatus: 0,
			Description:    "Vault connection failure should trigger exponential backoff retries",
		},
		{
			Name:           "52g_invalid_oidc_config",
			SubScenario:    "52g",
			Category:       "config",
			ExpectedStatus: 0,
			Description:    "Invalid OIDC config should fail gracefully; Basic auth should still work",
		},
		{
			Name:           "52h_missing_admin_secret",
			SubScenario:    "52h",
			Category:       "auth",
			ExpectedStatus: 401,
			Description:    "Missing admin password secret should cause Basic auth to fail with 401",
		},
	}
}

// QueryHistoryCase represents a test case for query history operations (Scenario 61).
type QueryHistoryCase struct {
	Name           string
	SubScenario    string // "61a", "61b", "61c", "61d"
	Category       string // "browse", "search", "export", "detail", "error", "auth"
	Description    string
	Pattern        string // search pattern (regex or wildcard)
	PatternType    string // "regex" or "wildcard"
	User           string // filter by username
	Database       string // filter by database name
	ResourceGroup  string // filter by resource group
	Limit          int    // pagination limit
	Offset         int    // pagination offset
	QueryID        string // for detail lookups
	ExpectedCount  int    // expected number of results
	ExpectedStatus int    // expected HTTP status code
	ExpectError    bool   // whether an error is expected
}

// QueryHistoryCases returns the test cases for query history (Scenario 61).
func QueryHistoryCases() []QueryHistoryCase {
	return []QueryHistoryCase{
		// --- 61a: Browse History with Charts ---
		{
			Name:           "61a_browse_all_history",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Browse all history with default pagination returns up to 50 entries",
			ExpectedCount:  3,
			ExpectedStatus: 200,
		},
		{
			Name:           "61a_browse_empty_history",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Browse empty history returns empty array with total=0",
			ExpectedCount:  0,
			ExpectedStatus: 200,
		},
		{
			Name:           "61a_browse_paginate_page1",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Paginate page 1 with limit=2, offset=0",
			Limit:          2,
			Offset:         0,
			ExpectedCount:  2,
			ExpectedStatus: 200,
		},
		{
			Name:           "61a_browse_paginate_page2",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Paginate page 2 with limit=2, offset=2",
			Limit:          2,
			Offset:         2,
			ExpectedCount:  1,
			ExpectedStatus: 200,
		},
		{
			Name:           "61a_browse_duration_metrics_present",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Each entry has durationMs field populated",
			ExpectedCount:  3,
			ExpectedStatus: 200,
		},
		{
			Name:           "61a_browse_resource_usage_present",
			SubScenario:    "61a",
			Category:       "browse",
			Description:    "Each entry has cpuTimeMs, memoryBytes, spillBytes fields",
			ExpectedCount:  3,
			ExpectedStatus: 200,
		},

		// --- 61b: Advanced Search ---
		{
			Name:           "61b_regex_search_match",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Regex search with SELECT.*FROM orders returns matching queries",
			Pattern:        "SELECT.*FROM orders",
			PatternType:    "regex",
			ExpectedCount:  1,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_wildcard_search_match",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Wildcard search with SELECT * returns matching queries",
			Pattern:        "SELECT *",
			PatternType:    "wildcard",
			ExpectedCount:  2,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_filter_by_user",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Filter by username returns only that user's queries",
			User:           "analyst",
			ExpectedCount:  2,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_filter_by_database",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Filter by database returns only that database's queries",
			Database:       "mydb",
			ExpectedCount:  1,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_filter_by_resource_group",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Filter by resource group returns only that group's queries",
			ResourceGroup:  "analytics",
			ExpectedCount:  1,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_combined_filters",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Combined user + database filters are AND-combined",
			User:           "analyst",
			Database:       "mydb",
			ExpectedCount:  1,
			ExpectedStatus: 200,
		},
		{
			Name:           "61b_invalid_regex",
			SubScenario:    "61b",
			Category:       "search",
			Description:    "Invalid regex pattern returns 400 error",
			Pattern:        "[invalid",
			PatternType:    "regex",
			ExpectedStatus: 400,
			ExpectError:    true,
		},

		// --- 61c: Export to CSV ---
		{
			Name:           "61c_export_basic_csv",
			SubScenario:    "61c",
			Category:       "export",
			Description:    "Export generates valid CSV with header and data rows",
			ExpectedStatus: 200,
		},
		{
			Name:           "61c_export_csv_headers",
			SubScenario:    "61c",
			Category:       "export",
			Description:    "CSV has correct column headers matching specification",
			ExpectedStatus: 200,
		},
		{
			Name:           "61c_export_csv_content_type",
			SubScenario:    "61c",
			Category:       "export",
			Description:    "Response Content-Type is text/csv",
			ExpectedStatus: 200,
		},
		{
			Name:           "61c_export_csv_with_filters",
			SubScenario:    "61c",
			Category:       "export",
			Description:    "Export with filter criteria applies filters",
			User:           "analyst",
			ExpectedStatus: 200,
		},

		// --- 61d: Historical Query Details ---
		{
			Name:           "61d_detail_with_metrics",
			SubScenario:    "61d",
			Category:       "detail",
			Description:    "Get historical query with all execution metrics",
			QueryID:        "q-1234-5678",
			ExpectedStatus: 200,
		},
		{
			Name:           "61d_detail_with_plan",
			SubScenario:    "61d",
			Category:       "detail",
			Description:    "Detail includes saved EXPLAIN plan",
			QueryID:        "q-1234-5678",
			ExpectedStatus: 200,
		},
		{
			Name:           "61d_detail_not_found",
			SubScenario:    "61d",
			Category:       "detail",
			Description:    "Returns 404 for unknown query ID",
			QueryID:        "q-nonexistent",
			ExpectedStatus: 404,
			ExpectError:    true,
		},

		// --- Cross-cutting ---
		{
			Name:           "61_unauthenticated",
			SubScenario:    "all",
			Category:       "auth",
			Description:    "Unauthenticated requests rejected with 401",
			ExpectedStatus: 401,
			ExpectError:    true,
		},
	}
}

// PlanCheckCase represents a plan analysis test case (Scenario 62).
type PlanCheckCase struct {
	Name               string
	PlanText           string
	ExpectError        bool
	ExpectedIssueCount int      // minimum expected issue count (-1 means don't check)
	ExpectedCategories []string // e.g., ["sequential_scan", "sort_spill"]
	Description        string
}

// Sample EXPLAIN ANALYZE plan text constants for plan check test cases.
const (
	// sampleSeqScanPlan contains a sequential scan on a large table.
	sampleSeqScanPlan = `Seq Scan on large_orders  (cost=0.00..5000.00 rows=100000 width=100) (actual time=0.020..120.000 rows=100000 loops=1)
  Filter: (region = 'US')
  Rows Removed by Filter: 400000
Planning Time: 0.200 ms
Execution Time: 120.500 ms`

	// sampleRowMismatchPlan contains a nested loop with row estimate mismatch.
	sampleRowMismatchPlan = `Nested Loop  (cost=0.00..500.00 rows=10 width=72) (actual time=0.050..2500.000 rows=200000 loops=1)
  ->  Seq Scan on dim_products p  (cost=0.00..1.10 rows=10 width=36) (actual time=0.010..0.020 rows=10 loops=1)
  ->  Index Scan using idx_sales_product on sales s  (cost=0.29..49.90 rows=1 width=36) (actual time=0.001..200.000 rows=20000 loops=10)
        Index Cond: (s.product_id = p.id)
Planning Time: 0.300 ms
Execution Time: 2500.500 ms`

	// sampleSortSpillPlan contains a sort that spills to disk.
	sampleSortSpillPlan = `Sort  (cost=8000.00..8025.00 rows=10000 width=150) (actual time=200.000..350.000 rows=10000 loops=1)
  Sort Key: event_timestamp DESC
  Sort Method: external merge  Disk: 8192kB
  ->  Seq Scan on events  (cost=0.00..500.00 rows=10000 width=150) (actual time=0.010..20.000 rows=10000 loops=1)
Planning Time: 0.150 ms
Execution Time: 350.500 ms`

	// sampleFullPlan contains all 3 issue types: seq scan, row mismatch, sort spill.
	sampleFullPlan = `Sort  (cost=15000.00..15025.00 rows=10000 width=200) (actual time=850.123..1200.456 rows=10000 loops=1)
  Sort Key: o.created_at DESC
  Sort Method: external merge  Disk: 16384kB
  ->  Nested Loop  (cost=0.00..12000.00 rows=100 width=200) (actual time=0.500..800.000 rows=500000 loops=1)
        ->  Seq Scan on orders o  (cost=0.00..2500.00 rows=50000 width=100) (actual time=0.020..45.000 rows=50000 loops=1)
              Filter: (status = 'active')
              Rows Removed by Filter: 150000
        ->  Index Scan using idx_items_order_id on order_items i  (cost=0.29..0.50 rows=1 width=100) (actual time=0.001..0.010 rows=10 loops=50000)
              Index Cond: (i.order_id = o.id)
Planning Time: 2.345 ms
Execution Time: 1234.567 ms`

	// sampleCleanPlan contains an optimized plan with no issues.
	sampleCleanPlan = `Index Scan using idx_orders_id on orders  (cost=0.29..8.31 rows=1 width=100) (actual time=0.020..0.025 rows=1 loops=1)
  Index Cond: (id = 42)
Planning Time: 0.100 ms
Execution Time: 0.050 ms`
)

// PlanCheckCases returns the test cases for plan analysis (Scenario 62).
func PlanCheckCases() []PlanCheckCase {
	return []PlanCheckCase{
		{
			Name:               "62a_sequential_scan_detected",
			PlanText:           sampleSeqScanPlan,
			ExpectedIssueCount: 1,
			ExpectedCategories: []string{"sequential_scan"},
			Description:        "Sequential scan on large table should be flagged",
		},
		{
			Name:               "62b_row_estimate_mismatch",
			PlanText:           sampleRowMismatchPlan,
			ExpectedIssueCount: 1,
			ExpectedCategories: []string{"row_estimate_mismatch"},
			Description:        "Row estimate mismatch should be flagged with ANALYZE recommendation",
		},
		{
			Name:               "62c_sort_spill_to_disk",
			PlanText:           sampleSortSpillPlan,
			ExpectedIssueCount: 1,
			ExpectedCategories: []string{"sort_spill"},
			Description:        "Sort spill to disk should be flagged with work_mem recommendation",
		},
		{
			Name:               "62d_all_issues_combined",
			PlanText:           sampleFullPlan,
			ExpectedIssueCount: 3,
			ExpectedCategories: []string{"sequential_scan", "row_estimate_mismatch", "sort_spill"},
			Description:        "Plan with all issue types should flag all of them",
		},
		{
			Name:               "62e_clean_plan_no_issues",
			PlanText:           sampleCleanPlan,
			ExpectedIssueCount: 0,
			ExpectedCategories: nil,
			Description:        "Optimized plan should have no issues",
		},
		{
			Name:        "62f_empty_plan",
			PlanText:    "",
			ExpectError: true,
			Description: "Empty plan text should return error",
		},
	}
}

// SamplePlanText returns a named sample plan text for use in tests.
// Valid names: "seq_scan", "row_mismatch", "sort_spill", "full", "clean".
func SamplePlanText(name string) string {
	switch name {
	case "seq_scan":
		return sampleSeqScanPlan
	case "row_mismatch":
		return sampleRowMismatchPlan
	case "sort_spill":
		return sampleSortSpillPlan
	case "full":
		return sampleFullPlan
	case "clean":
		return sampleCleanPlan
	default:
		return ""
	}
}

// APIEndpointCase represents a test case for an individual REST API endpoint (Scenario 63).
type APIEndpointCase struct {
	Name           string
	SubScenario    string   // "63a" through "63m"
	Method         string   // HTTP method
	Path           string   // relative path (without cluster prefix)
	Body           string   // JSON request body (empty for GET/DELETE)
	ExpectedStatus int      // expected HTTP status code
	ExpectedKeys   []string // expected top-level JSON keys in response
	ContentType    string   // expected Content-Type (default "application/json")
	Permission     string   // required permission level: "Basic", "OperatorBasic", "Operator"
	NeedsDB        bool     // whether the endpoint requires a DB connection
	Description    string
}

// APIEndpointCases returns the test cases for all 13 REST API endpoints (Scenario 63).
func APIEndpointCases() []APIEndpointCase {
	return []APIEndpointCase{
		// --- 63a: GET /queries — Query Monitoring Overview ---
		{
			Name:           "63a_list_queries_ok",
			SubScenario:    "63a",
			Method:         "GET",
			Path:           "/queries",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"activeQueries", "queuedQueries", "blockedQueries"},
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /queries returns monitoring config and query counts",
		},
		{
			Name:           "63a_list_queries_cluster_not_found",
			SubScenario:    "63a",
			Method:         "GET",
			Path:           "/queries",
			ExpectedStatus: 404,
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /queries for non-existent cluster returns 404",
		},

		// --- 63b: GET /queries/active — Active Query Counts ---
		{
			Name:           "63b_active_queries_ok",
			SubScenario:    "63b",
			Method:         "GET",
			Path:           "/queries/active",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"activeQueries", "queuedQueries", "blockedQueries"},
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /queries/active returns integer counts",
		},
		{
			Name:           "63b_active_queries_cluster_not_found",
			SubScenario:    "63b",
			Method:         "GET",
			Path:           "/queries/active",
			ExpectedStatus: 404,
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /queries/active for non-existent cluster returns 404",
		},

		// --- 63c: GET /queries/{pid} — Query Detail ---
		{
			Name:           "63c_query_detail_ok",
			SubScenario:    "63c",
			Method:         "GET",
			Path:           "/queries/1234",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "state", "query"},
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /queries/{pid} returns detail fields",
		},
		{
			Name:           "63c_query_detail_invalid_pid",
			SubScenario:    "63c",
			Method:         "GET",
			Path:           "/queries/abc",
			ExpectedStatus: 400,
			Permission:     "OperatorBasic",
			NeedsDB:        false,
			Description:    "GET /queries/{pid} with non-numeric PID returns 400",
		},
		{
			Name:           "63c_query_detail_negative_pid",
			SubScenario:    "63c",
			Method:         "GET",
			Path:           "/queries/-1",
			ExpectedStatus: 400,
			Permission:     "OperatorBasic",
			NeedsDB:        false,
			Description:    "GET /queries/{pid} with negative PID returns 400",
		},

		// --- 63d: POST /queries/{pid}/cancel — Cancel Query ---
		{
			Name:           "63d_cancel_query_ok",
			SubScenario:    "63d",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "canceled", "status"},
			Permission:     "Operator",
			NeedsDB:        true,
			Description:    "POST /queries/{pid}/cancel returns cancellation response",
		},
		{
			Name:           "63d_cancel_query_with_reason",
			SubScenario:    "63d",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{"reason":"too slow"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "canceled", "reason"},
			Permission:     "Operator",
			NeedsDB:        true,
			Description:    "POST /queries/{pid}/cancel with reason includes reason in response",
		},
		{
			Name:           "63d_cancel_query_invalid_pid",
			SubScenario:    "63d",
			Method:         "POST",
			Path:           "/queries/abc/cancel",
			Body:           `{}`,
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "POST /queries/{pid}/cancel with invalid PID returns 400",
		},

		// --- 63e: POST /queries/{pid}/move — Move Query ---
		{
			Name:           "63e_move_query_ok",
			SubScenario:    "63e",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{"targetGroup":"etl_group"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "targetGroup", "status"},
			Permission:     "Operator",
			NeedsDB:        true,
			Description:    "POST /queries/{pid}/move returns move response with targetGroup",
		},
		{
			Name:           "63e_move_query_missing_target",
			SubScenario:    "63e",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{}`,
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "POST /queries/{pid}/move without targetGroup returns 400",
		},
		{
			Name:           "63e_move_query_invalid_target",
			SubScenario:    "63e",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{"targetGroup":"DROP TABLE;--"}`,
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "POST /queries/{pid}/move with SQL injection targetGroup returns 400",
		},
		{
			Name:           "63e_move_query_invalid_pid",
			SubScenario:    "63e",
			Method:         "POST",
			Path:           "/queries/abc/move",
			Body:           `{"targetGroup":"etl_group"}`,
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "POST /queries/{pid}/move with invalid PID returns 400",
		},

		// --- 63f: GET /queries/history — Query History ---
		{
			Name:           "63f_query_history_ok",
			SubScenario:    "63f",
			Method:         "GET",
			Path:           "/queries/history",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"items", "total", "limit", "offset"},
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /queries/history returns list with pagination",
		},
		{
			Name:           "63f_query_history_invalid_limit",
			SubScenario:    "63f",
			Method:         "GET",
			Path:           "/queries/history?limit=-1",
			ExpectedStatus: 400,
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /queries/history with invalid limit returns 400",
		},

		// --- 63g: GET /queries/history/{qid} — Query History Detail ---
		{
			Name:           "63g_query_history_detail_ok",
			SubScenario:    "63g",
			Method:         "GET",
			Path:           "/queries/history/q-1234-5678",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"queryId"},
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /queries/history/{qid} returns detail with plan",
		},
		{
			Name:           "63g_query_history_detail_not_found",
			SubScenario:    "63g",
			Method:         "GET",
			Path:           "/queries/history/q-nonexistent",
			ExpectedStatus: 404,
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /queries/history/{qid} for unknown ID returns 404",
		},

		// --- 63h: POST /queries/history/export — Export Query History ---
		{
			Name:           "63h_export_history_ok",
			SubScenario:    "63h",
			Method:         "POST",
			Path:           "/queries/history/export",
			Body:           `{}`,
			ExpectedStatus: 200,
			ContentType:    "text/csv",
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "POST /queries/history/export returns CSV content",
		},

		// --- 63i: POST /queries/plan-check — Plan Analysis ---
		{
			Name:           "63i_plan_check_ok",
			SubScenario:    "63i",
			Method:         "POST",
			Path:           "/queries/plan-check",
			Body:           `{"planText":"Seq Scan on large_orders  (cost=0.00..5000.00 rows=100000 width=100) (actual time=0.020..120.000 rows=100000 loops=1)\n  Filter: (region = 'US')\n  Rows Removed by Filter: 400000\nPlanning Time: 0.200 ms\nExecution Time: 120.500 ms"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"issues", "summary", "totalNodes"},
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "POST /queries/plan-check returns issues detected",
		},
		{
			Name:           "63i_plan_check_empty",
			SubScenario:    "63i",
			Method:         "POST",
			Path:           "/queries/plan-check",
			Body:           `{"planText":""}`,
			ExpectedStatus: 400,
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "POST /queries/plan-check with empty plan returns 400",
		},

		// --- 63j: GET /metrics/exporters — Exporter Health ---
		{
			Name:           "63j_exporter_health_ok",
			SubScenario:    "63j",
			Method:         "GET",
			Path:           "/metrics/exporters",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"exporters", "total"},
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /metrics/exporters returns exporter list with status",
		},
		{
			Name:           "63j_exporter_health_no_config",
			SubScenario:    "63j",
			Method:         "GET",
			Path:           "/metrics/exporters",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"exporters", "total", "message"},
			Permission:     "Basic",
			NeedsDB:        false,
			Description:    "GET /metrics/exporters without config returns empty list with message",
		},

		// --- 63k: GET /sessions — List Sessions ---
		{
			Name:           "63k_list_sessions_ok",
			SubScenario:    "63k",
			Method:         "GET",
			Path:           "/sessions",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"sessions", "total"},
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /sessions returns session list",
		},
		{
			Name:           "63k_list_sessions_with_filter",
			SubScenario:    "63k",
			Method:         "GET",
			Path:           "/sessions?status=running",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"sessions", "total"},
			Permission:     "OperatorBasic",
			NeedsDB:        true,
			Description:    "GET /sessions with status filter returns filtered sessions",
		},

		// --- 63l: POST /sessions/{pid}/cancel — Cancel Session Query ---
		{
			Name:           "63l_cancel_session_ok",
			SubScenario:    "63l",
			Method:         "POST",
			Path:           "/sessions/1234/cancel",
			Body:           `{}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "canceled"},
			Permission:     "Operator",
			NeedsDB:        true,
			Description:    "POST /sessions/{pid}/cancel returns cancellation result",
		},
		{
			Name:           "63l_cancel_session_invalid_pid",
			SubScenario:    "63l",
			Method:         "POST",
			Path:           "/sessions/abc/cancel",
			Body:           `{}`,
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "POST /sessions/{pid}/cancel with invalid PID returns 400",
		},

		// --- 63m: DELETE /sessions/{pid} — Terminate Session ---
		{
			Name:           "63m_terminate_session_ok",
			SubScenario:    "63m",
			Method:         "DELETE",
			Path:           "/sessions/1234",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "terminated"},
			Permission:     "Operator",
			NeedsDB:        true,
			Description:    "DELETE /sessions/{pid} returns termination result",
		},
		{
			Name:           "63m_terminate_session_invalid_pid",
			SubScenario:    "63m",
			Method:         "DELETE",
			Path:           "/sessions/-1",
			ExpectedStatus: 400,
			Permission:     "Operator",
			NeedsDB:        false,
			Description:    "DELETE /sessions/{pid} with negative PID returns 400",
		},
	}
}

// CLICommandCase represents a test case for a CLI command API endpoint (Scenario 64).
type CLICommandCase struct {
	Name                  string
	SubScenario           string   // "64a" through "64i"
	Method                string   // HTTP method
	Path                  string   // relative path (without cluster prefix)
	Body                  string   // JSON request body (empty for GET/DELETE)
	ExpectedStatus        int      // expected HTTP status code
	ExpectedKeys          []string // expected top-level JSON keys in response
	ContentType           string   // expected Content-Type (default "application/json")
	UseNonExistentCluster bool     // whether to use a non-existent cluster path
	Description           string
}

// CLICommandCases returns the test cases for all CLI command API endpoints (Scenario 64).
func CLICommandCases() []CLICommandCase {
	return []CLICommandCase{
		// --- 64a: queries list — GET /sessions ---
		{
			Name:           "64a_queries_list_ok",
			SubScenario:    "64a",
			Method:         "GET",
			Path:           "/sessions",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"sessions", "total"},
			Description:    "queries list returns session list with total",
		},
		{
			Name:                  "64a_queries_list_cluster_not_found",
			SubScenario:           "64a",
			Method:                "GET",
			Path:                  "/sessions",
			ExpectedStatus:        404,
			UseNonExistentCluster: true,
			Description:           "queries list for non-existent cluster returns 404",
		},

		// --- 64b: queries detail — GET /queries/{pid} ---
		{
			Name:           "64b_queries_detail_ok",
			SubScenario:    "64b",
			Method:         "GET",
			Path:           "/queries/1234",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "state", "query"},
			Description:    "queries detail returns query info with locks and tables",
		},
		{
			Name:           "64b_queries_detail_invalid_pid",
			SubScenario:    "64b",
			Method:         "GET",
			Path:           "/queries/abc",
			ExpectedStatus: 400,
			Description:    "queries detail with non-numeric PID returns 400",
		},

		// --- 64c: queries cancel — POST /queries/{pid}/cancel ---
		{
			Name:           "64c_queries_cancel_ok",
			SubScenario:    "64c",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "canceled", "status"},
			Description:    "queries cancel returns cancellation response",
		},
		{
			Name:           "64c_queries_cancel_with_reason",
			SubScenario:    "64c",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{"reason":"too slow"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "canceled", "reason"},
			Description:    "queries cancel with reason includes reason in response",
		},
		{
			Name:           "64c_queries_cancel_invalid_pid",
			SubScenario:    "64c",
			Method:         "POST",
			Path:           "/queries/abc/cancel",
			Body:           `{}`,
			ExpectedStatus: 400,
			Description:    "queries cancel with invalid PID returns 400",
		},

		// --- 64d: queries move — POST /queries/{pid}/move ---
		{
			Name:           "64d_queries_move_ok",
			SubScenario:    "64d",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{"targetGroup":"etl_group"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"pid", "targetGroup", "status"},
			Description:    "queries move returns move response with targetGroup",
		},
		{
			Name:           "64d_queries_move_missing_target",
			SubScenario:    "64d",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{}`,
			ExpectedStatus: 400,
			Description:    "queries move without targetGroup returns 400",
		},
		{
			Name:           "64d_queries_move_invalid_target",
			SubScenario:    "64d",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{"targetGroup":"DROP TABLE;--"}`,
			ExpectedStatus: 400,
			Description:    "queries move with SQL injection targetGroup returns 400",
		},

		// --- 64e: queries history --last 24h — GET /queries/history?since=24h ---
		{
			Name:           "64e_queries_history_last_24h",
			SubScenario:    "64e",
			Method:         "GET",
			Path:           "/queries/history",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"items", "total", "limit", "offset"},
			Description:    "queries history with since filter returns paginated results",
		},

		// --- 64f: queries history --user --database ---
		{
			Name:           "64f_queries_history_user_filter",
			SubScenario:    "64f",
			Method:         "GET",
			Path:           "/queries/history",
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"items", "total"},
			Description:    "queries history with user filter returns filtered results",
		},

		// --- 64g: queries plan-check — POST /queries/plan-check ---
		{
			Name:           "64g_plan_check_ok",
			SubScenario:    "64g",
			Method:         "POST",
			Path:           "/queries/plan-check",
			Body:           `{"planText":"Seq Scan on large_orders  (cost=0.00..5000.00 rows=100000 width=100) (actual time=0.020..120.000 rows=100000 loops=1)\n  Filter: (region = 'US')\n  Rows Removed by Filter: 400000\nPlanning Time: 0.200 ms\nExecution Time: 120.500 ms"}`,
			ExpectedStatus: 200,
			ExpectedKeys:   []string{"issues", "summary", "totalNodes"},
			Description:    "plan-check returns issues detected in plan",
		},
		{
			Name:           "64g_plan_check_empty",
			SubScenario:    "64g",
			Method:         "POST",
			Path:           "/queries/plan-check",
			Body:           `{"planText":""}`,
			ExpectedStatus: 400,
			Description:    "plan-check with empty plan returns 400",
		},

		// --- 64h: queries export — POST /queries/export ---
		{
			Name:           "64h_queries_export_csv",
			SubScenario:    "64h",
			Method:         "POST",
			Path:           "/queries/export",
			ExpectedStatus: 200,
			ContentType:    "text/csv",
			Description:    "queries export returns CSV content",
		},
		{
			Name:                  "64h_queries_export_cluster_not_found",
			SubScenario:           "64h",
			Method:                "POST",
			Path:                  "/queries/export",
			ExpectedStatus:        404,
			UseNonExistentCluster: true,
			Description:           "queries export for non-existent cluster returns 404",
		},

		// --- 64i: queries history export — POST /queries/history/export ---
		{
			Name:           "64i_history_export_csv",
			SubScenario:    "64i",
			Method:         "POST",
			Path:           "/queries/history/export",
			Body:           `{}`,
			ExpectedStatus: 200,
			ContentType:    "text/csv",
			Description:    "history export returns CSV content",
		},
		{
			Name:                  "64i_history_export_cluster_not_found",
			SubScenario:           "64i",
			Method:                "POST",
			Path:                  "/queries/history/export",
			Body:                  `{}`,
			ExpectedStatus:        404,
			UseNonExistentCluster: true,
			Description:           "history export for non-existent cluster returns 404",
		},
	}
}

// QueryMonitoringCase represents a test case for query monitoring configuration.
type QueryMonitoringCase struct {
	Name               string
	Enabled            bool
	HistoryRetention   string
	SamplingInterval   int32
	GuestAccess        bool
	PlanCollection     bool
	SlowQueryThreshold string
	HasExporters       bool
	Description        string
}

// QueryMonitoringCases returns the test cases for query monitoring configuration.
func QueryMonitoringCases() []QueryMonitoringCase {
	return []QueryMonitoringCase{
		{
			Name:               "full_config",
			Enabled:            true,
			HistoryRetention:   "30d",
			SamplingInterval:   5,
			GuestAccess:        false,
			PlanCollection:     true,
			SlowQueryThreshold: "1000ms",
			HasExporters:       true,
			Description:        "Full query monitoring configuration with all exporters",
		},
		{
			Name:               "minimal_config",
			Enabled:            true,
			HistoryRetention:   "7d",
			SamplingInterval:   10,
			GuestAccess:        false,
			PlanCollection:     false,
			SlowQueryThreshold: "500ms",
			HasExporters:       false,
			Description:        "Minimal query monitoring without exporters",
		},
		{
			Name:               "disabled",
			Enabled:            false,
			HistoryRetention:   "",
			SamplingInterval:   0,
			GuestAccess:        false,
			PlanCollection:     false,
			SlowQueryThreshold: "",
			HasExporters:       false,
			Description:        "Query monitoring disabled",
		},
		{
			Name:               "guest_access_enabled",
			Enabled:            true,
			HistoryRetention:   "30d",
			SamplingInterval:   5,
			GuestAccess:        true,
			PlanCollection:     true,
			SlowQueryThreshold: "2000ms",
			HasExporters:       true,
			Description:        "Query monitoring with guest access enabled",
		},
	}
}

// PauseResumeCase represents a test case for pause/resume monitor verification (Scenario 66).
type PauseResumeCase struct {
	Name           string
	SubScenario    string // "66a"
	Step           string // step description
	Method         string // HTTP method
	Path           string // relative path (without cluster prefix)
	AuthUser       string // username for authenticated requests (empty = unauthenticated)
	AuthPass       string // password for authenticated requests
	ExpectedStatus int    // expected HTTP status code
	ExpectStale    bool   // whether stale=true is expected in response
	ExpectPausedAt bool   // whether pausedAt is expected in response
	Permission     string // permission level of the user
	Description    string
}

// PauseResumeCases returns the test cases for pause/resume monitor (Scenario 66).
func PauseResumeCases() []PauseResumeCase {
	return []PauseResumeCase{
		// --- 66a: Pause and Resume lifecycle ---
		{
			Name:           "66a_initial_state_not_paused",
			SubScenario:    "66a",
			Step:           "initial_state",
			Method:         "GET",
			Path:           "/queries/monitor/state",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    false,
			ExpectPausedAt: false,
			Permission:     "Basic",
			Description:    "Initial monitor state should be paused=false, stale=false",
		},
		{
			Name:           "66a_pause_monitor",
			SubScenario:    "66a",
			Step:           "pause",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectPausedAt: true,
			Permission:     "Operator",
			Description:    "POST pause should return status=paused with pausedAt timestamp",
		},
		{
			Name:           "66a_verify_paused_state",
			SubScenario:    "66a",
			Step:           "verify_paused",
			Method:         "GET",
			Path:           "/queries/monitor/state",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    true,
			ExpectPausedAt: true,
			Permission:     "Basic",
			Description:    "After pause, state should show paused=true, stale=true, pausedAt set",
		},
		{
			Name:           "66a_stale_active_queries",
			SubScenario:    "66a",
			Step:           "stale_active",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    true,
			ExpectPausedAt: true,
			Permission:     "Basic",
			Description:    "GET /queries/active while paused returns stale=true with pausedAt",
		},
		{
			Name:           "66a_stale_queries",
			SubScenario:    "66a",
			Step:           "stale_queries",
			Method:         "GET",
			Path:           "/queries",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    true,
			ExpectPausedAt: true,
			Permission:     "OperatorBasic",
			Description:    "GET /queries while paused returns stale=true with pausedAt",
		},
		{
			Name:           "66a_resume_monitor",
			SubScenario:    "66a",
			Step:           "resume",
			Method:         "POST",
			Path:           "/queries/monitor/resume",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			Permission:     "Operator",
			Description:    "POST resume should return status=resumed",
		},
		{
			Name:           "66a_verify_resumed_state",
			SubScenario:    "66a",
			Step:           "verify_resumed",
			Method:         "GET",
			Path:           "/queries/monitor/state",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    false,
			ExpectPausedAt: false,
			Permission:     "Basic",
			Description:    "After resume, state should show paused=false, stale=false",
		},
		{
			Name:           "66a_fresh_active_queries",
			SubScenario:    "66a",
			Step:           "fresh_active",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectStale:    false,
			ExpectPausedAt: false,
			Permission:     "Basic",
			Description:    "GET /queries/active after resume returns fresh data (no stale flag)",
		},
		{
			Name:           "66a_idempotent_pause",
			SubScenario:    "66a",
			Step:           "idempotent_pause",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			Permission:     "Operator",
			Description:    "Pausing twice should succeed (idempotent)",
		},
		{
			Name:           "66a_resume_without_pause",
			SubScenario:    "66a",
			Step:           "resume_no_pause",
			Method:         "POST",
			Path:           "/queries/monitor/resume",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			Permission:     "Operator",
			Description:    "Resuming when not paused should succeed",
		},
		{
			Name:           "66a_unauthenticated_pause",
			SubScenario:    "66a",
			Step:           "unauth_pause",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			ExpectedStatus: 401,
			Description:    "POST pause without auth should return 401",
		},
		{
			Name:           "66a_basic_user_pause_forbidden",
			SubScenario:    "66a",
			Step:           "basic_pause",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			AuthUser:       "basic-user",
			AuthPass:       "basic-pass",
			ExpectedStatus: 403,
			Permission:     "Basic",
			Description:    "Basic user POST pause should return 403 (needs Operator)",
		},
		{
			Name:           "66a_cluster_not_found",
			SubScenario:    "66a",
			Step:           "not_found",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 404,
			Permission:     "Operator",
			Description:    "POST pause for non-existent cluster should return 404",
		},
	}
}

// AccessControlCase represents a test case for access control and guest access verification (Scenario 65).
type AccessControlCase struct {
	Name           string
	SubScenario    string // "65a", "65b", "65c"
	Method         string // HTTP method
	Path           string // relative path (without cluster prefix)
	Body           string // JSON request body (empty for GET)
	AuthUser       string // username for authenticated requests (empty = unauthenticated)
	AuthPass       string // password for authenticated requests
	ExpectedStatus int    // expected HTTP status code
	Permission     string // permission level of the user (for 65c)
	Description    string
}

// AccessControlCases returns the test cases for access control and guest access (Scenario 65).
func AccessControlCases() []AccessControlCase {
	return []AccessControlCase{
		// --- 65a: guestAccess=false (default) — all unauthenticated → 401 ---
		{
			Name:           "65a_unauthenticated_active_queries",
			SubScenario:    "65a",
			Method:         "GET",
			Path:           "/queries/active",
			ExpectedStatus: 401,
			Description:    "Unauthenticated GET /queries/active returns 401 when guestAccess=false",
		},
		{
			Name:           "65a_unauthenticated_exporter_health",
			SubScenario:    "65a",
			Method:         "GET",
			Path:           "/metrics/exporters",
			ExpectedStatus: 401,
			Description:    "Unauthenticated GET /metrics/exporters returns 401 when guestAccess=false",
		},
		{
			Name:           "65a_unauthenticated_cancel_query",
			SubScenario:    "65a",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			ExpectedStatus: 401,
			Description:    "Unauthenticated POST /queries/{pid}/cancel returns 401",
		},
		{
			Name:           "65a_unauthenticated_list_queries",
			SubScenario:    "65a",
			Method:         "GET",
			Path:           "/queries",
			ExpectedStatus: 401,
			Description:    "Unauthenticated GET /queries returns 401 when guestAccess=false",
		},

		// --- 65b: guestAccess=true — guest identity with PermissionBasic ---
		{
			Name:           "65b_guest_active_queries_200",
			SubScenario:    "65b",
			Method:         "GET",
			Path:           "/queries/active",
			ExpectedStatus: 200,
			Description:    "Guest GET /queries/active returns 200 (guest has Basic permission)",
		},
		{
			Name:           "65b_guest_exporter_health_200",
			SubScenario:    "65b",
			Method:         "GET",
			Path:           "/metrics/exporters",
			ExpectedStatus: 200,
			Description:    "Guest GET /metrics/exporters returns 200 (guest has Basic permission)",
		},
		{
			Name:           "65b_guest_queries_403",
			SubScenario:    "65b",
			Method:         "GET",
			Path:           "/queries",
			ExpectedStatus: 403,
			Description:    "Guest GET /queries returns 403 (guest has Basic, needs OperatorBasic)",
		},
		{
			Name:           "65b_guest_cancel_401",
			SubScenario:    "65b",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			ExpectedStatus: 401,
			Description:    "Guest POST /queries/{pid}/cancel returns 401 (write ops always require auth)",
		},
		{
			Name:           "65b_guest_plan_check_401",
			SubScenario:    "65b",
			Method:         "POST",
			Path:           "/queries/plan-check",
			ExpectedStatus: 401,
			Description:    "Guest POST /queries/plan-check returns 401 (POST always requires auth)",
		},
		{
			Name:           "65b_authenticated_active_queries_200",
			SubScenario:    "65b",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "admin",
			AuthPass:       "admin-pass",
			ExpectedStatus: 200,
			Description:    "Authenticated GET /queries/active returns 200 when guestAccess=true",
		},

		// --- 65c: Permission enforcement — different permission levels ---
		{
			Name:           "65c_basic_active_queries_200",
			SubScenario:    "65c",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "basic-user",
			AuthPass:       "basic-pass",
			ExpectedStatus: 200,
			Permission:     "Basic",
			Description:    "Basic user GET /queries/active returns 200",
		},
		{
			Name:           "65c_basic_queries_403",
			SubScenario:    "65c",
			Method:         "GET",
			Path:           "/queries",
			AuthUser:       "basic-user",
			AuthPass:       "basic-pass",
			ExpectedStatus: 403,
			Permission:     "Basic",
			Description:    "Basic user GET /queries returns 403 (requires OperatorBasic)",
		},
		{
			Name:           "65c_opbasic_queries_200",
			SubScenario:    "65c",
			Method:         "GET",
			Path:           "/queries",
			AuthUser:       "opbasic-user",
			AuthPass:       "opbasic-pass",
			ExpectedStatus: 200,
			Permission:     "OperatorBasic",
			Description:    "OperatorBasic user GET /queries returns 200",
		},
		{
			Name:           "65c_opbasic_active_queries_200",
			SubScenario:    "65c",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "opbasic-user",
			AuthPass:       "opbasic-pass",
			ExpectedStatus: 200,
			Permission:     "OperatorBasic",
			Description:    "OperatorBasic user GET /queries/active returns 200",
		},
		{
			Name:           "65c_opbasic_cancel_403",
			SubScenario:    "65c",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{}`,
			AuthUser:       "opbasic-user",
			AuthPass:       "opbasic-pass",
			ExpectedStatus: 403,
			Permission:     "OperatorBasic",
			Description:    "OperatorBasic user POST /queries/{pid}/cancel returns 403 (requires Operator)",
		},
		{
			Name:           "65c_operator_cancel_200",
			SubScenario:    "65c",
			Method:         "POST",
			Path:           "/queries/1234/cancel",
			Body:           `{}`,
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			Permission:     "Operator",
			Description:    "Operator user POST /queries/{pid}/cancel returns 200",
		},
		{
			Name:           "65c_operator_move_200",
			SubScenario:    "65c",
			Method:         "POST",
			Path:           "/queries/1234/move",
			Body:           `{"targetGroup":"etl_group"}`,
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			Permission:     "Operator",
			Description:    "Operator user POST /queries/{pid}/move returns 200",
		},
	}
}

// MonitoringDisabledCase represents a test case for monitoring disabled and planCollection disabled
// verification (Scenario 67).
type MonitoringDisabledCase struct {
	Name           string
	SubScenario    string // "67a" (monitoring disabled), "67b" (planCollection disabled)
	Step           string // step description
	Method         string // HTTP method
	Path           string // relative path (without cluster prefix)
	AuthUser       string // username for authenticated requests (empty = unauthenticated)
	AuthPass       string // password for authenticated requests
	ExpectedStatus int    // expected HTTP status code
	ExpectMonOff   bool   // whether monitoringEnabled=false is expected in response
	ExpectPlanArg  bool   // whether --plan-collection arg is expected in exporter args
	Permission     string // permission level of the user
	Description    string
}

// MonitoringDisabledCases returns the test cases for monitoring disabled and planCollection disabled
// (Scenario 67).
func MonitoringDisabledCases() []MonitoringDisabledCase {
	return []MonitoringDisabledCase{
		// --- 67a: queryMonitoring.enabled=false ---
		{
			Name:           "67a_queries_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "queries_disabled",
			Method:         "GET",
			Path:           "/queries",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "OperatorBasic",
			Description:    "GET /queries with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_active_queries_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "active_disabled",
			Method:         "GET",
			Path:           "/queries/active",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "Basic",
			Description:    "GET /queries/active with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_query_history_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "history_disabled",
			Method:         "GET",
			Path:           "/queries/history",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "OperatorBasic",
			Description:    "GET /queries/history with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_exporter_health_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "exporters_disabled",
			Method:         "GET",
			Path:           "/metrics/exporters",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "Basic",
			Description:    "GET /metrics/exporters with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_monitor_state_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "state_disabled",
			Method:         "GET",
			Path:           "/queries/monitor/state",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "Basic",
			Description:    "GET /queries/monitor/state with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_pause_returns_monitoring_disabled",
			SubScenario:    "67a",
			Step:           "pause_disabled",
			Method:         "POST",
			Path:           "/queries/monitor/pause",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   true,
			Permission:     "Operator",
			Description:    "POST /queries/monitor/pause with monitoring disabled returns monitoringEnabled=false",
		},
		{
			Name:           "67a_sessions_still_works",
			SubScenario:    "67a",
			Step:           "sessions_ok",
			Method:         "GET",
			Path:           "/sessions",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   false,
			Permission:     "OperatorBasic",
			Description:    "GET /sessions still works when monitoring is disabled",
		},
		{
			Name:           "67a_plan_check_still_works",
			SubScenario:    "67a",
			Step:           "plan_check_ok",
			Method:         "POST",
			Path:           "/queries/plan-check",
			AuthUser:       "operator-user",
			AuthPass:       "operator-pass",
			ExpectedStatus: 200,
			ExpectMonOff:   false,
			Permission:     "Basic",
			Description:    "POST /queries/plan-check still works when monitoring is disabled",
		},

		// --- 67b: planCollection=false ---
		{
			Name:          "67b_plan_collection_enabled_has_arg",
			SubScenario:   "67b",
			Step:          "plan_enabled",
			ExpectPlanArg: true,
			Description:   "With planCollection=true, exporter args include --plan-collection",
		},
		{
			Name:          "67b_plan_collection_disabled_no_arg",
			SubScenario:   "67b",
			Step:          "plan_disabled",
			ExpectPlanArg: false,
			Description:   "With planCollection=false, exporter args do NOT include --plan-collection",
		},
		{
			Name:        "67b_history_retention_arg_present",
			SubScenario: "67b",
			Step:        "retention_arg",
			Description: "With historyRetention set, exporter args include --history-retention",
		},
	}
}

// Scenario70DefaultsCase represents a single Scenario 70 webhook backup
// defaulting case: a field that the mutating webhook must default to a known
// value when a minimal backup spec (enabled, destination, image only) is
// applied and the field is left unset. The defaulter mutates the object the
// API server persists, so these expected values are observed on the persisted
// object.
type Scenario70DefaultsCase struct {
	// ID is the spec rule id (e.g. "70a").
	ID string
	// Name is a short test name.
	Name string
	// Field is the spec field path that is defaulted.
	Field string
	// Expected is the expected defaulted value as a string for catalog display.
	Expected string
	// Description explains the default.
	Description string
}

// Scenario70DefaultsCases returns the Scenario 70 backup webhook defaulting
// cases enumerating the 12 fields the mutating webhook defaults when a minimal
// backup spec is applied. Defaulting is gated on backup.enabled and is
// non-destructive: explicit user values are never overwritten.
func Scenario70DefaultsCases() []Scenario70DefaultsCase {
	return []Scenario70DefaultsCase{
		{
			ID: "70a", Name: "gpbackup_compression_level",
			Field:       "backup.gpbackup.compressionLevel",
			Expected:    "1",
			Description: "gpbackup.compressionLevel defaults to 1 when unset",
		},
		{
			ID: "70b", Name: "gpbackup_compression_type",
			Field:       "backup.gpbackup.compressionType",
			Expected:    "gzip",
			Description: "gpbackup.compressionType defaults to gzip when unset",
		},
		{
			ID: "70c", Name: "gpbackup_jobs",
			Field:       "backup.gpbackup.jobs",
			Expected:    "1",
			Description: "gpbackup.jobs defaults to 1 when unset",
		},
		{
			ID: "70d", Name: "gpbackup_single_data_file",
			Field:       "backup.gpbackup.singleDataFile",
			Expected:    "false",
			Description: "gpbackup.singleDataFile defaults to false (zero value)",
		},
		{
			ID: "70e", Name: "gpbackup_with_stats",
			Field:       "backup.gpbackup.withStats",
			Expected:    "true",
			Description: "gpbackup.withStats defaults to true when unset",
		},
		{
			ID: "70f", Name: "gprestore_jobs",
			Field:       "backup.gprestore.jobs",
			Expected:    "1",
			Description: "gprestore.jobs defaults to 1 when unset",
		},
		{
			ID: "70g", Name: "gprestore_with_stats",
			Field:       "backup.gprestore.withStats",
			Expected:    "true",
			Description: "gprestore.withStats defaults to true when unset",
		},
		{
			ID: "70h", Name: "retention_full_count",
			Field:       "backup.retention.fullCount",
			Expected:    "3",
			Description: "retention.fullCount defaults to 3 when unset",
		},
		{
			ID: "70i", Name: "retention_max_age",
			Field:       "backup.retention.maxAge",
			Expected:    "30d",
			Description: "retention.maxAge defaults to 30d when unset",
		},
		{
			ID: "70j", Name: "job_template_backoff_limit",
			Field:       "backup.jobTemplate.backoffLimit",
			Expected:    "2",
			Description: "jobTemplate.backoffLimit defaults to 2 when unset",
		},
		{
			ID: "70k", Name: "job_template_active_deadline_seconds",
			Field:       "backup.jobTemplate.activeDeadlineSeconds",
			Expected:    "7200",
			Description: "jobTemplate.activeDeadlineSeconds defaults to 7200 (2h) when unset",
		},
		{
			ID: "70l", Name: "job_template_ttl_seconds_after_finished",
			Field:       "backup.jobTemplate.ttlSecondsAfterFinished",
			Expected:    "86400",
			Description: "jobTemplate.ttlSecondsAfterFinished defaults to 86400 (24h) when unset",
		},
	}
}
