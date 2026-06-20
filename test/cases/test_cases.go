// Package cases defines test case data structures and test case catalogs for the cloudberry-k8s project.
package cases

import (
	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
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

// Scenario89ValidationCase represents a single Scenario 89 PXF / data-loading
// webhook validation negative test case: an otherwise-valid dataLoading spec
// with exactly one offending field, expected to be rejected with a descriptive
// error mentioning the field path.
type Scenario89ValidationCase struct {
	// ID is the spec rule id (e.g. "W.1").
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

// Scenario89ValidationCases returns the Scenario 89 PXF / data-loading webhook
// validation negative cases W.1-W.15. Each row introduces exactly one offending
// field into an otherwise-valid dataLoading spec and asserts the rejection error
// contains the listed substrings. Some rules have two variants (e.g. W.2
// empty/duplicate name, W.4 missing endpoint/credentials, W.5 missing
// driver/url, W.7 empty/duplicate job name); those variants are exercised
// directly in the functional suite and are represented here by the primary
// case substring set.
func Scenario89ValidationCases() []Scenario89ValidationCase {
	return []Scenario89ValidationCase{
		{
			ID: "W.1", Name: "missing_pxf_image",
			OffendingField:  "dataLoading.pxf.image",
			ErrorSubstrings: []string{"dataLoading.pxf.image"},
			Description:     "pxf.enabled with empty pxf.image must be rejected",
		},
		{
			ID: "W.2", Name: "duplicate_server_name",
			OffendingField:  "dataLoading.pxf.servers[].name",
			ErrorSubstrings: []string{"dataLoading.pxf.servers", "name", "duplicate"},
			Description:     "duplicate pxf server name must be rejected",
		},
		{
			ID: "W.3", Name: "invalid_server_type",
			OffendingField:  "dataLoading.pxf.servers[].type",
			ErrorSubstrings: []string{"dataLoading.pxf.servers", "type"},
			Description:     "pxf server type 'ftp' must be rejected (s3;hdfs;jdbc;hbase;hive)",
		},
		{
			ID: "W.4", Name: "s3_server_missing_endpoint",
			OffendingField:  "dataLoading.pxf.servers[].config[fs.s3a.endpoint]",
			ErrorSubstrings: []string{"fs.s3a.endpoint"},
			Description:     "s3 server missing fs.s3a.endpoint must be rejected",
		},
		{
			ID: "W.5", Name: "jdbc_server_missing_driver",
			OffendingField:  "dataLoading.pxf.servers[].config[jdbc.driver]",
			ErrorSubstrings: []string{"jdbc.driver"},
			Description:     "jdbc server missing jdbc.driver must be rejected",
		},
		{
			ID: "W.6", Name: "hdfs_server_missing_default_fs",
			OffendingField:  "dataLoading.pxf.servers[].config[fs.defaultFS]",
			ErrorSubstrings: []string{"fs.defaultFS"},
			Description:     "hdfs server missing fs.defaultFS must be rejected",
		},
		{
			ID: "W.7", Name: "duplicate_job_name",
			OffendingField:  "dataLoading.jobs[].name",
			ErrorSubstrings: []string{"dataLoading.jobs", "name", "duplicate"},
			Description:     "duplicate job name must be rejected",
		},
		{
			ID: "W.8", Name: "invalid_job_type",
			OffendingField:  "dataLoading.jobs[].type",
			ErrorSubstrings: []string{"dataLoading.jobs", "type"},
			Description:     "job type 'spark' must be rejected (pxf;gpload)",
		},
		{
			ID: "W.9", Name: "pxf_job_undefined_server",
			OffendingField:  "dataLoading.jobs[].pxfJob.server",
			ErrorSubstrings: []string{"pxfJob.server"},
			Description:     "pxfJob.server referencing an undefined server must be rejected",
		},
		{
			ID: "W.10", Name: "pxf_job_invalid_profile",
			OffendingField:  "dataLoading.jobs[].pxfJob.profile",
			ErrorSubstrings: []string{"pxfJob.profile"},
			Description:     "pxfJob.profile 's3:nonsense' must be rejected",
		},
		{
			ID: "W.11", Name: "pxf_job_missing_target_table",
			OffendingField:  "dataLoading.jobs[].pxfJob.targetTable",
			ErrorSubstrings: []string{"pxfJob.targetTable"},
			Description:     "pxf job with no targetTable must be rejected",
		},
		{
			ID: "W.12", Name: "gpload_job_missing_target_table",
			OffendingField:  "dataLoading.jobs[].gploadJob.targetTable",
			ErrorSubstrings: []string{"gploadJob.targetTable"},
			Description:     "gpload job with no targetTable must be rejected",
		},
		{
			ID: "W.13", Name: "invalid_cron_schedule",
			OffendingField:  "dataLoading.jobs[].schedule",
			ErrorSubstrings: []string{"schedule"},
			Description:     "a non-cron job schedule must be rejected",
		},
		{
			ID: "W.14", Name: "partitioning_column_without_range_interval",
			OffendingField:  "dataLoading.jobs[].pxfJob.partitioning",
			ErrorSubstrings: []string{"partitioning"},
			Description:     "partitioning column without range/interval must be rejected",
		},
		{
			ID: "W.15", Name: "invalid_segment_reject_limit_type",
			OffendingField:  "dataLoading.jobs[].pxfJob.errorHandling.segmentRejectLimitType",
			ErrorSubstrings: []string{"segmentRejectLimitType"},
			Description:     "segmentRejectLimitType 'fraction' must be rejected (rows;percent)",
		},
	}
}

// Scenario90DefaultsCase represents a single Scenario 90 PXF / data-loading
// webhook defaulting test case: a field that the mutating webhook defaults to a
// known value when left unset on an otherwise-valid, enabled dataLoading spec.
// Pointer fields (*bool / *int32 / *int64) default only when nil so explicit
// user values (including explicit false) are preserved.
type Scenario90DefaultsCase struct {
	// ID is the defaults rule id (e.g. "D.1").
	ID string
	// FieldPath is the dotted spec path of the defaulted field.
	FieldPath string
	// ExpectedValue is the value the webhook applies, rendered as a string.
	ExpectedValue string
	// Description explains the default.
	Description string
}

// Scenario90DefaultsCases returns the Scenario 90 PXF / data-loading webhook
// defaulting cases D.1-D.14. Each row names a field that the mutating webhook
// defaults to ExpectedValue when the field is unset and dataLoading is enabled.
// The values mirror the default consts in internal/webhook/mutating.go exactly.
// The four *bool fields (D.4/D.5/D.9/D.10) default to true only when nil so an
// explicit false set by the user is preserved.
func Scenario90DefaultsCases() []Scenario90DefaultsCase {
	return []Scenario90DefaultsCase{
		{
			ID: "D.1", FieldPath: "dataLoading.pxf.port",
			ExpectedValue: "5888",
			Description:   "pxf.port defaults to 5888 when unset",
		},
		{
			ID: "D.2", FieldPath: "dataLoading.pxf.jvmOpts",
			ExpectedValue: "-Xmx1g -Xms256m",
			Description:   "pxf.jvmOpts defaults to '-Xmx1g -Xms256m' when unset",
		},
		{
			ID: "D.3", FieldPath: "dataLoading.pxf.logLevel",
			ExpectedValue: "INFO",
			Description:   "pxf.logLevel defaults to INFO when unset",
		},
		{
			ID: "D.4", FieldPath: "dataLoading.pxf.extensions.pxf",
			ExpectedValue: "true",
			Description:   "pxf.extensions.pxf (*bool) defaults to true only when nil",
		},
		{
			ID: "D.5", FieldPath: "dataLoading.pxf.extensions.pxfFdw",
			ExpectedValue: "true",
			Description:   "pxf.extensions.pxfFdw (*bool) defaults to true only when nil",
		},
		{
			ID: "D.6", FieldPath: "dataLoading.gpfdist.replicas",
			ExpectedValue: "1",
			Description:   "gpfdist.replicas (*int32) defaults to 1 only when nil",
		},
		{
			ID: "D.7", FieldPath: "dataLoading.gpfdist.port",
			ExpectedValue: "8080",
			Description:   "gpfdist.port defaults to 8080 when unset",
		},
		{
			ID: "D.8", FieldPath: "dataLoading.jobs[].pxfJob.mode",
			ExpectedValue: "insert",
			Description:   "pxfJob.mode defaults to insert when unset",
		},
		{
			ID: "D.9", FieldPath: "dataLoading.jobs[].pxfJob.filterPushdown",
			ExpectedValue: "true",
			Description:   "pxfJob.filterPushdown (*bool) defaults to true only when nil",
		},
		{
			ID: "D.10", FieldPath: "dataLoading.jobs[].pxfJob.columnProjection",
			ExpectedValue: "true",
			Description:   "pxfJob.columnProjection (*bool) defaults to true only when nil",
		},
		{
			ID: "D.11", FieldPath: "dataLoading.jobs[].gploadJob.mode",
			ExpectedValue: "insert",
			Description:   "gploadJob.mode defaults to insert when unset",
		},
		{
			ID: "D.12", FieldPath: "dataLoading.jobTemplate.backoffLimit",
			ExpectedValue: "3",
			Description:   "jobTemplate.backoffLimit (*int32) defaults to 3 only when nil",
		},
		{
			ID: "D.13", FieldPath: "dataLoading.jobTemplate.activeDeadlineSeconds",
			ExpectedValue: "14400",
			Description:   "jobTemplate.activeDeadlineSeconds (*int64) defaults to 14400 only when nil",
		},
		{
			ID: "D.14", FieldPath: "dataLoading.jobTemplate.ttlSecondsAfterFinished",
			ExpectedValue: "86400",
			Description:   "jobTemplate.ttlSecondsAfterFinished (*int32) defaults to 86400 only when nil",
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

// Scenario84MetricCase represents a single Scenario 84 (Prometheus Metrics /
// gpbackup_exporter) verification case: one of the 9 backup-lifecycle metrics,
// the recorder method that emits it, the Job-shape trigger that drives it, and
// the VictoriaMetrics query the live script asserts. Scenario 84 is a
// VERIFICATION scenario — all 9 metrics are already wired; these cases enumerate
// the proof that each one fires across a full lifecycle and lands in
// VictoriaMetrics.
type Scenario84MetricCase struct {
	// ID is the metric id (M1..M9).
	ID string
	// Metric is the Prometheus metric family name (namespace "cloudberry").
	Metric string
	// Recorder is the metrics.Recorder method that emits the metric.
	Recorder string
	// Trigger is the Job-shape / status that drives the recorder.
	Trigger string
	// VMQuery is the VictoriaMetrics query the live script asserts.
	VMQuery string
	// Expected is the human-readable assertion (e.g. ">= 1", "== 0").
	Expected string
	// Description explains the verification.
	Description string
}

// Scenario84MetricCases returns the Scenario 84 metric verification cases — the
// 9 backup-lifecycle metrics mapped to their recorder, trigger and the
// VictoriaMetrics query asserted by the live script (test/e2e/scripts/
// scenario84-metrics.sh) against the scenario84-s3 cluster. The outcome label is
// `result` (success|failed), NOT `status` (GAP-A): the Scenario-84 wording
// "status" maps to the implemented `result` label; do NOT rename it.
func Scenario84MetricCases() []Scenario84MetricCase {
	return []Scenario84MetricCase{
		{
			ID:       "M1",
			Metric:   "cloudberry_backup_total",
			Recorder: "RecordBackup",
			Trigger:  "latest Succeeded backup Job (avsoft.io/backup-type=full|incremental)",
			VMQuery:  `cloudberry_backup_total{type="full|incremental",result="success"}`,
			Expected: ">= 1 per type",
			Description: "a Succeeded full + incremental backup each increment " +
				"backup_total{type,result=success} on their own series",
		},
		{
			ID:       "M2",
			Metric:   "cloudberry_backup_duration_seconds",
			Recorder: "ObserveBackupDuration",
			Trigger:  "Succeeded backup Job with startTime + completionTime, per type",
			VMQuery:  `cloudberry_backup_duration_seconds_count{type="full|incremental"}`,
			Expected: ">= 1 per type",
			Description: "the per-type duration histogram is populated for the latest " +
				"Succeeded full + incremental backup",
		},
		{
			ID:       "M3",
			Metric:   "cloudberry_backup_size_bytes",
			Recorder: "SetBackupSizeBytes",
			Trigger:  "Succeeded backup Job + avsoft.io/backup-size-bytes annotation + 14-digit ts",
			VMQuery:  `cloudberry_backup_size_bytes{timestamp=...}`,
			Expected: ">= 1 series, value > 0",
			Description: "a Succeeded backup carrying the size annotation sets the " +
				"per-timestamp size gauge (no annotation => not set)",
		},
		{
			ID:       "M4",
			Metric:   "cloudberry_backup_last_success_timestamp",
			Recorder: "SetBackupLastSuccessTimestamp",
			Trigger:  "latest Succeeded backup Job with completionTime",
			VMQuery:  `time() - cloudberry_backup_last_success_timestamp`,
			Expected: "< 600 (recent)",
			Description: "the last successful backup's completionTime.Unix() is recorded " +
				"and is ~ now after a real backup",
		},
		{
			ID:       "M5",
			Metric:   "cloudberry_backup_last_status",
			Recorder: "SetBackupLastStatus",
			Trigger:  "latest backup Job status (Success=0, Failed=1, InProgress=2)",
			VMQuery:  `cloudberry_backup_last_status`,
			Expected: "0 after success; 1 after forced failure; 2 best-effort in-progress",
			Description: "reflects the LATEST backup Job: 0 on a Succeeded backup, 1 on a " +
				"forced-failure backup, 2 (best-effort) while a backup is running",
		},
		{
			ID:       "M6",
			Metric:   "cloudberry_restore_total",
			Recorder: "RecordRestore",
			Trigger:  "latest restore Job (avsoft.io/backup-operation=restore) Succeeded",
			VMQuery:  `cloudberry_restore_total{result="success"}`,
			Expected: ">= 1",
			Description: "a Succeeded restore Job increments restore_total{result=success} " +
				"via the latest-Job path",
		},
		{
			ID:       "M7",
			Metric:   "cloudberry_restore_duration_seconds",
			Recorder: "ObserveRestoreDuration",
			Trigger:  "Succeeded restore Job with startTime + completionTime (per-Job loop)",
			VMQuery:  `cloudberry_restore_duration_seconds_count`,
			Expected: ">= 1",
			Description: "the restore duration histogram is observed for a Succeeded restore " +
				"Job carrying both timestamps (no completionTime => not observed)",
		},
		{
			ID:       "M8",
			Metric:   "cloudberry_backup_retention_deleted_total",
			Recorder: "RecordBackupRetentionDeleted",
			Trigger:  "Succeeded cleanup Job + avsoft.io/backup-retention-deleted=N (N>0)",
			VMQuery:  `cloudberry_backup_retention_deleted_total`,
			Expected: ">= 1",
			Description: "a Succeeded retention cleanup Job carrying the retention-deleted " +
				"annotation increments the counter by N",
		},
		{
			ID:       "M9",
			Metric:   "cloudberry_backup_job_status",
			Recorder: "SetBackupJobStatus",
			Trigger:  "every observed backup/restore/cleanup Job (0=pending,1=running,2=succeeded,3=failed)",
			VMQuery:  `cloudberry_backup_job_status{operation="backup|restore|cleanup"}`,
			Expected: "2 for healthy ops; 3 for the bad backup Job",
			Description: "the per-Job gauge reflects the lifecycle: a healthy backup/restore/" +
				"cleanup reaches 2 (succeeded); a forced-failure backup reaches 3 (failed)",
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
			Expected:    "false",
			Description: "gprestore.withStats defaults to false when unset (statistics restore is opt-in: gprestore exits 2 on the upstream statistics.sql bug)",
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

// Scenario91Case represents a single Scenario 91 "Enable Data Loading with Full
// PXF CRD Configuration" parsed-field expectation. Each row anchors one
// dataLoading/pxf field (or rendered artifact) the operator must ACT ON: the
// sidecar env it produces, the resources it converts, the per-server *-site.xml
// values it renders into the ConfigMap, and the custom-connector listing. It
// mirrors the Scenario89/Scenario90 catalog structure (ID, FieldPath,
// ExpectedValue, Description) so the functional/e2e suites can iterate it and
// assert against the live built objects (CatalogHonest).
type Scenario91Case struct {
	// ID is the catalog rule id (e.g. "C.1").
	ID string
	// FieldPath is the dotted spec path (or rendered-artifact path, e.g.
	// "sidecar.env.PXF_LOG_LEVEL" / "configMap.<server>__<file>.<prop>") of the
	// field under test.
	FieldPath string
	// ExpectedValue is the value the operator parses/renders, as a string.
	ExpectedValue string
	// Description explains what the operator must do with the field.
	Description string
}

// Scenario91Cases returns the Scenario 91 parsed-field catalog (C.1-C.N). It
// enumerates every dataLoading/pxf field the operator must ACT ON for the full
// PXF configuration: the scalar pxf fields (enabled, image, jvmOpts, port,
// logLevel, extensions), the converted resources requests/limits, the 5 external
// servers (2 s3 incl. minio-warehouse, 1 hdfs with hive+hbase, 2 jdbc incl. the
// MySQL driver via config["jdbc.driver"]), and the customConnectors jarUrls.
//
// C.6 (pxf.logLevel) is the headline propagation field: the default is INFO and
// the value (DEBUG/WARN/ERROR) must propagate to the PXF_LOG_LEVEL sidecar env on
// every rebuild. The catalog records the default (INFO); the functional/e2e
// suites loop the non-default levels and assert PXF_LOG_LEVEL each time.
//
// The expected values mirror the FULL dataLoading spec produced by the
// scenario91FullDataLoading() helper in the functional/e2e suites exactly; the
// CatalogHonest test resolves each FieldPath against the LIVE built sidecar +
// ConfigMap so the catalog stays honest against the implementation.
func Scenario91Cases() []Scenario91Case {
	return []Scenario91Case{
		// --- top-level data loading + pxf scalars ---
		{
			ID: "C.1", FieldPath: "dataLoading.enabled",
			ExpectedValue: "true",
			Description:   "data loading is enabled (gates the PXF sidecar injection)",
		},
		{
			ID: "C.2", FieldPath: "dataLoading.pxf.enabled",
			ExpectedValue: "true",
			Description:   "pxf is enabled (second gate; with image set => sidecar injected)",
		},
		{
			ID: "C.3", FieldPath: "dataLoading.pxf.image",
			ExpectedValue: "cloudberry-pxf:7.1.0",
			Description:   "pxf.image becomes the sidecar container image",
		},
		{
			ID: "C.4", FieldPath: "sidecar.env.PXF_JVM_OPTS",
			ExpectedValue: "-Xmx2g -Xms512m",
			Description:   "pxf.jvmOpts propagates to the PXF_JVM_OPTS sidecar env",
		},
		{
			ID: "C.5", FieldPath: "sidecar.env.PXF_PORT",
			ExpectedValue: "5888",
			Description:   "pxf.port propagates to PXF_PORT (and the container/probe port)",
		},
		{
			ID: "C.6", FieldPath: "sidecar.env.PXF_LOG_LEVEL",
			ExpectedValue: "INFO",
			Description: "pxf.logLevel propagates to PXF_LOG_LEVEL; default INFO, and " +
				"DEBUG/WARN/ERROR must propagate on every rebuild (re-patch)",
		},
		{
			ID: "C.7", FieldPath: "sidecar.env.PXF_EXTENSION_PXF",
			ExpectedValue: "true",
			Description:   "extensions.pxf (*bool, explicit true) propagates to PXF_EXTENSION_PXF",
		},
		{
			ID: "C.8", FieldPath: "sidecar.env.PXF_EXTENSION_PXF_FDW",
			ExpectedValue: "false",
			Description:   "extensions.pxfFdw (*bool, explicit false) propagates to PXF_EXTENSION_PXF_FDW",
		},
		// --- converted resources (requests + limits) ---
		{
			ID: "C.9", FieldPath: "sidecar.resources.requests.cpu",
			ExpectedValue: "500m",
			Description:   "pxf.resources.requests.cpu is converted onto the sidecar container",
		},
		{
			ID: "C.10", FieldPath: "sidecar.resources.requests.memory",
			ExpectedValue: "512Mi",
			Description:   "pxf.resources.requests.memory is converted onto the sidecar container",
		},
		{
			ID: "C.11", FieldPath: "sidecar.resources.limits.cpu",
			ExpectedValue: "2",
			Description:   "pxf.resources.limits.cpu is converted onto the sidecar container",
		},
		{
			ID: "C.12", FieldPath: "sidecar.resources.limits.memory",
			ExpectedValue: "2Gi",
			Description:   "pxf.resources.limits.memory is converted onto the sidecar container",
		},
		// --- server 1: s3-datalake (s3) ---
		{
			ID: "C.13", FieldPath: "configMap.s3-datalake__s3-site.xml.fs.s3a.endpoint",
			ExpectedValue: "https://s3.amazonaws.com",
			Description:   "s3 server 's3-datalake' renders fs.s3a.endpoint into s3-site.xml",
		},
		// --- server 2: minio-warehouse (s3) ---
		{
			ID: "C.14", FieldPath: "configMap.minio-warehouse__s3-site.xml.fs.s3a.endpoint",
			ExpectedValue: "http://minio:9000",
			Description:   "s3 server 'minio-warehouse' renders fs.s3a.endpoint http://minio:9000",
		},
		// --- server 3: hadoop-cluster (hdfs + hive + hbase) ---
		{
			ID: "C.15", FieldPath: "configMap.hadoop-cluster__core-site.xml.fs.defaultFS",
			ExpectedValue: "hdfs://namenode:8020",
			Description:   "hdfs server 'hadoop-cluster' renders fs.defaultFS into core-site.xml",
		},
		{
			ID: "C.16", FieldPath: "configMap.hadoop-cluster__hive-site.xml.hive.metastore.uris",
			ExpectedValue: "thrift://hive-metastore:9083",
			Description:   "hdfs server with Hive renders hive.metastore.uris into hive-site.xml",
		},
		{
			ID: "C.17", FieldPath: "configMap.hadoop-cluster__hbase-site.xml.hbase.zookeeper.quorum",
			ExpectedValue: "zk1,zk2,zk3",
			Description:   "hdfs server with Hbase renders hbase.zookeeper.quorum into hbase-site.xml",
		},
		// --- server 4: mysql-oltp (jdbc, MySQL driver via config) ---
		{
			ID: "C.18", FieldPath: "configMap.mysql-oltp__jdbc-site.xml.jdbc.driver",
			ExpectedValue: "com.mysql.cj.jdbc.Driver",
			Description:   "jdbc server 'mysql-oltp' renders jdbc.driver=com.mysql.cj.jdbc.Driver",
		},
		{
			ID: "C.19", FieldPath: "configMap.mysql-oltp__jdbc-site.xml.jdbc.url",
			ExpectedValue: "jdbc:mysql://mysql-oltp:3306/sales",
			Description:   "jdbc server 'mysql-oltp' renders jdbc.url into jdbc-site.xml",
		},
		// --- server 5: postgres-source (jdbc, Postgres driver) ---
		{
			ID: "C.20", FieldPath: "configMap.postgres-source__jdbc-site.xml.jdbc.driver",
			ExpectedValue: "org.postgresql.Driver",
			Description:   "jdbc server 'postgres-source' renders jdbc.driver=org.postgresql.Driver",
		},
		// --- custom connectors (jarUrl listing) ---
		{
			ID: "C.21", FieldPath: "configMap.connectors.properties.mysql-connector",
			ExpectedValue: "https://repo.example.com/mysql-connector-j-8.0.33.jar",
			Description:   "customConnectors[mysql-connector].jarUrl appears in connectors.properties",
		},
		{
			ID: "C.22", FieldPath: "configMap.connectors.properties.postgresql-connector",
			ExpectedValue: "https://repo.example.com/postgresql-42.6.0.jar",
			Description:   "customConnectors[postgresql-connector].jarUrl appears in connectors.properties",
		},
	}
}

// Scenario92DataLoadCase represents a single Scenario 92 "Data-Loading
// INGESTION RUNTIME" expectation: the operator-generated external-table DDL plus
// the load Job the operator launches for one declarative dataLoading job. Each
// row anchors one job variant (a pxf:// job or an engine-native gpfdist/file/s3
// job) and records the EXACT byte-stable DDL the builder produces along with the
// Job naming/env/marker expectations the e2e suite asserts against the live
// built objects. It mirrors the Scenario89/Scenario90/Scenario91 catalog shape
// so the e2e suite can iterate it to stay honest against the implementation.
//
// HONESTY NOTE: the pxf:// variant captures the GENERATED DDL + Job spec only —
// it is IMAGE-BLOCKED at runtime (no cloudberry-pxf image, no PXF agent/
// extension), so no successful pxf:// row count is ever asserted. The genuine,
// row-count-verified load path is the NATIVE protocols (gpfdist/file/s3).
type Scenario92DataLoadCase struct {
	// ID is the catalog rule id (e.g. "DL.1").
	ID string
	// Name is a short test name.
	Name string
	// JobType is the declarative job type: "pxf" or "gpload".
	JobType string
	// Job is the declarative dataLoading job the operator acts on.
	Job cbv1alpha1.DataLoadingJob
	// ExpectedDDL is the EXACT byte-stable CREATE EXTERNAL TABLE statement the
	// builder generates for this job (the source of truth for the live load
	// shape). Empty means the DDL is not asserted for this row.
	ExpectedDDL string
	// ExpectedJobName is the deterministic Job/CronJob name
	// (<cluster>-dataload-<job>) the operator creates for ClusterName below.
	ExpectedJobName string
	// ClusterName is the cluster the ExpectedJobName/env are rendered against.
	ClusterName string
	// ExpectedImage is the data-loader container image (the cluster runtime image
	// — cloudberry-official — which ships psql + native protocols, no PXF).
	ExpectedImage string
	// ExpectsPXFExtension indicates the generated load script attempts the
	// best-effort `CREATE EXTENSION IF NOT EXISTS pxf_fdw` step (pxf jobs only).
	ExpectsPXFExtension bool
	// ImageBlocked marks the pxf:// runtime path as image-blocked: the Job + SQL
	// are generated correctly but a successful row count must NOT be asserted.
	ImageBlocked bool
	// Description explains the variant.
	Description string
}

// Scenario92DataLoadCases returns the Scenario 92 data-loading runtime catalog
// (DL.1-DL.4). It enumerates the operator-generated external-table DDL + load
// Job for representative pxf:// and engine-native (gpfdist/file/s3) jobs. The
// ExpectedDDL strings are byte-exact and mirror the builder's deterministic
// output (internal/builder/dataload_builder.go); the e2e suite resolves each
// case against the live BuildDataLoadJob/BuildDataLoadCronJob + buildExternalTableDDL
// output to stay honest against the implementation.
//
// The DDL/Job for ClusterName "test-cluster" matches the builder defaults
// (gpfdist service "<cluster>-gpfdist:8080", temp table "cbk_dataload_ext_<job>",
// FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') for pxf, FORMAT 'CSV'/'TEXT'
// for native). DL.1 (pxf) is image-blocked; DL.2-DL.4 (native) are the genuine
// row-count-verified path.
func Scenario92DataLoadCases() []Scenario92DataLoadCase {
	const cluster = "test-cluster"
	return []Scenario92DataLoadCase{
		{
			ID: "DL.1", Name: "pxf_s3_parquet_load",
			JobType: "pxf",
			Job: cbv1alpha1.DataLoadingJob{
				Name:    "s3-parquet-loader",
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
				},
			},
			ExpectedDDL: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3_parquet_loader\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
			ExpectedJobName:     "test-cluster-dataload-s3-parquet-loader",
			ClusterName:         cluster,
			ExpectedImage:       "cloudberrydb/cloudberry:2.1.0",
			ExpectsPXFExtension: true,
			ImageBlocked:        true,
			Description: "pxf s3:parquet job: operator generates the pxf:// external-table " +
				"DDL + load Job; runtime is IMAGE-BLOCKED (no pxf agent), so no rows asserted",
		},
		{
			ID: "DL.2", Name: "native_gpfdist_csv_load",
			JobType: "gpload",
			Job: cbv1alpha1.DataLoadingJob{
				Name:    "csv-bulk-load",
				Type:    "gpload",
				Enabled: true,
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.bulk_data",
					Format:      "csv",
					FilePaths:   []string{"/data/incoming/*.csv"},
				},
			},
			ExpectedDDL: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_csv_bulk_load\" (LIKE \"public\".\"bulk_data\")\n" +
				"LOCATION ('gpfdist://test-cluster-gpfdist:8080/data/incoming/*.csv')\n" +
				"FORMAT 'CSV';",
			ExpectedJobName: "test-cluster-dataload-csv-bulk-load",
			ClusterName:     cluster,
			ExpectedImage:   "cloudberrydb/cloudberry:2.1.0",
			Description: "native gpfdist CSV job: operator generates the gpfdist:// external-table " +
				"DDL (bare path served via the cluster gpfdist Service) — the genuine non-PXF path",
		},
		{
			// DL.3 previously used a file:///path filePath. That bare file://
			// scheme is NOT a valid CR input for a multi-segment gpload job: a
			// file:// external table needs a per-segment-host URI
			// ("file://<seghost>/path") the operator cannot synthesize, so the
			// webhook (rule W.16) now rejects it. DL.3 is re-pointed at a single
			// bare path served via the cluster gpfdist Service — a SUPPORTED
			// native source — exercising the bare-path → gpfdist:// conversion for
			// a single file (DL.2 covers the glob form).
			ID: "DL.3", Name: "native_bare_path_single_file_csv_load",
			JobType: "gpload",
			Job: cbv1alpha1.DataLoadingJob{
				Name:    "file-loader",
				Type:    "gpload",
				Enabled: true,
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.dataset",
					Format:      "csv",
					FilePaths:   []string{"/data-lake/dataset.csv"},
				},
			},
			ExpectedDDL: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_file_loader\" (LIKE \"public\".\"dataset\")\n" +
				"LOCATION ('gpfdist://test-cluster-gpfdist:8080/data-lake/dataset.csv')\n" +
				"FORMAT 'CSV';",
			ExpectedJobName: "test-cluster-dataload-file-loader",
			ClusterName:     cluster,
			ExpectedImage:   "cloudberrydb/cloudberry:2.1.0",
			Description: "native bare-path single-file CSV job: operator generates the gpfdist:// " +
				"external-table DDL (single file served via the cluster gpfdist Service) — the " +
				"genuine non-PXF path (file:// is admission-rejected for multi-segment, see W.16)",
		},
		{
			ID: "DL.4", Name: "native_s3_csv_scheduled_load",
			JobType: "gpload",
			Job: cbv1alpha1.DataLoadingJob{
				Name:     "s3-csv-load",
				Type:     "gpload",
				Enabled:  true,
				Schedule: "0 2 * * *",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.s3_data",
					Format:      "csv",
					FilePaths:   []string{"s3://cloudberry-data/dataset/ config=/cfg/s3.conf"},
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     50,
						SegmentRejectLimitType: "rows",
						LogErrors:              util.Ptr(true),
					},
				},
			},
			ExpectedDDL: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3_csv_load\" (LIKE \"public\".\"s3_data\")\n" +
				"LOCATION ('s3://cloudberry-data/dataset/ config=/cfg/s3.conf')\n" +
				"FORMAT 'CSV'\n" +
				"LOG ERRORS SEGMENT REJECT LIMIT 50 ROWS;",
			ExpectedJobName: "test-cluster-dataload-s3-csv-load",
			ClusterName:     cluster,
			ExpectedImage:   "cloudberrydb/cloudberry:2.1.0",
			Description: "native s3:// scheduled CSV job with LOG ERRORS: operator generates a " +
				"CronJob carrying the s3:// external-table DDL + reject-limit suffix — non-PXF path",
		},
	}
}

// Scenario93Case represents a single Scenario 93 "Server ConfigMap, File
// Mapping, Extensions, Sync" expectation. Each row anchors one verifiable fact
// about the <cluster>-pxf-servers ConfigMap the operator renders, the
// per-server-type file-mapping the builder produces, the credential init
// container that resolves placeholders at startup, the PXF extension/grant DB
// setup, or the shared-ConfigMap sync invariant. It mirrors the
// Scenario89/Scenario90/Scenario91/Scenario94 catalog shape so the
// functional/e2e suites can iterate it and assert against the LIVE built objects
// (CatalogHonest).
//
// Two row families:
//
//   - SL.1–SL.6 (Server-side file mapping): each names a server (Server) of a
//     given Type and the EXACT set of ConfigMap data keys the builder emits for
//     it (ExpectedKeys, each "<server>__<file>.xml"), plus the substring facts
//     (KeyContains: data-key → required substring) the rendered XML must carry
//     (e.g. fs.defaultFS in core-site, a ${PLACEHOLDER} token, etc). SL.6 also
//     records the literal secret values that must NEVER appear in any XML body
//     (ForbiddenSubstrings).
//
//   - RP.8–RP.12 (Runtime/DB requirements): each names an assertion Target the
//     suite verifies — the credential init container (RP.8), the CREATE
//     EXTENSION statements (RP.9/RP.10), the GRANT ON PROTOCOL (RP.11), and the
//     shared-ConfigMap sync invariant (RP.12). The functional layer asserts the
//     builder-observable targets (RP.8/RP.12) directly and treats RP.9–RP.11 as
//     contract documentation cross-checked live by the e2e suite (which queries
//     pg_extension and the protocol grant against the deployed coordinator).
//
// The values mirror the FULL multi-server dataLoading spec produced by the
// scenario93FullDataLoading() helper in the functional/e2e suites exactly.
type Scenario93Case struct {
	// ID is the catalog rule id ("SL.1".."SL.6", "RP.8".."RP.12").
	ID string
	// Name is a short test name.
	Name string
	// Server is the PXF server the SL row asserts (empty for RP rows).
	Server string
	// ServerType is the server type discriminator (s3/hdfs/jdbc/hive/hbase) for
	// SL rows (empty for RP rows).
	ServerType string
	// ExpectedKeys is the EXACT set of "<server>__<file>.xml" ConfigMap data keys
	// the builder must emit for this SL server (nil for RP rows).
	ExpectedKeys []string
	// KeyContains maps a ConfigMap data key to substrings the rendered XML body
	// MUST contain (config keys, placeholder tokens). Empty for RP rows.
	KeyContains map[string][]string
	// ForbiddenSubstrings are literal values (e.g. raw secret values) that must
	// NOT appear in ANY rendered XML body (SL.6 anti-leak rows).
	ForbiddenSubstrings []string
	// Target is the assertion target for RP rows (e.g. "initContainer.pxf-cred-init",
	// "db.CREATE EXTENSION pxf", "db.GRANT ON PROTOCOL pxf",
	// "configMap.shared-sync"). Empty for SL rows.
	Target string
	// Description explains the asserted fact.
	Description string
}

// Scenario93Cases returns the Scenario 93 "Server ConfigMap, File Mapping,
// Extensions, Sync" catalog (SL.1–SL.6, RP.8–RP.12). It is the single source of
// truth the functional/e2e CatalogHonest cross-checks consume.
//
// SL rows pin the IMPLEMENTED file-mapping per server type against the canonical
// 6-server fixture:
//
//	SL.1 s3-datalake     (s3)    → <srv>__s3-site.xml          (fs.s3a.* + cred placeholders)
//	SL.2 hadoop-cluster  (hdfs)  → core-site + hdfs-site (always) + hive-site + hbase-site
//	SL.3 mysql-oltp,postgres-source (jdbc) → <srv>__jdbc-site.xml (jdbc.driver/url + cred placeholders)
//	SL.4 hive-warehouse  (hive)  → core-site + hive-site       (both always)
//	SL.5 hbase-store     (hbase) → core-site + hbase-site      (both always)
//	SL.6 every credentialed body carries ${PLACEHOLDER}; NO literal secret values
//
// RP rows pin the runtime/DB contract:
//
//	RP.8  pxf-cred-init renders placeholders from mounted Secrets at startup
//	RP.9  CREATE EXTENSION IF NOT EXISTS pxf ran
//	RP.10 CREATE EXTENSION IF NOT EXISTS pxf_fdw ran
//	RP.11 GRANT SELECT,INSERT ON PROTOCOL pxf TO "gpadmin"
//	RP.12 all sidecars see byte-identical configs via the shared ConfigMap volume
func Scenario93Cases() []Scenario93Case {
	return []Scenario93Case{
		// --- SL.1 s3 (s3-datalake) → s3-site.xml ---
		{
			ID: "SL.1", Name: "s3_server_renders_s3_site",
			Server: "s3-datalake", ServerType: "s3",
			ExpectedKeys: []string{"s3-datalake__s3-site.xml"},
			KeyContains: map[string][]string{
				"s3-datalake__s3-site.xml": {
					"fs.s3a.endpoint", "http://minio:9000",
					"fs.s3a.access.key", "fs.s3a.secret.key",
					"${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}",
					"${BACKUP_S3_CREDENTIALS_AWS_SECRET_ACCESS_KEY}",
				},
			},
			Description: "s3 server 's3-datalake' renders s3-site.xml with fs.s3a.endpoint " +
				"Config + fs.s3a.access.key/secret.key credential ${...} placeholders",
		},
		// --- SL.2 hdfs (hadoop-cluster) → core+hdfs(always)+hive+hbase ---
		{
			ID: "SL.2", Name: "hdfs_server_renders_core_hdfs_hive_hbase",
			Server: "hadoop-cluster", ServerType: "hdfs",
			ExpectedKeys: []string{
				"hadoop-cluster__core-site.xml",
				"hadoop-cluster__hdfs-site.xml",
				"hadoop-cluster__hive-site.xml",
				"hadoop-cluster__hbase-site.xml",
			},
			KeyContains: map[string][]string{
				"hadoop-cluster__core-site.xml":  {"fs.defaultFS", "hdfs://namenode:8020"},
				"hadoop-cluster__hdfs-site.xml":  {"dfs.replication"},
				"hadoop-cluster__hive-site.xml":  {"hive.metastore.uris", "thrift://hive-metastore:9083"},
				"hadoop-cluster__hbase-site.xml": {"hbase.zookeeper.quorum", "zk:2181"},
			},
			Description: "hdfs server 'hadoop-cluster' renders core-site.xml (fs.*) + hdfs-site.xml " +
				"(dfs.*, always) + hive-site.xml (server.Hive) + hbase-site.xml (server.Hbase)",
		},
		// --- SL.3 jdbc (mysql-oltp, postgres-source) → jdbc-site.xml ---
		{
			ID: "SL.3", Name: "jdbc_servers_render_jdbc_site",
			Server: "mysql-oltp,postgres-source", ServerType: "jdbc",
			ExpectedKeys: []string{
				"mysql-oltp__jdbc-site.xml",
				"postgres-source__jdbc-site.xml",
			},
			KeyContains: map[string][]string{
				"mysql-oltp__jdbc-site.xml": {
					"jdbc.driver", "com.mysql.cj.jdbc.Driver",
					"jdbc.url", "jdbc:mysql://mysql:3306/oltp",
					"jdbc.user", "jdbc.password",
					"${MYSQL_CREDENTIALS_USERNAME}", "${MYSQL_CREDENTIALS_PASSWORD}",
				},
				"postgres-source__jdbc-site.xml": {
					"jdbc.driver", "org.postgresql.Driver",
					"jdbc.url", "jdbc:postgresql://pgsource:5432/sourcedb",
					"${PG_SOURCE_CREDENTIALS_USERNAME}", "${PG_SOURCE_CREDENTIALS_PASSWORD}",
				},
			},
			Description: "jdbc servers render jdbc-site.xml with jdbc.driver + jdbc.url Config and " +
				"jdbc.user/jdbc.password credential ${...} placeholders",
		},
		// --- SL.4 hive-typed → core-site + hive-site (both always) ---
		{
			ID: "SL.4", Name: "hive_server_renders_core_and_hive",
			Server: "hive-warehouse", ServerType: "hive",
			ExpectedKeys: []string{
				"hive-warehouse__core-site.xml",
				"hive-warehouse__hive-site.xml",
			},
			KeyContains: map[string][]string{
				"hive-warehouse__core-site.xml": {"fs.defaultFS"},
				"hive-warehouse__hive-site.xml": {"hive.metastore.uris"},
			},
			Description: "hive-typed server 'hive-warehouse' renders BOTH core-site.xml (fs.*) " +
				"and hive-site.xml (hive.*) — always emitted",
		},
		// --- SL.5 hbase-typed → core-site + hbase-site (both always) ---
		{
			ID: "SL.5", Name: "hbase_server_renders_core_and_hbase",
			Server: "hbase-store", ServerType: "hbase",
			ExpectedKeys: []string{
				"hbase-store__core-site.xml",
				"hbase-store__hbase-site.xml",
			},
			KeyContains: map[string][]string{
				"hbase-store__core-site.xml":  {"fs.defaultFS"},
				"hbase-store__hbase-site.xml": {"hbase.zookeeper.quorum"},
			},
			Description: "hbase-typed server 'hbase-store' renders BOTH core-site.xml (fs.*) " +
				"and hbase-site.xml (hbase.*) — always emitted",
		},
		// --- SL.6 placeholders not literals (anti-leak) ---
		{
			ID: "SL.6", Name: "credential_bodies_carry_placeholders_not_secrets",
			Server: "", ServerType: "",
			KeyContains: map[string][]string{
				"s3-datalake__s3-site.xml":  {"${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}"},
				"mysql-oltp__jdbc-site.xml": {"${MYSQL_CREDENTIALS_USERNAME}"},
			},
			ForbiddenSubstrings: []string{"minioadmin", "pxfpass"},
			Target:              "configMap.placeholders",
			Description: "every credentialed XML body contains ${PLACEHOLDER} env-var refs " +
				"(e.g. ${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}, ${MYSQL_CREDENTIALS_USERNAME}) " +
				"and NO literal secret value (no minioadmin/pxfpass)",
		},
		// --- RP.8 cred init container renders placeholders from mounted Secrets ---
		{
			ID: "RP.8", Name: "cred_init_renders_placeholders",
			Target: "initContainer.pxf-cred-init",
			Description: "the pxf-cred-init init container exists, mounts the templates ConfigMap " +
				"(pxf-templates) + the credentialSecrets as env (SecretKeyRef with sanitized names " +
				"matching the placeholders), and runs the envsubst render script writing nested " +
				"<server>/<file>.xml into the shared pxf-servers emptyDir",
		},
		// --- RP.9 CREATE EXTENSION pxf ---
		{
			ID: "RP.9", Name: "create_extension_pxf",
			Target: "db.CREATE EXTENSION pxf",
			Description: "SetupPXFExtensions issues CREATE EXTENSION IF NOT EXISTS pxf " +
				"(best-effort, non-fatal); the live e2e checks pg_extension contains pxf",
		},
		// --- RP.10 CREATE EXTENSION pxf_fdw ---
		{
			ID: "RP.10", Name: "create_extension_pxf_fdw",
			Target: "db.CREATE EXTENSION pxf_fdw",
			Description: "SetupPXFExtensions issues CREATE EXTENSION IF NOT EXISTS pxf_fdw " +
				"(best-effort, non-fatal); the live e2e checks pg_extension contains pxf_fdw",
		},
		// --- RP.11 GRANT ON PROTOCOL pxf to the data-loader role (gpadmin) ---
		{
			ID: "RP.11", Name: "grant_protocol_pxf_to_gpadmin",
			Target: "db.GRANT ON PROTOCOL pxf",
			Description: "SetupPXFExtensions issues GRANT SELECT ON PROTOCOL pxf TO \"gpadmin\" " +
				"and GRANT INSERT ON PROTOCOL pxf TO \"gpadmin\" after a successful pxf install " +
				"(best-effort, non-fatal); the live e2e checks the grant / an external-table read",
		},
		// --- RP.12 sync via shared ConfigMap (verify-only) ---
		{
			ID: "RP.12", Name: "sidecars_share_identical_configs",
			Target: "configMap.shared-sync",
			Description: "all sidecars see IDENTICAL server configs via the shared ConfigMap " +
				"volume: the SAME <cluster>-pxf-servers ConfigMap is mounted (as pxf-templates) on " +
				"EVERY segment-primary pod's credential init container, so every sidecar renders " +
				"byte-identical resolved configs (the shared ConfigMap IS the sync mechanism — no " +
				"explicit pxf sync command)",
		},
	}
}

// Scenario94Case represents a single Scenario 94 "PXF Sidecar Deployment
// Verification" expectation. Each row anchors ONE attribute of the PXF sidecar
// container the operator injects into the segment-primary pod template
// (BuildPXFSidecarContainers): the container name, an env var (name+value), the
// container port, the liveness/readiness probe path+delays/periods, the
// command-absence (entrypoint-owned lifecycle), a volume mount, or a converted
// resource request/limit. It mirrors the Scenario91Case/Scenario92DataLoadCase
// catalog shape (ID, FieldPath, ExpectedValue, Description) so the
// functional/e2e suites can iterate it and assert against the LIVE built
// container (CatalogHonest).
//
// SCENARIO NUMBERING NOTE: Scenario 92 is the data-loading INGESTION RUNTIME
// (external-table DDL + load Jobs, see Scenario92DataLoadCases across three test
// files); Scenario 94 is the SIDECAR DEPLOYMENT VERIFICATION (the pxf container
// shape on the segment pod). Scenario 91 also verifies sidecar CONFIG (env
// derived from the full 5-server spec + the rendered servers ConfigMap); this
// Scenario 94 catalog instead pins the full container CONTRACT (port, probes,
// command-absence, mounts, resources) deterministically.
//
// HONESTY NOTE: the liveness/readiness probe path is asserted as
// "/actuator/health" — the real Spring Boot actuator endpoint exposed by the
// apache/cloudberry-pxf 2.1.0 image (returns {"status":"UP"}). The legacy
// "/pxf/v15/Status" path is a DB-client endpoint that returns 404 on that image
// and is NOT used for health checks. The container also sets NO Command and NO
// Args: the "pxf prepare → pxf start → tail service log" lifecycle is owned by
// the image ENTRYPOINT (hack/docker-entrypoint-pxf.sh), so the operator injects
// no Command/Args. Both facts are pinned by dedicated catalog rows below.
type Scenario94Case struct {
	// ID is the catalog rule id (e.g. "S94.1").
	ID string
	// FieldPath is the dotted sidecar-attribute path under test (e.g.
	// "sidecar.name", "sidecar.env.PXF_HOME", "sidecar.port.containerPort",
	// "sidecar.liveness.path", "sidecar.command", "sidecar.volumeMount.pxf-base",
	// "sidecar.resources.requests.cpu").
	FieldPath string
	// ExpectedValue is the value the operator produces, as a string. For the
	// command/args absence rows the sentinel "<nil>" is used.
	ExpectedValue string
	// Description explains what the operator must produce for the field.
	Description string
}

// Scenario94Cases returns the Scenario 94 PXF-sidecar-deployment catalog
// (S94.1-S94.N). It enumerates every attribute of the injected "pxf" sidecar
// container the operator must produce on the segment-primary pod template:
//
//   - the container name ("pxf");
//   - the seven sidecar env vars with their exact values/sources (PXF_HOME,
//     PXF_BASE, PXF_JVM_OPTS == pxf.jvmOpts, PXF_PORT == "5888", PXF_LOG_LEVEL ==
//     pxf.logLevel, PXF_EXTENSION_PXF, PXF_EXTENSION_PXF_FDW);
//   - the container port (5888, name "pxf", TCP);
//   - the liveness probe (/actuator/health on 5888, delay 60, period 20) and the
//     readiness probe (/actuator/health on 5888, delay 30, period 10);
//   - the command/args ABSENCE (entrypoint-owned lifecycle);
//   - the three volume mounts (pxf-base→/pxf-base, pxf-servers→/pxf-base/servers,
//     pxf-lib→/pxf/lib/custom);
//   - the converted resources requests/limits.
//
// The expected values mirror the spec produced by the scenario94FullDataLoading()
// helper in the functional/e2e suites exactly; the CatalogHonest test resolves
// each FieldPath against the LIVE built sidecar container so the catalog stays
// honest against the implementation (internal/builder/pxf_builder.go).
func Scenario94Cases() []Scenario94Case {
	return []Scenario94Case{
		// --- container identity ---
		{
			ID: "S94.1", FieldPath: "sidecar.name",
			ExpectedValue: "pxf",
			Description:   "the operator injects a sidecar container named 'pxf' into the segment-primary pod",
		},
		// --- env vars (name => exact value/source) ---
		{
			ID: "S94.2", FieldPath: "sidecar.env.PXF_HOME",
			ExpectedValue: "/usr/local/cloudberry-pxf",
			Description:   "PXF_HOME is the fixed PXF install home in the sidecar image",
		},
		{
			ID: "S94.3", FieldPath: "sidecar.env.PXF_BASE",
			ExpectedValue: "/pxf-base",
			Description:   "PXF_BASE is the fixed PXF runtime base (emptyDir mount path)",
		},
		{
			ID: "S94.4", FieldPath: "sidecar.env.PXF_JVM_OPTS",
			ExpectedValue: "-Xmx1g -Xms256m",
			Description:   "PXF_JVM_OPTS == pxf.jvmOpts (default '-Xmx1g -Xms256m')",
		},
		{
			ID: "S94.5", FieldPath: "sidecar.env.PXF_PORT",
			ExpectedValue: "5888",
			Description:   "PXF_PORT == pxf.port rendered as the string '5888'",
		},
		{
			ID: "S94.6", FieldPath: "sidecar.env.PXF_LOG_LEVEL",
			ExpectedValue: "INFO",
			Description:   "PXF_LOG_LEVEL == pxf.logLevel (default INFO); propagates on rebuild",
		},
		{
			ID: "S94.7", FieldPath: "sidecar.env.PXF_EXTENSION_PXF",
			ExpectedValue: "true",
			Description:   "PXF_EXTENSION_PXF reflects extensions.pxf (*bool, default/true)",
		},
		{
			ID: "S94.8", FieldPath: "sidecar.env.PXF_EXTENSION_PXF_FDW",
			ExpectedValue: "true",
			Description:   "PXF_EXTENSION_PXF_FDW reflects extensions.pxfFdw (*bool, default/true)",
		},
		// --- container port ---
		{
			ID: "S94.9", FieldPath: "sidecar.port.name",
			ExpectedValue: "pxf",
			Description:   "the sidecar exposes a named container port 'pxf'",
		},
		{
			ID: "S94.10", FieldPath: "sidecar.port.containerPort",
			ExpectedValue: "5888",
			Description:   "the sidecar container port is 5888 (== pxf.port)",
		},
		{
			ID: "S94.11", FieldPath: "sidecar.port.protocol",
			ExpectedValue: "TCP",
			Description:   "the sidecar container port protocol is TCP",
		},
		// --- liveness probe ---
		{
			ID: "S94.12", FieldPath: "sidecar.liveness.path",
			ExpectedValue: "/actuator/health",
			Description: "liveness probe HTTPGet path is /actuator/health (PXF 2.1.0 Spring Boot " +
				"actuator), NOT the legacy /pxf/v15/Status (which 404s on the real image)",
		},
		{
			ID: "S94.13", FieldPath: "sidecar.liveness.port",
			ExpectedValue: "5888",
			Description:   "liveness probe HTTPGet port is the pxf port 5888",
		},
		{
			ID: "S94.14", FieldPath: "sidecar.liveness.initialDelaySeconds",
			ExpectedValue: "60",
			Description:   "liveness probe initialDelaySeconds is 60 (JVM cold start)",
		},
		{
			ID: "S94.15", FieldPath: "sidecar.liveness.periodSeconds",
			ExpectedValue: "20",
			Description:   "liveness probe periodSeconds is 20",
		},
		// --- readiness probe ---
		{
			ID: "S94.16", FieldPath: "sidecar.readiness.path",
			ExpectedValue: "/actuator/health",
			Description:   "readiness probe HTTPGet path is /actuator/health (same actuator endpoint)",
		},
		{
			ID: "S94.17", FieldPath: "sidecar.readiness.port",
			ExpectedValue: "5888",
			Description:   "readiness probe HTTPGet port is the pxf port 5888",
		},
		{
			ID: "S94.18", FieldPath: "sidecar.readiness.initialDelaySeconds",
			ExpectedValue: "30",
			Description:   "readiness probe initialDelaySeconds is 30",
		},
		{
			ID: "S94.19", FieldPath: "sidecar.readiness.periodSeconds",
			ExpectedValue: "10",
			Description:   "readiness probe periodSeconds is 10",
		},
		// --- command/args absence (entrypoint-owned lifecycle) ---
		{
			ID: "S94.20", FieldPath: "sidecar.command",
			ExpectedValue: "<nil>",
			Description: "the sidecar sets NO Command: the pxf prepare/start/tail lifecycle is owned " +
				"by the image ENTRYPOINT (hack/docker-entrypoint-pxf.sh)",
		},
		{
			ID: "S94.21", FieldPath: "sidecar.args",
			ExpectedValue: "<nil>",
			Description: "the sidecar sets NO Args: lifecycle is entrypoint-owned (no operator " +
				"override of the image command line)",
		},
		// --- volume mounts ---
		{
			ID: "S94.22", FieldPath: "sidecar.volumeMount.pxf-base",
			ExpectedValue: "/pxf-base",
			Description:   "the pxf-base volume is mounted at /pxf-base ($PXF_BASE)",
		},
		{
			ID: "S94.23", FieldPath: "sidecar.volumeMount.pxf-servers",
			ExpectedValue: "/pxf-base/servers",
			Description:   "the pxf-servers volume is mounted at /pxf-base/servers (resolved site files)",
		},
		{
			ID: "S94.24", FieldPath: "sidecar.volumeMount.pxf-lib",
			ExpectedValue: "/pxf/lib/custom",
			Description:   "the pxf-lib volume is mounted at /pxf/lib/custom (custom connector JARs)",
		},
		// --- converted resources (requests + limits) ---
		{
			ID: "S94.25", FieldPath: "sidecar.resources.requests.cpu",
			ExpectedValue: "250m",
			Description:   "pxf.resources.requests.cpu is converted onto the sidecar container",
		},
		{
			ID: "S94.26", FieldPath: "sidecar.resources.requests.memory",
			ExpectedValue: "256Mi",
			Description:   "pxf.resources.requests.memory is converted onto the sidecar container",
		},
		{
			ID: "S94.27", FieldPath: "sidecar.resources.limits.cpu",
			ExpectedValue: "1",
			Description:   "pxf.resources.limits.cpu is converted onto the sidecar container",
		},
		{
			ID: "S94.28", FieldPath: "sidecar.resources.limits.memory",
			ExpectedValue: "1Gi",
			Description:   "pxf.resources.limits.memory is converted onto the sidecar container",
		},
	}
}

// Scenario95Case represents a single Scenario 95 "PXF CLI Lifecycle" expectation.
// Each row documents ONE verb of the PXF lifecycle contract (L.1-L.6). It mirrors
// the Scenario94Case catalog shape (ID, Verb, Layer, Description) so the
// functional/e2e suites can document and (where operator-driven) assert the
// contract.
//
// SCENARIO NUMBERING NOTE: Scenario 94 (PXF Sidecar Deployment Verification) is
// RETAINED unchanged; Scenario 95 (this catalog) is the PXF CLI Lifecycle that
// follows it. Do NOT renumber 94.
//
// LAYER / HONESTY NOTE: the sidecar-local verbs (prepare/start/stop and the
// in-place sidecar restart) are exercised ONLY via `kubectl exec -c pxf` in the
// e2e suite (Layer "exec") — they are NOT cloudberry-ctl commands. The
// operator-driven verbs (restart, sync, status) are exercised at the
// controller/handler level over a fake client in the functional suite (Layer
// "operator") and end-to-end via `cloudberry-ctl pxf ...` in e2e. The
// operator-driven `pxf restart` is a segment-primary pod ROLL (STS template
// restart-trigger bump), heavier than an in-place sidecar restart.
type Scenario95Case struct {
	// ID is the catalog rule id (e.g. "L.1").
	ID string
	// Verb is the PXF lifecycle verb under test (e.g. "prepare", "restart").
	Verb string
	// Layer is "exec" (sidecar-local, kubectl exec only) or "operator"
	// (operator-driven via REST/CLI).
	Layer string
	// Description explains the contract the verb must satisfy.
	Description string
}

// Scenario95Cases returns the Scenario 95 PXF-CLI-lifecycle catalog (L.1-L.6).
// It documents the verb-level contract exercised across the functional (operator
// layer) and e2e (exec + operator layer) suites:
//
//   - L.1 prepare: idempotent — `pxf prepare` on the live sidecar is safe to
//     re-run (exec-only).
//   - L.2 start → status Running: `pxf start` brings the sidecar up and the
//     actuator/health reports UP; `pxf status` (operator) then reports the
//     segment-primary pxf containers Ready (exec + operator).
//   - L.3 stop → readiness fails: `pxf stop` takes the sidecar down and the
//     readiness aggregation reflects not-ready (exec + operator).
//   - L.4 restart recovers: `cloudberry-ctl pxf restart --cluster` rolls the
//     segment-primary pods (STS restart-trigger bump) and the sidecars come back
//     Ready; cloudberry_pxf_restart_total{result="started"} increments (operator).
//   - L.5 sync redistributes: `cloudberry-ctl pxf sync --cluster` refreshes the
//     <cluster>-pxf-servers ConfigMap and rolls the sidecars so the new server
//     config takes effect (operator).
//   - L.6 ctl pxf restart → all sidecars: the headline command restarts PXF
//     across ALL segment-primary sidecars in one operator action (operator + exec
//     verification of the roll).
func Scenario95Cases() []Scenario95Case {
	return []Scenario95Case{
		{
			ID: "L.1", Verb: "prepare", Layer: "exec",
			Description: "`pxf prepare` on the live segment-primary sidecar is idempotent " +
				"(safe to re-run; exercised via kubectl exec -c pxf in e2e)",
		},
		{
			ID: "L.2", Verb: "start", Layer: "exec+operator",
			Description: "`pxf start` brings the sidecar up (actuator/health UP); the operator " +
				"`pxf status` then reports the segment-primary pxf containers Ready (Running)",
		},
		{
			ID: "L.3", Verb: "stop", Layer: "exec+operator",
			Description: "`pxf stop` takes the sidecar down; the operator `pxf status` readiness " +
				"aggregation reflects the pxf container as not-ready",
		},
		{
			ID: "L.4", Verb: "restart", Layer: "operator",
			Description: "operator `pxf restart` rolls the <cluster>-segment-primary StatefulSet " +
				"(restart-trigger bump); sidecars recover Ready and " +
				"cloudberry_pxf_restart_total{result=\"started\"} increments",
		},
		{
			ID: "L.5", Verb: "sync", Layer: "operator",
			Description: "operator `pxf sync` refreshes the <cluster>-pxf-servers ConfigMap and bumps " +
				"the segment-primary restart-trigger so the new server config redistributes on roll",
		},
		{
			ID: "L.6", Verb: "ctl-restart-all", Layer: "operator+exec",
			Description: "`cloudberry-ctl pxf restart --cluster <name>` restarts PXF across ALL " +
				"segment-primary sidecars in one operator action (the headline command)",
		},
	}
}

// Scenario96Case represents a single Scenario 96 "Object Store Profiles & Format
// Write-Capability" expectation. It mirrors the Scenario95Case SHAPE (a small,
// flat row catalog) but carries the fields this scenario needs to resolve a case
// against the built artifact: the PXF Profile, the Server it targets, the Mode
// ("" read / "writable" export) and the Expected outcome token.
//
// The catalog covers three layers:
//   - OS.1-OS.10  object-store READS (s3-datalake & minio-warehouse ×
//     text/parquet/avro/json/orc). ORC is [CONFIG-ONLY] (not synthesizable
//     locally); the rest are live where the sample generator produces them.
//   - CFG.1-CFG.8 gs/abfss/wasbs/Dell-ECS server-config + LOCATION — all
//     [CONFIG-ONLY] (no local backing store).
//   - FF.1-FF.5   the write-capability matrix: text/parquet/avro writable
//     SUCCEED (admit + WRITABLE export DDL); json/orc writable REJECT
//     (admission DENY containing "write-unsupported"/"writable").
//
// HONESTY NOTE: every [CONFIG-ONLY] row is explicitly marked in Description so a
// reader can never mistake a config-only assertion for a live-data assertion.
type Scenario96Case struct {
	// ID is the catalog rule id (e.g. "OS.1", "CFG.3", "FF.4").
	ID string
	// Layer is one of:
	//   "builder/DDL"    — assert the generated external-table DDL/LOCATION.
	//   "server-config"  — assert the rendered <server>__s3-site.xml.
	//   "live-read"      — readable object-store profile with a live MinIO sample.
	//   "live-write"     — writable export profile (FF.1-FF.3).
	//   "webhook"        — admission DENY (FF.4/FF.5).
	// Combined layers are joined with "+" (e.g. "builder/DDL+live-read").
	Layer string
	// Profile is the PXF profile under test, "<scheme>:<format>" (e.g. s3:text).
	Profile string
	// Server is the PXF server the job/profile targets (e.g. s3-datalake).
	Server string
	// Mode is "" for read/import or pxfpolicy.ModeWritable ("writable") for the
	// write/export cases (FF.*).
	Mode string
	// Expected is the outcome token: "admit-read", "admit-write" or
	// "deny-write" — what resolving the case against the real artifact must show.
	Expected string
	// Description explains the case and ALWAYS marks [CONFIG-ONLY] where the row
	// has no live backing store / synthesizable sample.
	Description string
}

// Scenario 96 expected-outcome tokens.
const (
	// Scenario96ExpectAdmitRead — a read/import profile that admits and whose
	// read DDL (pxfwritable_import) + LOCATION is byte-correct.
	Scenario96ExpectAdmitRead = "admit-read"
	// Scenario96ExpectAdmitWrite — a writable profile that admits and whose
	// WRITABLE export DDL (pxfwritable_export) is byte-correct (FF.1-FF.3).
	Scenario96ExpectAdmitWrite = "admit-write"
	// Scenario96ExpectDenyWrite — a writable profile that is DENIED at admission
	// with an error containing "write-unsupported"/"writable" (FF.4/FF.5).
	Scenario96ExpectDenyWrite = "deny-write"
)

// Scenario96 server names (the object-store set shipped in the Scenario 96
// sample CR). s3-datalake and minio-warehouse are MinIO-backed (live); the rest
// are config-only (no local backing store).
const (
	Scenario96ServerS3Datalake     = "s3-datalake"
	Scenario96ServerMinioWarehouse = "minio-warehouse"
	Scenario96ServerGCSDatalake    = "gcs-datalake"
	Scenario96ServerADLSGen2       = "adls-gen2"
	Scenario96ServerAzureBlob      = "azure-blob"
	Scenario96ServerDellECS        = "dell-ecs"
)

// Scenario96Cases returns the full Scenario 96 catalog (OS.1-OS.10, CFG.1-CFG.8,
// FF.1-FF.5) from the task-breakdown §3. The rows are honest against the shipped
// production contract (internal/pxfpolicy, the webhook validatePxfJob writable
// rule, and the builder buildPXFExternalTableDDL writable branch): the
// CatalogHonest test resolves each row against the REAL built artifact.
func Scenario96Cases() []Scenario96Case {
	return []Scenario96Case{
		// --- OS.* object-store READS: s3-datalake (AWS-style) ---
		{
			ID: "OS.1", Layer: "builder/DDL+live-read",
			Profile: "s3:text", Server: Scenario96ServerS3Datalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:text on s3-datalake: LOCATION PROFILE=s3:text&SERVER=s3-datalake; " +
				"live rows land (text/CSV is natively synthesizable)",
		},
		{
			ID: "OS.2", Layer: "builder/DDL+live-read",
			Profile: "s3:parquet", Server: Scenario96ServerS3Datalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:parquet on s3-datalake: parquet sample → live rows land " +
				"(parquet synthesized via the python tooling container)",
		},
		{
			ID: "OS.3", Layer: "builder/DDL+live-read",
			Profile: "s3:avro", Server: Scenario96ServerS3Datalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:avro on s3-datalake: avro sample → live rows land " +
				"(avro synthesized via fastavro; [CONFIG-ONLY] if tooling absent)",
		},
		{
			ID: "OS.4", Layer: "builder/DDL+live-read",
			Profile: "s3:json", Server: Scenario96ServerS3Datalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:json on s3-datalake: json sample → live rows land " +
				"(json is natively synthesizable)",
		},
		{
			ID: "OS.5", Layer: "builder/DDL",
			Profile: "s3:orc", Server: Scenario96ServerS3Datalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] s3:orc on s3-datalake: DDL/LOCATION correctness only " +
				"(ORC is not synthesized locally — no easy tool)",
		},
		// --- OS.* object-store READS: minio-warehouse (path-style) ---
		{
			ID: "OS.6", Layer: "server-config+live-read",
			Profile: "s3:text", Server: Scenario96ServerMinioWarehouse,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:text on minio-warehouse: path-style s3-site.xml " +
				"(fs.s3a.path.style.access=true); live rows land",
		},
		{
			ID: "OS.7", Layer: "server-config+live-read",
			Profile: "s3:parquet", Server: Scenario96ServerMinioWarehouse,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:parquet on minio-warehouse: path-style; live rows land " +
				"(parquet synthesized via tooling container)",
		},
		{
			ID: "OS.8", Layer: "server-config+live-read",
			Profile: "s3:avro", Server: Scenario96ServerMinioWarehouse,
			Expected: Scenario96ExpectAdmitRead,
			Description: "s3:avro on minio-warehouse: live rows land " +
				"(avro synthesized via fastavro; [CONFIG-ONLY] if tooling absent)",
		},
		{
			ID: "OS.9", Layer: "server-config+live-read",
			Profile: "s3:json", Server: Scenario96ServerMinioWarehouse,
			Expected:    Scenario96ExpectAdmitRead,
			Description: "s3:json on minio-warehouse: live rows land (json natively synthesizable)",
		},
		{
			ID: "OS.10", Layer: "server-config",
			Profile: "s3:orc", Server: Scenario96ServerMinioWarehouse,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] s3:orc on minio-warehouse: DDL/LOCATION + path-style " +
				"config correctness only (ORC not synthesized locally)",
		},

		// --- CFG.* gs/abfss/wasbs/Dell-ECS server-config + LOCATION (all [CONFIG-ONLY]) ---
		{
			ID: "CFG.1", Layer: "server-config+builder/DDL",
			Profile: "gs:text", Server: Scenario96ServerGCSDatalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] gs:text on gcs-datalake: valid GCS object-store site XML; " +
				"LOCATION PROFILE=gs:text correct (no local GCS backing store)",
		},
		{
			ID: "CFG.2", Layer: "server-config+builder/DDL",
			Profile: "gs:parquet", Server: Scenario96ServerGCSDatalake,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] gs:parquet on gcs-datalake: site XML + LOCATION correct " +
				"(no local GCS backing store)",
		},
		{
			ID: "CFG.3", Layer: "server-config+builder/DDL",
			Profile: "abfss:text", Server: Scenario96ServerADLSGen2,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] abfss:text on adls-gen2: Azure ADLS Gen2 site XML; " +
				"LOCATION PROFILE=abfss:text correct (no local Azure backing store)",
		},
		{
			ID: "CFG.4", Layer: "server-config+builder/DDL",
			Profile: "abfss:parquet", Server: Scenario96ServerADLSGen2,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] abfss:parquet on adls-gen2: site XML + LOCATION correct " +
				"(no local Azure backing store)",
		},
		{
			ID: "CFG.5", Layer: "server-config+builder/DDL",
			Profile: "wasbs:text", Server: Scenario96ServerAzureBlob,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] wasbs:text on azure-blob: Azure Blob site XML; " +
				"LOCATION PROFILE=wasbs:text correct (no local Azure backing store)",
		},
		{
			ID: "CFG.6", Layer: "server-config+builder/DDL",
			Profile: "wasbs:json", Server: Scenario96ServerAzureBlob,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] wasbs:json on azure-blob: site XML + LOCATION correct " +
				"(no local Azure backing store)",
		},
		{
			ID: "CFG.7", Layer: "server-config+builder/DDL",
			Profile: "s3:text", Server: Scenario96ServerDellECS,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] s3:text on dell-ecs (s3 + custom fs.s3a.endpoint): " +
				"S3-compatible site XML carries the custom endpoint; LOCATION correct",
		},
		{
			ID: "CFG.8", Layer: "server-config+builder/DDL",
			Profile: "s3:parquet", Server: Scenario96ServerDellECS,
			Expected: Scenario96ExpectAdmitRead,
			Description: "[CONFIG-ONLY] s3:parquet on dell-ecs: endpoint-overridden s3-site.xml " +
				"+ LOCATION correct",
		},

		// --- FF.* write-capability matrix (the core deliverable) ---
		{
			ID: "FF.1", Layer: "webhook+builder/DDL+live-write",
			Profile: "s3:text", Server: Scenario96ServerS3Datalake, Mode: "writable",
			Expected: Scenario96ExpectAdmitWrite,
			Description: "SUCCEED — s3:text writable admitted; WRITABLE export DDL has " +
				"pxfwritable_export and NO LOG ERRORS; FF.1 live export round-trips",
		},
		{
			ID: "FF.2", Layer: "webhook+builder/DDL+live-write",
			Profile: "s3:parquet", Server: Scenario96ServerS3Datalake, Mode: "writable",
			Expected: Scenario96ExpectAdmitWrite,
			Description: "SUCCEED — s3:parquet writable admitted; export DDL correct; " +
				"live parquet export round-trips",
		},
		{
			ID: "FF.3", Layer: "webhook+builder/DDL+live-write",
			Profile: "s3:avro", Server: Scenario96ServerS3Datalake, Mode: "writable",
			Expected: Scenario96ExpectAdmitWrite,
			Description: "SUCCEED — s3:avro writable admitted; export DDL correct; live avro " +
				"export round-trips ([CONFIG-ONLY] build+admit only if avro tooling absent)",
		},
		{
			ID: "FF.4", Layer: "webhook",
			Profile: "s3:json", Server: Scenario96ServerS3Datalake, Mode: "writable",
			Expected: Scenario96ExpectDenyWrite,
			Description: "REJECT — s3:json writable admission DENY; error contains " +
				"write-unsupported/writable; builder also errors (defense in depth)",
		},
		{
			ID: "FF.5", Layer: "webhook",
			Profile: "s3:orc", Server: Scenario96ServerS3Datalake, Mode: "writable",
			Expected: Scenario96ExpectDenyWrite,
			Description: "REJECT — s3:orc writable admission DENY; error contains " +
				"write-unsupported/writable; builder also errors (defense in depth)",
		},
	}
}

// ============================================================================
// Scenario 97 — Hadoop Profiles (HDFS / Hive / HBase)
// ============================================================================
//
// NUMBERING NOTE: the request originally labelled this "Scenario 96", but
// Scenario 96 (Object Store Profiles & Format Write-Capability) is already
// implemented in this repo (see Scenario96Cases above; internal/pxfpolicy).
// This work is therefore Scenario 97 — Hadoop Profiles — following the
// 93→94→95→96 precedent.
//
// KEY FINDING: every behaviour Scenario 97 proves is ALREADY shipped — the
// pxfpolicy write-matrix (including SequenceFile), the webhook W.10/W.10b
// admission, the builder's site-file rendering (core/hdfs/hive/hbase-site.xml)
// and the writable export DDL. Scenario 97 is purely a TEST + LIVE-VERIFICATION
// scenario over the single hdfs-typed server `hadoop-cluster` (which also
// carries hive + hbase config).
//
// Scenario97Case mirrors the Scenario96Case SHAPE (a small, flat row catalog)
// with exactly the fields needed to resolve a row against the built artifact:
// the PXF Profile, the Server it targets, the Mode ("" read / "writable"
// export) and the Expected outcome token.
//
// The catalog covers five layers:
//   - HP.1-HP.6  HDFS READS (text/parquet/avro/json/orc/sequencefile).
//   - HV.1-HV.4  HIVE READS (hive auto-detect / hive:text / hive:orc / hive:rc).
//   - HB.1       HBASE READ (case-insensitive HBase profile).
//   - SITE.1-4   rendered site-file assertions (hive/hbase/core/hdfs-site.xml).
//   - FF.6/FF.7  the write-capability edge: hive:rc read OK + writable REJECT;
//     hdfs:sequencefile writable SUCCEED (companion hdfs:text export).
//   - WRej.1-7   the writable DENY matrix (json/orc/hive*/hbase write-unsupported).
//
// HONESTY NOTE: every [CONFIG-ONLY] row is explicitly marked in Description so a
// reader can never mistake a config-only assertion for a live-data assertion.
type Scenario97Case struct {
	// ID is the catalog rule id (e.g. "HP.1", "SITE.3", "WRej.4").
	ID string
	// Layer is one of:
	//   "builder/DDL"    — assert the generated external-table DDL/LOCATION.
	//   "server-config"  — assert the rendered <server>__<file>-site.xml.
	//   "live-read"      — readable Hadoop profile with a live sample.
	//   "live-write"     — writable export profile (FF.7 + companions).
	//   "webhook"        — admission DENY (WRej.* / FF.6b).
	// Combined layers are joined with "+" (e.g. "builder/DDL+live-read").
	Layer string
	// Profile is the PXF profile under test, "<scheme>[:<format>]" (e.g.
	// hdfs:text, hive, HBase). For SITE.* rows it is "" (server-config only).
	Profile string
	// Server is the PXF server the job/profile targets (hadoop-cluster).
	Server string
	// Mode is "" for read/import or pxfpolicy.ModeWritable ("writable") for the
	// write/export cases (FF.7 + WRej.*).
	Mode string
	// Expected is the outcome token: "admit-read", "admit-write", "deny-write"
	// or "render-ok" (SITE.* server-config rows).
	Expected string
	// SiteFile, for SITE.* rows, is the ConfigMap data-key suffix to assert
	// (e.g. "hive-site.xml"); empty for non-SITE rows.
	SiteFile string
	// SiteContains, for SITE.* rows, is a substring the rendered site file must
	// contain (e.g. "thrift://hive-metastore:9083"); empty for non-SITE rows.
	SiteContains string
	// Description explains the case and ALWAYS marks [CONFIG-ONLY] where the row
	// has no live backing store / synthesizable sample.
	Description string
}

// Scenario 97 expected-outcome tokens.
const (
	// Scenario97ExpectAdmitRead — a read/import profile that admits and whose
	// read DDL (pxfwritable_import) + LOCATION is byte-correct.
	Scenario97ExpectAdmitRead = "admit-read"
	// Scenario97ExpectAdmitWrite — a writable profile that admits and whose
	// WRITABLE export DDL (pxfwritable_export) is byte-correct (FF.7).
	Scenario97ExpectAdmitWrite = "admit-write"
	// Scenario97ExpectDenyWrite — a writable profile that is DENIED at admission
	// with an error containing "write-unsupported" (WRej.* / FF.6b).
	Scenario97ExpectDenyWrite = "deny-write"
	// Scenario97ExpectRenderOK — a SITE.* server-config row whose rendered site
	// file must contain the documented key/value.
	Scenario97ExpectRenderOK = "render-ok"
)

// Scenario97 server name. The single hdfs-typed server `hadoop-cluster` carries
// the HDFS, Hive (hive.metastore.uris) and HBase (hbase.zookeeper.quorum)
// config, so all HP/HV/HB/SITE rows target it. (The sample CR also ships
// dedicated hive-warehouse + hbase-store servers, but the catalog binds to the
// single combined server to mirror the spec's hadoop-cluster contract.)
const (
	Scenario97ServerHadoopCluster = "hadoop-cluster"
	Scenario97ServerHiveWarehouse = "hive-warehouse"
	Scenario97ServerHBaseStore    = "hbase-store"
)

// Scenario97 well-known endpoints carried by the hadoop-cluster server (mirror
// the scenario93/scenario97 sample CR values).
const (
	Scenario97FSDefaultFS    = "hdfs://namenode:8020"
	Scenario97HiveMetastore  = "thrift://hive-metastore:9083"
	Scenario97HBaseZKQuorum  = "hbase:2181"
	Scenario97DFSReplication = "1"
)

// Scenario97Cases returns the full Scenario 97 catalog (HP.1-6, HV.1-4, HB.1,
// SITE.1-4, FF.6a/6b, FF.7 + companion, WRej.1-7) from the task-breakdown §3.
// The rows are honest against the shipped production contract
// (internal/pxfpolicy.IsProfileWritable, the webhook W.10/W.10b writable rule,
// and the builder's renderPXFHDFSServer + writable DDL branch): the
// CatalogHonest97 functional/e2e test resolves each row against the REAL built
// artifact (DDL / site-file / admission deny).
func Scenario97Cases() []Scenario97Case {
	srv := Scenario97ServerHadoopCluster
	return []Scenario97Case{
		// --- HP.* HDFS reads ------------------------------------------------
		{
			ID: "HP.1", Layer: "builder/DDL+live-read",
			Profile: "hdfs:text", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "hdfs:text read on hadoop-cluster: LOCATION PROFILE=hdfs:text&SERVER=" +
				"hadoop-cluster; text/CSV is natively synthesizable → live rows land",
		},
		{
			ID: "HP.2", Layer: "builder/DDL+live-read",
			Profile: "hdfs:parquet", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "hdfs:parquet read: parquet synthesized via the python tooling " +
				"container / hive CTAS; [CONFIG-ONLY] if tooling absent",
		},
		{
			ID: "HP.3", Layer: "builder/DDL+live-read",
			Profile: "hdfs:avro", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "hdfs:avro read: avro synthesized via fastavro / hive CTAS; " +
				"[CONFIG-ONLY] if tooling absent",
		},
		{
			ID: "HP.4", Layer: "builder/DDL+live-read",
			Profile: "hdfs:json", Server: srv,
			Expected:    Scenario97ExpectAdmitRead,
			Description: "hdfs:json read: json is natively synthesizable → live rows land",
		},
		{
			ID: "HP.5", Layer: "builder/DDL",
			Profile: "hdfs:orc", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "[CONFIG-ONLY] hdfs:orc read: DDL/LOCATION correctness only " +
				"(ORC produced via hive CTAS into HDFS only when beeline available)",
		},
		{
			ID: "HP.6", Layer: "builder/DDL",
			Profile: "hdfs:sequencefile", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "[CONFIG-ONLY] hdfs:sequencefile read: DDL/LOCATION correctness only " +
				"(SequenceFile produced via hive CTAS into HDFS only when beeline available)",
		},

		// --- HV.* Hive reads ------------------------------------------------
		{
			ID: "HV.1", Layer: "builder/DDL+live-read",
			Profile: "hive", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "hive (auto-detect, bare profile) read: metastore-resolved table; " +
				"bare profile is admitted and is correctly NON-writable (caveat C1)",
		},
		{
			ID: "HV.2", Layer: "builder/DDL+live-read",
			Profile: "hive:text", Server: srv,
			Expected:    Scenario97ExpectAdmitRead,
			Description: "hive:text read: TEXTFILE-stored hive table via the metastore",
		},
		{
			ID: "HV.3", Layer: "builder/DDL+live-read",
			Profile: "hive:orc", Server: srv,
			Expected:    Scenario97ExpectAdmitRead,
			Description: "hive:orc read: ORC-stored hive table via the metastore",
		},
		{
			ID: "HV.4", Layer: "builder/DDL",
			Profile: "hive:rc", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "[CONFIG-ONLY] hive:rc read (= FF.6a read leg): RCFile-stored hive " +
				"table; DDL/LOCATION only unless RCFile CTAS is available",
		},

		// --- HB.* HBase read ------------------------------------------------
		{
			ID: "HB.1", Layer: "builder/DDL+live-read",
			Profile: "HBase", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "[CONFIG-ONLY] HBase read: case-insensitive admit; reads an hbase " +
				"table via the HBase profile (slow under QEMU; live where seeded)",
		},

		// --- SITE.* rendered site-file assertions ---------------------------
		{
			ID: "SITE.1", Layer: "server-config", Server: srv,
			Expected: Scenario97ExpectRenderOK,
			SiteFile: "hive-site.xml", SiteContains: Scenario97HiveMetastore,
			Description: "hadoop-cluster__hive-site.xml carries hive.metastore.uris = " +
				Scenario97HiveMetastore,
		},
		{
			ID: "SITE.2", Layer: "server-config", Server: srv,
			Expected: Scenario97ExpectRenderOK,
			SiteFile: "hbase-site.xml", SiteContains: Scenario97HBaseZKQuorum,
			Description: "hadoop-cluster__hbase-site.xml carries hbase.zookeeper.quorum = " +
				Scenario97HBaseZKQuorum,
		},
		{
			ID: "SITE.3", Layer: "server-config", Server: srv,
			Expected: Scenario97ExpectRenderOK,
			SiteFile: "core-site.xml", SiteContains: Scenario97FSDefaultFS,
			Description: "hadoop-cluster__core-site.xml carries fs.defaultFS = " +
				Scenario97FSDefaultFS,
		},
		{
			ID: "SITE.4", Layer: "server-config", Server: srv,
			Expected: Scenario97ExpectRenderOK,
			SiteFile: "hdfs-site.xml", SiteContains: "<configuration>",
			Description: "hadoop-cluster__hdfs-site.xml is ALWAYS emitted (valid " +
				"<configuration>); dfs.replication=1 present",
		},

		// --- FF.6 hive:rc read OK + writable REJECT -------------------------
		{
			ID: "FF.6a", Layer: "builder/DDL",
			Profile: "hive:rc", Server: srv,
			Expected: Scenario97ExpectAdmitRead,
			Description: "[CONFIG-ONLY] hive:rc read works (= HV.4 read leg); DDL/LOCATION " +
				"correctness asserted",
		},
		{
			ID: "FF.6b", Layer: "webhook+builder/DDL",
			Profile: "hive:rc", Server: srv, Mode: "writable",
			Expected: Scenario97ExpectDenyWrite,
			Description: "REJECT — hive:rc writable admission DENY (write-unsupported); the " +
				"Hive scheme is read-only; builder also errors (defense in depth)",
		},

		// --- FF.7 hdfs:sequencefile writable SUCCEEDS + companion -----------
		{
			ID: "FF.7", Layer: "webhook+builder/DDL+live-write",
			Profile: "hdfs:sequencefile", Server: srv, Mode: "writable",
			Expected: Scenario97ExpectAdmitWrite,
			Description: "SUCCEED — hdfs:sequencefile writable admitted; WRITABLE export DDL " +
				"has pxfwritable_export and NO LOG ERRORS; live export round-trips " +
				"([CONFIG-ONLY] build+admit if sequencefile sample gen unavailable)",
		},
		{
			ID: "FF.7t", Layer: "webhook+builder/DDL+live-write",
			Profile: "hdfs:text", Server: srv, Mode: "writable",
			Expected: Scenario97ExpectAdmitWrite,
			Description: "SUCCEED — hdfs:text writable admitted; export DDL correct; " +
				"text export round-trips",
		},

		// --- WRej.* writable DENY matrix ------------------------------------
		{
			ID: "WRej.1", Layer: "webhook",
			Profile: "hdfs:json", Server: srv, Mode: "writable",
			Expected:    Scenario97ExpectDenyWrite,
			Description: "REJECT — hdfs:json writable admission DENY; json write-unsupported",
		},
		{
			ID: "WRej.2", Layer: "webhook",
			Profile: "hdfs:orc", Server: srv, Mode: "writable",
			Expected:    Scenario97ExpectDenyWrite,
			Description: "REJECT — hdfs:orc writable admission DENY; orc write-unsupported",
		},
		{
			ID: "WRej.3", Layer: "webhook",
			Profile: "hive", Server: srv, Mode: "writable",
			Expected: Scenario97ExpectDenyWrite,
			Description: "REJECT — hive (bare) writable admission DENY; Hive scheme read-only " +
				"(caveat C1)",
		},
		{
			ID: "WRej.4", Layer: "webhook",
			Profile: "hive:text", Server: srv, Mode: "writable",
			Expected:    Scenario97ExpectDenyWrite,
			Description: "REJECT — hive:text writable admission DENY; Hive scheme read-only",
		},
		{
			ID: "WRej.5", Layer: "webhook",
			Profile: "hive:orc", Server: srv, Mode: "writable",
			Expected:    Scenario97ExpectDenyWrite,
			Description: "REJECT — hive:orc writable admission DENY; Hive scheme read-only",
		},
		{
			ID: "WRej.6", Layer: "webhook",
			Profile: "hive:rc", Server: srv, Mode: "writable",
			Expected:    Scenario97ExpectDenyWrite,
			Description: "REJECT — hive:rc writable admission DENY (= FF.6b); Hive scheme read-only",
		},
		{
			ID: "WRej.7", Layer: "webhook",
			Profile: "HBase", Server: srv, Mode: "writable",
			Expected: Scenario97ExpectDenyWrite,
			Description: "REJECT — HBase writable admission DENY; hbase scheme read-only " +
				"(case-insensitive)",
		},
	}
}

// ============================================================================
// Scenario 98: Filter Pushdown, Column Projection, Per-Row Error Handling
// ============================================================================
//
// NUMBERING NOTE: this is Scenario 98 — the RUNTIME-BEHAVIOR + HONEST-
// OBSERVABILITY verification scenario for the three PXF/load DDL knobs that
// ALREADY SHIP and are unit-tested:
//
//   - pxfJob.FilterPushdown=true   → "FILTER_PUSHDOWN=true" in the pxf:// LOCATION
//   - pxfJob.ColumnProjection=true → "PROJECT=true"         in the pxf:// LOCATION
//   - pxfJob.ErrorHandling{...}    → "[LOG ERRORS ]SEGMENT REJECT LIMIT N
//     ROWS|PERCENT" on the READ external table.
//
// The mutating webhook DEFAULTS FilterPushdown + ColumnProjection to true when
// unset (preserving an explicit user false). W.15 validates the reject-limit
// type. The writable export path intentionally OMITS the error-handling suffix.
//
// METRIC HONESTY (see the task-breakdown verdict): the PXF sidecar exposes NO
// honest external-bytes counter, so cloudberry_pxf_bytes_transferred_total stays
// PLANNED and is NEVER asserted/fabricated. The HONEST, operator-observable
// proofs per FE case are, in priority order:
//   - Filter pushdown : (1) cloudberry_data_loading_rows_total (REAL, harvested
//     from the DATALOAD_ROWS marker) is LOWER for a filtered job vs an unfiltered
//     baseline; (2) EXPLAIN shows the pushed filter / projected columns; (3)
//     JDBC/Hive source-side query logs show the WHERE predicate.
//   - Column projection: EXPLAIN shows ONLY the projected columns in the
//     external-scan target list. No honest byte meter ⇒ EXPLAIN-ONLY/CONFIG-ONLY.
//   - Error handling   : REAL cloudberry_data_loading_job_status (2=success /
//     3=failed) + cloudberry_data_loading_errors_total + rows_total (valid only).
//
// Scenario98Case mirrors the Scenario97Case SHAPE (a small, flat row catalog)
// with exactly the fields needed to resolve a row against the built artifact:
// the PXF Profile, the Source/Server it targets, the Knob under test and the
// Expected outcome token. Every [CONFIG-ONLY]/[EXPLAIN-ONLY] row is explicitly
// marked in Description so a reader can never mistake a config-only assertion
// for a live-data assertion, and the honest signal is named.
type Scenario98Case struct {
	// ID is the catalog rule id (e.g. "FE.1", "FE.4", "FE.12a").
	ID string
	// Layer describes the assertion layers, joined with "+":
	//   "builder/DDL"   — assert the generated external-table DDL/LOCATION knob.
	//   "live-rowcount" — filtered Job lands FEWER rows than the baseline.
	//   "live-explain"  — EXPLAIN shows the pushed filter / projected columns.
	//   "live-status"   — Job status / errors_total (FE.12 error handling).
	//   "source-log"    — JDBC/Hive source query log shows the WHERE predicate.
	Layer string
	// Profile is the PXF profile under test (e.g. s3:parquet, jdbc, hive,
	// s3:orc). For FE.12 native error-handling rows it names the read profile.
	Profile string
	// Source is the source family the predicate/projection is proven against
	// ("object-store", "jdbc", "hive").
	Source string
	// Server is the PXF server the job/profile targets.
	Server string
	// Knob is the DDL knob under test: "filterPushdown", "columnProjection" or
	// "errorHandling".
	Knob string
	// Expected is the outcome token: "filter-pushdown", "column-projection",
	// "error-tolerated" or "error-failed".
	Expected string
	// DDLContains is the byte-stable substring the built READ DDL must carry for
	// the row (e.g. "FILTER_PUSHDOWN=true", "PROJECT=true",
	// "SEGMENT REJECT LIMIT 10 ROWS").
	DDLContains string
	// Description explains the case, names the HONEST signal asserted, and ALWAYS
	// marks [CONFIG-ONLY]/[EXPLAIN-ONLY] where the row has no live backing store /
	// synthesizable sample. bytes_transferred is NEVER named as a signal.
	Description string
}

// Scenario 98 expected-outcome tokens.
const (
	// Scenario98ExpectFilterPushdown — a filter-pushdown row: the READ LOCATION
	// carries FILTER_PUSHDOWN=true and the filtered Job lands FEWER rows than the
	// unfiltered baseline (live), corroborated by EXPLAIN / source logs.
	Scenario98ExpectFilterPushdown = "filter-pushdown"
	// Scenario98ExpectColumnProjection — a column-projection row: the READ
	// LOCATION carries PROJECT=true and EXPLAIN shows only the projected columns
	// (EXPLAIN-ONLY — no honest byte meter).
	Scenario98ExpectColumnProjection = "column-projection"
	// Scenario98ExpectErrorTolerated — FE.12a: malformed rows ≤ the reject limit
	// → Job Completed (job_status=2), valid rows landed.
	Scenario98ExpectErrorTolerated = "error-tolerated"
	// Scenario98ExpectErrorFailed — FE.12b: malformed rows > the reject limit →
	// Job Failed (job_status=3), errors_total incremented.
	Scenario98ExpectErrorFailed = "error-failed"
)

// Scenario 98 DDL-knob substrings (byte-stable; mirror the builder output).
const (
	Scenario98FilterPushdownOpt = "FILTER_PUSHDOWN=true"
	Scenario98ProjectOpt        = "PROJECT=true"
)

// Scenario 98 source families (Source field).
const (
	Scenario98SourceObjectStore = "object-store"
	Scenario98SourceJDBC        = "jdbc"
	Scenario98SourceHive        = "hive"
)

// Scenario 98 servers — mirror the scenario98 sample CR (pushdown-test).
const (
	Scenario98ServerS3Datalake     = "s3-datalake"
	Scenario98ServerMinioWarehouse = "minio-warehouse"
	Scenario98ServerMySQLOLTP      = "mysql-oltp"
	Scenario98ServerPostgresSource = "postgres-source"
	Scenario98ServerHadoopCluster  = "hadoop-cluster"
)

// Scenario 98 well-known reject-limit thresholds for the malformed-row source.
// The malformed CSV ships K valid rows + Scenario98MalformedBadRows bad rows.
// FE.12a uses a limit ABOVE the bad-row count (tolerated); FE.12b uses a limit
// BELOW it (failed).
const (
	// Scenario98MalformedBadRows is the number of malformed rows the
	// gen-pushdown-samples.sh generator writes into the error source file.
	Scenario98MalformedBadRows = 5
	// Scenario98RejectLimitTolerated is set ABOVE the bad-row count → FE.12a
	// tolerates the malformed rows and the Job Completes.
	Scenario98RejectLimitTolerated int32 = 10
	// Scenario98RejectLimitFail is set BELOW the bad-row count → FE.12b breaches
	// the limit and the Job Fails.
	Scenario98RejectLimitFail int32 = 2
)

// Scenario98Cases returns the full Scenario 98 catalog (FE.1-5 + FE.12a/b) from
// the task-breakdown §4. The rows are honest against the shipped production
// contract (buildPXFLocation FILTER_PUSHDOWN/PROJECT emission, errorHandlingClause
// SEGMENT REJECT LIMIT, and the mutating defaulter): the Scenario98 CatalogHonest
// functional/e2e Part A tests resolve each row against the REAL built DDL.
//
// HONESTY: every Description names the REAL signal asserted and marks
// [CONFIG-ONLY]/[EXPLAIN-ONLY] where there is no live backing/synthesizable data.
// bytes_transferred is NEVER asserted (it stays PLANNED — PXF has no honest
// external-bytes counter).
func Scenario98Cases() []Scenario98Case {
	return []Scenario98Case{
		// --- FE.1-3 FILTER PUSHDOWN -----------------------------------------
		{
			ID: "FE.1", Layer: "builder/DDL+live-rowcount+live-explain",
			Profile: "s3:parquet", Source: Scenario98SourceObjectStore,
			Server: Scenario98ServerS3Datalake, Knob: "filterPushdown",
			Expected: Scenario98ExpectFilterPushdown, DDLContains: Scenario98FilterPushdownOpt,
			Description: "filter pushdown on s3:parquet (object store): LOCATION carries " +
				"FILTER_PUSHDOWN=true; HONEST signal = cloudberry_data_loading_rows_total " +
				"filtered < unfiltered baseline + EXPLAIN shows the pushed filter. ORC leg " +
				"is [CONFIG-ONLY] if ORC not synthesizable. NO bytes_transferred asserted.",
		},
		{
			ID: "FE.2", Layer: "builder/DDL+live-rowcount+source-log+live-explain",
			Profile: "jdbc", Source: Scenario98SourceJDBC,
			Server: Scenario98ServerPostgresSource, Knob: "filterPushdown",
			Expected: Scenario98ExpectFilterPushdown, DDLContains: Scenario98FilterPushdownOpt,
			Description: "filter pushdown on jdbc (mysql-oltp/postgres-source, table " +
				"jdbc_test_data, filter column 'category'): LOCATION carries " +
				"FILTER_PUSHDOWN=true; HONEST signal = SOURCE-SIDE QUERY LOG shows the WHERE " +
				"predicate (STRONGEST for JDBC) + rows_total filtered < baseline. " +
				"NO bytes_transferred asserted.",
		},
		{
			ID: "FE.3", Layer: "builder/DDL+live-rowcount+source-log+live-explain",
			Profile: "hive", Source: Scenario98SourceHive,
			Server: Scenario98ServerHadoopCluster, Knob: "filterPushdown",
			Expected: Scenario98ExpectFilterPushdown, DDLContains: Scenario98FilterPushdownOpt,
			Description: "[CONFIG-ONLY] filter pushdown on hive (warehouse.fact_sales): " +
				"LOCATION carries FILTER_PUSHDOWN=true; HONEST signal = Hive/HS2 query log " +
				"predicate + partition prune + rows_total filtered < baseline. CONFIG-ONLY " +
				"when no live Hive backing. NO bytes_transferred asserted.",
		},

		// --- FE.4-5 COLUMN PROJECTION ---------------------------------------
		{
			ID: "FE.4", Layer: "builder/DDL+live-explain",
			Profile: "s3:parquet", Source: Scenario98SourceObjectStore,
			Server: Scenario98ServerS3Datalake, Knob: "columnProjection",
			Expected: Scenario98ExpectColumnProjection, DDLContains: Scenario98ProjectOpt,
			Description: "[EXPLAIN-ONLY] column projection on WIDE s3:parquet: LOCATION " +
				"carries PROJECT=true; HONEST signal = EXPLAIN shows ONLY the projected " +
				"columns in the external-scan target list (vs SELECT *). No honest byte " +
				"meter ⇒ EXPLAIN-ONLY. NO bytes_transferred asserted.",
		},
		{
			ID: "FE.5", Layer: "builder/DDL+live-explain",
			Profile: "s3:orc", Source: Scenario98SourceObjectStore,
			Server: Scenario98ServerS3Datalake, Knob: "columnProjection",
			Expected: Scenario98ExpectColumnProjection, DDLContains: Scenario98ProjectOpt,
			Description: "[CONFIG-ONLY] column projection on WIDE s3:orc: LOCATION carries " +
				"PROJECT=true; EXPLAIN projected-columns assertion where ORC synthesizable, " +
				"else DDL+PROJECT correctness only (CONFIG-ONLY). NO bytes_transferred asserted.",
		},

		// --- FE.12a/b PER-ROW ERROR HANDLING --------------------------------
		{
			ID: "FE.12a", Layer: "builder/DDL+live-status",
			Profile: "s3:text", Source: Scenario98SourceObjectStore,
			Server: Scenario98ServerS3Datalake, Knob: "errorHandling",
			Expected:    Scenario98ExpectErrorTolerated,
			DDLContains: "SEGMENT REJECT LIMIT 10 ROWS",
			Description: "per-row error tolerance WITHIN limit: DDL carries LOG ERRORS " +
				"SEGMENT REJECT LIMIT 10 ROWS; the malformed source has 5 bad rows ≤ 10 " +
				"→ Job Completed. HONEST signal = cloudberry_data_loading_job_status=2 + " +
				"rows_total = VALID rows only (bad rows excluded). Fully operator-observable.",
		},
		{
			ID: "FE.12b", Layer: "builder/DDL+live-status",
			Profile: "s3:text", Source: Scenario98SourceObjectStore,
			Server: Scenario98ServerS3Datalake, Knob: "errorHandling",
			Expected:    Scenario98ExpectErrorFailed,
			DDLContains: "SEGMENT REJECT LIMIT 2 ROWS",
			Description: "per-row error tolerance OVER limit: same malformed source (5 bad " +
				"rows) with SEGMENT REJECT LIMIT 2 ROWS (< 5) → Job Failed at limit breach. " +
				"HONEST signal = cloudberry_data_loading_job_status=3 + " +
				"cloudberry_data_loading_errors_total incremented. Fully operator-observable.",
		},
	}
}

// ============================================================================
// Scenario 99 — Writable External Tables / Data Export
// ============================================================================
//
// NUMBERING NOTE: this is Scenario 99, the WRITABLE-EXPORT verification scenario.
// It EXTENDS the writable-external-table machinery that already ships
// (pxfpolicy.IsProfileWritable + builder.buildPXFExternalTableDDL writable branch
// + the reversed export INSERT) to HDFS + JDBC export targets, and adds the
// optional SourceFilter (a writable-only WHERE predicate, admission rule W.17).
//
// The PRODUCTION CONTRACT this catalog resolves against (verified + unit-tested):
//
//   - Writable export DDL: mode==writable → CREATE WRITABLE EXTERNAL TABLE ...
//     FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export'), NO LOG ERRORS / SEGMENT
//     REJECT LIMIT (writable tables take no reject limit). Profile-agnostic:
//     s3:text/parquet/avro, hdfs:text/parquet/avro/sequencefile and bare jdbc are
//     writable (IsProfileWritable); json/orc/rc/hive*/hbase are rejected (W.10b).
//   - Export script: the REVERSED INSERT `INSERT INTO <writable_ext> SELECT * FROM
//     <target>`; with PxfJob.SourceFilter set (writable only) the export INSERT
//     becomes `... SELECT * FROM <target> WHERE <sourceFilter>` (the rowcount
//     capture / DATALOAD_ROWS marker is preserved).
//   - SourceFilter admission (W.17): (a) sourceFilter on a NON-writable job → DENY
//     (the message names "sourceFilter" and "writable"); (b) sourceFilter
//     containing ';','--' or '/*' → DENY ("statement terminators or SQL comments").
//
// HONESTY: every Description names the REAL signal asserted and marks
// [CONFIG-ONLY] where a format needs DATA_SCHEMA / has no live backing (e.g.
// hdfs:parquet export). The ALWAYS-asserted DDL signal is
// FORMATTER='pxfwritable_export'. bytes_transferred is NEVER asserted (it stays
// PLANNED — PXF has no honest external-bytes counter); the live "data lands"
// proofs are object/file landing (S3/HDFS) and ROW landing (JDBC count(*)).
//
// Scenario99Case mirrors the Scenario98Case SHAPE (a small, flat row catalog
// resolved against the REAL built artifact).
type Scenario99Case struct {
	// ID is the catalog rule id (e.g. "FE.9", "WE.1", "SF.1", "SF.2").
	ID string
	// Layer describes the assertion layers, joined with "+":
	//   "builder/DDL"   — assert the generated WRITABLE export DDL/script.
	//   "live-object"   — exported objects land in S3 (MinIO list/HEAD).
	//   "live-file"     — exported files land in HDFS (WebHDFS LISTSTATUS).
	//   "live-rowcount" — exported rows land in the JDBC target (count(*)>0).
	//   "webhook"       — admission DENY (W.17) / admit.
	Layer string
	// Profile is the PXF profile under test (e.g. s3:text, hdfs:text, jdbc).
	Profile string
	// Target names the export target family ("object-store", "hdfs", "jdbc").
	Target string
	// Server is the PXF server the export job targets.
	Server string
	// Mode is the job mode: "writable" for an export, "" / "insert" for a
	// read/import (the SF.2 deny case).
	Mode string
	// SourceFilter is the optional writable-export WHERE predicate body (SF.*).
	SourceFilter string
	// Expected is the outcome token (see the Scenario99Expect* constants).
	Expected string
	// DDLContains is the byte-stable substring the built artifact must carry for
	// the row (e.g. "FORMATTER='pxfwritable_export'", "CREATE WRITABLE EXTERNAL
	// TABLE", a WHERE fragment) — empty for a pure webhook-deny row.
	DDLContains string
	// Description explains the case, names the HONEST signal asserted, and marks
	// [CONFIG-ONLY] where the row has no live backing. bytes_transferred is NEVER
	// named as a signal.
	Description string
}

// Scenario 99 expected-outcome tokens.
const (
	// Scenario99ExpectExportS3 — a writable export to an S3/object-store target:
	// WRITABLE DDL with FORMATTER='pxfwritable_export'; live → objects land in
	// MinIO under the export prefix.
	Scenario99ExpectExportS3 = "export-s3"
	// Scenario99ExpectExportHDFS — a writable export to an HDFS target: WRITABLE
	// DDL; live → files land in HDFS (WebHDFS LISTSTATUS).
	Scenario99ExpectExportHDFS = "export-hdfs"
	// Scenario99ExpectExportJDBC — a writable export to a JDBC target: WRITABLE
	// DDL; live → rows land in the pgsource export_target (count(*)>0).
	Scenario99ExpectExportJDBC = "export-jdbc"
	// Scenario99ExpectFormat — WE.2: the LANDED artifact matches the expected
	// FORMAT and the DDL carries FORMATTER='pxfwritable_export' (cross-cuts FE.*).
	Scenario99ExpectFormat = "export-format"
	// Scenario99ExpectFilteredExport — SF.1: a writable export with SourceFilter
	// set → the export SCRIPT carries `WHERE <sourceFilter>`; live → FEWER rows
	// land than the unfiltered baseline.
	Scenario99ExpectFilteredExport = "filtered-export"
	// Scenario99ExpectDenySourceFilter — SF.2: sourceFilter on a non-writable
	// job (or a dangerous predicate) → admission DENY (W.17).
	Scenario99ExpectDenySourceFilter = "deny-source-filter"
)

// Scenario 99 DDL substrings (byte-stable; mirror the builder output).
const (
	// Scenario99WritableFormatter is the writable-export FORMATTER clause every
	// writable export DDL must carry (the ALWAYS-asserted WE.2 signal).
	Scenario99WritableFormatter = "FORMATTER='pxfwritable_export'"
	// Scenario99WritableDDL is the writable-export table DDL prefix.
	Scenario99WritableDDL = "CREATE WRITABLE EXTERNAL TABLE"
)

// Scenario 99 export-target families (Target field).
const (
	Scenario99TargetObjectStore = "object-store"
	Scenario99TargetHDFS        = "hdfs"
	Scenario99TargetJDBC        = "jdbc"
)

// Scenario 99 servers — mirror the scenario99 sample CR (export-test).
const (
	Scenario99ServerS3Datalake     = "s3-datalake"
	Scenario99ServerMinioWarehouse = "minio-warehouse"
	Scenario99ServerHadoopCluster  = "hadoop-cluster"
	Scenario99ServerPostgresSource = "postgres-source"
	Scenario99ServerMySQLOLTP      = "mysql-oltp"
)

// Scenario 99 well-known SourceFilter predicate (SF.1) + the dangerous SF.2b
// predicate that the W.17 sanity check must reject.
const (
	// Scenario99SourceFilter selects a strict subset of the export source so the
	// filtered export lands FEWER rows than the unfiltered baseline.
	Scenario99SourceFilter = "region='us-east'"
	// Scenario99WhereFragment is the byte-stable WHERE fragment the filtered
	// export SCRIPT must carry (SF.1).
	Scenario99WhereFragment = "WHERE region='us-east'"
	// Scenario99DangerousFilter contains a statement terminator (';') the W.17
	// sanity check rejects (SF.2b).
	Scenario99DangerousFilter = "1=1; DROP TABLE x"
)

// Scenario 99 export target/resource paths (mirror the sample CR + gen script).
const (
	// Scenario99S3ExportResource is the MinIO export prefix (FE.9/WE.1).
	Scenario99S3ExportResource = "cloudberry-warehouse/exports/s3/"
	// Scenario99HDFSExportResource is the HDFS export prefix (FE.10).
	Scenario99HDFSExportResource = "/data-lake/exports/hdfs/"
	// Scenario99JDBCExportResource is the pgsource writable target table (FE.11).
	Scenario99JDBCExportResource = "export_target"
)

// Scenario99Cases returns the full Scenario 99 catalog from the task-breakdown
// §3 (FE.9/WE.1, FE.10, FE.11, WE.2, SF.1, SF.2/SF.2b). The rows are honest
// against the shipped writable-export contract: every writable row asserts
// FORMATTER='pxfwritable_export' (the WE.2 gate) and the reversed INSERT; SF.1
// adds the WHERE script delta; SF.2 is a webhook-deny row.
//
// HONESTY: every Description names the REAL signal asserted and marks
// [CONFIG-ONLY] where there is no live backing / a format needs DATA_SCHEMA.
// bytes_transferred is NEVER asserted (it stays PLANNED).
func Scenario99Cases() []Scenario99Case {
	return []Scenario99Case{
		// --- FE.9 / WE.1 — S3 WRITABLE EXPORT -------------------------------
		{
			ID: "FE.9", Layer: "webhook+builder/DDL+live-object",
			Profile: "s3:text", Target: Scenario99TargetObjectStore,
			Server: Scenario99ServerMinioWarehouse, Mode: "writable",
			Expected: Scenario99ExpectExportS3, DDLContains: Scenario99WritableFormatter,
			Description: "S3 writable export (== WE.1) on s3:text: WRITABLE DDL carries " +
				"FORMATTER='pxfwritable_export', NO LOG ERRORS; the export INSERT is reversed " +
				"(INSERT INTO <ext> SELECT * FROM <target>). HONEST signal = objects LAND in " +
				"MinIO under cloudberry-warehouse/exports/s3/ (S3 list/HEAD). The s3:parquet " +
				"companion is [CONFIG-ONLY] when parquet write tooling absent. " +
				"NO bytes_transferred asserted.",
		},
		// --- FE.10 — HDFS WRITABLE EXPORT -----------------------------------
		{
			ID: "FE.10", Layer: "webhook+builder/DDL+live-file",
			Profile: "hdfs:text", Target: Scenario99TargetHDFS,
			Server: Scenario99ServerHadoopCluster, Mode: "writable",
			Expected: Scenario99ExpectExportHDFS, DDLContains: Scenario99WritableFormatter,
			Description: "HDFS writable export on hdfs:text: WRITABLE DDL carries " +
				"FORMATTER='pxfwritable_export'; reversed export INSERT. HONEST signal = part " +
				"files LAND in HDFS under /data-lake/exports/hdfs/ (WebHDFS LISTSTATUS). " +
				"hdfs:parquet/avro export is [CONFIG-ONLY] (needs DATA_SCHEMA) — prefer " +
				"hdfs:text for the deterministic live landing. NO bytes_transferred asserted.",
		},
		// --- FE.11 — JDBC WRITABLE EXPORT (strongest, deterministic) --------
		{
			ID: "FE.11", Layer: "webhook+builder/DDL+live-rowcount",
			Profile: "jdbc", Target: Scenario99TargetJDBC,
			Server: Scenario99ServerPostgresSource, Mode: "writable",
			Expected: Scenario99ExpectExportJDBC, DDLContains: Scenario99WritableFormatter,
			Description: "JDBC writable export (bare jdbc → writable via bareWritableProfiles): " +
				"WRITABLE DDL carries FORMATTER='pxfwritable_export'; reversed export INSERT. " +
				"HONEST signal (STRONGEST) = rows LAND in pgsource sourcedb.export_target " +
				"(SELECT count(*) > 0, == the exported source rows). Target table pre-created " +
				"+ granted by gen-export-targets.sh. NO bytes_transferred asserted.",
		},
		// --- WE.2 — DATA LANDS WITH CORRECT FORMAT (cross-cuts FE.9/10/11) --
		{
			ID: "WE.2", Layer: "builder/DDL+live-object+live-file+live-rowcount",
			Profile: "s3:text", Target: Scenario99TargetObjectStore,
			Server: Scenario99ServerMinioWarehouse, Mode: "writable",
			Expected: Scenario99ExpectFormat, DDLContains: Scenario99WritableFormatter,
			Description: "data lands with CORRECT FORMAT: for each export the generated " +
				"WRITABLE DDL carries FORMATTER='pxfwritable_export' AND the correct format per " +
				"profile (s3:text/hdfs:text → text/CSV-shaped; jdbc → rows with expected " +
				"columns). This is the explicit WE.2 'correct format' gate. parquet/avro " +
				"format-landing is [CONFIG-ONLY] without write tooling/DATA_SCHEMA. " +
				"NO bytes_transferred asserted.",
		},
		// --- SF.1 — SOURCEFILTER FILTERED EXPORT ----------------------------
		{
			ID: "SF.1", Layer: "builder/DDL+live-rowcount",
			Profile: "s3:text", Target: Scenario99TargetObjectStore,
			Server: Scenario99ServerMinioWarehouse, Mode: "writable",
			SourceFilter: Scenario99SourceFilter,
			Expected:     Scenario99ExpectFilteredExport, DDLContains: Scenario99WhereFragment,
			Description: "filtered writable export: with sourceFilter=\"region='us-east'\" the " +
				"export SCRIPT emits `INSERT INTO <ext> SELECT * FROM <target> WHERE " +
				"region='us-east'` (the WHERE is the ONLY script delta vs the baseline; the " +
				"WRITABLE DDL is unchanged). HONEST signal = the filtered export lands FEWER " +
				"rows than the unfiltered baseline (JDBC count(*) deterministic). " +
				"NO bytes_transferred asserted.",
		},
		// --- SF.2 — SOURCEFILTER ON A READ JOB → WEBHOOK DENY (W.17) --------
		{
			ID: "SF.2", Layer: "webhook",
			Profile: "s3:text", Target: Scenario99TargetObjectStore,
			Server: Scenario99ServerMinioWarehouse, Mode: "",
			SourceFilter: Scenario99SourceFilter,
			Expected:     Scenario99ExpectDenySourceFilter, DDLContains: "",
			Description: "sourceFilter on a READ/import job (mode unset, not writable) → " +
				"admission DENY (W.17(a) mode gate): the error names 'sourceFilter' and " +
				"'writable'. Companion SF.2b: a writable job whose sourceFilter contains ';' " +
				"(\"1=1; DROP TABLE x\") → DENY (W.17(b): 'statement terminators or SQL " +
				"comments'). Decision recorded: REJECT (not silently ignore). Pure webhook " +
				"row — no DDL signal.",
		},
	}
}

// ============================================================================
// Scenario 101 — gpfdist Deployment + Job 4 (gpload-csv)
// ============================================================================
//
// Scenario101Case mirrors the Scenario99Case SHAPE: a small, flat row catalog
// resolved against the REAL built artifact (gpfdist Deployment/Service/PVC,
// gpload control file / ConfigMap / Job-CronJob) or the validator (W.18-W.22).
//
// Source: task-breakdown §10 (the SC101-* catalog). Layers are one of
// "builder", "webhook", "reconcile", "live"; rows whose ONLY honest signal is a
// running cluster carry Layer "live" (marked live-only). The catalog is honest:
// every builder/webhook row is resolved against the shipped artifact in the
// functional + e2e Part A suites; live rows are exercised at e2e Part B.
//
// METRIC HONESTY: cloudberry_gpfdist_* stay PLANNED and are NEVER asserted.
// gpload reuses cloudberry_data_loading_* (job_status/rows_total/errors_total)
// from real Job state; gpfdist Deployment readiness is observed via kubectl
// (kube-state-metrics is absent in the test env), NOT a VM metric.
type Scenario101Case struct {
	// ID is the catalog rule id (e.g. "SC101-GP2-NAME", "SC101-GL1-VERSION").
	ID string
	// Req is the spec requirement id family the row proves (e.g. "GP.2",
	// "GL.1", "J.25", "W.18").
	Req string
	// Layer is the assertion layer: "builder" (pure, byte-provable),
	// "webhook" (admission accept/deny), "reconcile" (envtest), or "live"
	// (requires a running cluster — live-only).
	Layer string
	// Artifact names the built object family the row asserts against:
	// "deployment", "service", "pvc", "control-file", "configmap", "job",
	// "cronjob", or "webhook".
	Artifact string
	// Expected is a short outcome token / human description of the asserted
	// outcome (e.g. "name <cluster>-gpfdist", "MODE UPDATE", "DENY").
	Expected string
	// Contains is the byte-stable substring the built artifact must carry for
	// a builder/control-file row (e.g. "VERSION: 1.0.0.1") — empty for rows
	// asserted structurally (names/ports) or for webhook/live rows.
	Contains string
	// Description explains the case + names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY]; config-only rows [CONFIG-ONLY].
	Description string
}

// Scenario 101 layer tokens.
const (
	Scenario101LayerBuilder   = "builder"
	Scenario101LayerWebhook   = "webhook"
	Scenario101LayerReconcile = "reconcile"
	Scenario101LayerLive      = "live"
)

// Scenario 101 artifact tokens.
const (
	Scenario101ArtifactDeployment  = "deployment"
	Scenario101ArtifactService     = "service"
	Scenario101ArtifactPVC         = "pvc"
	Scenario101ArtifactControlFile = "control-file"
	Scenario101ArtifactConfigMap   = "configmap"
	Scenario101ArtifactJob         = "job"
	Scenario101ArtifactCronJob     = "cronjob"
	Scenario101ArtifactWebhook     = "webhook"
)

// Scenario 101 control-file byte-stable substrings (mirror the builder output;
// see internal/builder/gpload_builder_test.go golden block). These are the GL.*
// lines the BuildGploadControlFile output must carry for the gpload-csv fixture.
const (
	Scenario101GLVersion   = "VERSION: 1.0.0.1"
	Scenario101GLDatabase  = "DATABASE: postgres"
	Scenario101GLUser      = "USER: gpadmin"
	Scenario101GLFormat    = "- FORMAT: csv"
	Scenario101GLDelimiter = "- DELIMITER: ','"
	Scenario101GLHeader    = "- HEADER: true"
	Scenario101GLEncoding  = "- ENCODING: UTF-8"
	Scenario101GLErrLimit  = "- ERROR_LIMIT: 50"
	Scenario101GLLogErrors = "- LOG_ERRORS: true"
	Scenario101GLTable     = "- TABLE: public.raw_data"
	Scenario101GLModeIns   = "- MODE: INSERT"
	Scenario101GLTruncate  = "- TRUNCATE: true"
	Scenario101GLAfter     = "- AFTER: \"ANALYZE public.raw_data\""
)

// Scenario 101 well-known names + values (mirror the sample CR + spec §11).
const (
	// Scenario101GpfdistImage is the gpfdist Deployment image.
	Scenario101GpfdistImage = "cloudberry-gpfdist:2.1.0"
	// Scenario101GpfdistPort is the default gpfdist port.
	Scenario101GpfdistPort = 8080
	// Scenario101JobName is the gpload-csv job name (== ConfigMap key prefix).
	Scenario101JobName = "gpload-csv"
	// Scenario101Schedule is the gpload-csv CronJob schedule (J.25).
	Scenario101Schedule = "*/30 * * * *"
	// Scenario101TargetTable is the gpload target table (GL.5).
	Scenario101TargetTable = "public.raw_data"
	// Scenario101FileGlob is the gpfdist source glob (GL.2 / J.26).
	Scenario101FileGlob = "/incoming/*.csv"
	// Scenario101TargetDDL creates the target table the live load fills.
	Scenario101TargetDDL = "CREATE TABLE IF NOT EXISTS public.raw_data " +
		"(id int, event_type text, payload jsonb, created_at timestamptz)"
)

// Scenario101Cases returns the full Scenario 101 catalog (task-breakdown §10):
// the gpfdist Deployment/Service/PVC rows (GP.*), the gpload control-file +
// Job/CronJob rows (GL.*, J.*) and the webhook rows (W.18-W.22). Builder/webhook
// rows are resolved against the shipped artifact; rows whose only honest signal
// is a running cluster carry Layer "live" (marked [LIVE-ONLY]).
//
// HONESTY: cloudberry_gpfdist_* are NEVER asserted (PLANNED). gpload reuses
// cloudberry_data_loading_* from real Job state.
func Scenario101Cases() []Scenario101Case {
	return []Scenario101Case{
		// --- gpfdist Deployment / Service / PVC (GP.*) ----------------------
		{
			ID: "SC101-GP2-NAME", Req: "GP.2", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "name <cluster>-gpfdist",
			Description: "BuildGpfdistDeployment metadata.name == \"<cluster>-gpfdist\".",
		},
		{
			ID: "SC101-GP2-REPLICAS", Req: "GP.2", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "replicas honor gpfdist.replicas (J/C.20)",
			Description: "spec.replicas == *Gpfdist.Replicas; default 1 (RWO-safe) when nil.",
		},
		{
			ID: "SC101-GP2-IMAGE", Req: "GP.2", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: Scenario101GpfdistImage,
			Description: "container[0].image == Gpfdist.Image or default cloudberry-gpfdist:2.1.0.",
		},
		{
			ID: "SC101-GP2-CMD", Req: "GP.2", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "command gpfdist, args -d /data -p 8080 -l /var/log/gpfdist.log",
			Description: "container command==[gpfdist], args==[-d /data -p <port> -l /var/log/gpfdist.log].",
		},
		{
			ID: "SC101-GP3-PORT", Req: "GP.3", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "named port gpfdist 8080",
			Description: "container port name gpfdist, containerPort == 8080.",
		},
		{
			ID: "SC101-GP4-PVCNAME", Req: "GP.4", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactPVC, Expected: "name <cluster>-gpfdist-data-pvc",
			Description: "BuildGpfdistPVC name <cluster>-gpfdist-data-pvc; Deployment volume claimName matches.",
		},
		{
			ID: "SC101-GP4-MOUNT", Req: "GP.4", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "volumeMount data -> /data",
			Description: "volumeMount name data mountPath /data.",
		},
		{
			ID: "SC101-GP5-SVCNAME", Req: "GP.5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactService, Expected: "name <cluster>-gpfdist-svc",
			Description: "BuildGpfdistService metadata.name == \"<cluster>-gpfdist-svc\".",
		},
		{
			ID: "SC101-GP5-SELECTOR", Req: "GP.5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactService, Expected: "selector avsoft.io/component=gpfdist == pod labels",
			Description: "Service selector avsoft.io/component==gpfdist and EQUALS Deployment pod labels.",
		},
		{
			ID: "SC101-GP5-PORT", Req: "GP.5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactService, Expected: "port 8080 targetPort 8080",
			Description: "Service port 8080 targetPort 8080.",
		},
		{
			ID: "SC101-GP-OWNERREF", Req: "GP.2-5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactDeployment, Expected: "ownerRef to cluster",
			Description: "every gpfdist object carries a controller ownerRef to the cluster.",
		},
		{
			ID: "SC101-GP-LIVE-READY", Req: "GP.2-5", Layer: Scenario101LayerLive,
			Artifact: Scenario101ArtifactDeployment, Expected: "Deployment Ready; Service resolves; PVC Bound",
			Description: "[LIVE-ONLY] Deployment <cluster>-gpfdist reaches Ready (readyReplicas); " +
				"Service <cluster>-gpfdist-svc resolves; PVC Bound + mounted /data. " +
				"Readiness via kubectl (kube-state-metrics absent) — NO VM metric.",
		},

		// --- gpload control-file + Job/CronJob (GL.*, J.*) ------------------
		{
			ID: "SC101-J25-CRONJOB", Req: "J.25", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactCronJob, Expected: "schedule -> CronJob; no schedule -> Job",
			Contains: Scenario101Schedule,
			Description: "gpload job WITH schedule -> BuildGploadCronJob spec.schedule==\"*/30 * * * *\"; " +
				"WITHOUT schedule -> nil CronJob (one-off Job).",
		},
		{
			ID: "SC101-J25-ROUTE", Req: "J.25", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactJob, Expected: "control-file path, not native DDL",
			Contains:    "gpload -f /etc/gpload/" + Scenario101JobName + ".yml",
			Description: "gpload type routes to the control-file path (ConfigMap + gpload -f), NOT native DDL.",
		},
		{
			ID: "SC101-GL1-VERSION", Req: "GL.1", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "VERSION line",
			Contains:    Scenario101GLVersion,
			Description: "control file carries VERSION: 1.0.0.1.",
		},
		{
			ID: "SC101-GL1-DB", Req: "GL.1", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "DATABASE line",
			Contains:    Scenario101GLDatabase,
			Description: "control file carries DATABASE: postgres.",
		},
		{
			ID: "SC101-GL1-USER", Req: "GL.1", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "USER line",
			Contains:    Scenario101GLUser,
			Description: "control file carries USER: gpadmin.",
		},
		{
			ID: "SC101-GL1-HOST", Req: "GL.1", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "HOST <cluster>-coord-hl",
			Contains:    "HOST: ",
			Description: "control file carries HOST: <cluster>-coord-hl (CoordinatorServiceName).",
		},
		{
			ID: "SC101-GL1-PORT", Req: "GL.1", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "PORT 5432",
			Contains:    "PORT: 5432",
			Description: "control file carries PORT: 5432.",
		},
		{
			ID: "SC101-GL2-GPFDIST", Req: "GL.2", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "gpfdist SOURCE LOCAL_HOSTNAME/PORT + local FILE",
			// Cluster-independent token (the FILE list now carries the LOCAL path,
			// not a gpfdist:// URL) so every consumer/cluster name resolves it.
			Contains:    "        FILE:\n          - /incoming/*.csv\n",
			Description: "gpfdist SOURCE emits LOCAL_HOSTNAME <cluster>-gpfdist-svc + PORT 8080 + local FILE /incoming/*.csv (J.26), no gpfdist:// URL.",
		},
		{
			ID: "SC101-J27-LOCAL", Req: "J.27", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "local verbatim path (no LOCAL_HOSTNAME/PORT, no gpfdist:// prefix)",
			Description: "inputSource.type=local -> FILE contains the verbatim local path (no LOCAL_HOSTNAME/PORT, no gpfdist://).",
		},
		{
			ID: "SC101-J28-HOST", Req: "J.28", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "custom host in LOCAL_HOSTNAME",
			Description: "custom inputSource.host -> SOURCE LOCAL_HOSTNAME: <host> (PORT 8080).",
		},
		{
			ID: "SC101-J29-PORT", Req: "J.29", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "custom port in PORT",
			Description: "custom inputSource.port -> SOURCE PORT: <port>.",
		},
		{
			ID: "SC101-GL3-FORMAT", Req: "GL.3", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "FORMAT line",
			Contains:    Scenario101GLFormat,
			Description: "control file carries - FORMAT: csv (J.30).",
		},
		{
			ID: "SC101-GL3-DELIM", Req: "GL.3", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "DELIMITER line",
			Contains:    Scenario101GLDelimiter,
			Description: "control file carries - DELIMITER: ',' (J.31).",
		},
		{
			ID: "SC101-GL3-HEADER", Req: "GL.3", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "HEADER line",
			Contains:    Scenario101GLHeader,
			Description: "control file carries - HEADER: true when Header==true (J.32).",
		},
		{
			ID: "SC101-GL3-ENC", Req: "GL.3", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "ENCODING line",
			Contains:    Scenario101GLEncoding,
			Description: "control file carries - ENCODING: UTF-8 (J.33).",
		},
		{
			ID: "SC101-GL4-LIMIT", Req: "GL.4", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "ERROR_LIMIT line",
			Contains:    Scenario101GLErrLimit,
			Description: "control file carries - ERROR_LIMIT: 50 from errorHandling.segmentRejectLimit (J.38).",
		},
		{
			ID: "SC101-GL4-LOGERR", Req: "GL.4", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "LOG_ERRORS line",
			Contains:    Scenario101GLLogErrors,
			Description: "control file carries - LOG_ERRORS: true from errorHandling.logErrors (J.38).",
		},
		{
			ID: "SC101-GL5-TABLE", Req: "GL.5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "TABLE line",
			Contains:    Scenario101GLTable,
			Description: "control file carries - TABLE: public.raw_data (J.34).",
		},
		{
			ID: "SC101-GL5-MODE-INS", Req: "GL.5", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "MODE INSERT",
			Contains:    Scenario101GLModeIns,
			Description: "control file carries - MODE: INSERT (J.35).",
		},
		{
			ID: "SC101-J36-UPDATE", Req: "J.36", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "MODE UPDATE + MATCH_COLUMNS",
			Description: "mode=update -> - MODE: UPDATE + - MATCH_COLUMNS present.",
		},
		{
			ID: "SC101-J37-MERGE", Req: "J.37", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "MODE MERGE + MATCH_COLUMNS",
			Description: "mode=merge -> - MODE: MERGE + - MATCH_COLUMNS present.",
		},
		{
			ID: "SC101-GL6-TRUNCATE", Req: "GL.6", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "PRELOAD TRUNCATE",
			Contains:    Scenario101GLTruncate,
			Description: "control file carries PRELOAD - TRUNCATE: true from preload.truncate (J.39).",
		},
		{
			ID: "SC101-GL7-AFTER", Req: "GL.7", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "SQL AFTER",
			Contains:    Scenario101GLAfter,
			Description: "control file carries SQL - AFTER: \"ANALYZE public.raw_data\" from postActions (J.40).",
		},
		{
			ID: "SC101-J-CM", Req: "J.25", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactConfigMap, Expected: "CM <cluster>-gpload-<job> data <job>.yml",
			Description: "BuildGploadControlFileConfigMap -> CM <cluster>-gpload-<job> data key <job>.yml == control file.",
		},
		{
			ID: "SC101-J-POD-ARGS", Req: "J.25", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactJob, Expected: "gpload -f /etc/gpload/<job>.yml; CM mounted /etc/gpload",
			Contains:    "gpload -f /etc/gpload/" + Scenario101JobName + ".yml",
			Description: "gpload pod args run gpload -f /etc/gpload/<job>.yml; CM mounted at /etc/gpload.",
		},
		{
			ID: "SC101-J-STABLE", Req: "GL.1-7", Layer: Scenario101LayerBuilder,
			Artifact: Scenario101ArtifactControlFile, Expected: "byte-stable golden",
			Contains:    Scenario101GLVersion,
			Description: "the full control file is byte-stable (golden) for the fixed gpload-csv input.",
		},
		{
			ID: "SC101-J-LIVE-LOAD", Req: "all", Layer: Scenario101LayerLive,
			Artifact: Scenario101ArtifactJob, Expected: "count(*) FROM public.raw_data > 0",
			Description: "[LIVE-ONLY] gpfdist serves /data/incoming/*.csv; gpload -f loads; " +
				"SELECT count(*) FROM public.raw_data > 0 (== the CSV data rows). " +
				"cloudberry_data_loading_job_status=2 success.",
		},
		{
			ID: "SC101-J-LIVE-TRUNCATE", Req: "GL.6", Layer: Scenario101LayerLive,
			Artifact: Scenario101ArtifactJob, Expected: "re-run count stable (not doubled)",
			Description: "[LIVE-ONLY] re-run truncates then reloads -> row count stable (PRELOAD TRUNCATE).",
		},

		// --- Webhook (W.18-W.22) -------------------------------------------
		{
			ID: "SC101-W18-BAD", Req: "W.18", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY bad inputSource.type",
			Description: "reject inputSource.type other than gpfdist|local; accept gpfdist/local.",
		},
		{
			ID: "SC101-W19-BAD", Req: "W.19", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY multi-char delimiter",
			Description: "reject a multi-char delimiter; accept a single character.",
		},
		{
			ID: "SC101-W20-BAD", Req: "W.20", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY update/merge without matchColumns",
			Description: "reject mode=update/merge with empty matchColumns; accept with matchColumns.",
		},
		{
			ID: "SC101-W21-BAD", Req: "W.21", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY unsafe postAction",
			Description: "reject a postActions entry containing ;/--//* (forbidden SQL fragment).",
		},
		{
			ID: "SC101-W22-BAD", Req: "W.22", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY host/port on type=local",
			Description: "reject inputSource.host/port set when type=local.",
		},
		{
			ID: "SC101-W16-RETAIN", Req: "W.16", Layer: Scenario101LayerWebhook,
			Artifact: Scenario101ArtifactWebhook, Expected: "DENY file:// in filePaths",
			Description: "regression: file:// in gploadJob.filePaths still rejected.",
		},
	}
}

// ============================================================================
// Scenario 102 — Job 5 kafka-cdc (Continuous Streaming, Custom Connector)
// ============================================================================
//
// Scenario102Case mirrors the Scenario101Case SHAPE: a small, flat row catalog
// resolved against the REAL built artifact (the pxf-connector-init init
// container, the kafka pxf:// DDL, the continuous dataload Job + CBK_* env) or
// the validator (W.23/W.24/W.23c). The catalog covers C.18 + J.41-J.46
// (task-breakdown §4).
//
// Layers are one of "builder", "webhook", "reconcile", "live". Rows whose ONLY
// honest signal is a running cluster carry Layer "live" (marked [LIVE-ONLY]);
// the kafka topic→table row landing needs a REAL Kafka→PXF connector JAR and is
// marked [CONFIG-ONLY] when only a placeholder JAR is staged.
//
// METRIC HONESTY: NO new metric. kafka-cdc reuses cloudberry_data_loading_*
// (a continuous consumer steady state is job_status=Running; rows_total is
// best-effort per flush). cloudberry_pxf_* / cloudberry_gpfdist_* stay PLANNED
// and are NEVER asserted.
type Scenario102Case struct {
	// ID is the catalog rule id (e.g. "SC102-C18-INIT-EXISTS").
	ID string
	// Req is the spec requirement id family the row proves (C.18 / J.41-J.46).
	Req string
	// Layer is the assertion layer: "builder" (pure, byte-provable),
	// "webhook" (admission accept/deny), "reconcile" (envtest), or "live"
	// (requires a running cluster — live-only).
	Layer string
	// Artifact names the built object family the row asserts against:
	// "init-container", "job", "cronjob", "container-env", "ddl", "volume",
	// or "webhook".
	Artifact string
	// Expected is a short outcome token / human description of the asserted
	// outcome (e.g. "CBK_CONTINUOUS=true", "DENY").
	Expected string
	// Contains is the byte-stable substring the built artifact must carry for
	// a builder row (e.g. the pxf:// DDL token) — empty for rows asserted
	// structurally or for webhook/live rows.
	Contains string
	// Description explains the case + names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY]; config-only rows [CONFIG-ONLY].
	Description string
}

// Scenario 102 layer tokens (alias the Scenario 101 layer values so the two
// catalogs share a vocabulary).
const (
	Scenario102LayerBuilder   = "builder"
	Scenario102LayerWebhook   = "webhook"
	Scenario102LayerReconcile = "reconcile"
	Scenario102LayerLive      = "live"
)

// Scenario 102 artifact tokens.
const (
	Scenario102ArtifactInitContainer = "init-container"
	Scenario102ArtifactJob           = "job"
	Scenario102ArtifactCronJob       = "cronjob"
	Scenario102ArtifactContainerEnv  = "container-env"
	Scenario102ArtifactDDL           = "ddl"
	Scenario102ArtifactVolume        = "volume"
	Scenario102ArtifactWebhook       = "webhook"
)

// Scenario 102 well-known names + values (mirror the scenario102-kafka-test
// sample CR + task-breakdown §6).
const (
	// Scenario102ClusterName is the live sample cluster name.
	Scenario102ClusterName = "kafka-test"
	// Scenario102Namespace is the deploy namespace.
	Scenario102Namespace = "cloudberry-test"
	// Scenario102JobName is the kafka-cdc job name (== dataload Job suffix).
	Scenario102JobName = "kafka-cdc"
	// Scenario102Topic is the kafka topic the kafka-cdc job consumes (the
	// pxfJob.resource).
	Scenario102Topic = "cloudberry-cdc"
	// Scenario102TargetTable is the kafka-cdc target table.
	Scenario102TargetTable = "public.kafka_events"
	// Scenario102ConnectorName is the custom-connector / custom-server name.
	Scenario102ConnectorName = "kafka-connector"
	// Scenario102ConnectorJarURL is the staged connector JAR (MinIO s3://).
	Scenario102ConnectorJarURL = "s3://cloudberry-data/connectors/kafka-connector.jar"
	// Scenario102ConnectorJarPath is the path the JAR lands at in the sidecar.
	Scenario102ConnectorJarPath = "/pxf/lib/custom/kafka-connector.jar"
	// Scenario102LibMountPath is the shared pxf-lib emptyDir mount path.
	Scenario102LibMountPath = "/pxf/lib/custom"
	// Scenario102BatchSize is the kafka-cdc streaming batch size (CBK_BATCH_SIZE).
	Scenario102BatchSize = 10000
	// Scenario102FlushInterval is the kafka-cdc flush interval (CBK_FLUSH_INTERVAL).
	Scenario102FlushInterval = "30s"
	// Scenario102KafkaPxfLocation is the byte-stable kafka pxf:// LOCATION token
	// the external-table DDL must carry (PROFILE=kafka&SERVER=kafka-connector).
	Scenario102KafkaPxfLocation = "pxf://cloudberry-cdc?PROFILE=kafka&SERVER=kafka-connector"
	// Scenario102TargetDDL creates the target table the live consume fills.
	Scenario102TargetDDL = "CREATE TABLE IF NOT EXISTS public.kafka_events " +
		"(id int, event_type text, payload jsonb, op text, ts timestamptz)"
)

// Scenario102Cases returns the full Scenario 102 catalog (task-breakdown §4):
// the connector-init rows (C.18), the custom-server/profile webhook rows
// (J.41/J.42 → W.23/W.24), the continuous-Job + CBK_* + kafka-DDL builder rows
// (J.43/J.44/J.45), and the Job-not-CronJob rows (J.46). Builder/webhook rows
// are resolved against the shipped artifact; rows whose only honest signal is a
// running cluster carry Layer "live" (marked [LIVE-ONLY] / [CONFIG-ONLY]).
//
// HONESTY: NO new metric. kafka-cdc reuses cloudberry_data_loading_* from real
// Job state; the end-to-end kafka→table row landing needs a REAL connector JAR
// and is [CONFIG-ONLY] when only a placeholder is staged.
func Scenario102Cases() []Scenario102Case {
	return []Scenario102Case{
		// --- C.18: connector JAR download init container --------------------
		{
			ID: "SC102-C18-INIT-EXISTS", Req: "C.18", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactInitContainer, Expected: "pxf-connector-init present",
			Description: "BuildPXFConnectorInitContainers yields a pxf-connector-init init " +
				"container when customConnectors non-empty + the PXF sidecar is enabled.",
		},
		{
			ID: "SC102-C18-INIT-MOUNT", Req: "C.18", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactInitContainer, Expected: "mount pxf-lib -> /pxf/lib/custom",
			Description: "pxf-connector-init mounts the shared pxf-lib emptyDir at /pxf/lib/custom " +
				"(visible to the sidecar).",
		},
		{
			ID: "SC102-C18-INIT-DOWNLOAD", Req: "C.18", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactInitContainer, Expected: "download into /pxf/lib/custom/<name>.jar",
			Contains: Scenario102ConnectorJarPath,
			Description: "the init script downloads the JAR (aws s3 cp for s3://, curl for http(s)://) " +
				"into /pxf/lib/custom/kafka-connector.jar with a non-empty assertion.",
		},
		{
			ID: "SC102-C18-JAR-PRESENT", Req: "C.18", Layer: Scenario102LayerLive,
			Artifact: Scenario102ArtifactVolume, Expected: "JAR exists + non-empty in the sidecar",
			Description: "[LIVE-ONLY] /pxf/lib/custom/kafka-connector.jar exists + is non-empty in the " +
				"segment-primary pxf sidecar (pxf-connector-init downloaded it). Provable with ANY " +
				"reachable jarUrl. KEY HEADLINE: the JAR is downloaded + mounted.",
		},

		// --- J.41: kafka-connector custom server from customConnectors ------
		{
			ID: "SC102-J41-SERVER-CUSTOM", Req: "J.41", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "server type=custom + connector ACCEPTED",
			Description: "a server kafka-connector type=custom WITH a matching customConnectors[] entry " +
				"is ACCEPTED (W.3 custom + W.24 satisfied).",
		},
		{
			ID: "SC102-J41-SERVER-NOCONN", Req: "J.41", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "DENY custom server without connector",
			Description: "a server type=custom WITHOUT a matching customConnectors[] entry is REJECTED " +
				"(W.24); the error names the server + connector name.",
		},
		{
			ID: "SC102-J41-CONN-JARURL", Req: "J.41", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactInitContainer, Expected: "jarUrl drives the download",
			Contains: Scenario102ConnectorJarURL,
			Description: "customConnectors[name=kafka-connector, jarUrl=s3://cloudberry-data/connectors/" +
				"kafka-connector.jar] drives the connector-init download command.",
		},

		// --- J.42: profile kafka accepted with connector / rejected without -
		{
			ID: "SC102-J42-PROFILE-OK", Req: "J.42", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "kafka profile on connector-backed server ACCEPTED",
			Description: "profile kafka on a connector-backed custom server is ACCEPTED (W.10 recognizes " +
				"the kafka scheme + W.23 passes).",
		},
		{
			ID: "SC102-J42-PROFILE-NOCONN", Req: "J.42", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "DENY kafka profile without a custom connector",
			Description: "profile kafka WITHOUT a custom connector / on a non-custom server is REJECTED " +
				"(W.23). Guards the \"no built-in streaming\" invariant.",
		},
		{
			ID: "SC102-J42-PROFILE-W10PURE", Req: "J.42", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "built-in W.10 allowlist unchanged",
			Description: "the built-in W.10 allowlist (isValidPxfProfile) stays false for kafka; " +
				"recognition is via isCustomConnectorProfile (no W.10 table regression).",
		},
		{
			ID: "SC102-KAFKA-DDL", Req: "J.42", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactDDL, Expected: "pxf:// LOCATION PROFILE=kafka&SERVER=kafka-connector",
			Contains:    Scenario102KafkaPxfLocation,
			Description: "the external-table DDL LOCATION carries pxf://<topic>?PROFILE=kafka&SERVER=kafka-connector.",
		},

		// --- J.43: continuous → long-running Job + CBK_CONTINUOUS -----------
		{
			ID: "SC102-J43-CONTINUOUS-JOB", Req: "J.43", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactJob, Expected: "nil ActiveDeadlineSeconds + RestartPolicy OnFailure",
			Description: "Continuous=true → Job ActiveDeadlineSeconds nil, BackoffLimit 6, RestartPolicy " +
				"OnFailure (a long-running consumer never killed by a short deadline).",
		},
		{
			ID: "SC102-J43-CONTINUOUS-ENV", Req: "J.43", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactContainerEnv, Expected: "CBK_CONTINUOUS=true",
			Contains:    "CBK_CONTINUOUS",
			Description: "CBK_CONTINUOUS=true is set on the dataload Job container for a continuous job.",
		},
		{
			ID: "SC102-J43-CONTINUOUS-W23c", Req: "J.43", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "DENY continuous + schedule",
			Description: "Continuous=true + a non-empty Schedule is REJECTED (W.23c — protects J.46 at " +
				"admission time).",
		},
		{
			ID: "SC102-J43-CONSUME-LIVE", Req: "J.43", Layer: Scenario102LayerLive,
			Artifact: Scenario102ArtifactJob, Expected: "dataload Job Running (streaming consumer)",
			Description: "[LIVE-ONLY] the dataload Job is RUNNING (a continuous consumer does not " +
				"complete) — cloudberry_data_loading_job_status steady at Running.",
		},

		// --- J.44: batchSize → CBK_BATCH_SIZE -------------------------------
		{
			ID: "SC102-J44-BATCHSIZE-ENV", Req: "J.44", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactContainerEnv, Expected: "CBK_BATCH_SIZE=10000",
			Contains:    "CBK_BATCH_SIZE",
			Description: "batchSize:10000 → CBK_BATCH_SIZE=10000 on the dataload Job container.",
		},
		{
			ID: "SC102-J44-BATCHSIZE-MIN", Req: "J.44", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "DENY batchSize < 1",
			Description: "a batchSize < 1 is REJECTED (kubebuilder Minimum=1 / W.23c).",
		},

		// --- J.45: flushInterval → CBK_FLUSH_INTERVAL -----------------------
		{
			ID: "SC102-J45-FLUSH-ENV", Req: "J.45", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactContainerEnv, Expected: "CBK_FLUSH_INTERVAL=30s",
			Contains:    "CBK_FLUSH_INTERVAL",
			Description: "flushInterval:\"30s\" → CBK_FLUSH_INTERVAL=30s on the dataload Job container.",
		},
		{
			ID: "SC102-J45-FLUSH-DUR", Req: "J.45", Layer: Scenario102LayerWebhook,
			Artifact: Scenario102ArtifactWebhook, Expected: "DENY non-duration flushInterval",
			Description: "a flushInterval that is not a Go duration is REJECTED (W.23c).",
		},

		// --- J.46: one-off Job NOT CronJob ----------------------------------
		{
			ID: "SC102-J46-CRON-NIL", Req: "J.46", Layer: Scenario102LayerBuilder,
			Artifact: Scenario102ArtifactCronJob, Expected: "BuildDataLoadCronJob nil; BuildDataLoadJob Job",
			Description: "kafka-cdc (no schedule) → BuildDataLoadCronJob returns nil; BuildDataLoadJob " +
				"returns the one-off Job.",
		},
		{
			ID: "SC102-J46-JOB-NOT-CRON", Req: "J.46", Layer: Scenario102LayerReconcile,
			Artifact: Scenario102ArtifactJob, Expected: "Job created, NO CronJob",
			Description: "[LIVE/reconcile] no schedule → a batchv1.Job <cluster>-dataload-kafka-cdc " +
				"exists; NO batchv1.CronJob of that name.",
		},

		// --- end-to-end row landing (config-only without a real connector) --
		{
			ID: "SC102-E2E-ROWS", Req: "C.18/J.42", Layer: Scenario102LayerLive,
			Artifact: Scenario102ArtifactJob, Expected: "kafka topic → public.kafka_events rows land",
			Description: "[LIVE-ONLY / CONFIG-ONLY] end-to-end kafka topic→public.kafka_events row " +
				"landing needs a REAL Kafka→PXF connector JAR. The staged JAR is a placeholder, so " +
				"this is CONFIG-ONLY: assert the Job runs as a streaming consumer + the JAR is " +
				"mounted + the loader is invoked; real row landing requires a real connector.",
		},
	}
}

// ============================================================================
// Scenario 103 — FDW-Based Loading Path (EX.5-EX.8)
// ============================================================================
//
// Scenario103Case mirrors the Scenario101Case SHAPE: a small, flat row catalog
// resolved against the REAL built artifact (the FDW DDL chain delivered as the
// dataload Job Args[0], the FDW load script) or the validator (W.25 / W.17).
// The catalog covers EX.5-EX.8 (task-breakdown §9).
//
// Layers are one of "builder", "webhook", "reconcile", "live". Builder/webhook
// rows are byte-provable and resolved against the shipped artifact in the
// functional + e2e Part A suites; rows whose ONLY honest signal is a running
// cluster carry Layer "live" (marked [LIVE-ONLY]). The live CREATE SERVER leg
// depends on a registered pxf_fdw wrapper; the wrapper s3_pxf_fdw is VERIFIED
// registered, so SC103-LIVE-SERVER is not config-only, but a row marked
// [CONFIG-ONLY-IF-NAME-DIFFERS] degrades to config-only if a DevOps \dew finding
// later shows a different registered name.
//
// METRIC HONESTY: NO new metric. An FDW load IS a data-loading job → it reuses
// cloudberry_data_loading_* (job_status/rows_total/errors_total). The
// cloudberry_pxf_* / cloudberry_gpfdist_* families stay PLANNED and are NEVER
// asserted; the EX.8 EQUIVALENCE is proven by ROW COUNTS, not a bytes metric.
type Scenario103Case struct {
	// ID is the catalog rule id (e.g. "SC103-EX5-SERVER").
	ID string
	// Req is the spec requirement id family the row proves (EX.5-EX.8 / W.25 /
	// W.17).
	Req string
	// Layer is the assertion layer: "builder" (pure, byte-provable),
	// "webhook" (admission accept/deny), "reconcile" (envtest), or "live"
	// (requires a running cluster — live-only).
	Layer string
	// Artifact names the built object family the row asserts against:
	// "fdw-ddl", "fdw-script", "job", "webhook", or "live".
	Artifact string
	// Expected is a short outcome token / human description of the asserted
	// outcome (e.g. "CREATE SERVER ... s3_pxf_fdw", "DENY").
	Expected string
	// Contains is the byte-stable substring the built artifact must carry for
	// a builder row (e.g. the FDW DDL token) — empty for rows asserted
	// structurally or for webhook/live rows.
	Contains string
	// Description explains the case + names the HONEST signal. Live-only rows
	// are marked [LIVE-ONLY]; wrapper-name-dependent rows
	// [CONFIG-ONLY-IF-NAME-DIFFERS].
	Description string
}

// Scenario 103 layer tokens (alias the Scenario 101/102 layer values so the
// catalogs share a vocabulary).
const (
	Scenario103LayerBuilder   = "builder"
	Scenario103LayerWebhook   = "webhook"
	Scenario103LayerReconcile = "reconcile"
	Scenario103LayerLive      = "live"
)

// Scenario 103 artifact tokens.
const (
	Scenario103ArtifactFDWDDL    = "fdw-ddl"
	Scenario103ArtifactFDWScript = "fdw-script"
	Scenario103ArtifactJob       = "job"
	Scenario103ArtifactWebhook   = "webhook"
	Scenario103ArtifactLive      = "live"
)

// Scenario 103 well-known names + values (mirror the scenario103-fdw-test
// sample CR + task-breakdown §9/§11). The FDW load reads the SAME MinIO s3
// dataset the external-table load reads (cloudberry-data/text/data.csv, 1000
// rows: id int, name text, value int) so the EQUIVALENCE comparison is over
// identical bytes.
const (
	// Scenario103ClusterName is the live sample cluster name.
	Scenario103ClusterName = "fdw-test"
	// Scenario103Namespace is the deploy namespace.
	Scenario103Namespace = "cloudberry-test"
	// Scenario103Server is the s3 PXF server the FDW jobs reference.
	Scenario103Server = "s3-datalake"
	// Scenario103FDWServerName is the derived FDW server name
	// (fdwServerName("s3-datalake") = foreign_s3_datalake).
	Scenario103FDWServerName = "foreign_s3_datalake"
	// Scenario103S3Wrapper is the registered FDW wrapper for the s3 scheme
	// (VERIFIED registered in cloudberry-official-pxf:2.1.0).
	Scenario103S3Wrapper = "s3_pxf_fdw"
	// Scenario103JDBCWrapper is the registered FDW wrapper for the jdbc scheme.
	Scenario103JDBCWrapper = "jdbc_pxf_fdw"
	// Scenario103Profile is the s3:text profile the live CSV dataset uses.
	Scenario103Profile = "s3:text"
	// Scenario103Resource is the s3 resource the FDW + external-table jobs read.
	Scenario103Resource = "cloudberry-data/text/data.csv"
	// Scenario103ExtJobName is the external-table (loadMethod unset) load job.
	Scenario103ExtJobName = "s3-ext-load"
	// Scenario103FDWJobName is the loadMethod:fdw load job.
	Scenario103FDWJobName = "s3-fdw-load"
	// Scenario103FilteredJobName is the loadMethod:fdw + sourceFilter load job.
	Scenario103FilteredJobName = "s3-fdw-filtered"
	// Scenario103ExtTargetTable is the external-table leg target table.
	Scenario103ExtTargetTable = "public.events_ext"
	// Scenario103FDWTargetTable is the FDW leg target table.
	Scenario103FDWTargetTable = "public.events_fdw"
	// Scenario103FilteredTargetTable is the filtered FDW leg target table.
	Scenario103FilteredTargetTable = "public.events_fdw_filtered"
	// Scenario103SourceFilter is the filtered FDW WHERE fragment (valid for the
	// id,name,value CSV schema).
	Scenario103SourceFilter = "value > 500"
	// Scenario103DataRows is the row count of the MinIO CSV dataset.
	Scenario103DataRows = 1000
	// Scenario103TargetDDL creates the two equivalence target tables + the
	// filtered target, matching the CSV schema (id int, name text, value int).
	Scenario103TargetDDL = "CREATE TABLE IF NOT EXISTS public.events_ext " +
		"(id int, name text, value int); " +
		"CREATE TABLE IF NOT EXISTS public.events_fdw " +
		"(id int, name text, value int); " +
		"CREATE TABLE IF NOT EXISTS public.events_fdw_filtered " +
		"(id int, name text, value int)"
)

// Scenario103Cases returns the full Scenario 103 catalog (task-breakdown §9):
// the FDW DDL builder rows (EX.5-EX.7), the FDW load-script builder rows (EX.8),
// the webhook rows (W.25 / W.17), the reconcile row (RECON), and the live rows
// (incl. the headline SC103-LIVE-EQUIV). Builder/webhook rows are resolved
// against the shipped artifact in the functional + e2e Part A suites; rows whose
// only honest signal is a running cluster carry Layer "live" (marked
// [LIVE-ONLY]).
//
// HONESTY: NO new metric. The FDW load reuses cloudberry_data_loading_* from
// real Job state; the EX.8 EQUIVALENCE is proven by count(ext) == count(fdw),
// NOT a fabricated bytes metric. The wrapper s3_pxf_fdw is VERIFIED registered.
func Scenario103Cases() []Scenario103Case {
	return []Scenario103Case{
		// --- EX.5: CREATE SERVER ----------------------------------------------
		{
			ID: "SC103-EX5-SERVER", Req: "EX.5", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "CREATE SERVER IF NOT EXISTS \"foreign_<server>\"",
			Contains: "CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"",
			Description: "the FDW DDL (dataload Job Args[0] for a loadMethod:fdw job) carries " +
				"CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\" (idempotent, persistent).",
		},
		{
			ID: "SC103-EX5-WRAPPER", Req: "EX.5", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "FOREIGN DATA WRAPPER s3_pxf_fdw",
			Contains: "FOREIGN DATA WRAPPER s3_pxf_fdw",
			Description: "the s3-scheme profile resolves the per-scheme wrapper s3_pxf_fdw " +
				"(VERIFIED registered). The GENERATED name is byte-provable.",
		},
		{
			ID: "SC103-EX5-OPTS", Req: "EX.5", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "OPTIONS (config '<pxf-server>')",
			Contains: "OPTIONS (config 's3-datalake')",
			Description: "the CREATE SERVER OPTIONS carry config '<pxf-server>' (the named PXF " +
				"server config whose credentials/endpoint the FDW read resolves) — NOT " +
				"resource/format: the pxf_fdw validator rejects resource at the server level " +
				"(it can only be defined at the pg_foreign_table level).",
		},
		{
			ID: "SC103-EX5-WRAPPER-JDBC", Req: "EX.5", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "jdbc profile -> jdbc_pxf_fdw + no format option",
			Contains: "FOREIGN DATA WRAPPER jdbc_pxf_fdw",
			Description: "a bare jdbc loadMethod:fdw profile resolves jdbc_pxf_fdw AND omits the " +
				"format OPTION (a JDBC FDW takes a resource=table, no format suffix).",
		},

		// --- EX.6: CREATE USER MAPPING ----------------------------------------
		{
			ID: "SC103-EX6-USERMAP", Req: "EX.6", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"",
			Contains: "CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"",
			Description: "the FDW DDL carries CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\" " +
				"SERVER \"foreign_<server>\" (the gpadmin data-loader role).",
		},

		// --- EX.7: CREATE FOREIGN TABLE (persistent) --------------------------
		{
			ID: "SC103-EX7-FTABLE", Req: "EX.7", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_<job>\" (LIKE <target>)",
			Contains: "CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")",
			Description: "the FDW DDL carries CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_<job>\" " +
				"(LIKE <target>) SERVER \"foreign_<server>\" (schema borrowed from the target).",
		},
		{
			ID: "SC103-EX7-FTABLE-OPTS", Req: "EX.7", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWDDL,
			Expected: "foreign table OPTIONS (resource '<resource>'[, format '<fmt>'])",
			Contains: "OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet')",
			Description: "the FOREIGN TABLE OPTIONS carry resource/format (the pg_foreign_table " +
				"level) — the ONLY place they may live: the pxf_fdw validator rejects resource " +
				"on the SERVER (which carries config '<pxf-server>' instead).",
		},
		{
			ID: "SC103-EX7-PERSIST", Req: "EX.7", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWScript,
			Expected: "NO DROP FOREIGN TABLE / DROP SERVER (persistent)",
			Description: "the FDW load script NEVER drops the FDW objects (persistent, directly " +
				"queryable). NO DROP FOREIGN TABLE / DROP SERVER / DROP EXTERNAL TABLE.",
		},

		// --- EX.8: direct query + INSERT...SELECT...WHERE + ANALYZE -----------
		{
			ID: "SC103-EX8-DIRECT", Req: "EX.8", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWScript,
			Expected: "SELECT count(*) FROM \"foreign_<job>\"",
			Contains: "SELECT count(*) FROM \"foreign_fdw_ingest\"",
			Description: "the FDW load script queries the persistent foreign table DIRECTLY " +
				"(SELECT count(*) FROM \"foreign_<job>\") — EX.8 \"query the foreign table directly\".",
		},
		{
			ID: "SC103-EX8-INSERT", Req: "EX.8", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWScript,
			Expected: "INSERT INTO \"<target>\" SELECT * FROM \"foreign_<job>\"",
			Contains: "INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\"",
			Description: "the FDW load INSERTs INTO <target> SELECT * FROM the foreign table — the " +
				"EQUIVALENT shape to the external-table read path (proves EX.8 equivalence).",
		},
		{
			ID: "SC103-EX8-WHERE", Req: "EX.8", Layer: Scenario103LayerBuilder,
			Artifact: Scenario103ArtifactFDWScript,
			Expected: "INSERT ... WHERE <sourceFilter> (quoted heredoc)",
			Description: "with sourceFilter set, the INSERT carries the WHERE predicate via the " +
				"shared single-quote-safe quoted heredoc path.",
		},
		{
			ID: "SC103-EX8-ANALYZE", Req: "EX.8", Layer: Scenario103LayerBuilder,
			Artifact:    Scenario103ArtifactFDWScript,
			Expected:    "ANALYZE \"<target>\"",
			Contains:    "ANALYZE \"public\".\"events\"",
			Description: "the read path runs ANALYZE <target> after the load (statistics refresh).",
		},

		// --- W.25 / W.17 webhook ---------------------------------------------
		{
			ID: "SC103-W25-ENUM", Req: "W.25", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "DENY loadMethod bogus (enum)",
			Description: "an unrecognized loadMethod is DENIED (must be external-table or fdw).",
		},
		{
			ID: "SC103-W25-FDW-WRITABLE", Req: "W.25", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "DENY loadMethod=fdw + mode=writable",
			Description: "loadMethod=fdw + mode=writable is DENIED (a writable FDW export is out " +
				"of scope; fdw is a read/import path only).",
		},
		{
			ID: "SC103-W25-FDW-CONTINUOUS", Req: "W.25", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "DENY loadMethod=fdw + continuous",
			Description: "loadMethod=fdw + continuous=true is DENIED (fdw is a one-off persistent " +
				"load, not a streaming consume loop).",
		},
		{
			ID: "SC103-W25-FDW-OK", Req: "W.25", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "ADMIT loadMethod=fdw read",
			Description: "loadMethod=fdw on a normal read profile (no mode, not continuous) is ADMITTED.",
		},
		{
			ID: "SC103-W17-FDW-FILTER", Req: "W.17", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "ADMIT sourceFilter on fdw read",
			Description: "sourceFilter on a loadMethod=fdw read job is ADMITTED (the extended W.17 " +
				"allows the predicate on the fdw read INSERT WHERE).",
		},
		{
			ID: "SC103-W17-EXT-READ-DENY", Req: "W.17", Layer: Scenario103LayerWebhook,
			Artifact: Scenario103ArtifactWebhook, Expected: "DENY sourceFilter on plain ext-table read",
			Description: "sourceFilter on a PLAIN external-table read (loadMethod unset, not " +
				"writable) is DENIED (only valid for a writable export OR an fdw read).",
		},

		// --- RECON: reconciled Job carries the FDW DDL + INSERT --------------
		{
			ID: "SC103-RECON-ARGS", Req: "EX.5-8", Layer: Scenario103LayerReconcile,
			Artifact: Scenario103ArtifactJob,
			Expected: "reconciled Job Args[0] carries the FDW DDL + INSERT substrings",
			Description: "[reconcile] a loadMethod:fdw PXF job reconciles → the dataload Job " +
				"Args[0] carries the FDW DDL (CREATE SERVER / FOREIGN TABLE) + the INSERT " +
				"INTO <target> SELECT * FROM foreign_<job> substrings.",
		},

		// --- LIVE: the FDW objects + the EQUIVALENCE proof -------------------
		{
			ID: "SC103-LIVE-SERVER", Req: "EX.5-7", Layer: Scenario103LayerLive,
			Artifact: Scenario103ArtifactLive,
			Expected: "live CREATE SERVER/USER MAPPING/FOREIGN TABLE succeed",
			Description: "[LIVE-ONLY][CONFIG-ONLY-IF-NAME-DIFFERS] the live FDW DDL succeeds and the " +
				"objects EXIST: SELECT count(*) FROM pg_foreign_server WHERE srvname=" +
				"'foreign_s3_datalake' > 0 + the foreign table exists. The wrapper s3_pxf_fdw " +
				"is VERIFIED registered.",
		},
		{
			ID: "SC103-LIVE-DIRECT", Req: "EX.8", Layer: Scenario103LayerLive,
			Artifact: Scenario103ArtifactLive,
			Expected: "SELECT count(*) FROM foreign_<job> > 0",
			Description: "[LIVE-ONLY] the persistent foreign table is queryable directly: " +
				"SELECT count(*) FROM the foreign table > 0 (the s3 dataset rows).",
		},
		{
			ID: "SC103-LIVE-EQUIV", Req: "EX.8", Layer: Scenario103LayerLive,
			Artifact: Scenario103ArtifactLive,
			Expected: "count(public.events_ext) == count(public.events_fdw)",
			Description: "[LIVE-ONLY] the HEADLINE: run BOTH the external-table load (-> " +
				"public.events_ext) and the FDW load (-> public.events_fdw) over the SAME " +
				"dataset → count(events_ext) == count(events_fdw) (data flows EQUIVALENTLY).",
		},
		{
			ID: "SC103-LIVE-FILTERED", Req: "EX.8", Layer: Scenario103LayerLive,
			Artifact: Scenario103ArtifactLive,
			Expected: "filtered FDW load rows < unfiltered FDW load rows",
			Description: "[LIVE-ONLY] the s3-fdw-filtered load (sourceFilter WHERE) lands FEWER " +
				"rows than the unfiltered FDW load.",
		},
	}
}

// ============================================================================
// Scenario 104 — Pre-Load Health Checks (HC.1-HC.5)
// ============================================================================
//
// Scenario104Case mirrors the Scenario101Case SHAPE: a small, flat row catalog
// resolved against the REAL built artifact (the dataload-healthcheck init
// container + its 5-check script + the shared scratch emptyDir) or — for the
// -L-* / -R rows — proven live/at-reconcile (the fail+restore of each HC).
//
// Source: task-breakdown §3 (the 104-* catalog). Layers are one of "builder",
// "reconcile", or "live"; rows whose ONLY honest signal is a running cluster
// (the fail+restore of a real condition, kube-state-metrics) carry Layer "live"
// and are marked live-only. The catalog is honest: every builder row is resolved
// against the shipped artifact in the functional + e2e Part A suites; live rows
// are exercised at e2e Part B.
//
// METRIC HONESTY: NO new operator metric. HC failures are observed via the REAL
// cloudberry_data_loading_job_status=3 (Failed) + cloudberry_data_loading_errors_total
// + the de-duplicated DataLoadingHealthCheckFailed Warning Event + the NEW
// kube-state-metrics (kube_job_status_failed{job_name=~".*-dataload-.*"},
// kube_pod_init_container_status_*). cloudberry_pxf_*/cloudberry_gpfdist_* stay
// PLANNED and are NEVER asserted.
//
// HC.1 HONESTY: HC.1 is a DB-PROXY PXF-readiness probe (the load pod CANNOT
// reach a segment's localhost-only sidecar). The builder proves the SCRIPT; the
// live "stop PXF → the job fails" is the behavioral proof. A direct sidecar curl
// from the load pod is out of scope (impossible per spec §3341).
type Scenario104Case struct {
	// ID is the catalog rule id (e.g. "104-HC1-B", "104-HC2-L-fail").
	ID string
	// Req is the spec requirement family the row proves (e.g. "HC.1", "HC.5",
	// "INIT", "KNOB", "BLOCK", "EVENT", "RESTORE", "KSM").
	Req string
	// Layer is the assertion layer: "builder" (pure, byte-provable),
	// "reconcile" (envtest), or "live" (requires a running cluster — live-only).
	Layer string
	// Artifact names the built object family the row asserts against:
	// "init-script", "init-container", "volume", "event", "metric", or "live".
	Artifact string
	// Expected is a short outcome token / human description of the asserted
	// outcome (e.g. "init script carries the PXF-readiness probe", "Job Failed + Event").
	Expected string
	// Contains is the byte-stable substring the built artifact must carry for a
	// builder row (e.g. "proname = 'pxf_read'", "df -Pk /dataload-scratch") — empty
	// for rows asserted structurally or for live/reconcile rows.
	Contains string
	// Description explains the case + names the HONEST signal. Live-only rows are
	// marked [LIVE-ONLY]; HC.1 carries the DB-proxy honesty note; rows that may
	// degrade to config-only carry [CONFIG-ONLY-IF-...].
	Description string
}

// Scenario 104 layer tokens (alias the Scenario 101/103 layer values so the
// catalogs share a vocabulary).
const (
	Scenario104LayerBuilder   = "builder"
	Scenario104LayerReconcile = "reconcile"
	Scenario104LayerLive      = "live"
)

// Scenario 104 artifact tokens.
const (
	Scenario104ArtifactInitScript    = "init-script"
	Scenario104ArtifactInitContainer = "init-container"
	Scenario104ArtifactVolume        = "volume"
	Scenario104ArtifactEvent         = "event"
	Scenario104ArtifactMetric        = "metric"
	Scenario104ArtifactLive          = "live"
)

// Scenario 104 well-known names + values (mirror the scenario104-healthcheck-test
// sample CR + task-breakdown §1/§3/§5 + the shipped builder/controller consts).
const (
	// Scenario104ClusterName is the live sample cluster name.
	Scenario104ClusterName = "healthcheck-test"
	// Scenario104Namespace is the deploy namespace.
	Scenario104Namespace = "cloudberry-test"
	// Scenario104InitName is the FIRST init container name on the dataload Job pod.
	Scenario104InitName = "dataload-healthcheck"
	// Scenario104ScratchVolume is the shared scratch emptyDir name.
	Scenario104ScratchVolume = "dataload-scratch"
	// Scenario104ScratchMount is the shared scratch mount path (HC.5).
	Scenario104ScratchMount = "/dataload-scratch"
	// Scenario104EventReason is the de-duplicated Warning Event reason emitted on
	// an init-container health-check failure.
	Scenario104EventReason = "DataLoadingHealthCheckFailed"
	// Scenario104PxfJobName is the s3 pxf load job (HC.1/HC.2/HC.3/HC.5).
	Scenario104PxfJobName = "s3-load"
	// Scenario104GploadJobName is the gpload load job (HC.4).
	Scenario104GploadJobName = "gpload-csv"
	// Scenario104Server is the s3 PXF server the pxf job references (MinIO).
	Scenario104Server = "s3-datalake"
	// Scenario104Profile is the s3:text profile the live CSV dataset uses.
	Scenario104Profile = "s3:text"
	// Scenario104Resource is the s3 resource the pxf job reads.
	Scenario104Resource = "cloudberry-data/text/data.csv"
	// Scenario104TargetTable is the HC.2 target table (create/drop to toggle).
	Scenario104TargetTable = "public.events"
	// Scenario104GpfdistPort is the gpfdist Service port (HC.4 probe).
	Scenario104GpfdistPort = 8080
	// Scenario104DiskMinFreeMB is the default HC.5 free-space threshold.
	Scenario104DiskMinFreeMB = 64
	// Scenario104TargetDDL creates the HC.2 target table matching the CSV schema
	// (id int, name text, value int). The e2e Part B drops/creates it to toggle
	// HC.2's deterministic fail+restore.
	Scenario104TargetDDL = "CREATE TABLE IF NOT EXISTS public.events " +
		"(id int, name text, value int)"
)

// Scenario104Cases returns the full Scenario 104 catalog (task-breakdown §3):
// the per-HC builder rows (-B), the cross-cutting init/knob builder rows, the
// reconcile rows (-R), and the live fail+restore rows (-L-fail/-L-restore) plus
// the kube-state-metrics dependency row. Builder rows are resolved against the
// shipped artifact in the functional + e2e Part A suites; rows whose only honest
// signal is a running cluster carry Layer "live" (marked [LIVE-ONLY]).
//
// HONESTY: NO new operator metric (see the type doc). HC.1 is a DB-proxy probe;
// the deterministic HEADLINE is HC.2 (target-table-exists), fully live-provable.
func Scenario104Cases() []Scenario104Case {
	return []Scenario104Case{
		// --- HC.1 — PXF readiness (DB proxy) --------------------------------
		{
			ID: "104-HC1-B", Req: "HC.1", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "pxf job init script carries the DB-proxy PXF-readiness probe",
			Contains: "proname = 'pxf_read'",
			Description: "the dataload-healthcheck init script for a pxf job (pxf enabled) carries " +
				"the DB-proxy PXF-readiness probe substrings (pg_extension, extname='pxf', " +
				"pg_proc proname = 'pxf_read' — PXF 2.1 has no pxf_version(), HC.1 FAIL) — NOT a " +
				"direct sidecar curl (impossible from the load pod per spec §3341).",
		},
		{
			ID: "104-HC1-B-gate", Req: "HC.1", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "non-pxf job OR pxf disabled → no HC.1 block",
			Description: "a gpload job (or a pxf-disabled cluster) emits NO HC.1 lines " +
				"(pg_proc pxf_read/pg_extension/HC.1 FAIL absent) — HC.1 is pxf-only.",
		},
		{
			ID: "104-HC1-L-fail", Req: "HC.1", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "stop PXF on a segment → s3-load init fails → Job Failed + Event",
			Description: "[LIVE-ONLY][CONFIG-ONLY-IF-PXF-STOP-NOT-REPRODUCIBLE] stop PXF on a " +
				"segment (kubectl exec <segpod> -c pxf -- pxf stop, or break the sidecar) → the " +
				"s3-load pxf job's DB-proxy HC.1 (or the subsequent load) fails → init exits " +
				"non-zero → Job Failed + DataLoadingHealthCheckFailed Event + " +
				"data_loading_job_status=3 + kube_pod_init_container_status_*. HONESTY: HC.1 is " +
				"a DB proxy; if pxf_version() still resolves with PXF stopped the honest proof " +
				"is 'the job fails when PXF is down'.",
		},
		{
			ID: "104-HC1-L-restore", Req: "HC.1", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "restart PXF → delete failed Job → re-run → init passes → proceeds",
			Description: "[LIVE-ONLY] restart PXF on the segment → delete the failed Job → next " +
				"reconcile re-creates it → init passes → the load proceeds → Job Succeeded.",
		},

		// --- HC.2 — target-table exists (the deterministic HEADLINE) --------
		{
			ID: "104-HC2-B", Req: "HC.2", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "ALL jobs → init script carries the to_regclass probe",
			Contains: "to_regclass('${tbl}')",
			Description: "every job type's init script carries the HC.2 target-table-exists probe " +
				"(tbl='<targetTable>', to_regclass('${tbl}'), HC.2 FAIL, does not exist).",
		},
		{
			ID: "104-HC2-L-fail", Req: "HC.2", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "non-existent target table → init HC.2 fails → Job Failed + Event",
			Description: "[LIVE-ONLY][HEADLINE] point the job at a non-existent target table " +
				"(patch targetTable to public.does_not_exist OR drop public.events) → init HC.2 " +
				"exits non-zero → Job Failed + DataLoadingHealthCheckFailed Event + " +
				"data_loading_job_status=3. THE DETERMINISTIC, FULLY LIVE-PROVABLE headline.",
		},
		{
			ID: "104-HC2-L-restore", Req: "HC.2", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "create public.events → re-run → init passes → proceeds",
			Description: "[LIVE-ONLY] create public.events (the target table) → re-run → init " +
				"HC.2 passes → the load proceeds → Job Succeeded; rows harvested.",
		},

		// --- HC.3 — external source connectivity (s3) -----------------------
		{
			ID: "104-HC3-B", Req: "HC.3", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "pxf s3 job → init status-code connectivity probe + AWS_* SecretKeyRef env",
			Contains: `code=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "${AWS_S3_ENDPOINT}/" || true)`,
			Description: "a pxf s3 job's init script carries the HC.3 status-code connectivity probe " +
				"against AWS_S3_ENDPOINT — any HTTP response (incl. 400/403 from S3-compatible " +
				"stores) is reachable (HC.3 FAIL only on connection failure); the init container " +
				"carries AWS_* via SecretKeyRef (NO plaintext) + AWS_S3_ENDPOINT.",
		},
		{
			ID: "104-HC3-B-skip", Req: "HC.3", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "non-object-store server (jdbc/hive) → HC.3 skipped",
			Description: "a non-object-store pxf job (jdbc/hive) is NOT auto-probed for HC.3 " +
				"connectivity (no AWS_S3_ENDPOINT curl line; no HC.3 FAIL block).",
		},
		{
			ID: "104-HC3-L-fail", Req: "HC.3", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "break the s3 endpoint → init HC.3 curl fails → Job Failed",
			Description: "[LIVE-ONLY] patch the s3 server fs.s3a.endpoint to a bad host → init " +
				"HC.3 curl --head fails → Job Failed + DataLoadingHealthCheckFailed Event + " +
				"data_loading_job_status=3.",
		},
		{
			ID: "104-HC3-L-restore", Req: "HC.3", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "fix the s3 endpoint → re-run → init passes → proceeds",
			Description: "[LIVE-ONLY] restore the s3 endpoint → re-run → init HC.3 passes → " +
				"the load proceeds → Job Succeeded.",
		},

		// --- HC.4 — gpfdist reachability (gpload only) ----------------------
		{
			ID: "104-HC4-B", Req: "HC.4", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "gpload job (gpfdist enabled) → init curls the gpfdist Service",
			Contains: "-gpfdist-svc:8080/",
			Description: "a gpload job's init script (gpfdist enabled) carries the HC.4 reachability " +
				"probe against http://<cluster>-gpfdist-svc:8080/ (curl -fsS, HC.4 FAIL).",
		},
		{
			ID: "104-HC4-B-gate", Req: "HC.4", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "pxf job OR gpfdist disabled → no HC.4 block",
			Description: "a pxf job (or a gpfdist-disabled cluster) emits NO HC.4 lines " +
				"(gpfdist-svc/HC.4 FAIL absent) — HC.4 is gpload-only.",
		},
		{
			ID: "104-HC4-L-fail", Req: "HC.4", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "scale gpfdist to 0 → gpload init HC.4 curl fails → Job Failed",
			Description: "[LIVE-ONLY] scale the gpfdist Deployment to 0 (kubectl scale deploy " +
				"<cluster>-gpfdist --replicas=0) → the gpload job's init HC.4 curl fails (no ready " +
				"endpoints) → Job Failed + DataLoadingHealthCheckFailed Event + " +
				"data_loading_job_status=3 + kube_deployment_status_replicas_available=0.",
		},
		{
			ID: "104-HC4-L-restore", Req: "HC.4", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "scale gpfdist back to 1 → re-run → init passes → proceeds",
			Description: "[LIVE-ONLY] scale the gpfdist Deployment back to 1 → re-run → init HC.4 " +
				"passes → the load proceeds → Job Succeeded.",
		},

		// --- HC.5 — disk space + scratch volume (ALL jobs) ------------------
		{
			ID: "104-HC5-B", Req: "HC.5", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitScript,
			Expected: "ALL jobs → df probe + default 64MB threshold",
			Contains: "df -Pk /dataload-scratch",
			Description: "every job type's init script carries the HC.5 df free-space probe " +
				"(df -Pk /dataload-scratch, 64 * 1024 threshold from diskMinFreeMB, HC.5 FAIL).",
		},
		{
			ID: "104-HC5-B-VOL", Req: "HC.5", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactVolume,
			Expected: "scratch emptyDir + mount on init AND main container",
			Description: "the shared dataload-scratch emptyDir is in podSpec.Volumes and mounted " +
				"at /dataload-scratch on BOTH the init container AND the main dataload container.",
		},
		{
			ID: "104-HC5-L-fail", Req: "HC.5", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "fill scratch / raise diskMinFreeMB → init HC.5 fails → Job Failed",
			Description: "[LIVE-ONLY][CONFIG-ONLY-IF-FILL-NOT-REPRODUCIBLE] make HC.5 fail by " +
				"patching diskMinFreeMB to a value exceeding the emptyDir free space (the most " +
				"deterministic mechanism) OR filling /dataload-scratch beyond scratchSizeLimit " +
				"(64Mi) → init df below threshold → Job Failed + DataLoadingHealthCheckFailed " +
				"Event + data_loading_job_status=3.",
		},
		{
			ID: "104-HC5-L-restore", Req: "HC.5", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactLive,
			Expected: "lower diskMinFreeMB / free space → re-run → init passes → proceeds",
			Description: "[LIVE-ONLY] restore the threshold (lower diskMinFreeMB) / free the " +
				"scratch space → re-run → init HC.5 passes → the load proceeds → Job Succeeded.",
		},

		// --- Cross-cutting: init container shape + knob ---------------------
		{
			ID: "104-INIT-B", Req: "INIT", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitContainer,
			Expected: "init FIRST, named dataload-healthcheck, image=dataLoaderImage, /bin/bash -c",
			Description: "the pre-load health-check init container is FIRST in PodSpec.InitContainers, " +
				"named dataload-healthcheck, uses the data-loader image, and runs /bin/bash -c " +
				"the 5-check script (which blocks the main load container on a non-zero exit).",
		},
		{
			ID: "104-KNOB-B", Req: "KNOB", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitContainer,
			Expected: "healthChecks.enabled=false → NO init container, NO scratch volume",
			Description: "healthChecks.enabled=false removes the init container, the scratch volume " +
				"AND the main-container scratch mount (byte-identical to a pre-Scenario-104 pod).",
		},
		{
			ID: "104-KNOB-B-default", Req: "KNOB", Layer: Scenario104LayerBuilder,
			Artifact: Scenario104ArtifactInitContainer,
			Expected: "nil healthChecks block → init present (default on)",
			Description: "a nil dataLoading.healthChecks block (or a nil Enabled pointer) leaves " +
				"the checks ON — the init container is present by default.",
		},

		// --- Reconcile (envtest-provable; proven at unit/controller level) --
		{
			ID: "104-BLOCK-R", Req: "BLOCK", Layer: Scenario104LayerReconcile,
			Artifact: Scenario104ArtifactMetric,
			Expected: "failed-init Job → job_status=3 + errors_total incremented",
			Description: "[reconcile] a dataload Job observed Failed with the dataload-healthcheck " +
				"init terminated non-zero is harvested as data_loading_job_status=3 (Failed) AND " +
				"data_loading_errors_total is incremented (the existing honest harvest).",
		},
		{
			ID: "104-EVENT-R", Req: "EVENT", Layer: Scenario104LayerReconcile,
			Artifact: Scenario104ArtifactEvent,
			Expected: "failed-init Job → ONE DataLoadingHealthCheckFailed Warning; de-dup on repeat",
			Description: "[reconcile] a failed-init dataload Job emits exactly ONE de-duplicated " +
				"DataLoadingHealthCheckFailed Warning Event naming the job + the init container; " +
				"a second reconcile of the unchanged Failed Job emits no new event.",
		},
		{
			ID: "104-EVENT-R-mainfail", Req: "EVENT", Layer: Scenario104LayerReconcile,
			Artifact: Scenario104ArtifactEvent,
			Expected: "MAIN-container failure → NO HC event (honest attribution)",
			Description: "[reconcile] a Job that failed in the MAIN dataload container (init " +
				"succeeded) emits NO DataLoadingHealthCheckFailed event — the failure is surfaced " +
				"via the generic status + errors_total handling instead (honest attribution).",
		},
		{
			ID: "104-RESTORE-R", Req: "RESTORE", Layer: Scenario104LayerReconcile,
			Artifact: Scenario104ArtifactInitContainer,
			Expected: "failed Job deleted → next reconcile re-creates the Job with the init",
			Description: "[reconcile] a failed Job deleted (TTL / explicit) is re-created on the " +
				"next reconcile with InitContainers[0]==dataload-healthcheck (the restore path).",
		},

		// --- kube-state-metrics dependency (live/devops) --------------------
		{
			ID: "104-KSM-DEP", Req: "KSM", Layer: Scenario104LayerLive,
			Artifact: Scenario104ArtifactMetric,
			Expected: "kube-state-metrics deployed → init-container + job-failed series present",
			Description: "[LIVE-ONLY] kube-state-metrics is deployed → " +
				"kube_job_status_failed{job_name=~\".*-dataload-.*\"}, " +
				"kube_pod_init_container_status_* and kube_deployment_status_replicas_available " +
				"(HC.4 gpfdist) flow into VictoriaMetrics. NO new operator metric is asserted.",
		},
	}
}
