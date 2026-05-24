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
			RequiredLevel: "Basic",
			Description:   "Get query monitoring requires Basic permission",
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
