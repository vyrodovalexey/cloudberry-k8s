package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPhase represents the current phase of the cluster lifecycle.
type ClusterPhase string

const (
	// ClusterPhasePending indicates the cluster is waiting to be created.
	ClusterPhasePending ClusterPhase = "Pending"
	// ClusterPhaseInitializing indicates the cluster is being initialized.
	ClusterPhaseInitializing ClusterPhase = "Initializing"
	// ClusterPhaseRunning indicates the cluster is running and healthy.
	ClusterPhaseRunning ClusterPhase = "Running"
	// ClusterPhaseUpdating indicates the cluster is being updated.
	ClusterPhaseUpdating ClusterPhase = "Updating"
	// ClusterPhaseScaling indicates the cluster is being scaled.
	ClusterPhaseScaling ClusterPhase = "Scaling"
	// ClusterPhaseFailed indicates the cluster has encountered an error.
	ClusterPhaseFailed ClusterPhase = "Failed"
	// ClusterPhaseDeleting indicates the cluster is being deleted.
	ClusterPhaseDeleting ClusterPhase = "Deleting"
	// ClusterPhaseStopped indicates the cluster is stopped.
	ClusterPhaseStopped ClusterPhase = "Stopped"
	// ClusterPhaseStopping indicates the cluster is being stopped.
	ClusterPhaseStopping ClusterPhase = "Stopping"
	// ClusterPhaseRestricted indicates the cluster is running in restricted mode.
	ClusterPhaseRestricted ClusterPhase = "Restricted"
	// ClusterPhaseMaintenance indicates the cluster is running in maintenance mode.
	ClusterPhaseMaintenance ClusterPhase = "Maintenance"
)

// MirroringStatus represents the current mirroring state.
type MirroringStatus string

const (
	// MirroringNotConfigured indicates mirroring is not set up.
	MirroringNotConfigured MirroringStatus = "NotConfigured"
	// MirroringInSync indicates all mirrors are synchronized.
	MirroringInSync MirroringStatus = "InSync"
	// MirroringSyncing indicates mirrors are currently synchronizing.
	MirroringSyncing MirroringStatus = "Syncing"
	// MirroringDegraded indicates one or more mirrors are out of sync.
	MirroringDegraded MirroringStatus = "Degraded"
	// MirroringDown indicates mirroring is completely down.
	MirroringDown MirroringStatus = "Down"
	// MirroringInitializing indicates mirrors are being initialized from primaries.
	MirroringInitializing MirroringStatus = "Initializing"
)

// MirroringLayout represents the mirror placement strategy.
type MirroringLayout string

const (
	// MirroringLayoutGroup places all mirrors for one host on another host.
	MirroringLayoutGroup MirroringLayout = "group"
	// MirroringLayoutSpread distributes mirrors across multiple hosts.
	MirroringLayoutSpread MirroringLayout = "spread"
)

// DeletionPolicy represents the PV reclaim policy on cluster deletion.
type DeletionPolicy string

const (
	// DeletionPolicyRetain keeps PVCs after cluster deletion.
	DeletionPolicyRetain DeletionPolicy = "Retain"
	// DeletionPolicyDelete removes PVCs after cluster deletion.
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

// AntiAffinityType represents the pod anti-affinity strategy.
type AntiAffinityType string

const (
	// AntiAffinityPreferred uses preferred anti-affinity scheduling.
	AntiAffinityPreferred AntiAffinityType = "preferred"
	// AntiAffinityRequired uses required anti-affinity scheduling.
	AntiAffinityRequired AntiAffinityType = "required"
)

// ImagePullPolicy represents the container image pull policy.
type ImagePullPolicy string

const (
	// ImagePullAlways always pulls the image.
	ImagePullAlways ImagePullPolicy = "Always"
	// ImagePullIfNotPresent pulls only if the image is not present.
	ImagePullIfNotPresent ImagePullPolicy = "IfNotPresent"
	// ImagePullNever never pulls the image.
	ImagePullNever ImagePullPolicy = "Never"
)

// HBAType represents the type of HBA rule.
type HBAType string

const (
	// HBATypeLocal matches local socket connections.
	HBATypeLocal HBAType = "local"
	// HBATypeHost matches TCP/IP connections.
	HBATypeHost HBAType = "host"
	// HBATypeHostSSL matches TCP/IP connections with SSL.
	HBATypeHostSSL HBAType = "hostssl"
	// HBATypeHostNoSSL matches TCP/IP connections without SSL.
	HBATypeHostNoSSL HBAType = "hostnossl"
)

// AuthMethod represents the pg_hba.conf authentication method.
type AuthMethod string

const (
	// AuthMethodTrust allows connections without authentication.
	AuthMethodTrust AuthMethod = "trust"
	// AuthMethodReject rejects all connections unconditionally.
	AuthMethodReject AuthMethod = "reject"
	// AuthMethodMD5 uses MD5-hashed password authentication.
	AuthMethodMD5 AuthMethod = "md5"
	// AuthMethodScramSHA256 uses SCRAM-SHA-256 password authentication.
	AuthMethodScramSHA256 AuthMethod = "scram-sha-256"
	// AuthMethodPassword uses plain-text password authentication.
	AuthMethodPassword AuthMethod = "password"
	// AuthMethodIdent uses ident server-based authentication.
	AuthMethodIdent AuthMethod = "ident"
	// AuthMethodPeer uses peer OS user name-based authentication.
	AuthMethodPeer AuthMethod = "peer"
	// AuthMethodGSS uses GSSAPI/Kerberos authentication.
	AuthMethodGSS AuthMethod = "gss"
	// AuthMethodLDAP uses LDAP directory-based authentication.
	AuthMethodLDAP AuthMethod = "ldap"
	// AuthMethodCert uses SSL client certificate authentication.
	AuthMethodCert AuthMethod = "cert"
	// AuthMethodPAM uses PAM (Pluggable Authentication Modules) authentication.
	AuthMethodPAM AuthMethod = "pam"
	// AuthMethodRadius uses RADIUS server-based authentication.
	AuthMethodRadius AuthMethod = "radius"
)

// VaultAuthMethod represents the Vault authentication method.
type VaultAuthMethod string

const (
	// VaultAuthToken uses a static token for authentication.
	VaultAuthToken VaultAuthMethod = "token"
	// VaultAuthKubernetes uses Kubernetes service account authentication.
	VaultAuthKubernetes VaultAuthMethod = "kubernetes"
	// VaultAuthAppRole uses AppRole authentication.
	VaultAuthAppRole VaultAuthMethod = "approle"
)

// OTLPProtocol represents the OTLP exporter protocol.
type OTLPProtocol string

const (
	// OTLPProtocolGRPC uses gRPC for OTLP export.
	OTLPProtocolGRPC OTLPProtocol = "grpc"
	// OTLPProtocolHTTP uses HTTP for OTLP export.
	OTLPProtocolHTTP OTLPProtocol = "http"
)

// RoleClaimSource represents where to extract role claims from.
type RoleClaimSource string

const (
	// RoleClaimSourceIDToken extracts roles from the ID token.
	RoleClaimSourceIDToken RoleClaimSource = "id_token"
	// RoleClaimSourceUserInfo extracts roles from the UserInfo endpoint.
	RoleClaimSourceUserInfo RoleClaimSource = "userinfo"
)

// RoleMatchMode represents how to match IdP roles.
type RoleMatchMode string

const (
	// RoleMatchExact requires an exact role match.
	RoleMatchExact RoleMatchMode = "exact"
	// RoleMatchSuffix matches roles ending with the value.
	RoleMatchSuffix RoleMatchMode = "suffix"
	// RoleMatchPrefix matches roles starting with the value.
	RoleMatchPrefix RoleMatchMode = "prefix"
	// RoleMatchContains matches roles containing the value.
	RoleMatchContains RoleMatchMode = "contains"
)

// ConditionType represents the type of a status condition.
type ConditionType string

const (
	// ConditionClusterReady indicates all components are running and healthy.
	ConditionClusterReady ConditionType = "ClusterReady"
	// ConditionCoordinatorReady indicates the coordinator is running and accepting connections.
	ConditionCoordinatorReady ConditionType = "CoordinatorReady"
	// ConditionStandbyReady indicates the standby coordinator is synced and ready.
	ConditionStandbyReady ConditionType = "StandbyReady"
	// ConditionSegmentsReady indicates all segment pods are running.
	ConditionSegmentsReady ConditionType = "SegmentsReady"
	// ConditionMirroringHealthy indicates all mirrors are in sync.
	ConditionMirroringHealthy ConditionType = "MirroringHealthy"
	// ConditionAuthConfigured indicates authentication is properly configured.
	ConditionAuthConfigured ConditionType = "AuthConfigured"
	// ConditionConfigApplied indicates all configuration parameters are applied.
	ConditionConfigApplied ConditionType = "ConfigApplied"
	// ConditionVaultConnected indicates Vault connection is established.
	ConditionVaultConnected ConditionType = "VaultConnected"
	// ConditionWorkloadConfigured indicates workload management is configured.
	ConditionWorkloadConfigured ConditionType = "WorkloadConfigured"
	// ConditionBackupConfigured indicates backup configuration is applied.
	ConditionBackupConfigured ConditionType = "BackupConfigured"
	// ConditionDataLoadingConfigured indicates data loading configuration is applied.
	ConditionDataLoadingConfigured ConditionType = "DataLoadingConfigured"
	// ConditionStorageConfigured indicates storage management is configured.
	ConditionStorageConfigured ConditionType = "StorageConfigured"
	// ConditionDataRedistribution indicates data redistribution status during scaling.
	ConditionDataRedistribution ConditionType = "DataRedistribution"
	// ConditionScaleOutFailed indicates a scale-out operation has failed.
	ConditionScaleOutFailed ConditionType = "ScaleOutFailed"
	// ConditionUpgradeCompleted indicates a cluster upgrade has completed successfully.
	ConditionUpgradeCompleted ConditionType = "UpgradeCompleted"
	// ConditionUpgradeFailed indicates a cluster upgrade has failed and was rolled back.
	ConditionUpgradeFailed ConditionType = "UpgradeFailed"
	// ConditionStorageExpanded indicates PVC storage has been expanded.
	ConditionStorageExpanded ConditionType = "StorageExpanded"
)

// Event reason constants for use in recorder.Event() calls.
const (
	// EventReasonScaleOutStarted indicates a scale-out operation has started.
	EventReasonScaleOutStarted = "ScaleOutStarted"
	// EventReasonScaleOutCompleted indicates a scale-out operation has completed.
	EventReasonScaleOutCompleted = "ScaleOutCompleted"
	// EventReasonScaleOutFailed indicates a scale-out operation has failed.
	EventReasonScaleOutFailed = "ScaleOutFailed"
	// EventReasonScaleInStarted indicates a scale-in operation has started.
	EventReasonScaleInStarted = "ScaleInStarted"
	// EventReasonScaleInCompleted indicates a scale-in operation has completed.
	EventReasonScaleInCompleted = "ScaleInCompleted"
	// EventReasonUpgradeStarted indicates a cluster upgrade has started.
	EventReasonUpgradeStarted = "UpgradeStarted"
	// EventReasonUpgradeCompleted indicates a cluster upgrade has completed.
	EventReasonUpgradeCompleted = "UpgradeCompleted"
	// EventReasonUpgradeRollback indicates a cluster upgrade has been rolled back.
	EventReasonUpgradeRollback = "UpgradeRollback"
	// EventReasonRebalanceStarted indicates a segment rebalance has started.
	EventReasonRebalanceStarted = "RebalanceStarted"
	// EventReasonRebalanceCompleted indicates a segment rebalance has completed.
	EventReasonRebalanceCompleted = "RebalanceCompleted"
	// EventReasonStorageExpanded indicates PVC storage has been expanded.
	EventReasonStorageExpanded = "StorageExpanded"
	// EventReasonWorkloadReconciled indicates workload management has been reconciled.
	EventReasonWorkloadReconciled = "WorkloadReconciled"
	// EventReasonWorkloadDisabled indicates workload management has been disabled.
	EventReasonWorkloadDisabled = "WorkloadDisabled"
	// EventReasonQueryMonitoringReconciled indicates query monitoring has been reconciled.
	EventReasonQueryMonitoringReconciled = "QueryMonitoringReconciled"
	// EventReasonBackupReconciled indicates backup configuration has been reconciled.
	EventReasonBackupReconciled = "BackupReconciled"
	// EventReasonDataLoadingReconciled indicates data loading has been reconciled.
	EventReasonDataLoadingReconciled = "DataLoadingReconciled"
	// EventReasonStorageReconciled indicates storage management has been reconciled.
	EventReasonStorageReconciled = "StorageReconciled"
	// EventReasonConfigReloaded indicates configuration has been reloaded.
	EventReasonConfigReloaded = "ConfigReloaded"
	// EventReasonRollingRestartStarted indicates a rolling restart has started.
	EventReasonRollingRestartStarted = "RollingRestartStarted"
	// EventReasonRollingRestartCompleted indicates a rolling restart has completed.
	EventReasonRollingRestartCompleted = "RollingRestartCompleted"
	// EventReasonMaintenanceStarted indicates a maintenance operation has started.
	EventReasonMaintenanceStarted = "MaintenanceStarted"
	// EventReasonMaintenanceCompleted indicates a maintenance operation has completed.
	EventReasonMaintenanceCompleted = "MaintenanceCompleted"
	// EventReasonMaintenanceUnknown indicates an unknown maintenance operation was requested.
	EventReasonMaintenanceUnknown = "MaintenanceUnknown"
	// EventReasonMirroringEnabled indicates mirroring has been enabled on the cluster.
	EventReasonMirroringEnabled = "MirroringEnabled"
	// EventReasonMirroringDisabled indicates mirroring has been disabled on the cluster.
	EventReasonMirroringDisabled = "MirroringDisabled"
	// EventReasonMirroringInitializing indicates mirror initialization is in progress.
	EventReasonMirroringInitializing = "MirroringInitializing"
	// EventReasonMirroringInSync indicates all mirrors are synchronized.
	EventReasonMirroringInSync = "MirroringInSync"
	// EventReasonMirroringFailed indicates a mirroring operation has failed.
	EventReasonMirroringFailed = "MirroringFailed"
	// EventReasonSegmentFailover indicates a segment failover has been triggered.
	EventReasonSegmentFailover = "SegmentFailover"
	// EventReasonSegmentFailoverCompleted indicates a segment failover has completed.
	EventReasonSegmentFailoverCompleted = "SegmentFailoverCompleted"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Segments",type=integer,JSONPath=`.status.segmentsReady`
// +kubebuilder:printcolumn:name="Mirroring",type=string,JSONPath=`.status.mirroringStatus`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CloudberryCluster is the Schema for the cloudberryclusters API.
type CloudberryCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CloudberryClusterSpec   `json:"spec,omitempty"`
	Status CloudberryClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CloudberryClusterList contains a list of CloudberryCluster.
type CloudberryClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CloudberryCluster `json:"items"`
}

// CloudberryClusterSpec defines the desired state of CloudberryCluster.
type CloudberryClusterSpec struct {
	// Version is the Cloudberry DB version.
	// +kubebuilder:default="2.1.0"
	// +optional
	Version string `json:"version,omitempty"`

	// Image is the container image for Cloudberry DB.
	// +kubebuilder:default="cloudberrydb/cloudberry:2.1.0"
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy defines when to pull the container image.
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default="IfNotPresent"
	// +optional
	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of references to secrets for pulling images.
	// +optional
	ImagePullSecrets []ImagePullSecret `json:"imagePullSecrets,omitempty"`

	// Coordinator defines the coordinator node configuration.
	Coordinator CoordinatorSpec `json:"coordinator"`

	// Standby defines the standby coordinator configuration.
	// +optional
	Standby *StandbySpec `json:"standby,omitempty"`

	// Segments defines the segment nodes configuration.
	Segments SegmentsSpec `json:"segments"`

	// Auth defines authentication and authorization configuration.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// Config defines cluster configuration parameters.
	// +optional
	Config *ConfigSpec `json:"config,omitempty"`

	// HA defines high availability configuration.
	// +optional
	HA *HASpec `json:"ha,omitempty"`

	// Vault defines HashiCorp Vault integration configuration.
	// +optional
	Vault *VaultSpec `json:"vault,omitempty"`

	// Monitoring defines monitoring configuration.
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring,omitempty"`

	// Telemetry defines OTLP telemetry configuration.
	// +optional
	Telemetry *TelemetrySpec `json:"telemetry,omitempty"`

	// Workload defines workload management configuration.
	// +optional
	Workload *WorkloadSpec `json:"workload,omitempty"`

	// QueryMonitoring defines query monitoring and analysis configuration.
	// +optional
	QueryMonitoring *QueryMonitoringSpec `json:"queryMonitoring,omitempty"`

	// Backup defines backup and restore configuration.
	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`

	// DataLoading defines data loading configuration.
	// +optional
	DataLoading *DataLoadingSpec `json:"dataLoading,omitempty"`

	// Storage defines storage management configuration.
	// +optional
	Storage *StorageManagementSpec `json:"storage,omitempty"`

	// DeletionPolicy defines the PV reclaim policy on cluster deletion.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default="Retain"
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// BackupOnDelete triggers a backup before cluster deletion.
	// +kubebuilder:default=false
	// +optional
	BackupOnDelete bool `json:"backupOnDelete,omitempty"`
}

// ImagePullSecret references a Kubernetes secret for image pulling.
type ImagePullSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`
}

// CoordinatorSpec defines the coordinator node configuration.
type CoordinatorSpec struct {
	// Replicas is the number of coordinator replicas (always 1).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources defines compute resource requirements.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`

	// Storage defines storage configuration.
	Storage StorageSpec `json:"storage"`

	// NodeSelector constrains scheduling to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow scheduling on tainted nodes.
	// +optional
	Tolerations []Toleration `json:"tolerations,omitempty"`

	// Port is the coordinator listening port.
	// +kubebuilder:default=5432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`
}

// StandbySpec defines the standby coordinator configuration.
type StandbySpec struct {
	// Enabled controls whether a standby coordinator is deployed.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Resources defines compute resource requirements.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`

	// Storage defines storage configuration.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// NodeSelector constrains scheduling to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// SegmentsSpec defines the segment nodes configuration.
type SegmentsSpec struct {
	// Count is the total number of primary segments.
	// +kubebuilder:validation:Minimum=1
	Count int32 `json:"count"`

	// PrimariesPerHost is the number of primary segments per host.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	PrimariesPerHost int32 `json:"primariesPerHost,omitempty"`

	// Mirroring defines segment mirroring configuration.
	// +optional
	Mirroring *MirroringSpec `json:"mirroring,omitempty"`

	// Rebalance defines segment rebalancing configuration.
	// +optional
	Rebalance *RebalanceSpec `json:"rebalance,omitempty"`

	// Resources defines compute resource requirements.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`

	// Storage defines storage configuration.
	Storage StorageSpec `json:"storage"`

	// NodeSelector constrains scheduling to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow scheduling on tainted nodes.
	// +optional
	Tolerations []Toleration `json:"tolerations,omitempty"`

	// AntiAffinity defines the pod anti-affinity strategy.
	// +kubebuilder:validation:Enum=preferred;required
	// +kubebuilder:default="preferred"
	// +optional
	AntiAffinity AntiAffinityType `json:"antiAffinity,omitempty"`
}

// RebalanceSpec defines segment rebalancing configuration.
type RebalanceSpec struct {
	// SkewThreshold is the percentage skew threshold to trigger rebalance.
	// +kubebuilder:default=10
	// +optional
	SkewThreshold int32 `json:"skewThreshold,omitempty"`

	// Parallelism is the number of tables to redistribute concurrently.
	// +kubebuilder:default=2
	// +optional
	Parallelism int32 `json:"parallelism,omitempty"`

	// ExcludeTables lists tables to skip during rebalance (supports glob patterns).
	// +optional
	ExcludeTables []string `json:"excludeTables,omitempty"`
}

// MirroringSpec defines segment mirroring configuration.
type MirroringSpec struct {
	// Enabled controls whether segment mirroring is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled"`

	// Layout defines the mirror placement strategy.
	// +kubebuilder:validation:Enum=group;spread
	// +kubebuilder:default="group"
	// +optional
	Layout MirroringLayout `json:"layout,omitempty"`
}

// AuthSpec defines authentication and authorization configuration.
type AuthSpec struct {
	// Basic defines basic authentication configuration.
	// +optional
	Basic *BasicAuthSpec `json:"basic,omitempty"`

	// OIDC defines OpenID Connect authentication configuration.
	// +optional
	OIDC *OIDCSpec `json:"oidc,omitempty"`

	// HBARules defines pg_hba.conf rules.
	// +optional
	HBARules []HBARule `json:"hbaRules,omitempty"`

	// SSL defines TLS/SSL configuration.
	// +optional
	SSL *SSLSpec `json:"ssl,omitempty"`
}

// BasicAuthSpec defines basic authentication configuration.
type BasicAuthSpec struct {
	// Enabled controls whether basic authentication is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// AdminUser is the admin username.
	// +kubebuilder:default="gpadmin"
	// +optional
	AdminUser string `json:"adminUser,omitempty"`

	// AdminPasswordSecret references the secret containing the admin password.
	// +optional
	AdminPasswordSecret *SecretKeyRef `json:"adminPasswordSecret,omitempty"`
}

// OIDCSpec defines OpenID Connect authentication configuration.
type OIDCSpec struct {
	// Enabled controls whether OIDC authentication is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// IssuerURL is the OIDC issuer URL.
	// +optional
	IssuerURL string `json:"issuerURL,omitempty"`

	// ClientID is the OIDC client identifier.
	// +optional
	ClientID string `json:"clientID,omitempty"`

	// ClientSecret references the secret containing the OIDC client secret.
	// +optional
	ClientSecret *OIDCSecretRef `json:"clientSecret,omitempty"`

	// Scopes defines the OIDC scopes to request.
	// +optional
	Scopes []string `json:"scopes,omitempty"`

	// RoleClaimPath is the JSON path to extract roles from the token.
	// +kubebuilder:default="realm_access.roles"
	// +optional
	RoleClaimPath string `json:"roleClaimPath,omitempty"`

	// RoleClaimSource defines where to extract role claims from.
	// +kubebuilder:validation:Enum=id_token;userinfo
	// +kubebuilder:default="id_token"
	// +optional
	RoleClaimSource RoleClaimSource `json:"roleClaimSource,omitempty"`

	// RoleMatchMode defines how to match IdP roles.
	// +kubebuilder:validation:Enum=exact;suffix;prefix;contains
	// +kubebuilder:default="exact"
	// +optional
	RoleMatchMode RoleMatchMode `json:"roleMatchMode,omitempty"`

	// RoleMapping maps IdP roles to permission levels.
	// +optional
	RoleMapping map[string]string `json:"roleMapping,omitempty"`

	// PKCE enables Proof Key for Code Exchange.
	// +kubebuilder:default=true
	// +optional
	PKCE bool `json:"pkce,omitempty"`

	// AllowLocalSignIn allows local sign-in when OIDC is enabled.
	// +kubebuilder:default=true
	// +optional
	AllowLocalSignIn bool `json:"allowLocalSignIn,omitempty"`
}

// OIDCSecretRef references the OIDC client secret.
type OIDCSecretRef struct {
	// SecretRef references a Kubernetes secret.
	SecretRef *SecretKeyRef `json:"secretRef,omitempty"`
}

// SecretKeyRef references a key in a Kubernetes secret.
type SecretKeyRef struct {
	// Name is the name of the secret.
	Name string `json:"name"`
	// Key is the key within the secret.
	Key string `json:"key"`
}

// HBARule defines a pg_hba.conf rule.
type HBARule struct {
	// Type is the connection type.
	// +kubebuilder:validation:Enum=local;host;hostssl;hostnossl
	Type HBAType `json:"type"`

	// Database is the target database.
	Database string `json:"database"`

	// User is the target user.
	User string `json:"user"`

	// Address is the client address (CIDR notation).
	// +optional
	Address string `json:"address,omitempty"`

	// Method is the authentication method.
	Method AuthMethod `json:"method"`

	// Options are additional authentication options.
	// +optional
	Options string `json:"options,omitempty"`
}

// SSLSpec defines TLS/SSL configuration.
type SSLSpec struct {
	// Enabled controls whether SSL is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// CertSecret references the TLS certificate secret.
	// +optional
	CertSecret *CertSecretRef `json:"certSecret,omitempty"`

	// MinTLSVersion is the minimum TLS version.
	// +kubebuilder:validation:Enum="1.2";"1.3"
	// +kubebuilder:default="1.2"
	// +optional
	MinTLSVersion string `json:"minTLSVersion,omitempty"`
}

// CertSecretRef references a TLS certificate secret.
type CertSecretRef struct {
	// Name is the name of the TLS secret.
	Name string `json:"name"`
}

// ConfigSpec defines cluster configuration parameters.
type ConfigSpec struct {
	// Parameters are cluster-wide postgresql.conf parameters.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// CoordinatorParameters are coordinator-only parameters.
	// +optional
	CoordinatorParameters map[string]string `json:"coordinatorParameters,omitempty"`

	// DatabaseParameters are per-database parameters.
	// +optional
	DatabaseParameters map[string]map[string]string `json:"databaseParameters,omitempty"`

	// RoleParameters are per-role parameters.
	// +optional
	RoleParameters map[string]map[string]string `json:"roleParameters,omitempty"`
}

// HASpec defines high availability configuration.
type HASpec struct {
	// FTSProbeInterval is the FTS probe interval in seconds.
	// +kubebuilder:default=60
	// +optional
	FTSProbeInterval int32 `json:"ftsProbeInterval,omitempty"`

	// FTSProbeTimeout is the FTS probe timeout in seconds.
	// +kubebuilder:default=20
	// +optional
	FTSProbeTimeout int32 `json:"ftsProbeTimeout,omitempty"`

	// FTSProbeRetries is the FTS probe retry count.
	// +kubebuilder:default=5
	// +optional
	FTSProbeRetries int32 `json:"ftsProbeRetries,omitempty"`

	// Checksums enables storage-layer checksums.
	// +kubebuilder:default=true
	// +optional
	Checksums bool `json:"checksums,omitempty"`
}

// VaultSpec defines HashiCorp Vault integration configuration.
type VaultSpec struct {
	// Enabled controls whether Vault integration is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Address is the Vault server address.
	// +optional
	Address string `json:"address,omitempty"`

	// AuthMethod is the Vault authentication method.
	// +kubebuilder:validation:Enum=token;kubernetes;approle
	// +kubebuilder:default="kubernetes"
	// +optional
	AuthMethod VaultAuthMethod `json:"authMethod,omitempty"`

	// AuthPath is the Vault auth mount path.
	// +optional
	AuthPath string `json:"authPath,omitempty"`

	// Role is the Vault role name.
	// +optional
	Role string `json:"role,omitempty"`

	// SecretPath is the Vault secret path.
	// +optional
	SecretPath string `json:"secretPath,omitempty"`

	// TLSSecret references the Vault TLS secret.
	// +optional
	TLSSecret *VaultTLSSecret `json:"tlsSecret,omitempty"`
}

// VaultTLSSecret references a Vault TLS secret.
type VaultTLSSecret struct {
	// Name is the name of the TLS secret.
	Name string `json:"name"`
}

// MonitoringSpec defines monitoring configuration.
type MonitoringSpec struct {
	// Enabled controls whether monitoring is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MetricsPort is the Prometheus metrics port.
	// +kubebuilder:default=9187
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	MetricsPort int32 `json:"metricsPort,omitempty"`

	// ServiceMonitor controls whether a ServiceMonitor is created.
	// +kubebuilder:default=false
	// +optional
	ServiceMonitor bool `json:"serviceMonitor,omitempty"`
}

// TelemetrySpec defines OTLP telemetry configuration.
type TelemetrySpec struct {
	// Enabled controls whether telemetry is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// OTLPEndpoint is the OTLP collector endpoint.
	// +optional
	OTLPEndpoint string `json:"otlpEndpoint,omitempty"`

	// OTLPProtocol is the OTLP exporter protocol.
	// +kubebuilder:validation:Enum=grpc;http
	// +kubebuilder:default="grpc"
	// +optional
	OTLPProtocol OTLPProtocol `json:"otlpProtocol,omitempty"`

	// SamplingRate is the trace sampling rate (0.0 to 1.0).
	// +kubebuilder:default=1
	// +optional
	SamplingRate float64 `json:"samplingRate,omitempty"`
}

// ResourceRequirements defines compute resource requirements.
type ResourceRequirements struct {
	// Requests defines minimum resource requirements.
	// +optional
	Requests *ResourceList `json:"requests,omitempty"`

	// Limits defines maximum resource limits.
	// +optional
	Limits *ResourceList `json:"limits,omitempty"`
}

// ResourceList defines CPU and memory resources.
type ResourceList struct {
	// CPU is the CPU resource quantity.
	// +optional
	CPU string `json:"cpu,omitempty"`

	// Memory is the memory resource quantity.
	// +optional
	Memory string `json:"memory,omitempty"`
}

// StorageSpec defines storage configuration.
type StorageSpec struct {
	// StorageClass is the Kubernetes storage class name.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// Size is the storage volume size.
	// +kubebuilder:default="10Gi"
	// +optional
	Size string `json:"size,omitempty"`
}

// Toleration defines a Kubernetes toleration.
type Toleration struct {
	// Key is the taint key.
	// +optional
	Key string `json:"key,omitempty"`

	// Operator is the toleration operator.
	// +optional
	Operator string `json:"operator,omitempty"`

	// Value is the taint value.
	// +optional
	Value string `json:"value,omitempty"`

	// Effect is the taint effect.
	// +optional
	Effect string `json:"effect,omitempty"`

	// TolerationSeconds is the toleration duration.
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty"`
}

// WorkloadSpec defines workload management configuration.
type WorkloadSpec struct {
	// Enabled controls whether workload management is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ResourceGroups defines the resource groups for workload management.
	// +optional
	ResourceGroups []ResourceGroupSpec `json:"resourceGroups,omitempty"`

	// Rules defines workload management rules.
	// +optional
	Rules []WorkloadRule `json:"rules,omitempty"`

	// IdleRules defines idle session termination rules.
	// +optional
	IdleRules []IdleSessionRule `json:"idleRules,omitempty"`
}

// TablespaceIOLimitSpec defines per-tablespace disk I/O limits for a resource group.
// Applied via ALTER RESOURCE GROUP ... SET io_limit.
type TablespaceIOLimitSpec struct {
	// Tablespace is the target tablespace name. Use "*" for all tablespaces.
	Tablespace string `json:"tablespace"`
	// ReadBytesPerSec is the maximum read throughput in bytes per second.
	// +optional
	ReadBytesPerSec int64 `json:"readBytesPerSec,omitempty"`
	// WriteBytesPerSec is the maximum write throughput in bytes per second.
	// +optional
	WriteBytesPerSec int64 `json:"writeBytesPerSec,omitempty"`
	// ReadIOPS is the maximum read I/O operations per second.
	// +optional
	ReadIOPS int32 `json:"readIOPS,omitempty"`
	// WriteIOPS is the maximum write I/O operations per second.
	// +optional
	WriteIOPS int32 `json:"writeIOPS,omitempty"`
}

// ResourceGroupSpec defines a resource group for workload management.
type ResourceGroupSpec struct {
	// Name is the resource group name.
	Name string `json:"name"`

	// Concurrency is the maximum number of concurrent transactions.
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`

	// CPUMaxPercent is the maximum CPU usage percentage.
	// +optional
	CPUMaxPercent int32 `json:"cpuMaxPercent,omitempty"`

	// CPUWeight is the CPU scheduling weight.
	// +optional
	CPUWeight int32 `json:"cpuWeight,omitempty"`

	// MemoryLimit is the memory limit in MB.
	// +optional
	MemoryLimit int32 `json:"memoryLimit,omitempty"`

	// MinCost is the minimum query cost to be managed.
	// +optional
	MinCost int32 `json:"minCost,omitempty"`

	// IOLimits defines per-tablespace disk I/O limits.
	// +optional
	IOLimits []TablespaceIOLimitSpec `json:"ioLimits,omitempty"`
}

// WorkloadRule defines a workload management rule.
type WorkloadRule struct {
	// Name is the rule name.
	Name string `json:"name"`

	// Enabled controls whether the rule is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ResourceGroup is the target resource group.
	// +optional
	ResourceGroup string `json:"resourceGroup,omitempty"`

	// QueryTag is the query tag to match.
	// +optional
	QueryTag string `json:"queryTag,omitempty"`

	// Role is the database role to match.
	// +optional
	Role string `json:"role,omitempty"`

	// Action is the action to take (cancel, move, log).
	// +kubebuilder:validation:Enum=cancel;move;log
	Action string `json:"action"`

	// MoveTarget is the target resource group for move actions.
	// +optional
	MoveTarget string `json:"moveTarget,omitempty"`

	// Threshold is the threshold value for the rule.
	// +optional
	Threshold string `json:"threshold,omitempty"`

	// ThresholdType is the type of threshold (cpu_skew, cpu_time, running_time, spill_size, etc.).
	// +optional
	ThresholdType string `json:"thresholdType,omitempty"`

	// Priority is the rule evaluation priority.
	// +optional
	Priority int32 `json:"priority,omitempty"`
}

// IdleSessionRule defines an idle session termination rule.
type IdleSessionRule struct {
	// Name is the rule name.
	Name string `json:"name"`

	// Enabled controls whether the rule is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ResourceGroup is the target resource group.
	ResourceGroup string `json:"resourceGroup"`

	// IdleTimeout is the idle timeout duration (e.g. "30m", "1h").
	IdleTimeout string `json:"idleTimeout"`

	// ExcludeInTransaction excludes sessions that are in a transaction.
	// +kubebuilder:default=false
	// +optional
	ExcludeInTransaction bool `json:"excludeInTransaction,omitempty"`

	// TerminateMessage is the message sent to the client on termination.
	// +optional
	TerminateMessage string `json:"terminateMessage,omitempty"`
}

// QueryMonitoringSpec defines query monitoring and analysis configuration.
type QueryMonitoringSpec struct {
	// Enabled controls whether query monitoring is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// HistoryRetention is the query history retention period (e.g. "30d", "90d").
	// +optional
	HistoryRetention string `json:"historyRetention,omitempty"`

	// SamplingInterval is the metrics sampling interval in seconds.
	// +optional
	SamplingInterval int32 `json:"samplingInterval,omitempty"`

	// GuestAccess allows guest access to query monitoring data.
	// +kubebuilder:default=false
	// +optional
	GuestAccess bool `json:"guestAccess,omitempty"`

	// PlanCollection enables query plan collection.
	// +kubebuilder:default=false
	// +optional
	PlanCollection bool `json:"planCollection,omitempty"`

	// SlowQueryThreshold is the threshold for slow query detection (e.g. "1000ms").
	// +optional
	SlowQueryThreshold string `json:"slowQueryThreshold,omitempty"`

	// Exporters defines the monitoring exporters configuration.
	// +optional
	Exporters *QueryMonitoringExportersSpec `json:"exporters,omitempty"`
}

// QueryMonitoringExportersSpec defines the monitoring exporters for query monitoring.
type QueryMonitoringExportersSpec struct {
	// PostgresExporter configures the Prometheus postgres_exporter sidecar.
	// +optional
	PostgresExporter *ExporterSpec `json:"postgresExporter,omitempty"`

	// NodeExporter configures the Prometheus node_exporter sidecar.
	// +optional
	NodeExporter *ExporterSpec `json:"nodeExporter,omitempty"`

	// CloudberryQueryExporter configures the Cloudberry query exporter sidecar.
	// +optional
	CloudberryQueryExporter *ExporterSpec `json:"cloudberryQueryExporter,omitempty"`

	// ServiceMonitor configures the Prometheus ServiceMonitor resource.
	// +optional
	ServiceMonitor *QueryServiceMonitorSpec `json:"serviceMonitor,omitempty"`

	// PrometheusRule configures the Prometheus PrometheusRule resource.
	// +optional
	PrometheusRule *QueryPrometheusRuleSpec `json:"prometheusRule,omitempty"`
}

// ExporterSpec defines the configuration for a monitoring exporter sidecar.
type ExporterSpec struct {
	// Enabled controls whether this exporter is deployed.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Image is the container image for the exporter.
	// +optional
	Image string `json:"image,omitempty"`

	// Port is the metrics port exposed by the exporter.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// Resources defines the compute resource requirements for the exporter.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`

	// Segments enables deploying this postgres-exporter as a sidecar in each primary segment pod.
	// Default false. Increases scrape targets and metric cardinality (one exporter per segment);
	// intended for deep per-segment diagnostics.
	// +optional
	// +kubebuilder:default=false
	Segments bool `json:"segments,omitempty"`

	// Mirrors enables deploying this postgres-exporter as a sidecar in each MIRROR
	// segment pod. Default false. Mirror segments are in WAL-replay recovery; the
	// exporter connects in utility mode and primarily yields recovery/replication
	// metrics. Increases scrape targets and cardinality (one exporter per mirror).
	// +optional
	// +kubebuilder:default=false
	Mirrors bool `json:"mirrors,omitempty"`
}

// QueryServiceMonitorSpec defines the Prometheus ServiceMonitor configuration
// for query monitoring exporters.
type QueryServiceMonitorSpec struct {
	// Enabled controls whether a ServiceMonitor resource is created.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Namespace is the namespace for the ServiceMonitor. Defaults to the cluster namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Interval is the Prometheus scrape interval.
	// +optional
	Interval string `json:"interval,omitempty"`

	// ScrapeTimeout is the Prometheus scrape timeout.
	// +optional
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`

	// Labels are additional labels to add to the ServiceMonitor.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// QueryPrometheusRuleSpec defines the Prometheus PrometheusRule configuration
// for query monitoring alerts.
type QueryPrometheusRuleSpec struct {
	// Enabled controls whether a PrometheusRule resource is created.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Namespace is the namespace for the PrometheusRule. Defaults to the cluster namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Labels are additional labels to add to the PrometheusRule.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// BackupSpec defines backup and restore configuration.
type BackupSpec struct {
	// Enabled controls whether backup is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is the cron expression for scheduled backups.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// Retention defines backup retention policy.
	// +optional
	Retention BackupRetention `json:"retention,omitempty"`

	// Destination defines where backups are stored.
	Destination BackupDestination `json:"destination"`

	// Compression is the compression level (0-9).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=9
	// +optional
	Compression int32 `json:"compression,omitempty"`

	// Parallelism is the number of parallel backup workers.
	// +optional
	Parallelism int32 `json:"parallelism,omitempty"`

	// Incremental enables incremental backups.
	// +kubebuilder:default=false
	// +optional
	Incremental bool `json:"incremental,omitempty"`
}

// BackupRetention defines backup retention policy.
type BackupRetention struct {
	// FullCount is the number of full backups to retain.
	// +optional
	FullCount int32 `json:"fullCount,omitempty"`

	// IncrementalCount is the number of incremental backups to retain.
	// +optional
	IncrementalCount int32 `json:"incrementalCount,omitempty"`

	// MaxAge is the maximum age of backups to retain (e.g. "30d").
	// +optional
	MaxAge string `json:"maxAge,omitempty"`
}

// BackupDestination defines where backups are stored.
type BackupDestination struct {
	// Type is the destination type (s3, local).
	// +kubebuilder:validation:Enum=s3;local
	Type string `json:"type"`

	// Bucket is the S3 bucket name.
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// Endpoint is the S3-compatible endpoint (for MinIO).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Region is the S3 region.
	// +optional
	Region string `json:"region,omitempty"`

	// Path is the storage path prefix.
	// +optional
	Path string `json:"path,omitempty"`

	// CredentialSecret references the secret containing storage credentials.
	// +optional
	CredentialSecret *SecretReference `json:"credentialSecret,omitempty"`

	// ForcePathStyle enables path-style addressing for S3-compatible storage.
	// +kubebuilder:default=false
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
}

// SecretReference references a Kubernetes secret.
type SecretReference struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// Key is the key within the secret.
	// +optional
	Key string `json:"key,omitempty"`
}

// DataLoadingSpec defines data loading configuration.
type DataLoadingSpec struct {
	// Enabled controls whether data loading is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// StreamingServer defines the streaming server configuration.
	// +optional
	StreamingServer *StreamingServerSpec `json:"streamingServer,omitempty"`

	// Jobs defines data loading job configurations.
	// +optional
	Jobs []DataLoadingJob `json:"jobs,omitempty"`
}

// StreamingServerSpec defines the streaming server configuration.
type StreamingServerSpec struct {
	// Host is the streaming server hostname.
	Host string `json:"host"`

	// Port is the streaming server port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// TLSMode defines the TLS mode (none, tls, skip-verify).
	// +kubebuilder:validation:Enum=none;tls;skip-verify
	// +kubebuilder:default="none"
	// +optional
	TLSMode string `json:"tlsMode,omitempty"`

	// CredentialSecret references the secret containing server credentials.
	// +optional
	CredentialSecret *SecretReference `json:"credentialSecret,omitempty"`
}

// DataLoadingJob defines a data loading job configuration.
type DataLoadingJob struct {
	// Name is the job name.
	Name string `json:"name"`

	// Type is the source type (s3, kafka, rabbitmq).
	// +kubebuilder:validation:Enum=s3;kafka;rabbitmq
	Type string `json:"type"`

	// Enabled controls whether the job is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is the cron expression for scheduled jobs.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// TargetTable is the target database table.
	TargetTable string `json:"targetTable"`

	// S3Source defines the S3 source configuration.
	// +optional
	S3Source *S3SourceSpec `json:"s3Source,omitempty"`

	// KafkaSource defines the Kafka source configuration.
	// +optional
	KafkaSource *KafkaSourceSpec `json:"kafkaSource,omitempty"`

	// RabbitMQSource defines the RabbitMQ source configuration.
	// +optional
	RabbitMQSource *RabbitMQSourceSpec `json:"rabbitMQSource,omitempty"`
}

// S3SourceSpec defines an S3 data source.
type S3SourceSpec struct {
	// Bucket is the S3 bucket name.
	Bucket string `json:"bucket"`

	// Path is the object path prefix.
	// +optional
	Path string `json:"path,omitempty"`

	// Endpoint is the S3-compatible endpoint.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Region is the S3 region.
	// +optional
	Region string `json:"region,omitempty"`

	// Format is the data format (csv, json, avro).
	// +kubebuilder:validation:Enum=csv;json;avro
	// +optional
	Format string `json:"format,omitempty"`

	// CredentialSecret references the secret containing S3 credentials.
	// +optional
	CredentialSecret *SecretReference `json:"credentialSecret,omitempty"`

	// ForcePathStyle enables path-style addressing for S3-compatible storage.
	// +kubebuilder:default=false
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
}

// KafkaSourceSpec defines a Kafka data source.
type KafkaSourceSpec struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string `json:"brokers"`

	// Topic is the Kafka topic to consume from.
	Topic string `json:"topic"`

	// GroupID is the consumer group ID.
	// +optional
	GroupID string `json:"groupId,omitempty"`

	// Format is the message format (json, avro, csv).
	// +kubebuilder:validation:Enum=json;avro;csv
	// +optional
	Format string `json:"format,omitempty"`

	// StartOffset defines where to start consuming (earliest, latest).
	// +kubebuilder:validation:Enum=earliest;latest
	// +kubebuilder:default="earliest"
	// +optional
	StartOffset string `json:"startOffset,omitempty"`
}

// RabbitMQSourceSpec defines a RabbitMQ data source.
type RabbitMQSourceSpec struct {
	// Host is the RabbitMQ hostname.
	Host string `json:"host"`

	// Port is the RabbitMQ port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// VHost is the RabbitMQ virtual host.
	// +optional
	VHost string `json:"vhost,omitempty"`

	// Queue is the RabbitMQ queue name.
	Queue string `json:"queue"`

	// Format is the message format (json, avro, csv).
	// +kubebuilder:validation:Enum=json;avro;csv
	// +optional
	Format string `json:"format,omitempty"`

	// CredentialSecret references the secret containing RabbitMQ credentials.
	// +optional
	CredentialSecret *SecretReference `json:"credentialSecret,omitempty"`
}

// StorageManagementSpec defines storage management configuration.
type StorageManagementSpec struct {
	// DiskMonitoring enables disk usage monitoring.
	// +kubebuilder:default=false
	// +optional
	DiskMonitoring bool `json:"diskMonitoring,omitempty"`

	// RecommendationScan defines recommendation scanning configuration.
	// +optional
	RecommendationScan *RecommendationScanSpec `json:"recommendationScan,omitempty"`

	// UsageReport defines usage reporting configuration.
	// +optional
	UsageReport *UsageReportSpec `json:"usageReport,omitempty"`
}

// RecommendationScanSpec defines recommendation scanning configuration.
type RecommendationScanSpec struct {
	// Enabled controls whether recommendation scanning is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is the cron expression for scheduled scans.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// BloatThreshold is the dead tuple percentage threshold.
	// +optional
	BloatThreshold int32 `json:"bloatThreshold,omitempty"`

	// SkewThreshold is the skew coefficient percentage threshold.
	// +optional
	SkewThreshold int32 `json:"skewThreshold,omitempty"`

	// AgeThreshold is the XID age threshold.
	// +optional
	AgeThreshold int64 `json:"ageThreshold,omitempty"`

	// IndexBloatThreshold is the index bloat percentage threshold.
	// +optional
	IndexBloatThreshold int32 `json:"indexBloatThreshold,omitempty"`

	// ScanDuration is the maximum scan duration (e.g. "2h").
	// +optional
	ScanDuration string `json:"scanDuration,omitempty"`
}

// UsageReportSpec defines usage reporting configuration.
type UsageReportSpec struct {
	// Enabled controls whether usage reporting is active.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Monthly enables monthly usage reports.
	// +kubebuilder:default=false
	// +optional
	Monthly bool `json:"monthly,omitempty"`
}

// CloudberryClusterStatus defines the observed state of CloudberryCluster.
type CloudberryClusterStatus struct {
	// Phase is the current cluster phase.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// CoordinatorReady indicates whether the coordinator is ready.
	// +optional
	CoordinatorReady bool `json:"coordinatorReady,omitempty"`

	// StandbyReady indicates whether the standby coordinator is ready.
	// +optional
	StandbyReady bool `json:"standbyReady,omitempty"`

	// SegmentsReady is the number of ready segments.
	// +optional
	SegmentsReady int32 `json:"segmentsReady,omitempty"`

	// SegmentsTotal is the total number of segments.
	// +optional
	SegmentsTotal int32 `json:"segmentsTotal,omitempty"`

	// MirroringStatus is the current mirroring state.
	// +optional
	MirroringStatus MirroringStatus `json:"mirroringStatus,omitempty"`

	// ClusterVersion is the running cluster version.
	// +optional
	ClusterVersion string `json:"clusterVersion,omitempty"`

	// LastReconcileTime is the timestamp of the last reconciliation.
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// LastConfigChangeTime is the timestamp of the last configuration change.
	// +optional
	LastConfigChangeTime *metav1.Time `json:"lastConfigChangeTime,omitempty"`

	// ActiveQueries is the number of currently active queries.
	// +optional
	ActiveQueries int32 `json:"activeQueries,omitempty"`

	// QueuedQueries is the number of currently queued queries.
	// +optional
	QueuedQueries int32 `json:"queuedQueries,omitempty"`

	// BlockedQueries is the number of currently blocked queries.
	// +optional
	BlockedQueries int32 `json:"blockedQueries,omitempty"`

	// LastBackupTime is the timestamp of the last backup.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// LastBackupStatus is the status of the last backup (Success, Failed, InProgress).
	// +optional
	LastBackupStatus string `json:"lastBackupStatus,omitempty"`

	// DataLoadingJobs is the number of active data loading jobs.
	// +optional
	DataLoadingJobs int32 `json:"dataLoadingJobs,omitempty"`

	// DiskUsagePercent is the current disk usage percentage.
	// +optional
	DiskUsagePercent int32 `json:"diskUsagePercent,omitempty"`

	// RecommendationCount is the number of active recommendations.
	// +optional
	RecommendationCount int32 `json:"recommendationCount,omitempty"`

	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the cluster state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// FailedSegments lists segments that are currently in a failed state.
	// +optional
	FailedSegments []FailedSegment `json:"failedSegments,omitempty"`
}

// FailedSegment describes a segment that is in a failed state.
type FailedSegment struct {
	// ContentID is the segment content identifier.
	ContentID int32 `json:"contentID"`

	// Hostname is the host where the segment was running.
	Hostname string `json:"hostname"`

	// Role is the segment role (primary or mirror).
	Role string `json:"role"`

	// Status is the segment status description.
	Status string `json:"status"`
}

// Types are registered via SchemeBuilder in groupversion_info.go.
