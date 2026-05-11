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
	AuthMethodTrust       AuthMethod = "trust"
	AuthMethodReject      AuthMethod = "reject"
	AuthMethodMD5         AuthMethod = "md5"
	AuthMethodScramSHA256 AuthMethod = "scram-sha-256"
	AuthMethodPassword    AuthMethod = "password"
	AuthMethodIdent       AuthMethod = "ident"
	AuthMethodPeer        AuthMethod = "peer"
	AuthMethodGSS         AuthMethod = "gss"
	AuthMethodLDAP        AuthMethod = "ldap"
	AuthMethodCert        AuthMethod = "cert"
	AuthMethodPAM         AuthMethod = "pam"
	AuthMethodRadius      AuthMethod = "radius"
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
	// +kubebuilder:default="7.7"
	// +optional
	Version string `json:"version,omitempty"`

	// Image is the container image for Cloudberry DB.
	// +kubebuilder:default="cloudberrydb/cloudberry:7.7"
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

// MirroringSpec defines segment mirroring configuration.
type MirroringSpec struct {
	// Enabled controls whether segment mirroring is active.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

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
