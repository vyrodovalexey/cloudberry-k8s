package v1alpha1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

func TestGroupVersion(t *testing.T) {
	assert.Equal(t, "avsoft.io", GroupVersion.Group)
	assert.Equal(t, "v1alpha1", GroupVersion.Version)
}

func TestAddToScheme(t *testing.T) {
	scheme := runtime.NewScheme()
	err := AddToScheme(scheme)
	require.NoError(t, err)

	// Verify types are registered
	gvk := schema.GroupVersionKind{
		Group:   "avsoft.io",
		Version: "v1alpha1",
		Kind:    "CloudberryCluster",
	}
	obj, err := scheme.New(gvk)
	require.NoError(t, err)
	assert.NotNil(t, obj)
}

func TestClusterPhaseConstants(t *testing.T) {
	tests := []struct {
		name  string
		phase ClusterPhase
		value string
	}{
		{"Pending", ClusterPhasePending, "Pending"},
		{"Initializing", ClusterPhaseInitializing, "Initializing"},
		{"Running", ClusterPhaseRunning, "Running"},
		{"Updating", ClusterPhaseUpdating, "Updating"},
		{"Scaling", ClusterPhaseScaling, "Scaling"},
		{"Failed", ClusterPhaseFailed, "Failed"},
		{"Deleting", ClusterPhaseDeleting, "Deleting"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, string(tt.phase))
		})
	}
}

func TestMirroringStatusConstants(t *testing.T) {
	tests := []struct {
		name   string
		status MirroringStatus
		value  string
	}{
		{"NotConfigured", MirroringNotConfigured, "NotConfigured"},
		{"InSync", MirroringInSync, "InSync"},
		{"Syncing", MirroringSyncing, "Syncing"},
		{"Degraded", MirroringDegraded, "Degraded"},
		{"Down", MirroringDown, "Down"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, string(tt.status))
		})
	}
}

func TestDeletionPolicyConstants(t *testing.T) {
	assert.Equal(t, "Retain", string(DeletionPolicyRetain))
	assert.Equal(t, "Delete", string(DeletionPolicyDelete))
}

func TestAntiAffinityTypeConstants(t *testing.T) {
	assert.Equal(t, "preferred", string(AntiAffinityPreferred))
	assert.Equal(t, "required", string(AntiAffinityRequired))
}

func TestImagePullPolicyConstants(t *testing.T) {
	assert.Equal(t, "Always", string(ImagePullAlways))
	assert.Equal(t, "IfNotPresent", string(ImagePullIfNotPresent))
	assert.Equal(t, "Never", string(ImagePullNever))
}

func TestHBATypeConstants(t *testing.T) {
	assert.Equal(t, "local", string(HBATypeLocal))
	assert.Equal(t, "host", string(HBATypeHost))
	assert.Equal(t, "hostssl", string(HBATypeHostSSL))
	assert.Equal(t, "hostnossl", string(HBATypeHostNoSSL))
}

func TestAuthMethodConstants(t *testing.T) {
	assert.Equal(t, "trust", string(AuthMethodTrust))
	assert.Equal(t, "reject", string(AuthMethodReject))
	assert.Equal(t, "md5", string(AuthMethodMD5))
	assert.Equal(t, "scram-sha-256", string(AuthMethodScramSHA256))
	assert.Equal(t, "password", string(AuthMethodPassword))
	assert.Equal(t, "ident", string(AuthMethodIdent))
	assert.Equal(t, "peer", string(AuthMethodPeer))
	assert.Equal(t, "gss", string(AuthMethodGSS))
	assert.Equal(t, "ldap", string(AuthMethodLDAP))
	assert.Equal(t, "cert", string(AuthMethodCert))
	assert.Equal(t, "pam", string(AuthMethodPAM))
	assert.Equal(t, "radius", string(AuthMethodRadius))
}

func TestVaultAuthMethodConstants(t *testing.T) {
	assert.Equal(t, "token", string(VaultAuthToken))
	assert.Equal(t, "kubernetes", string(VaultAuthKubernetes))
	assert.Equal(t, "approle", string(VaultAuthAppRole))
}

func TestOTLPProtocolConstants(t *testing.T) {
	assert.Equal(t, "grpc", string(OTLPProtocolGRPC))
	assert.Equal(t, "http", string(OTLPProtocolHTTP))
}

func TestRoleClaimSourceConstants(t *testing.T) {
	assert.Equal(t, "id_token", string(RoleClaimSourceIDToken))
	assert.Equal(t, "userinfo", string(RoleClaimSourceUserInfo))
}

func TestRoleMatchModeConstants(t *testing.T) {
	assert.Equal(t, "exact", string(RoleMatchExact))
	assert.Equal(t, "suffix", string(RoleMatchSuffix))
	assert.Equal(t, "prefix", string(RoleMatchPrefix))
	assert.Equal(t, "contains", string(RoleMatchContains))
}

func TestConditionTypeConstants(t *testing.T) {
	assert.Equal(t, "ClusterReady", string(ConditionClusterReady))
	assert.Equal(t, "CoordinatorReady", string(ConditionCoordinatorReady))
	assert.Equal(t, "StandbyReady", string(ConditionStandbyReady))
	assert.Equal(t, "SegmentsReady", string(ConditionSegmentsReady))
	assert.Equal(t, "MirroringHealthy", string(ConditionMirroringHealthy))
	assert.Equal(t, "AuthConfigured", string(ConditionAuthConfigured))
	assert.Equal(t, "ConfigApplied", string(ConditionConfigApplied))
	assert.Equal(t, "VaultConnected", string(ConditionVaultConnected))
}

func TestMirroringLayoutConstants(t *testing.T) {
	assert.Equal(t, "group", string(MirroringLayoutGroup))
	assert.Equal(t, "spread", string(MirroringLayoutSpread))
}

// DeepCopy tests

func TestCloudberryCluster_DeepCopy(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var cluster *CloudberryCluster
		result := cluster.DeepCopy()
		assert.Nil(t, result)
	})

	t.Run("deep copy with all fields", func(t *testing.T) {
		replicas := int32(1)
		tolSeconds := int64(300)
		cluster := &CloudberryCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
			Spec: CloudberryClusterSpec{
				Version:         "7.7",
				Image:           "cloudberrydb/cloudberry:7.7",
				ImagePullPolicy: ImagePullIfNotPresent,
				ImagePullSecrets: []ImagePullSecret{
					{Name: "my-secret"},
				},
				Coordinator: CoordinatorSpec{
					Replicas: &replicas,
					Resources: &ResourceRequirements{
						Requests: &ResourceList{CPU: "1", Memory: "2Gi"},
						Limits:   &ResourceList{CPU: "2", Memory: "4Gi"},
					},
					Storage:      StorageSpec{StorageClass: "fast", Size: "10Gi"},
					NodeSelector: map[string]string{"role": "coordinator"},
					Tolerations: []Toleration{
						{Key: "key1", Operator: "Equal", Value: "val1", Effect: "NoSchedule", TolerationSeconds: &tolSeconds},
					},
					Port: 5432,
				},
				Standby: &StandbySpec{
					Enabled:      true,
					Resources:    &ResourceRequirements{Requests: &ResourceList{CPU: "1"}},
					Storage:      &StorageSpec{Size: "10Gi"},
					NodeSelector: map[string]string{"role": "standby"},
				},
				Segments: SegmentsSpec{
					Count:            4,
					PrimariesPerHost: 2,
					Mirroring:        &MirroringSpec{Enabled: true, Layout: MirroringLayoutGroup},
					Resources:        &ResourceRequirements{Requests: &ResourceList{CPU: "2"}},
					Storage:          StorageSpec{Size: "20Gi"},
					NodeSelector:     map[string]string{"role": "segment"},
					Tolerations:      []Toleration{{Key: "key2"}},
					AntiAffinity:     AntiAffinityPreferred,
				},
				Auth: &AuthSpec{
					Basic: &BasicAuthSpec{
						Enabled:             true,
						AdminUser:           "gpadmin",
						AdminPasswordSecret: &SecretKeyRef{Name: "secret", Key: "password"},
					},
					OIDC: &OIDCSpec{
						Enabled:       true,
						IssuerURL:     "https://issuer.example.com",
						ClientID:      "client-id",
						ClientSecret:  &OIDCSecretRef{SecretRef: &SecretKeyRef{Name: "oidc-secret", Key: "secret"}},
						Scopes:        []string{"openid", "profile"},
						RoleMapping:   map[string]string{"admin": "Admin"},
						RoleMatchMode: RoleMatchExact,
					},
					HBARules: []HBARule{
						{Type: HBATypeHost, Database: "all", User: "all", Address: "0.0.0.0/0", Method: AuthMethodScramSHA256},
					},
					SSL: &SSLSpec{
						Enabled:       true,
						CertSecret:    &CertSecretRef{Name: "tls-secret"},
						MinTLSVersion: "1.2",
					},
				},
				Config: &ConfigSpec{
					Parameters:            map[string]string{"max_connections": "100"},
					CoordinatorParameters: map[string]string{"work_mem": "64MB"},
					DatabaseParameters:    map[string]map[string]string{"mydb": {"search_path": "public"}},
					RoleParameters:        map[string]map[string]string{"admin": {"statement_timeout": "60s"}},
				},
				HA: &HASpec{
					FTSProbeInterval: 60,
					FTSProbeTimeout:  20,
					FTSProbeRetries:  5,
					Checksums:        true,
				},
				Vault: &VaultSpec{
					Enabled:    true,
					Address:    "https://vault.example.com",
					AuthMethod: VaultAuthKubernetes,
					TLSSecret:  &VaultTLSSecret{Name: "vault-tls"},
				},
				Monitoring: &MonitoringSpec{
					Enabled:        true,
					MetricsPort:    9187,
					ServiceMonitor: true,
				},
				Telemetry: &TelemetrySpec{
					Enabled:      true,
					OTLPEndpoint: "localhost:4317",
					OTLPProtocol: OTLPProtocolGRPC,
					SamplingRate: 1.0,
				},
				DeletionPolicy: DeletionPolicyRetain,
				BackupOnDelete: true,
			},
			Status: CloudberryClusterStatus{
				Phase:            ClusterPhaseRunning,
				CoordinatorReady: true,
				StandbyReady:     true,
				SegmentsReady:    4,
				SegmentsTotal:    4,
				MirroringStatus:  MirroringInSync,
				ClusterVersion:   "7.7",
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue},
				},
				FailedSegments: []FailedSegment{
					{ContentID: 1, Hostname: "host1", Role: "primary", Status: "down"},
				},
			},
		}

		copy := cluster.DeepCopy()
		require.NotNil(t, copy)

		// Verify it's a deep copy (modifying copy shouldn't affect original)
		copy.Name = "modified"
		assert.Equal(t, "test-cluster", cluster.Name)

		copy.Spec.Config.Parameters["new_key"] = "new_value"
		_, exists := cluster.Spec.Config.Parameters["new_key"]
		assert.False(t, exists)

		copy.Spec.Coordinator.NodeSelector["new"] = "label"
		_, exists = cluster.Spec.Coordinator.NodeSelector["new"]
		assert.False(t, exists)
	})
}

func TestCloudberryCluster_DeepCopyObject(t *testing.T) {
	t.Run("non-nil returns runtime.Object", func(t *testing.T) {
		cluster := &CloudberryCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
		}
		obj := cluster.DeepCopyObject()
		require.NotNil(t, obj)
		_, ok := obj.(*CloudberryCluster)
		assert.True(t, ok)
	})

	t.Run("nil returns nil", func(t *testing.T) {
		var cluster *CloudberryCluster
		obj := cluster.DeepCopyObject()
		assert.Nil(t, obj)
	})
}

func TestCloudberryClusterList_DeepCopy(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var list *CloudberryClusterList
		result := list.DeepCopy()
		assert.Nil(t, result)
	})

	t.Run("deep copy with items", func(t *testing.T) {
		list := &CloudberryClusterList{
			Items: []CloudberryCluster{
				{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}},
			},
		}
		copy := list.DeepCopy()
		require.NotNil(t, copy)
		require.Len(t, copy.Items, 2)
		assert.Equal(t, "cluster1", copy.Items[0].Name)
		assert.Equal(t, "cluster2", copy.Items[1].Name)

		// Modify copy shouldn't affect original
		copy.Items[0].Name = "modified"
		assert.Equal(t, "cluster1", list.Items[0].Name)
	})
}

func TestCloudberryClusterList_DeepCopyObject(t *testing.T) {
	t.Run("non-nil returns runtime.Object", func(t *testing.T) {
		list := &CloudberryClusterList{}
		obj := list.DeepCopyObject()
		require.NotNil(t, obj)
		_, ok := obj.(*CloudberryClusterList)
		assert.True(t, ok)
	})

	t.Run("nil returns nil", func(t *testing.T) {
		var list *CloudberryClusterList
		obj := list.DeepCopyObject()
		assert.Nil(t, obj)
	})
}

func TestDeepCopy_NilReceivers(t *testing.T) {
	// Test all DeepCopy methods with nil receivers
	nilTests := []struct {
		name string
		fn   func() interface{}
	}{
		{"CloudberryClusterSpec", func() interface{} { var s *CloudberryClusterSpec; return s.DeepCopy() }},
		{"CloudberryClusterStatus", func() interface{} { var s *CloudberryClusterStatus; return s.DeepCopy() }},
		{"CoordinatorSpec", func() interface{} { var s *CoordinatorSpec; return s.DeepCopy() }},
		{"StandbySpec", func() interface{} { var s *StandbySpec; return s.DeepCopy() }},
		{"SegmentsSpec", func() interface{} { var s *SegmentsSpec; return s.DeepCopy() }},
		{"MirroringSpec", func() interface{} { var s *MirroringSpec; return s.DeepCopy() }},
		{"AuthSpec", func() interface{} { var s *AuthSpec; return s.DeepCopy() }},
		{"BasicAuthSpec", func() interface{} { var s *BasicAuthSpec; return s.DeepCopy() }},
		{"OIDCSpec", func() interface{} { var s *OIDCSpec; return s.DeepCopy() }},
		{"OIDCSecretRef", func() interface{} { var s *OIDCSecretRef; return s.DeepCopy() }},
		{"SecretKeyRef", func() interface{} { var s *SecretKeyRef; return s.DeepCopy() }},
		{"HBARule", func() interface{} { var s *HBARule; return s.DeepCopy() }},
		{"SSLSpec", func() interface{} { var s *SSLSpec; return s.DeepCopy() }},
		{"CertSecretRef", func() interface{} { var s *CertSecretRef; return s.DeepCopy() }},
		{"ConfigSpec", func() interface{} { var s *ConfigSpec; return s.DeepCopy() }},
		{"HASpec", func() interface{} { var s *HASpec; return s.DeepCopy() }},
		{"VaultSpec", func() interface{} { var s *VaultSpec; return s.DeepCopy() }},
		{"VaultTLSSecret", func() interface{} { var s *VaultTLSSecret; return s.DeepCopy() }},
		{"MonitoringSpec", func() interface{} { var s *MonitoringSpec; return s.DeepCopy() }},
		{"TelemetrySpec", func() interface{} { var s *TelemetrySpec; return s.DeepCopy() }},
		{"ResourceRequirements", func() interface{} { var s *ResourceRequirements; return s.DeepCopy() }},
		{"ResourceList", func() interface{} { var s *ResourceList; return s.DeepCopy() }},
		{"StorageSpec", func() interface{} { var s *StorageSpec; return s.DeepCopy() }},
		{"Toleration", func() interface{} { var s *Toleration; return s.DeepCopy() }},
		{"FailedSegment", func() interface{} { var s *FailedSegment; return s.DeepCopy() }},
		{"ImagePullSecret", func() interface{} { var s *ImagePullSecret; return s.DeepCopy() }},
		{"WorkloadSpec", func() interface{} { var s *WorkloadSpec; return s.DeepCopy() }},
		{"ResourceGroupSpec", func() interface{} { var s *ResourceGroupSpec; return s.DeepCopy() }},
		{"WorkloadRule", func() interface{} { var s *WorkloadRule; return s.DeepCopy() }},
		{"IdleSessionRule", func() interface{} { var s *IdleSessionRule; return s.DeepCopy() }},
		{"QueryMonitoringSpec", func() interface{} { var s *QueryMonitoringSpec; return s.DeepCopy() }},
		{"BackupSpec", func() interface{} { var s *BackupSpec; return s.DeepCopy() }},
		{"BackupRetention", func() interface{} { var s *BackupRetention; return s.DeepCopy() }},
		{"BackupDestination", func() interface{} { var s *BackupDestination; return s.DeepCopy() }},
		{"S3VaultSecret", func() interface{} { var s *S3VaultSecret; return s.DeepCopy() }},
		{"SecretReference", func() interface{} { var s *SecretReference; return s.DeepCopy() }},
		{"DataLoadingSpec", func() interface{} { var s *DataLoadingSpec; return s.DeepCopy() }},
		{"PxfSpec", func() interface{} { var s *PxfSpec; return s.DeepCopy() }},
		{"PxfExtensionsSpec", func() interface{} { var s *PxfExtensionsSpec; return s.DeepCopy() }},
		{"PxfServerSpec", func() interface{} { var s *PxfServerSpec; return s.DeepCopy() }},
		{"PxfCustomConnector", func() interface{} { var s *PxfCustomConnector; return s.DeepCopy() }},
		{"GpfdistSpec", func() interface{} { var s *GpfdistSpec; return s.DeepCopy() }},
		{"DataLoadingJob", func() interface{} { var s *DataLoadingJob; return s.DeepCopy() }},
		{"PxfJobSpec", func() interface{} { var s *PxfJobSpec; return s.DeepCopy() }},
		{"PartitioningSpec", func() interface{} { var s *PartitioningSpec; return s.DeepCopy() }},
		{"ErrorHandlingSpec", func() interface{} { var s *ErrorHandlingSpec; return s.DeepCopy() }},
		{"GploadJobSpec", func() interface{} { var s *GploadJobSpec; return s.DeepCopy() }},
		{"DataLoadingJobTemplate", func() interface{} { var s *DataLoadingJobTemplate; return s.DeepCopy() }},
		{"StorageManagementSpec", func() interface{} { var s *StorageManagementSpec; return s.DeepCopy() }},
		{"RecommendationScanSpec", func() interface{} { var s *RecommendationScanSpec; return s.DeepCopy() }},
		{"UsageReportSpec", func() interface{} { var s *UsageReportSpec; return s.DeepCopy() }},
		{"DataLoadingStatus", func() interface{} { var s *DataLoadingStatus; return s.DeepCopy() }},
		{"DataLoadingJobStatus", func() interface{} { var s *DataLoadingJobStatus; return s.DeepCopy() }},
		{"DataLoadingPxfStatus", func() interface{} { var s *DataLoadingPxfStatus; return s.DeepCopy() }},
	}

	for _, tt := range nilTests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn()
			assert.Nil(t, result)
		})
	}
}

// newFullyPopulatedCluster creates a CloudberryCluster with ALL fields set for deep copy testing.
func newFullyPopulatedCluster() *CloudberryCluster {
	replicas := int32(1)
	tolSeconds := int64(300)
	now := metav1.Now()

	return &CloudberryCluster{
		TypeMeta: metav1.TypeMeta{Kind: "CloudberryCluster", APIVersion: "avsoft.io/v1alpha1"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "full-cluster", Namespace: "production",
			Labels: map[string]string{"env": "prod"},
		},
		Spec: CloudberryClusterSpec{
			Version: "7.7", Image: "cloudberrydb/cloudberry:7.7",
			ImagePullPolicy:  ImagePullAlways,
			ImagePullSecrets: []ImagePullSecret{{Name: "registry-secret"}},
			Coordinator: CoordinatorSpec{
				Replicas: &replicas,
				Resources: &ResourceRequirements{
					Requests: &ResourceList{CPU: "2", Memory: "4Gi"},
					Limits:   &ResourceList{CPU: "4", Memory: "8Gi"},
				},
				Storage:      StorageSpec{StorageClass: "fast-ssd", Size: "50Gi"},
				NodeSelector: map[string]string{"role": "coordinator"},
				Tolerations: []Toleration{
					{Key: "dedicated", Operator: "Equal", Value: "db",
						Effect: "NoSchedule", TolerationSeconds: &tolSeconds},
				},
				Port: 5432,
			},
			Standby: &StandbySpec{
				Enabled: true,
				Resources: &ResourceRequirements{
					Requests: &ResourceList{CPU: "2", Memory: "4Gi"},
				},
				Storage:      &StorageSpec{StorageClass: "fast-ssd", Size: "50Gi"},
				NodeSelector: map[string]string{"role": "standby"},
			},
			Segments: SegmentsSpec{
				Count: 8, PrimariesPerHost: 4,
				Mirroring: &MirroringSpec{Enabled: true, Layout: MirroringLayoutSpread},
				Resources: &ResourceRequirements{
					Requests: &ResourceList{CPU: "4", Memory: "16Gi"},
					Limits:   &ResourceList{CPU: "8", Memory: "32Gi"},
				},
				Storage:      StorageSpec{StorageClass: "fast-ssd", Size: "100Gi"},
				NodeSelector: map[string]string{"role": "segment"},
				Tolerations:  []Toleration{{Key: "segment", Effect: "NoSchedule"}},
				AntiAffinity: AntiAffinityRequired,
			},
			Auth: &AuthSpec{
				Basic: &BasicAuthSpec{
					Enabled: true, AdminUser: "gpadmin",
					AdminPasswordSecret: &SecretKeyRef{Name: "admin-secret", Key: "password"},
				},
				OIDC: &OIDCSpec{
					Enabled: true, IssuerURL: "https://keycloak.example.com/realms/db",
					ClientID:      "cloudberry",
					ClientSecret:  &OIDCSecretRef{SecretRef: &SecretKeyRef{Name: "oidc", Key: "secret"}},
					Scopes:        []string{"openid", "profile", "email", "roles"},
					RoleClaimPath: "realm_access.roles", RoleClaimSource: RoleClaimSourceIDToken,
					RoleMatchMode: RoleMatchPrefix,
					RoleMapping:   map[string]string{"db-admin": "Admin", "db-viewer": "Basic"},
					PKCE:          true, AllowLocalSignIn: true,
				},
				HBARules: []HBARule{
					{Type: HBATypeHostSSL, Database: "all", User: "all",
						Address: "10.0.0.0/8", Method: AuthMethodScramSHA256},
					{Type: HBATypeLocal, Database: "all", User: "gpadmin", Method: AuthMethodPeer},
				},
				SSL: &SSLSpec{
					Enabled: true, CertSecret: &CertSecretRef{Name: "tls-cert"},
					MinTLSVersion: "1.3",
				},
			},
			Config: &ConfigSpec{
				Parameters:            map[string]string{"max_connections": "500", "work_mem": "128MB"},
				CoordinatorParameters: map[string]string{"log_statement": "all"},
				DatabaseParameters: map[string]map[string]string{
					"analytics": {"search_path": "analytics,public"},
				},
				RoleParameters: map[string]map[string]string{
					"etl_user": {"statement_timeout": "3600s"},
				},
			},
			HA: &HASpec{
				FTSProbeInterval: 30, FTSProbeTimeout: 10,
				FTSProbeRetries: 3, Checksums: true,
			},
			Vault: &VaultSpec{
				Enabled: true, Address: "https://vault.example.com:8200",
				AuthMethod: VaultAuthAppRole, AuthPath: "auth/approle",
				Role: "cloudberry", SecretPath: "secret/data/cloudberry",
				TLSSecret: &VaultTLSSecret{Name: "vault-ca"},
			},
			Monitoring: &MonitoringSpec{
				Enabled: true, MetricsPort: 9187, ServiceMonitor: true,
			},
			Telemetry: &TelemetrySpec{
				Enabled: true, OTLPEndpoint: "otel-collector:4317",
				OTLPProtocol: OTLPProtocolGRPC, SamplingRate: 0.5,
			},
			Workload: &WorkloadSpec{
				Enabled: true,
				ResourceGroups: []ResourceGroupSpec{
					{Name: "analytics", Concurrency: 20, CPUMaxPercent: 60,
						CPUWeight: 100, MemoryLimit: 4096, MinCost: 500},
				},
				Rules: []WorkloadRule{
					{Name: "cancel-long", Enabled: true, ResourceGroup: "analytics",
						Action: "cancel", Threshold: "3600", ThresholdType: "running_time", Priority: 1},
				},
				IdleRules: []IdleSessionRule{
					{Name: "idle-30m", Enabled: true, ResourceGroup: "analytics",
						IdleTimeout: "30m", ExcludeInTransaction: true, TerminateMessage: "idle timeout"},
				},
			},
			QueryMonitoring: &QueryMonitoringSpec{
				Enabled: true, HistoryRetention: "90d", SamplingInterval: 5,
				GuestAccess: false, PlanCollection: true, SlowQueryThreshold: "500ms",
			},
			Backup: &BackupSpec{
				Enabled: true, Schedule: "0 2 * * *",
				Retention: BackupRetention{FullCount: 7, IncrementalCount: 30, MaxAge: "90d"},
				Destination: BackupDestination{
					Type: "s3",
					S3: &S3Destination{
						Bucket: "db-backups", Endpoint: "s3.amazonaws.com",
						Region: "us-east-1", Folder: "/cloudberry",
						CredentialSecret: &S3CredentialSecret{Name: "s3-creds"},
						ForcePathStyle:   true,
						Multipart:        &S3Multipart{BackupMaxConcurrentRequests: 4, BackupMultipartChunksize: "10MB"},
					},
				},
				Gpbackup:    &GpbackupOptions{CompressionLevel: 6, CompressionType: "zstd", Jobs: 4, Incremental: true},
				Gprestore:   &GprestoreOptions{Jobs: 4, WithStats: ptr.To(true)},
				JobTemplate: &BackupJobTemplate{ServiceAccountName: "cloudberry-backup-sa"},
				Image:       "cloudberry-backup:2.1.0",
			},
			DataLoading: &DataLoadingSpec{
				Enabled: true,
				Pxf: &PxfSpec{
					Enabled: true, Image: "cloudberry-pxf:7.1.0", Port: 5888,
					JvmOpts: "-Xmx1g -Xms256m", LogLevel: "INFO",
					Extensions: &PxfExtensionsSpec{Pxf: ptr.To(true), PxfFdw: ptr.To(true)},
					Servers: []PxfServerSpec{
						{
							Name: "s3-datalake", Type: "s3",
							Config: map[string]string{
								"fs.s3a.endpoint": "s3.amazonaws.com",
							},
							CredentialSecrets: []SecretReference{{Name: "s3-credentials", Key: "access_key"}},
						},
						{
							Name: "mysql-oltp", Type: "jdbc",
							Config: map[string]string{
								"jdbc.driver": "com.mysql.cj.jdbc.Driver",
								"jdbc.url":    "jdbc:mysql://mysql:3306/production",
							},
							CredentialSecrets: []SecretReference{{Name: "mysql-credentials"}},
						},
						{
							Name: "hadoop-cluster", Type: "hdfs",
							Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
							Hive:   map[string]string{"hive.metastore.uris": "thrift://hive-metastore:9083"},
							Hbase:  map[string]string{"hbase.zookeeper.quorum": "zk1:2181"},
						},
					},
					CustomConnectors: []PxfCustomConnector{
						{Name: "custom-connector", JarURL: "s3://artifacts/pxf-plugins/my-connector.jar"},
					},
				},
				Gpfdist: &GpfdistSpec{
					Enabled: true, Replicas: ptr.To(int32(2)), Image: "cloudberry-gpfdist:2.1.0", Port: 8080,
				},
				Jobs: []DataLoadingJob{
					{
						Name: "s3-parquet-ingest", Type: "pxf", Enabled: true,
						Schedule: "*/15 * * * *",
						PxfJob: &PxfJobSpec{
							Server: "s3-datalake", Profile: "s3:parquet",
							Resource: "s3a://data-lake/events/", TargetTable: "public.events",
							Mode: "insert", FilterPushdown: ptr.To(true), ColumnProjection: ptr.To(true),
							ErrorHandling: &ErrorHandlingSpec{
								SegmentRejectLimit: 100, SegmentRejectLimitType: "rows", LogErrors: ptr.To(true),
							},
						},
					},
					{
						Name: "jdbc-sync", Type: "pxf", Enabled: true,
						PxfJob: &PxfJobSpec{
							Server: "mysql-oltp", Profile: "jdbc",
							Resource: "production.orders", TargetTable: "public.orders_staging",
							Mode: "insert-select", FilterPushdown: ptr.To(true),
							Partitioning: &PartitioningSpec{
								Column: "order_date", Range: "2024-01-01:2026-12-31", Interval: "1:month",
							},
						},
					},
					{
						Name: "gpload-csv", Type: "gpload", Enabled: false,
						GploadJob: &GploadJobSpec{
							TargetTable: "public.raw_data", Mode: "insert", Format: "csv",
							FilePaths: []string{"/data/incoming/*.csv"},
						},
					},
				},
				JobTemplate: &DataLoadingJobTemplate{
					ServiceAccountName:      "cloudberry-data-loading-sa",
					BackoffLimit:            ptr.To(int32(3)),
					ActiveDeadlineSeconds:   ptr.To(int64(14400)),
					TTLSecondsAfterFinished: ptr.To(int32(86400)),
				},
			},
			Storage: &StorageManagementSpec{
				DiskMonitoring: true,
				RecommendationScan: &RecommendationScanSpec{
					Enabled: true, Schedule: "0 3 * * 0", BloatThreshold: 20,
					SkewThreshold: 50, AgeThreshold: 200000000,
					IndexBloatThreshold: 30, ScanDuration: "2h",
				},
				UsageReport: &UsageReportSpec{Enabled: true, Monthly: true},
			},
			DeletionPolicy: DeletionPolicyDelete,
			BackupOnDelete: true,
		},
		Status: CloudberryClusterStatus{
			Phase: ClusterPhaseRunning, CoordinatorReady: true, StandbyReady: true,
			SegmentsReady: 8, SegmentsTotal: 8,
			MirroringStatus: MirroringInSync, ClusterVersion: "7.7",
			LastReconcileTime: &now, LastConfigChangeTime: &now,
			ActiveQueries: 15, QueuedQueries: 3, BlockedQueries: 1,
			LastBackupTime: &now, LastBackupStatus: "Success",
			DataLoadingJobs: 2, DiskUsagePercent: 45, RecommendationCount: 3,
			DataLoading: &DataLoadingStatus{
				Phase: "Configured", ConfiguredJobs: 3, ActiveJobs: 2,
				Jobs: []DataLoadingJobStatus{
					{
						Name: "job1", Enabled: true,
						LastRun: &now, LastStatus: "Succeeded",
						RowsLoaded: ptr.To[int64](183961), Duration: "1m30s",
					},
					{Name: "job2", Enabled: false},
					{Name: "job3", Enabled: true},
				},
				Pxf: &DataLoadingPxfStatus{Configured: true, Servers: 5},
			},
			ObservedGeneration: 5,
			Conditions: []metav1.Condition{
				{Type: "ClusterReady", Status: metav1.ConditionTrue,
					Reason: "AllReady", Message: "All components ready",
					LastTransitionTime: now},
				{Type: "BackupConfigured", Status: metav1.ConditionTrue,
					Reason: "Configured", Message: "Backup configured",
					LastTransitionTime: now},
			},
			FailedSegments: []FailedSegment{
				{ContentID: 3, Hostname: "seg-host-2", Role: "mirror", Status: "d"},
			},
		},
	}
}

func TestDeepCopyInto_FullyPopulatedCluster(t *testing.T) {
	original := newFullyPopulatedCluster()
	copied := &CloudberryCluster{}
	original.DeepCopyInto(copied)

	// Verify all top-level fields match.
	assert.Equal(t, original.Name, copied.Name)
	assert.Equal(t, original.Spec.Version, copied.Spec.Version)
	assert.Equal(t, original.Status.Phase, copied.Status.Phase)

	// Verify pointer fields are independent copies.
	copied.Spec.Coordinator.NodeSelector["new-key"] = "new-val"
	_, exists := original.Spec.Coordinator.NodeSelector["new-key"]
	assert.False(t, exists, "modifying copy's NodeSelector should not affect original")

	copied.Spec.Auth.OIDC.Scopes = append(copied.Spec.Auth.OIDC.Scopes, "extra")
	assert.NotEqual(t, len(original.Spec.Auth.OIDC.Scopes), len(copied.Spec.Auth.OIDC.Scopes))
}

func TestDeepCopy_IndependentCopies_Spec(t *testing.T) {
	original := newFullyPopulatedCluster()
	copied := original.DeepCopy()
	require.NotNil(t, copied)

	t.Run("modify coordinator resources", func(t *testing.T) {
		copied.Spec.Coordinator.Resources.Requests.CPU = "99"
		assert.Equal(t, "2", original.Spec.Coordinator.Resources.Requests.CPU)
	})

	t.Run("modify standby node selector", func(t *testing.T) {
		copied.Spec.Standby.NodeSelector["extra"] = "label"
		_, exists := original.Spec.Standby.NodeSelector["extra"]
		assert.False(t, exists)
	})

	t.Run("modify segment tolerations", func(t *testing.T) {
		copied.Spec.Segments.Tolerations[0].Key = "changed"
		assert.Equal(t, "segment", original.Spec.Segments.Tolerations[0].Key)
	})

	t.Run("modify config parameters", func(t *testing.T) {
		copied.Spec.Config.Parameters["new_param"] = "new_value"
		_, exists := original.Spec.Config.Parameters["new_param"]
		assert.False(t, exists)
	})

	t.Run("modify coordinator parameters", func(t *testing.T) {
		copied.Spec.Config.CoordinatorParameters["new_coord_param"] = "val"
		_, exists := original.Spec.Config.CoordinatorParameters["new_coord_param"]
		assert.False(t, exists)
	})

	t.Run("modify database parameters", func(t *testing.T) {
		copied.Spec.Config.DatabaseParameters["analytics"]["new_key"] = "val"
		_, exists := original.Spec.Config.DatabaseParameters["analytics"]["new_key"]
		assert.False(t, exists)
	})

	t.Run("modify role parameters", func(t *testing.T) {
		copied.Spec.Config.RoleParameters["etl_user"]["new_key"] = "val"
		_, exists := original.Spec.Config.RoleParameters["etl_user"]["new_key"]
		assert.False(t, exists)
	})

	t.Run("modify OIDC role mapping", func(t *testing.T) {
		copied.Spec.Auth.OIDC.RoleMapping["new-role"] = "Operator"
		_, exists := original.Spec.Auth.OIDC.RoleMapping["new-role"]
		assert.False(t, exists)
	})

	t.Run("modify workload resource groups", func(t *testing.T) {
		copied.Spec.Workload.ResourceGroups[0].Name = "changed"
		assert.Equal(t, "analytics", original.Spec.Workload.ResourceGroups[0].Name)
	})

	t.Run("modify data loading pxf server config", func(t *testing.T) {
		copied.Spec.DataLoading.Pxf.Servers[0].Config["fs.s3a.endpoint"] = "changed"
		assert.Equal(t, "s3.amazonaws.com",
			original.Spec.DataLoading.Pxf.Servers[0].Config["fs.s3a.endpoint"])
	})

	t.Run("modify data loading pxf job partitioning", func(t *testing.T) {
		copied.Spec.DataLoading.Jobs[1].PxfJob.Partitioning.Column = "changed"
		assert.Equal(t, "order_date",
			original.Spec.DataLoading.Jobs[1].PxfJob.Partitioning.Column)
	})
}

func TestDeepCopy_IndependentCopies_Status(t *testing.T) {
	original := newFullyPopulatedCluster()
	copied := original.DeepCopy()
	require.NotNil(t, copied)

	t.Run("modify conditions", func(t *testing.T) {
		copied.Status.Conditions[0].Reason = "Modified"
		assert.Equal(t, "AllReady", original.Status.Conditions[0].Reason)
	})

	t.Run("modify data loading status jobs", func(t *testing.T) {
		copied.Status.DataLoading.Jobs[0].Name = "changed-job"
		assert.Equal(t, "job1", original.Status.DataLoading.Jobs[0].Name)
		copied.Status.DataLoading.Phase = "Changed"
		assert.Equal(t, "Configured", original.Status.DataLoading.Phase)
	})

	t.Run("modify failed segments", func(t *testing.T) {
		copied.Status.FailedSegments[0].Hostname = "changed-host"
		assert.Equal(t, "seg-host-2", original.Status.FailedSegments[0].Hostname)
	})

	t.Run("modify last reconcile time", func(t *testing.T) {
		newTime := metav1.Now()
		copied.Status.LastReconcileTime = &newTime
		assert.NotEqual(t, copied.Status.LastReconcileTime, original.Status.LastReconcileTime)
	})
}

func TestDeepCopy_SubTypes_Populated(t *testing.T) {
	t.Run("BackupSpec with all fields", func(t *testing.T) {
		original := &BackupSpec{
			Enabled: true, Schedule: "0 2 * * *",
			Retention: BackupRetention{FullCount: 7, IncrementalCount: 30, MaxAge: "90d"},
			Destination: BackupDestination{
				Type: "s3",
				S3: &S3Destination{
					Bucket:           "backups",
					CredentialSecret: &S3CredentialSecret{Name: "creds"},
				},
			},
			Gpbackup: &GpbackupOptions{CompressionLevel: 6, Jobs: 4, Incremental: true},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, original.Schedule, copied.Schedule)
		assert.Equal(t, original.Destination.S3.CredentialSecret.Name, copied.Destination.S3.CredentialSecret.Name)

		copied.Destination.S3.CredentialSecret.Name = "changed"
		assert.Equal(t, "creds", original.Destination.S3.CredentialSecret.Name)
	})

	t.Run("DataLoadingJob with PxfJob", func(t *testing.T) {
		original := &DataLoadingJob{
			Name: "loader", Type: "pxf",
			PxfJob: &PxfJobSpec{
				Server: "s3-datalake", Profile: "s3:parquet", TargetTable: "public.data",
				Mode: "insert", SourceFilter: "region='us-east'",
				FilterPushdown: ptr.To(true), ColumnProjection: ptr.To(true),
				Partitioning: &PartitioningSpec{
					Column: "order_date", Range: "2024-01-01:2026-12-31", Interval: "1:month",
				},
				ErrorHandling: &ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "rows", LogErrors: ptr.To(true),
				},
			},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		copied.PxfJob.TargetTable = "changed"
		assert.Equal(t, "public.data", original.PxfJob.TargetTable)
		// SourceFilter (Scenario 99) is carried through the deep copy.
		assert.Equal(t, "region='us-east'", copied.PxfJob.SourceFilter)
		copied.PxfJob.SourceFilter = "changed"
		assert.Equal(t, "region='us-east'", original.PxfJob.SourceFilter)
		copied.PxfJob.Partitioning.Column = "changed"
		assert.Equal(t, "order_date", original.PxfJob.Partitioning.Column)
	})

	t.Run("DataLoadingJob with GploadJob", func(t *testing.T) {
		original := &DataLoadingJob{
			Name: "gpload-job", Type: "gpload",
			GploadJob: &GploadJobSpec{
				TargetTable: "public.raw_data", Mode: "insert", Format: "csv",
				FilePaths: []string{"/data/incoming/*.csv"},
			},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		copied.GploadJob.FilePaths[0] = "changed"
		assert.Equal(t, "/data/incoming/*.csv", original.GploadJob.FilePaths[0])
	})

	t.Run("PxfServerSpec with credentials", func(t *testing.T) {
		original := &PxfServerSpec{
			Name: "mysql-oltp", Type: "jdbc",
			Config:            map[string]string{"jdbc.driver": "drv", "jdbc.url": "url"},
			Hive:              map[string]string{"k": "v"},
			Hbase:             map[string]string{"k": "v"},
			Jdbc:              map[string]string{"k": "v"},
			CredentialSecrets: []SecretReference{{Name: "mysql-creds"}},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		copied.CredentialSecrets[0].Name = "changed"
		assert.Equal(t, "mysql-creds", original.CredentialSecrets[0].Name)
		copied.Config["jdbc.driver"] = "changed"
		assert.Equal(t, "drv", original.Config["jdbc.driver"])
	})

	t.Run("StorageManagementSpec with all sub-specs", func(t *testing.T) {
		original := &StorageManagementSpec{
			DiskMonitoring: true,
			RecommendationScan: &RecommendationScanSpec{
				Enabled: true, Schedule: "0 3 * * 0", BloatThreshold: 20,
			},
			UsageReport: &UsageReportSpec{Enabled: true, Monthly: true},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		copied.RecommendationScan.BloatThreshold = 99
		assert.Equal(t, int32(20), original.RecommendationScan.BloatThreshold)
	})

	t.Run("PxfSpec with extensions and connectors", func(t *testing.T) {
		original := &PxfSpec{
			Enabled: true, Image: "pxf:1.0", Port: 5888,
			Extensions: &PxfExtensionsSpec{Pxf: ptr.To(true), PxfFdw: ptr.To(false)},
			CustomConnectors: []PxfCustomConnector{
				{Name: "c1", JarURL: "s3://jars/c1.jar"},
			},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		copied.CustomConnectors[0].Name = "changed"
		assert.Equal(t, "c1", original.CustomConnectors[0].Name)
		*copied.Extensions.Pxf = false
		assert.True(t, *original.Extensions.Pxf)
	})

	t.Run("DataLoadingStatus with jobs", func(t *testing.T) {
		original := &DataLoadingStatus{
			Phase: "Configured", ConfiguredJobs: 2, ActiveJobs: 1,
			Jobs: []DataLoadingJobStatus{
				{Name: "job1", Enabled: true},
				{Name: "job2", Enabled: false},
			},
			Pxf: &DataLoadingPxfStatus{Configured: true, Servers: 5},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, original.Phase, copied.Phase)
		assert.Equal(t, original.ConfiguredJobs, copied.ConfiguredJobs)
		assert.Equal(t, original.ActiveJobs, copied.ActiveJobs)
		require.Len(t, copied.Jobs, 2)
		copied.Jobs[0].Name = "changed"
		assert.Equal(t, "job1", original.Jobs[0].Name)
		// Pxf is deep-copied independently of the source.
		require.NotNil(t, copied.Pxf)
		assert.True(t, copied.Pxf.Configured)
		assert.Equal(t, int32(5), copied.Pxf.Servers)
		copied.Pxf.Servers = 0
		copied.Pxf.Configured = false
		assert.Equal(t, int32(5), original.Pxf.Servers)
		assert.True(t, original.Pxf.Configured)
	})

	t.Run("DataLoadingStatus with empty jobs", func(t *testing.T) {
		original := &DataLoadingStatus{Phase: "Configured"}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "Configured", copied.Phase)
		assert.Nil(t, copied.Jobs)
		assert.Nil(t, copied.Pxf)
	})

	t.Run("DataLoadingPxfStatus populated roundtrip", func(t *testing.T) {
		// 105-S1/S3: the PXF status carries the observed-only Status and
		// ExtensionsInstalled fields in addition to the config-derived ones.
		original := &DataLoadingPxfStatus{
			Configured:          true,
			Servers:             5,
			Status:              "Running",
			ExtensionsInstalled: []string{"pxf", "pxf_fdw"},
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, original.Configured, copied.Configured)
		assert.Equal(t, original.Servers, copied.Servers)
		assert.Equal(t, original.Status, copied.Status)
		assert.Equal(t, original.ExtensionsInstalled, copied.ExtensionsInstalled)

		// The ExtensionsInstalled slice must be a DEEP copy: mutating the copy's
		// slice (element + length) must not alias back to the source.
		copied.ExtensionsInstalled[0] = "mutated"
		copied.ExtensionsInstalled = append(copied.ExtensionsInstalled, "extra")
		assert.Equal(t, []string{"pxf", "pxf_fdw"}, original.ExtensionsInstalled,
			"deepcopy must not alias the ExtensionsInstalled slice")

		// Mutating the scalar fields of the copy must not affect the source.
		copied.Configured = false
		copied.Servers = 0
		copied.Status = "Stopped"
		assert.True(t, original.Configured)
		assert.Equal(t, int32(5), original.Servers)
		assert.Equal(t, "Running", original.Status)
	})

	t.Run("DataLoadingPxfStatus nil ExtensionsInstalled roundtrip", func(t *testing.T) {
		// 105-S3-B4: the ABSENT (nil) extensions case must round-trip as nil —
		// deepcopy must never synthesize an empty slice for an unobservable probe.
		original := &DataLoadingPxfStatus{Configured: true, Servers: 0}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		assert.Nil(t, copied.ExtensionsInstalled)
		assert.Empty(t, copied.Status)
	})

	t.Run("Toleration with TolerationSeconds", func(t *testing.T) {
		seconds := int64(600)
		original := &Toleration{
			Key: "dedicated", Operator: "Equal", Value: "db",
			Effect: "NoSchedule", TolerationSeconds: &seconds,
		}
		copied := original.DeepCopy()
		require.NotNil(t, copied)
		newSeconds := int64(999)
		copied.TolerationSeconds = &newSeconds
		assert.Equal(t, int64(600), *original.TolerationSeconds)
	})
}

func TestCloudberryClusterList_DeepCopyObject_WithItems(t *testing.T) {
	list := &CloudberryClusterList{
		TypeMeta: metav1.TypeMeta{Kind: "CloudberryClusterList"},
		Items: []CloudberryCluster{
			*newFullyPopulatedCluster(),
		},
	}

	obj := list.DeepCopyObject()
	require.NotNil(t, obj)

	copiedList, ok := obj.(*CloudberryClusterList)
	require.True(t, ok)
	require.Len(t, copiedList.Items, 1)
	assert.Equal(t, "full-cluster", copiedList.Items[0].Name)

	// Verify independence.
	copiedList.Items[0].Name = "modified"
	assert.Equal(t, "full-cluster", list.Items[0].Name)
}

func TestCloudberryCluster_DeepCopyObject_FullyPopulated(t *testing.T) {
	original := newFullyPopulatedCluster()
	obj := original.DeepCopyObject()
	require.NotNil(t, obj)

	copied, ok := obj.(*CloudberryCluster)
	require.True(t, ok)
	assert.Equal(t, original.Name, copied.Name)
	assert.Equal(t, original.Spec.Version, copied.Spec.Version)

	// Verify it's a true deep copy.
	copied.Spec.Config.Parameters["extra"] = "value"
	_, exists := original.Spec.Config.Parameters["extra"]
	assert.False(t, exists)
}

func TestCloudberryClusterList_DeepCopy_EmptyItems(t *testing.T) {
	list := &CloudberryClusterList{Items: []CloudberryCluster{}}
	copied := list.DeepCopy()
	require.NotNil(t, copied)
	assert.Empty(t, copied.Items)
}

func TestDeepCopyInto_AllSubTypes(t *testing.T) {
	t.Run("MirroringSpec", func(t *testing.T) {
		src := MirroringSpec{Enabled: true, Layout: MirroringLayoutSpread}
		dst := MirroringSpec{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, src.Enabled, dst.Enabled)
		assert.Equal(t, src.Layout, dst.Layout)
	})

	t.Run("SecretKeyRef", func(t *testing.T) {
		src := SecretKeyRef{Name: "secret", Key: "password"}
		dst := SecretKeyRef{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "secret", dst.Name)
	})

	t.Run("HBARule", func(t *testing.T) {
		src := HBARule{Type: HBATypeHost, Database: "all", User: "all",
			Address: "0.0.0.0/0", Method: AuthMethodMD5, Options: "opt"}
		dst := HBARule{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, HBATypeHost, dst.Type)
	})

	t.Run("CertSecretRef", func(t *testing.T) {
		src := CertSecretRef{Name: "tls-cert"}
		dst := CertSecretRef{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "tls-cert", dst.Name)
	})

	t.Run("HASpec", func(t *testing.T) {
		src := HASpec{FTSProbeInterval: 30, FTSProbeTimeout: 10, FTSProbeRetries: 3, Checksums: true}
		dst := HASpec{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, int32(30), dst.FTSProbeInterval)
	})

	t.Run("VaultTLSSecret", func(t *testing.T) {
		src := VaultTLSSecret{Name: "vault-tls"}
		dst := VaultTLSSecret{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "vault-tls", dst.Name)
	})

	t.Run("MonitoringSpec", func(t *testing.T) {
		src := MonitoringSpec{Enabled: true, MetricsPort: 9187, ServiceMonitor: true}
		dst := MonitoringSpec{}
		src.DeepCopyInto(&dst)
		assert.True(t, dst.Enabled)
	})

	t.Run("TelemetrySpec", func(t *testing.T) {
		src := TelemetrySpec{Enabled: true, OTLPEndpoint: "localhost:4317",
			OTLPProtocol: OTLPProtocolGRPC, SamplingRate: 0.5}
		dst := TelemetrySpec{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, 0.5, dst.SamplingRate)
	})

	t.Run("ResourceList", func(t *testing.T) {
		src := ResourceList{CPU: "4", Memory: "8Gi"}
		dst := ResourceList{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "4", dst.CPU)
	})

	t.Run("StorageSpec", func(t *testing.T) {
		src := StorageSpec{StorageClass: "fast", Size: "100Gi"}
		dst := StorageSpec{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "fast", dst.StorageClass)
	})

	t.Run("FailedSegment", func(t *testing.T) {
		src := FailedSegment{ContentID: 3, Hostname: "host1", Role: "mirror", Status: "d"}
		dst := FailedSegment{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, int32(3), dst.ContentID)
	})

	t.Run("ImagePullSecret", func(t *testing.T) {
		src := ImagePullSecret{Name: "registry-secret"}
		dst := ImagePullSecret{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "registry-secret", dst.Name)
	})

	t.Run("ResourceGroupSpec", func(t *testing.T) {
		src := ResourceGroupSpec{Name: "analytics", Concurrency: 20, CPUMaxPercent: 60}
		dst := ResourceGroupSpec{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "analytics", dst.Name)
	})

	t.Run("WorkloadRule", func(t *testing.T) {
		src := WorkloadRule{Name: "rule1", Enabled: true, Action: "cancel", Priority: 1}
		dst := WorkloadRule{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "rule1", dst.Name)
	})

	t.Run("IdleSessionRule", func(t *testing.T) {
		src := IdleSessionRule{Name: "idle", Enabled: true, ResourceGroup: "default",
			IdleTimeout: "30m", ExcludeInTransaction: true, TerminateMessage: "timeout"}
		dst := IdleSessionRule{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "idle", dst.Name)
	})

	t.Run("QueryMonitoringSpec", func(t *testing.T) {
		src := QueryMonitoringSpec{Enabled: true, HistoryRetention: "30d",
			SamplingInterval: 5, SlowQueryThreshold: "1000ms"}
		dst := QueryMonitoringSpec{}
		src.DeepCopyInto(&dst)
		assert.True(t, dst.Enabled)
	})

	t.Run("BackupRetention", func(t *testing.T) {
		src := BackupRetention{FullCount: 7, IncrementalCount: 30, MaxAge: "90d"}
		dst := BackupRetention{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, int32(7), dst.FullCount)
	})

	t.Run("SecretReference", func(t *testing.T) {
		src := SecretReference{Name: "creds", Key: "key"}
		dst := SecretReference{}
		src.DeepCopyInto(&dst)
		assert.Equal(t, "creds", dst.Name)
	})

	t.Run("RecommendationScanSpec", func(t *testing.T) {
		src := RecommendationScanSpec{Enabled: true, Schedule: "0 3 * * 0",
			BloatThreshold: 20, SkewThreshold: 50, AgeThreshold: 200000000}
		dst := RecommendationScanSpec{}
		src.DeepCopyInto(&dst)
		assert.True(t, dst.Enabled)
	})

	t.Run("UsageReportSpec", func(t *testing.T) {
		src := UsageReportSpec{Enabled: true, Monthly: true}
		dst := UsageReportSpec{}
		src.DeepCopyInto(&dst)
		assert.True(t, dst.Monthly)
	})
}

func TestDeepCopy_SpecAndStatus_NonNil(t *testing.T) {
	t.Run("CloudberryClusterSpec non-nil", func(t *testing.T) {
		src := &CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Config:  &ConfigSpec{Parameters: map[string]string{"key": "val"}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "7.7", copied.Version)
		copied.Config.Parameters["new"] = "val"
		_, exists := src.Config.Parameters["new"]
		assert.False(t, exists)
	})

	t.Run("CloudberryClusterStatus non-nil", func(t *testing.T) {
		now := metav1.Now()
		src := &CloudberryClusterStatus{
			Phase:             ClusterPhaseRunning,
			CoordinatorReady:  true,
			LastReconcileTime: &now,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
			FailedSegments: []FailedSegment{{ContentID: 1}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, ClusterPhaseRunning, copied.Phase)
	})

	t.Run("CoordinatorSpec non-nil", func(t *testing.T) {
		replicas := int32(1)
		src := &CoordinatorSpec{
			Replicas:     &replicas,
			NodeSelector: map[string]string{"role": "coord"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(1), *copied.Replicas)
	})

	t.Run("StandbySpec non-nil", func(t *testing.T) {
		src := &StandbySpec{
			Enabled:      true,
			NodeSelector: map[string]string{"role": "standby"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("SegmentsSpec non-nil", func(t *testing.T) {
		src := &SegmentsSpec{
			Count:        4,
			NodeSelector: map[string]string{"role": "seg"},
			Tolerations:  []Toleration{{Key: "k"}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(4), copied.Count)
	})

	t.Run("AuthSpec non-nil", func(t *testing.T) {
		src := &AuthSpec{
			Basic:    &BasicAuthSpec{Enabled: true},
			HBARules: []HBARule{{Type: HBATypeHost}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Basic.Enabled)
	})

	t.Run("BasicAuthSpec non-nil", func(t *testing.T) {
		src := &BasicAuthSpec{
			Enabled:             true,
			AdminPasswordSecret: &SecretKeyRef{Name: "s"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("OIDCSpec non-nil", func(t *testing.T) {
		src := &OIDCSpec{
			Enabled:     true,
			Scopes:      []string{"openid"},
			RoleMapping: map[string]string{"admin": "Admin"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("OIDCSecretRef non-nil", func(t *testing.T) {
		src := &OIDCSecretRef{SecretRef: &SecretKeyRef{Name: "s"}}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "s", copied.SecretRef.Name)
	})

	t.Run("SSLSpec non-nil", func(t *testing.T) {
		src := &SSLSpec{Enabled: true, CertSecret: &CertSecretRef{Name: "c"}}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("ConfigSpec non-nil", func(t *testing.T) {
		src := &ConfigSpec{
			Parameters:            map[string]string{"k": "v"},
			CoordinatorParameters: map[string]string{"ck": "cv"},
			DatabaseParameters:    map[string]map[string]string{"db": {"k": "v"}},
			RoleParameters:        map[string]map[string]string{"role": {"k": "v"}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "v", copied.Parameters["k"])
	})

	t.Run("VaultSpec non-nil", func(t *testing.T) {
		src := &VaultSpec{
			Enabled:   true,
			TLSSecret: &VaultTLSSecret{Name: "tls"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("ResourceRequirements non-nil", func(t *testing.T) {
		src := &ResourceRequirements{
			Requests: &ResourceList{CPU: "1"},
			Limits:   &ResourceList{CPU: "2"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "1", copied.Requests.CPU)
	})

	t.Run("Toleration with TolerationSeconds non-nil", func(t *testing.T) {
		secs := int64(300)
		src := &Toleration{Key: "k", TolerationSeconds: &secs}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int64(300), *copied.TolerationSeconds)
	})

	t.Run("WorkloadSpec non-nil", func(t *testing.T) {
		src := &WorkloadSpec{
			Enabled:        true,
			ResourceGroups: []ResourceGroupSpec{{Name: "rg"}},
			Rules:          []WorkloadRule{{Name: "r"}},
			IdleRules:      []IdleSessionRule{{Name: "ir"}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("BackupSpec non-nil", func(t *testing.T) {
		src := &BackupSpec{
			Enabled: true,
			Destination: BackupDestination{
				Type: "s3",
				S3: &S3Destination{
					CredentialSecret: &S3CredentialSecret{Name: "cred"},
				},
			},
			Gpbackup:  &GpbackupOptions{CompressionLevel: 1},
			Gprestore: &GprestoreOptions{Jobs: 1},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("BackupDestination non-nil", func(t *testing.T) {
		src := &BackupDestination{
			Type: "s3",
			S3: &S3Destination{
				CredentialSecret: &S3CredentialSecret{Name: "cred"},
				ForcePathStyle:   true,
			},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "s3", copied.Type)
	})

	t.Run("DataLoadingSpec non-nil", func(t *testing.T) {
		src := &DataLoadingSpec{
			Enabled: true,
			Pxf:     &PxfSpec{Enabled: true, Image: "pxf:1.0"},
			Gpfdist: &GpfdistSpec{Enabled: true, Replicas: ptr.To(int32(1))},
			Jobs:    []DataLoadingJob{{Name: "j"}},
			JobTemplate: &DataLoadingJobTemplate{
				BackoffLimit: ptr.To(int32(3)),
			},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Enabled)
	})

	t.Run("PxfSpec non-nil with servers", func(t *testing.T) {
		src := &PxfSpec{
			Enabled: true, Image: "pxf:1.0",
			Extensions: &PxfExtensionsSpec{Pxf: ptr.To(true), PxfFdw: ptr.To(true)},
			Servers: []PxfServerSpec{
				{Name: "s1", Type: "s3", Config: map[string]string{"k": "v"},
					CredentialSecrets: []SecretReference{{Name: "c"}}},
			},
			Resources: &ResourceRequirements{},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "pxf:1.0", copied.Image)
	})

	t.Run("DataLoadingJob with pxf and gpload bodies", func(t *testing.T) {
		src := &DataLoadingJob{
			Name: "j",
			PxfJob: &PxfJobSpec{
				Server: "s1", Profile: "s3:parquet", TargetTable: "t",
				FilterPushdown: ptr.To(true), ColumnProjection: ptr.To(true),
				Partitioning:  &PartitioningSpec{Column: "c", Range: "r", Interval: "i"},
				ErrorHandling: &ErrorHandlingSpec{SegmentRejectLimitType: "rows", LogErrors: ptr.To(true)},
			},
			GploadJob: &GploadJobSpec{TargetTable: "t", FilePaths: []string{"f"}},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "j", copied.Name)
	})

	t.Run("StorageManagementSpec non-nil", func(t *testing.T) {
		src := &StorageManagementSpec{
			DiskMonitoring:     true,
			RecommendationScan: &RecommendationScanSpec{Enabled: true},
			UsageReport:        &UsageReportSpec{Enabled: true},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.DiskMonitoring)
	})

	t.Run("GpfdistSpec non-nil", func(t *testing.T) {
		src := &GpfdistSpec{
			Enabled: true, Replicas: ptr.To(int32(2)), Image: "g:1", Port: 8080,
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(8080), copied.Port)
	})

	t.Run("DataLoadingJobTemplate non-nil", func(t *testing.T) {
		src := &DataLoadingJobTemplate{
			Resources:               &ResourceRequirements{},
			NodeSelector:            map[string]string{"k": "v"},
			Tolerations:             []Toleration{{Key: "k"}},
			ServiceAccountName:      "sa",
			BackoffLimit:            ptr.To(int32(3)),
			ActiveDeadlineSeconds:   ptr.To(int64(14400)),
			TTLSecondsAfterFinished: ptr.To(int32(86400)),
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "sa", copied.ServiceAccountName)
	})
}

func TestDeepCopy_PopulatedSubTypes(t *testing.T) {
	t.Run("MirroringSpec non-nil", func(t *testing.T) {
		src := &MirroringSpec{Enabled: true, Layout: MirroringLayoutGroup}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, src.Enabled, copied.Enabled)
	})

	t.Run("HASpec non-nil", func(t *testing.T) {
		src := &HASpec{FTSProbeInterval: 60, Checksums: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(60), copied.FTSProbeInterval)
	})

	t.Run("MonitoringSpec non-nil", func(t *testing.T) {
		src := &MonitoringSpec{Enabled: true, MetricsPort: 9187}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(9187), copied.MetricsPort)
	})

	t.Run("TelemetrySpec non-nil", func(t *testing.T) {
		src := &TelemetrySpec{Enabled: true, SamplingRate: 0.5}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, 0.5, copied.SamplingRate)
	})

	t.Run("QueryMonitoringSpec non-nil", func(t *testing.T) {
		src := &QueryMonitoringSpec{Enabled: true, HistoryRetention: "30d"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "30d", copied.HistoryRetention)
	})

	t.Run("ResourceGroupSpec non-nil", func(t *testing.T) {
		src := &ResourceGroupSpec{Name: "test", Concurrency: 10}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "test", copied.Name)
	})

	t.Run("WorkloadRule non-nil", func(t *testing.T) {
		src := &WorkloadRule{Name: "rule", Action: "cancel"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "cancel", copied.Action)
	})

	t.Run("IdleSessionRule non-nil", func(t *testing.T) {
		src := &IdleSessionRule{Name: "idle", IdleTimeout: "30m", ResourceGroup: "default"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "30m", copied.IdleTimeout)
	})

	t.Run("BackupRetention non-nil", func(t *testing.T) {
		src := &BackupRetention{FullCount: 7}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(7), copied.FullCount)
	})

	t.Run("SecretReference non-nil", func(t *testing.T) {
		src := &SecretReference{Name: "creds"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "creds", copied.Name)
	})

	t.Run("RecommendationScanSpec non-nil", func(t *testing.T) {
		src := &RecommendationScanSpec{Enabled: true, BloatThreshold: 20}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(20), copied.BloatThreshold)
	})

	t.Run("UsageReportSpec non-nil", func(t *testing.T) {
		src := &UsageReportSpec{Enabled: true, Monthly: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Monthly)
	})

	t.Run("FailedSegment non-nil", func(t *testing.T) {
		src := &FailedSegment{ContentID: 1, Hostname: "host1"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, int32(1), copied.ContentID)
	})

	t.Run("ImagePullSecret non-nil", func(t *testing.T) {
		src := &ImagePullSecret{Name: "secret"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "secret", copied.Name)
	})

	t.Run("CertSecretRef non-nil", func(t *testing.T) {
		src := &CertSecretRef{Name: "cert"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "cert", copied.Name)
	})

	t.Run("VaultTLSSecret non-nil", func(t *testing.T) {
		src := &VaultTLSSecret{Name: "tls"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "tls", copied.Name)
	})

	t.Run("ResourceList non-nil", func(t *testing.T) {
		src := &ResourceList{CPU: "4", Memory: "8Gi"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "4", copied.CPU)
	})

	t.Run("StorageSpec non-nil", func(t *testing.T) {
		src := &StorageSpec{StorageClass: "fast", Size: "100Gi"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "fast", copied.StorageClass)
	})

	t.Run("HBARule non-nil", func(t *testing.T) {
		src := &HBARule{Type: HBATypeHost, Database: "all"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, HBATypeHost, copied.Type)
	})

	t.Run("SecretKeyRef non-nil", func(t *testing.T) {
		src := &SecretKeyRef{Name: "secret", Key: "key"}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "secret", copied.Name)
	})
}

// TestExporterSpec_Segments verifies the OPT-IN per-segment postgres-exporter
// toggle: it defaults to false (zero value) and survives a deepcopy round-trip
// independently of the source.
func TestExporterSpec_Segments(t *testing.T) {
	t.Run("zero value defaults to false (OFF)", func(t *testing.T) {
		var spec ExporterSpec
		assert.False(t, spec.Segments, "Segments must default to false (opt-in OFF)")
	})

	t.Run("deepcopy round-trips Segments=true independently", func(t *testing.T) {
		src := &ExporterSpec{Enabled: true, Image: "img", Port: 9187, Segments: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Segments)

		// Mutating the copy must not affect the source.
		copied.Segments = false
		assert.True(t, src.Segments, "deepcopy must be independent of the source")
	})
}

// TestExporterSpec_Mirrors verifies the OPT-IN per-mirror postgres-exporter
// toggle: it defaults to false (zero value) and survives a deepcopy round-trip
// independently of the source and of the Segments toggle.
func TestExporterSpec_Mirrors(t *testing.T) {
	t.Run("zero value defaults to false (OFF)", func(t *testing.T) {
		var spec ExporterSpec
		assert.False(t, spec.Mirrors, "Mirrors must default to false (opt-in OFF)")
	})

	t.Run("deepcopy round-trips Mirrors=true independently", func(t *testing.T) {
		src := &ExporterSpec{Enabled: true, Image: "img", Port: 9187, Mirrors: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Mirrors)

		// Mutating the copy must not affect the source.
		copied.Mirrors = false
		assert.True(t, src.Mirrors, "deepcopy must be independent of the source")
	})

	t.Run("Segments and Mirrors are independent toggles", func(t *testing.T) {
		src := &ExporterSpec{Enabled: true, Segments: true, Mirrors: false}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.True(t, copied.Segments)
		assert.False(t, copied.Mirrors)
	})
}

// TestS3VaultSecret_DeepCopy verifies the Vault-sourced S3 credential alternative
// (Scenario 69c) survives a deepcopy round-trip independently of the source, both
// as a standalone type and as the optional VaultSecret pointer field on
// S3Destination.
func TestS3VaultSecret_DeepCopy(t *testing.T) {
	t.Run("standalone round-trip is independent", func(t *testing.T) {
		src := &S3VaultSecret{
			Path:           "secret/data/cloudberry/backup-s3",
			AccessKeyField: "aws_access_key_id",
			SecretKeyField: "aws_secret_access_key",
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, src.Path, copied.Path)
		assert.Equal(t, src.AccessKeyField, copied.AccessKeyField)
		assert.Equal(t, src.SecretKeyField, copied.SecretKeyField)

		// Mutating the copy must not affect the source.
		copied.Path = "secret/data/changed"
		assert.Equal(t, "secret/data/cloudberry/backup-s3", src.Path)
	})

	t.Run("DeepCopyInto copies all fields", func(t *testing.T) {
		src := S3VaultSecret{Path: "secret/data/p", AccessKeyField: "ak", SecretKeyField: "sk"}
		var dst S3VaultSecret
		src.DeepCopyInto(&dst)
		assert.Equal(t, "secret/data/p", dst.Path)
		assert.Equal(t, "ak", dst.AccessKeyField)
		assert.Equal(t, "sk", dst.SecretKeyField)
	})

	t.Run("S3Destination with VaultSecret pointer field round-trips", func(t *testing.T) {
		src := &S3Destination{
			Bucket:      "my-bucket",
			VaultSecret: &S3VaultSecret{Path: "secret/data/cloudberry/backup-s3"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		require.NotNil(t, copied.VaultSecret)
		assert.Equal(t, src.VaultSecret.Path, copied.VaultSecret.Path)

		// The pointer must be an independent copy.
		copied.VaultSecret.Path = "secret/data/changed"
		assert.Equal(t, "secret/data/cloudberry/backup-s3", src.VaultSecret.Path)
		assert.NotSame(t, src.VaultSecret, copied.VaultSecret)
	})
}

// TestDataLoadingJobStatus_ExecutionFields exercises the additive execution
// status fields (lastRun/lastStatus/rowsLoaded/duration): backward-compatible
// marshaling of a {name,enabled}-only status, a full round-trip with all four
// fields set, and deep-copy independence of the *metav1.Time and *int64
// pointers.
func TestDataLoadingJobStatus_ExecutionFields(t *testing.T) {
	t.Run("name+enabled only marshals identically (backward compatible)", func(t *testing.T) {
		s := DataLoadingJobStatus{Name: "job1", Enabled: true}
		data, err := json.Marshal(s)
		require.NoError(t, err)
		// The four optional execution fields must be omitted entirely when unset.
		assert.JSONEq(t, `{"name":"job1","enabled":true}`, string(data))
		assert.NotContains(t, string(data), "lastRun")
		assert.NotContains(t, string(data), "lastStatus")
		assert.NotContains(t, string(data), "rowsLoaded")
		assert.NotContains(t, string(data), "duration")
	})

	t.Run("full execution fields JSON round-trip", func(t *testing.T) {
		now := metav1.Now()
		src := DataLoadingJobStatus{
			Name:       "s3-parquet-loader",
			Enabled:    true,
			LastRun:    &now,
			LastStatus: "Succeeded",
			RowsLoaded: ptr.To[int64](183961),
			Duration:   "1m30s",
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)

		var got DataLoadingJobStatus
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, src.Name, got.Name)
		assert.Equal(t, src.Enabled, got.Enabled)
		assert.Equal(t, src.LastStatus, got.LastStatus)
		assert.Equal(t, src.Duration, got.Duration)
		require.NotNil(t, got.RowsLoaded)
		assert.Equal(t, int64(183961), *got.RowsLoaded)
		require.NotNil(t, got.LastRun)
	})

	t.Run("DeepCopy independence of pointer fields", func(t *testing.T) {
		now := metav1.Now()
		src := DataLoadingJobStatus{
			Name:       "job1",
			Enabled:    true,
			LastRun:    &now,
			LastStatus: "Succeeded",
			RowsLoaded: ptr.To[int64](100),
			Duration:   "2m",
		}
		dst := src.DeepCopy()
		require.NotNil(t, dst)
		require.NotNil(t, dst.RowsLoaded)
		require.NotNil(t, dst.LastRun)

		// The pointers must be independent copies, not aliases of the source.
		assert.NotSame(t, src.RowsLoaded, dst.RowsLoaded)
		assert.NotSame(t, src.LastRun, dst.LastRun)

		// Mutating the copy must not affect the source.
		*dst.RowsLoaded = 999
		dst.LastStatus = "Failed"
		dst.Duration = "9m"
		assert.Equal(t, int64(100), *src.RowsLoaded)
		assert.Equal(t, "Succeeded", src.LastStatus)
		assert.Equal(t, "2m", src.Duration)
	})
}

// TestEventReasonDataLoadingDisabled pins the Scenario 112 (DIS.1) event-reason
// constant: the one-shot Normal event emitted when the data-loading subsystem is
// torn down (dataLoading.enabled=false). Its stable string value is part of the
// observable contract (consumers match on the literal reason).
func TestEventReasonDataLoadingDisabled(t *testing.T) {
	assert.Equal(t, "DataLoadingDisabled", EventReasonDataLoadingDisabled)
}
