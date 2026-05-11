package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		{"SecretReference", func() interface{} { var s *SecretReference; return s.DeepCopy() }},
		{"DataLoadingSpec", func() interface{} { var s *DataLoadingSpec; return s.DeepCopy() }},
		{"StreamingServerSpec", func() interface{} { var s *StreamingServerSpec; return s.DeepCopy() }},
		{"DataLoadingJob", func() interface{} { var s *DataLoadingJob; return s.DeepCopy() }},
		{"S3SourceSpec", func() interface{} { var s *S3SourceSpec; return s.DeepCopy() }},
		{"KafkaSourceSpec", func() interface{} { var s *KafkaSourceSpec; return s.DeepCopy() }},
		{"RabbitMQSourceSpec", func() interface{} { var s *RabbitMQSourceSpec; return s.DeepCopy() }},
	}

	for _, tt := range nilTests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn()
			assert.Nil(t, result)
		})
	}
}
