package testutil

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ClusterBuilder provides a fluent API for building CloudberryCluster test fixtures.
type ClusterBuilder struct {
	cluster *cbv1alpha1.CloudberryCluster
}

// NewClusterBuilder creates a new ClusterBuilder with sensible defaults.
func NewClusterBuilder(name, namespace string) *ClusterBuilder {
	replicas := int32(1)
	return &ClusterBuilder{
		cluster: &cbv1alpha1.CloudberryCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: cbv1alpha1.GroupVersion.String(),
				Kind:       "CloudberryCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   namespace,
				Annotations: make(map[string]string),
			},
			Spec: cbv1alpha1.CloudberryClusterSpec{
				Version:         util.DefaultVersion,
				Image:           util.DefaultImage,
				ImagePullPolicy: cbv1alpha1.ImagePullIfNotPresent,
				Coordinator: cbv1alpha1.CoordinatorSpec{
					Replicas: &replicas,
					Port:     int32(util.DefaultCoordinatorPort),
					Storage: cbv1alpha1.StorageSpec{
						Size: "10Gi",
					},
				},
				Segments: cbv1alpha1.SegmentsSpec{
					Count:            4,
					PrimariesPerHost: 2,
					Storage: cbv1alpha1.StorageSpec{
						Size: "20Gi",
					},
					AntiAffinity: cbv1alpha1.AntiAffinityPreferred,
				},
				DeletionPolicy: cbv1alpha1.DeletionPolicyRetain,
			},
		},
	}
}

// WithVersion sets the cluster version.
func (b *ClusterBuilder) WithVersion(version string) *ClusterBuilder {
	b.cluster.Spec.Version = version
	return b
}

// WithImage sets the container image.
func (b *ClusterBuilder) WithImage(image string) *ClusterBuilder {
	b.cluster.Spec.Image = image
	return b
}

// WithSegments sets the segment count.
func (b *ClusterBuilder) WithSegments(count int32) *ClusterBuilder {
	b.cluster.Spec.Segments.Count = count
	return b
}

// WithMirroring enables or disables mirroring.
func (b *ClusterBuilder) WithMirroring(enabled bool, layout cbv1alpha1.MirroringLayout) *ClusterBuilder {
	b.cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: enabled,
		Layout:  layout,
	}
	return b
}

// WithStandby enables or disables the standby coordinator.
func (b *ClusterBuilder) WithStandby(enabled bool) *ClusterBuilder {
	b.cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: enabled,
	}
	return b
}

// WithBasicAuth configures basic authentication.
func (b *ClusterBuilder) WithBasicAuth(enabled bool, adminUser string) *ClusterBuilder {
	if b.cluster.Spec.Auth == nil {
		b.cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	b.cluster.Spec.Auth.Basic = &cbv1alpha1.BasicAuthSpec{
		Enabled:   enabled,
		AdminUser: adminUser,
	}
	return b
}

// WithOIDC configures OIDC authentication.
func (b *ClusterBuilder) WithOIDC(enabled bool, issuerURL, clientID string) *ClusterBuilder {
	if b.cluster.Spec.Auth == nil {
		b.cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	b.cluster.Spec.Auth.OIDC = &cbv1alpha1.OIDCSpec{
		Enabled:   enabled,
		IssuerURL: issuerURL,
		ClientID:  clientID,
		Scopes:    []string{"openid", "profile", "email"},
	}
	return b
}

// WithHBARules sets the HBA rules.
func (b *ClusterBuilder) WithHBARules(rules []cbv1alpha1.HBARule) *ClusterBuilder {
	if b.cluster.Spec.Auth == nil {
		b.cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	b.cluster.Spec.Auth.HBARules = rules
	return b
}

// WithSSL enables SSL with the given cert secret.
func (b *ClusterBuilder) WithSSL(enabled bool, certSecretName string) *ClusterBuilder {
	if b.cluster.Spec.Auth == nil {
		b.cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	b.cluster.Spec.Auth.SSL = &cbv1alpha1.SSLSpec{
		Enabled: enabled,
	}
	if certSecretName != "" {
		b.cluster.Spec.Auth.SSL.CertSecret = &cbv1alpha1.CertSecretRef{
			Name: certSecretName,
		}
	}
	return b
}

// WithConfig sets configuration parameters.
func (b *ClusterBuilder) WithConfig(params map[string]string) *ClusterBuilder {
	b.cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: params,
	}
	return b
}

// WithHA configures high availability settings.
func (b *ClusterBuilder) WithHA(probeInterval, probeTimeout, probeRetries int32) *ClusterBuilder {
	b.cluster.Spec.HA = &cbv1alpha1.HASpec{
		FTSProbeInterval: probeInterval,
		FTSProbeTimeout:  probeTimeout,
		FTSProbeRetries:  probeRetries,
		Checksums:        true,
	}
	return b
}

// WithVault configures Vault integration.
func (b *ClusterBuilder) WithVault(enabled bool, address, authMethod string) *ClusterBuilder {
	b.cluster.Spec.Vault = &cbv1alpha1.VaultSpec{
		Enabled:    enabled,
		Address:    address,
		AuthMethod: cbv1alpha1.VaultAuthMethod(authMethod),
	}
	return b
}

// WithMonitoring configures monitoring.
func (b *ClusterBuilder) WithMonitoring(enabled bool, metricsPort int32) *ClusterBuilder {
	b.cluster.Spec.Monitoring = &cbv1alpha1.MonitoringSpec{
		Enabled:     enabled,
		MetricsPort: metricsPort,
	}
	return b
}

// WithTelemetry configures telemetry.
func (b *ClusterBuilder) WithTelemetry(enabled bool, endpoint string, protocol cbv1alpha1.OTLPProtocol) *ClusterBuilder {
	b.cluster.Spec.Telemetry = &cbv1alpha1.TelemetrySpec{
		Enabled:      enabled,
		OTLPEndpoint: endpoint,
		OTLPProtocol: protocol,
		SamplingRate: 1.0,
	}
	return b
}

// WithDeletionPolicy sets the deletion policy.
func (b *ClusterBuilder) WithDeletionPolicy(policy cbv1alpha1.DeletionPolicy) *ClusterBuilder {
	b.cluster.Spec.DeletionPolicy = policy
	return b
}

// WithAnnotation adds an annotation.
func (b *ClusterBuilder) WithAnnotation(key, value string) *ClusterBuilder {
	if b.cluster.Annotations == nil {
		b.cluster.Annotations = make(map[string]string)
	}
	b.cluster.Annotations[key] = value
	return b
}

// WithPhase sets the cluster status phase.
func (b *ClusterBuilder) WithPhase(phase cbv1alpha1.ClusterPhase) *ClusterBuilder {
	b.cluster.Status.Phase = phase
	return b
}

// WithStatusReady sets the cluster status to a ready state.
func (b *ClusterBuilder) WithStatusReady() *ClusterBuilder {
	b.cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	b.cluster.Status.CoordinatorReady = true
	b.cluster.Status.SegmentsReady = b.cluster.Spec.Segments.Count
	b.cluster.Status.SegmentsTotal = b.cluster.Spec.Segments.Count
	b.cluster.Status.ClusterVersion = b.cluster.Spec.Version
	return b
}

// WithResources sets resource requirements for the coordinator.
func (b *ClusterBuilder) WithResources(cpuReq, memReq, cpuLim, memLim string) *ClusterBuilder {
	b.cluster.Spec.Coordinator.Resources = &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{
			CPU:    cpuReq,
			Memory: memReq,
		},
		Limits: &cbv1alpha1.ResourceList{
			CPU:    cpuLim,
			Memory: memLim,
		},
	}
	return b
}

// WithFinalizer adds the operator finalizer.
func (b *ClusterBuilder) WithFinalizer() *ClusterBuilder {
	b.cluster.Finalizers = append(b.cluster.Finalizers, util.FinalizerName)
	return b
}

// WithGeneration sets the generation and observed generation.
func (b *ClusterBuilder) WithGeneration(gen int64) *ClusterBuilder {
	b.cluster.Generation = gen
	b.cluster.Status.ObservedGeneration = gen
	return b
}

// WithPendingGeneration sets the generation higher than observed generation
// to simulate a spec change that needs reconciliation.
func (b *ClusterBuilder) WithPendingGeneration() *ClusterBuilder {
	b.cluster.Generation = 1
	b.cluster.Status.ObservedGeneration = 0
	return b
}

// WithCoordinatorParameters sets coordinator-only parameters.
func (b *ClusterBuilder) WithCoordinatorParameters(params map[string]string) *ClusterBuilder {
	if b.cluster.Spec.Config == nil {
		b.cluster.Spec.Config = &cbv1alpha1.ConfigSpec{}
	}
	b.cluster.Spec.Config.CoordinatorParameters = params
	return b
}

// WithDatabaseParameters sets per-database parameters.
func (b *ClusterBuilder) WithDatabaseParameters(params map[string]map[string]string) *ClusterBuilder {
	if b.cluster.Spec.Config == nil {
		b.cluster.Spec.Config = &cbv1alpha1.ConfigSpec{}
	}
	b.cluster.Spec.Config.DatabaseParameters = params
	return b
}

// WithRoleParameters sets per-role parameters.
func (b *ClusterBuilder) WithRoleParameters(params map[string]map[string]string) *ClusterBuilder {
	if b.cluster.Spec.Config == nil {
		b.cluster.Spec.Config = &cbv1alpha1.ConfigSpec{}
	}
	b.cluster.Spec.Config.RoleParameters = params
	return b
}

// WithBackupOnDelete enables backup on delete.
func (b *ClusterBuilder) WithBackupOnDelete(enabled bool) *ClusterBuilder {
	b.cluster.Spec.BackupOnDelete = enabled
	return b
}

// Build returns the constructed CloudberryCluster.
func (b *ClusterBuilder) Build() *cbv1alpha1.CloudberryCluster {
	return b.cluster.DeepCopy()
}

// DefaultHBARules returns the default HBA rules for testing.
func DefaultHBARules() []cbv1alpha1.HBARule {
	return []cbv1alpha1.HBARule{
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
			Address:  "0.0.0.0/0",
			Method:   cbv1alpha1.AuthMethodScramSHA256,
		},
	}
}

// CustomHBARules returns custom HBA rules for testing.
func CustomHBARules() []cbv1alpha1.HBARule {
	return []cbv1alpha1.HBARule{
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
	}
}

// MinimalCluster creates a minimal valid CloudberryCluster for testing.
func MinimalCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	return NewClusterBuilder(name, namespace).Build()
}

// FullCluster creates a fully configured CloudberryCluster for testing.
func FullCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	return NewClusterBuilder(name, namespace).
		WithSegments(8).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithBasicAuth(true, "gpadmin").
		WithOIDC(true, "http://keycloak:8090/realms/test", "cloudberry-client").
		WithHBARules(DefaultHBARules()).
		WithSSL(true, "cloudberry-tls").
		WithConfig(map[string]string{
			"shared_buffers":  "256MB",
			"work_mem":        "64MB",
			"max_connections": "200",
		}).
		WithHA(60, 20, 5).
		WithVault(true, "http://vault:8200", "token").
		WithMonitoring(true, 9187).
		WithTelemetry(true, "tempo:4317", cbv1alpha1.OTLPProtocolGRPC).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithResources("1", "2Gi", "2", "4Gi").
		Build()
}
