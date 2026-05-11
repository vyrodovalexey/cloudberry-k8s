package webhook

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// CloudberryClusterDefaulter sets defaults on CloudberryCluster resources.
type CloudberryClusterDefaulter struct{}

// NewCloudberryClusterDefaulter creates a new CloudberryClusterDefaulter.
func NewCloudberryClusterDefaulter() *CloudberryClusterDefaulter {
	return &CloudberryClusterDefaulter{}
}

// Default sets defaults on a CloudberryCluster.
func (d *CloudberryClusterDefaulter) Default(_ context.Context, obj runtime.Object) error {
	cluster, ok := obj.(*cbv1alpha1.CloudberryCluster)
	if !ok {
		return fmt.Errorf("expected CloudberryCluster, got %T", obj)
	}

	setClusterDefaults(cluster)
	return nil
}

// setClusterDefaults applies default values to a CloudberryCluster.
func setClusterDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	setSpecDefaults(&cluster.Spec)
	setCoordinatorDefaults(&cluster.Spec.Coordinator)
	setSegmentDefaults(&cluster.Spec.Segments)
	setAuthDefaults(cluster)
	setHADefaults(cluster)
	setMonitoringDefaults(cluster)

	if cluster.Spec.DeletionPolicy == "" {
		cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyRetain
	}
}

// setSpecDefaults sets top-level spec defaults.
func setSpecDefaults(spec *cbv1alpha1.CloudberryClusterSpec) {
	if spec.Version == "" {
		spec.Version = util.DefaultVersion
	}
	if spec.Image == "" {
		spec.Image = util.DefaultImage
	}
	if spec.ImagePullPolicy == "" {
		spec.ImagePullPolicy = cbv1alpha1.ImagePullIfNotPresent
	}
}

// setCoordinatorDefaults sets coordinator defaults.
func setCoordinatorDefaults(coord *cbv1alpha1.CoordinatorSpec) {
	if coord.Replicas == nil {
		coord.Replicas = util.Ptr(int32(1))
	}
	if coord.Port == 0 {
		coord.Port = int32(util.DefaultCoordinatorPort)
	}
	if coord.Storage.Size == "" {
		coord.Storage.Size = "10Gi"
	}
}

// setSegmentDefaults sets segment defaults.
func setSegmentDefaults(seg *cbv1alpha1.SegmentsSpec) {
	if seg.PrimariesPerHost == 0 {
		seg.PrimariesPerHost = 2
	}
	if seg.Storage.Size == "" {
		seg.Storage.Size = "20Gi"
	}
	if seg.AntiAffinity == "" {
		seg.AntiAffinity = cbv1alpha1.AntiAffinityPreferred
	}
	if seg.Mirroring == nil {
		seg.Mirroring = &cbv1alpha1.MirroringSpec{
			Enabled: true,
			Layout:  cbv1alpha1.MirroringLayoutGroup,
		}
	}
	if seg.Mirroring.Layout == "" {
		seg.Mirroring.Layout = cbv1alpha1.MirroringLayoutGroup
	}
}

// setAuthDefaults sets authentication defaults.
func setAuthDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Auth == nil {
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	if cluster.Spec.Auth.Basic == nil {
		cluster.Spec.Auth.Basic = &cbv1alpha1.BasicAuthSpec{
			Enabled:   true,
			AdminUser: util.DefaultAdminUser,
		}
	}
	if cluster.Spec.Auth.Basic.AdminUser == "" {
		cluster.Spec.Auth.Basic.AdminUser = util.DefaultAdminUser
	}
	setOIDCDefaults(cluster.Spec.Auth.OIDC)
}

// setOIDCDefaults sets OIDC defaults if OIDC is enabled.
func setOIDCDefaults(oidc *cbv1alpha1.OIDCSpec) {
	if oidc == nil || !oidc.Enabled {
		return
	}
	if len(oidc.Scopes) == 0 {
		oidc.Scopes = []string{"openid", "profile", "email"}
	}
	if oidc.RoleClaimPath == "" {
		oidc.RoleClaimPath = "realm_access.roles"
	}
	if oidc.RoleClaimSource == "" {
		oidc.RoleClaimSource = cbv1alpha1.RoleClaimSourceIDToken
	}
	if oidc.RoleMatchMode == "" {
		oidc.RoleMatchMode = cbv1alpha1.RoleMatchExact
	}
}

// setHADefaults sets HA defaults.
func setHADefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.HA == nil {
		cluster.Spec.HA = &cbv1alpha1.HASpec{}
	}
	if cluster.Spec.HA.FTSProbeInterval == 0 {
		cluster.Spec.HA.FTSProbeInterval = 60
	}
	if cluster.Spec.HA.FTSProbeTimeout == 0 {
		cluster.Spec.HA.FTSProbeTimeout = 20
	}
	if cluster.Spec.HA.FTSProbeRetries == 0 {
		cluster.Spec.HA.FTSProbeRetries = 5
	}
}

// setMonitoringDefaults sets monitoring defaults.
func setMonitoringDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Monitoring == nil {
		cluster.Spec.Monitoring = &cbv1alpha1.MonitoringSpec{
			Enabled:     true,
			MetricsPort: int32(util.DefaultMetricsPort),
		}
	}
	if cluster.Spec.Monitoring.MetricsPort == 0 {
		cluster.Spec.Monitoring.MetricsPort = int32(util.DefaultMetricsPort)
	}
}
