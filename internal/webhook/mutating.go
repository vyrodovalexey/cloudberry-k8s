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
	setWorkloadDefaults(cluster)
	setQueryMonitoringDefaults(cluster)
	setBackupDefaults(cluster)
	setDataLoadingDefaults(cluster)
	setStorageManagementDefaults(cluster)

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

// setWorkloadDefaults sets workload management defaults.
func setWorkloadDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Workload == nil {
		// Workload management is optional; no defaults needed when not specified.
		return
	}

	for i := range cluster.Spec.Workload.ResourceGroups {
		rg := &cluster.Spec.Workload.ResourceGroups[i]
		if rg.Concurrency == 0 {
			rg.Concurrency = 20
		}
		if rg.CPUMaxPercent == 0 {
			rg.CPUMaxPercent = 100
		}
		if rg.CPUWeight == 0 {
			rg.CPUWeight = 100
		}
	}
}

// setQueryMonitoringDefaults sets query monitoring defaults.
func setQueryMonitoringDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.QueryMonitoring == nil {
		// Query monitoring is optional; no defaults needed when not specified.
		return
	}

	if cluster.Spec.QueryMonitoring.HistoryRetention == "" {
		cluster.Spec.QueryMonitoring.HistoryRetention = "30d"
	}
	if cluster.Spec.QueryMonitoring.SamplingInterval == 0 {
		cluster.Spec.QueryMonitoring.SamplingInterval = 15
	}
	if cluster.Spec.QueryMonitoring.SlowQueryThreshold == "" {
		cluster.Spec.QueryMonitoring.SlowQueryThreshold = "1000ms"
	}
}

// setBackupDefaults sets backup defaults.
func setBackupDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Backup == nil {
		// Backup is optional; no defaults needed when not specified.
		return
	}

	if cluster.Spec.Backup.Compression == 0 {
		cluster.Spec.Backup.Compression = 6
	}
	if cluster.Spec.Backup.Parallelism == 0 {
		cluster.Spec.Backup.Parallelism = 1
	}
	if cluster.Spec.Backup.Retention.FullCount == 0 {
		cluster.Spec.Backup.Retention.FullCount = 3
	}
	if cluster.Spec.Backup.Retention.MaxAge == "" {
		cluster.Spec.Backup.Retention.MaxAge = "30d"
	}
}

// setDataLoadingDefaults sets data loading defaults.
func setDataLoadingDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.DataLoading == nil {
		// Data loading is optional; no defaults needed when not specified.
		return
	}

	if cluster.Spec.DataLoading.StreamingServer != nil {
		if cluster.Spec.DataLoading.StreamingServer.Port == 0 {
			cluster.Spec.DataLoading.StreamingServer.Port = 5432
		}
		if cluster.Spec.DataLoading.StreamingServer.TLSMode == "" {
			cluster.Spec.DataLoading.StreamingServer.TLSMode = "none"
		}
	}
}

// setStorageManagementDefaults sets storage management defaults.
func setStorageManagementDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Storage == nil {
		// Storage management is optional; no defaults needed when not specified.
		return
	}

	scan := cluster.Spec.Storage.RecommendationScan
	if scan != nil && scan.Enabled {
		if scan.Schedule == "" {
			scan.Schedule = "0 3 * * 0"
		}
		if scan.BloatThreshold == 0 {
			scan.BloatThreshold = 20
		}
		if scan.SkewThreshold == 0 {
			scan.SkewThreshold = 50
		}
		if scan.AgeThreshold == 0 {
			scan.AgeThreshold = 500000000
		}
		if scan.IndexBloatThreshold == 0 {
			scan.IndexBloatThreshold = 30
		}
		if scan.ScanDuration == "" {
			scan.ScanDuration = "2h"
		}
	}
}
