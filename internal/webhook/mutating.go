package webhook

import (
	"context"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// Default values for CloudberryCluster fields.
const (
	// defaultPrimariesPerHost is the default number of primary segments per host.
	defaultPrimariesPerHost = 2
	// defaultFTSProbeInterval is the default FTS probe interval in seconds.
	defaultFTSProbeInterval = 60
	// defaultFTSProbeTimeout is the default FTS probe timeout in seconds.
	defaultFTSProbeTimeout = 20
	// defaultFTSProbeRetries is the default number of FTS probe retries.
	defaultFTSProbeRetries = 5
	// defaultResourceGroupConcurrency is the default concurrency for resource groups.
	defaultResourceGroupConcurrency = 20
	// defaultCPUMaxPercent is the default CPU max percent for resource groups.
	defaultCPUMaxPercent = 100
	// defaultCPUWeight is the default CPU weight for resource groups.
	defaultCPUWeight = 100
	// defaultSamplingInterval is the default query monitoring sampling interval in seconds.
	defaultSamplingInterval = 15
	// defaultBackupCompressionLevel is the default gpbackup compression level (1-9).
	defaultBackupCompressionLevel = 1
	// defaultBackupJobs is the default number of parallel gpbackup workers.
	defaultBackupJobs = 1
	// defaultBackupRetentionFullCount is the default number of full backups to retain.
	defaultBackupRetentionFullCount = 3
	// defaultBackupCompressionType is the default gpbackup compression algorithm.
	defaultBackupCompressionType = "gzip"
	// defaultBackupRetentionMaxAge is the default maximum backup age.
	defaultBackupRetentionMaxAge = "30d"
	// defaultBackupJobBackoffLimit is the default Job backoffLimit for backup/restore Jobs.
	defaultBackupJobBackoffLimit = int32(2)
	// defaultBackupJobActiveDeadlineSeconds is the default Job timeout (2 hours).
	defaultBackupJobActiveDeadlineSeconds = int64(7200)
	// defaultBackupJobTTLSecondsAfterFinished cleans up finished Jobs after 24 hours.
	defaultBackupJobTTLSecondsAfterFinished = int32(86400)
	// defaultStreamingServerPort is the default streaming server port.
	defaultStreamingServerPort = 5432
	// defaultBloatThreshold is the default table bloat threshold percentage.
	defaultBloatThreshold = 20
	// defaultSkewThreshold is the default data skew threshold percentage.
	defaultSkewThreshold = 50
	// defaultAgeThreshold is the default transaction age threshold for recommendations.
	defaultAgeThreshold = 500_000_000
	// defaultIndexBloatThreshold is the default index bloat threshold percentage.
	defaultIndexBloatThreshold = 30
)

// CloudberryClusterDefaulter sets defaults on CloudberryCluster resources.
type CloudberryClusterDefaulter struct {
	// recorder records admission metrics. It is optional and may be nil;
	// all metric recording is guarded with a nil check.
	recorder metrics.Recorder
}

// NewCloudberryClusterDefaulter creates a new CloudberryClusterDefaulter.
// An optional metrics recorder may be supplied to record admission metrics;
// when omitted (or nil), metric recording is a no-op.
func NewCloudberryClusterDefaulter(recorder ...metrics.Recorder) *CloudberryClusterDefaulter {
	d := &CloudberryClusterDefaulter{}
	if len(recorder) > 0 {
		d.recorder = recorder[0]
	}
	return d
}

// Default sets defaults on a CloudberryCluster.
func (d *CloudberryClusterDefaulter) Default(_ context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	setClusterDefaults(cluster)
	// The mutating webhook applies defaults on both create and update; record it
	// as a create admission for consistency. Defaulting never denies admission.
	if d.recorder != nil {
		d.recorder.RecordWebhookAdmission(webhookMutating, admissionOpCreate, admissionAllowed)
	}
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
		seg.PrimariesPerHost = defaultPrimariesPerHost
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
		cluster.Spec.HA.FTSProbeInterval = defaultFTSProbeInterval
	}
	if cluster.Spec.HA.FTSProbeTimeout == 0 {
		cluster.Spec.HA.FTSProbeTimeout = defaultFTSProbeTimeout
	}
	if cluster.Spec.HA.FTSProbeRetries == 0 {
		cluster.Spec.HA.FTSProbeRetries = defaultFTSProbeRetries
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
			rg.Concurrency = defaultResourceGroupConcurrency
		}
		if rg.CPUMaxPercent == 0 {
			rg.CPUMaxPercent = defaultCPUMaxPercent
		}
		if rg.CPUWeight == 0 {
			rg.CPUWeight = defaultCPUWeight
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
		cluster.Spec.QueryMonitoring.SamplingInterval = defaultSamplingInterval
	}
	if cluster.Spec.QueryMonitoring.SlowQueryThreshold == "" {
		cluster.Spec.QueryMonitoring.SlowQueryThreshold = "1000ms"
	}
}

// setBackupDefaults sets backup defaults per spec 11 §Webhook Defaults.
// All 12 defaults are applied only when backup is enabled and the corresponding
// field is unset/zero, taking care not to overwrite explicit user values.
func setBackupDefaults(cluster *cbv1alpha1.CloudberryCluster) {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		// Backup is optional; no defaults needed when not specified or disabled.
		return
	}
	setGpbackupDefaults(cluster.Spec.Backup)
	setGprestoreDefaults(cluster.Spec.Backup)
	setBackupRetentionDefaults(cluster.Spec.Backup)
	setBackupJobTemplateDefaults(cluster.Spec.Backup)
}

// setGpbackupDefaults applies gpbackup option defaults.
func setGpbackupDefaults(backup *cbv1alpha1.BackupSpec) {
	if backup.Gpbackup == nil {
		backup.Gpbackup = &cbv1alpha1.GpbackupOptions{}
	}
	gp := backup.Gpbackup
	if gp.CompressionLevel == 0 {
		gp.CompressionLevel = defaultBackupCompressionLevel
	}
	if gp.CompressionType == "" {
		gp.CompressionType = defaultBackupCompressionType
	}
	if gp.Jobs == 0 {
		gp.Jobs = defaultBackupJobs
	}
	// gp.SingleDataFile defaults to false; the zero value already matches the
	// spec default, so no explicit assignment is required here.
	// WithStats is a *bool: default to true only when unset (nil) so an explicit
	// withStats:false set by the user is preserved rather than silently reverted.
	if gp.WithStats == nil {
		gp.WithStats = util.Ptr(true)
	}
}

// setGprestoreDefaults applies gprestore option defaults.
func setGprestoreDefaults(backup *cbv1alpha1.BackupSpec) {
	if backup.Gprestore == nil {
		backup.Gprestore = &cbv1alpha1.GprestoreOptions{}
	}
	gr := backup.Gprestore
	if gr.Jobs == 0 {
		gr.Jobs = defaultBackupJobs
	}
	// WithStats is a *bool: default to true only when unset (nil) so an explicit
	// withStats:false set by the user is preserved rather than silently reverted.
	if gr.WithStats == nil {
		gr.WithStats = util.Ptr(true)
	}
}

// setBackupRetentionDefaults applies retention defaults.
func setBackupRetentionDefaults(backup *cbv1alpha1.BackupSpec) {
	if backup.Retention.FullCount == 0 {
		backup.Retention.FullCount = defaultBackupRetentionFullCount
	}
	if backup.Retention.MaxAge == "" {
		backup.Retention.MaxAge = defaultBackupRetentionMaxAge
	}
}

// setBackupJobTemplateDefaults applies Job template defaults, allocating the
// JobTemplate and its pointer fields when needed.
func setBackupJobTemplateDefaults(backup *cbv1alpha1.BackupSpec) {
	if backup.JobTemplate == nil {
		backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{}
	}
	jt := backup.JobTemplate
	if jt.BackoffLimit == nil {
		jt.BackoffLimit = util.Ptr(defaultBackupJobBackoffLimit)
	}
	if jt.ActiveDeadlineSeconds == nil {
		jt.ActiveDeadlineSeconds = util.Ptr(defaultBackupJobActiveDeadlineSeconds)
	}
	if jt.TTLSecondsAfterFinished == nil {
		jt.TTLSecondsAfterFinished = util.Ptr(defaultBackupJobTTLSecondsAfterFinished)
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
			cluster.Spec.DataLoading.StreamingServer.Port = defaultStreamingServerPort
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
			scan.BloatThreshold = defaultBloatThreshold
		}
		if scan.SkewThreshold == 0 {
			scan.SkewThreshold = defaultSkewThreshold
		}
		if scan.AgeThreshold == 0 {
			scan.AgeThreshold = defaultAgeThreshold
		}
		if scan.IndexBloatThreshold == 0 {
			scan.IndexBloatThreshold = defaultIndexBloatThreshold
		}
		if scan.ScanDuration == "" {
			scan.ScanDuration = "2h"
		}
	}
}
