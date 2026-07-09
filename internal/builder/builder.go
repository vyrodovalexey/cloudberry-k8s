// Package builder provides functions to construct Kubernetes resources from CloudberryCluster specs.
package builder

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	dataVolumeName   = "data"
	dataVolumePath   = "/data"
	pgDataSubDir     = "/data/pgdata"
	configVolumeName = "config"
	configVolumePath = "/etc/cloudberry"
	tlsVolumeName    = "tls"
	tlsVolumePath    = "/tls"
	tlsSecretVolName = "tls-secret" // source Secret volume (symlinked, root-owned)
	tlsSecretVolPath = "/tls-secret"

	containerName  = "cloudberry"
	initContainerN = "init-cloudberry"

	initImage = "busybox:1.36"

	portName = "postgresql"

	hbaTargetAll = "all"

	// psqlCommandFlag is the psql flag for executing a SQL command.
	psqlCommandFlag = "-c"

	// psqlHostFlag/psqlUserFlag/psqlDBFlag are the psql connection flags shared by
	// the maintenance and recommendation-scan job containers.
	psqlHostFlag = "-h"
	psqlUserFlag = "-U"
	psqlDBFlag   = "-d"

	// databasePostgres is the default maintenance database connected to by the
	// psql-driven maintenance and recommendation-scan job containers.
	databasePostgres = "postgres"

	// secretKeyPassword is the key used for password data in Kubernetes Secrets.
	secretKeyPassword = "password"

	// envPGPassword is the libpq PGPASSWORD env var name. It is sourced from a
	// Secret (never embedded as plaintext) on the backup/restore/migration pods.
	envPGPassword = "PGPASSWORD" //nolint:gosec // env var NAME, not a credential

	// envPGHost/envPGPort/envPGUser/envPGDatabase are the libpq connection env
	// var NAMES shared by the backup/restore/migration/data-loading pods.
	envPGHost     = "PGHOST"
	envPGPort     = "PGPORT"
	envPGUser     = "PGUSER"
	envPGDatabase = "PGDATABASE"

	// maintenanceContainerName is the container name for maintenance jobs.
	maintenanceContainerName = "maintenance"

	// maintenanceJobTTL is the TTL in seconds after a maintenance job finishes.
	maintenanceJobTTL int32 = 3600

	// maintenanceJobBackoffLimit is the number of retries for a maintenance job.
	maintenanceJobBackoffLimit int32 = 1

	// sqlAnalyze is the SQL command for analyze operations.
	sqlAnalyze = "ANALYZE"

	// gpadminUID is the UID of the gpadmin user in the Cloudberry container image.
	gpadminUID int64 = 1000

	// envCloudberryRole is the environment variable name for the Cloudberry role.
	envCloudberryRole = "CLOUDBERRY_ROLE"
	// envCloudberryContentID is the environment variable name for the segment content ID.
	envCloudberryContentID = "CLOUDBERRY_CONTENT_ID"
	// envCloudberryCoordinatorHost is the environment variable name for the coordinator host.
	envCloudberryCoordinatorHost = "CLOUDBERRY_COORDINATOR_HOST"
	// envCloudberrySegmentCount is the environment variable name for the segment count.
	envCloudberrySegmentCount = "CLOUDBERRY_SEGMENT_COUNT"
	// envCloudberryExpansionBaseCount is the environment variable name carrying
	// the pre-scale-out segment count (oldCount) onto the segment StatefulSets
	// WHILE a scale-out is in progress. The entrypoint marks any segment pod
	// whose ordinal >= this base as CLOUDBERRY_GPEXPAND_MANAGED (skip self-initdb)
	// so gpexpand — not the pod — initializes the new segment's datadir from the
	// coordinator template. Absent (no scale-out in flight) => normal bring-up.
	// (The operator sets only the base count, NOT CLOUDBERRY_GPEXPAND_MANAGED
	// directly: it cannot distinguish new from existing pods at the shared
	// StatefulSet level, so the per-pod decision is made in the entrypoint.)
	envCloudberryExpansionBaseCount = "CLOUDBERRY_EXPANSION_BASE_COUNT"

	// segmentStartup* configure the StartupProbe attached to the SEGMENT DB
	// container ONLY while a scale-out is in progress (see buildMainContainer).
	// The StartupProbe holds off liveness/readiness until postgres first listens,
	// so a gpexpand-managed NEW segment (empty datadir, postgres not yet started)
	// is NOT SIGKILLed during the gpexpand datadir-init window. Budget =
	// FailureThreshold * PeriodSeconds = 120 * 10s = 1200s (20 min), comfortably
	// covering a basebackup-based gpexpand segment init. Existing segments pass
	// immediately (port already up), so their normal cadence is unchanged.
	segmentStartupInitialDelaySeconds int32 = 10
	segmentStartupPeriodSeconds       int32 = 10
	segmentStartupTimeoutSeconds      int32 = 5
	segmentStartupFailureThreshold    int32 = 120
)

// isSegmentComponent reports whether the component is a primary or mirror
// segment (the roles whose datadir gpexpand initializes on scale-out).
func isSegmentComponent(component string) bool {
	return component == util.ComponentSegmentPrimary ||
		component == util.ComponentSegmentMirror
}

// maintenanceSQL maps maintenance operation types to their SQL commands.
var maintenanceSQL = map[string]string{
	util.MaintenanceVacuum:         "VACUUM",
	util.MaintenanceVacuumAnalyze:  "VACUUM ANALYZE",
	util.MaintenanceVacuumFull:     "VACUUM FULL",
	util.MaintenanceAnalyze:        sqlAnalyze,
	util.MaintenanceReindex:        "REINDEX DATABASE postgres",
	util.MaintenanceLogRotate:      "SELECT pg_rotate_logfile()",
	util.MaintenanceRedistribute:   "SELECT gp_expand.status()", // Redistribution handled via DB client
	util.MaintenanceRebalance:      "SELECT gp_expand.status()", // Rebalance handled via DB client
	util.MaintenanceBackupOnDelete: "SELECT 1",                  // In real Cloudberry: gpbackup
}

// resolvePort returns the coordinator port from the cluster spec,
// falling back to the default port if not specified.
func resolvePort(cluster *cbv1alpha1.CloudberryCluster) int32 {
	if cluster.Spec.Coordinator.Port != 0 {
		return cluster.Spec.Coordinator.Port
	}
	return int32(util.DefaultCoordinatorPort)
}

// ResourceBuilder defines the interface for building Kubernetes resources.
type ResourceBuilder interface {
	// BuildCoordinatorStatefulSet builds the coordinator StatefulSet.
	BuildCoordinatorStatefulSet(cluster *cbv1alpha1.CloudberryCluster) (*appsv1.StatefulSet, error)
	// BuildStandbyStatefulSet builds the standby coordinator StatefulSet.
	BuildStandbyStatefulSet(cluster *cbv1alpha1.CloudberryCluster) (*appsv1.StatefulSet, error)
	// BuildSegmentPrimaryStatefulSet builds the primary segment StatefulSet.
	BuildSegmentPrimaryStatefulSet(cluster *cbv1alpha1.CloudberryCluster) (*appsv1.StatefulSet, error)
	// BuildSegmentMirrorStatefulSet builds the mirror segment StatefulSet.
	BuildSegmentMirrorStatefulSet(cluster *cbv1alpha1.CloudberryCluster) (*appsv1.StatefulSet, error)
	// BuildCoordinatorService builds the coordinator headless service.
	BuildCoordinatorService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildStandbyService builds the standby headless service.
	BuildStandbyService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildSegmentService builds the segment headless service.
	BuildSegmentService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildClientService builds the client-facing service.
	BuildClientService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildPostgresqlConfConfigMap builds the postgresql.conf ConfigMap.
	BuildPostgresqlConfConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildPgHBAConfConfigMap builds the pg_hba.conf ConfigMap.
	BuildPgHBAConfConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildAdminPasswordSecret builds the admin password Secret.
	BuildAdminPasswordSecret(cluster *cbv1alpha1.CloudberryCluster, password string) *corev1.Secret
	// BuildClusterSSHSecret builds the cluster-wide shared gpadmin SSH keypair
	// Secret from a pre-generated private key and authorized_keys line.
	BuildClusterSSHSecret(
		cluster *cbv1alpha1.CloudberryCluster, privateKeyPEM, authorizedKey []byte) *corev1.Secret
	// BuildMaintenanceJob builds a Kubernetes Job for a maintenance operation.
	BuildMaintenanceJob(cluster *cbv1alpha1.CloudberryCluster, operation, timestamp string) *batchv1.Job
	// BuildExporterCredentialsSecret builds the exporter credentials Secret.
	BuildExporterCredentialsSecret(cluster *cbv1alpha1.CloudberryCluster, password, dsn string) *corev1.Secret
	// BuildExporterQueriesConfigMap builds the exporter queries ConfigMap.
	BuildExporterQueriesConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildExporterSidecarContainers returns the exporter sidecar containers.
	BuildExporterSidecarContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildPostgresExporterSidecarContainers returns only the postgres-exporter
	// sidecar container (used for the standby coordinator pod).
	BuildPostgresExporterSidecarContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildSegmentPostgresExporterSidecarContainers returns only the
	// postgres-exporter sidecar container configured for primary segments
	// (utility mode), used for the segment primary pod.
	BuildSegmentPostgresExporterSidecarContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildExporterSidecarVolumes returns the volumes for exporter sidecars.
	BuildExporterSidecarVolumes(cluster *cbv1alpha1.CloudberryCluster) []corev1.Volume
	// BuildPXFSidecarContainers returns the PXF sidecar container(s) for the
	// primary segment pod (empty when PXF is disabled).
	BuildPXFSidecarContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildPXFSidecarVolumes returns the volumes required by the PXF sidecar
	// (empty when PXF is disabled).
	BuildPXFSidecarVolumes(cluster *cbv1alpha1.CloudberryCluster) []corev1.Volume
	// BuildPXFCredentialInitContainers returns the credential-resolution init
	// container(s) that render the per-server site templates with live secret
	// values into the shared pxf-servers emptyDir (empty when PXF is disabled).
	BuildPXFCredentialInitContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildPXFConnectorInitContainers returns the connector-download init
	// container(s) that fetch each customConnectors[].jarUrl into the shared
	// pxf-lib emptyDir at /pxf/lib/custom (empty when PXF is disabled or no
	// custom connectors are declared).
	BuildPXFConnectorInitContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildPXFServersConfigMap renders the "<cluster>-pxf-servers" ConfigMap of
	// per-server *-site.xml + custom connectors (nil when PXF is disabled).
	BuildPXFServersConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildPXFClusterNetworkPolicy builds the SE.5 NetworkPolicy that confines
	// the PXF port (5888) on the segment-primary pods (nil when PXF is disabled).
	BuildPXFClusterNetworkPolicy(cluster *cbv1alpha1.CloudberryCluster) *networkingv1.NetworkPolicy
	// BuildDataLoadJob builds a one-off data-loading Job for a job spec (nil when
	// the job is mis-configured). Used when the job has no Schedule.
	BuildDataLoadJob(cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob) *batchv1.Job
	// BuildDataLoadCronJob builds a scheduled data-loading CronJob for a job spec
	// (nil when the job has no Schedule or is mis-configured).
	BuildDataLoadCronJob(cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob) *batchv1.CronJob
	// BuildGpfdistPVC builds the gpfdist data PersistentVolumeClaim (GP.4).
	BuildGpfdistPVC(cluster *cbv1alpha1.CloudberryCluster) *corev1.PersistentVolumeClaim
	// BuildGpfdistDeployment builds the gpfdist Deployment (GP.2/GP.3/GP.4).
	BuildGpfdistDeployment(cluster *cbv1alpha1.CloudberryCluster) *appsv1.Deployment
	// BuildGpfdistService builds the gpfdist Service (GP.5).
	BuildGpfdistService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildGploadControlFile renders the gpload YAML control file for a gpload
	// job (GL.1-GL.7); error when the job is mis-configured.
	BuildGploadControlFile(cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob) (string, error)
	// BuildGploadControlFileConfigMap builds the per-job control-file ConfigMap
	// (nil when the control file cannot be rendered).
	BuildGploadControlFileConfigMap(
		cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob,
	) *corev1.ConfigMap
	// BuildNodeExporterDaemonSet builds the node exporter DaemonSet.
	BuildNodeExporterDaemonSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.DaemonSet
	// BuildExporterService builds the exporter metrics Service.
	BuildExporterService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildQueryMetricsServiceMonitor builds the ServiceMonitor for Prometheus Operator.
	BuildQueryMetricsServiceMonitor(cluster *cbv1alpha1.CloudberryCluster) *unstructured.Unstructured
	// BuildQueryAlertsPrometheusRule builds the PrometheusRule for alerting.
	BuildQueryAlertsPrometheusRule(cluster *cbv1alpha1.CloudberryCluster) *unstructured.Unstructured
	// BuildBackupS3ConfigMap builds the gpbackup_s3_plugin config ConfigMap.
	// Returns nil when the destination is not S3.
	BuildBackupS3ConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildBackupCronJob builds the scheduled backup CronJob.
	// Returns nil when no schedule is configured.
	BuildBackupCronJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.CronJob
	// BuildRecommendationScanCronJob builds the scheduled storage
	// recommendation-scan CronJob (spec 13 §Reconciliation C.5). Returns nil when
	// storage management is absent, disk monitoring is off, the recommendation
	// scan is disabled, or no schedule is configured.
	BuildRecommendationScanCronJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.CronJob
	// BuildBackupJob builds an on-demand gpbackup Job.
	BuildBackupJob(cluster *cbv1alpha1.CloudberryCluster, opts *BackupJobOptions) *batchv1.Job
	// BuildRestoreJob builds a gprestore Job.
	BuildRestoreJob(cluster *cbv1alpha1.CloudberryCluster, opts *RestoreJobOptions) *batchv1.Job
	// BuildRetentionCleanupJob builds a gpbackman retention cleanup Job.
	BuildRetentionCleanupJob(cluster *cbv1alpha1.CloudberryCluster, timestamp string) *batchv1.Job
	// BuildPostRestoreValidationJob builds a post-restore validation Job.
	BuildPostRestoreValidationJob(
		cluster *cbv1alpha1.CloudberryCluster, opts *ValidationJobOptions) *batchv1.Job
	// BuildMigrationJob builds the single coordinated cross-cluster migration Job
	// that captures the real gpbackup timestamp and feeds it to gprestore (spec 11
	// §Cross-Cluster Migration).
	BuildMigrationJob(opts *MigrationJobOptions) *batchv1.Job
	// BuildGpexpandJob builds the coordinator-exec Job that runs the real
	// gpexpand flow (add+init new segments, redistribute, clean) to seed the
	// newly-desired segments during a scale-out. The Job name is deterministic
	// (<cluster>-gpexpand-<old>-<new>) so re-reconciles re-adopt the same Job.
	BuildGpexpandJob(
		cluster *cbv1alpha1.CloudberryCluster, oldCount, newCount int32, timestamp string) *batchv1.Job
}

// DefaultBuilder implements ResourceBuilder.
type DefaultBuilder struct{}

// NewBuilder creates a new DefaultBuilder.
func NewBuilder() *DefaultBuilder {
	return &DefaultBuilder{}
}

// BuildCoordinatorStatefulSet builds the coordinator StatefulSet.
func (b *DefaultBuilder) BuildCoordinatorStatefulSet(
	cluster *cbv1alpha1.CloudberryCluster,
) (*appsv1.StatefulSet, error) {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	replicas := int32(1)
	if cluster.Spec.Coordinator.Replicas != nil {
		replicas = *cluster.Spec.Coordinator.Replicas
	}

	port := resolvePort(cluster)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.CoordinatorName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: util.CoordinatorServiceName(cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					InitContainers: buildInitContainers(cluster),
					Containers:     []corev1.Container{},
					Volumes:        buildVolumes(cluster),
					NodeSelector:   cluster.Spec.Coordinator.NodeSelector,
					Tolerations:    convertTolerations(cluster.Spec.Coordinator.Tolerations),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(
		cluster, port, cluster.Spec.Coordinator.Resources, util.ComponentCoordinator,
	)
	if err != nil {
		return nil, fmt.Errorf("building coordinator main container: %w", err)
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	// Inject exporter sidecar containers if query monitoring exporters are enabled.
	if cluster.Spec.QueryMonitoring != nil && cluster.Spec.QueryMonitoring.Enabled &&
		cluster.Spec.QueryMonitoring.Exporters != nil {
		sidecars := b.BuildExporterSidecarContainers(cluster)
		sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, sidecars...)
		sidecarVolumes := b.BuildExporterSidecarVolumes(cluster)
		sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, sidecarVolumes...)
		// Add Prometheus scrape annotations so vmagent/Prometheus discovers the exporter metrics.
		if sts.Spec.Template.Annotations == nil {
			sts.Spec.Template.Annotations = make(map[string]string)
		}
		sts.Spec.Template.Annotations["prometheus.io/scrape"] = promScrapeTrue
		sts.Spec.Template.Annotations["prometheus.io/port"] = fmt.Sprintf("%d", pgExporterPort)
		sts.Spec.Template.Annotations["prometheus.io/path"] = keyMetricsPath
	}

	pvc, err := buildPVC(cluster.Spec.Coordinator.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building coordinator PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	// Mount the cluster-wide shared SSH keypair so the coordinator can SSH to
	// every segment for gpbackup/gprestore MPP dispatch.
	addClusterSSHSecret(cluster, &sts.Spec.Template.Spec)

	// Add the headless-service DNS search domains so gpexpand's internal
	// short-hostname rsync/ssh commands resolve cluster-wide.
	addClusterDNSSearchDomains(cluster, &sts.Spec.Template.Spec)

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts, nil
}

// BuildStandbyStatefulSet builds the standby coordinator StatefulSet.
func (b *DefaultBuilder) BuildStandbyStatefulSet(cluster *cbv1alpha1.CloudberryCluster) (*appsv1.StatefulSet, error) {
	if cluster.Spec.Standby == nil || !cluster.Spec.Standby.Enabled {
		return nil, nil
	}

	labels := util.CommonLabels(cluster.Name, util.ComponentStandby)
	replicas := int32(1)

	port := resolvePort(cluster)

	storage := cluster.Spec.Coordinator.Storage
	if cluster.Spec.Standby.Storage != nil {
		storage = *cluster.Spec.Standby.Storage
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.StandbyName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: util.StandbyServiceName(cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					InitContainers: buildInitContainers(cluster),
					Containers:     []corev1.Container{},
					Volumes:        buildVolumes(cluster),
					NodeSelector:   cluster.Spec.Standby.NodeSelector,
					Tolerations:    convertTolerations(cluster.Spec.Standby.Tolerations),
					Affinity:       buildStandbyAffinity(cluster),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(cluster, port, cluster.Spec.Standby.Resources, util.ComponentStandby)
	if err != nil {
		return nil, fmt.Errorf("building standby main container: %w", err)
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	// Inject ONLY the postgres-exporter sidecar on the standby for
	// instance/replication-scoped metrics. The cloudberry-query-exporter is
	// intentionally omitted (it is coordinator-only) because its cluster-global
	// queries would duplicate the coordinator's metric series from a
	// non-promoted standby.
	if cluster.Spec.QueryMonitoring != nil && cluster.Spec.QueryMonitoring.Enabled &&
		cluster.Spec.QueryMonitoring.Exporters != nil {
		sidecars := b.BuildPostgresExporterSidecarContainers(cluster)
		sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, sidecars...)
		sidecarVolumes := b.BuildExporterSidecarVolumes(cluster)
		sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, sidecarVolumes...)
		// Add Prometheus scrape annotations so vmagent/Prometheus discovers the standby exporter metrics.
		if sts.Spec.Template.Annotations == nil {
			sts.Spec.Template.Annotations = make(map[string]string)
		}
		sts.Spec.Template.Annotations["prometheus.io/scrape"] = promScrapeTrue
		sts.Spec.Template.Annotations["prometheus.io/port"] = fmt.Sprintf("%d", pgExporterPort)
		sts.Spec.Template.Annotations["prometheus.io/path"] = keyMetricsPath
	}

	pvc, err := buildPVC(storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building standby PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addClusterSSHSecret(cluster, &sts.Spec.Template.Spec)

	// Add the headless-service DNS search domains so gpexpand's internal
	// short-hostname rsync/ssh commands resolve cluster-wide.
	addClusterDNSSearchDomains(cluster, &sts.Spec.Template.Spec)

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts, nil
}

// BuildSegmentPrimaryStatefulSet builds the primary segment StatefulSet.
func (b *DefaultBuilder) BuildSegmentPrimaryStatefulSet(
	cluster *cbv1alpha1.CloudberryCluster,
) (*appsv1.StatefulSet, error) {
	labels := util.CommonLabels(cluster.Name, util.ComponentSegmentPrimary)
	replicas := cluster.Spec.Segments.Count

	port := resolvePort(cluster)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: util.SegmentServiceName(cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					InitContainers: buildInitContainers(cluster),
					Containers:     []corev1.Container{},
					Volumes:        buildVolumes(cluster),
					NodeSelector:   cluster.Spec.Segments.NodeSelector,
					Tolerations:    convertTolerations(cluster.Spec.Segments.Tolerations),
					Affinity:       buildSegmentAffinity(cluster, util.ComponentSegmentMirror),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(
		cluster, port, cluster.Spec.Segments.Resources, util.ComponentSegmentPrimary,
	)
	if err != nil {
		return nil, fmt.Errorf("building segment primary main container: %w", err)
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	// OPT-IN: Inject ONLY the postgres-exporter sidecar into each primary segment
	// pod when explicitly requested via PostgresExporter.Segments. Default is OFF,
	// so segment pods are unchanged unless the operator opts in. The
	// cloudberry-query-exporter is intentionally never added to segments: its
	// queries are cluster-global and would collide with the coordinator's series.
	// The DSN connects via localhost:5432, so each exporter scrapes its own
	// segment's local Postgres instance, yielding true per-segment metrics.
	if segmentPostgresExporterEnabled(cluster) {
		injectSegmentPostgresExporter(b, cluster, sts)
	}

	// Inject the PXF data-loading sidecar into each primary segment pod when the
	// full PXF config is enabled. Strictly gated by pxfSidecarEnabled so a
	// default cluster (DataLoading==nil) yields a byte-identical pod template.
	// Segment-primary scope only: coordinator/standby/mirror are untouched.
	if pxfSidecarEnabled(cluster) {
		injectPXFSidecar(b, cluster, sts)
	}

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building segment primary PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addClusterSSHSecret(cluster, &sts.Spec.Template.Spec)

	// Add the headless-service DNS search domains so gpexpand's internal
	// short-hostname rsync/ssh commands resolve cluster-wide.
	addClusterDNSSearchDomains(cluster, &sts.Spec.Template.Spec)

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts, nil
}

// segmentPostgresExporterEnabled reports whether the OPT-IN per-segment
// postgres-exporter sidecar should be injected into primary segment pods. It is
// strictly gated: query monitoring must be enabled, the exporters block and the
// PostgresExporter must be present and Enabled, AND the Segments opt-in toggle
// must be true. The default (any condition false) is OFF, leaving segment pods
// unchanged. A Segments=true with the exporter disabled is a silent no-op.
func segmentPostgresExporterEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	qm := cluster.Spec.QueryMonitoring
	return qm != nil && qm.Enabled &&
		qm.Exporters != nil &&
		qm.Exporters.PostgresExporter != nil &&
		qm.Exporters.PostgresExporter.Enabled &&
		qm.Exporters.PostgresExporter.Segments
}

// mirrorPostgresExporterEnabled reports whether the OPT-IN per-mirror
// postgres-exporter sidecar should be injected into mirror segment pods. It is
// gated identically to segmentPostgresExporterEnabled except it checks the
// Mirrors opt-in toggle instead of Segments. The default (any condition false)
// is OFF, leaving mirror pods unchanged. A Mirrors=true with the exporter
// disabled is a silent no-op.
func mirrorPostgresExporterEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	qm := cluster.Spec.QueryMonitoring
	return qm != nil && qm.Enabled &&
		qm.Exporters != nil &&
		qm.Exporters.PostgresExporter != nil &&
		qm.Exporters.PostgresExporter.Enabled &&
		qm.Exporters.PostgresExporter.Mirrors
}

// injectSegmentPostgresExporter appends the postgres-exporter sidecar, its
// volumes, the Prometheus scrape annotations, and the app.kubernetes.io/component
// label to the primary segment pod template. The component label is what vmagent
// relabels into the Prometheus "component" label so per-segment series are
// disambiguated (component="segment-primary" plus the unique per-pod "pod" label).
func injectSegmentPostgresExporter(
	b *DefaultBuilder, cluster *cbv1alpha1.CloudberryCluster, sts *appsv1.StatefulSet,
) {
	injectSegmentExporterWithComponent(b, cluster, sts, util.ComponentSegmentPrimary)
}

// injectPXFSidecar appends the PXF sidecar container, its cred/connector init
// containers, and its volumes to a segment pod template. It is only ever called
// when pxfSidecarEnabled is true, so it has no internal gate. Scope is BOTH the
// SEGMENT PRIMARY and SEGMENT MIRROR StatefulSets (so PXF follows the acting
// primary after a failover, D9); the coordinator/standby builders never call it.
// The helper is component-agnostic: it operates on any *appsv1.StatefulSet and
// makes no primary-specific assumptions, so the mirror injection is identical.
func injectPXFSidecar(
	b *DefaultBuilder, cluster *cbv1alpha1.CloudberryCluster, sts *appsv1.StatefulSet,
) {
	// The credential init container resolves the per-server site templates (with
	// live credential-secret values) into the shared pxf-servers emptyDir BEFORE
	// the sidecar starts, so the sidecar only ever sees fully-resolved files.
	sts.Spec.Template.Spec.InitContainers = append(
		sts.Spec.Template.Spec.InitContainers, b.BuildPXFCredentialInitContainers(cluster)...,
	)
	// The connector-download init container fetches each customConnectors[].jarUrl
	// into the shared pxf-lib emptyDir (/pxf/lib/custom) BEFORE the sidecar starts.
	// It touches a DIFFERENT emptyDir (pxf-lib) than the credential init container
	// (pxf-servers); it is listed AFTER pxf-cred-init for readability (C.18).
	sts.Spec.Template.Spec.InitContainers = append(
		sts.Spec.Template.Spec.InitContainers, b.BuildPXFConnectorInitContainers(cluster)...,
	)
	sts.Spec.Template.Spec.Containers = append(
		sts.Spec.Template.Spec.Containers, b.BuildPXFSidecarContainers(cluster)...,
	)
	sts.Spec.Template.Spec.Volumes = append(
		sts.Spec.Template.Spec.Volumes, b.BuildPXFSidecarVolumes(cluster)...,
	)
}

// injectMirrorPostgresExporter appends the postgres-exporter sidecar (in utility
// mode) and its volumes/annotations to the MIRROR segment pod template, tagging
// it with component="segment-mirror" so vmagent disambiguates the mirror series
// from the primary-segment series. Mirror segments are in WAL-replay recovery, so
// the utility-mode exporter primarily yields recovery/replication metrics.
func injectMirrorPostgresExporter(
	b *DefaultBuilder, cluster *cbv1alpha1.CloudberryCluster, sts *appsv1.StatefulSet,
) {
	injectSegmentExporterWithComponent(b, cluster, sts, util.ComponentSegmentMirror)
}

// injectSegmentExporterWithComponent is the shared implementation behind
// injectSegmentPostgresExporter and injectMirrorPostgresExporter. It appends the
// utility-mode postgres-exporter sidecar, its volumes, the Prometheus scrape
// annotations, and the app.kubernetes.io/component label to a segment pod
// template. The component argument is what vmagent's relabel
// (__meta_kubernetes_pod_label_app_kubernetes_io_component -> component)
// attaches to every per-segment series, disambiguating primaries
// (component="segment-primary") from mirrors (component="segment-mirror") and
// from coordinator/standby exporter series.
func injectSegmentExporterWithComponent(
	b *DefaultBuilder, cluster *cbv1alpha1.CloudberryCluster, sts *appsv1.StatefulSet, component string,
) {
	sidecars := b.BuildSegmentPostgresExporterSidecarContainers(cluster)
	sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, sidecars...)

	sidecarVolumes := b.BuildExporterSidecarVolumes(cluster)
	sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, sidecarVolumes...)

	// Add Prometheus scrape annotations so vmagent/Prometheus discovers the
	// per-segment exporter metrics.
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations["prometheus.io/scrape"] = promScrapeTrue
	sts.Spec.Template.Annotations["prometheus.io/port"] = fmt.Sprintf("%d", pgExporterPort)
	sts.Spec.Template.Annotations["prometheus.io/path"] = keyMetricsPath

	// Ensure the pod carries the standard component label so vmagent's relabel
	// attaches a distinct component label to every per-segment series.
	if sts.Spec.Template.Labels == nil {
		sts.Spec.Template.Labels = make(map[string]string)
	}
	sts.Spec.Template.Labels[labelAppComponent] = component
}

// BuildSegmentMirrorStatefulSet builds the mirror segment StatefulSet.
func (b *DefaultBuilder) BuildSegmentMirrorStatefulSet(
	cluster *cbv1alpha1.CloudberryCluster,
) (*appsv1.StatefulSet, error) {
	if cluster.Spec.Segments.Mirroring == nil || !cluster.Spec.Segments.Mirroring.Enabled {
		return nil, nil
	}

	labels := util.CommonLabels(cluster.Name, util.ComponentSegmentMirror)
	replicas := cluster.Spec.Segments.Count

	port := resolvePort(cluster)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentMirrorName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: util.SegmentServiceName(cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					InitContainers: buildInitContainers(cluster),
					Containers:     []corev1.Container{},
					Volumes:        buildVolumes(cluster),
					NodeSelector:   cluster.Spec.Segments.NodeSelector,
					Tolerations:    convertTolerations(cluster.Spec.Segments.Tolerations),
					Affinity:       buildSegmentAffinity(cluster, util.ComponentSegmentPrimary),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(
		cluster, port, cluster.Spec.Segments.Resources, util.ComponentSegmentMirror,
	)
	if err != nil {
		return nil, fmt.Errorf("building segment mirror main container: %w", err)
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	// OPT-IN: Inject ONLY the postgres-exporter sidecar (utility mode) into each
	// mirror segment pod when explicitly requested via PostgresExporter.Mirrors.
	// Default is OFF, so mirror pods are unchanged unless the operator opts in.
	// The cloudberry-query-exporter is intentionally never added to mirrors: its
	// queries are cluster-global and would collide with the coordinator's series.
	if mirrorPostgresExporterEnabled(cluster) {
		injectMirrorPostgresExporter(b, cluster, sts)
	}

	// Inject the PXF data-loading sidecar into each MIRROR segment pod too, so
	// PXF follows the acting primary after a failover (D9): when content-N's
	// primary role lands on segment-mirror-N, the local :5888 is still present.
	// Gated by pxfSidecarEnabled so a non-PXF cluster is byte-identical. On a
	// mirror (postgres in WAL recovery) the sidecar is a PASSIVE, independently
	// health-checked service (its probes hit PXF's own /actuator/health:5888,
	// not the local postgres) and only serves localhost:5888 once the mirror is
	// promoted. Config auto-syncs: the mirror mounts the SAME pxf-servers/
	// pxf-base/pxf-lib emptyDirs populated by the SAME cred/connector init
	// containers reading the SAME ConfigMap/Secrets as the primary.
	if pxfSidecarEnabled(cluster) {
		injectPXFSidecar(b, cluster, sts)
	}

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building segment mirror PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addClusterSSHSecret(cluster, &sts.Spec.Template.Spec)

	// Add the headless-service DNS search domains so gpexpand's internal
	// short-hostname rsync/ssh commands resolve cluster-wide.
	addClusterDNSSearchDomains(cluster, &sts.Spec.Template.Spec)

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts, nil
}

// BuildCoordinatorService builds the coordinator headless service.
func (b *DefaultBuilder) BuildCoordinatorService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	port := resolvePort(cluster)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.CoordinatorServiceName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			// PublishNotReadyAddresses ensures DNS records are available before
			// the pod passes its readiness probe. Cloudberry's FTS probe resolves
			// all hostnames in gp_segment_configuration at startup; without this
			// setting the coordinator's own hostname is unresolvable during init,
			// causing FTS to crash and blocking distributed transaction recovery.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       portName,
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildStandbyService builds the standby headless service.
func (b *DefaultBuilder) BuildStandbyService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentStandby)
	port := resolvePort(cluster)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.StandbyServiceName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			// PublishNotReadyAddresses ensures standby DNS is available before
			// readiness probe passes, required for WAL replication setup.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       portName,
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildSegmentService builds the segment headless service.
func (b *DefaultBuilder) BuildSegmentService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentSegmentPrimary)
	segPort := resolvePort(cluster)
	// Remove component label to match both primary and mirror.
	selectorLabels := map[string]string{
		util.LabelManagedBy: util.LabelManagedByValue,
		util.LabelCluster:   cluster.Name,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentServiceName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selectorLabels,
			// PublishNotReadyAddresses ensures segment DNS records are available
			// before pods pass readiness probes. Required for FTS hostname
			// resolution during coordinator startup.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       "segment",
					Port:       segPort,
					TargetPort: intstr.FromInt32(segPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildClientService builds the client-facing service.
func (b *DefaultBuilder) BuildClientService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	port := resolvePort(cluster)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.ClientServiceName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       portName,
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildPostgresqlConfConfigMap builds the postgresql.conf ConfigMap.
func (b *DefaultBuilder) BuildPostgresqlConfConfigMap(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.ConfigMap {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	confContent := renderPostgresqlConf(cluster)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.PostgresqlConfConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
			Annotations: map[string]string{
				util.AnnotationConfigHash: util.ComputeStringHash(confContent),
			},
		},
		Data: map[string]string{
			"postgresql.conf": confContent,
		},
	}
}

// BuildPgHBAConfConfigMap builds the pg_hba.conf ConfigMap.
func (b *DefaultBuilder) BuildPgHBAConfConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	hbaContent := renderPgHBAConf(cluster)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.PgHBAConfConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
			Annotations: map[string]string{
				util.AnnotationConfigHash: util.ComputeStringHash(hbaContent),
			},
		},
		Data: map[string]string{
			"pg_hba.conf": hbaContent,
		},
	}
}

// BuildAdminPasswordSecret builds the admin password Secret.
func (b *DefaultBuilder) BuildAdminPasswordSecret(
	cluster *cbv1alpha1.CloudberryCluster,
	password string,
) *corev1.Secret {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyPassword: []byte(password),
		},
	}
}

// BuildMaintenanceJob builds a Kubernetes Job for a maintenance operation.
// The operation parameter must be one of the maintenance constants defined in util.
func (b *DefaultBuilder) BuildMaintenanceJob(
	cluster *cbv1alpha1.CloudberryCluster,
	operation, timestamp string,
) *batchv1.Job {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)
	labels[util.LabelOperation] = operation

	sql, ok := maintenanceSQL[operation]
	if !ok {
		slog.Error("unknown maintenance operation", "operation", operation, "cluster", cluster.Name)
		return nil
	}

	coordinatorSvc := util.CoordinatorServiceName(cluster.Name)
	backoffLimit := maintenanceJobBackoffLimit
	ttl := maintenanceJobTTL

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.MaintenanceJobName(cluster.Name, operation, timestamp),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  maintenanceContainerName,
							Image: cluster.Spec.Image,
							Command: []string{
								"psql",
								psqlHostFlag, coordinatorSvc,
								psqlUserFlag, util.DefaultAdminUser,
								psqlDBFlag, databasePostgres,
								psqlCommandFlag, sql,
							},
							Env: []corev1.EnvVar{
								{
									Name: envPGPassword,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: util.AdminPasswordSecretName(cluster.Name),
											},
											Key: secretKeyPassword,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// ownerRef creates an OwnerReference for the given cluster.
func ownerRef(cluster *cbv1alpha1.CloudberryCluster) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         cbv1alpha1.GroupVersion.String(),
		Kind:               "CloudberryCluster",
		Name:               cluster.Name,
		UID:                cluster.UID,
		Controller:         util.Ptr(true),
		BlockOwnerDeletion: util.Ptr(true),
	}
}

// buildInitContainer creates the init container that prepares the data directory.
// Uses a lightweight busybox image to avoid entrypoint interference from database images.
// The container runs as root to ensure correct ownership (UID 1000 / gpadmin) of the
// data directory, then the main container runs as the unprivileged gpadmin user.
func buildInitContainer() corev1.Container {
	rootUser := int64(0)
	return corev1.Container{
		Name:  initContainerN,
		Image: initImage,
		Command: []string{
			"/bin/sh", "-c",
			"echo 'Preparing data directory...'; " +
				"mkdir -p " + pgDataSubDir + "; " +
				"chown -R " + fmt.Sprintf("%d:%d", gpadminUID, gpadminUID) + " " + dataVolumePath + "; " +
				"chmod 700 " + pgDataSubDir + "; " +
				// Also fix permissions on any existing segment data directories
				"find " + pgDataSubDir + " -maxdepth 1 -type d -name 'gpseg*'" +
				" -exec chmod 700 {} \\; 2>/dev/null || true; " +
				"echo 'Data directory ready'",
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: &rootUser,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: dataVolumeName, MountPath: dataVolumePath},
		},
	}
}

// buildTLSInitContainer creates an init container that copies TLS cert files from
// the Secret volume (which uses symlinks with 0777 permissions) to an emptyDir
// volume with correct ownership (gpadmin:gpadmin) and permissions (0600 for key,
// 0644 for certs). PostgreSQL requires the private key to have restricted perms.
func buildTLSInitContainer() corev1.Container {
	rootUser := int64(0)
	return corev1.Container{
		Name:  "init-tls",
		Image: initImage,
		Command: []string{
			"/bin/sh", "-c",
			"cp " + tlsSecretVolPath + "/tls.crt " + tlsVolumePath + "/tls.crt && " +
				"cp " + tlsSecretVolPath + "/tls.key " + tlsVolumePath + "/tls.key && " +
				"cp " + tlsSecretVolPath + "/ca.crt " + tlsVolumePath + "/ca.crt && " +
				"chown " + fmt.Sprintf("%d:%d", gpadminUID, gpadminUID) + " " + tlsVolumePath + "/* && " +
				"chmod 600 " + tlsVolumePath + "/tls.key && " +
				"chmod 644 " + tlsVolumePath + "/tls.crt " + tlsVolumePath + "/ca.crt && " +
				"echo 'TLS certificates ready'",
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: &rootUser,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: tlsSecretVolName, MountPath: tlsSecretVolPath, ReadOnly: true},
			{Name: tlsVolumeName, MountPath: tlsVolumePath},
		},
	}
}

// sslEnabled returns true if SSL is configured with a cert secret.
func sslEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.Auth != nil && cluster.Spec.Auth.SSL != nil &&
		cluster.Spec.Auth.SSL.Enabled && cluster.Spec.Auth.SSL.CertSecret != nil
}

// buildInitContainers returns the init containers for a pod, including the
// TLS init container when SSL is enabled.
func buildInitContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container {
	inits := []corev1.Container{buildInitContainer()}
	if sslEnabled(cluster) {
		inits = append(inits, buildTLSInitContainer())
	}
	return inits
}

// cloudberryRoleForComponent maps a component label to the CLOUDBERRY_ROLE env var value.
func cloudberryRoleForComponent(component string) string {
	switch component {
	case util.ComponentCoordinator:
		return "coordinator"
	case util.ComponentStandby:
		return "standby"
	case util.ComponentSegmentPrimary:
		return "primary"
	case util.ComponentSegmentMirror:
		return "mirror"
	default:
		return "coordinator"
	}
}

// buildCloudberryEnvVars returns the Cloudberry-specific environment variables
// for the given component type.
func buildCloudberryEnvVars(
	cluster *cbv1alpha1.CloudberryCluster, component string,
) []corev1.EnvVar {
	coordinatorSvc := util.CoordinatorServiceName(cluster.Name)
	segmentSvc := util.SegmentServiceName(cluster.Name)
	role := cloudberryRoleForComponent(component)
	isSegment := isSegmentComponent(component)
	isCoordinator := component == util.ComponentCoordinator

	hasMirroring := cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled

	// Pre-allocate: 3 base vars + content ID + optional POD_NAME + segment service + mirroring.
	capacity := 7
	envVars := make([]corev1.EnvVar, 0, capacity)

	envVars = append(envVars,
		corev1.EnvVar{Name: envCloudberryRole, Value: role},
		corev1.EnvVar{Name: envCloudberryCoordinatorHost, Value: coordinatorSvc},
		corev1.EnvVar{
			Name:  envCloudberrySegmentCount,
			Value: fmt.Sprintf("%d", cluster.Spec.Segments.Count),
		},
	)

	// Segments derive their content ID from the pod ordinal via the downward
	// API. We inject POD_NAME so the entrypoint can extract the ordinal.
	if isSegment {
		envVars = append(envVars,
			corev1.EnvVar{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			// Default content ID; overridden by entrypoint based on POD_NAME ordinal.
			corev1.EnvVar{
				Name:  envCloudberryContentID,
				Value: "-1",
			},
			// Segment service name for mirror-to-primary DNS resolution.
			corev1.EnvVar{
				Name:  "CLOUDBERRY_SEGMENT_SERVICE",
				Value: segmentSvc,
			},
		)
		// While a scale-out is in progress, tell the entrypoint the pre-scale
		// (old) segment count so each NEW pod (ordinal >= base) skips self-initdb
		// and lets gpexpand initialize its datadir. No-op outside scale-out.
		if base, ok := scaleOutBaseCount(cluster); ok {
			envVars = append(envVars, corev1.EnvVar{
				Name:  envCloudberryExpansionBaseCount,
				Value: strconv.Itoa(int(base)),
			})
		}
	} else {
		// Coordinator and standby use content ID -1.
		envVars = append(envVars, corev1.EnvVar{
			Name:  envCloudberryContentID,
			Value: "-1",
		})
	}

	// Coordinator needs POD_NAME and segment service for segment registration.
	if isCoordinator {
		envVars = append(envVars,
			corev1.EnvVar{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			corev1.EnvVar{
				Name:  "CLOUDBERRY_SEGMENT_SERVICE",
				Value: segmentSvc,
			},
		)
	}

	// Indicate whether mirroring is enabled so the entrypoint can register mirrors.
	if hasMirroring {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "CLOUDBERRY_MIRRORING_ENABLED",
			Value: "true",
		})
	}

	return envVars
}

// scaleOutBaseCount reports the pre-scale-out (old) segment count when a
// scale-out is currently in progress, so the segment StatefulSet can carry it as
// CLOUDBERRY_EXPANSION_BASE_COUNT. It reads the operator's scale-state
// annotation (util.AnnotationScaleState) off the cluster; the base count is the
// oldCount recorded there. It returns ok=false when no scale-out is in flight,
// the annotation is malformed, or the recorded newCount is not greater than the
// oldCount (nothing being added) — in which case the env is omitted and normal
// first-boot bring-up is unchanged.
//
// Using oldCount (not the desired Count) makes the flag PER-POD correct with a
// shared StatefulSet template: the entrypoint marks a pod gpexpand-managed iff
// its ordinal >= base, so only the newly-added segments skip self-initdb while
// the existing ones (ordinal < base) are untouched.
// ScaleOutBaseCount is the exported wrapper around scaleOutBaseCount so callers
// outside the builder package (the controller) can assert that the scale-state
// annotation was persisted with the expansion base count BEFORE scaling the
// segment StatefulSet — the fail-closed BUG-1 guard in handleScaleOut.
func ScaleOutBaseCount(cluster *cbv1alpha1.CloudberryCluster) (int32, bool) {
	return scaleOutBaseCount(cluster)
}

// EnvCloudberryExpansionBaseCount is the exported env var name so the controller
// can verify the built segment StatefulSet template carries it before scale-up.
const EnvCloudberryExpansionBaseCount = envCloudberryExpansionBaseCount

func scaleOutBaseCount(cluster *cbv1alpha1.CloudberryCluster) (int32, bool) {
	if cluster == nil || cluster.Annotations == nil {
		return 0, false
	}
	raw := cluster.Annotations[util.AnnotationScaleState]
	if raw == "" {
		return 0, false
	}
	var state struct {
		OldCount int32 `json:"oldCount"`
		NewCount int32 `json:"newCount"`
	}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return 0, false
	}
	// Only a genuine scale-OUT (adding segments) marks new pods gpexpand-managed.
	if state.NewCount <= state.OldCount || state.OldCount < 0 {
		return 0, false
	}
	return state.OldCount, true
}

// buildMainContainer creates the main database container.
// The component parameter identifies the Cloudberry role (coordinator, standby,
// segment-primary, segment-mirror) and is used to set role-specific environment
// variables expected by the Cloudberry entrypoint script.
// Returns an error if resource quantities are invalid.
func buildMainContainer(
	cluster *cbv1alpha1.CloudberryCluster,
	port int32,
	resources *cbv1alpha1.ResourceRequirements,
	component string,
) (corev1.Container, error) {
	runAsUser := gpadminUID
	runAsGroup := gpadminUID

	cloudberryEnvVars := buildCloudberryEnvVars(cluster, component)
	// 2 base env vars (PGDATA, POSTGRES_PASSWORD) + Cloudberry-specific vars.
	envVars := make([]corev1.EnvVar, 0, 2+len(cloudberryEnvVars))
	envVars = append(envVars,
		corev1.EnvVar{Name: "PGDATA", Value: pgDataSubDir},
		corev1.EnvVar{
			Name: "POSTGRES_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.AdminPasswordSecretName(cluster.Name),
					},
					Key: secretKeyPassword,
				},
			},
		},
	)
	envVars = append(envVars, cloudberryEnvVars...)

	container := corev1.Container{
		Name:  containerName,
		Image: cluster.Spec.Image,
		Ports: []corev1.ContainerPort{
			{
				Name:          portName,
				ContainerPort: port,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Env: envVars,
		VolumeMounts: []corev1.VolumeMount{
			{Name: dataVolumeName, MountPath: dataVolumePath},
			{Name: configVolumeName, MountPath: configVolumePath, ReadOnly: true},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(port),
				},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(port),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
		},
		ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:  &runAsUser,
			RunAsGroup: &runAsGroup,
		},
	}

	// Scale-out tolerance (BUG 2): a gpexpand-managed NEW segment pod has an EMPTY
	// datadir and does not start postgres until gpexpand initializes it — so its
	// TCP LivenessProbe would fail and the kubelet would SIGKILL the DB container
	// (CrashLoopBackOff -> pod NOT Running), which would in turn wedge the
	// operator's "wait for new pods Running" scale-out gate. While a scale-out is
	// in progress we therefore attach a StartupProbe to the SEGMENT DB container:
	// the StartupProbe HOLDS OFF liveness/readiness until postgres first listens,
	// with a budget generous enough to cover the gpexpand datadir init. Existing
	// segment pods (datadir already initialized) pass startup immediately, so
	// their normal liveness/readiness cadence is unaffected. No StartupProbe is
	// added outside a scale-out, so non-scale behavior is unchanged.
	if isSegmentComponent(component) {
		if _, scaling := scaleOutBaseCount(cluster); scaling {
			container.StartupProbe = &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromInt32(port),
					},
				},
				InitialDelaySeconds: segmentStartupInitialDelaySeconds,
				PeriodSeconds:       segmentStartupPeriodSeconds,
				TimeoutSeconds:      segmentStartupTimeoutSeconds,
				FailureThreshold:    segmentStartupFailureThreshold,
			}
		}
	}

	if resources != nil {
		k8sRes, err := convertResources(resources)
		if err != nil {
			return corev1.Container{}, fmt.Errorf("converting resources: %w", err)
		}
		container.Resources = k8sRes
	}

	// Add TLS volume mount if SSL is enabled.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.SSL != nil && cluster.Spec.Auth.SSL.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      tlsVolumeName,
			MountPath: tlsVolumePath,
			ReadOnly:  true,
		})
	}

	return container, nil
}

// buildVolumes creates the volumes for a pod.
func buildVolumes(cluster *cbv1alpha1.CloudberryCluster) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.PostgresqlConfConfigMapName(cluster.Name),
					},
				},
			},
		},
	}

	// Add TLS volumes if SSL is enabled.
	// PostgreSQL requires the private key file to have permissions u=rw (0600).
	// K8s Secret volumes use symlinks (always 0777) which PostgreSQL rejects.
	// Solution: mount the Secret to a staging path, then use an init container
	// to copy the files to an emptyDir with correct ownership and permissions.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.SSL != nil &&
		cluster.Spec.Auth.SSL.Enabled && cluster.Spec.Auth.SSL.CertSecret != nil {
		// Source: Secret volume (read-only, symlinked) + Target: emptyDir with correct perms
		volumes = append(volumes,
			corev1.Volume{
				Name: tlsSecretVolName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: cluster.Spec.Auth.SSL.CertSecret.Name,
					},
				},
			},
			corev1.Volume{
				Name: tlsVolumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		)
	}

	return volumes
}

// buildPVC creates a PersistentVolumeClaim template.
// Returns an error if the storage size string is invalid.
func buildPVC(storage cbv1alpha1.StorageSpec, labels map[string]string) (corev1.PersistentVolumeClaim, error) {
	storageQty, err := resource.ParseQuantity(storage.Size)
	if err != nil {
		return corev1.PersistentVolumeClaim{}, fmt.Errorf("parsing storage size %q: %w", storage.Size, err)
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   dataVolumeName,
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageQty,
				},
			},
		},
	}

	if storage.StorageClass != "" {
		pvc.Spec.StorageClassName = &storage.StorageClass
	}

	return pvc, nil
}

// buildSegmentAffinity creates the cross-component segment anti-affinity term
// (segment primary pods repel mirror pods and vice versa) via the shared
// resolver + merge helpers. antiAffinityComponent is the OPPOSITE role's
// component label (mirror for the primary STS, primary for the mirror STS).
//
// Placement/topology rules (see .opencode/output/antiaffinity-design_core_2026-07-07.md):
//   - The legacy segments.antiAffinity field still controls whether the segment
//     term is required or preferred, so existing clusters are unchanged.
//   - The NEW spec.affinity.type is intentionally NOT allowed to force the
//     segment cross-component term to required (an all-primaries-vs-all-mirrors
//     required term would wedge scheduling); the webhook warns and the term
//     stays best-effort. spec.affinity only contributes its resolved TopologyKey.
//   - When spec.affinity == nil the output is byte-identical to the historical
//     default: preferred, weight 100, topologyKey kubernetes.io/hostname.
func buildSegmentAffinity(
	cluster *cbv1alpha1.CloudberryCluster,
	antiAffinityComponent string,
) *corev1.Affinity {
	resolved := resolveAffinity(cluster.Spec.Affinity)
	if !resolved.segmentMirror {
		return nil
	}

	term := crossRoleAntiAffinityTerm(cluster, antiAffinityComponent, resolved.topologyKey)
	// Only the legacy segments.antiAffinity field may request a required segment
	// term. spec.affinity.type never upgrades the segment term to required.
	required := cluster.Spec.Segments.AntiAffinity == cbv1alpha1.AntiAffinityRequired
	return mergeAntiAffinityTerm(nil, required, term)
}

// parseResourceList converts a CRD ResourceList to a K8s ResourceList.
func parseResourceList(
	rv *cbv1alpha1.ResourceList, label string,
) (corev1.ResourceList, error) {
	if rv == nil {
		return nil, nil
	}
	rl := corev1.ResourceList{}
	if rv.CPU != "" {
		qty, err := resource.ParseQuantity(rv.CPU)
		if err != nil {
			return nil, fmt.Errorf("parsing CPU %s %q: %w", label, rv.CPU, err)
		}
		rl[corev1.ResourceCPU] = qty
	}
	if rv.Memory != "" {
		qty, err := resource.ParseQuantity(rv.Memory)
		if err != nil {
			return nil, fmt.Errorf("parsing memory %s %q: %w", label, rv.Memory, err)
		}
		rl[corev1.ResourceMemory] = qty
	}
	return rl, nil
}

// convertResources converts CRD resource requirements to K8s resource requirements.
// Returns an error if any resource quantity string is invalid.
func convertResources(res *cbv1alpha1.ResourceRequirements) (corev1.ResourceRequirements, error) {
	k8sRes := corev1.ResourceRequirements{}

	var err error
	if k8sRes.Requests, err = parseResourceList(res.Requests, "request"); err != nil {
		return k8sRes, err
	}
	if k8sRes.Limits, err = parseResourceList(res.Limits, "limit"); err != nil {
		return k8sRes, err
	}

	return k8sRes, nil
}

// convertTolerations converts CRD tolerations to K8s tolerations.
func convertTolerations(tolerations []cbv1alpha1.Toleration) []corev1.Toleration {
	if len(tolerations) == 0 {
		return nil
	}

	result := make([]corev1.Toleration, 0, len(tolerations))
	for _, t := range tolerations {
		result = append(result, corev1.Toleration{
			Key:               t.Key,
			Operator:          corev1.TolerationOperator(t.Operator),
			Value:             t.Value,
			Effect:            corev1.TaintEffect(t.Effect),
			TolerationSeconds: t.TolerationSeconds,
		})
	}
	return result
}

// addImagePullSecrets adds image pull secrets to a pod spec.
func addImagePullSecrets(spec *corev1.PodSpec, secrets []cbv1alpha1.ImagePullSecret) {
	for _, s := range secrets {
		spec.ImagePullSecrets = append(spec.ImagePullSecrets, corev1.LocalObjectReference{
			Name: s.Name,
		})
	}
}

// addClusterDNSSearchDomains augments the pod's DNS resolver search list with
// the cluster's headless-service search domains so a BARE segment pod name
// (e.g. "<cluster>-segment-primary-2", without any service suffix) resolves
// cluster-wide.
//
// This is the root-cause fix for gpexpand's short-hostname resolution failure:
// even though the operator feeds gpexpand a correct FQDN input file, gpexpand
// re-derives the SHORT hostname (pod name only) from gp_segment_configuration
// for its INTERNAL rsync/ssh commands. In Kubernetes only per-pod FQDNs
// (<pod>.<headless-svc>.<ns>.svc.cluster.local) resolve, so the bare name fails
// with "Could not resolve hostname". Adding the segment headless service's
// search domain makes <pod-name> resolve to
// <pod-name>.<seg-hl>.<ns>.svc.cluster.local via the DNS resolver's search list.
//
// The coordinator/standby headless-service search domains are added too so their
// short names resolve should gpexpand (or any MPP tool) ever emit them. The
// primary and mirror segment StatefulSets share the SegmentServiceName headless
// service (both set it as ServiceName), so one segment search domain covers both.
//
// dnsPolicy is deliberately left at its default (ClusterFirst): dnsConfig.searches
// AUGMENTS — it does not replace — the cluster's own search domains, so existing
// FQDN resolution is unaffected (safe and backward-compatible). Search entries
// are deduplicated and appended in a deterministic order for stable pod templates.
func addClusterDNSSearchDomains(cluster *cbv1alpha1.CloudberryCluster, spec *corev1.PodSpec) {
	ns := cluster.Namespace
	domains := []string{
		fmt.Sprintf("%s.%s.svc.cluster.local", util.SegmentServiceName(cluster.Name), ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", util.CoordinatorServiceName(cluster.Name), ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", util.StandbyServiceName(cluster.Name), ns),
	}

	if spec.DNSConfig == nil {
		spec.DNSConfig = &corev1.PodDNSConfig{}
	}

	existing := make(map[string]struct{}, len(spec.DNSConfig.Searches))
	for _, s := range spec.DNSConfig.Searches {
		existing[s] = struct{}{}
	}
	for _, d := range domains {
		if _, ok := existing[d]; ok {
			continue
		}
		spec.DNSConfig.Searches = append(spec.DNSConfig.Searches, d)
		existing[d] = struct{}{}
	}
}

// renderPostgresqlConf renders the postgresql.conf content from the cluster spec.
// Includes cluster-wide parameters and coordinator-only parameters (since the
// ConfigMap is mounted on the coordinator; coordinator-only params are also applied
// via ALTER SYSTEM SET for runtime changes).
func renderPostgresqlConf(cluster *cbv1alpha1.CloudberryCluster) string {
	var sb strings.Builder
	sb.WriteString("# Generated by cloudberry-operator\n")
	sb.WriteString("# Do not edit manually\n\n")

	fmt.Fprintf(&sb, "port = %d\n", resolvePort(cluster))
	sb.WriteString("listen_addresses = '*'\n")

	// SSL configuration.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.SSL != nil && cluster.Spec.Auth.SSL.Enabled {
		sb.WriteString("\n# SSL Configuration\n")
		sb.WriteString("ssl = on\n")
		fmt.Fprintf(&sb, "ssl_cert_file = '%s/tls.crt'\n", tlsVolumePath)
		fmt.Fprintf(&sb, "ssl_key_file = '%s/tls.key'\n", tlsVolumePath)
		fmt.Fprintf(&sb, "ssl_ca_file = '%s/ca.crt'\n", tlsVolumePath)
		minTLS := "TLSv1.2"
		if cluster.Spec.Auth.SSL.MinTLSVersion != "" {
			minTLS = "TLSv" + cluster.Spec.Auth.SSL.MinTLSVersion
		}
		fmt.Fprintf(&sb, "ssl_min_protocol_version = '%s'\n", minTLS)
	}

	// User-defined cluster-wide parameters.
	if cluster.Spec.Config != nil && len(cluster.Spec.Config.Parameters) > 0 {
		sb.WriteString("\n# User-defined parameters\n")
		keys := make([]string, 0, len(cluster.Spec.Config.Parameters))
		for k := range cluster.Spec.Config.Parameters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s = '%s'\n", k, cluster.Spec.Config.Parameters[k])
		}
	}

	// Coordinator-only parameters (applied to the shared ConfigMap which is
	// mounted on the coordinator; segments ignore these via ALTER SYSTEM SET scope).
	if cluster.Spec.Config != nil && len(cluster.Spec.Config.CoordinatorParameters) > 0 {
		sb.WriteString("\n# Coordinator-only parameters\n")
		keys := make([]string, 0, len(cluster.Spec.Config.CoordinatorParameters))
		for k := range cluster.Spec.Config.CoordinatorParameters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s = '%s'\n", k, cluster.Spec.Config.CoordinatorParameters[k])
		}
	}

	return sb.String()
}

// renderPgHBAConf renders the pg_hba.conf content from the cluster spec.
func renderPgHBAConf(cluster *cbv1alpha1.CloudberryCluster) string {
	var sb strings.Builder
	sb.WriteString("# Generated by cloudberry-operator\n")
	sb.WriteString("# Do not edit manually\n\n")

	rules := defaultHBARules()
	if cluster.Spec.Auth != nil && len(cluster.Spec.Auth.HBARules) > 0 {
		rules = cluster.Spec.Auth.HBARules
	}

	for _, rule := range rules {
		sb.WriteString(formatHBARule(rule))
		sb.WriteString("\n")
	}

	return sb.String()
}

// defaultHBARules returns the default pg_hba.conf rules.
func defaultHBARules() []cbv1alpha1.HBARule {
	return []cbv1alpha1.HBARule{
		{
			Type: cbv1alpha1.HBATypeLocal, Database: hbaTargetAll,
			User: "gpadmin", Method: cbv1alpha1.AuthMethodTrust,
		},
		{
			Type: cbv1alpha1.HBATypeLocal, Database: hbaTargetAll,
			User: hbaTargetAll, Method: cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type: cbv1alpha1.HBATypeHost, Database: hbaTargetAll, User: "gpadmin",
			Address: "127.0.0.1/32", Method: cbv1alpha1.AuthMethodTrust,
		},
		{
			Type: cbv1alpha1.HBATypeHost, Database: hbaTargetAll, User: hbaTargetAll,
			Address: "0.0.0.0/0", Method: cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type: cbv1alpha1.HBATypeHost, Database: "replication", User: hbaTargetAll,
			Address: "0.0.0.0/0", Method: cbv1alpha1.AuthMethodScramSHA256,
		},
	}
}

// formatHBARule formats a single HBA rule as a pg_hba.conf line.
func formatHBARule(rule cbv1alpha1.HBARule) string {
	parts := []string{string(rule.Type), rule.Database, rule.User}
	if rule.Address != "" {
		parts = append(parts, rule.Address)
	}
	parts = append(parts, string(rule.Method))
	if rule.Options != "" {
		parts = append(parts, rule.Options)
	}
	return strings.Join(parts, "\t")
}
