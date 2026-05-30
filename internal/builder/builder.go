// Package builder provides functions to construct Kubernetes resources from CloudberryCluster specs.
package builder

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

	// secretKeyPassword is the key used for password data in Kubernetes Secrets.
	secretKeyPassword = "password"

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
)

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
	// BuildMaintenanceJob builds a Kubernetes Job for a maintenance operation.
	BuildMaintenanceJob(cluster *cbv1alpha1.CloudberryCluster, operation, timestamp string) *batchv1.Job
	// BuildExporterCredentialsSecret builds the exporter credentials Secret.
	BuildExporterCredentialsSecret(cluster *cbv1alpha1.CloudberryCluster, password, dsn string) *corev1.Secret
	// BuildExporterQueriesConfigMap builds the exporter queries ConfigMap.
	BuildExporterQueriesConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap
	// BuildExporterSidecarContainers returns the exporter sidecar containers.
	BuildExporterSidecarContainers(cluster *cbv1alpha1.CloudberryCluster) []corev1.Container
	// BuildExporterSidecarVolumes returns the volumes for exporter sidecars.
	BuildExporterSidecarVolumes(cluster *cbv1alpha1.CloudberryCluster) []corev1.Volume
	// BuildNodeExporterDaemonSet builds the node exporter DaemonSet.
	BuildNodeExporterDaemonSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.DaemonSet
	// BuildExporterService builds the exporter metrics Service.
	BuildExporterService(cluster *cbv1alpha1.CloudberryCluster) *corev1.Service
	// BuildQueryMetricsServiceMonitor builds the ServiceMonitor for Prometheus Operator.
	BuildQueryMetricsServiceMonitor(cluster *cbv1alpha1.CloudberryCluster) *unstructured.Unstructured
	// BuildQueryAlertsPrometheusRule builds the PrometheusRule for alerting.
	BuildQueryAlertsPrometheusRule(cluster *cbv1alpha1.CloudberryCluster) *unstructured.Unstructured
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
		sts.Spec.Template.Annotations["prometheus.io/path"] = "/metrics"
	}

	pvc, err := buildPVC(cluster.Spec.Coordinator.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building coordinator PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

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

	pvc, err := buildPVC(storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building standby PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

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

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building segment primary PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts, nil
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

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		return nil, fmt.Errorf("building segment mirror PVC: %w", err)
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

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
								"-h", coordinatorSvc,
								"-U", util.DefaultAdminUser,
								"-d", "postgres",
								psqlCommandFlag, sql,
							},
							Env: []corev1.EnvVar{
								{
									Name: "PGPASSWORD",
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
	isSegment := component == util.ComponentSegmentPrimary ||
		component == util.ComponentSegmentMirror
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

// buildSegmentAffinity creates anti-affinity rules for segments.
func buildSegmentAffinity(
	cluster *cbv1alpha1.CloudberryCluster,
	antiAffinityComponent string,
) *corev1.Affinity {
	antiAffinityLabels := map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: antiAffinityComponent,
	}

	if cluster.Spec.Segments.AntiAffinity == cbv1alpha1.AntiAffinityRequired {
		return &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: antiAffinityLabels,
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		}
	}

	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: antiAffinityLabels,
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
	}
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
