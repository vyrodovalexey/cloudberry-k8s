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

	containerName  = "cloudberry"
	initContainerN = "init-cloudberry"

	initImage = "busybox:1.36"

	portName = "postgresql"

	hbaTargetAll = "all"

	// secretKeyPassword is the key used for password data in Kubernetes Secrets.
	secretKeyPassword = "password"

	// maintenanceContainerName is the container name for maintenance jobs.
	maintenanceContainerName = "maintenance"

	// maintenanceJobTTL is the TTL in seconds after a maintenance job finishes.
	maintenanceJobTTL int32 = 3600

	// maintenanceJobBackoffLimit is the number of retries for a maintenance job.
	maintenanceJobBackoffLimit int32 = 1
)

// maintenanceSQL maps maintenance operation types to their SQL commands.
var maintenanceSQL = map[string]string{
	util.MaintenanceVacuum:        "VACUUM",
	util.MaintenanceVacuumAnalyze: "VACUUM ANALYZE",
	util.MaintenanceVacuumFull:    "VACUUM FULL",
	util.MaintenanceAnalyze:       "ANALYZE",
	util.MaintenanceReindex:       "REINDEX DATABASE postgres",
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
	BuildCoordinatorStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet
	// BuildStandbyStatefulSet builds the standby coordinator StatefulSet.
	BuildStandbyStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet
	// BuildSegmentPrimaryStatefulSet builds the primary segment StatefulSet.
	BuildSegmentPrimaryStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet
	// BuildSegmentMirrorStatefulSet builds the mirror segment StatefulSet.
	BuildSegmentMirrorStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet
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
}

// DefaultBuilder implements ResourceBuilder.
type DefaultBuilder struct{}

// NewBuilder creates a new DefaultBuilder.
func NewBuilder() *DefaultBuilder {
	return &DefaultBuilder{}
}

// BuildCoordinatorStatefulSet builds the coordinator StatefulSet.
func (b *DefaultBuilder) BuildCoordinatorStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet {
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
					InitContainers: []corev1.Container{
						buildInitContainer(),
					},
					Containers:   []corev1.Container{},
					Volumes:      buildVolumes(cluster),
					NodeSelector: cluster.Spec.Coordinator.NodeSelector,
					Tolerations:  convertTolerations(cluster.Spec.Coordinator.Tolerations),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(cluster, port, cluster.Spec.Coordinator.Resources)
	if err != nil {
		slog.Error("failed to build coordinator main container", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	pvc, err := buildPVC(cluster.Spec.Coordinator.Storage, labels)
	if err != nil {
		slog.Error("failed to build coordinator PVC", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts
}

// BuildStandbyStatefulSet builds the standby coordinator StatefulSet.
func (b *DefaultBuilder) BuildStandbyStatefulSet(cluster *cbv1alpha1.CloudberryCluster) *appsv1.StatefulSet {
	if cluster.Spec.Standby == nil || !cluster.Spec.Standby.Enabled {
		return nil
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
					Containers:   []corev1.Container{},
					Volumes:      buildVolumes(cluster),
					NodeSelector: cluster.Spec.Standby.NodeSelector,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(cluster, port, cluster.Spec.Standby.Resources)
	if err != nil {
		slog.Error("failed to build standby main container", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	pvc, err := buildPVC(storage, labels)
	if err != nil {
		slog.Error("failed to build standby PVC", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts
}

// BuildSegmentPrimaryStatefulSet builds the primary segment StatefulSet.
func (b *DefaultBuilder) BuildSegmentPrimaryStatefulSet(
	cluster *cbv1alpha1.CloudberryCluster,
) *appsv1.StatefulSet {
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
					Containers:   []corev1.Container{},
					Volumes:      buildVolumes(cluster),
					NodeSelector: cluster.Spec.Segments.NodeSelector,
					Tolerations:  convertTolerations(cluster.Spec.Segments.Tolerations),
					Affinity:     buildSegmentAffinity(cluster, util.ComponentSegmentMirror),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(cluster, port, cluster.Spec.Segments.Resources)
	if err != nil {
		slog.Error("failed to build segment primary main container", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		slog.Error("failed to build segment primary PVC", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts
}

// BuildSegmentMirrorStatefulSet builds the mirror segment StatefulSet.
func (b *DefaultBuilder) BuildSegmentMirrorStatefulSet(
	cluster *cbv1alpha1.CloudberryCluster,
) *appsv1.StatefulSet {
	if cluster.Spec.Segments.Mirroring == nil || !cluster.Spec.Segments.Mirroring.Enabled {
		return nil
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
					Containers:   []corev1.Container{},
					Volumes:      buildVolumes(cluster),
					NodeSelector: cluster.Spec.Segments.NodeSelector,
					Tolerations:  convertTolerations(cluster.Spec.Segments.Tolerations),
					Affinity:     buildSegmentAffinity(cluster, util.ComponentSegmentPrimary),
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
		},
	}

	mainContainer, err := buildMainContainer(cluster, port, cluster.Spec.Segments.Resources)
	if err != nil {
		slog.Error("failed to build segment mirror main container", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.Template.Spec.Containers = []corev1.Container{mainContainer}

	pvc, err := buildPVC(cluster.Spec.Segments.Storage, labels)
	if err != nil {
		slog.Error("failed to build segment mirror PVC", "error", err, "cluster", cluster.Name)
		return nil
	}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	addImagePullSecrets(&sts.Spec.Template.Spec, cluster.Spec.ImagePullSecrets)
	return sts
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
								"-c", sql,
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
func buildInitContainer() corev1.Container {
	return corev1.Container{
		Name:  initContainerN,
		Image: initImage,
		Command: []string{
			"/bin/sh", "-c",
			"if [ ! -d " + pgDataSubDir + " ]; then " +
				"echo 'Creating pgdata subdirectory...'; " +
				"mkdir -p " + pgDataSubDir + "; " +
				"chmod 700 " + pgDataSubDir + "; " +
				"fi",
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: dataVolumeName, MountPath: dataVolumePath},
		},
	}
}

// buildMainContainer creates the main database container.
// Returns an error if resource quantities are invalid.
func buildMainContainer(
	cluster *cbv1alpha1.CloudberryCluster,
	port int32,
	resources *cbv1alpha1.ResourceRequirements,
) (corev1.Container, error) {
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
		Env: []corev1.EnvVar{
			{Name: "PGDATA", Value: pgDataSubDir},
			{
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
		},
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

	// Add TLS volume if SSL is enabled.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.SSL != nil &&
		cluster.Spec.Auth.SSL.Enabled && cluster.Spec.Auth.SSL.CertSecret != nil {
		volumes = append(volumes, corev1.Volume{
			Name: tlsVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cluster.Spec.Auth.SSL.CertSecret.Name,
				},
			},
		})
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

	// User-defined parameters.
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
