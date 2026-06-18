// Package builder: gpfdist_builder.go constructs the Kubernetes resources for
// the gpfdist FILE-SERVER runtime (GP.2-GP.5) — a Deployment running the gpfdist
// binary, a backing PVC mounted at /data and a Service that gpload control files
// target over gpfdist://. The resources are created/GC'd by reconcileGpfdist
// when dataLoading.gpfdist.enabled flips.
//
// All builders here are DETERMINISTIC and byte-stable for a given cluster spec:
// the same input always yields the same object, so they are golden-testable.
//
// LABEL NOTE: the Service selector MUST equal the Deployment pod-template labels
// (GP.5). Both use util.CommonLabels(cluster, util.ComponentGpfdist) so the
// selector cannot drift from the pod labels; the cluster label additionally
// scopes selection so two clusters in one namespace never cross-select.
package builder

import (
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultGpfdistImage is the gpfdist container image used when
	// gpfdist.image is unset. It matches the spec literal (a thin image that
	// just runs the gpfdist binary over the served /data directory).
	defaultGpfdistImage = "cloudberry-gpfdist:2.1.0"

	// defaultGpfdistReplicas is the gpfdist Deployment replica count used when
	// gpfdist.replicas is unset. It defaults to 1 because the backing PVC is
	// ReadWriteOnce (RWO): with RWO only one pod can mount the volume at a time,
	// so >1 replica requires an RWX-capable StorageClass (a DevOps decision).
	defaultGpfdistReplicas int32 = 1

	// gpfdistContainerName is the gpfdist container/port name.
	gpfdistContainerName = "gpfdist"

	// gpfdistDataVolumeName is the name of the data volume + volumeMount.
	gpfdistDataVolumeName = "data"

	// gpfdistDataMountPath is where the gpfdist data PVC is mounted (GP.4).
	gpfdistDataMountPath = "/data"

	// gpfdistLogPath is the gpfdist log file path passed via -l.
	gpfdistLogPath = "/var/log/gpfdist.log"

	// gpfdistPVCStorageRequest is the modest storage request for the gpfdist
	// data PVC. The PVC holds the staged source files (e.g. /data/incoming/*.csv).
	gpfdistPVCStorageRequest = "1Gi"
)

// gpfdistSpec returns the cluster's GpfdistSpec, or nil when data loading or the
// gpfdist sub-spec is unset.
func gpfdistSpec(cluster *cbv1alpha1.CloudberryCluster) *cbv1alpha1.GpfdistSpec {
	if cluster.Spec.DataLoading == nil {
		return nil
	}
	return cluster.Spec.DataLoading.Gpfdist
}

// gpfdistImage resolves the gpfdist container image from the spec, falling back
// to the documented default.
func gpfdistImage(cluster *cbv1alpha1.CloudberryCluster) string {
	if gp := gpfdistSpec(cluster); gp != nil && gp.Image != "" {
		return gp.Image
	}
	return defaultGpfdistImage
}

// gpfdistReplicas resolves the gpfdist Deployment replica count, honoring
// gpfdist.replicas (J/C.20) when set, else the RWO-safe default of 1.
func gpfdistReplicas(cluster *cbv1alpha1.CloudberryCluster) int32 {
	if gp := gpfdistSpec(cluster); gp != nil && gp.Replicas != nil {
		return *gp.Replicas
	}
	return defaultGpfdistReplicas
}

// BuildGpfdistPVC builds the gpfdist data PersistentVolumeClaim
// ("<cluster>-gpfdist-data-pvc", GP.4): ReadWriteOnce with a modest storage
// request, the gpfdist component labels and an ownerRef so it is GC'd with the
// cluster. The Deployment mounts this exact claim name at /data.
func (b *DefaultBuilder) BuildGpfdistPVC(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.PersistentVolumeClaim {
	labels := util.CommonLabels(cluster.Name, util.ComponentGpfdist)
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.GpfdistDataPVCName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(gpfdistPVCStorageRequest),
				},
			},
		},
	}
}

// BuildGpfdistDeployment builds the gpfdist Deployment ("<cluster>-gpfdist",
// GP.2/GP.3/GP.4): the container runs `gpfdist -d /data -p <port> -l
// /var/log/gpfdist.log`, exposes the named "gpfdist" container port, mounts the
// "<cluster>-gpfdist-data-pvc" at /data, and carries the gpfdist component
// labels on both the selector and the pod template (so the Service selector
// matches, GP.5). The replica count honors gpfdist.replicas (J/C.20).
func (b *DefaultBuilder) BuildGpfdistDeployment(
	cluster *cbv1alpha1.CloudberryCluster,
) *appsv1.Deployment {
	labels := util.CommonLabels(cluster.Name, util.ComponentGpfdist)
	replicas := gpfdistReplicas(cluster)
	port := gpfdistPort(cluster)

	container := corev1.Container{
		Name:    gpfdistContainerName,
		Image:   gpfdistImage(cluster),
		Command: []string{gpfdistContainerName},
		Args: []string{
			"-d", gpfdistDataMountPath,
			"-p", strconv.Itoa(int(port)),
			"-l", gpfdistLogPath,
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          gpfdistContainerName,
				ContainerPort: port,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      gpfdistDataVolumeName,
				MountPath: gpfdistDataMountPath,
			},
		},
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            GpfdistServiceName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{
						{
							Name: gpfdistDataVolumeName,
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: util.GpfdistDataPVCName(cluster.Name),
								},
							},
						},
					},
				},
			},
		},
	}
}

// BuildGpfdistService builds the gpfdist Service ("<cluster>-gpfdist-svc",
// GP.5): a ClusterIP Service selecting the gpfdist component pods (the selector
// EQUALS the Deployment pod-template labels) and exposing port -> targetPort on
// the resolved gpfdist port (8080 by default).
func (b *DefaultBuilder) BuildGpfdistService(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentGpfdist)
	port := gpfdistPort(cluster)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.GpfdistServiceName2(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       gpfdistContainerName,
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}
