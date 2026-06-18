package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// gpfdistCluster returns a test cluster whose data-loading spec carries the given
// gpfdist sub-spec (may be nil to exercise the defaults).
func gpfdistCluster(gp *cbv1alpha1.GpfdistSpec) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: gp,
	}
	return c
}

// assertOwnedByCluster asserts the object carries a single controller ownerRef
// pointing at the test cluster (GP.2-5: ownerRef on PVC/Deployment/Service).
func assertOwnedByCluster(t *testing.T, refs []metav1.OwnerReference, cluster *cbv1alpha1.CloudberryCluster) {
	t.Helper()
	require.Len(t, refs, 1)
	assert.Equal(t, "CloudberryCluster", refs[0].Kind)
	assert.Equal(t, cluster.Name, refs[0].Name)
	assert.Equal(t, cluster.UID, refs[0].UID)
	require.NotNil(t, refs[0].Controller)
	assert.True(t, *refs[0].Controller)
}

// TestBuildGpfdist_NilDataLoadingDefaults asserts the gpfdist builders fall back
// to defaults when DataLoading is entirely nil (gpfdistSpec returns nil): the
// default image, replicas 1 and port 8080.
func TestBuildGpfdist_NilDataLoadingDefaults(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster() // DataLoading == nil

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)
	require.NotNil(t, dep.Spec.Replicas)
	assert.Equal(t, int32(1), *dep.Spec.Replicas)
	assert.Equal(t, "cloudberry-gpfdist:2.1.0", dep.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, int32(8080), dep.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)

	svc := b.BuildGpfdistService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
}

// --- GP.4: PVC -------------------------------------------------------------

// TestBuildGpfdistPVC (SC101-GP4-PVCNAME, SC101-GP-OWNERREF) asserts the gpfdist
// data PVC: per-cluster name "<cluster>-gpfdist-data-pvc", ReadWriteOnce, a
// modest 1Gi request, gpfdist component labels and a cluster ownerRef.
func TestBuildGpfdistPVC(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	pvc := b.BuildGpfdistPVC(cluster)
	require.NotNil(t, pvc)

	assert.Equal(t, "test-cluster-gpfdist-data-pvc", pvc.Name)
	assert.Equal(t, util.GpfdistDataPVCName(cluster.Name), pvc.Name)
	assert.Equal(t, cluster.Namespace, pvc.Namespace)

	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.Spec.AccessModes)
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "1Gi", got.String())

	// gpfdist component labels.
	assert.Equal(t, util.ComponentGpfdist, pvc.Labels[util.LabelComponent])
	assertOwnedByCluster(t, pvc.OwnerReferences, cluster)
}

// --- GP.2 / GP.3 / GP.4: Deployment ---------------------------------------

// TestBuildGpfdistDeployment_NameAndOwnerRef (SC101-GP2-NAME) asserts the
// Deployment name "<cluster>-gpfdist" and the cluster ownerRef.
func TestBuildGpfdistDeployment_NameAndOwnerRef(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)

	assert.Equal(t, "test-cluster-gpfdist", dep.Name)
	assert.Equal(t, GpfdistServiceName(cluster.Name), dep.Name)
	assert.Equal(t, cluster.Namespace, dep.Namespace)
	assertOwnedByCluster(t, dep.OwnerReferences, cluster)
}

// TestBuildGpfdistDeployment_Replicas (SC101-GP2-REPLICAS, J/C.20) asserts the
// replica count honors gpfdist.replicas when set and defaults to 1 when nil.
func TestBuildGpfdistDeployment_Replicas(t *testing.T) {
	b := NewBuilder()

	tests := []struct {
		name     string
		replicas *int32
		want     int32
	}{
		{name: "honors explicit replicas", replicas: util.Ptr(int32(2)), want: 2},
		{name: "defaults to 1 when nil (RWO-safe)", replicas: nil, want: 1},
		{name: "honors replicas 3", replicas: util.Ptr(int32(3)), want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := gpfdistCluster(&cbv1alpha1.GpfdistSpec{Replicas: tt.replicas})
			dep := b.BuildGpfdistDeployment(cluster)
			require.NotNil(t, dep)
			require.NotNil(t, dep.Spec.Replicas)
			assert.Equal(t, tt.want, *dep.Spec.Replicas)
		})
	}
}

// TestBuildGpfdistDeployment_Image (SC101-GP2-IMAGE) asserts the container image
// honors gpfdist.image when set and falls back to the documented default.
func TestBuildGpfdistDeployment_Image(t *testing.T) {
	b := NewBuilder()

	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "custom image honored", image: "my-registry/gpfdist:9.9", want: "my-registry/gpfdist:9.9"},
		{name: "default image when unset", image: "", want: "cloudberry-gpfdist:2.1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := gpfdistCluster(&cbv1alpha1.GpfdistSpec{Image: tt.image})
			dep := b.BuildGpfdistDeployment(cluster)
			require.NotNil(t, dep)
			require.Len(t, dep.Spec.Template.Spec.Containers, 1)
			assert.Equal(t, tt.want, dep.Spec.Template.Spec.Containers[0].Image)
		})
	}
}

// TestBuildGpfdistDeployment_CommandAndArgs (SC101-GP2-CMD) asserts the gpfdist
// container command and the EXACT default args.
func TestBuildGpfdistDeployment_CommandAndArgs(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)
	c := dep.Spec.Template.Spec.Containers[0]

	assert.Equal(t, "gpfdist", c.Name)
	assert.Equal(t, []string{"gpfdist"}, c.Command)
	assert.Equal(t, []string{
		"-d", "/data",
		"-p", "8080",
		"-l", "/var/log/gpfdist.log",
	}, c.Args)
}

// TestBuildGpfdistDeployment_Port (SC101-GP3-PORT) asserts the named container
// port "gpfdist" on 8080 (the default).
func TestBuildGpfdistDeployment_Port(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)
	c := dep.Spec.Template.Spec.Containers[0]
	require.Len(t, c.Ports, 1)
	assert.Equal(t, "gpfdist", c.Ports[0].Name)
	assert.Equal(t, int32(8080), c.Ports[0].ContainerPort)
	assert.Equal(t, corev1.ProtocolTCP, c.Ports[0].Protocol)
}

// TestBuildGpfdistDeployment_VolumeAndMount (SC101-GP4-PVCNAME, SC101-GP4-MOUNT)
// asserts the data volumeMount at /data and that the pod volume references the
// "<cluster>-gpfdist-data-pvc" claim.
func TestBuildGpfdistDeployment_VolumeAndMount(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)
	c := dep.Spec.Template.Spec.Containers[0]

	require.Len(t, c.VolumeMounts, 1)
	assert.Equal(t, "data", c.VolumeMounts[0].Name)
	assert.Equal(t, "/data", c.VolumeMounts[0].MountPath)

	require.Len(t, dep.Spec.Template.Spec.Volumes, 1)
	vol := dep.Spec.Template.Spec.Volumes[0]
	assert.Equal(t, "data", vol.Name)
	require.NotNil(t, vol.PersistentVolumeClaim)
	assert.Equal(t, "test-cluster-gpfdist-data-pvc", vol.PersistentVolumeClaim.ClaimName)
	assert.Equal(t, util.GpfdistDataPVCName(cluster.Name), vol.PersistentVolumeClaim.ClaimName)
}

// TestBuildGpfdistDeployment_PodLabels asserts the pod-template labels carry the
// gpfdist component label (the basis for the Service selector, GP.5) and the
// selector matches the pod labels.
func TestBuildGpfdistDeployment_PodLabels(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)

	podLabels := dep.Spec.Template.ObjectMeta.Labels
	assert.Equal(t, util.ComponentGpfdist, podLabels[util.LabelComponent])
	require.NotNil(t, dep.Spec.Selector)
	assert.Equal(t, podLabels, dep.Spec.Selector.MatchLabels)
}

// --- GP.5: Service ---------------------------------------------------------

// TestBuildGpfdistService_Name (SC101-GP5-SVCNAME) asserts the Service name
// "<cluster>-gpfdist-svc" and the cluster ownerRef.
func TestBuildGpfdistService_Name(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	svc := b.BuildGpfdistService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, "test-cluster-gpfdist-svc", svc.Name)
	assert.Equal(t, util.GpfdistServiceName2(cluster.Name), svc.Name)
	assert.Equal(t, cluster.Namespace, svc.Namespace)
	assertOwnedByCluster(t, svc.OwnerReferences, cluster)
}

// TestBuildGpfdistService_SelectorEqualsPodLabels (SC101-GP5-SELECTOR) asserts
// the Service selector contains avsoft.io/component==gpfdist AND EQUALS the
// Deployment pod-template labels (so the selector cannot drift from the pods).
func TestBuildGpfdistService_SelectorEqualsPodLabels(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	svc := b.BuildGpfdistService(cluster)
	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, svc)
	require.NotNil(t, dep)

	assert.Equal(t, util.ComponentGpfdist, svc.Spec.Selector[util.LabelComponent])
	// The selector must EQUAL the Deployment pod-template labels (GP.5).
	assert.Equal(t, dep.Spec.Template.ObjectMeta.Labels, svc.Spec.Selector)
}

// TestBuildGpfdistService_Port (SC101-GP5-PORT) asserts the Service port 8080
// targetPort 8080 (the default).
func TestBuildGpfdistService_Port(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(nil)

	svc := b.BuildGpfdistService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	assert.Equal(t, 8080, svc.Spec.Ports[0].TargetPort.IntValue())
	assert.Equal(t, corev1.ProtocolTCP, svc.Spec.Ports[0].Protocol)
}

// --- Custom port honored across Deployment + Service ----------------------

// TestBuildGpfdist_CustomPortHonored asserts a custom gpfdist.port flows into the
// Deployment args (-p <port>), the named container port AND the Service
// port/targetPort, keeping them consistent.
func TestBuildGpfdist_CustomPortHonored(t *testing.T) {
	b := NewBuilder()
	cluster := gpfdistCluster(&cbv1alpha1.GpfdistSpec{Port: 8081})

	dep := b.BuildGpfdistDeployment(cluster)
	require.NotNil(t, dep)
	c := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(t, []string{
		"-d", "/data",
		"-p", "8081",
		"-l", "/var/log/gpfdist.log",
	}, c.Args)
	require.Len(t, c.Ports, 1)
	assert.Equal(t, int32(8081), c.Ports[0].ContainerPort)

	svc := b.BuildGpfdistService(cluster)
	require.NotNil(t, svc)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8081), svc.Spec.Ports[0].Port)
	assert.Equal(t, 8081, svc.Spec.Ports[0].TargetPort.IntValue())
}
