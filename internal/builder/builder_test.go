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

func newTestCluster() *cbv1alpha1.CloudberryCluster {
	replicas := int32(1)
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-123",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version:         "7.7",
			Image:           "cloudberrydb/cloudberry:7.7",
			ImagePullPolicy: cbv1alpha1.ImagePullIfNotPresent,
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Replicas: &replicas,
				Storage:  cbv1alpha1.StorageSpec{Size: "10Gi", StorageClass: "fast"},
				Port:     5432,
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:        4,
				Storage:      cbv1alpha1.StorageSpec{Size: "20Gi"},
				AntiAffinity: cbv1alpha1.AntiAffinityPreferred,
			},
			DeletionPolicy: cbv1alpha1.DeletionPolicyRetain,
		},
	}
}

func TestNewBuilder(t *testing.T) {
	b := NewBuilder()
	require.NotNil(t, b)
}

func TestBuildCoordinatorStatefulSet(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	assert.Equal(t, util.CoordinatorName("test-cluster"), sts.Name)
	assert.Equal(t, "default", sts.Namespace)
	assert.Equal(t, int32(1), *sts.Spec.Replicas)
	assert.Equal(t, util.CoordinatorServiceName("test-cluster"), sts.Spec.ServiceName)

	// Check labels
	assert.Equal(t, util.LabelManagedByValue, sts.Labels[util.LabelManagedBy])
	assert.Equal(t, "test-cluster", sts.Labels[util.LabelCluster])
	assert.Equal(t, util.ComponentCoordinator, sts.Labels[util.LabelComponent])

	// Check owner reference
	require.Len(t, sts.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", sts.OwnerReferences[0].Name)
	assert.True(t, *sts.OwnerReferences[0].Controller)

	// Check containers
	require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
	require.Len(t, sts.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "cloudberry", sts.Spec.Template.Spec.Containers[0].Name)
	assert.Equal(t, "cloudberrydb/cloudberry:7.7", sts.Spec.Template.Spec.Containers[0].Image)

	// Check PVC
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	assert.Equal(t, "data", sts.Spec.VolumeClaimTemplates[0].Name)
}

func TestBuildCoordinatorStatefulSet_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, int32(5432), container.Ports[0].ContainerPort)
}

func TestBuildCoordinatorStatefulSet_WithResources(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Resources = &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "1", Memory: "2Gi"},
		Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "4Gi"},
	}

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	container := sts.Spec.Template.Spec.Containers[0]
	assert.NotEmpty(t, container.Resources.Requests)
	assert.NotEmpty(t, container.Resources.Limits)
}

func TestBuildCoordinatorStatefulSet_WithNodeSelector(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.NodeSelector = map[string]string{"role": "coordinator"}

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)
	assert.Equal(t, "coordinator", sts.Spec.Template.Spec.NodeSelector["role"])
}

func TestBuildCoordinatorStatefulSet_WithTolerations(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Tolerations = []cbv1alpha1.Toleration{
		{Key: "dedicated", Operator: "Equal", Value: "coordinator", Effect: "NoSchedule"},
	}

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(t, "dedicated", sts.Spec.Template.Spec.Tolerations[0].Key)
}

func TestBuildCoordinatorStatefulSet_WithImagePullSecrets(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.ImagePullSecrets = []cbv1alpha1.ImagePullSecret{
		{Name: "my-registry-secret"},
	}

	sts := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "my-registry-secret", sts.Spec.Template.Spec.ImagePullSecrets[0].Name)
}

func TestBuildStandbyStatefulSet(t *testing.T) {
	t.Run("standby disabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = nil

		sts := b.BuildStandbyStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("standby not enabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: false}

		sts := b.BuildStandbyStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("standby enabled", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
			Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
		}

		sts := b.BuildStandbyStatefulSet(cluster)
		require.NotNil(t, sts)
		assert.Equal(t, util.StandbyName("test-cluster"), sts.Name)
		assert.Equal(t, int32(1), *sts.Spec.Replicas)
	})

	t.Run("standby uses coordinator storage when not specified", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
		}

		sts := b.BuildStandbyStatefulSet(cluster)
		require.NotNil(t, sts)
		// Should use coordinator storage
		assert.Equal(t, "10Gi", sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
	})
}

func TestBuildSegmentPrimaryStatefulSet(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	sts := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NotNil(t, sts)

	assert.Equal(t, util.SegmentPrimaryName("test-cluster"), sts.Name)
	assert.Equal(t, int32(4), *sts.Spec.Replicas)
	assert.Equal(t, util.SegmentServiceName("test-cluster"), sts.Spec.ServiceName)

	// Check anti-affinity
	require.NotNil(t, sts.Spec.Template.Spec.Affinity)
	require.NotNil(t, sts.Spec.Template.Spec.Affinity.PodAntiAffinity)
}

func TestBuildSegmentMirrorStatefulSet(t *testing.T) {
	t.Run("mirroring disabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Segments.Mirroring = nil

		sts := b.BuildSegmentMirrorStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("mirroring not enabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}

		sts := b.BuildSegmentMirrorStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("mirroring enabled", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

		sts := b.BuildSegmentMirrorStatefulSet(cluster)
		require.NotNil(t, sts)
		assert.Equal(t, util.SegmentMirrorName("test-cluster"), sts.Name)
		assert.Equal(t, int32(4), *sts.Spec.Replicas)
	})
}

func TestBuildCoordinatorService(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	svc := b.BuildCoordinatorService(cluster)
	require.NotNil(t, svc)

	assert.Equal(t, util.CoordinatorServiceName("test-cluster"), svc.Name)
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildStandbyService(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	svc := b.BuildStandbyService(cluster)
	require.NotNil(t, svc)

	assert.Equal(t, util.StandbyServiceName("test-cluster"), svc.Name)
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
}

func TestBuildSegmentService(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	svc := b.BuildSegmentService(cluster)
	require.NotNil(t, svc)

	assert.Equal(t, util.SegmentServiceName("test-cluster"), svc.Name)
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildClientService(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	svc := b.BuildClientService(cluster)
	require.NotNil(t, svc)

	assert.Equal(t, util.ClientServiceName("test-cluster"), svc.Name)
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Empty(t, svc.Spec.ClusterIP) // Not headless
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildPostgresqlConfConfigMap(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)

	assert.Equal(t, util.PostgresqlConfConfigMapName("test-cluster"), cm.Name)
	assert.Contains(t, cm.Data["postgresql.conf"], "port = 5432")
	assert.Contains(t, cm.Data["postgresql.conf"], "listen_addresses = '*'")
	assert.NotEmpty(t, cm.Annotations["cloudberry.example.com/config-hash"])
}

func TestBuildPostgresqlConfConfigMap_WithSSL(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:       true,
			CertSecret:    &cbv1alpha1.CertSecretRef{Name: "tls-secret"},
			MinTLSVersion: "1.3",
		},
	}

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)

	conf := cm.Data["postgresql.conf"]
	assert.Contains(t, conf, "ssl = on")
	assert.Contains(t, conf, "ssl_cert_file")
	assert.Contains(t, conf, "TLSv1.3")
}

func TestBuildPostgresqlConfConfigMap_WithParameters(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{
			"max_connections": "200",
			"work_mem":        "64MB",
		},
	}

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)

	conf := cm.Data["postgresql.conf"]
	assert.Contains(t, conf, "max_connections = '200'")
	assert.Contains(t, conf, "work_mem = '64MB'")
}

func TestBuildPgHBAConfConfigMap(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	cm := b.BuildPgHBAConfConfigMap(cluster)
	require.NotNil(t, cm)

	assert.Equal(t, util.PgHBAConfConfigMapName("test-cluster"), cm.Name)
	hba := cm.Data["pg_hba.conf"]
	assert.Contains(t, hba, "local")
	assert.Contains(t, hba, "gpadmin")
	assert.Contains(t, hba, "scram-sha-256")
}

func TestBuildPgHBAConfConfigMap_CustomRules(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		HBARules: []cbv1alpha1.HBARule{
			{
				Type:     cbv1alpha1.HBATypeHostSSL,
				Database: "mydb",
				User:     "myuser",
				Address:  "10.0.0.0/8",
				Method:   cbv1alpha1.AuthMethodCert,
				Options:  "clientcert=1",
			},
		},
	}

	cm := b.BuildPgHBAConfConfigMap(cluster)
	require.NotNil(t, cm)

	hba := cm.Data["pg_hba.conf"]
	assert.Contains(t, hba, "hostssl")
	assert.Contains(t, hba, "mydb")
	assert.Contains(t, hba, "myuser")
	assert.Contains(t, hba, "10.0.0.0/8")
	assert.Contains(t, hba, "cert")
	assert.Contains(t, hba, "clientcert=1")
}

func TestBuildAdminPasswordSecret(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	secret := b.BuildAdminPasswordSecret(cluster, "super-secret-password")
	require.NotNil(t, secret)

	assert.Equal(t, util.AdminPasswordSecretName("test-cluster"), secret.Name)
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)
	assert.Equal(t, []byte("super-secret-password"), secret.Data["password"])
}

func TestBuildSegmentAffinity_Required(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.AntiAffinity = cbv1alpha1.AntiAffinityRequired

	affinity := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	require.NotNil(t, affinity)
	require.NotNil(t, affinity.PodAntiAffinity)
	require.Len(t, affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
	assert.Equal(t, "kubernetes.io/hostname",
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey)
}

func TestBuildSegmentAffinity_Preferred(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.AntiAffinity = cbv1alpha1.AntiAffinityPreferred

	affinity := buildSegmentAffinity(cluster, util.ComponentSegmentPrimary)
	require.NotNil(t, affinity)
	require.NotNil(t, affinity.PodAntiAffinity)
	require.Len(t, affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	assert.Equal(t, int32(100),
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight)
}

func TestConvertResources(t *testing.T) {
	tests := []struct {
		name string
		res  *cbv1alpha1.ResourceRequirements
	}{
		{
			name: "full resources",
			res: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "1", Memory: "2Gi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "4Gi"},
			},
		},
		{
			name: "requests only",
			res: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "500m"},
			},
		},
		{
			name: "limits only",
			res: &cbv1alpha1.ResourceRequirements{
				Limits: &cbv1alpha1.ResourceList{Memory: "1Gi"},
			},
		},
		{
			name: "empty resources",
			res:  &cbv1alpha1.ResourceRequirements{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertResources(tt.res)
			_ = result // Just verify no panic
		})
	}
}

func TestConvertTolerations(t *testing.T) {
	t.Run("nil tolerations", func(t *testing.T) {
		result := convertTolerations(nil)
		assert.Nil(t, result)
	})

	t.Run("empty tolerations", func(t *testing.T) {
		result := convertTolerations([]cbv1alpha1.Toleration{})
		assert.Nil(t, result)
	})

	t.Run("with tolerations", func(t *testing.T) {
		seconds := int64(300)
		tolerations := []cbv1alpha1.Toleration{
			{
				Key:               "dedicated",
				Operator:          "Equal",
				Value:             "coordinator",
				Effect:            "NoSchedule",
				TolerationSeconds: &seconds,
			},
		}
		result := convertTolerations(tolerations)
		require.Len(t, result, 1)
		assert.Equal(t, "dedicated", result[0].Key)
		assert.Equal(t, corev1.TolerationOpEqual, result[0].Operator)
		assert.Equal(t, corev1.TaintEffectNoSchedule, result[0].Effect)
		require.NotNil(t, result[0].TolerationSeconds)
		assert.Equal(t, int64(300), *result[0].TolerationSeconds)
	})
}

func TestBuildVolumes_WithSSL(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: &cbv1alpha1.CertSecretRef{Name: "tls-secret"},
		},
	}

	volumes := buildVolumes(cluster)
	require.Len(t, volumes, 2) // config + tls
	assert.Equal(t, "tls", volumes[1].Name)
}

func TestBuildVolumes_WithoutSSL(t *testing.T) {
	cluster := newTestCluster()

	volumes := buildVolumes(cluster)
	require.Len(t, volumes, 1) // config only
	assert.Equal(t, "config", volumes[0].Name)
}

func TestDefaultBuilder_ImplementsInterface(t *testing.T) {
	var _ ResourceBuilder = &DefaultBuilder{}
}

func TestBuildPVC_WithStorageClass(t *testing.T) {
	storage := cbv1alpha1.StorageSpec{Size: "10Gi", StorageClass: "fast"}
	labels := map[string]string{"app": "test"}

	pvc := buildPVC(storage, labels)
	require.NotNil(t, pvc.Spec.StorageClassName)
	assert.Equal(t, "fast", *pvc.Spec.StorageClassName)
}

func TestBuildPVC_WithoutStorageClass(t *testing.T) {
	storage := cbv1alpha1.StorageSpec{Size: "10Gi"}
	labels := map[string]string{"app": "test"}

	pvc := buildPVC(storage, labels)
	assert.Nil(t, pvc.Spec.StorageClassName)
}
