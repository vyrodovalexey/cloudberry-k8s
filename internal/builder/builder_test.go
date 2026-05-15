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

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
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

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
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

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	container := sts.Spec.Template.Spec.Containers[0]
	assert.NotEmpty(t, container.Resources.Requests)
	assert.NotEmpty(t, container.Resources.Limits)
}

func TestBuildCoordinatorStatefulSet_WithNodeSelector(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.NodeSelector = map[string]string{"role": "coordinator"}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)
	assert.Equal(t, "coordinator", sts.Spec.Template.Spec.NodeSelector["role"])
}

func TestBuildCoordinatorStatefulSet_WithTolerations(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Tolerations = []cbv1alpha1.Toleration{
		{Key: "dedicated", Operator: "Equal", Value: "coordinator", Effect: "NoSchedule"},
	}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
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

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "my-registry-secret", sts.Spec.Template.Spec.ImagePullSecrets[0].Name)
}

func TestBuildStandbyStatefulSet(t *testing.T) {
	t.Run("standby disabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = nil

		sts, _ := b.BuildStandbyStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("standby not enabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: false}

		sts, _ := b.BuildStandbyStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("standby enabled", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
			Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
		}

		sts, _ := b.BuildStandbyStatefulSet(cluster)
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

		sts, _ := b.BuildStandbyStatefulSet(cluster)
		require.NotNil(t, sts)
		// Should use coordinator storage
		assert.Equal(t, "10Gi", sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
	})
}

func TestBuildSegmentPrimaryStatefulSet(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
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

		sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("mirroring not enabled returns nil", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}

		sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
		assert.Nil(t, sts)
	})

	t.Run("mirroring enabled", func(t *testing.T) {
		b := NewBuilder()
		cluster := newTestCluster()
		cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

		sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
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
	assert.NotEmpty(t, cm.Annotations[util.AnnotationConfigHash])
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
			result, err := convertResources(tt.res)
			require.NoError(t, err)
			_ = result // Just verify no error
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

	pvc, err := buildPVC(storage, labels)
	require.NoError(t, err)
	require.NotNil(t, pvc.Spec.StorageClassName)
	assert.Equal(t, "fast", *pvc.Spec.StorageClassName)
}

func TestBuildPVC_WithoutStorageClass(t *testing.T) {
	storage := cbv1alpha1.StorageSpec{Size: "10Gi"}
	labels := map[string]string{"app": "test"}

	pvc, err := buildPVC(storage, labels)
	require.NoError(t, err)
	assert.Nil(t, pvc.Spec.StorageClassName)
}

func TestBuildCoordinatorStatefulSet_WithBackupConfig(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Schedule:    "0 2 * * *",
		Compression: 6,
		Destination: cbv1alpha1.BackupDestination{
			Type:   "s3",
			Bucket: "cloudberry-backups",
		},
	}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	// Verify the StatefulSet is created with the correct name and labels.
	assert.Equal(t, util.CoordinatorName("test-cluster"), sts.Name)
	assert.Equal(t, util.ComponentCoordinator, sts.Labels[util.LabelComponent])

	// Verify the main container exists.
	require.Len(t, sts.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "cloudberry", sts.Spec.Template.Spec.Containers[0].Name)
}

func TestBuildCoordinatorStatefulSet_WithDataLoadingConfig(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		StreamingServer: &cbv1alpha1.StreamingServerSpec{
			Host:    "streaming.example.com",
			Port:    5432,
			TLSMode: "none",
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:        "s3-loader",
				Type:        "s3",
				Enabled:     true,
				TargetTable: "public.events",
			},
		},
	}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	// Verify the StatefulSet is created with config volume.
	volumes := sts.Spec.Template.Spec.Volumes
	require.NotEmpty(t, volumes)

	// Config volume should always be present.
	configVolumeFound := false
	for _, v := range volumes {
		if v.Name == "config" {
			configVolumeFound = true
			break
		}
	}
	assert.True(t, configVolumeFound, "config volume should be present")

	// Verify the main container has config volume mount.
	container := sts.Spec.Template.Spec.Containers[0]
	configMountFound := false
	for _, vm := range container.VolumeMounts {
		if vm.Name == "config" {
			configMountFound = true
			break
		}
	}
	assert.True(t, configMountFound, "config volume mount should be present")
}

func TestBuildPostgresqlConfConfigMap_WithQueryMonitoring(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "30d",
		SamplingInterval:   5,
		SlowQueryThreshold: "1000ms",
	}
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{
			"log_min_duration_statement": "1000",
		},
	}

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)

	conf := cm.Data["postgresql.conf"]
	assert.Contains(t, conf, "log_min_duration_statement = '1000'")
}

func TestBuildPostgresqlConfConfigMap_WithWorkloadParams(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{
			"gp_enable_global_deadlock_detector": "on",
			"gp_autostats_mode":                  "on_change",
		},
	}

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)

	conf := cm.Data["postgresql.conf"]
	assert.Contains(t, conf, "gp_autostats_mode = 'on_change'")
	assert.Contains(t, conf, "gp_enable_global_deadlock_detector = 'on'")
}

func TestBuildSegmentPrimaryStatefulSet_WithRequiredAntiAffinity(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.AntiAffinity = cbv1alpha1.AntiAffinityRequired
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NotNil(t, sts)

	affinity := sts.Spec.Template.Spec.Affinity
	require.NotNil(t, affinity)
	require.NotNil(t, affinity.PodAntiAffinity)
	require.Len(t, affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
}

func TestBuildCoordinatorStatefulSet_WithSSL(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: &cbv1alpha1.CertSecretRef{Name: "tls-secret"},
		},
	}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	// Verify TLS volume mount is present.
	container := sts.Spec.Template.Spec.Containers[0]
	tlsMountFound := false
	for _, vm := range container.VolumeMounts {
		if vm.Name == "tls" {
			tlsMountFound = true
			break
		}
	}
	assert.True(t, tlsMountFound, "TLS volume mount should be present")

	// Verify TLS volume is present.
	tlsVolumeFound := false
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == "tls" {
			tlsVolumeFound = true
			break
		}
	}
	assert.True(t, tlsVolumeFound, "TLS volume should be present")
}

func TestBuildCoordinatorStatefulSet_InvalidResources(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Resources = &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "invalid-cpu"},
	}

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when resources are invalid")
}

func TestBuildCoordinatorStatefulSet_InvalidStorageSize(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Storage.Size = "not-a-valid-size"

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when storage size is invalid")
}

func TestBuildStandbyStatefulSet_InvalidResources(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Resources: &cbv1alpha1.ResourceRequirements{
			Limits: &cbv1alpha1.ResourceList{Memory: "invalid-mem"},
		},
	}

	sts, _ := b.BuildStandbyStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when standby resources are invalid")
}

func TestBuildStandbyStatefulSet_InvalidStorageSize(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Storage: &cbv1alpha1.StorageSpec{Size: "bad-size"},
	}

	sts, _ := b.BuildStandbyStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when standby storage size is invalid")
}

func TestBuildStandbyStatefulSet_WithNodeSelector(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled:      true,
		NodeSelector: map[string]string{"zone": "us-east-1a"},
	}

	sts, _ := b.BuildStandbyStatefulSet(cluster)
	require.NotNil(t, sts)
	assert.Equal(t, "us-east-1a", sts.Spec.Template.Spec.NodeSelector["zone"])
}

func TestBuildSegmentPrimaryStatefulSet_InvalidResources(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Resources = &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "bad-cpu"},
	}

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when segment resources are invalid")
}

func TestBuildSegmentPrimaryStatefulSet_InvalidStorageSize(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Storage.Size = "bad-size"

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when segment storage size is invalid")
}

func TestBuildSegmentPrimaryStatefulSet_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NotNil(t, sts)
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, int32(5432), container.Ports[0].ContainerPort)
}

func TestBuildSegmentPrimaryStatefulSet_WithNodeSelectorAndTolerations(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.NodeSelector = map[string]string{"role": "segment"}
	cluster.Spec.Segments.Tolerations = []cbv1alpha1.Toleration{
		{Key: "dedicated", Operator: "Equal", Value: "segment", Effect: "NoSchedule"},
	}

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NotNil(t, sts)
	assert.Equal(t, "segment", sts.Spec.Template.Spec.NodeSelector["role"])
	require.Len(t, sts.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(t, "dedicated", sts.Spec.Template.Spec.Tolerations[0].Key)
}

func TestBuildSegmentMirrorStatefulSet_InvalidResources(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Segments.Resources = &cbv1alpha1.ResourceRequirements{
		Limits: &cbv1alpha1.ResourceList{CPU: "bad-cpu"},
	}

	sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when mirror resources are invalid")
}

func TestBuildSegmentMirrorStatefulSet_InvalidStorageSize(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Segments.Storage.Size = "bad-size"

	sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	assert.Nil(t, sts, "should return nil when mirror storage size is invalid")
}

func TestBuildSegmentMirrorStatefulSet_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Coordinator.Port = 0

	sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	require.NotNil(t, sts)
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, int32(5432), container.Ports[0].ContainerPort)
}

func TestBuildSegmentMirrorStatefulSet_WithNodeSelectorAndTolerations(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Segments.NodeSelector = map[string]string{"role": "segment"}
	cluster.Spec.Segments.Tolerations = []cbv1alpha1.Toleration{
		{Key: "dedicated", Operator: "Equal", Value: "segment", Effect: "NoSchedule"},
	}

	sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	require.NotNil(t, sts)
	assert.Equal(t, "segment", sts.Spec.Template.Spec.NodeSelector["role"])
	require.Len(t, sts.Spec.Template.Spec.Tolerations, 1)
}

func TestBuildCoordinatorService_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	svc := b.BuildCoordinatorService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildStandbyService_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	svc := b.BuildStandbyService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildSegmentService_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	svc := b.BuildSegmentService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildClientService_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	svc := b.BuildClientService(cluster)
	require.NotNil(t, svc)
	assert.Equal(t, int32(5432), svc.Spec.Ports[0].Port)
}

func TestBuildPVC_InvalidStorageSize(t *testing.T) {
	storage := cbv1alpha1.StorageSpec{Size: "not-a-valid-size"}
	labels := map[string]string{"app": "test"}

	_, err := buildPVC(storage, labels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing storage size")
}

func TestConvertResources_InvalidCPURequest(t *testing.T) {
	res := &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "invalid-cpu"},
	}
	_, err := convertResources(res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing CPU request")
}

func TestConvertResources_InvalidMemoryRequest(t *testing.T) {
	res := &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{Memory: "invalid-mem"},
	}
	_, err := convertResources(res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing memory request")
}

func TestConvertResources_InvalidCPULimit(t *testing.T) {
	res := &cbv1alpha1.ResourceRequirements{
		Limits: &cbv1alpha1.ResourceList{CPU: "invalid-cpu"},
	}
	_, err := convertResources(res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing CPU limit")
}

func TestConvertResources_InvalidMemoryLimit(t *testing.T) {
	res := &cbv1alpha1.ResourceRequirements{
		Limits: &cbv1alpha1.ResourceList{Memory: "invalid-mem"},
	}
	_, err := convertResources(res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing memory limit")
}

func TestBuildPostgresqlConfConfigMap_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)
	assert.Contains(t, cm.Data["postgresql.conf"], "port = 5432")
}

func TestBuildPostgresqlConfConfigMap_SSLDefaultMinTLS(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: &cbv1alpha1.CertSecretRef{Name: "tls-secret"},
			// MinTLSVersion not set, should default to TLSv1.2.
		},
	}

	cm := b.BuildPostgresqlConfConfigMap(cluster)
	require.NotNil(t, cm)
	assert.Contains(t, cm.Data["postgresql.conf"], "TLSv1.2")
}

func TestBuildMainContainer_WithSSL(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: &cbv1alpha1.CertSecretRef{Name: "tls-secret"},
		},
	}

	container, err := buildMainContainer(cluster, 5432, nil)
	require.NoError(t, err)

	// Verify TLS volume mount is present.
	tlsMountFound := false
	for _, vm := range container.VolumeMounts {
		if vm.Name == "tls" {
			tlsMountFound = true
			assert.True(t, vm.ReadOnly)
			break
		}
	}
	assert.True(t, tlsMountFound, "TLS volume mount should be present")
}

func TestBuildMainContainer_WithoutSSL(t *testing.T) {
	cluster := newTestCluster()

	container, err := buildMainContainer(cluster, 5432, nil)
	require.NoError(t, err)

	// Verify TLS volume mount is NOT present.
	for _, vm := range container.VolumeMounts {
		assert.NotEqual(t, "tls", vm.Name, "TLS volume mount should not be present")
	}
}

func TestBuildMainContainer_WithResources(t *testing.T) {
	cluster := newTestCluster()
	resources := &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "1", Memory: "2Gi"},
		Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "4Gi"},
	}

	container, err := buildMainContainer(cluster, 5432, resources)
	require.NoError(t, err)
	assert.NotEmpty(t, container.Resources.Requests)
	assert.NotEmpty(t, container.Resources.Limits)
}

func TestBuildMainContainer_InvalidResources(t *testing.T) {
	cluster := newTestCluster()
	resources := &cbv1alpha1.ResourceRequirements{
		Requests: &cbv1alpha1.ResourceList{CPU: "invalid"},
	}

	_, err := buildMainContainer(cluster, 5432, resources)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting resources")
}

func TestBuildInitContainer(t *testing.T) {
	container := buildInitContainer()
	assert.Equal(t, "init-cloudberry", container.Name)
	assert.Equal(t, "busybox:1.36", container.Image)
	require.Len(t, container.VolumeMounts, 1)
	assert.Equal(t, "data", container.VolumeMounts[0].Name)
}

func TestOwnerRef(t *testing.T) {
	cluster := newTestCluster()
	ref := ownerRef(cluster)
	assert.Equal(t, "test-cluster", ref.Name)
	assert.Equal(t, "CloudberryCluster", ref.Kind)
	assert.True(t, *ref.Controller)
	assert.True(t, *ref.BlockOwnerDeletion)
}

func TestAddImagePullSecrets_Empty(t *testing.T) {
	spec := &corev1.PodSpec{}
	addImagePullSecrets(spec, nil)
	assert.Empty(t, spec.ImagePullSecrets)
}

func TestAddImagePullSecrets_Multiple(t *testing.T) {
	spec := &corev1.PodSpec{}
	secrets := []cbv1alpha1.ImagePullSecret{
		{Name: "secret1"},
		{Name: "secret2"},
	}
	addImagePullSecrets(spec, secrets)
	require.Len(t, spec.ImagePullSecrets, 2)
	assert.Equal(t, "secret1", spec.ImagePullSecrets[0].Name)
	assert.Equal(t, "secret2", spec.ImagePullSecrets[1].Name)
}

func TestBuildSegmentService_SelectorLabels(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	svc := b.BuildSegmentService(cluster)
	require.NotNil(t, svc)

	// Segment service should NOT have component label in selector (matches both primary and mirror).
	_, hasComponent := svc.Spec.Selector[util.LabelComponent]
	assert.False(t, hasComponent, "segment service selector should not have component label")
	assert.Equal(t, cluster.Name, svc.Spec.Selector[util.LabelCluster])
}

func TestBuildCoordinatorStatefulSet_CustomPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 6432

	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	require.NotNil(t, sts)

	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, int32(6432), container.Ports[0].ContainerPort)
}

func TestBuildStandbyStatefulSet_DefaultPort(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Coordinator.Port = 0
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	sts, _ := b.BuildStandbyStatefulSet(cluster)
	require.NotNil(t, sts)
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, int32(5432), container.Ports[0].ContainerPort)
}

func TestBuildSegmentMirrorStatefulSet_WithImagePullSecrets(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.ImagePullSecrets = []cbv1alpha1.ImagePullSecret{
		{Name: "my-secret"},
	}

	sts, _ := b.BuildSegmentMirrorStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "my-secret", sts.Spec.Template.Spec.ImagePullSecrets[0].Name)
}

func TestBuildStandbyStatefulSet_WithImagePullSecrets(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.ImagePullSecrets = []cbv1alpha1.ImagePullSecret{
		{Name: "my-secret"},
	}

	sts, _ := b.BuildStandbyStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "my-secret", sts.Spec.Template.Spec.ImagePullSecrets[0].Name)
}

func TestBuildSegmentPrimaryStatefulSet_WithImagePullSecrets(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.ImagePullSecrets = []cbv1alpha1.ImagePullSecret{
		{Name: "my-secret"},
	}

	sts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NotNil(t, sts)
	require.Len(t, sts.Spec.Template.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "my-secret", sts.Spec.Template.Spec.ImagePullSecrets[0].Name)
}

func TestRenderPgHBAConf_DefaultRules(t *testing.T) {
	cluster := newTestCluster()
	content := renderPgHBAConf(cluster)

	assert.Contains(t, content, "Generated by cloudberry-operator")
	assert.Contains(t, content, "local")
	assert.Contains(t, content, "gpadmin")
	assert.Contains(t, content, "trust")
	assert.Contains(t, content, "scram-sha-256")
	assert.Contains(t, content, "replication")
}

func TestRenderPgHBAConf_CustomRules(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		HBARules: []cbv1alpha1.HBARule{
			{Type: cbv1alpha1.HBATypeHostSSL, Database: "mydb", User: "myuser",
				Address: "10.0.0.0/8", Method: cbv1alpha1.AuthMethodCert},
		},
	}
	content := renderPgHBAConf(cluster)

	assert.Contains(t, content, "hostssl")
	assert.Contains(t, content, "mydb")
	// Should NOT contain default rules.
	assert.NotContains(t, content, "trust")
}

func TestDefaultHBARules(t *testing.T) {
	rules := defaultHBARules()
	require.Len(t, rules, 5)
	assert.Equal(t, cbv1alpha1.HBATypeLocal, rules[0].Type)
	assert.Equal(t, "gpadmin", rules[0].User)
}

func TestBuildVolumes_SSLEnabledNoCertSecret(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: nil, // No cert secret.
		},
	}

	volumes := buildVolumes(cluster)
	// Should only have config volume, no TLS volume.
	require.Len(t, volumes, 1)
	assert.Equal(t, "config", volumes[0].Name)
}

func TestFormatHBARule_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		rule     cbv1alpha1.HBARule
		contains []string
	}{
		{
			name: "local rule",
			rule: cbv1alpha1.HBARule{
				Type: cbv1alpha1.HBATypeLocal, Database: "all",
				User: "gpadmin", Method: cbv1alpha1.AuthMethodTrust,
			},
			contains: []string{"local", "all", "gpadmin", "trust"},
		},
		{
			name: "host rule with address",
			rule: cbv1alpha1.HBARule{
				Type: cbv1alpha1.HBATypeHost, Database: "mydb",
				User: "appuser", Address: "10.0.0.0/8",
				Method: cbv1alpha1.AuthMethodScramSHA256,
			},
			contains: []string{"host", "mydb", "appuser", "10.0.0.0/8", "scram-sha-256"},
		},
		{
			name: "hostssl rule with options",
			rule: cbv1alpha1.HBARule{
				Type: cbv1alpha1.HBATypeHostSSL, Database: "all",
				User: "all", Address: "0.0.0.0/0",
				Method: cbv1alpha1.AuthMethodCert, Options: "clientcert=1",
			},
			contains: []string{"hostssl", "cert", "clientcert=1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatHBARule(tt.rule)
			for _, s := range tt.contains {
				assert.Contains(t, result, s)
			}
		})
	}
}
