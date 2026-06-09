package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func newMinimalCluster() *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Segments: cbv1alpha1.SegmentsSpec{
				Count: 4,
			},
		},
	}
}

func TestNewCloudberryClusterDefaulter(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	require.NotNil(t, d)
}

// TestDefault_RecordsAdmissionWhenRecorderConfigured verifies that when a
// metrics recorder is supplied via NewCloudberryClusterDefaulter, Default records
// a mutating-webhook admission with result "allowed" (defaulting never denies).
func TestDefault_RecordsAdmissionWhenRecorderConfigured(t *testing.T) {
	rec := newCapturingRecorder()
	d := NewCloudberryClusterDefaulter(rec)
	cluster := newMinimalCluster()

	require.NoError(t, d.Default(context.Background(), cluster))

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookMutating, rec.lastWebhook)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)
}

func TestDefault(t *testing.T) {
	d := NewCloudberryClusterDefaulter()
	cluster := newMinimalCluster()

	err := d.Default(context.Background(), cluster)
	require.NoError(t, err)

	// Verify defaults were set
	assert.Equal(t, util.DefaultVersion, cluster.Spec.Version)
	assert.Equal(t, util.DefaultImage, cluster.Spec.Image)
	assert.Equal(t, cbv1alpha1.ImagePullIfNotPresent, cluster.Spec.ImagePullPolicy)
}

func TestSetSpecDefaults(t *testing.T) {
	tests := []struct {
		name     string
		spec     cbv1alpha1.CloudberryClusterSpec
		validate func(t *testing.T, spec *cbv1alpha1.CloudberryClusterSpec)
	}{
		{
			name: "empty spec gets defaults",
			spec: cbv1alpha1.CloudberryClusterSpec{},
			validate: func(t *testing.T, spec *cbv1alpha1.CloudberryClusterSpec) {
				assert.Equal(t, util.DefaultVersion, spec.Version)
				assert.Equal(t, util.DefaultImage, spec.Image)
				assert.Equal(t, cbv1alpha1.ImagePullIfNotPresent, spec.ImagePullPolicy)
			},
		},
		{
			name: "existing values preserved",
			spec: cbv1alpha1.CloudberryClusterSpec{
				Version:         "8.0",
				Image:           "custom/image:8.0",
				ImagePullPolicy: cbv1alpha1.ImagePullAlways,
			},
			validate: func(t *testing.T, spec *cbv1alpha1.CloudberryClusterSpec) {
				assert.Equal(t, "8.0", spec.Version)
				assert.Equal(t, "custom/image:8.0", spec.Image)
				assert.Equal(t, cbv1alpha1.ImagePullAlways, spec.ImagePullPolicy)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setSpecDefaults(&tt.spec)
			tt.validate(t, &tt.spec)
		})
	}
}

func TestSetCoordinatorDefaults(t *testing.T) {
	t.Run("empty coordinator gets defaults", func(t *testing.T) {
		coord := cbv1alpha1.CoordinatorSpec{}
		setCoordinatorDefaults(&coord)

		require.NotNil(t, coord.Replicas)
		assert.Equal(t, int32(1), *coord.Replicas)
		assert.Equal(t, int32(util.DefaultCoordinatorPort), coord.Port)
		assert.Equal(t, "10Gi", coord.Storage.Size)
	})

	t.Run("existing values preserved", func(t *testing.T) {
		replicas := int32(1)
		coord := cbv1alpha1.CoordinatorSpec{
			Replicas: &replicas,
			Port:     5433,
			Storage:  cbv1alpha1.StorageSpec{Size: "50Gi"},
		}
		setCoordinatorDefaults(&coord)

		assert.Equal(t, int32(1), *coord.Replicas)
		assert.Equal(t, int32(5433), coord.Port)
		assert.Equal(t, "50Gi", coord.Storage.Size)
	})
}

func TestSetSegmentDefaults(t *testing.T) {
	t.Run("empty segments gets defaults", func(t *testing.T) {
		seg := cbv1alpha1.SegmentsSpec{Count: 4}
		setSegmentDefaults(&seg)

		assert.Equal(t, int32(2), seg.PrimariesPerHost)
		assert.Equal(t, "20Gi", seg.Storage.Size)
		assert.Equal(t, cbv1alpha1.AntiAffinityPreferred, seg.AntiAffinity)
		require.NotNil(t, seg.Mirroring)
		assert.True(t, seg.Mirroring.Enabled)
		assert.Equal(t, cbv1alpha1.MirroringLayoutGroup, seg.Mirroring.Layout)
	})

	t.Run("existing values preserved", func(t *testing.T) {
		seg := cbv1alpha1.SegmentsSpec{
			Count:            8,
			PrimariesPerHost: 4,
			Storage:          cbv1alpha1.StorageSpec{Size: "100Gi"},
			AntiAffinity:     cbv1alpha1.AntiAffinityRequired,
			Mirroring: &cbv1alpha1.MirroringSpec{
				Enabled: true,
				Layout:  cbv1alpha1.MirroringLayoutSpread,
			},
		}
		setSegmentDefaults(&seg)

		assert.Equal(t, int32(4), seg.PrimariesPerHost)
		assert.Equal(t, "100Gi", seg.Storage.Size)
		assert.Equal(t, cbv1alpha1.AntiAffinityRequired, seg.AntiAffinity)
		assert.Equal(t, cbv1alpha1.MirroringLayoutSpread, seg.Mirroring.Layout)
	})

	t.Run("mirroring with empty layout gets default", func(t *testing.T) {
		seg := cbv1alpha1.SegmentsSpec{
			Count: 4,
			Mirroring: &cbv1alpha1.MirroringSpec{
				Enabled: true,
			},
		}
		setSegmentDefaults(&seg)
		assert.Equal(t, cbv1alpha1.MirroringLayoutGroup, seg.Mirroring.Layout)
	})
}

func TestSetAuthDefaults(t *testing.T) {
	t.Run("nil auth gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		setAuthDefaults(cluster)

		require.NotNil(t, cluster.Spec.Auth)
		require.NotNil(t, cluster.Spec.Auth.Basic)
		assert.True(t, cluster.Spec.Auth.Basic.Enabled)
		assert.Equal(t, util.DefaultAdminUser, cluster.Spec.Auth.Basic.AdminUser)
	})

	t.Run("existing auth preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
			Basic: &cbv1alpha1.BasicAuthSpec{
				Enabled:   true,
				AdminUser: "custom-admin",
			},
		}
		setAuthDefaults(cluster)

		assert.Equal(t, "custom-admin", cluster.Spec.Auth.Basic.AdminUser)
	})

	t.Run("empty admin user gets default", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
			Basic: &cbv1alpha1.BasicAuthSpec{
				Enabled:   true,
				AdminUser: "",
			},
		}
		setAuthDefaults(cluster)

		assert.Equal(t, util.DefaultAdminUser, cluster.Spec.Auth.Basic.AdminUser)
	})
}

func TestSetOIDCDefaults(t *testing.T) {
	t.Run("nil oidc does nothing", func(t *testing.T) {
		setOIDCDefaults(nil)
	})

	t.Run("disabled oidc does nothing", func(t *testing.T) {
		oidc := &cbv1alpha1.OIDCSpec{Enabled: false}
		setOIDCDefaults(oidc)
		assert.Empty(t, oidc.Scopes)
	})

	t.Run("enabled oidc gets defaults", func(t *testing.T) {
		oidc := &cbv1alpha1.OIDCSpec{Enabled: true}
		setOIDCDefaults(oidc)

		assert.Equal(t, []string{"openid", "profile", "email"}, oidc.Scopes)
		assert.Equal(t, "realm_access.roles", oidc.RoleClaimPath)
		assert.Equal(t, cbv1alpha1.RoleClaimSourceIDToken, oidc.RoleClaimSource)
		assert.Equal(t, cbv1alpha1.RoleMatchExact, oidc.RoleMatchMode)
	})

	t.Run("existing oidc values preserved", func(t *testing.T) {
		oidc := &cbv1alpha1.OIDCSpec{
			Enabled:         true,
			Scopes:          []string{"openid"},
			RoleClaimPath:   "custom.path",
			RoleClaimSource: cbv1alpha1.RoleClaimSourceUserInfo,
			RoleMatchMode:   cbv1alpha1.RoleMatchPrefix,
		}
		setOIDCDefaults(oidc)

		assert.Equal(t, []string{"openid"}, oidc.Scopes)
		assert.Equal(t, "custom.path", oidc.RoleClaimPath)
		assert.Equal(t, cbv1alpha1.RoleClaimSourceUserInfo, oidc.RoleClaimSource)
		assert.Equal(t, cbv1alpha1.RoleMatchPrefix, oidc.RoleMatchMode)
	})
}

func TestSetHADefaults(t *testing.T) {
	t.Run("nil HA gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		setHADefaults(cluster)

		require.NotNil(t, cluster.Spec.HA)
		assert.Equal(t, int32(60), cluster.Spec.HA.FTSProbeInterval)
		assert.Equal(t, int32(20), cluster.Spec.HA.FTSProbeTimeout)
		assert.Equal(t, int32(5), cluster.Spec.HA.FTSProbeRetries)
	})

	t.Run("existing HA values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.HA = &cbv1alpha1.HASpec{
			FTSProbeInterval: 30,
			FTSProbeTimeout:  10,
			FTSProbeRetries:  3,
		}
		setHADefaults(cluster)

		assert.Equal(t, int32(30), cluster.Spec.HA.FTSProbeInterval)
		assert.Equal(t, int32(10), cluster.Spec.HA.FTSProbeTimeout)
		assert.Equal(t, int32(3), cluster.Spec.HA.FTSProbeRetries)
	})
}

func TestSetMonitoringDefaults(t *testing.T) {
	t.Run("nil monitoring gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		setMonitoringDefaults(cluster)

		require.NotNil(t, cluster.Spec.Monitoring)
		assert.True(t, cluster.Spec.Monitoring.Enabled)
		assert.Equal(t, int32(util.DefaultMetricsPort), cluster.Spec.Monitoring.MetricsPort)
	})

	t.Run("zero metrics port gets default", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Monitoring = &cbv1alpha1.MonitoringSpec{
			Enabled:     true,
			MetricsPort: 0,
		}
		setMonitoringDefaults(cluster)

		assert.Equal(t, int32(util.DefaultMetricsPort), cluster.Spec.Monitoring.MetricsPort)
	})
}

func TestSetClusterDefaults_DeletionPolicy(t *testing.T) {
	cluster := newMinimalCluster()
	setClusterDefaults(cluster)
	assert.Equal(t, cbv1alpha1.DeletionPolicyRetain, cluster.Spec.DeletionPolicy)
}

func TestSetClusterDefaults_ExistingDeletionPolicy(t *testing.T) {
	cluster := newMinimalCluster()
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete
	setClusterDefaults(cluster)
	assert.Equal(t, cbv1alpha1.DeletionPolicyDelete, cluster.Spec.DeletionPolicy)
}

func TestSetBackupDefaults(t *testing.T) {
	t.Run("nil backup does nothing", func(t *testing.T) {
		cluster := newMinimalCluster()
		setBackupDefaults(cluster)
		assert.Nil(t, cluster.Spec.Backup)
	})

	t.Run("backup gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
			Enabled: true,
			Destination: cbv1alpha1.BackupDestination{
				Type: "s3",
				S3:   &cbv1alpha1.S3Destination{Bucket: "my-bucket"},
			},
		}
		setBackupDefaults(cluster)

		gp := cluster.Spec.Backup.Gpbackup
		require.NotNil(t, gp)
		assert.Equal(t, int32(1), gp.CompressionLevel)
		assert.Equal(t, "gzip", gp.CompressionType)
		assert.Equal(t, int32(1), gp.Jobs)
		assert.False(t, gp.SingleDataFile)
		require.NotNil(t, gp.WithStats)
		assert.True(t, *gp.WithStats)

		gr := cluster.Spec.Backup.Gprestore
		require.NotNil(t, gr)
		assert.Equal(t, int32(1), gr.Jobs)
		require.NotNil(t, gr.WithStats)
		assert.True(t, *gr.WithStats)

		assert.Equal(t, int32(3), cluster.Spec.Backup.Retention.FullCount)
		assert.Equal(t, "30d", cluster.Spec.Backup.Retention.MaxAge)

		jt := cluster.Spec.Backup.JobTemplate
		require.NotNil(t, jt)
		require.NotNil(t, jt.BackoffLimit)
		assert.Equal(t, int32(2), *jt.BackoffLimit)
		require.NotNil(t, jt.ActiveDeadlineSeconds)
		assert.Equal(t, int64(7200), *jt.ActiveDeadlineSeconds)
		require.NotNil(t, jt.TTLSecondsAfterFinished)
		assert.Equal(t, int32(86400), *jt.TTLSecondsAfterFinished)
	})

	t.Run("disabled backup gets no defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: false}
		setBackupDefaults(cluster)
		assert.Nil(t, cluster.Spec.Backup.Gpbackup)
		assert.Nil(t, cluster.Spec.Backup.JobTemplate)
	})

	t.Run("existing backup values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
			Enabled:  true,
			Gpbackup: &cbv1alpha1.GpbackupOptions{CompressionLevel: 9, Jobs: 4},
			Retention: cbv1alpha1.BackupRetention{
				FullCount: 5,
				MaxAge:    "90d",
			},
			JobTemplate: &cbv1alpha1.BackupJobTemplate{
				BackoffLimit: util.Ptr(int32(7)),
			},
			Destination: cbv1alpha1.BackupDestination{
				Type: "s3",
				S3:   &cbv1alpha1.S3Destination{Bucket: "my-bucket"},
			},
		}
		setBackupDefaults(cluster)

		assert.Equal(t, int32(9), cluster.Spec.Backup.Gpbackup.CompressionLevel)
		assert.Equal(t, int32(4), cluster.Spec.Backup.Gpbackup.Jobs)
		assert.Equal(t, int32(5), cluster.Spec.Backup.Retention.FullCount)
		assert.Equal(t, "90d", cluster.Spec.Backup.Retention.MaxAge)
		// User-provided JobTemplate pointer fields are preserved; unset ones default.
		require.NotNil(t, cluster.Spec.Backup.JobTemplate.BackoffLimit)
		assert.Equal(t, int32(7), *cluster.Spec.Backup.JobTemplate.BackoffLimit)
		require.NotNil(t, cluster.Spec.Backup.JobTemplate.ActiveDeadlineSeconds)
		assert.Equal(t, int64(7200), *cluster.Spec.Backup.JobTemplate.ActiveDeadlineSeconds)
	})
}

// backupClusterForDefaults returns a minimal backup-enabled cluster whose
// Gpbackup/Gprestore options can be tuned per-test before defaulting.
func backupClusterForDefaults(
	gp *cbv1alpha1.GpbackupOptions,
	gr *cbv1alpha1.GprestoreOptions,
) *cbv1alpha1.CloudberryCluster {
	cluster := newMinimalCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "my-bucket"},
		},
		Gpbackup:  gp,
		Gprestore: gr,
	}
	return cluster
}

// TestSetBackupDefaults_WithStatsPointer verifies the *bool defaulting contract
// for WithStats: an UNSET (nil) value is defaulted to true, while an EXPLICIT
// false set by the user is preserved (never silently forced back to true). This
// guards the regression where WithStats was a plain bool and withStats:false was
// reverted to true on every admission.
func TestSetBackupDefaults_WithStatsPointer(t *testing.T) {
	t.Run("unset withStats defaults to true", func(t *testing.T) {
		cluster := backupClusterForDefaults(
			&cbv1alpha1.GpbackupOptions{},
			&cbv1alpha1.GprestoreOptions{},
		)
		setBackupDefaults(cluster)

		require.NotNil(t, cluster.Spec.Backup.Gpbackup.WithStats)
		assert.True(t, *cluster.Spec.Backup.Gpbackup.WithStats, "unset gpbackup.withStats defaults true")
		require.NotNil(t, cluster.Spec.Backup.Gprestore.WithStats)
		assert.True(t, *cluster.Spec.Backup.Gprestore.WithStats, "unset gprestore.withStats defaults true")
	})

	t.Run("explicit false is preserved", func(t *testing.T) {
		cluster := backupClusterForDefaults(
			&cbv1alpha1.GpbackupOptions{WithStats: util.Ptr(false)},
			&cbv1alpha1.GprestoreOptions{WithStats: util.Ptr(false)},
		)
		setBackupDefaults(cluster)

		require.NotNil(t, cluster.Spec.Backup.Gpbackup.WithStats)
		assert.False(t, *cluster.Spec.Backup.Gpbackup.WithStats,
			"explicit gpbackup.withStats:false must NOT be forced to true")
		require.NotNil(t, cluster.Spec.Backup.Gprestore.WithStats)
		assert.False(t, *cluster.Spec.Backup.Gprestore.WithStats,
			"explicit gprestore.withStats:false must NOT be forced to true")
	})

	t.Run("explicit true is preserved", func(t *testing.T) {
		cluster := backupClusterForDefaults(
			&cbv1alpha1.GpbackupOptions{WithStats: util.Ptr(true)},
			&cbv1alpha1.GprestoreOptions{WithStats: util.Ptr(true)},
		)
		setBackupDefaults(cluster)

		require.NotNil(t, cluster.Spec.Backup.Gpbackup.WithStats)
		assert.True(t, *cluster.Spec.Backup.Gpbackup.WithStats)
		require.NotNil(t, cluster.Spec.Backup.Gprestore.WithStats)
		assert.True(t, *cluster.Spec.Backup.Gprestore.WithStats)
	})
}

func TestSetDataLoadingDefaults(t *testing.T) {
	t.Run("nil data loading does nothing", func(t *testing.T) {
		cluster := newMinimalCluster()
		setDataLoadingDefaults(cluster)
		assert.Nil(t, cluster.Spec.DataLoading)
	})

	t.Run("streaming server gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			StreamingServer: &cbv1alpha1.StreamingServerSpec{
				Host: "streaming.example.com",
			},
		}
		setDataLoadingDefaults(cluster)

		assert.Equal(t, int32(5432), cluster.Spec.DataLoading.StreamingServer.Port)
		assert.Equal(t, "none", cluster.Spec.DataLoading.StreamingServer.TLSMode)
	})

	t.Run("existing streaming server values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			StreamingServer: &cbv1alpha1.StreamingServerSpec{
				Host:    "streaming.example.com",
				Port:    5433,
				TLSMode: "tls",
			},
		}
		setDataLoadingDefaults(cluster)

		assert.Equal(t, int32(5433), cluster.Spec.DataLoading.StreamingServer.Port)
		assert.Equal(t, "tls", cluster.Spec.DataLoading.StreamingServer.TLSMode)
	})

	t.Run("data loading without streaming server", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
		}
		setDataLoadingDefaults(cluster)
		assert.Nil(t, cluster.Spec.DataLoading.StreamingServer)
	})
}

func TestSetWorkloadDefaults(t *testing.T) {
	t.Run("nil workload does nothing", func(t *testing.T) {
		cluster := newMinimalCluster()
		setWorkloadDefaults(cluster)
		assert.Nil(t, cluster.Spec.Workload)
	})

	t.Run("resource groups get defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
				{Name: "analytics"},
			},
		}
		setWorkloadDefaults(cluster)

		rg := cluster.Spec.Workload.ResourceGroups[0]
		assert.Equal(t, int32(20), rg.Concurrency)
		assert.Equal(t, int32(100), rg.CPUMaxPercent)
		assert.Equal(t, int32(100), rg.CPUWeight)
	})

	t.Run("existing resource group values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
				{
					Name:          "analytics",
					Concurrency:   10,
					CPUMaxPercent: 50,
					CPUWeight:     75,
				},
			},
		}
		setWorkloadDefaults(cluster)

		rg := cluster.Spec.Workload.ResourceGroups[0]
		assert.Equal(t, int32(10), rg.Concurrency)
		assert.Equal(t, int32(50), rg.CPUMaxPercent)
		assert.Equal(t, int32(75), rg.CPUWeight)
	})

	t.Run("multiple resource groups get defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
				{Name: "analytics"},
				{Name: "etl", Concurrency: 5},
			},
		}
		setWorkloadDefaults(cluster)

		assert.Equal(t, int32(20), cluster.Spec.Workload.ResourceGroups[0].Concurrency)
		assert.Equal(t, int32(5), cluster.Spec.Workload.ResourceGroups[1].Concurrency)
		assert.Equal(t, int32(100), cluster.Spec.Workload.ResourceGroups[1].CPUMaxPercent)
	})
}

func TestSetStorageManagementDefaults(t *testing.T) {
	t.Run("nil storage does nothing", func(t *testing.T) {
		cluster := newMinimalCluster()
		setStorageManagementDefaults(cluster)
		assert.Nil(t, cluster.Spec.Storage)
	})

	t.Run("recommendation scan gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
			DiskMonitoring: true,
			RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
				Enabled: true,
			},
		}
		setStorageManagementDefaults(cluster)

		scan := cluster.Spec.Storage.RecommendationScan
		assert.Equal(t, "0 3 * * 0", scan.Schedule)
		assert.Equal(t, int32(20), scan.BloatThreshold)
		assert.Equal(t, int32(50), scan.SkewThreshold)
		assert.Equal(t, int64(500000000), scan.AgeThreshold)
		assert.Equal(t, int32(30), scan.IndexBloatThreshold)
		assert.Equal(t, "2h", scan.ScanDuration)
	})

	t.Run("existing recommendation scan values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
			DiskMonitoring: true,
			RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
				Enabled:             true,
				Schedule:            "0 1 * * *",
				BloatThreshold:      10,
				SkewThreshold:       25,
				AgeThreshold:        100000000,
				IndexBloatThreshold: 15,
				ScanDuration:        "1h",
			},
		}
		setStorageManagementDefaults(cluster)

		scan := cluster.Spec.Storage.RecommendationScan
		assert.Equal(t, "0 1 * * *", scan.Schedule)
		assert.Equal(t, int32(10), scan.BloatThreshold)
		assert.Equal(t, int32(25), scan.SkewThreshold)
		assert.Equal(t, int64(100000000), scan.AgeThreshold)
		assert.Equal(t, int32(15), scan.IndexBloatThreshold)
		assert.Equal(t, "1h", scan.ScanDuration)
	})

	t.Run("disabled recommendation scan does not get defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
			DiskMonitoring: true,
			RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
				Enabled: false,
			},
		}
		setStorageManagementDefaults(cluster)

		scan := cluster.Spec.Storage.RecommendationScan
		assert.Equal(t, "", scan.Schedule)
		assert.Equal(t, int32(0), scan.BloatThreshold)
	})
}

func TestSetQueryMonitoringDefaults(t *testing.T) {
	t.Run("nil query monitoring does nothing", func(t *testing.T) {
		cluster := newMinimalCluster()
		setQueryMonitoringDefaults(cluster)
		assert.Nil(t, cluster.Spec.QueryMonitoring)
	})

	t.Run("query monitoring gets defaults", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
			Enabled: true,
		}
		setQueryMonitoringDefaults(cluster)

		assert.Equal(t, "30d", cluster.Spec.QueryMonitoring.HistoryRetention)
		assert.Equal(t, int32(15), cluster.Spec.QueryMonitoring.SamplingInterval)
		assert.Equal(t, "1000ms", cluster.Spec.QueryMonitoring.SlowQueryThreshold)
	})

	t.Run("existing query monitoring values preserved", func(t *testing.T) {
		cluster := newMinimalCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
			Enabled:            true,
			HistoryRetention:   "90d",
			SamplingInterval:   5,
			SlowQueryThreshold: "500ms",
		}
		setQueryMonitoringDefaults(cluster)

		assert.Equal(t, "90d", cluster.Spec.QueryMonitoring.HistoryRetention)
		assert.Equal(t, int32(5), cluster.Spec.QueryMonitoring.SamplingInterval)
		assert.Equal(t, "500ms", cluster.Spec.QueryMonitoring.SlowQueryThreshold)
	})
}
