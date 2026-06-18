package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	return scheme
}

func newValidCluster() *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Storage: cbv1alpha1.StorageSpec{Size: "10Gi"},
				Port:    5432,
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:   4,
				Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
			},
			DeletionPolicy: cbv1alpha1.DeletionPolicyRetain,
		},
	}
}

func TestNewCloudberryClusterValidator(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)
	require.NotNil(t, v)
}

func TestValidateCreate(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "valid cluster",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "invalid segment count",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Count = 0
				return c
			}(),
			expectErr:   true,
			errContains: "segments.count",
		},
		{
			name: "missing coordinator storage size",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Coordinator.Storage.Size = ""
				return c
			}(),
			expectErr:   true,
			errContains: "coordinator.storage.size",
		},
		{
			name: "missing segment storage size",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Storage.Size = ""
				return c
			}(),
			expectErr:   true,
			errContains: "segments.storage.size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewCloudberryClusterValidator(nil)
			warnings, err := v.ValidateCreate(context.Background(), tt.cluster)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
			_ = warnings
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	newCluster := newValidCluster()

	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	require.NoError(t, err)
	_ = warnings
}

func TestValidateDelete(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)
	warnings, err := v.ValidateDelete(context.Background(), newValidCluster())
	require.NoError(t, err)
	assert.Nil(t, warnings)
}

func TestValidateOIDC(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "no auth spec",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "oidc disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{Enabled: false},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "oidc enabled without issuer url",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:  true,
						ClientID: "client-id",
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "issuerURL",
		},
		{
			name: "oidc enabled without client id",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "clientID",
		},
		{
			name: "oidc enabled with all required fields",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
						ClientID:  "client-id",
					},
				}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOIDC(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateVault(t *testing.T) {
	tests := []struct {
		name      string
		cluster   *cbv1alpha1.CloudberryCluster
		expectErr bool
	}{
		{
			name:      "no vault spec",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "vault disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: false}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "vault enabled without address",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true, Address: ""}
				return c
			}(),
			expectErr: true,
		},
		{
			name: "vault enabled with address",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true, Address: "https://vault.example.com"}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVault(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateDeletionPolicy(t *testing.T) {
	tests := []struct {
		name      string
		policy    cbv1alpha1.DeletionPolicy
		expectErr bool
	}{
		{"empty policy", "", false},
		{"retain policy", cbv1alpha1.DeletionPolicyRetain, false},
		{"delete policy", cbv1alpha1.DeletionPolicyDelete, false},
		{"invalid policy", cbv1alpha1.DeletionPolicy("Invalid"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newValidCluster()
			c.Spec.DeletionPolicy = tt.policy
			err := validateDeletionPolicy(c)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateSegments_SpreadWarning(t *testing.T) {
	c := newValidCluster()
	c.Spec.Segments.Count = 2
	c.Spec.Segments.PrimariesPerHost = 2
	c.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutSpread,
	}

	var warnings admission.Warnings
	err := validateSegments(c, &warnings)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "spread mirroring")
}

// backupCluster returns a valid cluster with a backup spec mutated by the
// provided functions.
func backupCluster(mutators ...func(*cbv1alpha1.BackupSpec)) *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	b := &cbv1alpha1.BackupSpec{Enabled: true}
	for _, m := range mutators {
		m(b)
	}
	c.Spec.Backup = b
	return c
}

// validS3Backup returns a mutator chain producing a fully valid s3 backup spec,
// optionally further mutated by the supplied functions.
func validS3Backup(extra ...func(*cbv1alpha1.BackupSpec)) func(*cbv1alpha1.BackupSpec) {
	return func(b *cbv1alpha1.BackupSpec) {
		b.Image = "cloudberry-backup:2.1.0"
		b.Destination = cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "backup-s3-credentials"},
			},
		}
		for _, m := range extra {
			m(b)
		}
	}
}

func TestValidateBackup(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "backup disabled",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name:      "backup enabled with valid s3 destination",
			cluster:   backupCluster(validS3Backup()),
			expectErr: false,
		},
		{
			name: "backup enabled missing destination type",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{Type: ""}
			}),
			expectErr:   true,
			errContains: "backup.destination.type",
		},
		{
			name: "backup invalid destination type",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{Type: "gcs"}
			}),
			expectErr:   true,
			errContains: "backup.destination.type",
		},
		{
			name: "backup s3 missing bucket",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{Type: "s3"}
			}),
			expectErr:   true,
			errContains: "backup.destination.s3.bucket",
		},
		{
			name: "backup s3 missing credential secret",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type: "s3",
					S3:   &cbv1alpha1.S3Destination{Bucket: "my-bucket"},
				}
			}),
			expectErr:   true,
			errContains: "credentialSecret",
		},
		{
			name: "backup s3 missing credential secret mentions vaultSecret",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type: "s3",
					S3:   &cbv1alpha1.S3Destination{Bucket: "my-bucket"},
				}
			}),
			expectErr:   true,
			errContains: "vaultSecret",
		},
		{
			name: "backup s3 with vaultSecret and no credentialSecret valid",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type: "s3",
					S3: &cbv1alpha1.S3Destination{
						Bucket: "my-bucket",
						VaultSecret: &cbv1alpha1.S3VaultSecret{
							Path: "secret/data/cloudberry/backup-s3",
						},
					},
				}
			}),
			expectErr: false,
		},
		{
			name: "backup s3 with empty vaultSecret path and no credentialSecret rejected",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type: "s3",
					S3: &cbv1alpha1.S3Destination{
						Bucket:      "my-bucket",
						VaultSecret: &cbv1alpha1.S3VaultSecret{Path: ""},
					},
				}
			}),
			expectErr:   true,
			errContains: "credentialSecret",
		},
		{
			name: "backup s3 with credentialSecret and no vaultSecret valid",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type: "s3",
					S3: &cbv1alpha1.S3Destination{
						Bucket:           "my-bucket",
						CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "backup-s3-credentials"},
					},
				}
			}),
			expectErr: false,
		},
		{
			name: "backup invalid compression too high",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 10}
			})),
			expectErr:   true,
			errContains: "backup.gpbackup.compressionLevel",
		},
		{
			name: "backup invalid compression level zero",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 0}
			})),
			expectErr:   true,
			errContains: "backup.gpbackup.compressionLevel",
		},
		{
			name: "backup invalid compression type",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 6, CompressionType: "lz4"}
			})),
			expectErr:   true,
			errContains: "backup.gpbackup.compressionType",
		},
		{
			name: "backup copyQueueSize without singleDataFile",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 6, CopyQueueSize: 4}
			})),
			expectErr:   true,
			errContains: "copyQueueSize",
		},
		{
			name: "backup jobs combined with singleDataFile",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 6, Jobs: 4, SingleDataFile: true}
			})),
			expectErr:   true,
			errContains: "jobs cannot be combined",
		},
		{
			name: "backup incremental without leafPartitionData",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionLevel: 6, Incremental: true}
			})),
			expectErr:   true,
			errContains: "leafPartitionData",
		},
		{
			name: "backup incremental with leafPartitionData valid",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{
					CompressionLevel: 6, Incremental: true, LeafPartitionData: true,
				}
			})),
			expectErr: false,
		},
		{
			name: "backup invalid schedule",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Schedule = "not a cron"
			})),
			expectErr:   true,
			errContains: "schedule",
		},
		{
			name: "backup valid schedule",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Schedule = "0 2 * * *"
			})),
			expectErr: false,
		},
		{
			name: "backup missing image",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Destination = cbv1alpha1.BackupDestination{
					Type:  "local",
					Local: &cbv1alpha1.LocalDestination{Path: "/backups"},
				}
			}),
			expectErr:   true,
			errContains: "backup.image",
		},
		{
			name: "backup local destination valid",
			cluster: backupCluster(func(b *cbv1alpha1.BackupSpec) {
				b.Image = "cloudberry-backup:2.1.0"
				b.Destination = cbv1alpha1.BackupDestination{
					Type:  "local",
					Local: &cbv1alpha1.LocalDestination{Path: "/backups"},
				}
			}),
			expectErr: false,
		},
		{
			name: "backup gprestore data-only and metadata-only conflict",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gprestore = &cbv1alpha1.GprestoreOptions{DataOnly: true, MetadataOnly: true}
			})),
			expectErr:   true,
			errContains: "dataOnly and backup.gprestore.metadataOnly",
		},
		{
			name: "backup gprestore data-only only valid",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gprestore = &cbv1alpha1.GprestoreOptions{DataOnly: true}
			})),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var warnings admission.Warnings
			err := validateBackup(tt.cluster, &warnings)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// validDataLoadingSpec returns a fully-valid PXF DataLoadingSpec: PXF enabled
// with an image, three valid servers (s3, jdbc, hdfs), and two valid jobs (one
// pxf, one gpload). Tests mutate a copy of this baseline to exercise a single
// failing condition.
func validDataLoadingSpec() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake", Type: "s3",
					Config:            map[string]string{"fs.s3a.endpoint": "s3.amazonaws.com"},
					CredentialSecrets: []cbv1alpha1.SecretReference{{Name: "s3-creds"}},
				},
				{
					Name: "mysql-oltp", Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/db",
					},
				},
				{
					Name: "hadoop-cluster", Type: "hdfs",
					Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name: "s3-ingest", Type: "pxf", Schedule: "0 */6 * * *",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server: "s3-datalake", Profile: "s3:parquet", TargetTable: "public.events",
				},
			},
			{
				Name: "csv-load", Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{TargetTable: "public.raw_data"},
			},
		},
	}
}

// clusterWithDataLoading returns a valid cluster whose data loading spec is the
// valid baseline mutated by fn.
func clusterWithDataLoading(fn func(dl *cbv1alpha1.DataLoadingSpec)) *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	dl := validDataLoadingSpec()
	if fn != nil {
		fn(dl)
	}
	c.Spec.DataLoading = dl
	return c
}

func TestValidateDataLoading(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "data loading disabled",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name:      "valid baseline accepted",
			cluster:   clusterWithDataLoading(nil),
			expectErr: false,
		},
		{
			name: "disabled spec with bad content is a no-op",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Enabled = false
				dl.Pxf.Image = ""
			}),
			expectErr: false,
		},
		// W.1 — pxf.enabled with empty image.
		{
			name: "W.1 pxf enabled without image",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Image = ""
			}),
			expectErr:   true,
			errContains: "dataLoading.pxf.image",
		},
		{
			name: "W.1 pxf disabled without image accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Enabled = false
				dl.Pxf.Image = ""
			}),
			expectErr: false,
		},
		// W.2 — server name empty / duplicate.
		{
			name: "W.2 empty server name",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Name = ""
			}),
			expectErr:   true,
			errContains: "dataLoading.pxf.servers[0].name",
		},
		{
			name: "W.2 duplicate server name",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
			}),
			expectErr:   true,
			errContains: "duplicate",
		},
		// W.3 — server type not in enum.
		{
			name: "W.3 invalid server type",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Type = "ftp"
			}),
			expectErr:   true,
			errContains: "dataLoading.pxf.servers[0].type",
		},
		{
			name: "W.3 hbase and hive server types accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = []cbv1alpha1.PxfServerSpec{
					{Name: "hb", Type: "hbase"},
					{Name: "hv", Type: "hive"},
				}
				dl.Jobs[0].PxfJob.Server = "hb"
				dl.Jobs[0].PxfJob.Profile = "hbase"
			}),
			expectErr: false,
		},
		{
			// Scenario 96: gs/abfss/wasbs object-store server types accepted with
			// only fs.s3a.endpoint (no credentialSecrets — cloud-native auth).
			name: "W.3 gs/abfss/wasbs object-store server types accepted (endpoint only)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = []cbv1alpha1.PxfServerSpec{
					{Name: "gcs", Type: "gs", Config: map[string]string{
						"fs.s3a.endpoint": "storage.googleapis.com"}},
					{Name: "adls", Type: "abfss", Config: map[string]string{
						"fs.s3a.endpoint": "x.dfs.core.windows.net"}},
					{Name: "blob", Type: "wasbs", Config: map[string]string{
						"fs.s3a.endpoint": "x.blob.core.windows.net"}},
				}
				dl.Jobs[0].PxfJob.Server = "gcs"
				dl.Jobs[0].PxfJob.Profile = "gs:parquet"
			}),
			expectErr: false,
		},
		{
			// Scenario 96: an object-store server (gs) without fs.s3a.endpoint is
			// rejected by the shared object-store check (W.4).
			name: "W.4 gs object-store server missing endpoint",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = []cbv1alpha1.PxfServerSpec{
					{Name: "gcs", Type: "gs"},
				}
				dl.Jobs[0].PxfJob.Server = "gcs"
				dl.Jobs[0].PxfJob.Profile = "gs:text"
			}),
			expectErr:   true,
			errContains: "fs.s3a.endpoint",
		},
		// W.4 — s3 server missing endpoint or credentials.
		{
			name: "W.4 s3 server missing endpoint",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
			}),
			expectErr:   true,
			errContains: "fs.s3a.endpoint",
		},
		{
			name: "W.4 s3 server missing credentials",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].CredentialSecrets = nil
			}),
			expectErr:   true,
			errContains: "credentialSecrets",
		},
		// W.5 — jdbc server missing driver or url.
		{
			name: "W.5 jdbc server missing driver",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[1].Config, "jdbc.driver")
			}),
			expectErr:   true,
			errContains: "jdbc.driver",
		},
		{
			name: "W.5 jdbc server missing url",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[1].Config, "jdbc.url")
			}),
			expectErr:   true,
			errContains: "jdbc.url",
		},
		// W.6 — hdfs server missing defaultFS.
		{
			name: "W.6 hdfs server missing defaultFS",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[2].Config, "fs.defaultFS")
			}),
			expectErr:   true,
			errContains: "fs.defaultFS",
		},
		// W.7 — job name empty / duplicate.
		{
			name: "W.7 empty job name",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Name = ""
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].name",
		},
		{
			name: "W.7 duplicate job name",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].Name = dl.Jobs[0].Name
			}),
			expectErr:   true,
			errContains: "duplicate",
		},
		// W.8 — job type not in enum.
		{
			name: "W.8 invalid job type",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Type = "kafka"
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].type",
		},
		// W.9 — pxf job server not defined.
		{
			name: "W.9 pxf job unknown server",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "does-not-exist"
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].pxfJob.server",
		},
		{
			name: "W.9 pxfJob nil for pxf type",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob = nil
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].pxfJob",
		},
		// W.10 — pxf job invalid profile.
		{
			name: "W.10 invalid profile",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].pxfJob.profile",
		},
		{
			name: "W.10 unknown scheme rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "foo:bar"
			}),
			expectErr:   true,
			errContains: "profile",
		},
		// W.11 — pxf job missing target table.
		{
			name: "W.11 pxf job missing target table",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.TargetTable = ""
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].pxfJob.targetTable",
		},
		// W.12 — gpload job missing target table / nil.
		{
			name: "W.12 gpload job missing target table",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.TargetTable = ""
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[1].gploadJob.targetTable",
		},
		{
			name: "W.12 gpload job nil body",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob = nil
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[1].gploadJob.targetTable",
		},
		// W.13 — invalid cron schedule.
		{
			name: "W.13 invalid cron schedule",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Schedule = "not-a-cron"
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].schedule",
		},
		{
			name: "W.13 empty schedule accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Schedule = ""
			}),
			expectErr: false,
		},
		// W.14 — partitioning column without range/interval.
		{
			name: "W.14 partitioning column without range",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning = &cbv1alpha1.PartitioningSpec{
					Column: "order_date", Interval: "1:month",
				}
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].pxfJob.partitioning",
		},
		{
			name: "W.14 partitioning column without interval",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning = &cbv1alpha1.PartitioningSpec{
					Column: "order_date", Range: "2024:2026",
				}
			}),
			expectErr:   true,
			errContains: "partitioning",
		},
		{
			name: "W.14 full partitioning triple accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning = &cbv1alpha1.PartitioningSpec{
					Column: "order_date", Range: "2024:2026", Interval: "1:month",
				}
			}),
			expectErr: false,
		},
		{
			name: "W.14 empty partitioning accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning = &cbv1alpha1.PartitioningSpec{}
			}),
			expectErr: false,
		},
		// W.15 — invalid segmentRejectLimitType.
		{
			name: "W.15 invalid segmentRejectLimitType",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "bytes",
				}
			}),
			expectErr:   true,
			errContains: "segmentRejectLimitType",
		},
		{
			name: "W.15 percent segmentRejectLimitType accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 5, SegmentRejectLimitType: "percent",
				}
			}),
			expectErr: false,
		},
		{
			// W.15 (Scenario 98, FE.12) — "rows" is an accepted reject-limit type.
			name: "W.15 rows segmentRejectLimitType accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "rows",
				}
			}),
			expectErr: false,
		},
		{
			// W.15 (Scenario 98, FE.12) — an EMPTY segmentRejectLimitType is
			// allowed (the builder defaults the unit to ROWS); only a non-empty
			// non-{rows,percent} value is rejected.
			name: "W.15 empty segmentRejectLimitType accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "",
				}
			}),
			expectErr: false,
		},
		{
			// W.15 (Scenario 98, FE.12) — a garbage non-empty type is rejected
			// (parity with the "bytes" case at a different value).
			name: "W.15 garbage segmentRejectLimitType rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "kilobytes",
				}
			}),
			expectErr:   true,
			errContains: "segmentRejectLimitType",
		},
		// W.16 — gpload job filePaths file:// scheme rejected for multi-segment loads.
		{
			name: "W.16 gpload file:// scheme rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{"file:///data-lake/dataset.csv"}
			}),
			expectErr: true,
			errContains: "dataLoading.jobs[1].gploadJob.filePaths[0]: file:// scheme is not " +
				"supported for multi-segment loads; use gpfdist:// or s3://",
		},
		{
			name: "W.16 gpload file:// scheme rejected at non-zero index",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{
					"/data/incoming/a.csv", "file:///data-lake/b.csv",
				}
			}),
			expectErr:   true,
			errContains: "dataLoading.jobs[1].gploadJob.filePaths[1]: file://",
		},
		{
			name: "W.16 gpload file:// with surrounding whitespace rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{"  file:///data-lake/dataset.csv"}
			}),
			expectErr:   true,
			errContains: "file:// scheme is not supported",
		},
		{
			name: "W.16 gpload gpfdist:// scheme accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{"gpfdist://host:8080/a.csv"}
			}),
			expectErr: false,
		},
		{
			name: "W.16 gpload s3:// scheme accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{"s3://bucket/prefix/ config=/cfg/s3.conf"}
			}),
			expectErr: false,
		},
		{
			name: "W.16 gpload bare path accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.FilePaths = []string{"/data/incoming/*.csv"}
			}),
			expectErr: false,
		},
		// W.10b (Scenario 96) — write-capability matrix (FF.1-FF.5). A WRITABLE
		// external table (mode=writable) is admitted only for a writable format
		// (text/parquet/avro); json/orc are write-unsupported and DENIED with an
		// error containing "write-unsupported" and "writable". The predicate is
		// driven by pxfpolicy.IsProfileWritable so it applies uniformly to every
		// object-store scheme (s3/gs/abfss/wasbs).
		{
			name: "FF.1 s3:text writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.2 s3:parquet writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:parquet"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.3 s3:avro writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:avro"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.4 s3:json writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:json"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			name: "FF.5 s3:orc writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:orc"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			// The DENY message must also name the "writable" mode so operators
			// understand WHY the format was rejected (read of json/orc is fine).
			name: "FF.4 s3:json writable deny message names writable",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:json"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "writable",
		},
		{
			// Mode is matched case-insensitively (EqualFold) per validatePxfJob.
			name: "FF.5 ORC WRITABLE uppercase mode still denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:orc"
				dl.Jobs[0].PxfJob.Mode = "WRITABLE"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		// Cross-scheme parity: the same predicate governs gs/abfss/wasbs.
		{
			name: "gs:json writable denied (scheme parity)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Type = "s3"
				dl.Jobs[0].PxfJob.Profile = "gs:json"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			name: "gs:parquet writable admitted (scheme parity)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "gs:parquet"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		// Read-mode parity: a read/import of json (Mode unset or "insert") is
		// allowed — only a WRITABLE json/orc export is rejected.
		{
			name: "read mode s3:json admitted (mode unset)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:json"
				dl.Jobs[0].PxfJob.Mode = ""
			}),
			expectErr: false,
		},
		{
			name: "insert mode s3:json admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:json"
				dl.Jobs[0].PxfJob.Mode = "insert"
			}),
			expectErr: false,
		},
		{
			name: "read mode s3:orc admitted (mode unset)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:orc"
				dl.Jobs[0].PxfJob.Mode = ""
			}),
			expectErr: false,
		},
		// ----------------------------------------------------------------------
		// Scenario 97 — Hadoop write-capability matrix (W.10b / FF.6/FF.7/WRej.*).
		// The baseline already defines an hdfs server "hadoop-cluster"
		// (Servers[2], with fs.defaultFS). The hive/hbase cases append a typed
		// server (no required config keys per W.4/W.6) and point Jobs[0] at it.
		// The DENY error contains both "write-unsupported" and "writable" (W.10b),
		// mirroring the object-store FF.* assertion pattern above.
		// ----------------------------------------------------------------------
		// FF.7 + companions: writable HDFS writable-format exports ADMIT.
		{
			name: "FF.7 hdfs:sequencefile writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:SequenceFile"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.7t hdfs:text writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.7p hdfs:parquet writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:parquet"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		{
			name: "FF.7a hdfs:avro writable admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:avro"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr: false,
		},
		// WRej.1/WRej.2: writable HDFS read-only formats DENY.
		{
			name: "WRej.1 hdfs:json writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:json"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			name: "WRej.2 hdfs:orc writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:orc"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			// WRej.2 DENY message also names the "writable" mode.
			name: "WRej.2 hdfs:orc writable deny message names writable",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:orc"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "writable",
		},
		// WRej.3-6: every hive profile is read-only — writable DENY.
		{
			name: "WRej.3 hive (bare) writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hive-warehouse", Type: "hive"})
				dl.Jobs[0].PxfJob.Server = "hive-warehouse"
				dl.Jobs[0].PxfJob.Profile = "hive"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			// WRej.4: hive:text is write-unsupported. Hive is a read-only SCHEME
			// (Write=No regardless of format), so a writable hive:text export is
			// DENIED even though "text" is a writable format on hdfs/object
			// stores. The DENY is driven by pxfpolicy.IsProfileWritable, which is
			// scheme-aware: the Hive scheme overrides the format check.
			name: "WRej.4 hive:text writable denied (read-only scheme)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hive-warehouse", Type: "hive"})
				dl.Jobs[0].PxfJob.Server = "hive-warehouse"
				dl.Jobs[0].PxfJob.Profile = "hive:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			name: "WRej.5 hive:orc writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hive-warehouse", Type: "hive"})
				dl.Jobs[0].PxfJob.Server = "hive-warehouse"
				dl.Jobs[0].PxfJob.Profile = "hive:orc"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		{
			name: "WRej.6/FF.6b hive:rc writable denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hive-warehouse", Type: "hive"})
				dl.Jobs[0].PxfJob.Server = "hive-warehouse"
				dl.Jobs[0].PxfJob.Profile = "hive:rc"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		// WRej.7: bare HBase (case-insensitive) is read-only — writable DENY.
		{
			name: "WRej.7 HBase writable denied (case-insensitive)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hbase-store", Type: "hbase"})
				dl.Jobs[0].PxfJob.Server = "hbase-store"
				dl.Jobs[0].PxfJob.Profile = "HBase"
				dl.Jobs[0].PxfJob.Mode = "writable"
			}),
			expectErr:   true,
			errContains: "write-unsupported",
		},
		// Read-mode parity (HP.5/HV.1-4/HB.1): a READ of any admitted Hadoop
		// profile (mode unset) is allowed — only the WRITABLE leg is gated.
		{
			name: "HV.1 hive (bare) read admitted (auto-detect, caveat C1)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hive-warehouse", Type: "hive"})
				dl.Jobs[0].PxfJob.Server = "hive-warehouse"
				dl.Jobs[0].PxfJob.Profile = "hive"
				dl.Jobs[0].PxfJob.Mode = ""
			}),
			expectErr: false,
		},
		{
			name: "WRej.2 read mode hdfs:orc admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "hadoop-cluster"
				dl.Jobs[0].PxfJob.Profile = "hdfs:orc"
				dl.Jobs[0].PxfJob.Mode = ""
			}),
			expectErr: false,
		},
		{
			name: "HB.1 HBase read admitted (case-insensitive)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = append(dl.Pxf.Servers,
					cbv1alpha1.PxfServerSpec{Name: "hbase-store", Type: "hbase"})
				dl.Jobs[0].PxfJob.Server = "hbase-store"
				dl.Jobs[0].PxfJob.Profile = "HBase"
				dl.Jobs[0].PxfJob.Mode = ""
			}),
			expectErr: false,
		},
		// ----------------------------------------------------------------------
		// W.17 (Scenario 99) — sourceFilter validation. SF.1/SF.2: the optional
		// writable-export WHERE predicate is only valid on a writable export and
		// must not smuggle a stacked query / SQL comment.
		//   W.17(a) MODE GATE: sourceFilter set on a non-writable (read/import)
		//           job → DENY (error names "sourceFilter" and "writable").
		//   W.17(b) SANITY CHECK: a writable job whose sourceFilter contains ';',
		//           '--', or '/*' → DENY ("statement terminators or SQL comments").
		//   A clean predicate on a writable job ADMITS; empty sourceFilter ADMITS
		//   on any mode.
		// ----------------------------------------------------------------------
		// W.17(a) — SF.2: sourceFilter on a read/import job is rejected.
		{
			name: "W.17a SF.2 sourceFilter on read job (mode unset) denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = ""
				dl.Jobs[0].PxfJob.SourceFilter = "region='us-east'"
			}),
			expectErr:   true,
			errContains: "sourceFilter",
		},
		{
			// The W.17(a) DENY message also names the "writable" mode so the
			// operator understands WHY the read-job sourceFilter was rejected.
			name: "W.17a SF.2 read-job sourceFilter deny message names writable",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = ""
				dl.Jobs[0].PxfJob.SourceFilter = "region='us-east'"
			}),
			expectErr:   true,
			errContains: "writable",
		},
		{
			name: "W.17a sourceFilter on insert-mode job denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "insert"
				dl.Jobs[0].PxfJob.SourceFilter = "category = 'A'"
			}),
			expectErr:   true,
			errContains: "sourceFilter",
		},
		// W.17(a) PASS — SF.1: a clean predicate on a writable export ADMITS.
		{
			name: "W.17a SF.1 clean sourceFilter on writable export admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = "region='us-east'"
			}),
			expectErr: false,
		},
		{
			// Mode is matched case-insensitively (EqualFold): a clean predicate
			// on an uppercase WRITABLE export still admits.
			name: "W.17a clean sourceFilter on uppercase WRITABLE export admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:parquet"
				dl.Jobs[0].PxfJob.Mode = "WRITABLE"
				dl.Jobs[0].PxfJob.SourceFilter = "category = 'A'"
			}),
			expectErr: false,
		},
		{
			// Empty sourceFilter admits on a writable export (the full-table
			// export, unchanged behavior).
			name: "W.17 empty sourceFilter on writable export admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = ""
			}),
			expectErr: false,
		},
		{
			// Empty sourceFilter on a read/import job admits — the mode gate only
			// fires when sourceFilter is actually set.
			name: "W.17 empty sourceFilter on read job admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = ""
				dl.Jobs[0].PxfJob.SourceFilter = ""
			}),
			expectErr: false,
		},
		// W.17(b) — SANITY CHECK: statement terminator / SQL comments rejected.
		{
			name: "W.17b SF.2b sourceFilter with semicolon denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = "1=1; DROP TABLE x"
			}),
			expectErr:   true,
			errContains: "statement terminators or SQL comments",
		},
		{
			name: "W.17b sourceFilter with SQL line comment denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = "region='us-east' -- drop"
			}),
			expectErr:   true,
			errContains: "statement terminators or SQL comments",
		},
		{
			name: "W.17b sourceFilter with SQL block comment denied",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = "region='us-east' /* c */"
			}),
			expectErr:   true,
			errContains: "statement terminators or SQL comments",
		},
		{
			// A clean predicate (no forbidden substrings, single quotes are fine)
			// passes the W.17(b) sanity check on a writable export.
			name: "W.17b clean predicate with single quotes admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:text"
				dl.Jobs[0].PxfJob.Mode = "writable"
				dl.Jobs[0].PxfJob.SourceFilter = "created_at > '2026-01-01' AND region='us-east'"
			}),
			expectErr: false,
		},

		// W.18 (Scenario 101) — gploadJob.inputSource.type enum gpfdist|local.
		// Jobs[1] is the gpload job in validDataLoadingSpec().
		{
			name: "W.18 inputSource.type gpfdist accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"}
			}),
			expectErr: false,
		},
		{
			name: "W.18 inputSource.type local accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "local"}
			}),
			expectErr: false,
		},
		{
			name: "W.18 inputSource.type ftp rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "ftp"}
			}),
			expectErr:   true,
			errContains: "inputSource.type must be \"gpfdist\" or \"local\"",
		},

		// W.19 (Scenario 101) — gploadJob.delimiter single character.
		{
			name: "W.19 single-char delimiter accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Delimiter = ","
			}),
			expectErr: false,
		},
		{
			name: "W.19 multi-char delimiter rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Delimiter = "||"
			}),
			expectErr:   true,
			errContains: "delimiter must be a single character",
		},

		// W.20 (Scenario 101) — mode update/merge requires matchColumns.
		{
			name: "W.20 mode update with matchColumns accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Mode = "update"
				dl.Jobs[1].GploadJob.MatchColumns = []string{"id"}
			}),
			expectErr: false,
		},
		{
			name: "W.20 mode update without matchColumns rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Mode = "update"
				dl.Jobs[1].GploadJob.MatchColumns = nil
			}),
			expectErr:   true,
			errContains: "requires gploadJob.matchColumns",
		},
		{
			name: "W.20 mode merge with matchColumns accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Mode = "merge"
				dl.Jobs[1].GploadJob.MatchColumns = []string{"id", "tenant"}
			}),
			expectErr: false,
		},
		{
			name: "W.20 mode merge without matchColumns rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.Mode = "merge"
			}),
			expectErr:   true,
			errContains: "requires gploadJob.matchColumns",
		},

		// W.21 (Scenario 101) — postActions[] SQL sanity (no ; / -- / /* / */).
		{
			name: "W.21 clean postAction accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.PostActions = []string{"ANALYZE public.raw_data"}
			}),
			expectErr: false,
		},
		{
			name: "W.21 postAction with semicolon rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.PostActions = []string{"DROP TABLE x; DROP TABLE y"}
			}),
			expectErr:   true,
			errContains: "postActions[0] contains a forbidden SQL fragment",
		},
		{
			name: "W.21 postAction with SQL comment rejected at index",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.PostActions = []string{
					"ANALYZE public.raw_data", "VACUUM -- sneaky",
				}
			}),
			expectErr:   true,
			errContains: "postActions[1] contains a forbidden SQL fragment",
		},

		// W.22 (Scenario 101) — host/port only valid for type gpfdist.
		{
			name: "W.22 host/port on local source rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{
					Type: "local", Host: "files.internal",
				}
			}),
			expectErr:   true,
			errContains: "inputSource.host/port are only valid for type gpfdist",
		},
		{
			name: "W.22 port on local source rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{
					Type: "local", Port: 8080,
				}
			}),
			expectErr:   true,
			errContains: "inputSource.host/port are only valid for type gpfdist",
		},
		{
			name: "W.22 host+port on gpfdist source accepted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{
					Type: "gpfdist", Host: "files.internal", Port: 8080,
				}
			}),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDataLoading(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestIsValidPxfProfile exercises the W.10 profile allowlist policy directly to
// cover the scheme/format branches.
func TestIsValidPxfProfile(t *testing.T) {
	valid := []string{
		"s3:text", "s3:parquet", "s3:avro", "s3:json", "s3:orc",
		"gs:parquet", "abfss:orc", "wasbs:text",
		// Scenario 97 (W.10): every Hadoop profile is a VALID profile and is
		// ADMITTED at W.10 regardless of write-capability (the writable rejection
		// is a SEPARATE W.10b check). hdfs:{json,orc} are valid read profiles;
		// hdfs:SequenceFile (HP.6/FF.7) and the avro variant are valid; bare hive
		// (auto-detect, caveat C1), hive:rc (FF.6a/HV.4) and bare HBase (HB.1,
		// case-insensitive) all admit at W.10.
		"hdfs:text", "hdfs:parquet", "hdfs:avro",
		"hdfs:json", "hdfs:orc", "hdfs:SequenceFile",
		"hive", "hive:text", "hive:orc", "hive:rc",
		"jdbc", "HBase", "hbase",
	}
	for _, p := range valid {
		assert.True(t, isValidPxfProfile(p), "expected %q to be valid", p)
	}
	invalid := []string{
		"", "s3", "s3:nonsense", "foo:bar", "jdbc:x", "hbase:x", "hdfs",
	}
	for _, p := range invalid {
		assert.False(t, isValidPxfProfile(p), "expected %q to be invalid", p)
	}
}

func TestValidateWorkload(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "workload disabled",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "workload enabled with valid config",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
						{Name: "analytics", Concurrency: 10},
					},
					Rules: []cbv1alpha1.WorkloadRule{
						{Name: "cancel-long", Action: "cancel"},
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "workload resource group missing name",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
						{Concurrency: 10},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.resourceGroups[0].name",
		},
		{
			name: "workload rule missing name",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					Rules: []cbv1alpha1.WorkloadRule{
						{Action: "cancel"},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.rules[0].name",
		},
		{
			name: "workload rule missing action",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					Rules: []cbv1alpha1.WorkloadRule{
						{Name: "my-rule"},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.rules[0].action",
		},
		{
			name: "workload idle rule missing resource group",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					IdleRules: []cbv1alpha1.IdleSessionRule{
						{Name: "idle-rule", IdleTimeout: "30m"},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.idleRules[0].resourceGroup",
		},
		{
			name: "workload idle rule missing timeout",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					IdleRules: []cbv1alpha1.IdleSessionRule{
						{Name: "idle-rule", ResourceGroup: "analytics"},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.idleRules[0].idleTimeout",
		},
		{
			name: "workload idle rule missing name",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					IdleRules: []cbv1alpha1.IdleSessionRule{
						{ResourceGroup: "analytics", IdleTimeout: "30m"},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "workload.idleRules[0].name",
		},
		{
			name: "workload all rule actions valid",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled: true,
					Rules: []cbv1alpha1.WorkloadRule{
						{Name: "cancel-rule", Action: "cancel"},
						{Name: "move-rule", Action: "move", MoveTarget: "etl"},
						{Name: "log-rule", Action: "log"},
					},
				}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkload(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateQueryMonitoring(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "query monitoring disabled",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "query monitoring enabled with valid config",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:            true,
					HistoryRetention:   "30d",
					SamplingInterval:   5,
					SlowQueryThreshold: "1000ms",
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "query monitoring negative sampling interval",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:          true,
					SamplingInterval: -1,
				}
				return c
			}(),
			expectErr:   true,
			errContains: "samplingInterval",
		},
		{
			name: "query monitoring with guest access enabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:     true,
					GuestAccess: true,
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "query monitoring with plan collection enabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:        true,
					PlanCollection: true,
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "query monitoring zero sampling interval is valid",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:          true,
					SamplingInterval: 0,
				}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryMonitoring(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateStorageManagement(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "no storage spec",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "storage with disk monitoring only",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					DiskMonitoring: true,
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "recommendation scan disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled: false,
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "recommendation scan with valid thresholds",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					DiskMonitoring: true,
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:             true,
						Schedule:            "0 3 * * 0",
						BloatThreshold:      20,
						SkewThreshold:       50,
						AgeThreshold:        500000000,
						IndexBloatThreshold: 30,
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "recommendation scan bloat threshold too high",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:        true,
						BloatThreshold: 101,
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "bloatThreshold",
		},
		{
			name: "recommendation scan skew threshold negative",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:       true,
						SkewThreshold: -1,
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "skewThreshold",
		},
		{
			name: "recommendation scan index bloat threshold too high",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:             true,
						IndexBloatThreshold: 150,
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "indexBloatThreshold",
		},
		{
			name: "recommendation scan negative age threshold",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:      true,
						AgeThreshold: -1,
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "ageThreshold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorageManagement(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateStorage(t *testing.T) {
	tests := []struct {
		name        string
		coordSize   string
		segSize     string
		expectErr   bool
		errContains string
	}{
		{"valid sizes", "10Gi", "20Gi", false, ""},
		{"missing coordinator size", "", "20Gi", true, "coordinator.storage.size"},
		{"missing segment size", "10Gi", "", true, "segments.storage.size"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newValidCluster()
			c.Spec.Coordinator.Storage.Size = tt.coordSize
			c.Spec.Segments.Storage.Size = tt.segSize
			err := validateStorage(c)
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateCreate_DuplicateNameDetection(t *testing.T) {
	scheme := newTestScheme()

	t.Run("no duplicate", func(t *testing.T) {
		existing := newValidCluster()
		existing.Name = "existing-cluster"
		existing.Namespace = "ns-a"

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(existing).
			Build()

		v := NewCloudberryClusterValidator(k8sClient)
		newCluster := newValidCluster()
		newCluster.Name = "new-cluster"
		newCluster.Namespace = "ns-b"

		_, err := v.ValidateCreate(context.Background(), newCluster)
		require.NoError(t, err)
	})

	t.Run("duplicate name in different namespace", func(t *testing.T) {
		existing := newValidCluster()
		existing.Name = "test-cluster"
		existing.Namespace = "ns-a"

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(existing).
			Build()

		v := NewCloudberryClusterValidator(k8sClient)
		newCluster := newValidCluster()
		newCluster.Name = "test-cluster"
		newCluster.Namespace = "ns-b"

		_, err := v.ValidateCreate(context.Background(), newCluster)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists in namespace")
		assert.Contains(t, err.Error(), "ns-a")
	})

	t.Run("same name same namespace is allowed", func(t *testing.T) {
		// Same namespace means it's the same resource (update, not create of duplicate).
		existing := newValidCluster()
		existing.Name = "test-cluster"
		existing.Namespace = "default"

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(existing).
			Build()

		v := NewCloudberryClusterValidator(k8sClient)
		newCluster := newValidCluster()
		newCluster.Name = "test-cluster"
		newCluster.Namespace = "default"

		_, err := v.ValidateCreate(context.Background(), newCluster)
		require.NoError(t, err)
	})

	t.Run("nil reader skips duplicate check", func(t *testing.T) {
		v := NewCloudberryClusterValidator(nil)
		newCluster := newValidCluster()

		_, err := v.ValidateCreate(context.Background(), newCluster)
		require.NoError(t, err)
	})
}

// ============================================================================
// Mirroring Transition Validation Tests
// ============================================================================

func TestValidateUpdate_MirroringEnable_Running_Allowed(t *testing.T) {
	// Arrange: Old cluster is Running, new cluster enables mirroring with sufficient segments.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 4
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	// Act
	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.NoError(t, err)
	_ = warnings
}

func TestValidateUpdate_MirroringEnable_Stopped_Rejected(t *testing.T) {
	// Arrange: Old cluster is Stopped, new cluster enables mirroring.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 4
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	// Act
	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Running phase")
}

func TestValidateUpdate_MirroringEnable_InsufficientSegments_Rejected(t *testing.T) {
	// Arrange: Old cluster is Running, new cluster enables mirroring with too few segments.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 1 // Too few for group layout.
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	// Act
	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot enable group mirroring")
}

func TestValidateUpdate_MirroringDisable_Allowed(t *testing.T) {
	// Arrange: Old cluster has mirroring enabled, new cluster disables it.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	oldCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	newCluster := newValidCluster()
	// Mirroring disabled (nil).

	// Act
	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.NoError(t, err)
	_ = warnings
}

func TestValidateUpdate_MirroringLayoutChange_Rejected(t *testing.T) {
	// Arrange: Both old and new have mirroring enabled but different layouts.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutSpread,
	}

	// Act
	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot change mirroring layout")
}

func TestValidateUpdate_NoMirroringChange_NoValidation(t *testing.T) {
	// Arrange: Both old and new have mirroring disabled.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	newCluster := newValidCluster()

	// Act
	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.NoError(t, err)
	_ = warnings
}

func TestValidateUpdate_MirroringLayoutChange_SameLayout_Allowed(t *testing.T) {
	// Arrange: Both old and new have mirroring enabled with same layout.
	v := NewCloudberryClusterValidator(nil)
	oldCluster := newValidCluster()
	oldCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	// Act
	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)

	// Assert
	require.NoError(t, err)
	_ = warnings
}

func TestValidateNodeCountForMirroring_Group_Sufficient(t *testing.T) {
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutGroup, 4, 2)
	require.NoError(t, err)
}

func TestValidateNodeCountForMirroring_Group_Insufficient(t *testing.T) {
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutGroup, 2, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot enable group mirroring")
}

func TestValidateNodeCountForMirroring_Spread_Sufficient(t *testing.T) {
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutSpread, 3, 2)
	require.NoError(t, err)
}

func TestValidateNodeCountForMirroring_Spread_Insufficient(t *testing.T) {
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutSpread, 2, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot enable spread mirroring")
}

func TestValidateNodeCountForMirroring_Group_ExactMinimum(t *testing.T) {
	// Exact minimum: 2 * primariesPerHost = 4.
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutGroup, 4, 2)
	require.NoError(t, err)
}

func TestValidateNodeCountForMirroring_Spread_ExactBoundary(t *testing.T) {
	// Spread requires > primariesPerHost, so equal is insufficient.
	err := validateNodeCountForMirroring(cbv1alpha1.MirroringLayoutSpread, 2, 2)
	require.Error(t, err)
}

func TestValidateMirroringEnable_DefaultLayout(t *testing.T) {
	// Arrange: Enable mirroring with empty layout (should default to group).
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 4
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  "", // Empty layout defaults to group.
	}

	// Act
	warnings, err := validateMirroringEnable(oldCluster, newCluster)

	// Assert
	require.NoError(t, err)
	_ = warnings
}

func TestValidateMirroringEnable_SpreadWithMarginalCount(t *testing.T) {
	// Arrange: Enable spread mirroring with marginal segment count.
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 3
	newCluster.Spec.Segments.PrimariesPerHost = 2
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutSpread,
	}

	// Act
	warnings, err := validateMirroringEnable(oldCluster, newCluster)

	// Assert: Should succeed but with warning.
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "marginal segment count")
}

func TestValidateMirroringEnable_ScalingPhase_Rejected(t *testing.T) {
	// Arrange: Old cluster is in Scaling phase.
	oldCluster := newValidCluster()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 4
	newCluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}

	// Act
	_, err := validateMirroringEnable(oldCluster, newCluster)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Running phase")
}

func TestIsMirroringEnabled(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *cbv1alpha1.CloudberryCluster
		expected bool
	}{
		{
			name:     "nil mirroring spec",
			cluster:  newValidCluster(),
			expected: false,
		},
		{
			name: "mirroring disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}
				return c
			}(),
			expected: false,
		},
		{
			name: "mirroring enabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
				return c
			}(),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isMirroringEnabled(tt.cluster))
		})
	}
}
