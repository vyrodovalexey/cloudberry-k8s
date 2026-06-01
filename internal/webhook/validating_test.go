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
			errContains: "backup.destination.s3.credentialSecret.name",
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
			name: "backup invalid compression type",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CompressionType: "lz4"}
			})),
			expectErr:   true,
			errContains: "backup.gpbackup.compressionType",
		},
		{
			name: "backup copyQueueSize without singleDataFile",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{CopyQueueSize: 4}
			})),
			expectErr:   true,
			errContains: "copyQueueSize",
		},
		{
			name: "backup jobs combined with singleDataFile",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{Jobs: 4, SingleDataFile: true}
			})),
			expectErr:   true,
			errContains: "jobs cannot be combined",
		},
		{
			name: "backup incremental without leafPartitionData",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true}
			})),
			expectErr:   true,
			errContains: "leafPartitionData",
		},
		{
			name: "backup incremental with leafPartitionData valid",
			cluster: backupCluster(validS3Backup(func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup = &cbv1alpha1.GpbackupOptions{Incremental: true, LeafPartitionData: true}
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
			err := validateBackup(tt.cluster)
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
			name: "data loading enabled with valid s3 job",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name:        "test-job",
							Type:        "s3",
							TargetTable: "public.data",
						},
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "data loading job missing name",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Type:        "s3",
							TargetTable: "public.data",
						},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].name",
		},
		{
			name: "data loading job missing type",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name:        "test-job",
							TargetTable: "public.data",
						},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].type",
		},
		{
			name: "data loading job missing target table",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name: "test-job",
							Type: "kafka",
						},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "dataLoading.jobs[0].targetTable",
		},
		{
			name: "data loading kafka job valid",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name:        "kafka-job",
							Type:        "kafka",
							TargetTable: "public.stream",
							KafkaSource: &cbv1alpha1.KafkaSourceSpec{
								Brokers: []string{"kafka:9092"},
								Topic:   "test-topic",
							},
						},
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "data loading rabbitmq job valid",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name:        "rmq-job",
							Type:        "rabbitmq",
							TargetTable: "public.queue_data",
							RabbitMQSource: &cbv1alpha1.RabbitMQSourceSpec{
								Host:  "rabbitmq",
								Queue: "data-queue",
							},
						},
					},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "data loading multiple jobs with one invalid",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs: []cbv1alpha1.DataLoadingJob{
						{
							Name:        "valid-job",
							Type:        "s3",
							TargetTable: "public.data",
						},
						{
							Name: "invalid-job",
							Type: "kafka",
							// missing TargetTable
						},
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "dataLoading.jobs[1].targetTable",
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
