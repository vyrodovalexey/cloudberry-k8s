//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 69: Webhook backup validation negative tests (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 69 negative cases at the e2e
// layer by exercising the validator directly (infra-free, deterministic). When
// a live cluster is available (KUBECONFIG set), the optional live test ALSO
// submits each invalid CR to the real API server through a controller-runtime
// client and asserts the create is rejected (apierrors) and a subsequent Get
// returns NotFound — proving the object was never persisted. The live test is
// skipped when no cluster/KUBECONFIG is available, consistent with the other
// live-gated e2e tests (e.g. Scenario 68 MinIO journey).
// ============================================================================

// envKubeconfig gates the live API-server rejection test.
const envKubeconfig = "KUBECONFIG"

// Scenario69WebhookValidationE2ESuite tests the backup webhook validation
// negative rules end-to-end.
type Scenario69WebhookValidationE2ESuite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario69(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario69WebhookValidationE2ESuite))
}

func (s *Scenario69WebhookValidationE2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario69E2EValidBackup returns a fully valid backup spec used as the basis
// for negative cases.
func scenario69E2EValidBackup() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "cloudberry-backups",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "backup-s3-credentials"},
			},
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
		},
	}
}

// scenario69E2ECluster builds a valid cluster with a valid backup spec and then
// applies the mutator to introduce one offending field.
func scenario69E2ECluster(
	name string, mutate func(*cbv1alpha1.BackupSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	backup := scenario69E2EValidBackup()
	if mutate != nil {
		mutate(backup)
	}
	cluster.Spec.Backup = backup
	return cluster
}

// scenario69Case is a single negative rule under test.
type scenario69Case struct {
	id      string
	name    string
	mutate  func(*cbv1alpha1.BackupSpec)
	substrs []string
}

// scenario69Cases returns the 10 negative rules 69a..69j.
func scenario69Cases() []scenario69Case {
	return []scenario69Case{
		{
			id: "69a", name: "missing destination type",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Destination.Type = "" },
			substrs: []string{"destination.type"},
		},
		{
			id: "69b", name: "missing s3 bucket",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Destination.S3.Bucket = "" },
			substrs: []string{"bucket"},
		},
		{
			id: "69c", name: "no credentialSecret and no vaultSecret",
			mutate: func(b *cbv1alpha1.BackupSpec) {
				b.Destination.S3.CredentialSecret = nil
				b.Destination.S3.VaultSecret = nil
			},
			substrs: []string{"credentialSecret", "vaultSecret"},
		},
		{
			id: "69d", name: "compressionLevel too high",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Gpbackup.CompressionLevel = 10 },
			substrs: []string{"compressionLevel"},
		},
		{
			id: "69d", name: "compressionLevel zero",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Gpbackup.CompressionLevel = 0 },
			substrs: []string{"compressionLevel"},
		},
		{
			id: "69e", name: "invalid compression type",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Gpbackup.CompressionType = "lz4" },
			substrs: []string{"compressionType"},
		},
		{
			id: "69f", name: "copyQueueSize without singleDataFile",
			mutate: func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup.CopyQueueSize = 4
				b.Gpbackup.SingleDataFile = false
			},
			substrs: []string{"copyQueueSize"},
		},
		{
			id: "69g", name: "jobs combined with singleDataFile",
			mutate: func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup.Jobs = 4
				b.Gpbackup.SingleDataFile = true
			},
			substrs: []string{"jobs cannot be combined"},
		},
		{
			id: "69h", name: "incremental without leafPartitionData",
			mutate: func(b *cbv1alpha1.BackupSpec) {
				b.Gpbackup.Incremental = true
				b.Gpbackup.LeafPartitionData = false
			},
			substrs: []string{"leafPartitionData"},
		},
		{
			id: "69i", name: "invalid cron schedule",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Schedule = "not a cron" },
			substrs: []string{"cron"},
		},
		{
			id: "69j", name: "missing image",
			mutate:  func(b *cbv1alpha1.BackupSpec) { b.Image = "" },
			substrs: []string{"backup.image"},
		},
	}
}

// TestE2E_Scenario69_ValidatorRejectsInvalidCRs exercises the validator directly
// for all 10 negative rules (parity with the functional suite).
func (s *Scenario69WebhookValidationE2ESuite) TestE2E_Scenario69_ValidatorRejectsInvalidCRs() {
	for _, tc := range scenario69Cases() {
		s.Run(tc.id+"_"+tc.name, func() {
			cluster := scenario69E2ECluster("e2e-s69-"+tc.id, tc.mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Error(s.T(), err)
			for _, substr := range tc.substrs {
				assert.Contains(s.T(), err.Error(), substr)
			}
		})
	}
}

// TestE2E_Scenario69_ValidBackupAccepted verifies the valid backup spec passes,
// and that the Vault-sourced credential alternative (69c) is accepted.
func (s *Scenario69WebhookValidationE2ESuite) TestE2E_Scenario69_ValidBackupAccepted() {
	valid := scenario69E2ECluster("e2e-s69-valid", nil)
	_, err := s.validator.ValidateCreate(s.ctx, valid)
	require.NoError(s.T(), err)

	vault := scenario69E2ECluster("e2e-s69-vault", func(b *cbv1alpha1.BackupSpec) {
		b.Destination.S3.CredentialSecret = nil
		b.Destination.S3.VaultSecret = &cbv1alpha1.S3VaultSecret{
			Path: "secret/data/cloudberry/backup-s3",
		}
	})
	_, err = s.validator.ValidateCreate(s.ctx, vault)
	require.NoError(s.T(), err)
}

// TestE2E_Scenario69_LiveAPIServerRejection submits each invalid CR to a live
// API server (via KUBECONFIG) and asserts the create is rejected and the object
// is not persisted. Skipped when no cluster/KUBECONFIG is available; the
// kubectl-apply rejection is otherwise verified at deploy time.
func (s *Scenario69WebhookValidationE2ESuite) TestE2E_Scenario69_LiveAPIServerRejection() {
	kubeconfig := os.Getenv(envKubeconfig)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live API-server rejection test")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		s.T().Skipf("could not build kubeconfig %q: %v", kubeconfig, err)
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Skipf("could not build live client: %v", err)
	}

	const ns = "default"
	for _, tc := range scenario69Cases() {
		s.Run("live_"+tc.id+"_"+tc.name, func() {
			cluster := scenario69E2ECluster("live-s69-"+tc.id, tc.mutate)
			cluster.Namespace = ns

			createErr := cl.Create(s.ctx, cluster)
			require.Error(s.T(), createErr, "live API server should reject invalid CR")
			assert.True(s.T(), apierrors.IsInvalid(createErr) ||
				apierrors.IsBadRequest(createErr) || apierrors.IsForbidden(createErr),
				"reject reason should be admission-related, got %v", createErr)

			// Prove it was not persisted.
			got := &cbv1alpha1.CloudberryCluster{}
			getErr := cl.Get(s.ctx, types.NamespacedName{
				Name:      cluster.Name,
				Namespace: ns,
			}, got)
			require.Error(s.T(), getErr)
			assert.True(s.T(), apierrors.IsNotFound(getErr),
				"rejected CR must not be persisted, got %v", getErr)
		})
	}
}
