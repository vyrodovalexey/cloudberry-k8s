//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 69: Webhook backup validation negative tests
// ============================================================================
//
// Each rule 69a..69j builds an otherwise-valid cluster with backup enabled and
// then mutates exactly one offending field, calls ValidateCreate, and asserts
// the create is rejected with a descriptive error mentioning the field path.
//
// "Not persisted": a ValidateCreate error is what makes the API server REJECT
// the object, so it is never persisted. We assert the error is returned here.
// The live/e2e kubectl-apply rejection is verified in the Scenario 69 e2e
// scenario and at live deployment time.
// ============================================================================

// Scenario69Suite exercises the backup webhook validation negative rules.
type Scenario69Suite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario69(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario69Suite))
}

func (s *Scenario69Suite) SetupTest() {
	s.ctx = context.Background()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario69ValidBackup returns a fully valid backup spec: enabled, s3
// destination with bucket + credentialSecret.name, image set, and a valid
// gpbackup block. Callers mutate exactly one field to produce a negative case.
func scenario69ValidBackup() *cbv1alpha1.BackupSpec {
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

// scenario69Cluster builds a valid cluster with a valid backup spec, then
// applies the supplied mutator to introduce a single offending field.
func scenario69Cluster(name string, mutate func(*cbv1alpha1.BackupSpec)) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	backup := scenario69ValidBackup()
	if mutate != nil {
		mutate(backup)
	}
	cluster.Spec.Backup = backup
	return cluster
}

// assertRejected runs ValidateCreate and asserts a descriptive error.
func (s *Scenario69Suite) assertRejected(
	cluster *cbv1alpha1.CloudberryCluster, substr string,
) {
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), substr)
}

// --- positive baselines ---

// TestFunctional_Scenario69_ValidBackup_Accepted verifies the valid backup
// spec passes validation (no offending field).
func (s *Scenario69Suite) TestFunctional_Scenario69_ValidBackup_Accepted() {
	cluster := scenario69Cluster("s69-valid", nil)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

// TestFunctional_Scenario69c_VaultSecret_Accepted verifies S3 credentials may
// come from Vault: vaultSecret.path set, no credentialSecret -> accepted.
func (s *Scenario69Suite) TestFunctional_Scenario69c_VaultSecret_Accepted() {
	cluster := scenario69Cluster("s69c-vault-ok", func(b *cbv1alpha1.BackupSpec) {
		b.Destination.S3.CredentialSecret = nil
		b.Destination.S3.VaultSecret = &cbv1alpha1.S3VaultSecret{
			Path: "secret/data/cloudberry/backup-s3",
		}
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

// --- negative rules 69a..69j ---

// 69a: destination.type empty -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69a_MissingDestinationType() {
	cluster := scenario69Cluster("s69a", func(b *cbv1alpha1.BackupSpec) {
		b.Destination.Type = ""
	})
	s.assertRejected(cluster, "destination.type")
}

// 69b: s3 with no bucket -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69b_MissingS3Bucket() {
	cluster := scenario69Cluster("s69b", func(b *cbv1alpha1.BackupSpec) {
		b.Destination.S3.Bucket = ""
	})
	s.assertRejected(cluster, "bucket")
}

// 69c: s3 with no credentialSecret AND no vaultSecret -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69c_MissingCredentialAndVault() {
	cluster := scenario69Cluster("s69c", func(b *cbv1alpha1.BackupSpec) {
		b.Destination.S3.CredentialSecret = nil
		b.Destination.S3.VaultSecret = nil
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "credentialSecret")
	assert.Contains(s.T(), err.Error(), "vaultSecret")
}

// 69d: compressionLevel=10 -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69d_CompressionLevelTooHigh() {
	cluster := scenario69Cluster("s69d-10", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.CompressionLevel = 10
	})
	s.assertRejected(cluster, "compressionLevel")
}

// 69d: compressionLevel=0 -> rejected (validator called directly bypasses the
// mutating defaulter; an explicit 0 is an invalid gpbackup level).
func (s *Scenario69Suite) TestFunctional_Scenario69d_CompressionLevelZero() {
	cluster := scenario69Cluster("s69d-0", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.CompressionLevel = 0
	})
	s.assertRejected(cluster, "compressionLevel")
}

// 69e: compressionType=lz4 -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69e_InvalidCompressionType() {
	cluster := scenario69Cluster("s69e", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.CompressionType = "lz4"
	})
	s.assertRejected(cluster, "compressionType")
}

// 69f: copyQueueSize set without singleDataFile -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69f_CopyQueueSizeWithoutSingleDataFile() {
	cluster := scenario69Cluster("s69f", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.CopyQueueSize = 4
		b.Gpbackup.SingleDataFile = false
	})
	s.assertRejected(cluster, "copyQueueSize")
}

// 69g: jobs > 1 combined with singleDataFile -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69g_JobsWithSingleDataFile() {
	cluster := scenario69Cluster("s69g", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.Jobs = 4
		b.Gpbackup.SingleDataFile = true
	})
	s.assertRejected(cluster, "jobs cannot be combined")
}

// 69h: incremental without leafPartitionData -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69h_IncrementalWithoutLeafPartitionData() {
	cluster := scenario69Cluster("s69h", func(b *cbv1alpha1.BackupSpec) {
		b.Gpbackup.Incremental = true
		b.Gpbackup.LeafPartitionData = false
	})
	s.assertRejected(cluster, "leafPartitionData")
}

// 69i: schedule is not a valid cron expression -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69i_InvalidCron() {
	cluster := scenario69Cluster("s69i", func(b *cbv1alpha1.BackupSpec) {
		b.Schedule = "not a cron"
	})
	s.assertRejected(cluster, "cron")
}

// 69j: backup.image empty -> rejected.
func (s *Scenario69Suite) TestFunctional_Scenario69j_MissingImage() {
	cluster := scenario69Cluster("s69j", func(b *cbv1alpha1.BackupSpec) {
		b.Image = ""
	})
	s.assertRejected(cluster, "backup.image")
}
