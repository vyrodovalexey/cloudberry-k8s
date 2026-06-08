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
// Scenario 70: Webhook backup defaults tests
// ============================================================================
//
// Applies a minimal backup spec (enabled, destination, image only) and verifies
// the mutating webhook defaults the 12 backup fields on the persisted object:
//
//	gpbackup.compressionLevel=1, gpbackup.compressionType=gzip, gpbackup.jobs=1,
//	gpbackup.singleDataFile=false, gpbackup.withStats=true,
//	gprestore.jobs=1, gprestore.withStats=true,
//	retention.fullCount=3, retention.maxAge=30d,
//	jobTemplate.backoffLimit=2, jobTemplate.activeDeadlineSeconds=7200,
//	jobTemplate.ttlSecondsAfterFinished=86400.
//
// "Persisted object": the admission server calls
// webhook.NewCloudberryClusterDefaulter().Default(ctx, cluster), which mutates
// the cluster IN PLACE. The mutated object is what the API server persists, so
// these tests exercise the real admission defaulting path via that public API
// (NOT the unexported setBackupDefaults).
//
// The scenario statement mentions "for mydb database"; BackupSpec has no
// per-database field — the database is a job-time/request concern — so the
// spec-level defaults are what is tested here.
// ============================================================================

// Scenario70Suite exercises the backup webhook defaulting rules.
type Scenario70Suite struct {
	suite.Suite
	ctx       context.Context
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestFunctional_Scenario70(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario70Suite))
}

func (s *Scenario70Suite) SetupTest() {
	s.ctx = context.Background()
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// scenario70MinimalBackupCluster builds a valid cluster with the MINIMAL backup
// spec only: enabled, image, and an s3 destination (type + bucket +
// credentialSecret.name). No Gpbackup, Gprestore, Retention, or JobTemplate is
// set — those must be DEFAULTED by the mutating webhook.
func scenario70MinimalBackupCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "cloudberry-backups",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "backup-s3-credentials"},
			},
		},
	}
	return cluster
}

// assertScenario70Defaults verifies the 12 defaulted backup fields on a cluster
// whose minimal backup spec has been run through the defaulter.
func (s *Scenario70Suite) assertScenario70Defaults(backup *cbv1alpha1.BackupSpec) {
	t := s.T()

	require.NotNil(t, backup.Gpbackup, "gpbackup must be allocated by defaulter")
	assert.Equal(t, int32(1), backup.Gpbackup.CompressionLevel, "gpbackup.compressionLevel")
	assert.Equal(t, "gzip", backup.Gpbackup.CompressionType, "gpbackup.compressionType")
	assert.Equal(t, int32(1), backup.Gpbackup.Jobs, "gpbackup.jobs")
	assert.False(t, backup.Gpbackup.SingleDataFile, "gpbackup.singleDataFile")
	assert.True(t, backup.Gpbackup.WithStats, "gpbackup.withStats")

	require.NotNil(t, backup.Gprestore, "gprestore must be allocated by defaulter")
	assert.Equal(t, int32(1), backup.Gprestore.Jobs, "gprestore.jobs")
	assert.True(t, backup.Gprestore.WithStats, "gprestore.withStats")

	assert.Equal(t, int32(3), backup.Retention.FullCount, "retention.fullCount")
	assert.Equal(t, "30d", backup.Retention.MaxAge, "retention.maxAge")

	require.NotNil(t, backup.JobTemplate, "jobTemplate must be allocated by defaulter")
	require.NotNil(t, backup.JobTemplate.BackoffLimit, "jobTemplate.backoffLimit")
	assert.Equal(t, int32(2), *backup.JobTemplate.BackoffLimit, "jobTemplate.backoffLimit")
	require.NotNil(t, backup.JobTemplate.ActiveDeadlineSeconds, "jobTemplate.activeDeadlineSeconds")
	assert.Equal(t, int64(7200), *backup.JobTemplate.ActiveDeadlineSeconds, "jobTemplate.activeDeadlineSeconds")
	require.NotNil(t, backup.JobTemplate.TTLSecondsAfterFinished, "jobTemplate.ttlSecondsAfterFinished")
	assert.Equal(t, int32(86400), *backup.JobTemplate.TTLSecondsAfterFinished, "jobTemplate.ttlSecondsAfterFinished")
}

// TestFunctional_Scenario70_MinimalBackupDefaults applies a minimal backup spec
// and asserts all 12 defaults on the persisted object.
func (s *Scenario70Suite) TestFunctional_Scenario70_MinimalBackupDefaults() {
	cluster := scenario70MinimalBackupCluster("s70-defaults")
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	s.assertScenario70Defaults(cluster.Spec.Backup)
}

// TestFunctional_Scenario70_DisabledBackupNotDefaulted verifies defaulting is
// gated on backup.enabled: with backup disabled, the gpbackup/jobTemplate blocks
// remain nil after Default() (defaults are NOT applied).
func (s *Scenario70Suite) TestFunctional_Scenario70_DisabledBackupNotDefaulted() {
	cluster := scenario70MinimalBackupCluster("s70-disabled")
	cluster.Spec.Backup.Enabled = false

	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	require.NotNil(s.T(), cluster.Spec.Backup, "backup spec preserved")
	assert.Nil(s.T(), cluster.Spec.Backup.Gpbackup, "gpbackup must NOT be defaulted when disabled")
	assert.Nil(s.T(), cluster.Spec.Backup.Gprestore, "gprestore must NOT be defaulted when disabled")
	assert.Nil(s.T(), cluster.Spec.Backup.JobTemplate, "jobTemplate must NOT be defaulted when disabled")
	assert.Equal(s.T(), int32(0), cluster.Spec.Backup.Retention.FullCount, "retention must NOT be defaulted when disabled")
}

// TestFunctional_Scenario70_ExplicitValuesPreserved sets a subset of fields to
// explicit non-default values and verifies they are preserved (not overwritten)
// while the other unset fields are still defaulted — proving non-destructive
// defaulting.
func (s *Scenario70Suite) TestFunctional_Scenario70_ExplicitValuesPreserved() {
	t := s.T()
	cluster := scenario70MinimalBackupCluster("s70-preserve")
	cluster.Spec.Backup.Gpbackup = &cbv1alpha1.GpbackupOptions{
		CompressionLevel: 9,
		Jobs:             4,
	}
	cluster.Spec.Backup.Retention = cbv1alpha1.BackupRetention{
		FullCount: 5,
		MaxAge:    "90d",
	}
	cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
		BackoffLimit: ptrInt32(7),
	}

	require.NoError(t, s.defaulter.Default(s.ctx, cluster))

	backup := cluster.Spec.Backup

	// Explicit values preserved.
	require.NotNil(t, backup.Gpbackup)
	assert.Equal(t, int32(9), backup.Gpbackup.CompressionLevel, "explicit compressionLevel preserved")
	assert.Equal(t, int32(4), backup.Gpbackup.Jobs, "explicit jobs preserved")
	assert.Equal(t, int32(5), backup.Retention.FullCount, "explicit retention.fullCount preserved")
	assert.Equal(t, "90d", backup.Retention.MaxAge, "explicit retention.maxAge preserved")
	require.NotNil(t, backup.JobTemplate.BackoffLimit)
	assert.Equal(t, int32(7), *backup.JobTemplate.BackoffLimit, "explicit backoffLimit preserved")

	// Other unset fields still defaulted.
	assert.Equal(t, "gzip", backup.Gpbackup.CompressionType, "unset compressionType still defaulted")
	assert.True(t, backup.Gpbackup.WithStats, "unset withStats still defaulted")
	require.NotNil(t, backup.Gprestore)
	assert.Equal(t, int32(1), backup.Gprestore.Jobs, "unset gprestore.jobs still defaulted")
	require.NotNil(t, backup.JobTemplate.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *backup.JobTemplate.ActiveDeadlineSeconds, "unset activeDeadlineSeconds still defaulted")
	require.NotNil(t, backup.JobTemplate.TTLSecondsAfterFinished)
	assert.Equal(t, int32(86400), *backup.JobTemplate.TTLSecondsAfterFinished, "unset ttlSecondsAfterFinished still defaulted")
}

// ptrInt32 returns a pointer to the supplied int32, used to set explicit
// pointer-typed JobTemplate fields in tests.
func ptrInt32(v int32) *int32 { return &v }
