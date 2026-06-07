//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 75: Compression Matrix (gzip vs zstd) (functional)
// ============================================================================
//
// Scenario 75 triggers two on-demand full backups of the SAME data that differ
// ONLY by compression algorithm — gzip and zstd — at the SAME compression level
// (6) so the comparison is apples-to-apples (same level, different codec). The
// operator builds a Job DIRECTLY (not a CronJob) for the on-demand backup path
// and renders the gpbackup CLI args from the per-request options.
//
//	gzip backup: gpbackupOptions{compressionType:"gzip", compressionLevel:6} ->
//	  args must contain --compression-type gzip and --compression-level 6.
//	zstd backup: gpbackupOptions{compressionType:"zstd", compressionLevel:6} ->
//	  args must contain --compression-type zstd and --compression-level 6.
//
// These tests black-box the operator through the public builder
// (BuildBackupJob). The builder shell-quotes each arg, so the rendered script
// contains the quoted form `'--compression-type' 'zstd'`; assertions match that
// form (mirroring scenario71/73/74). The per-request REST merge is covered in
// internal/api (white-box, scenario75_compression_test.go).
// ============================================================================

// Scenario75Suite exercises gzip vs zstd compression-matrix backups.
type Scenario75Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario75(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario75Suite))
}

func (s *Scenario75Suite) SetupTest() {
	s.ctx = context.Background()
}

// scenario75Cluster builds a running cluster with the full-S3 backup spec and
// harmless cluster-level gpbackup defaults; Scenario 75 options are passed
// per-request at BuildBackupJob time, overriding these defaults.
func scenario75Cluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "/backups",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests:  4,
					BackupMultipartChunksize:     "10MB",
					RestoreMaxConcurrentRequests: 4,
					RestoreMultipartChunksize:    "10MB",
				},
			},
		},
	}
	return cluster
}

// backupArgs returns the rendered gpbackup container args (shell-quoted) for the
// given compression type at level 6, and asserts the container is named gpbackup
// and that the build returns a *batchv1.Job (the on-demand path builds a Job
// DIRECTLY, never a CronJob).
func (s *Scenario75Suite) backupArgs(name, compressionType string) []string {
	cluster := scenario75Cluster(name)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260101010101",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionType:  compressionType,
			CompressionLevel: 6,
		},
	})

	// "Job DIRECTLY not via CronJob": BuildBackupJob returns a *batchv1.Job.
	var _ *batchv1.Job = job
	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gpbackup", container.Name)
	return container.Args
}

// TestFunctional_Scenario75_GzipBackup asserts the gzip backup args:
// --compression-type gzip + --compression-level 6 are present, and
// BuildBackupJob returns a *batchv1.Job.
func (s *Scenario75Suite) TestFunctional_Scenario75_GzipBackup() {
	script := joinArgs(s.backupArgs("s75-gzip-backup", "gzip"))
	assert.Contains(s.T(), script, "'--compression-type' 'gzip'")
	assert.Contains(s.T(), script, "'--compression-level' '6'")
	// gzip must not be mislabelled as zstd.
	assert.NotContains(s.T(), script, "'--compression-type' 'zstd'")
}

// TestFunctional_Scenario75_ZstdBackup asserts the zstd backup args:
// --compression-type zstd + --compression-level 6 are present, and
// BuildBackupJob returns a *batchv1.Job.
func (s *Scenario75Suite) TestFunctional_Scenario75_ZstdBackup() {
	script := joinArgs(s.backupArgs("s75-zstd-backup", "zstd"))
	assert.Contains(s.T(), script, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), script, "'--compression-level' '6'")
	// zstd must not be mislabelled as gzip.
	assert.NotContains(s.T(), script, "'--compression-type' 'gzip'")
}

// TestFunctional_Scenario75_MatrixDiffersOnlyByType asserts that, at the SAME
// compression level (6), the gzip and zstd backup arg slices differ ONLY in the
// compression-type value: replacing 'gzip' with 'zstd' in the gzip script yields
// exactly the zstd script. This proves the matrix is apples-to-apples (same
// level, different codec) and that nothing about zstd is special-cased.
func (s *Scenario75Suite) TestFunctional_Scenario75_MatrixDiffersOnlyByType() {
	gzipArgs := s.backupArgs("s75-matrix-gzip", "gzip")
	zstdArgs := s.backupArgs("s75-matrix-zstd", "zstd")

	require.Equal(s.T(), len(gzipArgs), len(zstdArgs),
		"gzip and zstd arg slices must have identical length at the same level")

	var diffs int
	for i := range gzipArgs {
		if gzipArgs[i] == zstdArgs[i] {
			continue
		}
		diffs++
		assert.Contains(s.T(), gzipArgs[i], "gzip",
			"the only differing arg must be the gzip compression-type value")
		assert.Contains(s.T(), zstdArgs[i], "zstd",
			"the only differing arg must be the zstd compression-type value")
	}
	assert.Equal(s.T(), 1, diffs,
		"gzip and zstd backups must differ in EXACTLY one arg (the compression-type value)")
}
