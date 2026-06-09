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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 73: On-Demand Backup with gpbackup Options (functional)
// ============================================================================
//
// Scenario 73 triggers an on-demand backup whose gpbackup options are supplied
// PER-REQUEST (not baked into the CR). The operator builds a Job DIRECTLY (not a
// CronJob) for the on-demand path and renders the gpbackup CLI args from the
// per-request BackupJobOptions.
//
//	73a (StandardOptions): compressionLevel=6, compressionType=zstd, jobs=4,
//	  withStats=true, withoutGlobals=true, includeSchemas=[public, analytics].
//	  All flags must surface in the gpbackup container args, and the returned
//	  object must be a *batchv1.Job (never a CronJob).
//	73b (NoCompressionOverride): noCompression=true with compressionLevel=6.
//	  The args must contain --no-compression and MUST NOT contain
//	  --compression-level (compression level is ignored when noCompression).
//
// These tests black-box the operator through the public builder
// (BuildBackupJob). The builder shell-quotes each arg, so the rendered script
// contains the quoted form `'--compression-level' '6'`; assertions match that
// form (mirroring scenario71/72). The per-request API merge (noCompression
// propagation through mergeGpbackupOptions + handleCreateBackup) is covered in
// internal/api (white-box), since mergeGpbackupOptions is unexported.
// ============================================================================

// Scenario73Suite exercises on-demand backups with per-request gpbackup options.
type Scenario73Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario73(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario73Suite))
}

func (s *Scenario73Suite) SetupTest() {
	s.ctx = context.Background()
}

// scenario73Cluster builds a running cluster with the full-S3 backup spec and
// harmless cluster-level gpbackup defaults; Scenario 73 options are passed
// per-request at BuildBackupJob time, overriding these defaults.
func scenario73Cluster(name string) *cbv1alpha1.CloudberryCluster {
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

// backupScript returns the rendered gpbackup container script (joined args).
func (s *Scenario73Suite) backupScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gpbackup", container.Name)
	return joinArgs(container.Args)
}

// TestFunctional_Scenario73_StandardOptions asserts 73a: all eight gpbackup
// flags surface in the container args, and BuildBackupJob returns a *batchv1.Job
// (the on-demand path builds a Job DIRECTLY, never a CronJob).
func (s *Scenario73Suite) TestFunctional_Scenario73_StandardOptions() {
	cluster := scenario73Cluster("s73-standard")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
			Jobs:             4,
			WithStats:        util.Ptr(true),
			WithoutGlobals:   true,
		},
		IncludeSchemas: []string{"public", "analytics"},
	})

	// 73a "Job DIRECTLY not via CronJob": BuildBackupJob returns a *batchv1.Job.
	var _ *batchv1.Job = job
	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	// A Job has a pod template and no CronJob scheduling fields.
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	script := s.backupScript(job)
	assert.Contains(s.T(), script, "'--compression-level' '6'")
	assert.Contains(s.T(), script, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), script, "'--jobs' '4'")
	assert.Contains(s.T(), script, "'--with-stats'")
	assert.Contains(s.T(), script, "'--without-globals'")
	assert.Contains(s.T(), script, "'--include-schema' 'public'")
	assert.Contains(s.T(), script, "'--include-schema' 'analytics'")
}

// TestFunctional_Scenario73_NoCompressionOverride asserts 73b: noCompression
// emits --no-compression and the compression level/type are ignored (no
// --compression-level / --compression-type flag is rendered).
func (s *Scenario73Suite) TestFunctional_Scenario73_NoCompressionOverride() {
	cluster := scenario73Cluster("s73-nocompress")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			NoCompression:    true,
			CompressionLevel: 6,
		},
	})

	script := s.backupScript(job)
	assert.Contains(s.T(), script, "'--no-compression'")
	assert.NotContains(s.T(), script, "--compression-level")
	assert.NotContains(s.T(), script, "--compression-type")
}

// TestFunctional_Scenario73_JobNotCronJob documents the 73a "Job DIRECTLY"
// guarantee explicitly: the on-demand BuildBackupJob path yields a Job (with a
// pod template) and never a CronJob.
func (s *Scenario73Suite) TestFunctional_Scenario73_JobNotCronJob() {
	cluster := scenario73Cluster("s73-jobkind")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
		Type:      "full",
	})
	require.NotNil(s.T(), job)
	assert.IsType(s.T(), &batchv1.Job{}, job)
	// A Job carries its pod template directly (a CronJob would nest it under a
	// JobTemplate and have a .Spec.Schedule). Assert the Job-level pod template
	// is populated.
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	assert.Equal(s.T(), "gpbackup", job.Spec.Template.Spec.Containers[0].Name)
}
