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
// Scenario 74: Single Data File + Copy Queue + Restore with all gprestore
// Options (functional)
// ============================================================================
//
// Scenario 74 triggers an on-demand single-data-file backup and a full-option
// restore whose options are supplied PER-REQUEST (not baked into the CR). The
// operator builds a Job DIRECTLY (not a CronJob) for both the on-demand backup
// and restore paths and renders the gpbackup/gprestore CLI args from the
// per-request options.
//
//	BACKUP (single-data-file): singleDataFile=true, copyQueueSize=4. The args
//	  must contain --single-data-file and --copy-queue-size 4 and MUST NOT
//	  contain --jobs (per gpbackup rules --jobs cannot be combined with
//	  --single-data-file; the builder returns early in single-data-file mode).
//	  --single-data-file requires gpbackup_helper on every segment host; the
//	  binary ships in cloudberry-official:2.1.0 at $GPHOME/bin/gpbackup_helper.
//	RESTORE (all gprestore options): jobs=4, redirectDb=mydb_restored,
//	  redirectSchema=restored, includeSchemas=[public,analytics],
//	  includeTables=[public.users,public.orders], createDb, withStats,
//	  runAnalyze, onErrorContinue (and withGlobals=false, truncateTable=false
//	  OMITTED). All enabled flags must surface; the two false bools must NOT.
//
// These tests black-box the operator through the public builder
// (BuildBackupJob / BuildRestoreJob). The builder shell-quotes each arg, so the
// rendered script contains the quoted form `'--copy-queue-size' '4'`;
// assertions match that form (mirroring scenario71/73). The per-request REST
// merge is covered in internal/api (white-box).
// ============================================================================

// Scenario74Suite exercises single-data-file backups and full-option restores.
type Scenario74Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario74(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario74Suite))
}

func (s *Scenario74Suite) SetupTest() {
	s.ctx = context.Background()
}

// scenario74Cluster builds a running cluster with the full-S3 backup spec and
// harmless cluster-level gpbackup defaults; Scenario 74 options are passed
// per-request at BuildBackupJob/BuildRestoreJob time, overriding these defaults.
func scenario74Cluster(name string) *cbv1alpha1.CloudberryCluster {
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
func (s *Scenario74Suite) backupScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gpbackup", container.Name)
	return joinArgs(container.Args)
}

// restoreScript returns the rendered gprestore container script (joined args).
func (s *Scenario74Suite) restoreScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gprestore", container.Name)
	return joinArgs(container.Args)
}

// TestFunctional_Scenario74_SingleDataFileBackup asserts the single-data-file
// backup args: --single-data-file + --copy-queue-size 4 are present, --jobs is
// OMITTED (single-data-file early return), and BuildBackupJob returns a
// *batchv1.Job (the on-demand path builds a Job DIRECTLY, never a CronJob).
func (s *Scenario74Suite) TestFunctional_Scenario74_SingleDataFileBackup() {
	cluster := scenario74Cluster("s74-sdf-backup")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260101010101",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			SingleDataFile: true,
			CopyQueueSize:  4,
			// Jobs is intentionally set to prove the early return in
			// single-data-file mode suppresses --jobs.
			Jobs: 4,
		},
	})

	// "Job DIRECTLY not via CronJob": BuildBackupJob returns a *batchv1.Job.
	var _ *batchv1.Job = job
	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	script := s.backupScript(job)
	assert.Contains(s.T(), script, "'--single-data-file'")
	assert.Contains(s.T(), script, "'--copy-queue-size' '4'")
	// Per gpbackup rules, --jobs cannot be combined with --single-data-file:
	// the builder returns early so --jobs must NOT appear.
	assert.NotContains(s.T(), script, "--jobs")
}

// TestFunctional_Scenario74_RestoreAllOptions asserts the full gprestore option
// set: --timestamp, --jobs 4, --redirect-db, --redirect-schema, --create-db,
// --run-analyze, --on-error-continue; and that the false bools
// (--with-globals, --truncate-table) are OMITTED. BuildRestoreJob returns a
// *batchv1.Job.
//
// gprestore forbids --include-schema and --include-table together. When BOTH
// includeSchemas and includeTables are supplied the operator emits the more
// specific --include-table (table-level precedence) and OMITS --include-schema,
// so this case asserts both --include-table flags are present and that NO
// --include-schema flag is rendered.
//
// gprestore ALSO forbids --run-analyze together with --with-stats. Scenario 74
// supplies BOTH withStats=true AND runAnalyze=true; the operator emits
// --run-analyze (precedence: ANALYZE supersedes restoring backed-up stats) and
// OMITS --with-stats so the gprestore invocation stays valid.
func (s *Scenario74Suite) TestFunctional_Scenario74_RestoreAllOptions() {
	cluster := scenario74Cluster("s74-restore-all")

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:      "20260101010101",
		Databases:      []string{"mydb"},
		RedirectDb:     "mydb_restored",
		RedirectSchema: "restored",
		IncludeSchemas: []string{"public", "analytics"},
		IncludeTables:  []string{"public.users", "public.orders"},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:            4,
			CreateDb:        true,
			WithStats:       util.Ptr(true),
			RunAnalyze:      true,
			OnErrorContinue: true,
			WithGlobals:     false,
			TruncateTable:   false,
		},
	})

	// "Job DIRECTLY not via CronJob": BuildRestoreJob returns a *batchv1.Job.
	var _ *batchv1.Job = job
	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	script := s.restoreScript(job)
	assert.Contains(s.T(), script, "'--timestamp' '20260101010101'")
	assert.Contains(s.T(), script, "'--jobs' '4'")
	assert.Contains(s.T(), script, "'--redirect-db' 'mydb_restored'")
	assert.Contains(s.T(), script, "'--redirect-schema' 'restored'")
	assert.Contains(s.T(), script, "'--create-db'")
	// Both filters supplied: --include-table takes precedence; --include-schema
	// must be OMITTED (gprestore rejects the two together).
	assert.Contains(s.T(), script, "'--include-table' 'public.users'")
	assert.Contains(s.T(), script, "'--include-table' 'public.orders'")
	assert.NotContains(s.T(), script, "--include-schema")
	// Both withStats and runAnalyze supplied: --run-analyze wins and
	// --with-stats is OMITTED (gprestore rejects the two together).
	assert.Contains(s.T(), script, "'--run-analyze'")
	assert.NotContains(s.T(), script, "--with-stats")
	assert.Contains(s.T(), script, "'--on-error-continue'")
	// False bools must NOT emit their flag.
	assert.NotContains(s.T(), script, "--with-globals")
	assert.NotContains(s.T(), script, "--truncate-table")
}

// TestFunctional_Scenario74_RestoreWithStatsOnly asserts that when ONLY
// withStats is supplied (runAnalyze=false), the operator emits --with-stats and
// NOT --run-analyze. This is the complementary case to the run-analyze/with-stats
// precedence rule above.
func (s *Scenario74Suite) TestFunctional_Scenario74_RestoreWithStatsOnly() {
	cluster := scenario74Cluster("s74-restore-with-stats-only")

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: "20260101010101",
		Databases: []string{"mydb"},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:       4,
			WithStats:  util.Ptr(true),
			RunAnalyze: false,
		},
	})

	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	script := s.restoreScript(job)
	// Only withStats supplied: --with-stats emitted, --run-analyze absent.
	assert.Contains(s.T(), script, "'--with-stats'")
	assert.NotContains(s.T(), script, "--run-analyze")
}

// TestFunctional_Scenario74_RestoreSchemaOnly asserts that when ONLY
// includeSchemas is supplied (no includeTables), the operator emits one
// --include-schema per schema (and obviously no --include-table). This is the
// complementary case to the both-set precedence rule above.
func (s *Scenario74Suite) TestFunctional_Scenario74_RestoreSchemaOnly() {
	cluster := scenario74Cluster("s74-restore-schema-only")

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:      "20260101010101",
		Databases:      []string{"mydb"},
		RedirectDb:     "mydb_restored",
		RedirectSchema: "restored",
		IncludeSchemas: []string{"public", "analytics"},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			Jobs:            4,
			CreateDb:        true,
			WithStats:       util.Ptr(true),
			RunAnalyze:      true,
			OnErrorContinue: true,
		},
	})

	assert.IsType(s.T(), &batchv1.Job{}, job)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	script := s.restoreScript(job)
	// Only schemas supplied: --include-schema per schema, no --include-table.
	assert.Contains(s.T(), script, "'--include-schema' 'public'")
	assert.Contains(s.T(), script, "'--include-schema' 'analytics'")
	assert.NotContains(s.T(), script, "--include-table")
}
