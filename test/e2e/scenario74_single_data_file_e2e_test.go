//go:build e2e

package e2e

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 74: Single Data File + Copy Queue + Restore with all gprestore
// Options (E2E)
// ============================================================================
//
// User journey: a user triggers an on-demand single-data-file backup
// (singleDataFile=true, copyQueueSize=4) and a full-option restore (jobs,
// redirect-db, redirect-schema, include-schema x2, include-table x2, create-db,
// with-stats, run-analyze, on-error-continue). The operator builds a Job
// DIRECTLY (not a CronJob) and renders the gpbackup/gprestore CLI args from the
// per-request options.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity for the single-data-file backup (--single-data-file +
//     --copy-queue-size 4, NO --jobs) and the full restore (all enabled flags;
//     --with-globals/--truncate-table OMITTED), and that BuildBackupJob /
//     BuildRestoreJob return a *batchv1.Job.
//   - A live-cluster portion gated on KUBECONFIG that reconciles against a fake
//     client seeded from the Scenario 74 spec and asserts resource shape plus
//     the builder args.
//
// This Go test never requires gpbackup binaries; the actual backup/restore data
// cycle is the live shell step (scenario74-single-data-file.sh).
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion and is declared
// package-scoped by the Scenario 69 e2e suite.

const scenario74E2EBackupImage = "cloudberry-backup:2.1.0"

// Scenario74SingleDataFileE2ESuite tests single-data-file backup + full restore.
type Scenario74SingleDataFileE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario74(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario74SingleDataFileE2ESuite))
}

func (s *Scenario74SingleDataFileE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario74SingleDataFileE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario74E2ECluster builds a running cluster with the Scenario 74 backup spec
// (full S3 destination + harmless cluster-level gpbackup defaults). Scenario 74
// options are supplied per-request at BuildBackupJob/BuildRestoreJob time.
func scenario74E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    scenario74E2EBackupImage,
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

// s74E2EBackupScript renders the gpbackup container script (joined args).
func (s *Scenario74SingleDataFileE2ESuite) s74E2EBackupScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	return strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
}

// s74E2ERestoreScript renders the gprestore container script (joined args).
func (s *Scenario74SingleDataFileE2ESuite) s74E2ERestoreScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	return strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
}

// s74E2ESingleDataFileJob builds a single-data-file backup Job for the cluster.
func s74E2ESingleDataFileJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.Job {
	return builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260101010101",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			SingleDataFile: true,
			CopyQueueSize:  4,
			// Jobs set to prove the single-data-file early return suppresses it.
			Jobs: 4,
		},
	})
}

// s74E2ERestoreJob builds a full-option restore Job for the cluster.
func s74E2ERestoreJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.Job {
	return builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
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
}

// --- 74.1: builder parity (infra-free) — single-data-file backup + restore ---

// TestE2E_Scenario74_BuilderParity verifies the single-data-file backup
// (--single-data-file + --copy-queue-size 4, NO --jobs, Job-not-CronJob) and the
// full restore (all enabled flags present; --with-globals/--truncate-table
// OMITTED, Job-not-CronJob).
func (s *Scenario74SingleDataFileE2ESuite) TestE2E_Scenario74_BuilderParity() {
	cluster := scenario74E2ECluster("test-s74e2e")

	// Backup: single-data-file + copy-queue, no --jobs.
	jobB := s74E2ESingleDataFileJob(cluster)
	assert.IsType(s.T(), &batchv1.Job{}, jobB)
	scriptB := s.s74E2EBackupScript(jobB)
	assert.Contains(s.T(), scriptB, "'--single-data-file'")
	assert.Contains(s.T(), scriptB, "'--copy-queue-size' '4'")
	assert.NotContains(s.T(), scriptB, "--jobs")

	// Restore: full option set, false bools omitted.
	jobR := s74E2ERestoreJob(cluster)
	assert.IsType(s.T(), &batchv1.Job{}, jobR)
	scriptR := s.s74E2ERestoreScript(jobR)
	assert.Contains(s.T(), scriptR, "'--timestamp' '20260101010101'")
	assert.Contains(s.T(), scriptR, "'--jobs' '4'")
	assert.Contains(s.T(), scriptR, "'--redirect-db' 'mydb_restored'")
	assert.Contains(s.T(), scriptR, "'--redirect-schema' 'restored'")
	assert.Contains(s.T(), scriptR, "'--create-db'")
	// gprestore forbids --include-schema with --include-table; when both are
	// supplied the builder emits --include-table (precedence) and OMITS
	// --include-schema so the gprestore invocation stays valid.
	assert.Contains(s.T(), scriptR, "'--include-table' 'public.users'")
	assert.Contains(s.T(), scriptR, "'--include-table' 'public.orders'")
	assert.NotContains(s.T(), scriptR, "--include-schema")
	// gprestore forbids --run-analyze with --with-stats; when both are supplied
	// the builder emits --run-analyze (precedence) and OMITS --with-stats so the
	// gprestore invocation stays valid.
	assert.Contains(s.T(), scriptR, "'--run-analyze'")
	assert.NotContains(s.T(), scriptR, "--with-stats")
	assert.Contains(s.T(), scriptR, "'--on-error-continue'")
	assert.NotContains(s.T(), scriptR, "--with-globals")
	assert.NotContains(s.T(), scriptR, "--truncate-table")
}

// --- 74.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario74_LiveResourceCreation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it reconciles against a fake client seeded
// from the Scenario 74 spec, asserts the S3 ConfigMap exists, and builds the
// single-data-file backup + full restore Jobs asserting their args (parity with
// the live shell step).
func (s *Scenario74SingleDataFileE2ESuite) TestE2E_Scenario74_LiveResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	cluster := scenario74E2ECluster("test-s74e2e-live")
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 ConfigMap should exist on a live-configured cluster")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	// Single-data-file backup Job lands in the cluster ns with the right args.
	jobB := s74E2ESingleDataFileJob(cluster)
	require.NotNil(s.T(), jobB)
	assert.Equal(s.T(), cluster.Namespace, jobB.Namespace)
	assert.Equal(s.T(), util.BackupOperationBackup, jobB.Labels[util.LabelBackupOperation])
	scriptB := s.s74E2EBackupScript(jobB)
	assert.Contains(s.T(), scriptB, "'--single-data-file'")
	assert.Contains(s.T(), scriptB, "'--copy-queue-size' '4'")
	assert.NotContains(s.T(), scriptB, "--jobs")

	// Full restore Job lands in the cluster ns with the right args.
	jobR := s74E2ERestoreJob(cluster)
	require.NotNil(s.T(), jobR)
	assert.Equal(s.T(), cluster.Namespace, jobR.Namespace)
	assert.Equal(s.T(), util.BackupOperationRestore, jobR.Labels[util.LabelBackupOperation])
	scriptR := s.s74E2ERestoreScript(jobR)
	assert.Contains(s.T(), scriptR, "'--redirect-db' 'mydb_restored'")
	assert.Contains(s.T(), scriptR, "'--redirect-schema' 'restored'")
	assert.Contains(s.T(), scriptR, "'--run-analyze'")
	// Both filters supplied: --include-table wins, --include-schema omitted.
	assert.Contains(s.T(), scriptR, "'--include-table' 'public.users'")
	assert.NotContains(s.T(), scriptR, "--include-schema")
	assert.NotContains(s.T(), scriptR, "--with-globals")
}
