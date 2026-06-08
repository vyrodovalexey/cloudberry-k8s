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
// Scenario 75: Compression Matrix (gzip vs zstd) (E2E)
// ============================================================================
//
// User journey: a user triggers two on-demand full backups of the SAME data
// that differ ONLY by compression algorithm — gzip and zstd — at the SAME
// compression level (6). The operator builds a Job DIRECTLY (not a CronJob) for
// each and renders the gpbackup CLI args from the per-request options. Each
// backup is restored to its OWN redirect DB (mydb_gzip_restored /
// mydb_zstd_restored, createDb=true).
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity for the gzip backup (--compression-type gzip +
//     --compression-level 6) and the zstd backup (--compression-type zstd +
//     --compression-level 6), and that each build returns a *batchv1.Job.
//   - Builder parity for the two restore Jobs (--redirect-db <db> + --create-db).
//   - A live-cluster portion gated on KUBECONFIG that reconciles against a fake
//     client seeded from the Scenario 75 spec and asserts resource shape plus
//     the builder args.
//
// This Go test never requires gpbackup binaries; the actual backup/restore data
// cycle plus the on-disk size comparison is the live shell step
// (scenario75-compression-matrix.sh).
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion and is declared
// package-scoped by the Scenario 69 e2e suite.

const scenario75E2EBackupImage = "cloudberry-backup:2.1.0"

// Scenario75CompressionMatrixE2ESuite tests gzip vs zstd compression backups.
type Scenario75CompressionMatrixE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario75(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario75CompressionMatrixE2ESuite))
}

func (s *Scenario75CompressionMatrixE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario75CompressionMatrixE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario75E2ECluster builds a running cluster with the Scenario 75 backup spec
// (full S3 destination + harmless cluster-level gpbackup defaults). Scenario 75
// options are supplied per-request at BuildBackupJob/BuildRestoreJob time.
func scenario75E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    scenario75E2EBackupImage,
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

// s75E2EScript renders the gpbackup/gprestore container script (joined args).
func (s *Scenario75CompressionMatrixE2ESuite) s75E2EScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	return strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
}

// s75E2EBackupJob builds a full backup Job for the given compression type at
// level 6 (the apples-to-apples compression-matrix arm).
func s75E2EBackupJob(cluster *cbv1alpha1.CloudberryCluster, compressionType string) *batchv1.Job {
	return builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260101010101",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionType:  compressionType,
			CompressionLevel: 6,
		},
	})
}

// s75E2ERestoreJob builds a restore Job redirecting into the given DB.
func s75E2ERestoreJob(cluster *cbv1alpha1.CloudberryCluster, redirectDb string) *batchv1.Job {
	return builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:  "20260101010101",
		Databases:  []string{"mydb"},
		RedirectDb: redirectDb,
		Gprestore: &cbv1alpha1.GprestoreOptions{
			CreateDb: true,
		},
	})
}

// --- 75.1: builder parity (infra-free) — gzip + zstd backups + restores ---

// TestE2E_Scenario75_BuilderParity verifies the gzip backup
// (--compression-type gzip + --compression-level 6, Job-not-CronJob) and the
// zstd backup (--compression-type zstd + --compression-level 6, Job-not-CronJob),
// plus the two restore Jobs (--redirect-db + --create-db).
func (s *Scenario75CompressionMatrixE2ESuite) TestE2E_Scenario75_BuilderParity() {
	cluster := scenario75E2ECluster("test-s75e2e")

	// gzip backup arm.
	jobG := s75E2EBackupJob(cluster, "gzip")
	assert.IsType(s.T(), &batchv1.Job{}, jobG)
	scriptG := s.s75E2EScript(jobG)
	assert.Contains(s.T(), scriptG, "'--compression-type' 'gzip'")
	assert.Contains(s.T(), scriptG, "'--compression-level' '6'")
	assert.NotContains(s.T(), scriptG, "'--compression-type' 'zstd'")

	// zstd backup arm.
	jobZ := s75E2EBackupJob(cluster, "zstd")
	assert.IsType(s.T(), &batchv1.Job{}, jobZ)
	scriptZ := s.s75E2EScript(jobZ)
	assert.Contains(s.T(), scriptZ, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), scriptZ, "'--compression-level' '6'")
	assert.NotContains(s.T(), scriptZ, "'--compression-type' 'gzip'")

	// Restore each backup to its OWN redirect DB.
	jobRG := s75E2ERestoreJob(cluster, "mydb_gzip_restored")
	assert.IsType(s.T(), &batchv1.Job{}, jobRG)
	scriptRG := s.s75E2EScript(jobRG)
	assert.Contains(s.T(), scriptRG, "'--redirect-db' 'mydb_gzip_restored'")
	assert.Contains(s.T(), scriptRG, "'--create-db'")

	jobRZ := s75E2ERestoreJob(cluster, "mydb_zstd_restored")
	assert.IsType(s.T(), &batchv1.Job{}, jobRZ)
	scriptRZ := s.s75E2EScript(jobRZ)
	assert.Contains(s.T(), scriptRZ, "'--redirect-db' 'mydb_zstd_restored'")
	assert.Contains(s.T(), scriptRZ, "'--create-db'")
}

// --- 75.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario75_LiveResourceCreation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it reconciles against a fake client seeded
// from the Scenario 75 spec, asserts the S3 ConfigMap exists, and builds the
// gzip + zstd backup Jobs asserting their args, namespace and labels (parity
// with the live shell step).
func (s *Scenario75CompressionMatrixE2ESuite) TestE2E_Scenario75_LiveResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	cluster := scenario75E2ECluster("test-s75e2e-live")
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

	// gzip backup Job lands in the cluster ns with the right args.
	jobG := s75E2EBackupJob(cluster, "gzip")
	require.NotNil(s.T(), jobG)
	assert.Equal(s.T(), cluster.Namespace, jobG.Namespace)
	assert.Equal(s.T(), util.BackupOperationBackup, jobG.Labels[util.LabelBackupOperation])
	scriptG := s.s75E2EScript(jobG)
	assert.Contains(s.T(), scriptG, "'--compression-type' 'gzip'")
	assert.Contains(s.T(), scriptG, "'--compression-level' '6'")

	// zstd backup Job lands in the cluster ns with the right args.
	jobZ := s75E2EBackupJob(cluster, "zstd")
	require.NotNil(s.T(), jobZ)
	assert.Equal(s.T(), cluster.Namespace, jobZ.Namespace)
	assert.Equal(s.T(), util.BackupOperationBackup, jobZ.Labels[util.LabelBackupOperation])
	scriptZ := s.s75E2EScript(jobZ)
	assert.Contains(s.T(), scriptZ, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), scriptZ, "'--compression-level' '6'")

	// Restore Jobs land in the cluster ns with the right redirect DBs.
	jobRG := s75E2ERestoreJob(cluster, "mydb_gzip_restored")
	require.NotNil(s.T(), jobRG)
	assert.Equal(s.T(), cluster.Namespace, jobRG.Namespace)
	assert.Equal(s.T(), util.BackupOperationRestore, jobRG.Labels[util.LabelBackupOperation])
	assert.Contains(s.T(), s.s75E2EScript(jobRG), "'--redirect-db' 'mydb_gzip_restored'")
}
