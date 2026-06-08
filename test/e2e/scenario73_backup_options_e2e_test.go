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
// Scenario 73: On-Demand Backup with gpbackup Options (E2E)
// ============================================================================
//
// User journey: a user triggers an on-demand backup whose gpbackup options are
// supplied PER-REQUEST (compression level/type, jobs, with-stats,
// without-globals, include-schema, or the noCompression override). The operator
// builds a Job DIRECTLY (not a CronJob) and renders the gpbackup CLI args from
// the request.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity for 73a (all eight flags) and 73b (--no-compression with
//     compression level ignored), and that BuildBackupJob returns a *batchv1.Job.
//   - A live-cluster portion gated on KUBECONFIG that reconciles against a fake
//     client seeded from the Scenario 73 spec and asserts resource shape plus the
//     73a/73b builder args.
//
// This Go test never requires gpbackup binaries; the actual backup data cycle is
// the live shell step.
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion and is declared
// package-scoped by the Scenario 69 e2e suite.

const scenario73E2EBackupImage = "cloudberry-backup:2.1.0"

// Scenario73BackupOptionsE2ESuite tests on-demand backup gpbackup options.
type Scenario73BackupOptionsE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario73(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario73BackupOptionsE2ESuite))
}

func (s *Scenario73BackupOptionsE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario73BackupOptionsE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario73E2ECluster builds a running cluster with the Scenario 73 backup spec
// (full S3 destination + harmless cluster-level gpbackup defaults). Scenario 73
// options are supplied per-request at BuildBackupJob time.
func scenario73E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    scenario73E2EBackupImage,
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

// s73E2EBackupScript renders the gpbackup container script (joined args).
func (s *Scenario73BackupOptionsE2ESuite) s73E2EBackupScript(job *batchv1.Job) string {
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	return strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
}

// s73E2EStandardJob builds a 73a backup Job for the given cluster.
func s73E2EStandardJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.Job {
	return builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 6,
			CompressionType:  "zstd",
			Jobs:             4,
			WithStats:        true,
			WithoutGlobals:   true,
		},
		IncludeSchemas: []string{"public", "analytics"},
	})
}

// s73E2ENoCompressionJob builds a 73b backup Job for the given cluster.
func s73E2ENoCompressionJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.Job {
	return builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
		Type:      "full",
		Databases: []string{"mydb"},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			NoCompression:    true,
			CompressionLevel: 6,
		},
	})
}

// --- 73.1: builder parity (infra-free) — 73a + 73b ---

// TestE2E_Scenario73_BuilderParity verifies 73a (all eight flags + Job-not-
// CronJob) and 73b (--no-compression with compression level ignored).
func (s *Scenario73BackupOptionsE2ESuite) TestE2E_Scenario73_BuilderParity() {
	cluster := scenario73E2ECluster("test-s73e2e")

	// 73a: standard options.
	jobA := s73E2EStandardJob(cluster)
	assert.IsType(s.T(), &batchv1.Job{}, jobA)
	scriptA := s.s73E2EBackupScript(jobA)
	assert.Contains(s.T(), scriptA, "'--compression-level' '6'")
	assert.Contains(s.T(), scriptA, "'--compression-type' 'zstd'")
	assert.Contains(s.T(), scriptA, "'--jobs' '4'")
	assert.Contains(s.T(), scriptA, "'--with-stats'")
	assert.Contains(s.T(), scriptA, "'--without-globals'")
	assert.Contains(s.T(), scriptA, "'--include-schema' 'public'")
	assert.Contains(s.T(), scriptA, "'--include-schema' 'analytics'")

	// 73b: noCompression override ignores the compression level.
	jobB := s73E2ENoCompressionJob(cluster)
	assert.IsType(s.T(), &batchv1.Job{}, jobB)
	scriptB := s.s73E2EBackupScript(jobB)
	assert.Contains(s.T(), scriptB, "'--no-compression'")
	assert.NotContains(s.T(), scriptB, "--compression-level")
	assert.NotContains(s.T(), scriptB, "--compression-type")
}

// --- 73.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario73_LiveResourceCreation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it reconciles against a fake client seeded
// from the Scenario 73 spec, asserts the S3 ConfigMap exists, and builds a 73a
// and a 73b backup Job asserting their args (parity with the live shell step).
func (s *Scenario73BackupOptionsE2ESuite) TestE2E_Scenario73_LiveResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	cluster := scenario73E2ECluster("test-s73e2e-live")
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

	// 73a: on-demand Job carries all eight flags and lands in the cluster ns.
	jobA := s73E2EStandardJob(cluster)
	require.NotNil(s.T(), jobA)
	assert.Equal(s.T(), cluster.Namespace, jobA.Namespace)
	assert.Equal(s.T(), util.BackupOperationBackup, jobA.Labels[util.LabelBackupOperation])
	scriptA := s.s73E2EBackupScript(jobA)
	assert.Contains(s.T(), scriptA, "'--compression-level' '6'")
	assert.Contains(s.T(), scriptA, "'--include-schema' 'public'")

	// 73b: on-demand Job ignores the compression level.
	jobB := s73E2ENoCompressionJob(cluster)
	scriptB := s.s73E2EBackupScript(jobB)
	assert.Contains(s.T(), scriptB, "'--no-compression'")
	assert.NotContains(s.T(), scriptB, "--compression-level")
}
