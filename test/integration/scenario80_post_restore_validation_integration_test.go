//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// Scenario 80: Post-Restore Validation (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// post-restore validation path: a Succeeded restore Job (carrying the
// avsoft.io/expected-row-counts annotation captured from gpbackup history) ->
// the AdminReconciler's reconcileBackup -> ensurePostRestoreValidation ->
// builder.BuildPostRestoreValidationJob -> the (fake) Kubernetes client. They
// assert the validation Job object actually persisted in the API server's k8s
// client carries the operation=validate label, the deterministic name, the
// owner-ref to the cluster and a rendered validation script that reflects the
// expected per-table counts (ROW_COUNT_MATCH/MISMATCH compare), the ANALYZE step
// (from Backup.Validation.RunAnalyze) and the configured health-check query:
//
//	80a/c : a Succeeded restore Job with the expected-row-counts annotation drives
//	        exactly one validation Job whose script embeds the per-table compare.
//	80d   : the validation Job is owner-ref'd + labelled operation=validate so the
//	        operator's observeValidationJobs metric/event loop can attribute it.
//
// The controller + builder + k8s client wiring is real; only the cluster/k8s
// backend is a fake client (no live MPP cluster). This mirrors the scenario79
// integration harness. The live row-count compare / mismatch FLAGGED path is
// exercised by the e2e live script via coordinator-exec gpbackup/gprestore.
// ============================================================================

const (
	scenario80IntNamespace = "cloudberry-test"
	scenario80IntCluster   = "scenario80-s3"
	scenario80IntTS        = "20260608130000"
)

// Scenario80IntegrationSuite drives the AdminReconciler against a fake k8s
// backend seeded with a validation-enabled cluster and a Succeeded restore Job.
type Scenario80IntegrationSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestIntegration_Scenario80(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario80IntegrationSuite))
}

func (s *Scenario80IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario80IntCluster builds a validation-enabled S3 (MinIO) backup cluster
// mirroring the scenario80-s3 sample CR.
func (s *Scenario80IntegrationSuite) cluster() *cbv1alpha1.CloudberryCluster {
	enabled := true
	cluster := testutil.NewClusterBuilder(scenario80IntCluster, scenario80IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Validation: &cbv1alpha1.BackupValidation{
			Enabled:          &enabled,
			RunAnalyze:       true,
			HealthCheckQuery: "SELECT 1",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Gprestore: &cbv1alpha1.GprestoreOptions{
			RunAnalyze: true,
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

// succeededRestoreJob builds a Succeeded restore-operation Job carrying the
// expected-row-counts annotation captured from the gpbackup history metadata.
func (s *Scenario80IntegrationSuite) succeededRestoreJob(
	cluster, ts, expectedJSON string,
) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RestoreJobName(cluster, ts),
			Namespace: scenario80IntNamespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
	if expectedJSON != "" {
		job.Annotations = map[string]string{
			util.AnnotationExpectedRowCounts: expectedJSON,
		}
	}
	return job
}

func (s *Scenario80IntegrationSuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// createdValidationJob reads back the validation Job named by the restore ts.
func (s *Scenario80IntegrationSuite) createdValidationJob(ts string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.PostRestoreValidationJobName(scenario80IntCluster, ts),
		Namespace: scenario80IntNamespace,
	}, job), "the operator-created validation Job must be persisted in k8s")
	return job
}

// --- 80a/c/d: Succeeded restore Job -> created validation Job ---

// TestIntegration_Scenario80_RestoreCreatesValidationJob reconciles a cluster
// with a Succeeded restore Job carrying the expected-row-counts annotation and
// asserts the persisted validation Job carries the deterministic name +
// operation=validate label + owner-ref and renders the per-table row-count
// compare reflecting the expected counts, plus the ANALYZE step and health check.
func (s *Scenario80IntegrationSuite) TestIntegration_Scenario80_RestoreCreatesValidationJob() {
	cluster := s.cluster()
	restore := s.succeededRestoreJob(cluster.Name, scenario80IntTS,
		`{"public.users":150000,"public.orders":300000}`)
	s.env = testutil.NewTestK8sEnv(cluster, restore)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should create the validation Job")

	job := s.createdValidationJob(scenario80IntTS)
	assert.Equal(s.T(), util.BackupOperationValidate,
		job.Labels[util.LabelBackupOperation],
		"the operator-created validation Job must carry operation=validate")
	require.Len(s.T(), job.OwnerReferences, 1)
	assert.Equal(s.T(), cluster.Name, job.OwnerReferences[0].Name,
		"the validation Job must be owner-ref'd to the cluster")

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.NotEmpty(s.T(), container.Args)
	script := container.Args[0]

	// The rendered script reflects the expected per-table counts from the
	// restore Job annotation (the headline row-count-vs-history compare).
	assert.Contains(s.T(), script, "row-count compare vs gpbackup history")
	assert.Contains(s.T(), script, "ROW_COUNT_MATCH")
	assert.Contains(s.T(), script, "ROW_COUNT_MISMATCH")
	assert.Contains(s.T(), script, "150000")
	assert.Contains(s.T(), script, "300000")
	assert.Contains(s.T(), script, "public.users")
	assert.Contains(s.T(), script, "public.orders")
	assert.Contains(s.T(), script, "exit 1")

	// RunAnalyze from Backup.Validation -> ANALYZE step.
	assert.Contains(s.T(), script, "ANALYZE_OK")
	// Default health-check query.
	assert.Contains(s.T(), script, "SELECT 1")
	// Must-pass invalid-index scan preserved.
	assert.Contains(s.T(), script, "indisvalid")

	// sh -c invocation.
	require.GreaterOrEqual(s.T(), len(container.Command), 2)
	assert.Contains(s.T(), container.Command, "-c")

	// Idempotent: a second reconcile does not create a duplicate.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	jobs := &batchv1.JobList{}
	require.NoError(s.T(), s.env.Client.List(s.ctx, jobs))
	count := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationValidate {
			count++
		}
	}
	assert.Equal(s.T(), 1, count,
		"reconcile must be idempotent: exactly one validation Job per restore")
}

// TestIntegration_Scenario80_RestoreWithoutAnnotationProbeFallback reconciles a
// cluster with a Succeeded restore Job WITHOUT the expected-row-counts annotation
// and asserts the persisted validation Job script falls back to the best-effort
// probe (ROW_COUNT_PROBE_SKIPPED) and no strict per-table compare.
func (s *Scenario80IntegrationSuite) TestIntegration_Scenario80_RestoreWithoutAnnotationProbeFallback() {
	cluster := s.cluster()
	restore := s.succeededRestoreJob(cluster.Name, scenario80IntTS, "")
	s.env = testutil.NewTestK8sEnv(cluster, restore)

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	job := s.createdValidationJob(scenario80IntTS)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, "ROW_COUNT_PROBE_SKIPPED")
	assert.NotContains(s.T(), script, "row-count compare vs gpbackup history")
}
