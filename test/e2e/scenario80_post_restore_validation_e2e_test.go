//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 80: Post-Restore Validation (E2E)
// ============================================================================
//
// User journey: an operator-managed cluster with backup.validation.{enabled:true,
// runAnalyze:true,healthCheckQuery:"SELECT 1"} takes a backup, restores it, and
// after the Succeeded restore the operator creates a single post-restore
// validation Job that:
//
//	80a row-count vs history : compares actual restored per-table counts against
//	    the gpbackup-history expected counts (ROW_COUNT_MATCH on a clean restore).
//	80b run-analyze          : refreshes planner stats (ANALYZE_OK).
//	80c invalid-index/health : invalid-index scan reports 0; health-check runs.
//	80e deliberate mismatch  : data-only restore into a pre-populated table makes
//	    actual > expected => ROW_COUNT_MISMATCH => the validation Job FAILS => the
//	    operator records cloudberry_restore_validation_total{result="failed"} and
//	    emits a ValidationFailed Warning event (the restore stays Succeeded).
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildPostRestoreValidationJob renders the per-table
//     row-count compare (ROW_COUNT_MATCH/MISMATCH + exit-on-mismatch), the
//     ANALYZE step (ANALYZE_OK) when RunAnalyze, the must-pass invalid-index scan
//     and the health-check; labels the Job operation=validate and runs via sh -c.
//   - Controller parity: a Succeeded restore Job (with the expected-row-counts
//     annotation) yields exactly one validation Job (idempotent) whose script
//     reflects the expected counts; a Succeeded validation Job records the
//     success metric and a Failed one records the failed metric + a
//     ValidationFailed Warning event (de-duplicated).
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario80-post-restore-validation.sh, which drives the full
//     success + deliberate-mismatch lifecycle on a running cluster (real
//     gpbackup/gprestore via coordinator-exec; validation Job creation +
//     metric/event asserted from the rendered operator spec / materialized
//     Succeeded+Failed validation Jobs).
//
// This Go test never requires gpbackup/gprestore binaries or a real cluster for
// its deterministic parts; the actual restore + validation + mismatch FLAG are
// the live shell step (scenario80-post-restore-validation.sh).
// ============================================================================

const (
	envS80Cluster = "SCENARIO80_S3_CLUSTER"
	envS80Script  = "SCENARIO80_SCRIPT"

	scenario80E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario80E2ERestoreTS   = "20260608130000"
)

// scenario80E2ECountingMetrics embeds NoopRecorder and records every
// RecordRestoreValidation call so the e2e suite can assert the {result} label
// without a live registry.
type scenario80E2ECountingMetrics struct {
	metrics.NoopRecorder
	results []string
}

func (m *scenario80E2ECountingMetrics) RecordRestoreValidation(_, _, result string) {
	m.results = append(m.results, result)
}

func (m *scenario80E2ECountingMetrics) count(result string) int {
	n := 0
	for _, r := range m.results {
		if r == result {
			n++
		}
	}
	return n
}

// Scenario80PostRestoreValidationE2ESuite tests the validation Job rendering, the
// idempotent validation-Job creation, and the metric/event plumbing.
type Scenario80PostRestoreValidationE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario80(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario80PostRestoreValidationE2ESuite))
}

func (s *Scenario80PostRestoreValidationE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario80PostRestoreValidationE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// scenario80E2ECluster builds a running cluster with an S3 (MinIO) backup
// destination and validation enabled (mirrors scenario80-s3).
func scenario80E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	enabled := true
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario80E2EBackupImage,
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
		Gprestore: &cbv1alpha1.GprestoreOptions{RunAnalyze: true},
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

// scenario80E2ESucceededRestoreJob builds a Succeeded restore-operation Job
// carrying the expected-row-counts annotation captured from gpbackup history.
func scenario80E2ESucceededRestoreJob(cluster, ts, expectedJSON string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RestoreJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1, StartTime: &start, CompletionTime: &completion,
		},
	}
	if expectedJSON != "" {
		job.Annotations = map[string]string{
			util.AnnotationExpectedRowCounts: expectedJSON,
		}
	}
	return job
}

// scenario80E2EValidationJob builds a validate-operation Job fixture with the
// given terminal status.
func scenario80E2EValidationJob(cluster, ts string, status batchv1.JobStatus) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	completion := metav1.NewTime(time.Now())
	if status.StartTime == nil {
		status.StartTime = &start
	}
	if status.CompletionTime == nil {
		status.CompletionTime = &completion
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.PostRestoreValidationJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationValidate,
			},
			CreationTimestamp: completion,
		},
		Status: status,
	}
}

// scenario80CountWarningValidationFailed counts drained Warning/ValidationFailed
// events.
func scenario80CountWarningValidationFailed(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonValidationFailed) {
			n++
		}
	}
	return n
}

func scenario80DrainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

// --- 80.1: builder parity (infra-free) — validation script + spec ---

// TestE2E_Scenario80_ValidationScriptParity verifies BuildPostRestoreValidationJob
// renders the per-table row-count compare (ROW_COUNT_MATCH/MISMATCH +
// exit-on-mismatch), the ANALYZE step when RunAnalyze, the must-pass
// invalid-index scan and the health-check, labels the Job operation=validate and
// runs via sh -c.
func (s *Scenario80PostRestoreValidationE2ESuite) TestE2E_Scenario80_ValidationScriptParity() {
	cluster := scenario80E2ECluster("test-s80e2e-script")

	job := builder.NewBuilder().BuildPostRestoreValidationJob(cluster, &builder.ValidationJobOptions{
		Timestamp: scenario80E2ERestoreTS,
		Database:  "mydb_restore",
		ExpectedRowCounts: map[string]int64{
			"public.users":  150000,
			"public.orders": 300000,
		},
		RunAnalyze:       true,
		HealthCheckQuery: "SELECT 1",
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(),
		util.PostRestoreValidationJobName(cluster.Name, scenario80E2ERestoreTS), job.Name)
	assert.Equal(s.T(), util.BackupOperationValidate,
		job.Labels[util.LabelBackupOperation])

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.NotEmpty(s.T(), container.Args)
	script := container.Args[0]

	assert.Contains(s.T(), script, "row-count compare vs gpbackup history")
	// Two expected tables => two per-table ROW_COUNT_MATCH/MISMATCH compare blocks.
	assert.Equal(s.T(), 2, strings.Count(script, "ROW_COUNT_MATCH table="))
	assert.Contains(s.T(), script, "ROW_COUNT_MISMATCH")
	assert.Contains(s.T(), script, "150000")
	assert.Contains(s.T(), script, "300000")
	assert.Contains(s.T(), script, "exit 1")
	assert.Contains(s.T(), script, "ANALYZE_OK")
	assert.Contains(s.T(), script, "indisvalid")
	assert.Contains(s.T(), script, "SELECT 1")
	assert.NotContains(s.T(), script, "ROW_COUNT_PROBE_SKIPPED")

	require.GreaterOrEqual(s.T(), len(container.Command), 2)
	assert.Contains(s.T(), container.Command, "-c")
}

// --- 80.2: controller parity (infra-free) — validation creation + metric ---

// TestE2E_Scenario80_EnsureValidationParity reconciles against a fake client
// seeded with a Succeeded restore Job (expected-row-counts annotation set) and
// asserts the operator creates exactly one validation Job keyed off the restore
// ts and is idempotent.
func (s *Scenario80PostRestoreValidationE2ESuite) TestE2E_Scenario80_EnsureValidationParity() {
	cluster := scenario80E2ECluster("test-s80e2e-ensure")
	restore := scenario80E2ESucceededRestoreJob(cluster.Name, scenario80E2ERestoreTS,
		`{"public.users":150000}`)

	env := testutil.NewTestK8sEnv(cluster, restore)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	name := util.PostRestoreValidationJobName(cluster.Name, scenario80E2ERestoreTS)
	created, err := env.GetJob(s.ctx, name, cluster.Namespace)
	require.NoError(s.T(), err, "a validation Job keyed off the restore ts must exist")
	assert.Equal(s.T(), util.BackupOperationValidate,
		created.Labels[util.LabelBackupOperation])
	assert.Contains(s.T(), created.Spec.Template.Spec.Containers[0].Args[0], "150000")

	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	validations := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationValidate {
			validations++
		}
	}
	assert.Equal(s.T(), 1, validations,
		"reconcile must be idempotent: exactly one validation Job per restore")
}

// TestE2E_Scenario80_SuccessMetricParity reconciles against a fake client seeded
// with a Succeeded validation Job and asserts the metrics loop records
// result=success and emits no Warning.
func (s *Scenario80PostRestoreValidationE2ESuite) TestE2E_Scenario80_SuccessMetricParity() {
	cluster := scenario80E2ECluster("test-s80e2e-success")
	validate := scenario80E2EValidationJob(cluster.Name, scenario80E2ERestoreTS,
		batchv1.JobStatus{Succeeded: 1})

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, validate).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario80E2ECountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder, builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 1, m.count("success"))
	assert.Equal(s.T(), 0, m.count("failed"))
	assert.Equal(s.T(), 0, scenario80CountWarningValidationFailed(scenario80DrainEvents(recorder)))
}

// TestE2E_Scenario80_FailedMetricEventParity reconciles against a fake client
// seeded with a Failed validation Job (the deliberate-mismatch outcome) and
// asserts the metrics loop records result=failed and emits exactly one
// ValidationFailed Warning event.
func (s *Scenario80PostRestoreValidationE2ESuite) TestE2E_Scenario80_FailedMetricEventParity() {
	cluster := scenario80E2ECluster("test-s80e2e-failed")
	validate := scenario80E2EValidationJob(cluster.Name, scenario80E2ERestoreTS,
		batchv1.JobStatus{Failed: 1})

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, validate).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario80E2ECountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder, builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 1, m.count("failed"))
	assert.Equal(s.T(), 0, m.count("success"))
	assert.Equal(s.T(), 1,
		scenario80CountWarningValidationFailed(scenario80DrainEvents(recorder)),
		"a failed validation must emit exactly one ValidationFailed Warning")
}

// --- 80.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario80_LivePostRestoreValidation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup/gprestore binaries. When live, it shells out to the scenario80 live
// script, which drives the full success + deliberate-mismatch lifecycle: a real
// FULL gpbackup, a real gprestore into a fresh db (ROW_COUNT_MATCH, ANALYZE_OK,
// invalid-index clean, health-check ok) asserting success metric; then a
// data-only restore into a pre-populated table (actual > expected) asserting
// ROW_COUNT_MISMATCH + a Failed validation Job + the failed metric + a
// ValidationFailed Warning event.
func (s *Scenario80PostRestoreValidationE2ESuite) TestE2E_Scenario80_LivePostRestoreValidation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live post-restore-validation verification")
	}

	cluster := os.Getenv(envS80Cluster)
	if cluster == "" {
		cluster = "scenario80-s3"
	}

	script := os.Getenv(envS80Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario80-post-restore-validation.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full success + mismatch lifecycle and prints a per-check PASS/FAIL
	// summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario80 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario80 live script must pass all post-restore-validation checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}
