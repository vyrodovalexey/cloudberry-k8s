//go:build functional

package functional

import (
	"context"
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
// Scenario 80: Post-Restore Validation (functional)
// ============================================================================
//
// Scenario 80 covers the post-restore validation lifecycle at the
// builder/controller layer (deterministic, no live cluster). After a successful
// restore the operator creates a validation Job that:
//
//	80a row-count vs history : the rendered validation script renders a
//	    deterministic per-table compare (SELECT count(*) vs the gpbackup-history
//	    expected counts passed in ExpectedRowCounts), emits ROW_COUNT_MATCH /
//	    ROW_COUNT_MISMATCH markers and exits 1 on ANY mismatch (the headline
//	    check). With RunAnalyze it ANALYZEs first (ANALYZE_OK); an empty expected
//	    map falls back to the best-effort probe (ROW_COUNT_PROBE_SKIPPED).
//	80b validation Job spec  : BuildPostRestoreValidationJob labels the Job
//	    operation=validate, runs the script via sh -c, owner-refs the cluster and
//	    sets PGDATABASE from the request.
//	80c createValidationJob  : a Succeeded restore Job carrying the
//	    avsoft.io/expected-row-counts annotation drives exactly one validation Job
//	    whose script reflects ExpectedRowCounts + RunAnalyze + HealthCheckQuery
//	    (RunAnalyze/HealthCheckQuery sourced from Backup.Validation).
//	80d observeValidationJobs: a Succeeded validation Job records the
//	    cloudberry_restore_validation_total{result="success"} metric; a Failed one
//	    records {result="failed"} AND emits a ValidationFailed Warning event,
//	    de-duplicated across reconciles. A validation failure NEVER mutates the
//	    Succeeded restore Job (validation is post-restore).
//
// These tests black-box the operator through the public builder
// (BuildPostRestoreValidationJob rendered script/spec) and the AdminReconciler
// with a fake client. They are deterministic and self-contained (no live infra).
// The live row-count compare / mismatch FLAGGED path is exercised by the e2e
// live script via coordinator-exec gpbackup/gprestore, since the standalone
// validation Job pod is not the coordinator.
// ============================================================================

const (
	scenario80BackupImage = "cloudberry-backup:2.1.0"
	// scenario80RestoreTS is the pinned restore timestamp used to key the
	// deterministic validation Job name.
	scenario80RestoreTS = "20260608130000"
)

// scenario80CountingMetrics embeds NoopRecorder and records every
// RecordRestoreValidation call so the functional suite can assert which {result}
// label was emitted and how many times, without a live Prometheus registry.
type scenario80CountingMetrics struct {
	metrics.NoopRecorder
	results []string
}

func (m *scenario80CountingMetrics) RecordRestoreValidation(_, _, result string) {
	m.results = append(m.results, result)
}

func (m *scenario80CountingMetrics) count(result string) int {
	n := 0
	for _, r := range m.results {
		if r == result {
			n++
		}
	}
	return n
}

// Scenario80Suite exercises the post-restore validation script rendering, the
// validation Job spec, the createValidationJob wiring (annotation -> expected
// counts) and the observeValidationJobs metric/event plumbing.
type Scenario80Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario80(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario80Suite))
}

func (s *Scenario80Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario80Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario80ValidationSpec returns an S3 (MinIO) destination BackupSpec with the
// given validation config, mirroring the scenario80-s3 sample CR.
func scenario80ValidationSpec(validation *cbv1alpha1.BackupValidation) *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:    true,
		Image:      scenario80BackupImage,
		Validation: validation,
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
}

// scenario80EnabledValidation returns the validation config used by the
// scenario80-s3 sample CR (enabled, runAnalyze, "SELECT 1" health check).
func scenario80EnabledValidation() *cbv1alpha1.BackupValidation {
	enabled := true
	return &cbv1alpha1.BackupValidation{
		Enabled:          &enabled,
		RunAnalyze:       true,
		HealthCheckQuery: "SELECT 1",
	}
}

// scenario80Cluster builds a Running cluster (pending generation) with the given
// backup spec. WithPendingGeneration drives the full spec-driven reconcile path
// (reconcileBackup -> ensurePostRestoreValidation/observeValidationJobs); a
// steady-state generation short-circuits before validation.
func scenario80Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario80SucceededRestoreJob builds a Succeeded restore-operation Job named
// like the operator's restore Job so the operator parses the 14-digit timestamp,
// optionally carrying the expected-row-counts annotation.
func scenario80SucceededRestoreJob(cluster, ts, expectedJSON string) *batchv1.Job {
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

// scenario80ValidationJob builds a validate-operation Job fixture for the given
// timestamp and terminal status (used to drive observeValidationJobs).
func scenario80ValidationJob(cluster, ts string, status batchv1.JobStatus) *batchv1.Job {
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

// countValidationJobs counts the validate-operation Jobs in a Job list.
func countValidationJobs(jobs []batchv1.Job) int {
	n := 0
	for i := range jobs {
		if jobs[i].Labels[util.LabelBackupOperation] == util.BackupOperationValidate {
			n++
		}
	}
	return n
}

// countWarningValidationFailed counts drained Warning/ValidationFailed events.
func countWarningValidationFailed(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonValidationFailed) {
			n++
		}
	}
	return n
}

// drainFakeRecorder drains the buffered events from a fake recorder.
func drainFakeRecorder(rec *record.FakeRecorder) []string {
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

// --- 80a: validation script rendering (row-count compare + analyze) ---

// TestFunctional_Scenario80_ValidationScriptWithExpectedCounts asserts the
// rendered validation script renders the per-table row-count compare vs gpbackup
// history (ROW_COUNT_MATCH/MISMATCH + exit-on-mismatch), the ANALYZE step
// (ANALYZE_OK) when RunAnalyze is set, the must-pass invalid-index scan and the
// health-check, with the ANALYZE preceding the compare.
func (s *Scenario80Suite) TestFunctional_Scenario80_ValidationScriptWithExpectedCounts() {
	job := builder.NewBuilder().BuildPostRestoreValidationJob(
		scenario80Cluster("s80-script", scenario80ValidationSpec(scenario80EnabledValidation())),
		&builder.ValidationJobOptions{
			Timestamp: scenario80RestoreTS,
			Database:  "mydb",
			ExpectedRowCounts: map[string]int64{
				"public.users":  150000,
				"public.orders": 300000,
			},
			RunAnalyze:       true,
			HealthCheckQuery: "SELECT 1",
		},
	)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	// Headline row-count-vs-history compare.
	assert.Contains(s.T(), script, "row-count compare vs gpbackup history")
	assert.Contains(s.T(), script, "ROW_COUNT_MATCH")
	assert.Contains(s.T(), script, "ROW_COUNT_MISMATCH")
	assert.Contains(s.T(), script, "150000")
	assert.Contains(s.T(), script, "300000")
	assert.Contains(s.T(), script, "public.users")
	assert.Contains(s.T(), script, "public.orders")
	assert.Contains(s.T(), script, `if [ "${rowcount_mismatch}" -gt 0 ]`)
	assert.Contains(s.T(), script, "exit 1")

	// RunAnalyze step.
	assert.Contains(s.T(), script, "ANALYZE")
	assert.Contains(s.T(), script, "ANALYZE_OK")

	// Must-pass invalid-index scan + health-check preserved.
	assert.Contains(s.T(), script, "indisvalid")
	assert.Contains(s.T(), script, "SELECT 1")

	// Empty-map fallback must NOT be rendered when expected counts are set.
	assert.NotContains(s.T(), script, "ROW_COUNT_PROBE_SKIPPED")

	// ANALYZE precedes the row-count compare which precedes the invalid-index scan.
	analyzeIdx := strings.Index(script, "ANALYZE_OK")
	compareIdx := strings.Index(script, "row-count compare vs gpbackup history")
	invalidIdx := strings.Index(script, "indisvalid")
	require.NotEqual(s.T(), -1, analyzeIdx)
	assert.Less(s.T(), analyzeIdx, compareIdx, "ANALYZE must precede the row-count compare")
	assert.Less(s.T(), compareIdx, invalidIdx, "row-count compare must precede the invalid-index scan")
}

// TestFunctional_Scenario80_ValidationScriptEmptyMapProbe asserts that with an
// empty ExpectedRowCounts map the script renders the best-effort probe
// (ROW_COUNT_PROBE_SKIPPED) and NO strict compare, and omits ANALYZE when
// RunAnalyze is false.
func (s *Scenario80Suite) TestFunctional_Scenario80_ValidationScriptEmptyMapProbe() {
	job := builder.NewBuilder().BuildPostRestoreValidationJob(
		scenario80Cluster("s80-probe", scenario80ValidationSpec(nil)),
		&builder.ValidationJobOptions{
			Timestamp:         scenario80RestoreTS,
			ExpectedRowCounts: map[string]int64{},
			RunAnalyze:        false,
		},
	)
	require.NotNil(s.T(), job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(s.T(), script, "ROW_COUNT_PROBE_SKIPPED")
	assert.Contains(s.T(), script, "indisvalid")
	assert.Contains(s.T(), script, "SELECT 1")

	assert.NotContains(s.T(), script, "ROW_COUNT_MISMATCH")
	assert.NotContains(s.T(), script, "ROW_COUNT_MATCH")
	assert.NotContains(s.T(), script, "row-count compare vs gpbackup history")
	assert.NotContains(s.T(), script, "ANALYZE_OK")
}

// --- 80b: validation Job spec (operation=validate label, sh -c) ---

// TestFunctional_Scenario80_ValidationJobSpec asserts the validation Job carries
// the operation=validate label, the deterministic name, the owner-ref to the
// cluster, an sh -c invocation and PGDATABASE set from the request.
func (s *Scenario80Suite) TestFunctional_Scenario80_ValidationJobSpec() {
	cluster := scenario80Cluster("s80-spec", scenario80ValidationSpec(scenario80EnabledValidation()))
	job := builder.NewBuilder().BuildPostRestoreValidationJob(cluster, &builder.ValidationJobOptions{
		Timestamp: scenario80RestoreTS,
		Database:  "mydb_restore",
	})
	require.NotNil(s.T(), job)

	assert.Equal(s.T(),
		util.PostRestoreValidationJobName(cluster.Name, scenario80RestoreTS), job.Name,
		"the validation Job name must be deterministic (cluster-validate-ts)")
	assert.Equal(s.T(), util.BackupOperationValidate,
		job.Labels[util.LabelBackupOperation],
		"the validation Job must carry operation=validate")

	require.Len(s.T(), job.OwnerReferences, 1)
	assert.Equal(s.T(), cluster.Name, job.OwnerReferences[0].Name)

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.GreaterOrEqual(s.T(), len(container.Command), 2,
		"the validation container must run a shell command")
	assert.Contains(s.T(), container.Command, "-c",
		"the validation container must invoke the script via sh -c")

	var pgdb string
	for _, e := range container.Env {
		if e.Name == "PGDATABASE" {
			pgdb = e.Value
		}
	}
	assert.Equal(s.T(), "mydb_restore", pgdb,
		"PGDATABASE must be set from opts.Database")
}

// --- 80c: createValidationJob populates ExpectedRowCounts + config ---

// TestFunctional_Scenario80_CreateValidationJobFromAnnotation reconciles a
// cluster with a Succeeded restore Job carrying the expected-row-counts
// annotation and a Validation config, then asserts exactly one validation Job is
// created whose rendered script reflects ExpectedRowCounts + RunAnalyze +
// HealthCheckQuery, and that a second reconcile is idempotent.
func (s *Scenario80Suite) TestFunctional_Scenario80_CreateValidationJobFromAnnotation() {
	validation := scenario80EnabledValidation()
	validation.HealthCheckQuery = "SELECT count(*) FROM app.heartbeat"
	cluster := scenario80Cluster("s80-create", scenario80ValidationSpec(validation))
	restore := scenario80SucceededRestoreJob(cluster.Name, scenario80RestoreTS,
		`{"public.users":150000}`)

	env := testutil.NewTestK8sEnv(cluster, restore)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "first reconcile should succeed")

	name := util.PostRestoreValidationJobName(cluster.Name, scenario80RestoreTS)
	created, err := env.GetJob(s.ctx, name, cluster.Namespace)
	require.NoError(s.T(), err, "a validation Job keyed off the restore ts must exist")
	assert.Equal(s.T(), util.BackupOperationValidate,
		created.Labels[util.LabelBackupOperation])

	require.NotEmpty(s.T(), created.Spec.Template.Spec.Containers)
	script := created.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, "150000", "expected counts from the annotation must be rendered")
	assert.Contains(s.T(), script, "public.users")
	assert.Contains(s.T(), script, "ROW_COUNT_MATCH")
	assert.Contains(s.T(), script, "ANALYZE_OK", "RunAnalyze from Backup.Validation must render ANALYZE")
	assert.Contains(s.T(), script, "psql -tA -c 'SELECT count(*) FROM app.heartbeat'",
		"the configured HealthCheckQuery must be rendered")

	// A second reconcile must NOT create a duplicate validation Job.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "second reconcile should succeed")

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	assert.Equal(s.T(), 1, countValidationJobs(jobs.Items),
		"reconcile must be idempotent: exactly one validation Job per restore")
}

// TestFunctional_Scenario80_CreateValidationJobNoAnnotation reconciles a cluster
// with a Succeeded restore Job WITHOUT the expected-row-counts annotation and
// asserts the validation script falls back to the best-effort probe and the
// default health-check query.
func (s *Scenario80Suite) TestFunctional_Scenario80_CreateValidationJobNoAnnotation() {
	cluster := scenario80Cluster("s80-noann", scenario80ValidationSpec(nil))
	restore := scenario80SucceededRestoreJob(cluster.Name, scenario80RestoreTS, "")

	env := testutil.NewTestK8sEnv(cluster, restore)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	name := util.PostRestoreValidationJobName(cluster.Name, scenario80RestoreTS)
	created, err := env.GetJob(s.ctx, name, cluster.Namespace)
	require.NoError(s.T(), err)
	script := created.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, "ROW_COUNT_PROBE_SKIPPED")
	assert.Contains(s.T(), script, "SELECT 1")
}

// TestFunctional_Scenario80_NoValidationWhenDisabled asserts that with the
// validation config explicitly disabled, no validation Job is created even after
// a Succeeded restore.
func (s *Scenario80Suite) TestFunctional_Scenario80_NoValidationWhenDisabled() {
	disabled := false
	cluster := scenario80Cluster("s80-disabled",
		scenario80ValidationSpec(&cbv1alpha1.BackupValidation{Enabled: &disabled}))
	restore := scenario80SucceededRestoreJob(cluster.Name, scenario80RestoreTS, "")

	env := testutil.NewTestK8sEnv(cluster, restore)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	assert.Equal(s.T(), 0, countValidationJobs(jobs.Items),
		"validation disabled => no validation Job")
}

// --- 80d: observeValidationJobs records metric + ValidationFailed event ---

// TestFunctional_Scenario80_SucceededValidationRecordsSuccess seeds a Succeeded
// validation Job and asserts a reconcile records exactly one "success" metric and
// emits NO Warning event.
func (s *Scenario80Suite) TestFunctional_Scenario80_SucceededValidationRecordsSuccess() {
	cluster := scenario80Cluster("s80-success", scenario80ValidationSpec(scenario80EnabledValidation()))
	validate := scenario80ValidationJob(cluster.Name, scenario80RestoreTS,
		batchv1.JobStatus{Succeeded: 1})

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, validate).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario80CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 1, m.count("success"),
		"a Succeeded validation Job must record result=success")
	assert.Equal(s.T(), 0, m.count("failed"))
	events := drainFakeRecorder(recorder)
	assert.Equal(s.T(), 0, countWarningValidationFailed(events),
		"a succeeded validation must not emit a Warning: %v", events)
}

// TestFunctional_Scenario80_FailedValidationFlagsAndEmits seeds a Failed
// validation Job (the deliberate-mismatch outcome) and asserts a reconcile
// records exactly one "failed" metric and emits exactly one ValidationFailed
// Warning event; a second reconcile is de-duplicated (no double-count, no second
// event).
func (s *Scenario80Suite) TestFunctional_Scenario80_FailedValidationFlagsAndEmits() {
	cluster := scenario80Cluster("s80-failed", scenario80ValidationSpec(scenario80EnabledValidation()))
	validate := scenario80ValidationJob(cluster.Name, scenario80RestoreTS,
		batchv1.JobStatus{Failed: 1})

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, validate).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario80CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	// A second reconcile must be de-duplicated via the recorded annotation.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 1, m.count("failed"),
		"a Failed validation Job must record result=failed exactly once across reconciles")
	assert.Equal(s.T(), 0, m.count("success"))

	events := drainFakeRecorder(recorder)
	assert.Equal(s.T(), 1, countWarningValidationFailed(events),
		"a failed validation must emit exactly one ValidationFailed Warning across reconciles: %v", events)

	// The recorded de-dup annotation must be patched onto the validation Job.
	got, err := func() (*batchv1.Job, error) {
		j := &batchv1.Job{}
		e := k8sClient.Get(s.ctx, types.NamespacedName{
			Name:      validate.Name,
			Namespace: cluster.Namespace,
		}, j)
		return j, e
	}()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "failed", got.Annotations[util.AnnotationValidationRecorded])
}

// TestFunctional_Scenario80_FailedValidationDoesNotMutateRestore seeds a
// Succeeded restore Job + a Failed validation Job and asserts the validation
// failure surfaces (metric + Warning) but leaves the Succeeded restore Job's
// status untouched (validation is post-restore).
func (s *Scenario80Suite) TestFunctional_Scenario80_FailedValidationDoesNotMutateRestore() {
	cluster := scenario80Cluster("s80-isolate", scenario80ValidationSpec(scenario80EnabledValidation()))
	restore := scenario80SucceededRestoreJob(cluster.Name, scenario80RestoreTS, "")
	validate := scenario80ValidationJob(cluster.Name, scenario80RestoreTS,
		batchv1.JobStatus{Failed: 1})

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, restore, validate).
		Build()
	m := &scenario80CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	gotRestore := &batchv1.Job{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      util.RestoreJobName(cluster.Name, scenario80RestoreTS),
		Namespace: cluster.Namespace,
	}, gotRestore))
	assert.EqualValues(s.T(), 1, gotRestore.Status.Succeeded,
		"restore Job must remain Succeeded despite validation failure")
	assert.EqualValues(s.T(), 0, gotRestore.Status.Failed)
	assert.Equal(s.T(), 1, m.count("failed"))
}
