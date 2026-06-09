package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// countingValidationMetrics embeds NoopRecorder and records every
// RecordRestoreValidation call so the controller tests can assert which {result}
// label was emitted and how many times.
type countingValidationMetrics struct {
	metrics.NoopRecorder

	results []string
}

func (m *countingValidationMetrics) RecordRestoreValidation(_, _, result string) {
	m.results = append(m.results, result)
}

func (m *countingValidationMetrics) count(result string) int {
	n := 0
	for _, r := range m.results {
		if r == result {
			n++
		}
	}
	return n
}

// validationJob builds a validate-operation Job fixture for the given timestamp
// and completion status.
func validationJob(cluster, timestamp string, status batchv1.JobStatus) *batchv1.Job {
	job := backupJob(cluster,
		util.PostRestoreValidationJobName(cluster, timestamp),
		util.BackupOperationValidate, status)
	if status.StartTime == nil {
		start := metav1.NewTime(time.Now().Add(-time.Minute))
		job.Status.StartTime = &start
	}
	return job
}

// succeededRestoreJob builds a Succeeded restore-operation Job carrying the
// expected-row-counts annotation.
func succeededRestoreJob(cluster, timestamp, expectedRowCountsJSON string) *batchv1.Job {
	job := backupJob(cluster,
		util.RestoreJobName(cluster, timestamp),
		util.BackupOperationRestore,
		batchv1.JobStatus{Succeeded: 1})
	if expectedRowCountsJSON != "" {
		job.Annotations = map[string]string{
			util.AnnotationExpectedRowCounts: expectedRowCountsJSON,
		}
	}
	return job
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

// ---------------------------------------------------------------------------
// expectedRowCountsFromJob
// ---------------------------------------------------------------------------

func TestExpectedRowCountsFromJob_Scenario80(t *testing.T) {
	tests := []struct {
		name string
		job  *batchv1.Job
		want map[string]int64
	}{
		{
			name: "valid json annotation parses to map",
			job: succeededRestoreJob("c", "20260101010101",
				`{"public.users":150000,"public.orders":300000}`),
			want: map[string]int64{"public.users": 150000, "public.orders": 300000},
		},
		{
			name: "missing annotation returns empty",
			job:  succeededRestoreJob("c", "20260101010101", ""),
			want: nil,
		},
		{
			name: "invalid json returns empty without panic",
			job:  succeededRestoreJob("c", "20260101010101", `{not-json`),
			want: nil,
		},
		{
			name: "blank annotation returns empty",
			job:  succeededRestoreJob("c", "20260101010101", "   "),
			want: nil,
		},
		{
			name: "empty json object returns empty",
			job:  succeededRestoreJob("c", "20260101010101", `{}`),
			want: nil,
		},
		{
			name: "nil job returns empty without panic",
			job:  nil,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := expectedRowCountsFromJob(context.Background(), tc.job)
			if tc.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// validation config helpers
// ---------------------------------------------------------------------------

func TestValidationHealthCheckQuery_Scenario80(t *testing.T) {
	t.Run("default empty when no validation config", func(t *testing.T) {
		assert.Equal(t, "", validationHealthCheckQuery(backupTestCluster()))
	})
	t.Run("nil backup returns empty", func(t *testing.T) {
		c := newTestCluster()
		assert.Equal(t, "", validationHealthCheckQuery(c))
	})
	t.Run("custom query honored", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{
			HealthCheckQuery: "SELECT count(*) FROM app.heartbeat",
		}
		assert.Equal(t, "SELECT count(*) FROM app.heartbeat",
			validationHealthCheckQuery(c))
	})
}

func TestValidationRunAnalyze_Scenario80(t *testing.T) {
	t.Run("nil backup is false", func(t *testing.T) {
		assert.False(t, validationRunAnalyze(newTestCluster()))
	})
	t.Run("no validation, no gprestore is false", func(t *testing.T) {
		assert.False(t, validationRunAnalyze(backupTestCluster()))
	})
	t.Run("validation runAnalyze true wins", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{RunAnalyze: true}
		assert.True(t, validationRunAnalyze(c))
	})
	t.Run("falls back to gprestore runAnalyze", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Gprestore = &cbv1alpha1.GprestoreOptions{RunAnalyze: true}
		assert.True(t, validationRunAnalyze(c))
	})
	t.Run("validation present but false falls back to gprestore", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{RunAnalyze: false}
		c.Spec.Backup.Gprestore = &cbv1alpha1.GprestoreOptions{RunAnalyze: true}
		assert.True(t, validationRunAnalyze(c))
	})
}

func TestValidationEnabled_Scenario80(t *testing.T) {
	trueVal := true
	falseVal := false
	t.Run("default enabled when no validation config", func(t *testing.T) {
		assert.True(t, validationEnabled(backupTestCluster()))
	})
	t.Run("default enabled when nil backup", func(t *testing.T) {
		assert.True(t, validationEnabled(newTestCluster()))
	})
	t.Run("explicit enabled true", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{Enabled: &trueVal}
		assert.True(t, validationEnabled(c))
	})
	t.Run("explicit enabled false disables", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{Enabled: &falseVal}
		assert.False(t, validationEnabled(c))
	})
	t.Run("nil pointer defaults enabled", func(t *testing.T) {
		c := backupTestCluster()
		c.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{Enabled: nil}
		assert.True(t, validationEnabled(c))
	})
}

// ---------------------------------------------------------------------------
// createValidationJob
// ---------------------------------------------------------------------------

// TestCreateValidationJob_Scenario80 verifies that a Succeeded restore Job with
// the expected-row-counts annotation plus a Validation config produces exactly
// one validation Job whose script reflects ExpectedRowCounts + RunAnalyze +
// HealthCheckQuery, and that a second call is idempotent.
func TestCreateValidationJob_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	cluster.Spec.Backup.Validation = &cbv1alpha1.BackupValidation{
		RunAnalyze:       true,
		HealthCheckQuery: "SELECT count(*) FROM app.heartbeat",
	}
	restore := succeededRestoreJob(cluster.Name, "20260608130000",
		`{"public.users":150000}`)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, restore).Build()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, &countingValidationMetrics{}, nil)

	require.NoError(t, r.createValidationJob(
		context.Background(), cluster, "20260608130000", restore))

	name := util.PostRestoreValidationJobName(cluster.Name, "20260608130000")
	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, got))
	assert.Equal(t, util.BackupOperationValidate, got.Labels[util.LabelBackupOperation])

	require.NotEmpty(t, got.Spec.Template.Spec.Containers)
	script := got.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "150000")
	assert.Contains(t, script, "public.users")
	assert.Contains(t, script, "ROW_COUNT_MATCH")
	assert.Contains(t, script, "ANALYZE_OK")
	assert.Contains(t, script, "psql -tA -c 'SELECT count(*) FROM app.heartbeat'")

	// Idempotent: a second call must not create a duplicate or error.
	require.NoError(t, r.createValidationJob(
		context.Background(), cluster, "20260608130000", restore))
	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	validateCount := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationValidate {
			validateCount++
		}
	}
	assert.Equal(t, 1, validateCount, "exactly one validation Job must exist")
}

// TestCreateValidationJob_NoAnnotation_Scenario80 verifies the best-effort probe
// path when the restore Job carries no expected-row-counts annotation.
func TestCreateValidationJob_NoAnnotation_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	restore := succeededRestoreJob(cluster.Name, "20260608130000", "")

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, restore).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &countingValidationMetrics{}, nil)

	require.NoError(t, r.createValidationJob(
		context.Background(), cluster, "20260608130000", restore))

	name := util.PostRestoreValidationJobName(cluster.Name, "20260608130000")
	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, got))
	script := got.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(t, script, "ROW_COUNT_PROBE_SKIPPED")
	// Default health-check query when no validation config.
	assert.Contains(t, script, "SELECT 1")
}

// ---------------------------------------------------------------------------
// observeValidationJobs / recordValidationOutcome / emitValidationFailureEvent
// ---------------------------------------------------------------------------

// TestObserveValidationJobs_Succeeded_Scenario80 verifies a Succeeded validation
// Job records exactly one "success" metric, sets the recorded annotation and
// emits NO Warning event.
func TestObserveValidationJobs_Succeeded_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := validationJob(cluster.Name, "20260608130000",
		batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	assert.Equal(t, 1, m.count(validationResultSuccess))
	assert.Equal(t, 0, m.count(validationResultFailed))

	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: job.Name, Namespace: cluster.Namespace}, got))
	assert.Equal(t, validationResultSuccess,
		got.Annotations[util.AnnotationValidationRecorded])

	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningValidationFailed(events),
		"a succeeded validation must not emit a Warning: %v", events)
}

// TestObserveValidationJobs_Failed_Scenario80 verifies a Failed validation Job
// records exactly one "failed" metric and emits exactly one Warning/
// ValidationFailed event.
func TestObserveValidationJobs_Failed_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := validationJob(cluster.Name, "20260608130000",
		batchv1.JobStatus{Failed: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	assert.Equal(t, 1, m.count(validationResultFailed))
	assert.Equal(t, 0, m.count(validationResultSuccess))

	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningValidationFailed(events),
		"a failed validation must emit exactly one Warning: %v", events)
}

// TestObserveValidationJobs_AlreadyRecorded_Scenario80 verifies a validation Job
// that already carries the recorded annotation is skipped: no double count and
// no new event (de-dup guard).
func TestObserveValidationJobs_AlreadyRecorded_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := validationJob(cluster.Name, "20260608130000",
		batchv1.JobStatus{Failed: 1})
	job.Annotations = map[string]string{
		util.AnnotationValidationRecorded: validationResultFailed,
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	assert.Empty(t, m.results, "already-recorded Job must not be counted again")
	events := drainEvents(recorder)
	assert.Equal(t, 0, countWarningValidationFailed(events),
		"already-recorded Job must not emit a new Warning: %v", events)
}

// TestObserveValidationJobs_DeDupAcrossReconciles_Scenario80 verifies that
// observing the SAME failed validation Job twice counts/events exactly once.
func TestObserveValidationJobs_DeDupAcrossReconciles_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := validationJob(cluster.Name, "20260608130000",
		batchv1.JobStatus{Failed: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))
	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	assert.Equal(t, 1, m.count(validationResultFailed),
		"failed validation must be counted exactly once across reconciles")
	events := drainEvents(recorder)
	assert.Equal(t, 1, countWarningValidationFailed(events),
		"failed validation must emit exactly one Warning across reconciles: %v", events)
}

// TestObserveValidationJobs_RunningSkipped_Scenario80 verifies a non-terminal
// validation Job is skipped (no metric, no annotation, no event).
func TestObserveValidationJobs_RunningSkipped_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	job := validationJob(cluster.Name, "20260608130000", batchv1.JobStatus{Active: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	assert.Empty(t, m.results)
	got := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: job.Name, Namespace: cluster.Namespace}, got))
	_, recorded := got.Annotations[util.AnnotationValidationRecorded]
	assert.False(t, recorded, "running Job must not be marked recorded")
}

// TestObserveValidationJobs_FailedDoesNotMutateRestore_Scenario80 verifies a
// Failed validation Job surfaces a Warning + metric but leaves the Succeeded
// restore Job's status untouched (validation is post-restore).
func TestObserveValidationJobs_FailedDoesNotMutateRestore_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	restore := succeededRestoreJob(cluster.Name, "20260608130000", "")
	validate := validationJob(cluster.Name, "20260608130000",
		batchv1.JobStatus{Failed: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, restore, validate).Build()
	recorder := record.NewFakeRecorder(10)
	m := &countingValidationMetrics{}
	r := NewAdminReconciler(k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil)

	require.NoError(t, r.observeValidationJobs(context.Background(), cluster))

	// Restore Job remains Succeeded; validation failure does not touch it.
	gotRestore := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{
			Name:      util.RestoreJobName(cluster.Name, "20260608130000"),
			Namespace: cluster.Namespace,
		}, gotRestore))
	assert.EqualValues(t, 1, gotRestore.Status.Succeeded,
		"restore Job must remain Succeeded despite validation failure")
	assert.EqualValues(t, 0, gotRestore.Status.Failed)
	_, recorded := gotRestore.Annotations[util.AnnotationValidationRecorded]
	assert.False(t, recorded, "restore Job must not be marked validation-recorded")

	assert.Equal(t, 1, m.count(validationResultFailed))
}

// TestValidationJobResult_Scenario80 is a table-driven test for the result
// derivation helper.
func TestValidationJobResult_Scenario80(t *testing.T) {
	tests := []struct {
		name         string
		status       batchv1.JobStatus
		wantResult   string
		wantTerminal bool
	}{
		{"succeeded", batchv1.JobStatus{Succeeded: 1}, validationResultSuccess, true},
		{"failed", batchv1.JobStatus{Failed: 1}, validationResultFailed, true},
		{"running", batchv1.JobStatus{Active: 1}, "", false},
		{"pending", batchv1.JobStatus{}, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := validationJob("c", "20260101010101", tc.status)
			result, terminal := validationJobResult(job)
			assert.Equal(t, tc.wantResult, result)
			assert.Equal(t, tc.wantTerminal, terminal)
		})
	}
}

// TestEmitValidationFailureEvent_Scenario80 exercises emitValidationFailureEvent
// directly: it emits exactly one Warning/ValidationFailed event referencing the
// restored timestamp.
func TestEmitValidationFailureEvent_Scenario80(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster()
	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(
		fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		scheme, recorder, builder.NewBuilder(), nil, &countingValidationMetrics{}, nil)

	job := validationJob(cluster.Name, "20260608130000", batchv1.JobStatus{Failed: 1})
	r.emitValidationFailureEvent(cluster, job)

	events := drainEvents(recorder)
	require.Equal(t, 1, countWarningValidationFailed(events))
	assert.Contains(t, events[0], "20260608130000")
}
