package controller

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// pxfExtDBClient is a db.Client mock that tracks SetupPXFExtensions calls and can
// be configured to return a specific installed-count and/or an error. It embeds
// mockDBClient for the rest of the interface. pxfInstalled defaults to 0; tests
// that expect the ready annotation must set it >= 1.
type pxfExtDBClient struct {
	mockDBClient
	pxfCalls     int
	pxfInstalled int
	pxfErr       error
}

func (m *pxfExtDBClient) SetupPXFExtensions(_ context.Context) (int, error) {
	m.pxfCalls++
	return m.pxfInstalled, m.pxfErr
}

func newPXFExtCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{{Name: "s3", Type: "s3"}},
		},
	}
	return c
}

func TestSetupPXFExtensions_Success_SetsAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	// installed==2 (both pxf and pxf_fdw) => the extension really installed.
	dbClient := &pxfExtDBClient{pxfInstalled: 2}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	assert.Equal(t, 1, dbClient.pxfCalls)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Equal(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady])
}

// TestSetupPXFExtensions_ZeroInstalled_NoAnnotation proves a reachable DB that
// installed ZERO extensions (pxf absent OR DB in recovery) does NOT set the
// ready annotation, so the install is retried on the next reconcile.
func TestSetupPXFExtensions_ZeroInstalled_NoAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	// SetupPXFExtensions returns (0, nil): reachable but nothing installed.
	dbClient := &pxfExtDBClient{pxfInstalled: 0}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	assert.Equal(t, 1, dbClient.pxfCalls)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.NotEqual(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady],
		"annotation must NOT be set when installed==0 (retryable)")
}

// TestSetupPXFExtensions_OneInstalled_SetsAnnotation proves a single successful
// CREATE EXTENSION (installed>=1) is enough to mark PXF ready.
func TestSetupPXFExtensions_OneInstalled_SetsAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{pxfInstalled: 1}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Equal(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady])
}

func TestSetupPXFExtensions_Idempotent_SkipsWhenAnnotationSet(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	cluster.Annotations = map[string]string{util.AnnotationPXFExtensionsReady: "true"}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	assert.Equal(t, 0, dbClient.pxfCalls, "annotation present => no DB round-trip")
}

func TestSetupPXFExtensions_NonFatal_OnSetupError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{pxfErr: fmt.Errorf("connection refused")}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	// Must not panic / error; the annotation is NOT set on failure (will retry).
	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	assert.Equal(t, 1, dbClient.pxfCalls)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.NotEqual(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady])
}

func TestSetupPXFExtensions_NoFactory_NoOp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// No dbFactory => silent no-op (must not panic).
	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
}

func TestSetupPXFExtensions_PXFDisabled_NoOp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true} // no Pxf
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	dbClient := &pxfExtDBClient{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &mockDBClientFactory{client: dbClient}, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	assert.Equal(t, 0, dbClient.pxfCalls)
}

// TestReconcileDataLoading_SetupPXFExtensionsRoundTrip_PreservesStatus is a
// regression test for a latent nil-pointer dereference in reconcileDataLoading.
//
// reconcileDataLoading builds cluster.Status.DataLoading IN MEMORY (counts + the
// observed Pxf status) BEFORE it is persisted by patchDataLoadingStatus at the
// end of the func. Between the build and the persist it calls setupPXFExtensions,
// which — on a reachable DB that installs >=1 extension — issues a MergePatch
// annotation write (setAnnotationPatch -> client.Patch). With the real API/fake
// client, controller-runtime writes the server response back into `cluster`,
// CLEARING the not-yet-persisted in-memory Status.DataLoading (back to nil). The
// subsequent `if cluster.Status.DataLoading.Pxf != nil` dereference then PANICs,
// and patchDataLoadingStatus would silently drop the status.
//
// This test wires a db.Client whose SetupPXFExtensions returns (1, nil) to force
// the annotation-patch path (AnnotationPXFExtensionsReady is NOT pre-set), with
// the real round-tripping fake client. It asserts reconcile does NOT panic and
// that the built status (counts + pxf) survives to be persisted.
func TestReconcileDataLoading_SetupPXFExtensionsRoundTrip_PreservesStatus(t *testing.T) {
	scheme := newTestScheme()
	cluster := dlCluster("dl-pxf-roundtrip",
		[]cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	cluster.Spec.DataLoading.Pxf = &cbv1alpha1.PxfSpec{
		Enabled: true,
		Image:   "cloudberry-pxf:2.1.0",
		Servers: []cbv1alpha1.PxfServerSpec{
			{Name: "s3", Type: "s3"},
			{Name: "jdbc", Type: "jdbc"},
		},
	}
	// AnnotationPXFExtensionsReady is intentionally absent so the annotation
	// patch path runs (it round-trips the server object back into `cluster`).

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	// installed==1 forces setupPXFExtensions to call setAnnotationPatch.
	dbClient := &pxfExtDBClient{pxfInstalled: 1}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	// Must NOT panic on the L3124 Pxf deref after the patch round-trip.
	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	})
	assert.Equal(t, 1, dbClient.pxfCalls, "the installed>=1 annotation patch path must run")

	// The in-memory status must survive the annotation-patch round-trip.
	require.NotNil(t, cluster.Status.DataLoading,
		"Status.DataLoading must survive the setupPXFExtensions annotation-patch round-trip")
	assert.Equal(t, int32(1), cluster.Status.DataLoading.ConfiguredJobs)
	assert.Equal(t, int32(1), cluster.Status.DataLoading.ActiveJobs)
	require.NotNil(t, cluster.Status.DataLoading.Pxf,
		"the observed PXF status must survive the round-trip")
	assert.True(t, cluster.Status.DataLoading.Pxf.Configured)
	assert.Equal(t, int32(2), cluster.Status.DataLoading.Pxf.Servers)

	// And it must be PERSISTED (patchDataLoadingStatus ran with non-nil status).
	persisted := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, persisted))
	require.NotNil(t, persisted.Status.DataLoading,
		"Status.DataLoading must be persisted, not dropped")
	require.NotNil(t, persisted.Status.DataLoading.Pxf)
	assert.Equal(t, int32(2), persisted.Status.DataLoading.Pxf.Servers)
}

// pxfResults returns the captured result labels from a dataLoadCapturingRecorder
// (test helper for the B-2 controller-level assertions, T5–T7).
func pxfResults(rec *dataLoadCapturingRecorder) []string {
	out := make([]string, 0, len(rec.pxfCalls))
	for _, c := range rec.pxfCalls {
		out = append(out, c.result)
	}
	return out
}

// TestSetupPXFExtensions_DBUnavailable_RecordsErrorMetric drives the NewClient
// error branch of setupPXFExtensions (T5, L138-145, previously UNCOVERED): the
// factory returns a connection error so the connectivity-boundary error path
// runs. A capturing recorder asserts RecordPXFExtensionSetup(result="error") is
// recorded exactly once, the ready annotation is NOT set, and no panic occurs.
func TestSetupPXFExtensions_DBUnavailable_RecordsErrorMetric(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	// NewClient fails at the connectivity boundary (no client returned).
	factory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}
	rec := &dataLoadCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, rec, nil)

	require.NotPanics(t, func() {
		r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	})

	require.Len(t, rec.pxfCalls, 1, "exactly one PXF setup outcome must be recorded")
	assert.Equal(t, pxfExtensionResultError, rec.pxfCalls[0].result,
		"a NewClient connectivity failure must record result=error")
	assert.Equal(t, cluster.Name, rec.pxfCalls[0].cluster)
	assert.Equal(t, cluster.Namespace, rec.pxfCalls[0].namespace)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.NotEqual(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady],
		"the ready annotation must NOT be set when the DB is unavailable")
}

// TestSetupPXFExtensions_SetupError_RecordsErrorMetric is the B-2 controller-
// level result-label verification for the SetupPXFExtensions hard-error branch
// (T6, result="error", L149-155): a reachable DB whose SetupPXFExtensions returns
// a connectivity error. The capturing recorder asserts result="error".
func TestSetupPXFExtensions_SetupError_RecordsErrorMetric(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{pxfErr: fmt.Errorf("connection refused")}
	factory := &mockDBClientFactory{client: dbClient}
	rec := &dataLoadCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, rec, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	assert.Equal(t, []string{pxfExtensionResultError}, pxfResults(rec),
		"a SetupPXFExtensions hard error must record result=error")
}

// TestSetupPXFExtensions_ZeroInstalled_RecordsAbsentMetric is the B-2 controller-
// level result-label verification for the absent branch (T6, result="absent",
// L174-180): a reachable DB that installed ZERO extensions records result=absent.
func TestSetupPXFExtensions_ZeroInstalled_RecordsAbsentMetric(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{pxfInstalled: 0}
	factory := &mockDBClientFactory{client: dbClient}
	rec := &dataLoadCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, rec, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	assert.Equal(t, []string{pxfExtensionResultAbsent}, pxfResults(rec),
		"a reachable DB with zero installs must record result=absent")
}

// TestSetupPXFExtensions_OneInstalled_RecordsInstalledMetric is the B-2
// controller-level result-label verification for the installed branch (T6,
// result="installed", L184): a successful install (>=1) records result=installed
// (the metric is recorded BEFORE the annotation patch).
func TestSetupPXFExtensions_OneInstalled_RecordsInstalledMetric(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &pxfExtDBClient{pxfInstalled: 1}
	factory := &mockDBClientFactory{client: dbClient}
	rec := &dataLoadCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, rec, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	assert.Equal(t, []string{pxfExtensionResultInstalled}, pxfResults(rec),
		"a successful install (>=1) must record result=installed")
}

// TestSetupPXFExtensions_AnnotationPatchFailure_NonFatal drives the
// setAnnotationPatch failure warn branch of setupPXFExtensions (T7, L186-189,
// previously UNCOVERED): installed>=1 so the patch path runs, but an interceptor
// fails the Patch. The function must stay non-fatal (no panic/no surfaced error)
// and RecordPXFExtensionSetup(result="installed") must STILL be captured because
// the metric is recorded BEFORE the patch attempt.
func TestSetupPXFExtensions_AnnotationPatchFailure_NonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return fmt.Errorf("patch boom")
			},
		}).Build()

	dbClient := &pxfExtDBClient{pxfInstalled: 1}
	factory := &mockDBClientFactory{client: dbClient}
	rec := &dataLoadCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, rec, nil)

	// Patch failure is logged, not surfaced: the function must not panic.
	require.NotPanics(t, func() {
		r.setupPXFExtensions(context.Background(), cluster, slog.Default())
	})

	// The install metric is recorded BEFORE the (failed) patch.
	assert.Equal(t, []string{pxfExtensionResultInstalled}, pxfResults(rec),
		"result=installed must be recorded even when the annotation patch fails")
}

// TestParseDataLoadRowsMessage covers the DATALOAD_ROWS marker parser.
func TestParseDataLoadRowsMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    int64
		ok      bool
	}{
		{"exact marker", "DATALOAD_ROWS=183961", 183961, true},
		{"marker in log tail", "some logs\nDATALOAD_ROWS=42\nmore", 42, true},
		{"zero rows", "DATALOAD_ROWS=0", 0, true},
		{"no marker", "no marker here", 0, false},
		{"marker no digits", "DATALOAD_ROWS=abc", 0, false},
		{"empty", "", 0, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseDataLoadRowsMessage(tc.message)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDataLoadJobStatusString covers the terminal-status string mapping.
func TestDataLoadJobStatusString(t *testing.T) {
	succeeded := &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}}
	failed := &batchv1.Job{Status: batchv1.JobStatus{Failed: 1}}
	start := metav1.Now()
	running := &batchv1.Job{Status: batchv1.JobStatus{Active: 1, StartTime: &start}}
	pending := &batchv1.Job{}

	assert.Equal(t, dataLoadStatusSucceeded, dataLoadJobStatusString(succeeded))
	assert.Equal(t, dataLoadStatusFailed, dataLoadJobStatusString(failed))
	assert.Equal(t, dataLoadStatusRunning, dataLoadJobStatusString(running))
	assert.Equal(t, dataLoadStatusPending, dataLoadJobStatusString(pending))
}

// TestDataLoadJobDuration covers the duration computation.
func TestDataLoadJobDuration(t *testing.T) {
	start := metav1.NewTime(time.Now().Add(-90 * time.Second))
	done := metav1.NewTime(time.Now())
	job := &batchv1.Job{Status: batchv1.JobStatus{StartTime: &start, CompletionTime: &done}}
	assert.NotEmpty(t, dataLoadJobDuration(job))
	assert.Positive(t, dataLoadJobDurationValue(job))

	// Missing completion => empty/zero.
	incomplete := &batchv1.Job{Status: batchv1.JobStatus{StartTime: &start}}
	assert.Empty(t, dataLoadJobDuration(incomplete))
	assert.Zero(t, dataLoadJobDurationValue(incomplete))
}

// TestDataLoadSourceType_Controller covers the controller-side source_type
// derivation (mirrors the builder's but is package-local).
func TestDataLoadSourceType_Controller(t *testing.T) {
	assert.Equal(t, "s3", dataLoadSourceType(cbv1alpha1.DataLoadingJob{
		Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{Profile: "s3:parquet"},
	}))
	assert.Equal(t, "jdbc", dataLoadSourceType(cbv1alpha1.DataLoadingJob{
		Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{Profile: "jdbc"},
	}))
	assert.Equal(t, "gpfdist", dataLoadSourceType(cbv1alpha1.DataLoadingJob{
		Type: "gpload", GploadJob: &cbv1alpha1.GploadJobSpec{},
	}))
}

// TestLatestDataLoadJobByName indexes the most recent Job per name label.
func TestLatestDataLoadJobByName(t *testing.T) {
	older := metav1.NewTime(time.Now().Add(-time.Hour))
	newer := metav1.NewTime(time.Now())
	jobs := []batchv1.Job{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "a-old",
				CreationTimestamp: older,
				Labels:            map[string]string{util.LabelDataLoadJob: "loader"},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "a-new",
				CreationTimestamp: newer,
				Labels:            map[string]string{util.LabelDataLoadJob: "loader"},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "no-label",
				Labels: map[string]string{},
			},
		},
	}
	out := latestDataLoadJobByName(jobs)
	require.Contains(t, out, "loader")
	assert.Equal(t, "a-new", out["loader"].Name)
	assert.Len(t, out, 1, "jobs without the label are ignored")
}

// TestDataLoadRowsFromPod covers extracting the marker from a pod.
func TestDataLoadRowsFromPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "dataload",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Message: "DATALOAD_ROWS=777"},
					},
				},
			},
		},
	}
	n, ok := dataLoadRowsFromPod(pod)
	assert.True(t, ok)
	assert.Equal(t, int64(777), n)

	// No terminated container => not found.
	empty := &corev1.Pod{}
	_, ok = dataLoadRowsFromPod(empty)
	assert.False(t, ok)
}

// ensure db.Client is satisfied by the tracking mock (compile-time check).
var _ db.Client = (*pxfExtDBClient)(nil)

// dataLoadMetricsRecorder captures the 5 data-loading metric calls for the
// in-package reconcile tests.
type dataLoadMetricsRecorder struct {
	metrics.NoopRecorder
	status      map[string]float64
	lastSuccess map[string]float64
	duration    map[string]time.Duration
	rows        map[string]float64
	source      map[string]string
	errors      map[string]int
}

func newDataLoadMetricsRecorder() *dataLoadMetricsRecorder {
	return &dataLoadMetricsRecorder{
		status:      map[string]float64{},
		lastSuccess: map[string]float64{},
		duration:    map[string]time.Duration{},
		rows:        map[string]float64{},
		source:      map[string]string{},
		errors:      map[string]int{},
	}
}

func (m *dataLoadMetricsRecorder) SetDataLoadingJobStatus(_, _, job string, s float64) {
	m.status[job] = s
}
func (m *dataLoadMetricsRecorder) SetDataLoadingJobLastSuccess(_, _, job string, ts float64) {
	m.lastSuccess[job] = ts
}
func (m *dataLoadMetricsRecorder) ObserveDataLoadingJobDuration(_, _, job string, d time.Duration) {
	m.duration[job] = d
}
func (m *dataLoadMetricsRecorder) RecordDataLoadingRows(_, _, job, src string, c float64) {
	m.rows[job] += c
	m.source[job] = src
}
func (m *dataLoadMetricsRecorder) RecordDataLoadingErrors(_, _, job string) {
	m.errors[job]++
}

func dlPxfJob(name string, enabled bool) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: name, Type: "pxf", Enabled: enabled,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server: "s3-datalake", Profile: "s3:parquet",
			Resource: "s3a://data-lake/events/", TargetTable: "public.events",
		},
	}
}

func dlGploadJob(name, schedule string, enabled bool) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: name, Type: "gpload", Enabled: enabled, Schedule: schedule,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.bulk_data", Format: "csv",
			FilePaths: []string{"/data/incoming/*.csv"},
		},
	}
}

func dlCluster(name string, jobs []cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Name = name
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase:          "Configured",
		ConfiguredJobs: int32(len(jobs)),
	}
	jobStatuses := make([]cbv1alpha1.DataLoadingJobStatus, 0, len(jobs))
	for _, j := range jobs {
		jobStatuses = append(jobStatuses, cbv1alpha1.DataLoadingJobStatus{Name: j.Name, Enabled: j.Enabled})
	}
	c.Status.DataLoading.Jobs = jobStatuses
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true, Jobs: jobs}
	return c
}

func dlReconciler(
	cluster *cbv1alpha1.CloudberryCluster, rec metrics.Recorder,
) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	return NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)
}

func TestReconcileDataLoadingJobs_EnabledPxfCreatesJob(t *testing.T) {
	cluster := dlCluster("dl-pxf", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})

	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))

	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(context.Background(), types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "loader"), Namespace: cluster.Namespace,
	}, job))
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Contains(t, job.Spec.Template.Spec.Containers[0].Args[0], "DATALOAD_ROWS=")
}

func TestReconcileDataLoadingJobs_ScheduledCreatesCronJob(t *testing.T) {
	cluster := dlCluster("dl-cron",
		[]cbv1alpha1.DataLoadingJob{dlGploadJob("nightly", "0 2 * * *", true)})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})

	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))

	cron := &batchv1.CronJob{}
	require.NoError(t, r.client.Get(context.Background(), types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "nightly"), Namespace: cluster.Namespace,
	}, cron))
	assert.Equal(t, "0 2 * * *", cron.Spec.Schedule)
}

func TestReconcileDataLoadingJobs_DisabledSkipped(t *testing.T) {
	cluster := dlCluster("dl-off", []cbv1alpha1.DataLoadingJob{dlPxfJob("off", false)})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})

	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))

	job := &batchv1.Job{}
	err := r.client.Get(context.Background(), types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "off"), Namespace: cluster.Namespace,
	}, job)
	assert.Error(t, err)
}

// dlReconcilerWith builds an AdminReconciler over a fake client seeded with the
// cluster plus interceptor funcs so the reconcileDataLoadingJobs error-path tests
// (MUST-2) can force a workload Create or a Job List failure.
func dlReconcilerWith(
	cluster *cbv1alpha1.CloudberryCluster,
	rec metrics.Recorder,
	funcs interceptor.Funcs,
) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(funcs).Build()
	return NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)
}

// TestReconcileDataLoadingJobs_WorkloadCreateError drives the
// ensureDataLoadingWorkloads error return of reconcileDataLoadingJobs (MUST-2,
// L212-214, previously UNCOVERED): the workload Job Create fails so the wrapped
// error is returned.
func TestReconcileDataLoadingJobs_WorkloadCreateError(t *testing.T) {
	cluster := dlCluster("dl-wlerr", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	r := dlReconcilerWith(cluster, &metrics.NoopRecorder{}, interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
			_ ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return fmt.Errorf("workload create boom")
			}
			return nil
		},
	})

	err := r.reconcileDataLoadingJobs(context.Background(), cluster)
	require.Error(t, err, "an ensureDataLoadingWorkloads failure must propagate")
	assert.Contains(t, err.Error(), "workload create boom")
}

// TestReconcileDataLoadingJobs_ListError drives the client.List error return of
// reconcileDataLoadingJobs (MUST-2, L225-227, previously UNCOVERED): the workload
// is created cleanly but the subsequent owned-Job List fails, so the wrapped
// "listing data loading jobs" error is returned.
func TestReconcileDataLoadingJobs_ListError(t *testing.T) {
	cluster := dlCluster("dl-listerr", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	r := dlReconcilerWith(cluster, &metrics.NoopRecorder{}, interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, list client.ObjectList,
			_ ...client.ListOption) error {
			if _, ok := list.(*batchv1.JobList); ok {
				return fmt.Errorf("list boom")
			}
			return nil
		},
	})

	err := r.reconcileDataLoadingJobs(context.Background(), cluster)
	require.Error(t, err, "the owned-Job List failure must propagate")
	assert.Contains(t, err.Error(), "listing data loading jobs")
}

func TestReconcileDataLoadingJobs_Idempotent(t *testing.T) {
	cluster := dlCluster("dl-idem", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})

	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))
	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, r.client.List(context.Background(), jobs))
	count := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelComponent] == util.ComponentDataLoad {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func TestReconcileDataLoadingJobs_SucceededEnrichesStatusAndMetrics(t *testing.T) {
	cluster := dlCluster("dl-ok", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	r := dlReconciler(cluster, rec)

	ctx := context.Background()
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	start := metav1.NewTime(time.Now().Add(-90 * time.Second))
	done := metav1.NewTime(time.Now())
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(t, r.client.Status().Update(ctx, job))

	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_ROWS=183961")

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	require.Len(t, cluster.Status.DataLoading.Jobs, 1)
	js := cluster.Status.DataLoading.Jobs[0]
	assert.Equal(t, "Succeeded", js.LastStatus)
	require.NotNil(t, js.RowsLoaded)
	assert.Equal(t, int64(183961), *js.RowsLoaded)
	require.NotNil(t, js.LastRun)
	assert.NotEmpty(t, js.Duration)

	assert.Equal(t, 2.0, rec.status["loader"])
	assert.NotZero(t, rec.lastSuccess["loader"])
	assert.Positive(t, rec.duration["loader"])
	assert.Equal(t, 183961.0, rec.rows["loader"])
	assert.Equal(t, "s3", rec.source["loader"])
}

func TestReconcileDataLoadingJobs_FailedRecordsError(t *testing.T) {
	cluster := dlCluster("dl-fail", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	r := dlReconciler(cluster, rec)

	ctx := context.Background()
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(t, r.client.Status().Update(ctx, job))

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	require.Len(t, cluster.Status.DataLoading.Jobs, 1)
	assert.Equal(t, "Failed", cluster.Status.DataLoading.Jobs[0].LastStatus)
	assert.Equal(t, 3.0, rec.status["loader"])
	assert.Equal(t, 1, rec.errors["loader"])
	assert.Zero(t, rec.rows["loader"])
}

func TestReconcileDataLoadingJobs_NoJobsNoOp(t *testing.T) {
	cluster := dlCluster("dl-none", nil)
	r := dlReconciler(cluster, &metrics.NoopRecorder{})
	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))
}

func TestReconcileDataLoadingJobs_MisconfiguredSkipped(t *testing.T) {
	cluster := dlCluster("dl-bad", []cbv1alpha1.DataLoadingJob{
		{Name: "bad-pxf", Type: "pxf", Enabled: true},                            // nil PxfJob
		{Name: "bad-cron", Type: "gpload", Enabled: true, Schedule: "* * * * *"}, // nil GploadJob
	})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})
	// Mis-configured jobs are skipped (logged), never error the reconcile.
	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))
}

// dlSteadyStateReconciler builds a reconciler with the cluster at steady state
// (ObservedGeneration == Generation) so refreshDataLoadingStatusOnSteadyState is
// the path under test. Status subresource is enabled for the patch.
func dlSteadyStateReconciler(
	cluster *cbv1alpha1.CloudberryCluster, rec metrics.Recorder,
) *AdminReconciler {
	cluster.Generation = 5
	cluster.Status.ObservedGeneration = 5
	return dlReconciler(cluster, rec)
}

// TestRefreshDataLoadingStatusOnSteadyState_RecreatesDeletedJob proves a
// data-loading Job deleted out from under the operator is re-created on the
// generation-gated steady-state path (mirrors backup status refresh).
func TestRefreshDataLoadingStatusOnSteadyState_RecreatesDeletedJob(t *testing.T) {
	cluster := dlCluster("dl-steady-recreate",
		[]cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	r := dlSteadyStateReconciler(cluster, &metrics.NoopRecorder{})
	ctx := context.Background()

	// First steady-state pass creates the Job.
	r.refreshDataLoadingStatusOnSteadyState(ctx, cluster)
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	key := types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx, key, job))

	// Delete the Job out from under the operator.
	require.NoError(t, r.client.Delete(ctx, job))
	require.Error(t, r.client.Get(ctx, key, &batchv1.Job{}))

	// The NEXT steady-state requeue must re-create it (no generation bump).
	r.refreshDataLoadingStatusOnSteadyState(ctx, cluster)
	require.NoError(t, r.client.Get(ctx, key, &batchv1.Job{}),
		"deleted dataload Job must be re-created on the steady-state path")
}

// TestRefreshDataLoadingStatusOnSteadyState_RefreshesStatusWithoutGenBump proves
// a Succeeded Job's terminal status + rows are harvested into status on the
// steady-state path without a generation bump.
func TestRefreshDataLoadingStatusOnSteadyState_RefreshesStatusWithoutGenBump(t *testing.T) {
	cluster := dlCluster("dl-steady-status",
		[]cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadMetricsRecorder()
	r := dlSteadyStateReconciler(cluster, rec)
	ctx := context.Background()

	// First pass creates the Job.
	r.refreshDataLoadingStatusOnSteadyState(ctx, cluster)
	jobName := util.DataLoadJobName(cluster.Name, "loader")
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))

	// Mark the Job Succeeded with a harvestable rowcount.
	start := metav1.NewTime(time.Now().Add(-30 * time.Second))
	done := metav1.NewTime(time.Now())
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(t, r.client.Status().Update(ctx, job))
	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_ROWS=183961")

	genBefore := cluster.Generation
	r.refreshDataLoadingStatusOnSteadyState(ctx, cluster)

	require.Len(t, cluster.Status.DataLoading.Jobs, 1)
	js := cluster.Status.DataLoading.Jobs[0]
	assert.Equal(t, dataLoadStatusSucceeded, js.LastStatus)
	require.NotNil(t, js.RowsLoaded)
	assert.Equal(t, int64(183961), *js.RowsLoaded)
	assert.Equal(t, 2.0, rec.status["loader"])
	assert.Equal(t, 183961.0, rec.rows["loader"])
	assert.Equal(t, genBefore, cluster.Generation,
		"steady-state status refresh must not bump the spec generation")
}

// TestRefreshDataLoadingStatusOnSteadyState_PreservesPXFStatus proves the
// steady-state refresh preserves the spec-derived PXF summary
// (status.dataLoading.pxf.{configured,servers}) so a status patch here never
// drops it.
func TestRefreshDataLoadingStatusOnSteadyState_PreservesPXFStatus(t *testing.T) {
	cluster := dlCluster("dl-steady-pxf",
		[]cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	cluster.Spec.DataLoading.Pxf = &cbv1alpha1.PxfSpec{
		Enabled: true,
		Image:   "cloudberry-pxf:2.1.0",
		Servers: []cbv1alpha1.PxfServerSpec{
			{Name: "s3", Type: "s3"},
			{Name: "jdbc", Type: "jdbc"},
		},
	}
	r := dlSteadyStateReconciler(cluster, &metrics.NoopRecorder{})

	r.refreshDataLoadingStatusOnSteadyState(context.Background(), cluster)

	require.NotNil(t, cluster.Status.DataLoading.Pxf)
	assert.True(t, cluster.Status.DataLoading.Pxf.Configured)
	assert.Equal(t, int32(2), cluster.Status.DataLoading.Pxf.Servers)
}

// TestRefreshDataLoadingStatusOnSteadyState_DisabledNoOp proves a disabled/absent
// data-loading spec is a clean no-op (no Jobs, no panic).
func TestRefreshDataLoadingStatusOnSteadyState_DisabledNoOp(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Generation = 3
	cluster.Status.ObservedGeneration = 3
	// No DataLoading spec at all.
	r := dlReconciler(cluster, &metrics.NoopRecorder{})

	r.refreshDataLoadingStatusOnSteadyState(context.Background(), cluster)

	jobs := &batchv1.JobList{}
	require.NoError(t, r.client.List(context.Background(), jobs))
	assert.Empty(t, jobs.Items, "disabled data loading must create no Jobs")
}

// injectDataLoadPodCtrl creates a terminated pod with the DATALOAD_ROWS marker.
func injectDataLoadPodCtrl(
	t *testing.T,
	r *AdminReconciler,
	cluster *cbv1alpha1.CloudberryCluster,
	jobName, message string,
) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{"job-name": jobName},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dataload", Image: "img"}}},
	}
	require.NoError(t, r.client.Create(ctx, pod))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "dataload",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Message: message},
			},
		},
	}
	require.NoError(t, r.client.Status().Update(ctx, pod))
}
