package controller

import (
	"context"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Scenario 109 — controller wiring for M.1 (pxf_service_up) and M.10
// (data_loading_bytes_total) + M.14 (job_status cycle).
//
// HONESTY: pxf_service_up is emitted per OBSERVED segment host (no pods → no
// calls); data_loading_bytes_total is recorded ONLY when a real DATALOAD_BYTES
// marker is harvested (no marker → no call).
// ============================================================================

// pxfServiceUpRecorder captures SetPXFServiceUp calls per segment_host so the
// observePxfStatus wiring can be asserted (109-M1-F at unit level). It embeds
// NoopRecorder so all other Recorder methods are no-ops.
type pxfServiceUpRecorder struct {
	metrics.NoopRecorder
	serviceUp map[string]float64
	calls     int
}

func newPXFServiceUpRecorder() *pxfServiceUpRecorder {
	return &pxfServiceUpRecorder{serviceUp: map[string]float64{}}
}

func (m *pxfServiceUpRecorder) SetPXFServiceUp(_, _, segmentHost string, up float64) {
	m.serviceUp[segmentHost] = up
	m.calls++
}

// dataLoadBytesRecorder captures the M.10 RecordDataLoadingBytes calls (with the
// rows/status/errors fields from dataLoadMetricsRecorder reused) so the harvest
// wiring can assert bytes are recorded ONLY when the marker is present.
type dataLoadBytesRecorder struct {
	dataLoadMetricsRecorder
	bytes      map[string]float64
	bytesSrc   map[string]string
	bytesCalls int
}

func newDataLoadBytesRecorder() *dataLoadBytesRecorder {
	return &dataLoadBytesRecorder{
		dataLoadMetricsRecorder: *newDataLoadMetricsRecorder(),
		bytes:                   map[string]float64{},
		bytesSrc:                map[string]string{},
	}
}

func (m *dataLoadBytesRecorder) RecordDataLoadingBytes(_, _, job, src string, b float64) {
	m.bytes[job] += b
	m.bytesSrc[job] = src
	m.bytesCalls++
}

// ---------------------------------------------------------------------------
// M.1 — observePxfStatus emits pxf_service_up per observed host (109-M1-F)
// ---------------------------------------------------------------------------

// TestObservePxfStatus_ServiceUpPerHost drives observePxfStatus with a fake
// client returning segment-primary pods of mixed "pxf" readiness and asserts
// SetPXFServiceUp is called per host with the right 1/0 from real readiness. A
// host missing the pxf container reports 0 (honest: not observably up).
func TestObservePxfStatus_ServiceUpPerHost(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	pods := []*corev1.Pod{
		pxfSegmentPrimaryPod("seg-0", cluster.Name, cluster.Namespace, true, true),
		pxfSegmentPrimaryPod("seg-1", cluster.Name, cluster.Namespace, false, true),
		pxfSegmentPrimaryPod("seg-2", cluster.Name, cluster.Namespace, false, false), // no pxf → 0
	}
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster)
	for _, p := range pods {
		builder = builder.WithObjects(p)
	}
	k8sClient := builder.WithStatusSubresource(cluster).Build()

	rec := newPXFServiceUpRecorder()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		newBuilderForScenario109(), nil, rec, nil)

	status := r.observePxfStatus(context.Background(), cluster)

	// Aggregate status is the honest "Error" (some but not all ready).
	assert.Equal(t, util.PXFStatusError, status)

	// Per-host gauge: one call per observed pod, with the right value.
	assert.Equal(t, 3, rec.calls, "one SetPXFServiceUp per observed segment host")
	assert.Equal(t, 1.0, rec.serviceUp["seg-0"], "ready pxf → 1")
	assert.Equal(t, 0.0, rec.serviceUp["seg-1"], "not-ready pxf → 0")
	assert.Equal(t, 0.0, rec.serviceUp["seg-2"], "missing pxf container → 0 (honest)")
}

// TestObservePxfStatus_ServiceUp_AllReady covers the all-ready case: every
// observed host's gauge is 1 and the aggregate status is Running.
func TestObservePxfStatus_ServiceUp_AllReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster,
			pxfSegmentPrimaryPod("seg-0", cluster.Name, cluster.Namespace, true, true),
			pxfSegmentPrimaryPod("seg-1", cluster.Name, cluster.Namespace, true, true),
		).
		WithStatusSubresource(cluster).Build()

	rec := newPXFServiceUpRecorder()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		newBuilderForScenario109(), nil, rec, nil)

	status := r.observePxfStatus(context.Background(), cluster)
	assert.Equal(t, util.PXFStatusRunning, status)
	assert.Equal(t, 2, rec.calls)
	assert.Equal(t, 1.0, rec.serviceUp["seg-0"])
	assert.Equal(t, 1.0, rec.serviceUp["seg-1"])
}

// TestObservePxfStatus_NoPods_NoServiceUp covers 109-M1-F honesty: with NO
// segment-primary pods observed, SetPXFServiceUp is NEVER called — no host is
// fabricated — and the aggregate status is absent ("").
func TestObservePxfStatus_NoPods_NoServiceUp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()

	rec := newPXFServiceUpRecorder()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		newBuilderForScenario109(), nil, rec, nil)

	status := r.observePxfStatus(context.Background(), cluster)
	assert.Empty(t, status, "no pods → status absent")
	assert.Zero(t, rec.calls, "no observed host → no pxf_service_up emission (honest)")
	assert.Empty(t, rec.serviceUp)
}

// newBuilderForScenario109 returns the default builder used by the controller
// tests (kept as a tiny helper so the scenario file is self-documenting).
func newBuilderForScenario109() *builder.DefaultBuilder {
	return builder.NewBuilder()
}

// ---------------------------------------------------------------------------
// M.10 — DATALOAD_BYTES parse / harvest / record (109-M10-U / 109-M10-ABSENT)
// ---------------------------------------------------------------------------

// TestParseDataLoadBytesMessage covers the DATALOAD_BYTES marker parser
// (109-M10-U): a valid marker → (n, true); absent/malformed → (0, false); a
// marker in the FallbackToLogsOnError tail is still found.
func TestParseDataLoadBytesMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    int64
		ok      bool
	}{
		{"exact marker", "DATALOAD_BYTES=12345", 12345, true},
		{"marker in log tail", "staged input\nDATALOAD_BYTES=4096\nwc done", 4096, true},
		{"zero bytes", "DATALOAD_BYTES=0", 0, true},
		{"rows marker present too", "DATALOAD_ROWS=10\nDATALOAD_BYTES=512", 512, true},
		{"no marker", "no bytes here", 0, false},
		{"marker no digits", "DATALOAD_BYTES=abc", 0, false},
		{"marker empty value", "DATALOAD_BYTES=", 0, false},
		{"empty message", "", 0, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseDataLoadBytesMessage(tc.message)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDataLoadBytesFromPod covers extracting the DATALOAD_BYTES marker from a
// pod's terminated container message (mirrors dataLoadRowsFromPod).
func TestDataLoadBytesFromPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "gpload",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Message: "DATALOAD_ROWS=7\nDATALOAD_BYTES=98765",
						},
					},
				},
			},
		},
	}
	n, ok := dataLoadBytesFromPod(pod)
	assert.True(t, ok)
	assert.Equal(t, int64(98765), n)

	// No terminated container → not found (honest absence).
	_, ok = dataLoadBytesFromPod(&corev1.Pod{})
	assert.False(t, ok)

	// Terminated but no bytes marker → not found.
	noBytes := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "gpload",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Message: "DATALOAD_ROWS=7"},
					},
				},
			},
		},
	}
	_, ok = dataLoadBytesFromPod(noBytes)
	assert.False(t, ok)

	// A running container (nil Terminated) and a terminated-but-empty-message
	// container are both skipped (the continue branch); a later container with a
	// real marker is still found — proving the loop scans past unobservable ones.
	mixed := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "init", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				{Name: "empty", State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: ""}}},
				{Name: "gpload", State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: "DATALOAD_BYTES=321"}}},
			},
		},
	}
	n, ok = dataLoadBytesFromPod(mixed)
	assert.True(t, ok)
	assert.Equal(t, int64(321), n)
}

// TestHarvestDataLoadBytes_ListErrorNonFatal covers the harvestDataLoadBytes
// non-fatal list-error contract: when listing the Job's pods FAILS the harvest
// returns (0, false) and never errors reconcile — an unobservable harvest leaves
// the bytes metric honestly absent.
func TestHarvestDataLoadBytes_ListErrorNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := dlCluster("dl-bytes-listerr", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				ctx context.Context, c client.WithWatch, list client.ObjectList,
				opts ...client.ListOption,
			) error {
				if _, ok := list.(*corev1.PodList); ok {
					return assert.AnError
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		newBuilderForScenario109(), nil, &metrics.NoopRecorder{}, nil)

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: cluster.Namespace}}
	n, ok := r.harvestDataLoadBytes(context.Background(), cluster, job)
	assert.False(t, ok, "a pod-list error must leave bytes harvest unobservable")
	assert.Zero(t, n)
}

// TestReconcileDataLoadingJobs_RecordsBytesWhenMarkerPresent covers 109-M10-F:
// a Succeeded Job whose pod carries a DATALOAD_BYTES marker harvests the bytes
// and RecordDataLoadingBytes is called with the right job/source_type/value.
func TestReconcileDataLoadingJobs_RecordsBytesWhenMarkerPresent(t *testing.T) {
	cluster := dlCluster("dl-bytes-ok", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadBytesRecorder()
	r := dlReconciler(cluster, rec)

	ctx := context.Background()
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	start := metav1.NewTime(time.Now().Add(-30 * time.Second))
	done := metav1.NewTime(time.Now())
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(t, r.client.Status().Update(ctx, job))

	// Pod carries BOTH markers; the bytes harvest must consume DATALOAD_BYTES.
	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_ROWS=100\nDATALOAD_BYTES=204800")

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	assert.Equal(t, 1, rec.bytesCalls, "bytes recorded exactly once for the succeeded job")
	assert.Equal(t, 204800.0, rec.bytes["loader"])
	assert.Equal(t, "s3", rec.bytesSrc["loader"], "source_type matches the rows metric")
	// Rows are still harvested independently.
	assert.Equal(t, 100.0, rec.rows["loader"])
}

// TestReconcileDataLoadingJobs_NoBytesWhenMarkerAbsent covers 109-M10-ABSENT:
// a Succeeded Job whose pod carries ONLY the rows marker (no DATALOAD_BYTES)
// records rows but NEVER calls RecordDataLoadingBytes — the bytes metric stays
// honestly absent for that job (never synthesized).
func TestReconcileDataLoadingJobs_NoBytesWhenMarkerAbsent(t *testing.T) {
	cluster := dlCluster("dl-bytes-absent", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadBytesRecorder()
	r := dlReconciler(cluster, rec)

	ctx := context.Background()
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	start := metav1.NewTime(time.Now().Add(-30 * time.Second))
	done := metav1.NewTime(time.Now())
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(t, r.client.Status().Update(ctx, job))

	// Only the rows marker is present — no honest byte count for this load.
	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_ROWS=100")

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	assert.Zero(t, rec.bytesCalls, "no DATALOAD_BYTES marker → bytes NOT recorded (honest)")
	assert.Zero(t, rec.bytes["loader"])
	// Rows are still recorded (the rows path is independent of bytes).
	assert.Equal(t, 100.0, rec.rows["loader"])
}

// TestReconcileDataLoadingJobs_NoBytesOnFailure covers 109-M10-ABSENT for the
// failure branch: a Failed Job never harvests bytes (the harvest is gated to the
// succeeded branch), so RecordDataLoadingBytes is not called even if a marker
// somehow existed.
func TestReconcileDataLoadingJobs_NoBytesOnFailure(t *testing.T) {
	cluster := dlCluster("dl-bytes-fail", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadBytesRecorder()
	r := dlReconciler(cluster, rec)

	ctx := context.Background()
	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	jobName := util.DataLoadJobName(cluster.Name, "loader")
	job := &batchv1.Job{}
	require.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(t, r.client.Status().Update(ctx, job))
	injectDataLoadPodCtrl(t, r, cluster, jobName, "DATALOAD_BYTES=4096")

	require.NoError(t, r.reconcileDataLoadingJobs(ctx, cluster))

	assert.Zero(t, rec.bytesCalls, "failed job must not record bytes (harvest is success-gated)")
	assert.Equal(t, 1, rec.errors["loader"], "failure increments the honest errors counter")
}

// ---------------------------------------------------------------------------
// M.14 — data_loading_job_status lifecycle 0→1→2 and →3 (109-M14-CYCLE)
// ---------------------------------------------------------------------------

// TestDataLoadJobStatusCode_Cycle covers 109-M14-CYCLE: the dataLoadJobStatusCode
// mapping drives the job_status gauge through its full lifecycle — 0 (pending),
// 1 (running), 2 (success), and 3 (failed) — each from a real Kubernetes Job
// status, asserting every transition value.
func TestDataLoadJobStatusCode_Cycle(t *testing.T) {
	start := metav1.Now()
	tests := []struct {
		name string
		job  *batchv1.Job
		want float64
	}{
		{
			name: "pending → 0",
			job:  &batchv1.Job{},
			want: backupJobStatusPending,
		},
		{
			name: "running → 1",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Active: 1, StartTime: &start}},
			want: backupJobStatusRunning,
		},
		{
			name: "success → 2",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}},
			want: backupJobStatusSucceeded,
		},
		{
			name: "failed → 3",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Failed: 1,
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
				},
			}},
			want: backupJobStatusFailed,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, dataLoadJobStatusCode(tc.job))
		})
	}
}

// TestRecordDataLoadJobMetrics_StatusGaugeTracksCode covers 109-M14-CYCLE at the
// metric-emission boundary: recordDataLoadJobMetrics always sets the status gauge
// to the current code, and a failure flips it to 3 while incrementing errors.
func TestRecordDataLoadJobMetrics_StatusGaugeTracksCode(t *testing.T) {
	cluster := dlCluster("dl-status-cycle", []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)})
	rec := newDataLoadBytesRecorder()
	r := dlReconciler(cluster, rec)
	specJob := dlPxfJob("loader", true)

	// running → status 1.
	start := metav1.Now()
	running := &batchv1.Job{Status: batchv1.JobStatus{Active: 1, StartTime: &start}}
	r.recordDataLoadJobMetrics(cluster, specJob, running, 0, false, 0, false)
	assert.Equal(t, backupJobStatusRunning, rec.status["loader"])

	// success → status 2 (+ bytes recorded because haveBytes==true).
	done := metav1.Now()
	succeeded := &batchv1.Job{Status: batchv1.JobStatus{
		Succeeded: 1, StartTime: &start, CompletionTime: &done,
	}}
	r.recordDataLoadJobMetrics(cluster, specJob, succeeded, 50, true, 8192, true)
	assert.Equal(t, backupJobStatusSucceeded, rec.status["loader"])
	assert.Equal(t, 8192.0, rec.bytes["loader"])

	// failure → status 3 (+ errors incremented, no bytes recorded).
	failed := &batchv1.Job{Status: batchv1.JobStatus{
		Failed: 1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		},
	}}
	r.recordDataLoadJobMetrics(cluster, specJob, failed, 0, false, 0, false)
	assert.Equal(t, backupJobStatusFailed, rec.status["loader"])
	assert.Equal(t, 1, rec.errors["loader"])
	// Bytes total unchanged after the failure (still the one success record).
	assert.Equal(t, 8192.0, rec.bytes["loader"])
}

// ensure the bytes recorder satisfies the Recorder interface (compile-time).
var _ metrics.Recorder = (*dataLoadBytesRecorder)(nil)
var _ metrics.Recorder = (*pxfServiceUpRecorder)(nil)
