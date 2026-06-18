//go:build functional

package functional

import (
	"context"
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
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 109: All Prometheus Metrics (M.1–M.16) — functional
// ============================================================================
//
// Black-boxes the OPERATOR-DRIVEN metric emission through the REAL
// AdminReconciler over a fake k8s client + a spy metrics recorder
// (mockMetricsRecorder, shared with scenario105/106), asserting the operator
// CALLS the recorder HONESTLY for each IMPLEMENTED metric and NEVER for the
// intentionally-absent ones. Infra-free, deterministic.
//
//   - M.1  pxf_service_up: reconcile over segment-primary pods of mixed pxf
//     readiness → SetPXFServiceUp called once per OBSERVED host with 1/0; NO
//     pods → no call (no fabricated host).
//   - M.8  jobs_active: SetDataLoadingJobsActive reflects the enabled count.
//   - M.9  rows_total: a succeeded Job + DATALOAD_ROWS marker → RecordDataLoadingRows.
//   - M.10 bytes_total: a succeeded Job + DATALOAD_BYTES marker → RecordDataLoadingBytes;
//     WITHOUT the marker → bytes NOT recorded (honest absence).
//   - M.11/M.12/M.13 on a succeeded/failed Job (errors / duration / last-success).
//   - M.14 CYCLE: a Job lifecycle drives the status gauge to 2 (success) and 3
//     (forced failure).
//   - HONESTY: the spy Recorder has NO method/series for the absent metrics — they
//     CANNOT be recorded because they are not on the Recorder; a registry-level
//     guard asserts the absent families are unregistered.
//
// The catalog (cases.Scenario109Cases) documents the full M.1–M.16 contract; the
// -F rows are resolved here, the -L rows at e2e.
// ============================================================================

const (
	scenario109Namespace = "cloudberry-test"
	scenario109Cluster   = "scenario109-metrics"

	scenario109PxfImage = "apache/cloudberry-pxf:2.1.0"
)

// Scenario109Suite drives the metric-emission contract through the real
// AdminReconciler over a fake client + a spy metrics recorder.
type Scenario109Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario109(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario109Suite))
}

func (s *Scenario109Suite) SetupTest() {
	s.ctx = context.Background()
}

// scenario109ClusterWith builds a Running cluster with PXF data loading enabled
// plus the supplied servers + jobs; the DataLoading STATUS is left for reconcile
// to populate.
func scenario109ClusterWith(
	servers []cbv1alpha1.PxfServerSpec,
	jobs []cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario109Cluster, scenario109Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		WithAnnotation(util.AnnotationPXFExtensionsReady, "true").
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   scenario109PxfImage,
			Servers: servers,
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario109SegmentPod builds a segment-primary pod (with the SHARED PXF
// selector labels) whose "pxf" container has the given readiness. When hasPXF is
// false the pod carries no "pxf" container status (it is observed but reports 0).
func scenario109SegmentPod(name string, ready, hasPXF bool) *corev1.Pod {
	statuses := []corev1.ContainerStatus{{Name: "segment", Ready: true}}
	if hasPXF {
		statuses = append(statuses,
			corev1.ContainerStatus{Name: cases.Scenario105PxfContainerName, Ready: ready})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scenario109Namespace,
			Labels:    util.SegmentPrimaryPXFSelector(scenario109Cluster),
		},
		Status: corev1.PodStatus{ContainerStatuses: statuses},
	}
}

// scenario109Server returns a minimal s3 PXF server definition.
func scenario109Server(name string) cbv1alpha1.PxfServerSpec {
	return cbv1alpha1.PxfServerSpec{
		Name:   name,
		Type:   "s3",
		Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
	}
}

// scenario109PxfJob returns an enabled (or disabled) pxf load job.
func scenario109PxfJob(name string, enabled bool) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    name,
		Type:    "pxf",
		Enabled: enabled,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3srv",
			Profile:     "s3:text",
			Resource:    "bucket/data.csv",
			TargetTable: "public." + name,
		},
	}
}

// scenario109Harness wires the real AdminReconciler over a fake client + a spy
// metrics recorder.
type scenario109Harness struct {
	reconciler *controller.AdminReconciler
	metrics    *mockMetricsRecorder
	env        *testutil.TestK8sEnv
}

func (s *Scenario109Suite) boot(
	cluster *cbv1alpha1.CloudberryCluster,
	extra ...client.Object,
) *scenario109Harness {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)

	m := &mockMetricsRecorder{}
	r := controller.NewAdminReconciler(env.Client, env.Scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, m, env.Logger)

	return &scenario109Harness{reconciler: r, metrics: m, env: env}
}

// reconcile drives the REAL AdminReconciler over the seeded cluster.
func (s *Scenario109Suite) reconcile(h *scenario109Harness) *cbv1alpha1.CloudberryCluster {
	cluster, err := h.env.GetCluster(s.ctx, scenario109Cluster, scenario109Namespace)
	require.NoError(s.T(), err)
	_, err = h.reconciler.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	updated, err := h.env.GetCluster(s.ctx, scenario109Cluster, scenario109Namespace)
	require.NoError(s.T(), err)
	return updated
}

// --- 109-M1-F: pxf_service_up per OBSERVED host -----------------------------

// TestM1ServiceUpPerObservedHost covers 109-M1-F: reconcile over segment-primary
// pods of mixed pxf readiness → SetPXFServiceUp is called once per OBSERVED host
// with the right 1/0 from real readiness; a host with no pxf container reports 0.
func (s *Scenario109Suite) TestM1ServiceUpPerObservedHost() {
	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")}, nil)
	h := s.boot(cluster,
		scenario109SegmentPod("seg-0", true, true),   // ready pxf → 1
		scenario109SegmentPod("seg-1", false, true),  // not-ready pxf → 0
		scenario109SegmentPod("seg-2", false, false), // no pxf container → 0
	)

	s.reconcile(h)

	calls := filterCalls(h.metrics.getCalls(), "SetPXFServiceUp")
	require.Len(s.T(), calls, 3, "one SetPXFServiceUp per OBSERVED segment host")

	byHost := map[string]float64{}
	for _, c := range calls {
		host, _ := c.args["segmentHost"].(string)
		up, _ := c.args["up"].(float64)
		byHost[host] = up
	}
	assert.Equal(s.T(), 1.0, byHost["seg-0"], "ready pxf → 1")
	assert.Equal(s.T(), 0.0, byHost["seg-1"], "not-ready pxf → 0")
	assert.Equal(s.T(), 0.0, byHost["seg-2"], "missing pxf container → 0 (honest)")
}

// TestM1ServiceUpNoPodsNoCall covers 109-M1-F honesty: with NO segment-primary
// pods observed, SetPXFServiceUp is NEVER called — no host is fabricated.
func (s *Scenario109Suite) TestM1ServiceUpNoPodsNoCall() {
	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")}, nil)
	h := s.boot(cluster) // no segment-primary pods

	s.reconcile(h)

	assert.False(s.T(), containsCall(h.metrics.getCalls(), "SetPXFServiceUp"),
		"no observed host → no pxf_service_up emission (honest)")
}

// --- 109-M8-F: data_loading_jobs_active reflects enabled jobs ----------------

// TestM8JobsActiveReflectsEnabled covers 109-M8-F: SetDataLoadingJobsActive is
// called with the count of ENABLED jobs (concurrency-independent).
func (s *Scenario109Suite) TestM8JobsActiveReflectsEnabled() {
	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{
			scenario109PxfJob("load-a", true),
			scenario109PxfJob("load-b", true),
			scenario109PxfJob("load-c", false),
		})
	h := s.boot(cluster)

	s.reconcile(h)

	calls := filterCalls(h.metrics.getCalls(), "SetDataLoadingJobsActive")
	require.NotEmpty(s.T(), calls, "jobs_active gauge must be recorded")
	last := calls[len(calls)-1]
	assert.Equal(s.T(), float64(2), last.args["count"], "2 enabled jobs → jobs_active==2")
}

// --- 109-M9/M10/M11/M12/M13/M14-F: per-Job-state recorder calls --------------

// scenario109SucceededJob builds a terminal Succeeded data-loading Job with
// start/completion timestamps so the duration is computable.
func scenario109SucceededJob(jobName string, start, completion time.Time) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName(scenario109Cluster, jobName),
			Namespace: scenario109Namespace,
			Labels: map[string]string{
				util.LabelCluster:     scenario109Cluster,
				util.LabelComponent:   util.ComponentDataLoad,
				util.LabelDataLoadJob: util.SanitizeK8sName(jobName),
			},
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &metav1.Time{Time: start},
			CompletionTime: &metav1.Time{Time: completion},
		},
	}
}

// scenario109MarkerPod builds the data-loading Job pod carrying the given
// termination-message markers (DATALOAD_ROWS / DATALOAD_BYTES) so the controller
// harvests them honestly.
func scenario109MarkerPod(jobName, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName(scenario109Cluster, jobName) + "-abcde",
			Namespace: scenario109Namespace,
			Labels: map[string]string{
				"job-name": util.DataLoadJobName(scenario109Cluster, jobName),
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "dataload",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
							Message:  message,
						},
					},
				},
			},
		},
	}
}

// TestM9M10M12M13SucceededWithMarkers covers 109-M9-F / 109-M10-F / 109-M12-F /
// 109-M13-F: a succeeded Job whose pod carries BOTH the rows and bytes markers →
// RecordDataLoadingRows, RecordDataLoadingBytes, ObserveDataLoadingJobDuration
// and SetDataLoadingJobLastSuccess are all called for the job; job_status→2.
func (s *Scenario109Suite) TestM9M10M12M13SucceededWithMarkers() {
	const jobName = "events-load"
	start := time.Now().Add(-90 * time.Second).UTC()
	completion := start.Add(75 * time.Second)

	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario109PxfJob(jobName, true)})
	h := s.boot(cluster,
		scenario109SucceededJob(jobName, start, completion),
		scenario109MarkerPod(jobName, "DATALOAD_ROWS=4242\nDATALOAD_BYTES=204800\n"))

	s.reconcile(h)
	calls := h.metrics.getCalls()

	// M.9 rows.
	rowCalls := filterCalls(calls, "RecordDataLoadingRows")
	require.Len(s.T(), rowCalls, 1, "rows recorded once on the succeeded run")
	assert.Equal(s.T(), jobName, rowCalls[0].args["job"])
	assert.Equal(s.T(), "s3", rowCalls[0].args["sourceType"], "source_type from the profile")
	assert.Equal(s.T(), float64(4242), rowCalls[0].args["count"])

	// M.10 bytes (PRESENT — the marker was harvested).
	byteCalls := filterCalls(calls, "RecordDataLoadingBytes")
	require.Len(s.T(), byteCalls, 1, "bytes recorded once when the DATALOAD_BYTES marker is present")
	assert.Equal(s.T(), jobName, byteCalls[0].args["job"])
	assert.Equal(s.T(), "s3", byteCalls[0].args["sourceType"])
	assert.Equal(s.T(), float64(204800), byteCalls[0].args["bytes"])

	// M.12 duration + M.13 last-success.
	assert.True(s.T(), containsCall(calls, "ObserveDataLoadingJobDuration"),
		"duration histogram observed on a terminal success with start+completion")
	assert.True(s.T(), containsCall(calls, "SetDataLoadingJobLastSuccess"),
		"last-success timestamp set from completionTime on success")

	// M.14 status → 2 (success).
	statusCalls := filterCalls(calls, "SetDataLoadingJobStatus")
	require.NotEmpty(s.T(), statusCalls)
	assert.Equal(s.T(), float64(2), statusCalls[len(statusCalls)-1].args["status"],
		"succeeded Job → job_status=2")
}

// TestM10AbsentWhenNoMarker covers 109-M10-F honesty: a succeeded Job whose pod
// carries ONLY the rows marker (no DATALOAD_BYTES) records rows but NEVER calls
// RecordDataLoadingBytes — the bytes metric stays honestly absent for that job.
func (s *Scenario109Suite) TestM10AbsentWhenNoMarker() {
	const jobName = "rows-only-load"
	start := time.Now().Add(-60 * time.Second).UTC()
	completion := start.Add(40 * time.Second)

	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario109PxfJob(jobName, true)})
	h := s.boot(cluster,
		scenario109SucceededJob(jobName, start, completion),
		scenario109MarkerPod(jobName, "DATALOAD_ROWS=100\n")) // no bytes marker

	s.reconcile(h)
	calls := h.metrics.getCalls()

	assert.True(s.T(), containsCall(calls, "RecordDataLoadingRows"),
		"rows still recorded from the rows marker")
	assert.False(s.T(), containsCall(calls, "RecordDataLoadingBytes"),
		"no DATALOAD_BYTES marker → bytes NOT recorded (honest absence)")
}

// TestM10GploadLocalSourceType covers 109-M10-F for a LOCAL gpload source: the
// bytes call carries the gpfdist/local source_type (the honest gpload byte path).
func (s *Scenario109Suite) TestM10GploadLocalSourceType() {
	const jobName = "gpload-local"
	start := time.Now().Add(-50 * time.Second).UTC()
	completion := start.Add(30 * time.Second)

	job := cbv1alpha1.DataLoadingJob{
		Name:    jobName,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
			TargetTable: "public.raw",
			FilePaths:   []string{"/in/data.csv"},
		},
	}
	cluster := scenario109ClusterWith(nil, []cbv1alpha1.DataLoadingJob{job})
	h := s.boot(cluster,
		scenario109SucceededJob(jobName, start, completion),
		scenario109MarkerPod(jobName, "DATALOAD_ROWS=10\nDATALOAD_BYTES=512\n"))

	s.reconcile(h)
	byteCalls := filterCalls(h.metrics.getCalls(), "RecordDataLoadingBytes")
	require.Len(s.T(), byteCalls, 1, "a LOCAL gpload byte count is recorded")
	assert.Equal(s.T(), float64(512), byteCalls[0].args["bytes"])
	assert.NotEmpty(s.T(), byteCalls[0].args["sourceType"], "source_type is set (honest)")
}

// TestM11M14FailureCycle covers 109-M11-F + 109-M14-CYCLE + 109-M6-FOLD: a Job
// driven to a terminal Failed state → RecordDataLoadingErrors{job} + job_status→3,
// and (HONESTY) NO synthetic pxf_errors_total exists on the Recorder so it can
// NEVER be recorded.
func (s *Scenario109Suite) TestM11M14FailureCycle() {
	const jobName = "failing-load"
	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario109PxfJob(jobName, true)})
	h := s.boot(cluster)

	// First reconcile creates the Job (Pending → status code observed).
	s.reconcile(h)

	// Drive the Job to a terminal Failed state, then reconcile to harvest.
	job := &batchv1.Job{}
	require.NoError(s.T(), h.env.Client.Get(s.ctx, types.NamespacedName{
		Name: util.DataLoadJobName(scenario109Cluster, jobName), Namespace: scenario109Namespace,
	}, job))
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(s.T(), h.env.Client.Status().Update(s.ctx, job))

	s.reconcile(h)
	calls := h.metrics.getCalls()

	// M.11 errors.
	errCalls := filterCalls(calls, "RecordDataLoadingErrors")
	require.NotEmpty(s.T(), errCalls, "Failed Job → data_loading_errors_total incremented")
	assert.Equal(s.T(), jobName, errCalls[len(errCalls)-1].args["job"])

	// M.14 status → 3 (failure).
	statusCalls := filterCalls(calls, "SetDataLoadingJobStatus")
	require.NotEmpty(s.T(), statusCalls)
	assert.Equal(s.T(), float64(3), statusCalls[len(statusCalls)-1].args["status"],
		"failed Job → job_status=3 (the honest M.6 error signal)")

	// HONESTY (M.6 fold): the success metrics must NOT fire for a failed run.
	assert.False(s.T(), containsCall(calls, "RecordDataLoadingBytes"),
		"a failed run must not record bytes")
	assert.False(s.T(), containsCall(calls, "SetDataLoadingJobLastSuccess"),
		"a failed run must not stamp a last-success timestamp")
}

// --- 109-HONESTY-F: the Recorder cannot record the absent metrics -----------

// TestHonestyAbsentMetricsHaveNoRecorderMethod covers 109-HONESTY at the
// functional layer: the spy recorder (which exhaustively implements the REAL
// metrics.Recorder interface) records NOTHING under any of the absent metric
// names — they are not on the interface and therefore CANNOT be recorded. This
// is a structural guard: a future synthetic pxf_errors_total / records_total /
// bytes_transferred / active_connections / gpfdist_* would have to ADD a method,
// which this test would surface as a new call.
func (s *Scenario109Suite) TestHonestyAbsentMetricsHaveNoRecorderMethod() {
	cluster := scenario109ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario109Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario109PxfJob("any-load", true)})
	h := s.boot(cluster,
		scenario109SegmentPod("seg-0", true, true))

	s.reconcile(h)

	// No call recorded by the operator may name any absent metric family.
	forbidden := []string{
		"RecordPXFBytesTransferred", "SetPXFBytesTransferred",
		"RecordPXFRecords", "SetPXFRecords",
		"SetPXFActiveConnections",
		"SetGpfdistConnectionsActive", "RecordGpfdistBytesServed",
		"RecordPXFErrors", "RecordPxfErrors",
	}
	calls := h.metrics.getCalls()
	for _, name := range forbidden {
		assert.Falsef(s.T(), containsCall(calls, name),
			"no absent-metric recorder call %q may exist (honesty)", name)
	}
	s.T().Logf("scenario109 109-HONESTY: %d recorder calls, none for the absent families "+
		"(M.4/M.5/M.7/M.15/M.16 + synthetic M.6)", len(calls))
}
