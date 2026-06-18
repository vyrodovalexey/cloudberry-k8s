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
// Scenario 105: DataLoadingStatus PXF Fields (S.1–S.5) — functional
// ============================================================================
//
// Black-boxes the OPERATOR-DRIVEN data-loading status enrichment through the
// REAL AdminReconciler (reconcileDataLoading → reconcilePxf →
// patchDataLoadingStatus) over a fake k8s client + a fake db.Client + a spy
// metrics recorder — infra-free, no live cluster. Every assertion is HONEST:
//
//   - S.1 pxf.status is derived ONLY from the real segment-primary "pxf"
//     container ContainerStatuses (no exec / HTTP). Running (all ready) / Error
//     (some down) / Stopped (none ready) / ABSENT (no pods observed).
//   - S.2 pxf.servers == len(pxf.servers) (spec-derived config count).
//   - S.3 pxf.extensionsInstalled is the real pg_extension probe result (via the
//     fake db.Client's ListPXFExtensionsFunc); ABSENT when the DB is unreachable
//     or no extensions are installed (an empty array is NEVER synthesized).
//   - S.4 activeJobs == the count of enabled jobs (concurrency-independent).
//   - S.5 jobs[] runtime fields (name/lastRun/lastStatus/rowsLoaded/duration) are
//     harvested from the real Job status + the DATALOAD_ROWS marker pod;
//     rowsLoaded is present ONLY on a succeeded Job with a harvested marker.
//   - MX the cloudberry_pxf_status gauge is recorded ONLY when observable.
//
// The catalog (cases.Scenario105Cases) documents the full S.1–S.5 + MX contract;
// the -B/-reconcile rows are resolved here, the -L rows at e2e.
// ============================================================================

const (
	scenario105Namespace = "cloudberry-test"
	scenario105Cluster   = "scenario105-pxf"

	scenario105PxfImage = "apache/cloudberry-pxf:2.1.0"
)

// Scenario105Suite drives the data-loading status enrichment through the real
// AdminReconciler over a fake client + a fake db.Client + a spy metrics recorder.
type Scenario105Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario105(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario105Suite))
}

func (s *Scenario105Suite) SetupTest() {
	s.ctx = context.Background()
}

// scenario105Cluster builds a Running cluster with PXF data loading enabled and
// the supplied servers + jobs. The DataLoading STATUS is left for the reconcile
// to populate (the reconcile rebuilds it from the spec).
func scenario105ClusterWith(
	servers []cbv1alpha1.PxfServerSpec,
	jobs []cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario105Cluster, scenario105Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		// Mark the (write-path) PXF extension SETUP as already done so the
		// best-effort setupPXFExtensions DB round-trip is skipped — this isolates
		// the READ-only S.3 extension PROBE (observePxfExtensions/ListPXFExtensions),
		// which is independent of this annotation. NOTE: this also avoids a latent
		// production nil-deref when setupPXFExtensions' annotation MergePatch
		// refreshes the in-memory cluster and clears the freshly-built (unpersisted)
		// Status.DataLoading before reconcileDataLoading reads Pxf (reported, not
		// fixed here).
		WithAnnotation(util.AnnotationPXFExtensionsReady, "true").
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   scenario105PxfImage,
			Servers: servers,
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario105SegmentPod builds a segment-primary pod (with the SHARED PXF
// selector labels) whose "pxf" container has the given readiness. When hasPXF is
// false the pod carries no "pxf" container status (counts toward total, never
// ready).
func scenario105SegmentPod(name string, ready, hasPXF bool) *corev1.Pod {
	statuses := []corev1.ContainerStatus{{Name: "segment", Ready: true}}
	if hasPXF {
		statuses = append(statuses,
			corev1.ContainerStatus{Name: cases.Scenario105PxfContainerName, Ready: ready})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scenario105Namespace,
			Labels:    util.SegmentPrimaryPXFSelector(scenario105Cluster),
		},
		Status: corev1.PodStatus{ContainerStatuses: statuses},
	}
}

// scenario105Server returns a minimal s3 PXF server definition.
func scenario105Server(name string) cbv1alpha1.PxfServerSpec {
	return cbv1alpha1.PxfServerSpec{
		Name:   name,
		Type:   "s3",
		Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
	}
}

// scenario105PxfJob returns an enabled (or disabled) pxf load job.
func scenario105PxfJob(name string, enabled bool) cbv1alpha1.DataLoadingJob {
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

// scenario105Harness wires the real AdminReconciler over a fake client seeded
// with the cluster + extra objects, a spy metrics recorder and (optionally) a
// fake db.Client factory whose ListPXFExtensions is set.
type scenario105Harness struct {
	reconciler *controller.AdminReconciler
	metrics    *mockMetricsRecorder
	env        *testutil.TestK8sEnv
}

func (s *Scenario105Suite) boot(
	cluster *cbv1alpha1.CloudberryCluster,
	listExtensions func(ctx context.Context) ([]string, error),
	extra ...client.Object,
) *scenario105Harness {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)

	m := &mockMetricsRecorder{}

	var factory *testutil.MockDBClientFactory
	if listExtensions != nil {
		factory = &testutil.MockDBClientFactory{
			Client: &testutil.MockDBClient{ListPXFExtensionsFunc: listExtensions},
		}
	}

	var r *controller.AdminReconciler
	if factory != nil {
		r = controller.NewAdminReconciler(env.Client, env.Scheme, record.NewFakeRecorder(50),
			builder.NewBuilder(), factory, m, env.Logger)
	} else {
		r = controller.NewAdminReconciler(env.Client, env.Scheme, record.NewFakeRecorder(50),
			builder.NewBuilder(), nil, m, env.Logger)
	}

	return &scenario105Harness{reconciler: r, metrics: m, env: env}
}

// reconcile drives the REAL AdminReconciler over the seeded cluster (the full
// Reconcile reaches reconcileDataLoading → reconcilePxf → patchDataLoadingStatus)
// and reads back the patched cluster status from the fake client (so assertions
// reflect the persisted status).
func (s *Scenario105Suite) reconcile(h *scenario105Harness) *cbv1alpha1.CloudberryCluster {
	cluster, err := h.env.GetCluster(s.ctx, scenario105Cluster, scenario105Namespace)
	require.NoError(s.T(), err)
	_, err = h.reconciler.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	updated, err := h.env.GetCluster(s.ctx, scenario105Cluster, scenario105Namespace)
	require.NoError(s.T(), err)
	return updated
}

// --- 105-S1-Fx: pxf.status from real segment-primary "pxf" readiness --------

// TestStatusFromReadiness covers 105-S1-B1..B4: seed segment-primary pods with a
// mix of pxf container readiness → reconcile → assert pxf.status maps to the
// honest Running/Error/Stopped value; no pods → status ABSENT.
func (s *Scenario105Suite) TestStatusFromReadiness() {
	cases105 := []struct {
		name       string
		pods       []client.Object
		wantStatus string
	}{
		{
			name: "all pxf ready → Running (105-S1-B1)",
			pods: []client.Object{
				scenario105SegmentPod("seg-0", true, true),
				scenario105SegmentPod("seg-1", true, true),
			},
			wantStatus: cases.Scenario105StatusRunning,
		},
		{
			name: "partial readiness → Error (105-S1-B2)",
			pods: []client.Object{
				scenario105SegmentPod("seg-0", true, true),
				scenario105SegmentPod("seg-1", false, true),
			},
			wantStatus: cases.Scenario105StatusError,
		},
		{
			name: "none ready → Stopped (105-S1-B3)",
			pods: []client.Object{
				scenario105SegmentPod("seg-0", false, true),
				scenario105SegmentPod("seg-1", false, true),
			},
			wantStatus: cases.Scenario105StatusStopped,
		},
		{
			name:       "no segment-primary pods → ABSENT (105-S1-B4)",
			pods:       nil,
			wantStatus: "",
		},
	}

	for _, tc := range cases105 {
		s.Run(tc.name, func() {
			cluster := scenario105ClusterWith(
				[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
			h := s.boot(cluster, nil, tc.pods...)

			updated := s.reconcile(h)
			require.NotNil(s.T(), updated.Status.DataLoading)
			require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
			assert.Equal(s.T(), tc.wantStatus, updated.Status.DataLoading.Pxf.Status)
		})
	}
}

// --- 105-S2-Fx: pxf.servers == len(pxf.servers) -----------------------------

// TestServersEqualsConfiguredCount covers 105-S2-B1/B2/B3: N configured server
// definitions → pxf.servers == N (read back from the patched status); pxf
// enabled with 0 servers → configured=true, servers=0.
func (s *Scenario105Suite) TestServersEqualsConfiguredCount() {
	s.Run("three servers → servers==3 (105-S2-B1/B3)", func() {
		cluster := scenario105ClusterWith([]cbv1alpha1.PxfServerSpec{
			scenario105Server("s3srv"),
			scenario105Server("hdfssrv"),
			scenario105Server("jdbcsrv"),
		}, nil)
		h := s.boot(cluster, nil)

		updated := s.reconcile(h)
		require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
		assert.True(s.T(), updated.Status.DataLoading.Pxf.Configured)
		assert.Equal(s.T(), int32(3), updated.Status.DataLoading.Pxf.Servers)
	})

	s.Run("zero servers → configured=true, servers=0 (105-S2-B2)", func() {
		cluster := scenario105ClusterWith(nil, nil)
		h := s.boot(cluster, nil)

		updated := s.reconcile(h)
		require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
		assert.True(s.T(), updated.Status.DataLoading.Pxf.Configured)
		assert.Equal(s.T(), int32(0), updated.Status.DataLoading.Pxf.Servers)
	})
}

// --- 105-S3-Fx: pxf.extensionsInstalled from the live pg_extension probe -----

// TestExtensionsInstalled covers 105-S3-B1..B4: the fake db.Client's
// ListPXFExtensions result is reflected honestly; a DB error / none-installed
// leaves the field ABSENT (nil) and reconcile still succeeds.
func (s *Scenario105Suite) TestExtensionsInstalled() {
	cases105 := []struct {
		name     string
		listFunc func(ctx context.Context) ([]string, error)
		wantExts []string
	}{
		{
			name:     "both observed → [pxf,pxf_fdw] (105-S3-B1)",
			listFunc: func(_ context.Context) ([]string, error) { return []string{"pxf", "pxf_fdw"}, nil },
			wantExts: []string{cases.Scenario105ExtensionPxf, cases.Scenario105ExtensionPxfFdw},
		},
		{
			name:     "only pxf → [pxf] honest subset (105-S3-B2)",
			listFunc: func(_ context.Context) ([]string, error) { return []string{"pxf"}, nil },
			wantExts: []string{cases.Scenario105ExtensionPxf},
		},
		{
			name:     "reachable, none installed → ABSENT (105-S3-B3)",
			listFunc: func(_ context.Context) ([]string, error) { return nil, nil },
			wantExts: nil,
		},
		{
			name:     "DB error → ABSENT, non-fatal (105-S3-B4)",
			listFunc: func(_ context.Context) ([]string, error) { return nil, context.DeadlineExceeded },
			wantExts: nil,
		},
	}

	for _, tc := range cases105 {
		s.Run(tc.name, func() {
			cluster := scenario105ClusterWith(
				[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
			h := s.boot(cluster, tc.listFunc)

			updated := s.reconcile(h)
			require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
			assert.Equal(s.T(), tc.wantExts, updated.Status.DataLoading.Pxf.ExtensionsInstalled)
		})
	}
}

// TestExtensionsAbsentWhenNoDBFactory covers the nil-dbFactory absent case:
// without a db factory the extensions probe is skipped → extensionsInstalled NIL.
func (s *Scenario105Suite) TestExtensionsAbsentWhenNoDBFactory() {
	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
	h := s.boot(cluster, nil)

	updated := s.reconcile(h)
	require.NotNil(s.T(), updated.Status.DataLoading.Pxf)
	assert.Nil(s.T(), updated.Status.DataLoading.Pxf.ExtensionsInstalled,
		"no db factory → extensionsInstalled must stay ABSENT (nil)")
}

// --- 105-S4-Fx: activeJobs matches enabled jobs -----------------------------

// TestActiveJobsMatchesEnabled covers 105-S4-B1: M jobs, K enabled →
// activeJobs==K, configuredJobs==M (and the dataLoadingJobs back-compat mirror
// equals activeJobs). The enabled-count invariant is concurrency-independent.
func (s *Scenario105Suite) TestActiveJobsMatchesEnabled() {
	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{
			scenario105PxfJob("load-a", true),
			scenario105PxfJob("load-b", true),
			scenario105PxfJob("load-c", false),
			scenario105PxfJob("load-d", true),
		})
	h := s.boot(cluster, nil)

	updated := s.reconcile(h)
	require.NotNil(s.T(), updated.Status.DataLoading)
	assert.Equal(s.T(), int32(4), updated.Status.DataLoading.ConfiguredJobs,
		"4 declared jobs → configuredJobs==4")
	assert.Equal(s.T(), int32(3), updated.Status.DataLoading.ActiveJobs,
		"3 enabled jobs → activeJobs==3")
	assert.Equal(s.T(), int32(3), updated.Status.DataLoadingJobs,
		"the dataLoadingJobs back-compat mirror equals activeJobs")
}

// --- 105-S5-Fx: jobs[] runtime fields honestly harvested --------------------

// scenario105SucceededJob builds a terminal Succeeded data-loading Job for the
// named spec job (the deterministic <cluster>-dataload-<job> name + the per-job
// label) with start/completion timestamps so duration is computable.
func scenario105SucceededJob(jobName string, start, completion time.Time) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName(scenario105Cluster, jobName),
			Namespace: scenario105Namespace,
			Labels: map[string]string{
				util.LabelCluster:     scenario105Cluster,
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

// scenario105MarkerPod builds the data-loading Job pod carrying the DATALOAD_ROWS
// termination marker (job-name label correlates it to the Job) so rowsLoaded is
// harvested honestly.
func scenario105MarkerPod(jobName string, rows int) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName(scenario105Cluster, jobName) + "-abcde",
			Namespace: scenario105Namespace,
			Labels: map[string]string{
				"job-name": util.DataLoadJobName(scenario105Cluster, jobName),
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "dataload",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
							Message:  "DATALOAD_ROWS=" + itoa(rows) + "\n",
						},
					},
				},
			},
		},
	}
}

// itoa is a tiny dependency-free int→string helper for the marker message.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestJobRuntimeFieldsHarvested covers 105-S5-B1: a terminal Succeeded Job + a
// DATALOAD_ROWS marker pod → the jobs[] entry carries name, lastRun, lastStatus
// ==Succeeded, rowsLoaded==marker and a non-empty duration — all harvested from
// real Job status, never synthesized.
func (s *Scenario105Suite) TestJobRuntimeFieldsHarvested() {
	const jobName = "events-load"
	const rows = 4242

	start := time.Now().Add(-90 * time.Second).UTC()
	completion := start.Add(75 * time.Second)

	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario105PxfJob(jobName, true)})

	h := s.boot(cluster, nil,
		scenario105SucceededJob(jobName, start, completion),
		scenario105MarkerPod(jobName, rows))

	updated := s.reconcile(h)
	require.NotNil(s.T(), updated.Status.DataLoading)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)

	js := updated.Status.DataLoading.Jobs[0]
	assert.Equal(s.T(), jobName, js.Name, "name populated")
	assert.True(s.T(), js.Enabled)
	assert.Equal(s.T(), "Succeeded", js.LastStatus, "terminal status mapped")
	require.NotNil(s.T(), js.LastRun, "lastRun (=startTime) populated")
	assert.WithinDuration(s.T(), start, js.LastRun.Time, time.Second)
	require.NotNil(s.T(), js.RowsLoaded, "rowsLoaded harvested from the DATALOAD_ROWS marker")
	assert.Equal(s.T(), int64(rows), *js.RowsLoaded)
	assert.NotEmpty(s.T(), js.Duration, "duration computed from start→completion")
}

// TestRunningJobHasNoRowsOrDuration covers 105-S5-B2: a non-terminal (Running)
// Job → lastStatus set ("Running") but rowsLoaded & duration ABSENT (honest: not
// yet terminal / no marker).
func (s *Scenario105Suite) TestRunningJobHasNoRowsOrDuration() {
	const jobName = "running-load"
	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario105PxfJob(jobName, true)})
	h := s.boot(cluster, nil)

	// First reconcile creates the Job; mark it Running (Active>0, started, no
	// completion) then reconcile again to harvest.
	s.reconcile(h)
	job := &batchv1.Job{}
	require.NoError(s.T(), h.env.Client.Get(s.ctx, types.NamespacedName{
		Name: util.DataLoadJobName(scenario105Cluster, jobName), Namespace: scenario105Namespace,
	}, job))
	job.Status.Active = 1
	job.Status.StartTime = &metav1.Time{Time: time.Now().Add(-30 * time.Second)}
	require.NoError(s.T(), h.env.Client.Status().Update(s.ctx, job))

	updated := s.reconcile(h)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	js := updated.Status.DataLoading.Jobs[0]
	assert.Equal(s.T(), "Running", js.LastStatus)
	assert.Nil(s.T(), js.RowsLoaded, "rowsLoaded ABSENT until terminal Succeeded + marker")
	assert.Empty(s.T(), js.Duration, "duration ABSENT until completion")
}

// TestFailedJobHasNoRows covers 105-S5-B3: a Failed Job → lastStatus=="Failed"
// and rowsLoaded ABSENT (never synthesized for a non-successful run).
func (s *Scenario105Suite) TestFailedJobHasNoRows() {
	const jobName = "failed-load"
	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario105PxfJob(jobName, true)})
	h := s.boot(cluster, nil)

	s.reconcile(h)
	job := &batchv1.Job{}
	require.NoError(s.T(), h.env.Client.Get(s.ctx, types.NamespacedName{
		Name: util.DataLoadJobName(scenario105Cluster, jobName), Namespace: scenario105Namespace,
	}, job))
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(s.T(), h.env.Client.Status().Update(s.ctx, job))

	updated := s.reconcile(h)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	js := updated.Status.DataLoading.Jobs[0]
	assert.Equal(s.T(), "Failed", js.LastStatus)
	assert.Nil(s.T(), js.RowsLoaded, "rowsLoaded never synthesized for a Failed run")
}

// TestJobNeverRunHasNoRuntimeFields covers 105-S5-B4: a job with no observed Job
// carries only name/enabled — no lastRun/lastStatus/rowsLoaded/duration. A
// DISABLED job is used so the controller creates NO workload (an enabled job
// would create a Pending Job in the same reconcile, honestly surfacing
// LastStatus="Pending"); this isolates the "never executed" contract.
func (s *Scenario105Suite) TestJobNeverRunHasNoRuntimeFields() {
	const jobName = "never-run"
	cluster := scenario105ClusterWith(
		[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")},
		[]cbv1alpha1.DataLoadingJob{scenario105PxfJob(jobName, false)})
	h := s.boot(cluster, nil) // disabled job → no Job created/observed

	updated := s.reconcile(h)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	js := updated.Status.DataLoading.Jobs[0]
	assert.Equal(s.T(), jobName, js.Name)
	assert.False(s.T(), js.Enabled)
	assert.Empty(s.T(), js.LastStatus, "no runtime status until a Job runs")
	assert.Nil(s.T(), js.LastRun)
	assert.Nil(s.T(), js.RowsLoaded, "rowsLoaded never synthesized")
	assert.Empty(s.T(), js.Duration)
}

// --- 105-MX-Fx: honest cloudberry_pxf_status gauge --------------------------

// TestPXFStatusMetricRecordedWhenObservable covers 105-MX-Fx: the
// cloudberry_pxf_status gauge is recorded (Running→1) when the status is
// observable, and is NOT recorded when the status is ABSENT (no pods observed).
func (s *Scenario105Suite) TestPXFStatusMetricRecordedWhenObservable() {
	s.Run("observable Running → gauge recorded value 1", func() {
		cluster := scenario105ClusterWith(
			[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
		h := s.boot(cluster, nil,
			scenario105SegmentPod("seg-0", true, true),
			scenario105SegmentPod("seg-1", true, true))

		s.reconcile(h)

		statusCalls := filterCalls(h.metrics.getCalls(), "SetPXFStatus")
		require.Len(s.T(), statusCalls, 1, "status gauge must be recorded when observable")
		assert.Equal(s.T(), float64(1), statusCalls[0].args["value"], "Running → 1")
	})

	s.Run("unobservable (no pods) → gauge NOT recorded", func() {
		cluster := scenario105ClusterWith(
			[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
		h := s.boot(cluster, nil) // no segment-primary pods

		s.reconcile(h)

		assert.False(s.T(), containsCall(h.metrics.getCalls(), "SetPXFStatus"),
			"absent/unobservable status must NOT record the cloudberry_pxf_status gauge")
	})
}

// TestPXFExtensionsMetricRecordedWhenObserved covers 105-MX-Fx (extensions): the
// cloudberry_pxf_extensions_installed gauge is recorded == len(extensions) when
// observed and is NOT recorded when the DB is unreachable.
func (s *Scenario105Suite) TestPXFExtensionsMetricRecordedWhenObserved() {
	s.Run("both observed → gauge recorded value 2", func() {
		cluster := scenario105ClusterWith(
			[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
		h := s.boot(cluster, func(_ context.Context) ([]string, error) {
			return []string{"pxf", "pxf_fdw"}, nil
		})

		s.reconcile(h)

		extCalls := filterCalls(h.metrics.getCalls(), "SetPXFExtensionsInstalled")
		require.Len(s.T(), extCalls, 1, "extensions gauge must be recorded when observed")
		assert.Equal(s.T(), float64(2), extCalls[0].args["count"])
	})

	s.Run("DB unreachable → extensions gauge NOT recorded", func() {
		cluster := scenario105ClusterWith(
			[]cbv1alpha1.PxfServerSpec{scenario105Server("s3srv")}, nil)
		h := s.boot(cluster, func(_ context.Context) ([]string, error) {
			return nil, context.DeadlineExceeded
		})

		s.reconcile(h)

		assert.False(s.T(), containsCall(h.metrics.getCalls(), "SetPXFExtensionsInstalled"),
			"an unreachable DB must NOT record the extensions gauge")
	})
}
