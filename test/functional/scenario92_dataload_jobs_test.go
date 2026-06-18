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
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 92: Data-Loading Ingestion Runtime — Job/CronJob reconcile
// ============================================================================
//
// This functional suite drives the CONTROLLER (fake client) over the
// data-loading ingestion runtime (reconcileDataLoadingJobs) and asserts the
// operator genuinely BUILDS and LAUNCHES correct load Jobs, harvests the
// DATALOAD_ROWS marker, enriches status and records the 5 honest metrics:
//
//   - JobCreated:    an enabled one-off pxf job => Job created with correct
//     args[0] SQL (DDL + INSERT + marker) and ownerRef.
//   - CronJobCreated: an enabled scheduled native job => CronJob (not Job).
//   - DisabledSkipped: a disabled job => no Job/CronJob created.
//   - Idempotent:     a second reconcile does not duplicate the Job.
//   - SucceededHarvest: a Succeeded Job with a DATALOAD_ROWS marker => status
//     RowsLoaded/LastStatus/LastRun/Duration populated + metrics (status=2,
//     last_success, duration observed, rows_total added with source_type).
//   - FailedErrors:   a Failed Job => status=Failed + errors_total incremented.
//
// HONESTY: the genuine row-count path is the native protocols; the pxf:// Job is
// generated/launched but its live read-back is image-blocked. These tests prove
// the controller machinery (create -> status -> marker harvest -> metric), not a
// live pxf execution.
// ============================================================================

// dataLoadCaptureRecorder captures the 5 data-loading metric calls.
type dataLoadCaptureRecorder struct {
	metrics.NoopRecorder
	statusByJob   map[string]float64
	lastSuccess   map[string]float64
	durationByJob map[string]time.Duration
	rowsByJob     map[string]float64
	sourceByJob   map[string]string
	errorsByJob   map[string]int
}

func newDataLoadCaptureRecorder() *dataLoadCaptureRecorder {
	return &dataLoadCaptureRecorder{
		statusByJob:   map[string]float64{},
		lastSuccess:   map[string]float64{},
		durationByJob: map[string]time.Duration{},
		rowsByJob:     map[string]float64{},
		sourceByJob:   map[string]string{},
		errorsByJob:   map[string]int{},
	}
}

func (m *dataLoadCaptureRecorder) SetDataLoadingJobStatus(_, _, job string, status float64) {
	m.statusByJob[job] = status
}

func (m *dataLoadCaptureRecorder) SetDataLoadingJobLastSuccess(_, _, job string, ts float64) {
	m.lastSuccess[job] = ts
}

func (m *dataLoadCaptureRecorder) ObserveDataLoadingJobDuration(_, _, job string, d time.Duration) {
	m.durationByJob[job] = d
}

func (m *dataLoadCaptureRecorder) RecordDataLoadingRows(_, _, job, sourceType string, count float64) {
	m.rowsByJob[job] += count
	m.sourceByJob[job] = sourceType
}

func (m *dataLoadCaptureRecorder) RecordDataLoadingErrors(_, _, job string) {
	m.errorsByJob[job]++
}

type Scenario92Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario92(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario92Suite))
}

func (s *Scenario92Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// s92Cluster builds a cluster with the given data-loading jobs.
func s92Cluster(name string, jobs []cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true, Jobs: jobs}
	return cluster
}

func s92PxfJob(name string, enabled bool) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: name, Type: "pxf", Enabled: enabled,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server: "s3-datalake", Profile: "s3:parquet",
			Resource: "s3a://data-lake/events/", TargetTable: "public.events",
		},
	}
}

func s92GploadJob(name, schedule string, enabled bool) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: name, Type: "gpload", Enabled: enabled, Schedule: schedule,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.bulk_data", Format: "csv",
			FilePaths: []string{"/data/incoming/*.csv"},
		},
	}
}

func (s *Scenario92Suite) newReconciler(
	env *testutil.TestK8sEnv, rec metrics.Recorder,
) *controller.AdminReconciler {
	return controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder, s.builder, nil, rec, env.Logger,
	)
}

// TestFunctional_Scenario92_EnabledPxfJobCreated asserts an enabled one-off pxf
// job yields a Job with the correct args[0] SQL and ownerRef.
func (s *Scenario92Suite) TestFunctional_Scenario92_EnabledPxfJobCreated() {
	cluster := s92Cluster("s92-pxf", []cbv1alpha1.DataLoadingJob{s92PxfJob("s3-loader", true)})
	env := testutil.NewTestK8sEnv(cluster)
	r := s.newReconciler(env, &metrics.NoopRecorder{})

	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	job := &batchv1.Job{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.DataLoadJobName(cluster.Name, "s3-loader"),
		Namespace: cluster.Namespace,
	}, job))

	require.Len(s.T(), job.OwnerReferences, 1)
	assert.Equal(s.T(), cluster.Name, job.OwnerReferences[0].Name)
	assert.Equal(s.T(), util.ComponentDataLoad, job.Labels[util.LabelComponent])

	require.Len(s.T(), job.Spec.Template.Spec.Containers, 1)
	script := job.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, "CREATE EXTERNAL TABLE")
	assert.Contains(s.T(), script, "pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake")
	assert.Contains(s.T(), script, "INSERT INTO \"public\".\"events\"")
	assert.Contains(s.T(), script, "DATALOAD_ROWS=")
}

// TestFunctional_Scenario92_ScheduledJobIsCronJob asserts a scheduled native job
// yields a CronJob (not a Job).
func (s *Scenario92Suite) TestFunctional_Scenario92_ScheduledJobIsCronJob() {
	cluster := s92Cluster("s92-cron",
		[]cbv1alpha1.DataLoadingJob{s92GploadJob("nightly", "0 2 * * *", true)})
	env := testutil.NewTestK8sEnv(cluster)
	r := s.newReconciler(env, &metrics.NoopRecorder{})

	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	name := util.DataLoadJobName(cluster.Name, "nightly")
	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, cron))
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)

	// No one-off Job of the same name exists.
	job := &batchv1.Job{}
	err = env.Client.Get(s.ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, job)
	assert.Error(s.T(), err, "scheduled job must not create a one-off Job")
}

// TestFunctional_Scenario92_DisabledJobSkipped asserts a disabled job creates no
// workload.
func (s *Scenario92Suite) TestFunctional_Scenario92_DisabledJobSkipped() {
	cluster := s92Cluster("s92-disabled",
		[]cbv1alpha1.DataLoadingJob{s92PxfJob("off", false)})
	env := testutil.NewTestK8sEnv(cluster)
	r := s.newReconciler(env, &metrics.NoopRecorder{})

	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	job := &batchv1.Job{}
	err = env.Client.Get(s.ctx, types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "off"), Namespace: cluster.Namespace,
	}, job)
	assert.Error(s.T(), err, "disabled job must not create a Job")
}

// TestFunctional_Scenario92_Idempotent asserts a second reconcile does not
// duplicate the Job.
func (s *Scenario92Suite) TestFunctional_Scenario92_Idempotent() {
	cluster := s92Cluster("s92-idem", []cbv1alpha1.DataLoadingJob{s92PxfJob("loader", true)})
	env := testutil.NewTestK8sEnv(cluster)
	r := s.newReconciler(env, &metrics.NoopRecorder{})

	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)
	_, err = r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs,
		client.InNamespace(cluster.Namespace)))
	count := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelComponent] == util.ComponentDataLoad {
			count++
		}
	}
	assert.Equal(s.T(), 1, count, "second reconcile must not duplicate the dataload Job")
}

// TestFunctional_Scenario92_SucceededHarvest injects a Succeeded Job + a
// terminated pod carrying the DATALOAD_ROWS marker and asserts the enriched
// status + metrics.
func (s *Scenario92Suite) TestFunctional_Scenario92_SucceededHarvest() {
	cluster := s92Cluster("s92-ok", []cbv1alpha1.DataLoadingJob{s92PxfJob("s3-loader", true)})
	env := testutil.NewTestK8sEnv(cluster)
	rec := newDataLoadCaptureRecorder()
	r := s.newReconciler(env, rec)

	// First reconcile creates the Job.
	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	jobName := util.DataLoadJobName(cluster.Name, "s3-loader")
	start := metav1.NewTime(time.Now().Add(-90 * time.Second))
	done := metav1.NewTime(time.Now())

	// Mark the Job Succeeded with start/completion times.
	job := &batchv1.Job{}
	require.NoError(s.T(), env.Client.Get(s.ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Succeeded = 1
	job.Status.StartTime = &start
	job.Status.CompletionTime = &done
	require.NoError(s.T(), env.Client.Status().Update(s.ctx, job))

	// Inject the terminated pod carrying the DATALOAD_ROWS marker.
	s.injectDataLoadPod(env, cluster, jobName, "DATALOAD_ROWS=183961")

	// Second reconcile harvests the marker and enriches status + metrics.
	_, err = r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), updated.Status.DataLoading)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	js := updated.Status.DataLoading.Jobs[0]
	assert.Equal(s.T(), "s3-loader", js.Name)
	assert.Equal(s.T(), "Succeeded", js.LastStatus)
	require.NotNil(s.T(), js.RowsLoaded)
	assert.Equal(s.T(), int64(183961), *js.RowsLoaded)
	require.NotNil(s.T(), js.LastRun)
	assert.NotEmpty(s.T(), js.Duration)

	// Metrics: status=2(success), last_success set, duration observed, rows added
	// with the s3 source_type derived from the profile.
	assert.Equal(s.T(), 2.0, rec.statusByJob["s3-loader"])
	assert.NotZero(s.T(), rec.lastSuccess["s3-loader"])
	assert.Positive(s.T(), rec.durationByJob["s3-loader"])
	assert.Equal(s.T(), 183961.0, rec.rowsByJob["s3-loader"])
	assert.Equal(s.T(), "s3", rec.sourceByJob["s3-loader"])
	assert.Zero(s.T(), rec.errorsByJob["s3-loader"])
}

// TestFunctional_Scenario92_FailedRecordsError injects a Failed Job and asserts
// the status=Failed + errors_total increment.
func (s *Scenario92Suite) TestFunctional_Scenario92_FailedRecordsError() {
	cluster := s92Cluster("s92-fail", []cbv1alpha1.DataLoadingJob{s92PxfJob("s3-loader", true)})
	env := testutil.NewTestK8sEnv(cluster)
	rec := newDataLoadCaptureRecorder()
	r := s.newReconciler(env, rec)

	_, err := r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	jobName := util.DataLoadJobName(cluster.Name, "s3-loader")
	job := &batchv1.Job{}
	require.NoError(s.T(), env.Client.Get(s.ctx,
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(s.T(), env.Client.Status().Update(s.ctx, job))

	_, err = r.Reconcile(s.ctx, ctrlRequestFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.Len(s.T(), updated.Status.DataLoading.Jobs, 1)
	assert.Equal(s.T(), "Failed", updated.Status.DataLoading.Jobs[0].LastStatus)

	assert.Equal(s.T(), 3.0, rec.statusByJob["s3-loader"])
	assert.Equal(s.T(), 1, rec.errorsByJob["s3-loader"])
	assert.Zero(s.T(), rec.rowsByJob["s3-loader"])
}

// injectDataLoadPod creates a terminated pod labeled with the Job's job-name so
// the controller's marker harvest finds the DATALOAD_ROWS termination message.
func (s *Scenario92Suite) injectDataLoadPod(
	env *testutil.TestK8sEnv,
	cluster *cbv1alpha1.CloudberryCluster,
	jobName, message string,
) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{"job-name": jobName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "dataload", Image: "img"}},
		},
	}
	require.NoError(s.T(), env.Client.Create(s.ctx, pod))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "dataload",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Message: message},
			},
		},
	}
	require.NoError(s.T(), env.Client.Status().Update(s.ctx, pod))
}
