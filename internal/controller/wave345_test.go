package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// assertExporterSecretBoom is the sentinel injected into the exporter-credentials
// Secret Create so TASK 4 can prove the real cause propagates through
// ensureExporterCoreResources and onto its span.
var assertExporterSecretBoom = errors.New("exporter secret create boom")

// wave345Recorder captures the controller-operation metric calls added in
// waves 3-5 (B-9, C-7) on top of the reconcile counts.
type wave345Recorder struct {
	metrics.NoopRecorder
	reconcileCalls    []reconcileCall
	storageExpansions []string
	backupOnDelete    []string
	scalePhases       []string
	scaleOps          []string
	connectionsMax    []float64
	pvcSizes          map[string]float64
}

func (r *wave345Recorder) RecordReconcile(cluster, namespace, result string, d time.Duration) {
	r.reconcileCalls = append(r.reconcileCalls, reconcileCall{
		cluster: cluster, namespace: namespace, result: result, duration: d,
	})
}

func (r *wave345Recorder) RecordStorageExpansion(_, _, result string) {
	r.storageExpansions = append(r.storageExpansions, result)
}

func (r *wave345Recorder) RecordBackupOnDelete(_, _, result string) {
	r.backupOnDelete = append(r.backupOnDelete, result)
}

func (r *wave345Recorder) ObserveScalePhaseDuration(direction, phase string, _ time.Duration) {
	r.scalePhases = append(r.scalePhases, direction+"/"+phase)
}

func (r *wave345Recorder) RecordScaleOperation(_, _, operation string) {
	r.scaleOps = append(r.scaleOps, operation)
}

func (r *wave345Recorder) SetConnectionsMax(_, _ string, count float64) {
	r.connectionsMax = append(r.connectionsMax, count)
}

func (r *wave345Recorder) SetPVCSizeBytes(_, _, component string, sizeBytes float64) {
	if r.pvcSizes == nil {
		r.pvcSizes = map[string]float64{}
	}
	r.pvcSizes[component] = sizeBytes
}

// newWave345Reconciler builds a ClusterReconciler over a fake client.
func newWave345Reconciler(
	rec metrics.Recorder,
	objs ...client.Object,
) (*ClusterReconciler, client.Client) {
	scheme := newTestScheme()
	b := fake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		b = b.WithObjects(o)
		b = b.WithStatusSubresource(o)
	}
	k8sClient := b.Build()
	return NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(40), builder.NewBuilder(), rec, nil), k8sClient
}

// ---------------------------------------------------------------------------
// B-9: reconcile metrics on ALL cluster-controller exit paths
// ---------------------------------------------------------------------------

// TestClusterReconcile_MetricOnNotFound: a deleted cluster records exactly one
// success sample (benign exit).
func TestClusterReconcile_MetricOnNotFound(t *testing.T) {
	rec := &wave345Recorder{}
	r, _ := newWave345Reconciler(rec)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

// TestClusterReconcile_MetricOnFetchError: a transport-level Get failure
// records exactly one error sample and returns the error.
func TestClusterReconcile_MetricOnFetchError(t *testing.T) {
	rec := &wave345Recorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return assertNewClientErr
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result)
}

// TestClusterReconcile_MetricOnStoppedPhase: the Stopped lifecycle branch
// records exactly one sample.
func TestClusterReconcile_MetricOnStoppedPhase(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Finalizers = []string{util.FinalizerName}
	r, _ := newWave345Reconciler(rec, cluster)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

// TestClusterReconcile_MetricOnGenerationUnchanged: the steady-state skip
// path records exactly one sample.
func TestClusterReconcile_MetricOnGenerationUnchanged(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Finalizers = []string{util.FinalizerName}
	r, _ := newWave345Reconciler(rec, cluster)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

// TestClusterReconcile_MetricOnFinalizerAdd: the finalizer-add fast exit
// records exactly one sample.
func TestClusterReconcile_MetricOnFinalizerAdd(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	r, _ := newWave345Reconciler(rec, cluster)

	result, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, requeueAfterImmediate, result.RequeueAfter)
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

// TestClusterReconcile_MetricOnDeletion: the deletion branch records exactly
// one sample.
func TestClusterReconcile_MetricOnDeletion(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{util.FinalizerName}
	r, _ := newWave345Reconciler(rec, cluster)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Len(t, rec.reconcileCalls, 1)
}

// TestLifecyclePhaseErrorsAreReturned verifies B-9: a lifecycle-phase error
// (Updating/upgrade progress failure due to a status patch interceptor error)
// is RETURNED from Reconcile, recording result="error".
func TestLifecyclePhaseErrorsAreReturned(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Finalizers = []string{util.FinalizerName}
	// A scale-started annotation in the distant past forces the timeout path,
	// whose status update we sabotage to produce an error.
	cluster.Annotations = map[string]string{
		util.AnnotationScaleStarted: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ ...client.SubResourceUpdateOption) error {
				return assertNewClientErr
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err, "lifecycle-phase errors must no longer be swallowed")
	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result)
}

// ---------------------------------------------------------------------------
// B-2: configurable reconcile interval / operation timeout
// ---------------------------------------------------------------------------

// TestReconcileIntervalHonored verifies a custom interval appears in
// RequeueAfter on the steady-state path, and the default is preserved when
// unset (golden value).
func TestReconcileIntervalHonored(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Finalizers = []string{util.FinalizerName}

	// Default path: 30s golden value.
	r, _ := newWave345Reconciler(&wave345Recorder{}, cluster.DeepCopy())
	result, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)

	// Custom interval honored.
	r2, _ := newWave345Reconciler(&wave345Recorder{}, cluster.DeepCopy())
	r2.SetIntervals(90*time.Second, 0)
	result, err = r2.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, result.RequeueAfter)
}

// TestOperationTimeoutOverride verifies the opTimeout helper semantics.
func TestOperationTimeoutOverride(t *testing.T) {
	var i reconcileIntervals
	assert.Equal(t, scaleTimeout, i.opTimeout(scaleTimeout), "zero keeps the per-op default")

	i.SetIntervals(0, 42*time.Minute)
	assert.Equal(t, 42*time.Minute, i.opTimeout(scaleTimeout))
	assert.Equal(t, requeueAfterDefault, i.requeueDefault(), "zero interval keeps default")
}

// ---------------------------------------------------------------------------
// B-11: rebalance fallback Job tracked to terminal state
// ---------------------------------------------------------------------------

// rebalanceCluster returns a Running cluster with the rebalance action set.
func rebalanceCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Finalizers = []string{util.FinalizerName}
	c.Annotations = map[string]string{util.AnnotationAction: util.ActionRebalance}
	return c
}

func newHAWaveReconciler(
	rec metrics.Recorder,
	objs ...client.Object,
) (*HAReconciler, client.Client, *record.FakeRecorder) {
	scheme := newTestScheme()
	b := fake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		b = b.WithObjects(o).WithStatusSubresource(o)
	}
	k8sClient := b.Build()
	events := record.NewFakeRecorder(40)
	return NewHAReconciler(k8sClient, scheme, events, nil,
		builder.NewBuilder(), rec, nil), k8sClient, events
}

// TestRebalanceFallback_NoFireAndForgetSuccess verifies the fallback Job is
// created and tracked: while it is pending, NO completion metric/condition is
// recorded and the reconciler requeues.
func TestRebalanceFallback_NoFireAndForgetSuccess(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := rebalanceCluster()
	r, k8sClient, _ := newHAWaveReconciler(rec, cluster)

	result, err := r.handleRebalance(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterRebalanceJob, result.RequeueAfter)
	assert.NotContains(t, rec.scaleOps, "rebalance",
		"no completion metric while the Job is pending")

	// The tracking annotation was stamped with the Job name.
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	jobName := updated.Annotations[util.AnnotationRebalanceJob]
	require.NotEmpty(t, jobName)

	// The Job exists.
	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job))

	// While the Job is still running: requeue, still no completion.
	result, err = r.observeRebalanceJob(context.Background(), updated, jobName)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterRebalanceJob, result.RequeueAfter)
	assert.NotContains(t, rec.scaleOps, "rebalance")
}

// TestRebalanceFallback_JobSucceeded verifies the success terminal state:
// exactly one completion metric, completed condition, annotation removed.
func TestRebalanceFallback_JobSucceeded(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := rebalanceCluster()
	cluster.Annotations = map[string]string{util.AnnotationRebalanceJob: "rebalance-job-1"}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "rebalance-job-1", Namespace: cluster.Namespace},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	r, k8sClient, _ := newHAWaveReconciler(rec, cluster, job)

	_, err := r.observeRebalanceJob(context.Background(), cluster, "rebalance-job-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"rebalance"}, rec.scaleOps, "exactly one completion metric")

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Empty(t, updated.Annotations[util.AnnotationRebalanceJob])
	cond := util.FindCondition(updated.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution))
	require.NotNil(t, cond)
	assert.Equal(t, "RebalanceCompleted", cond.Reason)
}

// TestRebalanceFallback_JobFailed verifies the failed terminal state: failed
// metric + warning event, no completion sample.
func TestRebalanceFallback_JobFailed(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := rebalanceCluster()
	cluster.Annotations = map[string]string{util.AnnotationRebalanceJob: "rebalance-job-2"}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "rebalance-job-2", Namespace: cluster.Namespace},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	r, _, events := newHAWaveReconciler(rec, cluster, job)

	_, err := r.observeRebalanceJob(context.Background(), cluster, "rebalance-job-2")
	require.NoError(t, err)
	assert.Equal(t, []string{"rebalance-failed"}, rec.scaleOps)
	assert.NotContains(t, rec.scaleOps, "rebalance")

	select {
	case ev := <-events.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "RebalanceFailed")
	default:
		t.Fatal("expected a warning event for the failed rebalance Job")
	}
}

// TestRebalanceFallback_JobLost verifies a disappeared Job fails the tracking
// without wedging it (annotation removed).
func TestRebalanceFallback_JobLost(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := rebalanceCluster()
	cluster.Annotations = map[string]string{util.AnnotationRebalanceJob: "gone-job"}
	r, k8sClient, _ := newHAWaveReconciler(rec, cluster)

	_, err := r.observeRebalanceJob(context.Background(), cluster, "gone-job")
	require.NoError(t, err)
	assert.Equal(t, []string{"rebalance-failed"}, rec.scaleOps)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Empty(t, updated.Annotations[util.AnnotationRebalanceJob])
}

// ---------------------------------------------------------------------------
// C-7: controller operation metrics
// ---------------------------------------------------------------------------

// TestStorageExpansionMetrics verifies success and error outcomes are counted.
func TestStorageExpansionMetrics(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-coordinator-0",
			Namespace: "default",
			Labels:    map[string]string{util.LabelCluster: "test-cluster"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	r, _ := newWave345Reconciler(rec, cluster, pvc)

	changed, err := r.expandPVCIfNeeded(context.Background(),
		"default", "data-test-cluster-coordinator-0", "10Gi")
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, []string{reconcileResultSuccess}, rec.storageExpansions)

	// Error path: Update interceptor fails.
	rec2 := &wave345Recorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pvc.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch,
				_ client.Object, _ ...client.UpdateOption) error {
				return assertNewClientErr
			},
		}).Build()
	r2 := NewClusterReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), rec2, nil)
	_, err = r2.expandPVCIfNeeded(context.Background(),
		"default", "data-test-cluster-coordinator-0", "10Gi")
	require.Error(t, err)
	assert.Equal(t, []string{reconcileResultError}, rec2.storageExpansions)
}

// TestPVCSizeGaugeOnSteadyState verifies SetPVCSizeBytes fires on every
// metrics snapshot (no expansion needed).
func TestPVCSizeGaugeOnSteadyState(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	r, _ := newWave345Reconciler(rec, cluster)

	r.recordMetricsSnapshot(cluster)

	require.NotNil(t, rec.pvcSizes)
	assert.Positive(t, rec.pvcSizes["coordinator"])
	assert.Positive(t, rec.pvcSizes["segment"])
}

// TestConnectionsMaxNotZeroedOnSnapshot verifies L-5/C-5: the snapshot path
// never writes a 0 ceiling.
func TestConnectionsMaxNotZeroedOnSnapshot(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	r, _ := newWave345Reconciler(rec, cluster)

	r.recordMetricsSnapshot(cluster)
	assert.Empty(t, rec.connectionsMax, "snapshot must not write connections_max at all")
}

// TestScalePhaseDurationObserved verifies the C-7 histogram fires on phase
// transitions for both directions.
func TestScalePhaseDurationObserved(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	r, _ := newWave345Reconciler(rec, cluster)

	outState := &scaleStateData{
		Phase:          scalePhaseRegistering,
		PhaseStartedAt: time.Now().Add(-time.Minute).Format(time.RFC3339),
		StartedAt:      time.Now().Add(-2 * time.Minute).Format(time.RFC3339),
	}
	_, err := r.advanceScalePhase(context.Background(), cluster, outState, scalePhaseRedistributing)
	require.NoError(t, err)

	inState := &scaleInStateData{
		Phase:     scaleInPhaseRedistributing,
		StartedAt: time.Now().Add(-time.Minute).Format(time.RFC3339),
	}
	_, err = r.advanceScaleInPhase(context.Background(), cluster, inState, scaleInPhaseDeregistering)
	require.NoError(t, err)

	assert.Equal(t, []string{"out/registering", "in/redistributing"}, rec.scalePhases)
	// The next phase's start timestamp was stamped.
	assert.NotEmpty(t, outState.PhaseStartedAt)
	assert.NotEmpty(t, inState.PhaseStartedAt)
}

// TestObserveScalePhaseSkipsBadTimestamps verifies unparseable timestamps do
// not record bogus durations.
func TestObserveScalePhaseSkipsBadTimestamps(t *testing.T) {
	rec := &wave345Recorder{}
	r, _ := newWave345Reconciler(rec)
	r.observeScalePhase(scaleDirectionOut, "registering", "not-a-time", "")
	assert.Empty(t, rec.scalePhases)
}

// TestBackupOnDeleteDedicatedCounter verifies C-7: terminal deletion-backup
// outcomes increment the dedicated counter.
func TestBackupOnDeleteDedicatedCounter(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	r, _ := newWave345Reconciler(rec, cluster)

	r.recordBackupOnDelete(cluster, "completed")
	r.recordBackupOnDelete(cluster, "failed")
	assert.Equal(t, []string{"completed", "failed"}, rec.backupOnDelete)
}

// ---------------------------------------------------------------------------
// D-4: controller phase spans
// ---------------------------------------------------------------------------

// TestControllerPhaseSpans verifies the per-operation child spans exist with
// the Reconcile root as ancestor for a representative set of operations.
func TestControllerPhaseSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	rec := &wave345Recorder{}

	// Deletion span via a full Reconcile of a deleting cluster.
	cluster := newTestCluster()
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{util.FinalizerName}
	r, _ := newWave345Reconciler(rec, cluster)
	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	// Storage expansion span (direct call).
	cluster2 := newTestCluster()
	r2, _ := newWave345Reconciler(rec, cluster2)
	require.NoError(t, r2.reconcileStorageExpansion(context.Background(), cluster2))

	// Scale-out phase span with phase attribute (unknown phase completes).
	state, _ := json.Marshal(scaleStateData{Phase: scalePhaseCompleted})
	cluster3 := newTestCluster()
	cluster3.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster3.Annotations = map[string]string{annotationScaleState: string(state)}
	r3, _ := newWave345Reconciler(rec, cluster3)
	_, _ = r3.checkScaleOutPhases(context.Background(), cluster3, string(state))

	// D-01/D-02: reconcileCoreResources and reconcileStatefulSets child spans via
	// direct calls (the span is created regardless of the call's success/error).
	cluster4 := newTestCluster()
	r4, _ := newWave345Reconciler(rec, cluster4)
	_ = r4.reconcileCoreResources(context.Background(), cluster4)
	_ = r4.reconcileStatefulSets(context.Background(), cluster4)

	spans := sr.Ended()
	names := map[string]bool{}
	var deletionParented bool
	var reconcileRootSpanIDs = map[string]bool{}
	for _, s := range spans {
		names[s.Name()] = true
		if s.Name() == "Reconcile" {
			reconcileRootSpanIDs[s.SpanContext().SpanID().String()] = true
		}
	}
	for _, s := range spans {
		if s.Name() == "controller.deletion" &&
			reconcileRootSpanIDs[s.Parent().SpanID().String()] {
			deletionParented = true
		}
	}

	assert.True(t, names["controller.deletion"], "missing controller.deletion span")
	assert.True(t, names["controller.storageExpansion"], "missing controller.storageExpansion span")
	assert.True(t, names["controller.scaleOut.phase"], "missing controller.scaleOut.phase span")
	assert.True(t, deletionParented, "controller.deletion must be parented on the Reconcile root")

	// D-01/D-02: the two new reconcile child spans must be produced.
	assert.True(t, names["controller.reconcileCoreResources"], "missing reconcileCoreResources span")
	assert.True(t, names["controller.reconcileStatefulSets"], "missing reconcileStatefulSets span")
}

// TestAdminControllerPhaseSpans verifies the admin controller operation spans.
func TestAdminControllerPhaseSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), nil, &wave345Recorder{}, nil)

	// reconcileWorkload (disabled → cleanup, still spanned).
	require.NoError(t, r.reconcileWorkload(context.Background(), cluster))

	// handleMaintenance with an analyze operation.
	cluster.Annotations = map[string]string{util.AnnotationMaintenance: "analyze"}
	_, _ = r.handleMaintenance(context.Background(), cluster, "analyze")

	names := map[string]bool{}
	for _, s := range sr.Ended() {
		names[s.Name()] = true
	}
	assert.True(t, names["controller.reconcileWorkload"])
	assert.True(t, names["controller.handleMaintenance"])
}

// pxfDataLoadingCluster returns a running cluster with data-loading + PXF enabled
// so reconcileDataLoading executes its body and nests the reconcilePxf span.
func pxfDataLoadingCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "s3.local"}},
			},
		},
	}
	return c
}

// TestAdminControllerSubReconcilerSpans verifies the W3-C1 child spans for the
// five named sub-reconcilers exist and are parented on the per-call span
// (TASK 9). reconcilePxf nests inside reconcileDataLoading; the others are
// driven directly. Confirms NO double-span on a sibling (reconcileWorkload).
func TestAdminControllerSubReconcilerSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := pxfDataLoadingCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	// reconcileResourceGroups dereferences Spec.Workload (the production caller
	// gates on it); supply an empty spec so the span is emitted as a no-op.
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), &mockDBClientFactory{client: &mockDBClient{}},
		&wave345Recorder{}, nil)

	// reconcileDataLoading (enabled) → nests reconcilePxf.
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	// reconcileStorage (disk monitoring enabled).
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	// reconcileResourceGroups (DB-backed; empty lists → no-op success).
	require.NoError(t, r.reconcileResourceGroups(context.Background(), cluster, &mockDBClient{}))

	names := map[string]int{}
	for _, s := range sr.Ended() {
		names[s.Name()]++
	}
	assert.GreaterOrEqual(t, names["controller.reconcileDataLoading"], 1,
		"missing controller.reconcileDataLoading span")
	assert.GreaterOrEqual(t, names["controller.reconcilePxf"], 1,
		"missing controller.reconcilePxf span (must nest in reconcileDataLoading)")
	assert.GreaterOrEqual(t, names["controller.reconcileStorage"], 1,
		"missing controller.reconcileStorage span")
	assert.GreaterOrEqual(t, names["controller.reconcileResourceGroups"], 1,
		"missing controller.reconcileResourceGroups span")
	// No accidental double-span on a sibling we never called here.
	assert.Equal(t, 0, names["controller.reconcileWorkload"],
		"reconcileWorkload must not be spanned when it was never invoked")
}

// TestAdminControllerDataLoadSpans verifies the new C-1/C-2/C-3 sub-reconciler
// spans (T8): driving reconcileDataLoadingJobs, reconcileGpfdist and
// setupPXFExtensions on a fake-client reconciler emits the
// "controller.reconcileDataLoadingJobs", "controller.reconcileGpfdist" and
// "controller.setupPXFExtensions" spans via startControllerSpan. The existing
// TestAdminControllerSubReconcilerSpans asserts only the wave-3 sub-reconcilers,
// so these three are otherwise unasserted.
func TestAdminControllerDataLoadSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := pxfDataLoadingCluster()
	// An enabled dataload job (drives reconcileDataLoadingJobs body) and gpfdist
	// enabled (drives reconcileGpfdist create path).
	cluster.Spec.DataLoading.Jobs = []cbv1alpha1.DataLoadingJob{
		{
			Name: "loader", Type: "pxf", Enabled: true,
			PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "s3srv", Profile: "s3:parquet",
				Resource: "s3a://data-lake/events/", TargetTable: "public.events",
			},
		},
	}
	cluster.Spec.DataLoading.Gpfdist = &cbv1alpha1.GpfdistSpec{Enabled: true}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), &mockDBClientFactory{client: &mockDBClient{}},
		&wave345Recorder{}, nil)

	// C-1: reconcileDataLoadingJobs (enabled job → body executes).
	require.NoError(t, r.reconcileDataLoadingJobs(context.Background(), cluster))
	// C-2: reconcileGpfdist (gpfdist enabled → create path).
	require.NoError(t, r.reconcileGpfdist(context.Background(), cluster))
	// C-3: setupPXFExtensions (PXF enabled + mock dbFactory → body executes).
	r.setupPXFExtensions(context.Background(), cluster, r.logger)

	names := map[string]int{}
	for _, s := range sr.Ended() {
		names[s.Name()]++
	}
	assert.GreaterOrEqual(t, names["controller.reconcileDataLoadingJobs"], 1,
		"missing controller.reconcileDataLoadingJobs span")
	assert.GreaterOrEqual(t, names["controller.reconcileGpfdist"], 1,
		"missing controller.reconcileGpfdist span")
	assert.GreaterOrEqual(t, names["controller.setupPXFExtensions"], 1,
		"missing controller.setupPXFExtensions span")
}

// TestReconcileGpfdist_CreateError_RecordsSpanError forces the gpfdist PVC Create
// to fail and asserts the reconcileGpfdist span is marked codes.Error (T8 bonus),
// mirroring the TestEnsureExporterCoreResources_*_RecordsSpanError idiom so the
// error-status propagation through end(err) is verified, not just span presence.
func TestReconcileGpfdist_CreateError_RecordsSpanError(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return assertGpfdistBoom
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), nil, &wave345Recorder{}, nil)

	err := r.reconcileGpfdist(context.Background(), cluster)
	require.Error(t, err)
	assert.ErrorIs(t, err, assertGpfdistBoom)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "controller.reconcileGpfdist" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code,
				"the reconcileGpfdist span must be codes.Error on the propagated error")
		}
	}
	assert.True(t, found, "controller.reconcileGpfdist span must exist")
}

// TestEnsureExporterCoreResources_SubResourceError_RecordsSpanError forces the
// exporter-credentials Secret Create to fail, driving the currently-unexecuted
// error return of ensureExporterCoreResources, and asserts the propagated error
// AND the codes.Error span status (TASK 4 + the ensureExporterCoreResources slice
// of TASK 9).
func TestEnsureExporterCoreResources_SubResourceError_RecordsSpanError(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := newTestCluster()

	// Fail Create for the exporter credentials Secret so the first ensure*
	// helper returns an error and ensureExporterCoreResources propagates it.
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return assertExporterSecretBoom
				}
				return nil
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), nil, &wave345Recorder{}, nil)

	err := r.ensureExporterCoreResources(context.Background(), cluster,
		"exporter-password", "postgres://user@host:5432/db", r.logger)

	require.Error(t, err, "the sub-resource Create error must propagate")
	assert.ErrorIs(t, err, assertExporterSecretBoom)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "controller.ensureExporterCoreResources" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code,
				"the span must be marked codes.Error on the propagated error")
		}
	}
	assert.True(t, found, "controller.ensureExporterCoreResources span must exist")
}

// TestEnsureExporterCoreResources_ConfigMapError covers the SECOND error branch
// of ensureExporterCoreResources: the credentials Secret is created, but the
// queries ConfigMap Create fails, so the configmap-error return is exercised
// (lifts the function past the secret-only path) and the span is codes.Error.
func TestEnsureExporterCoreResources_ConfigMapError(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				// Secret create succeeds; ConfigMap create fails.
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return assertExporterSecretBoom
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), nil, &wave345Recorder{}, nil)

	err := r.ensureExporterCoreResources(context.Background(), cluster,
		"exporter-password", "postgres://user@host:5432/db", r.logger)
	require.Error(t, err)
	assert.ErrorIs(t, err, assertExporterSecretBoom)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "controller.ensureExporterCoreResources" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code)
		}
	}
	assert.True(t, found, "controller.ensureExporterCoreResources span must exist")
}

// TestEnsureExporterCoreResources_Success covers the full happy path: all three
// sub-resources (Secret, ConfigMap, Service) are created on a clean client, so
// the ConfigMap-success and Service paths execute and the span ends with no
// error status.
func TestEnsureExporterCoreResources_Success(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(40),
		builder.NewBuilder(), nil, &wave345Recorder{}, nil)

	require.NoError(t, r.ensureExporterCoreResources(context.Background(), cluster,
		"exporter-password", "postgres://user@host:5432/db", r.logger))

	// All three exporter core resources now exist.
	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: util.ExporterCredentialsSecretName(cluster.Name), Namespace: cluster.Namespace}, secret))
	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: util.ExporterQueriesConfigMapName(cluster.Name), Namespace: cluster.Namespace}, cm))

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "controller.ensureExporterCoreResources" {
			found = true
			assert.NotEqual(t, codes.Error, s.Status().Code,
				"the span must NOT be errored on the success path")
		}
	}
	assert.True(t, found, "controller.ensureExporterCoreResources span must exist")
}

// TestHandleLifecyclePhaseReturnsErrors verifies the new three-value contract
// of handleLifecyclePhase directly for the Stopping branch.
func TestHandleLifecyclePhaseReturnsErrors(t *testing.T) {
	rec := &wave345Recorder{}
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	r, _ := newWave345Reconciler(rec, cluster)

	result, handled, err := r.handleLifecyclePhase(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ctrl.Result{}, result)
}

// ---------------------------------------------------------------------------
// C-5: real cloudberry_connections_max from the DB
// ---------------------------------------------------------------------------

// TestUpdateQueryStatusSetsRealConnectionsMax verifies the admin controller
// publishes the REAL max_connections value alongside the connection count.
func TestUpdateQueryStatusSetsRealConnectionsMax(t *testing.T) {
	rec := &wave345Recorder{}
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	factory := &mockDBClientFactory{client: &mockDBClient{maxConnections: 250}}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), factory, rec, nil)

	r.updateQueryStatusFromDB(context.Background(), cluster, r.logger)
	assert.Equal(t, []float64{250}, rec.connectionsMax)
}

// TestUpdateQueryStatusKeepsLastValueOnError verifies the gauge is NOT
// written (never zeroed) when the max_connections query fails.
func TestUpdateQueryStatusKeepsLastValueOnError(t *testing.T) {
	rec := &wave345Recorder{}
	scheme := newTestScheme()
	cluster := newTestCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	factory := &mockDBClientFactory{client: &mockDBClient{
		maxConnectionsErr: assertNewClientErr,
	}}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), factory, rec, nil)

	r.updateQueryStatusFromDB(context.Background(), cluster, r.logger)
	assert.Empty(t, rec.connectionsMax, "gauge must not be written (or zeroed) on error")
}
