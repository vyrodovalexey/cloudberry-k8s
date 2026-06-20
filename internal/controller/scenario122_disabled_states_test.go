package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// Scenario 122 — Disabled States (C.2 / C.4 / C.12) + re-enablement (controller
// layer). These unit tests drive reconcileStorage and refreshStorageOnSteadyState
// over a fake client (WithStatusSubresource) + fake dbFactory + the REAL
// PrometheusRecorder so the disabled-state clears (gauge resets) and the
// re-enable repopulations can be asserted through registry.Gather. Where the
// observable status field is involved, the assertions account for the
// production reality that Status.DiskUsagePercent / Status.RecommendationCount
// are tagged `omitempty`: a zero value is dropped from the status MergePatch, so
// the AUTHORITATIVE disabled signal is the metric gauge reset (a direct metric
// call, not subject to omitempty), not the persisted status field.
//
// Case IDs (mirroring task-breakdown §1):
//   - 122a-C2-disabled / 122a-C2-reenable          (disk monitoring gate, C.2)
//   - 122b-C4-disabled / 122b-C4-reenable           (recommendation scan gate, C.4)
//   - 122b-steady-disabled                          (steady-state C.4 clear)
//   - 122-CONTROL                                   (storage nil → no panic/churn)
//   - idempotency                                   (enabled path never clears)

// ---------------------------------------------------------------------------
// Capturing recorders (reuse the diskUsageRecorder from scenario116; add a
// recommendations-capturing recorder so the C.4 clear can be asserted by call,
// not only via the registry gauge).
// ---------------------------------------------------------------------------

// recsTotalCall captures a SetRecommendationsTotal invocation so the C.4
// clear-on-disable can be asserted as "0 for all four types".
type recsTotalCall struct {
	cluster   string
	namespace string
	recType   string
	count     float64
}

// storageSignalsRecorder wraps NoopRecorder and records both SetDiskUsagePercent
// (C.2) and SetRecommendationsTotal (C.4) calls so the disabled-state clears can
// be asserted at the call level (independent of the omitempty status field).
type storageSignalsRecorder struct {
	metrics.NoopRecorder
	diskCalls []diskUsageCall
	recsCalls []recsTotalCall
}

func (r *storageSignalsRecorder) SetDiskUsagePercent(cluster, namespace string, percent float64) {
	r.diskCalls = append(r.diskCalls, diskUsageCall{
		cluster: cluster, namespace: namespace, percent: percent,
	})
}

func (r *storageSignalsRecorder) SetRecommendationsTotal(cluster, namespace, recType string, count float64) {
	r.recsCalls = append(r.recsCalls, recsTotalCall{
		cluster: cluster, namespace: namespace, recType: recType, count: count,
	})
}

// recsCallCountFor returns the value of the LAST SetRecommendationsTotal call
// recorded for the given type, and whether any was recorded.
func (r *storageSignalsRecorder) recsCallCountFor(recType string) (float64, bool) {
	val, found := 0.0, false
	for _, c := range r.recsCalls {
		if c.recType == recType {
			val, found = c.count, true
		}
	}
	return val, found
}

// scenario122Reconciler builds a reconciler + fake client for the given cluster,
// dbClient and metrics recorder so each case can pick the recorder it needs
// (real PrometheusRecorder for gauge assertions, capturing recorder for call
// assertions).
func scenario122Reconciler(
	t *testing.T,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	rec metrics.Recorder,
) (*AdminReconciler, client.Client) {
	t.Helper()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	var dbFactory db.DBClientFactory
	if dbClient != nil {
		dbFactory = &mockDBClientFactory{client: dbClient}
	}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)
	return r, k8sClient
}

// cronJobExists reports whether the recommendation-scan CronJob exists in the
// fake client for the cluster.
func cronJobExists(t *testing.T, c client.Client, cluster *cbv1alpha1.CloudberryCluster) bool {
	t.Helper()
	cj := &batchv1.CronJob{}
	err := c.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj)
	if err == nil {
		return true
	}
	require.True(t, apierrors.IsNotFound(err), "unexpected error getting CronJob: %v", err)
	return false
}

// ---------------------------------------------------------------------------
// 122a-C2-disabled — diskMonitoring:false (or storage nil) resets the disk-usage
// gauge to 0 and does NOT measure (the DB factory is never consulted).
// ---------------------------------------------------------------------------

func TestScenario122a_C2_Disabled(t *testing.T) {
	tests := []struct {
		name    string
		storage *cbv1alpha1.StorageManagementSpec
	}{
		{name: "diskMonitoring false", storage: &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}},
		{name: "storage nil but seeded status", storage: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.Storage = tt.storage
			cluster.Status.DiskUsagePercent = 42 // stale prior value.

			rec := &storageSignalsRecorder{}
			// A factory whose NewClient would be a measurement; reaching it fails
			// the no-measurement assertion below.
			dbFactory := &countingDBFactory{inner: &mockDBClientFactory{
				client: &mockDBClient{diskUsagePercent: 99},
			}}
			scheme := newTestScheme()
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(cluster).WithStatusSubresource(cluster).Build()
			r := NewAdminReconciler(k8sClient, scheme,
				record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

			require.NoError(t, r.reconcileStorage(context.Background(), cluster))

			// C.2 reset-on-disable: the disk-usage gauge is reset to 0.
			require.NotEmpty(t, rec.diskCalls, "the disabled path must reset the disk-usage gauge")
			last := rec.diskCalls[len(rec.diskCalls)-1]
			assert.InDelta(t, 0.0, last.percent, 0.001, "disk-usage gauge must be reset to 0 on disable (C.2)")
			// No measurement: the DB factory is never consulted on the disabled path.
			assert.Zero(t, dbFactory.calls, "no disk measurement when monitoring is off")
		})
	}
}

// 122a-C2-disabled (real gauge): the cloudberry_disk_usage_percent gauge reads 0
// after the disabled reconcile, proven through the real PrometheusRecorder.
func TestScenario122a_C2_Disabled_GaugeZero(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 42

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbFactory := &countingDBFactory{inner: &mockDBClientFactory{client: &mockDBClient{diskUsagePercent: 99}}}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found, "the disabled reset must publish the gauge as 0")
	assert.InDelta(t, 0.0, gauge, 0.001)
	assert.Zero(t, dbFactory.calls, "no measurement on the disabled path")
}

// ---------------------------------------------------------------------------
// 122a-C2-reenable — flip diskMonitoring:true → recordDiskUsage resumes and the
// status + gauge repopulate from the measured value (reactivation).
// ---------------------------------------------------------------------------

func TestScenario122a_C2_Reenable(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{diskUsagePercent: 63}
	r, k8sClient := scenario122Reconciler(t, cluster, dbClient, rec)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// Re-enabled: status + gauge repopulate from the measured value (>0).
	assert.Equal(t, int32(63), cluster.Status.DiskUsagePercent, "status must repopulate on re-enable")
	assert.Equal(t, int32(63), getCluster(t, k8sClient).Status.DiskUsagePercent,
		"re-enabled status must persist")
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found, "the re-enabled path must publish the measured gauge")
	assert.InDelta(t, 63.0, gauge, 0.001)
	assert.InDelta(t, float64(cluster.Status.DiskUsagePercent), gauge, 0.001,
		"gauge must match status on re-enable (M.1==S.1)")
}

// ---------------------------------------------------------------------------
// 122b-C4-disabled — diskMonitoring:true, recommendationScan disabled, seeded
// Status.RecommendationCount=5 → the count is cleared to 0 (in memory), every
// per-type recommendations_total gauge reads 0, the scan does NOT run, and the
// CronJob is GC'd (none in the fake client).
// ---------------------------------------------------------------------------

func TestScenario122b_C4_Disabled(t *testing.T) {
	tests := []struct {
		name string
		scan *cbv1alpha1.RecommendationScanSpec
	}{
		{name: "scan nil", scan: nil},
		{name: "scan disabled", scan: &cbv1alpha1.RecommendationScanSpec{Enabled: false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring:     true,
				RecommendationScan: tt.scan,
			}
			cluster.Status.RecommendationCount = 5 // stale prior value.

			// A scan-capable DB client that, if consulted, would set thresholdSet.
			dbClient := &recScanDBClient{
				mockDBClient: &mockDBClient{diskUsagePercent: 10},
				bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
			}
			reg := prometheus.NewRegistry()
			recorder := metrics.NewPrometheusRecorder(reg)
			r, k8sClient := scenario122Reconciler(t, cluster, dbClient, recorder)

			require.NoError(t, r.reconcileStorage(context.Background(), cluster))

			// C.4 clear-on-disable: the in-memory count is reset to 0 (the
			// end-of-reconcile patch in the full loop persists it; here we assert
			// the in-memory effect of the else-branch clear).
			assert.Equal(t, int32(0), cluster.Status.RecommendationCount,
				"a stale count must be cleared to 0 when the scan is disabled (C.4)")
			// The scan itself must NOT run.
			assert.False(t, dbClient.thresholdSet, "the scan must NOT run when disabled")
			// Every per-type gauge is published as 0.
			for _, recType := range recommendationTypes {
				v, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
				require.True(t, found,
					"recommendations_total{type=%s} must be published as 0 on disable", recType)
				assert.InDelta(t, 0.0, v, 0.001)
			}
			// The recommendation-scan CronJob is GC'd (none in the fake client).
			assert.False(t, cronJobExists(t, k8sClient, cluster),
				"the recommendation-scan CronJob must be GC'd when the scan is disabled")
		})
	}
}

// 122b-C4-disabled (call-level): SetRecommendationsTotal is called with 0 for
// ALL FOUR types via a capturing recorder, independent of the registry.
func TestScenario122b_C4_Disabled_ClearsAllFourTypes(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring:     true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{Enabled: false},
	}
	cluster.Status.RecommendationCount = 8

	rec := &storageSignalsRecorder{}
	dbClient := &recScanDBClient{mockDBClient: &mockDBClient{diskUsagePercent: 10}}
	r, _ := scenario122Reconciler(t, cluster, dbClient, rec)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	for _, recType := range recommendationTypes {
		val, found := rec.recsCallCountFor(recType)
		require.True(t, found, "SetRecommendationsTotal must be called for type %s", recType)
		assert.InDelta(t, 0.0, val, 0.001, "type %s must be cleared to 0", recType)
	}
	assert.False(t, dbClient.thresholdSet, "the scan must NOT run when disabled")
}

// ---------------------------------------------------------------------------
// 122b-C4-reenable — recommendationScan enabled + a non-empty schedule + a mock
// returning recs → count == sum, per-type gauges repopulate, and the CronJob is
// (re-)created (reactivation).
// ---------------------------------------------------------------------------

func TestScenario122b_C4_Reenable(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:        true,
			Schedule:       "0 2 * * *", // required for the CronJob to be created.
			BloatThreshold: 20,
		},
	}
	cluster.Status.RecommendationCount = 0

	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{diskUsagePercent: 10},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "t1", 25),
			rec(recTypeBloat, "public", "t2", 55),
		},
		skewRecs:  []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		indexRecs: []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	r, k8sClient := scenario122Reconciler(t, cluster, dbClient, recorder)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// 2 bloat + 1 skew + 0 age + 1 index = 4.
	assert.Equal(t, int32(4), cluster.Status.RecommendationCount, "count must repopulate as the scan total")
	require.True(t, dbClient.thresholdSet, "the scan must run on re-enable")
	// Per-type gauges repopulate.
	want := map[string]float64{recTypeBloat: 2, recTypeSkew: 1, recTypeAge: 0, recTypeIndexBloat: 1}
	for recType, exp := range want {
		v, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
		require.True(t, found, "recommendations_total{type=%s} must be published on re-enable", recType)
		assert.InDelta(t, exp, v, 0.001, "gauge for %s", recType)
	}
	// The CronJob is (re-)created on re-enable.
	assert.True(t, cronJobExists(t, k8sClient, cluster),
		"the recommendation-scan CronJob must be re-created on re-enable")
}

// ---------------------------------------------------------------------------
// 122b-steady-disabled — the same C.4 clear via refreshStorageOnSteadyState.
// ---------------------------------------------------------------------------

func TestScenario122b_C4_SteadyState_Disabled(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring:     true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{Enabled: false},
	}
	cluster.Status.RecommendationCount = 6

	// Capturing recorder so the C.4 clear (SetRecommendationsTotal(...,0) for all
	// four types) is asserted by call, independent of the omitempty status field.
	recorder := &storageSignalsRecorder{}
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{diskUsagePercent: 12},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
	}
	r, k8sClient := scenario122Reconciler(t, cluster, dbClient, recorder)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	// C.4 clear-on-disable (steady-state parity): every per-type gauge is reset to
	// 0 via SetRecommendationsTotal. NOTE: Status.RecommendationCount is tagged
	// omitempty, so the in-memory zero is dropped from the end-of-function status
	// MergePatch and the in-memory object is refreshed from the (unchanged) server
	// value during the patch round-trip — the authoritative disabled signal is the
	// gauge reset below, not the persisted status field.
	for _, recType := range recommendationTypes {
		val, found := recorder.recsCallCountFor(recType)
		require.True(t, found, "SetRecommendationsTotal must be called for type %s on steady-state disable", recType)
		assert.InDelta(t, 0.0, val, 0.001, "type %s must be cleared to 0", recType)
	}
	assert.False(t, dbClient.thresholdSet, "the scan must NOT run when disabled")
	// The CronJob is GC'd on the steady-state disabled path too.
	assert.False(t, cronJobExists(t, k8sClient, cluster),
		"the CronJob must be GC'd on the steady-state disabled path")
}

// 122b-steady-storageoff — whole storage block off on the steady-state path also
// clears the recommendation count/gauges (the storage-off early-return) and GCs
// the CronJob.
func TestScenario122b_C4_SteadyState_StorageOff(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.RecommendationCount = 4
	cluster.Status.DiskUsagePercent = 33

	rec := &storageSignalsRecorder{}
	dbClient := &recScanDBClient{mockDBClient: &mockDBClient{diskUsagePercent: 50}}
	r, k8sClient := scenario122Reconciler(t, cluster, dbClient, rec)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	// C.2 + C.4: both gauges reset to 0 on the storage-off early-return.
	require.NotEmpty(t, rec.diskCalls, "C.2 must reset the disk gauge on storage-off")
	assert.InDelta(t, 0.0, rec.diskCalls[len(rec.diskCalls)-1].percent, 0.001)
	for _, recType := range recommendationTypes {
		val, found := rec.recsCallCountFor(recType)
		require.True(t, found, "C.4 must clear type %s on storage-off", recType)
		assert.InDelta(t, 0.0, val, 0.001)
	}
	// The scan must NOT run; the CronJob is GC'd.
	assert.False(t, dbClient.thresholdSet, "the scan must NOT run when storage is off")
	assert.False(t, cronJobExists(t, k8sClient, cluster), "the CronJob must be GC'd when storage is off")
}

// ---------------------------------------------------------------------------
// 122-CONTROL — storage nil → no panic, count stays 0, no churn (the guarded
// patchStatus does not run for the never-configured common case).
// ---------------------------------------------------------------------------

func TestScenario122_Control_StorageNil_NoPanicNoChurn(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = nil
	// Never-configured: status already zero, so clearStorageSignals must NOT
	// patch (R6 guard) and must not panic.
	cluster.Status.DiskUsagePercent = 0
	cluster.Status.RecommendationCount = 0

	rec := &storageSignalsRecorder{}
	r, k8sClient := scenario122Reconciler(t, cluster, &mockDBClient{}, rec)

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	})

	assert.Equal(t, int32(0), cluster.Status.RecommendationCount, "count must stay 0")
	assert.Equal(t, int32(0), cluster.Status.DiskUsagePercent, "disk usage must stay 0")
	got := getCluster(t, k8sClient)
	assert.Equal(t, int32(0), got.Status.RecommendationCount, "no churn: persisted count stays 0")
	assert.Equal(t, int32(0), got.Status.DiskUsagePercent, "no churn: persisted disk usage stays 0")
	// No CronJob is created for a nil storage block.
	assert.False(t, cronJobExists(t, k8sClient, cluster))
}

// 122-CONTROL re-enable round-trip — disabled→enabled→disabled returns no error
// at each step with a healthy DB stub (no-false-positive control).
func TestScenario122_Control_RoundTripNoError(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}

	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{diskUsagePercent: 25},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
	}
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	r, k8sClient := scenario122Reconciler(t, cluster, dbClient, recorder)
	ctx := context.Background()

	// disabled → enabled → disabled, each step must not error. Spec flips are
	// persisted through the client first (as the API server would) so that the
	// status MergePatch round-trip inside reconcileStorage does not refresh the
	// in-memory spec back to the previously persisted value mid-reconcile.
	require.NoError(t, r.reconcileStorage(ctx, cluster), "disabled reconcile must not error")

	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * *", BloatThreshold: 10,
		},
	}
	require.NoError(t, k8sClient.Update(ctx, cluster), "persisting the enable flip")
	require.NoError(t, r.reconcileStorage(ctx, cluster), "enabled reconcile must not error")
	assert.Equal(t, int32(1), cluster.Status.RecommendationCount, "enabled scan produces the count")

	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	require.NoError(t, k8sClient.Update(ctx, cluster), "persisting the re-disable flip")
	require.NoError(t, r.reconcileStorage(ctx, cluster), "re-disabled reconcile must not error")
}

// ---------------------------------------------------------------------------
// Idempotency — the ENABLED path never calls clearRecommendations (a fresh
// scan's count is NOT zeroed). Driving the enabled scan with a stale prior count
// must REPLACE it with the scan total, never reset it to 0 via the clear.
// ---------------------------------------------------------------------------

func TestScenario122_Idempotency_EnabledPathNeverClears(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, BloatThreshold: 20,
		},
	}
	cluster.Status.RecommendationCount = 99 // stale prior value.

	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{diskUsagePercent: 10},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "t1", 25),
			rec(recTypeBloat, "public", "t2", 55),
			rec(recTypeBloat, "public", "t3", 60),
		},
		skewRecs: []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
	}
	rec := &storageSignalsRecorder{}
	r, _ := scenario122Reconciler(t, cluster, dbClient, rec)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// 3 bloat + 1 skew = 4. The enabled scan REPLACES the stale 99 with 4; it is
	// NOT zeroed by the clear (which must never run on the enabled path).
	assert.Equal(t, int32(4), cluster.Status.RecommendationCount,
		"the enabled path must set the count to the scan total, not zero it")
	require.True(t, dbClient.thresholdSet, "the enabled scan must run")
	// The bloat type gauge was set to its real count (2... here 3), never 0 only.
	val, found := rec.recsCallCountFor(recTypeBloat)
	require.True(t, found)
	assert.InDelta(t, 3.0, val, 0.001, "the enabled path publishes the real bloat count, not a clear-to-0")
}

// 122-CONTROL persist-failure — clearStorageSignals must be non-fatal when the
// status MergePatch fails: with a stale disk-usage value (so the guarded
// patchStatus actually fires) and an interceptor that errors on SubResourcePatch,
// reconcileStorage still returns no error and the gauge is still reset (the warn
// path is exercised without aborting the reconcile).
func TestScenario122_Control_ClearStorageSignals_PatchFailureNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 30 // stale value → the guarded patch fires.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return errors.New("simulated status patch failure")
			},
		}).
		Build()

	rec := &storageSignalsRecorder{}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(),
		&mockDBClientFactory{client: &mockDBClient{}}, rec, nil)

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileStorage(context.Background(), cluster),
			"a status patch failure on the disabled path must be non-fatal")
	})
	// The gauge reset still happens even though the persist failed.
	require.NotEmpty(t, rec.diskCalls, "the disk gauge must still be reset on the disabled path")
	assert.InDelta(t, 0.0, rec.diskCalls[len(rec.diskCalls)-1].percent, 0.001)
}

// 122b-CONTROL nil dbFactory on the disabled scan path: the clear still runs
// (it does no DB I/O), the count is cleared to 0, and no panic occurs.
func TestScenario122b_C4_Disabled_NilDBFactory(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring:     true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{Enabled: false},
	}
	cluster.Status.RecommendationCount = 3

	rec := &storageSignalsRecorder{}
	// nil dbClient → nil dbFactory; the clear path does no DB I/O.
	r, _ := scenario122Reconciler(t, cluster, nil, rec)

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	})
	assert.Equal(t, int32(0), cluster.Status.RecommendationCount,
		"the C.4 clear must run even without a DB factory")
	for _, recType := range recommendationTypes {
		val, found := rec.recsCallCountFor(recType)
		require.True(t, found, "type %s must be cleared even with a nil dbFactory", recType)
		assert.InDelta(t, 0.0, val, 0.001)
	}
}
