package controller

import (
	"context"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// Scenario 117 — Recommendation Scan Across All Four Types (controller layer).
//
// These unit tests drive recordRecommendations / scanRecommendations /
// publishTableBloatRatios through reconcileStorage and
// refreshStorageOnSteadyState with a configurable mock DB returning per-type
// recommendation rows, a fake client (WithStatusSubresource for status patch),
// and the REAL PrometheusRecorder so the per-type gauges and the M.2==count
// invariant can be asserted via registry.Gather.

// recScanDBClient is a configurable db.Client that returns a fixed set of
// recommendations (or an error) per type, so the controller's count/metric/
// invariant behavior can be exercised deterministically. It captures the
// thresholds it was called with so threshold-threading (R.3) can be asserted.
type recScanDBClient struct {
	*mockDBClient

	bloatRecs []db.Recommendation
	skewRecs  []db.Recommendation
	ageRecs   []db.Recommendation
	indexRecs []db.Recommendation

	bloatErr error
	skewErr  error
	ageErr   error
	indexErr error

	gotThreshold db.RecommendationThresholds
	thresholdSet bool
}

func (m *recScanDBClient) GetBloatRecommendations(
	_ context.Context, th db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	m.gotThreshold = th
	m.thresholdSet = true
	return m.bloatRecs, m.bloatErr
}

func (m *recScanDBClient) GetSkewRecommendations(
	_ context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.skewRecs, m.skewErr
}

func (m *recScanDBClient) GetAgeRecommendations(
	_ context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.ageRecs, m.ageErr
}

func (m *recScanDBClient) GetIndexBloatRecommendations(
	_ context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.indexRecs, m.indexErr
}

// recScanCluster returns a cluster wired for the recommendation-scan path:
// DiskMonitoring on (required for the steady-state gate) and RecommendationScan
// enabled with the four CRD thresholds populated.
func recScanCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			BloatThreshold:      20,
			SkewThreshold:       30,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 40,
		},
	}
	return c
}

// rec is a small helper to build a recommendation row of a given type/table/ratio.
func rec(recType, schema, table string, ratio float64) db.Recommendation {
	return db.Recommendation{Type: recType, Schema: schema, Table: table, Ratio: ratio}
}

// recsTotalGaugeValue gathers cloudberry_recommendations_total for the given
// {cluster,namespace,type} series (0/false when absent).
func recsTotalGaugeValue(
	t *testing.T, reg *prometheus.Registry, cluster, namespace, recType string,
) (float64, bool) {
	t.Helper()
	return labeledGaugeValue(t, reg, "cloudberry_recommendations_total", map[string]string{
		"cluster": cluster, "namespace": namespace, "type": recType,
	})
}

// tableBloatRatioGaugeValue gathers cloudberry_table_bloat_ratio for the given
// {cluster,namespace,table} series (0/false when absent).
func tableBloatRatioGaugeValue(
	t *testing.T, reg *prometheus.Registry, cluster, namespace, table string,
) (float64, bool) {
	t.Helper()
	return labeledGaugeValue(t, reg, "cloudberry_table_bloat_ratio", map[string]string{
		"cluster": cluster, "namespace": namespace, "table": table,
	})
}

// labeledGaugeValue returns the value of the gauge series whose labels match all
// of the provided key/value pairs.
func labeledGaugeValue(
	t *testing.T, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			matched := true
			for k, v := range want {
				if labels[k] != v {
					matched = false
					break
				}
			}
			if matched {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// recScanReconciler builds a reconciler + fake client + real PrometheusRecorder
// for the given cluster and DB client.
func recScanReconciler(
	t *testing.T, cluster *cbv1alpha1.CloudberryCluster, dbClient db.Client,
) (*AdminReconciler, *prometheus.Registry) {
	t.Helper()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, recorder, nil)
	return r, reg
}

// ---------------------------------------------------------------------------
// 117-S2-R4-count — Status.RecommendationCount == sum of the four per-type counts.
// ---------------------------------------------------------------------------

func TestScenario117_CountIsSumOfAllTypes(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "t1", 25),
			rec(recTypeBloat, "public", "t2", 55),
		},
		skewRecs:  []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		ageRecs:   []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
		indexRecs: []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, _ := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// 2 bloat + 1 skew + 1 age + 1 index = 5. recordRecommendations owns the
	// in-memory count (S.2/R.4); the outer reconcile loop flushes it via the
	// end-of-reconcile status patch (verified on the steady-state path below).
	assert.Equal(t, int32(5), cluster.Status.RecommendationCount)
}

// 117-R3-processed — the controller threads the CRD thresholds into the queries.
func TestScenario117_ThresholdsThreadedFromSpec(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{mockDBClient: &mockDBClient{}}
	r, _ := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	require.True(t, dbClient.thresholdSet, "the bloat scan must run (R.3)")
	assert.Equal(t, db.RecommendationThresholds{
		Bloat: 20, Skew: 30, Age: 100000000, IndexBloat: 40,
	}, dbClient.gotThreshold, "the CRD thresholds must be threaded into the queries")
}

// ---------------------------------------------------------------------------
// 117-M2-bytype — cloudberry_recommendations_total{type} per type.
// 117-M2-count-invariant — sum of per-type gauges == Status.RecommendationCount.
// ---------------------------------------------------------------------------

func TestScenario117_PerTypeGaugesAndInvariant(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "t1", 25),
			rec(recTypeBloat, "public", "t2", 55),
		},
		skewRecs:  []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		ageRecs:   []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
		indexRecs: []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// M.2: each type gauge carries that type's count.
	wantByType := map[string]float64{
		recTypeBloat: 2, recTypeSkew: 1, recTypeAge: 1, recTypeIndexBloat: 1,
	}
	var sum float64
	for recType, want := range wantByType {
		got, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
		require.True(t, found, "recommendations_total{type=%s} must be published", recType)
		assert.InDelta(t, want, got, 0.001, "gauge for type %s", recType)
		sum += got
	}

	// M.2 == count invariant.
	assert.InDelta(t, float64(cluster.Status.RecommendationCount), sum, 0.001,
		"sum of per-type gauges must equal Status.RecommendationCount")
}

// ---------------------------------------------------------------------------
// 117a-M4 — table_bloat_ratio is published from the bloat recs.
// ---------------------------------------------------------------------------

func TestScenario117_TableBloatRatioFromBloatRecs(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "orders", 42),
			rec(recTypeBloat, "analytics", "events", 88),
		},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	v1, found := tableBloatRatioGaugeValue(t, reg, "test-cluster", "default", "public.orders")
	require.True(t, found, "table_bloat_ratio must be published for public.orders")
	assert.InDelta(t, 42.0, v1, 0.001)

	v2, found := tableBloatRatioGaugeValue(t, reg, "test-cluster", "default", "analytics.events")
	require.True(t, found)
	assert.InDelta(t, 88.0, v2, 0.001)
}

// ---------------------------------------------------------------------------
// 117a-CLEAR / 117b-BOUNDARY — a type returning 0 on the next scan drops its
// gauge to 0 AND decreases the count.
// ---------------------------------------------------------------------------

func TestScenario117_ClearResetsGaugeAndCount(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
		skewRecs:     []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	// First scan: 1 bloat + 1 skew = 2.
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, int32(2), cluster.Status.RecommendationCount)
	skew1, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recTypeSkew)
	require.True(t, found)
	assert.InDelta(t, 1.0, skew1, 0.001)

	// Second scan: skew clears (e.g. threshold raised above the coefficient).
	dbClient.skewRecs = nil
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// Count drops to 1 (only bloat remains).
	assert.Equal(t, int32(1), cluster.Status.RecommendationCount)

	// The skew gauge resets to 0 (published every scan, even when 0).
	skew2, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recTypeSkew)
	require.True(t, found, "skew gauge must still be published (reset to 0), not stale")
	assert.InDelta(t, 0.0, skew2, 0.001)
}

// ---------------------------------------------------------------------------
// 117-DISABLED-noop — scan disabled / nil → recordRecommendations not run, AND
// the C.4 clear-on-disable (Scenario 122) fires: a prior enabled->disabled scan
// leaves NO stale count/gauge. The count is reset to 0 and every per-type
// recommendations_total gauge is published as 0, WITHOUT running the scan (the
// count is CLEARED, not scanned). See task-breakdown §4.1.
// ---------------------------------------------------------------------------

func TestScenario117_Disabled_NoOp(t *testing.T) {
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
			cluster.Status.RecommendationCount = 7 // stale prior value must be CLEARED.

			dbClient := &recScanDBClient{
				mockDBClient: &mockDBClient{},
				bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
			}
			r, reg := recScanReconciler(t, cluster, dbClient)

			require.NoError(t, r.reconcileStorage(context.Background(), cluster))

			// C.4 clear-on-disable: the stale count is reset to 0.
			assert.Equal(t, int32(0), cluster.Status.RecommendationCount,
				"a stale count must be CLEARED to 0 when the scan is disabled (C.4)")
			// The scan itself still does NOT run — the count is cleared, not scanned.
			assert.False(t, dbClient.thresholdSet,
				"the bloat scan must NOT run when the scan is disabled")
			// Every per-type gauge is published as 0 (found==true, value 0).
			for _, recType := range []string{recTypeBloat, recTypeSkew, recTypeAge, recTypeIndexBloat} {
				v, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
				require.True(t, found,
					"recommendations_total{type=%s} must be published as 0 on disable (C.4)", recType)
				assert.InDelta(t, 0.0, v, 0.001,
					"recommendations_total{type=%s} must read 0 on disable", recType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 117-DBERR-nonfatal — a DB/connection error is non-fatal and never fabricates.
// ---------------------------------------------------------------------------

func TestScenario117_DBErr_NonFatal_NewClientFailure(t *testing.T) {
	cluster := recScanCluster()
	cluster.Status.RecommendationCount = 9 // prior value must survive a NewClient failure.

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	dbFactory := &mockDBClientFactory{err: assertNewClientErr}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, recorder, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster),
		"a NewClient failure must be non-fatal")
	assert.Equal(t, int32(9), cluster.Status.RecommendationCount,
		"count must NOT be fabricated when the DB client cannot be created")
}

// 117-DBERR-nonfatal (per-type) — a single Get* error skips that type only; the
// count is the sum of the SUCCESSFUL types and the reconcile still succeeds.
func TestScenario117_DBErr_PerTypeSkip(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
		skewErr:      assertNewClientErr, // skew fails → contributes 0, not fabricated.
		ageRecs:      []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
		indexRecs:    []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// 1 bloat + 0 skew (error) + 1 age + 1 index = 3.
	assert.Equal(t, int32(3), cluster.Status.RecommendationCount)
	skew, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recTypeSkew)
	require.True(t, found, "skew gauge must still be published as 0 on a per-type error")
	assert.InDelta(t, 0.0, skew, 0.001, "failing type must contribute 0, not a fabricated value")
}

// ---------------------------------------------------------------------------
// 117-R3-processed / 117-CONTROL — healthy reconcile succeeds; nil dbFactory is
// a safe no-op.
// ---------------------------------------------------------------------------

func TestScenario117_Control_HealthyReconcile(t *testing.T) {
	cluster := recScanCluster()
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
		skewRecs:     []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		ageRecs:      []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
		indexRecs:    []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, _ := recScanReconciler(t, cluster, dbClient)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster),
		"healthy reconcile must succeed (no false-positive control)")
	// StorageConfigured must be True after a successful storage reconcile (R.5).
	assert.Equal(t, metav1.ConditionTrue, storageConfiguredStatus(cluster))
}

func TestScenario117_Control_NilDBFactory(t *testing.T) {
	cluster := recScanCluster()
	cluster.Status.RecommendationCount = 4
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	// dbFactory == nil → recordRecommendations returns early (safe no-op).
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, recorder, nil)

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	})
	assert.Equal(t, int32(4), cluster.Status.RecommendationCount,
		"nil dbFactory must not change the count")
}

// ---------------------------------------------------------------------------
// Steady-state — refreshStorageOnSteadyState runs the scan + persists the count.
// ---------------------------------------------------------------------------

func TestScenario117_SteadyState_RunsScanAndPersists(t *testing.T) {
	cluster := recScanCluster()
	cluster.Status.RecommendationCount = 0
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
		skewRecs:     []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		indexRecs:    []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	// 1 bloat + 1 skew + 1 index = 3.
	assert.Equal(t, int32(3), cluster.Status.RecommendationCount)
	assert.Equal(t, int32(3), getCluster(t, r.client).Status.RecommendationCount,
		"steady-state count must be persisted")
	bloat, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recTypeBloat)
	require.True(t, found)
	assert.InDelta(t, 1.0, bloat, 0.001)
}

func TestScenario117_SteadyState_DisabledNoOp(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring:     true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{Enabled: false},
	}
	cluster.Status.RecommendationCount = 6
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
	}
	r, _ := recScanReconciler(t, cluster, dbClient)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.Equal(t, int32(0), cluster.Status.RecommendationCount,
		"disabled steady-state scan must clear the count to 0 (C.4)")
	assert.False(t, dbClient.thresholdSet, "the scan must not run when disabled")
}

// ---------------------------------------------------------------------------
// Direct unit on recordRecommendations / publishTableBloatRatios (helper-level).
// ---------------------------------------------------------------------------

// TestScenario117_RecordRecommendations_InvariantInSinglePass asserts the
// M.2==count invariant at the helper level: the value summed into the status
// equals the sum published per type, both derived from the SAME single pass.
func TestScenario117_RecordRecommendations_InvariantInSinglePass(t *testing.T) {
	cluster := recScanCluster()
	cluster.Status.RecommendationCount = 99 // stale prior value must be replaced.
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs: []db.Recommendation{
			rec(recTypeBloat, "public", "t1", 25),
			rec(recTypeBloat, "public", "t2", 55),
			rec(recTypeBloat, "public", "t3", 60),
		},
		ageRecs: []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	r.recordRecommendations(context.Background(), cluster, slog.Default())

	assert.Equal(t, int32(4), cluster.Status.RecommendationCount,
		"stale prior count (99) must be replaced by the current scan total (4)")

	var sum float64
	for _, recType := range []string{recTypeBloat, recTypeSkew, recTypeAge, recTypeIndexBloat} {
		v, found := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
		require.True(t, found, "every type gauge must be published (incl. 0): %s", recType)
		sum += v
	}
	assert.InDelta(t, float64(cluster.Status.RecommendationCount), sum, 0.001)
}

// TestScenario117_PublishTableBloatRatios_CapsCardinality covers the
// maxBloatRatioTables cap directly: with more than the cap bloat recs, only the
// first maxBloatRatioTables are published.
func TestScenario117_PublishTableBloatRatios_CapsCardinality(t *testing.T) {
	cluster := recScanCluster()
	recorder := &recCapRecorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithStatusSubresource(cluster).Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, recorder, nil)

	var recs []db.Recommendation
	for i := 0; i < maxBloatRatioTables+5; i++ {
		recs = append(recs, rec(recTypeBloat, "public", "t", float64(i)))
	}
	r.publishTableBloatRatios(cluster, recs, slog.Default())

	assert.Equal(t, maxBloatRatioTables, recorder.bloatCalls,
		"only maxBloatRatioTables ratios may be published")
}

// recCapRecorder counts SetTableBloatRatio calls for the cap test.
type recCapRecorder struct {
	metrics.NoopRecorder
	bloatCalls int
}

func (r *recCapRecorder) SetTableBloatRatio(_, _, _ string, _ float64) {
	r.bloatCalls++
}

// storageConfiguredStatus returns the StorageConfigured condition status
// (or the unknown status when the condition is absent).
func storageConfiguredStatus(cluster *cbv1alpha1.CloudberryCluster) metav1.ConditionStatus {
	for i := range cluster.Status.Conditions {
		c := cluster.Status.Conditions[i]
		if c.Type == string(cbv1alpha1.ConditionStorageConfigured) {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}
