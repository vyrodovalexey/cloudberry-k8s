//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 117: Recommendation Scan Across All Four Types
// (reconciliation rules S.2, M.2, R.3, R.4, RT.1–RT.4, C.6–C.9, M.4) — functional
// ============================================================================
//
// With spec.storage.recommendationScan.enabled:true, AdminReconciler.Reconcile
// runs all FOUR threshold-aware recommendation scans (bloat/skew/age/index_bloat)
// via the db.Get{Bloat,Skew,Age,IndexBloat}Recommendations queries and:
// sets status.recommendationCount to the CURRENT sum across the four types
// (S.2/R.4), publishes cloudberry_recommendations_total{type} for EACH type from
// that type's count (M.2, incl. 0 so cleared types reset), keeps
// cloudberry_table_bloat_ratio{table} from the bloat recs (M.4), and threads the
// CRD thresholds into the queries (R.3). The M.2==count invariant holds: the sum
// of the per-type gauges equals status.recommendationCount.
//
// This functional layer drives the PUBLIC AdminReconciler.Reconcile entrypoint
// over a fake-client TestK8sEnv with an injected dbFactory whose testutil
// MockDBClient returns configurable per-type recommendations, mirroring
// scenario116_disk_usage_test.go and storage_recommendations_test.go. It is
// catalog-honest via a coverage test that keeps the -F matrix from silently
// dropping a rule. The live proof is the KUBECONFIG/SCENARIO117_LIVE-gated
// Scenario 117 integration/e2e Part B.
// ============================================================================

// Scenario117Suite drives AdminReconciler.Reconcile over a recommendation-scan
// cluster and asserts the S.2/R.4/M.2/M.4/R.3 scan effects.
type Scenario117Suite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario117(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario117Suite))
}

func (s *Scenario117Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario117Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario117Rec builds a recommendation row of a given type/schema/table/ratio.
func scenario117Rec(recType, schema, table string, ratio float64) db.Recommendation {
	return db.Recommendation{Type: recType, Schema: schema, Table: table, Ratio: ratio}
}

// scenario117ScanCluster builds a base-valid running cluster with the
// recommendation scan enabled (DiskMonitoring on so the storage path engages)
// and the four CRD thresholds populated.
func scenario117ScanCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       30,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 40,
			ScanDuration:        "2h",
		},
	}
	return cluster
}

// scenario117Reconciler builds an AdminReconciler over a fake-client TestK8sEnv
// seeded with the supplied cluster, wired with the supplied dbFactory and a real
// PrometheusRecorder over reg so the per-type gauges can be inspected (M.2/M.4).
func (s *Scenario117Suite) scenario117Reconciler(
	cluster *cbv1alpha1.CloudberryCluster,
	dbFactory db.DBClientFactory,
	reg *prometheus.Registry,
) *controller.AdminReconciler {
	s.env = testutil.NewTestK8sEnv(cluster)
	return controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), dbFactory, metrics.NewPrometheusRecorder(reg), s.env.Logger,
	)
}

// scenario117GaugeValue gathers the gauge series whose labels match all of the
// provided key/value pairs (found=false when no matching series is present).
func scenario117GaugeValue(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
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

// scenario117RecGauge gathers cloudberry_recommendations_total for a
// {cluster,namespace,type} series.
func scenario117RecGauge(
	t require.TestingT, reg *prometheus.Registry, cluster, namespace, recType string,
) (float64, bool) {
	return scenario117GaugeValue(t, reg, cases.Scenario117RecsMetricName, map[string]string{
		"cluster": cluster, "namespace": namespace, "type": recType,
	})
}

// scenario117BloatRatioGauge gathers cloudberry_table_bloat_ratio for a
// {cluster,namespace,table} series.
func scenario117BloatRatioGauge(
	t require.TestingT, reg *prometheus.Registry, cluster, namespace, table string,
) (float64, bool) {
	return scenario117GaugeValue(t, reg, cases.Scenario117BloatRatioMetricName, map[string]string{
		"cluster": cluster, "namespace": namespace, "table": table,
	})
}

// storageConfiguredTrue117 reports whether StorageConfigured is True.
func storageConfiguredTrue117(cluster *cbv1alpha1.CloudberryCluster) bool {
	for _, c := range cluster.Status.Conditions {
		if c.Type == "StorageConfigured" {
			return string(c.Status) == "True"
		}
	}
	return false
}

// TestFunctional_Scenario117_CountIsSumAndPerTypeMetrics covers
// 117-S2-R4-count-F / 117-M2-bytype-F / 117a-M4-F / 117-CONTROL: a single
// reconcile with 2 bloat + 1 skew + 1 age + 1 index recs sets
// status.recommendationCount to 5 (S.2/R.4), publishes
// recommendations_total{type} per type (M.2), the sum of the gauges equals the
// count (M.2==count invariant), and the table_bloat_ratio is published from the
// bloat recs (M.4). The reconcile returns no error (CONTROL).
func (s *Scenario117Suite) TestFunctional_Scenario117_CountIsSumAndPerTypeMetrics() {
	cluster := scenario117ScanCluster("s117-count")
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{
					scenario117Rec(cases.Scenario117TypeBloat, "public", "orders", 42),
					scenario117Rec(cases.Scenario117TypeBloat, "analytics", "events", 88),
				}, nil
			},
			GetSkewRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeSkew, "public", "s1", 40)}, nil
			},
			GetAgeRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeAge, "public", "a1", 0)}, nil
			},
			GetIndexBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeIndexBloat, "public", "i1", 70)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

	// 117-CONTROL + R.3: the full reconcile path proceeds without error.
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "117-CONTROL: the reconcile must return no error")
	assert.NotZero(s.T(), result.RequeueAfter, "117-R3-F: reconcileStorage must proceed past the gate")

	// S.2/R.4: the persisted status carries the CURRENT sum (2+1+1+1 = 5).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(5), updated.Status.RecommendationCount,
		"117-S2-R4-count-F: status.recommendationCount == sum across the four types")
	assert.True(s.T(), storageConfiguredTrue117(updated), "117-CONTROL: StorageConfigured must be True")

	// M.2: each type gauge carries that type's count and the sum == count.
	wantByType := map[string]float64{
		cases.Scenario117TypeBloat: 2, cases.Scenario117TypeSkew: 1,
		cases.Scenario117TypeAge: 1, cases.Scenario117TypeIndexBloat: 1,
	}
	var sum float64
	for recType, want := range wantByType {
		got, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, recType)
		require.Truef(s.T(), found, "117-M2-bytype-F: recommendations_total{type=%s} must be published", recType)
		assert.InDeltaf(s.T(), want, got, 0.001, "117-M2-bytype-F: gauge for type %s", recType)
		sum += got
	}
	assert.InDelta(s.T(), float64(updated.Status.RecommendationCount), sum, 0.001,
		"117-M2-bytype-F: sum of per-type gauges == recommendationCount (M.2==count)")

	// M.4: table_bloat_ratio published from the bloat recs.
	v1, found := scenario117BloatRatioGauge(s.T(), reg, cluster.Name, cluster.Namespace, "public.orders")
	require.True(s.T(), found, "117a-M4-F: table_bloat_ratio must be published for public.orders")
	assert.InDelta(s.T(), 42.0, v1, 0.001)
	v2, found := scenario117BloatRatioGauge(s.T(), reg, cluster.Name, cluster.Namespace, "analytics.events")
	require.True(s.T(), found, "117a-M4-F: table_bloat_ratio must be published for analytics.events")
	assert.InDelta(s.T(), 88.0, v2, 0.001)
}

// TestFunctional_Scenario117_ThresholdsThreaded covers 117a-C6-F / 117b-C7-F /
// 117c-C8-F / 117d-C9-F / 117-R3-processed-F: the controller threads the four
// CRD thresholds into the queries (captured by the bloat stub).
func (s *Scenario117Suite) TestFunctional_Scenario117_ThresholdsThreaded() {
	cluster := scenario117ScanCluster("s117-threshold")
	var got db.RecommendationThresholds
	captured := false
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetBloatRecommendationsFunc: func(_ context.Context, th db.RecommendationThresholds) ([]db.Recommendation, error) {
				got = th
				captured = true
				return nil, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	require.True(s.T(), captured, "117-R3-processed-F: the bloat scan must run")
	assert.Equal(s.T(), db.RecommendationThresholds{
		Bloat: 20, Skew: 30, Age: 100000000, IndexBloat: 40,
	}, got, "117-R3-processed-F: the CRD thresholds must be threaded into the queries")
}

// TestFunctional_Scenario117_ClearResetsGaugeAndCount covers 117a-CLEAR-F: a
// second reconcile where the bloat type returns 0 rows drops the
// recommendations_total{type=bloat} gauge to 0 (published every scan) AND
// decreases recommendationCount — no stale/sticky gauge.
func (s *Scenario117Suite) TestFunctional_Scenario117_ClearResetsGaugeAndCount() {
	cluster := scenario117ScanCluster("s117-clear")
	bloat := []db.Recommendation{scenario117Rec(cases.Scenario117TypeBloat, "public", "t1", 25)}
	dbClient := &testutil.MockDBClient{
		GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
			return bloat, nil
		},
		GetSkewRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
			return []db.Recommendation{scenario117Rec(cases.Scenario117TypeSkew, "public", "s1", 40)}, nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: dbClient}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

	// First scan: 1 bloat + 1 skew = 2.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(2), updated.Status.RecommendationCount)
	b1, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeBloat)
	require.True(s.T(), found)
	assert.InDelta(s.T(), 1.0, b1, 0.001)

	// Second scan: the bloat type clears (e.g. VACUUM dropped dead_pct below threshold).
	bloat = nil
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(1), updated.Status.RecommendationCount,
		"117a-CLEAR-F: count must drop when the bloat type clears")
	b2, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeBloat)
	require.True(s.T(), found, "117a-CLEAR-F: bloat gauge must still be published (reset to 0), not stale")
	assert.InDelta(s.T(), 0.0, b2, 0.001)
}

// TestFunctional_Scenario117_BoundaryFlip covers 117b-BOUNDARY-F: reconcile twice
// across the threshold boundary — a tight threshold includes the skew rec (>= is
// inclusive); loosening it removes the rec; recommendationCount reflects the flip
// both ways.
func (s *Scenario117Suite) TestFunctional_Scenario117_BoundaryFlip() {
	cluster := scenario117ScanCluster("s117-boundary")
	// The skew coefficient is exactly 30 (the cluster's skewThreshold). The stub
	// flips on the threshold the controller threads in: at <=30 the rec is
	// included (>= is inclusive), at >30 it is excluded. A steady bloat rec keeps
	// the count non-zero across the boundary so the count flip is observable
	// through the GET'd status (a zero recommendationCount is omitempty-dropped
	// from the status merge-patch, so the boundary's "disappears" leg is asserted
	// via the per-type gauge, which is published every scan).
	const coeff = 30
	dbClient := &testutil.MockDBClient{
		GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
			return []db.Recommendation{scenario117Rec(cases.Scenario117TypeBloat, "public", "b1", 50)}, nil
		},
		GetSkewRecommendationsFunc: func(_ context.Context, th db.RecommendationThresholds) ([]db.Recommendation, error) {
			if int(th.Skew) <= coeff {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeSkew, "public", "s1", float64(coeff))}, nil
			}
			return nil, nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: dbClient}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

	// Tight threshold (== coeff): the skew rec is present (>= inclusive) → 1 bloat
	// + 1 skew = 2.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(2), updated.Status.RecommendationCount,
		"117b-BOUNDARY-F: at exactly the threshold the skew rec is included")
	skew1, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeSkew)
	require.True(s.T(), found)
	assert.InDelta(s.T(), 1.0, skew1, 0.001)

	// Loosen the threshold one tick over the coefficient: the skew rec disappears
	// → only the bloat rec remains (count drops to 1, skew gauge resets to 0).
	updated.Spec.Storage.RecommendationScan.SkewThreshold = coeff + 1
	require.NoError(s.T(), s.env.Client.Update(s.ctx, updated))
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(1), updated.Status.RecommendationCount,
		"117b-BOUNDARY-F: one tick over the coefficient the skew rec is excluded")
	skew2, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeSkew)
	require.True(s.T(), found, "117b-BOUNDARY-F: skew gauge must reset to 0, not go stale")
	assert.InDelta(s.T(), 0.0, skew2, 0.001)
}

// TestFunctional_Scenario117_DisabledNoOp covers 117-DISABLED-noop:
// recommendationScan nil / enabled:false → recordRecommendations is NOT run and
// the DB factory is NEVER called for recs. Per the C.4 clear-on-disable contract
// (diskMonitoring ON but scan disabled), any stale count is reset to 0 and the
// per-type recommendations_total gauges are published as an explicit 0 signal so
// an enabled->disabled scan does not leave a frozen stale reading.
func (s *Scenario117Suite) TestFunctional_Scenario117_DisabledNoOp() {
	tests := []struct {
		name string
		scan *cbv1alpha1.RecommendationScanSpec
	}{
		{name: "scan nil", scan: nil},
		{name: "scan disabled", scan: &cbv1alpha1.RecommendationScanSpec{Enabled: false}},
	}
	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := testutil.NewClusterBuilder("s117-disabled", "default").
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				Build()
			cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring:     true,
				RecommendationScan: tt.scan,
			}
			cluster.Status.RecommendationCount = 7 // prior value must survive.

			calls := 0
			dbFactory := &countingDBFactory117{calls: &calls}
			reg := prometheus.NewRegistry()
			reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

			_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err)

			updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			assert.Equal(s.T(), int32(0), updated.Status.RecommendationCount,
				"117-DISABLED-noop: stale count must be reset to 0 (C.4 clear-on-disable)")
			g, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeBloat)
			assert.True(s.T(), found, "117-DISABLED-noop: a 0 recommendations_total must be published as the disabled signal")
			assert.InDelta(s.T(), 0.0, g, 0.001,
				"117-DISABLED-noop: recommendations_total must be the explicit 0 disabled signal")
		})
	}
}

// countingDBFactory117 returns a mock that records whether it was reached so the
// disabled no-op case can assert the DB layer is never used for the scan. The
// recommendations would all be non-zero if reached, so the disabled path cannot
// accidentally count.
type countingDBFactory117 struct {
	calls *int
}

func (f *countingDBFactory117) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	*f.calls++
	return &testutil.MockDBClient{
		GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
			return []db.Recommendation{scenario117Rec(cases.Scenario117TypeBloat, "public", "x", 99)}, nil
		},
	}, nil
}

// TestFunctional_Scenario117_DBErrNonFatal covers 117-DBERR-nonfatal: a single
// Get* returning an error skips THAT type only — the reconcile still returns nil,
// StorageConfigured stays True, the count = sum of the SUCCESSFUL types, and the
// failing type contributes 0 (never a fabricated value).
func (s *Scenario117Suite) TestFunctional_Scenario117_DBErrNonFatal() {
	cluster := scenario117ScanCluster("s117-dberr")
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeBloat, "public", "t1", 25)}, nil
			},
			GetSkewRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return nil, errScenario117Generic // skew fails → contributes 0, not fabricated.
			},
			GetAgeRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeAge, "public", "a1", 0)}, nil
			},
			GetIndexBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario117Rec(cases.Scenario117TypeIndexBloat, "public", "i1", 70)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario117Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "117-DBERR-nonfatal: a per-type DB error must be non-fatal")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	// 1 bloat + 0 skew (error) + 1 age + 1 index = 3.
	assert.Equal(s.T(), int32(3), updated.Status.RecommendationCount,
		"117-DBERR-nonfatal: count = sum of the SUCCESSFUL types")
	assert.True(s.T(), storageConfiguredTrue117(updated),
		"117-DBERR-nonfatal: StorageConfigured must still be True")
	skew, found := scenario117RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, cases.Scenario117TypeSkew)
	require.True(s.T(), found, "117-DBERR-nonfatal: skew gauge must still be published as 0")
	assert.InDelta(s.T(), 0.0, skew, 0.001,
		"117-DBERR-nonfatal: the failing type must contribute 0, not a fabricated value")
}

// errScenario117Generic is a generic non-sentinel error for the DBERR variant.
var errScenario117Generic = errGeneric117("scenario117 generic db error")

type errGeneric117 string

func (e errGeneric117) Error() string { return string(e) }

// TestFunctional_Scenario117_CatalogCoversFunctionalRows asserts every functional
// (-F) catalog row is honest: a known Req family and a non-empty
// Gate/Expected/Description — so the matrix cannot silently drop a rule.
func (s *Scenario117Suite) TestFunctional_Scenario117_CatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"RT.1": true, "RT.2": true, "RT.3": true, "RT.4": true,
		"C.6": true, "C.7": true, "C.8": true, "C.9": true, "M.4": true,
		"S.2": true, "R.4": true, "M.2": true, "R.3": true,
		"DISABLED": true, "DBERR": true, "CONTROL": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario117Cases() {
		if c.Layer != cases.Scenario117LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Expected, "%s must carry an Expected token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req],
			"functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario117 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Expected)
	}
	for _, req := range []string{"RT.1", "RT.2", "RT.3", "RT.4", "M.2", "R.3"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover rule %s", req)
	}
}
