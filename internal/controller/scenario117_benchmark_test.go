package controller

import (
	"context"
	"log/slog"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// =============================================================================
// Scenario 117 — Performance Benchmarks for recordRecommendations / reconcileStorage
// =============================================================================
//
// These benchmarks measure the CPU cost of the threshold-aware recommendation
// scanning path (recordRecommendations, which runs all four Get{Bloat,Skew,Age,
// IndexBloat}Recommendations scans) and the full reconcileStorage path when
// recommendation scanning is enabled. They run without any real cluster
// (fake.Client + mock DB) and report ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - recordRecommendations with a mock DB returning recs for all 4 types is
//     bounded and does not regress vs the Scenario 116 baseline.
//   - recordRecommendations with all 4 types returning empty is bounded and
//     sets all gauges to 0.
//   - recordRecommendations with a nil dbFactory (early return) is essentially
//     free.
//   - recordRecommendations with a DB error (NewClient failure) is bounded and
//     does not fabricate a count.
//   - reconcileStorage with recommendationScan enabled and a full dbFactory
//     exercises the complete 4-type scan + CronJob lifecycle.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario117 -benchmem ./internal/controller/

// bench117RecScanDBClient returns a recScanDBClient with configurable per-type
// recommendations for benchmarking.
func bench117RecScanDBClient(bloat, skew, age, index int) *recScanDBClient {
	c := &recScanDBClient{mockDBClient: &mockDBClient{}}
	for i := 0; i < bloat; i++ {
		c.bloatRecs = append(c.bloatRecs, db.Recommendation{
			Type: recTypeBloat, Schema: "public", Table: "t_bloat", Ratio: float64(20 + i),
		})
	}
	for i := 0; i < skew; i++ {
		c.skewRecs = append(c.skewRecs, db.Recommendation{
			Type: recTypeSkew, Schema: "public", Table: "t_skew", Ratio: float64(30 + i),
		})
	}
	for i := 0; i < age; i++ {
		c.ageRecs = append(c.ageRecs, db.Recommendation{
			Type: recTypeAge, Schema: "public", Table: "t_age", Ratio: 0,
		})
	}
	for i := 0; i < index; i++ {
		c.indexRecs = append(c.indexRecs, db.Recommendation{
			Type: recTypeIndexBloat, Schema: "public", Table: "t_idx", Ratio: float64(40 + i),
		})
	}
	return c
}

// bench117Cluster returns a cluster wired for the recommendation-scan benchmark
// path: DiskMonitoring on and RecommendationScan enabled with thresholds.
func bench117Cluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       30,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 40,
		},
	}
	return c
}

// bench117Reconciler builds an AdminReconciler wired to a fake client and the
// given DB client for benchmarking the recommendation-scan path.
func bench117Reconciler(dbClient db.Client) (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := bench117Cluster()

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
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	return r, cluster
}

// BenchmarkScenario117_RecordRecommendations_FourTypes benchmarks the
// recordRecommendations path with a mock DB returning recommendations for all
// four types (2 bloat + 1 skew + 1 age + 1 index_bloat = 5 total). This
// measures the full scan overhead: dbFactory.NewClient + 4x Get* + per-type
// gauge publish + status count + table bloat ratio publish.
func BenchmarkScenario117_RecordRecommendations_FourTypes(b *testing.B) {
	dbClient := bench117RecScanDBClient(2, 1, 1, 1)
	r, cluster := bench117Reconciler(dbClient)
	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordRecommendations(ctx, cluster, logger)
	}
}

// BenchmarkScenario117_RecordRecommendations_Empty benchmarks the
// recordRecommendations path with all four types returning empty slices. This
// exercises the full scan path but with zero recommendations — all gauges are
// set to 0 and Status.RecommendationCount = 0.
func BenchmarkScenario117_RecordRecommendations_Empty(b *testing.B) {
	dbClient := bench117RecScanDBClient(0, 0, 0, 0)
	r, cluster := bench117Reconciler(dbClient)
	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordRecommendations(ctx, cluster, logger)
	}
}

// BenchmarkScenario117_RecordRecommendations_NilFactory benchmarks the
// recordRecommendations path with a nil dbFactory. This is the early-return
// path — no DB operations run, no gauge is published, no count is changed.
func BenchmarkScenario117_RecordRecommendations_NilFactory(b *testing.B) {
	r, cluster := bench117Reconciler(nil)
	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordRecommendations(ctx, cluster, logger)
	}
}

// BenchmarkScenario117_RecordRecommendations_DBError benchmarks the
// recordRecommendations path when dbFactory.NewClient returns an error. This
// exercises the skip path: the factory is non-nil but the connection fails, so
// no scan runs and no count is fabricated.
func BenchmarkScenario117_RecordRecommendations_DBError(b *testing.B) {
	scheme := newTestScheme()
	cluster := bench117Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	dbFactory := &mockDBClientFactory{err: assertNewClientErr}

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordRecommendations(ctx, cluster, logger)
	}
}

// BenchmarkScenario117_ReconcileStorage_WithRecommendations benchmarks the full
// reconcileStorage path with recommendationScan enabled and a mock DB returning
// recommendations for all four types. This exercises the complete Scenario 117
// reconciliation including 4-type scan, CronJob ensure, disk-usage measurement,
// and fake client Create/Update calls.
func BenchmarkScenario117_ReconcileStorage_WithRecommendations(b *testing.B) {
	dbClient := bench117RecScanDBClient(2, 1, 1, 1)
	r, cluster := bench117Reconciler(dbClient)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}
