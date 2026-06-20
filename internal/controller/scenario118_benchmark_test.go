package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// =============================================================================
// Scenario 118 -- Performance Benchmarks for resolveScanDuration / recordRecommendations
// =============================================================================
//
// These benchmarks measure the CPU cost of the Scenario 118 scan-duration
// enforcement path:
//
//   - resolveScanDuration: the cheap-path cost of parsing a scanDuration string
//     ("30s", "2h", "") and applying the fallback/clamp policy (C.10).
//   - recordRecommendations with a generous scanDuration ("2h") and a fast mock
//     DB: the full scan completes without truncation (normal-cap path).
//   - recordRecommendations with a tiny scanDuration ("10ms") and a blocking
//     mock DB: the shared budget trips context.DeadlineExceeded, exercising the
//     truncation path (flag + counter).
//
// They run without any real cluster (fake.Client + mock DB) and report ns/op +
// allocs/op.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario118 -benchmem ./internal/controller/
// =============================================================================

// bench118ScanDurationCluster returns a recommendation-scan-enabled cluster with
// the given scanDuration string for benchmarking the C.10 cap path.
func bench118ScanDurationCluster(scanDuration string) *cbv1alpha1.CloudberryCluster {
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
			ScanDuration:        scanDuration,
		},
	}
	return c
}

// bench118Reconciler builds an AdminReconciler wired to a fake client and the
// given DB client for benchmarking the Scenario 118 scan-duration paths.
func bench118Reconciler(dbClient db.Client, scanDuration string) (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := bench118ScanDurationCluster(scanDuration)

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

// BenchmarkScenario118_ResolveScanDuration benchmarks the cheap-path cost of
// resolveScanDuration: parsing a scanDuration string and applying the
// fallback/clamp policy. This is a pure-compute function with no I/O, so it
// should be very fast (sub-microsecond). The sub-benchmarks cover the three
// representative inputs: a normal duration ("30s"), a large duration ("2h"),
// and the empty-string fallback ("").
func BenchmarkScenario118_ResolveScanDuration(b *testing.B) {
	logger := slog.Default()

	cases := []struct {
		name  string
		input string
	}{
		{name: "30s", input: "30s"},
		{name: "2h", input: "2h"},
		{name: "empty_fallback", input: ""},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			scan := &cbv1alpha1.RecommendationScanSpec{ScanDuration: tc.input}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resolveScanDuration(scan, logger)
			}
		})
	}
}

// BenchmarkScenario118_RecordRecommendations_NormalCap benchmarks the
// recordRecommendations path with a generous scanDuration ("2h") and a fast
// mock DB returning recommendations for all four types (2 bloat + 1 skew +
// 1 age + 1 index_bloat = 5 total). The scan completes well within the cap,
// so no truncation occurs. This measures the full scan overhead including the
// C.10 context.WithTimeout setup + resolveScanDuration parse.
func BenchmarkScenario118_RecordRecommendations_NormalCap(b *testing.B) {
	dbClient := bench117RecScanDBClient(2, 1, 1, 1)
	r, cluster := bench118Reconciler(dbClient, "2h")
	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordRecommendations(ctx, cluster, logger)
	}
}

// BenchmarkScenario118_RecordRecommendations_Truncated benchmarks the
// recordRecommendations path with a tiny scanDuration ("10ms") and a blocking
// mock DB. The shared budget trips context.DeadlineExceeded, exercising the
// truncation path: the RecommendationScanTruncated status flag is set and the
// IncRecommendationScanTruncated counter fires. This measures the overhead of
// the truncation detection + metric increment under a capped scan.
func BenchmarkScenario118_RecordRecommendations_Truncated(b *testing.B) {
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second, // far longer than the 10ms cap.
	}
	r, cluster := bench118Reconciler(dbClient, "10ms")
	ctx := context.Background()
	logger := slog.Default()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the truncation flag so each iteration starts clean.
		cluster.Status.RecommendationScanTruncated = false
		r.recordRecommendations(ctx, cluster, logger)
	}
}
