package builder

import (
	"testing"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// =============================================================================
// Scenario 115 — Performance Benchmarks for BuildRecommendationScanCronJob
// =============================================================================
//
// These benchmarks measure the raw CPU cost of the BuildRecommendationScanCronJob
// builder (spec 13 §Reconciliation C.5). They run without any infrastructure
// (no k8s, no network) and report ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - Full build is BOUNDED: an enabled scan with schedule and all five
//     thresholds produces a complete CronJob in O(1) with bounded heap
//     allocations (labels, env vars, owner reference).
//   - Disabled gate is FREE: Enabled=false bypasses the entire build and
//     returns nil immediately with zero allocations.
//   - Nil storage is FREE: spec.storage==nil returns nil immediately with
//     zero allocations.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario115 -benchmem ./internal/builder/

// BenchmarkScenario115_BuildRecommendationScanCronJob_Full benchmarks the
// BuildRecommendationScanCronJob function with a FULL enabled scan: disk
// monitoring on, recommendation scan enabled with schedule and all five
// thresholds. This is the hot-path cost for building the CronJob object.
func BenchmarkScenario115_BuildRecommendationScanCronJob_Full(b *testing.B) {
	bldr := NewBuilder()
	cluster := bench115ClusterFull()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bldr.BuildRecommendationScanCronJob(cluster)
	}
}

// BenchmarkScenario115_BuildRecommendationScanCronJob_Disabled benchmarks the
// BuildRecommendationScanCronJob function with Enabled=false. This measures the
// enabled-gate bypass — the builder returns nil immediately without constructing
// any Kubernetes objects.
func BenchmarkScenario115_BuildRecommendationScanCronJob_Disabled(b *testing.B) {
	bldr := NewBuilder()
	cluster := bench115ClusterDisabled()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bldr.BuildRecommendationScanCronJob(cluster)
	}
}

// BenchmarkScenario115_BuildRecommendationScanCronJob_NilStorage benchmarks the
// BuildRecommendationScanCronJob function with spec.storage==nil. This is the
// absolute minimum cost path — immediate nil-check return.
func BenchmarkScenario115_BuildRecommendationScanCronJob_NilStorage(b *testing.B) {
	bldr := NewBuilder()
	cluster := newTestCluster() // no storage spec

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bldr.BuildRecommendationScanCronJob(cluster)
	}
}

// =============================================================================
// Benchmark helpers — construct clusters for each scenario 115 benchmark path
// =============================================================================

// bench115ClusterFull returns a cluster with the FULL Scenario 115 storage
// block: disk monitoring on, recommendation scan enabled with schedule and all
// five thresholds + scan duration.
func bench115ClusterFull() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       50,
			AgeThreshold:        500000000,
			IndexBloatThreshold: 30,
			ScanDuration:        "2h",
		},
	}
	return c
}

// bench115ClusterDisabled returns a cluster with a disabled recommendation scan
// (Enabled=false). The builder should return nil immediately.
func bench115ClusterDisabled() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}
	return c
}
