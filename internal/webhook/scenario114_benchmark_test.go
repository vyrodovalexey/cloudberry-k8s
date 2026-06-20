package webhook

import (
	"context"
	"testing"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// =============================================================================
// Scenario 114 — Performance Benchmarks for storage-recommendation MUTATING
// webhook defaults (D.1–D.6)
// =============================================================================
//
// These benchmarks measure the raw CPU cost of the setStorageManagementDefaults
// path (D.1–D.6) and the full public Default(ctx, cluster) admission chain for
// CRs with recommendationScan enabled. They run without any infrastructure (no
// k8s, no network) and report ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - Defaulting all six fields is CHEAP: an enabled scan with all fields
//     omitted completes in O(1) with minimal heap allocations.
//   - Already-set preservation is CHEAP: when all fields are explicit, the
//     defaulter short-circuits each if-check with zero writes.
//   - Disabled gate is FREE: Enabled=false bypasses all six default assignments.
//   - Nil storage is FREE: spec.storage==nil returns immediately.
//   - Full Default() chain overhead is bounded and does not regress vs the
//     Scenario 113 baseline.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario114 -benchmem ./internal/webhook/

// BenchmarkScenario114_SetStorageManagementDefaults_AllOmitted benchmarks the
// setStorageManagementDefaults function with an enabled scan where ALL six
// defaulted fields (D.1–D.6) are omitted (zero values). This is the worst-case
// defaulting path — every field triggers a write.
func BenchmarkScenario114_SetStorageManagementDefaults_AllOmitted(b *testing.B) {
	cluster := scenario114BenchClusterAllOmitted()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the scan fields to zero before each iteration so the
		// defaulter actually writes all six values every time.
		resetScan114(cluster)
		setStorageManagementDefaults(cluster)
	}
}

// BenchmarkScenario114_SetStorageManagementDefaults_AlreadySet benchmarks the
// setStorageManagementDefaults function with an enabled scan where ALL six
// fields are already set to explicit (non-zero) values. This is the no-op
// preserve path — every if-check short-circuits.
func BenchmarkScenario114_SetStorageManagementDefaults_AlreadySet(b *testing.B) {
	cluster := scenario114BenchClusterAlreadySet()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setStorageManagementDefaults(cluster)
	}
}

// BenchmarkScenario114_SetStorageManagementDefaults_Disabled benchmarks the
// setStorageManagementDefaults function with Enabled=false. This measures the
// enabled-gate bypass — no default assignments run.
func BenchmarkScenario114_SetStorageManagementDefaults_Disabled(b *testing.B) {
	cluster := scenario114BenchClusterDisabled()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setStorageManagementDefaults(cluster)
	}
}

// BenchmarkScenario114_SetStorageManagementDefaults_NilStorage benchmarks the
// setStorageManagementDefaults function with spec.storage==nil. This is the
// absolute minimum cost path — immediate nil-check return.
func BenchmarkScenario114_SetStorageManagementDefaults_NilStorage(b *testing.B) {
	cluster := newMinimalCluster() // no storage spec

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setStorageManagementDefaults(cluster)
	}
}

// BenchmarkScenario114_Default_FullChain benchmarks the FULL public
// Default(ctx, cluster) admission chain for a CR with an enabled scan and all
// six fields omitted. This measures the total mutating webhook overhead for the
// Scenario 114 defaulting path end-to-end (Default -> setClusterDefaults ->
// setStorageManagementDefaults + all other defaulters).
func BenchmarkScenario114_Default_FullChain(b *testing.B) {
	d := NewCloudberryClusterDefaulter()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster := scenario114BenchClusterAllOmitted()
		_ = d.Default(ctx, cluster)
	}
}

// =============================================================================
// Benchmark helpers — construct clusters for each scenario 114 benchmark path
// =============================================================================

// scenario114BenchClusterAllOmitted returns a cluster with an enabled
// recommendation scan and ALL six defaulted fields at their zero values.
func scenario114BenchClusterAllOmitted() *cbv1alpha1.CloudberryCluster {
	c := newMinimalCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true,
			// All six fields omitted (zero values) — D.1–D.6 will be applied.
		},
	}
	return c
}

// scenario114BenchClusterAlreadySet returns a cluster with an enabled
// recommendation scan and ALL six fields set to explicit non-default values.
func scenario114BenchClusterAlreadySet() *cbv1alpha1.CloudberryCluster {
	c := newMinimalCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 1 * * *",
			BloatThreshold:      10,
			SkewThreshold:       25,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 15,
			ScanDuration:        "1h",
		},
	}
	return c
}

// scenario114BenchClusterDisabled returns a cluster with a disabled
// recommendation scan (Enabled=false). The defaulter should bypass all six
// default assignments.
func scenario114BenchClusterDisabled() *cbv1alpha1.CloudberryCluster {
	c := newMinimalCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}
	return c
}

// resetScan114 resets the six defaulted fields on the recommendation scan to
// their zero values so the benchmark loop measures the actual write path on
// every iteration.
func resetScan114(cluster *cbv1alpha1.CloudberryCluster) {
	scan := cluster.Spec.Storage.RecommendationScan
	scan.Schedule = ""
	scan.BloatThreshold = 0
	scan.SkewThreshold = 0
	scan.AgeThreshold = 0
	scan.IndexBloatThreshold = 0
	scan.ScanDuration = ""
}
