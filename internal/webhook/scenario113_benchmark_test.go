package webhook

import (
	"context"
	"testing"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// =============================================================================
// Scenario 113 — Performance Benchmarks for storage-recommendation validation
// =============================================================================
//
// These benchmarks measure the raw CPU cost of the validateStorageManagement
// path (W.1–W.4) and the full ValidateCreate admission chain for CRs with
// recommendationScan enabled. They run without any infrastructure (no k8s, no
// network) and report ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - Rejection is CHEAP: a single threshold violation returns in O(1) with
//     zero heap allocations beyond the error string.
//   - Acceptance is CHEAP: all four threshold checks pass in O(1).
//   - Full ValidateCreate overhead for a storage-recommendation CR is bounded
//     and does not regress vs the baseline (no reader → no List RPC).
//
// Run:
//   go test -run=^$ -bench=BenchmarkScenario113 -benchmem ./internal/webhook/

// BenchmarkScenario113_ValidateStorageManagement_Accept benchmarks the
// validateStorageManagement function with a fully-valid enabled scan (all four
// thresholds in-range). This is the hot-path cost for ACCEPTED CRs.
func BenchmarkScenario113_ValidateStorageManagement_Accept(b *testing.B) {
	cluster := valid113Cluster()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateStorageManagement(cluster)
	}
}

// BenchmarkScenario113_ValidateStorageManagement_Reject_W1 benchmarks the
// validateStorageManagement function with a W.1 violation (bloatThreshold=150).
// This measures the cost of the REJECTION path — proving rejections are cheap.
func BenchmarkScenario113_ValidateStorageManagement_Reject_W1(b *testing.B) {
	cluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.BloatThreshold = 150
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateStorageManagement(cluster)
	}
}

// BenchmarkScenario113_ValidateStorageManagement_Reject_W4 benchmarks the
// validateStorageManagement function with a W.4 violation (ageThreshold=-5).
// W.4 is the LAST check in the chain, so this measures worst-case rejection
// latency (all prior checks pass before the final one rejects).
func BenchmarkScenario113_ValidateStorageManagement_Reject_W4(b *testing.B) {
	cluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.AgeThreshold = -5
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateStorageManagement(cluster)
	}
}

// BenchmarkScenario113_ValidateStorageManagement_Disabled benchmarks the
// short-circuit path when scan.Enabled=false (the enabled-gate). This proves
// the gate is essentially free — no threshold checks run.
func BenchmarkScenario113_ValidateStorageManagement_Disabled(b *testing.B) {
	cluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.Enabled = false
		// Out-of-range values that would fail if checked:
		scan.BloatThreshold = 150
		scan.SkewThreshold = -1
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateStorageManagement(cluster)
	}
}

// BenchmarkScenario113_ValidateStorageManagement_NilStorage benchmarks the
// nil-storage short-circuit (spec.storage == nil). This is the absolute minimum
// cost path.
func BenchmarkScenario113_ValidateStorageManagement_NilStorage(b *testing.B) {
	cluster := newValidCluster() // no storage spec

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateStorageManagement(cluster)
	}
}

// BenchmarkScenario113_ValidateCreate_FullChain_Accept benchmarks the FULL
// ValidateCreate admission chain (validateCreate → validateCluster →
// validateStorageManagement) for a valid CR with storage recommendation scan
// enabled. This measures the total webhook overhead for the Scenario 113 path.
// reader=nil skips the duplicate-name List RPC (isolating validation logic).
func BenchmarkScenario113_ValidateCreate_FullChain_Accept(b *testing.B) {
	v := NewCloudberryClusterValidator(nil)
	cluster := valid113Cluster()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.ValidateCreate(ctx, cluster)
	}
}

// BenchmarkScenario113_ValidateCreate_FullChain_Reject benchmarks the FULL
// ValidateCreate admission chain for a CR that is REJECTED by W.1
// (bloatThreshold=150). This proves the full-chain rejection cost is bounded.
func BenchmarkScenario113_ValidateCreate_FullChain_Reject(b *testing.B) {
	v := NewCloudberryClusterValidator(nil)
	cluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.BloatThreshold = 150
	})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.ValidateCreate(ctx, cluster)
	}
}

// BenchmarkScenario113_ValidateUpdate_Accept benchmarks the ValidateUpdate path
// for a valid storage-recommendation CR (old==new, both valid). This measures
// the update admission overhead.
func BenchmarkScenario113_ValidateUpdate_Accept(b *testing.B) {
	v := NewCloudberryClusterValidator(nil)
	cluster := valid113Cluster()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.ValidateUpdate(ctx, cluster, cluster)
	}
}

// BenchmarkScenario113_ValidateUpdate_Reject benchmarks the ValidateUpdate path
// for a CR that transitions from valid to invalid (W.1 violation on new object).
func BenchmarkScenario113_ValidateUpdate_Reject(b *testing.B) {
	v := NewCloudberryClusterValidator(nil)
	oldCluster := valid113Cluster()
	newCluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.BloatThreshold = 150
	})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.ValidateUpdate(ctx, oldCluster, newCluster)
	}
}
