package controller

import (
	"context"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// =============================================================================
// Scenario 122 -- Performance Benchmarks for Disabled-Path Reconcile
// =============================================================================
//
// These benchmarks measure the CPU cost of the Scenario 122 clear-on-disable
// paths:
//
//   - reconcileStorage with diskMonitoring:false (R.1 early-return +
//     clearStorageSignals): the cheapest reconcile path, exercising the C.2
//     reset-on-disable + C.4 clear-on-disable helpers.
//   - reconcileStorage with diskMonitoring:true but scan disabled (the
//     clearRecommendations else-branch): diskMonitoring is on, but the scan
//     is nil/disabled, so only clearRecommendations runs (not the full scan).
//   - clearRecommendations directly: the helper that resets
//     Status.RecommendationCount + per-type gauges to 0.
//   - clearStorageSignals directly: the helper that resets both disk-usage
//     and recommendation signals on the storage-off path.
//   - reconcileStorage with Storage==nil (control): no storage block at all,
//     exercising the same R.1 early-return + clearStorageSignals path.
//
// They run without any real cluster (fake.Client + mock DB) and report ns/op +
// allocs/op. The disabled path should be cheaper than the enabled scan path
// (Scenario 117 benchmarks).
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario122 -benchmem ./internal/controller/
// =============================================================================

// bench122DiskMonitoringOffCluster returns a cluster with diskMonitoring:false
// for benchmarking the R.1 early-return + clearStorageSignals path.
func bench122DiskMonitoringOffCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: false,
	}
	return c
}

// bench122ScanDisabledCluster returns a cluster with diskMonitoring:true but
// recommendation scan disabled (nil) for benchmarking the clearRecommendations
// else-branch.
func bench122ScanDisabledCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		// RecommendationScan is nil => disabled path.
	}
	return c
}

// bench122StorageNilCluster returns a cluster with no storage block at all
// (Storage==nil) for benchmarking the R.1 early-return control path.
func bench122StorageNilCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = nil
	return c
}

// bench122Reconciler builds an AdminReconciler wired to a fake client and the
// given cluster for benchmarking the Scenario 122 disabled paths.
func bench122Reconciler(cluster *cbv1alpha1.CloudberryCluster) *AdminReconciler {
	scheme := newTestScheme()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	var dbFactory db.DBClientFactory
	dbClient := &mockDBClient{diskUsagePercent: 42}
	dbFactory = &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	return r
}

// BenchmarkScenario122_ReconcileStorage_DiskMonitoringOff benchmarks the
// reconcileStorage path with diskMonitoring:false. This is the R.1 early-return
// path: clearStorageSignals resets disk-usage + recommendation signals, then
// returns immediately. This should be the cheapest reconcileStorage path.
func BenchmarkScenario122_ReconcileStorage_DiskMonitoringOff(b *testing.B) {
	cluster := bench122DiskMonitoringOffCluster()
	r := bench122Reconciler(cluster)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}

// BenchmarkScenario122_ReconcileStorage_ScanDisabled benchmarks the
// reconcileStorage path with diskMonitoring:true but recommendation scan
// disabled (nil). This exercises the clearRecommendations else-branch: disk
// monitoring runs (recordDiskUsage), but the scan is skipped and
// clearRecommendations resets the count + per-type gauges.
func BenchmarkScenario122_ReconcileStorage_ScanDisabled(b *testing.B) {
	cluster := bench122ScanDisabledCluster()
	r := bench122Reconciler(cluster)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}

// BenchmarkScenario122_ClearRecommendations benchmarks the clearRecommendations
// helper directly. This is a pure in-memory operation: reset
// Status.RecommendationCount + Status.RecommendationScanTruncated to zero and
// publish 0 for all four recommendation type gauges. Should be sub-microsecond.
func BenchmarkScenario122_ClearRecommendations(b *testing.B) {
	cluster := bench122DiskMonitoringOffCluster()
	// Seed non-zero values so the clear has work to do.
	cluster.Status.RecommendationCount = 5
	cluster.Status.RecommendationScanTruncated = true

	r := bench122Reconciler(cluster)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset to non-zero so each iteration clears a real value.
		cluster.Status.RecommendationCount = 5
		cluster.Status.RecommendationScanTruncated = true
		r.clearRecommendations(cluster)
	}
}

// BenchmarkScenario122_ClearStorageSignals benchmarks the clearStorageSignals
// helper directly. This resets both disk-usage (C.2) and recommendation (C.4)
// signals: Status.DiskUsagePercent=0, gauge=0, clearRecommendations, and a
// conditional patchStatus (only when stale values exist). Seeded with non-zero
// stale values so the patch path is exercised.
func BenchmarkScenario122_ClearStorageSignals(b *testing.B) {
	cluster := bench122DiskMonitoringOffCluster()
	// Seed stale values so the R6 patch path fires.
	cluster.Status.DiskUsagePercent = 42
	cluster.Status.RecommendationCount = 3

	r := bench122Reconciler(cluster)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset stale values so each iteration exercises the patch path.
		cluster.Status.DiskUsagePercent = 42
		cluster.Status.RecommendationCount = 3
		r.clearStorageSignals(ctx, cluster, r.logger)
	}
}

// BenchmarkScenario122_ReconcileStorage_StorageNil benchmarks the
// reconcileStorage path with Storage==nil (no storage block). This is the
// control path: identical to diskMonitoring:false (R.1 early-return +
// clearStorageSignals) but with no storage spec at all.
func BenchmarkScenario122_ReconcileStorage_StorageNil(b *testing.B) {
	cluster := bench122StorageNilCluster()
	r := bench122Reconciler(cluster)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}
