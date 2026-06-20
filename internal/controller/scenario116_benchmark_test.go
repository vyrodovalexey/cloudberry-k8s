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
// Scenario 116 — Performance Benchmarks for recordDiskUsage / reconcileStorage
// =============================================================================
//
// These benchmarks measure the CPU cost of the disk-usage measurement path
// (recordDiskUsage) and the full reconcileStorage path when disk monitoring is
// enabled. They run without any real cluster (fake.Client + mock DB) and report
// ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - recordDiskUsage with a mock DB returning a known percent (42) is bounded
//     and does not regress vs the Scenario 115 baseline.
//   - recordDiskUsage with a nil dbFactory (early return) is essentially free.
//   - recordDiskUsage with a DB error (ErrDiskUsageUnavailable) is bounded and
//     does not fabricate a value.
//   - reconcileStorage with diskMonitoring:true and a full dbFactory exercises
//     the complete measurement + CronJob lifecycle.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario116 -benchmem ./internal/controller/

// bench116ReconcilerMeasured returns an AdminReconciler wired to a fake client
// and a mock dbFactory whose DB client returns GetDiskUsagePercent=42, plus the
// cluster with disk monitoring enabled.
func bench116ReconcilerMeasured() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	dbClient := &mockDBClient{diskUsagePercent: 42}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	return r, cluster
}

// bench116ReconcilerNilFactory returns an AdminReconciler with a nil dbFactory
// (early-return path in recordDiskUsage) and disk monitoring enabled.
func bench116ReconcilerNilFactory() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), nil,
		&metrics.NoopRecorder{}, nil)

	return r, cluster
}

// bench116ReconcilerDBError returns an AdminReconciler wired to a mock dbFactory
// whose DB client returns ErrDiskUsageUnavailable from GetDiskUsagePercent.
func bench116ReconcilerDBError() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	dbClient := &mockDBClient{
		diskUsagePercent: 99,
		diskUsageErr:     db.ErrDiskUsageUnavailable,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	return r, cluster
}

// bench116ReconcilerFull returns an AdminReconciler wired to a fake client with
// disk monitoring enabled and a mock dbFactory returning 42%, for the full
// reconcileStorage path (measurement + CronJob lifecycle).
func bench116ReconcilerFull() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	// Enable recommendation scan so reconcileStorage exercises the full path.
	cluster.Spec.Storage.RecommendationScan = &cbv1alpha1.RecommendationScanSpec{
		Enabled:  true,
		Schedule: "0 3 * * 0",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	dbClient := &mockDBClient{diskUsagePercent: 42}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(100), builder.NewBuilder(), dbFactory,
		&metrics.NoopRecorder{}, nil)

	return r, cluster
}

// BenchmarkScenario116_RecordDiskUsage_Measured benchmarks the recordDiskUsage
// path with a mock DB returning GetDiskUsagePercent=42. This measures the full
// measurement overhead: dbFactory.NewClient + GetDiskUsagePercent + patchStatus
// + SetDiskUsagePercent gauge publish.
func BenchmarkScenario116_RecordDiskUsage_Measured(b *testing.B) {
	r, cluster := bench116ReconcilerMeasured()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordDiskUsage(ctx, cluster, r.logger)
	}
}

// BenchmarkScenario116_RecordDiskUsage_NilFactory benchmarks the recordDiskUsage
// path with a nil dbFactory. This is the early-return path — no DB operations
// run, no gauge is published.
func BenchmarkScenario116_RecordDiskUsage_NilFactory(b *testing.B) {
	r, cluster := bench116ReconcilerNilFactory()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordDiskUsage(ctx, cluster, r.logger)
	}
}

// BenchmarkScenario116_RecordDiskUsage_DBError benchmarks the recordDiskUsage
// path when GetDiskUsagePercent returns ErrDiskUsageUnavailable. This exercises
// the skip path: dbFactory.NewClient succeeds but the query fails, so no status
// is overwritten and no gauge is published.
func BenchmarkScenario116_RecordDiskUsage_DBError(b *testing.B) {
	r, cluster := bench116ReconcilerDBError()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.recordDiskUsage(ctx, cluster, r.logger)
	}
}

// BenchmarkScenario116_ReconcileStorage_Full benchmarks the reconcileStorage
// path with diskMonitoring:true and a full dbFactory. This exercises the
// complete Scenario 116 reconciliation including disk-usage measurement,
// CronJob ensure, and fake client Create/Update calls.
func BenchmarkScenario116_ReconcileStorage_Full(b *testing.B) {
	r, cluster := bench116ReconcilerFull()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}
