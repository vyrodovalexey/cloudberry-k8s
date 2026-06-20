package controller

import (
	"context"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// =============================================================================
// Scenario 115 — Performance Benchmarks for reconcileStorage (controller)
// =============================================================================
//
// These benchmarks measure the CPU cost of the reconcileStorage path in the
// AdminReconciler, exercising the full ensure/remove CronJob lifecycle over a
// fake client. They run without any real cluster (fake.Client) and report
// ns/op + allocs/op.
//
// The key performance assertions these benchmarks evidence:
//   - reconcileStorage with a full enabled scan (ensure CronJob path) is
//     bounded and does not regress vs the Scenario 114 baseline.
//   - reconcileStorage with disk monitoring disabled (early return) is
//     essentially free.
//   - reconcileStorage with a disabled scan (remove/GC path) is bounded.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario115 -benchmem ./internal/controller/

// bench115ReconcilerFull returns an AdminReconciler wired to a fake client with
// the cluster pre-registered, and the cluster itself with the full Scenario 115
// storage block.
func bench115ReconcilerFull() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

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

// bench115ReconcilerDisabledMonitoring returns an AdminReconciler + cluster with
// disk monitoring disabled (early return path).
func bench115ReconcilerDisabledMonitoring() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}

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

// bench115ReconcilerScanDisabled returns an AdminReconciler + cluster with disk
// monitoring on but recommendation scan disabled (remove/GC path).
func bench115ReconcilerScanDisabled() (*AdminReconciler, *cbv1alpha1.CloudberryCluster) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}

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

// BenchmarkScenario115_ReconcileStorage_Full benchmarks the reconcileStorage
// path with a full enabled scan (ensure CronJob path). This measures the total
// controller overhead for the Scenario 115 storage reconciliation including
// fake client Create/Update calls.
func BenchmarkScenario115_ReconcileStorage_Full(b *testing.B) {
	r, cluster := bench115ReconcilerFull()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}

// BenchmarkScenario115_ReconcileStorage_DisabledMonitoring benchmarks the
// reconcileStorage path with disk monitoring disabled. This is the early-return
// path — no CronJob operations run.
func BenchmarkScenario115_ReconcileStorage_DisabledMonitoring(b *testing.B) {
	r, cluster := bench115ReconcilerDisabledMonitoring()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}

// BenchmarkScenario115_ReconcileStorage_ScanDisabled benchmarks the
// reconcileStorage path with disk monitoring on but recommendation scan
// disabled. This exercises the remove/GC CronJob path.
func BenchmarkScenario115_ReconcileStorage_ScanDisabled(b *testing.B) {
	r, cluster := bench115ReconcilerScanDisabled()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.reconcileStorage(ctx, cluster)
	}
}
