package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// =============================================================================
// Scenario 120 -- Performance Benchmarks for Usage Report with Per-Table Tables
// =============================================================================
//
// These benchmarks measure the CPU cost of the P.6 handleGetUsageReport handler
// after Scenario 120 enriched the usage report with per-table consumption
// (db.UsageReportEntry.Tables / db.TableUsage). Three paths are benchmarked:
//
//   - BenchmarkScenario120_UsageReport_Enabled:  usageReport.enabled with
//     entries carrying a per-table Tables slice (full marshalling cost).
//   - BenchmarkScenario120_UsageReport_Disabled: usageReport disabled (soft-gate
//     fast path, no DB call).
//   - BenchmarkScenario120_UsageReport_WithMonth: ?month=2026-05 scoped query
//     (enabled path with month parameter threading).
//
// They run without any real cluster (fake.Client + mockDBClient) and report
// ns/op + allocs/op via httptest.NewRecorder.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario120 -benchmem ./internal/api/
// =============================================================================

// bench120Cluster builds a storage-enabled cluster with usageReport.enabled for
// the Scenario 120 benchmarks.
func bench120Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("bench-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	return cluster
}

// bench120ClusterDisabled builds a cluster with usageReport explicitly disabled
// so the handler takes the soft-gate fast path.
func bench120ClusterDisabled() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("bench-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: false},
	}
	return cluster
}

// bench120DBClient returns a mockDBClient pre-loaded with usage report entries
// that carry the per-table Tables slice (Scenario 120 C.11 enrichment),
// mirroring the unit-test harness in scenario120_usage_report_test.go.
func bench120DBClient() *mockDBClient {
	return &mockDBClient{
		usageReport: []db.UsageReportEntry{
			{
				Month: "2026-05", Database: "analytics",
				SizeBytes: 10737418240, SizeHuman: "10 GB", Connections: 25,
				Tables: []db.TableUsage{
					{Schema: "public", Table: "orders", SizeBytes: 5368709120, SizeHuman: "5 GB"},
					{Schema: "public", Table: "lineitem", SizeBytes: 3221225472, SizeHuman: "3 GB"},
					{Schema: "audit", Table: "events", SizeBytes: 2147483648, SizeHuman: "2 GB"},
				},
			},
			{
				Month: "2026-05", Database: "postgres",
				SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 5,
			},
		},
	}
}

// bench120ServerEnabled builds a Server wired with the mock DB and a noop
// recorder for the enabled-path benchmarks (avoids counting overhead).
func bench120ServerEnabled() *Server {
	cluster := bench120Cluster()
	dbClient := bench120DBClient()
	factory := &mockDBFactory{client: dbClient}
	return newTestServerWithDBAndMetrics(factory, &metrics.NoopRecorder{}, cluster)
}

// bench120ServerDisabled builds a Server with usageReport disabled so the
// handler takes the soft-gate fast path without touching the DB.
func bench120ServerDisabled() *Server {
	cluster := bench120ClusterDisabled()
	dbClient := bench120DBClient() // data exists but must NOT be queried
	factory := &mockDBFactory{client: dbClient}
	return newTestServerWithDBAndMetrics(factory, &metrics.NoopRecorder{}, cluster)
}

// BenchmarkScenario120_UsageReport_Enabled benchmarks the P.6
// handleGetUsageReport handler with usageReport.enabled and entries carrying
// per-table Tables slices (full marshalling cost including C.11 enrichment).
func BenchmarkScenario120_UsageReport_Enabled(b *testing.B) {
	s := bench120ServerEnabled()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/usage-report?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleGetUsageReport(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario120_UsageReport_Disabled benchmarks the P.6
// handleGetUsageReport handler with usageReport disabled (soft-gate fast path).
// The DB is never queried; this measures the overhead of the cluster lookup,
// the enabled check, and the empty-entries JSON marshalling.
func BenchmarkScenario120_UsageReport_Disabled(b *testing.B) {
	s := bench120ServerDisabled()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/usage-report?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleGetUsageReport(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario120_UsageReport_WithMonth benchmarks the P.6
// handleGetUsageReport handler with ?month=2026-05 scoped query (enabled path).
// This exercises the month parameter threading through the handler and the DB
// mock, measuring the full marshalling cost with per-table Tables.
func BenchmarkScenario120_UsageReport_WithMonth(b *testing.B) {
	s := bench120ServerEnabled()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/usage-report?namespace=default&month=2026-05", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleGetUsageReport(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}
