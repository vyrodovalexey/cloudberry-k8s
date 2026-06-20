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
// Scenario 119 -- Performance Benchmarks for Storage REST API Handlers
// =============================================================================
//
// These benchmarks measure the CPU cost of the six storage REST API handlers
// wired in Scenario 119:
//
//   - P.1 handleGetDiskUsage:              disk-usage percent + per-DB + segment breakdown
//   - P.2 handleListTables:                table listing with size/bloat/skew/rowCount
//   - P.3 handleGetTableDetail:            single-table detail with index sizes
//   - P.4 handleListRecommendations:       four-type threshold-aware recommendations
//   - P.5 handleTriggerRecommendationScan: POST scan trigger (enabled path)
//   - P.6 handleGetUsageReport:            monthly usage report entries
//
// They run without any real cluster (fake.Client + mockDBClient) and report
// ns/op + allocs/op via httptest.NewRecorder.
//
// Run:
//
//	go test -run=^$ -bench=BenchmarkScenario119 -benchmem ./internal/api/
// =============================================================================

// bench119Cluster builds a storage-enabled cluster for benchmarking.
func bench119Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("bench-cluster", "default")
	cluster.Status.DiskUsagePercent = 65
	cluster.Status.RecommendationCount = 4
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       30,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 40,
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	return cluster
}

// bench119DBClient returns a mockDBClient pre-loaded with representative data
// for all six storage endpoints.
func bench119DBClient() *mockDBClient {
	return &mockDBClient{
		diskUsage: []db.DiskUsage{
			{Database: "analytics", SizeBytes: 10737418240, SizeHuman: "10 GB"},
			{Database: "postgres", SizeBytes: 8388608, SizeHuman: "8 MB"},
		},
		storageDiskUsage: []db.DiskUsageInfo{
			{Tablespace: "pg_default", SizeBytes: 10745806848, SizeHuman: "10 GB", UsagePercent: 65},
			{Tablespace: "pg_global", SizeBytes: 1048576, SizeHuman: "1 MB", UsagePercent: 1},
		},
		tables: []db.TableStorageInfo{
			{Schema: "public", Table: "events", SizeBytes: 2147483648, SizeHuman: "2 GB",
				BloatPercent: 55, SkewPercent: 42, RowCount: 5000000},
			{Schema: "public", Table: "users", SizeBytes: 1073741824, SizeHuman: "1 GB",
				BloatPercent: 10, SkewPercent: 7, RowCount: 1000000},
			{Schema: "audit", Table: "logs", SizeBytes: 536870912, SizeHuman: "512 MB",
				BloatPercent: 3, SkewPercent: 2, RowCount: 250000},
		},
		tableDetail: &db.TableDetail{
			Schema: "public", Table: "events",
			SizeBytes: 2147483648, SizeHuman: "2 GB",
			RowCount: 5000000, BloatPercent: 55, SkewPercent: 42,
			LastVacuum: "2025-06-01", LastAnalyze: "2025-06-02",
			IndexSizes: []db.IndexSizeInfo{
				{Name: "events_pkey", SizeBytes: 2097152, SizeHuman: "2 MB"},
				{Name: "events_ts_idx", SizeBytes: 4194304, SizeHuman: "4 MB"},
			},
		},
		bloatRecs: []db.Recommendation{
			{Type: "bloat", Schema: "public", Table: "events",
				Value: 55, Ratio: 55, Severity: "critical", Description: "high bloat"},
		},
		skewRecs: []db.Recommendation{
			{Type: "skew", Schema: "public", Table: "users",
				Value: 42, Ratio: 42, Severity: "warning", Description: "data skew"},
		},
		ageRecs: []db.Recommendation{
			{Type: "age", Schema: "public", Table: "old_data",
				Value: 150000000, Severity: "warning", Description: "high xid age"},
		},
		indexBloatRecs: []db.Recommendation{
			{Type: "index_bloat", Schema: "public", Table: "events",
				Value: 60, Ratio: 60, Severity: "critical", Description: "index bloat"},
		},
		usageReport: []db.UsageReportEntry{
			{Month: "2025-06", Database: "analytics", SizeBytes: 10737418240, SizeHuman: "10 GB", Connections: 25},
			{Month: "2025-06", Database: "postgres", SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 5},
		},
	}
}

// bench119Server builds a Server wired with the mock DB and counting recorder.
func bench119Server() *Server {
	cluster := bench119Cluster()
	dbClient := bench119DBClient()
	factory := &mockDBFactory{client: dbClient}
	rec := &countingRecorder{}
	return newTestServerWithDBAndMetrics(factory, rec, cluster)
}

// bench119ServerNoMetrics builds a Server wired with the mock DB and a noop
// recorder (avoids counting overhead in pure-handler benchmarks).
func bench119ServerNoMetrics() *Server {
	cluster := bench119Cluster()
	dbClient := bench119DBClient()
	factory := &mockDBFactory{client: dbClient}
	return newTestServerWithDBAndMetrics(factory, &metrics.NoopRecorder{}, cluster)
}

// BenchmarkScenario119_DiskUsage benchmarks the P.1 handleGetDiskUsage handler:
// status-sourced percent + per-DB usage + per-segment breakdown from the mock DB.
func BenchmarkScenario119_DiskUsage(b *testing.B) {
	s := bench119Server()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/disk-usage?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleGetDiskUsage(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario119_ListTables benchmarks the P.2 handleListTables handler:
// table listing with size, bloat, skew, and row count from the mock DB.
func BenchmarkScenario119_ListTables(b *testing.B) {
	s := bench119ServerNoMetrics()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/tables?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleListTables(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario119_TableDetail benchmarks the P.3 handleGetTableDetail
// handler: single-table detail with index sizes from the mock DB.
func BenchmarkScenario119_TableDetail(b *testing.B) {
	s := bench119ServerNoMetrics()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/tables/public/events?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")
	req.SetPathValue("schema", "public")
	req.SetPathValue("table", "events")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleGetTableDetail(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario119_ListRecommendations benchmarks the P.4
// handleListRecommendations handler: four-type threshold-aware recommendations
// (bloat + skew + age + index_bloat) from the mock DB.
func BenchmarkScenario119_ListRecommendations(b *testing.B) {
	s := bench119ServerNoMetrics()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/recommendations?namespace=default", nil)
	req.SetPathValue("name", "bench-cluster")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		s.handleListRecommendations(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario119_TriggerScan benchmarks the P.5
// handleTriggerRecommendationScan handler: POST scan trigger with the enabled
// path, exercising the scan-duration histogram observation.
func BenchmarkScenario119_TriggerScan(b *testing.B) {
	s := bench119Server()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/bench-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetPathValue("name", "bench-cluster")
		rr := httptest.NewRecorder()
		s.handleTriggerRecommendationScan(rr, req)
		if rr.Code != http.StatusAccepted {
			b.Fatalf("unexpected status %d", rr.Code)
		}
	}
}

// BenchmarkScenario119_UsageReport benchmarks the P.6 handleGetUsageReport
// handler: monthly usage report entries from the mock DB.
func BenchmarkScenario119_UsageReport(b *testing.B) {
	s := bench119ServerNoMetrics()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/bench-cluster/storage/usage-report?namespace=default&month=2025-06", nil)
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
