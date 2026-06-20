package main

// Scenario 121 — Storage CLI Request-Building Benchmarks.
//
// These benchmarks measure the CHEAP, DETERMINISTIC request-building path of
// the six storage CLI commands (L.1–L.6). They do NOT make live HTTP calls;
// they exercise only the pure functions and path-construction logic that the
// CLI executes before handing off to the HTTP client.
//
// Benchmarked paths:
//   BenchmarkResolveTableDetail       — the L.3 flag/positional resolution helper
//   BenchmarkStorageDiskUsagePath     — L.1 disk-usage path construction
//   BenchmarkStorageTablesListPath    — L.2 tables list path construction
//   BenchmarkStorageTablesDetailPath  — L.3 tables detail path construction (full)
//   BenchmarkStorageRecommendationsPath — L.4 recommendations list path construction
//   BenchmarkStorageRecommendationsScanPath — L.5 recommendations scan path construction
//   BenchmarkStorageUsageReportPath   — L.6 usage-report path construction with --month
//
// Run:
//   go test -bench=BenchmarkScenario121 -benchmem -count=3 ./cmd/cloudberry-ctl/

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
)

// --- BenchmarkScenario121_ResolveTableDetail --------------------------------

// BenchmarkScenario121_ResolveTableDetail measures the pure resolveTableDetail
// helper across the three happy-path resolution modes: flags-only, positional-
// only, and flags-win-over-positional (precedence). The error paths are not
// benchmarked because they are validation-only (no allocation).
func BenchmarkScenario121_ResolveTableDetail(b *testing.B) {
	cases := []struct {
		name       string
		flagSchema string
		flagTable  string
		args       []string
	}{
		{"flags_only", "public", "orders", nil},
		{"positional_only", "", "", []string{"public", "orders"}},
		{"flags_win", "public", "orders", []string{"sales", "legacy"}},
		{"mixed_schema_flag_table_positional", "public", "", []string{"ignored", "users"}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				schema, table, err := resolveTableDetail(tc.flagSchema, tc.flagTable, tc.args)
				if err != nil {
					b.Fatalf("unexpected error: %v", err)
				}
				// Prevent the compiler from optimizing away the call.
				_ = schema
				_ = table
			}
		})
	}
}

// --- BenchmarkScenario121_StorageDiskUsagePath (L.1) ------------------------

// BenchmarkScenario121_StorageDiskUsagePath measures the path construction for
// `storage disk-usage`, which calls ctl.ClusterSubresourcePath.
func BenchmarkScenario121_StorageDiskUsagePath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := ctl.ClusterSubresourcePath(cluster, "storage/disk-usage", namespace)
		_ = p
	}
}

// --- BenchmarkScenario121_StorageTablesListPath (L.2) -----------------------

// BenchmarkScenario121_StorageTablesListPath measures the path construction for
// `storage tables list`, which calls ctl.ClusterSubresourcePath.
func BenchmarkScenario121_StorageTablesListPath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := ctl.ClusterSubresourcePath(cluster, "storage/tables", namespace)
		_ = p
	}
}

// --- BenchmarkScenario121_StorageTablesDetailPath (L.3) ---------------------

// BenchmarkScenario121_StorageTablesDetailPath measures the full L.3 path
// construction: resolveTableDetail + fmt.Sprintf + appendNamespaceQuery.
func BenchmarkScenario121_StorageTablesDetailPath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"
	const flagSchema = "public"
	const flagTable = "orders"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		schema, table, err := resolveTableDetail(flagSchema, flagTable, nil)
		if err != nil {
			b.Fatal(err)
		}
		p := appendNamespaceQuery(
			fmt.Sprintf("/clusters/%s/storage/tables/%s/%s",
				url.PathEscape(cluster),
				url.PathEscape(schema), url.PathEscape(table)),
			namespace)
		_ = p
	}
}

// --- BenchmarkScenario121_StorageRecommendationsPath (L.4) ------------------

// BenchmarkScenario121_StorageRecommendationsPath measures the path construction
// for `storage recommendations list`.
func BenchmarkScenario121_StorageRecommendationsPath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := ctl.ClusterSubresourcePath(cluster, "storage/recommendations", namespace)
		_ = p
	}
}

// --- BenchmarkScenario121_StorageRecommendationsScanPath (L.5) --------------

// BenchmarkScenario121_StorageRecommendationsScanPath measures the path
// construction for `storage recommendations scan` (POST).
func BenchmarkScenario121_StorageRecommendationsScanPath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := ctl.ClusterSubresourcePath(cluster, "storage/recommendations/scan", namespace)
		_ = p
	}
}

// --- BenchmarkScenario121_StorageUsageReportPath (L.6) ----------------------

// BenchmarkScenario121_StorageUsageReportPath measures the full L.6 path
// construction with the --month query parameter: url.Values building +
// fmt.Sprintf + conditional query encoding. This mirrors the RunE closure of
// the usage-report command.
func BenchmarkScenario121_StorageUsageReportPath(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"
	const month = "2026-05"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := url.Values{}
		if namespace != "" {
			params.Set("namespace", namespace)
		}
		if month != "" {
			params.Set("month", month)
		}
		p := fmt.Sprintf("/clusters/%s/storage/usage-report",
			url.PathEscape(cluster))
		if len(params) > 0 {
			p += "?" + params.Encode()
		}
		_ = p
	}
}

// --- BenchmarkScenario121_StorageUsageReportPathNoMonth ---------------------

// BenchmarkScenario121_StorageUsageReportPathNoMonth measures the L.6 path
// construction WITHOUT the --month flag (namespace-only query).
func BenchmarkScenario121_StorageUsageReportPathNoMonth(b *testing.B) {
	const cluster = "test-cluster"
	const namespace = "default"
	const month = "" // no month

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := url.Values{}
		if namespace != "" {
			params.Set("namespace", namespace)
		}
		if month != "" {
			params.Set("month", month)
		}
		p := fmt.Sprintf("/clusters/%s/storage/usage-report",
			url.PathEscape(cluster))
		if len(params) > 0 {
			p += "?" + params.Encode()
		}
		_ = p
	}
}
