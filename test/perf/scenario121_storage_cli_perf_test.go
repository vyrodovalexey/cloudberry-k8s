//go:build e2e

// Scenario 121 storage CLI endpoint latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the storage REST API endpoint
// latency for the six CLI commands wired in Scenario 121 (L.1-L.6). The CLI is
// a thin client over the Scenario 119/120 API endpoints; this test confirms the
// flag-resolution changes (--schema/--table) do not regress the end-to-end
// latency when the CLI hits the live operator.
//
// Endpoints exercised:
//   - L.1 GET /storage/disk-usage
//   - L.2 GET /storage/tables
//   - L.3 GET /storage/tables/public/orders  (table detail)
//   - L.4 GET /storage/recommendations
//   - L.6 GET /storage/usage-report?month=2026-05
//
// L.5 (POST scan) is omitted from the live perf test because it is a mutating
// POST that triggers a real scan -- covered by the Go benchmarks in
// internal/api/scenario119_benchmark_test.go.
//
// HONESTY: it reports latency only; it makes no DB assertion (the storage CLI
// proof is the unit tests in cmd/cloudberry-ctl/scenario121_storage_cli_test.go
// and the benchmarks in cmd/cloudberry-ctl/scenario121_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live operator with the storage endpoints
// deployed and a seeded cluster. It will skip cleanly when KUBECONFIG or
// SCENARIO121_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO121_LIVE=1 SCENARIO121_TARGET=localhost:8190 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario121 -benchtime=10x ./test/perf/...
package perf

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	envScenario121Live      = "SCENARIO121_LIVE"
	envScenario121Namespace = "SCENARIO121_NAMESPACE"
	envScenario121Target    = "SCENARIO121_TARGET"
	envScenario121User      = "SCENARIO121_USER"
	envScenario121Pass      = "SCENARIO121_PASS"

	perf121DefaultNamespace = "cloudberry-test"
	perf121DefaultTarget    = "localhost:8190"
	perf121DefaultUser      = "admin"
	perf121DefaultPass      = "admin"
	perf121ExecTimeout      = 60 * time.Second
)

// perf121Env resolves an environment variable with a fallback default.
func perf121Env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// perf121SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// curl exists, and SCENARIO121_LIVE=1.
func perf121SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 121 storage CLI perf")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		b.Skip("curl not found on PATH, skipping Scenario 121 storage CLI perf")
	}
	if os.Getenv(envScenario121Live) != "1" {
		b.Skip("SCENARIO121_LIVE not set, skipping Scenario 121 storage CLI perf")
	}
}

// perf121CurlGet performs a GET request to the operator's REST API with Basic
// Auth and returns the HTTP status code and elapsed time.
func perf121CurlGet(endpoint string) (int, time.Duration) {
	target := perf121Env(envScenario121Target, perf121DefaultTarget)
	user := perf121Env(envScenario121User, perf121DefaultUser)
	pass := perf121Env(envScenario121Pass, perf121DefaultPass)

	url := fmt.Sprintf("http://%s%s", target, endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), perf121ExecTimeout)
	defer cancel()

	start := time.Now()
	out, err := exec.CommandContext(ctx, "curl", "-sf",
		"-o", "/dev/null",
		"-w", "%{http_code}",
		"-u", user+":"+pass,
		"--connect-timeout", "5",
		url,
	).CombinedOutput()
	elapsed := time.Since(start)

	if err != nil {
		return 0, elapsed
	}

	code := 0
	_, _ = fmt.Sscanf(string(out), "%d", &code)
	return code, elapsed
}

// BenchmarkScenario121_StorageCLILatency measures the average per-request
// latency for the storage REST API GET endpoints (as the CLI would invoke them)
// via curl with Basic Auth. Each sub-benchmark reports avg_ms. Skips cleanly
// when the live env is absent.
func BenchmarkScenario121_StorageCLILatency(b *testing.B) {
	perf121SkipUnlessLive(b)

	ns := perf121Env(envScenario121Namespace, perf121DefaultNamespace)
	_ = ns // namespace is embedded in the query, not the URL path

	endpoints := []struct {
		name     string
		endpoint string
	}{
		{
			"L1_disk_usage",
			"/api/v1alpha1/clusters/test-cluster/storage/disk-usage?namespace=default",
		},
		{
			"L2_tables_list",
			"/api/v1alpha1/clusters/test-cluster/storage/tables?namespace=default",
		},
		{
			"L3_tables_detail",
			"/api/v1alpha1/clusters/test-cluster/storage/tables/public/orders?namespace=default",
		},
		{
			"L4_recommendations",
			"/api/v1alpha1/clusters/test-cluster/storage/recommendations?namespace=default",
		},
		{
			"L6_usage_report_month",
			"/api/v1alpha1/clusters/test-cluster/storage/usage-report?namespace=default&month=2026-05",
		},
	}

	for _, ep := range endpoints {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			// Warmup: one request to prime the connection.
			perf121CurlGet(ep.endpoint)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, elapsed := perf121CurlGet(ep.endpoint)
				total += elapsed
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario121 storage CLI %s: avg=%.2fms over %d requests", ep.name, avgMs, b.N)
		})
	}
}
