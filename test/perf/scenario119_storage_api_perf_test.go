//go:build e2e

// Scenario 119 storage REST API endpoint latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the storage REST API endpoint
// latency for the six handlers wired in Scenario 119:
//   - P.1 GET /storage/disk-usage
//   - P.2 GET /storage/tables
//   - P.4 GET /storage/recommendations
//   - P.6 GET /storage/usage-report
//
// It hits the live operator's REST API via curl (with Basic Auth) a number of
// times and reports the average latency per endpoint.
//
// P.3 (table-detail) and P.5 (POST scan) are omitted from the live perf test
// because P.3 requires a known schema/table pair and P.5 is a mutating POST
// that triggers a real scan -- both are covered by the Go benchmarks in
// internal/api/scenario119_benchmark_test.go.
//
// HONESTY: it reports latency only; it makes no DB assertion (the storage API
// proof is the unit tests in internal/api/scenario119_storage_api_test.go and
// the benchmarks in internal/api/scenario119_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live operator with the storage endpoints
// deployed and a seeded cluster. It will skip cleanly when KUBECONFIG or
// SCENARIO119_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO119_LIVE=1 SCENARIO119_TARGET=localhost:8190 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario119 -benchtime=10x ./test/perf/...
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
	envScenario119Live      = "SCENARIO119_LIVE"
	envScenario119Namespace = "SCENARIO119_NAMESPACE"
	envScenario119Target    = "SCENARIO119_TARGET"
	envScenario119User      = "SCENARIO119_USER"
	envScenario119Pass      = "SCENARIO119_PASS"

	perf119DefaultNamespace = "cloudberry-test"
	perf119DefaultTarget    = "localhost:8190"
	perf119DefaultUser      = "admin"
	perf119DefaultPass      = "admin"
	perf119ExecTimeout      = 60 * time.Second
)

// perf119Env resolves an environment variable with a fallback default.
func perf119Env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// perf119SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// curl exists, and SCENARIO119_LIVE=1.
func perf119SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 119 storage API perf")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		b.Skip("curl not found on PATH, skipping Scenario 119 storage API perf")
	}
	if os.Getenv(envScenario119Live) != "1" {
		b.Skip("SCENARIO119_LIVE not set, skipping Scenario 119 storage API perf")
	}
}

// perf119CurlGet performs a GET request to the operator's REST API with Basic
// Auth and returns the HTTP status code and elapsed time.
func perf119CurlGet(endpoint string) (int, time.Duration) {
	target := perf119Env(envScenario119Target, perf119DefaultTarget)
	user := perf119Env(envScenario119User, perf119DefaultUser)
	pass := perf119Env(envScenario119Pass, perf119DefaultPass)

	url := fmt.Sprintf("http://%s%s", target, endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), perf119ExecTimeout)
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

// BenchmarkScenario119_StorageAPILatency measures the average per-request
// latency for the storage REST API GET endpoints via curl with Basic Auth.
// Each sub-benchmark reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario119_StorageAPILatency(b *testing.B) {
	perf119SkipUnlessLive(b)

	ns := perf119Env(envScenario119Namespace, perf119DefaultNamespace)
	_ = ns // namespace is embedded in the cluster name, not the URL path

	endpoints := []struct {
		name     string
		endpoint string
	}{
		{
			"disk_usage",
			"/api/v1alpha1/clusters/test-cluster/storage/disk-usage?namespace=default",
		},
		{
			"tables",
			"/api/v1alpha1/clusters/test-cluster/storage/tables?namespace=default",
		},
		{
			"recommendations",
			"/api/v1alpha1/clusters/test-cluster/storage/recommendations?namespace=default",
		},
		{
			"usage_report",
			"/api/v1alpha1/clusters/test-cluster/storage/usage-report?namespace=default&month=2025-06",
		},
	}

	for _, ep := range endpoints {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			// Warmup: one request to prime the connection.
			perf119CurlGet(ep.endpoint)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, elapsed := perf119CurlGet(ep.endpoint)
				total += elapsed
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario119 storage API %s: avg=%.2fms over %d requests", ep.name, avgMs, b.N)
		})
	}
}
