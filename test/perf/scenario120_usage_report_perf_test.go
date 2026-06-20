//go:build e2e

// Scenario 120 usage-report endpoint latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the P.6 GET
// /storage/usage-report endpoint latency after Scenario 120 enriched the
// response with per-table consumption (db.UsageReportEntry.Tables).
//
// It hits the live operator's REST API via curl (with Basic Auth) a number of
// times and reports the average latency per endpoint variant:
//   - usage_report_with_month: GET /storage/usage-report?month=2026-05
//   - usage_report_no_month:   GET /storage/usage-report (no month filter)
//
// HONESTY: it reports latency only; it makes no DB assertion (the usage-report
// proof is the unit tests in internal/api/scenario120_usage_report_test.go and
// the benchmarks in internal/api/scenario120_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live operator with the storage endpoints
// deployed and a seeded cluster. It will skip cleanly when KUBECONFIG or
// SCENARIO120_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO120_LIVE=1 SCENARIO120_TARGET=localhost:8190 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario120 -benchtime=10x ./test/perf/...
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
	envScenario120Live      = "SCENARIO120_LIVE"
	envScenario120Namespace = "SCENARIO120_NAMESPACE"
	envScenario120Target    = "SCENARIO120_TARGET"
	envScenario120User      = "SCENARIO120_USER"
	envScenario120Pass      = "SCENARIO120_PASS"

	perf120DefaultNamespace = "cloudberry-test"
	perf120DefaultTarget    = "localhost:8190"
	perf120DefaultUser      = "admin"
	perf120DefaultPass      = "admin"
	perf120ExecTimeout      = 60 * time.Second
)

// perf120Env resolves an environment variable with a fallback default.
func perf120Env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// perf120SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// curl exists, and SCENARIO120_LIVE=1.
func perf120SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 120 usage-report perf")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		b.Skip("curl not found on PATH, skipping Scenario 120 usage-report perf")
	}
	if os.Getenv(envScenario120Live) != "1" {
		b.Skip("SCENARIO120_LIVE not set, skipping Scenario 120 usage-report perf")
	}
}

// perf120CurlGet performs a GET request to the operator's REST API with Basic
// Auth and returns the HTTP status code and elapsed time.
func perf120CurlGet(endpoint string) (int, time.Duration) {
	target := perf120Env(envScenario120Target, perf120DefaultTarget)
	user := perf120Env(envScenario120User, perf120DefaultUser)
	pass := perf120Env(envScenario120Pass, perf120DefaultPass)

	url := fmt.Sprintf("http://%s%s", target, endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), perf120ExecTimeout)
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

// BenchmarkScenario120_UsageReportLatency measures the average per-request
// latency for the usage-report REST API GET endpoint via curl with Basic Auth.
// Each sub-benchmark reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario120_UsageReportLatency(b *testing.B) {
	perf120SkipUnlessLive(b)

	ns := perf120Env(envScenario120Namespace, perf120DefaultNamespace)
	_ = ns // namespace is embedded in the cluster name, not the URL path

	endpoints := []struct {
		name     string
		endpoint string
	}{
		{
			"usage_report_with_month",
			"/api/v1alpha1/clusters/test-cluster/storage/usage-report?namespace=default&month=2026-05",
		},
		{
			"usage_report_no_month",
			"/api/v1alpha1/clusters/test-cluster/storage/usage-report?namespace=default",
		},
	}

	for _, ep := range endpoints {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			// Warmup: one request to prime the connection.
			perf120CurlGet(ep.endpoint)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, elapsed := perf120CurlGet(ep.endpoint)
				total += elapsed
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario120 usage-report %s: avg=%.2fms over %d requests", ep.name, avgMs, b.N)
		})
	}
}
