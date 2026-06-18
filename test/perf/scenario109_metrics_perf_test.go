//go:build e2e

// Scenario 109 metrics VictoriaMetrics query-latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the VictoriaMetrics instant
// query latency for the Scenario 109 IMPLEMENTED metric series (the M.x families
// the operator emits + the honesty queries for the absent families). It mirrors
// the Scenario 105 perf harness:
//
//   - BenchmarkScenario109_VMQueryLatency (gated): hits the real VictoriaMetrics
//     /api/v1/query endpoint for each Scenario 109 metric query and reports the
//     average per-query latency. Skips cleanly when VM is unreachable.
//
// HONESTY: it reports query latency only — it never fabricates metrics, and it
// makes no assertion about whether the implemented series have samples (the
// presence proof is the e2e Part B). Build tag e2e (shared with the perf
// package).
//
//	SCENARIO109_VM_BASE=http://127.0.0.1:8428 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario109 -benchtime=20x ./test/perf/...
package perf

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

const (
	envScenario109VMBaseP = "SCENARIO109_VM_BASE"
	perf109DefaultVMBase  = "http://127.0.0.1:8428"
	perf109VMQueryPath    = "/api/v1/query"
	perf109HTTPTimeout    = 8 * time.Second
)

// perf109VMBase resolves the VictoriaMetrics base URL (SCENARIO109_VM_BASE >
// VICTORIAMETRICS_ADDR > default).
func perf109VMBase() string {
	if v := strings.TrimSpace(os.Getenv(envScenario109VMBaseP)); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("VICTORIAMETRICS_ADDR")); v != "" {
		return v
	}
	return perf109DefaultVMBase
}

// perf109VMQuery issues one instant query and returns whether the request
// succeeded (HTTP 200).
func perf109VMQuery(ctx context.Context, client *http.Client, query string) bool {
	u := perf109VMBase() + perf109VMQueryPath + "?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// perf109Queries returns the Scenario 109 metric queries benchmarked: the
// implemented data-loading + pxf families plus the honesty (absent) queries.
func perf109Queries() []string {
	queries := []string{
		cases.Scenario109MetricServiceUp,
		cases.Scenario109MetricJobsActive,
		cases.Scenario109MetricRowsTotal,
		cases.Scenario109MetricBytesTotal,
		cases.Scenario109MetricErrorsTotal,
		cases.Scenario109MetricJobStatus,
		cases.Scenario109MetricLastSuccess,
		cases.Scenario109MetricJobDuration + "_count",
	}
	return append(queries, cases.Scenario109AbsentMetrics...)
}

// perf109SkipUnlessVM skips the benchmark cleanly unless VictoriaMetrics answers
// a trivial query.
func perf109SkipUnlessVM(b *testing.B) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), perf109HTTPTimeout)
	defer cancel()
	client := &http.Client{Timeout: perf109HTTPTimeout}
	if !perf109VMQuery(ctx, client, "vm_app_version") {
		b.Skipf("VictoriaMetrics not reachable at %s, skipping Scenario 109 VM query perf",
			perf109VMBase())
	}
}

// BenchmarkScenario109_VMQueryLatency measures the average per-query latency of
// the Scenario 109 metric queries against VictoriaMetrics. Each sub-benchmark is
// one metric query; it reports avg_ms. Skips cleanly when VM is unreachable.
func BenchmarkScenario109_VMQueryLatency(b *testing.B) {
	perf109SkipUnlessVM(b)
	client := &http.Client{Timeout: perf109HTTPTimeout}

	for _, query := range perf109Queries() {
		query := query
		b.Run(query, func(b *testing.B) {
			// Warmup: one query to prime the connection.
			warmCtx, cancel := context.WithTimeout(context.Background(), perf109HTTPTimeout)
			ok := perf109VMQuery(warmCtx, client, query)
			cancel()
			if !ok {
				b.Skipf("warmup query for %s failed (VM may not be reachable)", query)
			}

			var total time.Duration
			var done int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, c := context.WithTimeout(context.Background(), perf109HTTPTimeout)
				start := time.Now()
				got := perf109VMQuery(ctx, client, query)
				elapsed := time.Since(start)
				c()
				if !got {
					continue
				}
				total += elapsed
				done++
			}
			b.StopTimer()

			if done == 0 {
				b.Skipf("no successful query iterations for %s", query)
			}
			avgMs := float64(total.Microseconds()) / float64(done) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario109 VM query %s: %d/%d ok, avg=%.2fms",
				query, done, b.N, avgMs)
		})
	}
}
