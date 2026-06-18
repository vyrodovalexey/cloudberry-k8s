//go:build e2e

// Scenario 107 Data-Loading API read-path performance benchmarks.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the data-loading READ endpoints
// (P.2 GET pxf/servers, P.7 GET jobs, P.15 GET external-tables). It mirrors the
// Scenario 105/106 perf harness:
//
//   - A PURE benchmark (no infra, always runnable) measures the cheap pure helper
//     the P.15 "expected" derivation drives: builder.ForeignTableName across a
//     growing job count. This bounds the per-request derivation cost without any
//     cluster.
//
//   - Live benchmarks (SCENARIO107_LIVE=1 gated) hit the real operator API via a
//     port-forward + an auth token and measure per-request latency of each read
//     endpoint. Load is kept MODEST (a handful of requests) so it is
//     docker-desktop friendly, and it skips cleanly when the live infra is
//     unreachable. Rate-limit (429) responses are tolerated (logged, not fatal).
//
// HONESTY: reports latency only — it never fabricates metrics. Build tag e2e
// (shared with the perf package); the pure benchmark runs without any infra.
//
//	SCENARIO107_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario107 -benchtime=50x ./test/perf/...
package perf

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
)

const (
	envKubeconfigS107Perf  = "KUBECONFIG"
	envScenario107LiveP    = "SCENARIO107_LIVE"
	envScenario107ClusterP = "SCENARIO107_CLUSTER"
	envScenario107NsP      = "SCENARIO107_NAMESPACE"
	envScenario107TokenP   = "SCENARIO107_OIDC_TOKEN"
	envScenario107UserP    = "SCENARIO107_API_USER"
	envScenario107PassP    = "SCENARIO107_API_PASS"
	envScenario107BaseP    = "SCENARIO107_API_BASE"

	perf107DefaultCluster   = "s107"
	perf107DefaultNamespace = "cloudberry-test"
	perf107DefaultBase      = "http://localhost:8190"
	perf107DefaultUser      = "adminuser"
	perf107DefaultPass      = "adminpass"
)

// perf107Env returns the ENV value or a default.
func perf107Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf107Cluster() string   { return perf107Env(envScenario107ClusterP, perf107DefaultCluster) }
func perf107Namespace() string { return perf107Env(envScenario107NsP, perf107DefaultNamespace) }
func perf107Base() string      { return perf107Env(envScenario107BaseP, perf107DefaultBase) }

// perf107JobCounts is the modest sweep used by the pure benchmark.
var perf107JobCounts = []int{1, 8, 32}

// BenchmarkScenario107_ForeignTableNameDerivation measures the pure cost of the
// P.15 "expected" foreign-table-name derivation (builder.ForeignTableName) across
// a growing job count — the cheap pure work the external-tables read drives.
func BenchmarkScenario107_ForeignTableNameDerivation(b *testing.B) {
	for _, n := range perf107JobCounts {
		n := n
		jobs := make([]string, n)
		for i := 0; i < n; i++ {
			jobs[i] = fmt.Sprintf("load-job-%03d", i)
		}
		b.Run(fmt.Sprintf("jobs=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, j := range jobs {
					_ = builder.ForeignTableName(j)
				}
			}
		})
	}
}

// perf107SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO107_LIVE=1.
func perf107SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS107Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 107 read-endpoint perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 107 read-endpoint perf")
	}
	if os.Getenv(envScenario107LiveP) != "1" {
		b.Skip("SCENARIO107_LIVE not set, skipping the live read-endpoint perf " +
			"(the deployed s107 cluster + operator API must be available)")
	}
}

// perf107SetAuth sets the Authorization header: a bearer OIDC token when
// SCENARIO107_OIDC_TOKEN is set, otherwise basic-auth.
func perf107SetAuth(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envScenario107TokenP)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(perf107Env(envScenario107UserP, perf107DefaultUser),
		perf107Env(envScenario107PassP, perf107DefaultPass))
}

// perf107ReadURL builds the full URL for a data-loading read endpoint suffix.
func perf107ReadURL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s/api/v1alpha1/clusters/%s/data-loading%s%snamespace=%s",
		perf107Base(), perf107Cluster(), suffix, sep, perf107Namespace())
}

// perf107BenchRead is the shared live read-endpoint benchmark body: warm up,
// then measure per-request latency, tolerating 429 rate-limit responses.
func perf107BenchRead(b *testing.B, suffix string) {
	perf107SkipUnlessLive(b)
	url := perf107ReadURL(suffix)
	client := &http.Client{Timeout: 5 * time.Second}

	doOne := func(ctx context.Context) (int, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		perf107SetAuth(req)
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}

	// Warmup: one request to prime the connection + verify reachability.
	warmCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	code, err := doOne(warmCtx)
	cancel()
	if err != nil {
		b.Skipf("warmup request failed (operator API may not be port-forwarded): %v", err)
	}
	if code != http.StatusOK && code != http.StatusTooManyRequests {
		b.Skipf("warmup returned %d (expected 200/429); auth or cluster may not be ready", code)
	}

	var total time.Duration
	var ok, rateLimited int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		start := time.Now()
		sc, derr := doOne(ctx)
		elapsed := time.Since(start)
		c()
		if derr != nil {
			b.Logf("scenario107 read %s iteration %d failed: %v", suffix, i, derr)
			continue
		}
		if sc == http.StatusTooManyRequests {
			rateLimited++
			continue
		}
		total += elapsed
		ok++
	}
	b.StopTimer()

	if ok == 0 {
		b.Skipf("no successful read iterations for %s (rate-limited=%d)", suffix, rateLimited)
	}
	avgMs := float64(total.Microseconds()) / float64(ok) / 1000.0
	b.ReportMetric(avgMs, "avg_ms")
	b.ReportMetric(float64(rateLimited), "rate_limited")
	b.Logf("scenario107 read %s: %d/%d ok, rate-limited=%d, avg=%.2fms",
		suffix, ok, b.N, rateLimited, avgMs)
}

// BenchmarkScenario107_ListServersLatency measures GET pxf/servers (P.2) latency.
func BenchmarkScenario107_ListServersLatency(b *testing.B) {
	perf107BenchRead(b, "/pxf/servers")
}

// BenchmarkScenario107_ListJobsLatency measures GET jobs (P.7) latency.
func BenchmarkScenario107_ListJobsLatency(b *testing.B) {
	perf107BenchRead(b, "/jobs")
}

// BenchmarkScenario107_ExternalTablesLatency measures GET external-tables (P.15)
// latency (the read may probe the live DB; honest ABSENT is tolerated).
func BenchmarkScenario107_ExternalTablesLatency(b *testing.B) {
	perf107BenchRead(b, "/external-tables")
}
