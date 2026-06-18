//go:build e2e

// Scenario 108 CLI-backed read-path performance benchmarks.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the two read endpoints the
// Scenario 108 CLI verbs drive most often — L.15 data-loading test-read and L.2
// pxf servers list. It mirrors the Scenario 107 perf harness:
//
//   - A PURE benchmark (no infra, always runnable) measures the cheap query-string
//     assembly the test-read CLI verb does per call (url.Values build + Encode),
//     bounding the per-request client-side cost without any cluster.
//
//   - Live benchmarks (SCENARIO108_LIVE=1 gated) hit the real operator API via a
//     port-forward + an auth token and measure per-request latency of test-read
//     and pxf servers list. Load is kept MODEST (a handful of requests) so it is
//     docker-desktop friendly, and it skips cleanly when the live infra is
//     unreachable. Rate-limit (429) responses are tolerated (logged, not fatal).
//
// HONESTY: reports latency only — it never fabricates metrics. Build tag e2e
// (shared with the perf package); the pure benchmark runs without any infra.
//
//	SCENARIO108_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario108 -benchtime=20x ./test/perf/...
package perf

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	envKubeconfigS108Perf  = "KUBECONFIG"
	envScenario108LiveP    = "SCENARIO108_LIVE"
	envScenario108ClusterP = "SCENARIO108_CLUSTER"
	envScenario108NsP      = "SCENARIO108_NAMESPACE"
	envScenario108TokenP   = "SCENARIO108_OIDC_TOKEN"
	envScenario108UserP    = "SCENARIO108_API_USER"
	envScenario108PassP    = "SCENARIO108_API_PASS"
	envScenario108BaseP    = "SCENARIO108_API_BASE"
	envScenario108JobP     = "SCENARIO108_TESTREAD_JOB"

	perf108DefaultCluster   = "s108"
	perf108DefaultNamespace = "cloudberry-test"
	perf108DefaultBase      = "http://localhost:8190"
	perf108DefaultUser      = "adminuser"
	perf108DefaultPass      = "adminpass"
)

// perf108Env returns the ENV value or a default.
func perf108Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf108Cluster() string   { return perf108Env(envScenario108ClusterP, perf108DefaultCluster) }
func perf108Namespace() string { return perf108Env(envScenario108NsP, perf108DefaultNamespace) }
func perf108Base() string      { return perf108Env(envScenario108BaseP, perf108DefaultBase) }

// perf108LimitSweep is the modest sweep used by the pure query-assembly benchmark.
var perf108LimitSweep = []int{10, 100, 1000}

// BenchmarkScenario108_TestReadQueryAssembly measures the pure client-side cost
// of building the test-read query string (the L.15 CLI verb's per-call work):
// url.Values{namespace,job,limit}.Encode() across a growing limit.
func BenchmarkScenario108_TestReadQueryAssembly(b *testing.B) {
	for _, lim := range perf108LimitSweep {
		lim := lim
		b.Run(fmt.Sprintf("limit=%d", lim), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := url.Values{}
				q.Set("namespace", perf108Namespace())
				q.Set("job", "loadfdw")
				q.Set("limit", fmt.Sprintf("%d", lim))
				_ = "/clusters/" + perf108Cluster() + "/data-loading/test-read?" + q.Encode()
			}
		})
	}
}

// perf108SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO108_LIVE=1.
func perf108SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS108Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 108 read-endpoint perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 108 read-endpoint perf")
	}
	if os.Getenv(envScenario108LiveP) != "1" {
		b.Skip("SCENARIO108_LIVE not set, skipping the live read-endpoint perf " +
			"(the deployed s108 cluster + operator API must be available)")
	}
}

// perf108SetAuth sets the Authorization header: a bearer OIDC token when
// SCENARIO108_OIDC_TOKEN is set, otherwise basic-auth.
func perf108SetAuth(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envScenario108TokenP)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(perf108Env(envScenario108UserP, perf108DefaultUser),
		perf108Env(envScenario108PassP, perf108DefaultPass))
}

// perf108ReadURL builds the full URL for a data-loading read endpoint suffix.
func perf108ReadURL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s/api/v1alpha1/clusters/%s/data-loading%s%snamespace=%s",
		perf108Base(), perf108Cluster(), suffix, sep, perf108Namespace())
}

// perf108BenchRead is the shared live read-endpoint benchmark body: warm up, then
// measure per-request latency, tolerating 429 rate-limit responses.
func perf108BenchRead(b *testing.B, suffix string, okCodes ...int) {
	perf108SkipUnlessLive(b)
	if len(okCodes) == 0 {
		okCodes = []int{http.StatusOK}
	}
	url := perf108ReadURL(suffix)
	client := &http.Client{Timeout: 8 * time.Second}

	isOK := func(code int) bool {
		for _, c := range okCodes {
			if code == c {
				return true
			}
		}
		return false
	}

	doOne := func(ctx context.Context) (int, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		perf108SetAuth(req)
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, nil
	}

	// Warmup: one request to prime the connection + verify reachability.
	warmCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	code, err := doOne(warmCtx)
	cancel()
	if err != nil {
		b.Skipf("warmup request failed (operator API may not be port-forwarded): %v", err)
	}
	if !isOK(code) && code != http.StatusTooManyRequests {
		b.Skipf("warmup returned %d (expected ok/429); auth or cluster may not be ready", code)
	}

	var total time.Duration
	var ok, rateLimited int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, c := context.WithTimeout(context.Background(), 8*time.Second)
		start := time.Now()
		sc, derr := doOne(ctx)
		elapsed := time.Since(start)
		c()
		if derr != nil {
			b.Logf("scenario108 read %s iteration %d failed: %v", suffix, i, derr)
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
	b.Logf("scenario108 read %s: %d/%d ok, rate-limited=%d, avg=%.2fms",
		suffix, ok, b.N, rateLimited, avgMs)
}

// BenchmarkScenario108_ListServersLatency measures GET pxf/servers (L.2) latency.
func BenchmarkScenario108_ListServersLatency(b *testing.B) {
	perf108BenchRead(b, "/pxf/servers")
}

// BenchmarkScenario108_TestReadLatency measures GET test-read (L.15) latency. The
// read may probe the live DB; an honest available:false is still a 200, so only
// 200 is the OK code (the body's available flag is not asserted here — this is a
// latency-only check).
func BenchmarkScenario108_TestReadLatency(b *testing.B) {
	job := perf108Env(envScenario108JobP, "")
	suffix := "/test-read?profile=s3:text&resource=data/probe.csv&limit=10"
	if job != "" {
		suffix = "/test-read?job=" + url.QueryEscape(job) + "&limit=10"
	}
	perf108BenchRead(b, suffix)
}
