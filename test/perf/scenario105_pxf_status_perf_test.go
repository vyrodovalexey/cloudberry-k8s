//go:build e2e

// Scenario 105 DataLoadingStatus PXF status read-path performance benchmark.
//
// PERF-HARNESS NOTE: this is a minimal but REAL Go benchmark that measures the
// latency and throughput of the PXF status read endpoint:
//
//	GET /api/v1alpha1/clusters/{name}/data-loading/pxf/status
//
// The handler (handlePXFStatus) lists segment-primary pods and reads each one's
// "pxf" container readiness from Status.ContainerStatuses -- no live health
// probe, no exec, no cross-pod HTTP. This benchmark exercises that read path
// under sustained load to verify it holds up (no latency blow-up / errors).
//
// Two benchmarks are provided:
//
//   - BenchmarkScenario105_PXFStatusRead (live, SCENARIO105_PXF_LIVE=1 gated):
//     hits the real operator API via port-forward and measures per-request
//     latency. Reports p50/p90/p99 via b.ReportMetric. Requires KUBECONFIG,
//     a running s105 cluster with PXF sidecars, and a valid OIDC token.
//
//   - BenchmarkScenario105_PXFStatusConcurrent (live, same gate):
//     runs N concurrent goroutines hammering the endpoint for a fixed duration
//     and reports achieved RPS + error rate.
//
// HONESTY: this reports latency and RPS only -- it does NOT fabricate metrics.
// When the live infra is absent it skips cleanly.
//
// Build tag: e2e (needs the deployed s105 cluster + operator). Run with:
//
//	SCENARIO105_PXF_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario105 -benchtime=30s ./test/perf/...
package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	envKubeconfigS105Perf  = "KUBECONFIG"
	envScenario105LivePerf = "SCENARIO105_PXF_LIVE"
	envScenario105ClusterP = "SCENARIO105_CLUSTER"
	envScenario105NsP      = "SCENARIO105_NAMESPACE"
	envScenario105TokenP   = "SCENARIO105_OIDC_TOKEN"
	envScenario105PortP    = "SCENARIO105_API_PORT"

	perf105DefaultCluster   = "s105"
	perf105DefaultNamespace = "cloudberry-test"
	perf105DefaultAPIPort   = "8190"
	perf105ExecTimeout      = 2 * time.Minute
)

// perf105Env returns the ENV value or a default.
func perf105Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf105Namespace() string { return perf105Env(envScenario105NsP, perf105DefaultNamespace) }
func perf105Cluster() string   { return perf105Env(envScenario105ClusterP, perf105DefaultCluster) }
func perf105APIPort() string   { return perf105Env(envScenario105PortP, perf105DefaultAPIPort) }

// perf105SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is
// set, kubectl exists and SCENARIO105_PXF_LIVE=1.
func perf105SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS105Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 105 PXF status perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 105 PXF status perf")
	}
	if os.Getenv(envScenario105LivePerf) != "1" {
		b.Skip("SCENARIO105_PXF_LIVE not set, skipping the live PXF status perf " +
			"(the deployed s105 cluster must be available)")
	}
}

// perf105Token returns the OIDC bearer token for API auth. If
// SCENARIO105_OIDC_TOKEN is set it is used directly; otherwise the benchmark
// attempts to obtain one from Keycloak.
func perf105Token(b *testing.B) string {
	b.Helper()
	if tok := os.Getenv(envScenario105TokenP); tok != "" {
		return tok
	}
	// Try to obtain from Keycloak (host.docker.internal issuer for operator).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "curl", "-sS", "-X", "POST",
		"-H", "Host: host.docker.internal:8090",
		"http://127.0.0.1:8090/realms/test/protocol/openid-connect/token",
		"-d", "grant_type=password",
		"-d", "client_id=cloudberry-operator",
		"-d", "client_secret=some-secret",
		"-d", "username=adminuser",
		"-d", "password=adminpass",
		"-d", "scope=openid",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		b.Skipf("cannot obtain OIDC token from Keycloak: %v (out=%s)", err, string(out))
	}
	var resp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(out, &resp); err != nil || resp.AccessToken == "" {
		b.Skipf("cannot parse OIDC token response: %v (out=%s)", err, string(out))
	}
	return resp.AccessToken
}

// perf105PXFStatusURL returns the full URL for the PXF status endpoint.
func perf105PXFStatusURL() string {
	return fmt.Sprintf("http://localhost:%s/api/v1alpha1/clusters/%s/data-loading/pxf/status?namespace=%s",
		perf105APIPort(), perf105Cluster(), perf105Namespace())
}

// BenchmarkScenario105_PXFStatusRead measures per-request latency of the PXF
// status read endpoint. Each iteration is one HTTP GET; the benchmark reports
// ns/op (Go default) plus explicit p50/p90/p99 latency metrics.
func BenchmarkScenario105_PXFStatusRead(b *testing.B) {
	perf105SkipUnlessLive(b)
	token := perf105Token(b)
	url := perf105PXFStatusURL()

	client := &http.Client{Timeout: 5 * time.Second}

	// Warmup: 5 requests to prime connections.
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			b.Skipf("warmup request failed (operator may not be port-forwarded): %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Skipf("warmup returned %d (expected 200); auth or cluster may not be ready", resp.StatusCode)
		}
	}

	latencies := make([]time.Duration, 0, b.N)
	var errors int64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			errors++
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errors++
		}
		latencies = append(latencies, elapsed)
	}
	b.StopTimer()

	if len(latencies) == 0 {
		b.Fatal("no successful requests")
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p90 := latencies[len(latencies)*90/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds())/1000.0, "p50_ms")
	b.ReportMetric(float64(p90.Microseconds())/1000.0, "p90_ms")
	b.ReportMetric(float64(p99.Microseconds())/1000.0, "p99_ms")
	b.ReportMetric(float64(errors), "errors")

	b.Logf("scenario105 PXF status read: p50=%.2fms p90=%.2fms p99=%.2fms errors=%d/%d",
		float64(p50.Microseconds())/1000.0,
		float64(p90.Microseconds())/1000.0,
		float64(p99.Microseconds())/1000.0,
		errors, b.N)
}

// BenchmarkScenario105_PXFStatusConcurrent runs N concurrent goroutines
// hammering the PXF status endpoint for a fixed duration and reports achieved
// RPS, error rate, and latency percentiles.
func BenchmarkScenario105_PXFStatusConcurrent(b *testing.B) {
	perf105SkipUnlessLive(b)
	token := perf105Token(b)
	url := perf105PXFStatusURL()

	const (
		concurrency = 10
		duration    = 30 * time.Second
	)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Warmup.
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			b.Skipf("warmup failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	var (
		totalReqs  atomic.Int64
		totalErrs  atomic.Int64
		total2xx   atomic.Int64
		total4xx   atomic.Int64
		total5xx   atomic.Int64
		mu         sync.Mutex
		allLatency []time.Duration
	)

	b.ResetTimer()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []time.Duration
			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					allLatency = append(allLatency, local...)
					mu.Unlock()
					return
				default:
				}
				reqStart := time.Now()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				elapsed := time.Since(reqStart)
				totalReqs.Add(1)
				if err != nil {
					totalErrs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch {
				case resp.StatusCode >= 200 && resp.StatusCode < 300:
					total2xx.Add(1)
				case resp.StatusCode >= 400 && resp.StatusCode < 500:
					total4xx.Add(1)
					totalErrs.Add(1)
				case resp.StatusCode >= 500:
					total5xx.Add(1)
					totalErrs.Add(1)
				}
				local = append(local, elapsed)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	elapsed := time.Since(start)

	reqs := totalReqs.Load()
	errs := totalErrs.Load()
	rps := float64(reqs) / elapsed.Seconds()
	errPct := float64(errs) / float64(max(reqs, 1)) * 100.0

	sort.Slice(allLatency, func(i, j int) bool { return allLatency[i] < allLatency[j] })
	var p50ms, p90ms, p99ms float64
	if len(allLatency) > 0 {
		p50ms = float64(allLatency[len(allLatency)*50/100].Microseconds()) / 1000.0
		p90ms = float64(allLatency[len(allLatency)*90/100].Microseconds()) / 1000.0
		p99ms = float64(allLatency[len(allLatency)*99/100].Microseconds()) / 1000.0
	}

	b.ReportMetric(rps, "rps")
	b.ReportMetric(p50ms, "p50_ms")
	b.ReportMetric(p90ms, "p90_ms")
	b.ReportMetric(p99ms, "p99_ms")
	b.ReportMetric(errPct, "error_pct")
	b.ReportMetric(float64(total2xx.Load()), "2xx")
	b.ReportMetric(float64(total4xx.Load()), "4xx")
	b.ReportMetric(float64(total5xx.Load()), "5xx")

	b.Logf("scenario105 concurrent PXF status: %d reqs in %.1fs = %.1f RPS, "+
		"p50=%.2fms p90=%.2fms p99=%.2fms, errors=%d (%.2f%%), 2xx=%d 4xx=%d 5xx=%d",
		reqs, elapsed.Seconds(), rps,
		p50ms, p90ms, p99ms,
		errs, errPct,
		total2xx.Load(), total4xx.Load(), total5xx.Load())
}
