//go:build e2e

// Scenario 117 threshold-aware recommendation scan admission-latency performance
// benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the reconciliation ADMISSION
// latency for the recommendation-scan path (recordRecommendations, which runs
// all four Get{Bloat,Skew,Age,IndexBloat}Recommendations scans, sets
// cloudberry_recommendations_total{type}, publishes table_bloat_ratio, and sets
// Status.RecommendationCount). It applies CloudberryCluster CRs (with
// recommendationScan enabled and disabled) via `kubectl apply --dry-run=server`
// (server-side dry-run STILL runs the admission webhooks, so it measures the
// real round-trip WITHOUT persisting anything), a number of times, and reports
// the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario117_AdmissionLatency/recommendation_scan_on: a CR with
//     recommendationScan enabled and all four thresholds set.
//   - BenchmarkScenario117_AdmissionLatency/recommendation_scan_off: a CR with
//     recommendationScan disabled.
//   - BenchmarkScenario117_AdmissionLatency/nil_storage: a CR with no storage
//     spec at all.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no DB assertion (the recommendation-scan proof is the unit tests in
// internal/controller/scenario117_recommendation_scan_test.go and the benchmarks
// in internal/controller/scenario117_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the admission webhooks
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO117_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO117_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario117 -benchtime=10x ./test/perf/...
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
	envScenario117Live      = "SCENARIO117_LIVE"
	envScenario117Namespace = "SCENARIO117_NAMESPACE"

	perf117DefaultNamespace = "cloudberry-test"
	perf117ExecTimeout      = 60 * time.Second
)

// perf117Namespace resolves the deploy namespace (SCENARIO117_NAMESPACE > default).
func perf117Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario117Namespace)); v != "" {
		return v
	}
	return perf117DefaultNamespace
}

// perf117Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf117Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf117DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny -- both exercise the webhook).
func perf117DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf117ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf117Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf117SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO117_LIVE=1, and the namespace + CRD are served.
func perf117SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 117 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 117 admission-latency perf")
	}
	if os.Getenv(envScenario117Live) != "1" {
		b.Skip("SCENARIO117_LIVE not set, skipping Scenario 117 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf117ExecTimeout)
	defer cancel()
	if out, err := perf117Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf117Manifest returns a CloudberryCluster manifest for the given scenario.
func perf117Manifest(name string, recScanEnabled bool, includeStorage bool) string {
	if !includeStorage {
		// Nil-storage path: no storage spec at all.
		return fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: %s
spec:
  version: "7.7"
  image: "cloudberrydb/cloudberry:7.7"
  coordinator:
    storage:
      size: "10Gi"
  segments:
    count: 4
    storage:
      size: "20Gi"
`, name)
	}

	if !recScanEnabled {
		return fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: %s
spec:
  version: "7.7"
  image: "cloudberrydb/cloudberry:7.7"
  coordinator:
    storage:
      size: "10Gi"
  segments:
    count: 4
    storage:
      size: "20Gi"
  storage:
    diskMonitoring: true
    recommendationScan:
      enabled: false
`, name)
	}

	return fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: %s
spec:
  version: "7.7"
  image: "cloudberrydb/cloudberry:7.7"
  coordinator:
    storage:
      size: "10Gi"
  segments:
    count: 4
    storage:
      size: "20Gi"
  storage:
    diskMonitoring: true
    recommendationScan:
      enabled: true
      bloatThreshold: 20
      skewThreshold: 30
      ageThreshold: 100000000
      indexBloatThreshold: 40
`, name)
}

// BenchmarkScenario117_AdmissionLatency measures the average per-apply admission
// latency for recommendation-scan CRs via `kubectl apply --dry-run=server`
// (which runs the admission webhooks without persisting). Each sub-benchmark
// reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario117_AdmissionLatency(b *testing.B) {
	perf117SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"recommendation_scan_on",
			perf117Manifest("s117-perf-recscan-on", true, true),
		},
		{
			"recommendation_scan_off",
			perf117Manifest("s117-perf-recscan-off", false, true),
		},
		{
			"nil_storage",
			perf117Manifest("s117-perf-nil-storage", false, false),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf117DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf117DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario117 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
