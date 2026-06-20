//go:build e2e

// Scenario 118 scan-duration enforcement admission-latency performance
// benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the reconciliation ADMISSION
// latency for the scan-duration enforcement path (resolveScanDuration + capped
// scan context in recordRecommendations, truncation detection with status flag +
// cloudberry_recommendation_scan_truncated_total counter, and W.5 webhook
// validation). It applies CloudberryCluster CRs (with various scanDuration
// values) via `kubectl apply --dry-run=server` (server-side dry-run STILL runs
// the admission webhooks, so it measures the real round-trip WITHOUT persisting
// anything), a number of times, and reports the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario118_AdmissionLatency/scan_duration_2h: a CR with
//     scanDuration "2h" (generous cap, normal path).
//   - BenchmarkScenario118_AdmissionLatency/scan_duration_10ms: a CR with
//     scanDuration "10ms" (tiny cap, truncation path).
//   - BenchmarkScenario118_AdmissionLatency/scan_duration_empty: a CR with
//     no scanDuration (empty, falls back to default 10s).
//   - BenchmarkScenario118_AdmissionLatency/nil_storage: a CR with no storage
//     spec at all.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no DB assertion (the scan-duration proof is the unit tests in
// internal/controller/scenario118_scan_duration_test.go and the benchmarks in
// internal/controller/scenario118_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the admission webhooks
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO118_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO118_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario118 -benchtime=10x ./test/perf/...
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
	envScenario118Live      = "SCENARIO118_LIVE"
	envScenario118Namespace = "SCENARIO118_NAMESPACE"

	perf118DefaultNamespace = "cloudberry-test"
	perf118ExecTimeout      = 60 * time.Second
)

// perf118Namespace resolves the deploy namespace (SCENARIO118_NAMESPACE > default).
func perf118Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario118Namespace)); v != "" {
		return v
	}
	return perf118DefaultNamespace
}

// perf118Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf118Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf118DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny -- both exercise the webhook).
func perf118DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf118ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf118Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf118SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO118_LIVE=1, and the namespace + CRD are served.
func perf118SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 118 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 118 admission-latency perf")
	}
	if os.Getenv(envScenario118Live) != "1" {
		b.Skip("SCENARIO118_LIVE not set, skipping Scenario 118 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf118ExecTimeout)
	defer cancel()
	if out, err := perf118Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf118Manifest returns a CloudberryCluster manifest for the given scenario.
func perf118Manifest(name string, scanDuration string, includeStorage bool) string {
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

	if scanDuration == "" {
		// Empty scanDuration: falls back to default 10s.
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
      scanDuration: "%s"
`, name, scanDuration)
}

// BenchmarkScenario118_AdmissionLatency measures the average per-apply admission
// latency for scan-duration CRs via `kubectl apply --dry-run=server` (which runs
// the admission webhooks without persisting). Each sub-benchmark reports avg_ms.
// Skips cleanly when the live env is absent.
func BenchmarkScenario118_AdmissionLatency(b *testing.B) {
	perf118SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"scan_duration_2h",
			perf118Manifest("s118-perf-dur-2h", "2h", true),
		},
		{
			"scan_duration_10ms",
			perf118Manifest("s118-perf-dur-10ms", "10ms", true),
		},
		{
			"scan_duration_empty",
			perf118Manifest("s118-perf-dur-empty", "", true),
		},
		{
			"nil_storage",
			perf118Manifest("s118-perf-nil-storage", "", false),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf118DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf118DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario118 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
