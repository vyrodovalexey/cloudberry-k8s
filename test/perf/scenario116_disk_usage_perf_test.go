//go:build e2e

// Scenario 116 disk-usage measurement admission-latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the reconciliation ADMISSION
// latency for the disk-usage measurement path (recordDiskUsage, wired into
// reconcileStorage + refreshStorageOnSteadyState). It applies CloudberryCluster
// CRs (with diskMonitoring enabled and disabled) via `kubectl apply
// --dry-run=server` (server-side dry-run STILL runs the admission webhooks, so
// it measures the real round-trip WITHOUT persisting anything), a number of
// times, and reports the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario116_AdmissionLatency/disk_monitoring_on: a CR with
//     diskMonitoring=true — the full measurement path.
//   - BenchmarkScenario116_AdmissionLatency/disk_monitoring_off: a CR with
//     diskMonitoring=false — the early-return path.
//   - BenchmarkScenario116_AdmissionLatency/nil_storage: a CR with no storage
//     spec — the builder short-circuits on nil.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no DB assertion (the disk-usage proof is the unit tests in
// internal/controller/scenario116_disk_usage_test.go and the benchmarks in
// internal/controller/scenario116_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the admission webhooks
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO116_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO116_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario116 -benchtime=10x ./test/perf/...
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
	envScenario116Live      = "SCENARIO116_LIVE"
	envScenario116Namespace = "SCENARIO116_NAMESPACE"

	perf116DefaultNamespace = "cloudberry-test"
	perf116ExecTimeout      = 60 * time.Second
)

// perf116Namespace resolves the deploy namespace (SCENARIO116_NAMESPACE > default).
func perf116Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario116Namespace)); v != "" {
		return v
	}
	return perf116DefaultNamespace
}

// perf116Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf116Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf116DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny — both exercise the webhook).
func perf116DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf116ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf116Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf116SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO116_LIVE=1, and the namespace + CRD are served.
func perf116SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 116 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 116 admission-latency perf")
	}
	if os.Getenv(envScenario116Live) != "1" {
		b.Skip("SCENARIO116_LIVE not set, skipping Scenario 116 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf116ExecTimeout)
	defer cancel()
	if out, err := perf116Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf116Manifest returns a CloudberryCluster manifest for the given scenario.
func perf116Manifest(name string, diskMonitoring bool, includeStorage bool) string {
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

	monitoringStr := "false"
	if diskMonitoring {
		monitoringStr = "true"
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
    diskMonitoring: %s
`, name, monitoringStr)
}

// BenchmarkScenario116_AdmissionLatency measures the average per-apply admission
// latency for disk-usage CRs via `kubectl apply --dry-run=server` (which runs
// the admission webhooks without persisting). Each sub-benchmark reports avg_ms.
// Skips cleanly when the live env is absent.
func BenchmarkScenario116_AdmissionLatency(b *testing.B) {
	perf116SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"disk_monitoring_on",
			perf116Manifest("s116-perf-monitoring-on", true, true),
		},
		{
			"disk_monitoring_off",
			perf116Manifest("s116-perf-monitoring-off", false, true),
		},
		{
			"nil_storage",
			perf116Manifest("s116-perf-nil-storage", false, false),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf116DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf116DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario116 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
