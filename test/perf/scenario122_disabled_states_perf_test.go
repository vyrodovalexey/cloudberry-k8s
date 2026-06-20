//go:build e2e

// Scenario 122 disabled-states clear-on-disable admission-latency performance
// benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the reconciliation ADMISSION
// latency for the clear-on-disable paths (clearRecommendations +
// clearStorageSignals helpers in reconcileStorage + refreshStorageOnSteadyState).
// It applies CloudberryCluster CRs (with storage disabled / scan disabled /
// storage nil) via `kubectl apply --dry-run=server` (server-side dry-run STILL
// runs the admission webhooks, so it measures the real round-trip WITHOUT
// persisting anything), a number of times, and reports the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario122_AdmissionLatency/disk_monitoring_off: a CR with
//     diskMonitoring:false (R.1 early-return + clearStorageSignals).
//   - BenchmarkScenario122_AdmissionLatency/scan_disabled: a CR with
//     diskMonitoring:true but no recommendationScan (clearRecommendations
//     else-branch).
//   - BenchmarkScenario122_AdmissionLatency/nil_storage: a CR with no storage
//     spec at all (control path).
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no DB assertion (the clear-on-disable proof is the unit tests in
// internal/controller/scenario122_disabled_states_test.go and the benchmarks in
// internal/controller/scenario122_benchmark_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the admission webhooks
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO122_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO122_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario122 -benchtime=10x ./test/perf/...
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
	envScenario122Live      = "SCENARIO122_LIVE"
	envScenario122Namespace = "SCENARIO122_NAMESPACE"

	perf122DefaultNamespace = "cloudberry-test"
	perf122ExecTimeout      = 60 * time.Second
)

// perf122Namespace resolves the deploy namespace (SCENARIO122_NAMESPACE > default).
func perf122Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario122Namespace)); v != "" {
		return v
	}
	return perf122DefaultNamespace
}

// perf122Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf122Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf122DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny -- both exercise the webhook).
func perf122DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf122ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf122Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf122SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO122_LIVE=1, and the namespace + CRD are served.
func perf122SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 122 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 122 admission-latency perf")
	}
	if os.Getenv(envScenario122Live) != "1" {
		b.Skip("SCENARIO122_LIVE not set, skipping Scenario 122 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf122ExecTimeout)
	defer cancel()
	if out, err := perf122Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf122Manifest returns a CloudberryCluster manifest for the given scenario.
func perf122Manifest(name string, storageMode string) string {
	switch storageMode {
	case "disk_monitoring_off":
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
    diskMonitoring: false
`, name)

	case "scan_disabled":
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
`, name)

	default: // nil_storage
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
}

// BenchmarkScenario122_AdmissionLatency measures the average per-apply admission
// latency for disabled-state CRs via `kubectl apply --dry-run=server` (which runs
// the admission webhooks without persisting). Each sub-benchmark reports avg_ms.
// Skips cleanly when the live env is absent.
func BenchmarkScenario122_AdmissionLatency(b *testing.B) {
	perf122SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"disk_monitoring_off",
			perf122Manifest("s122-perf-dm-off", "disk_monitoring_off"),
		},
		{
			"scan_disabled",
			perf122Manifest("s122-perf-scan-off", "scan_disabled"),
		},
		{
			"nil_storage",
			perf122Manifest("s122-perf-nil-storage", "nil_storage"),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf122DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf122DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario122 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
