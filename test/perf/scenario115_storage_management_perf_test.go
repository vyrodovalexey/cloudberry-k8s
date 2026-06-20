//go:build e2e

// Scenario 115 recommendation-scan CronJob reconciliation admission-latency
// performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the reconciliation ADMISSION
// latency for the recommendation-scan CronJob builder (C.5). It applies
// CloudberryCluster CRs (with recommendationScan enabled + schedule, and with
// scan disabled) via `kubectl apply --dry-run=server` (server-side dry-run STILL
// runs the admission webhooks, so it measures the real round-trip WITHOUT
// persisting anything), a number of times, and reports the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario115_AdmissionLatency/full_scan: a CR with enabled scan,
//     schedule, and all five thresholds — the full CronJob build path.
//   - BenchmarkScenario115_AdmissionLatency/scan_disabled: a CR with
//     Enabled=false — the builder returns nil (GC path).
//   - BenchmarkScenario115_AdmissionLatency/nil_storage: a CR with no storage
//     spec — the builder short-circuits on nil.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no builder assertion (the CronJob-build proof is the unit tests in
// internal/builder/scenario115_recommendation_scan_builder_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the admission webhooks
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO115_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO115_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario115 -benchtime=10x ./test/perf/...
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
	envScenario115Live      = "SCENARIO115_LIVE"
	envScenario115Namespace = "SCENARIO115_NAMESPACE"

	perf115DefaultNamespace = "cloudberry-test"
	perf115ExecTimeout      = 60 * time.Second
)

// perf115Namespace resolves the deploy namespace (SCENARIO115_NAMESPACE > default).
func perf115Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario115Namespace)); v != "" {
		return v
	}
	return perf115DefaultNamespace
}

// perf115Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf115Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf115DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny — both exercise the webhook).
func perf115DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf115ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf115Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf115SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO115_LIVE=1, and the namespace + CRD are served.
func perf115SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 115 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 115 admission-latency perf")
	}
	if os.Getenv(envScenario115Live) != "1" {
		b.Skip("SCENARIO115_LIVE not set, skipping Scenario 115 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf115ExecTimeout)
	defer cancel()
	if out, err := perf115Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf115Manifest returns a CloudberryCluster manifest for the given scenario.
func perf115Manifest(name string, enabled bool, includeStorage bool) string {
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

	enabledStr := "false"
	if enabled {
		enabledStr = "true"
	}

	if enabled {
		// Full scan: enabled with schedule and all five thresholds.
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
      enabled: %s
      schedule: "0 3 * * 0"
      bloatThreshold: 20
      skewThreshold: 50
      indexBloatThreshold: 30
      ageThreshold: 500000000
      scanDuration: "2h"
`, name, enabledStr)
	}

	// Disabled scan.
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
      enabled: %s
`, name, enabledStr)
}

// BenchmarkScenario115_AdmissionLatency measures the average per-apply admission
// latency for recommendation-scan CRs via `kubectl apply --dry-run=server`
// (which runs the admission webhooks without persisting). Each sub-benchmark
// reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario115_AdmissionLatency(b *testing.B) {
	perf115SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"full_scan",
			perf115Manifest("s115-perf-full-scan", true, true),
		},
		{
			"scan_disabled",
			perf115Manifest("s115-perf-disabled", false, true),
		},
		{
			"nil_storage",
			perf115Manifest("s115-perf-nil-storage", false, false),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf115DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf115DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario115 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
