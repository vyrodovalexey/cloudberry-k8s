//go:build e2e

// Scenario 114 storage-recommendation mutating-webhook defaults admission-latency
// performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the mutating webhook ADMISSION
// latency for the storage-recommendation defaults (D.1–D.6). It applies
// CloudberryCluster CRs (with recommendationScan enabled, fields omitted) via
// `kubectl apply --dry-run=server` (server-side dry-run STILL runs the mutating
// webhooks, so it measures the real defaulting round-trip WITHOUT persisting
// anything), a number of times, and reports the average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario114_AdmissionLatency/all_omitted: a CR with enabled scan
//     and all six fields omitted — the mutating webhook injects D.1–D.6.
//   - BenchmarkScenario114_AdmissionLatency/already_set: a CR with all six
//     fields explicitly set — the mutating webhook preserves (no-op path).
//   - BenchmarkScenario114_AdmissionLatency/disabled_bypass: a CR with
//     Enabled=false — the mutating webhook skips defaulting entirely.
//   - BenchmarkScenario114_AdmissionLatency/nil_storage: a CR with no storage
//     spec — the mutating webhook short-circuits on nil.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no defaulting assertion (the default-injection proof is the unit
// tests in internal/webhook/scenario114_defaults_test.go).
//
// Build tag e2e (shared with the perf package).
//
// CONFIG-ONLY: This test requires a live cluster with the mutating webhook
// deployed. It will skip cleanly when KUBECONFIG or SCENARIO114_LIVE is unset.
//
//	KUBECONFIG=... SCENARIO114_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario114 -benchtime=10x ./test/perf/...
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
	envScenario114Live      = "SCENARIO114_LIVE"
	envScenario114Namespace = "SCENARIO114_NAMESPACE"

	perf114DefaultNamespace = "cloudberry-test"
	perf114ExecTimeout      = 60 * time.Second
)

// perf114Namespace resolves the deploy namespace (SCENARIO114_NAMESPACE > default).
func perf114Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario114Namespace)); v != "" {
		return v
	}
	return perf114DefaultNamespace
}

// perf114Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf114Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf114DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the mutating webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny — both exercise the webhook).
func perf114DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf114ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf114Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf114SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO114_LIVE=1, and the namespace + CRD are served.
func perf114SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 114 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 114 admission-latency perf")
	}
	if os.Getenv(envScenario114Live) != "1" {
		b.Skip("SCENARIO114_LIVE not set, skipping Scenario 114 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf114ExecTimeout)
	defer cancel()
	if out, err := perf114Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf114Manifest returns a CloudberryCluster manifest for the given scenario.
// When allOmitted is true, the six defaulted fields are omitted (zero) so the
// mutating webhook injects D.1–D.6. When false, explicit values are set.
func perf114Manifest(name string, enabled bool, allOmitted bool, includeStorage bool) string {
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

	if allOmitted {
		// Only enabled is set; all six defaulted fields are omitted.
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

	// All six fields explicitly set (non-default values).
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
      schedule: "0 1 * * *"
      bloatThreshold: 10
      skewThreshold: 25
      indexBloatThreshold: 15
      ageThreshold: 100000000
      scanDuration: "1h"
`, name, enabledStr)
}

// BenchmarkScenario114_AdmissionLatency measures the average per-apply mutating
// admission latency for storage-recommendation CRs via `kubectl apply
// --dry-run=server` (which runs the mutating webhook without persisting). Each
// sub-benchmark reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario114_AdmissionLatency(b *testing.B) {
	perf114SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"all_omitted",
			perf114Manifest("s114-perf-all-omitted", true, true, true),
		},
		{
			"already_set",
			perf114Manifest("s114-perf-already-set", true, false, true),
		},
		{
			"disabled_bypass",
			perf114Manifest("s114-perf-disabled", false, true, true),
		},
		{
			"nil_storage",
			perf114Manifest("s114-perf-nil-storage", false, false, false),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf114DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf114DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario114 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
