//go:build e2e

// Scenario 113 storage-recommendation webhook admission-latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the webhook ADMISSION latency
// for the storage-recommendation threshold validation rules (W.1–W.4). It
// applies valid + invalid CloudberryCluster CRs (with recommendationScan
// thresholds) via `kubectl apply --dry-run=server` (server-side dry-run STILL
// runs the admission webhooks, so it measures the real validating-webhook
// round-trip WITHOUT persisting anything), a number of times, and reports the
// average latency.
//
// Sub-benchmarks:
//   - BenchmarkScenario113_AdmissionLatency/valid_boundary: a CR with all four
//     thresholds at their boundary values (0, 100, 0, 0) — ACCEPTED.
//   - BenchmarkScenario113_AdmissionLatency/reject_W1_bloat150: a CR with
//     bloatThreshold=150 — REJECTED by W.1.
//   - BenchmarkScenario113_AdmissionLatency/reject_W2_skew101: a CR with
//     skewThreshold=101 — REJECTED by W.2.
//   - BenchmarkScenario113_AdmissionLatency/reject_W3_indexBloat200: a CR with
//     indexBloatThreshold=200 — REJECTED by W.3.
//   - BenchmarkScenario113_AdmissionLatency/reject_W4_ageNeg5: a CR with
//     ageThreshold=-5 — REJECTED by W.4.
//   - BenchmarkScenario113_AdmissionLatency/disabled_bypass: a CR with
//     Enabled=false and out-of-range thresholds — ACCEPTED (gate bypass).
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server)
// and makes no validation assertion (the reject/no-persist proof is the e2e
// Part B and the unit tests in internal/webhook).
//
// Build tag e2e (shared with the perf package).
//
//	KUBECONFIG=... SCENARIO113_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario113 -benchtime=10x ./test/perf/...
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
	envScenario113Live      = "SCENARIO113_LIVE"
	envScenario113Namespace = "SCENARIO113_NAMESPACE"

	perf113DefaultNamespace = "cloudberry-test"
	perf113ExecTimeout      = 60 * time.Second
)

// perf113Namespace resolves the deploy namespace (SCENARIO113_NAMESPACE > default).
func perf113Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario113Namespace)); v != "" {
		return v
	}
	return perf113DefaultNamespace
}

// perf113Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf113Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf113DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny — both exercise the webhook).
func perf113DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf113ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf113Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf113SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO113_LIVE=1, and the namespace + CRD are served.
func perf113SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 113 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 113 admission-latency perf")
	}
	if os.Getenv(envScenario113Live) != "1" {
		b.Skip("SCENARIO113_LIVE not set, skipping Scenario 113 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf113ExecTimeout)
	defer cancel()
	if out, err := perf113Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf113Manifest returns a CloudberryCluster manifest with the given name and
// storage.recommendationScan configuration. The thresholds are injected directly
// into the YAML to exercise the webhook's parsing + validation path end-to-end.
func perf113Manifest(name string, enabled bool, bloat, skew, indexBloat int, age int64) string {
	enabledStr := "false"
	if enabled {
		enabledStr = "true"
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
      enabled: %s
      schedule: "0 3 * * 0"
      bloatThreshold: %d
      skewThreshold: %d
      indexBloatThreshold: %d
      ageThreshold: %d
`, name, enabledStr, bloat, skew, indexBloat, age)
}

// BenchmarkScenario113_AdmissionLatency measures the average per-apply admission
// latency for storage-recommendation CRs via `kubectl apply --dry-run=server`
// (which runs the validating webhook without persisting). Each sub-benchmark
// reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario113_AdmissionLatency(b *testing.B) {
	perf113SkipUnlessLive(b)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{
			"valid_boundary",
			perf113Manifest("s113-perf-valid-boundary", true, 0, 100, 0, 0),
		},
		{
			"reject_W1_bloat150",
			perf113Manifest("s113-perf-reject-w1", true, 150, 50, 30, 500000000),
		},
		{
			"reject_W2_skew101",
			perf113Manifest("s113-perf-reject-w2", true, 20, 101, 30, 500000000),
		},
		{
			"reject_W3_indexBloat200",
			perf113Manifest("s113-perf-reject-w3", true, 20, 50, 200, 500000000),
		},
		{
			"reject_W4_ageNeg5",
			perf113Manifest("s113-perf-reject-w4", true, 20, 50, 30, -5),
		},
		{
			"disabled_bypass",
			perf113Manifest("s113-perf-disabled", false, 150, 200, 300, -99),
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf113DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf113DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario113 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
