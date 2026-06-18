//go:build e2e

// Scenario 110 webhook admission-latency performance benchmark.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the webhook ADMISSION latency.
// It applies a valid + an invalid CloudberryCluster with `kubectl apply
// --dry-run=server` (server-side dry-run STILL runs the admission webhooks, so it
// measures the real validating-webhook round-trip WITHOUT persisting anything),
// a number of times, and reports the average latency. It mirrors the Scenario 109
// perf harness shape and is GATED behind the live env:
//
//   - BenchmarkScenario110_AdmissionLatency (gated): for a valid CR and a
//     representative invalid CR, runs `kubectl apply --dry-run=server` b.N times
//     and reports avg_ms. Skips cleanly when KUBECONFIG/kubectl/the namespace/CRD
//     are absent or SCENARIO110_LIVE is unset.
//
// HONESTY: it reports latency only; it never persists a CR (--dry-run=server) and
// makes no validation assertion (the reject/no-persist proof is the e2e Part B).
// Build tag e2e (shared with the perf package).
//
//	KUBECONFIG=... SCENARIO110_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario110 -benchtime=10x ./test/perf/...
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
	envScenario110Live      = "SCENARIO110_LIVE"
	envScenario110Namespace = "SCENARIO110_NAMESPACE"
	envScenario110Kubeconf  = "KUBECONFIG"

	perf110DefaultNamespace = "cloudberry-test"
	perf110ExecTimeout      = 60 * time.Second
)

// perf110Namespace resolves the deploy namespace (SCENARIO110_NAMESPACE > default).
func perf110Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario110Namespace)); v != "" {
		return v
	}
	return perf110DefaultNamespace
}

// perf110Kubectl runs a kubectl subcommand bounded by a short timeout.
func perf110Kubectl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// perf110DryRunApply pipes a manifest to `kubectl apply --dry-run=server` (which
// runs the admission webhooks without persisting) and returns whether the call
// completed (regardless of accept/deny — both exercise the webhook).
func perf110DryRunApply(manifest string) {
	ctx, cancel := context.WithTimeout(context.Background(), perf110ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply",
		"-n", perf110Namespace(), "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, _ = cmd.CombinedOutput()
}

// perf110SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists, SCENARIO110_LIVE=1, and the namespace + CRD are served.
func perf110SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario110Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 110 admission-latency perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 110 admission-latency perf")
	}
	if os.Getenv(envScenario110Live) != "1" {
		b.Skip("SCENARIO110_LIVE not set, skipping Scenario 110 admission-latency perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf110ExecTimeout)
	defer cancel()
	if out, err := perf110Kubectl(ctx, "get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		b.Skipf("CloudberryCluster CRD not served, skipping perf [CONFIG-ONLY]: %s", out)
	}
}

// perf110ValidManifest returns a base-valid CR manifest with the given name.
func perf110ValidManifest(name string) string {
	return fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: %s
spec:
  version: "1.6.0"
  image: "cloudberrydb/cloudberry:1.6.0"
  coordinator:
    replicas: 1
    storage:
      size: "10Gi"
  segments:
    count: 2
    primariesPerHost: 1
    storage:
      size: "10Gi"
    mirroring:
      enabled: true
      layout: spread
  dataLoading:
    enabled: true
    pxf:
      enabled: true
      image: "cloudberry-pxf:7.1.0"
      servers:
        - name: s3-datalake
          type: s3
          config:
            fs.s3a.endpoint: "http://minio:9000"
          credentialSecrets:
            - name: s3-creds
    jobs:
      - name: s3-csv-loader
        type: pxf
        enabled: true
        pxfJob:
          server: s3-datalake
          profile: "s3:text"
          targetTable: "public.events"
`, name)
}

// BenchmarkScenario110_AdmissionLatency measures the average per-apply admission
// latency of a valid + a representative invalid CR via `kubectl apply
// --dry-run=server` (which runs the validating webhook without persisting). Each
// sub-benchmark reports avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario110_AdmissionLatency(b *testing.B) {
	perf110SkipUnlessLive(b)

	valid := perf110ValidManifest("s110-perf-valid")
	// A representative WEBHOOK-rejected invalid CR (empty pxf.image, W.1).
	invalid := strings.Replace(perf110ValidManifest("s110-perf-invalid"),
		`image: "cloudberry-pxf:7.1.0"`, `image: ""`, 1)

	scenarios := []struct {
		name     string
		manifest string
	}{
		{"valid", valid},
		{"invalid_W1", invalid},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			// Warmup: one dry-run apply to prime the connection + webhook.
			perf110DryRunApply(sc.manifest)

			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				perf110DryRunApply(sc.manifest)
				total += time.Since(start)
			}
			b.StopTimer()

			if b.N == 0 {
				b.Skip("no iterations")
			}
			avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
			b.ReportMetric(avgMs, "avg_ms")
			b.Logf("scenario110 admission %s: avg=%.2fms over %d applies", sc.name, avgMs, b.N)
		})
	}
}
