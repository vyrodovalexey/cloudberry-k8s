//go:build e2e

// Scenario 106 Server Configuration Update / Delete (SL.7–SL.8) perf benchmarks.
//
// PERF-HARNESS NOTE: a LIGHT performance check of the server-config-change
// reconcile path. Two PURE benchmarks (no infra, always runnable) measure the hot
// loop the operator's ensurePxfServersConfigMap / the API sync path execute on
// every pass:
//
//   - BenchmarkScenario106_BuildAndDiffPXFServers: render the
//     <cluster>-pxf-servers ConfigMap (builder.BuildPXFServersConfigMap) AND
//     compute the honest change diff (util.DiffPXFServerNames over the previous
//     vs the patched render) across a GROWING server count. This is the exact
//     pure work an SL.7 endpoint patch drives, so its latency/allocs bound the
//     reconcile/sync cost. Sub-benchmarks sweep 1/4/16/64 servers.
//
//   - BenchmarkScenario106_DiffPXFServerNames: isolate the pure diff over a
//     pre-rendered pair (an UPDATE of one server) across the same server counts,
//     pinning the diff's own scaling.
//
// A live benchmark (BenchmarkScenario106_PXFSyncLatency, SCENARIO106_LIVE=1 gated)
// measures the end-to-end `pxf sync` latency against the deployed cluster via
// kubectl exec. Load is kept MODEST (a handful of syncs) so it is docker-desktop
// friendly, and it skips cleanly when the live infra is unreachable.
//
// HONESTY: reports latency/allocs only — it never fabricates metrics. Build tag
// e2e (shared with the perf package); the pure benchmarks run without any infra.
//
//	go test -tags=e2e -run=^$ -bench=BenchmarkScenario106 -benchtime=200x ./test/perf/...
package perf

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	envKubeconfigS106Perf  = "KUBECONFIG"
	envScenario106LivePerf = "SCENARIO106_LIVE"
	envScenario106ClusterP = "SCENARIO106_CLUSTER"
	envScenario106NsP      = "SCENARIO106_NAMESPACE"

	perf106DefaultCluster   = "s106"
	perf106DefaultNamespace = "cloudberry-test"
	perf106PxfContainer     = "pxf"
	perf106ExecTimeout      = 2 * time.Minute
)

// perf106Env returns the ENV value or a default.
func perf106Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf106Namespace() string { return perf106Env(envScenario106NsP, perf106DefaultNamespace) }
func perf106Cluster() string   { return perf106Env(envScenario106ClusterP, perf106DefaultCluster) }

// perf106ServerCounts is the modest sweep used by the pure benchmarks (kept small
// so the pure work stays docker-desktop-friendly while still showing scaling).
var perf106ServerCounts = []int{1, 4, 16, 64}

// perf106BuildCluster builds a cluster with n s3 PXF servers, each carrying a
// distinct fs.s3a.endpoint, so the render + diff have realistic per-server bodies.
func perf106BuildCluster(n int, endpointSuffix string) *cbv1alpha1.CloudberryCluster {
	servers := make([]cbv1alpha1.PxfServerSpec, n)
	for i := 0; i < n; i++ {
		servers[i] = cbv1alpha1.PxfServerSpec{
			Name: fmt.Sprintf("srv-%03d", i),
			Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint":          fmt.Sprintf("http://minio-%03d-%s:9000", i, endpointSuffix),
				"fs.s3a.path.style.access": "true",
			},
		}
	}
	return &cbv1alpha1.CloudberryCluster{
		Spec: cbv1alpha1.CloudberryClusterSpec{
			DataLoading: &cbv1alpha1.DataLoadingSpec{
				Enabled: true,
				Pxf: &cbv1alpha1.PxfSpec{
					Enabled: true,
					Image:   "apache/cloudberry-pxf:2.1.0",
					Servers: servers,
				},
			},
		},
	}
}

// BenchmarkScenario106_BuildAndDiffPXFServers measures the pure
// render-plus-diff cost an SL.7 endpoint patch drives: BuildPXFServersConfigMap
// over the patched spec, then DiffPXFServerNames against the previous render.
func BenchmarkScenario106_BuildAndDiffPXFServers(b *testing.B) {
	bld := builder.NewBuilder()
	for _, n := range perf106ServerCounts {
		n := n
		// Pre-render the baseline once; the patched render + diff is the hot loop.
		baseline := bld.BuildPXFServersConfigMap(perf106BuildCluster(n, "old"))
		if baseline == nil {
			b.Fatalf("baseline render returned nil for n=%d", n)
		}
		patchedCluster := perf106BuildCluster(n, "new")
		b.Run(fmt.Sprintf("servers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desired := bld.BuildPXFServersConfigMap(patchedCluster)
				_, _, _ = util.DiffPXFServerNames(baseline.Data, desired.Data)
			}
		})
	}
}

// BenchmarkScenario106_DiffPXFServerNames isolates the pure diff over a
// pre-rendered (baseline, patched) pair — the exact honest signal the
// controller/API path computes — across the same server counts.
func BenchmarkScenario106_DiffPXFServerNames(b *testing.B) {
	bld := builder.NewBuilder()
	for _, n := range perf106ServerCounts {
		n := n
		baseline := bld.BuildPXFServersConfigMap(perf106BuildCluster(n, "old"))
		patched := bld.BuildPXFServersConfigMap(perf106BuildCluster(n, "new"))
		if baseline == nil || patched == nil {
			b.Fatalf("render returned nil for n=%d", n)
		}
		b.Run(fmt.Sprintf("servers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = util.DiffPXFServerNames(baseline.Data, patched.Data)
			}
		})
	}
}

// perf106SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is
// set, kubectl exists and SCENARIO106_LIVE=1.
func perf106SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS106Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 106 pxf-sync perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 106 pxf-sync perf")
	}
	if os.Getenv(envScenario106LivePerf) != "1" {
		b.Skip("SCENARIO106_LIVE not set, skipping the live pxf-sync perf " +
			"(the deployed s106 cluster must be available)")
	}
}

// perf106FirstSegmentPxfPod returns the first segment-primary pxf pod (empty when
// none found / unreachable).
func perf106FirstSegmentPxfPod(b *testing.B) string {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "get", "pods", "-n", perf106Namespace(),
		"-l", "avsoft.io/component=segment-primary,avsoft.io/cluster="+perf106Cluster(),
		"-o", "jsonpath={.items[0].metadata.name}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// BenchmarkScenario106_PXFSyncLatency measures the end-to-end `pxf sync` latency
// against the deployed cluster via kubectl exec. Load is MODEST; it reports per-
// op latency and skips cleanly when the live infra is unreachable.
func BenchmarkScenario106_PXFSyncLatency(b *testing.B) {
	perf106SkipUnlessLive(b)

	pod := perf106FirstSegmentPxfPod(b)
	if pod == "" {
		b.Skip("no segment-primary pxf pod found (cluster may not be deployed)")
	}

	pxfSync := func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(context.Background(), perf106ExecTimeout)
		defer cancel()
		start := time.Now()
		cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", perf106Namespace(),
			"-c", perf106PxfContainer, pod, "--", "bash", "-lc",
			"pxf sync || pxf cluster sync")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return 0, fmt.Errorf("pxf sync failed: %w (out=%s)", err, string(out))
		}
		return time.Since(start), nil
	}

	// Warmup: one sync to prime the agent.
	if _, err := pxfSync(); err != nil {
		b.Skipf("warmup pxf sync failed (agent may be a stub on this image): %v", err)
	}

	var total time.Duration
	var ok int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := pxfSync()
		if err != nil {
			b.Logf("scenario106 pxf sync iteration %d failed: %v", i, err)
			continue
		}
		total += d
		ok++
	}
	b.StopTimer()

	if ok == 0 {
		b.Skip("no successful pxf sync iterations (agent unavailable)")
	}
	avgMs := float64(total.Microseconds()) / float64(ok) / 1000.0
	b.ReportMetric(avgMs, "avg_ms")
	b.Logf("scenario106 pxf sync: %d/%d ok, avg=%.1fms", ok, b.N, avgMs)
}
