//go:build e2e

// Scenario 111 security performance benchmark (LIGHT).
//
// PERF-HARNESS NOTE: a LIGHT performance check of the SE.5 cluster NetworkPolicy
// builder (a pure, infra-free benchmark) plus a GATED live netpol-reconcile
// latency probe. It mirrors the Scenario 110 perf harness shape:
//
//   - BenchmarkScenario111_BuildPXFClusterNetworkPolicy (infra-free): benchmarks
//     the pure builder that assembles the SE.5 cluster NetworkPolicy over a
//     multi-server PXF cluster, reporting ns/op + allocs. No env required.
//   - BenchmarkScenario111_NetpolReconcileLatency (gated): measures the average
//     `kubectl get networkpolicy <cluster>-pxf-netpol` round-trip latency against
//     the deployed cluster, reporting avg_ms. Skips cleanly when KUBECONFIG/
//     kubectl/the namespace are absent or SCENARIO111_LIVE is unset.
//
// HONESTY: the live probe reports latency only; it makes no security assertion
// (the SE.5 reject/no-break proof is the e2e Part B). Build tag e2e (shared with
// the perf package).
//
//	KUBECONFIG=... SCENARIO111_LIVE=1 \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario111 -benchtime=10x ./test/perf/...
package perf

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
)

const (
	envScenario111Live      = "SCENARIO111_LIVE"
	envScenario111Cluster   = "SCENARIO111_CLUSTER"
	envScenario111Namespace = "SCENARIO111_NAMESPACE"
	envScenario111Kubeconf  = "KUBECONFIG"

	perf111DefaultNamespace = "cloudberry-test"
	perf111DefaultCluster   = "s111"
	perf111ExecTimeout      = 60 * time.Second
)

func perf111Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario111Namespace)); v != "" {
		return v
	}
	return perf111DefaultNamespace
}

func perf111Cluster() string {
	if v := strings.TrimSpace(os.Getenv(envScenario111Cluster)); v != "" {
		return v
	}
	return perf111DefaultCluster
}

// perf111PXFCluster builds a PXF-enabled multi-server cluster used by the pure
// NetworkPolicy builder benchmark.
func perf111PXFCluster() *cbv1alpha1.CloudberryCluster {
	cluster := &cbv1alpha1.CloudberryCluster{}
	cluster.Name = "s111-perf"
	cluster.Namespace = perf111DefaultNamespace
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Port:    5888,
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3-datalake", Type: "s3", Config: map[string]string{
					"fs.s3a.endpoint": "http://minio:9000",
				}},
				{Name: "pg-src", Type: "jdbc", Config: map[string]string{
					"jdbc.driver": "org.postgresql.Driver",
					"jdbc.url":    "jdbc:postgresql://pg:5432/src",
				}},
			},
		},
	}
	return cluster
}

// BenchmarkScenario111_BuildPXFClusterNetworkPolicy benchmarks the pure SE.5
// cluster NetworkPolicy builder (infra-free). It reports ns/op + allocs/op.
func BenchmarkScenario111_BuildPXFClusterNetworkPolicy(b *testing.B) {
	bld := builder.NewBuilder()
	cluster := perf111PXFCluster()

	// Warmup + sanity: the builder must yield a non-nil policy.
	if np := bld.BuildPXFClusterNetworkPolicy(cluster); np == nil {
		b.Fatal("expected a non-nil NetworkPolicy for a PXF cluster")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bld.BuildPXFClusterNetworkPolicy(cluster)
	}
}

// perf111SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is
// set, kubectl exists, SCENARIO111_LIVE=1, and the namespace is served.
func perf111SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario111Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 111 netpol-reconcile perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 111 netpol-reconcile perf")
	}
	if os.Getenv(envScenario111Live) != "1" {
		b.Skip("SCENARIO111_LIVE not set, skipping Scenario 111 netpol-reconcile perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf111ExecTimeout)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "kubectl", "get", "namespace",
		perf111Namespace()).CombinedOutput(); err != nil {
		b.Skipf("namespace %q not reachable, skipping perf [CONFIG-ONLY]: %s",
			perf111Namespace(), string(out))
	}
}

// BenchmarkScenario111_NetpolReconcileLatency measures the average
// `kubectl get networkpolicy <cluster>-pxf-netpol` round-trip latency against the
// deployed cluster, reporting avg_ms. Skips cleanly when the live env is absent.
func BenchmarkScenario111_NetpolReconcileLatency(b *testing.B) {
	perf111SkipUnlessLive(b)

	npName := perf111Cluster() + "-pxf-netpol"
	ns := perf111Namespace()

	get := func() {
		ctx, cancel := context.WithTimeout(context.Background(), perf111ExecTimeout)
		defer cancel()
		_, _ = exec.CommandContext(ctx, "kubectl", "get", "networkpolicy", npName,
			"-n", ns, "-o", "name").CombinedOutput()
	}

	// Warmup: prime the connection.
	get()

	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		get()
		total += time.Since(start)
	}
	b.StopTimer()

	if b.N == 0 {
		b.Skip("no iterations")
	}
	avgMs := float64(total.Microseconds()) / float64(b.N) / 1000.0
	b.ReportMetric(avgMs, "avg_ms")
	b.Logf("scenario111 netpol get %s: avg=%.2fms over %d gets", npName, avgMs, b.N)
}
