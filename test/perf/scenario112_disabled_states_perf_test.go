//go:build e2e

// Scenario 112 disabled-states performance benchmark (LIGHT).
//
// PERF-HARNESS NOTE: a LIGHT performance check of the DIS.1 cleanup object-GC
// PLANNING (a pure, infra-free benchmark over a fake client: seed the stale
// object set + run the label-scoped list/delete the teardown performs) plus a
// GATED live disable→teardown latency probe. It mirrors the Scenario 111 perf
// harness shape (build tag e2e, shared with the perf package):
//
//   - BenchmarkScenario112_CleanupGCPlanning (infra-free): benchmarks the
//     list-by-label + delete-each GC plan the disabled teardown runs, over a
//     fake client seeded with the full stale data-loading object set. Reports
//     ns/op + allocs/op. No env required.
//
//   - BenchmarkScenario112_DisableTeardownLatency (gated): measures the average
//     `kubectl get deploy <cluster>-gpfdist` round-trip latency against the
//     deployed cluster (a proxy for the teardown-observability path), reporting
//     avg_ms. Skips cleanly when KUBECONFIG/kubectl/the namespace are absent or
//     SCENARIO112_LIVE is unset. It makes NO assertion + mutates NOTHING (the
//     destructive disable→teardown latency is the e2e Part B's job).
//
//     KUBECONFIG=... SCENARIO112_LIVE=1 \
//     go test -tags=e2e -run=^$ -bench=BenchmarkScenario112 -benchtime=10x ./test/perf/...
package perf

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	envScenario112Live      = "SCENARIO112_LIVE"
	envScenario112Cluster   = "SCENARIO112_CLUSTER"
	envScenario112Namespace = "SCENARIO112_NAMESPACE"
	envScenario112Kubeconf  = "KUBECONFIG"

	perf112DefaultNamespace = "cloudberry-test"
	perf112DefaultCluster   = "s112"
	perf112ExecTimeout      = 60 * time.Second

	// avsoft.io label keys (the operator's teardown selector).
	perf112LabelCluster   = "avsoft.io/cluster"
	perf112LabelComponent = "avsoft.io/component"
	perf112ComponentDL    = "dataload"
)

func perf112Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envScenario112Namespace)); v != "" {
		return v
	}
	return perf112DefaultNamespace
}

func perf112Cluster() string {
	if v := strings.TrimSpace(os.Getenv(envScenario112Cluster)); v != "" {
		return v
	}
	return perf112DefaultCluster
}

// perf112Scheme returns a scheme with the teardown object kinds registered.
func perf112Scheme(b *testing.B) *runtime.Scheme {
	b.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, batchv1.AddToScheme, networkingv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			b.Fatalf("scheme add: %v", err)
		}
	}
	return scheme
}

// perf112StaleObjects returns the full stale data-loading object set the disabled
// teardown reclaims (the GC-planning benchmark input).
func perf112StaleObjects(cluster, ns string) []client.Object {
	labels := map[string]string{perf112LabelCluster: cluster, perf112LabelComponent: perf112ComponentDL}
	meta := func(name string, l map[string]string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: ns, Labels: l}
	}
	return []client.Object{
		&appsv1.Deployment{ObjectMeta: meta(cluster+"-gpfdist", nil)},
		&corev1.Service{ObjectMeta: meta(cluster+"-gpfdist-svc", nil)},
		&corev1.PersistentVolumeClaim{ObjectMeta: meta(cluster+"-gpfdist-data-pvc", nil)},
		&batchv1.Job{ObjectMeta: meta(cluster+"-dataload-loader", labels)},
		&batchv1.CronJob{ObjectMeta: meta(cluster+"-dataload-nightly", labels)},
		&corev1.ConfigMap{ObjectMeta: meta(cluster+"-gpload-loader", labels)},
		&networkingv1.NetworkPolicy{ObjectMeta: meta(cluster+"-pxf", nil)},
	}
}

// BenchmarkScenario112_CleanupGCPlanning benchmarks the DIS.1 cleanup GC PLAN: a
// list-by-{cluster,component=dataload} of Jobs/CronJobs/ConfigMaps + a delete of
// each, plus the by-name deletes of the gpfdist objects + the PXF NetworkPolicy.
// Infra-free; reports ns/op + allocs/op.
func BenchmarkScenario112_CleanupGCPlanning(b *testing.B) {
	scheme := perf112Scheme(b)
	cluster := perf112Cluster()
	ns := perf112Namespace()
	sel := client.MatchingLabels{perf112LabelCluster: cluster, perf112LabelComponent: perf112ComponentDL}
	ctx := context.Background()

	plan := func(c client.Client) {
		jobs := &batchv1.JobList{}
		_ = c.List(ctx, jobs, client.InNamespace(ns), sel)
		for i := range jobs.Items {
			_ = client.IgnoreNotFound(c.Delete(ctx, &jobs.Items[i]))
		}
		crons := &batchv1.CronJobList{}
		_ = c.List(ctx, crons, client.InNamespace(ns), sel)
		for i := range crons.Items {
			_ = client.IgnoreNotFound(c.Delete(ctx, &crons.Items[i]))
		}
		cms := &corev1.ConfigMapList{}
		_ = c.List(ctx, cms, client.InNamespace(ns), sel)
		for i := range cms.Items {
			_ = client.IgnoreNotFound(c.Delete(ctx, &cms.Items[i]))
		}
		for _, obj := range []client.Object{
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: cluster + "-gpfdist", Namespace: ns}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster + "-gpfdist-svc", Namespace: ns}},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: cluster + "-gpfdist-data-pvc", Namespace: ns}},
			&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: cluster + "-pxf", Namespace: ns}},
		} {
			_ = client.IgnoreNotFound(c.Delete(ctx, obj))
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(perf112StaleObjects(cluster, ns)...).Build()
		b.StartTimer()
		plan(c)
	}
}

// perf112SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is
// set, kubectl exists, SCENARIO112_LIVE=1, and the namespace is served.
func perf112SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envScenario112Kubeconf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 112 disable-teardown perf")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 112 disable-teardown perf")
	}
	if os.Getenv(envScenario112Live) != "1" {
		b.Skip("SCENARIO112_LIVE not set, skipping Scenario 112 disable-teardown perf")
	}
	ctx, cancel := context.WithTimeout(context.Background(), perf112ExecTimeout)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "kubectl", "get", "namespace",
		perf112Namespace()).CombinedOutput(); err != nil {
		b.Skipf("namespace %q not reachable, skipping perf [CONFIG-ONLY]: %s",
			perf112Namespace(), string(out))
	}
}

// BenchmarkScenario112_DisableTeardownLatency measures the average
// `kubectl get deploy <cluster>-gpfdist` round-trip latency against the deployed
// cluster (a proxy for the teardown-observability path), reporting avg_ms. Skips
// cleanly when the live env is absent. Mutates NOTHING + asserts NOTHING.
func BenchmarkScenario112_DisableTeardownLatency(b *testing.B) {
	perf112SkipUnlessLive(b)

	depName := perf112Cluster() + "-gpfdist"
	ns := perf112Namespace()

	get := func() {
		ctx, cancel := context.WithTimeout(context.Background(), perf112ExecTimeout)
		defer cancel()
		_, _ = exec.CommandContext(ctx, "kubectl", "get", "deployment", depName,
			"-n", ns, "-o", "name").CombinedOutput()
	}

	get() // warmup

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
	b.Logf("scenario112 gpfdist deploy get %s: avg=%.2fms over %d gets", depName, avgMs, b.N)
}
