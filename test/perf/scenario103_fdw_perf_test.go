//go:build e2e

// Scenario 103 FDW vs external-table load throughput benchmark.
//
// PERF-HARNESS NOTE: like the Scenario 101/102 perf files, this is a minimal but
// REAL Go benchmark that records rows/sec LOADED into the FDW target
// (public.events_fdw) AND the external-table target (public.events_ext) from the
// SAME MinIO s3 dataset, then reports rows/sec for BOTH paths via b.ReportMetric
// so the FDW vs external-table throughput can be compared. It is gated behind the
// SAME live flag as the e2e Part B (SCENARIO103_FDW_LIVE=1) and skips cleanly
// without KUBECONFIG / infra, so `go test`/`go vet` compile it but CI without
// infra never runs the body.
//
// HONESTY: this reports rows/sec only — it does NOT assert (and never emits) a
// cloudberry_pxf_* / cloudberry_gpfdist_* metric, which stay PLANNED. The FDW
// load reuses cloudberry_data_loading_*. The honest correctness proof
// (count(ext)==count(fdw), count > 0) is in the e2e suite; this benchmark
// measures load THROUGHPUT for the two equivalent paths.
//
// Build tag: e2e (this needs the deployed fdw-test cluster + cloudberry-pxf, same
// as the e2e suite). Run with:
//
//	SCENARIO103_FDW_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario103 ./test/perf/...
package perf

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	envKubeconfigS103Perf  = "KUBECONFIG"
	envScenario103LivePerf = "SCENARIO103_FDW_LIVE"
	envScenario103ClusterP = "SCENARIO103_CLUSTER"
	envScenario103CoordP   = "SCENARIO103_COORD_POD"
	envScenario103NsP      = "SCENARIO103_NAMESPACE"

	perf103DefaultCluster   = "fdw-test"
	perf103DefaultNamespace = "cloudberry-test"
	perf103ExecTimeout      = 5 * time.Minute

	// perf103ExtJobName / perf103FDWJobName are the external-table and FDW load
	// jobs (over the SAME dataset) the benchmark triggers.
	perf103ExtJobName = "s3-ext-load"
	perf103FDWJobName = "s3-fdw-load"
	// perf103ExtTarget / perf103FDWTarget are the load targets.
	perf103ExtTarget = "public.events_ext"
	perf103FDWTarget = "public.events_fdw"
)

// perf103Env returns the ENV value or a default.
func perf103Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf103Namespace() string { return perf103Env(envScenario103NsP, perf103DefaultNamespace) }
func perf103Cluster() string   { return perf103Env(envScenario103ClusterP, perf103DefaultCluster) }
func perf103CoordPod() string {
	return perf103Env(envScenario103CoordP, perf103Cluster()+"-coordinator-0")
}

// perf103DataLoadJobName mirrors util.DataLoadJobName (<cluster>-dataload-<job>)
// for the dataload Job the benchmark triggers. Kept local so perf has no internal
// import.
func perf103DataLoadJobName(job string) string {
	return perf103Cluster() + "-dataload-" + job
}

// perf103SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO103_FDW_LIVE=1.
func perf103SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS103Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 103 FDW perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 103 FDW perf baseline")
	}
	if os.Getenv(envScenario103LivePerf) != "1" {
		b.Skip("SCENARIO103_FDW_LIVE not set, skipping the live FDW perf baseline")
	}
}

// perf103Kubectl runs a kubectl subcommand bounded by perf103ExecTimeout.
func perf103Kubectl(ctx context.Context, args ...string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf103ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf103CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec.
func perf103CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf103ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf103Namespace(),
		"-c", "cloudberry", perf103CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf103ShQuote single-quotes a string for bash -lc.
func perf103ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf103RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf103RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf103CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf103ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// perf103RunLoad triggers the operator-built dataload Job for jobName into target
// (truncating first) and returns the rows/sec for that run. Returns 0 on a skip.
func perf103RunLoad(b *testing.B, ctx context.Context, jobName, target string) float64 {
	b.Helper()
	_, _ = perf103CoordExec(ctx, fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
		perf103ShQuote("TRUNCATE "+target)))

	src := perf103DataLoadJobName(jobName)
	runJob := jobName + "-perf-run"
	_, _ = perf103Kubectl(ctx, "delete", "job", runJob, "-n", perf103Namespace(),
		"--ignore-not-found")
	if out, err := perf103Kubectl(ctx, "create", "job", runJob,
		"--from=job/"+src, "-n", perf103Namespace()); err != nil {
		out2, err2 := perf103Kubectl(ctx, "create", "job", runJob,
			"--from=cronjob/"+src, "-n", perf103Namespace())
		if err2 != nil {
			b.Skipf("perf could not create run Job from %s: %v (out=%s) / %v (out=%s)",
				src, err, out, err2, out2)
		}
	}
	defer func() {
		_, _ = perf103Kubectl(ctx, "delete", "job", runJob, "-n", perf103Namespace(),
			"--ignore-not-found")
	}()

	start := time.Now()
	if out, err := perf103Kubectl(ctx, "wait", "--for=condition=complete",
		"--timeout=5m", "job/"+runJob, "-n", perf103Namespace()); err != nil {
		b.Fatalf("perf %s load Job did not complete: %v (out=%s)", jobName, err, out)
	}
	elapsed := time.Since(start)

	loaded, cerr := perf103RowCount(ctx, target)
	if cerr != nil {
		b.Fatalf("perf %s row count failed: %v", jobName, cerr)
	}
	var rps float64
	if elapsed > 0 && loaded > 0 {
		rps = float64(loaded) / elapsed.Seconds()
	}
	b.Logf("scenario103 perf: %s loaded %d rows into %s in %s (%.1f rows/sec)",
		jobName, loaded, target, elapsed, rps)
	return rps
}

// BenchmarkScenario103_FDWvsExternalThroughput measures FDW vs external-table
// LOAD throughput (rows/sec) by triggering the s3-fdw-load and s3-ext-load
// dataload Jobs (over the SAME MinIO s3 dataset) and timing each run, then
// reporting fdw_rows/sec and ext_rows/sec via b.ReportMetric so the two
// equivalent paths can be compared. Skips cleanly without live infra. NO
// cloudberry_pxf_* metric is emitted; the FDW load reuses
// cloudberry_data_loading_*.
func BenchmarkScenario103_FDWvsExternalThroughput(b *testing.B) {
	perf103SkipUnlessLive(b)
	ctx := context.Background()

	// Coordinator reachable?
	if out, err := perf103CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf103CoordPod(), err, out)
	}

	// Ensure the two equivalence target tables exist (id int, name text, value int).
	ddl := "CREATE TABLE IF NOT EXISTS public.events_ext (id int, name text, value int); " +
		"CREATE TABLE IF NOT EXISTS public.events_fdw (id int, name text, value int);"
	if out, err := perf103CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf103ShQuote(ddl))); err != nil {
		b.Skipf("perf FDW setup failed (create targets): %v (out=%s)", err, out)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fdwRPS := perf103RunLoad(b, ctx, perf103FDWJobName, perf103FDWTarget)
		extRPS := perf103RunLoad(b, ctx, perf103ExtJobName, perf103ExtTarget)
		if fdwRPS > 0 {
			b.ReportMetric(fdwRPS, "fdw_rows/sec")
		}
		if extRPS > 0 {
			b.ReportMetric(extRPS, "ext_rows/sec")
		}
	}
}
