//go:build e2e

// Scenario 101 gpload throughput benchmark.
//
// PERF-HARNESS NOTE: like the Scenario 99 perf file, this is a minimal but REAL
// Go benchmark that records rows/sec LOADED into public.raw_data from a larger
// gpfdist-served CSV via the gpload Job (gpload -f <ctl>). It is gated behind the
// SAME live flag as the e2e Part B (SCENARIO101_GPFDIST_LIVE=1) and skips cleanly
// without KUBECONFIG / infra, so `go test`/`go vet` compile it but CI without
// infra never runs the body.
//
// HONESTY: this reports rows/sec only — it does NOT assert (and never emits) a
// cloudberry_gpfdist_* metric, which stays PLANNED. gpload reuses
// cloudberry_data_loading_*. The honest correctness proof (count(*) > 0 in
// public.raw_data) is in the e2e suite; this benchmark measures load THROUGHPUT.
//
// Build tag: e2e (this needs the deployed gpfdist-test cluster + gpfdist, same as
// the e2e suite). Run with:
//
//	SCENARIO101_GPFDIST_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario101 ./test/perf/...
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
	envKubeconfigS101Perf   = "KUBECONFIG"
	envScenario101LivePerf  = "SCENARIO101_GPFDIST_LIVE"
	envScenario101ClusterP  = "SCENARIO101_CLUSTER"
	envScenario101CoordPodP = "SCENARIO101_COORD_POD"
	envScenario101NsP       = "SCENARIO101_NAMESPACE"

	perf101DefaultCluster   = "gpfdist-test"
	perf101DefaultNamespace = "cloudberry-test"
	perf101ExecTimeout      = 5 * time.Minute

	// perf101TargetTable is the gpload target table (GL.5).
	perf101TargetTable = "public.raw_data"
	// perf101JobName is the gpload-csv job name.
	perf101JobName = "gpload-csv"
)

// perf101Env returns the ENV value or a default.
func perf101Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf101Namespace() string { return perf101Env(envScenario101NsP, perf101DefaultNamespace) }
func perf101Cluster() string   { return perf101Env(envScenario101ClusterP, perf101DefaultCluster) }
func perf101CoordPod() string {
	return perf101Env(envScenario101CoordPodP, perf101Cluster()+"-coordinator-0")
}

// perf101DataLoadJobName mirrors util.DataLoadJobName (<cluster>-dataload-<job>)
// for the gpload CronJob the benchmark triggers. Kept local so perf has no
// internal import.
func perf101DataLoadJobName() string {
	return perf101Cluster() + "-dataload-" + perf101JobName
}

// perf101SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO101_GPFDIST_LIVE=1.
func perf101SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS101Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 101 gpload perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 101 gpload perf baseline")
	}
	if os.Getenv(envScenario101LivePerf) != "1" {
		b.Skip("SCENARIO101_GPFDIST_LIVE not set, skipping the live gpload perf baseline")
	}
}

// perf101Kubectl runs a kubectl subcommand bounded by perf101ExecTimeout.
func perf101Kubectl(ctx context.Context, args ...string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf101ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf101CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec.
func perf101CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf101ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf101Namespace(),
		"-c", "cloudberry", perf101CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf101ShQuote single-quotes a string for bash -lc.
func perf101ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf101RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf101RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf101CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf101ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario101_GploadThroughput measures gpload LOAD throughput
// (rows/sec) by triggering the gpload CronJob (kubectl create job --from=cronjob)
// to load the gpfdist-served CSVs into public.raw_data and timing the run, then
// reporting loaded_rows/sec via b.ReportMetric. The CSV size is whatever the
// gpfdist PVC serves (seed a larger CSV via gen-gpload-csv.sh for a meaningful
// baseline). Skips cleanly without live infra. NO gpfdist metric is emitted.
func BenchmarkScenario101_GploadThroughput(b *testing.B) {
	perf101SkipUnlessLive(b)
	ctx := context.Background()

	// Coordinator reachable?
	if out, err := perf101CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf101CoordPod(), err, out)
	}

	// Ensure the target table exists.
	ddl := "CREATE TABLE IF NOT EXISTS public.raw_data " +
		"(id int, event_type text, payload jsonb, created_at timestamptz);"
	if out, err := perf101CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf101ShQuote(ddl))); err != nil {
		b.Skipf("perf gpload setup failed (create %s): %v (out=%s)", perf101TargetTable, err, out)
	}

	cron := perf101DataLoadJobName()
	runJob := "gpload-csv-perf-run"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, _ = perf101Kubectl(ctx, "delete", "job", runJob, "-n", perf101Namespace(),
			"--ignore-not-found")
		if out, err := perf101Kubectl(ctx, "create", "job", runJob,
			"--from=cronjob/"+cron, "-n", perf101Namespace()); err != nil {
			b.Skipf("perf could not create job from cronjob/%s: %v (out=%s)", cron, err, out)
		}
		b.StartTimer()

		start := time.Now()
		if out, err := perf101Kubectl(ctx, "wait", "--for=condition=complete",
			"--timeout=5m", "job/"+runJob, "-n", perf101Namespace()); err != nil {
			b.Fatalf("perf gpload Job did not complete: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)

		b.StopTimer()
		loaded, cerr := perf101RowCount(ctx, perf101TargetTable)
		if cerr != nil {
			b.Fatalf("perf gpload row count failed: %v", cerr)
		}
		_, _ = perf101Kubectl(ctx, "delete", "job", runJob, "-n", perf101Namespace(),
			"--ignore-not-found")
		b.StartTimer()

		if elapsed > 0 && loaded > 0 {
			b.ReportMetric(float64(loaded)/elapsed.Seconds(), "loaded_rows/sec")
		}
		b.Logf("scenario101 gpload perf: loaded %d rows into %s in %s",
			loaded, perf101TargetTable, elapsed)
	}
}
