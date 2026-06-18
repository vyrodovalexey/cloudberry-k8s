//go:build e2e

// Package perf holds Scenario 98 filter-pushdown / column-projection read
// throughput benchmarks.
//
// PERF-HARNESS NOTE: like the Scenario 96/97 perf files, these are minimal but
// REAL Go benchmarks that record rows/sec for a FILTERED read vs an UNFILTERED
// baseline (filter pushdown) and a PROJECTED column-subset read vs a SELECT *
// (column projection) against the live pushdown-test cluster + PXF. They are
// gated behind the SAME live flag as the e2e Part B (SCENARIO98_PUSHDOWN_LIVE=1)
// and skip cleanly without KUBECONFIG / infra, so `go test`/`go vet` compile them
// but CI without infra never runs the body.
//
// HONESTY: these report rows/sec only — they do NOT assert (and never emit) a
// bytes_transferred metric, which stays PLANNED (PXF has no honest external-bytes
// counter). The honest correctness proof (filtered < baseline row count) is in
// the e2e suite; these benchmarks measure THROUGHPUT.
//
// Build tag: e2e (these need the deployed cluster + sources, same as the e2e
// suite). Run with:
//
//	SCENARIO98_PUSHDOWN_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario98 ./test/perf/...
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
	envKubeconfigS98Perf   = "KUBECONFIG"
	envScenario98LivePerf  = "SCENARIO98_PUSHDOWN_LIVE"
	envScenario98ClusterP  = "SCENARIO98_CLUSTER"
	envScenario98CoordPodP = "SCENARIO98_COORD_POD"
	envScenario98NsP       = "SCENARIO98_NAMESPACE"

	perf98DefaultCluster   = "pushdown-test"
	perf98DefaultNamespace = "cloudberry-test"
	perf98ExecTimeout      = 5 * time.Minute
)

// perf98Env returns the ENV value or a default.
func perf98Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf98Namespace() string { return perf98Env(envScenario98NsP, perf98DefaultNamespace) }
func perf98Cluster() string   { return perf98Env(envScenario98ClusterP, perf98DefaultCluster) }
func perf98CoordPod() string {
	return perf98Env(envScenario98CoordPodP, perf98Cluster()+"-coordinator-0")
}

// perf98SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO98_PUSHDOWN_LIVE=1.
func perf98SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS98Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 98 pushdown perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 98 perf baseline")
	}
	if os.Getenv(envScenario98LivePerf) != "1" {
		b.Skip("SCENARIO98_PUSHDOWN_LIVE not set, skipping the live perf baseline")
	}
}

// perf98CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by perf98ExecTimeout.
func perf98CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf98ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf98Namespace(),
		"-c", "cloudberry", perf98CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf98ShQuote single-quotes a string for bash -lc.
func perf98ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf98RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf98RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf98CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf98ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario98_FilterPushdownThroughput measures read throughput
// (rows/sec) for a FILTERED load vs an UNFILTERED baseline load over the same
// filterable external table. It reports rows/sec for each leg via b.ReportMetric.
// The external table under test is SCENARIO98_PERF_EXT (default a parquet-backed
// pxf external table the deploy agent stages) and the filter predicate is
// SCENARIO98_PERF_FILTER (default "region = 'us-east'"). Skips cleanly without
// live infra. NO bytes_transferred metric is emitted.
func BenchmarkScenario98_FilterPushdownThroughput(b *testing.B) {
	perf98SkipUnlessLive(b)
	ctx := context.Background()

	ext := perf98Env("SCENARIO98_PERF_EXT", "s98_wide_ext")
	filter := perf98Env("SCENARIO98_PERF_FILTER", "region = 'us-east'")
	baseTgt := "public.s98_perf_baseline"
	filtTgt := "public.s98_perf_filtered"

	if out, err := perf98CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf98CoordPod(), err, out)
	}

	prep := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; DROP TABLE IF EXISTS %s; "+
			"CREATE TABLE %s (LIKE %s); CREATE TABLE %s (LIKE %s);",
		baseTgt, filtTgt, baseTgt, ext, filtTgt, ext)
	if out, err := perf98CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf98ShQuote(prep))); err != nil {
		b.Skipf("perf pushdown setup failed (external table %s may be absent): %v (out=%s)",
			ext, err, out)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, _ = perf98CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
			perf98ShQuote(fmt.Sprintf("TRUNCATE %s; TRUNCATE %s;", baseTgt, filtTgt))))
		b.StartTimer()

		// Unfiltered baseline.
		startBase := time.Now()
		if out, err := perf98CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perf98ShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", baseTgt, ext)))); err != nil {
			b.Fatalf("perf baseline INSERT failed: %v (out=%s)", err, out)
		}
		baseElapsed := time.Since(startBase)
		baseRows, err := perf98RowCount(ctx, baseTgt)
		if err != nil {
			b.Fatalf("perf baseline count failed: %v", err)
		}

		// Filtered (filter pushdown).
		startFilt := time.Now()
		if out, err := perf98CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perf98ShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE %s",
					filtTgt, ext, filter)))); err != nil {
			b.Fatalf("perf filtered INSERT failed: %v (out=%s)", err, out)
		}
		filtElapsed := time.Since(startFilt)
		filtRows, err := perf98RowCount(ctx, filtTgt)
		if err != nil {
			b.Fatalf("perf filtered count failed: %v", err)
		}

		if baseElapsed > 0 {
			b.ReportMetric(float64(baseRows)/baseElapsed.Seconds(), "baseline_rows/sec")
		}
		if filtElapsed > 0 {
			b.ReportMetric(float64(filtRows)/filtElapsed.Seconds(), "filtered_rows/sec")
		}
		b.Logf("scenario98 pushdown perf: baseline=%d rows (%s) filtered=%d rows (%s)",
			baseRows, baseElapsed, filtRows, filtElapsed)
	}

	_, _ = perf98CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
		perf98ShQuote(fmt.Sprintf("DROP TABLE IF EXISTS %s; DROP TABLE IF EXISTS %s;",
			baseTgt, filtTgt))))
}

// BenchmarkScenario98_ProjectionThroughput measures read throughput (rows/sec)
// for a PROJECTED column-subset load vs a SELECT * load over the same WIDE
// external table. It reports rows/sec for each leg. The wide external table is
// SCENARIO98_PERF_WIDE_EXT (default s98_wide_ext) and the projected columns are
// SCENARIO98_PERF_PROJECT_COLS (default "col_a, col_b"). Skips cleanly without
// live infra. NO bytes_transferred metric is emitted.
func BenchmarkScenario98_ProjectionThroughput(b *testing.B) {
	perf98SkipUnlessLive(b)
	ctx := context.Background()

	ext := perf98Env("SCENARIO98_PERF_WIDE_EXT", "s98_wide_ext")
	cols := perf98Env("SCENARIO98_PERF_PROJECT_COLS", "col_a, col_b")

	if out, err := perf98CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf98CoordPod(), err, out)
	}

	// A cheap presence probe: count over the wide external table. If absent, skip.
	if _, err := perf98RowCount(ctx, ext); err != nil {
		b.Skipf("perf projection: wide external table %s absent: %v", ext, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// SELECT * (full width) throughput.
		startAll := time.Now()
		outAll, err := perf98CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -tA -c %s",
				perf98ShQuote(fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM %s) q", ext))))
		if err != nil {
			b.Fatalf("perf projection SELECT * failed: %v (out=%s)", err, outAll)
		}
		allElapsed := time.Since(startAll)
		allRows, _ := strconv.ParseInt(strings.TrimSpace(outAll), 10, 64)

		// Projected subset throughput.
		startProj := time.Now()
		outProj, err := perf98CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -tA -c %s",
				perf98ShQuote(fmt.Sprintf("SELECT count(*) FROM (SELECT %s FROM %s) q", cols, ext))))
		if err != nil {
			b.Fatalf("perf projection subset failed: %v (out=%s)", err, outProj)
		}
		projElapsed := time.Since(startProj)
		projRows, _ := strconv.ParseInt(strings.TrimSpace(outProj), 10, 64)

		if allElapsed > 0 {
			b.ReportMetric(float64(allRows)/allElapsed.Seconds(), "selectstar_rows/sec")
		}
		if projElapsed > 0 {
			b.ReportMetric(float64(projRows)/projElapsed.Seconds(), "projected_rows/sec")
		}
		b.Logf("scenario98 projection perf: select*=%d rows (%s) projected=%d rows (%s)",
			allRows, allElapsed, projRows, projElapsed)
	}
}
