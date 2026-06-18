//go:build e2e

// Package perf holds the Scenario 99 writable-export throughput benchmark.
//
// PERF-HARNESS NOTE: like the Scenario 96/97/98 perf files, this is a minimal but
// REAL Go benchmark that records rows/sec EXPORTED (writable external table) over
// a sized source against the live export-test cluster + PXF. It is gated behind
// the SAME live flag as the e2e Part B (SCENARIO99_EXPORT_LIVE=1) and skips
// cleanly without KUBECONFIG / infra, so `go test`/`go vet` compile it but CI
// without infra never runs the body.
//
// HONESTY: this reports rows/sec only — it does NOT assert (and never emits) a
// bytes_transferred metric, which stays PLANNED (PXF has no honest external-bytes
// counter). The honest correctness proof (data lands + FORMATTER) is in the e2e
// suite; this benchmark measures export THROUGHPUT.
//
// Build tag: e2e (this needs the deployed cluster + export targets, same as the
// e2e suite). Run with:
//
//	SCENARIO99_EXPORT_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario99 ./test/perf/...
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
	envKubeconfigS99Perf   = "KUBECONFIG"
	envScenario99LivePerf  = "SCENARIO99_EXPORT_LIVE"
	envScenario99ClusterP  = "SCENARIO99_CLUSTER"
	envScenario99CoordPodP = "SCENARIO99_COORD_POD"
	envScenario99NsP       = "SCENARIO99_NAMESPACE"

	perf99DefaultCluster   = "export-test"
	perf99DefaultNamespace = "cloudberry-test"
	perf99ExecTimeout      = 5 * time.Minute

	// perf99JDBCExportResource is the pgsource writable target table (FE.11), the
	// most deterministic export target for the throughput baseline.
	perf99JDBCExportResource = "export_target"
	// perf99JDBCServer is the postgres-source PXF server.
	perf99JDBCServer = "postgres-source"
)

// perf99Env returns the ENV value or a default.
func perf99Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf99Namespace() string { return perf99Env(envScenario99NsP, perf99DefaultNamespace) }
func perf99Cluster() string   { return perf99Env(envScenario99ClusterP, perf99DefaultCluster) }
func perf99CoordPod() string {
	return perf99Env(envScenario99CoordPodP, perf99Cluster()+"-coordinator-0")
}

// perf99SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO99_EXPORT_LIVE=1.
func perf99SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS99Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 99 export perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 99 export perf baseline")
	}
	if os.Getenv(envScenario99LivePerf) != "1" {
		b.Skip("SCENARIO99_EXPORT_LIVE not set, skipping the live export perf baseline")
	}
}

// perf99CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by perf99ExecTimeout.
func perf99CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf99ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf99Namespace(),
		"-c", "cloudberry", perf99CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf99ShQuote single-quotes a string for bash -lc.
func perf99ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf99RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf99RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf99CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf99ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario99_ExportThroughput measures WRITABLE-EXPORT throughput
// (rows/sec) by exporting a sized source table TO the JDBC export target via the
// reversed INSERT (INSERT INTO <writable_ext> SELECT * FROM <src>). It reports
// rows/sec via b.ReportMetric. The source size is SCENARIO99_PERF_ROWS (default
// 5000). Skips cleanly without live infra. NO bytes_transferred metric is emitted.
func BenchmarkScenario99_ExportThroughput(b *testing.B) {
	perf99SkipUnlessLive(b)
	ctx := context.Background()

	rows := perf99Env("SCENARIO99_PERF_ROWS", "5000")
	src := "public.s99_perf_export_src"
	ext := "s99_perf_export_ext"
	loc := "pxf://" + perf99JDBCExportResource + "?PROFILE=jdbc&SERVER=" + perf99JDBCServer

	if out, err := perf99CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf99CoordPod(), err, out)
	}

	// Prepare a sized source table.
	prep := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; "+
			"CREATE TABLE %s (id int, region text, amount numeric); "+
			"INSERT INTO %s SELECT g, 'us-east', g*1.0 FROM generate_series(1, %s) g;",
		src, src, src, rows)
	if out, err := perf99CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf99ShQuote(prep))); err != nil {
		b.Skipf("perf export setup failed: %v (out=%s)", err, out)
	}
	defer func() {
		_, _ = perf99CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
			perf99ShQuote(fmt.Sprintf("DROP TABLE IF EXISTS %s; DROP EXTERNAL TABLE IF EXISTS %s;",
				src, ext))))
	}()

	srcRows, err := perf99RowCount(ctx, src)
	if err != nil || srcRows == 0 {
		b.Skipf("perf export source empty/absent: %v (rows=%d)", err, srcRows)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// (Re)create the writable external table over the JDBC export target.
		createDDL := fmt.Sprintf(
			"DROP EXTERNAL TABLE IF EXISTS %s; "+
				"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')",
			ext, ext, src, loc)
		if out, err := perf99CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf99ShQuote(createDDL))); err != nil {
			b.Skipf("perf writable ext create failed: %v (out=%s)", err, out)
		}
		b.StartTimer()

		start := time.Now()
		if out, err := perf99CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perf99ShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)))); err != nil {
			b.Fatalf("perf export INSERT failed: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)

		if elapsed > 0 {
			b.ReportMetric(float64(srcRows)/elapsed.Seconds(), "exported_rows/sec")
		}
		b.Logf("scenario99 export perf: exported %d rows in %s", srcRows, elapsed)
	}
}
