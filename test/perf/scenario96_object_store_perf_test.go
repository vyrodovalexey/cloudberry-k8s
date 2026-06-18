//go:build e2e

// Package perf holds Scenario 96 object-store read/write throughput benchmarks.
//
// PERF-HARNESS NOTE: the repository's primary performance harness is
// Yandex-Tank + shell (test/performance/, see run-perftest.sh and the
// scenarios/*.yaml). That harness targets the operator REST API over HTTP and is
// not a natural fit for measuring PXF object-store read/write THROUGHPUT against
// MinIO (which is a psql-on-coordinator + MinIO data-path measurement). Rather
// than bolt an unrelated HTTP ammo file onto Yandex-Tank, this file adds minimal
// but REAL Go benchmarks that record rows/sec for a read (text/parquet) and a
// write (text/parquet export) baseline against the live MinIO-backed cluster.
//
// They are gated behind the SAME live flag as the e2e Part B
// (SCENARIO96_OBJSTORE_LIVE=1) and skip cleanly without KUBECONFIG / infra, so
// `go test`/`go vet` compile them but CI without infra never runs the body.
//
// Build tag: e2e (these need the deployed cluster + MinIO, same as the e2e
// suite). Run with:
//
//	SCENARIO96_OBJSTORE_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario96 ./test/perf/...
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
	envKubeconfigPerf      = "KUBECONFIG"
	envScenario96LivePerf  = "SCENARIO96_OBJSTORE_LIVE"
	envScenario96ClusterP  = "SCENARIO96_CLUSTER"
	envScenario96CoordPodP = "SCENARIO96_COORD_POD"
	envScenario96NsP       = "SCENARIO96_NAMESPACE"

	perfDefaultCluster   = "objstore-test"
	perfDefaultNamespace = "cloudberry-test"
	perfExecTimeout      = 5 * time.Minute
)

// perfEnv returns the ENV value or a default.
func perfEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perfNamespace() string { return perfEnv(envScenario96NsP, perfDefaultNamespace) }
func perfCluster() string   { return perfEnv(envScenario96ClusterP, perfDefaultCluster) }
func perfCoordPod() string {
	return perfEnv(envScenario96CoordPodP, perfCluster()+"-coordinator-0")
}

// perfSkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO96_OBJSTORE_LIVE=1.
func perfSkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigPerf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 96 object-store perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 96 perf baseline")
	}
	if os.Getenv(envScenario96LivePerf) != "1" {
		b.Skip("SCENARIO96_OBJSTORE_LIVE not set, skipping the live perf baseline")
	}
}

// perfCoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by perfExecTimeout.
func perfCoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perfExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perfNamespace(),
		"-c", "cloudberry", perfCoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perfShQuote single-quotes a string for bash -lc.
func perfShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perfRowCount runs SELECT count(*) FROM <table> on the coordinator.
func perfRowCount(ctx context.Context, table string) (int64, error) {
	out, err := perfCoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perfShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario96_ReadThroughput measures READ throughput (rows/sec) by
// (re)loading a PXF object-store external table into a target table and timing
// the INSERT...SELECT. It reports rows/sec via b.ReportMetric. The profile under
// test is SCENARIO96_PERF_READ_PROFILE (default s3:text) on
// SCENARIO96_PERF_READ_SERVER (default minio-warehouse) reading
// SCENARIO96_PERF_READ_RESOURCE. Skips cleanly without live infra.
func BenchmarkScenario96_ReadThroughput(b *testing.B) {
	perfSkipUnlessLive(b)
	ctx := context.Background()

	profile := perfEnv("SCENARIO96_PERF_READ_PROFILE", "s3:text")
	server := perfEnv("SCENARIO96_PERF_READ_SERVER", "minio-warehouse")
	resource := perfEnv("SCENARIO96_PERF_READ_RESOURCE", "cloudberry-warehouse/text/data.csv")
	target := "public.s96_perf_read"
	ext := "s96_perf_read_ext"

	// Preflight: psql reachable.
	if out, err := perfCoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perfCoordPod(), err, out)
	}

	setup := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE EXTERNAL TABLE %s (LIKE %s) "+
			"LOCATION ('pxf://%s?PROFILE=%s&SERVER=%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		ext, ext, target, resource, profile, server)
	prep := fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE TABLE %s (line text) DISTRIBUTED RANDOMLY;", target, target)
	if out, err := perfCoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s -c %s",
			perfShQuote(prep), perfShQuote(setup))); err != nil {
		b.Skipf("perf read setup failed (PXF/extension/sample may be absent): %v (out=%s)", err, out)
	}

	b.ResetTimer()
	var totalRows int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, _ = perfCoordExec(ctx,
			fmt.Sprintf("psql -d postgres -c %s", perfShQuote(fmt.Sprintf("TRUNCATE %s;", target))))
		b.StartTimer()

		start := time.Now()
		if out, err := perfCoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perfShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", target, ext)))); err != nil {
			b.Fatalf("perf read INSERT failed: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)

		rows, err := perfRowCount(ctx, target)
		if err != nil {
			b.Fatalf("perf read count failed: %v", err)
		}
		totalRows += rows
		if elapsed > 0 {
			b.ReportMetric(float64(rows)/elapsed.Seconds(), "rows/sec")
		}
	}
	b.Logf("scenario96 read perf: profile=%s server=%s totalRows=%d over %d iters",
		profile, server, totalRows, b.N)

	_, _ = perfCoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
		perfShQuote(fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s; DROP TABLE IF EXISTS %s;", ext, target))))
}

// BenchmarkScenario96_WriteThroughput measures WRITE/EXPORT throughput (rows/sec)
// by creating a WRITABLE PXF external table (pxfwritable_export) and timing the
// INSERT (export) of a staged source table to MinIO. It reports rows/sec. The
// profile is SCENARIO96_PERF_WRITE_PROFILE (default s3:text) on
// SCENARIO96_PERF_WRITE_SERVER (default s3-datalake). Skips cleanly without live
// infra.
func BenchmarkScenario96_WriteThroughput(b *testing.B) {
	perfSkipUnlessLive(b)
	ctx := context.Background()

	profile := perfEnv("SCENARIO96_PERF_WRITE_PROFILE", "s3:text")
	server := perfEnv("SCENARIO96_PERF_WRITE_SERVER", "s3-datalake")
	resourceBase := perfEnv("SCENARIO96_PERF_WRITE_RESOURCE", "cloudberry-warehouse/perf-exports/")
	src := "public.s96_perf_write_src"

	if out, err := perfCoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perfCoordPod(), err, out)
	}

	// Stage a small source table to export.
	stage := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; CREATE TABLE %s (line text) DISTRIBUTED RANDOMLY; "+
			"INSERT INTO %s SELECT 'row-'||g FROM generate_series(1, 10000) g;",
		src, src, src)
	if out, err := perfCoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perfShQuote(stage))); err != nil {
		b.Skipf("perf write source staging failed: %v (out=%s)", err, out)
	}

	srcRows, err := perfRowCount(ctx, src)
	if err != nil {
		b.Skipf("perf write source count failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ext := fmt.Sprintf("s96_perf_write_ext_%d", i)
		resource := fmt.Sprintf("%siter-%d/", resourceBase, i)
		setup := fmt.Sprintf(
			"DROP EXTERNAL TABLE IF EXISTS %s; "+
				"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) "+
				"LOCATION ('pxf://%s?PROFILE=%s&SERVER=%s') "+
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
			ext, ext, src, resource, profile, server)
		if out, err := perfCoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perfShQuote(setup))); err != nil {
			b.Skipf("perf write setup failed (PXF/extension may be absent): %v (out=%s)", err, out)
		}
		b.StartTimer()

		start := time.Now()
		if out, err := perfCoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perfShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)))); err != nil {
			b.Fatalf("perf write export failed: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)
		if elapsed > 0 {
			b.ReportMetric(float64(srcRows)/elapsed.Seconds(), "rows/sec")
		}

		b.StopTimer()
		_, _ = perfCoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
			perfShQuote(fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s;", ext))))
		b.StartTimer()
	}
	b.Logf("scenario96 write perf: profile=%s server=%s srcRows=%d over %d iters",
		profile, server, srcRows, b.N)

	_, _ = perfCoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
		perfShQuote(fmt.Sprintf("DROP TABLE IF EXISTS %s;", src))))
}
