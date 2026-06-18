//go:build e2e

// Package perf holds Scenario 97 Hadoop (HDFS/Hive/HBase) read/write throughput
// benchmarks.
//
// PERF-HARNESS NOTE: like the Scenario 96 object-store perf file, these are
// minimal but REAL Go benchmarks that record rows/sec for a read (hdfs:text or
// hdfs:parquet) and a write (hdfs:sequencefile export) baseline against the live
// hadoop-test cluster + PXF. They are gated behind the SAME live flag as the e2e
// Part B (SCENARIO97_HADOOP_LIVE=1) and skip cleanly without KUBECONFIG / infra,
// so `go test`/`go vet` compile them but CI without infra never runs the body.
//
// Build tag: e2e (these need the deployed cluster + Hadoop stack, same as the
// e2e suite). Run with:
//
//	SCENARIO97_HADOOP_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario97 ./test/perf/...
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
	envKubeconfigS97Perf   = "KUBECONFIG"
	envScenario97LivePerf  = "SCENARIO97_HADOOP_LIVE"
	envScenario97ClusterP  = "SCENARIO97_CLUSTER"
	envScenario97CoordPodP = "SCENARIO97_COORD_POD"
	envScenario97NsP       = "SCENARIO97_NAMESPACE"

	perf97DefaultCluster   = "hadoop-test"
	perf97DefaultNamespace = "cloudberry-test"
	perf97ExecTimeout      = 5 * time.Minute
)

// perf97Env returns the ENV value or a default.
func perf97Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf97Namespace() string { return perf97Env(envScenario97NsP, perf97DefaultNamespace) }
func perf97Cluster() string   { return perf97Env(envScenario97ClusterP, perf97DefaultCluster) }
func perf97CoordPod() string {
	return perf97Env(envScenario97CoordPodP, perf97Cluster()+"-coordinator-0")
}

// perf97SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO97_HADOOP_LIVE=1.
func perf97SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS97Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 97 Hadoop perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 97 perf baseline")
	}
	if os.Getenv(envScenario97LivePerf) != "1" {
		b.Skip("SCENARIO97_HADOOP_LIVE not set, skipping the live perf baseline")
	}
}

// perf97CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by perf97ExecTimeout.
func perf97CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf97ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf97Namespace(),
		"-c", "cloudberry", perf97CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf97ShQuote single-quotes a string for bash -lc.
func perf97ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf97RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf97RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf97CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf97ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario97_ReadThroughput measures HDFS READ throughput (rows/sec) by
// (re)loading a PXF Hadoop external table into a target table and timing the
// INSERT...SELECT. It reports rows/sec via b.ReportMetric. The profile under
// test is SCENARIO97_PERF_READ_PROFILE (default hdfs:text) on
// SCENARIO97_PERF_READ_SERVER (default hadoop-cluster) reading
// SCENARIO97_PERF_READ_RESOURCE. Skips cleanly without live infra.
func BenchmarkScenario97_ReadThroughput(b *testing.B) {
	perf97SkipUnlessLive(b)
	ctx := context.Background()

	profile := perf97Env("SCENARIO97_PERF_READ_PROFILE", "hdfs:text")
	server := perf97Env("SCENARIO97_PERF_READ_SERVER", "hadoop-cluster")
	resource := perf97Env("SCENARIO97_PERF_READ_RESOURCE", "/data-lake/events")
	target := "public.s97_perf_read"
	ext := "s97_perf_read_ext"

	if out, err := perf97CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf97CoordPod(), err, out)
	}

	setup := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE EXTERNAL TABLE %s (LIKE %s) "+
			"LOCATION ('pxf://%s?PROFILE=%s&SERVER=%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		ext, ext, target, resource, profile, server)
	prep := fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE TABLE %s (line text) DISTRIBUTED RANDOMLY;", target, target)
	if out, err := perf97CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s -c %s",
			perf97ShQuote(prep), perf97ShQuote(setup))); err != nil {
		b.Skipf("perf read setup failed (PXF/extension/sample may be absent): %v (out=%s)", err, out)
	}

	b.ResetTimer()
	var totalRows int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, _ = perf97CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -c %s", perf97ShQuote(fmt.Sprintf("TRUNCATE %s;", target))))
		b.StartTimer()

		start := time.Now()
		if out, err := perf97CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perf97ShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", target, ext)))); err != nil {
			b.Fatalf("perf read INSERT failed: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)

		rows, err := perf97RowCount(ctx, target)
		if err != nil {
			b.Fatalf("perf read count failed: %v", err)
		}
		totalRows += rows
		if elapsed > 0 {
			b.ReportMetric(float64(rows)/elapsed.Seconds(), "rows/sec")
		}
	}
	b.Logf("scenario97 read perf: profile=%s server=%s totalRows=%d over %d iters",
		profile, server, totalRows, b.N)

	_, _ = perf97CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
		perf97ShQuote(fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s; DROP TABLE IF EXISTS %s;", ext, target))))
}

// BenchmarkScenario97_WriteThroughput measures HDFS WRITE/EXPORT throughput
// (rows/sec) by creating a WRITABLE PXF external table (pxfwritable_export) for
// hdfs:sequencefile and timing the INSERT (export) of a staged source table to
// HDFS. It reports rows/sec. The profile is SCENARIO97_PERF_WRITE_PROFILE
// (default hdfs:sequencefile) on SCENARIO97_PERF_WRITE_SERVER (default
// hadoop-cluster). Skips cleanly without live infra.
func BenchmarkScenario97_WriteThroughput(b *testing.B) {
	perf97SkipUnlessLive(b)
	ctx := context.Background()

	profile := perf97Env("SCENARIO97_PERF_WRITE_PROFILE", "hdfs:sequencefile")
	server := perf97Env("SCENARIO97_PERF_WRITE_SERVER", "hadoop-cluster")
	resourceBase := perf97Env("SCENARIO97_PERF_WRITE_RESOURCE", "/data-lake/perf-exports/")
	src := "public.s97_perf_write_src"

	if out, err := perf97CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf97CoordPod(), err, out)
	}

	stage := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; CREATE TABLE %s (line text) DISTRIBUTED RANDOMLY; "+
			"INSERT INTO %s SELECT 'row-'||g FROM generate_series(1, 10000) g;",
		src, src, src)
	if out, err := perf97CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf97ShQuote(stage))); err != nil {
		b.Skipf("perf write source staging failed: %v (out=%s)", err, out)
	}

	srcRows, err := perf97RowCount(ctx, src)
	if err != nil {
		b.Skipf("perf write source count failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ext := fmt.Sprintf("s97_perf_write_ext_%d", i)
		resource := fmt.Sprintf("%siter-%d/", resourceBase, i)
		setup := fmt.Sprintf(
			"DROP EXTERNAL TABLE IF EXISTS %s; "+
				"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) "+
				"LOCATION ('pxf://%s?PROFILE=%s&SERVER=%s') "+
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
			ext, ext, src, resource, profile, server)
		if out, err := perf97CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf97ShQuote(setup))); err != nil {
			b.Skipf("perf write setup failed (PXF/extension may be absent): %v (out=%s)", err, out)
		}
		b.StartTimer()

		start := time.Now()
		if out, err := perf97CoordExec(ctx,
			fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
				perf97ShQuote(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)))); err != nil {
			b.Fatalf("perf write export failed: %v (out=%s)", err, out)
		}
		elapsed := time.Since(start)
		if elapsed > 0 {
			b.ReportMetric(float64(srcRows)/elapsed.Seconds(), "rows/sec")
		}

		b.StopTimer()
		_, _ = perf97CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
			perf97ShQuote(fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s;", ext))))
		b.StartTimer()
	}
	b.Logf("scenario97 write perf: profile=%s server=%s srcRows=%d over %d iters",
		profile, server, srcRows, b.N)

	_, _ = perf97CoordExec(ctx, fmt.Sprintf("psql -d postgres -c %s",
		perf97ShQuote(fmt.Sprintf("DROP TABLE IF EXISTS %s;", src))))
}
