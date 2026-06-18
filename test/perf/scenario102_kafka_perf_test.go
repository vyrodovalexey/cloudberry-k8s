//go:build e2e

// Scenario 102 kafka-cdc continuous-consume throughput benchmark.
//
// PERF-HARNESS NOTE: like the Scenario 101 perf file, this is a minimal Go
// benchmark gated behind the SAME live flag as the e2e Part B
// (SCENARIO102_KAFKA_LIVE=1) and skips cleanly without KUBECONFIG / infra, so
// `go test`/`go vet` compile it but CI without infra never runs the body.
//
// HONESTY: a continuous kafka-cdc consumer's throughput (rows/sec into
// public.kafka_events) can only be measured with a REAL Kafka→PXF connector JAR.
// The staged JAR is a placeholder, so this benchmark is CONFIG-ONLY: it documents
// N/A and emits NO fabricated metric. NO new metric is introduced for kafka-cdc;
// it reuses cloudberry_data_loading_* (job_status=Running for a steady consumer).
// cloudberry_pxf_* stays PLANNED and is never asserted/emitted.
//
// When a REAL connector JAR is staged, the benchmark measures the delta in
// public.kafka_events row count over a fixed observation window and reports
// consumed_rows/sec via b.ReportMetric — but only when the row count actually
// advances (proving a real consumer); otherwise it skips (CONFIG-ONLY).
//
// Build tag: e2e (this needs the deployed kafka-test cluster, same as the e2e
// suite). Run with:
//
//	SCENARIO102_KAFKA_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario102 ./test/perf/...
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
	envKubeconfigS102Perf   = "KUBECONFIG"
	envScenario102LivePerf  = "SCENARIO102_KAFKA_LIVE"
	envScenario102ClusterP  = "SCENARIO102_CLUSTER"
	envScenario102CoordPodP = "SCENARIO102_COORD_POD"
	envScenario102NsP       = "SCENARIO102_NAMESPACE"

	perf102DefaultCluster   = "kafka-test"
	perf102DefaultNamespace = "cloudberry-test"
	perf102ExecTimeout      = 5 * time.Minute

	// perf102TargetTable is the kafka-cdc target table.
	perf102TargetTable = "public.kafka_events"
	// perf102ObserveWindow is the consume-observation window for the throughput
	// delta when a real connector is present.
	perf102ObserveWindow = 30 * time.Second
)

// perf102Env returns the ENV value or a default.
func perf102Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf102Namespace() string { return perf102Env(envScenario102NsP, perf102DefaultNamespace) }
func perf102Cluster() string   { return perf102Env(envScenario102ClusterP, perf102DefaultCluster) }
func perf102CoordPod() string {
	return perf102Env(envScenario102CoordPodP, perf102Cluster()+"-coordinator-0")
}

// perf102SkipUnlessLive skips the benchmark cleanly unless KUBECONFIG is set,
// kubectl exists and SCENARIO102_KAFKA_LIVE=1.
func perf102SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS102Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 102 kafka-cdc perf baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 102 kafka-cdc perf baseline")
	}
	if os.Getenv(envScenario102LivePerf) != "1" {
		b.Skip("SCENARIO102_KAFKA_LIVE not set, skipping the live kafka-cdc perf baseline")
	}
}

// perf102CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec.
func perf102CoordExec(ctx context.Context, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf102ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", perf102Namespace(),
		"-c", "cloudberry", perf102CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// perf102ShQuote single-quotes a string for bash -lc.
func perf102ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// perf102RowCount runs SELECT count(*) FROM <table> on the coordinator.
func perf102RowCount(ctx context.Context, table string) (int64, error) {
	out, err := perf102CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -tA -c %s",
			perf102ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (out=%s)", table, err, out)
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

// BenchmarkScenario102_KafkaConsumeThroughput measures the continuous kafka-cdc
// consume throughput (rows/sec into public.kafka_events) by observing the row
// count delta over a fixed window while the streaming Job runs. It is CONFIG-ONLY
// without a REAL Kafka→PXF connector JAR: when the row count does NOT advance
// (a placeholder JAR cannot actually consume), the benchmark SKIPS and emits NO
// fabricated metric. NO cloudberry_pxf_* metric is emitted.
func BenchmarkScenario102_KafkaConsumeThroughput(b *testing.B) {
	perf102SkipUnlessLive(b)
	ctx := context.Background()

	// Coordinator reachable?
	if out, err := perf102CoordExec(ctx, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		b.Skipf("coordinator %s not reachable: %v (out=%s)", perf102CoordPod(), err, out)
	}

	// Ensure the target table exists.
	ddl := "CREATE TABLE IF NOT EXISTS public.kafka_events " +
		"(id int, event_type text, payload jsonb, op text, ts timestamptz);"
	if out, err := perf102CoordExec(ctx,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", perf102ShQuote(ddl))); err != nil {
		b.Skipf("perf kafka-cdc setup failed (create %s): %v (out=%s)", perf102TargetTable, err, out)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		before, err := perf102RowCount(ctx, perf102TargetTable)
		if err != nil {
			b.Skipf("perf kafka-cdc row count (before) failed: %v", err)
		}
		b.StartTimer()

		// Observe the consume for a fixed window; the continuous Job streams in
		// the background.
		time.Sleep(perf102ObserveWindow)

		b.StopTimer()
		after, err := perf102RowCount(ctx, perf102TargetTable)
		if err != nil {
			b.Skipf("perf kafka-cdc row count (after) failed: %v", err)
		}
		delta := after - before
		if delta <= 0 {
			// No advance => no real consumer (placeholder JAR). CONFIG-ONLY:
			// emit NO fabricated metric.
			b.Skipf("perf kafka-cdc CONFIG-ONLY: row count did not advance over %s "+
				"(before=%d after=%d) — a continuous-consume throughput baseline needs a REAL "+
				"Kafka->PXF connector JAR; NO metric emitted (N/A).",
				perf102ObserveWindow, before, after)
		}
		b.ReportMetric(float64(delta)/perf102ObserveWindow.Seconds(), "consumed_rows/sec")
		b.Logf("scenario102 kafka-cdc perf: consumed %d rows into %s over %s",
			delta, perf102TargetTable, perf102ObserveWindow)
		b.StartTimer()
	}
}
