package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger returns a logger that discards output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewMetricCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	require.NotNil(t, mc)
	assert.NotNil(t, mc.queriesTotal)
	assert.NotNil(t, mc.resgroupRunningQueries)
	assert.NotNil(t, mc.spillFilesActive)
	assert.NotNil(t, mc.segmentsTotal)
	assert.NotNil(t, mc.distTxnActive)
	assert.NotNil(t, mc.tableSkewCoefficient)
}

func TestSafeDeref(t *testing.T) {
	t.Parallel()
	val := "hello"
	assert.Equal(t, "hello", safeDeref(&val, "fallback"))
	assert.Equal(t, "fallback", safeDeref(nil, "fallback"))
}

func TestSafeDerefFloat(t *testing.T) {
	t.Parallel()
	val := 3.14
	assert.InDelta(t, 3.14, safeDerefFloat(&val, 0), 0.0001)
	assert.InDelta(t, 9.9, safeDerefFloat(nil, 9.9), 0.0001)
}

func TestReformatInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"hms", "01:23:45.000000", "1h23m45.000000s"},
		{"zero", "00:00:00", "0h0m0.000000s"},
		{"invalid", "not-an-interval", "0s"},
		{"partial", "12:30", "0s"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, reformatInterval(tc.input))
		})
	}
}

func TestParseIntervalToSeconds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    float64
		wantErr bool
	}{
		{"one hour", "01:00:00.000000", 3600, false},
		{"mixed", "00:01:30.000000", 90, false},
		{"zero", "00:00:00.000000", 0, false},
		{"invalid", "garbage", 0, false}, // reformat returns "0s" which parses fine
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseIntervalToSeconds(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tc.want, got, 0.001)
		})
	}
}

func TestCollectQueryActivity(t *testing.T) {
	fields := []fieldDesc{
		textField("state"), textField("datname"), textField("usename"),
		float8Field("duration_seconds"), textField("wait_event_type"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"active", "db1", "user1", "10.0", ""},
			{"idle in transaction", "db1", "user2", "0", ""},
			{"active", "db1", "user3", "0.5", "Lock"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectQueryActivity(context.Background(), conn, time.Second, testLogger())

	// One active query with duration 10s exceeds the 1s threshold -> slow.
	assert.Equal(t, float64(1), testGauge(t, mc.queriesIdleInTransaction))
	assert.Equal(t, float64(1), testGauge(t, mc.queriesBlocked))
	assert.Equal(t, float64(10), testGauge(t, mc.queryMaxDuration))
}

func TestCollectQueryActivity_QueryError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	// Should not panic on query error.
	mc.collectQueryActivity(context.Background(), conn, time.Second, testLogger())
}

func TestCollectResgroupStatusSummary(t *testing.T) {
	fields := []fieldDesc{
		textField("rsgname"), int8Field("num_running"), int8Field("num_queueing"),
		int8Field("num_executed"), textField("total_queue_duration"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"default_group", "5", "2", "100", "00:01:30.000000"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupStatusSummary(context.Background(), conn, testLogger())

	assert.Equal(t, float64(5), testGaugeVec(t, mc.resgroupRunningQueries, "default_group"))
	assert.Equal(t, float64(2), testGaugeVec(t, mc.resgroupQueuedQueries, "default_group"))
}

func TestCollectResgroupStatusSummary_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no such view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupStatusSummary(context.Background(), conn, testLogger())
}

func TestCollectResgroupStatusPerHost(t *testing.T) {
	fields := []fieldDesc{
		textField("rsgname"), textField("hostname"), float8Field("cpu"),
		float8Field("memory_used"), float8Field("memory_available"),
		float8Field("memory_quota_used"), float8Field("memory_shared_used"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"default_group", "host1", "0.45", "1024", "2048", "512", "256"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupStatusPerHost(context.Background(), conn, testLogger())

	assert.InDelta(t, 0.45, testGaugeVec(t, mc.resgroupCPUUsagePercent, "default_group", "host1"), 0.001)
}

func TestCollectResgroupStatusPerHost_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupStatusPerHost(context.Background(), conn, testLogger())
}

func TestCollectResgroupStatus(t *testing.T) {
	fields := []fieldDesc{
		textField("rsgname"), int8Field("num_running"), int8Field("num_queueing"),
		int8Field("num_executed"), textField("total_queue_duration"),
	}
	perHostFields := []fieldDesc{
		textField("rsgname"), textField("hostname"), float8Field("cpu"),
		float8Field("memory_used"), float8Field("memory_available"),
		float8Field("memory_quota_used"), float8Field("memory_shared_used"),
	}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "per_host") {
			return rowsResponseTyped(perHostFields, [][]string{
				{"default_group", "host1", "0.1", "1", "2", "3", "4"},
			})
		}
		return rowsResponseTyped(fields, [][]string{
			{"default_group", "1", "0", "10", "00:00:01.000000"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupStatus(context.Background(), conn, testLogger())
}

func TestCollectResgroupIOStats(t *testing.T) {
	fields := []fieldDesc{
		textField("rsgname"), textField("hostname"), textField("tablespace"),
		float8Field("rbps"), float8Field("wbps"), float8Field("riops"), float8Field("wiops"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"default_group", "host1", "pg_default", "1000", "2000", "10", "20"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupIOStats(context.Background(), conn, testLogger())

	assert.Equal(t, float64(1000),
		testGaugeVec(t, mc.resgroupIOReadBytesPerSec, "default_group", "host1", "pg_default"))
}

func TestCollectResgroupIOStats_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectResgroupIOStats(context.Background(), conn, testLogger())
}

func TestCollectSpillFiles(t *testing.T) {
	summaryFields := []fieldDesc{int8Field("active_count"), int8Field("total_bytes")}
	perSegFields := []fieldDesc{textField("segment_id"), textField("hostname"), int8Field("bytes")}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "per_segment") {
			return rowsResponseTyped(perSegFields, [][]string{
				{"0", "host1", "1024"},
				{"1", "host2", "2048"},
			})
		}
		return singleRowTyped(summaryFields, []string{"3", "4096"})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectSpillFiles(context.Background(), conn, testLogger())

	assert.Equal(t, float64(3), testGauge(t, mc.spillFilesActive))
	assert.Equal(t, float64(4096), testGauge(t, mc.spillFilesBytes))
	assert.Equal(t, float64(1024), testGaugeVec(t, mc.spillFilesPerSegment, "0", "host1"))
}

func TestCollectSpillFileSummary_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectSpillFileSummary(context.Background(), conn, testLogger())
}

func TestCollectSpillFilePerSegment_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectSpillFilePerSegment(context.Background(), conn, testLogger())
}

func TestCollectSegmentHealth(t *testing.T) {
	healthFields := []fieldDesc{
		int8Field("primary_total"), int8Field("mirror_total"), int8Field("up_count"),
		int8Field("down_count"), int8Field("not_synced"), int8Field("not_preferred"),
	}
	uptimeFields := []fieldDesc{float8Field("uptime_seconds")}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "uptime") || strings.Contains(query, "postmaster_start_time") {
			return singleRowTyped(uptimeFields, []string{"123456.5"})
		}
		return singleRowTyped(healthFields, []string{"4", "4", "8", "0", "0", "0"})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectSegmentHealth(context.Background(), conn, testLogger())

	assert.Equal(t, float64(4), testGaugeVec(t, mc.segmentsTotal, "primary"))
	assert.Equal(t, float64(8), testGauge(t, mc.segmentsUp))
	assert.InDelta(t, 123456.5, testGauge(t, mc.clusterUptime), 0.1)
}

func TestCollectSegmentStatus_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no table")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectSegmentStatus(context.Background(), conn, testLogger())
}

func TestCollectClusterUptime_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("error")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectClusterUptime(context.Background(), conn, testLogger())
}

func TestCollectDistributedTransactions(t *testing.T) {
	xactFields := []fieldDesc{int8Field("active"), int8Field("committed"), int8Field("aborted")}
	oldestFields := []fieldDesc{float8Field("oldest_age")}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "xact_start") {
			return singleRowTyped(oldestFields, []string{"42.5"})
		}
		return singleRowTyped(xactFields, []string{"2", "100", "5"})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectDistributedTransactions(context.Background(), conn, testLogger())

	assert.Equal(t, float64(2), testGauge(t, mc.distTxnActive))
	assert.InDelta(t, 42.5, testGauge(t, mc.oldestTxnAge), 0.1)
}

func TestCollectDistributedXacts_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectDistributedXacts(context.Background(), conn, testLogger())
}

func TestCollectOldestTransaction_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("error")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectOldestTransaction(context.Background(), conn, testLogger())
}

func TestCollectTableSkew(t *testing.T) {
	fields := []fieldDesc{
		textField("schemaname"), textField("tablename"), float8Field("skew_coefficient"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"public", "orders", "0.25"},
			{"public", "events", "0.10"},
		})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectTableSkew(context.Background(), conn, testLogger())

	assert.InDelta(t, 0.25, testGaugeVec(t, mc.tableSkewCoefficient, "public", "orders"), 0.001)
}

func TestCollectTableSkew_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("no view")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectTableSkew(context.Background(), conn, testLogger())
}

func TestCollectAll(t *testing.T) {
	// A responder that returns plausible results for each collector based on
	// query content. Any unmatched query returns an empty single-column row.
	conn, cleanup := newMockConn(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "pg_stat_activity") && strings.Contains(query, "backend_type"):
			return rowsResponseTyped([]fieldDesc{
				textField("state"), textField("datname"), textField("usename"),
				float8Field("d"), textField("w"),
			}, [][]string{{"active", "db", "u", "1.0", ""}})
		case strings.Contains(query, "per_host"):
			return rowsResponseTyped([]fieldDesc{
				textField("rsgname"), textField("hostname"), float8Field("cpu"),
				float8Field("a"), float8Field("b"), float8Field("c"), float8Field("d"),
			}, [][]string{{"g", "h", "0.1", "1", "2", "3", "4"}})
		case strings.Contains(query, "gp_resgroup_status"):
			return rowsResponseTyped([]fieldDesc{
				textField("rsgname"), int8Field("a"), int8Field("b"),
				int8Field("c"), textField("d"),
			}, [][]string{{"g", "1", "0", "10", "00:00:01.000000"}})
		case strings.Contains(query, "iostats"):
			return rowsResponseTyped([]fieldDesc{
				textField("rsgname"), textField("hostname"), textField("ts"),
				float8Field("a"), float8Field("b"), float8Field("c"), float8Field("d"),
			}, [][]string{{"g", "h", "ts", "1", "2", "3", "4"}})
		case strings.Contains(query, "per_segment"):
			return rowsResponseTyped([]fieldDesc{
				textField("seg"), textField("host"), int8Field("bytes"),
			}, [][]string{{"0", "h", "1024"}})
		case strings.Contains(query, "workfile_usage_per_query"):
			return singleRowTyped([]fieldDesc{int8Field("c"), int8Field("b")}, []string{"1", "2"})
		case strings.Contains(query, "gp_segment_configuration"):
			return singleRowTyped([]fieldDesc{
				int8Field("a"), int8Field("b"), int8Field("c"),
				int8Field("d"), int8Field("e"), int8Field("f"),
			}, []string{"4", "4", "8", "0", "0", "0"})
		case strings.Contains(query, "postmaster_start_time"):
			return singleRowTyped([]fieldDesc{float8Field("u")}, []string{"100"})
		case strings.Contains(query, "gp_distributed_xacts"):
			return singleRowTyped([]fieldDesc{
				int8Field("a"), int8Field("b"), int8Field("c"),
			}, []string{"1", "2", "3"})
		case strings.Contains(query, "xact_start"):
			return singleRowTyped([]fieldDesc{float8Field("o")}, []string{"5"})
		case strings.Contains(query, "skew_coefficients"):
			return rowsResponseTyped([]fieldDesc{
				textField("s"), textField("t"), float8Field("c"),
			}, [][]string{{"public", "t1", "0.1"}})
		default:
			return execResponse("SELECT 0")
		}
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	mc := newMetricCollectors(reg)
	mc.collectAll(context.Background(), conn, time.Second, testLogger())
}

// --- prometheus metric value helpers ---

func testGauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	return readMetricValue(t, g)
}

func testGaugeVec(t *testing.T, gv *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	g, err := gv.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	return readMetricValue(t, g)
}

func readMetricValue(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	c.Collect(ch)
	close(ch)
	m := <-ch
	var metric dto.Metric
	require.NoError(t, m.Write(&metric))
	if metric.Gauge != nil {
		return metric.Gauge.GetValue()
	}
	if metric.Counter != nil {
		return metric.Counter.GetValue()
	}
	return 0
}
