package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExporterMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	require.NotNil(t, m)
	assert.NotNil(t, m.activeQueries)
	assert.NotNil(t, m.idleSessions)
	assert.NotNil(t, m.slowQueries)
	assert.NotNil(t, m.totalConnections)
	assert.NotNil(t, m.up)
	assert.NotNil(t, m.collectors)
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	handleHealth(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "ok", rr.Body.String())
}

func TestAddJitter(t *testing.T) {
	t.Parallel()
	base := time.Second
	for i := 0; i < 50; i++ {
		got := addJitter(base)
		assert.GreaterOrEqual(t, got, base)
		assert.LessOrEqual(t, got, base+time.Duration(float64(base)*jitterFraction))
	}
}

// TestRun exercises the run() entry point end-to-end against a mock PG server.
//
// run() calls parseConfig(), which registers flags on the global
// flag.CommandLine. Because that registration panics on a second invocation
// ("flag redefined"), parseConfig may only be invoked once per test binary.
// We therefore drive parseConfig exclusively through run() here and assert the
// resulting configuration via a side channel rather than calling parseConfig
// directly in a separate test.
func TestRun(t *testing.T) {
	addr, cleanup := mockPGServer(t, func(query string) []byte {
		if strings.Contains(query, "INSERT") || strings.Contains(query, "CREATE") {
			return execResponse("OK")
		}
		return countResponder(query)
	})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable"
	t.Setenv(envDataSourceName, dsn)

	// Reserve a free TCP port, then release it so run()'s HTTP server can bind
	// it deterministically. Using a concrete port (rather than ":0") avoids any
	// dependency on run() exposing its chosen ephemeral port back to the test.
	lst, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listenAddr := lst.Addr().String()
	require.NoError(t, lst.Close())

	// Provide CLI args so flag.Parse picks the reserved listen address and a
	// short sampling interval. Restore os.Args afterwards. Reset the global
	// flag.CommandLine to a fresh FlagSet so parseConfig can re-register its
	// flags without panicking ("flag redefined"); this keeps the test safe under
	// repeated invocation (e.g. -count>1).
	origArgs := os.Args
	origFlags := flag.CommandLine
	defer func() {
		os.Args = origArgs
		flag.CommandLine = origFlags
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cloudberry-query-exporter",
		"-listen-address=" + listenAddr,
		"-sampling-interval=20ms",
	}

	// Drive run() in a goroutine and cancel once the HTTP /health endpoint is
	// serving, so the graceful-shutdown path is exercised without relying on a
	// fixed wall-clock deadline (which can be flaky under load / -race).
	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- run(ctx) }()

	require.Eventually(t, func() bool {
		resp, getErr := http.Get("http://" + listenAddr + "/health") //nolint:noctx // test poll
		if getErr != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond, "health endpoint never became ready")

	cancel()

	select {
	case runErr := <-runErrCh:
		require.NoError(t, runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after context cancellation")
	}
}

func TestQueryCount(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return singleRowTyped([]fieldDesc{int8Field("count")}, []string{"42"})
	})
	defer cleanup()

	count, err := queryCount(context.Background(), conn, queryActiveCount)
	require.NoError(t, err)
	assert.Equal(t, int64(42), count)
}

func TestQueryCount_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	_, err := queryCount(context.Background(), conn, queryActiveCount)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executing query")
}

func TestQueryCountWithParam(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return singleRowTyped([]fieldDesc{int8Field("count")}, []string{"7"})
	})
	defer cleanup()

	count, err := queryCountWithParam(context.Background(), conn, querySlowCount, "1s")
	require.NoError(t, err)
	assert.Equal(t, int64(7), count)
}

func TestQueryCountWithParam_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	_, err := queryCountWithParam(context.Background(), conn, querySlowCount, "1s")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executing parameterized query")
}

// countResponder returns count results for base metric queries and empty
// results for the extended collectors so scrapeMetrics can run end-to-end.
func countResponder(query string) []byte {
	switch {
	case strings.Contains(query, "count(*)"):
		return singleRowTyped([]fieldDesc{int8Field("count")}, []string{"3"})
	case strings.Contains(query, "backend_type"):
		return rowsResponseTyped([]fieldDesc{
			textField("state"), textField("datname"), textField("usename"),
			float8Field("d"), textField("w"),
		}, [][]string{})
	default:
		return execResponse("SELECT 0")
	}
}

func TestScrapeMetrics(t *testing.T) {
	conn, cleanup := newMockConn(t, countResponder)
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	err := scrapeMetrics(context.Background(), conn, time.Second, m, testLogger())
	require.NoError(t, err)
	assert.Equal(t, float64(3), testGauge(t, m.activeQueries))
}

func TestScrapeMetrics_ActiveError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	err := scrapeMetrics(context.Background(), conn, time.Second, m, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying active count")
}

func TestScrapeMetrics_SlowError(t *testing.T) {
	callNum := 0
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "interval") {
			return errorResponseMsg("slow query failed")
		}
		callNum++
		return singleRowTyped([]fieldDesc{int8Field("count")}, []string{"1"})
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	err := scrapeMetrics(context.Background(), conn, time.Second, m, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying slow count")
}

func TestEnsureConnection_NoDSN(t *testing.T) {
	t.Setenv(envDataSourceName, "")
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	got := ensureConnection(context.Background(), "", nil, m, testLogger())
	assert.Nil(t, got)
}

func TestEnsureConnection_ExistingAlive(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	got := ensureConnection(context.Background(), "host=x", conn, m, testLogger())
	assert.Equal(t, conn, got)
}

func TestEnsureConnection_ReconnectFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	// Bad DSN, no existing conn -> reconnect fails -> nil.
	got := ensureConnection(context.Background(), "host=127.0.0.1 port=1 connect_timeout=1 sslmode=disable",
		nil, m, testLogger())
	assert.Nil(t, got)
}

func TestEnsureConnection_DSNFromEnv(t *testing.T) {
	t.Setenv(envDataSourceName, "host=127.0.0.1 port=1 connect_timeout=1 sslmode=disable")
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	got := ensureConnection(context.Background(), "", nil, m, testLogger())
	assert.Nil(t, got)
}

func TestCollectOnce_NoConnection(t *testing.T) {
	t.Setenv(envDataSourceName, "")
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{dsn: "", slowQueryThreshold: time.Second}
	got := collectOnce(context.Background(), cfg, nil, m, testLogger(), hc)
	assert.Nil(t, got)
	assert.Equal(t, float64(0), testGauge(t, m.up))
}

func TestCollectOnce_Success(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "INSERT") {
			return execResponse("INSERT 0 1")
		}
		return countResponder(query)
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{dsn: "host=x", slowQueryThreshold: time.Second}
	got := collectOnce(context.Background(), cfg, conn, m, testLogger(), hc)
	assert.NotNil(t, got)
	assert.Equal(t, float64(1), testGauge(t, m.up))
}

func TestCollectOnce_ScrapeError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("scrape failed")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{dsn: "host=x", slowQueryThreshold: time.Second}
	got := collectOnce(context.Background(), cfg, conn, m, testLogger(), hc)
	assert.Nil(t, got)
	assert.Equal(t, float64(0), testGauge(t, m.up))
}

func TestConnectWithBackoff_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := connectWithBackoff(ctx, "host=x", testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestConnectWithBackoff_Success(t *testing.T) {
	addr, cleanup := mockPGServer(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := connectWithBackoff(ctx, dsn, testLogger())
	require.NoError(t, err)
	require.NotNil(t, conn)
	_ = conn.Close(context.Background())
}

func TestCloseConn_Nil(t *testing.T) {
	t.Parallel()
	// Should not panic.
	closeConn(nil, testLogger())
}

func TestCloseConn_Open(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()
	closeConn(conn, testLogger())
}

func TestShutdownServer(t *testing.T) {
	t.Parallel()
	srv := &http.Server{Addr: "127.0.0.1:0"} //nolint:gosec // test server
	err := shutdownServer(srv, testLogger())
	require.NoError(t, err)
}

func TestCollectLoop_ContextCancel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{
		dsn:              "",
		samplingInterval: time.Millisecond,
		historyRetention: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		collectLoop(ctx, cfg, nil, m, testLogger(), hc)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collectLoop did not return after cancel")
	}
}
