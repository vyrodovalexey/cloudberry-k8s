package main

// Tests for the E-2 testability seams: parseConfigFromFlagSet (flag-set
// injection), the collectLoop cleanup-interval seam, and the
// connectWithBackoff retry/cancel paths. These close the parse-error,
// retention-cleanup, and reconnect branches that were unreachable through
// the process-global flag.CommandLine.

import (
	"context"
	"flag"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestFlagSet returns a quiet flag set for parse tests.
func newTestFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func TestParseConfigFromFlagSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, cfg *exporterConfig)
	}{
		{
			name: "defaults with DSN from env",
			args: nil,
			env:  map[string]string{envDataSourceName: "host=db"},
			check: func(t *testing.T, cfg *exporterConfig) {
				assert.Equal(t, ":9188", cfg.listenAddress)
				assert.Equal(t, 5*time.Second, cfg.samplingInterval)
				assert.Equal(t, time.Second, cfg.slowQueryThreshold)
				assert.Equal(t, "host=db", cfg.dsn)
				assert.False(t, cfg.planCollection)
				assert.Equal(t, defaultHistoryRetention, cfg.historyRetention)
				assert.Equal(t, defaultCleanupInterval, cfg.cleanupInterval)
			},
		},
		{
			name: "explicit flags override defaults",
			args: []string{
				"-listen-address=127.0.0.1:9999",
				"-sampling-interval=42ms",
				"-slow-query-threshold=2s",
				"-plan-collection=true",
				"-history-retention=2w",
			},
			env: map[string]string{},
			check: func(t *testing.T, cfg *exporterConfig) {
				assert.Equal(t, "127.0.0.1:9999", cfg.listenAddress)
				assert.Equal(t, 42*time.Millisecond, cfg.samplingInterval)
				assert.Equal(t, 2*time.Second, cfg.slowQueryThreshold)
				assert.True(t, cfg.planCollection)
				assert.Equal(t, 336*time.Hour, cfg.historyRetention)
			},
		},
		{
			name: "missing DSN starts degraded (no error)",
			args: nil,
			env:  map[string]string{},
			check: func(t *testing.T, cfg *exporterConfig) {
				assert.Empty(t, cfg.dsn)
			},
		},
		{
			name:    "empty listen-address is rejected",
			args:    []string{"-listen-address="},
			env:     map[string]string{},
			wantErr: "listen-address must not be empty",
		},
		{
			name:    "invalid history-retention is rejected",
			args:    []string{"-history-retention=bogus"},
			env:     map[string]string{},
			wantErr: "parsing history-retention",
		},
		{
			name:    "unknown flag is a parse error",
			args:    []string{"--no-such-flag"},
			env:     map[string]string{},
			wantErr: "not defined",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			getenv := func(key string) string { return tc.env[key] }

			cfg, err := parseConfigFromFlagSet(newTestFlagSet(), tc.args, getenv)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Nil(t, cfg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			tc.check(t, cfg)
		})
	}
}

// ----------------------------------------------------------------------------
// run() error and degraded-mode branches
// ----------------------------------------------------------------------------

func TestRun_ParseError(t *testing.T) {
	err := run(context.Background(), []string{"--definitely-not-a-flag"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing configuration")
}

func TestRun_HelpReturnsErrHelp(t *testing.T) {
	err := run(context.Background(), []string{"-h"})
	require.ErrorIs(t, err, flag.ErrHelp)
}

func TestRun_DegradedModeNoDSN(t *testing.T) {
	t.Setenv(envDataSourceName, "")
	listenAddr := reserveListenAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, []string{"-listen-address=" + listenAddr})
	}()

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", listenAddr, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond, "HTTP server never came up in degraded mode")

	cancel()
	select {
	case err := <-runErrCh:
		require.NoError(t, err, "degraded mode (no DSN) must run and shut down cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestRun_InitialConnectFails_ContinuesInBackground(t *testing.T) {
	// DSN points at a port nothing listens on; connectWithBackoff keeps
	// retrying until the context is canceled, then run proceeds through the
	// degraded path and shuts down cleanly.
	closedAddr := reserveListenAddr(t)
	t.Setenv(envDataSourceName, "host=127.0.0.1 port="+portOf(t, closedAddr)+
		" connect_timeout=1 sslmode=disable")

	prev := connInitialBackoff
	connInitialBackoff = time.Millisecond
	defer func() { connInitialBackoff = prev }()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, []string{"-listen-address=" + reserveListenAddr(t)})
	}()

	select {
	case err := <-runErrCh:
		require.NoError(t, err, "initial connect failure must degrade, not fail run")
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRun_EnsureTableFails_Warns(t *testing.T) {
	addr, cleanup := mockPGServer(t, func(query string) []byte {
		if strings.Contains(query, "CREATE") {
			return errorResponseMsg("permission denied")
		}
		return countResponder(query)
	})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	t.Setenv(envDataSourceName, "host="+host+" port="+port+
		" dbname=testdb user=testuser password=testpass sslmode=disable")

	listenAddr := reserveListenAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, []string{"-listen-address=" + listenAddr})
	}()

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", listenAddr, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case runErr := <-runErrCh:
		require.NoError(t, runErr, "an ensureTable failure must only warn")
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestRun_ListenAddressBusy_ReturnsError(t *testing.T) {
	t.Setenv(envDataSourceName, "")

	// Hold the port so ListenAndServe fails.
	lst, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lst.Close() }()

	err = run(context.Background(), []string{"-listen-address=" + lst.Addr().String()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP server error")
}

// portOf extracts the port from a host:port string.
func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return port
}

// ----------------------------------------------------------------------------
// connectWithBackoff retry paths
// ----------------------------------------------------------------------------

// flakyPGListener accepts TCP connections, closing the first failCount of
// them immediately (so the pgx handshake fails) and serving the mock protocol
// afterwards.
func flakyPGListener(t *testing.T, failCount int, responder func(string) []byte) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var attempts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			if int(attempts.Add(1)) <= failCount {
				_ = conn.Close() // handshake failure → connect error
				continue
			}
			go handleConn(conn, responder)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func TestConnectWithBackoff_RetryThenSuccess(t *testing.T) {
	addr, cleanup := flakyPGListener(t, 1, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	prev := connInitialBackoff
	connInitialBackoff = time.Millisecond
	defer func() { connInitialBackoff = prev }()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := connectWithBackoff(ctx, dsn, testLogger())
	require.NoError(t, err, "second attempt must succeed after the first refusal")
	require.NotNil(t, conn)
	_ = conn.Close(context.Background())
}

func TestConnectWithBackoff_CancelDuringBackoff(t *testing.T) {
	// Every connection attempt fails; a generous backoff guarantees the
	// cancellation lands inside the backoff wait.
	addr, cleanup := flakyPGListener(t, 1<<30, nil)
	defer cleanup()

	prev := connInitialBackoff
	connInitialBackoff = 30 * time.Second
	defer func() { connInitialBackoff = prev }()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable"

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel shortly after the first attempt has failed and the backoff
		// wait has begun.
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, err = connectWithBackoff(ctx, dsn, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled during backoff")
}

// ----------------------------------------------------------------------------
// collectLoop retention-cleanup tick (cleanup-interval seam)
// ----------------------------------------------------------------------------

func TestCollectLoop_CleanupTickRunsRetentionCleanup(t *testing.T) {
	var mu sync.Mutex
	var sawDelete bool
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "DELETE FROM cloudberry_query_history") {
			mu.Lock()
			sawDelete = true
			mu.Unlock()
			return execResponse("DELETE 3")
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{
		dsn:              "host=x",
		samplingInterval: time.Hour, // never fires during the test
		cleanupInterval:  5 * time.Millisecond,
		historyRetention: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		collectLoop(ctx, cfg, conn, m, testLogger(), hc)
		close(done)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return sawDelete
	}, 5*time.Second, 5*time.Millisecond, "cleanup tick never executed the retention DELETE")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collectLoop did not return after cancel")
	}
}

func TestCollectLoop_ZeroCleanupIntervalDefaults(t *testing.T) {
	// A zero cleanupInterval (e.g. an exporterConfig constructed in code
	// rather than via parseConfig) must fall back to the 1h default rather
	// than panic in time.NewTicker.
	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second)
	cfg := &exporterConfig{
		dsn:              "",
		samplingInterval: time.Millisecond,
		cleanupInterval:  0,
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
