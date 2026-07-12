package main

// Tests for the L-5/G-5 log-level and G-3 telemetry ENV-over-flag resolution
// (handoff gaps UT-23..UT-25), the collectOnce scrape-failure branch (UT-26),
// the setupTelemetry cleanup contract (UT-27) and goleak verification of the
// run() exit paths (UT-28).

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// mapGetenv returns a getenv func backed by a fixed map (pure seam, no
// t.Setenv needed).
func mapGetenv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

// ---------------------------------------------------------------------------
// UT-23: resolveLogLevel ENV>flag precedence and validation
// ---------------------------------------------------------------------------

func TestResolveLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		flagValue string
		env       map[string]string
		want      string
		wantErr   string
	}{
		{
			name:      "env empty: flag value used",
			flagValue: "warn",
			env:       map[string]string{},
			want:      "warn",
		},
		{
			name:      "LOG_LEVEL=debug overrides -log-level=info (ENV beats flag)",
			flagValue: "info",
			env:       map[string]string{envLogLevel: "debug"},
			want:      "debug",
		},
		{
			name:      "case-insensitive WARN normalizes to warn",
			flagValue: "info",
			env:       map[string]string{envLogLevel: "WARN"},
			want:      "warn",
		},
		{
			name:      "case-insensitive flag value Error normalizes to error",
			flagValue: "Error",
			env:       map[string]string{},
			want:      "error",
		},
		{
			name:      "invalid value errors naming the offender",
			flagValue: "verbose",
			env:       map[string]string{},
			wantErr:   `"verbose"`,
		},
		{
			name:      "invalid env value errors naming the offending env value",
			flagValue: "info",
			env:       map[string]string{envLogLevel: "loud"},
			wantErr:   `"loud"`,
		},
		{
			name:      "invalid flag with valid env: env wins, no error",
			flagValue: "bogus",
			env:       map[string]string{envLogLevel: "error"},
			want:      "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveLogLevel(tt.flagValue, mapGetenv(tt.env))

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "log-level must be one of debug, info, warn, error")
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// UT-24: resolveTelemetrySettings ENV>flag precedence and validation
// ---------------------------------------------------------------------------

func TestResolveTelemetrySettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		flagEnabled  bool
		flagEndpoint string
		env          map[string]string
		wantEnabled  bool
		wantEndpoint string
		wantErr      string
	}{
		{
			name:         "defaults: disabled with empty endpoint (disabled-by-default contract)",
			flagEnabled:  false,
			flagEndpoint: "",
			env:          map[string]string{},
			wantEnabled:  false,
			wantEndpoint: "",
		},
		{
			name:        "TELEMETRY_ENABLED=true overrides flag=false",
			flagEnabled: false,
			env:         map[string]string{envTelemetryEnabled: "true"},
			wantEnabled: true,
		},
		{
			name:        "TELEMETRY_ENABLED=false overrides flag=true",
			flagEnabled: true,
			env:         map[string]string{envTelemetryEnabled: "false"},
			wantEnabled: false,
		},
		{
			name:        "garbage TELEMETRY_ENABLED errors naming the variable",
			flagEnabled: false,
			env:         map[string]string{envTelemetryEnabled: "garbage"},
			wantErr:     envTelemetryEnabled,
		},
		{
			name:         "OTLP_ENDPOINT env overrides the flag verbatim",
			flagEnabled:  true,
			flagEndpoint: "flag-endpoint:4317",
			env:          map[string]string{envOTLPEndpoint: "env-endpoint:4317"},
			wantEnabled:  true,
			wantEndpoint: "env-endpoint:4317",
		},
		{
			name:         "no env: flag endpoint kept",
			flagEnabled:  true,
			flagEndpoint: "collector:4317",
			env:          map[string]string{},
			wantEnabled:  true,
			wantEndpoint: "collector:4317",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enabled, endpoint, err := resolveTelemetrySettings(
				tt.flagEnabled, tt.flagEndpoint, mapGetenv(tt.env))

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Contains(t, err.Error(), `"garbage"`)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, enabled)
			assert.Equal(t, tt.wantEndpoint, endpoint)
		})
	}
}

// ---------------------------------------------------------------------------
// UT-25: parseConfigFromFlagSet propagates the new error paths and env values
// ---------------------------------------------------------------------------

func TestParseConfigFromFlagSet_LogLevelAndTelemetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, cfg *exporterConfig)
	}{
		{
			name:    "invalid -log-level flag propagates the resolve error",
			args:    []string{"-log-level=bogus"},
			env:     map[string]string{},
			wantErr: "log-level must be one of",
		},
		{
			name:    "invalid TELEMETRY_ENABLED env propagates the resolve error",
			args:    nil,
			env:     map[string]string{envTelemetryEnabled: "notabool"},
			wantErr: envTelemetryEnabled,
		},
		{
			name: "env overrides land in the parsed config",
			args: []string{"-log-level=info", "-telemetry-enabled=false", "-otlp-endpoint=flag:4317"},
			env: map[string]string{
				envLogLevel:         "DEBUG",
				envTelemetryEnabled: "true",
				envOTLPEndpoint:     "env:4317",
			},
			check: func(t *testing.T, cfg *exporterConfig) {
				assert.Equal(t, "debug", cfg.logLevel)
				assert.True(t, cfg.telemetryEnabled)
				assert.Equal(t, "env:4317", cfg.otlpEndpoint)
			},
		},
		{
			name: "defaults: info level, telemetry disabled",
			args: nil,
			env:  map[string]string{},
			check: func(t *testing.T, cfg *exporterConfig) {
				assert.Equal(t, "info", cfg.logLevel)
				assert.False(t, cfg.telemetryEnabled, "telemetry must be disabled by default")
				assert.Empty(t, cfg.otlpEndpoint)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parseConfigFromFlagSet(newTestFlagSet(), tt.args, mapGetenv(tt.env))

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, cfg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			tt.check(t, cfg)
		})
	}
}

// ---------------------------------------------------------------------------
// UT-26: collectOnce scrape failure closes the broken connection
// ---------------------------------------------------------------------------

func TestCollectOnce_ScrapeFailure_ClosesBrokenConnForReconnect(t *testing.T) {
	// Arrange: the connection is alive (ping succeeds) but every metric
	// scrape query fails — the mid-cycle failure mode, distinct from the
	// dead-connection path.
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "count(*)") {
			return errorResponseMsg("pg_stat_activity unavailable")
		}
		return execResponse("SELECT 1") // ping and everything else succeed
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, nil)
	cfg := &exporterConfig{dsn: "host=x", slowQueryThreshold: time.Second}

	// Act.
	got := collectOnce(context.Background(), cfg, conn, m, testLogger(), hc)

	// Assert: the cycle reports down, returns nil so the NEXT cycle
	// reconnects, and the broken connection has really been closed.
	assert.Nil(t, got, "a failed scrape must drop the connection so the next cycle reconnects")
	assert.Equal(t, float64(0), testGauge(t, m.up), "up must report 0 after a scrape failure")
	assert.True(t, conn.IsClosed(), "the broken connection must be closed (no fd leak)")
}

// ---------------------------------------------------------------------------
// UT-27: setupTelemetry cleanup contract
// ---------------------------------------------------------------------------

func TestSetupTelemetry(t *testing.T) {
	// Restore the global noop tracer provider whatever the subtests install.
	t.Cleanup(func() {
		_, _ = telemetry.InitTracer(context.Background(), telemetry.Config{Enabled: false})
	})

	t.Run("disabled: returns usable no-op cleanup", func(t *testing.T) {
		cfg := &exporterConfig{telemetryEnabled: false}

		cleanup := setupTelemetry(context.Background(), cfg, testLogger())

		require.NotNil(t, cleanup, "cleanup must never be nil")
		cleanup() // must not panic and must return promptly
	})

	t.Run("enabled with unreachable endpoint: init lazy, cleanup shuts down", func(t *testing.T) {
		// gRPC OTLP export is lazy: construction succeeds even though nothing
		// listens on the endpoint; the returned cleanup must still complete.
		lst, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		endpoint := lst.Addr().String()
		require.NoError(t, lst.Close()) // free the port: guaranteed-unreachable

		cfg := &exporterConfig{telemetryEnabled: true, otlpEndpoint: endpoint}

		cleanup := setupTelemetry(context.Background(), cfg, testLogger())

		require.NotNil(t, cleanup)
		done := make(chan struct{})
		go func() {
			cleanup()
			close(done)
		}()
		select {
		case <-done:
			// Shutdown completed within its own bounded context.
		case <-time.After(2 * tracerShutdownTimeout):
			t.Fatal("telemetry cleanup did not complete within twice the shutdown timeout")
		}
	})
}

// ---------------------------------------------------------------------------
// UT-28: goleak on run() exit paths (locks the L-4 symmetric-join guarantee)
// ---------------------------------------------------------------------------

func TestRun_ListenBusyError_NoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	t.Setenv(envDataSourceName, "")

	// Hold the port so ListenAndServe fails and run() takes the error path.
	lst, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lst.Close() }()

	err = run(context.Background(), []string{"-listen-address=" + lst.Addr().String()})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP server error")
}

func TestRun_CleanShutdown_NoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	t.Setenv(envDataSourceName, "")
	listenAddr := reserveListenAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, []string{"-listen-address=" + listenAddr})
	}()

	// Wait for the HTTP server, then trigger the clean-shutdown path.
	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", listenAddr, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond, "HTTP server never came up")

	cancel()
	select {
	case err := <-runErrCh:
		require.NoError(t, err, "clean shutdown must not error")
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}
