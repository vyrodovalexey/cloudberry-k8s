// Package main is the entry point for the cloudberry-query-exporter.
// It exposes Prometheus metrics about Cloudberry Database query activity
// by periodically querying pg_stat_activity.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// version is set via ldflags at build time (e.g. -X main.version=...).
//
//nolint:gochecknoglobals // set by ldflags
var version = "dev"

const (
	metricsNamespace = "cloudberry"
	// metricsSubsystem is the subsystem for exporter self-metrics
	// (cloudberry_query_exporter_*).
	metricsSubsystem = "query_exporter"

	// Exponential backoff parameters for database connection retries.
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2.0
	jitterFraction = 0.1

	// HTTP server timeouts.
	httpReadTimeout     = 5 * time.Second
	httpWriteTimeout    = 10 * time.Second
	httpIdleTimeout     = 60 * time.Second
	httpShutdownTimeout = 5 * time.Second

	// envDataSourceName is the environment variable for the PostgreSQL connection string.
	envDataSourceName = "DATA_SOURCE_NAME"

	// envLogLevel is the environment variable overriding the -log-level flag
	// (ENV > flag, project convention — L-5/G-5).
	envLogLevel = "LOG_LEVEL"

	// envTelemetryEnabled and envOTLPEndpoint override the -telemetry-enabled
	// and -otlp-endpoint flags (ENV > flag, project convention — G-3).
	envTelemetryEnabled = "TELEMETRY_ENABLED"
	envOTLPEndpoint     = "OTLP_ENDPOINT"

	// exporterServiceName identifies this binary in exported traces; it is
	// also the tracer name for exporter spans (G-3).
	exporterServiceName = "cloudberry-query-exporter"

	// exporterSamplingRate is the fixed trace sampling rate (the project-wide
	// default, mirroring internal/config's telemetry.sampling-rate). Only the
	// enabled/endpoint knobs are exposed in this pass.
	exporterSamplingRate = 1.0

	// tracerShutdownTimeout bounds the OTLP tracer shutdown on exit.
	tracerShutdownTimeout = 5 * time.Second

	// defaultHistoryRetention is the default retention period for query history entries.
	defaultHistoryRetention = 30 * 24 * time.Hour

	// defaultCleanupInterval is how often the retention cleanup runs.
	defaultCleanupInterval = 1 * time.Hour

	// hoursPerDay and hoursPerWeek are used to expand the custom "d" and "w" suffixes.
	hoursPerDay  = 24
	hoursPerWeek = 7 * hoursPerDay
)

// SQL queries used to collect metrics from pg_stat_activity.
// Each query is a simple aggregate that returns a single integer value.
const (
	queryActiveCount = "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"
	queryIdleCount   = "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'"
	queryTotalConns  = "SELECT count(*) FROM pg_stat_activity"
)

// querySlowCount returns the SQL query for counting slow queries.
// The threshold is injected as a parameter placeholder ($1) to prevent SQL injection.
const querySlowCount = `SELECT count(*) FROM pg_stat_activity
WHERE state = 'active'
  AND query_start IS NOT NULL
  AND now() - query_start > $1::interval`

// exporterMetrics holds all Prometheus metrics exposed by the exporter.
type exporterMetrics struct {
	activeQueries    prometheus.Gauge
	idleSessions     prometheus.Gauge
	slowQueries      prometheus.Gauge
	totalConnections prometheus.Gauge
	up               prometheus.Gauge
	scrapeDuration   prometheus.Histogram
	// historyErrors counts history-pipeline failures per stage
	// (snapshot|insert|explain), E2:
	// cloudberry_query_exporter_history_errors_total{stage}.
	historyErrors *prometheus.CounterVec
	collectors    *metricCollectors
}

// newExporterMetrics creates and registers all Prometheus metrics,
// including the extended metric collectors for Cloudberry-specific views.
func newExporterMetrics(reg prometheus.Registerer) *exporterMetrics {
	m := &exporterMetrics{
		activeQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "active_queries",
			Help:      "Number of currently active queries.",
		}),
		idleSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "idle_sessions",
			Help:      "Number of currently idle sessions.",
		}),
		slowQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "slow_queries",
			Help:      "Number of queries running longer than the configured threshold.",
		}),
		totalConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "total_connections",
			Help:      "Total number of database connections.",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "up",
			Help:      "Whether the database connection is healthy (1=up, 0=down).",
		}),
		scrapeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "scrape_duration_seconds",
			Help:      "Duration of database metric scrape in seconds.",
			Buckets:   prometheus.DefBuckets,
		}),
		historyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "history_errors_total",
			Help:      "Total number of query-history pipeline failures by stage (snapshot|insert|explain).",
		}, []string{"stage"}),
		collectors: newMetricCollectors(reg),
	}

	baseCollectors := []prometheus.Collector{
		m.activeQueries,
		m.idleSessions,
		m.slowQueries,
		m.totalConnections,
		m.up,
		m.scrapeDuration,
		m.historyErrors,
	}
	for _, c := range baseCollectors {
		reg.MustRegister(c)
	}

	return m
}

// exporterConfig holds the parsed command-line flags and environment configuration.
type exporterConfig struct {
	listenAddress      string
	samplingInterval   time.Duration
	slowQueryThreshold time.Duration
	dsn                string
	planCollection     bool
	historyRetention   time.Duration
	// cleanupInterval is how often the retention cleanup tick fires in
	// collectLoop. It is not exposed as a flag; the default is kept at one
	// hour and tests inject shorter intervals (testability seam, E-2).
	cleanupInterval time.Duration
	// logLevel is the effective logging level (debug|info|warn|error),
	// resolved with ENV (LOG_LEVEL) taking priority over the -log-level flag
	// (L-5/G-5).
	logLevel string
	// telemetryEnabled turns optional OTLP tracing on (G-3); disabled by
	// default so behavior is unchanged unless explicitly requested.
	telemetryEnabled bool
	// otlpEndpoint is the OTLP collector endpoint used when telemetry is
	// enabled.
	otlpEndpoint string
}

// validLogLevels are the accepted -log-level / LOG_LEVEL values (validated
// case-insensitively, mirroring internal/config). Immutable lookup table.
var validLogLevels = map[string]bool{
	"debug": true, "info": true, "warn": true, "error": true,
}

// resolveLogLevel applies the ENV-over-flag precedence and validates the
// result case-insensitively. It returns the normalized (lower-case) level.
func resolveLogLevel(flagValue string, getenv func(string) string) (string, error) {
	level := flagValue
	if env := getenv(envLogLevel); env != "" {
		level = env
	}
	normalized := strings.ToLower(level)
	if !validLogLevels[normalized] {
		return "", fmt.Errorf("log-level must be one of debug, info, warn, error; got %q", level)
	}
	return normalized, nil
}

// resolveTelemetrySettings applies the ENV-over-flag precedence for the
// optional OTLP tracing knobs (G-3). TELEMETRY_ENABLED must parse as a
// boolean when set; OTLP_ENDPOINT overrides the flag verbatim.
func resolveTelemetrySettings(
	flagEnabled bool,
	flagEndpoint string,
	getenv func(string) string,
) (enabled bool, endpoint string, err error) {
	enabled = flagEnabled
	if env := getenv(envTelemetryEnabled); env != "" {
		parsed, parseErr := strconv.ParseBool(env)
		if parseErr != nil {
			return false, "", fmt.Errorf("invalid %s value %q: %w", envTelemetryEnabled, env, parseErr)
		}
		enabled = parsed
	}
	endpoint = flagEndpoint
	if env := getenv(envOTLPEndpoint); env != "" {
		endpoint = env
	}
	return enabled, endpoint, nil
}

// parseRetention converts a retention string into a time.Duration.
//
// It accepts standard Go durations (handled by time.ParseDuration, e.g. "720h",
// "1000ms") as well as the CRD-friendly "d" (days) and "w" (weeks) suffixes
// (e.g. "30d" -> 720h, "2w" -> 336h). An empty string returns the default
// retention period. Negative or otherwise invalid values are rejected with a
// clear error.
func parseRetention(s string) (time.Duration, error) {
	if s == "" {
		return defaultHistoryRetention, nil
	}

	if unit := s[len(s)-1]; unit == 'd' || unit == 'w' {
		value, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid retention %q: %w", s, err)
		}
		if value < 0 {
			return 0, fmt.Errorf("invalid retention %q: must not be negative", s)
		}
		hours := hoursPerDay
		if unit == 'w' {
			hours = hoursPerWeek
		}
		return time.Duration(value) * time.Duration(hours) * time.Hour, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid retention %q: %w", s, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid retention %q: must not be negative", s)
	}
	return d, nil
}

// parseConfigFromFlagSet parses the exporter command-line flags from the given
// flag set and reads the DATA_SOURCE_NAME environment variable through getenv.
// The environment variable takes priority over any default for the DSN.
//
// The flag set / args / getenv injection is a testability seam (E-2): the
// production caller passes a fresh flag set with os.Args[1:] and os.Getenv,
// while tests can drive every parse-error branch repeatedly without touching
// the process-global flag.CommandLine (which panics on re-registration).
func parseConfigFromFlagSet(fs *flag.FlagSet, args []string, getenv func(string) string) (*exporterConfig, error) {
	listenAddress := fs.String("listen-address", ":9188", "Address to listen on for HTTP requests")
	samplingInterval := fs.Duration("sampling-interval", 5*time.Second, "Interval between metric collection cycles")
	slowQueryThreshold := fs.Duration(
		"slow-query-threshold",
		1000*time.Millisecond,
		"Duration threshold for classifying a query as slow",
	)
	planCollection := fs.Bool("plan-collection", false, "Enable EXPLAIN plan collection for slow queries")
	historyRetention := fs.String(
		"history-retention", "30d",
		`Retention period for query history entries (e.g. "30d", "2w", "720h")`,
	)
	logLevel := fs.String("log-level", "info", "Logging level (debug, info, warn, error)")
	telemetryEnabled := fs.Bool("telemetry-enabled", false, "Enable OTLP trace export")
	otlpEndpoint := fs.String("otlp-endpoint", "", "OTLP collector endpoint for trace export")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *listenAddress == "" {
		return nil, fmt.Errorf("listen-address must not be empty")
	}

	retention, err := parseRetention(*historyRetention)
	if err != nil {
		return nil, fmt.Errorf("parsing history-retention: %w", err)
	}

	// LOG_LEVEL (env) beats -log-level (flag), matching the project-wide
	// config precedence (L-5/G-5).
	level, err := resolveLogLevel(*logLevel, getenv)
	if err != nil {
		return nil, err
	}

	// TELEMETRY_ENABLED / OTLP_ENDPOINT (env) beat their flags (G-3).
	traceEnabled, traceEndpoint, err := resolveTelemetrySettings(*telemetryEnabled, *otlpEndpoint, getenv)
	if err != nil {
		return nil, err
	}

	dsn := getenv(envDataSourceName)
	if dsn == "" {
		slog.Warn("DATA_SOURCE_NAME not set, starting in degraded mode (will retry reading env)")
	}

	return &exporterConfig{
		listenAddress:      *listenAddress,
		samplingInterval:   *samplingInterval,
		slowQueryThreshold: *slowQueryThreshold,
		dsn:                dsn,
		planCollection:     *planCollection,
		historyRetention:   retention,
		cleanupInterval:    defaultCleanupInterval,
		logLevel:           level,
		telemetryEnabled:   traceEnabled,
		otlpEndpoint:       traceEndpoint,
	}, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("exporter failed", "error", err)
		cancel()
		os.Exit(1) //nolint:gocritic // intentional exit after cancel
	}
}

func run(ctx context.Context, args []string) error {
	// A fresh flag set keeps run re-entrant for tests; ContinueOnError makes
	// parse failures observable as returned errors (main handles ErrHelp).
	fs := flag.NewFlagSet("cloudberry-query-exporter", flag.ContinueOnError)
	cfg, err := parseConfigFromFlagSet(fs, args, os.Getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return fmt.Errorf("parsing configuration: %w", err)
	}

	// Build the JSON logger at the configured level (L-5/G-5): the shared
	// util.NewLogger parses the level, making logger.Debug diagnostics
	// reachable in the field via -log-level/LOG_LEVEL.
	logger := util.NewLogger(cfg.logLevel, util.LogFormatJSON, os.Stdout)
	slog.SetDefault(logger)

	logger.Info("starting cloudberry-query-exporter",
		"version", version,
		"listenAddress", cfg.listenAddress,
		"samplingInterval", cfg.samplingInterval.String(),
		"slowQueryThreshold", cfg.slowQueryThreshold.String(),
		"logLevel", cfg.logLevel,
		"telemetryEnabled", cfg.telemetryEnabled,
	)

	// Optional OTLP tracing (G-3): disabled by default, so behavior is
	// unchanged unless -telemetry-enabled/TELEMETRY_ENABLED is set. The
	// returned cleanup joins the tracer shutdown on all exit paths.
	defer setupTelemetry(ctx, cfg, logger)()

	// Register Prometheus metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())
	metrics := newExporterMetrics(reg)

	// Establish initial database connection with exponential backoff.
	var conn *pgx.Conn
	if cfg.dsn == "" {
		logger.Warn("no DSN configured, metrics will report up=0 until DATA_SOURCE_NAME is set")
		metrics.up.Set(0)
	} else {
		var connErr error
		conn, connErr = connectWithBackoff(ctx, cfg.dsn, logger)
		if connErr != nil {
			logger.Warn("initial database connection failed, will keep retrying in background",
				"error", connErr,
			)
			metrics.up.Set(0)
		} else {
			logger.Info("database connection established")
			metrics.up.Set(1)
		}
	}

	// Create history collector wired to the per-stage error counter (E2).
	histCollector := newHistoryCollector(logger, cfg.planCollection, cfg.slowQueryThreshold,
		metrics.historyErrors)

	// Ensure history table exists on startup.
	if conn != nil {
		if tableErr := histCollector.ensureTable(ctx, conn); tableErr != nil {
			logger.Warn("failed to ensure query history table on startup", "error", tableErr)
		}
	}

	// Start the periodic metric collection loop in a background goroutine.
	// Single-owner model (B3): ownership of conn transfers to the loop here —
	// the loop closes whatever connection is CURRENT on exit (it may have
	// reconnected), and run() only joins via the done channel.
	loopCtx, loopCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go collectLoop(loopCtx, cfg, conn, metrics, logger, histCollector, done)
	// Join the collect loop on EVERY exit path (L-4): the deferred closure
	// cancels the loop context (guaranteeing the join cannot hang even when
	// the parent context was never canceled) and waits for the loop to close
	// the current database connection. Registered right after the goroutine
	// starts, so the HTTP-error return below joins too; defer LIFO order runs
	// it after the inline shutdownServer on the clean path.
	defer func() {
		loopCancel()
		<-done
	}()

	// Set up HTTP server with /metrics and /health endpoints.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/health", handleHealth)

	srv := &http.Server{
		Addr:         cfg.listenAddress,
		Handler:      mux,
		ReadTimeout:  httpReadTimeout,
		WriteTimeout: httpWriteTimeout,
		IdleTimeout:  httpIdleTimeout,
	}

	// Start HTTP server in a goroutine.
	srvErrCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "address", cfg.listenAddress)
		if srvErr := srv.ListenAndServe(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			srvErrCh <- fmt.Errorf("HTTP server error: %w", srvErr)
		}
		close(srvErrCh)
	}()

	// Wait for shutdown signal or server error.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, stopping gracefully")
	case srvErr := <-srvErrCh:
		if srvErr != nil {
			return srvErr
		}
	}

	// Graceful shutdown: use a fresh context because the parent context
	// may already be canceled when this code runs. The collect loop is joined
	// by the deferred closure above (all-paths join, L-4) — run() must NOT
	// close the original conn pointer (it may be stale after a reconnect, and
	// closing it here could double-close a conn the loop already closed).
	if err := shutdownServer(srv, logger); err != nil {
		return err
	}

	logger.Info("cloudberry-query-exporter stopped")
	return nil
}

// setupTelemetry initializes the optional OTLP tracer (G-3): a no-op provider
// when telemetry is disabled, an OTLP exporter (grpc protocol, TLS on,
// project-default sampling) otherwise. Only the enabled/endpoint knobs are
// exposed in this pass. It returns a never-nil cleanup that shuts the tracer
// down with a fresh bounded context (the parent context may already be
// canceled when the cleanup runs), mirroring cmd/operator.
func setupTelemetry(ctx context.Context, cfg *exporterConfig, logger *slog.Logger) (cleanup func()) {
	shutdownTracer, err := telemetry.InitTracer(ctx, telemetry.Config{
		Enabled:        cfg.telemetryEnabled,
		OTLPEndpoint:   cfg.otlpEndpoint,
		SamplingRate:   exporterSamplingRate,
		ServiceName:    exporterServiceName,
		ServiceVersion: version,
	})
	if err != nil {
		logger.Warn("failed to initialize telemetry", "error", err)
		return func() {
			// No-op: tracer initialization failed, so there is nothing to
			// shut down; the non-nil closure keeps the call site simple.
		}
	}
	//nolint:contextcheck // fresh ctx needed; parent may be canceled
	return func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(
			context.Background(), tracerShutdownTimeout,
		)
		defer shutdownCancel()
		if shutdownErr := shutdownTracer(shutdownCtx); shutdownErr != nil {
			logger.Error("failed to shutdown tracer", "error", shutdownErr)
		}
	}
}

// handleHealth responds with HTTP 200 to indicate the exporter process is alive.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

// connInitialBackoff is the initial backoff between connection attempts. It
// is a variable (not the const) so tests can shrink the retry cadence
// (testability seam, E-2); production never mutates it.
//
//nolint:gochecknoglobals // test seam, constant in production
var connInitialBackoff = initialBackoff

// connectWithBackoff attempts to connect to PostgreSQL with exponential backoff.
// It returns the connection on success, or an error if the context is canceled
// before a connection is established.
func connectWithBackoff(ctx context.Context, dsn string, logger *slog.Logger) (*pgx.Conn, error) {
	backoff := connInitialBackoff

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context canceled before connection established: %w", err)
		}

		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			return conn, nil
		}

		logger.Warn("database connection attempt failed",
			"attempt", attempt,
			"error", err,
			"nextRetryIn", backoff.String(),
		)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled during backoff: %w", ctx.Err())
		case <-time.After(addJitter(backoff)):
			// Continue to next attempt.
		}

		backoff = time.Duration(math.Min(float64(backoff)*backoffFactor, float64(maxBackoff)))
	}
}

// addJitter adds a random jitter to the given duration to prevent thundering herd.
func addJitter(d time.Duration) time.Duration {
	jitterRand := rand.Float64() //nolint:gosec // jitter does not need crypto rand
	jitter := time.Duration(float64(d) * jitterFraction * jitterRand)
	return d + jitter
}

// collectLoop periodically collects metrics from the database.
// If the connection is lost, it reconnects with exponential backoff.
// The loop runs until the context is canceled.
//
// Ownership (B3): the loop OWNS the current database connection from the
// moment it starts — collectOnce may close a broken conn and reconnect, so
// only the loop knows which conn is live. On exit it closes the current conn
// (exactly once) and then closes done so run() can join deterministically.
func collectLoop(
	ctx context.Context,
	cfg *exporterConfig,
	conn *pgx.Conn,
	metrics *exporterMetrics,
	logger *slog.Logger,
	histCollector *historyCollector,
	done chan struct{},
) {
	defer close(done)

	ticker := time.NewTicker(cfg.samplingInterval)
	defer ticker.Stop()

	// Retention cleanup runs every hour by default; the interval is
	// configurable through exporterConfig so tests can exercise the tick.
	cleanupEvery := cfg.cleanupInterval
	if cleanupEvery <= 0 {
		cleanupEvery = defaultCleanupInterval
	}
	cleanupTicker := time.NewTicker(cleanupEvery)
	defer cleanupTicker.Stop()

	currentConn := conn

	for {
		select {
		case <-ctx.Done():
			// Close whatever connection is current (the original or a
			// reconnected one) — the loop is the single owner.
			closeConn(currentConn, logger)
			return
		case <-ticker.C:
			currentConn = collectOnceTraced(ctx, cfg, currentConn, metrics, logger, histCollector)
		case <-cleanupTicker.C:
			if currentConn != nil {
				cleanupHistoryTraced(ctx, cfg, currentConn, histCollector)
			}
		}
	}
}

// errCollectCycleDegraded marks the exporter.collect span errored when a
// collection cycle ends without a live database connection (connection loss,
// scrape failure, or no DSN yet).
var errCollectCycleDegraded = errors.New("collect cycle ended without a live database connection")

// collectOnceTraced wraps one collection cycle in an "exporter.collect" span
// (G-3). collectOnce returning nil is exactly the cycle-failure condition
// (broken/unavailable connection), so the span error status is derived from
// it. No statement text is attached (PII-safe, matching the pgx tracer
// policy). The span is a no-op when telemetry is disabled.
func collectOnceTraced(
	ctx context.Context,
	cfg *exporterConfig,
	conn *pgx.Conn,
	metrics *exporterMetrics,
	logger *slog.Logger,
	histCollector *historyCollector,
) *pgx.Conn {
	ctx, span := telemetry.StartSpan(ctx, exporterServiceName, "exporter.collect")
	defer span.End()
	newConn := collectOnce(ctx, cfg, conn, metrics, logger, histCollector)
	if newConn == nil {
		telemetry.SetSpanError(span, errCollectCycleDegraded)
	}
	return newConn
}

// cleanupHistoryTraced wraps one retention cleanup tick in an
// "exporter.history.cleanup" span (G-3). cleanupHistory is best-effort and
// reports failures via its own logging/metrics, so the span carries no error
// status. The span is a no-op when telemetry is disabled.
func cleanupHistoryTraced(
	ctx context.Context,
	cfg *exporterConfig,
	conn *pgx.Conn,
	histCollector *historyCollector,
) {
	ctx, span := telemetry.StartSpan(ctx, exporterServiceName, "exporter.history.cleanup")
	defer span.End()
	histCollector.cleanupHistory(ctx, conn, cfg.historyRetention)
}

// collectOnce performs a single metric collection cycle.
// It returns the (possibly reconnected) database connection.
func collectOnce(
	ctx context.Context,
	cfg *exporterConfig,
	conn *pgx.Conn,
	metrics *exporterMetrics,
	logger *slog.Logger,
	histCollector *historyCollector,
) *pgx.Conn {
	start := time.Now()

	// Ensure we have a valid connection.
	currentConn := ensureConnection(ctx, cfg.dsn, conn, metrics, logger)
	if currentConn == nil {
		metrics.up.Set(0)
		metrics.scrapeDuration.Observe(time.Since(start).Seconds())
		return nil
	}

	// Collect all metrics.
	if err := scrapeMetrics(ctx, currentConn, cfg.slowQueryThreshold, metrics, logger); err != nil {
		logger.Error("metric scrape failed", "error", err)
		metrics.up.Set(0)

		// Close the broken connection so the next cycle reconnects.
		if closeErr := currentConn.Close(ctx); closeErr != nil {
			logger.Debug("error closing broken connection", "error", closeErr)
		}
		currentConn = nil
	} else {
		metrics.up.Set(1)

		// Collect query history after successful metric scrape.
		histCollector.collectHistory(ctx, currentConn)
	}

	metrics.scrapeDuration.Observe(time.Since(start).Seconds())
	return currentConn
}

// ensureConnection verifies the existing connection is alive, or establishes a new one.
// Returns nil if the connection cannot be established (caller should set up=0).
func ensureConnection(
	ctx context.Context,
	dsn string,
	conn *pgx.Conn,
	metrics *exporterMetrics,
	logger *slog.Logger,
) *pgx.Conn {
	// If no DSN is configured, try re-reading from environment (the Secret
	// may have been mounted after the container started).
	if dsn == "" {
		dsn = os.Getenv(envDataSourceName)
		if dsn == "" {
			return nil
		}
		logger.Info("DATA_SOURCE_NAME now available, attempting connection")
	}

	if conn != nil {
		// Verify the connection is still alive with a ping.
		if err := conn.Ping(ctx); err == nil {
			return conn
		}
		logger.Warn("database connection lost, attempting to reconnect")
		// Close the stale connection; ignore errors since it is already broken.
		_ = conn.Close(ctx)
	}

	// Attempt a single reconnection (non-blocking for the collection loop).
	// Use a short timeout so we don't block the entire sampling interval.
	reconnCtx, reconnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reconnCancel()

	newConn, err := pgx.Connect(reconnCtx, dsn)
	if err != nil {
		logger.Warn("database reconnection failed", "error", err)
		metrics.up.Set(0)
		return nil
	}

	logger.Info("database connection re-established")
	return newConn
}

// scrapeMetrics executes all metric queries against the database and updates gauges.
func scrapeMetrics(
	ctx context.Context,
	conn *pgx.Conn,
	slowThreshold time.Duration,
	metrics *exporterMetrics,
	logger *slog.Logger,
) error {
	// Collect active query count.
	active, err := queryCount(ctx, conn, queryActiveCount)
	if err != nil {
		return fmt.Errorf("querying active count: %w", err)
	}
	metrics.activeQueries.Set(float64(active))

	// Collect idle session count.
	idle, err := queryCount(ctx, conn, queryIdleCount)
	if err != nil {
		return fmt.Errorf("querying idle count: %w", err)
	}
	metrics.idleSessions.Set(float64(idle))

	// Collect slow query count using parameterized query.
	slow, err := queryCountWithParam(ctx, conn, querySlowCount, slowThreshold.String())
	if err != nil {
		return fmt.Errorf("querying slow count: %w", err)
	}
	metrics.slowQueries.Set(float64(slow))

	// Collect total connection count.
	total, err := queryCount(ctx, conn, queryTotalConns)
	if err != nil {
		return fmt.Errorf("querying total connections: %w", err)
	}
	metrics.totalConnections.Set(float64(total))

	logger.Debug("base metrics collected",
		"active", active,
		"idle", idle,
		"slow", slow,
		"total", total,
	)

	// Collect extended Cloudberry-specific metrics.
	// Each collector handles its own errors internally by logging warnings.
	metrics.collectors.collectAll(ctx, conn, slowThreshold, logger)

	return nil
}

// queryCount executes a query that returns a single integer count.
func queryCount(ctx context.Context, conn *pgx.Conn, query string) (int64, error) {
	var count int64
	if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("executing query: %w", err)
	}
	return count, nil
}

// queryCountWithParam executes a parameterized query that returns a single integer count.
func queryCountWithParam(ctx context.Context, conn *pgx.Conn, query string, param string) (int64, error) {
	var count int64
	if err := conn.QueryRow(ctx, query, param).Scan(&count); err != nil {
		return 0, fmt.Errorf("executing parameterized query: %w", err)
	}
	return count, nil
}

// shutdownServer gracefully shuts down the HTTP server using a fresh
// background context, since the parent context is already canceled.
//
//nolint:contextcheck // fresh ctx needed; parent may be canceled
func shutdownServer(srv *http.Server, logger *slog.Logger) error {
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), httpShutdownTimeout,
	)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("HTTP server shutdown error: %w", err)
	}
	logger.Info("HTTP server stopped")
	return nil
}

// closeConn closes the database connection if it is non-nil.
// Uses a fresh background context because the parent is already canceled.
//
//nolint:contextcheck // fresh ctx needed; parent may be canceled
func closeConn(conn *pgx.Conn, logger *slog.Logger) {
	if conn == nil {
		return
	}
	if err := conn.Close(context.Background()); err != nil {
		logger.Warn("error closing database connection", "error", err)
	}
}
