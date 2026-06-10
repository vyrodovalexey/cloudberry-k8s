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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// version is set via ldflags at build time (e.g. -X main.version=...).
//
//nolint:gochecknoglobals // set by ldflags
var version = "dev"

const (
	metricsNamespace = "cloudberry"

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
	collectors       *metricCollectors
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
			Subsystem: "query_exporter",
			Name:      "up",
			Help:      "Whether the database connection is healthy (1=up, 0=down).",
		}),
		scrapeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: "query_exporter",
			Name:      "scrape_duration_seconds",
			Help:      "Duration of database metric scrape in seconds.",
			Buckets:   prometheus.DefBuckets,
		}),
		collectors: newMetricCollectors(reg),
	}

	baseCollectors := []prometheus.Collector{
		m.activeQueries,
		m.idleSessions,
		m.slowQueries,
		m.totalConnections,
		m.up,
		m.scrapeDuration,
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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting cloudberry-query-exporter",
		"version", version,
		"listenAddress", cfg.listenAddress,
		"samplingInterval", cfg.samplingInterval.String(),
		"slowQueryThreshold", cfg.slowQueryThreshold.String(),
	)

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

	// Create history collector.
	histCollector := newHistoryCollector(logger, cfg.planCollection, cfg.slowQueryThreshold)

	// Ensure history table exists on startup.
	if conn != nil {
		if tableErr := histCollector.ensureTable(ctx, conn); tableErr != nil {
			logger.Warn("failed to ensure query history table on startup", "error", tableErr)
		}
	}

	// Start the periodic metric collection loop in a background goroutine.
	go collectLoop(ctx, cfg, conn, metrics, logger, histCollector)

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
	// may already be canceled when this code runs.
	if err := shutdownServer(srv, logger); err != nil {
		return err
	}

	// Close the database connection if it was established.
	closeConn(conn, logger)

	logger.Info("cloudberry-query-exporter stopped")
	return nil
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
func collectLoop(
	ctx context.Context,
	cfg *exporterConfig,
	conn *pgx.Conn,
	metrics *exporterMetrics,
	logger *slog.Logger,
	histCollector *historyCollector,
) {
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
			return
		case <-ticker.C:
			currentConn = collectOnce(ctx, cfg, currentConn, metrics, logger, histCollector)
		case <-cleanupTicker.C:
			if currentConn != nil {
				histCollector.cleanupHistory(ctx, currentConn, cfg.historyRetention)
			}
		}
	}
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
