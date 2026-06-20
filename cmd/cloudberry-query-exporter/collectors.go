// Package main contains extended metric collectors for the cloudberry-query-exporter.
// These collectors gather detailed metrics from Cloudberry Database system views
// covering query activity, resource groups, I/O, spill files, segment health,
// distributed transactions, and data distribution skew.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// collectorQueryTimeout is the per-query timeout for each collector.
const collectorQueryTimeout = 5 * time.Second

// Prometheus label name constants to avoid string duplication.
const (
	labelDatname    = "datname"
	labelRsgname    = "rsgname"
	labelHostname   = "hostname"
	labelTablespace = "tablespace"
	// labelCollector is the BOUNDED-enum label identifying which scrape
	// collector produced an error/duration observation (P3a). Values come from
	// the collector* name constants below — never free-form strings.
	labelCollector = "collector"
)

// Bounded-enum collector names for the scrape error/duration metrics (P3a).
// One constant per top-level collector; these are the ONLY values that ever
// appear in the `collector` label, keeping cardinality bounded.
const (
	collectorQueryActivity   = "query_activity"
	collectorResgroupStatus  = "resgroup_status"
	collectorResgroupIOStats = "resgroup_iostats"
	collectorSpillFiles      = "spill_files"
	collectorSegmentHealth   = "segment_health"
	collectorDistTxns        = "dist_txns"
	collectorTableSkew       = "table_skew"
)

// SQL queries for query activity metrics (57a).
const (
	queryActivitySQL = `SELECT state, datname, usename,
       EXTRACT(EPOCH FROM (now() - query_start)) as duration_seconds,
       wait_event_type
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND pid != pg_backend_pid()`
)

// SQL queries for resource group metrics (57b).
const (
	resgroupStatusSQL = `SELECT rsgname, num_running, num_queueing, num_executed, total_queue_duration
FROM gp_toolkit.gp_resgroup_status`

	resgroupStatusPerHostSQL = `SELECT rsgname, hostname, cpu, memory_used, memory_available,
       memory_quota_used, memory_shared_used
FROM gp_toolkit.gp_resgroup_status_per_host`
)

// SQL query for resource group I/O metrics (57c).
const resgroupIOStatsSQL = `SELECT rsgname, hostname, tablespace,
       rbps as read_bytes_per_sec, wbps as write_bytes_per_sec,
       riops as read_ops_per_sec, wiops as write_ops_per_sec
FROM gp_toolkit.gp_resgroup_iostats_per_host`

// SQL queries for spill file metrics (57d).
const (
	spillFileSummarySQL = `SELECT count(*) as active_count, COALESCE(sum(size), 0) as total_bytes
FROM gp_toolkit.gp_workfile_usage_per_query`

	spillFilePerSegmentSQL = `SELECT segid::text as segment_id, hostname, COALESCE(sum(size), 0) as bytes
FROM gp_toolkit.gp_workfile_usage_per_segment
GROUP BY segid, hostname`
)

// SQL queries for segment health metrics (57e).
const (
	segmentHealthSQL = `SELECT
  COUNT(*) FILTER (WHERE role = 'p') as primary_total,
  COUNT(*) FILTER (WHERE role = 'm') as mirror_total,
  COUNT(*) FILTER (WHERE status = 'u') as up_count,
  COUNT(*) FILTER (WHERE status = 'd') as down_count,
  COUNT(*) FILTER (WHERE mode = 'n') as not_synced,
  COUNT(*) FILTER (WHERE role != preferred_role) as not_preferred
FROM gp_segment_configuration WHERE content >= 0`

	clusterUptimeSQL = `SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as uptime_seconds`
)

// SQL queries for distributed transaction metrics (57f).
const (
	distributedXactsSQL = `SELECT
  COUNT(*) FILTER (WHERE state = 'Active') as active,
  COUNT(*) FILTER (WHERE state = 'Committed') as committed,
  COUNT(*) FILTER (WHERE state = 'Aborted') as aborted
FROM gp_distributed_xacts`

	oldestTransactionSQL = `SELECT COALESCE(EXTRACT(EPOCH FROM max(now() - xact_start)), 0) as oldest_age
FROM pg_stat_activity WHERE state != 'idle' AND xact_start IS NOT NULL`
)

// SQL query for data distribution / skew metrics (57g).
const tableSkewSQL = `SELECT psdname as schemaname, psdrelname as tablename,
       psdskewcoefficient as skew_coefficient
FROM gp_toolkit.gp_skew_coefficients
LIMIT 50`

// metricCollectors holds all extended Prometheus metrics for the exporter.
type metricCollectors struct {
	// 57a - Query activity metrics.
	queriesIdleInTransaction prometheus.Gauge
	queriesBlocked           prometheus.Gauge
	queriesTotal             *prometheus.CounterVec
	queriesSlowTotal         *prometheus.CounterVec
	queryDuration            prometheus.Histogram
	queryMaxDuration         prometheus.Gauge
	queriesCanceledTotal     *prometheus.CounterVec

	// 57b - Resource group metrics.
	resgroupRunningQueries       *prometheus.GaugeVec
	resgroupQueuedQueries        *prometheus.GaugeVec
	resgroupExecutedTotal        *prometheus.CounterVec
	resgroupQueueDurationTotal   *prometheus.CounterVec
	resgroupCPUUsagePercent      *prometheus.GaugeVec
	resgroupMemoryUsageBytes     *prometheus.GaugeVec
	resgroupMemoryAvailableBytes *prometheus.GaugeVec
	resgroupMemoryQuotaUsed      *prometheus.GaugeVec
	resgroupMemorySharedUsed     *prometheus.GaugeVec

	// 57c - Resource group I/O metrics.
	resgroupIOReadBytesPerSec  *prometheus.GaugeVec
	resgroupIOWriteBytesPerSec *prometheus.GaugeVec
	resgroupIOReadOpsPerSec    *prometheus.GaugeVec
	resgroupIOWriteOpsPerSec   *prometheus.GaugeVec

	// 57d - Spill file metrics.
	spillFilesActive     prometheus.Gauge
	spillFilesBytes      prometheus.Gauge
	spillFilesPerSegment *prometheus.GaugeVec
	spillFilesPerQuery   *prometheus.GaugeVec

	// 57e - Segment health metrics.
	segmentsTotal        *prometheus.GaugeVec
	segmentsUp           prometheus.Gauge
	segmentsDown         prometheus.Gauge
	segmentsNotSynced    prometheus.Gauge
	segmentsNotPreferred prometheus.Gauge
	clusterUptime        prometheus.Gauge

	// 57f - Distributed transaction metrics.
	distTxnActive    prometheus.Gauge
	distTxnCommitted prometheus.Counter
	distTxnAborted   prometheus.Counter
	oldestTxnAge     prometheus.Gauge

	// 57g - Data distribution / skew metrics.
	tableSkewCoefficient *prometheus.GaugeVec

	// P3a - Scrape self-observability metrics.
	// collectorErrorsTotal counts collector failures per scrape so a
	// permanently-failing collector is visible in Grafana; collectorDuration
	// observes per-collector latency on every scrape. Both labeled by the
	// bounded `collector` enum only.
	collectorErrorsTotal *prometheus.CounterVec
	collectorDuration    *prometheus.HistogramVec
}

// newMetricCollectors creates and registers all extended Prometheus metrics.
func newMetricCollectors(reg prometheus.Registerer) *metricCollectors {
	mc := &metricCollectors{
		// 57a - Query activity metrics.
		queriesIdleInTransaction: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "queries_idle_in_transaction",
			Help:      "Number of queries in idle-in-transaction state.",
		}),
		queriesBlocked: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "queries_blocked",
			Help:      "Number of queries blocked waiting for locks.",
		}),
		queriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "queries_total",
			Help:      "Total number of queries observed, by database and state.",
		}, []string{labelDatname, "state"}),
		queriesSlowTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "queries_slow_total",
			Help:      "Total number of slow queries observed, by database.",
		}, []string{labelDatname}),
		queryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "query_duration_seconds",
			Help:      "Histogram of query durations in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		}),
		queryMaxDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "query_max_duration_seconds",
			Help:      "Duration of the longest currently running query in seconds.",
		}),
		queriesCanceledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "queries_canceled_total",
			Help:      "Total number of canceled queries, by reason.",
		}, []string{"reason"}),

		// 57b - Resource group metrics.
		resgroupRunningQueries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_running_queries",
			Help:      "Number of running queries per resource group.",
		}, []string{labelRsgname}),
		resgroupQueuedQueries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_queued_queries",
			Help:      "Number of queued queries per resource group.",
		}, []string{labelRsgname}),
		resgroupExecutedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_executed_total",
			Help:      "Total number of executed queries per resource group.",
		}, []string{labelRsgname}),
		resgroupQueueDurationTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_queue_duration_seconds_total",
			Help:      "Total queue wait duration in seconds per resource group.",
		}, []string{labelRsgname}),
		resgroupCPUUsagePercent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_cpu_usage_percent",
			Help:      "CPU usage percentage per resource group and host.",
		}, []string{labelRsgname, labelHostname}),
		resgroupMemoryUsageBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_memory_usage_bytes",
			Help:      "Memory usage in bytes per resource group and host.",
		}, []string{labelRsgname, labelHostname}),
		resgroupMemoryAvailableBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_memory_available_bytes",
			Help:      "Available memory in bytes per resource group and host.",
		}, []string{labelRsgname, labelHostname}),
		resgroupMemoryQuotaUsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_memory_quota_used_bytes",
			Help:      "Memory quota used in bytes per resource group and host.",
		}, []string{labelRsgname, labelHostname}),
		resgroupMemorySharedUsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_memory_shared_used_bytes",
			Help:      "Shared memory used in bytes per resource group and host.",
		}, []string{labelRsgname, labelHostname}),

		// 57c - Resource group I/O metrics.
		resgroupIOReadBytesPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_io_read_bytes_per_sec",
			Help:      "Read bytes per second per resource group, host, and tablespace.",
		}, []string{labelRsgname, labelHostname, labelTablespace}),
		resgroupIOWriteBytesPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_io_write_bytes_per_sec",
			Help:      "Write bytes per second per resource group, host, and tablespace.",
		}, []string{labelRsgname, labelHostname, labelTablespace}),
		resgroupIOReadOpsPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_io_read_ops_per_sec",
			Help:      "Read operations per second per resource group, host, and tablespace.",
		}, []string{labelRsgname, labelHostname, labelTablespace}),
		resgroupIOWriteOpsPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "resgroup_io_write_ops_per_sec",
			Help:      "Write operations per second per resource group, host, and tablespace.",
		}, []string{labelRsgname, labelHostname, labelTablespace}),

		// 57d - Spill file metrics.
		spillFilesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spill_files_active",
			Help:      "Number of active spill files.",
		}),
		spillFilesBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spill_files_bytes",
			Help:      "Total size of active spill files in bytes.",
		}),
		spillFilesPerSegment: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spill_files_per_segment",
			Help:      "Spill file size in bytes per segment.",
		}, []string{"segment_id", labelHostname}),
		spillFilesPerQuery: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spill_files_per_query",
			Help:      "Spill file size in bytes per query.",
		}, []string{labelDatname, "pid"}),

		// 57e - Segment health metrics.
		segmentsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_total",
			Help:      "Total number of segments by role.",
		}, []string{"role"}),
		segmentsUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_up",
			Help:      "Number of segments in up state.",
		}),
		segmentsDown: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_down",
			Help:      "Number of segments in down state.",
		}),
		segmentsNotSynced: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_not_synced",
			Help:      "Number of segments not in sync mode.",
		}),
		segmentsNotPreferred: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_not_preferred_role",
			Help:      "Number of segments not running in their preferred role.",
		}),
		clusterUptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "cluster_uptime_seconds",
			Help:      "Cluster uptime in seconds since postmaster start.",
		}),

		// 57f - Distributed transaction metrics.
		distTxnActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "distributed_transactions_active",
			Help:      "Number of active distributed transactions.",
		}),
		distTxnCommitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "distributed_transactions_committed_total",
			Help:      "Total number of committed distributed transactions.",
		}),
		distTxnAborted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "distributed_transactions_aborted_total",
			Help:      "Total number of aborted distributed transactions.",
		}),
		oldestTxnAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "oldest_transaction_age_seconds",
			Help:      "Age of the oldest active transaction in seconds.",
		}),

		// 57g - Data distribution / skew metrics.
		tableSkewCoefficient: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "table_skew_coefficient",
			Help:      "Data distribution skew coefficient per table.",
		}, []string{"schemaname", "tablename"}),

		// P3a - Scrape self-observability metrics.
		collectorErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "collector_errors_total",
			Help:      "Total number of collector errors per scrape, by collector.",
		}, []string{labelCollector}),
		collectorDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "collector_duration_seconds",
			Help:      "Duration of each scrape collector in seconds, by collector.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
		}, []string{labelCollector}),
	}

	mc.register(reg)

	return mc
}

// register registers all metric collectors with the provided registerer.
func (mc *metricCollectors) register(reg prometheus.Registerer) {
	allCollectors := []prometheus.Collector{
		// 57a
		mc.queriesIdleInTransaction,
		mc.queriesBlocked,
		mc.queriesTotal,
		mc.queriesSlowTotal,
		mc.queryDuration,
		mc.queryMaxDuration,
		mc.queriesCanceledTotal,
		// 57b
		mc.resgroupRunningQueries,
		mc.resgroupQueuedQueries,
		mc.resgroupExecutedTotal,
		mc.resgroupQueueDurationTotal,
		mc.resgroupCPUUsagePercent,
		mc.resgroupMemoryUsageBytes,
		mc.resgroupMemoryAvailableBytes,
		mc.resgroupMemoryQuotaUsed,
		mc.resgroupMemorySharedUsed,
		// 57c
		mc.resgroupIOReadBytesPerSec,
		mc.resgroupIOWriteBytesPerSec,
		mc.resgroupIOReadOpsPerSec,
		mc.resgroupIOWriteOpsPerSec,
		// 57d
		mc.spillFilesActive,
		mc.spillFilesBytes,
		mc.spillFilesPerSegment,
		mc.spillFilesPerQuery,
		// 57e
		mc.segmentsTotal,
		mc.segmentsUp,
		mc.segmentsDown,
		mc.segmentsNotSynced,
		mc.segmentsNotPreferred,
		mc.clusterUptime,
		// 57f
		mc.distTxnActive,
		mc.distTxnCommitted,
		mc.distTxnAborted,
		mc.oldestTxnAge,
		// 57g
		mc.tableSkewCoefficient,
		// P3a - scrape self-observability.
		mc.collectorErrorsTotal,
		mc.collectorDuration,
	}
	for _, c := range allCollectors {
		reg.MustRegister(c)
	}
}

// instrument runs a single top-level collector, observing its duration on
// collectorDuration{collector} and incrementing collectorErrorsTotal{collector}
// exactly once if it returns an error. It preserves best-effort scrape
// semantics: an error is recorded and logged but never aborts the scrape.
func (mc *metricCollectors) instrument(name string, logger *slog.Logger, fn func() error) {
	start := time.Now()
	err := fn()
	mc.collectorDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
	if err != nil {
		mc.collectorErrorsTotal.WithLabelValues(name).Inc()
		logger.Warn("collector reported an error", "collector", name, "error", err)
	}
}

// collectAll runs all metric collectors. Each collector handles its own errors
// by logging warnings without failing the entire scrape cycle.
func (mc *metricCollectors) collectAll(
	ctx context.Context,
	conn *pgx.Conn,
	slowThreshold time.Duration,
	logger *slog.Logger,
) {
	mc.instrument(collectorQueryActivity, logger, func() error {
		return mc.collectQueryActivity(ctx, conn, slowThreshold, logger)
	})
	mc.instrument(collectorResgroupStatus, logger, func() error {
		return mc.collectResgroupStatus(ctx, conn, logger)
	})
	mc.instrument(collectorResgroupIOStats, logger, func() error {
		return mc.collectResgroupIOStats(ctx, conn, logger)
	})
	mc.instrument(collectorSpillFiles, logger, func() error {
		return mc.collectSpillFiles(ctx, conn, logger)
	})
	mc.instrument(collectorSegmentHealth, logger, func() error {
		return mc.collectSegmentHealth(ctx, conn, logger)
	})
	mc.instrument(collectorDistTxns, logger, func() error {
		return mc.collectDistributedTransactions(ctx, conn, logger)
	})
	mc.instrument(collectorTableSkew, logger, func() error {
		return mc.collectTableSkew(ctx, conn, logger)
	})
}

// collectQueryActivity collects query activity metrics from pg_stat_activity (57a).
func (mc *metricCollectors) collectQueryActivity(
	ctx context.Context,
	conn *pgx.Conn,
	slowThreshold time.Duration,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, queryActivitySQL)
	if err != nil {
		return fmt.Errorf("collecting query activity metrics: %w", err)
	}
	defer rows.Close()

	var (
		idleInTxnCount int64
		blockedCount   int64
		maxDuration    float64
	)

	for rows.Next() {
		var (
			state         *string
			datname       *string
			usename       *string
			duration      *float64
			waitEventType *string
		)

		if scanErr := rows.Scan(&state, &datname, &usename, &duration, &waitEventType); scanErr != nil {
			logger.Warn("failed to scan query activity row", "error", scanErr)
			continue
		}

		stateVal := safeDeref(state, "unknown")
		datnameVal := safeDeref(datname, "unknown")
		// usename is intentionally NOT used as a metric label (H-1): DB
		// role/username is unbounded in a multi-tenant cluster and would blow up
		// TSDB cardinality. It is still selected/scanned for potential debug use
		// but never attached to a metric series.
		_ = usename
		durationVal := safeDerefFloat(duration, 0)

		// Count queries by state.
		mc.queriesTotal.WithLabelValues(datnameVal, stateVal).Inc()

		// Track idle-in-transaction sessions.
		if stateVal == "idle in transaction" {
			idleInTxnCount++
		}

		// Track blocked queries (waiting for locks).
		if safeDeref(waitEventType, "") == "Lock" {
			blockedCount++
		}

		// Observe query duration for active queries.
		if stateVal == "active" && durationVal > 0 {
			mc.queryDuration.Observe(durationVal)

			if durationVal > maxDuration {
				maxDuration = durationVal
			}

			// Track slow queries.
			if durationVal > slowThreshold.Seconds() {
				mc.queriesSlowTotal.WithLabelValues(datnameVal).Inc()
			}
		}
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating query activity rows: %w", rowErr)
	}

	mc.queriesIdleInTransaction.Set(float64(idleInTxnCount))
	mc.queriesBlocked.Set(float64(blockedCount))
	mc.queryMaxDuration.Set(maxDuration)

	logger.Debug("query activity metrics collected",
		"idle_in_transaction", idleInTxnCount,
		"blocked", blockedCount,
		"max_duration_seconds", maxDuration,
	)
	return nil
}

// collectResgroupStatus collects resource group metrics (57b).
func (mc *metricCollectors) collectResgroupStatus(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	return errors.Join(
		mc.collectResgroupStatusSummary(ctx, conn, logger),
		mc.collectResgroupStatusPerHost(ctx, conn, logger),
	)
}

// collectResgroupStatusSummary collects aggregate resource group status.
func (mc *metricCollectors) collectResgroupStatusSummary(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, resgroupStatusSQL)
	if err != nil {
		return fmt.Errorf("collecting resource group status metrics "+
			"(gp_toolkit.gp_resgroup_status may not be available): %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rsgname            string
			numRunning         int64
			numQueueing        int64
			numExecuted        int64
			totalQueueDuration string
		)

		if scanErr := rows.Scan(&rsgname, &numRunning, &numQueueing, &numExecuted,
			&totalQueueDuration); scanErr != nil {
			logger.Warn("failed to scan resource group status row", "error", scanErr)
			continue
		}

		mc.resgroupRunningQueries.WithLabelValues(rsgname).Set(float64(numRunning))
		mc.resgroupQueuedQueries.WithLabelValues(rsgname).Set(float64(numQueueing))
		mc.resgroupExecutedTotal.WithLabelValues(rsgname).Add(float64(numExecuted))

		queueDuration, parseErr := parseIntervalToSeconds(totalQueueDuration)
		if parseErr != nil {
			logger.Warn("failed to parse queue duration", "error", parseErr, "value", totalQueueDuration)
			continue
		}
		mc.resgroupQueueDurationTotal.WithLabelValues(rsgname).Add(queueDuration)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating resource group status rows: %w", rowErr)
	}
	return nil
}

// collectResgroupStatusPerHost collects per-host resource group metrics.
func (mc *metricCollectors) collectResgroupStatusPerHost(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, resgroupStatusPerHostSQL)
	if err != nil {
		return fmt.Errorf("collecting resource group per-host metrics "+
			"(gp_toolkit.gp_resgroup_status_per_host may not be available): %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rsgname       string
			hostname      string
			cpu           float64
			memUsed       float64
			memAvailable  float64
			memQuotaUsed  float64
			memSharedUsed float64
		)

		if scanErr := rows.Scan(&rsgname, &hostname, &cpu, &memUsed, &memAvailable,
			&memQuotaUsed, &memSharedUsed); scanErr != nil {
			logger.Warn("failed to scan resource group per-host row", "error", scanErr)
			continue
		}

		mc.resgroupCPUUsagePercent.WithLabelValues(rsgname, hostname).Set(cpu)
		mc.resgroupMemoryUsageBytes.WithLabelValues(rsgname, hostname).Set(memUsed)
		mc.resgroupMemoryAvailableBytes.WithLabelValues(rsgname, hostname).Set(memAvailable)
		mc.resgroupMemoryQuotaUsed.WithLabelValues(rsgname, hostname).Set(memQuotaUsed)
		mc.resgroupMemorySharedUsed.WithLabelValues(rsgname, hostname).Set(memSharedUsed)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating resource group per-host rows: %w", rowErr)
	}
	return nil
}

// collectResgroupIOStats collects resource group I/O metrics (57c).
func (mc *metricCollectors) collectResgroupIOStats(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, resgroupIOStatsSQL)
	if err != nil {
		return fmt.Errorf("collecting resource group I/O metrics "+
			"(gp_toolkit.gp_resgroup_iostats_per_host may not be available): %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rsgname    string
			hostname   string
			tablespace string
			readBPS    float64
			writeBPS   float64
			readIOPS   float64
			writeIOPS  float64
		)

		if scanErr := rows.Scan(&rsgname, &hostname, &tablespace,
			&readBPS, &writeBPS, &readIOPS, &writeIOPS); scanErr != nil {
			logger.Warn("failed to scan resource group I/O row", "error", scanErr)
			continue
		}

		labels := []string{rsgname, hostname, tablespace}
		mc.resgroupIOReadBytesPerSec.WithLabelValues(labels...).Set(readBPS)
		mc.resgroupIOWriteBytesPerSec.WithLabelValues(labels...).Set(writeBPS)
		mc.resgroupIOReadOpsPerSec.WithLabelValues(labels...).Set(readIOPS)
		mc.resgroupIOWriteOpsPerSec.WithLabelValues(labels...).Set(writeIOPS)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating resource group I/O rows: %w", rowErr)
	}
	return nil
}

// collectSpillFiles collects spill file metrics (57d).
func (mc *metricCollectors) collectSpillFiles(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	return errors.Join(
		mc.collectSpillFileSummary(ctx, conn),
		mc.collectSpillFilePerSegment(ctx, conn, logger),
	)
}

// collectSpillFileSummary collects aggregate spill file metrics.
func (mc *metricCollectors) collectSpillFileSummary(
	ctx context.Context,
	conn *pgx.Conn,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	var activeCount int64
	var totalBytes int64

	err := conn.QueryRow(queryCtx, spillFileSummarySQL).Scan(&activeCount, &totalBytes)
	if err != nil {
		return fmt.Errorf("collecting spill file summary metrics "+
			"(gp_toolkit.gp_workfile_usage_per_query may not be available): %w", err)
	}

	mc.spillFilesActive.Set(float64(activeCount))
	mc.spillFilesBytes.Set(float64(totalBytes))
	return nil
}

// collectSpillFilePerSegment collects per-segment spill file metrics.
func (mc *metricCollectors) collectSpillFilePerSegment(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, spillFilePerSegmentSQL)
	if err != nil {
		return fmt.Errorf("collecting spill file per-segment metrics "+
			"(gp_toolkit.gp_workfile_usage_per_segment may not be available): %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			segmentID string
			hostname  string
			bytes     int64
		)

		if scanErr := rows.Scan(&segmentID, &hostname, &bytes); scanErr != nil {
			logger.Warn("failed to scan spill file per-segment row", "error", scanErr)
			continue
		}

		mc.spillFilesPerSegment.WithLabelValues(segmentID, hostname).Set(float64(bytes))
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating spill file per-segment rows: %w", rowErr)
	}
	return nil
}

// collectSegmentHealth collects segment health metrics (57e).
func (mc *metricCollectors) collectSegmentHealth(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	return errors.Join(
		mc.collectSegmentStatus(ctx, conn, logger),
		mc.collectClusterUptime(ctx, conn),
	)
}

// collectSegmentStatus collects segment configuration status.
func (mc *metricCollectors) collectSegmentStatus(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	var primaryTotal, mirrorTotal, upCount, downCount, notSynced, notPreferred int64

	err := conn.QueryRow(queryCtx, segmentHealthSQL).Scan(
		&primaryTotal, &mirrorTotal, &upCount, &downCount, &notSynced, &notPreferred,
	)
	if err != nil {
		return fmt.Errorf("collecting segment health metrics: %w", err)
	}

	mc.segmentsTotal.WithLabelValues("primary").Set(float64(primaryTotal))
	mc.segmentsTotal.WithLabelValues("mirror").Set(float64(mirrorTotal))
	mc.segmentsUp.Set(float64(upCount))
	mc.segmentsDown.Set(float64(downCount))
	mc.segmentsNotSynced.Set(float64(notSynced))
	mc.segmentsNotPreferred.Set(float64(notPreferred))

	logger.Debug("segment health metrics collected",
		"primary", primaryTotal,
		"mirror", mirrorTotal,
		"up", upCount,
		"down", downCount,
		"not_synced", notSynced,
		"not_preferred", notPreferred,
	)
	return nil
}

// collectClusterUptime collects the cluster uptime metric.
func (mc *metricCollectors) collectClusterUptime(
	ctx context.Context,
	conn *pgx.Conn,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	var uptimeSeconds float64

	err := conn.QueryRow(queryCtx, clusterUptimeSQL).Scan(&uptimeSeconds)
	if err != nil {
		return fmt.Errorf("collecting cluster uptime metric: %w", err)
	}

	mc.clusterUptime.Set(uptimeSeconds)
	return nil
}

// collectDistributedTransactions collects distributed transaction metrics (57f).
func (mc *metricCollectors) collectDistributedTransactions(
	ctx context.Context,
	conn *pgx.Conn,
	_ *slog.Logger,
) error {
	return errors.Join(
		mc.collectDistributedXacts(ctx, conn),
		mc.collectOldestTransaction(ctx, conn),
	)
}

// collectDistributedXacts collects distributed transaction state counts.
func (mc *metricCollectors) collectDistributedXacts(
	ctx context.Context,
	conn *pgx.Conn,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	var active, committed, aborted int64

	err := conn.QueryRow(queryCtx, distributedXactsSQL).Scan(&active, &committed, &aborted)
	if err != nil {
		return fmt.Errorf("collecting distributed transaction metrics "+
			"(gp_distributed_xacts may not be available): %w", err)
	}

	mc.distTxnActive.Set(float64(active))
	mc.distTxnCommitted.Add(float64(committed))
	mc.distTxnAborted.Add(float64(aborted))
	return nil
}

// collectOldestTransaction collects the age of the oldest active transaction.
func (mc *metricCollectors) collectOldestTransaction(
	ctx context.Context,
	conn *pgx.Conn,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	var oldestAge float64

	err := conn.QueryRow(queryCtx, oldestTransactionSQL).Scan(&oldestAge)
	if err != nil {
		return fmt.Errorf("collecting oldest transaction age metric: %w", err)
	}

	mc.oldestTxnAge.Set(oldestAge)
	return nil
}

// collectTableSkew collects data distribution skew metrics (57g).
// This query is expensive and should be called less frequently in production.
func (mc *metricCollectors) collectTableSkew(
	ctx context.Context,
	conn *pgx.Conn,
	logger *slog.Logger,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, tableSkewSQL)
	if err != nil {
		return fmt.Errorf("collecting table skew metrics "+
			"(gp_toolkit.gp_skew_coefficients may not be available): %w", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var (
			schemaname      string
			tablename       string
			skewCoefficient float64
		)

		if scanErr := rows.Scan(&schemaname, &tablename, &skewCoefficient); scanErr != nil {
			logger.Warn("failed to scan table skew row", "error", scanErr)
			continue
		}

		mc.tableSkewCoefficient.WithLabelValues(schemaname, tablename).Set(skewCoefficient)
		count++
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating table skew rows: %w", rowErr)
	}

	logger.Debug("table skew metrics collected", "tables_measured", count)
	return nil
}

// safeDeref returns the dereferenced string pointer value, or the fallback if nil.
func safeDeref(ptr *string, fallback string) string {
	if ptr == nil {
		return fallback
	}
	return *ptr
}

// safeDerefFloat returns the dereferenced float64 pointer value, or the fallback if nil.
func safeDerefFloat(ptr *float64, fallback float64) float64 {
	if ptr == nil {
		return fallback
	}
	return *ptr
}

// parseIntervalToSeconds parses a PostgreSQL interval string to seconds.
// It handles the common format "HH:MM:SS" or "HH:MM:SS.ffffff".
func parseIntervalToSeconds(interval string) (float64, error) {
	d, err := time.ParseDuration(reformatInterval(interval))
	if err != nil {
		return 0, fmt.Errorf("parsing interval %q: %w", interval, err)
	}
	return d.Seconds(), nil
}

// reformatInterval converts a PostgreSQL interval string (e.g. "01:23:45.678")
// to a Go duration string (e.g. "1h23m45.678s").
func reformatInterval(interval string) string {
	var hours, minutes int
	var seconds float64

	if _, err := fmt.Sscanf(interval, "%d:%d:%f", &hours, &minutes, &seconds); err != nil {
		// Return a zero-duration string; the caller's ParseDuration will handle the error.
		return "0s"
	}

	return fmt.Sprintf("%dh%dm%fs", hours, minutes, seconds)
}
