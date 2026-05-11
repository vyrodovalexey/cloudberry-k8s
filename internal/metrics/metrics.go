// Package metrics provides Prometheus metrics registration and recording for the cloudberry operator.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "cloudberry"

	labelCluster    = "cluster"
	labelNamespace  = "namespace"
	labelPhase      = "phase"
	labelVersion    = "version"
	labelSegment    = "segment"
	labelComponent  = "component"
	labelOperation  = "operation"
	labelResult     = "result"
	labelType       = "type"
	labelJob        = "job"
	labelSourceType = "source_type"
)

// Recorder defines the interface for recording metrics.
type Recorder interface {
	// RecordReconcile records a reconciliation event.
	RecordReconcile(cluster, namespace, result string, duration time.Duration)
	// UpdateClusterInfo updates cluster metadata gauge.
	UpdateClusterInfo(cluster, namespace, version, phase string, segments float64)
	// SetCoordinatorUp sets the coordinator availability gauge.
	SetCoordinatorUp(cluster, namespace string, up bool)
	// SetStandbyUp sets the standby availability gauge.
	SetStandbyUp(cluster, namespace string, up bool)
	// SetSegmentsReady sets the number of ready segments.
	SetSegmentsReady(cluster, namespace string, count float64)
	// SetSegmentsTotal sets the total number of segments.
	SetSegmentsTotal(cluster, namespace string, count float64)
	// SetSegmentsFailed sets the number of failed segments.
	SetSegmentsFailed(cluster, namespace string, count float64)
	// SetMirroringInSync sets the mirroring sync status.
	SetMirroringInSync(cluster, namespace string, inSync bool)
	// RecordFTSProbe records an FTS probe event.
	RecordFTSProbe(cluster, namespace, result string, duration time.Duration)
	// RecordFTSFailover records an FTS failover event.
	RecordFTSFailover(cluster, namespace string)
	// SetSegmentStatus sets the status of a specific segment.
	SetSegmentStatus(cluster, namespace, segment string, up bool)
	// SetReplicationLag sets the replication lag for a segment.
	SetReplicationLag(cluster, namespace, segment string, bytes float64)
	// SetStandbyReplicationLag sets the standby replication lag.
	SetStandbyReplicationLag(cluster, namespace string, bytes float64)
	// RecordConfigReload records a configuration reload event.
	RecordConfigReload(cluster, namespace string)
	// SetConnectionsActive sets the number of active connections.
	SetConnectionsActive(cluster, namespace string, count float64)
	// SetConnectionsMax sets the maximum number of connections.
	SetConnectionsMax(cluster, namespace string, count float64)
	// SetDiskUsageBytes sets the disk usage for a database.
	SetDiskUsageBytes(cluster, namespace, database string, bytes float64)
	// RecordAuthAttempt records an authentication attempt.
	RecordAuthAttempt(method, result string)
	// SetActiveQueries sets the number of active queries.
	SetActiveQueries(cluster, namespace string, count float64)
	// SetQueuedQueries sets the number of queued queries.
	SetQueuedQueries(cluster, namespace string, count float64)
	// SetBlockedQueries sets the number of blocked queries.
	SetBlockedQueries(cluster, namespace string, count float64)
	// RecordWorkloadRuleAction records a workload rule action event.
	RecordWorkloadRuleAction(cluster, namespace, rule, action string)
	// SetResourceGroupUsage sets the resource group usage gauge.
	SetResourceGroupUsage(cluster, namespace, group string, cpu, memory float64)
	// RecordIdleSessionTermination records an idle session termination event.
	RecordIdleSessionTermination(cluster, namespace, rule string)
	// RecordSlowQuery records a slow query event.
	RecordSlowQuery(cluster, namespace string)
	// RecordBackup records a backup event by type and status.
	RecordBackup(cluster, namespace, backupType, status string)
	// ObserveBackupDuration records the duration of a backup operation.
	ObserveBackupDuration(cluster, namespace string, duration time.Duration)
	// SetBackupSizeBytes sets the size of the last backup.
	SetBackupSizeBytes(cluster, namespace string, bytes float64)
	// RecordRestore records a restore event.
	RecordRestore(cluster, namespace, status string)
	// SetDataLoadingJobsActive sets the number of active data loading jobs.
	SetDataLoadingJobsActive(cluster, namespace string, count float64)
	// RecordDataLoadingRows records the number of rows loaded by a job.
	RecordDataLoadingRows(cluster, namespace, job, sourceType string, count float64)
	// SetDiskUsagePercent sets the disk usage percentage for a cluster.
	SetDiskUsagePercent(cluster, namespace string, percent float64)
	// SetRecommendationsTotal sets the total recommendations by type.
	SetRecommendationsTotal(cluster, namespace, recType string, count float64)
	// ObserveRecommendationScanDuration records the duration of a recommendation scan.
	ObserveRecommendationScanDuration(cluster, namespace string, duration time.Duration)
	// SetTableBloatRatio sets the bloat ratio for a table.
	SetTableBloatRatio(cluster, namespace, table string, ratio float64)
}

// PrometheusRecorder implements Recorder using Prometheus metrics.
type PrometheusRecorder struct {
	reconcileTotal    *prometheus.CounterVec
	reconcileErrors   *prometheus.CounterVec
	reconcileDuration *prometheus.HistogramVec

	clusterInfo    *prometheus.GaugeVec
	coordinatorUp  *prometheus.GaugeVec
	standbyUp      *prometheus.GaugeVec
	segmentsReady  *prometheus.GaugeVec
	segmentsTotal  *prometheus.GaugeVec
	segmentsFailed *prometheus.GaugeVec
	mirroringSync  *prometheus.GaugeVec

	ftsProbeTotal    *prometheus.CounterVec
	ftsProbeFailures *prometheus.CounterVec
	ftsProbeDuration *prometheus.HistogramVec
	ftsFailoverTotal *prometheus.CounterVec
	segmentStatus    *prometheus.GaugeVec
	replicationLag   *prometheus.GaugeVec
	standbyRepLag    *prometheus.GaugeVec

	configReloadTotal *prometheus.CounterVec
	connectionsActive *prometheus.GaugeVec
	connectionsMax    *prometheus.GaugeVec
	diskUsageBytes    *prometheus.GaugeVec

	authAttempts *prometheus.CounterVec

	activeQueries          *prometheus.GaugeVec
	queuedQueries          *prometheus.GaugeVec
	blockedQueries         *prometheus.GaugeVec
	workloadRuleActions    *prometheus.CounterVec
	resourceGroupCPU       *prometheus.GaugeVec
	resourceGroupMemory    *prometheus.GaugeVec
	idleSessionTermination *prometheus.CounterVec
	slowQueries            *prometheus.CounterVec

	backupTotal          *prometheus.CounterVec
	backupDuration       *prometheus.HistogramVec
	backupSizeBytes      *prometheus.GaugeVec
	restoreTotal         *prometheus.CounterVec
	dataLoadingJobsGauge *prometheus.GaugeVec
	dataLoadingRows      *prometheus.CounterVec

	diskUsagePercent      *prometheus.GaugeVec
	recommendationsTotal  *prometheus.GaugeVec
	recommendationScanDur *prometheus.HistogramVec
	tableBloatRatio       *prometheus.GaugeVec
}

// initCoreMetrics initializes core reconciliation and cluster metrics.
func (r *PrometheusRecorder) initCoreMetrics() {
	r.reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "reconcile_total",
		Help:      "Total number of reconciliations.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.reconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "reconcile_errors_total",
		Help:      "Total number of reconciliation errors.",
	}, []string{labelCluster, labelNamespace})
	r.reconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "reconcile_duration_seconds",
		Help:      "Duration of reconciliations in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelCluster, labelNamespace})
	r.clusterInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "cluster_info",
		Help:      "Cluster metadata information.",
	}, []string{labelCluster, labelNamespace, labelVersion, labelPhase})
	r.coordinatorUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "coordinator_up",
		Help:      "Coordinator availability (0/1).",
	}, []string{labelCluster, labelNamespace})
	r.standbyUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "standby_up",
		Help:      "Standby coordinator availability (0/1).",
	}, []string{labelCluster, labelNamespace})
	r.segmentsReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "segments_ready",
		Help:      "Number of ready segments.",
	}, []string{labelCluster, labelNamespace})
	r.segmentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "segments_total",
		Help:      "Total number of segments.",
	}, []string{labelCluster, labelNamespace})
	r.segmentsFailed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "segments_failed",
		Help:      "Number of failed segments.",
	}, []string{labelCluster, labelNamespace})
	r.mirroringSync = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "mirroring_in_sync",
		Help:      "Mirroring sync status (0/1).",
	}, []string{labelCluster, labelNamespace})
	r.authAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "auth_attempts_total",
		Help:      "Total number of authentication attempts.",
	}, []string{"method", labelResult})
}

// initHAMetrics initializes high availability and replication metrics.
func (r *PrometheusRecorder) initHAMetrics() {
	r.ftsProbeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "fts_probe_total",
		Help:      "Total number of FTS probes.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.ftsProbeFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "fts_probe_failures_total",
		Help:      "Total number of failed FTS probes.",
	}, []string{labelCluster, labelNamespace})
	r.ftsProbeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "fts_probe_duration_seconds",
		Help:      "Duration of FTS probes in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelCluster, labelNamespace})
	r.ftsFailoverTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "fts_failover_total",
		Help:      "Total number of FTS failovers.",
	}, []string{labelCluster, labelNamespace})
	r.segmentStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "segment_status",
		Help:      "Per-segment status (1=up, 0=down).",
	}, []string{labelCluster, labelNamespace, labelSegment})
	r.replicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "replication_lag_bytes",
		Help:      "Replication lag per segment in bytes.",
	}, []string{labelCluster, labelNamespace, labelSegment})
	r.standbyRepLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "standby_replication_lag_bytes",
		Help:      "Standby coordinator replication lag in bytes.",
	}, []string{labelCluster, labelNamespace})
}

// initOperationalMetrics initializes operational and connection metrics.
func (r *PrometheusRecorder) initOperationalMetrics() {
	r.configReloadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "config_reload_total",
		Help:      "Total number of configuration reloads.",
	}, []string{labelCluster, labelNamespace})
	r.connectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "connections_active",
		Help:      "Number of active database connections.",
	}, []string{labelCluster, labelNamespace})
	r.connectionsMax = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "connections_max",
		Help:      "Maximum allowed database connections.",
	}, []string{labelCluster, labelNamespace})
	r.diskUsageBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "disk_usage_bytes",
		Help:      "Disk usage per database in bytes.",
	}, []string{labelCluster, labelNamespace, "database"})
}

// initWorkloadMetrics initializes workload management and query metrics.
func (r *PrometheusRecorder) initWorkloadMetrics() {
	r.activeQueries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "active_queries",
		Help:      "Number of currently active queries.",
	}, []string{labelCluster, labelNamespace})
	r.queuedQueries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "queued_queries",
		Help:      "Number of currently queued queries.",
	}, []string{labelCluster, labelNamespace})
	r.blockedQueries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "blocked_queries",
		Help:      "Number of currently blocked queries.",
	}, []string{labelCluster, labelNamespace})
	r.workloadRuleActions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "workload_rule_actions_total",
		Help:      "Total number of workload rule actions.",
	}, []string{labelCluster, labelNamespace, "rule", "action"})
	r.resourceGroupCPU = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "resource_group_cpu_usage",
		Help:      "CPU usage percentage per resource group.",
	}, []string{labelCluster, labelNamespace, "group"})
	r.resourceGroupMemory = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "resource_group_memory_usage",
		Help:      "Memory usage percentage per resource group.",
	}, []string{labelCluster, labelNamespace, "group"})
	r.idleSessionTermination = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "idle_session_terminations_total",
		Help:      "Total number of idle session terminations.",
	}, []string{labelCluster, labelNamespace, "rule"})
	r.slowQueries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "slow_queries_total",
		Help:      "Total number of slow queries detected.",
	}, []string{labelCluster, labelNamespace})
}

// initBackupMetrics initializes backup and data loading metrics.
func (r *PrometheusRecorder) initBackupMetrics() {
	r.backupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "backup_total",
		Help:      "Total number of backup operations.",
	}, []string{labelCluster, labelNamespace, labelType, labelResult})
	r.backupDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "backup_duration_seconds",
		Help:      "Duration of backup operations in seconds.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{labelCluster, labelNamespace})
	r.backupSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "backup_size_bytes",
		Help:      "Size of the last backup in bytes.",
	}, []string{labelCluster, labelNamespace})
	r.restoreTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "restore_total",
		Help:      "Total number of restore operations.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.dataLoadingJobsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "data_loading_jobs_active",
		Help:      "Number of active data loading jobs.",
	}, []string{labelCluster, labelNamespace})
	r.dataLoadingRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "data_loading_rows_total",
		Help:      "Total number of rows loaded by data loading jobs.",
	}, []string{labelCluster, labelNamespace, labelJob, labelSourceType})
}

// initStorageMetrics initializes storage management and recommendation metrics.
func (r *PrometheusRecorder) initStorageMetrics() {
	r.diskUsagePercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "disk_usage_percent",
		Help:      "Disk usage percentage per cluster.",
	}, []string{labelCluster, labelNamespace})
	r.recommendationsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "recommendations_total",
		Help:      "Total number of recommendations by type.",
	}, []string{labelCluster, labelNamespace, labelType})
	r.recommendationScanDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "recommendation_scan_duration_seconds",
		Help:      "Duration of recommendation scans in seconds.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{labelCluster, labelNamespace})
	r.tableBloatRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "table_bloat_ratio",
		Help:      "Bloat ratio for top tables.",
	}, []string{labelCluster, labelNamespace, "table"})
}

// NewPrometheusRecorder creates a new PrometheusRecorder and registers all metrics.
func NewPrometheusRecorder(reg prometheus.Registerer) *PrometheusRecorder {
	r := &PrometheusRecorder{}
	r.initCoreMetrics()
	r.initHAMetrics()
	r.initOperationalMetrics()
	r.initWorkloadMetrics()
	r.initBackupMetrics()
	r.initStorageMetrics()
	r.register(reg)
	return r
}

// register registers all metrics with the given registerer.
func (r *PrometheusRecorder) register(reg prometheus.Registerer) {
	collectors := []prometheus.Collector{
		r.reconcileTotal, r.reconcileErrors, r.reconcileDuration,
		r.clusterInfo, r.coordinatorUp, r.standbyUp,
		r.segmentsReady, r.segmentsTotal, r.segmentsFailed, r.mirroringSync,
		r.ftsProbeTotal, r.ftsProbeFailures, r.ftsProbeDuration,
		r.ftsFailoverTotal, r.segmentStatus, r.replicationLag, r.standbyRepLag,
		r.configReloadTotal, r.connectionsActive, r.connectionsMax, r.diskUsageBytes,
		r.authAttempts,
		r.activeQueries, r.queuedQueries, r.blockedQueries,
		r.workloadRuleActions, r.resourceGroupCPU, r.resourceGroupMemory,
		r.idleSessionTermination, r.slowQueries,
		r.backupTotal, r.backupDuration, r.backupSizeBytes,
		r.restoreTotal, r.dataLoadingJobsGauge, r.dataLoadingRows,
		r.diskUsagePercent, r.recommendationsTotal,
		r.recommendationScanDur, r.tableBloatRatio,
	}
	for _, c := range collectors {
		reg.MustRegister(c)
	}
}

// RecordReconcile records a reconciliation event.
func (r *PrometheusRecorder) RecordReconcile(cluster, namespace, result string, duration time.Duration) {
	r.reconcileTotal.WithLabelValues(cluster, namespace, result).Inc()
	r.reconcileDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
	if result == "error" {
		r.reconcileErrors.WithLabelValues(cluster, namespace).Inc()
	}
}

// UpdateClusterInfo updates cluster metadata gauge.
func (r *PrometheusRecorder) UpdateClusterInfo(cluster, namespace, version, phase string, segments float64) {
	r.clusterInfo.WithLabelValues(cluster, namespace, version, phase).Set(segments)
}

// SetCoordinatorUp sets the coordinator availability gauge.
func (r *PrometheusRecorder) SetCoordinatorUp(cluster, namespace string, up bool) {
	r.coordinatorUp.WithLabelValues(cluster, namespace).Set(boolToFloat64(up))
}

// SetStandbyUp sets the standby availability gauge.
func (r *PrometheusRecorder) SetStandbyUp(cluster, namespace string, up bool) {
	r.standbyUp.WithLabelValues(cluster, namespace).Set(boolToFloat64(up))
}

// SetSegmentsReady sets the number of ready segments.
func (r *PrometheusRecorder) SetSegmentsReady(cluster, namespace string, count float64) {
	r.segmentsReady.WithLabelValues(cluster, namespace).Set(count)
}

// SetSegmentsTotal sets the total number of segments.
func (r *PrometheusRecorder) SetSegmentsTotal(cluster, namespace string, count float64) {
	r.segmentsTotal.WithLabelValues(cluster, namespace).Set(count)
}

// SetSegmentsFailed sets the number of failed segments.
func (r *PrometheusRecorder) SetSegmentsFailed(cluster, namespace string, count float64) {
	r.segmentsFailed.WithLabelValues(cluster, namespace).Set(count)
}

// SetMirroringInSync sets the mirroring sync status.
func (r *PrometheusRecorder) SetMirroringInSync(cluster, namespace string, inSync bool) {
	r.mirroringSync.WithLabelValues(cluster, namespace).Set(boolToFloat64(inSync))
}

// RecordFTSProbe records an FTS probe event.
func (r *PrometheusRecorder) RecordFTSProbe(cluster, namespace, result string, duration time.Duration) {
	r.ftsProbeTotal.WithLabelValues(cluster, namespace, result).Inc()
	r.ftsProbeDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
	if result == "failure" {
		r.ftsProbeFailures.WithLabelValues(cluster, namespace).Inc()
	}
}

// RecordFTSFailover records an FTS failover event.
func (r *PrometheusRecorder) RecordFTSFailover(cluster, namespace string) {
	r.ftsFailoverTotal.WithLabelValues(cluster, namespace).Inc()
}

// SetSegmentStatus sets the status of a specific segment.
func (r *PrometheusRecorder) SetSegmentStatus(cluster, namespace, segment string, up bool) {
	r.segmentStatus.WithLabelValues(cluster, namespace, segment).Set(boolToFloat64(up))
}

// SetReplicationLag sets the replication lag for a segment.
func (r *PrometheusRecorder) SetReplicationLag(cluster, namespace, segment string, bytes float64) {
	r.replicationLag.WithLabelValues(cluster, namespace, segment).Set(bytes)
}

// SetStandbyReplicationLag sets the standby replication lag.
func (r *PrometheusRecorder) SetStandbyReplicationLag(cluster, namespace string, bytes float64) {
	r.standbyRepLag.WithLabelValues(cluster, namespace).Set(bytes)
}

// RecordConfigReload records a configuration reload event.
func (r *PrometheusRecorder) RecordConfigReload(cluster, namespace string) {
	r.configReloadTotal.WithLabelValues(cluster, namespace).Inc()
}

// SetConnectionsActive sets the number of active connections.
func (r *PrometheusRecorder) SetConnectionsActive(cluster, namespace string, count float64) {
	r.connectionsActive.WithLabelValues(cluster, namespace).Set(count)
}

// SetConnectionsMax sets the maximum number of connections.
func (r *PrometheusRecorder) SetConnectionsMax(cluster, namespace string, count float64) {
	r.connectionsMax.WithLabelValues(cluster, namespace).Set(count)
}

// SetDiskUsageBytes sets the disk usage for a database.
func (r *PrometheusRecorder) SetDiskUsageBytes(cluster, namespace, database string, bytes float64) {
	r.diskUsageBytes.WithLabelValues(cluster, namespace, database).Set(bytes)
}

// RecordAuthAttempt records an authentication attempt.
func (r *PrometheusRecorder) RecordAuthAttempt(method, result string) {
	r.authAttempts.WithLabelValues(method, result).Inc()
}

// SetActiveQueries sets the number of active queries.
func (r *PrometheusRecorder) SetActiveQueries(cluster, namespace string, count float64) {
	r.activeQueries.WithLabelValues(cluster, namespace).Set(count)
}

// SetQueuedQueries sets the number of queued queries.
func (r *PrometheusRecorder) SetQueuedQueries(cluster, namespace string, count float64) {
	r.queuedQueries.WithLabelValues(cluster, namespace).Set(count)
}

// SetBlockedQueries sets the number of blocked queries.
func (r *PrometheusRecorder) SetBlockedQueries(cluster, namespace string, count float64) {
	r.blockedQueries.WithLabelValues(cluster, namespace).Set(count)
}

// RecordWorkloadRuleAction records a workload rule action event.
func (r *PrometheusRecorder) RecordWorkloadRuleAction(cluster, namespace, rule, action string) {
	r.workloadRuleActions.WithLabelValues(cluster, namespace, rule, action).Inc()
}

// SetResourceGroupUsage sets the resource group usage gauge.
func (r *PrometheusRecorder) SetResourceGroupUsage(cluster, namespace, group string, cpu, memory float64) {
	r.resourceGroupCPU.WithLabelValues(cluster, namespace, group).Set(cpu)
	r.resourceGroupMemory.WithLabelValues(cluster, namespace, group).Set(memory)
}

// RecordIdleSessionTermination records an idle session termination event.
func (r *PrometheusRecorder) RecordIdleSessionTermination(cluster, namespace, rule string) {
	r.idleSessionTermination.WithLabelValues(cluster, namespace, rule).Inc()
}

// RecordSlowQuery records a slow query event.
func (r *PrometheusRecorder) RecordSlowQuery(cluster, namespace string) {
	r.slowQueries.WithLabelValues(cluster, namespace).Inc()
}

// RecordBackup records a backup event by type and status.
func (r *PrometheusRecorder) RecordBackup(cluster, namespace, backupType, status string) {
	r.backupTotal.WithLabelValues(cluster, namespace, backupType, status).Inc()
}

// ObserveBackupDuration records the duration of a backup operation.
func (r *PrometheusRecorder) ObserveBackupDuration(cluster, namespace string, duration time.Duration) {
	r.backupDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// SetBackupSizeBytes sets the size of the last backup.
func (r *PrometheusRecorder) SetBackupSizeBytes(cluster, namespace string, bytes float64) {
	r.backupSizeBytes.WithLabelValues(cluster, namespace).Set(bytes)
}

// RecordRestore records a restore event.
func (r *PrometheusRecorder) RecordRestore(cluster, namespace, status string) {
	r.restoreTotal.WithLabelValues(cluster, namespace, status).Inc()
}

// SetDataLoadingJobsActive sets the number of active data loading jobs.
func (r *PrometheusRecorder) SetDataLoadingJobsActive(cluster, namespace string, count float64) {
	r.dataLoadingJobsGauge.WithLabelValues(cluster, namespace).Set(count)
}

// RecordDataLoadingRows records the number of rows loaded by a job.
func (r *PrometheusRecorder) RecordDataLoadingRows(
	cluster, namespace, job, sourceType string,
	count float64,
) {
	r.dataLoadingRows.WithLabelValues(cluster, namespace, job, sourceType).Add(count)
}

// SetDiskUsagePercent sets the disk usage percentage for a cluster.
func (r *PrometheusRecorder) SetDiskUsagePercent(cluster, namespace string, percent float64) {
	r.diskUsagePercent.WithLabelValues(cluster, namespace).Set(percent)
}

// SetRecommendationsTotal sets the total recommendations by type.
func (r *PrometheusRecorder) SetRecommendationsTotal(
	cluster, namespace, recType string,
	count float64,
) {
	r.recommendationsTotal.WithLabelValues(cluster, namespace, recType).Set(count)
}

// ObserveRecommendationScanDuration records the duration of a recommendation scan.
func (r *PrometheusRecorder) ObserveRecommendationScanDuration(
	cluster, namespace string,
	duration time.Duration,
) {
	r.recommendationScanDur.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// SetTableBloatRatio sets the bloat ratio for a table.
func (r *PrometheusRecorder) SetTableBloatRatio(cluster, namespace, table string, ratio float64) {
	r.tableBloatRatio.WithLabelValues(cluster, namespace, table).Set(ratio)
}

// boolToFloat64 converts a boolean to a float64 (1.0 for true, 0.0 for false).
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// NoopRecorder is a no-op implementation of Recorder for testing.
type NoopRecorder struct{}

func (n *NoopRecorder) RecordReconcile(_, _, _ string, _ time.Duration)                {}
func (n *NoopRecorder) UpdateClusterInfo(_, _, _, _ string, _ float64)                 {}
func (n *NoopRecorder) SetCoordinatorUp(_, _ string, _ bool)                           {}
func (n *NoopRecorder) SetStandbyUp(_, _ string, _ bool)                               {}
func (n *NoopRecorder) SetSegmentsReady(_, _ string, _ float64)                        {}
func (n *NoopRecorder) SetSegmentsTotal(_, _ string, _ float64)                        {}
func (n *NoopRecorder) SetSegmentsFailed(_, _ string, _ float64)                       {}
func (n *NoopRecorder) SetMirroringInSync(_, _ string, _ bool)                         {}
func (n *NoopRecorder) RecordFTSProbe(_, _, _ string, _ time.Duration)                 {}
func (n *NoopRecorder) RecordFTSFailover(_, _ string)                                  {}
func (n *NoopRecorder) SetSegmentStatus(_, _, _ string, _ bool)                        {}
func (n *NoopRecorder) SetReplicationLag(_, _, _ string, _ float64)                    {}
func (n *NoopRecorder) SetStandbyReplicationLag(_, _ string, _ float64)                {}
func (n *NoopRecorder) RecordConfigReload(_, _ string)                                 {}
func (n *NoopRecorder) SetConnectionsActive(_, _ string, _ float64)                    {}
func (n *NoopRecorder) SetConnectionsMax(_, _ string, _ float64)                       {}
func (n *NoopRecorder) SetDiskUsageBytes(_, _, _ string, _ float64)                    {}
func (n *NoopRecorder) RecordAuthAttempt(_, _ string)                                  {}
func (n *NoopRecorder) SetActiveQueries(_, _ string, _ float64)                        {}
func (n *NoopRecorder) SetQueuedQueries(_, _ string, _ float64)                        {}
func (n *NoopRecorder) SetBlockedQueries(_, _ string, _ float64)                       {}
func (n *NoopRecorder) RecordWorkloadRuleAction(_, _, _, _ string)                     {}
func (n *NoopRecorder) SetResourceGroupUsage(_, _, _ string, _, _ float64)             {}
func (n *NoopRecorder) RecordIdleSessionTermination(_, _, _ string)                    {}
func (n *NoopRecorder) RecordSlowQuery(_, _ string)                                    {}
func (n *NoopRecorder) RecordBackup(_, _, _, _ string)                                 {}
func (n *NoopRecorder) ObserveBackupDuration(_, _ string, _ time.Duration)             {}
func (n *NoopRecorder) SetBackupSizeBytes(_, _ string, _ float64)                      {}
func (n *NoopRecorder) RecordRestore(_, _, _ string)                                   {}
func (n *NoopRecorder) SetDataLoadingJobsActive(_, _ string, _ float64)                {}
func (n *NoopRecorder) RecordDataLoadingRows(_, _, _, _ string, _ float64)             {}
func (n *NoopRecorder) SetDiskUsagePercent(_, _ string, _ float64)                     {}
func (n *NoopRecorder) SetRecommendationsTotal(_, _, _ string, _ float64)              {}
func (n *NoopRecorder) ObserveRecommendationScanDuration(_, _ string, _ time.Duration) {}
func (n *NoopRecorder) SetTableBloatRatio(_, _, _ string, _ float64)                   {}
