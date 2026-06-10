// Package metrics provides Prometheus metrics registration and recording for the cloudberry operator.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "cloudberry"

	labelCluster   = "cluster"
	labelNamespace = "namespace"
	labelPhase     = "phase"
	labelVersion   = "version"
	labelSegment   = "segment"
	labelComponent = "component"
	labelOperation = "operation"
	labelResult    = "result"
	labelType      = "type"
	labelJob       = "job"
	// labelJobName is the non-reserved label carrying the Kubernetes Job name
	// for backup_job_status. The reserved Prometheus label `job` is overwritten
	// by the scrape config's job_name during scrape, so a dedicated label is
	// required to preserve the per-Job identity in VictoriaMetrics.
	labelJobName    = "job_name"
	labelSourceType = "source_type"
	labelSource     = "source"
	labelWebhook    = "webhook"
	labelTimestamp  = "timestamp"
	labelRoute      = "route"
	labelMethod     = "method"
	labelCode       = "code"
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
	ObserveBackupDuration(cluster, namespace, backupType string, duration time.Duration)
	// SetBackupSizeBytes sets the size of a backup identified by its timestamp.
	SetBackupSizeBytes(cluster, namespace, timestamp string, bytes float64)
	// SetBackupLastSuccessTimestamp sets the Unix timestamp of the last successful backup.
	SetBackupLastSuccessTimestamp(cluster, namespace string, ts float64)
	// SetBackupLastStatus sets the last backup status (0=success, 1=failed, 2=in-progress).
	SetBackupLastStatus(cluster, namespace string, status float64)
	// ObserveRestoreDuration records the duration of a restore operation.
	ObserveRestoreDuration(cluster, namespace string, duration time.Duration)
	// RecordBackupRetentionDeleted records the number of backups deleted by retention policy.
	RecordBackupRetentionDeleted(cluster, namespace string, n int)
	// SetBackupJobStatus sets a Kubernetes backup Job status
	// (0=pending, 1=running, 2=succeeded, 3=failed).
	SetBackupJobStatus(cluster, namespace, job, operation string, status float64)
	// RecordRestore records a restore event.
	RecordRestore(cluster, namespace, status string)
	// RecordRestoreValidation records a post-restore validation outcome
	// (result is "success" or "failed").
	RecordRestoreValidation(cluster, namespace, result string)
	// SetDataLoadingJobsActive sets the number of active data loading jobs.
	SetDataLoadingJobsActive(cluster, namespace string, count float64)
	// RecordDataLoadingRows records the number of rows loaded by a job.
	//
	// DOCUMENTED NO-OP IN PRODUCTION (C-6): this method currently has ZERO
	// production call sites — intentionally. The data-loading job mutation
	// endpoints return 501 NOT_IMPLEMENTED (see internal/api
	// msgDataLoadingNotImplemented), so there is no real row count to record
	// and faking one would violate the metrics-honesty rule. The method (and
	// its registered cloudberry_data_loading_rows_total family) is kept and
	// test-covered so the wiring lands together with the data-loading Job
	// feature (rows will come from Job status/completion records).
	RecordDataLoadingRows(cluster, namespace, job, sourceType string, count float64)
	// SetDiskUsagePercent sets the disk usage percentage for a cluster.
	SetDiskUsagePercent(cluster, namespace string, percent float64)
	// SetRecommendationsTotal sets the total recommendations by type.
	SetRecommendationsTotal(cluster, namespace, recType string, count float64)
	// ObserveRecommendationScanDuration records the duration of a recommendation scan.
	ObserveRecommendationScanDuration(cluster, namespace string, duration time.Duration)
	// SetTableBloatRatio sets the bloat ratio for a table.
	SetTableBloatRatio(cluster, namespace, table string, ratio float64)
	// RecordScaleOperation records a scale operation event.
	RecordScaleOperation(cluster, namespace, operation string)
	// SetRedistributionProgress sets the data redistribution progress.
	SetRedistributionProgress(cluster, namespace string, progress float64)
	// SetDataSkewCoefficient sets the data skew coefficient for a cluster.
	SetDataSkewCoefficient(cluster, namespace string, coefficient float64)
	// SetPVCSizeBytes sets the PVC size in bytes for a specific component.
	SetPVCSizeBytes(cluster, namespace, component string, sizeBytes float64)
	// RecordMirroringOperation records a mirroring enable/disable operation.
	RecordMirroringOperation(cluster, namespace, operation string)
	// RecordMaintenanceOperation records a maintenance operation event.
	// result is "success", "failed", or "started".
	RecordMaintenanceOperation(cluster, namespace, operation, result string)
	// RecordPasswordRotation records a password rotation event.
	RecordPasswordRotation()
	// RecordQueryHistoryInsert records a query history insert event.
	RecordQueryHistoryInsert(cluster, namespace string)
	// ObserveQueryHistorySearchDuration records the duration of a history search operation.
	ObserveQueryHistorySearchDuration(cluster, namespace string, duration time.Duration)
	// RecordQueryHistoryExport records a query history export event.
	RecordQueryHistoryExport(cluster, namespace, format string)
	// RecordQueryHistoryRetentionCleanup records the number of entries deleted by retention cleanup.
	RecordQueryHistoryRetentionCleanup(cluster, namespace string, deleted int64)
	// SetQueryHistorySizeBytes sets the current size of the query history table.
	SetQueryHistorySizeBytes(cluster, namespace string, bytes float64)
	// RecordPlanCheck records a plan check operation.
	RecordPlanCheck(cluster, namespace string)
	// RecordPlanCheckIssue records a plan check issue by severity and category.
	RecordPlanCheckIssue(cluster, namespace, severity, category string)
	// ObservePlanCheckDuration records the duration of a plan check operation.
	ObservePlanCheckDuration(cluster, namespace string, duration time.Duration)
	// RecordQueryCancel records a query cancellation via the queries API.
	RecordQueryCancel(cluster, namespace string)
	// RecordQueryMove records a query resource group move.
	RecordQueryMove(cluster, namespace string)
	// RecordExporterHealthCheck records an exporter health check.
	RecordExporterHealthCheck(cluster, namespace string)
	// RecordActiveQueryExport records an active query export operation.
	RecordActiveQueryExport()
	// RecordGuestAccess records a guest access attempt with the result (allowed/denied).
	RecordGuestAccess(cluster, namespace string, allowed bool)
	// RecordMonitorPause records a query monitor pause event.
	RecordMonitorPause(cluster, namespace string)
	// RecordMonitorResume records a query monitor resume event.
	RecordMonitorResume(cluster, namespace string)
	// RecordMonitoringDisabledAccess records an access attempt to a monitoring endpoint
	// when query monitoring is disabled for the cluster.
	RecordMonitoringDisabledAccess(cluster, namespace string)
	// RecordCertRotation records a webhook certificate rotation event.
	// source is "vault-pki" or "self-signed"; result is "success" or "error".
	RecordCertRotation(component, source, result string)
	// SetCertExpirySeconds sets the seconds until the certificate expires for a component.
	SetCertExpirySeconds(component string, seconds float64)
	// RecordClusterCertIssuance records an auto-issuance (or renewal) of a
	// CloudberryCluster server certificate from Vault PKI (spec.auth.ssl).
	// result is "success" or "error".
	RecordClusterCertIssuance(cluster, namespace, result string)
	// RecordVaultOperation records a Vault operation event.
	// operation is "auth", "read", "write", "renew" (background token
	// renewal), or "reauth" (re-login after a 401/403 or token expiry);
	// result is "success" or "error".
	RecordVaultOperation(operation, result string)
	// ObserveVaultOperationDuration records the duration of a Vault operation.
	ObserveVaultOperationDuration(operation string, d time.Duration)
	// RecordWebhookAdmission records an admission webhook decision.
	// webhook is "validating" or "mutating"; operation is "create", "update", or "delete";
	// result is "allowed", "denied", or "error".
	RecordWebhookAdmission(webhook, operation, result string)
	// RecordUpgradeOperation records a cluster upgrade operation event.
	// result is "started", "completed", "rollback", or "failed".
	RecordUpgradeOperation(cluster, namespace, result string)
	// RecordRollingRestart records a rolling restart operation event.
	// result is "started", "completed", or "failed".
	RecordRollingRestart(cluster, namespace, result string)
	// RecordRecoveryOperation records a recovery operation event.
	// recoveryType is "incremental", "full", or "differential";
	// result is "started", "completed", or "failed".
	RecordRecoveryOperation(cluster, namespace, recoveryType, result string)

	// RecordAPIRequest records a completed REST API request. route is the
	// matched route TEMPLATE (e.g. "/api/v1alpha1/clusters/{name}"), never the
	// raw path, to keep label cardinality bounded; code is the HTTP status.
	RecordAPIRequest(route, method, code string, duration time.Duration)
	// AddAPIRequestsInFlight adjusts the in-flight REST API request gauge
	// (+1 on request start, -1 on completion).
	AddAPIRequestsInFlight(delta float64)
	// RecordRateLimitRejection records a request rejected (429) by the API
	// rate limiter for the given route template.
	RecordRateLimitRejection(route string)

	// RecordDBConnect records a database connection attempt for a cluster.
	// result is "success" or "error"; duration is the connect (incl. retry) time.
	RecordDBConnect(cluster, namespace, result string, duration time.Duration)
	// ObserveDBQueryDuration records the duration of a database operation.
	// operation is the db.Client method name (bounded set).
	ObserveDBQueryDuration(operation string, d time.Duration)
	// RegisterDBPoolStats registers a per-cluster connection-pool stats
	// provider sampled on every Prometheus scrape (pgxpool.Stat()). The
	// returned function unregisters the provider and MUST be called when the
	// client is closed.
	RegisterDBPoolStats(cluster, namespace string, stats DBPoolStatsFunc) (unregister func())

	// SetIdleDaemonUp sets the idle-session daemon liveness gauge
	// (1 while the daemon scan loop runs, 0 after Stop or a fatal exit).
	SetIdleDaemonUp(cluster, namespace string, up bool)
	// RecordIdleScanFailure records a failed idle-session scan cycle.
	RecordIdleScanFailure(cluster, namespace string)
	// RecordIdleReconnectAttempt records an idle-daemon DB reconnect attempt.
	// result is "success" or "error".
	RecordIdleReconnectAttempt(cluster, namespace, result string)

	// RecordSessionTermination records a session termination via the API
	// (pg_terminate_backend). result is "success" or "error".
	RecordSessionTermination(cluster, namespace, result string)

	// RecordStorageExpansion records a PVC storage expansion attempt.
	// result is "success" or "error".
	RecordStorageExpansion(cluster, namespace, result string)
	// RecordBackupOnDelete records the terminal outcome of a backup-on-delete
	// Job ("completed" or "failed").
	RecordBackupOnDelete(cluster, namespace, result string)
	// ObserveScalePhaseDuration records the duration of one scale state-machine
	// phase. direction is "out" or "in"; phase is from the bounded phase enum.
	ObserveScalePhaseDuration(direction, phase string, d time.Duration)

	// RecordMigrateOperation records a cross-cluster migrate API operation.
	// result is "started" or "error".
	RecordMigrateOperation(result string)
	// RecordAPIClusterOperation records a cluster CRUD operation via the API.
	// operation is "create" or "delete"; result is "success" or "error".
	RecordAPIClusterOperation(operation, result string)
	// RecordLogStreamSession records a completed backup-Job log stream session.
	// result is "success" or "error".
	RecordLogStreamSession(result string)
	// AddLogStreamBytes adds the number of bytes delivered to log stream clients.
	AddLogStreamBytes(n float64)
	// RecordOIDCDiscovery records an OIDC provider discovery attempt.
	// result is "success" or "error".
	RecordOIDCDiscovery(result string)
	// ObserveAuthTokenVerifyDuration records the latency of a Bearer token
	// verification (JWKS fetch + signature check).
	ObserveAuthTokenVerifyDuration(d time.Duration)
}

// DBPoolStatsFunc returns a point-in-time snapshot of a database connection
// pool: acquired (in-use) connections, idle connections, and the pool's
// configured maximum. It is invoked on every Prometheus scrape.
type DBPoolStatsFunc func() (acquired, idle, maxConns float64)

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

	backupTotal                *prometheus.CounterVec
	backupDuration             *prometheus.HistogramVec
	backupSizeBytes            *prometheus.GaugeVec
	backupLastSuccessTimestamp *prometheus.GaugeVec
	backupLastStatus           *prometheus.GaugeVec
	restoreDuration            *prometheus.HistogramVec
	backupRetentionDeleted     *prometheus.CounterVec
	backupJobStatus            *prometheus.GaugeVec
	restoreTotal               *prometheus.CounterVec
	restoreValidationTotal     *prometheus.CounterVec
	dataLoadingJobsGauge       *prometheus.GaugeVec
	dataLoadingRows            *prometheus.CounterVec

	diskUsagePercent      *prometheus.GaugeVec
	recommendationsTotal  *prometheus.GaugeVec
	recommendationScanDur *prometheus.HistogramVec
	tableBloatRatio       *prometheus.GaugeVec

	scaleOperationsTotal       *prometheus.CounterVec
	redistributionProgressVec  *prometheus.GaugeVec
	dataSkewCoefficient        *prometheus.GaugeVec
	pvcSizeBytes               *prometheus.GaugeVec
	mirroringOperationsTotal   *prometheus.CounterVec
	maintenanceOperationsTotal *prometheus.CounterVec
	passwordRotationTotal      prometheus.Counter

	queryHistoryTotal          *prometheus.CounterVec
	queryHistorySearchDuration *prometheus.HistogramVec
	queryHistoryExportTotal    *prometheus.CounterVec
	queryHistoryRetentionTotal *prometheus.CounterVec
	queryHistorySizeBytes      *prometheus.GaugeVec

	planCheckTotal       *prometheus.CounterVec
	planCheckIssuesTotal *prometheus.CounterVec
	planCheckDuration    *prometheus.HistogramVec

	queryCancelTotal              *prometheus.CounterVec
	queryMoveTotal                *prometheus.CounterVec
	exporterHealthCheckTotal      *prometheus.CounterVec
	activeQueryExportTotal        prometheus.Counter
	guestAccessTotal              *prometheus.CounterVec
	monitorPauseTotal             *prometheus.CounterVec
	monitorResumeTotal            *prometheus.CounterVec
	monitoringDisabledAccessTotal *prometheus.CounterVec

	certRotationTotal       *prometheus.CounterVec
	certExpirySeconds       *prometheus.GaugeVec
	clusterCertIssuance     *prometheus.CounterVec
	vaultOperationsTotal    *prometheus.CounterVec
	vaultOperationDuration  *prometheus.HistogramVec
	webhookAdmissionTotal   *prometheus.CounterVec
	upgradeOperationsTotal  *prometheus.CounterVec
	rollingRestartTotal     *prometheus.CounterVec
	recoveryOperationsTotal *prometheus.CounterVec

	apiRequestsTotal        *prometheus.CounterVec
	apiRequestDuration      *prometheus.HistogramVec
	apiRequestsInFlight     prometheus.Gauge
	rateLimitRejectionTotal *prometheus.CounterVec

	dbConnectTotal    *prometheus.CounterVec
	dbConnectDuration *prometheus.HistogramVec
	dbQueryDuration   *prometheus.HistogramVec
	dbPoolCollector   *dbPoolStatsCollector

	idleDaemonUp              *prometheus.GaugeVec
	idleScanFailuresTotal     *prometheus.CounterVec
	idleReconnectAttemptTotal *prometheus.CounterVec

	sessionTerminationsTotal *prometheus.CounterVec

	storageExpansionsTotal *prometheus.CounterVec
	backupOnDeleteTotal    *prometheus.CounterVec
	scalePhaseDuration     *prometheus.HistogramVec

	migrateOperationsTotal    *prometheus.CounterVec
	apiClusterOperationsTotal *prometheus.CounterVec
	logStreamSessionsTotal    *prometheus.CounterVec
	logStreamBytesTotal       prometheus.Counter
	oidcDiscoveryTotal        *prometheus.CounterVec
	authTokenVerifyDuration   prometheus.Histogram
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
	r.passwordRotationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "password_rotation_total",
		Help:      "Total number of admin password rotations.",
	})
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
	}, []string{labelCluster, labelNamespace, labelType})
	r.backupSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "backup_size_bytes",
		Help:      "Size of a backup in bytes per timestamp.",
	}, []string{labelCluster, labelNamespace, labelTimestamp})
	r.backupLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "backup_last_success_timestamp",
		Help:      "Unix timestamp of the last successful backup.",
	}, []string{labelCluster, labelNamespace})
	r.backupLastStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "backup_last_status",
		Help:      "Last backup status (0=success, 1=failed, 2=in-progress).",
	}, []string{labelCluster, labelNamespace})
	r.restoreDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "restore_duration_seconds",
		Help:      "Duration of restore operations in seconds.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{labelCluster, labelNamespace})
	r.backupRetentionDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "backup_retention_deleted_total",
		Help:      "Total number of backups deleted by retention policy.",
	}, []string{labelCluster, labelNamespace})
	r.backupJobStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "backup_job_status",
		Help:      "Kubernetes backup Job status (0=pending, 1=running, 2=succeeded, 3=failed).",
	}, []string{labelCluster, labelNamespace, labelJobName, labelOperation})
	r.restoreTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "restore_total",
		Help:      "Total number of restore operations.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.restoreValidationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "restore_validation_total",
		Help:      "Total number of post-restore validation outcomes.",
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
	r.scaleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "scale_operations_total",
		Help:      "Total number of scale operations.",
	}, []string{labelCluster, labelNamespace, labelOperation})
	r.redistributionProgressVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "redistribution_progress",
		Help:      "Data redistribution progress (0.0 to 1.0).",
	}, []string{labelCluster, labelNamespace})
	r.dataSkewCoefficient = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "data_skew_coefficient",
		Help:      "Data skew coefficient across segments.",
	}, []string{labelCluster, labelNamespace})
	r.pvcSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "pvc_size_bytes",
		Help:      "PVC size in bytes per component.",
	}, []string{labelCluster, labelNamespace, labelComponent})
	r.mirroringOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "mirroring_operations_total",
		Help:      "Total number of mirroring enable/disable operations.",
	}, []string{labelCluster, labelNamespace, labelOperation})
	r.maintenanceOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "maintenance_operations_total",
		Help:      "Total number of maintenance operations (vacuum, analyze, reindex, log-rotate).",
	}, []string{labelCluster, labelNamespace, labelOperation, labelResult})
}

// initQueryHistoryMetrics initializes query history metrics.
func (r *PrometheusRecorder) initQueryHistoryMetrics() {
	r.queryHistoryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "query_history_total",
		Help:      "Total queries recorded in history.",
	}, []string{labelCluster, labelNamespace})
	r.queryHistorySearchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "query_history_search_duration_seconds",
		Help:      "Duration of history search operations in seconds.",
		Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{labelCluster, labelNamespace})
	r.queryHistoryExportTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "query_history_export_total",
		Help:      "Total history exports.",
	}, []string{labelCluster, labelNamespace, "format"})
	r.queryHistoryRetentionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "query_history_retention_deleted_total",
		Help:      "Queries deleted by retention policy.",
	}, []string{labelCluster, labelNamespace})
	r.queryHistorySizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "query_history_size_bytes",
		Help:      "Current size of query history table.",
	}, []string{labelCluster, labelNamespace})
}

// initPlanCheckMetrics initializes plan check metrics.
func (r *PrometheusRecorder) initPlanCheckMetrics() {
	r.planCheckTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "plan_check_total",
		Help:      "Total plan checks performed.",
	}, []string{labelCluster, labelNamespace})
	r.planCheckIssuesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "plan_check_issues_total",
		Help:      "Plan check issues found by severity and category.",
	}, []string{labelCluster, labelNamespace, "severity", "category"})
	r.planCheckDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "plan_check_duration_seconds",
		Help:      "Duration of plan check operations in seconds.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{labelCluster, labelNamespace})
}

// initQueryOperationMetrics initializes query operation metrics (cancel, move, exporter health).
func (r *PrometheusRecorder) initQueryOperationMetrics() {
	r.queryCancelTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "query_cancel_total",
		Help:      "Total number of query cancellations via the queries API.",
	}, []string{labelCluster, labelNamespace})
	r.queryMoveTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "query_move_total",
		Help:      "Total number of query resource group moves.",
	}, []string{labelCluster, labelNamespace})
	r.exporterHealthCheckTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "exporter_health_check_total",
		Help:      "Total number of exporter health checks.",
	}, []string{labelCluster, labelNamespace})
	r.activeQueryExportTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "active_query_export_total",
		Help:      "Total number of active query exports.",
	})
	r.guestAccessTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "guest_access_total",
		Help:      "Total number of guest access attempts.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.monitorPauseTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "monitor_pause_total",
		Help:      "Total number of query monitor pause operations.",
	}, []string{labelCluster, labelNamespace})
	r.monitorResumeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "monitor_resume_total",
		Help:      "Total number of query monitor resume operations.",
	}, []string{labelCluster, labelNamespace})
	r.monitoringDisabledAccessTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "monitoring_disabled_access_total",
		Help:      "Total number of access attempts to monitoring endpoints when monitoring is disabled.",
	}, []string{labelCluster, labelNamespace})
}

// initSecurityMetrics initializes certificate and vault security metrics.
func (r *PrometheusRecorder) initSecurityMetrics() {
	r.certRotationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "cert_rotation_total",
		Help:      "Total number of webhook certificate rotations.",
	}, []string{labelComponent, labelSource, labelResult})
	r.certExpirySeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "cert_expiry_seconds",
		Help:      "Seconds until the certificate expires per component.",
	}, []string{labelComponent})
	r.clusterCertIssuance = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "cluster_cert_issuance_total",
		Help: "Total number of CloudberryCluster server certificate issuances " +
			"(and renewals) from Vault PKI.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.vaultOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "vault_operations_total",
		Help:      "Total number of Vault operations.",
	}, []string{labelOperation, labelResult})
	r.vaultOperationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "vault_operation_duration_seconds",
		Help:      "Duration of Vault operations in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelOperation})
}

// initAdmissionMetrics initializes webhook admission metrics.
func (r *PrometheusRecorder) initAdmissionMetrics() {
	r.webhookAdmissionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "webhook_admission_total",
		Help:      "Total number of admission webhook decisions.",
	}, []string{labelWebhook, labelOperation, labelResult})
}

// initLifecycleMetrics initializes upgrade, rolling restart, and recovery metrics.
func (r *PrometheusRecorder) initLifecycleMetrics() {
	r.upgradeOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "upgrade_operations_total",
		Help:      "Total number of cluster upgrade operations.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.rollingRestartTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "rolling_restart_total",
		Help:      "Total number of rolling restart operations.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.recoveryOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "recovery_operations_total",
		Help:      "Total number of recovery operations.",
	}, []string{labelCluster, labelNamespace, labelType, labelResult})
}

// initAPIServerMetrics initializes the generic REST API server metrics.
func (r *PrometheusRecorder) initAPIServerMetrics() {
	r.apiRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "api_requests_total",
		Help:      "Total number of REST API requests by route template, method and status code.",
	}, []string{labelRoute, labelMethod, labelCode})
	r.apiRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "api_request_duration_seconds",
		Help:      "Duration of REST API requests in seconds by route template and method.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelRoute, labelMethod})
	r.apiRequestsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "api_requests_in_flight",
		Help:      "Number of REST API requests currently being served.",
	})
	r.rateLimitRejectionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "api_rate_limit_rejections_total",
		Help:      "Total number of REST API requests rejected by the rate limiter (429).",
	}, []string{labelRoute})
}

// initDBMetrics initializes database connection, pool and query metrics.
func (r *PrometheusRecorder) initDBMetrics() {
	r.dbConnectTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "db_connect_total",
		Help:      "Total number of database connection attempts per cluster.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.dbConnectDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "db_connect_duration_seconds",
		Help:      "Duration of database connection establishment in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelCluster, labelNamespace})
	r.dbQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "db_query_duration_seconds",
		Help:      "Duration of database operations in seconds by operation name.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300},
	}, []string{labelOperation})
	r.dbPoolCollector = newDBPoolStatsCollector()
}

// initIdleDaemonMetrics initializes idle-session daemon health metrics.
func (r *PrometheusRecorder) initIdleDaemonMetrics() {
	r.idleDaemonUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "idle_daemon_up",
		Help:      "Idle session daemon liveness per cluster (1=running, 0=stopped).",
	}, []string{labelCluster, labelNamespace})
	r.idleScanFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "idle_scan_failures_total",
		Help:      "Total number of failed idle session scan cycles.",
	}, []string{labelCluster, labelNamespace})
	r.idleReconnectAttemptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "idle_reconnect_attempts_total",
		Help:      "Total number of idle daemon database reconnect attempts.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.sessionTerminationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "session_terminations_total",
		Help:      "Total number of session terminations requested via the API.",
	}, []string{labelCluster, labelNamespace, labelResult})
}

// initControllerOperationMetrics initializes storage expansion, backup-on-delete
// and scale phase metrics.
func (r *PrometheusRecorder) initControllerOperationMetrics() {
	r.storageExpansionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "storage_expansions_total",
		Help:      "Total number of PVC storage expansion attempts.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.backupOnDeleteTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "backup_on_delete_total",
		Help:      "Total number of backup-on-delete terminal outcomes.",
	}, []string{labelCluster, labelNamespace, labelResult})
	r.scalePhaseDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "scale_phase_duration_seconds",
		Help:      "Duration of scale operation phases in seconds.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"direction", labelPhase})
}

// initAPIBusinessMetrics initializes migrate, cluster CRUD, log stream and
// auth/OIDC metrics.
func (r *PrometheusRecorder) initAPIBusinessMetrics() {
	r.migrateOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "migrate_operations_total",
		Help:      "Total number of cross-cluster migrate operations via the API.",
	}, []string{labelResult})
	r.apiClusterOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "api_cluster_operations_total",
		Help:      "Total number of cluster create/delete operations via the API.",
	}, []string{labelOperation, labelResult})
	r.logStreamSessionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "log_stream_sessions_total",
		Help:      "Total number of backup Job log streaming sessions.",
	}, []string{labelResult})
	r.logStreamBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "log_stream_bytes_total",
		Help:      "Total number of bytes streamed to log stream clients.",
	})
	r.oidcDiscoveryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "oidc_discovery_total",
		Help:      "Total number of OIDC provider discovery attempts.",
	}, []string{labelResult})
	r.authTokenVerifyDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "auth_token_verify_duration_seconds",
		Help:      "Duration of Bearer token verification in seconds.",
		Buckets:   prometheus.DefBuckets,
	})
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
	r.initQueryHistoryMetrics()
	r.initPlanCheckMetrics()
	r.initQueryOperationMetrics()
	r.initSecurityMetrics()
	r.initAdmissionMetrics()
	r.initLifecycleMetrics()
	r.initAPIServerMetrics()
	r.initDBMetrics()
	r.initIdleDaemonMetrics()
	r.initControllerOperationMetrics()
	r.initAPIBusinessMetrics()
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
		r.backupLastSuccessTimestamp, r.backupLastStatus, r.restoreDuration,
		r.backupRetentionDeleted, r.backupJobStatus,
		r.restoreTotal, r.restoreValidationTotal,
		r.dataLoadingJobsGauge, r.dataLoadingRows,
		r.diskUsagePercent, r.recommendationsTotal,
		r.recommendationScanDur, r.tableBloatRatio,
		r.scaleOperationsTotal, r.redistributionProgressVec,
		r.dataSkewCoefficient, r.pvcSizeBytes,
		r.mirroringOperationsTotal, r.maintenanceOperationsTotal,
		r.passwordRotationTotal,
		r.queryHistoryTotal, r.queryHistorySearchDuration,
		r.queryHistoryExportTotal, r.queryHistoryRetentionTotal,
		r.queryHistorySizeBytes,
		r.planCheckTotal, r.planCheckIssuesTotal, r.planCheckDuration,
		r.queryCancelTotal, r.queryMoveTotal, r.exporterHealthCheckTotal,
		r.activeQueryExportTotal, r.guestAccessTotal,
		r.monitorPauseTotal, r.monitorResumeTotal,
		r.monitoringDisabledAccessTotal,
		r.certRotationTotal, r.certExpirySeconds, r.clusterCertIssuance,
		r.vaultOperationsTotal, r.vaultOperationDuration,
		r.webhookAdmissionTotal,
		r.upgradeOperationsTotal, r.rollingRestartTotal,
		r.recoveryOperationsTotal,
		r.apiRequestsTotal, r.apiRequestDuration,
		r.apiRequestsInFlight, r.rateLimitRejectionTotal,
		r.dbConnectTotal, r.dbConnectDuration, r.dbQueryDuration,
		r.dbPoolCollector,
		r.idleDaemonUp, r.idleScanFailuresTotal, r.idleReconnectAttemptTotal,
		r.sessionTerminationsTotal,
		r.storageExpansionsTotal, r.backupOnDeleteTotal, r.scalePhaseDuration,
		r.migrateOperationsTotal, r.apiClusterOperationsTotal,
		r.logStreamSessionsTotal, r.logStreamBytesTotal,
		r.oidcDiscoveryTotal, r.authTokenVerifyDuration,
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
func (r *PrometheusRecorder) ObserveBackupDuration(cluster, namespace, backupType string, duration time.Duration) {
	r.backupDuration.WithLabelValues(cluster, namespace, backupType).Observe(duration.Seconds())
}

// SetBackupSizeBytes sets the size of a backup identified by its timestamp.
func (r *PrometheusRecorder) SetBackupSizeBytes(cluster, namespace, timestamp string, bytes float64) {
	r.backupSizeBytes.WithLabelValues(cluster, namespace, timestamp).Set(bytes)
}

// SetBackupLastSuccessTimestamp sets the Unix timestamp of the last successful backup.
func (r *PrometheusRecorder) SetBackupLastSuccessTimestamp(cluster, namespace string, ts float64) {
	r.backupLastSuccessTimestamp.WithLabelValues(cluster, namespace).Set(ts)
}

// SetBackupLastStatus sets the last backup status (0=success, 1=failed, 2=in-progress).
func (r *PrometheusRecorder) SetBackupLastStatus(cluster, namespace string, status float64) {
	r.backupLastStatus.WithLabelValues(cluster, namespace).Set(status)
}

// ObserveRestoreDuration records the duration of a restore operation.
func (r *PrometheusRecorder) ObserveRestoreDuration(cluster, namespace string, duration time.Duration) {
	r.restoreDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// RecordBackupRetentionDeleted records the number of backups deleted by retention policy.
func (r *PrometheusRecorder) RecordBackupRetentionDeleted(cluster, namespace string, n int) {
	r.backupRetentionDeleted.WithLabelValues(cluster, namespace).Add(float64(n))
}

// SetBackupJobStatus sets a Kubernetes backup Job status
// (0=pending, 1=running, 2=succeeded, 3=failed).
func (r *PrometheusRecorder) SetBackupJobStatus(cluster, namespace, job, operation string, status float64) {
	r.backupJobStatus.WithLabelValues(cluster, namespace, job, operation).Set(status)
}

// RecordRestore records a restore event.
func (r *PrometheusRecorder) RecordRestore(cluster, namespace, status string) {
	r.restoreTotal.WithLabelValues(cluster, namespace, status).Inc()
}

// RecordRestoreValidation records a post-restore validation outcome
// (result is "success" or "failed").
func (r *PrometheusRecorder) RecordRestoreValidation(cluster, namespace, result string) {
	r.restoreValidationTotal.WithLabelValues(cluster, namespace, result).Inc()
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

// RecordScaleOperation records a scale operation event.
func (r *PrometheusRecorder) RecordScaleOperation(cluster, namespace, operation string) {
	r.scaleOperationsTotal.WithLabelValues(cluster, namespace, operation).Inc()
}

// SetRedistributionProgress sets the data redistribution progress.
func (r *PrometheusRecorder) SetRedistributionProgress(cluster, namespace string, progress float64) {
	r.redistributionProgressVec.WithLabelValues(cluster, namespace).Set(progress)
}

// SetDataSkewCoefficient sets the data skew coefficient for a cluster.
func (r *PrometheusRecorder) SetDataSkewCoefficient(cluster, namespace string, coefficient float64) {
	r.dataSkewCoefficient.WithLabelValues(cluster, namespace).Set(coefficient)
}

// SetPVCSizeBytes sets the PVC size in bytes for a specific component.
func (r *PrometheusRecorder) SetPVCSizeBytes(cluster, namespace, component string, sizeBytes float64) {
	r.pvcSizeBytes.WithLabelValues(cluster, namespace, component).Set(sizeBytes)
}

// RecordMirroringOperation records a mirroring enable/disable operation.
func (r *PrometheusRecorder) RecordMirroringOperation(cluster, namespace, operation string) {
	r.mirroringOperationsTotal.WithLabelValues(cluster, namespace, operation).Inc()
}

// RecordMaintenanceOperation records a maintenance operation event.
func (r *PrometheusRecorder) RecordMaintenanceOperation(cluster, namespace, operation, result string) {
	r.maintenanceOperationsTotal.WithLabelValues(cluster, namespace, operation, result).Inc()
}

// RecordPasswordRotation records a password rotation event.
func (r *PrometheusRecorder) RecordPasswordRotation() {
	r.passwordRotationTotal.Inc()
}

// RecordQueryHistoryInsert records a query history insert event.
func (r *PrometheusRecorder) RecordQueryHistoryInsert(cluster, namespace string) {
	r.queryHistoryTotal.WithLabelValues(cluster, namespace).Inc()
}

// ObserveQueryHistorySearchDuration records the duration of a history search operation.
func (r *PrometheusRecorder) ObserveQueryHistorySearchDuration(cluster, namespace string, duration time.Duration) {
	r.queryHistorySearchDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// RecordQueryHistoryExport records a query history export event.
func (r *PrometheusRecorder) RecordQueryHistoryExport(cluster, namespace, format string) {
	r.queryHistoryExportTotal.WithLabelValues(cluster, namespace, format).Inc()
}

// RecordQueryHistoryRetentionCleanup records the number of entries deleted by retention cleanup.
func (r *PrometheusRecorder) RecordQueryHistoryRetentionCleanup(cluster, namespace string, deleted int64) {
	r.queryHistoryRetentionTotal.WithLabelValues(cluster, namespace).Add(float64(deleted))
}

// SetQueryHistorySizeBytes sets the current size of the query history table.
func (r *PrometheusRecorder) SetQueryHistorySizeBytes(cluster, namespace string, bytes float64) {
	r.queryHistorySizeBytes.WithLabelValues(cluster, namespace).Set(bytes)
}

// RecordPlanCheck records a plan check operation.
func (r *PrometheusRecorder) RecordPlanCheck(cluster, namespace string) {
	r.planCheckTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordPlanCheckIssue records a plan check issue by severity and category.
func (r *PrometheusRecorder) RecordPlanCheckIssue(cluster, namespace, severity, category string) {
	r.planCheckIssuesTotal.WithLabelValues(cluster, namespace, severity, category).Inc()
}

// ObservePlanCheckDuration records the duration of a plan check operation.
func (r *PrometheusRecorder) ObservePlanCheckDuration(cluster, namespace string, duration time.Duration) {
	r.planCheckDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// RecordQueryCancel records a query cancellation via the queries API.
func (r *PrometheusRecorder) RecordQueryCancel(cluster, namespace string) {
	r.queryCancelTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordQueryMove records a query resource group move.
func (r *PrometheusRecorder) RecordQueryMove(cluster, namespace string) {
	r.queryMoveTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordExporterHealthCheck records an exporter health check.
func (r *PrometheusRecorder) RecordExporterHealthCheck(cluster, namespace string) {
	r.exporterHealthCheckTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordActiveQueryExport records an active query export operation.
func (r *PrometheusRecorder) RecordActiveQueryExport() {
	r.activeQueryExportTotal.Inc()
}

// RecordGuestAccess records a guest access attempt with the result (allowed/denied).
func (r *PrometheusRecorder) RecordGuestAccess(cluster, namespace string, allowed bool) {
	result := "denied"
	if allowed {
		result = "allowed"
	}
	r.guestAccessTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordMonitorPause records a query monitor pause event.
func (r *PrometheusRecorder) RecordMonitorPause(cluster, namespace string) {
	r.monitorPauseTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordMonitorResume records a query monitor resume event.
func (r *PrometheusRecorder) RecordMonitorResume(cluster, namespace string) {
	r.monitorResumeTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordMonitoringDisabledAccess records an access attempt to a monitoring endpoint
// when query monitoring is disabled for the cluster.
func (r *PrometheusRecorder) RecordMonitoringDisabledAccess(cluster, namespace string) {
	r.monitoringDisabledAccessTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordCertRotation records a webhook certificate rotation event.
func (r *PrometheusRecorder) RecordCertRotation(component, source, result string) {
	r.certRotationTotal.WithLabelValues(component, source, result).Inc()
}

// SetCertExpirySeconds sets the seconds until the certificate expires for a component.
func (r *PrometheusRecorder) SetCertExpirySeconds(component string, seconds float64) {
	r.certExpirySeconds.WithLabelValues(component).Set(seconds)
}

// RecordClusterCertIssuance records an auto-issuance (or renewal) of a
// CloudberryCluster server certificate from Vault PKI.
func (r *PrometheusRecorder) RecordClusterCertIssuance(cluster, namespace, result string) {
	r.clusterCertIssuance.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordVaultOperation records a Vault operation event.
func (r *PrometheusRecorder) RecordVaultOperation(operation, result string) {
	r.vaultOperationsTotal.WithLabelValues(operation, result).Inc()
}

// ObserveVaultOperationDuration records the duration of a Vault operation.
func (r *PrometheusRecorder) ObserveVaultOperationDuration(operation string, d time.Duration) {
	r.vaultOperationDuration.WithLabelValues(operation).Observe(d.Seconds())
}

// RecordWebhookAdmission records an admission webhook decision.
func (r *PrometheusRecorder) RecordWebhookAdmission(webhook, operation, result string) {
	r.webhookAdmissionTotal.WithLabelValues(webhook, operation, result).Inc()
}

// RecordUpgradeOperation records a cluster upgrade operation event.
func (r *PrometheusRecorder) RecordUpgradeOperation(cluster, namespace, result string) {
	r.upgradeOperationsTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordRollingRestart records a rolling restart operation event.
func (r *PrometheusRecorder) RecordRollingRestart(cluster, namespace, result string) {
	r.rollingRestartTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordRecoveryOperation records a recovery operation event.
func (r *PrometheusRecorder) RecordRecoveryOperation(cluster, namespace, recoveryType, result string) {
	r.recoveryOperationsTotal.WithLabelValues(cluster, namespace, recoveryType, result).Inc()
}

// RecordAPIRequest records a completed REST API request.
func (r *PrometheusRecorder) RecordAPIRequest(route, method, code string, duration time.Duration) {
	r.apiRequestsTotal.WithLabelValues(route, method, code).Inc()
	r.apiRequestDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// AddAPIRequestsInFlight adjusts the in-flight REST API request gauge.
func (r *PrometheusRecorder) AddAPIRequestsInFlight(delta float64) {
	r.apiRequestsInFlight.Add(delta)
}

// RecordRateLimitRejection records a request rejected (429) by the API rate limiter.
func (r *PrometheusRecorder) RecordRateLimitRejection(route string) {
	r.rateLimitRejectionTotal.WithLabelValues(route).Inc()
}

// RecordDBConnect records a database connection attempt for a cluster.
func (r *PrometheusRecorder) RecordDBConnect(cluster, namespace, result string, duration time.Duration) {
	r.dbConnectTotal.WithLabelValues(cluster, namespace, result).Inc()
	r.dbConnectDuration.WithLabelValues(cluster, namespace).Observe(duration.Seconds())
}

// ObserveDBQueryDuration records the duration of a database operation.
func (r *PrometheusRecorder) ObserveDBQueryDuration(operation string, d time.Duration) {
	r.dbQueryDuration.WithLabelValues(operation).Observe(d.Seconds())
}

// RegisterDBPoolStats registers a per-cluster connection-pool stats provider
// sampled on every Prometheus scrape. The returned function unregisters it.
func (r *PrometheusRecorder) RegisterDBPoolStats(
	cluster, namespace string,
	stats DBPoolStatsFunc,
) func() {
	return r.dbPoolCollector.add(cluster, namespace, stats)
}

// SetIdleDaemonUp sets the idle-session daemon liveness gauge.
func (r *PrometheusRecorder) SetIdleDaemonUp(cluster, namespace string, up bool) {
	r.idleDaemonUp.WithLabelValues(cluster, namespace).Set(boolToFloat64(up))
}

// RecordIdleScanFailure records a failed idle-session scan cycle.
func (r *PrometheusRecorder) RecordIdleScanFailure(cluster, namespace string) {
	r.idleScanFailuresTotal.WithLabelValues(cluster, namespace).Inc()
}

// RecordIdleReconnectAttempt records an idle-daemon DB reconnect attempt.
func (r *PrometheusRecorder) RecordIdleReconnectAttempt(cluster, namespace, result string) {
	r.idleReconnectAttemptTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordSessionTermination records a session termination via the API.
func (r *PrometheusRecorder) RecordSessionTermination(cluster, namespace, result string) {
	r.sessionTerminationsTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordStorageExpansion records a PVC storage expansion attempt.
func (r *PrometheusRecorder) RecordStorageExpansion(cluster, namespace, result string) {
	r.storageExpansionsTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// RecordBackupOnDelete records the terminal outcome of a backup-on-delete Job.
func (r *PrometheusRecorder) RecordBackupOnDelete(cluster, namespace, result string) {
	r.backupOnDeleteTotal.WithLabelValues(cluster, namespace, result).Inc()
}

// ObserveScalePhaseDuration records the duration of one scale state-machine phase.
func (r *PrometheusRecorder) ObserveScalePhaseDuration(direction, phase string, d time.Duration) {
	r.scalePhaseDuration.WithLabelValues(direction, phase).Observe(d.Seconds())
}

// RecordMigrateOperation records a cross-cluster migrate API operation.
func (r *PrometheusRecorder) RecordMigrateOperation(result string) {
	r.migrateOperationsTotal.WithLabelValues(result).Inc()
}

// RecordAPIClusterOperation records a cluster CRUD operation via the API.
func (r *PrometheusRecorder) RecordAPIClusterOperation(operation, result string) {
	r.apiClusterOperationsTotal.WithLabelValues(operation, result).Inc()
}

// RecordLogStreamSession records a completed backup-Job log stream session.
func (r *PrometheusRecorder) RecordLogStreamSession(result string) {
	r.logStreamSessionsTotal.WithLabelValues(result).Inc()
}

// AddLogStreamBytes adds the number of bytes delivered to log stream clients.
func (r *PrometheusRecorder) AddLogStreamBytes(n float64) {
	r.logStreamBytesTotal.Add(n)
}

// RecordOIDCDiscovery records an OIDC provider discovery attempt.
func (r *PrometheusRecorder) RecordOIDCDiscovery(result string) {
	r.oidcDiscoveryTotal.WithLabelValues(result).Inc()
}

// ObserveAuthTokenVerifyDuration records the latency of a Bearer token verification.
func (r *PrometheusRecorder) ObserveAuthTokenVerifyDuration(d time.Duration) {
	r.authTokenVerifyDuration.Observe(d.Seconds())
}

// boolToFloat64 converts a boolean to a float64 (1.0 for true, 0.0 for false).
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// NoopRecorder is a no-op implementation of Recorder for testing.
// All methods intentionally do nothing as this recorder is used in
// unit tests where metric recording is not needed.
type NoopRecorder struct{}

// NewNoopRecorder creates a new NoopRecorder instance.
func NewNoopRecorder() *NoopRecorder {
	return &NoopRecorder{}
}

// RecordReconcile is a no-op implementation for testing.
func (n *NoopRecorder) RecordReconcile(_, _, _ string, _ time.Duration) {}

// UpdateClusterInfo is a no-op implementation for testing.
func (n *NoopRecorder) UpdateClusterInfo(_, _, _, _ string, _ float64) {}

// SetCoordinatorUp is a no-op implementation for testing.
func (n *NoopRecorder) SetCoordinatorUp(_, _ string, _ bool) {}

// SetStandbyUp is a no-op implementation for testing.
func (n *NoopRecorder) SetStandbyUp(_, _ string, _ bool) {}

// SetSegmentsReady is a no-op implementation for testing.
func (n *NoopRecorder) SetSegmentsReady(_, _ string, _ float64) {}

// SetSegmentsTotal is a no-op implementation for testing.
func (n *NoopRecorder) SetSegmentsTotal(_, _ string, _ float64) {}

// SetSegmentsFailed is a no-op implementation for testing.
func (n *NoopRecorder) SetSegmentsFailed(_, _ string, _ float64) {}

// SetMirroringInSync is a no-op implementation for testing.
func (n *NoopRecorder) SetMirroringInSync(_, _ string, _ bool) {}

// RecordFTSProbe is a no-op implementation for testing.
func (n *NoopRecorder) RecordFTSProbe(_, _, _ string, _ time.Duration) {}

// RecordFTSFailover is a no-op implementation for testing.
func (n *NoopRecorder) RecordFTSFailover(_, _ string) {}

// SetSegmentStatus is a no-op implementation for testing.
func (n *NoopRecorder) SetSegmentStatus(_, _, _ string, _ bool) {}

// SetReplicationLag is a no-op implementation for testing.
func (n *NoopRecorder) SetReplicationLag(_, _, _ string, _ float64) {}

// SetStandbyReplicationLag is a no-op implementation for testing.
func (n *NoopRecorder) SetStandbyReplicationLag(_, _ string, _ float64) {}

// RecordConfigReload is a no-op implementation for testing.
func (n *NoopRecorder) RecordConfigReload(_, _ string) {}

// SetConnectionsActive is a no-op implementation for testing.
func (n *NoopRecorder) SetConnectionsActive(_, _ string, _ float64) {}

// SetConnectionsMax is a no-op implementation for testing.
func (n *NoopRecorder) SetConnectionsMax(_, _ string, _ float64) {}

// SetDiskUsageBytes is a no-op implementation for testing.
func (n *NoopRecorder) SetDiskUsageBytes(_, _, _ string, _ float64) {}

// RecordAuthAttempt is a no-op implementation for testing.
func (n *NoopRecorder) RecordAuthAttempt(_, _ string) {}

// SetActiveQueries is a no-op implementation for testing.
func (n *NoopRecorder) SetActiveQueries(_, _ string, _ float64) {}

// SetQueuedQueries is a no-op implementation for testing.
func (n *NoopRecorder) SetQueuedQueries(_, _ string, _ float64) {}

// SetBlockedQueries is a no-op implementation for testing.
func (n *NoopRecorder) SetBlockedQueries(_, _ string, _ float64) {}

// RecordWorkloadRuleAction is a no-op implementation for testing.
func (n *NoopRecorder) RecordWorkloadRuleAction(_, _, _, _ string) {}

// SetResourceGroupUsage is a no-op implementation for testing.
func (n *NoopRecorder) SetResourceGroupUsage(_, _, _ string, _, _ float64) {}

// RecordIdleSessionTermination is a no-op implementation for testing.
func (n *NoopRecorder) RecordIdleSessionTermination(_, _, _ string) {}

// RecordSlowQuery is a no-op implementation for testing.
func (n *NoopRecorder) RecordSlowQuery(_, _ string) {}

// RecordBackup is a no-op implementation for testing.
func (n *NoopRecorder) RecordBackup(_, _, _, _ string) {}

// ObserveBackupDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveBackupDuration(_, _, _ string, _ time.Duration) {}

// SetBackupSizeBytes is a no-op implementation for testing.
func (n *NoopRecorder) SetBackupSizeBytes(_, _, _ string, _ float64) {}

// SetBackupLastSuccessTimestamp is a no-op implementation for testing.
func (n *NoopRecorder) SetBackupLastSuccessTimestamp(_, _ string, _ float64) {}

// SetBackupLastStatus is a no-op implementation for testing.
func (n *NoopRecorder) SetBackupLastStatus(_, _ string, _ float64) {}

// ObserveRestoreDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveRestoreDuration(_, _ string, _ time.Duration) {}

// RecordBackupRetentionDeleted is a no-op implementation for testing.
func (n *NoopRecorder) RecordBackupRetentionDeleted(_, _ string, _ int) {}

// SetBackupJobStatus is a no-op implementation for testing.
func (n *NoopRecorder) SetBackupJobStatus(_, _, _, _ string, _ float64) {}

// RecordRestore is a no-op implementation for testing.
func (n *NoopRecorder) RecordRestore(_, _, _ string) {}

// RecordRestoreValidation is a no-op implementation for testing.
func (n *NoopRecorder) RecordRestoreValidation(_, _, _ string) {}

// SetDataLoadingJobsActive is a no-op implementation for testing.
func (n *NoopRecorder) SetDataLoadingJobsActive(_, _ string, _ float64) {}

// RecordDataLoadingRows is a no-op implementation for testing.
func (n *NoopRecorder) RecordDataLoadingRows(_, _, _, _ string, _ float64) {}

// SetDiskUsagePercent is a no-op implementation for testing.
func (n *NoopRecorder) SetDiskUsagePercent(_, _ string, _ float64) {}

// SetRecommendationsTotal is a no-op implementation for testing.
func (n *NoopRecorder) SetRecommendationsTotal(_, _, _ string, _ float64) {}

// ObserveRecommendationScanDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveRecommendationScanDuration(_, _ string, _ time.Duration) {}

// SetTableBloatRatio is a no-op implementation for testing.
func (n *NoopRecorder) SetTableBloatRatio(_, _, _ string, _ float64) {}

// RecordScaleOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordScaleOperation(_, _, _ string) {}

// SetRedistributionProgress is a no-op implementation for testing.
func (n *NoopRecorder) SetRedistributionProgress(_, _ string, _ float64) {}

// SetDataSkewCoefficient is a no-op implementation for testing.
func (n *NoopRecorder) SetDataSkewCoefficient(_, _ string, _ float64) {}

// SetPVCSizeBytes is a no-op implementation for testing.
func (n *NoopRecorder) SetPVCSizeBytes(_, _, _ string, _ float64) {}

// RecordMirroringOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordMirroringOperation(_, _, _ string) {}

// RecordMaintenanceOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordMaintenanceOperation(_, _, _, _ string) {}

// RecordPasswordRotation is a no-op implementation for testing.
func (n *NoopRecorder) RecordPasswordRotation() {}

// RecordQueryHistoryInsert is a no-op implementation for testing.
func (n *NoopRecorder) RecordQueryHistoryInsert(_, _ string) {}

// ObserveQueryHistorySearchDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveQueryHistorySearchDuration(_, _ string, _ time.Duration) {}

// RecordQueryHistoryExport is a no-op implementation for testing.
func (n *NoopRecorder) RecordQueryHistoryExport(_, _, _ string) {}

// RecordQueryHistoryRetentionCleanup is a no-op implementation for testing.
func (n *NoopRecorder) RecordQueryHistoryRetentionCleanup(_, _ string, _ int64) {}

// SetQueryHistorySizeBytes is a no-op implementation for testing.
func (n *NoopRecorder) SetQueryHistorySizeBytes(_, _ string, _ float64) {}

// RecordPlanCheck is a no-op implementation for testing.
func (n *NoopRecorder) RecordPlanCheck(_, _ string) {}

// RecordPlanCheckIssue is a no-op implementation for testing.
func (n *NoopRecorder) RecordPlanCheckIssue(_, _, _, _ string) {}

// ObservePlanCheckDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObservePlanCheckDuration(_, _ string, _ time.Duration) {}

// RecordQueryCancel is a no-op implementation for testing.
func (n *NoopRecorder) RecordQueryCancel(_, _ string) {}

// RecordQueryMove is a no-op implementation for testing.
func (n *NoopRecorder) RecordQueryMove(_, _ string) {}

// RecordExporterHealthCheck is a no-op implementation for testing.
func (n *NoopRecorder) RecordExporterHealthCheck(_, _ string) {}

// RecordActiveQueryExport is a no-op implementation for testing.
func (n *NoopRecorder) RecordActiveQueryExport() {}

// RecordGuestAccess is a no-op implementation for testing.
func (n *NoopRecorder) RecordGuestAccess(_, _ string, _ bool) {}

// RecordMonitorPause is a no-op implementation for testing.
func (n *NoopRecorder) RecordMonitorPause(_, _ string) {}

// RecordMonitorResume is a no-op implementation for testing.
func (n *NoopRecorder) RecordMonitorResume(_, _ string) {}

// RecordMonitoringDisabledAccess is a no-op implementation for testing.
func (n *NoopRecorder) RecordMonitoringDisabledAccess(_, _ string) {}

// RecordCertRotation is a no-op implementation for testing.
func (n *NoopRecorder) RecordCertRotation(_, _, _ string) {}

// SetCertExpirySeconds is a no-op implementation for testing.
func (n *NoopRecorder) SetCertExpirySeconds(_ string, _ float64) {}

// RecordClusterCertIssuance is a no-op implementation for testing.
func (n *NoopRecorder) RecordClusterCertIssuance(_, _, _ string) {}

// RecordVaultOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordVaultOperation(_, _ string) {}

// ObserveVaultOperationDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveVaultOperationDuration(_ string, _ time.Duration) {}

// RecordWebhookAdmission is a no-op implementation for testing.
func (n *NoopRecorder) RecordWebhookAdmission(_, _, _ string) {}

// RecordUpgradeOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordUpgradeOperation(_, _, _ string) {}

// RecordRollingRestart is a no-op implementation for testing.
func (n *NoopRecorder) RecordRollingRestart(_, _, _ string) {}

// RecordRecoveryOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordRecoveryOperation(_, _, _, _ string) {}

// RecordAPIRequest is a no-op implementation for testing.
func (n *NoopRecorder) RecordAPIRequest(_, _, _ string, _ time.Duration) {}

// AddAPIRequestsInFlight is a no-op implementation for testing.
func (n *NoopRecorder) AddAPIRequestsInFlight(_ float64) {}

// RecordRateLimitRejection is a no-op implementation for testing.
func (n *NoopRecorder) RecordRateLimitRejection(_ string) {}

// RecordDBConnect is a no-op implementation for testing.
func (n *NoopRecorder) RecordDBConnect(_, _, _ string, _ time.Duration) {}

// ObserveDBQueryDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveDBQueryDuration(_ string, _ time.Duration) {}

// RegisterDBPoolStats is a no-op implementation for testing. The returned
// unregister function is itself a no-op (nothing was registered).
func (n *NoopRecorder) RegisterDBPoolStats(_, _ string, _ DBPoolStatsFunc) func() {
	return func() {
		// Nothing to unregister: the no-op recorder never registered the
		// pool stats provider in the first place.
	}
}

// SetIdleDaemonUp is a no-op implementation for testing.
func (n *NoopRecorder) SetIdleDaemonUp(_, _ string, _ bool) {}

// RecordIdleScanFailure is a no-op implementation for testing.
func (n *NoopRecorder) RecordIdleScanFailure(_, _ string) {}

// RecordIdleReconnectAttempt is a no-op implementation for testing.
func (n *NoopRecorder) RecordIdleReconnectAttempt(_, _, _ string) {}

// RecordSessionTermination is a no-op implementation for testing.
func (n *NoopRecorder) RecordSessionTermination(_, _, _ string) {}

// RecordStorageExpansion is a no-op implementation for testing.
func (n *NoopRecorder) RecordStorageExpansion(_, _, _ string) {}

// RecordBackupOnDelete is a no-op implementation for testing.
func (n *NoopRecorder) RecordBackupOnDelete(_, _, _ string) {}

// ObserveScalePhaseDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveScalePhaseDuration(_, _ string, _ time.Duration) {}

// RecordMigrateOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordMigrateOperation(_ string) {}

// RecordAPIClusterOperation is a no-op implementation for testing.
func (n *NoopRecorder) RecordAPIClusterOperation(_, _ string) {}

// RecordLogStreamSession is a no-op implementation for testing.
func (n *NoopRecorder) RecordLogStreamSession(_ string) {}

// AddLogStreamBytes is a no-op implementation for testing.
func (n *NoopRecorder) AddLogStreamBytes(_ float64) {}

// RecordOIDCDiscovery is a no-op implementation for testing.
func (n *NoopRecorder) RecordOIDCDiscovery(_ string) {}

// ObserveAuthTokenVerifyDuration is a no-op implementation for testing.
func (n *NoopRecorder) ObserveAuthTokenVerifyDuration(_ time.Duration) {}
