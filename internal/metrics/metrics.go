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
}

// NewPrometheusRecorder creates a new PrometheusRecorder and registers all metrics.
func NewPrometheusRecorder(reg prometheus.Registerer) *PrometheusRecorder {
	r := &PrometheusRecorder{
		reconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_total",
			Help:      "Total number of reconciliations.",
		}, []string{labelCluster, labelNamespace, labelResult}),
		reconcileErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_errors_total",
			Help:      "Total number of reconciliation errors.",
		}, []string{labelCluster, labelNamespace}),
		reconcileDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_duration_seconds",
			Help:      "Duration of reconciliations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{labelCluster, labelNamespace}),

		clusterInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "cluster_info",
			Help:      "Cluster metadata information.",
		}, []string{labelCluster, labelNamespace, labelVersion, labelPhase}),
		coordinatorUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "coordinator_up",
			Help:      "Coordinator availability (0/1).",
		}, []string{labelCluster, labelNamespace}),
		standbyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "standby_up",
			Help:      "Standby coordinator availability (0/1).",
		}, []string{labelCluster, labelNamespace}),
		segmentsReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_ready",
			Help:      "Number of ready segments.",
		}, []string{labelCluster, labelNamespace}),
		segmentsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_total",
			Help:      "Total number of segments.",
		}, []string{labelCluster, labelNamespace}),
		segmentsFailed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segments_failed",
			Help:      "Number of failed segments.",
		}, []string{labelCluster, labelNamespace}),
		mirroringSync: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "mirroring_in_sync",
			Help:      "Mirroring sync status (0/1).",
		}, []string{labelCluster, labelNamespace}),

		ftsProbeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fts_probe_total",
			Help:      "Total number of FTS probes.",
		}, []string{labelCluster, labelNamespace, labelResult}),
		ftsProbeFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fts_probe_failures_total",
			Help:      "Total number of failed FTS probes.",
		}, []string{labelCluster, labelNamespace}),
		ftsProbeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "fts_probe_duration_seconds",
			Help:      "Duration of FTS probes in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{labelCluster, labelNamespace}),
		ftsFailoverTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fts_failover_total",
			Help:      "Total number of FTS failovers.",
		}, []string{labelCluster, labelNamespace}),
		segmentStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "segment_status",
			Help:      "Per-segment status (1=up, 0=down).",
		}, []string{labelCluster, labelNamespace, labelSegment}),
		replicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "replication_lag_bytes",
			Help:      "Replication lag per segment in bytes.",
		}, []string{labelCluster, labelNamespace, labelSegment}),
		standbyRepLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "standby_replication_lag_bytes",
			Help:      "Standby coordinator replication lag in bytes.",
		}, []string{labelCluster, labelNamespace}),

		configReloadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "config_reload_total",
			Help:      "Total number of configuration reloads.",
		}, []string{labelCluster, labelNamespace}),
		connectionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "connections_active",
			Help:      "Number of active database connections.",
		}, []string{labelCluster, labelNamespace}),
		connectionsMax: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "connections_max",
			Help:      "Maximum allowed database connections.",
		}, []string{labelCluster, labelNamespace}),
		diskUsageBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "disk_usage_bytes",
			Help:      "Disk usage per database in bytes.",
		}, []string{labelCluster, labelNamespace, "database"}),

		authAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "auth_attempts_total",
			Help:      "Total number of authentication attempts.",
		}, []string{"method", labelResult}),
	}

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

// boolToFloat64 converts a boolean to a float64 (1.0 for true, 0.0 for false).
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// NoopRecorder is a no-op implementation of Recorder for testing.
type NoopRecorder struct{}

func (n *NoopRecorder) RecordReconcile(_, _, _ string, _ time.Duration) {}
func (n *NoopRecorder) UpdateClusterInfo(_, _, _, _ string, _ float64)  {}
func (n *NoopRecorder) SetCoordinatorUp(_, _ string, _ bool)            {}
func (n *NoopRecorder) SetStandbyUp(_, _ string, _ bool)                {}
func (n *NoopRecorder) SetSegmentsReady(_, _ string, _ float64)         {}
func (n *NoopRecorder) SetSegmentsTotal(_, _ string, _ float64)         {}
func (n *NoopRecorder) SetSegmentsFailed(_, _ string, _ float64)        {}
func (n *NoopRecorder) SetMirroringInSync(_, _ string, _ bool)          {}
func (n *NoopRecorder) RecordFTSProbe(_, _, _ string, _ time.Duration)  {}
func (n *NoopRecorder) RecordFTSFailover(_, _ string)                   {}
func (n *NoopRecorder) SetSegmentStatus(_, _, _ string, _ bool)         {}
func (n *NoopRecorder) SetReplicationLag(_, _, _ string, _ float64)     {}
func (n *NoopRecorder) SetStandbyReplicationLag(_, _ string, _ float64) {}
func (n *NoopRecorder) RecordConfigReload(_, _ string)                  {}
func (n *NoopRecorder) SetConnectionsActive(_, _ string, _ float64)     {}
func (n *NoopRecorder) SetConnectionsMax(_, _ string, _ float64)        {}
func (n *NoopRecorder) SetDiskUsageBytes(_, _, _ string, _ float64)     {}
func (n *NoopRecorder) RecordAuthAttempt(_, _ string)                   {}
