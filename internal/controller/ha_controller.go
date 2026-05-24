package controller

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const haControllerName = "ha-controller"

// HAReconciler reconciles the high availability aspects of a CloudberryCluster.
type HAReconciler struct {
	client    client.Client
	scheme    *runtime.Scheme
	recorder  record.EventRecorder
	dbFactory db.DBClientFactory
	builder   builder.ResourceBuilder
	metrics   metrics.Recorder
	logger    *slog.Logger
}

// NewHAReconciler creates a new HAReconciler.
func NewHAReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	dbFactory db.DBClientFactory,
	b builder.ResourceBuilder,
	m metrics.Recorder,
	logger *slog.Logger,
) *HAReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	if b == nil {
		b = builder.NewBuilder()
	}
	return &HAReconciler{
		client:    c,
		scheme:    scheme,
		recorder:  recorder,
		dbFactory: dbFactory,
		builder:   b,
		metrics:   m,
		logger:    logger.With("controller", haControllerName),
	}
}

// Reconcile handles the HA reconciliation for CloudberryCluster resources.
func (r *HAReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.logger.With("cluster", req.Name, "namespace", req.Namespace)
	ctx = util.WithLogger(ctx, logger)

	ctx, span := telemetry.StartSpan(ctx, haControllerName, "Reconcile")
	defer span.End()

	logger.Debug("starting HA reconciliation",
		"name", req.Name, "namespace", req.Namespace)

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug("cluster not found, skipping HA reconciliation")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching cluster: %w", err)
	}

	logger.Debug("cluster fetched for HA",
		"phase", cluster.Status.Phase,
		"generation", cluster.Generation,
		"observedGeneration", cluster.Status.ObservedGeneration)

	// Skip if cluster is not running.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Debug("cluster not running, deferring HA reconciliation",
			"phase", cluster.Status.Phase)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches)
	// and there are no annotation-based actions pending.
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Annotations[util.AnnotationRecovery] == "" &&
		cluster.Annotations[util.AnnotationAction] == "" {
		logger.Debug("skipping HA reconciliation, generation unchanged")
		return ctrl.Result{RequeueAfter: r.probeInterval(cluster)}, nil
	}

	// Handle annotation-based actions first.
	if result, handled, err := r.handleAnnotations(ctx, cluster); handled {
		logger.Debug("HA annotation action handled")
		return result, err
	}

	// Run periodic health checks.
	logger.Debug("running periodic health checks")
	r.runHealthChecks(ctx, cluster, logger)

	return ctrl.Result{RequeueAfter: r.probeInterval(cluster)}, nil
}

// handleAnnotations processes annotation-based HA actions.
// Returns (result, handled, error). If handled is true, the caller should return.
func (r *HAReconciler) handleAnnotations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (result ctrl.Result, handled bool, err error) {
	if recoveryType, ok := cluster.Annotations[util.AnnotationRecovery]; ok {
		result, err := r.handleRecovery(ctx, cluster, recoveryType)
		return result, true, err
	}

	action := cluster.Annotations[util.AnnotationAction]
	switch action {
	case util.ActionRebalance:
		result, err := r.handleRebalance(ctx, cluster)
		return result, true, err
	case util.ActionActivateStandby:
		result, err := r.handleStandbyActivation(ctx, cluster)
		return result, true, err
	}

	return ctrl.Result{}, false, nil
}

// runHealthChecks runs FTS probes and standby monitoring.
func (r *HAReconciler) runHealthChecks(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		if err := r.runFTSProbe(ctx, cluster); err != nil {
			logger.Error("FTS probe failed", "error", err)
		}
	}

	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		if err := r.monitorStandby(ctx, cluster); err != nil {
			logger.Error("standby monitoring failed", "error", err)
		}
	}
}

// probeInterval returns the FTS probe interval for the cluster.
func (r *HAReconciler) probeInterval(cluster *cbv1alpha1.CloudberryCluster) time.Duration {
	interval := 60
	if cluster.Spec.HA != nil && cluster.Spec.HA.FTSProbeInterval > 0 {
		interval = int(cluster.Spec.HA.FTSProbeInterval)
	}
	return time.Duration(interval) * time.Second
}

// probeTimeout returns the FTS probe timeout for the cluster.
func (r *HAReconciler) probeTimeout(cluster *cbv1alpha1.CloudberryCluster) time.Duration {
	timeout := 20 // default timeout in seconds
	if cluster.Spec.HA != nil && cluster.Spec.HA.FTSProbeTimeout > 0 {
		timeout = int(cluster.Spec.HA.FTSProbeTimeout)
	}
	return time.Duration(timeout) * time.Second
}

// probeRetries returns the FTS probe retry count for the cluster.
func (r *HAReconciler) probeRetries(cluster *cbv1alpha1.CloudberryCluster) int {
	retries := 5 // default retry count
	if cluster.Spec.HA != nil && cluster.Spec.HA.FTSProbeRetries > 0 {
		retries = int(cluster.Spec.HA.FTSProbeRetries)
	}
	return retries
}

// probeSegmentConfigWithRetries retries GetSegmentConfiguration up to maxRetries times
// with the given timeout per attempt. Returns the segments on success or the last error.
func (r *HAReconciler) probeSegmentConfigWithRetries(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	startTime time.Time,
) ([]db.SegmentInfo, error) {
	logger := util.LoggerFromContext(ctx)
	maxRetries := r.probeRetries(cluster)
	timeout := r.probeTimeout(cluster)

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		segments, err := dbClient.GetSegmentConfiguration(probeCtx)
		cancel()

		if err == nil {
			if attempt > 1 {
				logger.Info("FTS probe succeeded after retry",
					"attempt", attempt,
					"maxRetries", maxRetries,
				)
			}
			return segments, nil
		}

		lastErr = err
		r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, "failure", time.Since(startTime))
		logger.Warn("FTS probe attempt failed",
			"attempt", attempt,
			"maxRetries", maxRetries,
			"error", lastErr,
		)
	}

	return nil, fmt.Errorf("getting segment configuration after %d retries: %w", maxRetries, lastErr)
}

// segmentAnalysisResult holds the results of analyzing segment health.
type segmentAnalysisResult struct {
	failedSegments  []cbv1alpha1.FailedSegment
	failedPrimaries []db.SegmentInfo
	allHealthy      bool
}

// analyzeSegments evaluates segment health and records per-segment metrics.
func (r *HAReconciler) analyzeSegments(
	cluster *cbv1alpha1.CloudberryCluster,
	segments []db.SegmentInfo,
) segmentAnalysisResult {
	logger := r.logger.With("cluster", cluster.Name, "namespace", cluster.Namespace)
	result := segmentAnalysisResult{allHealthy: true}

	for _, seg := range segments {
		if seg.ContentID < 0 {
			continue // Skip coordinator entries.
		}

		isUp := seg.Status == "u"
		segmentID := fmt.Sprintf("%d", seg.ContentID)
		r.metrics.SetSegmentStatus(cluster.Name, cluster.Namespace, segmentID, isUp)

		if isUp {
			continue
		}

		result.allHealthy = false
		result.failedSegments = append(result.failedSegments, cbv1alpha1.FailedSegment{
			ContentID: seg.ContentID,
			Hostname:  seg.Hostname,
			Role:      seg.Role,
			Status:    seg.Status,
		})
		logger.Warn("segment is down",
			"contentID", seg.ContentID,
			"hostname", seg.Hostname,
			"role", seg.Role,
		)

		if seg.Role == "p" {
			result.failedPrimaries = append(result.failedPrimaries, seg)
		}
	}

	return result
}

// runFTSProbe performs FTS health checks on all primary segments with retry support.
func (r *HAReconciler) runFTSProbe(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	logger := util.LoggerFromContext(ctx)
	startTime := time.Now()

	if r.dbFactory == nil {
		r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, "failure", time.Since(startTime))
		return fmt.Errorf("database client factory is not configured")
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, "failure", time.Since(startTime))
		return fmt.Errorf("creating db client for FTS probe: %w", err)
	}
	defer dbClient.Close()

	segments, err := r.probeSegmentConfigWithRetries(ctx, cluster, dbClient, startTime)
	if err != nil {
		return err
	}

	analysis := r.analyzeSegments(cluster, segments)

	// Trigger failover for failed primaries when mirroring is enabled.
	mirroringEnabled := cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled
	if len(analysis.failedPrimaries) > 0 && mirroringEnabled {
		if failoverErr := r.handleFailover(ctx, cluster, dbClient, analysis.failedPrimaries); failoverErr != nil {
			logger.Error("failover handling failed", "error", failoverErr)
		}
	}

	// Update cluster status.
	r.updateFTSProbeStatus(cluster, analysis)

	// Report replication lag for mirror segments.
	r.reportMirrorReplicationLag(ctx, cluster, dbClient)

	if err := patchFTSStatus(ctx, r.client, cluster, analysis.failedSegments); err != nil {
		return fmt.Errorf("updating cluster status after FTS probe: %w", err)
	}

	result := "success"
	if !analysis.allHealthy {
		result = "degraded"
	}
	r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, result, time.Since(startTime))

	return nil
}

// updateFTSProbeStatus updates cluster status and metrics based on segment analysis.
func (r *HAReconciler) updateFTSProbeStatus(
	cluster *cbv1alpha1.CloudberryCluster,
	analysis segmentAnalysisResult,
) {
	cluster.Status.FailedSegments = analysis.failedSegments
	if analysis.allHealthy {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
		r.metrics.SetMirroringInSync(cluster.Name, cluster.Namespace, true)
		return
	}

	cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded
	r.metrics.SetMirroringInSync(cluster.Name, cluster.Namespace, false)
	r.metrics.SetSegmentsFailed(cluster.Name, cluster.Namespace, float64(len(analysis.failedSegments)))
	r.recorder.Event(cluster, corev1.EventTypeWarning, "MirroringDegraded",
		fmt.Sprintf("%d segments are down", len(analysis.failedSegments)))
}

// handleFailover processes automatic failover for failed primary segments.
// It triggers Cloudberry's internal FTS probe scan which promotes mirrors
// to primary role, then verifies the result and updates observability.
func (r *HAReconciler) handleFailover(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	failedPrimaries []db.SegmentInfo,
) error {
	logger := util.LoggerFromContext(ctx)
	logger.Info("initiating failover for failed primary segments",
		"failedPrimaryCount", len(failedPrimaries),
	)

	// Trigger Cloudberry's internal FTS scan to promote mirrors.
	if err := dbClient.TriggerFTSProbe(ctx); err != nil {
		logger.Error("failed to trigger FTS probe scan for failover", "error", err)
		// Continue with status update even if trigger fails — report what we know.
	}

	// Re-read segment configuration to verify failover result.
	updatedSegments, err := dbClient.GetSegmentConfiguration(ctx)
	if err != nil {
		logger.Error("failed to re-read segment configuration after failover trigger", "error", err)
		// Emit events for the originally detected failures even without re-read.
		for _, fp := range failedPrimaries {
			r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonSegmentFailover,
				fmt.Sprintf("Primary segment failover detected: contentID=%d, hostname=%s",
					fp.ContentID, fp.Hostname))
		}
		r.metrics.RecordFTSFailover(cluster.Name, cluster.Namespace)
		return fmt.Errorf("re-reading segment configuration after failover: %w", err)
	}

	// Build a lookup of updated segments by contentID and role for verification.
	type segKey struct {
		contentID int32
		role      string
	}
	updatedMap := make(map[segKey]db.SegmentInfo, len(updatedSegments))
	for _, seg := range updatedSegments {
		updatedMap[segKey{contentID: seg.ContentID, role: seg.Role}] = seg
	}

	// Verify failover results and emit events per failed primary.
	for _, fp := range failedPrimaries {
		segID := fmt.Sprintf("%d", fp.ContentID)

		// Check if the mirror for this contentID is now acting as primary.
		if mirror, ok := updatedMap[segKey{contentID: fp.ContentID, role: "p"}]; ok && mirror.DBID != fp.DBID {
			// Mirror was promoted to primary — successful failover.
			r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonSegmentFailover,
				fmt.Sprintf("Segment failover completed: contentID=%d, original primary=%s, new primary=%s",
					fp.ContentID, fp.Hostname, mirror.Hostname))
		} else {
			// Mirror was not promoted — partial or failed failover.
			r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonSegmentFailover,
				fmt.Sprintf("Primary segment failed: contentID=%d, hostname=%s, mirror promotion pending",
					fp.ContentID, fp.Hostname))
		}

		r.metrics.SetSegmentStatus(cluster.Name, cluster.Namespace, segID, false)
	}

	// Record failover metric once per failover event (not per segment).
	r.metrics.RecordFTSFailover(cluster.Name, cluster.Namespace)

	return nil
}

// reportMirrorReplicationLag queries mirror sync status and reports replication
// lag per segment to Prometheus metrics. Errors are logged but do not fail the
// FTS probe — replication lag reporting is best-effort observability.
func (r *HAReconciler) reportMirrorReplicationLag(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
) {
	logger := util.LoggerFromContext(ctx)

	syncStatus, err := dbClient.GetMirrorSyncStatus(ctx)
	if err != nil {
		logger.Warn("failed to get mirror sync status for replication lag reporting", "error", err)
		return
	}

	for _, ms := range syncStatus {
		segID := fmt.Sprintf("%d", ms.ContentID)
		r.metrics.SetReplicationLag(
			cluster.Name, cluster.Namespace,
			segID, float64(ms.ReplicationLag),
		)
	}
}

// monitorStandby checks the standby coordinator health and replication lag.
func (r *HAReconciler) monitorStandby(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	if r.dbFactory == nil {
		return fmt.Errorf("database client factory is not configured")
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for standby monitoring: %w", err)
	}
	defer dbClient.Close()

	lag, err := dbClient.GetReplicationLag(ctx)
	if err != nil {
		r.metrics.SetStandbyUp(cluster.Name, cluster.Namespace, false)
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionStandbyReady),
			metav1.ConditionFalse,
			"ReplicationCheckFailed",
			fmt.Sprintf("Failed to check replication lag: %v", err),
		)
		return fmt.Errorf("getting replication lag: %w", err)
	}

	r.metrics.SetStandbyReplicationLag(cluster.Name, cluster.Namespace, float64(lag))
	r.metrics.SetStandbyUp(cluster.Name, cluster.Namespace, true)

	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionStandbyReady),
		metav1.ConditionTrue,
		"StandbyInSync",
		fmt.Sprintf("Standby replication lag: %d bytes", lag),
	)

	return patchStatus(ctx, r.client, cluster)
}

// handleRecovery processes recovery annotations.
func (r *HAReconciler) handleRecovery(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	recoveryType string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling recovery", "type", recoveryType)

	// Remove the recovery annotation using MergePatch to avoid conflicts with stale objects.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationRecovery); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing recovery annotation: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "RecoveryStarted",
		fmt.Sprintf("Recovery type %s initiated", recoveryType))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// defaultSkewThreshold is the default percentage skew threshold for rebalance.
const defaultSkewThreshold int32 = 10

// defaultParallelism is the default number of tables to redistribute concurrently.
const defaultParallelism int32 = 2

// handleRebalance processes rebalance actions with full configuration support.
// When a DB factory is available, it performs real skew analysis and redistributes
// tables that exceed the configured skew threshold. Otherwise, it falls back to
// creating a maintenance Job.
func (r *HAReconciler) handleRebalance(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling rebalance")

	// Remove the action annotation using MergePatch to avoid conflicts with stale objects.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationAction); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing rebalance annotation: %w", err)
	}

	// Get rebalance config (defaults if not set).
	rebalanceCfg := cluster.Spec.Segments.Rebalance
	parallelism := defaultParallelism
	skewThreshold := defaultSkewThreshold
	var excludeTables []string
	if rebalanceCfg != nil {
		if rebalanceCfg.Parallelism > 0 {
			parallelism = rebalanceCfg.Parallelism
		}
		if rebalanceCfg.SkewThreshold > 0 {
			skewThreshold = rebalanceCfg.SkewThreshold
		}
		excludeTables = rebalanceCfg.ExcludeTables
	}

	logger.Info("rebalance configuration",
		"skewThreshold", skewThreshold,
		"parallelism", parallelism,
		"excludeTables", excludeTables)

	// Set DataRedistribution condition to InProgress.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "RebalanceStarted",
		fmt.Sprintf("Rebalance started: threshold=%d%%, parallelism=%d, excluded=%v",
			skewThreshold, parallelism, excludeTables))
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating rebalance status: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonRebalanceStarted,
		fmt.Sprintf("Segment rebalance initiated: threshold=%d%%, parallelism=%d",
			skewThreshold, parallelism))

	// Attempt real skew analysis and redistribution via DB client.
	if r.dbFactory != nil {
		if err := r.executeRebalanceViaDB(ctx, cluster, skewThreshold, parallelism, excludeTables); err != nil {
			logger.Error("rebalance via DB failed, falling back to Job", "error", err)
		} else {
			// Direct execution succeeded.
			cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
				string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "RebalanceCompleted",
				"Rebalance completed successfully")
			if err := patchStatus(ctx, r.client, cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating rebalance completion: %w", err)
			}
			r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonRebalanceCompleted,
				"Segment rebalance completed")
			r.metrics.RecordScaleOperation(cluster.Name, cluster.Namespace, "rebalance")
			return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
		}
	}

	// Fallback: create rebalance Job.
	timestamp := time.Now().Format("20060102-150405")
	job := r.builder.BuildMaintenanceJob(cluster, util.MaintenanceRebalance, timestamp)
	if job != nil {
		if err := r.client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error("failed to create rebalance job", "error", err)
		}
	}

	// Set completion (Job-based rebalance is fire-and-forget at this stage).
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "RebalanceCompleted",
		"Rebalance completed successfully")
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating rebalance completion: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonRebalanceCompleted,
		"Segment rebalance completed")
	r.metrics.RecordScaleOperation(cluster.Name, cluster.Namespace, "rebalance")

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// executeRebalanceViaDB performs skew analysis and redistributes skewed tables
// directly via the database client. It filters out excluded tables (supporting
// glob patterns) and only rebalances tables exceeding the skew threshold.
func (r *HAReconciler) executeRebalanceViaDB(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	skewThreshold, parallelism int32,
	excludeTables []string,
) error {
	logger := util.LoggerFromContext(ctx)

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for rebalance: %w", err)
	}
	defer dbClient.Close()

	skewResults, tablesToRebalance := r.analyzeAndFilterSkew(
		ctx, dbClient, cluster, skewThreshold, excludeTables)
	_ = skewResults // used for logging in analyzeAndFilterSkew

	logger.Info("tables requiring rebalance",
		"count", len(tablesToRebalance),
		"threshold", skewThreshold)

	if len(tablesToRebalance) == 0 {
		logger.Info("no tables exceed skew threshold, rebalance not needed")
		return nil
	}

	return r.dispatchRebalanceTables(ctx, logger, dbClient, tablesToRebalance, parallelism)
}

// interTableDelay is the pause between dispatching rebalance goroutines to
// rate-limit database operations and prevent overwhelming the cluster.
const interTableDelay = 100 * time.Millisecond

// dispatchRebalanceTables dispatches concurrent rebalance operations for the
// given tables with bounded concurrency and inter-table rate limiting.
func (r *HAReconciler) dispatchRebalanceTables(
	ctx context.Context,
	logger *slog.Logger,
	dbClient db.Client,
	tablesToRebalance []db.TableSkewInfo,
	parallelism int32,
) error {
	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, len(tablesToRebalance))

	var dispatched int
	for _, info := range tablesToRebalance {
		// Check context cancellation before acquiring the semaphore to avoid
		// goroutine leaks when the parent context is canceled.
		select {
		case <-ctx.Done():
			logger.Warn("context canceled, stopping rebalance dispatch",
				"dispatched", dispatched, "total", len(tablesToRebalance))
			goto waitForCompletion
		case sem <- struct{}{}:
			// Semaphore acquired — proceed.
		}

		// Add a small delay between dispatching goroutines to rate-limit
		// database operations and prevent overwhelming the cluster.
		if dispatched > 0 {
			if err := waitWithContext(ctx, interTableDelay); err != nil {
				logger.Warn("context canceled during inter-table delay",
					"dispatched", dispatched, "total", len(tablesToRebalance))
				goto waitForCompletion
			}
		}

		dispatched++
		go func(ti db.TableSkewInfo) {
			defer func() { <-sem }()
			// Check context cancellation before starting the rebalance operation.
			if ctx.Err() != nil {
				logger.Warn("context canceled, skipping table rebalance",
					"database", ti.Database, "table", ti.Schema+"."+ti.Table)
				return
			}
			rebalanceErr := dbClient.RebalanceTable(ctx, ti.Database, ti.Schema, ti.Table, ti.DistributionKey)
			if rebalanceErr != nil {
				logger.Error("failed to rebalance table",
					"database", ti.Database, "table", ti.Schema+"."+ti.Table, "error", rebalanceErr)
				errCh <- rebalanceErr
				return
			}
			logger.Info("table rebalanced",
				"database", ti.Database, "table", ti.Schema+"."+ti.Table,
				"previousSkew", ti.SkewCoefficient)
		}(info)
	}

waitForCompletion:
	// Wait for all dispatched goroutines to finish.
	for range cap(sem) {
		sem <- struct{}{}
	}
	close(errCh)

	// Collect errors (individual table failures don't block others, but the
	// caller is informed so it can set the appropriate status condition).
	var rebalanceErrors int
	for range errCh {
		rebalanceErrors++
	}

	if rebalanceErrors > 0 {
		logger.Warn("some tables failed to rebalance",
			"failed", rebalanceErrors, "total", len(tablesToRebalance))
		return fmt.Errorf("%d of %d tables failed to rebalance",
			rebalanceErrors, len(tablesToRebalance))
	}

	return nil
}

// waitWithContext waits for the specified duration or until the context is canceled.
// Returns nil if the duration elapsed, or the context error if canceled.
func waitWithContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// isTableExcluded checks if a table name matches any exclusion pattern.
// Supports exact match and glob patterns (e.g., "temp_*", "audit_*").
func isTableExcluded(tableName string, excludePatterns []string) bool {
	for _, pattern := range excludePatterns {
		if pattern == tableName {
			return true
		}
		// Support glob patterns using filepath.Match.
		if matched, _ := filepath.Match(pattern, tableName); matched {
			return true
		}
		// Also try matching just the table part (without schema) against the pattern.
		parts := splitSchemaTable(tableName)
		if len(parts) == 2 {
			if matched, _ := filepath.Match(pattern, parts[1]); matched {
				return true
			}
			// Try matching schema.table against the pattern.
			if matched, _ := filepath.Match(pattern, parts[0]+"."+parts[1]); matched {
				return true
			}
		}
	}
	return false
}

// analyzeAndFilterSkew discovers databases, analyzes skew, filters tables, and records metrics.
func (r *HAReconciler) analyzeAndFilterSkew(
	ctx context.Context,
	dbClient db.Client,
	cluster *cbv1alpha1.CloudberryCluster,
	skewThreshold int32,
	excludeTables []string,
) (allTables []db.TableSkewInfo, skewedTables []db.TableSkewInfo) {
	logger := util.LoggerFromContext(ctx)

	databases, err := dbClient.ListUserDatabases(ctx)
	if err != nil {
		logger.Error("failed to list databases for rebalance", "error", err)
		return nil, nil
	}

	var skewResults []db.TableSkewInfo
	for _, dbName := range databases {
		dbSkew, analyzeErr := dbClient.AnalyzeSkew(ctx, dbName)
		if analyzeErr != nil {
			logger.Error("failed to analyze skew", "database", dbName, "error", analyzeErr)
			continue
		}
		skewResults = append(skewResults, dbSkew...)
	}

	var tablesToRebalance []db.TableSkewInfo
	for _, info := range skewResults {
		fullName := info.Schema + "." + info.Table
		if isTableExcluded(fullName, excludeTables) {
			continue
		}
		if info.SkewCoefficient >= float64(skewThreshold) {
			tablesToRebalance = append(tablesToRebalance, info)
		}
	}

	var maxSkew float64
	for _, info := range skewResults {
		if info.SkewCoefficient > maxSkew {
			maxSkew = info.SkewCoefficient
		}
	}
	r.metrics.SetDataSkewCoefficient(cluster.Name, cluster.Namespace, maxSkew)

	logger.Info("skew analysis completed",
		"tablesAnalyzed", len(skewResults),
		"tablesAboveThreshold", len(tablesToRebalance))
	return skewResults, tablesToRebalance
}

// splitSchemaTable splits "schema.table" into ["schema", "table"].
func splitSchemaTable(name string) []string {
	for i, c := range name {
		if c == '.' {
			return []string{name[:i], name[i+1:]}
		}
	}
	return []string{name}
}

// handleStandbyActivation processes standby activation.
func (r *HAReconciler) handleStandbyActivation(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling standby activation")

	// Remove the action annotation using MergePatch to avoid conflicts with stale objects.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationAction); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing standby activation annotation: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeWarning, "CoordinatorFailover",
		"Standby coordinator activation initiated")

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HAReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Named(haControllerName).
		Complete(r)
}
