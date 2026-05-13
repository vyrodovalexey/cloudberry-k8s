package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
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
	dbFactory DBClientFactory
	metrics   metrics.Recorder
	logger    *slog.Logger
}

// DBClientFactory creates database clients for clusters.
type DBClientFactory interface {
	// NewClient creates a new database client for the given cluster.
	NewClient(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) (db.Client, error)
}

// NewHAReconciler creates a new HAReconciler.
func NewHAReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	dbFactory DBClientFactory,
	m metrics.Recorder,
	logger *slog.Logger,
) *HAReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HAReconciler{
		client:    c,
		scheme:    scheme,
		recorder:  recorder,
		dbFactory: dbFactory,
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

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching cluster: %w", err)
	}

	// Skip if cluster is not running.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches)
	// and there are no annotation-based actions pending.
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Annotations[util.AnnotationRecovery] == "" &&
		cluster.Annotations[util.AnnotationAction] == "" {
		logger.Info("skipping HA reconciliation, generation unchanged")
		return ctrl.Result{RequeueAfter: r.probeInterval(cluster)}, nil
	}

	// Handle annotation-based actions first.
	if result, handled, err := r.handleAnnotations(ctx, cluster); handled {
		return result, err
	}

	// Run periodic health checks.
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

// runFTSProbe performs FTS health checks on all primary segments.
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

	segments, err := dbClient.GetSegmentConfiguration(ctx)
	if err != nil {
		r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, "failure", time.Since(startTime))
		return fmt.Errorf("getting segment configuration: %w", err)
	}

	var failedSegments []cbv1alpha1.FailedSegment
	allHealthy := true

	for _, seg := range segments {
		if seg.ContentID < 0 {
			continue // Skip coordinator entries.
		}

		isUp := seg.Status == "u"
		segmentID := fmt.Sprintf("%d", seg.ContentID)
		r.metrics.SetSegmentStatus(cluster.Name, cluster.Namespace, segmentID, isUp)

		if !isUp {
			allHealthy = false
			failedSegments = append(failedSegments, cbv1alpha1.FailedSegment{
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
		}
	}

	// Update cluster status.
	cluster.Status.FailedSegments = failedSegments
	if allHealthy {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
		r.metrics.SetMirroringInSync(cluster.Name, cluster.Namespace, true)
	} else {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded
		r.metrics.SetMirroringInSync(cluster.Name, cluster.Namespace, false)
		r.metrics.SetSegmentsFailed(cluster.Name, cluster.Namespace, float64(len(failedSegments)))
		r.recorder.Event(cluster, corev1.EventTypeWarning, "MirroringDegraded",
			fmt.Sprintf("%d segments are down", len(failedSegments)))
	}

	if err := patchFTSStatus(ctx, r.client, cluster, failedSegments); err != nil {
		return fmt.Errorf("updating cluster status after FTS probe: %w", err)
	}

	result := "success"
	if !allHealthy {
		result = "degraded"
	}
	r.metrics.RecordFTSProbe(cluster.Name, cluster.Namespace, result, time.Since(startTime))

	return nil
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

// handleRebalance processes rebalance actions.
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

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Rebalanced", "Segment rebalance initiated")

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
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
