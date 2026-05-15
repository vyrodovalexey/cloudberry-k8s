// Package controller implements the Kubernetes controllers for the cloudberry operator.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	clusterControllerName = "cluster-controller"
	requeueAfterDefault   = 30 * time.Second
	requeueAfterError     = 10 * time.Second
	requeueAfterStopping  = 5 * time.Second
	scaleTimeout          = 10 * time.Minute
	upgradePhaseTimeout   = 10 * time.Minute
)

// ClusterReconciler reconciles a CloudberryCluster object.
type ClusterReconciler struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
	builder  builder.ResourceBuilder
	metrics  metrics.Recorder
	logger   *slog.Logger
}

// NewClusterReconciler creates a new ClusterReconciler.
func NewClusterReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	m metrics.Recorder,
	logger *slog.Logger,
) *ClusterReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &ClusterReconciler{
		client:   c,
		scheme:   scheme,
		recorder: recorder,
		builder:  b,
		metrics:  m,
		logger:   logger.With("controller", clusterControllerName),
	}
}

// Reconcile handles the reconciliation loop for CloudberryCluster resources.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()
	logger := r.logger.With("cluster", req.Name, "namespace", req.Namespace)
	ctx = util.WithLogger(ctx, logger)

	ctx, span := telemetry.StartSpan(ctx, clusterControllerName, "Reconcile")
	defer span.End()

	logger.Info("starting reconciliation")

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("cluster resource not found, likely deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching cluster: %w", err)
	}

	// Handle annotation-based actions FIRST — annotations don't change the
	// generation, so they must be checked before the generation skip.
	if action, ok := cluster.Annotations[util.AnnotationAction]; ok {
		result, err := r.handleAction(ctx, cluster, action)
		if err != nil {
			r.recordReconcileResult(cluster, startTime, "error")
			return result, err
		}
		return result, nil
	}

	// Check if the cluster is in a lifecycle phase that should short-circuit reconciliation.
	if result, handled := r.handleLifecyclePhase(ctx, cluster); handled {
		return result, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches).
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Status.Phase == cbv1alpha1.ClusterPhaseRunning {
		logger.Info("skipping reconciliation, generation unchanged and cluster running")
		r.recordMetricsSnapshot(cluster)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Handle deletion.
	if !cluster.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, cluster)
	}

	// Ensure finalizer is set.
	if !controllerutil.ContainsFinalizer(cluster, util.FinalizerName) {
		controllerutil.AddFinalizer(cluster, util.FinalizerName)
		if err := r.client.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Main reconciliation flow.
	result, err := r.reconcileCluster(ctx, cluster)
	if err != nil {
		r.recordReconcileResult(cluster, startTime, "error")
		telemetry.SetSpanError(span, err)
		return result, err
	}

	r.recordReconcileResult(cluster, startTime, "success")
	logger.Info("reconciliation completed", "duration", time.Since(startTime))
	return result, nil
}

// handleLifecyclePhase checks whether the cluster is in a lifecycle phase
// (Stopped, Stopping, Restricted, Maintenance) that should short-circuit
// normal reconciliation when no action annotation is pending.
// Returns (result, true) if the phase was handled, or (_, false) to continue.
func (r *ClusterReconciler) handleLifecyclePhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, bool) {
	_, hasAction := cluster.Annotations[util.AnnotationAction]
	if hasAction {
		// An action is pending — let the main reconciliation handle it.
		return ctrl.Result{}, false
	}

	logger := util.LoggerFromContext(ctx)

	switch cluster.Status.Phase {
	case cbv1alpha1.ClusterPhaseStopped:
		logger.Info("cluster is stopped, skipping reconciliation")
		r.recordMetricsSnapshot(cluster)
		return ctrl.Result{}, true

	case cbv1alpha1.ClusterPhaseStopping:
		result, _ := r.checkStopProgress(ctx, cluster)
		return result, true

	case cbv1alpha1.ClusterPhaseRestricted, cbv1alpha1.ClusterPhaseMaintenance:
		logger.Info("cluster in limited mode, skipping full reconciliation",
			"phase", cluster.Status.Phase)
		r.recordMetricsSnapshot(cluster)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, true

	case cbv1alpha1.ClusterPhaseScaling:
		result, _ := r.checkScaleProgress(ctx, cluster)
		return result, true

	case cbv1alpha1.ClusterPhaseUpdating:
		result, _ := r.continueUpgrade(ctx, cluster)
		return result, true

	default:
		return ctrl.Result{}, false
	}
}

// reconcileCluster performs the main reconciliation logic.
func (r *ClusterReconciler) reconcileCluster(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Set initial status if not set.
	if cluster.Status.Phase == "" {
		return r.updatePhase(ctx, cluster, cbv1alpha1.ClusterPhasePending)
	}

	// Reconcile admin password Secret (must exist before StatefulSets reference it).
	if err := r.reconcileAdminSecret(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling admin secret: %w", err)
	}

	// Reconcile ConfigMaps.
	if err := r.reconcileConfigMaps(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling configmaps: %w", err)
	}

	// Reconcile Services.
	if err := r.reconcileServices(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling services: %w", err)
	}

	// Check for in-progress or needed upgrade before reconciling StatefulSets.
	if r.isUpgradeNeeded(cluster) {
		return r.handleUpgrade(ctx, cluster)
	}

	// Reconcile coordinator StatefulSet.
	if err := r.reconcileCoordinator(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling coordinator: %w", err)
	}

	// Reconcile standby if enabled.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		if err := r.reconcileStandby(ctx, cluster); err != nil {
			return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling standby: %w", err)
		}
	}

	// Reconcile segments.
	if err := r.reconcileSegments(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling segments: %w", err)
	}

	// Reconcile storage expansion (PVC resizing).
	if err := r.reconcileStorageExpansion(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("reconciling storage expansion: %w", err)
	}

	// Update status.
	if err := r.updateStatus(ctx, cluster); err != nil {
		logger.Error("failed to update status", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// reconcileConfigMaps ensures ConfigMaps are in the desired state.
func (r *ClusterReconciler) reconcileConfigMaps(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// PostgreSQL configuration.
	desiredPgConf := r.builder.BuildPostgresqlConfConfigMap(cluster)
	if err := r.createOrUpdateConfigMap(ctx, desiredPgConf); err != nil {
		return fmt.Errorf("postgresql.conf configmap: %w", err)
	}

	// pg_hba.conf.
	desiredHBA := r.builder.BuildPgHBAConfConfigMap(cluster)
	if err := r.createOrUpdateConfigMap(ctx, desiredHBA); err != nil {
		return fmt.Errorf("pg_hba.conf configmap: %w", err)
	}

	return nil
}

// reconcileAdminSecret ensures the admin password Secret exists.
// If the Secret doesn't exist, it generates a random password and creates it.
// If the Secret already exists (e.g. user-provided), it is left unchanged.
func (r *ClusterReconciler) reconcileAdminSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	secretName := util.AdminPasswordSecretName(cluster.Name)
	existing := &corev1.Secret{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, existing)

	if err == nil {
		// Secret already exists, nothing to do.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting admin password secret %s: %w", secretName, err)
	}

	// Generate a random password.
	password, err := util.GenerateRandomPassword()
	if err != nil {
		return fmt.Errorf("generating admin password: %w", err)
	}

	desired := r.builder.BuildAdminPasswordSecret(cluster, password)
	if createErr := r.client.Create(ctx, desired); createErr != nil {
		return fmt.Errorf("creating admin password secret %s: %w", secretName, createErr)
	}

	util.LoggerFromContext(ctx).Info("created admin password secret", "name", secretName)
	r.recorder.Event(cluster, corev1.EventTypeNormal, "SecretCreated",
		fmt.Sprintf("Admin password secret %s created", secretName))

	return nil
}

// reconcileServices ensures Services are in the desired state.
func (r *ClusterReconciler) reconcileServices(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	services := []*corev1.Service{
		r.builder.BuildCoordinatorService(cluster),
		r.builder.BuildSegmentService(cluster),
		r.builder.BuildClientService(cluster),
	}

	// Only create standby service when standby is enabled.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		services = append(services, r.builder.BuildStandbyService(cluster))
	}

	for _, svc := range services {
		if err := r.createOrUpdateService(ctx, svc); err != nil {
			return fmt.Errorf("service %s: %w", svc.Name, err)
		}
	}

	return nil
}

// reconcileCoordinator ensures the coordinator StatefulSet is in the desired state.
func (r *ClusterReconciler) reconcileCoordinator(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired, err := r.builder.BuildCoordinatorStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building coordinator StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	return r.createOrUpdateStatefulSet(ctx, desired)
}

// reconcileStandby ensures the standby StatefulSet is in the desired state.
func (r *ClusterReconciler) reconcileStandby(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired, err := r.builder.BuildStandbyStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building standby StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	if desired == nil {
		return nil
	}
	return r.createOrUpdateStatefulSet(ctx, desired)
}

// reconcileSegments ensures segment StatefulSets are in the desired state.
// It detects scale-out operations by comparing the desired segment count
// against the current StatefulSet replicas and delegates to handleScaleOut
// when a scale-out is needed.
func (r *ClusterReconciler) reconcileSegments(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	// Check for scale-out by comparing desired vs actual replicas.
	existingSts := &appsv1.StatefulSet{}
	getErr := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, existingSts)

	if getErr == nil && existingSts.Spec.Replicas != nil {
		currentCount := *existingSts.Spec.Replicas
		desiredCount := cluster.Spec.Segments.Count

		// Only detect scale operations when the current count is > 0.
		// A currentCount of 0 indicates a restart or initial creation,
		// which should be handled by the normal reconciliation path.
		if currentCount > 0 && desiredCount > currentCount {
			logger.Info("scale-out detected in reconcileSegments",
				"from", currentCount, "to", desiredCount)
			return r.handleScaleOut(ctx, cluster, currentCount, desiredCount)
		}
		if currentCount > 0 && desiredCount < currentCount {
			logger.Info("scale-in detected in reconcileSegments",
				"from", currentCount, "to", desiredCount)
			return r.handleScaleIn(ctx, cluster, currentCount, desiredCount)
		}
	}

	// Normal reconciliation — primary segments.
	primarySts, err := r.builder.BuildSegmentPrimaryStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building primary segment StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	if err := r.createOrUpdateStatefulSet(ctx, primarySts); err != nil {
		return fmt.Errorf("primary segments: %w", err)
	}

	// Mirror segments.
	mirrorSts, err := r.builder.BuildSegmentMirrorStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building mirror segment StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	if mirrorSts != nil {
		if err := r.createOrUpdateStatefulSet(ctx, mirrorSts); err != nil {
			return fmt.Errorf("mirror segments: %w", err)
		}
	}

	return nil
}

// handleScaleOut orchestrates a scale-out operation: transitions the cluster
// to the Scaling phase, updates StatefulSet replicas, creates a redistribution
// Job, and records the appropriate events and conditions.
func (r *ClusterReconciler) handleScaleOut(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	oldCount, newCount int32,
) error {
	logger := util.LoggerFromContext(ctx)

	// Pre-flight: cluster must be in Running phase.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Warn("scale-out blocked: cluster not in Running phase",
			"currentPhase", cluster.Status.Phase)
		r.recorder.Event(cluster, corev1.EventTypeWarning, "ScaleOutBlocked",
			fmt.Sprintf("Scale-out blocked: cluster is in %s phase, must be Running", cluster.Status.Phase))
		return nil // Don't error, just skip — will retry on next reconcile.
	}

	logger.Info("scale-out detected", "from", oldCount, "to", newCount)

	// Track scale start time.
	scaleStartTime := time.Now().Format(time.RFC3339)
	if err := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationScaleStarted, scaleStartTime); err != nil {
		return fmt.Errorf("setting scale-started annotation: %w", err)
	}

	// Set phase to Scaling.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionFalse, "ScaleOutStarted",
		fmt.Sprintf("Scaling from %d to %d segments", oldCount, newCount))
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("setting scaling phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonScaleOutStarted,
		fmt.Sprintf("Scale-out from %d to %d segments initiated", oldCount, newCount))

	// Update primary StatefulSet replicas.
	primarySts, buildErr := r.builder.BuildSegmentPrimaryStatefulSet(cluster)
	if buildErr != nil {
		return fmt.Errorf("building primary segment StatefulSet for cluster %s: %w", cluster.Name, buildErr)
	}
	if err := r.createOrUpdateStatefulSet(ctx, primarySts); err != nil {
		return fmt.Errorf("scaling primary segments: %w", err)
	}

	// Update mirror StatefulSet replicas if mirroring is enabled.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		mirrorSts, mirrorErr := r.builder.BuildSegmentMirrorStatefulSet(cluster)
		if mirrorErr != nil {
			return fmt.Errorf("building mirror segment StatefulSet for cluster %s: %w", cluster.Name, mirrorErr)
		}
		if mirrorSts != nil {
			if err := r.createOrUpdateStatefulSet(ctx, mirrorSts); err != nil {
				return fmt.Errorf("scaling mirror segments: %w", err)
			}
		}
	}

	// Create redistribution Job.
	timestamp := time.Now().Format("20060102-150405")
	redistJob := r.builder.BuildMaintenanceJob(cluster, util.MaintenanceRedistribute, timestamp)
	if redistJob != nil {
		if err := r.client.Create(ctx, redistJob); err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error("failed to create redistribution job", "error", err)
		}
	}

	// Set redistribution in progress.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "InProgress",
		"Data redistribution in progress")
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating redistribution status: %w", err)
	}

	return nil
}

// handleScaleIn orchestrates a scale-in operation: validates the reduction,
// transitions the cluster to the Scaling phase, scales down StatefulSets
// (mirrors first, then primaries), creates a redistribution Job, and records
// the appropriate events and conditions.
func (r *ClusterReconciler) handleScaleIn(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	oldCount, newCount int32,
) error {
	logger := util.LoggerFromContext(ctx)

	// Pre-flight: cluster must be in Running phase.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Warn("scale-in blocked: cluster not in Running phase",
			"currentPhase", cluster.Status.Phase)
		r.recorder.Event(cluster, corev1.EventTypeWarning, "ScaleInBlocked",
			fmt.Sprintf("Scale-in blocked: cluster is in %s phase, must be Running", cluster.Status.Phase))
		return nil // Don't error, just skip — will retry on next reconcile.
	}

	logger.Info("scale-in detected", "from", oldCount, "to", newCount)

	// Safety check: scale-in by more than 50% requires confirmation annotation.
	if float64(newCount) < float64(oldCount)*0.5 {
		if cluster.Annotations[util.AnnotationConfirmScaleIn] != "true" {
			logger.Warn("scale-in by more than 50% requires confirmation annotation",
				"from", oldCount, "to", newCount)
			r.recorder.Event(cluster, corev1.EventTypeWarning, "ScaleInBlocked",
				fmt.Sprintf("Scale-in from %d to %d requires annotation %s=true",
					oldCount, newCount, util.AnnotationConfirmScaleIn))
			return nil
		}
	}

	// Track scale start time.
	scaleStartTime := time.Now().Format(time.RFC3339)
	if err := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationScaleStarted, scaleStartTime); err != nil {
		return fmt.Errorf("setting scale-started annotation: %w", err)
	}

	// Set phase to Scaling.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionFalse, "ScaleInStarted",
		fmt.Sprintf("Scaling from %d to %d segments", oldCount, newCount))
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("setting scaling phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonScaleInStarted,
		fmt.Sprintf("Scale-in from %d to %d segments initiated", oldCount, newCount))

	// Create redistribution Job (move data OFF segments being removed).
	timestamp := time.Now().Format("20060102-150405")
	redistJob := r.builder.BuildMaintenanceJob(cluster, util.MaintenanceRedistribute, timestamp)
	if redistJob != nil {
		if err := r.client.Create(ctx, redistJob); err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error("failed to create redistribution job", "error", err)
		}
	}

	// Scale down mirror StatefulSet FIRST (if mirroring enabled).
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		mirrorName := util.SegmentMirrorName(cluster.Name)
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, mirrorName, newCount); err != nil {
			return fmt.Errorf("scaling down mirror segments: %w", err)
		}
	}

	// Scale down primary StatefulSet.
	primaryName := util.SegmentPrimaryName(cluster.Name)
	if err := r.scaleStatefulSet(ctx, cluster.Namespace, primaryName, newCount); err != nil {
		return fmt.Errorf("scaling down primary segments: %w", err)
	}

	// Set redistribution in progress.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "InProgress",
		"Data redistribution in progress for scale-in")
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating redistribution status: %w", err)
	}

	return nil
}

// cleanupOrphanedPVCs deletes PVCs for segments that have been removed during
// scale-in. It iterates over segment indices starting from newCount and deletes
// any PVCs that still exist for both primary and mirror components.
func (r *ClusterReconciler) cleanupOrphanedPVCs(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	newCount int32,
) {
	logger := util.LoggerFromContext(ctx)

	components := []string{util.ComponentSegmentPrimary, util.ComponentSegmentMirror}
	for _, component := range components {
		stsName := util.SanitizeK8sName(fmt.Sprintf("%s-%s", cluster.Name, component))
		for i := newCount; ; i++ {
			pvcName := fmt.Sprintf("data-%s-%d", stsName, i)
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.client.Get(ctx, types.NamespacedName{
				Name:      pvcName,
				Namespace: cluster.Namespace,
			}, pvc)
			if apierrors.IsNotFound(err) {
				break // No more PVCs for this component.
			}
			if err != nil {
				logger.Error("failed to get PVC during orphan cleanup",
					"pvc", pvcName, "error", err)
				break
			}
			if delErr := r.client.Delete(ctx, pvc); delErr != nil {
				logger.Error("failed to delete orphaned PVC",
					"pvc", pvcName, "error", delErr)
			} else {
				logger.Info("deleted orphaned PVC after scale-in", "pvc", pvcName)
			}
		}
	}
}

// reconcileStorageExpansion compares desired storage sizes from the CR spec
// against actual PVC sizes and patches PVCs if the desired size is larger.
// Shrinking PVCs is not supported and is silently skipped.
// Before expanding, it verifies the StorageClass supports volume expansion;
// if not, the operation is blocked with a warning event and log message.
func (r *ClusterReconciler) reconcileStorageExpansion(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	expanded := false

	// Coordinator PVC.
	coordChanged, err := r.expandCoordinatorPVC(ctx, cluster)
	if err != nil {
		return err
	}
	expanded = expanded || coordChanged

	// Standby PVC.
	standbyChanged, err := r.expandStandbyPVC(ctx, cluster)
	if err != nil {
		return err
	}
	expanded = expanded || standbyChanged

	// Segment PVCs (all primaries + all mirrors).
	segChanged := r.expandSegmentPVCs(ctx, cluster)
	expanded = expanded || segChanged

	if expanded {
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionStorageExpanded), metav1.ConditionTrue,
			"PVCsExpanded",
			"Persistent volume claims expanded to new sizes",
		)
		if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
			return fmt.Errorf("updating storage expansion status: %w", statusErr)
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonStorageExpanded,
			"PVC storage expanded successfully")
	}

	return nil
}

// expandCoordinatorPVC expands the coordinator PVC if needed.
func (r *ClusterReconciler) expandCoordinatorPVC(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (bool, error) {
	logger := util.LoggerFromContext(ctx)
	coordPVCName := fmt.Sprintf("data-%s-0",
		util.CoordinatorName(cluster.Name))
	desiredSize := cluster.Spec.Coordinator.Storage.Size

	changed, err := r.expandPVCIfNeeded(
		ctx, cluster.Namespace, coordPVCName, desiredSize,
	)
	if err != nil {
		return false, fmt.Errorf("expanding coordinator PVC: %w", err)
	}
	if changed {
		logger.Info("coordinator PVC expanded",
			"pvc", coordPVCName, "newSize", desiredSize)
	}
	return changed, nil
}

// expandStandbyPVC expands the standby PVC if needed.
func (r *ClusterReconciler) expandStandbyPVC(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (bool, error) {
	if cluster.Spec.Standby == nil ||
		!cluster.Spec.Standby.Enabled ||
		cluster.Spec.Standby.Storage == nil {
		return false, nil
	}

	logger := util.LoggerFromContext(ctx)
	standbyPVCName := fmt.Sprintf("data-%s-0",
		util.StandbyName(cluster.Name))
	desiredSize := cluster.Spec.Standby.Storage.Size

	changed, err := r.expandPVCIfNeeded(
		ctx, cluster.Namespace, standbyPVCName, desiredSize,
	)
	if err != nil {
		return false, fmt.Errorf("expanding standby PVC: %w", err)
	}
	if changed {
		logger.Info("standby PVC expanded",
			"pvc", standbyPVCName, "newSize", desiredSize)
	}
	return changed, nil
}

// expandSegmentPVCs expands all segment PVCs (primary + mirror) if needed.
func (r *ClusterReconciler) expandSegmentPVCs(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) bool {
	logger := util.LoggerFromContext(ctx)
	segmentSize := cluster.Spec.Segments.Storage.Size
	expanded := false
	mirroringEnabled := cluster.Spec.Segments.Mirroring != nil &&
		cluster.Spec.Segments.Mirroring.Enabled

	for i := int32(0); i < cluster.Spec.Segments.Count; i++ {
		// Primary.
		primaryPVC := fmt.Sprintf("data-%s-%d",
			util.SegmentPrimaryName(cluster.Name), i)
		if changed, err := r.expandPVCIfNeeded(
			ctx, cluster.Namespace, primaryPVC, segmentSize,
		); err != nil {
			logger.Error("failed to expand primary PVC",
				"pvc", primaryPVC, "error", err)
		} else if changed {
			expanded = true
		}

		// Mirror.
		if mirroringEnabled {
			mirrorPVC := fmt.Sprintf("data-%s-%d",
				util.SegmentMirrorName(cluster.Name), i)
			if changed, err := r.expandPVCIfNeeded(
				ctx, cluster.Namespace, mirrorPVC, segmentSize,
			); err != nil {
				logger.Error("failed to expand mirror PVC",
					"pvc", mirrorPVC, "error", err)
			} else if changed {
				expanded = true
			}
		}
	}

	return expanded
}

// expandPVCIfNeeded checks whether a PVC needs expansion and patches it if so.
// Returns (true, nil) if the PVC was expanded, (false, nil) if no expansion was
// needed (including when the PVC does not exist), or (false, err) on failure.
// It verifies that the StorageClass supports volume expansion before attempting
// the resize; if not, it logs a warning and emits an event instead of failing.
func (r *ClusterReconciler) expandPVCIfNeeded(
	ctx context.Context,
	namespace, pvcName, desiredSize string,
) (bool, error) {
	logger := util.LoggerFromContext(ctx)

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // PVC doesn't exist yet — nothing to expand.
		}
		return false, fmt.Errorf("getting PVC %s: %w", pvcName, err)
	}

	desired := resource.MustParse(desiredSize)
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

	if desired.Cmp(current) <= 0 {
		return false, nil // No expansion needed (same or smaller).
	}

	// Check that the StorageClass supports volume expansion.
	if supported, reason := r.storageClassSupportsExpansion(ctx, pvc); !supported {
		logger.Warn("storage expansion blocked: StorageClass does not support volume expansion",
			"pvc", pvcName,
			"storageClass", reason,
			"currentSize", current.String(),
			"desiredSize", desired.String(),
		)
		return false, nil
	}

	// Patch the PVC with the new size.
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
	if err := r.client.Update(ctx, pvc); err != nil {
		return false, fmt.Errorf("updating PVC %s: %w", pvcName, err)
	}

	logger.Info("PVC expansion requested",
		"pvc", pvcName,
		"from", current.String(),
		"to", desired.String(),
	)
	return true, nil
}

// storageClassSupportsExpansion checks whether the StorageClass used by a PVC
// has allowVolumeExpansion set to true. Returns (true, "") if expansion is
// supported, or (false, reason) with a human-readable reason if not.
func (r *ClusterReconciler) storageClassSupportsExpansion(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (supported bool, reason string) {
	scName := ""
	if pvc.Spec.StorageClassName != nil {
		scName = *pvc.Spec.StorageClassName
	}
	if scName == "" {
		// No explicit StorageClass — try the annotation (pre-v1.6 convention).
		scName = pvc.Annotations["volume.beta.kubernetes.io/storage-class"]
	}
	if scName == "" {
		// PVC uses the cluster default StorageClass; we cannot determine
		// expansion support without listing all StorageClasses, so allow
		// the attempt and let the API server reject it if unsupported.
		return true, ""
	}

	sc := &storagev1.StorageClass{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: scName}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Sprintf("StorageClass %q not found", scName)
		}
		// On transient errors, allow the attempt rather than blocking.
		return true, ""
	}

	if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
		return false, fmt.Sprintf("StorageClass %q has allowVolumeExpansion=false", scName)
	}

	return true, ""
}

// createOrUpdateStatefulSet creates or updates a StatefulSet.
func (r *ClusterReconciler) createOrUpdateStatefulSet(ctx context.Context, desired *appsv1.StatefulSet) error {
	existing := &appsv1.StatefulSet{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}, existing)

	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating statefulset %s: %w", desired.Name, createErr)
		}
		util.LoggerFromContext(ctx).Info("created statefulset", "name", desired.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting statefulset %s: %w", desired.Name, err)
	}

	// Update if spec changed.
	if !equality.Semantic.DeepEqual(existing.Spec.Template, desired.Spec.Template) ||
		!equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) {
		existing.Spec.Template = desired.Spec.Template
		existing.Spec.Replicas = desired.Spec.Replicas
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating statefulset %s: %w", desired.Name, updateErr)
		}
		util.LoggerFromContext(ctx).Info("updated statefulset", "name", desired.Name)
	}

	return nil
}

// createOrUpdateConfigMap creates or updates a ConfigMap.
func (r *ClusterReconciler) createOrUpdateConfigMap(ctx context.Context, desired *corev1.ConfigMap) error {
	existing := &corev1.ConfigMap{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}, existing)

	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating configmap %s: %w", desired.Name, createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting configmap %s: %w", desired.Name, err)
	}

	if !equality.Semantic.DeepEqual(existing.Data, desired.Data) {
		existing.Data = desired.Data
		existing.Annotations = desired.Annotations
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating configmap %s: %w", desired.Name, updateErr)
		}
	}

	return nil
}

// createOrUpdateService creates or updates a Service.
func (r *ClusterReconciler) createOrUpdateService(ctx context.Context, desired *corev1.Service) error {
	existing := &corev1.Service{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}, existing)

	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating service %s: %w", desired.Name, createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting service %s: %w", desired.Name, err)
	}

	// Services are mostly immutable; update ports and selector.
	if !equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) ||
		!equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Ports = desired.Spec.Ports
		existing.Spec.Selector = desired.Spec.Selector
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating service %s: %w", desired.Name, updateErr)
		}
	}

	return nil
}

// handleDeletion handles the deletion of a CloudberryCluster.
func (r *ClusterReconciler) handleDeletion(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling cluster deletion")

	if !controllerutil.ContainsFinalizer(cluster, util.FinalizerName) {
		return ctrl.Result{}, nil
	}

	// Update phase to Deleting.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseDeleting {
		if _, err := r.updatePhase(ctx, cluster, cbv1alpha1.ClusterPhaseDeleting); err != nil {
			return ctrl.Result{}, err
		}
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Deleting", "Cluster deletion initiated")

	// Trigger backup before deletion if configured.
	if cluster.Spec.BackupOnDelete {
		logger.Info("triggering backup before deletion")
		timestamp := time.Now().Format("20060102-150405")
		backupJob := r.builder.BuildMaintenanceJob(cluster, util.MaintenanceBackupOnDelete, timestamp)
		if err := r.client.Create(ctx, backupJob); err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error("failed to create backup-on-delete job", "error", err)
		} else {
			r.recorder.Event(cluster, corev1.EventTypeNormal, "BackupOnDelete",
				"Backup triggered before cluster deletion")
		}
	}

	// Clean up PVCs if deletion policy is Delete.
	if cluster.Spec.DeletionPolicy == cbv1alpha1.DeletionPolicyDelete {
		if err := r.deletePVCs(ctx, cluster); err != nil {
			logger.Error("failed to delete PVCs", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, err
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, "PVCsDeleted",
			"All PVCs deleted (deletionPolicy: Delete)")
	} else {
		r.recorder.Event(cluster, corev1.EventTypeNormal, "PVCsRetained",
			"PVCs retained (deletionPolicy: Retain)")
	}

	// Remove finalizer.
	controllerutil.RemoveFinalizer(cluster, util.FinalizerName)
	if err := r.client.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}

	logger.Info("cluster deletion completed")
	r.recorder.Event(cluster, corev1.EventTypeNormal, "Deleted", "Cluster deletion completed")
	return ctrl.Result{}, nil
}

// deletePVCs deletes all PVCs owned by the cluster.
func (r *ClusterReconciler) deletePVCs(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.client.List(ctx, pvcList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing PVCs: %w", err)
	}

	for i := range pvcList.Items {
		if err := r.client.Delete(ctx, &pvcList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting PVC %s: %w", pvcList.Items[i].Name, err)
		}
	}

	return nil
}

// handleAction processes annotation-based actions.
func (r *ClusterReconciler) handleAction(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	action string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling action", "action", action)

	// Remove the action annotation using a MergePatch to avoid conflicts with stale objects.
	annotationKey := util.AnnotationAction
	patchData, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				annotationKey: nil,
			},
		},
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling annotation removal patch: %w", err)
	}

	if patchErr := r.client.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patchData)); patchErr != nil {
		logger.Error("failed to remove action annotation", "error", patchErr)
		return ctrl.Result{}, fmt.Errorf("removing action annotation: %w", patchErr)
	}

	// Re-fetch the cluster to get the latest state after the patch.
	if fetchErr := r.client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, cluster); fetchErr != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching cluster after annotation removal: %w", fetchErr)
	}

	switch action {
	case util.ActionStart, util.ActionStartRestricted, util.ActionStartMaintenance:
		return r.handleStart(ctx, cluster, action)
	case util.ActionStop, util.ActionStopFast, util.ActionStopImmediate:
		return r.handleStop(ctx, cluster, action)
	case util.ActionRestart:
		return r.handleRestart(ctx, cluster)
	default:
		logger.Warn("unknown action", "action", action)
		return ctrl.Result{}, nil
	}
}

// handleStop processes stop actions (stop, stop-fast, stop-immediate).
// It sets the phase to Stopping, scales all StatefulSets to 0, and transitions
// to Stopped once all pods are terminated.
func (r *ClusterReconciler) handleStop(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	mode string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("stopping cluster", "mode", mode)

	// Set phase to Stopping.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopping phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopping",
		fmt.Sprintf("Cluster stop initiated (mode: %s)", mode))

	// Scale StatefulSets to 0 in order: mirrors, primaries, standby, coordinator.
	for _, name := range r.getScaleDownOrder(cluster) {
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, name, 0); err != nil {
			logger.Error("failed to scale down", "statefulset", name, "error", err)
		}
	}

	// Check if all pods are terminated.
	if !r.allStatefulSetsAtScale(ctx, cluster, 0) {
		// Requeue to check again.
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	// All stopped — update status.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopped phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopped",
		fmt.Sprintf("Cluster stopped (mode: %s)", mode))
	return ctrl.Result{}, nil
}

// handleStart processes start actions (start, start-restricted, start-maintenance).
func (r *ClusterReconciler) handleStart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	mode string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("starting cluster", "mode", mode)

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Starting",
		fmt.Sprintf("Cluster start initiated (mode: %s)", mode))

	switch mode {
	case util.ActionStartRestricted:
		// Only coordinator, restricted connections.
		coordName := util.CoordinatorName(cluster.Name)
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, coordName, 1); err != nil {
			return ctrl.Result{}, fmt.Errorf("scaling coordinator for restricted start: %w", err)
		}
		// Re-fetch to get the latest resourceVersion before status update.
		if err := r.client.Get(ctx, types.NamespacedName{
			Name: cluster.Name, Namespace: cluster.Namespace,
		}, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-fetching cluster for restricted start: %w", err)
		}
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseRestricted
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting restricted phase: %w", err)
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Started",
			"Cluster started in restricted mode")
		return ctrl.Result{}, nil

	case util.ActionStartMaintenance:
		// Only coordinator in utility mode.
		coordName := util.CoordinatorName(cluster.Name)
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, coordName, 1); err != nil {
			return ctrl.Result{}, fmt.Errorf("scaling coordinator for maintenance start: %w", err)
		}
		// Re-fetch to get the latest resourceVersion before status update.
		if err := r.client.Get(ctx, types.NamespacedName{
			Name: cluster.Name, Namespace: cluster.Namespace,
		}, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-fetching cluster for maintenance start: %w", err)
		}
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseMaintenance
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting maintenance phase: %w", err)
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Started",
			"Cluster started in maintenance mode")
		return ctrl.Result{}, nil

	default:
		// Normal start — reset phase so updateStatus can transition to Running,
		// then scale everything back via full reconciliation.
		// Re-fetch to get the latest resourceVersion before status update.
		if err := r.client.Get(ctx, types.NamespacedName{
			Name: cluster.Name, Namespace: cluster.Namespace,
		}, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-fetching cluster for start: %w", err)
		}
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("resetting phase for normal start: %w", err)
		}
		return r.reconcileCluster(ctx, cluster)
	}
}

// handleRestart processes the restart action.
// It marks the cluster with a restart-pending annotation, then initiates a stop.
// When the stop completes, the lifecycle handler detects the pending restart
// and triggers a full start.
func (r *ClusterReconciler) handleRestart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("restarting cluster")

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Restarting", "Cluster restart initiated")

	// Mark restart as pending so the lifecycle handler knows to start after stop.
	if err := setAnnotationPatch(ctx, r.client, cluster, util.AnnotationRestartPending, "true"); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting restart-pending annotation: %w", err)
	}

	// Set phase to Stopping.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopping phase for restart: %w", err)
	}

	// Scale all StatefulSets to 0.
	stsNames := r.getScaleDownOrder(cluster)
	for _, name := range stsNames {
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, name, 0); err != nil {
			logger.Error("failed to scale down for restart", "statefulset", name, "error", err)
		}
	}

	// Check if all pods are terminated.
	if r.allStatefulSetsAtScale(ctx, cluster, 0) {
		return r.completeRestart(ctx, cluster)
	}

	return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
}

// completeRestart finishes a restart by removing the restart-pending annotation,
// transitioning through Stopped, and starting the cluster back up.
func (r *ClusterReconciler) completeRestart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	// Remove the restart-pending annotation.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationRestartPending); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing restart-pending annotation: %w", err)
	}

	// Re-fetch to get the latest resourceVersion before status update.
	if err := r.client.Get(ctx, types.NamespacedName{
		Name: cluster.Name, Namespace: cluster.Namespace,
	}, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching cluster for restart: %w", err)
	}

	// Reset phase to Initializing so updateStatus can transition to Running.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("resetting phase for restart: %w", err)
	}

	// Start everything back up via full reconciliation.
	r.recorder.Event(cluster, corev1.EventTypeNormal, "Restarted",
		"Cluster restart: scaling back up")
	return r.reconcileCluster(ctx, cluster)
}

// checkStopProgress checks whether a cluster in Stopping phase has completed scale-down.
// If a restart is pending, it completes the restart instead of just stopping.
func (r *ClusterReconciler) checkStopProgress(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("checking stop progress")

	if !r.allStatefulSetsAtScale(ctx, cluster, 0) {
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	// If a restart is pending, complete the restart instead of just stopping.
	if _, pending := cluster.Annotations[util.AnnotationRestartPending]; pending {
		return r.completeRestart(ctx, cluster)
	}

	// All stopped.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopped phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopped", "Cluster stopped")
	return ctrl.Result{}, nil
}

// checkScaleProgress checks whether a cluster in Scaling phase has completed
// the scale operation (scale-out or scale-in). When all segment StatefulSets
// are ready at the desired replica count, it transitions the cluster back to
// Running and handles PVC cleanup for scale-in when the deletion policy is Delete.
func (r *ClusterReconciler) checkScaleProgress(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("checking scale progress")

	if r.allSegmentStatefulSetsReady(ctx, cluster) {
		return r.completeScaleOperation(ctx, cluster)
	}

	// Check for timeout — if scaling has been in progress too long, mark as failed.
	startedStr := cluster.Annotations[util.AnnotationScaleStarted]
	if startedStr != "" {
		started, err := time.Parse(time.RFC3339, startedStr)
		if err == nil && time.Since(started) > scaleTimeout {
			return r.handleScaleFailure(ctx, cluster)
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
}

// completeScaleOperation finalizes a scale-in or scale-out that has reached
// the desired replica count. It transitions the cluster back to Running,
// emits the appropriate events and metrics, cleans up orphaned PVCs (for
// scale-in with Delete policy), and removes transient annotations.
func (r *ClusterReconciler) completeScaleOperation(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	// Determine if this was scale-in or scale-out by comparing
	// the desired count against the previously recorded total.
	isScaleIn := cluster.Spec.Segments.Count < cluster.Status.SegmentsTotal

	// Scale complete — transition to Running.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.SegmentsReady = cluster.Spec.Segments.Count
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "Completed",
		"Data redistribution completed")

	if isScaleIn {
		r.finaliseScaleIn(ctx, cluster)
	} else {
		r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonScaleOutCompleted,
			fmt.Sprintf("Scale-out completed, segments: %d", cluster.Spec.Segments.Count))
		r.metrics.RecordScaleOperation(cluster.Name, cluster.Namespace, "scale-out")
	}

	cluster.Status.SegmentsTotal = cluster.Spec.Segments.Count
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after scale completion: %w", err)
	}

	// Remove transient annotations after status update to avoid
	// re-fetch overwriting in-memory status changes.
	r.cleanupScaleAnnotations(ctx, cluster, isScaleIn)

	return ctrl.Result{Requeue: true}, nil
}

// finaliseScaleIn handles scale-in specific completion tasks: PVC cleanup,
// event emission, and metrics recording.
func (r *ClusterReconciler) finaliseScaleIn(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	// Handle PVC cleanup for removed segments when deletion policy is Delete.
	if cluster.Spec.DeletionPolicy == cbv1alpha1.DeletionPolicyDelete {
		r.cleanupOrphanedPVCs(ctx, cluster, cluster.Spec.Segments.Count)
	}
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonScaleInCompleted,
		fmt.Sprintf("Scale-in completed, segments: %d", cluster.Spec.Segments.Count))
	r.metrics.RecordScaleOperation(cluster.Name, cluster.Namespace, "scale-in")
}

// cleanupScaleAnnotations removes transient scale-related annotations after
// a successful scale operation completes.
func (r *ClusterReconciler) cleanupScaleAnnotations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	isScaleIn bool,
) {
	logger := util.LoggerFromContext(ctx)

	if err := removeAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationScaleStarted); err != nil {
		logger.Error("failed to remove scale-started annotation", "error", err)
	}

	// Clean up confirmation annotation after successful scale-in to avoid
	// stale annotations persisting across future reconciliation cycles.
	if isScaleIn && cluster.Annotations[util.AnnotationConfirmScaleIn] == "true" {
		if err := removeAnnotationPatch(ctx, r.client, cluster,
			util.AnnotationConfirmScaleIn); err != nil {
			logger.Error("failed to remove confirm-scale-in annotation", "error", err)
		}
	}
}

// handleScaleFailure handles a scale operation that has timed out. It identifies
// which segments are not ready, records the failure in status conditions and events,
// and removes the scale-started annotation. The cluster stays in Scaling phase —
// the operator does NOT automatically roll back.
func (r *ClusterReconciler) handleScaleFailure(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Error("scale operation timed out", "timeout", scaleTimeout)

	// Identify which segments are not ready.
	var failedSegments []cbv1alpha1.FailedSegment

	primarySts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primarySts); err == nil {
		if primarySts.Spec.Replicas != nil {
			for i := primarySts.Status.ReadyReplicas; i < *primarySts.Spec.Replicas; i++ {
				failedSegments = append(failedSegments, cbv1alpha1.FailedSegment{
					ContentID: i,
					Hostname:  fmt.Sprintf("%s-%d", util.SegmentPrimaryName(cluster.Name), i),
					Role:      "primary",
					Status:    "NotReady",
				})
			}
		}
	}

	// Check mirror segments for failures.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		mirrorSts := &appsv1.StatefulSet{}
		if err := r.client.Get(ctx, types.NamespacedName{
			Name:      util.SegmentMirrorName(cluster.Name),
			Namespace: cluster.Namespace,
		}, mirrorSts); err == nil {
			if mirrorSts.Spec.Replicas != nil {
				for i := mirrorSts.Status.ReadyReplicas; i < *mirrorSts.Spec.Replicas; i++ {
					failedSegments = append(failedSegments, cbv1alpha1.FailedSegment{
						ContentID: i,
						Hostname:  fmt.Sprintf("%s-%d", util.SegmentMirrorName(cluster.Name), i),
						Role:      "mirror",
						Status:    "NotReady",
					})
				}
			}
		}
	}

	// Update status — do NOT change phase back to Running (no automatic rollback).
	cluster.Status.FailedSegments = failedSegments
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionScaleOutFailed), metav1.ConditionTrue, "SegmentsNotReady",
		fmt.Sprintf("Scale-out failed: %d segments not ready after %v",
			len(failedSegments), scaleTimeout))

	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating scale failure status: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonScaleOutFailed,
		fmt.Sprintf("Scale-out failed: %d segments not ready after timeout",
			len(failedSegments)))

	// Remove scale-started annotation (after status update to avoid
	// re-fetch overwriting in-memory status changes).
	if err := removeAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationScaleStarted); err != nil {
		logger.Error("failed to remove scale-started annotation after failure",
			"error", err)
	}

	// Stay in Scaling phase — operator does NOT automatically scale back.
	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// allSegmentStatefulSetsReady checks whether all segment StatefulSets
// (primary and mirror) have reached the desired replica count.
func (r *ClusterReconciler) allSegmentStatefulSetsReady(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) bool {
	desiredCount := cluster.Spec.Segments.Count

	// Check primary segments.
	ready, _ := r.isStatefulSetAtScale(ctx, cluster.Namespace,
		util.SegmentPrimaryName(cluster.Name), desiredCount)
	if !ready {
		return false
	}

	// Check mirror segments if mirroring is enabled.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		mirrorReady, _ := r.isStatefulSetAtScale(ctx, cluster.Namespace,
			util.SegmentMirrorName(cluster.Name), desiredCount)
		if !mirrorReady {
			return false
		}
	}

	return true
}

// allStatefulSetsAtScale checks whether all cluster StatefulSets have reached
// the desired replica count.
func (r *ClusterReconciler) allStatefulSetsAtScale(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	replicas int32,
) bool {
	stsNames := r.getScaleDownOrder(cluster)
	for _, name := range stsNames {
		ready, _ := r.isStatefulSetAtScale(ctx, cluster.Namespace, name, replicas)
		if !ready {
			return false
		}
	}
	return true
}

// scaleStatefulSet scales a StatefulSet to the desired number of replicas.
func (r *ClusterReconciler) scaleStatefulSet(
	ctx context.Context,
	namespace, name string,
	replicas int32,
) error {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			// StatefulSet doesn't exist; nothing to scale.
			return nil
		}
		return fmt.Errorf("getting statefulset %s for scaling: %w", name, err)
	}

	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == replicas {
		// Already at the desired scale.
		return nil
	}

	sts.Spec.Replicas = &replicas
	if err := r.client.Update(ctx, sts); err != nil {
		return fmt.Errorf("scaling statefulset %s to %d: %w", name, replicas, err)
	}

	util.LoggerFromContext(ctx).Info("scaled statefulset",
		"name", name, "replicas", replicas)
	return nil
}

// isStatefulSetAtScale checks whether a StatefulSet has reached the desired replica count.
// Returns true if the StatefulSet does not exist (nothing to wait for).
func (r *ClusterReconciler) isStatefulSetAtScale(
	ctx context.Context,
	namespace, name string,
	replicas int32,
) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			// StatefulSet doesn't exist — treat as "at scale" (nothing to wait for).
			return true, nil
		}
		return false, fmt.Errorf("getting statefulset %s for scale check: %w", name, err)
	}

	if replicas == 0 {
		return sts.Status.Replicas == 0, nil
	}
	return sts.Status.ReadyReplicas >= replicas, nil
}

// getScaleDownOrder returns the StatefulSet names in the order they should be
// scaled down: mirrors first, then primaries, then standby, then coordinator.
func (r *ClusterReconciler) getScaleDownOrder(
	cluster *cbv1alpha1.CloudberryCluster,
) []string {
	var names []string

	// Mirrors first.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		names = append(names, util.SegmentMirrorName(cluster.Name))
	}

	// Primary segments.
	names = append(names, util.SegmentPrimaryName(cluster.Name))

	// Standby coordinator.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		names = append(names, util.StandbyName(cluster.Name))
	}

	// Coordinator last.
	names = append(names, util.CoordinatorName(cluster.Name))

	return names
}

// updatePhase updates the cluster phase in the status.
func (r *ClusterReconciler) updatePhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	phase cbv1alpha1.ClusterPhase,
) (ctrl.Result, error) {
	cluster.Status.Phase = phase
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating phase to %s: %w", phase, err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// updateStatus updates the cluster status based on current resource state.
func (r *ClusterReconciler) updateStatus(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	now := metav1.Now()
	cluster.Status.LastReconcileTime = &now
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.ClusterVersion = cluster.Spec.Version
	cluster.Status.SegmentsTotal = cluster.Spec.Segments.Count

	// Check coordinator readiness.
	coordSts := &appsv1.StatefulSet{}
	coordErr := r.client.Get(ctx, types.NamespacedName{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, coordSts)
	if coordErr == nil {
		cluster.Status.CoordinatorReady = coordSts.Status.ReadyReplicas > 0
	}

	// Check standby readiness.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		standbySts := &appsv1.StatefulSet{}
		standbyErr := r.client.Get(ctx, types.NamespacedName{
			Name:      util.StandbyName(cluster.Name),
			Namespace: cluster.Namespace,
		}, standbySts)
		if standbyErr == nil {
			cluster.Status.StandbyReady = standbySts.Status.ReadyReplicas > 0
		}
	}

	// Check segment readiness.
	primarySts := &appsv1.StatefulSet{}
	primaryErr := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primarySts)
	if primaryErr == nil {
		cluster.Status.SegmentsReady = primarySts.Status.ReadyReplicas
	}

	// Determine mirroring status.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		mirrorSts := &appsv1.StatefulSet{}
		mirrorErr := r.client.Get(ctx, types.NamespacedName{
			Name:      util.SegmentMirrorName(cluster.Name),
			Namespace: cluster.Namespace,
		}, mirrorSts)
		if mirrorErr == nil && mirrorSts.Spec.Replicas != nil &&
			mirrorSts.Status.ReadyReplicas == *mirrorSts.Spec.Replicas {
			cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
		} else {
			cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded
		}
	} else {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	}

	// Determine overall phase.
	// Preserve intentional lifecycle phases that should not be overridden.
	switch cluster.Status.Phase {
	case cbv1alpha1.ClusterPhaseDeleting,
		cbv1alpha1.ClusterPhaseStopped,
		cbv1alpha1.ClusterPhaseStopping,
		cbv1alpha1.ClusterPhaseRestricted,
		cbv1alpha1.ClusterPhaseMaintenance,
		cbv1alpha1.ClusterPhaseScaling,
		cbv1alpha1.ClusterPhaseUpdating:
		// Do not override these phases — they are managed by action handlers.
	default:
		if cluster.Status.CoordinatorReady && cluster.Status.SegmentsReady == cluster.Status.SegmentsTotal {
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			cluster.Status.Conditions = util.SetCondition(
				cluster.Status.Conditions,
				string(cbv1alpha1.ConditionClusterReady),
				metav1.ConditionTrue,
				"AllComponentsReady",
				"All cluster components are running and healthy",
			)
		} else {
			if cluster.Status.Phase == cbv1alpha1.ClusterPhasePending {
				cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
			}
			cluster.Status.Conditions = util.SetCondition(
				cluster.Status.Conditions,
				string(cbv1alpha1.ConditionClusterReady),
				metav1.ConditionFalse,
				"ComponentsNotReady",
				"Some cluster components are not yet ready",
			)
		}
	}

	// Update metrics.
	r.metrics.UpdateClusterInfo(
		cluster.Name, cluster.Namespace,
		cluster.Spec.Version, string(cluster.Status.Phase),
		float64(cluster.Status.SegmentsReady),
	)
	r.metrics.SetCoordinatorUp(cluster.Name, cluster.Namespace, cluster.Status.CoordinatorReady)
	r.metrics.SetStandbyUp(cluster.Name, cluster.Namespace, cluster.Status.StandbyReady)
	r.metrics.SetSegmentsReady(cluster.Name, cluster.Namespace, float64(cluster.Status.SegmentsReady))
	r.metrics.SetSegmentsTotal(cluster.Name, cluster.Namespace, float64(cluster.Status.SegmentsTotal))

	return r.client.Status().Update(ctx, cluster)
}

// recordReconcileResult records the reconciliation result in metrics.
func (r *ClusterReconciler) recordReconcileResult(
	cluster *cbv1alpha1.CloudberryCluster,
	startTime time.Time,
	result string,
) {
	r.metrics.RecordReconcile(cluster.Name, cluster.Namespace, result, time.Since(startTime))
}

// recordMetricsSnapshot publishes the current cluster state to Prometheus
// metrics without performing a full reconciliation. This ensures metrics
// are always up-to-date even when the generation-skip optimisation fires.
func (r *ClusterReconciler) recordMetricsSnapshot(cluster *cbv1alpha1.CloudberryCluster) {
	r.metrics.UpdateClusterInfo(
		cluster.Name, cluster.Namespace,
		cluster.Spec.Version, string(cluster.Status.Phase),
		float64(cluster.Status.SegmentsReady),
	)
	r.metrics.SetCoordinatorUp(cluster.Name, cluster.Namespace, cluster.Status.CoordinatorReady)
	r.metrics.SetStandbyUp(cluster.Name, cluster.Namespace, cluster.Status.StandbyReady)
	r.metrics.SetSegmentsReady(cluster.Name, cluster.Namespace, float64(cluster.Status.SegmentsReady))
	r.metrics.SetSegmentsTotal(cluster.Name, cluster.Namespace, float64(cluster.Status.SegmentsTotal))

	r.metrics.SetMirroringInSync(
		cluster.Name, cluster.Namespace,
		cluster.Status.MirroringStatus == cbv1alpha1.MirroringInSync,
	)
	r.metrics.SetConnectionsMax(cluster.Name, cluster.Namespace, 0)
}

// upgradeStateData holds the state of an in-progress cluster upgrade.
type upgradeStateData struct {
	PreviousImage   string `json:"previousImage"`
	PreviousVersion string `json:"previousVersion"`
	Phase           string `json:"phase"`
	StartedAt       string `json:"startedAt"`
	PhaseStartedAt  string `json:"phaseStartedAt"`
}

// Upgrade phase constants define the ordered phases of a rolling upgrade.
const (
	upgradePhaseMirrors     = restartPhaseMirrors
	upgradePhasePrimaries   = restartPhasePrimaries
	upgradePhaseStandby     = restartPhaseStandby
	upgradePhaseCoordinator = restartPhaseCoordinator
	upgradePhaseVerify      = "verify"
)

// isUpgradeNeeded checks whether a cluster upgrade is needed or in progress.
func (r *ClusterReconciler) isUpgradeNeeded(cluster *cbv1alpha1.CloudberryCluster) bool {
	// Check if an upgrade is already in progress.
	if cluster.Annotations[util.AnnotationUpgrade] != "" {
		return true
	}
	// Do not re-attempt an upgrade that has already failed and been rolled back.
	// The user must acknowledge the failure (e.g. fix the image, remove the
	// UpgradeFailed condition) before the operator will try again.
	for _, c := range cluster.Status.Conditions {
		if c.Type == string(cbv1alpha1.ConditionUpgradeFailed) && c.Status == metav1.ConditionTrue {
			return false
		}
	}
	// Check if the version changed from the last known cluster version.
	if cluster.Status.ClusterVersion != "" && cluster.Status.ClusterVersion != cluster.Spec.Version {
		return true
	}
	return false
}

// handleUpgrade initiates or continues a cluster upgrade.
func (r *ClusterReconciler) handleUpgrade(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// If an upgrade is already in progress, continue it.
	if cluster.Annotations[util.AnnotationUpgrade] != "" {
		return r.continueUpgrade(ctx, cluster)
	}

	// Pre-flight: cluster must be in Running phase to start an upgrade.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Warn("upgrade blocked: cluster not in Running phase",
			"currentPhase", cluster.Status.Phase)
		r.recorder.Event(cluster, corev1.EventTypeWarning, "UpgradeBlocked",
			fmt.Sprintf("Cluster must be Running to upgrade, current phase: %s", cluster.Status.Phase))
		return ctrl.Result{}, nil
	}

	// Determine the current image from an existing StatefulSet.
	currentImage := r.getCurrentImage(ctx, cluster)

	// Store previous image/version for rollback.
	now := time.Now().Format(time.RFC3339)
	state := upgradeStateData{
		PreviousImage:   currentImage,
		PreviousVersion: cluster.Status.ClusterVersion,
		Phase:           upgradePhaseMirrors,
		StartedAt:       now,
		PhaseStartedAt:  now,
	}

	// Set phase to Updating.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting updating phase: %w", err)
	}

	// Save upgrade state annotation.
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling upgrade state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster, util.AnnotationUpgrade, string(stateJSON)); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting upgrade annotation: %w", err)
	}

	logger.Info("upgrade initiated",
		"previousImage", state.PreviousImage,
		"previousVersion", state.PreviousVersion,
		"newImage", cluster.Spec.Image,
		"newVersion", cluster.Spec.Version)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonUpgradeStarted,
		fmt.Sprintf("Upgrade from %s to %s initiated", state.PreviousVersion, cluster.Spec.Version))

	return r.continueUpgrade(ctx, cluster)
}

// continueUpgrade processes the current upgrade phase and advances to the next
// when the current phase's StatefulSet is ready with the new image.
func (r *ClusterReconciler) continueUpgrade(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Parse upgrade state from annotation.
	stateJSON := cluster.Annotations[util.AnnotationUpgrade]
	if stateJSON == "" {
		// No upgrade in progress — should not happen, but handle gracefully.
		logger.Warn("continueUpgrade called but no upgrade annotation found")
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	var state upgradeStateData
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		logger.Error("failed to parse upgrade state", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil
	}

	logger.Info("continuing upgrade", "phase", state.Phase,
		"previousImage", state.PreviousImage, "newImage", cluster.Spec.Image)

	// Check for phase timeout.
	if state.PhaseStartedAt != "" {
		phaseStart, parseErr := time.Parse(time.RFC3339, state.PhaseStartedAt)
		if parseErr == nil && time.Since(phaseStart) > upgradePhaseTimeout {
			reason := fmt.Sprintf("phase %q timed out after %v", state.Phase, upgradePhaseTimeout)
			return r.rollbackUpgrade(ctx, cluster, state, reason)
		}
	}

	newImage := cluster.Spec.Image

	switch state.Phase {
	case upgradePhaseMirrors:
		return r.upgradePhase(ctx, cluster, &state, newImage,
			util.SegmentMirrorName(cluster.Name),
			cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled,
			upgradePhasePrimaries)

	case upgradePhasePrimaries:
		return r.upgradePhase(ctx, cluster, &state, newImage,
			util.SegmentPrimaryName(cluster.Name),
			true,
			upgradePhaseStandby)

	case upgradePhaseStandby:
		return r.upgradePhase(ctx, cluster, &state, newImage,
			util.StandbyName(cluster.Name),
			cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled,
			upgradePhaseCoordinator)

	case upgradePhaseCoordinator:
		return r.upgradePhase(ctx, cluster, &state, newImage,
			util.CoordinatorName(cluster.Name),
			true,
			upgradePhaseVerify)

	case upgradePhaseVerify:
		return r.verifyUpgrade(ctx, cluster, state)

	default:
		logger.Warn("unknown upgrade phase, completing upgrade", "phase", state.Phase)
		return r.completeUpgrade(ctx, cluster, state)
	}
}

// upgradePhase handles a single upgrade phase: updates the StatefulSet image,
// checks readiness, and advances to the next phase when ready.
func (r *ClusterReconciler) upgradePhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *upgradeStateData,
	newImage, stsName string,
	componentEnabled bool,
	nextPhase string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Skip this phase if the component is not enabled.
	if !componentEnabled {
		logger.Info("skipping upgrade phase, component not enabled",
			"phase", state.Phase, "statefulset", stsName)
		return r.advanceUpgradePhase(ctx, cluster, state, nextPhase)
	}

	// Update the StatefulSet image.
	if err := r.updateStatefulSetImage(ctx, cluster.Namespace, stsName, newImage); err != nil {
		logger.Error("failed to update StatefulSet image",
			"statefulset", stsName, "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil
	}

	// Check if the StatefulSet is ready with the new image.
	if r.isStatefulSetReady(ctx, cluster.Namespace, stsName) {
		logger.Info("upgrade phase complete, advancing",
			"phase", state.Phase, "nextPhase", nextPhase)
		return r.advanceUpgradePhase(ctx, cluster, state, nextPhase)
	}

	// Not ready yet — requeue.
	logger.Info("waiting for StatefulSet to be ready",
		"phase", state.Phase, "statefulset", stsName)
	return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
}

// advanceUpgradePhase updates the upgrade state to the next phase.
func (r *ClusterReconciler) advanceUpgradePhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *upgradeStateData,
	nextPhase string,
) (ctrl.Result, error) {
	state.Phase = nextPhase
	state.PhaseStartedAt = time.Now().Format(time.RFC3339)

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling upgrade state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster, util.AnnotationUpgrade, string(stateJSON)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating upgrade annotation: %w", err)
	}

	// Immediately continue to the next phase.
	return ctrl.Result{Requeue: true}, nil
}

// verifyUpgrade checks that all cluster components are healthy after the upgrade.
func (r *ClusterReconciler) verifyUpgrade(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state upgradeStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Check coordinator readiness.
	coordReady := r.isStatefulSetReady(ctx, cluster.Namespace, util.CoordinatorName(cluster.Name))
	if !coordReady {
		logger.Info("verify phase: coordinator not ready yet")
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	// Check primary segments readiness.
	primaryReady := r.isStatefulSetReady(ctx, cluster.Namespace, util.SegmentPrimaryName(cluster.Name))
	if !primaryReady {
		logger.Info("verify phase: primary segments not ready yet")
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	logger.Info("upgrade verification passed, completing upgrade")
	return r.completeUpgrade(ctx, cluster, state)
}

// completeUpgrade finalizes a successful upgrade: removes the annotation,
// updates the cluster version, and transitions back to Running.
func (r *ClusterReconciler) completeUpgrade(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state upgradeStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Update status: phase → Running, clusterVersion → new version.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ClusterVersion = cluster.Spec.Version
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionUpgradeCompleted), metav1.ConditionTrue, "UpgradeSucceeded",
		fmt.Sprintf("Upgraded from %s to %s", state.PreviousVersion, cluster.Spec.Version))
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after upgrade: %w", err)
	}

	// Remove upgrade annotation (after status update to avoid re-fetch overwriting).
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationUpgrade); err != nil {
		logger.Error("failed to remove upgrade annotation", "error", err)
	}

	logger.Info("upgrade completed successfully",
		"previousVersion", state.PreviousVersion,
		"newVersion", cluster.Spec.Version)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonUpgradeCompleted,
		fmt.Sprintf("Upgrade from %s to %s completed", state.PreviousVersion, cluster.Spec.Version))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// rollbackUpgrade reverts all StatefulSets to the previous image and
// transitions the cluster back to Running with the old version.
func (r *ClusterReconciler) rollbackUpgrade(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state upgradeStateData,
	reason string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Error("upgrade failed, rolling back",
		"reason", reason,
		"previousImage", state.PreviousImage,
		"previousVersion", state.PreviousVersion)

	// Revert all StatefulSets to the previous image.
	for _, stsName := range r.getAllStatefulSetNames(cluster) {
		if err := r.revertStatefulSetImage(ctx, cluster.Namespace, stsName, state.PreviousImage); err != nil {
			logger.Error("failed to revert StatefulSet image",
				"statefulset", stsName, "error", err)
		}
	}

	// Update status: phase → Running, clusterVersion → old version.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ClusterVersion = state.PreviousVersion
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionUpgradeFailed), metav1.ConditionTrue, "RolledBack", reason)
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after rollback: %w", err)
	}

	// Remove upgrade annotation (after status update to avoid re-fetch overwriting).
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationUpgrade); err != nil {
		logger.Error("failed to remove upgrade annotation after rollback", "error", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonUpgradeRollback,
		fmt.Sprintf("Upgrade rolled back: %s", reason))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// getCurrentImage retrieves the current container image from the coordinator
// StatefulSet. Falls back to the spec image if the StatefulSet is not found.
func (r *ClusterReconciler) getCurrentImage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return cluster.Spec.Image
	}
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Image != "" {
			return c.Image
		}
	}
	return cluster.Spec.Image
}

// updateStatefulSetImage updates the container image of a StatefulSet.
func (r *ClusterReconciler) updateStatefulSetImage(
	ctx context.Context,
	namespace, name, image string,
) error {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // StatefulSet doesn't exist; nothing to update.
		}
		return fmt.Errorf("getting statefulset %s: %w", name, err)
	}

	// Check if image already matches.
	updated := false
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Image != image {
			sts.Spec.Template.Spec.Containers[i].Image = image
			updated = true
		}
	}

	if !updated {
		return nil // Image already matches.
	}

	if err := r.client.Update(ctx, sts); err != nil {
		return fmt.Errorf("updating statefulset %s image: %w", name, err)
	}

	util.LoggerFromContext(ctx).Info("updated StatefulSet image",
		"statefulset", name, "image", image)
	return nil
}

// revertStatefulSetImage reverts the container image of a StatefulSet to the
// previous image. This is used during upgrade rollback.
func (r *ClusterReconciler) revertStatefulSetImage(
	ctx context.Context,
	namespace, name, image string,
) error {
	return r.updateStatefulSetImage(ctx, namespace, name, image)
}

// isStatefulSetReady checks whether a StatefulSet has all desired replicas ready.
func (r *ClusterReconciler) isStatefulSetReady(
	ctx context.Context,
	namespace, name string,
) bool {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return true // StatefulSet doesn't exist — nothing to wait for.
		}
		return false
	}

	if sts.Spec.Replicas == nil {
		return sts.Status.ReadyReplicas >= 1
	}
	return sts.Status.ReadyReplicas >= *sts.Spec.Replicas
}

// getAllStatefulSetNames returns all StatefulSet names for a cluster,
// in the order: mirrors, primaries, standby, coordinator.
func (r *ClusterReconciler) getAllStatefulSetNames(
	cluster *cbv1alpha1.CloudberryCluster,
) []string {
	// Reuse the existing getScaleDownOrder which returns the same order.
	return r.getScaleDownOrder(cluster)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Named(clusterControllerName).
		Complete(r)
}

// buildAnnotationPatch builds a MergePatch payload for setting or removing an annotation.
// A nil value removes the annotation; a non-nil value sets it.
func buildAnnotationPatch(key string, value interface{}) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		patchKeyMetadata: map[string]interface{}{
			patchKeyAnnotations: map[string]interface{}{
				key: value,
			},
		},
	})
}

// removeAnnotationPatch removes an annotation from a cluster using a MergePatch
// to avoid conflicts with stale objects. After patching, it re-fetches the
// cluster to ensure the caller has the latest state.
func removeAnnotationPatch(
	ctx context.Context,
	c client.Client,
	cluster *cbv1alpha1.CloudberryCluster,
	annotationKey string,
) error {
	patchData, err := buildAnnotationPatch(annotationKey, nil)
	if err != nil {
		return fmt.Errorf("marshaling annotation removal patch: %w", err)
	}

	if patchErr := c.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patchData)); patchErr != nil {
		return fmt.Errorf("patching annotation removal: %w", patchErr)
	}

	// Re-fetch the cluster to get the latest state after the patch.
	if fetchErr := c.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, cluster); fetchErr != nil {
		return fmt.Errorf("re-fetching cluster after annotation removal: %w", fetchErr)
	}

	return nil
}

// setAnnotationPatch sets an annotation on a cluster using a MergePatch.
func setAnnotationPatch(
	ctx context.Context,
	c client.Client,
	cluster *cbv1alpha1.CloudberryCluster,
	annotationKey, value string,
) error {
	patchData, err := buildAnnotationPatch(annotationKey, value)
	if err != nil {
		return fmt.Errorf("marshaling annotation set patch: %w", err)
	}

	return c.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patchData))
}

// patchStatus patches the status subresource of a CloudberryCluster using MergePatch.
// This prevents clobbering status changes from other controllers.
func patchStatus(
	ctx context.Context,
	c client.Client,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	statusPatch, err := json.Marshal(map[string]interface{}{
		"status": cluster.Status,
	})
	if err != nil {
		return fmt.Errorf("marshaling status patch: %w", err)
	}

	return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}

// patchFTSStatus patches the FTS-related status fields using a manually constructed
// MergePatch. This is necessary because the FailedSegments field uses omitempty,
// which causes json.Marshal to omit it when empty. With MergePatch, omitted fields
// are left unchanged, so we must explicitly include failedSegments as an empty array
// to clear previously failed segments.
func patchFTSStatus(
	ctx context.Context,
	c client.Client,
	cluster *cbv1alpha1.CloudberryCluster,
	failedSegments []cbv1alpha1.FailedSegment,
) error {
	// Build the patch manually to ensure failedSegments is always included,
	// even when empty (to clear previous failures via MergePatch).
	statusMap := map[string]interface{}{
		"mirroringStatus": cluster.Status.MirroringStatus,
	}
	if len(failedSegments) == 0 {
		statusMap["failedSegments"] = []interface{}{}
	} else {
		statusMap["failedSegments"] = failedSegments
	}

	statusPatch, err := json.Marshal(map[string]interface{}{
		"status": statusMap,
	})
	if err != nil {
		return fmt.Errorf("marshaling FTS status patch: %w", err)
	}

	return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}
