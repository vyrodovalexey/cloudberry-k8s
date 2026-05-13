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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	desired := r.builder.BuildCoordinatorStatefulSet(cluster)
	if desired == nil {
		return fmt.Errorf("failed to build coordinator StatefulSet for cluster %s", cluster.Name)
	}
	return r.createOrUpdateStatefulSet(ctx, desired)
}

// reconcileStandby ensures the standby StatefulSet is in the desired state.
func (r *ClusterReconciler) reconcileStandby(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired := r.builder.BuildStandbyStatefulSet(cluster)
	if desired == nil {
		return nil
	}
	return r.createOrUpdateStatefulSet(ctx, desired)
}

// reconcileSegments ensures segment StatefulSets are in the desired state.
func (r *ClusterReconciler) reconcileSegments(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Primary segments.
	primarySts := r.builder.BuildSegmentPrimaryStatefulSet(cluster)
	if primarySts == nil {
		return fmt.Errorf("failed to build primary segment StatefulSet for cluster %s", cluster.Name)
	}
	if err := r.createOrUpdateStatefulSet(ctx, primarySts); err != nil {
		return fmt.Errorf("primary segments: %w", err)
	}

	// Mirror segments.
	mirrorSts := r.builder.BuildSegmentMirrorStatefulSet(cluster)
	if mirrorSts != nil {
		if err := r.createOrUpdateStatefulSet(ctx, mirrorSts); err != nil {
			return fmt.Errorf("mirror segments: %w", err)
		}
	}

	return nil
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

	// Clean up PVCs if deletion policy is Delete.
	if cluster.Spec.DeletionPolicy == cbv1alpha1.DeletionPolicyDelete {
		if err := r.deletePVCs(ctx, cluster); err != nil {
			logger.Error("failed to delete PVCs", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, err
		}
	}

	// Remove finalizer.
	controllerutil.RemoveFinalizer(cluster, util.FinalizerName)
	if err := r.client.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}

	logger.Info("cluster deletion completed")
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
		cbv1alpha1.ClusterPhaseMaintenance:
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
