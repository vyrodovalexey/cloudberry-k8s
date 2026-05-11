// Package controller implements the Kubernetes controllers for the cloudberry operator.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

	// Handle annotation-based actions.
	if action, ok := cluster.Annotations[util.AnnotationAction]; ok {
		result, err := r.handleAction(ctx, cluster, action)
		if err != nil {
			r.recordReconcileResult(cluster, startTime, "error")
			return result, err
		}
		return result, nil
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

// reconcileServices ensures Services are in the desired state.
func (r *ClusterReconciler) reconcileServices(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	services := []*corev1.Service{
		r.builder.BuildCoordinatorService(cluster),
		r.builder.BuildStandbyService(cluster),
		r.builder.BuildSegmentService(cluster),
		r.builder.BuildClientService(cluster),
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

	// Remove the action annotation after processing.
	defer func() {
		delete(cluster.Annotations, util.AnnotationAction)
		if err := r.client.Update(ctx, cluster); err != nil {
			logger.Error("failed to remove action annotation", "error", err)
		}
	}()

	switch action {
	case util.ActionStart, util.ActionStartRestricted, util.ActionStartMaintenance:
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Starting", "Cluster start initiated")
		return r.reconcileCluster(ctx, cluster)
	case util.ActionStop:
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopping", "Cluster stop initiated")
		return r.reconcileCluster(ctx, cluster)
	case util.ActionRestart:
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Restarting", "Cluster restart initiated")
		return r.reconcileCluster(ctx, cluster)
	default:
		logger.Warn("unknown action", "action", action)
		return ctrl.Result{}, nil
	}
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
		if mirrorErr == nil && mirrorSts.Status.ReadyReplicas == *mirrorSts.Spec.Replicas {
			cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
		} else {
			cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded
		}
	} else {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	}

	// Determine overall phase.
	if cluster.Status.CoordinatorReady && cluster.Status.SegmentsReady == cluster.Status.SegmentsTotal {
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionClusterReady),
			metav1.ConditionTrue,
			"AllComponentsReady",
			"All cluster components are running and healthy",
		)
	} else if cluster.Status.Phase != cbv1alpha1.ClusterPhaseDeleting {
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

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Named(clusterControllerName).
		Complete(r)
}
