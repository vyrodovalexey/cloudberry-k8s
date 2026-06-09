// Package controller implements the Kubernetes controllers for the cloudberry operator.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
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
	"log/slog"
	"net/url"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	clusterControllerName  = "cluster-controller"
	requeueAfterDefault    = 30 * time.Second
	requeueAfterError      = 10 * time.Second
	requeueAfterStopping   = 5 * time.Second
	scaleTimeout           = 10 * time.Minute
	upgradePhaseTimeout    = 10 * time.Minute
	mirroringEnableTimeout = 30 * time.Minute

	// patchKeyStatus is the JSON key used in MergePatch payloads for status subresource.
	patchKeyStatus = "status"

	// annotationValueTrue is the canonical string value for boolean-true annotations.
	annotationValueTrue = "true"

	// reconcileResultSuccess and reconcileResultError are the `result` label
	// values recorded for the cloudberry_reconcile_total / _errors_total /
	// _duration_seconds metrics across all controllers.
	reconcileResultSuccess = "success"
	reconcileResultError   = "error"
)

// recordReconcileOutcome records the reconcile outcome and duration for a
// controller in a single, nil-safe call. It is shared by the admin, HA and
// auth controllers (and mirrors recordReconcileResult used by the cluster
// controller) so that cloudberry_reconcile_total / _errors_total /
// _duration_seconds cover all four controllers. The result label is derived
// from err: "error" when non-nil, "success" otherwise. The recorder may be nil
// (e.g. in some test constructions), in which case this is a no-op.
func recordReconcileOutcome(
	rec metrics.Recorder,
	name, namespace string,
	startTime time.Time,
	err error,
) {
	if rec == nil {
		return
	}
	result := reconcileResultSuccess
	if err != nil {
		result = reconcileResultError
	}
	rec.RecordReconcile(name, namespace, result, time.Since(startTime))
}

// ClusterReconciler reconciles a CloudberryCluster object.
type ClusterReconciler struct {
	client    client.Client
	scheme    *runtime.Scheme
	recorder  record.EventRecorder
	builder   builder.ResourceBuilder
	metrics   metrics.Recorder
	dbFactory db.DBClientFactory
	logger    *slog.Logger
}

// NewClusterReconciler creates a new ClusterReconciler.
// The dbFactory parameter is optional (nil-safe) and is only used for
// mirroring initialization operations that require database connectivity.
func NewClusterReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	m metrics.Recorder,
	logger *slog.Logger,
	dbFactory ...db.DBClientFactory,
) *ClusterReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	var factory db.DBClientFactory
	if len(dbFactory) > 0 {
		factory = dbFactory[0]
	}
	return &ClusterReconciler{
		client:    c,
		scheme:    scheme,
		recorder:  recorder,
		builder:   b,
		metrics:   m,
		dbFactory: factory,
		logger:    logger.With("controller", clusterControllerName),
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
	logger.Debug("reconciliation details",
		"name", req.Name, "namespace", req.Namespace, "startTime", startTime)

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("cluster resource not found, likely deleted")
			return ctrl.Result{}, nil
		}
		wrapped := fmt.Errorf("fetching cluster: %w", err)
		telemetry.SetSpanError(span, wrapped)
		return ctrl.Result{}, wrapped
	}

	logger.Debug("cluster resource fetched",
		"phase", cluster.Status.Phase,
		"generation", cluster.Generation,
		"observedGeneration", cluster.Status.ObservedGeneration,
		"deletionTimestamp", cluster.DeletionTimestamp)

	// Handle annotation-based actions FIRST — annotations don't change the
	// generation, so they must be checked before the generation skip.
	if action, ok := cluster.Annotations[util.AnnotationAction]; ok {
		logger.Debug("handling action annotation", "action", action)
		result, err := r.handleAction(ctx, cluster, action)
		if err != nil {
			r.recordReconcileResult(cluster, startTime, "error")
			telemetry.SetSpanError(span, err)
			return result, err
		}
		return result, nil
	}

	// Check if the cluster is in a lifecycle phase that should short-circuit reconciliation.
	if result, handled := r.handleLifecyclePhase(ctx, cluster); handled {
		logger.Debug("lifecycle phase handled, short-circuiting", "phase", cluster.Status.Phase)
		return result, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches).
	// However, if a scale-state annotation is present, the scale operation is still in progress
	// and we must continue processing it even if the phase was reset to Running.
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Status.Phase == cbv1alpha1.ClusterPhaseRunning {
		if res := r.handleGenerationUnchanged(ctx, cluster); res.handled {
			return res.result, res.err
		}
	}

	// Handle deletion.
	if !cluster.DeletionTimestamp.IsZero() {
		logger.Debug("handling cluster deletion")
		return r.handleDeletion(ctx, cluster)
	}

	// Ensure finalizer is set.
	if !controllerutil.ContainsFinalizer(cluster, util.FinalizerName) {
		logger.Debug("adding finalizer to cluster")
		controllerutil.AddFinalizer(cluster, util.FinalizerName)
		if err := r.client.Update(ctx, cluster); err != nil {
			wrapped := fmt.Errorf("adding finalizer: %w", err)
			telemetry.SetSpanError(span, wrapped)
			return ctrl.Result{}, wrapped
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Main reconciliation flow.
	logger.Debug("entering main reconciliation flow")
	result, err := r.reconcileCluster(ctx, cluster)
	if err != nil {
		r.recordReconcileResult(cluster, startTime, "error")
		telemetry.SetSpanError(span, err)
		return result, err
	}

	r.recordReconcileResult(cluster, startTime, "success")
	logger.Info("reconciliation completed", "duration", time.Since(startTime))
	logger.Debug("reconciliation result", "requeue", result.RequeueAfter > 0, "requeueAfter", result.RequeueAfter)
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
		result, err := r.checkStopProgress(ctx, cluster)
		if err != nil {
			logger.Error("error checking stop progress", "error", err)
		}
		return result, true

	case cbv1alpha1.ClusterPhaseRestricted, cbv1alpha1.ClusterPhaseMaintenance:
		logger.Info("cluster in limited mode, skipping full reconciliation",
			"phase", cluster.Status.Phase)
		r.recordMetricsSnapshot(cluster)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, true

	case cbv1alpha1.ClusterPhaseScaling:
		result, err := r.checkScaleProgress(ctx, cluster)
		if err != nil {
			logger.Error("error checking scale progress", "error", err)
		}
		return result, true

	case cbv1alpha1.ClusterPhaseUpdating:
		// Check if this is a mirroring operation (vs upgrade).
		if cluster.Annotations[util.AnnotationMirroringState] != "" {
			result, err := r.checkMirroringProgress(ctx, cluster)
			if err != nil {
				logger.Error("error checking mirroring progress", "error", err)
			}
			return result, true
		}
		result, err := r.continueUpgrade(ctx, cluster)
		if err != nil {
			logger.Error("error continuing upgrade", "error", err)
		}
		return result, true

	default:
		return ctrl.Result{}, false
	}
}

// generationUnchangedResult holds the result of handleGenerationUnchanged.
type generationUnchangedResult struct {
	result  ctrl.Result
	err     error
	handled bool
}

// handleGenerationUnchanged handles the case where the cluster generation has not changed
// and the cluster is in Running phase. It checks for in-progress scale operations and
// confirm-scale-in annotations.
func (r *ClusterReconciler) handleGenerationUnchanged(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) generationUnchangedResult {
	logger := util.LoggerFromContext(ctx)

	_, hasScaleState := cluster.Annotations[annotationScaleState]
	_, hasScaleInState := cluster.Annotations[annotationScaleInState]
	if hasScaleState || hasScaleInState {
		// Scale operation in progress but phase was reset — restore Scaling phase and continue.
		logger.Info("scale state annotation found but phase is Running, restoring Scaling phase")
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return generationUnchangedResult{
				err: fmt.Errorf("restoring scaling phase: %w", err), handled: true,
			}
		}
		return generationUnchangedResult{result: ctrl.Result{Requeue: true}, handled: true}
	}
	// If confirm-scale-in annotation is present, a previously blocked scale-in
	// may now be ready to proceed. Force reconciliation to re-evaluate.
	if cluster.Annotations[util.AnnotationConfirmScaleIn] == annotationValueTrue {
		logger.Info("confirm-scale-in annotation detected, forcing reconciliation")
		return generationUnchangedResult{handled: false}
	}
	logger.Debug("skipping reconciliation, generation unchanged and cluster running",
		"generation", cluster.Generation)
	r.recordMetricsSnapshot(cluster)
	return generationUnchangedResult{
		result: ctrl.Result{RequeueAfter: requeueAfterDefault}, handled: true,
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

	// Reconcile core resources: admin Secret, ConfigMaps, Services, and
	// exporter prerequisites (must exist before StatefulSets reference them).
	if err := r.reconcileCoreResources(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, err
	}

	// Check for in-progress or needed upgrade before reconciling StatefulSets.
	if r.isUpgradeNeeded(cluster) {
		return r.handleUpgrade(ctx, cluster)
	}

	// If a previous upgrade failed and was rolled back, do NOT reconcile
	// StatefulSets — the spec still contains the broken image. The user must
	// fix the spec (set a valid image/version) or remove the UpgradeFailed
	// condition before the operator will update StatefulSets again.
	if r.isUpgradeRolledBack(cluster) {
		logger.Info("upgrade previously failed and rolled back, skipping STS reconciliation")
		if err := r.updateStatus(ctx, cluster); err != nil {
			return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("updating status: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Reconcile StatefulSets and storage.
	if err := r.reconcileStatefulSets(ctx, cluster); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, err
	}

	// Update status.
	if err := r.updateStatus(ctx, cluster); err != nil {
		logger.Error("failed to update status", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// reconcileCoreResources reconciles the core Kubernetes resources that must exist
// before StatefulSets are created: admin password Secret, ConfigMaps, Services,
// and exporter prerequisites (when query monitoring is enabled).
func (r *ClusterReconciler) reconcileCoreResources(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Reconcile admin password Secret (must exist before StatefulSets reference it).
	if err := r.reconcileAdminSecret(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling admin secret: %w", err)
	}

	// Reconcile the cluster-wide shared gpadmin SSH keypair Secret (must exist
	// before StatefulSets reference it as a volume). Every cluster pod and the
	// backup/restore Jobs mount this single identity so gpbackup/gprestore can
	// dispatch over SSH to all segments.
	if err := r.reconcileClusterSSHSecret(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling cluster ssh secret: %w", err)
	}

	// Reconcile ConfigMaps.
	if err := r.reconcileConfigMaps(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling configmaps: %w", err)
	}

	// Reconcile Services.
	if err := r.reconcileServices(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling services: %w", err)
	}

	// Ensure exporter resources exist before the coordinator StatefulSet
	// references them (Secret for DATA_SOURCE_NAME, ConfigMap for queries).
	if needsExporterPrerequisites(cluster) {
		if err := r.ensureExporterPrerequisites(ctx, cluster); err != nil {
			util.LoggerFromContext(ctx).Warn("failed to create exporter prerequisites", "error", err)
		}
	}

	return nil
}

// reconcileStatefulSets reconciles all StatefulSets (coordinator, standby,
// segments) and storage expansion.
func (r *ClusterReconciler) reconcileStatefulSets(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Reconcile coordinator StatefulSet.
	if err := r.reconcileCoordinator(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling coordinator: %w", err)
	}

	// Reconcile standby if enabled.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		if err := r.reconcileStandby(ctx, cluster); err != nil {
			return fmt.Errorf("reconciling standby: %w", err)
		}
	}

	// Reconcile segments.
	if err := r.reconcileSegments(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling segments: %w", err)
	}

	// Reconcile storage expansion (PVC resizing).
	if err := r.reconcileStorageExpansion(ctx, cluster); err != nil {
		return fmt.Errorf("reconciling storage expansion: %w", err)
	}

	return nil
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

// reconcileClusterSSHSecret ensures the cluster-wide shared gpadmin SSH keypair
// Secret exists. If it is absent the operator generates ONE ed25519 keypair and
// creates the Secret (private key, public key and authorized_keys all derived
// from that single key). If it already exists it is left unchanged so the shared
// identity is stable across reconciles and pod restarts.
//
// This shared identity is what makes cluster-wide passwordless SSH work:
// gpbackup/gprestore (MPP tools) dispatch over SSH from the coordinator to every
// segment, so all pods MUST trust the same key.
func (r *ClusterReconciler) reconcileClusterSSHSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	secretName := util.ClusterSSHSecretName(cluster.Name)
	existing := &corev1.Secret{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, existing)

	if err == nil {
		// Secret already exists — keep the stable shared identity.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting cluster ssh secret %s: %w", secretName, err)
	}

	privateKeyPEM, authorizedKey, genErr := builder.GenerateClusterSSHKeyPair()
	if genErr != nil {
		return fmt.Errorf("generating cluster ssh keypair: %w", genErr)
	}

	desired := r.builder.BuildClusterSSHSecret(cluster, privateKeyPEM, authorizedKey)
	if createErr := r.client.Create(ctx, desired); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			// Lost a race with a concurrent reconcile — the Secret now exists.
			return nil
		}
		return fmt.Errorf("creating cluster ssh secret %s: %w", secretName, createErr)
	}

	util.LoggerFromContext(ctx).Info("created cluster ssh secret", "name", secretName)
	r.recorder.Event(cluster, corev1.EventTypeNormal, "SecretCreated",
		fmt.Sprintf("Cluster SSH keypair secret %s created", secretName))

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

	// Check for scale-out/scale-in by comparing desired vs actual replicas.
	existingSts := &appsv1.StatefulSet{}
	getErr := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, existingSts)

	if handled, scaleErr := r.detectAndHandleScale(ctx, cluster, existingSts, getErr); handled {
		return scaleErr
	}

	// Check for mirroring enable transition.
	if r.isMirroringEnableNeeded(ctx, cluster) {
		return r.handleEnableMirroring(ctx, cluster)
	}
	// Check for mirroring disable transition.
	if r.isMirroringDisableNeeded(ctx, cluster) {
		return r.handleDisableMirroring(ctx, cluster)
	}

	// Normal reconciliation — primary segments.
	primarySts, err := r.builder.BuildSegmentPrimaryStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building primary segment StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	// Safety: never scale down replicas via the normal path — scale-in must go
	// through handleScaleIn to ensure data redistribution and segment deregistration.
	if getErr == nil && existingSts.Spec.Replicas != nil && primarySts.Spec.Replicas != nil {
		if *primarySts.Spec.Replicas < *existingSts.Spec.Replicas {
			logger.Info("scale-in required but not yet confirmed/processed, preserving current replicas",
				"current", *existingSts.Spec.Replicas, "desired", *primarySts.Spec.Replicas)
			primarySts.Spec.Replicas = existingSts.Spec.Replicas
		}
	}
	if err := r.createOrUpdateStatefulSet(ctx, primarySts); err != nil {
		return fmt.Errorf("primary segments: %w", err)
	}

	// Mirror segments.
	mirrorSts, err := r.builder.BuildSegmentMirrorStatefulSet(cluster)
	if err != nil {
		return fmt.Errorf("building mirror segment StatefulSet for cluster %s: %w", cluster.Name, err)
	}
	if mirrorSts == nil {
		return nil
	}
	r.preserveMirrorReplicasIfNeeded(ctx, mirrorSts, getErr)
	if err := r.createOrUpdateStatefulSet(ctx, mirrorSts); err != nil {
		return fmt.Errorf("mirror segments: %w", err)
	}

	return nil
}

// detectAndHandleScale checks for scale-out/scale-in by comparing desired vs actual replicas.
// Returns (true, err) if a scale operation was detected (caller should return err),
// or (false, nil) if no scale change was detected and normal reconciliation should continue.
func (r *ClusterReconciler) detectAndHandleScale(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	existingSts *appsv1.StatefulSet,
	getErr error,
) (bool, error) {
	logger := util.LoggerFromContext(ctx)

	if getErr != nil {
		logger.Debug("could not get existing primary StatefulSet for scale check", "error", getErr)
		return false, nil
	}
	if existingSts.Spec.Replicas == nil {
		return false, nil
	}

	currentCount := *existingSts.Spec.Replicas
	desiredCount := cluster.Spec.Segments.Count

	logger.Debug("segment scale check",
		"currentReplicas", currentCount, "desiredCount", desiredCount)

	// Only detect scale operations when the current count is > 0.
	// A currentCount of 0 indicates a restart or initial creation,
	// which should be handled by the normal reconciliation path.
	if currentCount > 0 && desiredCount > currentCount {
		logger.Info("scale-out detected in reconcileSegments",
			"from", currentCount, "to", desiredCount)
		return true, r.handleScaleOut(ctx, cluster, currentCount, desiredCount)
	}
	if currentCount > 0 && desiredCount < currentCount {
		logger.Info("scale-in detected in reconcileSegments",
			"from", currentCount, "to", desiredCount)
		return true, r.handleScaleIn(ctx, cluster, currentCount, desiredCount)
	}

	return false, nil
}

// preserveMirrorReplicasIfNeeded applies the safety check for mirrors: never scale down
// replicas via the normal path. Scale-in must go through handleScaleIn.
func (r *ClusterReconciler) preserveMirrorReplicasIfNeeded(
	ctx context.Context,
	mirrorSts *appsv1.StatefulSet,
	primaryGetErr error,
) {
	if primaryGetErr != nil {
		return
	}
	logger := util.LoggerFromContext(ctx)
	mirrorExisting := &appsv1.StatefulSet{}
	mirrorGetErr := r.client.Get(ctx, types.NamespacedName{
		Name:      mirrorSts.Name,
		Namespace: mirrorSts.Namespace,
	}, mirrorExisting)
	if mirrorGetErr != nil || mirrorExisting.Spec.Replicas == nil || mirrorSts.Spec.Replicas == nil {
		return
	}
	if *mirrorSts.Spec.Replicas < *mirrorExisting.Spec.Replicas {
		logger.Info("mirror scale-in required but not yet confirmed/processed, preserving current replicas",
			"current", *mirrorExisting.Spec.Replicas, "desired", *mirrorSts.Spec.Replicas)
		mirrorSts.Spec.Replicas = mirrorExisting.Spec.Replicas
	}
}

// handleScaleOut orchestrates a scale-out operation: transitions the cluster
// to the Scaling phase, updates StatefulSet replicas, and stores scale state
// for the multi-phase scale-out process (STS scaling → segment registration →
// data redistribution).
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

	// Store scale state for multi-phase operation.
	state := scaleStateData{
		Phase:     scalePhaseScalingSTS,
		OldCount:  oldCount,
		NewCount:  newCount,
		StartedAt: scaleStartTime,
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling scale state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		annotationScaleState, string(stateJSON)); err != nil {
		return fmt.Errorf("setting scale-state annotation: %w", err)
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

	// Set redistribution pending.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "InProgress",
		"Waiting for new segment pods to be ready")
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating redistribution status: %w", err)
	}

	return nil
}

// handleScaleIn orchestrates a scale-in operation using a multi-phase state machine:
//  1. Redistribute data OFF segments being removed
//  2. Deregister segments from gp_segment_configuration
//  3. Scale down mirror StatefulSet (if mirroring enabled)
//  4. Scale down primary StatefulSet
//  5. Clean up PVCs based on deletionPolicy
//  6. Complete: transition back to Running
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
		if cluster.Annotations[util.AnnotationConfirmScaleIn] != annotationValueTrue {
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

	// Store scale-in state for multi-phase operation.
	state := scaleInStateData{
		Phase:     scaleInPhaseRedistributing,
		OldCount:  oldCount,
		NewCount:  newCount,
		StartedAt: scaleStartTime,
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling scale-in state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		annotationScaleInState, string(stateJSON)); err != nil {
		return fmt.Errorf("setting scale-in-state annotation: %w", err)
	}

	// Set phase to Scaling.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "ScaleInRedistributing",
		fmt.Sprintf("Redistributing data off segments %d-%d before scale-in", newCount, oldCount-1))
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("setting scaling phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonScaleInStarted,
		fmt.Sprintf("Scale-in from %d to %d segments initiated — redistributing data", oldCount, newCount))

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
		r.recordPVCSize(cluster, "coordinator", desiredSize)
	}
	return changed, nil
}

// recordPVCSize parses the desired PVC size quantity to bytes and records it
// on the pvc_size_bytes gauge for the given component. Parsing failures are
// ignored since the size was already validated during expansion.
func (r *ClusterReconciler) recordPVCSize(
	cluster *cbv1alpha1.CloudberryCluster,
	component, size string,
) {
	q, err := resource.ParseQuantity(size)
	if err != nil {
		return
	}
	r.metrics.SetPVCSizeBytes(cluster.Name, cluster.Namespace, component, float64(q.Value()))
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
		r.recordPVCSize(cluster, "standby", desiredSize)
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

	if expanded {
		r.recordPVCSize(cluster, "segment", segmentSize)
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
// The annotation is removed AFTER successful processing so that failed actions
// are retried on the next reconciliation.
func (r *ClusterReconciler) handleAction(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	action string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling action", "action", action)

	// Process the action first — annotation removal happens only on success.
	var result ctrl.Result
	var actionErr error

	switch action {
	case util.ActionStart, util.ActionStartRestricted, util.ActionStartMaintenance:
		result, actionErr = r.handleStart(ctx, cluster, action)
	case util.ActionStop, util.ActionStopFast, util.ActionStopImmediate:
		result, actionErr = r.handleStop(ctx, cluster, action)
	case util.ActionRestart:
		result, actionErr = r.handleRestart(ctx, cluster)
	default:
		logger.Warn("unknown action", "action", action)
		// Unknown actions are removed to prevent infinite reconciliation loops.
	}

	if actionErr != nil {
		// Action failed — leave the annotation in place for retry on next reconcile.
		logger.Error("action processing failed, annotation retained for retry",
			"action", action, "error", actionErr)
		return result, actionErr
	}

	// Action succeeded — remove the annotation using a MergePatch to avoid conflicts.
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
		logger.Error("failed to remove action annotation after successful processing", "error", patchErr)
		return ctrl.Result{}, fmt.Errorf("removing action annotation: %w", patchErr)
	}

	// Re-fetch the cluster to get the latest state after the patch.
	if fetchErr := r.client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, cluster); fetchErr != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching cluster after annotation removal: %w", fetchErr)
	}

	return result, nil
}

// handleStop processes stop actions (stop, stop-fast, stop-immediate).
// It performs mode-specific graceful shutdown via the database client,
// sets the phase to Stopping, scales all StatefulSets to 0, and transitions
// to Stopped once all pods are terminated.
//
// Stop modes:
//   - stop (smart): Cancel active queries, wait briefly for clients to disconnect,
//     then terminate remaining backends before scaling down.
//   - stop-fast: Immediately terminate all backends (rollback active transactions),
//     then scale down.
//   - stop-immediate: Scale to 0 without any DB-level graceful shutdown.
func (r *ClusterReconciler) handleStop(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	mode string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("stopping cluster", "mode", mode)

	// Set phase to Stopping using status patch to avoid conflicts.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopping phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopping",
		fmt.Sprintf("Cluster stop initiated (mode: %s)", mode))

	// Perform DB-level graceful shutdown based on mode.
	// The stop-immediate mode skips DB-level operations entirely.
	if mode != util.ActionStopImmediate {
		r.performGracefulShutdown(ctx, cluster, mode)
	}

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

	// All stopped — update status using an explicit-value patch to avoid conflicts
	// and to actually persist the cleared readiness fields (omitempty would
	// otherwise drop the zero values from a generic MergePatch).
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := patchClearReadinessStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopped phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopped",
		fmt.Sprintf("Cluster stopped (mode: %s)", mode))
	return ctrl.Result{}, nil
}

// performGracefulShutdown executes DB-level shutdown operations based on the stop mode.
// For smart stop: cancels active queries first, then terminates remaining backends.
// For fast stop: immediately terminates all backends.
// Errors are logged but do not block the scale-down — the cluster will still stop
// even if the DB client is unreachable (e.g., coordinator already unhealthy).
func (r *ClusterReconciler) performGracefulShutdown(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	mode string,
) {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping DB-level graceful shutdown")
		return
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		// DB client creation may fail if the coordinator is already unhealthy.
		// Log and continue — the scale-down will still proceed.
		logger.Warn("failed to create DB client for graceful shutdown, proceeding with scale-down",
			"error", err)
		return
	}
	defer dbClient.Close()

	switch mode {
	case util.ActionStop:
		// Smart stop: cancel active queries first, then terminate remaining backends.
		canceled, cancelErr := dbClient.CancelAllQueries(ctx)
		if cancelErr != nil {
			logger.Warn("failed to cancel active queries during smart stop", "error", cancelErr)
		} else {
			logger.Info("canceled active queries for smart stop", "count", canceled)
		}

		// Terminate remaining backends after cancellation.
		terminated, termErr := dbClient.TerminateAllBackends(ctx)
		if termErr != nil {
			logger.Warn("failed to terminate backends during smart stop", "error", termErr)
		} else {
			logger.Info("terminated backends for smart stop", "count", terminated)
		}

	case util.ActionStopFast:
		// Fast stop: immediately terminate all backends.
		terminated, termErr := dbClient.TerminateAllBackends(ctx)
		if termErr != nil {
			logger.Warn("failed to terminate backends during fast stop", "error", termErr)
		} else {
			logger.Info("terminated backends for fast stop", "count", terminated)
		}
	}
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
		// Restricted mode: start all components but only allow superuser connections.
		// Scale up in order: coordinator, standby, primaries, mirrors.
		for _, name := range r.getScaleUpOrder(cluster) {
			replicas := r.getDesiredReplicas(cluster, name)
			if err := r.scaleStatefulSet(ctx, cluster.Namespace, name, replicas); err != nil {
				return ctrl.Result{}, fmt.Errorf("scaling %s for restricted start: %w", name, err)
			}
		}
		// Use status patch to avoid conflicts with concurrent metadata changes.
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseRestricted
		if err := patchStatus(ctx, r.client, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting restricted phase: %w", err)
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Started",
			"Cluster started in restricted mode (superuser connections only)")
		return ctrl.Result{}, nil

	case util.ActionStartMaintenance:
		// Only coordinator in utility mode.
		coordName := util.CoordinatorName(cluster.Name)
		if err := r.scaleStatefulSet(ctx, cluster.Namespace, coordName, 1); err != nil {
			return ctrl.Result{}, fmt.Errorf("scaling coordinator for maintenance start: %w", err)
		}
		// Use status patch to avoid conflicts with concurrent metadata changes.
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseMaintenance
		if err := patchStatus(ctx, r.client, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting maintenance phase: %w", err)
		}
		r.recorder.Event(cluster, corev1.EventTypeNormal, "Started",
			"Cluster started in maintenance mode")
		return ctrl.Result{}, nil

	default:
		// Normal start — reset phase so updateStatus can transition to Running,
		// then scale everything back via full reconciliation.
		// Use status patch to avoid conflicts with concurrent metadata changes.
		cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
		if err := patchStatus(ctx, r.client, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("resetting phase for normal start: %w", err)
		}
		// Re-fetch to get the latest state after the status patch for reconciliation.
		if err := r.client.Get(ctx, types.NamespacedName{
			Name: cluster.Name, Namespace: cluster.Namespace,
		}, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-fetching cluster for start: %w", err)
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
	if err := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationRestartPending, annotationValueTrue); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting restart-pending annotation: %w", err)
	}

	// Set phase to Stopping using status patch to avoid conflicts.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopping
	if err := patchStatus(ctx, r.client, cluster); err != nil {
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

	// Reset phase to Initializing using an explicit-value status patch to avoid
	// conflicts and to actually persist the cleared readiness fields (omitempty
	// would otherwise drop the zero values from a generic MergePatch).
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := patchClearReadinessStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("resetting phase for restart: %w", err)
	}

	// Re-fetch to get the latest state after the status patch for reconciliation.
	if err := r.client.Get(ctx, types.NamespacedName{
		Name: cluster.Name, Namespace: cluster.Namespace,
	}, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching cluster for restart: %w", err)
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

	// All stopped — use an explicit-value status patch to avoid conflicts and to
	// actually persist the cleared readiness fields (omitempty would otherwise drop
	// the zero values from a generic MergePatch).
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	cluster.Status.CoordinatorReady = false
	cluster.Status.StandbyReady = false
	cluster.Status.SegmentsReady = 0
	if err := patchClearReadinessStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting stopped phase: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "Stopped", "Cluster stopped")
	return ctrl.Result{}, nil
}

// checkScaleProgress checks whether a cluster in Scaling phase has completed
// the scale operation (scale-out or scale-in). For scale-out, it follows a
// multi-phase process: wait for pods → register segments → redistribute data.
// For scale-in, it follows: redistribute → deregister → scale mirrors → scale primaries → cleanup.
// When all phases complete, it transitions the cluster back to Running.
func (r *ClusterReconciler) checkScaleProgress(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("checking scale progress")

	// Check for timeout — if scaling has been in progress too long, mark as failed.
	startedStr := cluster.Annotations[util.AnnotationScaleStarted]
	if startedStr != "" {
		started, err := time.Parse(time.RFC3339, startedStr)
		if err == nil && time.Since(started) > scaleTimeout {
			return r.handleScaleFailure(ctx, cluster)
		}
	}

	// Check if this is a multi-phase scale-in with state annotation.
	scaleInJSON := cluster.Annotations[annotationScaleInState]
	if scaleInJSON != "" {
		return r.checkScaleInPhases(ctx, cluster, scaleInJSON)
	}

	// Check if this is a multi-phase scale-out with state annotation.
	stateJSON := cluster.Annotations[annotationScaleState]
	if stateJSON != "" {
		return r.checkScaleOutPhases(ctx, cluster, stateJSON)
	}

	// Fallback: simple scale check (for legacy scale operations without state).
	if r.allSegmentStatefulSetsReady(ctx, cluster) {
		return r.completeScaleOperation(ctx, cluster)
	}

	return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
}

// checkScaleOutPhases processes the multi-phase scale-out operation.
func (r *ClusterReconciler) checkScaleOutPhases(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	stateJSON string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	var state scaleStateData
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		logger.Error("failed to parse scale state, falling back to simple check", "error", err)
		if r.allSegmentStatefulSetsReady(ctx, cluster) {
			return r.completeScaleOperation(ctx, cluster)
		}
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	logger.Info("scale-out phase check", "phase", state.Phase,
		"oldCount", state.OldCount, "newCount", state.NewCount)

	switch state.Phase {
	case scalePhaseScalingSTS:
		// Wait for all segment StatefulSets to be ready.
		if !r.allSegmentStatefulSetsReady(ctx, cluster) {
			return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
		}
		// Advance to registering phase.
		logger.Info("all segment pods ready, advancing to segment registration")
		return r.advanceScalePhase(ctx, cluster, &state, scalePhaseRegistering)

	case scalePhaseRegistering:
		// Register new segments in gp_segment_configuration.
		if err := r.registerNewSegments(ctx, cluster, state); err != nil {
			logger.Error("segment registration failed", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, nil
		}
		// Advance to redistribution phase.
		logger.Info("segment registration completed, advancing to redistribution")
		return r.advanceScalePhase(ctx, cluster, &state, scalePhaseRedistributing)

	case scalePhaseRedistributing:
		// Run data redistribution.
		if err := r.redistributeData(ctx, cluster); err != nil {
			logger.Error("data redistribution failed", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, nil
		}
		// Advance to completed.
		logger.Info("data redistribution completed")
		return r.advanceScalePhase(ctx, cluster, &state, scalePhaseCompleted)

	case scalePhaseCompleted:
		// Remove scale state annotation and complete.
		if err := removeAnnotationPatch(ctx, r.client, cluster, annotationScaleState); err != nil {
			logger.Error("failed to remove scale-state annotation", "error", err)
		}
		return r.completeScaleOperation(ctx, cluster)

	default:
		logger.Warn("unknown scale phase, completing", "phase", state.Phase)
		if err := removeAnnotationPatch(ctx, r.client, cluster, annotationScaleState); err != nil {
			logger.Error("failed to remove scale-state annotation", "error", err)
		}
		return r.completeScaleOperation(ctx, cluster)
	}
}

// advanceScalePhase updates the scale state to the next phase.
func (r *ClusterReconciler) advanceScalePhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleStateData,
	nextPhase string,
) (ctrl.Result, error) {
	state.Phase = nextPhase

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling scale state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		annotationScaleState, string(stateJSON)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating scale-state annotation: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// checkScaleInPhases processes the multi-phase scale-in operation.
func (r *ClusterReconciler) checkScaleInPhases(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	stateJSON string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	var state scaleInStateData
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		logger.Error("failed to parse scale-in state, falling back to simple check", "error", err)
		if r.allSegmentStatefulSetsReady(ctx, cluster) {
			return r.completeScaleOperation(ctx, cluster)
		}
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}

	logger.Info("scale-in phase check", "phase", state.Phase,
		"oldCount", state.OldCount, "newCount", state.NewCount)

	switch state.Phase {
	case scaleInPhaseRedistributing:
		return r.processScaleInRedistributing(ctx, cluster, &state)
	case scaleInPhaseDeregistering:
		return r.processScaleInDeregistering(ctx, cluster, &state)
	case scaleInPhaseScalingMirrors:
		return r.processScaleInScalingMirrors(ctx, cluster, &state)
	case scaleInPhaseScalingPrimaries:
		return r.processScaleInScalingPrimaries(ctx, cluster, &state)
	case scaleInPhaseCleanup:
		return r.processScaleInCleanup(ctx, cluster, &state)
	case scaleInPhaseCompleted:
		return r.processScaleInCompleted(ctx, cluster)
	default:
		logger.Warn("unknown scale-in phase, completing", "phase", state.Phase)
		return r.processScaleInCompleted(ctx, cluster)
	}
}

// processScaleInRedistributing handles the redistribution phase of scale-in.
func (r *ClusterReconciler) processScaleInRedistributing(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	if err := r.redistributeBeforeScaleIn(ctx, cluster, *state); err != nil {
		logger.Error("scale-in redistribution failed, will retry", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil //nolint:nilerr // retry on next reconcile
	}
	logger.Info("scale-in redistribution completed, advancing to deregistration")
	return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseDeregistering)
}

// processScaleInDeregistering handles the segment deregistration phase of scale-in.
func (r *ClusterReconciler) processScaleInDeregistering(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	if err := r.deregisterSegments(ctx, cluster, *state); err != nil {
		logger.Error("segment deregistration failed, will retry", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil //nolint:nilerr // retry on next reconcile
	}
	logger.Info("segment deregistration completed, advancing to scale-down")
	// If mirroring is enabled, scale mirrors first; otherwise skip to primaries.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseScalingMirrors)
	}
	return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseScalingPrimaries)
}

// processScaleInScalingMirrors handles the mirror scale-down phase of scale-in.
func (r *ClusterReconciler) processScaleInScalingMirrors(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	mirrorName := util.SegmentMirrorName(cluster.Name)
	if err := r.scaleStatefulSet(ctx, cluster.Namespace, mirrorName, state.NewCount); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("scaling down mirrors: %w", err)
	}
	ready, _ := r.isStatefulSetAtScale(ctx, cluster.Namespace, mirrorName, state.NewCount)
	if !ready {
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}
	logger.Info("mirror scale-down completed, advancing to primary scale-down")
	return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseScalingPrimaries)
}

// processScaleInScalingPrimaries handles the primary scale-down phase of scale-in.
func (r *ClusterReconciler) processScaleInScalingPrimaries(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	primaryName := util.SegmentPrimaryName(cluster.Name)
	if err := r.scaleStatefulSet(ctx, cluster.Namespace, primaryName, state.NewCount); err != nil {
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("scaling down primaries: %w", err)
	}
	ready, _ := r.isStatefulSetAtScale(ctx, cluster.Namespace, primaryName, state.NewCount)
	if !ready {
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil
	}
	logger.Info("primary scale-down completed, advancing to cleanup")
	return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseCleanup)
}

// processScaleInCleanup handles the PVC cleanup phase of scale-in.
func (r *ClusterReconciler) processScaleInCleanup(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	if cluster.Spec.DeletionPolicy == cbv1alpha1.DeletionPolicyDelete {
		r.cleanupOrphanedPVCs(ctx, cluster, state.NewCount)
	}
	logger.Info("cleanup completed, advancing to completed")
	return r.advanceScaleInPhase(ctx, cluster, state, scaleInPhaseCompleted)
}

// processScaleInCompleted handles the final phase of scale-in.
func (r *ClusterReconciler) processScaleInCompleted(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	if err := removeAnnotationPatch(ctx, r.client, cluster, annotationScaleInState); err != nil {
		logger.Error("failed to remove scale-in-state annotation", "error", err)
	}
	return r.completeScaleOperation(ctx, cluster)
}

// advanceScaleInPhase updates the scale-in state to the next phase.
func (r *ClusterReconciler) advanceScaleInPhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *scaleInStateData,
	nextPhase string,
) (ctrl.Result, error) {
	state.Phase = nextPhase

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling scale-in state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		annotationScaleInState, string(stateJSON)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating scale-in-state annotation: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// redistributeBeforeScaleIn calls the DB client to redistribute data off segments
// being removed. If no dbFactory is configured, it logs a warning and returns nil.
func (r *ClusterReconciler) redistributeBeforeScaleIn(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state scaleInStateData,
) error {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping scale-in redistribution")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for scale-in redistribution: %w", err)
	}
	defer dbClient.Close()

	if redistErr := dbClient.RedistributeBeforeScaleIn(ctx, db.ScaleInRedistributionOptions{
		NewCount: state.NewCount,
	}); redistErr != nil {
		return fmt.Errorf("redistributing data before scale-in: %w", redistErr)
	}

	// Update condition to reflect redistribution completion.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "ScaleInRedistributed",
		"Data redistributed off segments being removed")
	if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
		logger.Error("failed to update redistribution status", "error", statusErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "ScaleInRedistributed",
		fmt.Sprintf("Data redistributed off segments %d-%d", state.NewCount, state.OldCount-1))

	logger.Info("scale-in redistribution completed",
		"oldCount", state.OldCount, "newCount", state.NewCount)
	return nil
}

// deregisterSegments calls the DB client to remove segment entries from
// gp_segment_configuration for segments being removed during scale-in.
// If no dbFactory is configured, it logs a warning and returns nil.
func (r *ClusterReconciler) deregisterSegments(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state scaleInStateData,
) error {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping segment deregistration")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for segment deregistration: %w", err)
	}
	defer dbClient.Close()

	if deregErr := dbClient.DeregisterSegments(ctx, state.NewCount); deregErr != nil {
		return fmt.Errorf("deregistering segments: %w", deregErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "SegmentsDeregistered",
		fmt.Sprintf("Deregistered segments with content >= %d from gp_segment_configuration",
			state.NewCount))

	logger.Info("segments deregistered successfully",
		"removedContentIDs", fmt.Sprintf("%d-%d", state.NewCount, state.OldCount-1))
	return nil
}

// registerNewSegments calls the DB client to register new segments in gp_segment_configuration.
// If no dbFactory is configured, it logs a warning and returns nil.
func (r *ClusterReconciler) registerNewSegments(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state scaleStateData,
) error {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping segment registration")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for segment registration: %w", err)
	}
	defer dbClient.Close()

	port := cluster.Spec.Coordinator.Port
	if port == 0 {
		port = int32(util.DefaultCoordinatorPort)
	}

	segmentSvc := util.SegmentServiceName(cluster.Name)

	mirrorEnabled := cluster.Spec.Segments.Mirroring != nil &&
		cluster.Spec.Segments.Mirroring.Enabled

	if regErr := dbClient.RegisterNewSegments(ctx, db.SegmentRegistrationOptions{
		OldCount:       state.OldCount,
		NewCount:       state.NewCount,
		MirrorEnabled:  mirrorEnabled,
		SegmentService: segmentSvc,
		ClusterName:    cluster.Name,
		Port:           port,
	}); regErr != nil {
		return fmt.Errorf("registering new segments: %w", regErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "SegmentsRegistered",
		fmt.Sprintf("Registered %d new segments in gp_segment_configuration",
			state.NewCount-state.OldCount))

	logger.Info("new segments registered successfully",
		"oldCount", state.OldCount, "newCount", state.NewCount)
	return nil
}

// redistributeData calls the DB client to redistribute data across all segments.
// If no dbFactory is configured, it logs a warning and returns nil.
func (r *ClusterReconciler) redistributeData(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping data redistribution")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for redistribution: %w", err)
	}
	defer dbClient.Close()

	if redistErr := dbClient.RedistributeData(ctx, db.RedistributionOptions{
		Database:    "postgres",
		Parallelism: 2,
	}); redistErr != nil {
		return fmt.Errorf("redistributing data: %w", redistErr)
	}

	// Best-effort progress reporting. GetRedistributionProgress returns a
	// percentage (0-100); convert to a 0.0..1.0 ratio for the gauge. On a
	// query error we fall back to 1.0 since RedistributeData has completed.
	progress := 1.0
	if pct, progErr := dbClient.GetRedistributionProgress(ctx); progErr != nil {
		logger.Warn("failed to query redistribution progress", "error", progErr)
	} else if pct < 100 {
		progress = float64(pct) / 100.0
	}
	r.metrics.SetRedistributionProgress(cluster.Name, cluster.Namespace, progress)

	// Update condition to reflect redistribution completion.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataRedistribution), metav1.ConditionTrue, "Completed",
		"Data redistribution completed across all segments")
	if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
		logger.Error("failed to update redistribution status", "error", statusErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, "RedistributionCompleted",
		"Data redistribution completed across all segments")

	logger.Info("data redistribution completed")
	return nil
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
	if isScaleIn && cluster.Annotations[util.AnnotationConfirmScaleIn] == annotationValueTrue {
		if err := removeAnnotationPatch(ctx, r.client, cluster,
			util.AnnotationConfirmScaleIn); err != nil {
			logger.Error("failed to remove confirm-scale-in annotation", "error", err)
		}
	}

	// Clean up scale-in state annotation if still present.
	if _, hasScaleInState := cluster.Annotations[annotationScaleInState]; hasScaleInState {
		if err := removeAnnotationPatch(ctx, r.client, cluster,
			annotationScaleInState); err != nil {
			logger.Error("failed to remove scale-in-state annotation", "error", err)
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
	r.metrics.RecordScaleOperation(cluster.Name, cluster.Namespace, "scale-out-failed")

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

// getScaleUpOrder returns the StatefulSet names in the order they should be
// scaled up: coordinator first, then standby, then primaries, then mirrors.
func (r *ClusterReconciler) getScaleUpOrder(
	cluster *cbv1alpha1.CloudberryCluster,
) []string {
	var names []string

	// Coordinator first.
	names = append(names, util.CoordinatorName(cluster.Name))

	// Standby coordinator.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		names = append(names, util.StandbyName(cluster.Name))
	}

	// Primary segments.
	names = append(names, util.SegmentPrimaryName(cluster.Name))

	// Mirrors last.
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		names = append(names, util.SegmentMirrorName(cluster.Name))
	}

	return names
}

// getDesiredReplicas returns the desired replica count for a given StatefulSet name.
func (r *ClusterReconciler) getDesiredReplicas(
	cluster *cbv1alpha1.CloudberryCluster,
	stsName string,
) int32 {
	switch stsName {
	case util.CoordinatorName(cluster.Name):
		return 1
	case util.StandbyName(cluster.Name):
		return 1
	case util.SegmentPrimaryName(cluster.Name):
		return cluster.Spec.Segments.Count
	case util.SegmentMirrorName(cluster.Name):
		return cluster.Spec.Segments.Count
	default:
		return 1
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
	// Only update ClusterVersion if no upgrade is pending. During an upgrade,
	// ClusterVersion is updated by completeUpgrade after verification passes.
	// Setting it prematurely would defeat the upgrade detection logic.
	if !r.isUpgradeNeeded(cluster) {
		cluster.Status.ClusterVersion = cluster.Spec.Version
	}
	cluster.Status.SegmentsTotal = cluster.Spec.Segments.Count

	r.updateObservedGeneration(ctx, cluster)
	r.updateComponentReadiness(ctx, cluster)
	r.updateMirroringStatus(ctx, cluster)
	r.determineOverallPhase(cluster)

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

// updateObservedGeneration advances ObservedGeneration unless a blocked scale-in is pending.
// A blocked scale-in occurs when the desired segment count is less than the actual
// StatefulSet replicas but the confirmation annotation is missing.
func (r *ClusterReconciler) updateObservedGeneration(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)
	primaryStsForGen := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primaryStsForGen); err == nil && primaryStsForGen.Spec.Replicas != nil {
		currentReplicas := *primaryStsForGen.Spec.Replicas
		desiredCount := cluster.Spec.Segments.Count
		if desiredCount < currentReplicas {
			_, hasScaleInState := cluster.Annotations[annotationScaleInState]
			if !hasScaleInState {
				logger.Debug("not advancing ObservedGeneration due to pending blocked scale-in",
					"currentReplicas", currentReplicas, "desiredCount", desiredCount)
				return
			}
		}
	}
	cluster.Status.ObservedGeneration = cluster.Generation
}

// updateComponentReadiness checks the readiness of coordinator, standby, and segment StatefulSets.
func (r *ClusterReconciler) updateComponentReadiness(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	// Check coordinator readiness.
	coordSts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.CoordinatorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, coordSts); err == nil {
		cluster.Status.CoordinatorReady = coordSts.Status.ReadyReplicas > 0
	}

	// Check standby readiness.
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		standbySts := &appsv1.StatefulSet{}
		if err := r.client.Get(ctx, types.NamespacedName{
			Name:      util.StandbyName(cluster.Name),
			Namespace: cluster.Namespace,
		}, standbySts); err == nil {
			cluster.Status.StandbyReady = standbySts.Status.ReadyReplicas > 0
		}
	}

	// Check segment readiness.
	primarySts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primarySts); err == nil {
		cluster.Status.SegmentsReady = primarySts.Status.ReadyReplicas
	}
}

// updateMirroringStatus determines the mirroring status based on the mirror StatefulSet state.
func (r *ClusterReconciler) updateMirroringStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	if cluster.Spec.Segments.Mirroring == nil || !cluster.Spec.Segments.Mirroring.Enabled {
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
		return
	}
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
}

// determineOverallPhase sets the cluster phase based on component readiness.
// Intentional lifecycle phases (Deleting, Stopped, etc.) are preserved.
func (r *ClusterReconciler) determineOverallPhase(cluster *cbv1alpha1.CloudberryCluster) {
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

// scaleStateData holds the state of an in-progress scale-out operation.
type scaleStateData struct {
	// Phase is the current scale operation phase.
	Phase string `json:"phase"` // "scaling-sts", "registering", "redistributing", "completed"
	// OldCount is the previous segment count.
	OldCount int32 `json:"oldCount"`
	// NewCount is the new segment count.
	NewCount int32 `json:"newCount"`
	// StartedAt is the timestamp when the operation started.
	StartedAt string `json:"startedAt"`
}

// Scale-out phase constants define the ordered phases of a scale-out operation.
const (
	scalePhaseScalingSTS     = "scaling-sts"
	scalePhaseRegistering    = "registering"
	scalePhaseRedistributing = "redistributing"
	scalePhaseCompleted      = "completed"

	// AnnotationScaleState tracks in-progress scale-out state as JSON.
	annotationScaleState = "avsoft.io/scale-state"
)

// Scale-in phase constants define the ordered phases of a scale-in operation.
const (
	scaleInPhaseRedistributing   = "redistributing"
	scaleInPhaseDeregistering    = "deregistering"
	scaleInPhaseScalingMirrors   = "scaling-mirrors"
	scaleInPhaseScalingPrimaries = "scaling-primaries"
	scaleInPhaseCleanup          = "cleanup"
	scaleInPhaseCompleted        = "completed"

	// annotationScaleInState tracks in-progress scale-in state as JSON.
	annotationScaleInState = "avsoft.io/scale-in-state"
)

// scaleInStateData holds the state of an in-progress scale-in operation.
type scaleInStateData struct {
	// Phase is the current scale-in operation phase.
	Phase string `json:"phase"`
	// OldCount is the previous segment count.
	OldCount int32 `json:"oldCount"`
	// NewCount is the new (target) segment count.
	NewCount int32 `json:"newCount"`
	// StartedAt is the timestamp when the operation started.
	StartedAt string `json:"startedAt"`
}

// mirroringStateData holds the state of an in-progress mirroring enable/disable operation.
type mirroringStateData struct {
	// Phase is the current mirroring operation phase.
	Phase string `json:"phase"` // "creating-sts", "initializing", "syncing", "completed"
	// StartedAt is the timestamp when the operation started.
	StartedAt string `json:"startedAt"`
	// PhaseStartedAt is the timestamp when the current phase started.
	PhaseStartedAt string `json:"phaseStartedAt"`
	// Layout is the mirroring layout being configured.
	Layout string `json:"layout"`
}

// Mirroring phase constants define the ordered phases of a mirroring enable operation.
const (
	mirroringPhaseCreatingSTS = "creating-sts"
	mirroringPhaseInit        = "initializing"
	mirroringPhaseSyncing     = "syncing"
	mirroringPhaseCompleted   = "completed"
)

// isMirroringEnableNeeded detects whether mirroring needs to be enabled as a
// day-2 operation on an existing running cluster. It only triggers when the
// cluster is in Running phase with MirroringNotConfigured status, meaning the
// cluster was initially created without mirroring and the user is now enabling it.
// During initial cluster creation, the normal reconcileSegments path handles
// creating the mirror StatefulSet.
func (r *ClusterReconciler) isMirroringEnableNeeded(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) bool {
	// Mirroring must be requested in spec.
	if cluster.Spec.Segments.Mirroring == nil || !cluster.Spec.Segments.Mirroring.Enabled {
		return false
	}

	// Only trigger for day-2 enable on a Running cluster.
	// During initial creation (Initializing/Pending), the normal path handles it.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		return false
	}

	// Status must indicate mirroring is not yet configured.
	if cluster.Status.MirroringStatus != cbv1alpha1.MirroringNotConfigured {
		return false
	}

	// Mirror StatefulSet must NOT exist.
	mirrorSts := &appsv1.StatefulSet{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentMirrorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, mirrorSts)

	// If the STS exists, mirroring is already being set up or is configured.
	return apierrors.IsNotFound(err)
}

// isMirroringDisableNeeded detects whether mirroring needs to be disabled as a
// day-2 operation. It only triggers when the cluster is in Running phase with
// mirroring currently configured but the spec requests it disabled.
func (r *ClusterReconciler) isMirroringDisableNeeded(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) bool {
	// Mirroring must be disabled in spec (nil or enabled=false).
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		return false
	}

	// Only trigger for day-2 disable on a Running cluster.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		return false
	}

	// Status must indicate mirroring is currently configured.
	if cluster.Status.MirroringStatus == cbv1alpha1.MirroringNotConfigured ||
		cluster.Status.MirroringStatus == "" {
		return false
	}

	// Mirror StatefulSet must exist.
	mirrorSts := &appsv1.StatefulSet{}
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      util.SegmentMirrorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, mirrorSts)

	return err == nil
}

// handleEnableMirroring orchestrates enabling mirroring on an existing unmirrored cluster.
// It validates prerequisites, transitions the cluster to Updating phase, creates the
// mirror StatefulSet, and requeues for progress monitoring.
func (r *ClusterReconciler) handleEnableMirroring(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	// Phase 1: Pre-flight validation.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Warn("mirroring enable blocked: cluster not in Running phase",
			"currentPhase", cluster.Status.Phase)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMirroringFailed,
			fmt.Sprintf("Mirroring enable blocked: cluster is in %s phase, must be Running",
				cluster.Status.Phase))
		return nil
	}

	// Validate node count for layout.
	layout := cluster.Spec.Segments.Mirroring.Layout
	if layout == "" {
		layout = cbv1alpha1.MirroringLayoutGroup
	}
	primariesPerHost := cluster.Spec.Segments.PrimariesPerHost
	if primariesPerHost == 0 {
		primariesPerHost = 2
	}

	if !r.validateMirroringNodeCount(cluster, layout, primariesPerHost) {
		logger.Warn("mirroring enable blocked: insufficient segments for layout",
			"layout", layout, "segmentCount", cluster.Spec.Segments.Count,
			"primariesPerHost", primariesPerHost)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMirroringFailed,
			fmt.Sprintf("Mirroring enable blocked: insufficient segments (%d) for %s layout",
				cluster.Spec.Segments.Count, layout))
		return nil
	}

	logger.Info("enabling mirroring", "layout", layout,
		"segmentCount", cluster.Spec.Segments.Count)

	// Phase 2: Transition to Updating phase.
	now := time.Now().Format(time.RFC3339)
	state := mirroringStateData{
		Phase:          mirroringPhaseCreatingSTS,
		StartedAt:      now,
		PhaseStartedAt: now,
		Layout:         string(layout),
	}

	cluster.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringInitializing
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionMirroringHealthy), metav1.ConditionFalse,
		"MirroringInitializing", "Mirror initialization in progress")
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("setting updating phase for mirroring: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMirroringEnabled,
		fmt.Sprintf("Mirroring enable initiated with %s layout", layout))

	// Save mirroring state annotation.
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling mirroring state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationMirroringState, string(stateJSON)); err != nil {
		return fmt.Errorf("setting mirroring state annotation: %w", err)
	}

	// Phase 3: Create mirror StatefulSet.
	mirrorSts, buildErr := r.builder.BuildSegmentMirrorStatefulSet(cluster)
	if buildErr != nil {
		return fmt.Errorf("building mirror StatefulSet for cluster %s: %w", cluster.Name, buildErr)
	}
	if mirrorSts != nil {
		if createErr := r.createOrUpdateStatefulSet(ctx, mirrorSts); createErr != nil {
			return fmt.Errorf("creating mirror StatefulSet: %w", createErr)
		}
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMirroringInitializing,
		"Mirror StatefulSet created, waiting for pods to be ready")

	// Requeue to check progress.
	return nil
}

// validateMirroringNodeCount checks whether the segment count is sufficient
// for the chosen mirroring layout.
func (r *ClusterReconciler) validateMirroringNodeCount(
	cluster *cbv1alpha1.CloudberryCluster,
	layout cbv1alpha1.MirroringLayout,
	primariesPerHost int32,
) bool {
	count := cluster.Spec.Segments.Count
	switch layout {
	case cbv1alpha1.MirroringLayoutGroup:
		return count >= 2*primariesPerHost
	case cbv1alpha1.MirroringLayoutSpread:
		return count > primariesPerHost
	default:
		return count >= 2*primariesPerHost
	}
}

// checkMirroringProgress checks the progress of an in-flight mirroring enable operation.
// It is called from handleLifecyclePhase when the cluster is in Updating phase
// with a mirroring state annotation.
func (r *ClusterReconciler) checkMirroringProgress(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	stateJSON := cluster.Annotations[util.AnnotationMirroringState]
	if stateJSON == "" {
		logger.Warn("checkMirroringProgress called but no mirroring state annotation found")
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	var state mirroringStateData
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		logger.Error("failed to parse mirroring state", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil
	}

	// Check for timeout.
	if state.StartedAt != "" {
		started, parseErr := time.Parse(time.RFC3339, state.StartedAt)
		if parseErr == nil && time.Since(started) > mirroringEnableTimeout {
			return r.handleMirroringTimeout(ctx, cluster)
		}
	}

	logger.Info("checking mirroring progress", "phase", state.Phase)

	switch state.Phase {
	case mirroringPhaseCreatingSTS:
		// Check if mirror STS is ready.
		if r.isStatefulSetReady(ctx, cluster.Namespace, util.SegmentMirrorName(cluster.Name)) {
			logger.Info("mirror StatefulSet is ready, advancing to initialization")
			return r.advanceMirroringPhase(ctx, cluster, &state, mirroringPhaseInit)
		}
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil

	case mirroringPhaseInit:
		// Initialize mirrors via DB client.
		if err := r.initializeMirrors(ctx, cluster); err != nil {
			logger.Error("mirror initialization failed", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, nil
		}
		return r.advanceMirroringPhase(ctx, cluster, &state, mirroringPhaseSyncing)

	case mirroringPhaseSyncing:
		// Monitor sync progress.
		allSynced, err := r.monitorMirrorSync(ctx, cluster)
		if err != nil {
			logger.Error("mirror sync monitoring failed", "error", err)
			return ctrl.Result{RequeueAfter: requeueAfterError}, nil
		}
		if allSynced {
			return r.completeMirroringEnable(ctx, cluster)
		}
		// Update status to Syncing.
		cluster.Status.MirroringStatus = cbv1alpha1.MirroringSyncing
		if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
			logger.Error("failed to update syncing status", "error", statusErr)
		}
		return ctrl.Result{RequeueAfter: requeueAfterStopping}, nil

	default:
		logger.Warn("unknown mirroring phase, completing", "phase", state.Phase)
		return r.completeMirroringEnable(ctx, cluster)
	}
}

// advanceMirroringPhase updates the mirroring state to the next phase.
func (r *ClusterReconciler) advanceMirroringPhase(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state *mirroringStateData,
	nextPhase string,
) (ctrl.Result, error) {
	state.Phase = nextPhase
	state.PhaseStartedAt = time.Now().Format(time.RFC3339)

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling mirroring state: %w", err)
	}
	if err := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationMirroringState, string(stateJSON)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating mirroring state annotation: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// completeMirroringEnable finalizes a successful mirroring enable operation.
func (r *ClusterReconciler) completeMirroringEnable(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	// Update status: phase → Running, mirroringStatus → InSync.
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionMirroringHealthy), metav1.ConditionTrue,
		"MirroringInSync", "All mirrors are synchronized")
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after mirroring enable: %w", err)
	}

	// Remove mirroring state annotation (after status update to avoid re-fetch overwriting).
	if err := removeAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationMirroringState); err != nil {
		logger.Error("failed to remove mirroring state annotation", "error", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMirroringInSync,
		"Mirroring enabled and all mirrors are synchronized")
	r.metrics.RecordMirroringOperation(cluster.Name, cluster.Namespace, "enable")

	logger.Info("mirroring enable completed successfully")
	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// handleMirroringTimeout handles a mirroring enable operation that has exceeded
// the timeout. It sets the mirroring status to Degraded and emits a failure event.
func (r *ClusterReconciler) handleMirroringTimeout(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Error("mirroring enable timed out", "timeout", mirroringEnableTimeout)

	cluster.Status.MirroringStatus = cbv1alpha1.MirroringDegraded
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionMirroringHealthy), metav1.ConditionFalse,
		"MirroringTimeout",
		fmt.Sprintf("Mirroring initialization timed out after %v", mirroringEnableTimeout))
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after mirroring timeout: %w", err)
	}

	// Remove mirroring state annotation.
	if err := removeAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationMirroringState); err != nil {
		logger.Error("failed to remove mirroring state annotation after timeout", "error", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMirroringFailed,
		fmt.Sprintf("Mirroring initialization timed out after %v", mirroringEnableTimeout))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// handleDisableMirroring orchestrates disabling mirroring on a cluster.
// It deletes the mirror StatefulSet, optionally cleans up PVCs, and updates status.
func (r *ClusterReconciler) handleDisableMirroring(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	// Pre-flight: cluster must be in Running phase.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Warn("mirroring disable blocked: cluster not in Running phase",
			"currentPhase", cluster.Status.Phase)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMirroringFailed,
			fmt.Sprintf("Mirroring disable blocked: cluster is in %s phase, must be Running",
				cluster.Status.Phase))
		return nil
	}

	logger.Info("disabling mirroring")

	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMirroringDisabled,
		"Mirroring disable initiated — data protection will be reduced")

	// Delete mirror StatefulSet.
	mirrorSts := &appsv1.StatefulSet{}
	mirrorName := util.SegmentMirrorName(cluster.Name)
	if err := r.client.Get(ctx, types.NamespacedName{
		Name:      mirrorName,
		Namespace: cluster.Namespace,
	}, mirrorSts); err == nil {
		if delErr := r.client.Delete(ctx, mirrorSts); delErr != nil && !apierrors.IsNotFound(delErr) {
			return fmt.Errorf("deleting mirror StatefulSet %s: %w", mirrorName, delErr)
		}
		logger.Info("deleted mirror StatefulSet", "name", mirrorName)
	}

	// Clean up mirror PVCs based on deletion policy.
	if cluster.Spec.DeletionPolicy == cbv1alpha1.DeletionPolicyDelete {
		r.cleanupMirrorPVCs(ctx, cluster)
	} else {
		logger.Info("retaining mirror PVCs (deletionPolicy: Retain)")
	}

	// Update status.
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	// Remove MirroringHealthy condition by setting it to False with a clear reason.
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionMirroringHealthy), metav1.ConditionFalse,
		"MirroringDisabled", "Mirroring has been disabled")
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating status after mirroring disable: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMirroringDisabled,
		"Mirroring disabled successfully")
	r.metrics.RecordMirroringOperation(cluster.Name, cluster.Namespace, "disable")

	logger.Info("mirroring disable completed")
	return nil
}

// cleanupMirrorPVCs deletes PVCs associated with mirror segments.
func (r *ClusterReconciler) cleanupMirrorPVCs(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)
	mirrorStsName := util.SegmentMirrorName(cluster.Name)

	for i := int32(0); i < cluster.Spec.Segments.Count; i++ {
		pvcName := fmt.Sprintf("data-%s-%d", mirrorStsName, i)
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.client.Get(ctx, types.NamespacedName{
			Name:      pvcName,
			Namespace: cluster.Namespace,
		}, pvc); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error("failed to get mirror PVC", "pvc", pvcName, "error", err)
			}
			continue
		}
		if delErr := r.client.Delete(ctx, pvc); delErr != nil {
			logger.Error("failed to delete mirror PVC", "pvc", pvcName, "error", delErr)
		} else {
			logger.Info("deleted mirror PVC", "pvc", pvcName)
		}
	}
}

// initializeMirrors calls the DB client to initialize mirror segments.
// If no dbFactory is configured, it logs a warning and returns nil (the mirrors
// will be initialized by the Cloudberry utilities running inside the pods).
func (r *ClusterReconciler) initializeMirrors(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		logger.Info("no database client factory configured, skipping DB-level mirror initialization")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating db client for mirror initialization: %w", err)
	}
	defer dbClient.Close()

	layout := string(cbv1alpha1.MirroringLayoutGroup)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Layout != "" {
		layout = string(cluster.Spec.Segments.Mirroring.Layout)
	}

	// Initialize mirrors.
	if initErr := dbClient.InitializeMirrors(ctx, db.MirrorInitOptions{
		Layout:       layout,
		SegmentCount: cluster.Spec.Segments.Count,
		Parallelism:  2,
	}); initErr != nil {
		return fmt.Errorf("initializing mirrors: %w", initErr)
	}

	// Configure replication.
	if repErr := dbClient.ConfigureReplication(ctx, db.ReplicationOptions{
		Mode: "sync",
	}); repErr != nil {
		return fmt.Errorf("configuring replication: %w", repErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMirroringInitializing,
		"Mirror initialization and replication configuration completed")

	logger.Info("mirror initialization completed")
	return nil
}

// monitorMirrorSync checks the synchronization status of all mirror segments.
// Returns true when all mirrors are synced, false otherwise.
func (r *ClusterReconciler) monitorMirrorSync(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (bool, error) {
	logger := util.LoggerFromContext(ctx)

	if r.dbFactory == nil {
		// Without a DB factory, assume mirrors are synced once the STS is ready.
		logger.Info("no database client factory, assuming mirrors synced based on STS readiness")
		return r.isStatefulSetReady(ctx, cluster.Namespace,
			util.SegmentMirrorName(cluster.Name)), nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return false, fmt.Errorf("creating db client for sync monitoring: %w", err)
	}
	defer dbClient.Close()

	syncStatus, err := dbClient.GetMirrorSyncStatus(ctx)
	if err != nil {
		return false, fmt.Errorf("getting mirror sync status: %w", err)
	}

	// If no mirror segments found, check STS readiness as fallback.
	if len(syncStatus) == 0 {
		return r.isStatefulSetReady(ctx, cluster.Namespace,
			util.SegmentMirrorName(cluster.Name)), nil
	}

	allSynced := true
	for _, ms := range syncStatus {
		segID := fmt.Sprintf("%d", ms.ContentID)
		r.metrics.SetReplicationLag(cluster.Name, cluster.Namespace,
			segID, float64(ms.ReplicationLag))

		if !ms.IsSynced {
			allSynced = false
			logger.Info("mirror segment not yet synced",
				"contentID", ms.ContentID,
				"state", ms.State,
				"replicationLag", ms.ReplicationLag)
		}
	}

	return allSynced, nil
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

// isUpgradeRolledBack checks whether a previous upgrade failed and was rolled back.
// When this is true, the operator should NOT reconcile StatefulSets because the
// spec still contains the broken image that caused the failure.
func (r *ClusterReconciler) isUpgradeRolledBack(cluster *cbv1alpha1.CloudberryCluster) bool {
	for _, c := range cluster.Status.Conditions {
		if c.Type == string(cbv1alpha1.ConditionUpgradeFailed) && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

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
	r.metrics.RecordUpgradeOperation(cluster.Name, cluster.Namespace, "started")

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
	r.metrics.RecordUpgradeOperation(cluster.Name, cluster.Namespace, "completed")

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
	// A rollback represents a failed upgrade that is being reverted; record both
	// the failure and the rollback outcome.
	r.metrics.RecordUpgradeOperation(cluster.Name, cluster.Namespace, "failed")
	r.metrics.RecordUpgradeOperation(cluster.Name, cluster.Namespace, "rollback")

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

// isStatefulSetReady checks whether a StatefulSet has all desired replicas ready
// AND fully updated (i.e., the rolling update has completed).
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

	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}

	// Both ReadyReplicas and UpdatedReplicas must match the desired count.
	// UpdatedReplicas tracks pods that have been updated to the current spec
	// (i.e., the rolling update has reached them). Without this check, the
	// operator may consider a StatefulSet "ready" while old pods are still
	// serving — which is incorrect during an upgrade.
	return sts.Status.ReadyReplicas >= desired && sts.Status.UpdatedReplicas >= desired
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
		patchKeyStatus: cluster.Status,
	})
	if err != nil {
		return fmt.Errorf("marshaling status patch: %w", err)
	}

	return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}

// patchClearReadinessStatus explicitly clears the persisted readiness status
// fields (coordinatorReady, standbyReady, segmentsReady) together with the phase
// when a cluster is Stopped or restarted. A plain patchStatus cannot clear these
// fields because they are tagged with omitempty (api/v1alpha1/types.go): when they
// hold their zero value (false / 0), json.Marshal drops them, and a MergePatch only
// changes keys that are present — leaving the previously-persisted
// coordinatorReady=true / standbyReady=true / segmentsReady=N intact. We therefore
// build a raw MergePatch map with EXPLICIT zero values (mirroring patchClearCronJobName
// / patchFTSStatus) so the fields are actually reset in the CR. The phase is included
// because it is the authoritative transition driving this clear, so the cleared
// readiness and the new phase are persisted atomically in a single patch.
func patchClearReadinessStatus(
	ctx context.Context,
	c client.Client,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Build the patch manually so the zero values are explicitly present and the
	// omitempty struct tags are bypassed. Mirrors the in-memory zeroing already
	// done by the callers so the live object and the persisted object agree.
	statusPatch, err := json.Marshal(map[string]interface{}{
		patchKeyStatus: map[string]interface{}{
			"phase":            cluster.Status.Phase,
			"coordinatorReady": false,
			"standbyReady":     false,
			"segmentsReady":    0,
		},
	})
	if err != nil {
		return fmt.Errorf("marshaling readiness clear patch: %w", err)
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
		patchKeyStatus: statusMap,
	})
	if err != nil {
		return fmt.Errorf("marshaling FTS status patch: %w", err)
	}

	return c.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, statusPatch))
}

// needsExporterPrerequisites returns true if the cluster has query monitoring
// enabled with exporters configured, meaning exporter prerequisite resources
// (Secret, ConfigMap) must exist before the coordinator StatefulSet is created.
func needsExporterPrerequisites(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.QueryMonitoring != nil &&
		cluster.Spec.QueryMonitoring.Enabled &&
		cluster.Spec.QueryMonitoring.Exporters != nil
}

// ensureExporterPrerequisites creates the exporter credentials Secret and
// queries ConfigMap before the coordinator StatefulSet is created, so the
// sidecar containers can reference them via env vars and volume mounts.
func (r *ClusterReconciler) ensureExporterPrerequisites(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Resolve or generate the exporter password.
	password, secretMissing, err := r.resolveExporterPrereqPassword(ctx, cluster)
	if err != nil {
		return err
	}

	// Build DSN.
	dsn := r.buildExporterDSN(cluster, password)

	// Create the Secret if it does not exist yet.
	if secretMissing {
		if err := r.createExporterPrereqSecret(ctx, cluster, password, dsn); err != nil {
			return err
		}
	}

	// Create the queries ConfigMap if it does not exist yet.
	return r.createExporterPrereqConfigMap(ctx, cluster)
}

// resolveExporterPrereqPassword retrieves the exporter password from an existing
// Secret, or generates a new one. Returns the password, whether the Secret was
// missing (needs creation), and any error.
func (r *ClusterReconciler) resolveExporterPrereqPassword(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (password string, secretMissing bool, err error) {
	secretName := util.ExporterCredentialsSecretName(cluster.Name)
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}

	getErr := r.client.Get(ctx, key, existing)
	if getErr == nil && len(existing.Data["password"]) > 0 {
		return string(existing.Data["password"]), false, nil
	}
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return "", false, fmt.Errorf("getting exporter credentials secret: %w", getErr)
	}

	// Secret does not exist or has no password — generate a new one.
	pw, genErr := util.GenerateRandomPassword()
	if genErr != nil {
		return "", false, fmt.Errorf("generating exporter password: %w", genErr)
	}
	return pw, apierrors.IsNotFound(getErr), nil
}

// buildExporterDSN constructs the PostgreSQL DSN for the exporter sidecar.
func (r *ClusterReconciler) buildExporterDSN(
	cluster *cbv1alpha1.CloudberryCluster,
	password string,
) string {
	port := int32(util.DefaultCoordinatorPort)
	if cluster.Spec.Coordinator.Port > 0 {
		port = cluster.Spec.Coordinator.Port
	}
	return fmt.Sprintf("postgresql://cloudberry_exporter:%s@localhost:%d/postgres?sslmode=disable",
		url.QueryEscape(password), port)
}

// createExporterPrereqSecret creates the exporter credentials Secret.
// If the Secret already exists (race condition), the error is ignored.
func (r *ClusterReconciler) createExporterPrereqSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	password, dsn string,
) error {
	logger := util.LoggerFromContext(ctx)
	secretName := util.ExporterCredentialsSecretName(cluster.Name)
	desired := r.builder.BuildExporterCredentialsSecret(cluster, password, dsn)

	if createErr := r.client.Create(ctx, desired); createErr != nil {
		if !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("creating exporter credentials secret: %w", createErr)
		}
	} else {
		logger.Info("created exporter credentials secret (prerequisite)", "name", secretName)
	}
	return nil
}

// createExporterPrereqConfigMap creates the exporter queries ConfigMap if it
// does not exist. If the ConfigMap already exists, this is a no-op.
func (r *ClusterReconciler) createExporterPrereqConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)
	cmName := util.ExporterQueriesConfigMapName(cluster.Name)
	existingCM := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{Name: cmName, Namespace: cluster.Namespace}

	cmErr := r.client.Get(ctx, cmKey, existingCM)
	if !apierrors.IsNotFound(cmErr) {
		return nil // Already exists or transient error — skip creation.
	}

	desiredCM := r.builder.BuildExporterQueriesConfigMap(cluster)
	if createErr := r.client.Create(ctx, desiredCM); createErr != nil {
		if !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("creating exporter queries configmap: %w", createErr)
		}
	} else {
		logger.Info("created exporter queries configmap (prerequisite)", "name", cmName)
	}
	return nil
}
