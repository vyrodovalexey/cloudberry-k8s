package controller

import (
	"context"
	"fmt"
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const adminControllerName = "admin-controller"

// AdminReconciler reconciles the administration aspects of a CloudberryCluster.
type AdminReconciler struct {
	client    client.Client
	scheme    *runtime.Scheme
	recorder  record.EventRecorder
	builder   builder.ResourceBuilder
	dbFactory DBClientFactory
	metrics   metrics.Recorder
	logger    *slog.Logger
	// configHashes tracks the last known config hash per cluster for change detection.
	configHashes map[string]string
}

// NewAdminReconciler creates a new AdminReconciler.
func NewAdminReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	dbFactory DBClientFactory,
	m metrics.Recorder,
	logger *slog.Logger,
) *AdminReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminReconciler{
		client:       c,
		scheme:       scheme,
		recorder:     recorder,
		builder:      b,
		dbFactory:    dbFactory,
		metrics:      m,
		logger:       logger.With("controller", adminControllerName),
		configHashes: make(map[string]string),
	}
}

// Reconcile handles the admin reconciliation for CloudberryCluster resources.
func (r *AdminReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.logger.With("cluster", req.Name, "namespace", req.Namespace)
	ctx = util.WithLogger(ctx, logger)

	ctx, span := telemetry.StartSpan(ctx, adminControllerName, "Reconcile")
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

	// Handle maintenance annotations.
	if maintenance, ok := cluster.Annotations[util.AnnotationMaintenance]; ok {
		return r.handleMaintenance(ctx, cluster, maintenance)
	}

	// Reconcile configuration parameters.
	if err := r.reconcileConfig(ctx, cluster); err != nil {
		logger.Error("failed to reconcile config", "error", err)
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionConfigApplied),
			metav1.ConditionFalse,
			"ConfigReconcileFailed",
			fmt.Sprintf("Failed to reconcile configuration: %v", err),
		)
		if statusErr := r.client.Status().Update(ctx, cluster); statusErr != nil {
			logger.Error("failed to update status", "error", statusErr)
		}
		return ctrl.Result{RequeueAfter: requeueAfterError}, err
	}

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// reconcileConfig detects and applies configuration changes.
func (r *AdminReconciler) reconcileConfig(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	logger := util.LoggerFromContext(ctx)

	if cluster.Spec.Config == nil {
		return nil
	}

	// Compute current config hash.
	currentHash := util.ComputeHash(cluster.Spec.Config.Parameters)
	clusterKey := fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	lastHash := r.configHashes[clusterKey]

	if currentHash == lastHash {
		return nil
	}

	logger.Info("configuration change detected", "previousHash", util.ShortHash(lastHash),
		"currentHash", util.ShortHash(currentHash))

	// Update the postgresql.conf ConfigMap.
	desired := r.builder.BuildPostgresqlConfConfigMap(cluster)
	existing := desired.DeepCopy()
	err := r.client.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting postgresql.conf configmap: %w", err)
	}

	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating postgresql.conf configmap: %w", createErr)
		}
	} else {
		existing.Data = desired.Data
		existing.Annotations = desired.Annotations
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating postgresql.conf configmap: %w", updateErr)
		}
	}

	// Update config hash.
	r.configHashes[clusterKey] = currentHash

	// Update status.
	now := metav1.Now()
	cluster.Status.LastConfigChangeTime = &now
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionTrue,
		"ConfigApplied",
		"All configuration parameters are applied",
	)

	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating config status: %w", err)
	}

	r.metrics.RecordConfigReload(cluster.Name, cluster.Namespace)
	r.recorder.Event(cluster, "Normal", "ConfigApplied", "Configuration parameters updated")

	return nil
}

// handleMaintenance processes maintenance annotations.
func (r *AdminReconciler) handleMaintenance(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	maintenance string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling maintenance operation", "type", maintenance)

	// Remove the maintenance annotation.
	delete(cluster.Annotations, util.AnnotationMaintenance)
	if err := r.client.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing maintenance annotation: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "MaintenanceStarted",
		fmt.Sprintf("Maintenance operation %s initiated", maintenance))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AdminReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Named(adminControllerName).
		Complete(r)
}
