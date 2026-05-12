package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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
	// Uses sync.Map for thread-safe concurrent access from multiple reconcile goroutines.
	configHashes sync.Map
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
		client:    c,
		scheme:    scheme,
		recorder:  recorder,
		builder:   b,
		dbFactory: dbFactory,
		metrics:   m,
		logger:    logger.With("controller", adminControllerName),
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

	// Skip full reconciliation if only status changed (ObservedGeneration matches)
	// and there are no maintenance annotations pending.
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Annotations[util.AnnotationMaintenance] == "" {
		logger.Info("skipping admin reconciliation, generation unchanged")
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

	// Reconcile workload management.
	if err := r.reconcileWorkload(ctx, cluster); err != nil {
		logger.Error("failed to reconcile workload management", "error", err)
	}

	// Reconcile query monitoring status.
	if err := r.reconcileQueryMonitoring(ctx, cluster); err != nil {
		logger.Error("failed to reconcile query monitoring", "error", err)
	}

	// Reconcile backup configuration.
	if err := r.reconcileBackup(ctx, cluster); err != nil {
		logger.Error("failed to reconcile backup", "error", err)
	}

	// Reconcile data loading configuration.
	if err := r.reconcileDataLoading(ctx, cluster); err != nil {
		logger.Error("failed to reconcile data loading", "error", err)
	}

	// Reconcile storage management.
	if err := r.reconcileStorage(ctx, cluster); err != nil {
		logger.Error("failed to reconcile storage management", "error", err)
	}

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// reconcileWorkload reconciles workload management configuration.
func (r *AdminReconciler) reconcileWorkload(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.Workload == nil || !cluster.Spec.Workload.Enabled {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Info("reconciling workload management",
		"resourceGroups", len(cluster.Spec.Workload.ResourceGroups),
		"rules", len(cluster.Spec.Workload.Rules),
		"idleRules", len(cluster.Spec.Workload.IdleRules),
	)

	r.recorder.Event(cluster, "Normal", "WorkloadReconciled",
		fmt.Sprintf("Workload management reconciled: %d resource groups, %d rules",
			len(cluster.Spec.Workload.ResourceGroups), len(cluster.Spec.Workload.Rules)))

	// Update workload-related metrics for each resource group.
	for _, rg := range cluster.Spec.Workload.ResourceGroups {
		r.metrics.SetResourceGroupUsage(cluster.Name, cluster.Namespace, rg.Name, 0, 0)
	}

	// Persist workload reconciliation status.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionWorkloadConfigured),
		metav1.ConditionTrue,
		"WorkloadReconciled",
		"Workload management is configured",
	)
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating workload status: %w", err)
	}

	return nil
}

// reconcileQueryMonitoring reconciles query monitoring status.
func (r *AdminReconciler) reconcileQueryMonitoring(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.QueryMonitoring == nil || !cluster.Spec.QueryMonitoring.Enabled {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Info("reconciling query monitoring",
		"historyRetention", cluster.Spec.QueryMonitoring.HistoryRetention,
		"samplingInterval", cluster.Spec.QueryMonitoring.SamplingInterval,
	)

	// Update query monitoring metrics.
	r.metrics.SetActiveQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.ActiveQueries))
	r.metrics.SetQueuedQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.QueuedQueries))
	r.metrics.SetBlockedQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.BlockedQueries))

	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating query monitoring status: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "QueryMonitoringReconciled",
		"Query monitoring configuration reconciled")

	return nil
}

// reconcileBackup reconciles backup configuration and status.
func (r *AdminReconciler) reconcileBackup(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Info("reconciling backup configuration",
		"schedule", cluster.Spec.Backup.Schedule,
		"incremental", cluster.Spec.Backup.Incremental,
		"destination", cluster.Spec.Backup.Destination.Type,
	)

	// Update backup-related status conditions.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionBackupConfigured),
		metav1.ConditionTrue,
		"BackupReconciled",
		"Backup configuration is applied",
	)
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating backup status: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "BackupReconciled",
		fmt.Sprintf("Backup configuration reconciled: schedule=%s, destination=%s",
			cluster.Spec.Backup.Schedule, cluster.Spec.Backup.Destination.Type))

	return nil
}

// reconcileDataLoading reconciles data loading configuration and status.
func (r *AdminReconciler) reconcileDataLoading(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.DataLoading == nil || !cluster.Spec.DataLoading.Enabled {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	activeJobs := int32(0)
	for _, job := range cluster.Spec.DataLoading.Jobs {
		if job.Enabled {
			activeJobs++
		}
	}

	logger.Info("reconciling data loading configuration",
		"totalJobs", len(cluster.Spec.DataLoading.Jobs),
		"activeJobs", activeJobs,
	)

	// Update data loading status.
	cluster.Status.DataLoadingJobs = activeJobs
	r.metrics.SetDataLoadingJobsActive(
		cluster.Name, cluster.Namespace, float64(activeJobs),
	)

	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataLoadingConfigured),
		metav1.ConditionTrue,
		"DataLoadingReconciled",
		"Data loading configuration is applied",
	)
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating data loading status: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "DataLoadingReconciled",
		fmt.Sprintf("Data loading reconciled: %d jobs configured, %d active",
			len(cluster.Spec.DataLoading.Jobs), activeJobs))

	return nil
}

// reconcileStorage reconciles storage management configuration and status.
func (r *AdminReconciler) reconcileStorage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.Storage == nil || !cluster.Spec.Storage.DiskMonitoring {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Info("reconciling storage management",
		"diskMonitoring", cluster.Spec.Storage.DiskMonitoring,
		"recommendationScanEnabled", cluster.Spec.Storage.RecommendationScan != nil &&
			cluster.Spec.Storage.RecommendationScan.Enabled,
		"usageReportEnabled", cluster.Spec.Storage.UsageReport != nil &&
			cluster.Spec.Storage.UsageReport.Enabled,
	)

	// Update disk usage metrics.
	r.metrics.SetDiskUsagePercent(
		cluster.Name, cluster.Namespace, float64(cluster.Status.DiskUsagePercent),
	)

	// Process recommendation scan configuration.
	recommendationCount := int32(0)
	if cluster.Spec.Storage.RecommendationScan != nil &&
		cluster.Spec.Storage.RecommendationScan.Enabled {
		logger.Info("recommendation scan is configured",
			"schedule", cluster.Spec.Storage.RecommendationScan.Schedule,
			"bloatThreshold", cluster.Spec.Storage.RecommendationScan.BloatThreshold,
			"skewThreshold", cluster.Spec.Storage.RecommendationScan.SkewThreshold,
		)
		recommendationCount = cluster.Status.RecommendationCount
	}

	// Update status fields.
	cluster.Status.RecommendationCount = recommendationCount

	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionStorageConfigured),
		metav1.ConditionTrue,
		"StorageReconciled",
		"Storage management is configured",
	)
	if err := r.client.Status().Update(ctx, cluster); err != nil {
		return fmt.Errorf("updating storage status: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "StorageReconciled",
		fmt.Sprintf("Storage management reconciled: diskMonitoring=%t, recommendations=%d",
			cluster.Spec.Storage.DiskMonitoring, recommendationCount))

	return nil
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
	lastHashVal, _ := r.configHashes.Load(clusterKey)
	lastHash, _ := lastHashVal.(string)

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
	r.configHashes.Store(clusterKey, currentHash)

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
