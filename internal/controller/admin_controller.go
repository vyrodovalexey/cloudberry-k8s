package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

const (
	adminControllerName = "admin-controller"

	// requeueAfterShort is used when waiting for a rolling restart phase to complete.
	requeueAfterShort = 5 * time.Second

	// Rolling restart phase constants.
	restartPhaseMirrors     = "mirrors"
	restartPhasePrimaries   = "primaries"
	restartPhaseStandby     = "standby"
	restartPhaseCoordinator = "coordinator"
	restartPhaseCompleted   = "completed"

	// patchKeyMetadata is the JSON key for metadata in MergePatch payloads.
	patchKeyMetadata = "metadata"
	// patchKeyAnnotations is the JSON key for annotations in MergePatch payloads.
	patchKeyAnnotations = "annotations"
)

// restartRequiredParams lists PostgreSQL parameters that require a server
// restart to take effect (context = postmaster). Reload-safe parameters
// (context = sighup) are everything else.
var restartRequiredParams = map[string]bool{
	"shared_buffers":                 true,
	"max_connections":                true,
	"max_prepared_transactions":      true,
	"max_worker_processes":           true,
	"max_wal_senders":                true,
	"wal_level":                      true,
	"wal_buffers":                    true,
	"huge_pages":                     true,
	"shared_preload_libraries":       true,
	"max_locks_per_transaction":      true,
	"max_files_per_process":          true,
	"port":                           true,
	"superuser_reserved_connections": true,
	"unix_socket_directories":        true,
	"listen_addresses":               true,
	"bonjour":                        true,
	"ssl":                            true,
}

// rollingRestartState tracks the progress of a rolling restart operation.
type rollingRestartState struct {
	Phase         string   `json:"phase"`
	StartedAt     string   `json:"startedAt"`
	RestartParams []string `json:"restartParams"`
}

// AdminReconciler reconciles the administration aspects of a CloudberryCluster.
type AdminReconciler struct {
	client    client.Client
	scheme    *runtime.Scheme
	recorder  record.EventRecorder
	builder   builder.ResourceBuilder
	dbFactory db.DBClientFactory
	metrics   metrics.Recorder
	logger    *slog.Logger
	// configHashes tracks the last known config hash per cluster for change detection.
	// Uses sync.Map for thread-safe concurrent access from multiple reconcile goroutines.
	configHashes sync.Map
	// configParams tracks the last known config parameters per cluster for diff-based
	// change classification. Keyed by "namespace/name", value is map[string]string.
	configParams sync.Map
}

// NewAdminReconciler creates a new AdminReconciler.
func NewAdminReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	dbFactory db.DBClientFactory,
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

	logger.Debug("starting admin reconciliation",
		"name", req.Name, "namespace", req.Namespace)

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug("cluster not found, skipping reconciliation")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching cluster: %w", err)
	}

	logger.Debug("cluster fetched",
		"phase", cluster.Status.Phase,
		"generation", cluster.Generation,
		"observedGeneration", cluster.Status.ObservedGeneration)

	// Check for in-progress rolling restart (must continue regardless of generation).
	if _, hasRestart := cluster.Annotations[util.AnnotationRollingRestart]; hasRestart {
		logger.Debug("continuing in-progress rolling restart")
		return r.continueRollingRestart(ctx, cluster)
	}

	// Check for pending config reload. If the generation changed (new config applied),
	// skip the pending reload and let reconcileConfig handle the new change.
	if cluster.Status.ObservedGeneration == cluster.Generation {
		if result, handled := r.completePendingReload(ctx, cluster); handled {
			return result, nil
		}
	}

	// Skip if cluster is not running.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Debug("cluster not running, deferring admin reconciliation",
			"phase", cluster.Status.Phase)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches)
	// and there are no maintenance annotations pending.
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Annotations[util.AnnotationMaintenance] == "" {
		logger.Debug("skipping admin reconciliation, generation unchanged")
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Handle maintenance annotations.
	if maintenance, ok := cluster.Annotations[util.AnnotationMaintenance]; ok {
		logger.Debug("handling maintenance annotation", "operation", maintenance)
		return r.handleMaintenance(ctx, cluster, maintenance)
	}

	// Reconcile configuration parameters.
	logger.Debug("reconciling configuration parameters")
	if err := r.reconcileConfig(ctx, cluster); err != nil {
		return r.handleConfigError(ctx, logger, cluster, err)
	}

	// Reconcile all sub-components and patch status.
	logger.Debug("reconciling sub-components")
	r.reconcileSubComponents(ctx, logger, cluster)

	// Re-read the current phase from the API server to avoid overwriting phase changes
	// made by the cluster-controller (e.g., Scaling phase during scale-out).
	var latest cbv1alpha1.CloudberryCluster
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err == nil {
		cluster.Status.Phase = latest.Status.Phase
	}

	// Perform a single status patch for all sub-reconciler changes.
	// Using MergePatch prevents clobbering status changes from other controllers.
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		logger.Error("failed to update cluster status", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("updating cluster status: %w", err)
	}

	logger.Debug("admin reconciliation completed successfully")
	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// handleConfigError handles a config reconciliation error by setting the condition and returning.
func (r *AdminReconciler) handleConfigError(
	ctx context.Context,
	logger *slog.Logger,
	cluster *cbv1alpha1.CloudberryCluster,
	err error,
) (ctrl.Result, error) {
	logger.Error("failed to reconcile config", "error", err)
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionFalse,
		"ConfigReconcileFailed",
		fmt.Sprintf("Failed to reconcile configuration: %v", err),
	)
	if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
		logger.Error("failed to update status", "error", statusErr)
	}
	return ctrl.Result{RequeueAfter: requeueAfterError}, err
}

// reconcileSubComponents runs all non-config sub-reconcilers.
// Each sub-reconciler modifies cluster.Status in-place; errors are logged but non-fatal.
func (r *AdminReconciler) reconcileSubComponents(
	ctx context.Context,
	logger *slog.Logger,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	if err := r.reconcileWorkload(ctx, cluster); err != nil {
		logger.Error("failed to reconcile workload management", "error", err)
	}
	if err := r.reconcileQueryMonitoring(ctx, cluster); err != nil {
		logger.Error("failed to reconcile query monitoring", "error", err)
	}
	if err := r.reconcileBackup(ctx, cluster); err != nil {
		logger.Error("failed to reconcile backup", "error", err)
	}
	if err := r.reconcileDataLoading(ctx, cluster); err != nil {
		logger.Error("failed to reconcile data loading", "error", err)
	}
	if err := r.reconcileStorage(ctx, cluster); err != nil {
		logger.Error("failed to reconcile storage management", "error", err)
	}
}

// reconcileWorkload reconciles workload management configuration.
// When a DB client factory is available, it creates/alters/drops resource groups
// in the database to match the desired state from the CRD spec, stores workload
// and idle session rules in a ConfigMap, and updates resource group usage metrics.
// When no DB client factory is available (e.g. unit tests), it falls back to
// condition-only mode (log + event + condition).
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

	// If no dbFactory, fall back to condition-only mode (log + event + condition).
	if r.dbFactory == nil {
		r.reconcileWorkloadConditionOnly(cluster)
		return nil
	}

	// Create DB client for workload reconciliation.
	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		logger.Error("failed to create DB client for workload reconciliation", "error", err)
		// Fall back to condition-only mode — don't fail the whole reconciliation.
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionWorkloadConfigured),
			metav1.ConditionFalse,
			"DBUnavailable",
			fmt.Sprintf("Database client unavailable for workload reconciliation: %v", err),
		)
		return nil
	}
	defer dbClient.Close()

	// 1. Reconcile resource groups (diff desired vs actual).
	if err := r.reconcileResourceGroups(ctx, cluster, dbClient); err != nil {
		logger.Error("failed to reconcile resource groups", "error", err)
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionWorkloadConfigured),
			metav1.ConditionFalse,
			"ResourceGroupReconcileFailed",
			fmt.Sprintf("Failed to reconcile resource groups: %v", err),
		)
		return nil
	}

	// 2. Apply workload rules to ConfigMap.
	if err := r.applyWorkloadRules(ctx, cluster); err != nil {
		logger.Error("failed to apply workload rules", "error", err)
	}

	// 3. Apply idle session rules to ConfigMap.
	if err := r.applyIdleSessionRules(ctx, cluster); err != nil {
		logger.Error("failed to apply idle session rules", "error", err)
	}

	// 4. Update resource group usage metrics from DB.
	for _, rg := range cluster.Spec.Workload.ResourceGroups {
		cpu, mem, usageErr := dbClient.GetResourceGroupUsage(ctx, rg.Name)
		if usageErr == nil {
			r.metrics.SetResourceGroupUsage(cluster.Name, cluster.Namespace, rg.Name, cpu, mem)
		}
	}

	// Set success condition.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionWorkloadConfigured),
		metav1.ConditionTrue,
		"WorkloadReconciled",
		"Workload management is configured",
	)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonWorkloadReconciled,
		fmt.Sprintf("Workload management reconciled: %d resource groups, %d rules",
			len(cluster.Spec.Workload.ResourceGroups), len(cluster.Spec.Workload.Rules)))

	return nil
}

// reconcileWorkloadConditionOnly sets the workload condition and emits events
// without performing any DB operations. Used when dbFactory is nil.
func (r *AdminReconciler) reconcileWorkloadConditionOnly(
	cluster *cbv1alpha1.CloudberryCluster,
) {
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonWorkloadReconciled,
		fmt.Sprintf("Workload management reconciled: %d resource groups, %d rules",
			len(cluster.Spec.Workload.ResourceGroups), len(cluster.Spec.Workload.Rules)))

	// Update workload-related metrics for each resource group (zero usage without DB).
	for _, rg := range cluster.Spec.Workload.ResourceGroups {
		r.metrics.SetResourceGroupUsage(cluster.Name, cluster.Namespace, rg.Name, 0, 0)
	}

	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionWorkloadConfigured),
		metav1.ConditionTrue,
		"WorkloadReconciled",
		"Workload management is configured",
	)
}

// reconcileResourceGroups diffs the desired resource groups (from the CRD spec)
// against the actual resource groups in the database, and creates, alters, or
// drops resource groups as needed to converge to the desired state.
func (r *AdminReconciler) reconcileResourceGroups(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
) error {
	// Get existing resource groups from DB.
	existing, err := dbClient.ListResourceGroups(ctx)
	if err != nil {
		return fmt.Errorf("list resource groups: %w", err)
	}

	// Build maps for diffing.
	existingMap := make(map[string]db.ResourceGroupInfo, len(existing))
	for _, rg := range existing {
		existingMap[rg.Name] = rg
	}

	desiredMap := make(map[string]struct{}, len(cluster.Spec.Workload.ResourceGroups))
	for _, rg := range cluster.Spec.Workload.ResourceGroups {
		desiredMap[rg.Name] = struct{}{}
	}

	// Create or alter resource groups that are in desired but not in actual (or changed).
	desiredGroups := cluster.Spec.Workload.ResourceGroups
	if err := r.ensureDesiredResourceGroups(ctx, desiredGroups, existingMap, dbClient); err != nil {
		return err
	}

	// Drop resource groups that are in actual but not in desired.
	if err := r.dropOrphanedResourceGroups(ctx, existingMap, desiredMap, dbClient); err != nil {
		return err
	}

	return nil
}

// ensureDesiredResourceGroups creates or alters resource groups to match the desired spec.
func (r *AdminReconciler) ensureDesiredResourceGroups(
	ctx context.Context,
	desired []cbv1alpha1.ResourceGroupSpec,
	existingMap map[string]db.ResourceGroupInfo,
	dbClient db.Client,
) error {
	logger := util.LoggerFromContext(ctx)

	for _, rg := range desired {
		opts := db.ResourceGroupOptions{
			Name:          rg.Name,
			Concurrency:   rg.Concurrency,
			CPUMaxPercent: rg.CPUMaxPercent,
			CPUWeight:     rg.CPUWeight,
			MemoryLimit:   rg.MemoryLimit,
			MinCost:       rg.MinCost,
		}

		if actual, exists := existingMap[rg.Name]; exists {
			if needsAlter(rg, actual) {
				logger.Info("altering resource group", "name", rg.Name)
				if alterErr := dbClient.AlterResourceGroup(ctx, opts); alterErr != nil {
					return fmt.Errorf("alter resource group %s: %w", rg.Name, alterErr)
				}
			}
		} else {
			logger.Info("creating resource group", "name", rg.Name)
			if createErr := dbClient.CreateResourceGroup(ctx, opts); createErr != nil {
				return fmt.Errorf("create resource group %s: %w", rg.Name, createErr)
			}
		}
	}

	return nil
}

// dropOrphanedResourceGroups drops resource groups that exist in the database
// but are not in the desired spec.
func (r *AdminReconciler) dropOrphanedResourceGroups(
	ctx context.Context,
	existingMap map[string]db.ResourceGroupInfo,
	desiredMap map[string]struct{},
	dbClient db.Client,
) error {
	logger := util.LoggerFromContext(ctx)

	for name := range existingMap {
		if _, desired := desiredMap[name]; !desired {
			logger.Info("dropping resource group", "name", name)
			if dropErr := dbClient.DropResourceGroup(ctx, name); dropErr != nil {
				return fmt.Errorf("drop resource group %s: %w", name, dropErr)
			}
		}
	}

	return nil
}

// needsAlter returns true if the desired resource group spec differs from the
// actual resource group info in the database, indicating an ALTER is needed.
func needsAlter(desired cbv1alpha1.ResourceGroupSpec, actual db.ResourceGroupInfo) bool {
	if desired.Concurrency != 0 && desired.Concurrency != actual.Concurrency {
		return true
	}
	if desired.CPUMaxPercent != 0 && desired.CPUMaxPercent != actual.CPUMaxPercent {
		return true
	}
	if desired.CPUWeight != 0 && desired.CPUWeight != actual.CPUWeight {
		return true
	}
	if desired.MemoryLimit != actual.MemoryLimit {
		return true
	}
	return desired.MinCost != actual.MinCost
}

// applyWorkloadRules serializes workload rules to JSON and stores them in a
// ConfigMap named "{cluster}-workload-rules". The ConfigMap is created if it
// does not exist, or updated if it already exists.
func (r *AdminReconciler) applyWorkloadRules(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if len(cluster.Spec.Workload.Rules) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)

	rulesJSON, err := json.Marshal(cluster.Spec.Workload.Rules)
	if err != nil {
		return fmt.Errorf("marshal workload rules: %w", err)
	}

	cm := &corev1.ConfigMap{}
	cmName := types.NamespacedName{
		Name:      cluster.Name + "-workload-rules",
		Namespace: cluster.Namespace,
	}

	err = r.client.Get(ctx, cmName, cm)

	switch {
	case apierrors.IsNotFound(err):
		cm = r.buildWorkloadRulesConfigMap(cluster, cmName)
		cm.Data["rules.json"] = string(rulesJSON)
		if createErr := r.client.Create(ctx, cm); createErr != nil {
			return fmt.Errorf("create workload rules ConfigMap: %w", createErr)
		}
		logger.Info("created workload rules ConfigMap",
			"name", cmName.Name, "rules", len(cluster.Spec.Workload.Rules))
	case err != nil:
		return fmt.Errorf("get workload rules ConfigMap: %w", err)
	default:
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data["rules.json"] = string(rulesJSON)
		if updateErr := r.client.Update(ctx, cm); updateErr != nil {
			return fmt.Errorf("update workload rules ConfigMap: %w", updateErr)
		}
		logger.Info("updated workload rules ConfigMap",
			"name", cmName.Name, "rules", len(cluster.Spec.Workload.Rules))
	}

	return nil
}

// applyIdleSessionRules serializes idle session rules to JSON and stores them
// in the same ConfigMap ("{cluster}-workload-rules") under the key
// "idle-rules.json". The ConfigMap is created if it does not exist, or updated
// if it already exists.
func (r *AdminReconciler) applyIdleSessionRules(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if len(cluster.Spec.Workload.IdleRules) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)

	idleRulesJSON, err := json.Marshal(cluster.Spec.Workload.IdleRules)
	if err != nil {
		return fmt.Errorf("marshal idle session rules: %w", err)
	}

	cm := &corev1.ConfigMap{}
	cmName := types.NamespacedName{
		Name:      cluster.Name + "-workload-rules",
		Namespace: cluster.Namespace,
	}

	err = r.client.Get(ctx, cmName, cm)

	switch {
	case apierrors.IsNotFound(err):
		cm = r.buildWorkloadRulesConfigMap(cluster, cmName)
		cm.Data["idle-rules.json"] = string(idleRulesJSON)
		if createErr := r.client.Create(ctx, cm); createErr != nil {
			return fmt.Errorf("create idle rules ConfigMap: %w", createErr)
		}
		logger.Info("created idle session rules ConfigMap",
			"name", cmName.Name, "idleRules", len(cluster.Spec.Workload.IdleRules))
	case err != nil:
		return fmt.Errorf("get idle rules ConfigMap: %w", err)
	default:
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data["idle-rules.json"] = string(idleRulesJSON)
		if updateErr := r.client.Update(ctx, cm); updateErr != nil {
			return fmt.Errorf("update idle rules ConfigMap: %w", updateErr)
		}
		logger.Info("updated idle session rules ConfigMap",
			"name", cmName.Name, "idleRules", len(cluster.Spec.Workload.IdleRules))
	}

	return nil
}

// buildWorkloadRulesConfigMap creates a new ConfigMap skeleton for workload rules
// with standard labels and owner references.
func (r *AdminReconciler) buildWorkloadRulesConfigMap(
	cluster *cbv1alpha1.CloudberryCluster,
	cmName types.NamespacedName,
) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName.Name,
			Namespace: cmName.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "cloudberry-operator",
				"app.kubernetes.io/component":  "workload-rules",
				"app.kubernetes.io/instance":   cluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, cbv1alpha1.GroupVersion.WithKind("CloudberryCluster")),
			},
		},
		Data: make(map[string]string),
	}
}

// reconcileQueryMonitoring reconciles query monitoring status.
//
//nolint:unparam // error return reserved for future DB operations
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

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonQueryMonitoringReconciled,
		"Query monitoring configuration reconciled")

	return nil
}

// reconcileBackup reconciles backup configuration and status.
//
//nolint:unparam // error return reserved for future DB operations
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

	// Set backup-related status conditions (persisted by the caller).
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionBackupConfigured),
		metav1.ConditionTrue,
		"BackupReconciled",
		"Backup configuration is applied",
	)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonBackupReconciled,
		fmt.Sprintf("Backup configuration reconciled: schedule=%s, destination=%s",
			cluster.Spec.Backup.Schedule, cluster.Spec.Backup.Destination.Type))

	return nil
}

// reconcileDataLoading reconciles data loading configuration and status.
//
//nolint:unparam // error return reserved for future DB operations
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

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonDataLoadingReconciled,
		fmt.Sprintf("Data loading reconciled: %d jobs configured, %d active",
			len(cluster.Spec.DataLoading.Jobs), activeJobs))

	return nil
}

// reconcileStorage reconciles storage management configuration and status.
//
//nolint:unparam // error return reserved for future DB operations
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

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonStorageReconciled,
		fmt.Sprintf("Storage management reconciled: diskMonitoring=%t, recommendations=%d",
			cluster.Spec.Storage.DiskMonitoring, recommendationCount))

	return nil
}

// configChanges holds the classified parameter changes.
type configChanges struct {
	restartNeeded []string
	reloadSafe    []string
}

// classifyConfigChanges compares current and previous parameters and classifies
// each changed parameter as restart-required or reload-safe.
func classifyConfigChanges(current, previous map[string]string) configChanges {
	var changes configChanges

	for key, val := range current {
		if oldVal, exists := previous[key]; exists && oldVal == val {
			continue
		}
		if restartRequiredParams[key] {
			changes.restartNeeded = append(changes.restartNeeded, key)
		} else {
			changes.reloadSafe = append(changes.reloadSafe, key)
		}
	}
	for key := range previous {
		if _, exists := current[key]; !exists {
			if restartRequiredParams[key] {
				changes.restartNeeded = append(changes.restartNeeded, key)
			} else {
				changes.reloadSafe = append(changes.reloadSafe, key)
			}
		}
	}

	sort.Strings(changes.restartNeeded)
	sort.Strings(changes.reloadSafe)
	return changes
}

// updateConfigMap creates or updates the postgresql.conf ConfigMap.
func (r *AdminReconciler) updateConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired := r.builder.BuildPostgresqlConfConfigMap(cluster)

	// Retry on conflict (another controller may be updating the same ConfigMap).
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		existing := desired.DeepCopy()
		err := r.client.Get(ctx, client.ObjectKeyFromObject(desired), existing)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("getting postgresql.conf configmap: %w", err)
		}

		if apierrors.IsNotFound(err) {
			if createErr := r.client.Create(ctx, desired); createErr != nil {
				return fmt.Errorf("creating postgresql.conf configmap: %w", createErr)
			}
			return nil
		}

		existing.Data = desired.Data
		existing.Annotations = desired.Annotations
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			if apierrors.IsConflict(updateErr) && attempt < maxRetries-1 {
				r.logger.Debug("configmap update conflict, retrying", "attempt", attempt+1)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			return fmt.Errorf("updating postgresql.conf configmap: %w", updateErr)
		}
		return nil
	}
	return nil
}

// applyConfigChange updates status and emits events based on whether the change
// requires a restart or only a reload.
func (r *AdminReconciler) applyConfigChange(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	changes configChanges,
) error {
	now := metav1.Now()
	cluster.Status.LastConfigChangeTime = &now

	if len(changes.restartNeeded) > 0 {
		return r.applyRestartRequired(ctx, cluster, changes.restartNeeded)
	}
	return r.applyReloadSafe(ctx, cluster)
}

// applyRestartRequired handles the case where restart-required parameters changed.
func (r *AdminReconciler) applyRestartRequired(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	params []string,
) error {
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionFalse,
		"RestartRequired",
		fmt.Sprintf("Parameters requiring restart changed: %s", strings.Join(params, ", ")),
	)

	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating config status: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonRollingRestartStarted,
		fmt.Sprintf("Rolling restart initiated for parameters: %s", strings.Join(params, ", ")))

	if err := r.triggerRollingRestart(ctx, cluster, params); err != nil {
		return fmt.Errorf("triggering rolling restart: %w", err)
	}
	return nil
}

// applyReloadSafe handles the case where only reload-safe parameters changed.
// It sets a pending-reload annotation so that on the next reconciliation (after
// ConfigMap volume propagation), pg_reload_conf() is called to apply the new
// configuration without requiring a restart.
func (r *AdminReconciler) applyReloadSafe(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionFalse,
		"ConfigReloadPending",
		"Configuration updated, waiting for ConfigMap volume propagation before reload",
	)

	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating config status: %w", err)
	}

	// Set the pending-reload annotation with the current timestamp using a patch
	// to avoid conflicts with other controllers updating the same resource.
	now := time.Now().UTC().Format(time.RFC3339)
	patch := client.MergeFrom(cluster.DeepCopy())
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[util.AnnotationPendingReload] = now

	if err := r.client.Patch(ctx, cluster, patch); err != nil {
		return fmt.Errorf("setting pending-reload annotation: %w", err)
	}

	r.logger.Info("pending-reload annotation set, will reload after ConfigMap propagation")
	return nil
}

// completePendingReload checks if a pending reload annotation exists and enough
// time has passed for ConfigMap volume propagation, then calls pg_reload_conf().
func (r *AdminReconciler) completePendingReload(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, bool) {
	pendingTime, hasPending := cluster.Annotations[util.AnnotationPendingReload]
	if !hasPending {
		return ctrl.Result{}, false
	}

	// Parse the timestamp and check if enough time has passed.
	// Kubernetes ConfigMap volume propagation typically takes up to
	// kubelet sync period + cache propagation delay (~30-60s).
	// We use 30s to account for most environments while keeping responsiveness.
	const configMapPropagationDelay = 30 * time.Second

	parsedTime, err := time.Parse(time.RFC3339, pendingTime)
	if err != nil {
		r.logger.Error("failed to parse pending-reload timestamp, executing reload now", "error", err)
	} else {
		elapsed := time.Since(parsedTime)
		if elapsed < configMapPropagationDelay {
			remaining := configMapPropagationDelay - elapsed
			r.logger.Info("waiting for ConfigMap propagation before reload",
				"elapsed", elapsed.Round(time.Second),
				"remaining", remaining.Round(time.Second))
			return ctrl.Result{RequeueAfter: remaining}, true
		}
	}

	// Enough time has passed — call pg_reload_conf().
	if r.dbFactory != nil {
		dbClient, dbErr := r.dbFactory.NewClient(ctx, cluster)
		if dbErr != nil {
			r.logger.Error("failed to create DB client for config reload", "error", dbErr)
		} else {
			defer dbClient.Close()
			if reloadErr := dbClient.ReloadConfig(ctx); reloadErr != nil {
				r.logger.Error("failed to reload config on coordinator", "error", reloadErr)
			} else {
				r.logger.Info("configuration reloaded on coordinator via pg_reload_conf()")
			}
		}
	}

	// Remove the pending-reload annotation using a patch to avoid conflicts.
	patch := client.MergeFrom(cluster.DeepCopy())
	delete(cluster.Annotations, util.AnnotationPendingReload)
	if err := r.client.Patch(ctx, cluster, patch); err != nil {
		r.logger.Error("failed to remove pending-reload annotation", "error", err)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true
	}

	// Update status to reflect successful reload.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionTrue,
		"ConfigReloaded",
		"All configuration parameters are applied via reload",
	)
	if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
		r.logger.Error("failed to update config status after reload", "error", statusErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonConfigReloaded,
		"Configuration parameters reloaded without restart")
	r.metrics.RecordConfigReload(cluster.Name, cluster.Namespace)

	return ctrl.Result{}, true
}

// reconcileConfig detects and applies configuration changes.
// It classifies changed parameters into reload-safe (sighup) and restart-required
// (postmaster) categories and triggers the appropriate action.
// Additionally, it applies coordinator-only, database-specific, and role-specific parameters.
func (r *AdminReconciler) reconcileConfig(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	logger := util.LoggerFromContext(ctx)

	if cluster.Spec.Config == nil {
		return nil
	}

	// Compute current config hash including all config layers.
	currentHash := r.computeFullConfigHash(cluster.Spec.Config)
	clusterKey := fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	lastHashVal, _ := r.configHashes.Load(clusterKey)
	lastHash, _ := lastHashVal.(string)

	if currentHash == lastHash {
		return nil
	}

	logger.Info("configuration change detected",
		"previousHash", util.ShortHash(lastHash),
		"currentHash", util.ShortHash(currentHash))

	// Load previous parameters and classify changes.
	prevParamsVal, _ := r.configParams.Load(clusterKey)
	prevParams, _ := prevParamsVal.(map[string]string)
	if prevParams == nil {
		prevParams = make(map[string]string)
	}

	changes := classifyConfigChanges(cluster.Spec.Config.Parameters, prevParams)
	logger.Info("classified config changes",
		"restartRequired", changes.restartNeeded,
		"reloadSafe", changes.reloadSafe,
	)

	// Update the postgresql.conf ConfigMap.
	if err := r.updateConfigMap(ctx, cluster); err != nil {
		return err
	}

	// Apply coordinator-only parameters via the database client.
	if err := r.applyCoordinatorParameters(ctx, cluster); err != nil {
		logger.Error("failed to apply coordinator parameters", "error", err)
		// Non-fatal: continue with other config layers.
	}

	// Apply database-specific parameters via ALTER DATABASE SET.
	if err := r.applyDatabaseParameters(ctx, cluster); err != nil {
		logger.Error("failed to apply database parameters", "error", err)
		// Non-fatal: continue with other config layers.
	}

	// Apply role-specific parameters via ALTER ROLE SET.
	if err := r.applyRoleParameters(ctx, cluster); err != nil {
		logger.Error("failed to apply role parameters", "error", err)
		// Non-fatal: continue with other config layers.
	}

	// Apply the change (restart or reload).
	if err := r.applyConfigChange(ctx, cluster, changes); err != nil {
		return err
	}

	// Update config hash and stored parameters AFTER successful apply.
	// This ensures retries will re-detect the change if apply fails.
	r.configHashes.Store(clusterKey, currentHash)
	r.storeConfigParams(clusterKey, cluster.Spec.Config.Parameters)

	// Record metric for restart-required changes immediately.
	// For reload-safe changes, the metric is recorded in completePendingReload
	// after the actual pg_reload_conf() call succeeds.
	if len(changes.restartNeeded) > 0 {
		r.metrics.RecordConfigReload(cluster.Name, cluster.Namespace)
	}
	return nil
}

// computeFullConfigHash computes a hash over all config layers to detect any change.
func (r *AdminReconciler) computeFullConfigHash(config *cbv1alpha1.ConfigSpec) string {
	// Combine all config layers into a single map for hashing.
	combined := make(map[string]string)
	for k, v := range config.Parameters {
		combined["param:"+k] = v
	}
	for k, v := range config.CoordinatorParameters {
		combined["coord:"+k] = v
	}
	for dbName, params := range config.DatabaseParameters {
		for k, v := range params {
			combined["db:"+dbName+":"+k] = v
		}
	}
	for roleName, params := range config.RoleParameters {
		for k, v := range params {
			combined["role:"+roleName+":"+k] = v
		}
	}
	return util.ComputeHash(combined)
}

// applyCoordinatorParameters applies coordinator-only parameters via ALTER SYSTEM SET
// on the coordinator. These parameters are only applied to the coordinator node.
func (r *AdminReconciler) applyCoordinatorParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if len(cluster.Spec.Config.CoordinatorParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying coordinator-only parameters",
		"count", len(cluster.Spec.Config.CoordinatorParameters))

	if r.dbFactory == nil {
		logger.Debug("database client factory not available, skipping coordinator parameters")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating database client for coordinator parameters: %w", err)
	}
	defer dbClient.Close()

	for name, value := range cluster.Spec.Config.CoordinatorParameters {
		if setErr := dbClient.SetParameter(ctx, name, value, db.ParameterScope{Level: "cluster"}); setErr != nil {
			logger.Error("failed to set coordinator parameter",
				"name", name, "value", value, "error", setErr)
			continue
		}
		logger.Debug("coordinator parameter applied", "name", name, "value", value)
	}

	// Reload configuration to apply changes.
	if reloadErr := dbClient.ReloadConfig(ctx); reloadErr != nil {
		return fmt.Errorf("reloading config after coordinator parameters: %w", reloadErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonConfigReloaded,
		fmt.Sprintf("Coordinator-only parameters applied: %d parameters",
			len(cluster.Spec.Config.CoordinatorParameters)))

	return nil
}

// applyDatabaseParameters applies per-database parameters via ALTER DATABASE SET.
func (r *AdminReconciler) applyDatabaseParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if len(cluster.Spec.Config.DatabaseParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying database-specific parameters",
		"databases", len(cluster.Spec.Config.DatabaseParameters))

	if r.dbFactory == nil {
		logger.Debug("database client factory not available, skipping database parameters")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating database client for database parameters: %w", err)
	}
	defer dbClient.Close()

	for dbName, params := range cluster.Spec.Config.DatabaseParameters {
		for name, value := range params {
			scope := db.ParameterScope{Level: "database", Target: dbName}
			if setErr := dbClient.SetParameter(ctx, name, value, scope); setErr != nil {
				logger.Error("failed to set database parameter",
					"database", dbName, "name", name, "value", value, "error", setErr)
				continue
			}
			logger.Debug("database parameter applied",
				"database", dbName, "name", name, "value", value)
		}
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonConfigReloaded,
		fmt.Sprintf("Database-specific parameters applied for %d databases",
			len(cluster.Spec.Config.DatabaseParameters)))

	return nil
}

// applyRoleParameters applies per-role parameters via ALTER ROLE SET.
func (r *AdminReconciler) applyRoleParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if len(cluster.Spec.Config.RoleParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying role-specific parameters",
		"roles", len(cluster.Spec.Config.RoleParameters))

	if r.dbFactory == nil {
		logger.Debug("database client factory not available, skipping role parameters")
		return nil
	}

	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating database client for role parameters: %w", err)
	}
	defer dbClient.Close()

	for roleName, params := range cluster.Spec.Config.RoleParameters {
		for name, value := range params {
			scope := db.ParameterScope{Level: "role", Target: roleName}
			if setErr := dbClient.SetParameter(ctx, name, value, scope); setErr != nil {
				logger.Error("failed to set role parameter",
					"role", roleName, "name", name, "value", value, "error", setErr)
				continue
			}
			logger.Debug("role parameter applied",
				"role", roleName, "name", name, "value", value)
		}
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonConfigReloaded,
		fmt.Sprintf("Role-specific parameters applied for %d roles",
			len(cluster.Spec.Config.RoleParameters)))

	return nil
}

// storeConfigParams stores a copy of the parameters map in the sync.Map.
func (r *AdminReconciler) storeConfigParams(key string, params map[string]string) {
	paramsCopy := make(map[string]string, len(params))
	for k, v := range params {
		paramsCopy[k] = v
	}
	r.configParams.Store(key, paramsCopy)
}

// triggerRollingRestart sets the rolling-restart annotation on the cluster to begin
// a phased rolling restart. The phase order is: mirrors → primaries → standby → coordinator.
// Phases that don't apply (e.g., no mirrors, no standby) are skipped at continue time.
func (r *AdminReconciler) triggerRollingRestart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	restartParams []string,
) error {
	logger := util.LoggerFromContext(ctx)

	// Determine starting phase based on cluster topology.
	startPhase := restartPhaseMirrors
	hasMirroring := cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled
	if !hasMirroring {
		startPhase = restartPhasePrimaries
	}

	state := rollingRestartState{
		Phase:         startPhase,
		StartedAt:     time.Now().UTC().Format(time.RFC3339),
		RestartParams: restartParams,
	}

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling rolling restart state: %w", err)
	}

	logger.Info("triggering rolling restart",
		"startPhase", startPhase,
		"params", restartParams,
	)

	// Set the annotation via MergePatch to avoid conflicts.
	if patchErr := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationRollingRestart, string(stateJSON)); patchErr != nil {
		return fmt.Errorf("setting rolling restart annotation: %w", patchErr)
	}

	// Restart the first StatefulSet to kick off the rolling restart.
	stsName := r.statefulSetNameForPhase(cluster, startPhase)
	if stsName != "" {
		if restartErr := r.restartStatefulSet(ctx, cluster.Namespace, stsName); restartErr != nil {
			// Non-fatal: continueRollingRestart will retry on next reconcile.
			logger.Error("failed to restart initial statefulset",
				"phase", startPhase, "sts", stsName, "error", restartErr)
		}
	}

	return nil
}

// continueRollingRestart checks the current rolling restart phase and advances
// to the next phase when the current StatefulSet is ready. Returns a short
// requeue to poll progress.
func (r *AdminReconciler) continueRollingRestart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)

	annotationVal := cluster.Annotations[util.AnnotationRollingRestart]
	var state rollingRestartState
	if err := json.Unmarshal([]byte(annotationVal), &state); err != nil {
		logger.Error("failed to parse rolling restart annotation, removing it", "error", err)
		if removeErr := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationRollingRestart); removeErr != nil {
			return ctrl.Result{}, fmt.Errorf("removing invalid rolling restart annotation: %w", removeErr)
		}
		return ctrl.Result{RequeueAfter: requeueAfterShort}, nil
	}

	logger.Info("continuing rolling restart", "phase", state.Phase, "params", state.RestartParams)

	// Determine the StatefulSet name for the current phase.
	stsName := r.statefulSetNameForPhase(cluster, state.Phase)

	// If the current phase doesn't apply, advance to the next phase.
	if stsName == "" {
		nextPhase := r.nextRestartPhase(cluster, state.Phase)
		if nextPhase == restartPhaseCompleted {
			return r.completeRollingRestart(ctx, cluster, state)
		}
		state.Phase = nextPhase
		return r.updateRestartAnnotation(ctx, cluster, state)
	}

	// Check if the StatefulSet for the current phase has completed its rolling update.
	// Using isStatefulSetRolled instead of isStatefulSetReady ensures we wait for
	// the StatefulSet controller to actually roll all pods (CurrentRevision == UpdateRevision),
	// not just check that replicas are ready (which is already true before rolling starts).
	rolled, err := r.isStatefulSetRolled(ctx, cluster.Namespace, stsName)
	if err != nil {
		// StatefulSet may not exist; skip this phase.
		logger.Info("statefulset not found, skipping phase", "phase", state.Phase, "sts", stsName, "error", err)
		nextPhase := r.nextRestartPhase(cluster, state.Phase)
		if nextPhase == restartPhaseCompleted {
			return r.completeRollingRestart(ctx, cluster, state)
		}
		state.Phase = nextPhase
		return r.updateRestartAnnotation(ctx, cluster, state)
	}

	if !rolled {
		// StatefulSet is still rolling; requeue to check again.
		logger.Info("waiting for statefulset rolling update to complete",
			"phase", state.Phase, "sts", stsName)
		return ctrl.Result{RequeueAfter: requeueAfterShort}, nil
	}

	// Current phase StatefulSet is ready. Advance to the next phase.
	nextPhase := r.nextRestartPhase(cluster, state.Phase)
	if nextPhase == restartPhaseCompleted {
		return r.completeRollingRestart(ctx, cluster, state)
	}

	// Restart the StatefulSet for the next phase.
	nextSTS := r.statefulSetNameForPhase(cluster, nextPhase)
	if nextSTS != "" {
		if restartErr := r.restartStatefulSet(ctx, cluster.Namespace, nextSTS); restartErr != nil {
			logger.Error("failed to restart statefulset", "phase", nextPhase, "sts", nextSTS, "error", restartErr)
			return ctrl.Result{RequeueAfter: requeueAfterShort}, restartErr
		}
		logger.Info("restarted statefulset for phase", "phase", nextPhase, "sts", nextSTS)
	}

	state.Phase = nextPhase
	return r.updateRestartAnnotation(ctx, cluster, state)
}

// completeRollingRestart finalizes the rolling restart: removes the annotation,
// sets ConfigApplied=True, and emits a completion event.
func (r *AdminReconciler) completeRollingRestart(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state rollingRestartState,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("rolling restart completed", "params", state.RestartParams)

	// Remove the rolling restart annotation.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationRollingRestart); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing rolling restart annotation: %w", err)
	}

	// Set ConfigApplied=True.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionTrue,
		"ConfigAppliedAfterRestart",
		fmt.Sprintf("Configuration applied after rolling restart of parameters: %s",
			strings.Join(state.RestartParams, ", ")),
	)

	// Re-read the current phase from the API server to avoid overwriting phase changes
	// made by the cluster-controller (e.g., Scaling phase during scale-out).
	var latest cbv1alpha1.CloudberryCluster
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err == nil {
		cluster.Status.Phase = latest.Status.Phase
	}

	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after rolling restart: %w", err)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonRollingRestartCompleted,
		fmt.Sprintf("Rolling restart completed for parameters: %s", strings.Join(state.RestartParams, ", ")))

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// updateRestartAnnotation writes the updated rolling restart state back to the
// cluster annotation and requeues.
func (r *AdminReconciler) updateRestartAnnotation(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	state rollingRestartState,
) (ctrl.Result, error) {
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshaling rolling restart state: %w", err)
	}

	if patchErr := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationRollingRestart, string(stateJSON)); patchErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating rolling restart annotation: %w", patchErr)
	}

	return ctrl.Result{RequeueAfter: requeueAfterShort}, nil
}

// restartStatefulSet triggers a rolling update of a StatefulSet by patching
// the pod template annotation with the current timestamp.
func (r *AdminReconciler) restartStatefulSet(ctx context.Context, namespace, name string) error {
	logger := util.LoggerFromContext(ctx)

	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting statefulset %s/%s: %w", namespace, name, err)
	}

	// Patch the pod template annotation to trigger a rolling update.
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations[util.AnnotationRestartTrigger] = time.Now().UTC().Format(time.RFC3339Nano)

	if err := r.client.Update(ctx, sts); err != nil {
		return fmt.Errorf("updating statefulset %s/%s: %w", namespace, name, err)
	}

	logger.Info("patched statefulset pod template for rolling restart", "sts", name)
	return nil
}

// isStatefulSetRolled checks whether a StatefulSet has completed its rolling update.
// It verifies that all replicas are ready, all replicas are updated to the latest
// revision, and the current revision matches the update revision. This prevents
// the controller from advancing to the next phase before pods are actually rolled.
func (r *AdminReconciler) isStatefulSetRolled(ctx context.Context, namespace, name string) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return false, fmt.Errorf("getting statefulset %s/%s: %w", namespace, name, err)
	}

	if sts.Spec.Replicas == nil {
		return false, nil
	}

	desired := *sts.Spec.Replicas
	return sts.Status.ReadyReplicas == desired &&
		sts.Status.UpdatedReplicas == desired &&
		sts.Status.CurrentRevision == sts.Status.UpdateRevision, nil
}

// statefulSetNameForPhase returns the StatefulSet name for the given restart phase,
// or an empty string if the phase doesn't apply to this cluster.
func (r *AdminReconciler) statefulSetNameForPhase(
	cluster *cbv1alpha1.CloudberryCluster,
	phase string,
) string {
	switch phase {
	case restartPhaseMirrors:
		if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
			return util.SegmentMirrorName(cluster.Name)
		}
		return ""
	case restartPhasePrimaries:
		return util.SegmentPrimaryName(cluster.Name)
	case restartPhaseStandby:
		if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
			return util.StandbyName(cluster.Name)
		}
		return ""
	case restartPhaseCoordinator:
		return util.CoordinatorName(cluster.Name)
	default:
		return ""
	}
}

// nextRestartPhase returns the phase that follows the given phase.
// Phases that don't apply are skipped by continueRollingRestart.
func (r *AdminReconciler) nextRestartPhase(
	cluster *cbv1alpha1.CloudberryCluster,
	current string,
) string {
	phases := []string{
		restartPhaseMirrors,
		restartPhasePrimaries,
		restartPhaseStandby,
		restartPhaseCoordinator,
	}

	for i, p := range phases {
		if p == current && i+1 < len(phases) {
			next := phases[i+1]
			// Skip phases that don't apply.
			if r.statefulSetNameForPhase(cluster, next) == "" {
				return r.nextRestartPhase(cluster, next)
			}
			return next
		}
	}
	return restartPhaseCompleted
}

// handleMaintenance processes maintenance annotations by executing the requested
// database maintenance operation (vacuum, analyze, reindex, log-rotate) directly
// via the DB client on the coordinator. For operations that may be long-running
// (vacuum-full), a Kubernetes Job is also created as a fallback mechanism.
func (r *AdminReconciler) handleMaintenance(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	maintenance string,
) (ctrl.Result, error) {
	logger := util.LoggerFromContext(ctx)
	logger.Info("handling maintenance operation", "type", maintenance)

	// Remove the maintenance annotation using MergePatch to avoid conflicts with stale objects.
	if err := removeAnnotationPatch(ctx, r.client, cluster, util.AnnotationMaintenance); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing maintenance annotation: %w", err)
	}

	// Validate the maintenance operation type.
	validOps := map[string]bool{
		util.MaintenanceVacuum:        true,
		util.MaintenanceVacuumAnalyze: true,
		util.MaintenanceVacuumFull:    true,
		util.MaintenanceAnalyze:       true,
		util.MaintenanceReindex:       true,
		util.MaintenanceLogRotate:     true,
	}
	if !validOps[maintenance] {
		logger.Warn("unknown maintenance operation", "type", maintenance)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonMaintenanceUnknown,
			fmt.Sprintf("Unknown maintenance operation: %s", maintenance))
		return ctrl.Result{}, nil
	}

	// Attempt to execute the maintenance operation directly via the DB client.
	// This is preferred over Jobs for simplicity and immediate feedback.
	if r.dbFactory != nil {
		if err := r.executeMaintenanceViaDB(ctx, cluster, maintenance); err != nil {
			logger.Error("direct maintenance execution failed, falling back to Job",
				"operation", maintenance, "error", err)
		} else {
			// Direct execution succeeded — emit completion event and record metric.
			r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMaintenanceCompleted,
				fmt.Sprintf("Maintenance operation %s completed successfully", maintenance))
			r.metrics.RecordMaintenanceOperation(cluster.Name, cluster.Namespace, maintenance)
			logger.Info("maintenance operation completed via DB client", "operation", maintenance)
			return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
		}
	}

	// Fallback: create a maintenance Job for operations that failed via DB client
	// or when the DB factory is not available.
	timestamp := time.Now().Format("20060102-150405")
	job := r.builder.BuildMaintenanceJob(cluster, maintenance, timestamp)
	if err := r.client.Create(ctx, job); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("creating maintenance job: %w", err)
		}
		logger.Info("maintenance job already exists", "job", job.Name)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMaintenanceStarted,
		fmt.Sprintf("Maintenance operation %s initiated, job: %s", maintenance, job.Name))
	r.metrics.RecordMaintenanceOperation(cluster.Name, cluster.Namespace, maintenance)

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// executeMaintenanceViaDB executes a maintenance operation directly on the coordinator
// via the database client. This provides immediate feedback and avoids the overhead
// of creating a Kubernetes Job for simple operations.
func (r *AdminReconciler) executeMaintenanceViaDB(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	operation string,
) error {
	dbClient, err := r.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("creating database client: %w", err)
	}
	defer dbClient.Close()

	switch operation {
	case util.MaintenanceVacuum:
		return dbClient.Vacuum(ctx, db.VacuumOptions{})
	case util.MaintenanceVacuumAnalyze:
		return dbClient.Vacuum(ctx, db.VacuumOptions{Analyze: true})
	case util.MaintenanceVacuumFull:
		return dbClient.Vacuum(ctx, db.VacuumOptions{Full: true})
	case util.MaintenanceAnalyze:
		return dbClient.Analyze(ctx, "")
	case util.MaintenanceReindex:
		return dbClient.Reindex(ctx, db.ReindexOptions{Database: "postgres"})
	case util.MaintenanceLogRotate:
		return r.executeLogRotate(ctx, dbClient)
	default:
		return fmt.Errorf("unsupported maintenance operation for direct execution: %s", operation)
	}
}

// executeLogRotate rotates the PostgreSQL log file by calling pg_rotate_logfile().
// This is a Cloudberry/PostgreSQL built-in function that signals the logger process
// to switch to a new log file immediately.
func (r *AdminReconciler) executeLogRotate(ctx context.Context, dbClient db.Client) error {
	return dbClient.LogRotate(ctx)
}

// SetupWithManager sets up the controller with the Manager.
func (r *AdminReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Named(adminControllerName).
		Complete(r)
}
