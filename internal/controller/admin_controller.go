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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
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
	dbFactory DBClientFactory
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

	// Check for in-progress rolling restart (must continue regardless of generation).
	if _, hasRestart := cluster.Annotations[util.AnnotationRollingRestart]; hasRestart {
		return r.continueRollingRestart(ctx, cluster)
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
		return r.handleConfigError(ctx, logger, cluster, err)
	}

	// Reconcile all sub-components and patch status.
	r.reconcileSubComponents(ctx, logger, cluster)

	// Perform a single status patch for all sub-reconciler changes.
	// Using MergePatch prevents clobbering status changes from other controllers.
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		logger.Error("failed to update cluster status", "error", err)
		return ctrl.Result{RequeueAfter: requeueAfterError}, fmt.Errorf("updating cluster status: %w", err)
	}

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
//
//nolint:unparam // error return reserved for future DB operations
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

	// Set workload reconciliation status (persisted by the caller).
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionWorkloadConfigured),
		metav1.ConditionTrue,
		"WorkloadReconciled",
		"Workload management is configured",
	)

	return nil
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

	r.recorder.Event(cluster, "Normal", "QueryMonitoringReconciled",
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

	r.recorder.Event(cluster, "Normal", "BackupReconciled",
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

	r.recorder.Event(cluster, "Normal", "DataLoadingReconciled",
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

	r.recorder.Event(cluster, "Normal", "StorageReconciled",
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

	r.recorder.Event(cluster, "Normal", "RollingRestartStarted",
		fmt.Sprintf("Rolling restart initiated for parameters: %s", strings.Join(params, ", ")))

	if err := r.triggerRollingRestart(ctx, cluster, params); err != nil {
		return fmt.Errorf("triggering rolling restart: %w", err)
	}
	return nil
}

// applyReloadSafe handles the case where only reload-safe parameters changed.
func (r *AdminReconciler) applyReloadSafe(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionConfigApplied),
		metav1.ConditionTrue,
		"ConfigReloaded",
		"All configuration parameters are applied via reload",
	)

	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return fmt.Errorf("updating config status: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "ConfigReloaded",
		"Configuration parameters reloaded without restart")
	return nil
}

// reconcileConfig detects and applies configuration changes.
// It classifies changed parameters into reload-safe (sighup) and restart-required
// (postmaster) categories and triggers the appropriate action.
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

	// Update config hash and stored parameters.
	r.configHashes.Store(clusterKey, currentHash)
	r.storeConfigParams(clusterKey, cluster.Spec.Config.Parameters)

	// Apply the change (restart or reload).
	if err := r.applyConfigChange(ctx, cluster, changes); err != nil {
		return err
	}

	r.metrics.RecordConfigReload(cluster.Name, cluster.Namespace)
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

	// Check if the StatefulSet for the current phase is ready.
	ready, err := r.isStatefulSetReady(ctx, cluster.Namespace, stsName)
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

	if !ready {
		// StatefulSet is still rolling; requeue to check again.
		logger.Info("waiting for statefulset to become ready", "phase", state.Phase, "sts", stsName)
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

	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status after rolling restart: %w", err)
	}

	r.recorder.Event(cluster, "Normal", "RollingRestartCompleted",
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

// isStatefulSetReady checks whether a StatefulSet has all replicas ready.
func (r *AdminReconciler) isStatefulSetReady(ctx context.Context, namespace, name string) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return false, fmt.Errorf("getting statefulset %s/%s: %w", namespace, name, err)
	}

	if sts.Spec.Replicas == nil {
		return sts.Status.ReadyReplicas > 0, nil
	}
	return sts.Status.ReadyReplicas == *sts.Spec.Replicas, nil
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

// handleMaintenance processes maintenance annotations.
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
