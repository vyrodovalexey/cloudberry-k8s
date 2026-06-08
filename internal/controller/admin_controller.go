package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/idle"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

const (
	adminControllerName = "admin-controller"

	// requeueAfterShort is used when waiting for a rolling restart phase to complete.
	requeueAfterShort = 5 * time.Second

	// configMapPropagationDelay is the time to wait for Kubernetes ConfigMap volume
	// propagation before calling pg_reload_conf(). Kubernetes ConfigMap volume
	// propagation typically takes up to kubelet sync period + cache propagation
	// delay (~30-60s). We use 30s to account for most environments while keeping
	// responsiveness.
	configMapPropagationDelay = 30 * time.Second

	// Rolling restart phase constants.
	restartPhaseMirrors     = "mirrors"
	restartPhasePrimaries   = "primaries"
	restartPhaseStandby     = "standby"
	restartPhaseCoordinator = "coordinator"
	restartPhaseCompleted   = "completed"

	// Backup type label values used for backup/recovery metrics.
	backupTypeFull        = "full"
	backupTypeIncremental = "incremental"

	// batchJobNameLabel is the well-known label Kubernetes sets on Job-spawned
	// pods, used to list a cleanup Job's pods when recovering its deletion count.
	batchJobNameLabel = "job-name"

	// retentionDeletedMarkerPrefix is the stdout/termination-message prefix the
	// cleanup script emits with the number of deleted backups (see the builder's
	// retentionDeletedMarker); the controller parses it to patch the
	// avsoft.io/backup-retention-deleted annotation.
	retentionDeletedMarkerPrefix = "RETENTION_DELETED="

	// backupTimestampLayout is the gpbackup-style YYYYMMDDHHMMSS timestamp layout,
	// used to derive a valid 14-digit timestamp for CronJob-spawned backup Jobs.
	backupTimestampLayout = "20060102150405"

	// Human-readable backup Job statuses recorded in cluster.Status.LastBackupStatus
	// and the BackupHistory entries.
	backupStatusSuccess    = "Success"
	backupStatusFailed     = "Failed"
	backupStatusInProgress = "InProgress"

	// Backup Job status codes for the cloudberry_backup_job_status gauge.
	backupJobStatusPending   = 0.0
	backupJobStatusRunning   = 1.0
	backupJobStatusSucceeded = 2.0
	backupJobStatusFailed    = 3.0

	// Backup last-status codes for the cloudberry_backup_last_status gauge.
	backupLastStatusSuccess    = 0.0
	backupLastStatusFailed     = 1.0
	backupLastStatusInProgress = 2.0

	// patchKeyMetadata is the JSON key for metadata in MergePatch payloads.
	patchKeyMetadata = "metadata"
	// patchKeyAnnotations is the JSON key for annotations in MergePatch payloads.
	patchKeyAnnotations = "annotations"

	// secretKeyPassword is the key used for password data in Kubernetes Secrets.
	secretKeyPassword = "password"

	// backupDestinationTypeS3 is the S3 backup destination discriminator.
	backupDestinationTypeS3 = "s3"
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
	// idleDaemon is the idle session enforcement daemon, started when idle rules are present.
	idleDaemon *idle.Daemon
	// idleDaemonMu protects idleDaemon access.
	idleDaemonMu sync.Mutex
	// vault is an optional Vault client used to source backup S3 credentials from
	// a Vault path (spec.backup.destination.s3.vaultSecret). It may be nil; when
	// nil the vaultSecret path logs a warning and is skipped. All usage is
	// nil-safe, mirroring the optional metrics-recorder pattern.
	vault vault.Client
}

// NewAdminReconciler creates a new AdminReconciler.
//
// An optional Vault client may be supplied as the final variadic argument to
// enable sourcing backup S3 credentials from a Vault path
// (spec.backup.destination.s3.vaultSecret). When omitted (or nil), the
// vaultSecret credential path is skipped with a warning. This is kept variadic
// so existing call sites that do not need Vault continue to compile unchanged.
func NewAdminReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	dbFactory db.DBClientFactory,
	m metrics.Recorder,
	logger *slog.Logger,
	vaultClient ...vault.Client,
) *AdminReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	r := &AdminReconciler{
		client:    c,
		scheme:    scheme,
		recorder:  recorder,
		builder:   b,
		dbFactory: dbFactory,
		metrics:   m,
		logger:    logger.With("controller", adminControllerName),
	}
	if len(vaultClient) > 0 {
		r.vault = vaultClient[0]
	}
	return r
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

	// Handle early-return cases: rolling restart, pending reload, not-running, unchanged generation.
	if result, handled, err := r.handleAdminEarlyReturns(ctx, cluster); handled {
		return result, err
	}

	// Reconcile configuration parameters.
	logger.Debug("reconciling configuration parameters")
	if err := r.reconcileConfig(ctx, cluster); err != nil {
		return r.handleConfigError(ctx, logger, cluster, err)
	}

	// Reconcile all sub-components and patch status.
	// Sub-component errors are non-fatal: they are logged individually and
	// aggregated here for observability, but do not block the reconcile loop.
	logger.Debug("reconciling sub-components")
	if subErr := r.reconcileSubComponents(ctx, logger, cluster); subErr != nil {
		logger.Warn("some sub-components failed to reconcile", "error", subErr)
	}

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

	// If the exporter role isn't ready yet, requeue sooner to retry.
	if !isExporterRoleReady(cluster) {
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil
	}

	return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
}

// isExporterRoleReady returns true if the exporter role is already set up
// or query monitoring is disabled (i.e., no exporter role setup is needed).
func isExporterRoleReady(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Annotations[util.AnnotationExporterRoleReady] == "true" ||
		cluster.Spec.QueryMonitoring == nil || !cluster.Spec.QueryMonitoring.Enabled
}

// handleAdminEarlyReturns checks for conditions that should short-circuit the
// admin reconciliation: in-progress rolling restart, pending config reload,
// cluster not running, unchanged generation, and maintenance annotations.
// Returns (result, true, err) if the reconciliation should stop, or (_, false, nil) to continue.
func (r *AdminReconciler) handleAdminEarlyReturns(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (result ctrl.Result, handled bool, err error) {
	logger := util.LoggerFromContext(ctx)

	// Check for in-progress rolling restart (must continue regardless of generation).
	if _, hasRestart := cluster.Annotations[util.AnnotationRollingRestart]; hasRestart {
		logger.Debug("continuing in-progress rolling restart")
		result, err := r.continueRollingRestart(ctx, cluster)
		return result, true, err
	}

	// Check for pending config reload. If the generation changed (new config applied),
	// skip the pending reload and let reconcileConfig handle the new change.
	if cluster.Status.ObservedGeneration == cluster.Generation {
		if result, handled := r.completePendingReload(ctx, cluster); handled {
			return result, true, nil
		}
	}

	// Skip if cluster is not running.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		logger.Debug("cluster not running, deferring admin reconciliation",
			"phase", cluster.Status.Phase)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, true, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches),
	// there are no maintenance annotations pending, and the exporter role is set up
	// (or query monitoring is disabled).
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Annotations[util.AnnotationMaintenance] == "" &&
		isExporterRoleReady(cluster) {
		logger.Debug("skipping admin reconciliation, generation unchanged")
		// The generation gate only short-circuits SPEC-DRIVEN reconciliation.
		// Time/Job-derived status (e.g. backup status from completed Jobs) must
		// still be refreshed on the periodic requeue; otherwise, once the cluster
		// reaches steady state, backup status would never be populated after a
		// scheduled/on-demand backup Job succeeds.
		r.refreshBackupStatusOnSteadyState(ctx, cluster)
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, true, nil
	}

	// Handle maintenance annotations.
	if maintenance, ok := cluster.Annotations[util.AnnotationMaintenance]; ok {
		logger.Debug("handling maintenance annotation", "operation", maintenance)
		result, err := r.handleMaintenance(ctx, cluster, maintenance)
		return result, true, err
	}

	return ctrl.Result{}, false, nil
}

// refreshBackupStatusOnSteadyState refreshes the backup-related status fields
// from completed backup/restore Jobs and persists them, even when the spec
// generation is unchanged (steady state). This is required because the
// generation gate in handleAdminEarlyReturns short-circuits spec-driven
// reconciliation (which normally runs refreshBackupStatus via reconcileBackup),
// but time/Job-derived status must still be refreshed on each periodic requeue.
// Errors are handled non-fatally (logged and ignored), mirroring how the main
// reconcile path treats sub-component failures.
func (r *AdminReconciler) refreshBackupStatusOnSteadyState(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		return
	}

	logger := util.LoggerFromContext(ctx)

	if err := r.refreshBackupStatus(ctx, cluster); err != nil {
		logger.Warn("failed to refresh backup status on steady-state reconcile", "error", err)
		return
	}

	// Retention cleanup is Job-derived (it reacts to the newest Succeeded backup
	// Job), so like backup status it must also run on the steady-state periodic
	// reconcile — otherwise, once the cluster settles (generation unchanged), the
	// per-backup retention cleanup Job would never be created (Scenario 79d).
	// Non-fatal: log and continue so a cleanup hiccup never blocks status persist.
	if err := r.ensureRetentionCleanup(ctx, cluster); err != nil {
		logger.Warn("failed to ensure retention cleanup on steady-state reconcile", "error", err)
	}

	// Post-restore validation is Job-derived (it reacts to the newest Succeeded
	// restore Job and to validation Job terminal status), so like backup status
	// and retention cleanup it must also run on the steady-state periodic
	// reconcile — otherwise, once the cluster settles (generation unchanged), the
	// per-restore validation Job would never be created and its outcome metric/
	// event would never be recorded (Scenario 80d). Non-fatal: log and continue.
	if err := r.ensurePostRestoreValidation(ctx, cluster); err != nil {
		logger.Warn("failed to ensure post-restore validation on steady-state reconcile", "error", err)
	}
	if err := r.observeValidationJobs(ctx, cluster); err != nil {
		logger.Warn("failed to observe validation jobs on steady-state reconcile", "error", err)
	}

	// Persist refreshed backup status. MergePatch is used so already-set fields
	// written by other controllers (e.g. cronJobName) are not clobbered, and a
	// status-only change does not bump the spec generation (status subresource).
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		logger.Warn("failed to patch backup status on steady-state reconcile", "error", err)
	}
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
// Each sub-reconciler modifies cluster.Status in-place; individual errors are
// logged and collected. The aggregated error is returned so the caller can
// decide how to handle partial failures (e.g., set a status condition).
func (r *AdminReconciler) reconcileSubComponents(
	ctx context.Context,
	logger *slog.Logger,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	var errs []error
	// Query monitoring runs first because it creates K8s resources (Secret,
	// ConfigMap, DaemonSet, Service) that don't require a DB connection.
	// Workload reconciliation may block on DB connection attempts.
	if err := r.reconcileQueryMonitoring(ctx, cluster); err != nil {
		logger.Error("failed to reconcile query monitoring", "error", err)
		errs = append(errs, err)
	}
	if err := r.reconcileWorkload(ctx, cluster); err != nil {
		logger.Error("failed to reconcile workload management", "error", err)
		errs = append(errs, err)
	}
	if err := r.reconcileBackup(ctx, cluster); err != nil {
		logger.Error("failed to reconcile backup", "error", err)
		errs = append(errs, err)
	}
	if err := r.reconcileDataLoading(ctx, cluster); err != nil {
		logger.Error("failed to reconcile data loading", "error", err)
		errs = append(errs, err)
	}
	if err := r.reconcileStorage(ctx, cluster); err != nil {
		logger.Error("failed to reconcile storage management", "error", err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
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
		return r.cleanupWorkload(ctx, cluster)
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

	// 3.5. Start/update idle session daemon if idle rules are present.
	if r.dbFactory != nil {
		r.startOrUpdateIdleDaemon(ctx, cluster)
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
		// Map IOLimits from CRD spec to DB options.
		if len(rg.IOLimits) > 0 {
			opts.IOLimits = make([]db.IOLimitOption, len(rg.IOLimits))
			for i, iol := range rg.IOLimits {
				opts.IOLimits[i] = db.IOLimitOption{
					Tablespace:       iol.Tablespace,
					ReadBytesPerSec:  iol.ReadBytesPerSec,
					WriteBytesPerSec: iol.WriteBytesPerSec,
					ReadIOPS:         iol.ReadIOPS,
					WriteIOPS:        iol.WriteIOPS,
				}
			}
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
	if desired.MinCost != actual.MinCost {
		return true
	}
	// Compare IOLimits: if desired has IOLimits, always trigger ALTER
	// (we can't easily read back io_limit from the DB in a structured way).
	// This is safe because ALTER RESOURCE GROUP ... SET io_limit is idempotent.
	if len(desired.IOLimits) > 0 {
		return true
	}
	return false
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

	// Sort rules by priority (lowest number first), preserving CRD spec order
	// for rules with the same priority (stable sort).
	sortedRules := make([]cbv1alpha1.WorkloadRule, len(cluster.Spec.Workload.Rules))
	copy(sortedRules, cluster.Spec.Workload.Rules)
	sort.SliceStable(sortedRules, func(i, j int) bool {
		return sortedRules[i].Priority < sortedRules[j].Priority
	})

	// Record a metric for each workload rule/action being applied.
	for i := range sortedRules {
		rule := &sortedRules[i]
		r.metrics.RecordWorkloadRuleAction(
			cluster.Name, cluster.Namespace, rule.Name, rule.Action,
		)
	}

	rulesJSON, err := json.Marshal(sortedRules)
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
				util.LabelManagedBy:           util.LabelManagedByValue,
				"app.kubernetes.io/component": "workload-rules",
				"app.kubernetes.io/instance":  cluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, cbv1alpha1.GroupVersion.WithKind("CloudberryCluster")),
			},
		},
		Data: make(map[string]string),
	}
}

// cleanupWorkload handles the transition to workload-disabled state.
// It drops user-created resource groups from the database, deletes the
// workload-rules ConfigMap, stops the idle daemon, zeros out metrics,
// and updates the WorkloadConfigured condition to False.
func (r *AdminReconciler) cleanupWorkload(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)
	logger.Info("cleaning up workload management (disabled)")

	// 1. Drop user-created resource groups from DB (best-effort, with timeout).
	if r.dbFactory != nil {
		dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
		dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
		if err == nil {
			r.dropAllUserResourceGroups(dbCtx, cluster, dbClient, logger)
			dbClient.Close()
		} else {
			logger.Warn("cannot connect to DB for resource group cleanup (will retry)", "error", err)
		}
		dbCancel()
	}

	// 2. Delete workload-rules ConfigMap.
	if err := r.deleteWorkloadRulesConfigMap(ctx, cluster); err != nil {
		return fmt.Errorf("deleting workload-rules ConfigMap: %w", err)
	}

	// 3. Stop idle daemon if running.
	r.stopIdleDaemon()

	// 4. Update condition.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionWorkloadConfigured),
		metav1.ConditionFalse,
		"WorkloadDisabled",
		"Workload management is disabled",
	)

	// 5. Emit event.
	r.recorder.Event(cluster, corev1.EventTypeNormal,
		cbv1alpha1.EventReasonWorkloadDisabled,
		"Workload management disabled: resource groups dropped, rules cleared")

	return nil
}

// dropAllUserResourceGroups drops all user-created resource groups from the database.
// System groups (default_group, admin_group, system_group) are excluded by ListResourceGroups.
// Errors are logged but do not fail the cleanup.
func (r *AdminReconciler) dropAllUserResourceGroups(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	logger *slog.Logger,
) {
	groups, err := dbClient.ListResourceGroups(ctx)
	if err != nil {
		logger.Warn("failed to list resource groups for cleanup", "error", err)
		return
	}

	for _, g := range groups {
		if dropErr := dbClient.DropResourceGroup(ctx, g.Name); dropErr != nil {
			logger.Warn("failed to drop resource group during cleanup",
				"group", g.Name, "error", dropErr)
		} else {
			logger.Info("dropped resource group during workload cleanup", "group", g.Name)
			// Zero out metrics for this specific group.
			r.metrics.SetResourceGroupUsage(cluster.Name, cluster.Namespace, g.Name, 0, 0)
		}
	}
}

// deleteWorkloadRulesConfigMap deletes the workload-rules ConfigMap for the cluster.
// If the ConfigMap does not exist, this is a no-op.
func (r *AdminReconciler) deleteWorkloadRulesConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	cm := &corev1.ConfigMap{}
	cmName := types.NamespacedName{
		Name:      cluster.Name + "-workload-rules",
		Namespace: cluster.Namespace,
	}

	if err := r.client.Get(ctx, cmName, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Already gone — no-op.
		}
		return fmt.Errorf("getting workload-rules ConfigMap: %w", err)
	}

	if err := r.client.Delete(ctx, cm); err != nil {
		return fmt.Errorf("deleting workload-rules ConfigMap %s: %w", cmName.Name, err)
	}

	logger.Info("deleted workload-rules ConfigMap", "name", cmName.Name)
	return nil
}

// stopIdleDaemon stops the idle session daemon if it is running.
func (r *AdminReconciler) stopIdleDaemon() {
	r.idleDaemonMu.Lock()
	defer r.idleDaemonMu.Unlock()

	if r.idleDaemon != nil {
		r.idleDaemon.Stop()
		r.idleDaemon = nil
	}
}

// startOrUpdateIdleDaemon starts the idle daemon if idle rules are present,
// or updates the rules if the daemon is already running.
func (r *AdminReconciler) startOrUpdateIdleDaemon(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	if len(cluster.Spec.Workload.IdleRules) == 0 {
		r.stopIdleDaemon()
		return
	}

	rules, err := idle.ParseIdleRules(cluster.Spec.Workload.IdleRules)
	if err != nil {
		r.logger.Error("failed to parse idle rules", "error", err)
		return
	}

	// Check if any rule is enabled.
	hasEnabled := false
	for i := range rules {
		if rules[i].Enabled {
			hasEnabled = true
			break
		}
	}
	if !hasEnabled {
		r.stopIdleDaemon()
		return
	}

	r.idleDaemonMu.Lock()
	defer r.idleDaemonMu.Unlock()

	if r.idleDaemon != nil {
		// Daemon already running — just update rules.
		r.idleDaemon.UpdateRules(rules)
		return
	}

	// Create and start a new daemon.
	// The daemon needs its own DB client that won't be closed by the reconciler.
	daemonDBClient, dbErr := r.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		r.logger.Error("failed to create DB client for idle daemon", "error", dbErr)
		return
	}

	// Create a factory adapter so the daemon can reconnect on connection failures.
	daemonFactory := &idleDaemonDBClientFactory{
		dbFactory: r.dbFactory,
		cluster:   cluster.DeepCopy(),
	}

	d := idle.New(idle.Config{
		ClusterName:     cluster.Name,
		Namespace:       cluster.Namespace,
		ScanInterval:    idle.DefaultScanInterval,
		DBClient:        daemonDBClient,
		DBClientFactory: daemonFactory,
		Metrics:         r.metrics,
		Logger:          r.logger,
	})
	d.UpdateRules(rules)

	// Ensure the DB client is closed if daemon start fails or panics,
	// preventing resource leaks.
	started := false
	defer func() {
		if !started {
			daemonDBClient.Close()
			r.logger.Warn("closed DB client after idle daemon failed to start")
		}
	}()

	d.Start(ctx)
	started = true
	r.idleDaemon = d
}

// idleDaemonDBClientFactory adapts db.DBClientFactory to idle.DBClientFactory
// so the idle daemon can reconnect to the database on connection failures.
type idleDaemonDBClientFactory struct {
	dbFactory db.DBClientFactory
	cluster   *cbv1alpha1.CloudberryCluster
}

// NewClient creates a new database client using the stored cluster reference.
func (f *idleDaemonDBClientFactory) NewClient(ctx context.Context) (db.Client, error) {
	return f.dbFactory.NewClient(ctx, f.cluster)
}

// reconcileQueryMonitoring reconciles query monitoring configuration by creating
// and updating the required Kubernetes resources: exporter credentials Secret,
// exporter queries ConfigMap, node exporter DaemonSet, exporter Service,
// ServiceMonitor, and PrometheusRule. It also sets up the database exporter role
// when a DB client factory is available.
func (r *AdminReconciler) reconcileQueryMonitoring(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.QueryMonitoring == nil || !cluster.Spec.QueryMonitoring.Enabled {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	qm := cluster.Spec.QueryMonitoring

	logger.Info("reconciling query monitoring",
		"historyRetention", qm.HistoryRetention,
		"samplingInterval", qm.SamplingInterval,
		"guestAccess", qm.GuestAccess,
		"planCollection", qm.PlanCollection,
		"slowQueryThreshold", qm.SlowQueryThreshold,
	)

	r.logQueryMonitoringExporters(logger, cluster)

	// Retrieve or generate the exporter password.
	password, err := r.resolveExporterPassword(ctx, cluster)
	if err != nil {
		return fmt.Errorf("resolving exporter password: %w", err)
	}

	// Construct the DSN for postgres_exporter.
	port := resolveExporterDSNPort(cluster)
	dsn := fmt.Sprintf("postgresql://cloudberry_exporter:%s@localhost:%d/postgres?sslmode=disable",
		url.QueryEscape(password), port)

	// Create/update core exporter resources.
	if err := r.ensureExporterCoreResources(ctx, cluster, password, dsn, logger); err != nil {
		return err
	}

	// Setup DB exporter role if DB client factory is available.
	if r.dbFactory != nil {
		r.setupExporterRole(ctx, cluster, password, logger)
	}

	// Create/update optional exporter resources (DaemonSet).
	if isNodeExporterEnabled(qm) {
		if err := r.ensureNodeExporterDaemonSet(ctx, cluster, logger); err != nil {
			logger.Warn("failed to create node exporter DaemonSet", "error", err)
		}
	}

	// Fetch live query counts from the database and patch status explicitly
	// (MergePatch with omitempty would skip zero values, leaving stale counts).
	if r.dbFactory != nil {
		r.updateQueryStatusFromDB(ctx, cluster, logger)
		if patchErr := r.patchQueryStatus(ctx, cluster); patchErr != nil {
			logger.Warn("failed to patch query status", "error", patchErr)
		}
	}

	// Update query monitoring metrics.
	r.metrics.SetActiveQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.ActiveQueries))
	r.metrics.SetQueuedQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.QueuedQueries))
	r.metrics.SetBlockedQueries(cluster.Name, cluster.Namespace, float64(cluster.Status.BlockedQueries))

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonQueryMonitoringReconciled,
		"Query monitoring configuration reconciled")

	return nil
}

// logQueryMonitoringExporters logs the exporter configuration details for
// query monitoring, including ServiceMonitor and PrometheusRule settings.
func (r *AdminReconciler) logQueryMonitoringExporters(
	logger *slog.Logger,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	qm := cluster.Spec.QueryMonitoring
	if qm.Exporters == nil {
		return
	}

	r.logExporterConfig(logger, "postgresExporter", qm.Exporters.PostgresExporter)
	r.logExporterConfig(logger, "nodeExporter", qm.Exporters.NodeExporter)
	r.logExporterConfig(logger, "cloudberryQueryExporter", qm.Exporters.CloudberryQueryExporter)

	r.logServiceMonitorConfig(logger, qm.Exporters.ServiceMonitor, cluster.Namespace)
	r.logPrometheusRuleConfig(logger, qm.Exporters.PrometheusRule, cluster.Namespace)
}

// logServiceMonitorConfig logs the ServiceMonitor configuration if present.
func (r *AdminReconciler) logServiceMonitorConfig(
	logger *slog.Logger,
	sm *cbv1alpha1.QueryServiceMonitorSpec,
	defaultNamespace string,
) {
	if sm == nil {
		return
	}
	ns := sm.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
	logger.Info("query monitoring ServiceMonitor config",
		"enabled", sm.Enabled,
		"namespace", ns,
		"interval", sm.Interval,
		"scrapeTimeout", sm.ScrapeTimeout,
		"labels", sm.Labels,
	)
}

// logPrometheusRuleConfig logs the PrometheusRule configuration if present.
func (r *AdminReconciler) logPrometheusRuleConfig(
	logger *slog.Logger,
	pr *cbv1alpha1.QueryPrometheusRuleSpec,
	defaultNamespace string,
) {
	if pr == nil {
		return
	}
	ns := pr.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
	logger.Info("query monitoring PrometheusRule config",
		"enabled", pr.Enabled,
		"namespace", ns,
		"labels", pr.Labels,
	)
}

// ensureExporterCoreResources creates or updates the core exporter resources:
// credentials Secret, queries ConfigMap, and exporter Service.
func (r *AdminReconciler) ensureExporterCoreResources(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	password, dsn string,
	logger *slog.Logger,
) error {
	if err := r.ensureExporterCredentialsSecret(ctx, cluster, password, dsn, logger); err != nil {
		return err
	}
	if err := r.ensureExporterQueriesConfigMap(ctx, cluster, logger); err != nil {
		return err
	}
	return r.ensureExporterService(ctx, cluster, logger)
}

// isNodeExporterEnabled returns true if the node exporter is configured and enabled.
func isNodeExporterEnabled(qm *cbv1alpha1.QueryMonitoringSpec) bool {
	return qm.Exporters != nil && qm.Exporters.NodeExporter != nil && qm.Exporters.NodeExporter.Enabled
}

// resolveExporterPassword retrieves the exporter password from an existing Secret,
// or generates a new one if the Secret does not exist yet.
func (r *AdminReconciler) resolveExporterPassword(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (string, error) {
	secretName := util.ExporterCredentialsSecretName(cluster.Name)
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}

	err := r.client.Get(ctx, key, existing)
	if err == nil {
		// Secret exists — reuse the stored password.
		if pw, ok := existing.Data[secretKeyPassword]; ok && len(pw) > 0 {
			return string(pw), nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("getting exporter credentials secret: %w", err)
	}

	// Secret does not exist or has no password — generate a new one.
	password, genErr := util.GenerateRandomPassword()
	if genErr != nil {
		return "", fmt.Errorf("generating exporter password: %w", genErr)
	}
	return password, nil
}

// resolveExporterDSNPort returns the coordinator port for the exporter DSN.
func resolveExporterDSNPort(cluster *cbv1alpha1.CloudberryCluster) int32 {
	if cluster.Spec.Coordinator.Port != 0 {
		return cluster.Spec.Coordinator.Port
	}
	return int32(util.DefaultCoordinatorPort)
}

// ensureExporterCredentialsSecret creates or updates the exporter credentials Secret.
func (r *AdminReconciler) ensureExporterCredentialsSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	password, dsn string,
	logger *slog.Logger,
) error {
	secretName := util.ExporterCredentialsSecretName(cluster.Name)
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}

	err := r.client.Get(ctx, key, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildExporterCredentialsSecret(cluster, password, dsn)
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating exporter credentials secret: %w", createErr)
		}
		logger.Info("created exporter credentials secret", "name", secretName)
	case err != nil:
		return fmt.Errorf("getting exporter credentials secret: %w", err)
	default:
		desired := r.builder.BuildExporterCredentialsSecret(cluster, password, dsn)
		existing.Data = desired.Data
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating exporter credentials secret: %w", updateErr)
		}
		logger.Info("updated exporter credentials secret", "name", secretName)
	}

	return nil
}

// ensureExporterQueriesConfigMap creates or updates the exporter queries ConfigMap.
func (r *AdminReconciler) ensureExporterQueriesConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) error {
	cmName := util.ExporterQueriesConfigMapName(cluster.Name)
	existing := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: cmName, Namespace: cluster.Namespace}

	err := r.client.Get(ctx, key, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildExporterQueriesConfigMap(cluster)
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating exporter queries configmap: %w", createErr)
		}
		logger.Info("created exporter queries configmap", "name", cmName)
	case err != nil:
		return fmt.Errorf("getting exporter queries configmap: %w", err)
	default:
		desired := r.builder.BuildExporterQueriesConfigMap(cluster)
		existing.Data = desired.Data
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating exporter queries configmap: %w", updateErr)
		}
		logger.Info("updated exporter queries configmap", "name", cmName)
	}

	return nil
}

// setupExporterRole creates the database exporter role using the DB client.
// Uses a short timeout to avoid blocking the reconciliation loop.
// On success, sets the AnnotationExporterRoleReady annotation so the
// admin-controller stops retrying. On failure, the annotation stays absent
// and the controller will retry on the next reconcile cycle.
func (r *AdminReconciler) setupExporterRole(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	password string,
	logger *slog.Logger,
) {
	// Use a short timeout so DB connection issues don't block the entire reconcile.
	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		logger.Warn("failed to create DB client for exporter role setup (will retry)", "error", err)
		return
	}
	defer dbClient.Close()

	if setupErr := dbClient.SetupExporterRole(dbCtx, password); setupErr != nil {
		logger.Warn("failed to setup exporter role (will retry)", "error", setupErr)
		return
	}

	logger.Info("exporter role configured successfully")

	// Mark the role as ready so the admin-controller stops retrying.
	if setErr := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationExporterRoleReady, "true"); setErr != nil {
		logger.Warn("failed to set exporter-role-ready annotation", "error", setErr)
	}
}

// updateQueryStatusFromDB queries pg_stat_activity for live query counts
// and updates the cluster status fields. Errors are logged but non-fatal.
func (r *AdminReconciler) updateQueryStatusFromDB(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		logger.Debug("skipping query status update, DB not available", "error", err)
		return
	}
	defer dbClient.Close()

	active, queued, blocked, err := dbClient.GetActiveQueryCount(dbCtx)
	if err != nil {
		logger.Warn("failed to get active query counts", "error", err)
		return
	}

	cluster.Status.ActiveQueries = active
	cluster.Status.QueuedQueries = queued
	cluster.Status.BlockedQueries = blocked

	logger.Info("updated query status from database",
		"activeQueries", active,
		"queuedQueries", queued,
		"blockedQueries", blocked,
	)

	// Record the number of active database connections (sessions) from real data
	// and detect any currently running slow queries.
	if sessions, sessErr := dbClient.ListSessions(dbCtx); sessErr != nil {
		logger.Debug("failed to list sessions for connection metrics", "error", sessErr)
	} else {
		r.metrics.SetConnectionsActive(cluster.Name, cluster.Namespace, float64(len(sessions)))
		r.recordSlowQueries(cluster, sessions)
	}
}

// recordSlowQueries inspects active sessions and records a slow-query metric for
// each running query whose elapsed time exceeds the configured SlowQueryThreshold.
func (r *AdminReconciler) recordSlowQueries(
	cluster *cbv1alpha1.CloudberryCluster,
	sessions []db.Session,
) {
	qm := cluster.Spec.QueryMonitoring
	if qm == nil || qm.SlowQueryThreshold == "" {
		return
	}
	threshold, err := time.ParseDuration(qm.SlowQueryThreshold)
	if err != nil || threshold <= 0 {
		return
	}
	now := time.Now()
	for i := range sessions {
		s := &sessions[i]
		if s.State != "active" || s.QueryStart.IsZero() {
			continue
		}
		if now.Sub(s.QueryStart) >= threshold {
			r.metrics.RecordSlowQuery(cluster.Name, cluster.Namespace)
		}
	}
}

// patchQueryStatus explicitly patches the query count status fields.
// This is needed because the standard patchStatus uses json.Marshal with
// omitempty, which omits zero values and leaves stale counts in the CR.
func (r *AdminReconciler) patchQueryStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	patch, err := json.Marshal(map[string]interface{}{
		patchKeyStatus: map[string]interface{}{
			"activeQueries":  cluster.Status.ActiveQueries,
			"queuedQueries":  cluster.Status.QueuedQueries,
			"blockedQueries": cluster.Status.BlockedQueries,
		},
	})
	if err != nil {
		return fmt.Errorf("marshaling query status patch: %w", err)
	}
	return r.client.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch))
}

// ensureNodeExporterDaemonSet creates or updates the node exporter DaemonSet.
func (r *AdminReconciler) ensureNodeExporterDaemonSet(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) error {
	dsName := util.NodeExporterDaemonSetName(cluster.Name)
	existing := &appsv1.DaemonSet{}
	key := types.NamespacedName{Name: dsName, Namespace: cluster.Namespace}

	err := r.client.Get(ctx, key, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildNodeExporterDaemonSet(cluster)
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating node exporter daemonset: %w", createErr)
		}
		logger.Info("created node exporter daemonset", "name", dsName)
	case err != nil:
		return fmt.Errorf("getting node exporter daemonset: %w", err)
	default:
		desired := r.builder.BuildNodeExporterDaemonSet(cluster)
		existing.Spec = desired.Spec
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating node exporter daemonset: %w", updateErr)
		}
		logger.Info("updated node exporter daemonset", "name", dsName)
	}

	return nil
}

// ensureExporterService creates or updates the exporter metrics Service.
func (r *AdminReconciler) ensureExporterService(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) error {
	svcName := util.ExporterMetricsServiceName(cluster.Name)
	existing := &corev1.Service{}
	key := types.NamespacedName{Name: svcName, Namespace: cluster.Namespace}

	err := r.client.Get(ctx, key, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildExporterService(cluster)
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating exporter service: %w", createErr)
		}
		logger.Info("created exporter service", "name", svcName)
	case err != nil:
		return fmt.Errorf("getting exporter service: %w", err)
	default:
		desired := r.builder.BuildExporterService(cluster)
		existing.Spec.Ports = desired.Spec.Ports
		existing.Spec.Selector = desired.Spec.Selector
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating exporter service: %w", updateErr)
		}
		logger.Info("updated exporter service", "name", svcName)
	}

	return nil
}

// logExporterConfig logs the configuration of a monitoring exporter.
func (r *AdminReconciler) logExporterConfig(logger *slog.Logger, name string, spec *cbv1alpha1.ExporterSpec) {
	if spec == nil {
		return
	}
	logger.Info("query monitoring exporter config",
		"exporter", name,
		"enabled", spec.Enabled,
		"image", spec.Image,
		"port", spec.Port,
	)
	if spec.Resources != nil {
		logger.Info("query monitoring exporter resources",
			"exporter", name,
			"requestsCPU", spec.Resources.Requests.CPU,
			"requestsMemory", spec.Resources.Requests.Memory,
			"limitsCPU", spec.Resources.Limits.CPU,
			"limitsMemory", spec.Resources.Limits.Memory,
		)
	}
}

// backupHistoryLimit is the maximum number of entries retained in status.backupHistory.
const backupHistoryLimit = 10

// reconcileBackup reconciles backup configuration and status. When backup is
// enabled, it ensures the gpbackup_s3_plugin ConfigMap, the scheduled-backup
// CronJob (when a schedule is set) and refreshes the backup status from the most
// recent backup/restore Jobs owned by the cluster.
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
		"destination", cluster.Spec.Backup.Destination.Type,
	)

	if err := r.ensureBackupS3ConfigMap(ctx, cluster); err != nil {
		return err
	}

	// Materialize Vault-sourced S3 credentials into a Secret BEFORE any Jobs or
	// the CronJob are created, so the Job spec can reference the Secret uniformly
	// (never embedding plaintext credentials).
	if err := r.ensureBackupS3VaultCredentials(ctx, cluster); err != nil {
		return err
	}

	if err := r.ensureBackupCronJob(ctx, cluster); err != nil {
		return err
	}

	if err := r.refreshBackupStatus(ctx, cluster); err != nil {
		return err
	}

	if err := r.ensureRetentionCleanup(ctx, cluster); err != nil {
		return err
	}

	if err := r.ensurePostRestoreValidation(ctx, cluster); err != nil {
		return err
	}

	if err := r.observeValidationJobs(ctx, cluster); err != nil {
		return err
	}

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

// ensureBackupS3ConfigMap creates or updates the S3 plugin ConfigMap for S3 destinations.
func (r *AdminReconciler) ensureBackupS3ConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired := r.builder.BuildBackupS3ConfigMap(cluster)
	if desired == nil {
		// Non-S3 destination: nothing to ensure.
		return nil
	}

	existing := &corev1.ConfigMap{}
	err := r.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating backup s3 configmap %s: %w", desired.Name, createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting backup s3 configmap %s: %w", desired.Name, err)
	}

	if !equality.Semantic.DeepEqual(existing.Data, desired.Data) {
		existing.Data = desired.Data
		existing.Labels = desired.Labels
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating backup s3 configmap %s: %w", desired.Name, updateErr)
		}
	}
	return nil
}

// vaultS3CredsField extracts a string value from a Vault secret data map for the
// given field, tolerating both flat and KV-v2 "data"-nested shapes already
// normalized by the vault client. Non-string values are stringified.
func vaultS3CredsField(data map[string]interface{}, field string) (string, bool) {
	v, ok := data[field]
	if !ok {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// s3VaultCredentialFields returns the access-key and secret-key Vault field names
// for the given VaultSecret, applying the canonical defaults when unset.
func s3VaultCredentialFields(vs *cbv1alpha1.S3VaultSecret) (accessKeyField, secretKeyField string) {
	accessKeyField = vs.AccessKeyField
	if accessKeyField == "" {
		accessKeyField = util.DefaultS3AccessKeyField
	}
	secretKeyField = vs.SecretKeyField
	if secretKeyField == "" {
		secretKeyField = util.DefaultS3SecretKeyField
	}
	return accessKeyField, secretKeyField
}

// ensureBackupS3VaultCredentials reads S3 credentials from the configured Vault
// path and materializes them into a Kubernetes Secret owned by the cluster, so
// backup/restore Jobs reference a Secret uniformly without embedding plaintext.
//
// It is a no-op unless the destination is S3 with a vaultSecret configured. When
// no Vault client is wired (r.vault == nil) it logs a clear warning and skips,
// so existing construction with a nil Vault client never panics.
func (r *AdminReconciler) ensureBackupS3VaultCredentials(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	s3 := backupS3VaultSpec(cluster)
	if s3 == nil {
		return nil
	}
	logger := util.LoggerFromContext(ctx)

	if r.vault == nil || !r.vault.IsEnabled() {
		logger.Warn("backup s3 vaultSecret configured but no Vault client is available; "+
			"skipping vault credential materialization",
			"path", s3.VaultSecret.Path,
		)
		return nil
	}

	accessKeyField, secretKeyField := s3VaultCredentialFields(s3.VaultSecret)
	data, err := r.vault.ReadSecret(ctx, s3.VaultSecret.Path)
	if err != nil {
		return fmt.Errorf("reading backup s3 credentials from vault path %s: %w",
			s3.VaultSecret.Path, err)
	}
	if data == nil {
		return fmt.Errorf("no data found at vault path %s for backup s3 credentials",
			s3.VaultSecret.Path)
	}

	accessKey, ok := vaultS3CredsField(data, accessKeyField)
	if !ok {
		return fmt.Errorf("vault path %s missing access key field %q",
			s3.VaultSecret.Path, accessKeyField)
	}
	secretKey, ok := vaultS3CredsField(data, secretKeyField)
	if !ok {
		return fmt.Errorf("vault path %s missing secret key field %q",
			s3.VaultSecret.Path, secretKeyField)
	}

	return r.materializeBackupS3VaultSecret(ctx, cluster, accessKey, secretKey, logger)
}

// materializeBackupS3VaultSecret creates or updates the Kubernetes Secret holding
// the Vault-sourced S3 credentials, owner-ref'd to the cluster, using the
// canonical default field names consumed by the backup Job env.
func (r *AdminReconciler) materializeBackupS3VaultSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	accessKey, secretKey string,
	logger *slog.Logger,
) error {
	secretName := util.BackupS3VaultCredentialsSecretName(cluster.Name)
	desiredData := map[string][]byte{
		util.DefaultS3AccessKeyField: []byte(accessKey),
		util.DefaultS3SecretKeyField: []byte(secretKey),
	}

	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}
	err := r.client.Get(ctx, key, existing)
	switch {
	case apierrors.IsNotFound(err):
		desired := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: cluster.Namespace,
				Labels:    util.CommonLabels(cluster.Name, util.ComponentBackup),
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(cluster, cbv1alpha1.GroupVersion.WithKind("CloudberryCluster")),
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: desiredData,
		}
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating backup s3 vault credentials secret %s: %w", secretName, createErr)
		}
		logger.Info("materialized backup s3 vault credentials secret", "name", secretName)
	case err != nil:
		return fmt.Errorf("getting backup s3 vault credentials secret %s: %w", secretName, err)
	default:
		if !equality.Semantic.DeepEqual(existing.Data, desiredData) {
			existing.Data = desiredData
			if updateErr := r.client.Update(ctx, existing); updateErr != nil {
				return fmt.Errorf("updating backup s3 vault credentials secret %s: %w", secretName, updateErr)
			}
			logger.Info("updated backup s3 vault credentials secret", "name", secretName)
		}
	}
	return nil
}

// backupS3VaultSpec returns the S3 destination when the cluster has an S3 backup
// destination configured with a non-empty vaultSecret path; otherwise nil.
func backupS3VaultSpec(cluster *cbv1alpha1.CloudberryCluster) *cbv1alpha1.S3Destination {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		return nil
	}
	dest := cluster.Spec.Backup.Destination
	if dest.Type != backupDestinationTypeS3 || dest.S3 == nil {
		return nil
	}
	if dest.S3.VaultSecret == nil || dest.S3.VaultSecret.Path == "" {
		return nil
	}
	return dest.S3
}

// ensureBackupCronJob creates/updates the scheduled backup CronJob when a
// schedule is set, or deletes it when the schedule has been cleared.
func (r *AdminReconciler) ensureBackupCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	cronName := util.BackupCronJobName(cluster.Name)
	desired := r.builder.BuildBackupCronJob(cluster)

	if desired == nil {
		// No schedule configured: delete the CronJob if it exists.
		existing := &batchv1.CronJob{}
		err := r.client.Get(ctx, types.NamespacedName{Name: cronName, Namespace: cluster.Namespace}, existing)
		if apierrors.IsNotFound(err) {
			cluster.Status.CronJobName = ""
			return nil
		}
		if err != nil {
			return fmt.Errorf("getting backup cronjob %s: %w", cronName, err)
		}
		if delErr := r.client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return fmt.Errorf("deleting backup cronjob %s: %w", cronName, delErr)
		}
		cluster.Status.CronJobName = ""
		return nil
	}

	existing := &batchv1.CronJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating backup cronjob %s: %w", desired.Name, createErr)
		}
		cluster.Status.CronJobName = desired.Name
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting backup cronjob %s: %w", desired.Name, err)
	}

	if !equality.Semantic.DeepEqual(existing.Spec, desired.Spec) {
		existing.Spec = desired.Spec
		existing.Labels = desired.Labels
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating backup cronjob %s: %w", desired.Name, updateErr)
		}
	}
	cluster.Status.CronJobName = desired.Name
	return nil
}

// refreshBackupStatus inspects backup/restore Jobs owned by the cluster and
// updates the backup-related status fields and history from the latest Job.
func (r *AdminReconciler) refreshBackupStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	jobs := &batchv1.JobList{}
	if err := r.client.List(ctx, jobs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing backup jobs: %w", err)
	}

	// Emit per-Job status metrics for every observed backup/restore/cleanup Job
	// and record terminal metrics (restore duration, retention deletions) as Jobs
	// reach a terminal state.
	r.recordBackupJobMetrics(cluster, jobs.Items)

	latest := latestBackupJob(jobs.Items)
	if latest == nil {
		return nil
	}

	status := backupJobStatus(latest)
	r.applyBackupJobToStatus(cluster, latest, status)
	return nil
}

// ensurePostRestoreValidation creates a post-restore validation Job for each
// successfully completed restore Job that does not yet have one (spec 11
// §Post-Restore Validation). It is idempotent: the validation Job is named
// deterministically from the restore timestamp and skipped when it already
// exists. Each created Job is owner-ref'd to the cluster and emits an Event.
func (r *AdminReconciler) ensurePostRestoreValidation(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if !validationEnabled(cluster) {
		// Post-restore validation explicitly disabled via the validation config.
		return nil
	}
	jobs := &batchv1.JobList{}
	if err := r.client.List(ctx, jobs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing restore jobs for validation: %w", err)
	}

	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationRestore {
			continue
		}
		if job.Status.Succeeded == 0 {
			continue
		}
		timestamp := strings.TrimPrefix(job.Name, util.RestoreJobName(cluster.Name, ""))
		timestamp = strings.TrimPrefix(timestamp, "-")
		if timestamp == "" {
			continue
		}
		if err := r.createValidationJob(ctx, cluster, timestamp, job); err != nil {
			return err
		}
	}
	return nil
}

// validationEnabled reports whether post-restore validation Jobs should be
// created. Validation defaults to enabled (historical behavior); it is disabled
// only when the optional validation config sets Enabled to false.
func validationEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Validation == nil {
		return true
	}
	if enabled := cluster.Spec.Backup.Validation.Enabled; enabled != nil {
		return *enabled
	}
	return true
}

// createValidationJob creates a single post-restore validation Job when one does
// not already exist for the given restore timestamp. It populates the validation
// options from the cluster config and the restore Job: the expected per-table row
// counts are read from the restore Job's avsoft.io/expected-row-counts annotation
// (a JSON map captured from the gpbackup history metadata) when present, and
// RunAnalyze/HealthCheckQuery are sourced from the optional validation config or,
// for RunAnalyze, from the cluster's gprestore run-analyze intent. When the
// annotation is absent the expected counts are empty and the validation script
// falls back to a best-effort total-table probe (documented).
func (r *AdminReconciler) createValidationJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	timestamp string,
	restoreJob *batchv1.Job,
) error {
	name := util.PostRestoreValidationJobName(cluster.Name, timestamp)
	existing := &batchv1.Job{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		// Validation Job already exists: idempotent no-op.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting validation job %s: %w", name, err)
	}

	opts := &builder.ValidationJobOptions{
		Timestamp:         timestamp,
		ExpectedRowCounts: expectedRowCountsFromJob(ctx, restoreJob),
		HealthCheckQuery:  validationHealthCheckQuery(cluster),
		RunAnalyze:        validationRunAnalyze(cluster),
	}
	job := r.builder.BuildPostRestoreValidationJob(cluster, opts)
	if createErr := r.client.Create(ctx, job); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			return nil
		}
		return fmt.Errorf("creating validation job %s: %w", name, createErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonBackupReconciled,
		fmt.Sprintf("Post-restore validation Job created for timestamp %s", timestamp))
	return nil
}

// expectedRowCountsFromJob reads the expected per-table row counts from the
// restore Job's avsoft.io/expected-row-counts annotation (a JSON object of
// fully-qualified table -> count). It returns nil when the annotation is absent or
// unparsable so the validation falls back to the best-effort probe; a parse error
// is logged but never fatal.
func expectedRowCountsFromJob(ctx context.Context, job *batchv1.Job) map[string]int64 {
	if job == nil {
		return nil
	}
	raw, ok := job.Annotations[util.AnnotationExpectedRowCounts]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	counts := map[string]int64{}
	if err := json.Unmarshal([]byte(raw), &counts); err != nil {
		util.LoggerFromContext(ctx).Warn("ignoring unparsable expected-row-counts annotation",
			"job", job.Name, "error", err)
		return nil
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

// validationHealthCheckQuery resolves the validation health-check query from the
// optional validation config, defaulting to empty (the builder substitutes
// "SELECT 1").
func validationHealthCheckQuery(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Validation == nil {
		return ""
	}
	return cluster.Spec.Backup.Validation.HealthCheckQuery
}

// validationRunAnalyze resolves whether the validation Job should run ANALYZE.
// It honors the optional validation config's RunAnalyze when set, otherwise it
// inherits the cluster's gprestore run-analyze intent so post-restore planner
// stats are confirmed fresh whenever the restore itself refreshed them.
func validationRunAnalyze(cluster *cbv1alpha1.CloudberryCluster) bool {
	if cluster.Spec.Backup == nil {
		return false
	}
	if v := cluster.Spec.Backup.Validation; v != nil && v.RunAnalyze {
		return true
	}
	if gr := cluster.Spec.Backup.Gprestore; gr != nil {
		return gr.RunAnalyze
	}
	return false
}

// validationResultSuccess / validationResultFailed are the {result} label values
// for the cloudberry_restore_validation_total metric and the value recorded in
// the avsoft.io/validation-recorded annotation de-dup guard.
const (
	validationResultSuccess = "success"
	validationResultFailed  = "failed"
)

// observeValidationJobs records the post-restore validation outcome (metric +
// de-duplicated Warning Event) for every validation-operation Job that has
// reached a terminal state and has not yet been recorded. Recording is gated on
// the avsoft.io/validation-recorded annotation (patched onto the Job) so a
// finished Job is counted exactly once and the Warning Event does not storm on
// periodic reconciles. A FAILED validation Job is surfaced as a Warning but does
// NOT alter the restore status: validation runs post-restore and the restore Job
// remains Succeeded regardless of the validation outcome.
func (r *AdminReconciler) observeValidationJobs(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	jobs := &batchv1.JobList{}
	if err := r.client.List(ctx, jobs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing validation jobs: %w", err)
	}

	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationValidate {
			continue
		}
		result, terminal := validationJobResult(job)
		if !terminal {
			continue
		}
		if _, ok := job.Annotations[util.AnnotationValidationRecorded]; ok {
			// Outcome already recorded for this Job: idempotent skip.
			continue
		}
		if err := r.recordValidationOutcome(ctx, cluster, job, result); err != nil {
			return err
		}
	}
	return nil
}

// validationJobResult derives the validation result and whether the Job has
// reached a terminal state. Succeeded -> ("success", true); Failed ->
// ("failed", true); otherwise ("", false) (still running).
func validationJobResult(job *batchv1.Job) (string, bool) {
	switch {
	case job.Status.Succeeded > 0:
		return validationResultSuccess, true
	case job.Status.Failed > 0 || jobHasFailedCondition(job):
		return validationResultFailed, true
	default:
		return "", false
	}
}

// recordValidationOutcome records the validation metric for a terminal Job, emits
// a Warning Event on failure, and patches the avsoft.io/validation-recorded
// annotation so the outcome is recorded exactly once. The annotation patch is the
// commit point of the de-dup guard: the metric/event are recorded only when the
// patch succeeds, avoiding a double-count if the patch fails and the Job is
// re-observed on a later reconcile.
func (r *AdminReconciler) recordValidationOutcome(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	result string,
) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[util.AnnotationValidationRecorded] = result
	if err := r.client.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("patching validation job %s recorded annotation: %w", job.Name, err)
	}

	r.metrics.RecordRestoreValidation(cluster.Name, cluster.Namespace, result)
	if result == validationResultFailed {
		r.emitValidationFailureEvent(cluster, job)
	}
	return nil
}

// emitValidationFailureEvent emits a Warning Event for a failed post-restore
// validation Job (e.g. a row-count mismatch vs gpbackup history or an invalid
// index). It is called exactly once per Job because recordValidationOutcome only
// invokes it after the de-dup annotation has been committed, mirroring the
// transition de-dup of emitRestoreFailureEvent without producing event storms.
func (r *AdminReconciler) emitValidationFailureEvent(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) {
	timestamp := strings.TrimPrefix(job.Name, util.PostRestoreValidationJobName(cluster.Name, ""))
	timestamp = strings.TrimPrefix(timestamp, "-")
	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonValidationFailed,
		fmt.Sprintf("Post-restore validation job %s failed (timestamp %s); "+
			"the restore remains successful", job.Name, timestamp))
}

// retentionPolicyActive reports whether the cluster has any retention policy
// configured (count-based or time-based). Cleanup is a no-op otherwise.
func retentionPolicyActive(cluster *cbv1alpha1.CloudberryCluster) bool {
	if cluster.Spec.Backup == nil {
		return false
	}
	r := cluster.Spec.Backup.Retention
	return r.FullCount > 0 || r.IncrementalCount > 0 || r.MaxAge != ""
}

// ensureRetentionCleanup creates a retention cleanup Job after the newest
// successful backup and feeds the cleanup Job's deletion count into the retention
// metric (spec 11 / Scenario 79). It is a no-op when backup is disabled or no
// retention policy is set. The cleanup Job name is keyed off the latest
// successful backup timestamp (util.RetentionCleanupJobName), so a Get-before-
// Create makes creation idempotent: cleanup runs exactly once per successful
// backup. After a cleanup Job has Succeeded, its deletion count is read from the
// terminating pod and patched onto the Job as the
// avsoft.io/backup-retention-deleted annotation (once), which the existing
// metrics loop turns into cloudberry_backup_retention_deleted_total.
func (r *AdminReconciler) ensureRetentionCleanup(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		return nil
	}
	if !retentionPolicyActive(cluster) {
		return nil
	}

	jobs := &batchv1.JobList{}
	if err := r.client.List(ctx, jobs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing jobs for retention cleanup: %w", err)
	}

	latest := latestSucceededBackupJob(jobs.Items)
	if latest == nil {
		// No successful backup yet: nothing to clean up.
		return nil
	}
	timestamp := backupTimestampFromJob(cluster, latest)
	if timestamp == "" {
		return nil
	}

	if err := r.createRetentionCleanupJob(ctx, cluster, timestamp); err != nil {
		return err
	}
	return r.reconcileRetentionCleanupAnnotations(ctx, cluster, jobs.Items)
}

// latestSucceededBackupJob returns the most recently created Succeeded
// backup-operation Job (operation==backup, Succeeded>0), or nil when none exists.
func latestSucceededBackupJob(jobs []batchv1.Job) *batchv1.Job {
	var latest *batchv1.Job
	for i := range jobs {
		job := &jobs[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationBackup {
			continue
		}
		if job.Status.Succeeded == 0 {
			continue
		}
		if latest == nil || job.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = job
		}
	}
	return latest
}

// createRetentionCleanupJob creates the retention cleanup Job for the given
// latest-backup timestamp when it does not already exist (idempotent
// Get-before-Create keyed off the deterministic cleanup Job name).
func (r *AdminReconciler) createRetentionCleanupJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	timestamp string,
) error {
	name := util.RetentionCleanupJobName(cluster.Name, timestamp)
	existing := &batchv1.Job{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		// Cleanup Job already exists for this backup: idempotent no-op.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting retention cleanup job %s: %w", name, err)
	}

	job := r.builder.BuildRetentionCleanupJob(cluster, timestamp)
	if createErr := r.client.Create(ctx, job); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			return nil
		}
		return fmt.Errorf("creating retention cleanup job %s: %w", name, createErr)
	}

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonBackupReconciled,
		fmt.Sprintf("Retention cleanup Job created for latest backup timestamp %s", timestamp))
	return nil
}

// reconcileRetentionCleanupAnnotations patches the
// avsoft.io/backup-retention-deleted annotation onto each Succeeded cleanup Job
// that does not yet carry it, reading the deletion count from the cleanup pod's
// terminated container message ("RETENTION_DELETED=<n>"). The existing metrics
// loop turns the annotation into cloudberry_backup_retention_deleted_total. It is
// non-fatal: parse/permission issues are logged and skipped so a single Job never
// blocks reconciliation.
func (r *AdminReconciler) reconcileRetentionCleanupAnnotations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	jobs []batchv1.Job,
) error {
	logger := util.LoggerFromContext(ctx)
	for i := range jobs {
		job := &jobs[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationCleanup {
			continue
		}
		if job.Status.Succeeded == 0 {
			continue
		}
		if _, ok := job.Annotations[util.AnnotationBackupRetentionDeleted]; ok {
			// Annotation already set: idempotent skip.
			continue
		}
		count, ok := r.readRetentionDeletedCount(ctx, cluster, job)
		if !ok {
			// Count not recoverable yet (pod gone / message missing): skip
			// without error so a later reconcile can retry.
			continue
		}
		if err := r.patchRetentionDeletedAnnotation(ctx, job, count); err != nil {
			logger.Warn("failed to patch retention-deleted annotation",
				"job", job.Name, "error", err)
		}
	}
	return nil
}

// patchRetentionDeletedAnnotation patches the cleanup Job with the
// avsoft.io/backup-retention-deleted annotation carrying the deletion count.
func (r *AdminReconciler) patchRetentionDeletedAnnotation(
	ctx context.Context,
	job *batchv1.Job,
	count int,
) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[util.AnnotationBackupRetentionDeleted] = strconv.Itoa(count)
	if err := r.client.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("patching cleanup job %s annotation: %w", job.Name, err)
	}
	return nil
}

// readRetentionDeletedCount recovers the number of backups deleted by a cleanup
// Job from its terminating pod. It lists the Job's pods by the job-name label and
// parses the "RETENTION_DELETED=<n>" marker from the terminated container's
// message (terminationMessagePath / FallbackToLogsOnError). Returns (count, true)
// when a count is recovered, or (0, false) when the pod or message is not
// available yet. Non-fatal: list errors are logged and reported as not-found.
func (r *AdminReconciler) readRetentionDeletedCount(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (int, bool) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list cleanup pods",
			"job", job.Name, "error", err)
		return 0, false
	}
	for i := range pods.Items {
		if n, ok := retentionDeletedFromPod(&pods.Items[i]); ok {
			return n, true
		}
	}
	return 0, false
}

// retentionDeletedFromPod extracts the deletion count from a cleanup pod's
// terminated container message, parsing the "RETENTION_DELETED=<n>" marker (or a
// bare integer written to the termination message). Returns (0, false) when no
// container has terminated with a parsable message.
func retentionDeletedFromPod(pod *corev1.Pod) (int, bool) {
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil || term.Message == "" {
			continue
		}
		if n, ok := parseRetentionDeletedMessage(term.Message); ok {
			return n, true
		}
	}
	return 0, false
}

// parseRetentionDeletedMessage parses a cleanup container's termination message
// into a deletion count. It accepts the "RETENTION_DELETED=<n>" marker (anywhere
// in the message, e.g. the FallbackToLogsOnError log tail) and a bare integer
// (the direct /dev/termination-log write). Returns (0, false) when no count is
// found.
func parseRetentionDeletedMessage(message string) (int, bool) {
	trimmed := strings.TrimSpace(message)
	if n, err := strconv.Atoi(trimmed); err == nil && n >= 0 {
		return n, true
	}
	idx := strings.LastIndex(message, retentionDeletedMarkerPrefix)
	if idx < 0 {
		return 0, false
	}
	rest := message[idx+len(retentionDeletedMarkerPrefix):]
	digits := strings.Builder{}
	for _, c := range rest {
		if c < '0' || c > '9' {
			break
		}
		digits.WriteRune(c)
	}
	if digits.Len() == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(digits.String())
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// jobHasFailedCondition reports whether the Job has a terminal Failed
// condition (status True) — e.g. reason DeadlineExceeded (activeDeadlineSeconds
// hit) or BackoffLimitExceeded. This is authoritative even when the failed-pod
// count (Status.Failed) is 0.
func jobHasFailedCondition(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// backupJobStatusCode maps a Job's status to the spec-11 numeric code used by
// the cloudberry_backup_job_status gauge (0=pending, 1=running, 2=succeeded,
// 3=failed).
func backupJobStatusCode(job *batchv1.Job) float64 {
	switch {
	case job.Status.Succeeded > 0:
		return backupJobStatusSucceeded
	case job.Status.Failed > 0 || jobHasFailedCondition(job):
		return backupJobStatusFailed
	case job.Status.Active > 0 || job.Status.StartTime != nil:
		return backupJobStatusRunning
	default:
		return backupJobStatusPending
	}
}

// recordBackupJobMetrics emits the per-Job status gauge for all backup, restore
// and cleanup Jobs, plus terminal metrics for succeeded restore and cleanup Jobs.
func (r *AdminReconciler) recordBackupJobMetrics(
	cluster *cbv1alpha1.CloudberryCluster,
	jobs []batchv1.Job,
) {
	for i := range jobs {
		job := &jobs[i]
		operation := job.Labels[util.LabelBackupOperation]
		switch operation {
		case util.BackupOperationBackup, util.BackupOperationRestore, util.BackupOperationCleanup:
		default:
			continue
		}

		code := backupJobStatusCode(job)
		r.metrics.SetBackupJobStatus(cluster.Name, cluster.Namespace, job.Name, operation, code)

		if code != backupJobStatusSucceeded {
			continue
		}
		switch operation {
		case util.BackupOperationRestore:
			if d := backupJobDurationValue(job); d > 0 {
				r.metrics.ObserveRestoreDuration(cluster.Name, cluster.Namespace, d)
			}
		case util.BackupOperationCleanup:
			if n := backupRetentionDeletedCount(job); n > 0 {
				r.metrics.RecordBackupRetentionDeleted(cluster.Name, cluster.Namespace, n)
			}
		}
	}
}

// backupRetentionDeletedCount extracts the number of backups deleted by a cleanup
// Job from its annotations, returning 0 when the count is unknown.
func backupRetentionDeletedCount(job *batchv1.Job) int {
	raw, ok := job.Annotations[util.AnnotationBackupRetentionDeleted]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// latestBackupJob returns the most recently created backup operation Job, or nil.
func latestBackupJob(jobs []batchv1.Job) *batchv1.Job {
	var latest *batchv1.Job
	for i := range jobs {
		op := jobs[i].Labels[util.LabelBackupOperation]
		if op != util.BackupOperationBackup && op != util.BackupOperationRestore {
			continue
		}
		if latest == nil || jobs[i].CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = &jobs[i]
		}
	}
	return latest
}

// backupJobStatus derives a human-readable status from a Job's status.
func backupJobStatus(job *batchv1.Job) string {
	switch {
	case job.Status.Succeeded > 0:
		return backupStatusSuccess
	case job.Status.Failed > 0 || jobHasFailedCondition(job):
		return backupStatusFailed
	default:
		return backupStatusInProgress
	}
}

// applyBackupJobToStatus updates cluster.Status backup fields, appends a history
// entry and records metrics for the given Job.
func (r *AdminReconciler) applyBackupJobToStatus(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	status string,
) {
	timestamp := backupTimestampFromJob(cluster, job)
	backupType := backupTypeFromJob(job, cluster)
	duration := backupJobDuration(job)

	// Capture the previous status/job name BEFORE overwriting so a backup
	// failure Warning Event is emitted only on a real transition into "Failed"
	// for this Job (de-duplicated across periodic reconciles of the same Job).
	prevStatus := cluster.Status.LastBackupStatus
	prevJobName := cluster.Status.LastBackupJobName

	cluster.Status.LastBackupStatus = status
	cluster.Status.LastBackupJobName = job.Name
	cluster.Status.LastBackupTimestamp = timestamp
	cluster.Status.LastBackupType = backupType
	if job.Status.CompletionTime != nil {
		cluster.Status.LastBackupTime = job.Status.CompletionTime
	} else if job.Status.StartTime != nil {
		cluster.Status.LastBackupTime = job.Status.StartTime
	}

	cluster.Status.BackupHistory = appendBackupHistory(cluster.Status.BackupHistory, cbv1alpha1.BackupHistoryEntry{
		Timestamp: timestamp,
		Type:      backupType,
		Status:    status,
		Size:      backupJobSizeHuman(job),
		Duration:  duration,
	})

	operation := job.Labels[util.LabelBackupOperation]
	if operation == util.BackupOperationRestore {
		r.metrics.RecordRestore(cluster.Name, cluster.Namespace, strings.ToLower(status))
		// Restore failures (e.g. gprestore refusing an incomplete incremental
		// set, Scenario 78d) are surfaced as a distinct RestoreFailed Warning so
		// they are observable. The backup-only BackupFailed event semantics
		// (Scenario 77) are intentionally left unchanged.
		r.emitRestoreFailureEvent(cluster, job, status, prevStatus, prevJobName)
		return
	}

	r.recordLatestBackupMetrics(cluster, job, backupType, timestamp, status)
	r.emitBackupFailureEvent(cluster, job, status, prevStatus, prevJobName)
}

// emitBackupFailureEvent emits a single de-duplicated Warning Event when a
// backup-operation Job transitions into the "Failed" state (spec 11
// §Pre-Backup Health Checks / Scenario 77). De-duplication gates on a real
// transition: the event fires only when the previously recorded status was not
// already "Failed" for this same Job name, so periodic reconciles of an
// unchanged failed Job do not produce an event storm.
func (r *AdminReconciler) emitBackupFailureEvent(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	status, prevStatus, prevJobName string,
) {
	if status != backupStatusFailed {
		return
	}
	// Only emit on a transition into Failed for this Job: skip when the same Job
	// was already recorded as Failed on a prior reconcile.
	if prevJobName == job.Name && prevStatus == backupStatusFailed {
		return
	}
	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonBackupFailed,
		fmt.Sprintf("Backup job %s failed pre-backup checks or execution (timestamp %s)",
			job.Name, backupTimestampFromJob(cluster, job)))
}

// emitRestoreFailureEvent emits a single de-duplicated Warning Event when a
// restore-operation Job transitions into the "Failed" state (spec 11 /
// Scenario 78d — e.g. gprestore refusing an incomplete incremental set). It
// mirrors emitBackupFailureEvent's de-duplication: the event fires only on a
// real transition into Failed for this Job name, so periodic reconciles of an
// unchanged failed Job do not produce an event storm. It is intentionally
// separate from EventReasonBackupFailed so backup-only semantics stay intact.
func (r *AdminReconciler) emitRestoreFailureEvent(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	status, prevStatus, prevJobName string,
) {
	if status != backupStatusFailed {
		return
	}
	// Only emit on a transition into Failed for this Job: skip when the same Job
	// was already recorded as Failed on a prior reconcile.
	if prevJobName == job.Name && prevStatus == backupStatusFailed {
		return
	}
	r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonRestoreFailed,
		fmt.Sprintf("Restore job %s failed (timestamp %s)",
			job.Name, backupTimestampFromJob(cluster, job)))
}

// recordLatestBackupMetrics wires the spec-11 backup metrics for the latest
// backup Job: the aggregate backup counter, the last-status gauge, and (on
// success) the last-success timestamp, typed duration histogram and size gauge.
func (r *AdminReconciler) recordLatestBackupMetrics(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	backupType, timestamp, status string,
) {
	name, namespace := cluster.Name, cluster.Namespace
	r.metrics.RecordBackup(name, namespace, backupType, strings.ToLower(status))

	switch status {
	case backupStatusSuccess:
		r.metrics.SetBackupLastStatus(name, namespace, backupLastStatusSuccess)
		if job.Status.CompletionTime != nil {
			r.metrics.SetBackupLastSuccessTimestamp(
				name, namespace, float64(job.Status.CompletionTime.Unix()),
			)
		}
		if d := backupJobDurationValue(job); d > 0 {
			r.metrics.ObserveBackupDuration(name, namespace, backupType, d)
		}
		if size := backupJobSizeBytes(job); size > 0 && timestamp != "" {
			r.metrics.SetBackupSizeBytes(name, namespace, timestamp, size)
		}
	case backupStatusFailed:
		r.metrics.SetBackupLastStatus(name, namespace, backupLastStatusFailed)
	default:
		r.metrics.SetBackupLastStatus(name, namespace, backupLastStatusInProgress)
	}
}

// backupJobSizeBytes extracts the backup size in bytes from a Job's annotations,
// returning 0 when the size is unknown.
func backupJobSizeBytes(job *batchv1.Job) float64 {
	raw, ok := job.Annotations[util.AnnotationBackupSizeBytes]
	if !ok {
		return 0
	}
	size, err := strconv.ParseFloat(raw, 64)
	if err != nil || size < 0 {
		return 0
	}
	return size
}

// backupJobSizeHuman returns a human-readable backup size (e.g. "2400Mi") derived
// from the Job's avsoft.io/backup-size-bytes annotation. It returns "" when the
// size is unknown (best-effort) so the omitempty BackupHistoryEntry.Size is dropped.
func backupJobSizeHuman(job *batchv1.Job) string {
	bytes := backupJobSizeBytes(job)
	if bytes <= 0 {
		return ""
	}
	return resource.NewQuantity(int64(bytes), resource.BinarySI).String()
}

// backupTimestampRegex validates a gpbackup-style YYYYMMDDHHMMSS (14-digit) timestamp.
var backupTimestampRegex = regexp.MustCompile(`^\d{14}$`)

// backupTimestampFromJob extracts a gpbackup-style 14-digit YYYYMMDDHHMMSS
// timestamp for a backup/restore Job.
//
// On-demand Jobs encode the timestamp in their name
// ("{cluster}-backup-<timestamp>"), which is parsed by prefix-trimming. CronJob
// spawned Jobs are named "{cluster}-backup-schedule-<hash>" by Kubernetes, from
// which a real 14-digit timestamp cannot be parsed; for these we fall back to the
// Job's CompletionTime (else StartTime) formatted as YYYYMMDDHHMMSS in UTC. This
// guarantees status.lastBackupTimestamp (and BackupHistoryEntry.Timestamp) is
// always a valid 14-digit value.
func backupTimestampFromJob(cluster *cbv1alpha1.CloudberryCluster, job *batchv1.Job) string {
	prefixes := []string{
		util.BackupJobName(cluster.Name, ""),
		util.RestoreJobName(cluster.Name, ""),
	}
	for _, prefix := range prefixes {
		if !strings.HasPrefix(job.Name, prefix) {
			continue
		}
		// util.BackupJobName/RestoreJobName sanitize the name, trimming the
		// trailing hyphen of the empty-timestamp prefix, so the remainder keeps a
		// leading "-" (e.g. "-20260101020000"); trim it before validating.
		parsed := strings.TrimPrefix(strings.TrimPrefix(job.Name, prefix), "-")
		if backupTimestampRegex.MatchString(parsed) {
			return parsed
		}
	}
	if ts := backupTimestampFromJobTimes(job); ts != "" {
		return ts
	}
	return cluster.Status.LastBackupTimestamp
}

// backupTimestampFromJobTimes derives a 14-digit YYYYMMDDHHMMSS timestamp from a
// Job's CompletionTime (preferred) or StartTime in UTC, returning "" when neither
// is set.
func backupTimestampFromJobTimes(job *batchv1.Job) string {
	switch {
	case job.Status.CompletionTime != nil:
		return job.Status.CompletionTime.UTC().Format(backupTimestampLayout)
	case job.Status.StartTime != nil:
		return job.Status.StartTime.UTC().Format(backupTimestampLayout)
	default:
		return ""
	}
}

// backupTypeFromJob resolves the backup type for a specific Job, preferring the
// Job's own avsoft.io/backup-type label (set by the builder to the type that
// actually ran) and falling back to the spec-derived backupTypeFromLabels when
// the label is absent (older Jobs / backward compatibility). This makes
// LastBackupType and BackupHistoryEntry.Type reflect the actual Job — e.g. a
// per-request incremental run while the cluster spec defaults to full.
func backupTypeFromJob(job *batchv1.Job, cluster *cbv1alpha1.CloudberryCluster) string {
	if t := job.Labels[util.LabelBackupType]; t != "" {
		return t
	}
	return backupTypeFromLabels(cluster)
}

// backupTypeFromLabels resolves the backup type from the cluster's gpbackup spec.
// It is the fallback used by backupTypeFromJob when a Job carries no
// avsoft.io/backup-type label.
func backupTypeFromLabels(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.Gpbackup != nil &&
		cluster.Spec.Backup.Gpbackup.Incremental {
		return backupTypeIncremental
	}
	return backupTypeFull
}

// backupJobDuration returns a human-readable duration for the Job, or "".
func backupJobDuration(job *batchv1.Job) string {
	d := backupJobDurationValue(job)
	if d <= 0 {
		return ""
	}
	return d.Round(time.Second).String()
}

// backupJobDurationValue returns the elapsed time between Job start and completion.
func backupJobDurationValue(job *batchv1.Job) time.Duration {
	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		return 0
	}
	return job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
}

// appendBackupHistory prepends a new entry and trims to backupHistoryLimit,
// replacing any existing entry with the same timestamp.
func appendBackupHistory(
	history []cbv1alpha1.BackupHistoryEntry,
	entry cbv1alpha1.BackupHistoryEntry,
) []cbv1alpha1.BackupHistoryEntry {
	filtered := make([]cbv1alpha1.BackupHistoryEntry, 0, len(history)+1)
	filtered = append(filtered, entry)
	for _, e := range history {
		if e.Timestamp != "" && e.Timestamp == entry.Timestamp {
			continue
		}
		filtered = append(filtered, e)
	}
	if len(filtered) > backupHistoryLimit {
		filtered = filtered[:backupHistoryLimit]
	}
	return filtered
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

	// Create a single DB client for all parameter operations to avoid
	// creating separate connections for coordinator, database, and role parameters.
	var configDBClient db.Client
	if r.dbFactory != nil {
		var dbErr error
		configDBClient, dbErr = r.dbFactory.NewClient(ctx, cluster)
		if dbErr != nil {
			logger.Error("failed to create DB client for config parameters", "error", dbErr)
			// Continue without DB client — individual methods will skip DB operations.
		} else {
			defer configDBClient.Close()
		}
	}

	// Apply coordinator-only parameters via the database client.
	if err := r.applyCoordinatorParameters(ctx, cluster, configDBClient); err != nil {
		logger.Error("failed to apply coordinator parameters", "error", err)
		// Non-fatal: continue with other config layers.
	}

	// Apply database-specific parameters via ALTER DATABASE SET.
	if err := r.applyDatabaseParameters(ctx, cluster, configDBClient); err != nil {
		logger.Error("failed to apply database parameters", "error", err)
		// Non-fatal: continue with other config layers.
	}

	// Apply role-specific parameters via ALTER ROLE SET.
	if err := r.applyRoleParameters(ctx, cluster, configDBClient); err != nil {
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
// If sharedClient is non-nil, it is used instead of creating a new DB client.
func (r *AdminReconciler) applyCoordinatorParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	sharedClient db.Client,
) error {
	if len(cluster.Spec.Config.CoordinatorParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying coordinator-only parameters",
		"count", len(cluster.Spec.Config.CoordinatorParameters))

	dbClient := sharedClient
	if dbClient == nil {
		logger.Debug("no shared DB client available, skipping coordinator parameters")
		return nil
	}

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
// If sharedClient is non-nil, it is used instead of creating a new DB client.
//
//nolint:unparam // error return used when sharedClient encounters DB errors
func (r *AdminReconciler) applyDatabaseParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	sharedClient db.Client,
) error {
	if len(cluster.Spec.Config.DatabaseParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying database-specific parameters",
		"databases", len(cluster.Spec.Config.DatabaseParameters))

	dbClient := sharedClient
	if dbClient == nil {
		logger.Debug("no shared DB client available, skipping database parameters")
		return nil
	}

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
// If sharedClient is non-nil, it is used instead of creating a new DB client.
//
//nolint:unparam // error return used when sharedClient encounters DB errors
func (r *AdminReconciler) applyRoleParameters(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	sharedClient db.Client,
) error {
	if len(cluster.Spec.Config.RoleParameters) == 0 {
		return nil
	}

	logger := util.LoggerFromContext(ctx)
	logger.Debug("applying role-specific parameters",
		"roles", len(cluster.Spec.Config.RoleParameters))

	dbClient := sharedClient
	if dbClient == nil {
		logger.Debug("no shared DB client available, skipping role parameters")
		return nil
	}

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
	r.metrics.RecordRollingRestart(cluster.Name, cluster.Namespace, "started")

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
			r.metrics.RecordRollingRestart(cluster.Name, cluster.Namespace, "failed")
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
	r.metrics.RecordRollingRestart(cluster.Name, cluster.Namespace, "completed")

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
			r.metrics.RecordMaintenanceOperation(cluster.Name, cluster.Namespace, maintenance, "failed")
		} else {
			// Direct execution succeeded — emit completion event and record metric.
			r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonMaintenanceCompleted,
				fmt.Sprintf("Maintenance operation %s completed successfully", maintenance))
			r.metrics.RecordMaintenanceOperation(cluster.Name, cluster.Namespace, maintenance, "success")
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
	r.metrics.RecordMaintenanceOperation(cluster.Name, cluster.Namespace, maintenance, "started")

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
//
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=create;delete;get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=create;delete;get;list;watch;update;patch
func (r *AdminReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Owns(&batchv1.Job{}).
		Owns(&batchv1.CronJob{}).
		Named(adminControllerName).
		Complete(r)
}
