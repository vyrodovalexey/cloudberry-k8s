package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

	// dbOpTimeout is the shared budget for short, best-effort DB operations
	// issued from reconcile (session/connection inspection, disk-usage scan,
	// exporter-role checks). It bounds DB connectivity issues so they do not
	// block the reconcile worker. Extracted (S-1) to avoid duplicating the
	// 10s literal across call sites.
	dbOpTimeout = 10 * time.Second

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

	// backupTimestampMarkerPrefix is the stdout/termination-message prefix the
	// backup script emits with gpbackup's REAL runtime timestamp (see the
	// builder's backupTimestampMarker / writeGpbackupTimestampCapture); the
	// controller parses it to patch the avsoft.io/backup-timestamp annotation
	// and PREFERS that value for status.lastBackupTimestamp so a later
	// restore-by-timestamp resolves the correct S3 prefix.
	backupTimestampMarkerPrefix = "BACKUP_TIMESTAMP="

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
	reconcileIntervals
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
	// idleDaemons holds one idle-session enforcement daemon per cluster,
	// keyed by namespace/name. A per-cluster daemon keeps rules, DB
	// connection and metric labels correct when multiple CloudberryClusters
	// define idle rules (a single shared daemon would mix them up).
	idleDaemons map[types.NamespacedName]*idleDaemonEntry
	// idleDaemonMu protects idleDaemons access.
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
func (r *AdminReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	startTime := time.Now()
	logger := r.logger.With("cluster", req.Name, "namespace", req.Namespace)
	ctx = util.WithLogger(ctx, logger)

	ctx, span := telemetry.StartSpan(ctx, adminControllerName, "Reconcile")
	defer span.End()

	// Record the reconcile outcome/duration and mark the span on error exactly
	// once on return. The deferred closure captures the named error so both
	// success and error paths are recorded (recorder is nil-guarded).
	// subComponentsErr carries sub-component failures that intentionally do
	// NOT abort the reconcile (existing aggregation style) but must still be
	// recorded as result="error" on the reconcile outcome metric (M-2).
	var subComponentsErr error
	defer func() {
		recordReconcileOutcome(r.metrics, req.Name, req.Namespace, startTime,
			errors.Join(err, subComponentsErr))
		telemetry.SetSpanError(span, err)
	}()

	logger.Debug("starting admin reconciliation",
		"name", req.Name, "namespace", req.Namespace)

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err = r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug("cluster not found, skipping reconciliation")
			// The cluster was deleted: stop its idle daemon (if any) so the
			// enforcement goroutine and its DB connection are not leaked.
			r.stopIdleDaemonFor(req.NamespacedName)
			err = nil
			return ctrl.Result{}, nil
		}
		err = fmt.Errorf("fetching cluster: %w", err)
		return ctrl.Result{}, err
	}

	logger.Debug("cluster fetched",
		"phase", cluster.Status.Phase,
		"generation", cluster.Generation,
		"observedGeneration", cluster.Status.ObservedGeneration)

	// Handle early-return cases: rolling restart, pending reload, not-running, unchanged generation.
	if earlyResult, handled, earlyErr := r.handleAdminEarlyReturns(ctx, cluster); handled {
		err = earlyErr
		return earlyResult, earlyErr
	}

	// Reconcile configuration parameters.
	logger.Debug("reconciling configuration parameters")
	if cfgErr := r.reconcileConfig(ctx, cluster); cfgErr != nil {
		result, err = r.handleConfigError(ctx, logger, cluster, cfgErr)
		return result, err
	}

	// Reconcile all sub-components and patch status.
	// Sub-component errors are non-fatal: they are logged individually and
	// aggregated here for observability, but do not block the reconcile loop.
	// The aggregated error IS surfaced on the reconcile outcome metric
	// (result="error") via subComponentsErr so partial failures are visible.
	logger.Debug("reconciling sub-components")
	if subErr := r.reconcileSubComponents(ctx, logger, cluster); subErr != nil {
		logger.Warn("some sub-components failed to reconcile", "error", subErr)
		subComponentsErr = subErr
	}

	// Re-read the current phase from the API server to avoid overwriting phase changes
	// made by the cluster-controller (e.g., Scaling phase during scale-out).
	var latest cbv1alpha1.CloudberryCluster
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err == nil {
		cluster.Status.Phase = latest.Status.Phase
	}

	// Perform a single status patch for all sub-reconciler changes.
	// Using MergePatch prevents clobbering status changes from other controllers.
	if patchErr := patchStatus(ctx, r.client, cluster); patchErr != nil {
		logger.Error("failed to update cluster status", "error", patchErr)
		err = fmt.Errorf("updating cluster status: %w", patchErr)
		return ctrl.Result{RequeueAfter: requeueAfterError}, err
	}

	logger.Debug("admin reconciliation completed successfully")

	// If the exporter role isn't ready yet, requeue sooner to retry.
	if !isExporterRoleReady(cluster) {
		return ctrl.Result{RequeueAfter: requeueAfterError}, nil
	}

	return ctrl.Result{RequeueAfter: r.requeueDefault()}, nil
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
		return ctrl.Result{RequeueAfter: r.requeueDefault()}, true, nil
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
		// Data-loading Jobs are likewise Job-derived: a deleted dataload Job must
		// be re-created and its terminal status/metrics refreshed on the periodic
		// requeue, even when the generation is unchanged (mirrors backup status).
		r.refreshDataLoadingStatusOnSteadyState(ctx, cluster)
		// Storage management (recommendation-scan CronJob C.5 + StorageConfigured
		// condition R.5) must likewise converge on the steady-state path: without
		// this, enabling disk monitoring on an already-settled cluster would never
		// create the CronJob or set the condition (mirrors backup/data-loading).
		r.refreshStorageOnSteadyState(ctx, cluster)
		return ctrl.Result{RequeueAfter: r.requeueDefault()}, true, nil
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
	logger := util.LoggerFromContext(ctx)

	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		// Disabled-backup cleanup must ALSO run on the steady-state path, not just
		// on the spec-driven reconcile. Once the cluster settles and the
		// cluster-controller advances ObservedGeneration to match Generation, the
		// generation gate in handleAdminEarlyReturns short-circuits the spec-driven
		// reconcileBackup (which is what normally removes the CronJob and clears
		// status.cronJobName). Without this branch, a "disable backup" change that
		// is observed only after the generation has caught up — or whose clear was
		// clobbered by a concurrent full patchStatus from the cluster-controller —
		// would leave a stale persisted status.cronJobName forever (Scenario 88a,
		// live). removeBackupCronJob is idempotent and clears the persisted value
		// via an explicit-empty MergePatch (patchClearCronJobName), so running it on
		// every steady-state requeue guarantees convergence to "" even if another
		// controller re-marshaled the stale value in the same window.
		//
		// NOTE: removeBackupCronJob now early-returns nil when there is genuinely
		// nothing to clean up (no CronJob exists AND Status.CronJobName == ""), so
		// this call is a no-op for clusters that never had backup configured. That
		// guard (the single source of truth) avoids an unnecessary status patch on
		// the steady-state path, which previously regressed the Scenario 25/26 +
		// workload functional tests.
		if err := r.removeBackupCronJob(ctx, cluster); err != nil {
			logger.Warn("failed to remove backup cronjob on steady-state reconcile", "error", err)
		}
		return
	}

	// Reconcile the backup CronJob against the current schedule on the steady-state
	// path too. ensureBackupCronJob creates/updates the CronJob when a schedule is
	// set and removes it (clearing status.cronJobName via the explicit-empty
	// MergePatch) when the schedule is empty. This is required for the
	// enabled-but-empty-schedule case (Scenario 88b): once the generation settles,
	// the spec-driven reconcileBackup is gated out, so without this a schedule that
	// was cleared to "" — or whose status.cronJobName was re-set by a concurrent
	// full patchStatus from the cluster-controller — would leave a stale CronJob /
	// persisted cronJobName forever. ensureBackupCronJob/removeBackupCronJob are
	// idempotent (and a no-op when there is genuinely nothing to do), so running on
	// every steady-state requeue converges both the CronJob and the status.
	if err := r.ensureBackupCronJob(ctx, cluster); err != nil {
		logger.Warn("failed to ensure backup cronjob on steady-state reconcile", "error", err)
	}

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

// refreshDataLoadingStatusOnSteadyState re-creates any missing enabled
// data-loading Job/CronJob and refreshes the per-job runtime status (and the 5
// data-loading metrics) from completed Jobs, even when the spec generation is
// unchanged (steady state). This is required because the generation gate in
// handleAdminEarlyReturns short-circuits spec-driven reconciliation (which
// normally runs reconcileDataLoadingJobs via reconcileDataLoading), but
// Job-derived status — and the re-creation of a Job that was deleted out from
// under the operator — must still happen on each periodic requeue. It mirrors
// refreshBackupStatusOnSteadyState exactly: idempotent, non-fatal (errors are
// logged and ignored), and it only persists when there is data-loading status to
// write, so a non-data-loading cluster sees no spurious status updates.
func (r *AdminReconciler) refreshDataLoadingStatusOnSteadyState(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)

	if cluster.Spec.DataLoading == nil || !cluster.Spec.DataLoading.Enabled {
		// Data loading disabled / absent: nothing Job-derived to refresh. No-op
		// (no status patch), so default/non-data-loading clusters are untouched.
		return
	}

	// Rebuild the spec-derived per-job status skeleton (name/enabled + counts) so
	// enrichDataLoadingStatus has entries to enrich. This is the same lightweight
	// status reconcileDataLoading computes; recomputing it here is idempotent and
	// carries no generation bump (status subresource).
	jobs := cluster.Spec.DataLoading.Jobs
	configuredJobs := int32(0)
	activeJobs := int32(0)
	jobStatuses := make([]cbv1alpha1.DataLoadingJobStatus, 0, len(jobs))
	for _, job := range jobs {
		configuredJobs++
		if job.Enabled {
			activeJobs++
		}
		jobStatuses = append(jobStatuses, cbv1alpha1.DataLoadingJobStatus{
			Name:    job.Name,
			Enabled: job.Enabled,
		})
	}
	cluster.Status.DataLoadingJobs = activeJobs
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase:          dataLoadingPhaseConfigured,
		ConfiguredJobs: configuredJobs,
		ActiveJobs:     activeJobs,
		Jobs:           jobStatuses,
	}

	// Preserve the spec-derived PXF summary on the steady-state path too, so a
	// status patch here never drops status.dataLoading.pxf.{configured,servers},
	// and refresh the HONEST observed-only runtime fields (status from live
	// segment-primary "pxf" readiness, extensionsInstalled from a live
	// pg_extension probe) so steady-state reconciles keep them current. Both
	// observed fields stay ABSENT when unobservable.
	if pxf := cluster.Spec.DataLoading.Pxf; pxf != nil && pxf.Enabled {
		pxfStatus := &cbv1alpha1.DataLoadingPxfStatus{
			Configured: true,
			Servers:    clampInt32(len(pxf.Servers)),
		}
		r.populatePxfObservedStatus(ctx, cluster, pxfStatus)
		cluster.Status.DataLoading.Pxf = pxfStatus
		r.recordPxfObservedMetrics(cluster, pxfStatus)
	}

	r.metrics.SetDataLoadingJobsActive(
		cluster.Name, cluster.Namespace, float64(activeJobs),
	)

	// Re-create missing enabled Jobs/CronJobs and re-harvest terminal status +
	// metrics. reconcileDataLoadingJobs is idempotent (get-or-create by the
	// deterministic name), so a Job deleted out from under the operator is
	// re-created here, and an existing Job's terminal status is refreshed.
	if err := r.reconcileDataLoadingJobs(ctx, cluster); err != nil {
		logger.Warn("failed to reconcile data loading jobs on steady-state reconcile", "error", err)
		return
	}

	// Persist refreshed data-loading status. patchDataLoadingStatus uses an
	// explicit MergePatch (zero counters and an empty jobs slice are always
	// included) so a status-only change never bumps the spec generation.
	if err := r.patchDataLoadingStatus(ctx, cluster); err != nil {
		logger.Warn("failed to patch data loading status on steady-state reconcile", "error", err)
	}
}

// refreshStorageOnSteadyState converges the storage-management resources — the
// scheduled recommendation-scan CronJob (C.5) and the StorageConfigured
// condition (R.5) — on the steady-state periodic requeue, even when the spec
// generation is unchanged. This is required because the generation gate in
// handleAdminEarlyReturns short-circuits spec-driven reconciliation (which
// normally runs reconcileStorage via reconcileSubComponents). Without this,
// once a cluster settles (ObservedGeneration == Generation) with disk
// monitoring enabled, the recommendation-scan CronJob and the StorageConfigured
// condition would never be created (Scenario 115, live). It mirrors
// refreshBackupStatusOnSteadyState exactly: idempotent (the ensure/remove
// helpers are Get->Create/Update-on-drift / Get->Delete-IgnoreNotFound), and
// non-fatal (errors are logged and ignored so a storage hiccup never blocks the
// requeue).
func (r *AdminReconciler) refreshStorageOnSteadyState(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)

	// R.1: diskMonitoring gate. Storage absent or disk monitoring off => the
	// recommendation-scan CronJob must be GC'd on the steady-state path too, not
	// just on the spec-driven reconcile. Once the cluster settles and the
	// cluster-controller advances ObservedGeneration to match Generation, the
	// generation gate short-circuits the spec-driven reconcileStorage (which is
	// what normally drives the enabled->disabled GC). removeRecommendationScanCronJob
	// is idempotent and tolerates NotFound, so it is a no-op for clusters that
	// never had a scan configured and converges to no-CronJob otherwise.
	if cluster.Spec.Storage == nil || !cluster.Spec.Storage.DiskMonitoring {
		// C.2 reset-on-disable + C.4 clear-on-disable: the whole storage block is
		// off, so reset the disk-usage status/gauge and clear any stale
		// recommendation count/gauges BEFORE the CronJob GC (this steady-state
		// path OWNS the storage-off GC per R5). clearStorageSignals persists the
		// cleared status only when there is an actual stale value (R6).
		r.clearStorageSignals(ctx, cluster, logger)
		if err := r.removeRecommendationScanCronJob(ctx, cluster); err != nil {
			logger.Warn("failed to remove recommendation-scan cronjob on steady-state reconcile", "error", err)
		}
		return
	}

	// R.2/S.1/M.1: measure disk usage on the steady-state path too so growth is
	// tracked on settled clusters. recordDiskUsage sets Status.DiskUsagePercent
	// from the current measured value and publishes the gauge from that same
	// value. It persists the measured value via patchStatus; the end-of-function
	// patchStatus below (for the StorageConfigured condition) is an idempotent
	// MergePatch that also carries the in-memory DiskUsagePercent, so the status
	// stays consistent. Best-effort and non-fatal (skips on DB error).
	r.recordDiskUsage(ctx, cluster, logger)

	// R.3/R.4/S.2/M.2/M.4: run the four threshold-aware recommendation scans on the
	// steady-state path too so the per-type recommendations_total gauges and
	// Status.RecommendationCount track changes (cleared/boundary cases) on settled
	// clusters, not just on a spec-driven reconcile. recordRecommendations sets the
	// in-memory Status.RecommendationCount; the end-of-function patchStatus below
	// flushes it. Best-effort and non-fatal (skips on DB error). Gated on the scan
	// being enabled so a disabled scan never publishes counts (DISABLED no-op).
	if cluster.Spec.Storage.RecommendationScan != nil &&
		cluster.Spec.Storage.RecommendationScan.Enabled {
		r.recordRecommendations(ctx, cluster, logger)
	} else {
		// C.4 clear-on-disable (steady-state parity with the spec-driven path):
		// diskMonitoring is ON but the scan is nil/disabled, so reset the count +
		// per-type gauges. Mutually exclusive with recordRecommendations (clear
		// never runs on the enabled path). The end-of-function patchStatus below
		// flushes the zeroed count.
		r.clearRecommendations(cluster)
	}

	// C.5: converge the scheduled recommendation-scan CronJob. ensureRecommendationScanCronJob
	// is idempotent (Get->Create/Update-on-drift, and delete-if-exists when the
	// builder returns nil for a disabled scan / empty schedule), and it keeps the
	// cloudberry_recommendation_scan_cronjob gauge current, so calling it on every
	// steady-state requeue is safe — exactly like ensureBackupCronJob.
	if err := r.ensureRecommendationScanCronJob(ctx, cluster); err != nil {
		logger.Warn("failed to ensure recommendation-scan cronjob on steady-state reconcile", "error", err)
		return
	}

	// R.5: storage reconcile succeeded => StorageConfigured=True, set the same
	// way reconcileStorage does so the condition converges on the steady-state
	// path too.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionStorageConfigured),
		metav1.ConditionTrue,
		"StorageReconciled",
		"Storage management is configured",
	)

	// Persist the refreshed condition. MergePatch (via patchStatus) is used so
	// already-set fields written by other controllers are not clobbered, and a
	// status-only change does not bump the spec generation (status subresource).
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		logger.Warn("failed to patch storage status on steady-state reconcile", "error", err)
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
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileWorkload")
	defer func() { end(err) }()

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
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileResourceGroups")
	defer func() { end(err) }()

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
		dbCtx, dbCancel := context.WithTimeout(ctx, dbOpTimeout)
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

	// 3. Stop this cluster's idle daemon if running (other clusters' daemons
	// keep enforcing their own rules).
	r.stopIdleDaemonFor(types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})

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

// idleDaemonEntry pairs a per-cluster idle daemon with its reconnect factory
// so the factory's cluster snapshot can be refreshed on every reconcile.
type idleDaemonEntry struct {
	daemon  *idle.Daemon
	factory *idleDaemonDBClientFactory
}

// stopIdleDaemonFor stops and removes the idle session daemon of the given
// cluster if it is running. Used when the cluster's idle rules are removed or
// the cluster itself is deleted; other clusters' daemons are untouched.
func (r *AdminReconciler) stopIdleDaemonFor(key types.NamespacedName) {
	r.idleDaemonMu.Lock()
	entry := r.idleDaemons[key]
	delete(r.idleDaemons, key)
	r.idleDaemonMu.Unlock()

	// Stop outside the lock: Stop blocks until the scan loop exits and must
	// not stall daemon management for other clusters.
	if entry != nil {
		entry.daemon.Stop()
	}
}

// startOrUpdateIdleDaemon starts the cluster's idle daemon if idle rules are
// present, or updates the rules if the daemon is already running. The
// reconnect factory's cluster snapshot is refreshed on EVERY reconcile so
// credential rotations or port changes propagate to daemon reconnects.
func (r *AdminReconciler) startOrUpdateIdleDaemon(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

	if len(cluster.Spec.Workload.IdleRules) == 0 {
		r.stopIdleDaemonFor(key)
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
		r.stopIdleDaemonFor(key)
		return
	}

	r.idleDaemonMu.Lock()
	defer r.idleDaemonMu.Unlock()

	if r.idleDaemons == nil {
		r.idleDaemons = make(map[types.NamespacedName]*idleDaemonEntry)
	}

	if entry, ok := r.idleDaemons[key]; ok {
		// Daemon already running — update rules and refresh the factory's
		// cluster snapshot so the next reconnect uses current spec/credentials.
		entry.factory.setCluster(cluster.DeepCopy())
		entry.daemon.UpdateRules(rules)
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
	r.idleDaemons[key] = &idleDaemonEntry{daemon: d, factory: daemonFactory}
}

// idleDaemonDBClientFactory adapts db.DBClientFactory to idle.DBClientFactory
// so the idle daemon can reconnect to the database on connection failures.
// The cluster snapshot is refreshed by the reconciler on every reconcile
// (setCluster) so reconnects pick up rotated credentials or changed ports.
type idleDaemonDBClientFactory struct {
	dbFactory db.DBClientFactory
	// mu protects cluster (read by daemon reconnects, replaced by reconciles).
	mu      sync.RWMutex
	cluster *cbv1alpha1.CloudberryCluster
}

// setCluster atomically replaces the cluster snapshot used for reconnects.
func (f *idleDaemonDBClientFactory) setCluster(cluster *cbv1alpha1.CloudberryCluster) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cluster = cluster
}

// NewClient creates a new database client using the current cluster snapshot.
func (f *idleDaemonDBClientFactory) NewClient(ctx context.Context) (db.Client, error) {
	f.mu.RLock()
	cluster := f.cluster
	f.mu.RUnlock()
	return f.dbFactory.NewClient(ctx, cluster)
}

// reconcileQueryMonitoring reconciles query monitoring configuration by creating
// and updating the required Kubernetes resources: exporter credentials Secret,
// exporter queries ConfigMap, node exporter DaemonSet, exporter Service,
// ServiceMonitor, and PrometheusRule. It also sets up the database exporter role
// when a DB client factory is available.
func (r *AdminReconciler) reconcileQueryMonitoring(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	if cluster.Spec.QueryMonitoring == nil || !cluster.Spec.QueryMonitoring.Enabled {
		return nil
	}

	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileQueryMonitoring")
	defer func() { end(err) }()

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
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "ensureExporterCoreResources")
	defer func() { end(err) }()

	if secretErr := r.ensureExporterCredentialsSecret(ctx, cluster, password, dsn, logger); secretErr != nil {
		err = secretErr
		return err
	}
	if cmErr := r.ensureExporterQueriesConfigMap(ctx, cluster, logger); cmErr != nil {
		err = cmErr
		return err
	}
	err = r.ensureExporterService(ctx, cluster, logger)
	return err
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
	dbCtx, cancel := context.WithTimeout(ctx, dbOpTimeout)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		// A real setup attempt that failed at the connectivity boundary: count it
		// honestly as an error so a persistently-unreachable DB is visible.
		r.metrics.RecordExporterRoleSetup(cluster.Name, cluster.Namespace, metricResultError)
		logger.Warn("failed to create DB client for exporter role setup (will retry)", "error", err)
		return
	}
	defer dbClient.Close()

	if setupErr := dbClient.SetupExporterRole(dbCtx, password); setupErr != nil {
		r.metrics.RecordExporterRoleSetup(cluster.Name, cluster.Namespace, metricResultError)
		logger.Warn("failed to setup exporter role (will retry)", "error", setupErr)
		return
	}

	r.metrics.RecordExporterRoleSetup(cluster.Name, cluster.Namespace, metricResultSuccess)
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
	dbCtx, cancel := context.WithTimeout(ctx, dbOpTimeout)
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

	// Publish the REAL max_connections value alongside the active connection
	// count. On error the gauge is NOT written (it keeps its last sample) so
	// dashboards never see a bogus 0 ceiling.
	if maxConns, maxErr := dbClient.GetMaxConnections(dbCtx); maxErr != nil {
		logger.Debug("failed to query max_connections; keeping last value", "error", maxErr)
	} else {
		r.metrics.SetConnectionsMax(cluster.Name, cluster.Namespace, float64(maxConns))
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
// reconcileBackup wraps the backup reconciliation in a child span (under the
// admin Reconcile span) so a slow backup reconcile (ConfigMap/Secret/CronJob
// ensure, status refresh, retention, validation) is visible in traces. The
// actual work is in doReconcileBackup; this thin wrapper keeps the span/error
// handling in one place. No-op when telemetry is disabled.
func (r *AdminReconciler) reconcileBackup(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	ctx, span := telemetry.StartSpan(ctx, adminControllerName, "reconcileBackup")
	defer span.End()

	err := r.doReconcileBackup(ctx, cluster)
	telemetry.SetSpanError(span, err)
	return err
}

// doReconcileBackup performs the backup reconciliation work.
func (r *AdminReconciler) doReconcileBackup(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		// Backup disabled (or unconfigured): ensure no stale per-cluster CronJob
		// is left behind and clear its recorded name from status (Scenario 88a).
		// This is idempotent — a no-op when no CronJob exists. Retention cleanup
		// and other backup resources are intentionally not reconciled here.
		return r.removeBackupCronJob(ctx, cluster)
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
// no Vault client is wired (r.vault == nil) it emits a Warning Event, logs a
// clear warning and skips, so existing construction with a nil Vault client
// never panics. Every failure path emits the BackupVaultCredentialsFailed
// Warning Event so silently-missing backup credentials are observable (M-2).
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
		r.recorder.Event(cluster, corev1.EventTypeWarning,
			cbv1alpha1.EventReasonBackupVaultCredentialsFailed,
			fmt.Sprintf("Backup S3 credentials are configured from Vault path %s but the operator "+
				"has no enabled Vault client; credential materialization skipped", s3.VaultSecret.Path))
		return nil
	}

	if err := r.readAndMaterializeBackupS3VaultCredentials(ctx, cluster, s3, logger); err != nil {
		r.recorder.Event(cluster, corev1.EventTypeWarning,
			cbv1alpha1.EventReasonBackupVaultCredentialsFailed,
			fmt.Sprintf("Failed to materialize backup S3 credentials from Vault: %v", err))
		return err
	}
	return nil
}

// readAndMaterializeBackupS3VaultCredentials reads the configured Vault path
// and materializes the credentials Secret. Split out of
// ensureBackupS3VaultCredentials so the caller can emit the
// BackupVaultCredentialsFailed Warning Event for every failure path in one
// place.
func (r *AdminReconciler) readAndMaterializeBackupS3VaultCredentials(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	s3 *cbv1alpha1.S3Destination,
	logger *slog.Logger,
) error {
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

// removeBackupCronJob deletes the per-cluster backup CronJob if it exists and
// clears cluster.Status.CronJobName. It is idempotent: a missing CronJob is a
// no-op. Used both when backup is disabled (Scenario 88a) and when the schedule
// has been cleared (on-demand-only backups, Scenario 88b).
func (r *AdminReconciler) removeBackupCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	cronName := util.BackupCronJobName(cluster.Name)
	existing := &batchv1.CronJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: cronName, Namespace: cluster.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		// No CronJob exists. If there is also no persisted name to clear, there is
		// genuinely nothing to clean up — return early WITHOUT issuing a status
		// patch. This guard is the single source of truth that keeps both callers
		// (the spec-driven reconcileBackup and the steady-state
		// refreshBackupStatusOnSteadyState) a no-op for clusters that never had
		// backup configured. Without it, the steady-state path performed an
		// unnecessary explicit-empty Status().Patch on every requeue, perturbing
		// the reconcile/status flow and regressing the Scenario 25/26 + workload
		// functional tests. When a stale name IS persisted (Scenario 88a: backup
		// disabled after having had a schedule), we still clear it below.
		if cluster.Status.CronJobName == "" {
			return nil
		}
		// A stale name is still persisted in the CR status (e.g. the CronJob was
		// deleted out-of-band). Clear it explicitly so the persisted status
		// reflects the absence of a scheduled backup.
		return r.patchClearCronJobName(ctx, cluster)
	}
	if err != nil {
		return fmt.Errorf("getting backup cronjob %s: %w", cronName, err)
	}
	if delErr := r.client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
		return fmt.Errorf("deleting backup cronjob %s: %w", cronName, delErr)
	}
	return r.patchClearCronJobName(ctx, cluster)
}

// patchClearCronJobName explicitly clears the persisted status.cronJobName field
// and the in-memory value. A plain patchStatus cannot clear it because the field
// is tagged json:"cronJobName,omitempty": json.Marshal drops the empty string, and
// a MergePatch only changes keys that are present, leaving the previously-persisted
// value intact. We therefore build a raw MergePatch map with an EXPLICIT empty
// string (mirroring patchQueryStatus / patchFTSStatus) so the field is actually
// reset in the CR. NotFound and conflict errors are tolerated so disabled-backup
// reconcile paths do not fail on transient/stale-object conditions.
func (r *AdminReconciler) patchClearCronJobName(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	// Keep the live object consistent within this reconcile.
	cluster.Status.CronJobName = ""

	patch, err := json.Marshal(map[string]interface{}{
		patchKeyStatus: map[string]interface{}{
			// Explicit empty string bypasses the struct's omitempty so the
			// MergePatch actually clears the persisted value.
			"cronJobName": "",
		},
	})
	if err != nil {
		return fmt.Errorf("marshaling cronJobName clear patch: %w", err)
	}

	if patchErr := r.client.Status().Patch(
		ctx, cluster, client.RawPatch(types.MergePatchType, patch),
	); patchErr != nil && !apierrors.IsNotFound(patchErr) && !apierrors.IsConflict(patchErr) {
		return fmt.Errorf("clearing status.cronJobName: %w", patchErr)
	}
	return nil
}

// ensureBackupCronJob creates/updates the scheduled backup CronJob when a
// schedule is set, or deletes it when the schedule has been cleared.
func (r *AdminReconciler) ensureBackupCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired := r.builder.BuildBackupCronJob(cluster)

	if desired == nil {
		// No schedule configured: delete the CronJob if it exists.
		return r.removeBackupCronJob(ctx, cluster)
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

// ensureRecommendationScanCronJob creates/updates the scheduled storage
// recommendation-scan CronJob when the scan is enabled with a schedule
// (spec 13 §Reconciliation C.5), or GCs it (delete-if-exists) when the builder
// returns nil — the nil-means-delete contract shared with ensureBackupCronJob.
// It is called UNCONDITIONALLY from reconcileStorage so the create path and the
// enabled->disabled GC are both driven deterministically every reconcile. The
// cloudberry_recommendation_scan_cronjob gauge is set 1 when provisioned, 0 when
// removed.
func (r *AdminReconciler) ensureRecommendationScanCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	desired := r.builder.BuildRecommendationScanCronJob(cluster)

	if desired == nil {
		// Scan disabled / no schedule: delete the CronJob if it exists.
		r.metrics.SetRecommendationScanCronJob(cluster.Name, cluster.Namespace, 0)
		return r.removeRecommendationScanCronJob(ctx, cluster)
	}

	existing := &batchv1.CronJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating recommendation-scan cronjob %s: %w", desired.Name, createErr)
		}
		r.metrics.SetRecommendationScanCronJob(cluster.Name, cluster.Namespace, 1)
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting recommendation-scan cronjob %s: %w", desired.Name, err)
	}

	if !equality.Semantic.DeepEqual(existing.Spec, desired.Spec) {
		existing.Spec = desired.Spec
		existing.Labels = desired.Labels
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating recommendation-scan cronjob %s: %w", desired.Name, updateErr)
		}
	}
	r.metrics.SetRecommendationScanCronJob(cluster.Name, cluster.Namespace, 1)
	return nil
}

// removeRecommendationScanCronJob deletes the scheduled recommendation-scan
// CronJob if it exists, tolerating NotFound so the disabled/GC path is a no-op
// for clusters that never had a scan configured (spec 13 §C.5 nil-means-delete).
func (r *AdminReconciler) removeRecommendationScanCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	cronName := util.RecommendationScanCronJobName(cluster.Name)
	existing := &batchv1.CronJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: cronName, Namespace: cluster.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting recommendation-scan cronjob %s: %w", cronName, err)
	}
	if delErr := r.client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
		return fmt.Errorf("deleting recommendation-scan cronjob %s: %w", cronName, delErr)
	}
	return nil
}

// clearRecommendations resets the recommendation count + per-type gauges to 0
// when the recommendation scan is disabled (C.4 clear-on-disable). Without this,
// an enabled->disabled scan leaves a STALE Status.RecommendationCount and stale
// cloudberry_recommendations_total{type} gauges, because recordRecommendations
// (which OWNS both) is gated behind the scan being enabled and never runs on the
// disabled path. It is best-effort, non-fatal, and IDEMPOTENT (zeroing an
// already-zero count/gauge is a no-op), and it must ONLY be called on the
// disabled path — never on the enabled path (that would zero a fresh scan).
//
// It reuses the canonical recommendationTypes slice (all four: bloat/skew/age/
// index_bloat) — the same source recordRecommendations publishes from, so it
// cannot drift. It also clears RecommendationScanTruncated (a disabled scan can
// never be truncated; the flag stays non-sticky, consistent with C.10).
//
// It does NOT clear the per-table cloudberry_table_bloat_ratio{table} gauge:
// that is a per-table cardinality signal (M.4) the operator does not enumerate
// on disable, so there is no precise label set to zero here. The CronJob GC and
// the on-disable count clear are the primary disabled-state signals (honest,
// documented limitation).
func (r *AdminReconciler) clearRecommendations(cluster *cbv1alpha1.CloudberryCluster) {
	cluster.Status.RecommendationCount = 0
	cluster.Status.RecommendationScanTruncated = false // never sticky on disable
	for _, recType := range recommendationTypes {
		r.metrics.SetRecommendationsTotal(cluster.Name, cluster.Namespace, recType, 0)
	}
}

// clearStorageSignals resets BOTH disabled-state storage signals on the
// diskMonitoring:false / whole-storage-off path so the two early-return sites
// (reconcileStorage R.1 + refreshStorageOnSteadyState storage-off block) behave
// identically:
//   - C.2 reset-on-disable: Status.DiskUsagePercent is reset to 0 and
//     cloudberry_disk_usage_percent is published 0, so a reader sees an explicit
//     "monitoring off" signal rather than a frozen stale reading. (0 here is a
//     disabled signal, NOT "empty"; the live check asserts the gauge does not
//     ADVANCE after the flip.)
//   - C.4 clear-on-disable: clearRecommendations resets the recommendation count
//   - per-type gauges (the whole storage block off => no recommendations).
//
// To avoid status-patch churn on the never-configured common case (R6), the
// cleared status is persisted ONLY when there is an actual stale value to clear.
func (r *AdminReconciler) clearStorageSignals(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	staleDisk := cluster.Status.DiskUsagePercent != 0
	staleRecs := cluster.Status.RecommendationCount != 0

	// C.2 reset-on-disable: zero the disk-usage status + gauge.
	cluster.Status.DiskUsagePercent = 0
	r.metrics.SetDiskUsagePercent(cluster.Name, cluster.Namespace, 0)

	// C.4 clear-on-disable: zero the recommendation count + per-type gauges.
	r.clearRecommendations(cluster)

	// R6: only patch when a real stale value was actually cleared, so the
	// common Storage==nil / never-configured case does not churn the status.
	if staleDisk || staleRecs {
		if perr := patchStatus(ctx, r.client, cluster); perr != nil {
			logger.Warn("failed to persist cleared storage signals on disabled storage", "error", perr)
		}
	}
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

	// Annotate succeeded restore Jobs whose statistics restore failed (the
	// GPRESTORE_PARTIAL termination marker) BEFORE metrics/status are derived,
	// so the same reconcile already reports the "partial" result.
	r.reconcileRestorePartialAnnotations(ctx, cluster, jobs.Items)

	// Annotate succeeded backup Jobs with gpbackup's REAL runtime timestamp (the
	// BACKUP_TIMESTAMP termination marker) BEFORE status is derived, so the
	// recorded status.lastBackupTimestamp / BackupHistory reference the true S3
	// object prefix and a later restore-by-timestamp resolves it correctly.
	r.reconcileBackupTimestampAnnotations(ctx, cluster, jobs.Items)

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

	// transitioned mirrors the event gate below (M-1): the backup/restore
	// COUNTERS increment only when this Job's observed status actually
	// changed (new Job name or new status), so periodic no-op reconciles of
	// an unchanged Job do not inflate cloudberry_backup_total /
	// cloudberry_restore_total. Gauges remain recorded unconditionally — they
	// are idempotent by definition.
	transitioned := job.Name != prevJobName || status != prevStatus

	operation := job.Labels[util.LabelBackupOperation]
	if operation == util.BackupOperationRestore {
		if transitioned {
			r.metrics.RecordRestore(cluster.Name, cluster.Namespace, restoreMetricStatus(job, status))
		}
		// Restore failures (e.g. gprestore refusing an incomplete incremental
		// set, Scenario 78d) are surfaced as a distinct RestoreFailed Warning so
		// they are observable. The backup-only BackupFailed event semantics
		// (Scenario 77) are intentionally left unchanged.
		r.emitRestoreFailureEvent(cluster, job, status, prevStatus, prevJobName)
		return
	}

	r.recordLatestBackupMetrics(cluster, job, backupType, timestamp, status, transitioned)
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
// backup Job: the aggregate backup counter (transition-gated, M-1), the
// last-status gauge, and (on success) the last-success timestamp, typed
// duration histogram and size gauge.
func (r *AdminReconciler) recordLatestBackupMetrics(
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
	backupType, timestamp, status string,
	transitioned bool,
) {
	name, namespace := cluster.Name, cluster.Namespace
	if transitioned {
		r.metrics.RecordBackup(name, namespace, backupType, strings.ToLower(status))
	}

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

// backupTimestampFromJob extracts a gpbackup-style 14-digit YYYYMMDDHHMMSS
// timestamp for a backup/restore Job.
//
// PREFERRED source (correctness fix): the REAL gpbackup runtime timestamp
// captured on the avsoft.io/backup-timestamp annotation (patched from the
// backup pod's "BACKUP_TIMESTAMP=<ts>" termination marker — see
// reconcileBackupTimestampAnnotations). gpbackup generates its own timestamp at
// runtime with no flag to pin it, so the operator's Job-name timestamp drifts
// from the real S3 object prefix; preferring the captured value makes
// status.lastBackupTimestamp resolve the correct prefix on a later
// restore-by-timestamp.
//
// FALLBACK (backward compatible, annotation absent): on-demand Jobs encode the
// timestamp in their name ("{cluster}-backup-<timestamp>"), which is parsed by
// prefix-trimming. CronJob spawned Jobs are named
// "{cluster}-backup-schedule-<hash>" by Kubernetes, from which a real 14-digit
// timestamp cannot be parsed; for these we fall back to the Job's CompletionTime
// (else StartTime) formatted as YYYYMMDDHHMMSS in UTC. This guarantees
// status.lastBackupTimestamp (and BackupHistoryEntry.Timestamp) is always a
// valid 14-digit value.
func backupTimestampFromJob(cluster *cbv1alpha1.CloudberryCluster, job *batchv1.Job) string {
	// Prefer gpbackup's REAL captured timestamp when present and valid.
	if ts := backupTimestampFromAnnotation(job); ts != "" {
		return ts
	}
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
		if util.IsGpbackupTimestamp(parsed) {
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
		return job.Status.CompletionTime.UTC().Format(util.GpbackupTimestampLayout)
	case job.Status.StartTime != nil:
		return job.Status.StartTime.UTC().Format(util.GpbackupTimestampLayout)
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
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileDataLoading")
	defer func() { end(err) }()

	if cluster.Spec.DataLoading == nil || !cluster.Spec.DataLoading.Enabled {
		return r.cleanupDataLoading(ctx, cluster)
	}

	logger := util.LoggerFromContext(ctx)

	jobs := cluster.Spec.DataLoading.Jobs
	configuredJobs := int32(0)
	activeJobs := int32(0)
	jobStatuses := make([]cbv1alpha1.DataLoadingJobStatus, 0, len(jobs))
	for _, job := range jobs {
		configuredJobs++
		if job.Enabled {
			activeJobs++
		}
		jobStatuses = append(jobStatuses, cbv1alpha1.DataLoadingJobStatus{
			Name:    job.Name,
			Enabled: job.Enabled,
		})
	}

	logger.Info("reconciling data loading configuration",
		"totalJobs", configuredJobs,
		"activeJobs", activeJobs,
	)

	// Update data loading status. DataLoadingJobs is retained as a backward
	// compatible mirror of the enabled-job count (ActiveJobs).
	cluster.Status.DataLoadingJobs = activeJobs
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase:          dataLoadingPhaseConfigured,
		ConfiguredJobs: configuredJobs,
		ActiveJobs:     activeJobs,
		Jobs:           jobStatuses,
	}

	r.metrics.SetDataLoadingJobsActive(
		cluster.Name, cluster.Namespace, float64(activeJobs),
	)

	// Reconcile the PXF configuration when enabled: publish the config-derived
	// gauge and populate Status.DataLoading.Pxf. The "<cluster>-pxf-servers"
	// ConfigMap is created by the CLUSTER controller (before segment pods start),
	// not here. When PXF is not enabled, Pxf status stays nil.
	pxfServers := r.reconcilePxf(ctx, cluster)

	// Snapshot the in-memory DataLoading status (counts + jobs + the just-built
	// observed Pxf status) BEFORE setupPXFExtensions. When setupPXFExtensions
	// installs >=1 extension it issues a MergePatch annotation write, and the
	// API client writes the server response back into `cluster`, CLEARING the
	// not-yet-persisted in-memory Status.DataLoading (back to nil). dlStatus
	// holds the same pointer reconcilePxf mutated, so re-assigning it after the
	// patch round-trip preserves the observed status for patchDataLoadingStatus.
	dlStatus := cluster.Status.DataLoading

	// Best-effort PXF client extension setup (CREATE EXTENSION IF NOT EXISTS pxf
	// / pxf_fdw). NON-FATAL: the pxf agent is absent in cloudberry-official, so a
	// failure logs a warning and never fails reconcile. Idempotent via the
	// AnnotationPXFExtensionsReady guard.
	r.setupPXFExtensions(ctx, cluster, logger)

	// Restore the in-memory status the annotation-patch round-trip may have
	// cleared, so the counts/jobs/pxf observed status survive to be persisted by
	// patchDataLoadingStatus below (and to keep the L3124 condition honest).
	cluster.Status.DataLoading = dlStatus

	// Reconcile the gpfdist file-server (Deployment/Service/PVC) when
	// dataLoading.gpfdist.enabled (GP.2-GP.5); best-effort GC the objects when
	// disabled. NON-FATAL: an object error is logged but never fails the
	// data-loading reconcile (the gpfdist runtime is independent of the gpload
	// Jobs, which connect to the coordinator directly).
	if gpfdistErr := r.reconcileGpfdist(ctx, cluster); gpfdistErr != nil {
		logger.Warn("gpfdist reconcile failed (non-fatal)", "error", gpfdistErr)
	}

	// Build and launch the per-job data-loading Jobs/CronJobs, harvest the
	// DATALOAD_ROWS markers and enrich the per-job status with the real execution
	// state. The enriched job statuses replace the spec-only jobStatuses above.
	// NON-FATAL on the happy path (the genuine native load path runs on
	// cloudberry-official; pxf:// is generated but image-blocked for execution).
	if jobErr := r.reconcileDataLoadingJobs(ctx, cluster); jobErr != nil {
		return fmt.Errorf("reconciling data loading jobs: %w", jobErr)
	}

	conditionMsg := "Data loading configuration is applied"
	if cluster.Status.DataLoading != nil && cluster.Status.DataLoading.Pxf != nil {
		conditionMsg = fmt.Sprintf("%s; PXF configured: %d servers", conditionMsg, pxfServers)
	}

	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataLoadingConfigured),
		metav1.ConditionTrue,
		"DataLoadingReconciled",
		conditionMsg,
	)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonDataLoadingReconciled,
		fmt.Sprintf("Data loading reconciled: %d jobs configured, %d active",
			configuredJobs, activeJobs))

	if err := r.patchDataLoadingStatus(ctx, cluster); err != nil {
		return fmt.Errorf("patching data loading status: %w", err)
	}

	return nil
}

// cleanupDataLoading tears down the data-loading subsystem when it is disabled
// or absent (DIS.1). It mirrors cleanupWorkload: the disabled branch of the
// spec-driven reconcile is the ONLY caller, so the enabled path is never
// affected. Every step is best-effort / non-fatal (logged + continue), because
// disabling (not deleting) the cluster does NOT fire the ownerRef GC, so these
// explicit deletes are what reclaim the stale resources promptly:
//
//   - gpfdist Deployment/Service/PVC (reuse deleteGpfdistResources);
//   - the data-loading Jobs AND CronJobs (label-scoped GC);
//   - the gpload control-file ConfigMaps (same label-scoped GC);
//   - the cluster PXF NetworkPolicy (SE.5, gated on pxf — reaped on disable too).
//
// The "<cluster>-pxf-servers" ConfigMap and the PXF sidecar removal are owned by
// the CLUSTER controller (ensurePxfServersConfigMap delete-when-disabled and the
// segment-primary StatefulSet re-render without the sidecar), NOT here, because
// the admin reconcile only runs once the cluster is Running.
//
// It then clears Status.DataLoading, zeroes the data-loading + PXF gauges, sets
// the DataLoadingConfigured condition to False (reason DataLoadingDisabled),
// emits a one-shot Normal event on the transition into the disabled state, and
// persists the cleared status. Re-enabling redeploys everything via the normal
// (idempotent get-or-create) reconcile body — no special-casing needed there.
func (r *AdminReconciler) cleanupDataLoading(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	logger := util.LoggerFromContext(ctx)

	// transitioning is true ONLY on the first reconcile that observes the
	// disabled state with status still present, so the Normal event is emitted
	// once (de-dup), mirroring cleanupWorkload's condition-driven semantics.
	transitioning := cluster.Status.DataLoading != nil || cluster.Status.DataLoadingJobs != 0

	logger.Info("cleaning up data loading (disabled)")

	// 1. gpfdist Deployment/Service/PVC (best-effort).
	r.deleteGpfdistResources(ctx, cluster)

	// 2. data-loading Jobs + CronJobs (label-scoped best-effort).
	r.deleteDataLoadingWorkloads(ctx, cluster)

	// 3. gpload control-file ConfigMaps (label-scoped best-effort).
	r.deleteGploadControlFileConfigMaps(ctx, cluster)

	// 4. cluster PXF NetworkPolicy (SE.5) — present only when pxf was enabled;
	// reclaim it on disable too (NotFound-tolerant, non-fatal).
	pxfNetPol := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
		Name: util.PxfNetworkPolicyName(cluster.Name), Namespace: cluster.Namespace}}
	if err := r.client.Delete(ctx, pxfNetPol); err != nil && !apierrors.IsNotFound(err) {
		logger.Warn("best-effort PXF NetworkPolicy delete failed (non-fatal)",
			"name", pxfNetPol.Name, "error", err)
	}

	// 5. Clear status + zero the gauges (honest: disabled => 0 active jobs,
	// 0 configured PXF servers).
	cluster.Status.DataLoading = nil
	cluster.Status.DataLoadingJobs = 0
	r.metrics.SetDataLoadingJobsActive(cluster.Name, cluster.Namespace, 0)
	r.metrics.SetPXFServersConfigured(cluster.Name, cluster.Namespace, 0)

	// 6. Condition False (reason DataLoadingDisabled).
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataLoadingConfigured),
		metav1.ConditionFalse,
		"DataLoadingDisabled",
		"Data loading is disabled",
	)

	// 7. One-shot Normal event on the transition into disabled.
	if transitioning {
		r.recorder.Event(cluster, corev1.EventTypeNormal,
			cbv1alpha1.EventReasonDataLoadingDisabled,
			"Data loading disabled: gpfdist, jobs/cronjobs and control-file ConfigMaps removed")
	}

	// 8. Persist the cleared status. patchDataLoadingStatus dereferences
	// Status.DataLoading, so build a zeroed status object for the patch (the
	// in-memory pointer was cleared above to keep the steady-state path honest).
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase:          dataLoadingPhaseDisabled,
		ConfiguredJobs: 0,
		ActiveJobs:     0,
		Jobs:           nil,
	}
	if err := r.patchDataLoadingStatus(ctx, cluster); err != nil {
		return fmt.Errorf("patching cleared data loading status: %w", err)
	}
	// Drop the in-memory status back to nil AFTER persisting the zeroed snapshot,
	// so subsequent in-process reads see "disabled" (no resurrection) and the
	// next disabled reconcile observes transitioning=false (no event storm).
	cluster.Status.DataLoading = nil
	return nil
}

// reconcilePxf sets the cloudberry_pxf_servers_configured gauge and populates
// Status.DataLoading.Pxf. It is a no-op (returns 0) when PXF is not enabled,
// leaving Pxf status nil. The returned int is the configured server count.
//
// NOTE: the rendered "<cluster>-pxf-servers" ConfigMap is NO LONGER created here.
// Its creation moved to the CLUSTER controller (ensurePxfServersConfigMap, called
// from reconcileSegments before the segment-primary StatefulSet is applied) so it
// exists by the time segment pods first start during INITIALIZATION — the admin
// reconcile only runs once the cluster reaches Running, which was too late and
// caused the pxf-cred-init init container to mount an empty templates volume.
// The admin path keeps status/metric/condition + SetupPXFExtensions only; since
// the only error source (ConfigMap apply) moved out, this no longer returns one.
func (r *AdminReconciler) reconcilePxf(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) int {
	// reconcilePxf no longer returns an error (its only error source moved out),
	// so the span always ends with a nil status; it exists to attribute the
	// phase's latency and nest the downstream observe-probe DB spans.
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcilePxf")
	defer func() { end(nil) }()

	pxf := cluster.Spec.DataLoading.Pxf
	if pxf == nil || !pxf.Enabled {
		return 0
	}

	serverCount := len(pxf.Servers)

	r.metrics.SetPXFServersConfigured(
		cluster.Name, cluster.Namespace, float64(serverCount),
	)

	pxfStatus := &cbv1alpha1.DataLoadingPxfStatus{
		Configured: true,
		Servers:    clampInt32(serverCount),
	}
	// Enrich with the HONEST observed-only runtime fields. Both are best-effort:
	// an unobservable probe leaves the field ABSENT (empty/nil), never fabricated.
	r.populatePxfObservedStatus(ctx, cluster, pxfStatus)
	cluster.Status.DataLoading.Pxf = pxfStatus

	// Publish the honest PXF status gauge only when the status is OBSERVABLE
	// (non-empty); an absent status is skipped so the metric never claims a state
	// that was not observed. Extension count is emitted only when observed.
	r.recordPxfObservedMetrics(cluster, pxfStatus)

	return serverCount
}

// populatePxfObservedStatus enriches the given PXF status with the two
// observed-only runtime fields — Status (segment-primary "pxf" container
// readiness aggregation) and ExtensionsInstalled (live pg_extension probe). Both
// are best-effort and HONEST: any failure or unobservable probe leaves the
// corresponding field ABSENT (Status "" / ExtensionsInstalled nil), so the
// status never claims health or installed state that was not observed.
func (r *AdminReconciler) populatePxfObservedStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	pxfStatus *cbv1alpha1.DataLoadingPxfStatus,
) {
	pxfStatus.Status = r.observePxfStatus(ctx, cluster)
	pxfStatus.ExtensionsInstalled = r.observePxfExtensions(ctx, cluster)
}

// observePxfStatus lists the segment-primary pods with the SHARED selector,
// aggregates the real "pxf" container readiness via the SHARED helper and maps
// it to the honest status string. It is NON-FATAL: a list error leaves the
// status ABSENT ("") and logs a warning — it never fails reconcile and never
// fabricates a status. An unobservable aggregation (no pods / no pxf containers)
// also maps to "" per util.PXFStatusFromReadiness.
func (r *AdminReconciler) observePxfStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(util.SegmentPrimaryPXFSelector(cluster.Name)),
	); err != nil {
		util.LoggerFromContext(ctx).Warn(
			"failed to list segment-primary pods for PXF status (leaving status absent)",
			"cluster", cluster.Name, "error", err)
		return ""
	}
	// Publish the HONEST per-segment-host pxf_service_up gauge (M.1) for every
	// OBSERVED segment-primary pod: 1 when its "pxf" container is Ready, 0
	// otherwise. This is the per-host disaggregation of the aggregate readiness
	// below — emitted only for pods actually listed, so killing a segment's pxf
	// container drives that host's gauge to 0 without fabricating an unobserved
	// host. Non-fatal: it shares the same list as the aggregate status path.
	for host, ready := range util.PXFReadyByHost(podList) {
		r.metrics.SetPXFServiceUp(cluster.Name, cluster.Namespace, host, boolToFloat64Metric(ready))
	}

	readyCount, total := util.PXFReadyCount(podList)
	return util.PXFStatusFromReadiness(readyCount, total)
}

// boolToFloat64Metric maps a readiness bool to the pxf_service_up gauge value
// (1.0 Ready, 0.0 not). Kept local to the controller so the metrics package's
// gauge contract (a float64) stays the single boundary.
func boolToFloat64Metric(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// observePxfExtensions best-effort probes the live pg_extension catalog for the
// installed PXF client extensions, modeled on setupPXFExtensions and capped by
// pxfExtensionSetupTimeout. It is NON-FATAL: a nil dbFactory, a connect error or
// a query error all leave the result ABSENT (nil) and log — so an unobservable
// probe is honestly reported as absent rather than as "[]" (none installed). A
// reachable DB with no PXF extensions also returns nil (absent), matching the
// honesty invariant that an empty array is never synthesized.
func (r *AdminReconciler) observePxfExtensions(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) []string {
	logger := util.LoggerFromContext(ctx)
	if r.dbFactory == nil {
		logger.Debug("skipping PXF extension probe: no dbFactory configured")
		return nil
	}

	dbCtx, cancel := context.WithTimeout(ctx, pxfExtensionSetupTimeout)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		logger.Warn("skipping PXF extension probe: DB not available (leaving extensions absent)",
			"error", err)
		return nil
	}
	defer dbClient.Close()

	extensions, listErr := dbClient.ListPXFExtensions(dbCtx)
	if listErr != nil {
		logger.Warn("PXF extension probe failed (leaving extensions absent)", "error", listErr)
		return nil
	}
	// A reachable DB with zero extensions returns an empty slice from the probe;
	// normalize it to nil so the field stays ABSENT (never a synthesized "[]").
	if len(extensions) == 0 {
		return nil
	}
	return extensions
}

// recordPxfObservedMetrics publishes the honest PXF observability gauges from an
// already-computed status. The status gauge is set ONLY when the status is
// OBSERVABLE (non-empty); an absent status is skipped so the metric never claims
// a state that was not observed. The extensions-installed count is published
// only when extensions were observed.
func (r *AdminReconciler) recordPxfObservedMetrics(
	cluster *cbv1alpha1.CloudberryCluster,
	pxfStatus *cbv1alpha1.DataLoadingPxfStatus,
) {
	if value, ok := pxfStatusMetricValue(pxfStatus.Status); ok {
		r.metrics.SetPXFStatus(cluster.Name, cluster.Namespace, value)
	}
	if len(pxfStatus.ExtensionsInstalled) > 0 {
		r.metrics.SetPXFExtensionsInstalled(
			cluster.Name, cluster.Namespace, float64(len(pxfStatus.ExtensionsInstalled)),
		)
	}
}

// pxfStatusMetricValue maps the honest PXF status string to its gauge value
// (0=Stopped, 1=Running, 2=Error). The bool is false for the ABSENT/unobservable
// status ("") so the caller can SKIP emitting the gauge entirely — keeping the
// metric honest (no value implies an observation that did not happen).
func pxfStatusMetricValue(status string) (float64, bool) {
	switch status {
	case util.PXFStatusStopped:
		return 0, true
	case util.PXFStatusRunning:
		return 1, true
	case util.PXFStatusError:
		return 2, true
	default:
		return 0, false
	}
}

// clampInt32 safely narrows a non-negative int to int32, capping at math.MaxInt32
// to avoid integer-overflow on platforms where int is 64-bit. The PXF server
// count is bounded by the CRD in practice; the clamp is a defensive guard.
func clampInt32(n int) int32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// dataLoadingPhaseConfigured is the data-loading phase reported once an enabled
// DataLoading spec has been reconciled.
const dataLoadingPhaseConfigured = "Configured"

// dataLoadingPhaseDisabled is the data-loading phase persisted by the teardown
// path (DIS.1) so the status honestly reflects the subsystem being off.
const dataLoadingPhaseDisabled = "Disabled"

// patchDataLoadingStatus explicitly patches the data loading status fields.
// It mirrors patchQueryStatus / patchFTSStatus: a manual MergePatch is used so
// that zero-valued counters and an empty jobs slice are always included (an
// empty jobs array clears any previously reported jobs instead of being
// dropped by json.Marshal omitempty).
//
// IMPORTANT: this is a MID-RECONCILE status patch. controller-runtime's
// Status().Patch writes the SERVER's response object back into the passed-in
// `cluster`, which OVERWRITES any in-memory cluster.Status fields that earlier
// sub-reconcilers set but the single authoritative final patchStatus in
// Reconcile has not yet persisted (e.g. cluster.Status.Conditions written by
// reconcileWorkload/reconcileBackup, lastBackupStatus, etc.). Because this
// merge patch only touches dataLoading/dataLoadingJobs, the round-trip would
// silently DROP those not-yet-persisted in-memory fields, and the final
// patchStatus would then persist a status missing them (the proven
// status-persistence bug). To stay consistent with the "one final patchStatus"
// design while still issuing this patch (the disabled-teardown and steady-state
// callers, plus their unit tests, rely on it firing), we SNAPSHOT the in-memory
// status before the patch and RESTORE it afterward. The snapshot already holds
// the data-loading mutation being persisted here, so after restore the
// in-memory object and the server agree on dataLoading AND every other
// in-memory status field survives intact for the final patchStatus.
func (r *AdminReconciler) patchDataLoadingStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	dataLoading := map[string]interface{}{
		"phase":          cluster.Status.DataLoading.Phase,
		"configuredJobs": cluster.Status.DataLoading.ConfiguredJobs,
		"activeJobs":     cluster.Status.DataLoading.ActiveJobs,
	}
	if len(cluster.Status.DataLoading.Jobs) == 0 {
		dataLoading["jobs"] = []interface{}{}
	} else {
		dataLoading["jobs"] = cluster.Status.DataLoading.Jobs
	}

	// Include the spec-derived PXF status sub-object only when PXF is enabled, so
	// default/non-PXF clusters keep status.dataLoading.pxf unset (nil).
	if cluster.Status.DataLoading.Pxf != nil {
		pxf := cluster.Status.DataLoading.Pxf
		pxfMap := map[string]interface{}{
			"configured": pxf.Configured,
			"servers":    pxf.Servers,
		}
		// HONESTY: emit the observed-only fields ONLY when they are set in memory.
		// An ABSENT status ("") / empty extensions list must NOT round-trip as an
		// empty string/array, which would falsely claim an observation. Omitting
		// the key leaves status.dataLoading.pxf.{status,extensionsInstalled} unset.
		if pxf.Status != "" {
			pxfMap["status"] = pxf.Status
		}
		if len(pxf.ExtensionsInstalled) > 0 {
			pxfMap["extensionsInstalled"] = pxf.ExtensionsInstalled
		}
		dataLoading["pxf"] = pxfMap
	}

	patch, err := json.Marshal(map[string]interface{}{
		patchKeyStatus: map[string]interface{}{
			"dataLoadingJobs": cluster.Status.DataLoadingJobs,
			"dataLoading":     dataLoading,
		},
	})
	if err != nil {
		return fmt.Errorf("marshaling data loading status patch: %w", err)
	}

	// Snapshot the in-memory status BEFORE the patch and restore it AFTER, so the
	// Status().Patch round-trip cannot clobber cross-cutting in-memory status
	// (conditions, backup, workload, …) that earlier sub-reconcilers set but the
	// final patchStatus has not yet persisted. The snapshot already contains the
	// dataLoading mutation this patch persists, so the restore keeps the in-memory
	// object and the server in agreement on dataLoading while preserving the rest.
	statusSnapshot := cluster.Status.DeepCopy()
	patchErr := r.client.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch))
	cluster.Status = *statusSnapshot
	return patchErr
}

// reconcileStorage reconciles storage management configuration and status
// (spec 13 §Reconciliation). It implements:
//   - R.1: gate on diskMonitoring (early-return no-op when storage is absent or
//     disk monitoring is off).
//   - C.1: accept/parse the recommendationScan config (schedule + thresholds).
//   - C.3: accept the threshold set (bloat/skew/age/indexBloat/scanDuration)
//     unchanged (passed to the CronJob as env vars by the builder).
//   - C.5: create/update (or GC) the scheduled recommendation-scan CronJob via
//     ensureRecommendationScanCronJob.
//   - R.2: measure worst-case segment-volume filesystem usage (recordDiskUsage).
//   - S.1: populate status.diskUsagePercent with the current measured value.
//   - M.1: publish cloudberry_disk_usage_percent from the same measured value so
//     the gauge matches the status.
//   - R.5: set the StorageConfigured=True status condition.
func (r *AdminReconciler) reconcileStorage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileStorage")
	defer func() { end(err) }()

	// R.1: diskMonitoring gate. Storage absent or disk monitoring off => no-op
	// for measurement. The whole storage block off means NO recommendations can
	// be produced, so clear any stale disk-usage (C.2 reset-on-disable) + stale
	// recommendation count/gauges (C.4 clear-on-disable) before returning. The
	// CronJob GC on this storage-off path is owned by refreshStorageOnSteadyState
	// (see R5) — not duplicated here to avoid a behavior shift. clearStorageSignals
	// persists only when there is an actual stale value (R6).
	if cluster.Spec.Storage == nil || !cluster.Spec.Storage.DiskMonitoring {
		r.clearStorageSignals(ctx, cluster, util.LoggerFromContext(ctx))
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

	// R.2: measure disk usage, populate Status.DiskUsagePercent (S.1), and publish
	// the gauge from the measured value (M.1). Best-effort and non-fatal.
	r.recordDiskUsage(ctx, cluster, logger)

	// C.1/C.3: process the recommendationScan config (schedule + thresholds).
	if cluster.Spec.Storage.RecommendationScan != nil &&
		cluster.Spec.Storage.RecommendationScan.Enabled {
		logger.Info("recommendation scan is configured",
			"schedule", cluster.Spec.Storage.RecommendationScan.Schedule,
			"bloatThreshold", cluster.Spec.Storage.RecommendationScan.BloatThreshold,
			"skewThreshold", cluster.Spec.Storage.RecommendationScan.SkewThreshold,
			"ageThreshold", cluster.Spec.Storage.RecommendationScan.AgeThreshold,
			"indexBloatThreshold", cluster.Spec.Storage.RecommendationScan.IndexBloatThreshold,
		)
		// R.3/R.4/S.2/M.2/M.4: run all four threshold-aware recommendation scans,
		// count per type, publish recommendations_total{type} + table_bloat_ratio,
		// and set Status.RecommendationCount to the CURRENT total (not stale).
		// Best-effort and non-fatal: a DB/connection failure only skips the scan.
		r.recordRecommendations(ctx, cluster, logger)
	} else {
		// C.4 clear-on-disable: diskMonitoring is ON but the scan is nil/disabled.
		// No recommendations are produced; reset the count + per-type gauges so a
		// prior enabled->disabled scan does not leave a stale signal. This else is
		// mutually exclusive with recordRecommendations (the idempotency guard:
		// clear never runs on the enabled path). The controller's end-of-reconcile
		// status patch persists the zeroed count — no extra patch needed here.
		r.clearRecommendations(cluster)
	}

	// C.5: materialize the scheduled recommendation-scan CronJob. Called
	// UNCONDITIONALLY so create AND the enabled->disabled GC are both driven
	// every reconcile (the builder returns nil when the scan is disabled / has
	// no schedule, which triggers the delete-if-exists path). A create/update
	// error surfaces as a reconcile error (the no-false-positive control).
	if csErr := r.ensureRecommendationScanCronJob(ctx, cluster); csErr != nil {
		return csErr
	}

	// R.5: storage reconcile succeeded => StorageConfigured=True. recordRecommendations
	// now OWNS Status.RecommendationCount (set to the current scan total), so it is
	// not re-assigned here; the controller's end-of-reconcile status patch persists it.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionStorageConfigured),
		metav1.ConditionTrue,
		"StorageReconciled",
		"Storage management is configured",
	)

	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonStorageReconciled,
		fmt.Sprintf("Storage management reconciled: diskMonitoring=%t, recommendations=%d",
			cluster.Spec.Storage.DiskMonitoring, cluster.Status.RecommendationCount))

	return nil
}

// maxBloatRatioTables bounds the number of per-table bloat ratios published to
// the cloudberry_table_bloat_ratio gauge, keeping metric cardinality in check.
const maxBloatRatioTables = 20

// Recommendation type labels for cloudberry_recommendations_total{type}.
const (
	recTypeBloat      = "bloat"
	recTypeSkew       = "skew"
	recTypeAge        = "age"
	recTypeIndexBloat = "index_bloat"
)

// recommendationTypes is the canonical, ordered set of recommendation types.
// recordRecommendations publishes cloudberry_recommendations_total for EVERY
// type in this slice on each scan (0 when none) so a cleared / out-of-threshold
// type's gauge resets to 0 rather than going stale (117a-CLEAR / 117b-BOUNDARY).
var recommendationTypes = []string{recTypeBloat, recTypeSkew, recTypeAge, recTypeIndexBloat}

// Scan-duration cap policy constants (C.10).
const (
	// defaultScanDuration is the fallback cap used when scanDuration is empty,
	// unparseable, or non-positive. It preserves the historical hardcoded 10s
	// behavior so already-deployed CRs with an empty scanDuration are unaffected.
	defaultScanDuration = 10 * time.Second
	// maxScanDuration is the ceiling guard so a typo like "2000h" cannot pin the
	// reconcile worker for days.
	maxScanDuration = 24 * time.Hour
	// connectTimeout is the FIXED budget for establishing the DB client
	// (dbFactory.NewClient) in recordRecommendations. It is intentionally
	// SEPARATE from the scanDuration cap: connection establishment (~10-40ms in
	// practice) must not consume the scan budget, otherwise a tiny scanDuration
	// (e.g. "1ms"/"50ms") would trip on NewClient FIRST and the C.10 truncation
	// signal — which must key off the QUERY phase — would never be observable.
	// It preserves the prior 10s connection behavior.
	connectTimeout = 10 * time.Second
)

// resolveScanDuration derives the C.10 scan-context cap from the configured
// scanDuration, parsing it with time.ParseDuration and applying a defensive
// fallback/clamp policy:
//
//   - empty, unparseable, or <= 0 -> defaultScanDuration (10s). This defends in
//     depth (the field is a free string at the type level and a controller call
//     outside the webhook path must never run unbounded) while preserving the
//     prior hardcoded 10s behavior for the empty case.
//   - > maxScanDuration (24h)     -> clamped down to 24h (ceiling guard).
//   - otherwise                   -> the parsed value VERBATIM, so a tiny "10ms"
//     deterministically trips the deadline (no production floor that would mask
//     the configured cap).
//
// The webhook (W.5) rejects an unparseable value upstream; this helper is the
// runtime guard that guarantees a sane, bounded deadline regardless.
func resolveScanDuration(scan *cbv1alpha1.RecommendationScanSpec, logger *slog.Logger) time.Duration {
	if scan == nil || scan.ScanDuration == "" {
		return defaultScanDuration
	}
	parsed, err := time.ParseDuration(scan.ScanDuration)
	if err != nil {
		logger.Warn("invalid scanDuration, falling back to default cap",
			"scanDuration", scan.ScanDuration, "default", defaultScanDuration, "error", err)
		return defaultScanDuration
	}
	if parsed <= 0 {
		return defaultScanDuration
	}
	if parsed > maxScanDuration {
		return maxScanDuration
	}
	return parsed
}

// recordRecommendations runs all FOUR threshold-aware recommendation scans
// (bloat/skew/age/index_bloat) for an enabled recommendationScan, COUNTS the
// active recommendations per type, and publishes the results in a single pass:
//
//   - R.3 — it PROCESSES the recommendationScan config: it reads the four CRD
//     thresholds (bloat/skew/age/indexBloat) into a db.RecommendationThresholds
//     and threads that into each DB query.
//   - S.2/R.4 — it sets cluster.Status.RecommendationCount to the CURRENT total
//     active count (sum across the four types), NOT the stale prior value.
//   - M.2 — it sets cloudberry_recommendations_total{type} for EACH type from the
//     per-type count, including 0 for absent/cleared types so the gauge resets.
//   - M.4 — it publishes per-table bloat ratios (cloudberry_table_bloat_ratio)
//     from the SAME bloat scan, so the bloat query runs ONCE per reconcile.
//   - duration — it observes recommendation_scan_duration_seconds over the scan.
//
// Best-effort and non-fatal: a missing dbFactory, a NewClient failure, or any
// single Get* error SKIPS that contribution WITHOUT fabricating a count for the
// failing type and WITHOUT failing the reconcile. HONEST per-type fallback
// mirrors Scenario 116: a missing gp_toolkit view (skew) or absent catalog
// column returns no rows, so that type counts 0 + a debug log, never a fabricated
// value (RT.1–RT.4 / C.6–C.9).
//
// M.2 == count INVARIANT: Status.RecommendationCount == sum over types of the
// value passed to SetRecommendationsTotal{type}. Both derive from the SAME
// per-type counts computed in this single pass — the stale status count is never
// read to publish the metric.
func (r *AdminReconciler) recordRecommendations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	if r.dbFactory == nil {
		logger.Debug("skipping recommendation scan, no DB factory configured")
		return
	}

	ctx, span := telemetry.StartSpan(ctx, adminControllerName, "recordRecommendations")
	defer span.End()

	// R.3: build the per-type thresholds from the CRD recommendationScan spec.
	scan := cluster.Spec.Storage.RecommendationScan

	// CONNECTION budget (FIXED, SEPARATE from the scan cap): establishing the DB
	// client is bounded by connectTimeout (the prior 10s behavior), NOT by the
	// configured scanDuration. Keeping connection establishment out of the scan
	// budget is what makes the C.10 truncation signal observable: a tiny
	// scanDuration (e.g. "1ms"/"50ms") would otherwise trip on NewClient FIRST
	// and the truncation detection (which must key off the QUERY phase) would
	// never be reached.
	connectCtx, connectCancel := context.WithTimeout(ctx, connectTimeout)
	defer connectCancel()

	dbClient, err := r.dbFactory.NewClient(connectCtx, cluster)
	if err != nil {
		// DB unavailable: the scan never ran. Record an honest "skipped"
		// outcome (B-2) so the previously-silent early return is visible.
		r.metrics.RecordRecommendationScan(cluster.Name, cluster.Namespace, metricResultSkipped)
		logger.Debug("skipping recommendation scan, DB not available", "error", err)
		return
	}
	defer dbClient.Close()

	th := db.RecommendationThresholds{
		Bloat:      scan.BloatThreshold,
		Skew:       scan.SkewThreshold,
		Age:        scan.AgeThreshold,
		IndexBloat: scan.IndexBloatThreshold,
	}

	// SCAN budget (C.10): bound ONLY the four Get* queries by the configured
	// scanDuration. scanCtx is derived from the parent ctx (NOT connectCtx, whose
	// connectTimeout deadline is unrelated to the query budget) so a single
	// shared scanDuration deadline caps the TOTAL of all four queries (one shared
	// budget, NOT 4x the cap); a deadline-trip on query N short-circuits the
	// rest, which observe the canceled ctx and error out fast. Because the
	// connection is already established above, a tiny cap now deterministically
	// truncates the QUERY phase (the intended C.10 behavior), independent of
	// connection time.
	scanCtx, scanCancel := context.WithTimeout(ctx, resolveScanDuration(scan, logger))
	defer scanCancel()

	start := time.Now()
	counts, bloatRecs := r.scanRecommendations(scanCtx, span, dbClient, th, logger)
	// M.3: observe the actual (capped) elapsed time of the scan loop — under a
	// truncated run this reflects the capped run, not an unbounded scan.
	r.metrics.ObserveRecommendationScanDuration(cluster.Name, cluster.Namespace, time.Since(start))

	// C.10 truncation signal (118b): if the SCAN context hit its deadline mid-run
	// the scan was capped. Record the truncation on the status (for kubectl
	// visibility) and on the counter metric (for alerting). The flag is set on
	// EVERY scan so it is always current and never sticky. The per-type counts
	// below are HONEST: only the types that completed before the deadline are
	// counted; un-run types contribute 0 (no fabrication).
	truncated := errors.Is(scanCtx.Err(), context.DeadlineExceeded)
	cluster.Status.RecommendationScanTruncated = truncated
	if truncated {
		r.metrics.IncRecommendationScanTruncated(cluster.Name, cluster.Namespace)
		logger.Warn("recommendation scan truncated at scanDuration cap",
			"scanDuration", scan.ScanDuration)
	}
	now := metav1.Now()
	cluster.Status.LastRecommendationScanTime = &now

	// M.2 + S.2/R.4: publish the per-type gauge for ALL types (0s included) and
	// sum the SAME per-type counts into the status total, preserving the
	// M.2 == count invariant.
	total := int32(0)
	for _, recType := range recommendationTypes {
		r.metrics.SetRecommendationsTotal(cluster.Name, cluster.Namespace, recType, counts[recType])
		total += int32(counts[recType])
	}
	cluster.Status.RecommendationCount = total

	// M.4: publish per-table bloat ratios from the bloat recs already fetched in
	// the single pass above (no second bloat query).
	r.publishTableBloatRatios(cluster, bloatRecs, logger)

	// B-2: a completed scan (even if truncated at the scanDuration cap) is a
	// "success" outcome. Truncation stays its own orthogonal signal
	// (IncRecommendationScanTruncated above) and is intentionally NOT conflated
	// with this result enum.
	r.metrics.RecordRecommendationScan(cluster.Name, cluster.Namespace, metricResultSuccess)

	logger.Debug("recorded recommendations", "total", total,
		"bloat", int32(counts[recTypeBloat]), "skew", int32(counts[recTypeSkew]),
		"age", int32(counts[recTypeAge]), "indexBloat", int32(counts[recTypeIndexBloat]))
}

// scanRecommendations runs the four threshold-aware Get* scans once, returning
// the per-type active counts and the bloat recs (reused for M.4). A single Get*
// error is logged (and recorded on the span) and SKIPPED — that type contributes
// 0 to the counts without failing the scan or fabricating a value.
func (r *AdminReconciler) scanRecommendations(
	dbCtx context.Context,
	span trace.Span,
	dbClient db.Client,
	th db.RecommendationThresholds,
	logger *slog.Logger,
) (map[string]float64, []db.Recommendation) {
	counts := map[string]float64{}
	var bloatRecs []db.Recommendation

	fetchers := []struct {
		recType string
		fetch   func(context.Context, db.RecommendationThresholds) ([]db.Recommendation, error)
	}{
		{recTypeBloat, dbClient.GetBloatRecommendations},
		{recTypeSkew, dbClient.GetSkewRecommendations},
		{recTypeAge, dbClient.GetAgeRecommendations},
		{recTypeIndexBloat, dbClient.GetIndexBloatRecommendations},
	}

	for _, f := range fetchers {
		recs, fetchErr := r.fetchRecommendationType(dbCtx, span, f.recType, th, f.fetch, logger)
		if fetchErr != nil {
			continue
		}
		for i := range recs {
			counts[recs[i].Type]++
		}
		if f.recType == recTypeBloat {
			bloatRecs = recs
		}
	}

	return counts, bloatRecs
}

// fetchRecommendationType runs a single per-type recommendation fetch inside a
// dedicated child span (O-3) so a slow or failing individual recommendation
// query is localizable in a trace. The span carries only the bounded `rec_type`
// enum attribute (never resource-derived strings). A fetch error is recorded on
// BOTH the child span and the parent span (preserving the prior parent
// behavior) and returned so the caller skips that type honestly.
func (r *AdminReconciler) fetchRecommendationType(
	dbCtx context.Context,
	parentSpan trace.Span,
	recType string,
	th db.RecommendationThresholds,
	fetch func(context.Context, db.RecommendationThresholds) ([]db.Recommendation, error),
	logger *slog.Logger,
) ([]db.Recommendation, error) {
	childCtx, childSpan := telemetry.StartSpan(dbCtx, adminControllerName,
		"controller.scanRecommendations.fetch",
		trace.WithAttributes(attribute.String("rec_type", recType)))
	defer childSpan.End()

	recs, fetchErr := fetch(childCtx, th)
	if fetchErr != nil {
		telemetry.SetSpanError(childSpan, fetchErr)
		telemetry.SetSpanError(parentSpan, fetchErr)
		logger.Warn("recommendation fetch failed, skipping type",
			"type", recType, "error", fetchErr)
		return nil, fetchErr
	}
	return recs, nil
}

// publishTableBloatRatios publishes the dead-tuple bloat ratio of the top-N
// most-bloated tables to the cloudberry_table_bloat_ratio gauge (M.4), capped by
// maxBloatRatioTables to bound metric cardinality. The recs are the already
// fetched bloat recommendations (ordered by dead_pct DESC), so no extra query is
// issued.
func (r *AdminReconciler) publishTableBloatRatios(
	cluster *cbv1alpha1.CloudberryCluster,
	recs []db.Recommendation,
	logger *slog.Logger,
) {
	limit := len(recs)
	if limit > maxBloatRatioTables {
		limit = maxBloatRatioTables
	}
	for i := 0; i < limit; i++ {
		rec := recs[i]
		table := rec.Table
		if rec.Schema != "" {
			table = rec.Schema + "." + rec.Table
		}
		r.metrics.SetTableBloatRatio(cluster.Name, cluster.Namespace, table, rec.Ratio)
	}
	logger.Debug("published table bloat ratios", "tables", limit)
}

// recordDiskUsage measures disk usage and populates Status.DiskUsagePercent with
// the CURRENT measured value (S.1), persists it via patchStatus, and publishes
// the cloudberry_disk_usage_percent gauge FROM the measured value so the metric
// matches the status (M.1). It is the measurement step of R.2.
//
// It uses a ROBUST, PORTABLE fallback chain so it produces a real value on
// Cloudberry 2.1.0 (where gp_toolkit.gp_disk_free does NOT exist) while still
// preferring the TRUE filesystem source when it is present:
//
//  1. PREFERRED (true filesystem usage): GetDiskUsagePercent reads
//     gp_toolkit.gp_disk_free and returns the worst-case
//     100*(df_total-df_free)/df_total across segment volumes. This is the real
//     filesystem usage of the data volumes.
//  2. FALLBACK (portable LOGICAL-vs-provisioned proxy): when GetDiskUsagePercent
//     returns db.ErrDiskUsageUnavailable (view/columns absent), the percent is
//     computed as clamp(100 * usedBytes / provisionedBytes), where usedBytes is
//     the LOGICAL cluster data size (GetClusterDataSizeBytes) and provisionedBytes
//     is the CRD-provisioned PVC capacity (see computeProvisionedCapacityBytes).
//     This measures LOGICAL stored data against PROVISIONED capacity — NOT the
//     raw filesystem usage — and is documented as such so the signal is honest.
//
// It is best-effort and non-fatal: a missing dbFactory, a NewClient failure, or
// BOTH measurement sources failing simply SKIPS the measurement WITHOUT
// fabricating a value or overwriting the prior status (so a transient DB outage
// never reports a misleading "disk empty" signal). A child span makes a slow
// query visible in traces.
//
// M.1==S.1 invariant: both the persisted status and the published gauge derive
// from the single measured local pct; the stale Status.DiskUsagePercent is never
// read to publish the metric.
func (r *AdminReconciler) recordDiskUsage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	if r.dbFactory == nil {
		logger.Debug("skipping disk usage measurement, no DB factory configured")
		return
	}

	ctx, span := telemetry.StartSpan(ctx, adminControllerName, "recordDiskUsage")
	defer span.End()

	// Use a short timeout so DB connection issues don't block the reconcile.
	dbCtx, cancel := context.WithTimeout(ctx, dbOpTimeout)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		// DB unavailable: the scan never ran. Record an honest "skipped"
		// outcome (B-1) so the gap is visible for alerting.
		r.metrics.RecordDiskUsageScan(cluster.Name, cluster.Namespace, metricResultSkipped)
		logger.Debug("skipping disk usage measurement, DB not available", "error", err)
		return
	}
	defer dbClient.Close()

	pct, ok := r.measureDiskUsagePercent(dbCtx, span, cluster, dbClient, logger)
	if !ok {
		// Both the gp_disk_free path and the logical-proxy fallback failed:
		// SKIP, do not fabricate (S.1/R.2). This is a real failure to obtain a
		// measurement, recorded as result="error" (B-1).
		r.metrics.RecordDiskUsageScan(cluster.Name, cluster.Namespace, metricResultError)
		return
	}

	r.publishDiskUsage(ctx, cluster, pct, logger)
	r.metrics.RecordDiskUsageScan(cluster.Name, cluster.Namespace, metricResultSuccess)
}

// measureDiskUsagePercent runs the fallback chain and returns the measured disk
// usage percentage (0..100). It first tries the PREFERRED true filesystem source
// (db.GetDiskUsagePercent / gp_toolkit.gp_disk_free); only when that returns
// db.ErrDiskUsageUnavailable does it fall back to the PORTABLE logical-size vs
// provisioned-capacity proxy. ok is false (and the caller skips honestly) when
// the preferred path fails for any other reason or the fallback cannot be
// computed.
func (r *AdminReconciler) measureDiskUsagePercent(
	dbCtx context.Context,
	span trace.Span,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	logger *slog.Logger,
) (int32, bool) {
	pct, err := dbClient.GetDiskUsagePercent(dbCtx)
	if err == nil {
		return pct, true
	}
	if !errors.Is(err, db.ErrDiskUsageUnavailable) {
		// A real query/connectivity error on the preferred path: skip, do not
		// fabricate (S.1/R.2).
		telemetry.SetSpanError(span, err)
		logger.Warn("failed to measure disk usage, skipping", "error", err)
		return 0, false
	}

	// gp_toolkit.gp_disk_free is absent on this server version (e.g. Cloudberry
	// 2.1.0). Fall back to the portable LOGICAL-vs-provisioned proxy.
	logger.Debug("gp_disk_free unavailable, using logical-size proxy", "error", err)
	return r.measureLogicalDiskUsageProxy(dbCtx, span, cluster, dbClient, logger)
}

// measureLogicalDiskUsageProxy computes the PORTABLE fallback percentage:
// clamp(100 * usedBytes / provisionedBytes), where usedBytes is the LOGICAL
// cluster data size (sum of pg_database_size, always available) and
// provisionedBytes is the CRD-provisioned PVC capacity. This is honestly a
// LOGICAL-size-vs-provisioned-capacity proxy, NOT raw filesystem usage. ok is
// false when the used size cannot be read or the provisioned capacity is
// unknown/zero, so the caller skips without fabricating.
func (r *AdminReconciler) measureLogicalDiskUsageProxy(
	dbCtx context.Context,
	span trace.Span,
	cluster *cbv1alpha1.CloudberryCluster,
	dbClient db.Client,
	logger *slog.Logger,
) (int32, bool) {
	usedBytes, err := dbClient.GetClusterDataSizeBytes(dbCtx)
	if err != nil {
		telemetry.SetSpanError(span, err)
		logger.Warn("failed to measure cluster data size, skipping", "error", err)
		return 0, false
	}

	provisionedBytes := computeProvisionedCapacityBytes(cluster)
	if provisionedBytes <= 0 {
		// No usable provisioned capacity (unparseable/zero spec.segments.storage.size):
		// skip without fabricating a percentage.
		logger.Warn("cannot compute disk usage proxy, provisioned capacity unknown",
			"usedBytes", usedBytes)
		return 0, false
	}

	pct := clampUsagePercent(usedBytes, provisionedBytes)
	logger.Debug("computed logical disk usage proxy",
		"usedBytes", usedBytes, "provisionedBytes", provisionedBytes, "percent", pct)
	return pct, true
}

// clampUsagePercent computes 100 * used / provisioned, truncated to an int32 and
// clamped to the inclusive 0..100 range. The ratio is computed in float64 to
// avoid any int64 overflow from a 100*used multiplication on extreme (petabyte+)
// sizes; the float precision loss is immaterial for an integer percentage.
// provisioned <= 0 yields 0 (defensive; the caller already guards this).
func clampUsagePercent(used, provisioned int64) int32 {
	if provisioned <= 0 {
		return 0
	}
	pct := int32((float64(used) / float64(provisioned)) * 100.0)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// computeProvisionedCapacityBytes returns the total PVC capacity (in bytes)
// provisioned for the cluster's SEGMENT data volumes, derived from the CRD spec.
//
// Formula (documented, consistent):
//
//	provisioned = perVolumeBytes * volumeCount
//	perVolumeBytes = parse(spec.segments.storage.size)   // resource.Quantity
//	volumeCount    = primaries + mirrors
//	primaries      = spec.segments.count
//	mirrors        = spec.segments.count                 // only when mirroring enabled
//
// Rationale for which components are counted:
//   - PRIMARIES: spec.segments.count primary segments, each with its OWN PVC of
//     spec.segments.storage.size — always counted.
//   - MIRRORS: when spec.segments.mirroring.Enabled is true, each primary has a
//     mirror with its own PVC of the SAME spec.segments.storage.size, so mirrors
//     double the provisioned segment capacity — counted only when mirroring is on.
//   - The coordinator/standby volumes are intentionally NOT counted: the
//     numerator (sum of pg_database_size) reflects user data that lives on the
//     SEGMENTS, so the denominator is kept to the segment data volumes for a
//     consistent, comparable ratio.
//
// Returns 0 when spec.segments.storage.size is empty/unparseable or the segment
// count is non-positive, signaling the caller to skip the proxy honestly.
func computeProvisionedCapacityBytes(cluster *cbv1alpha1.CloudberryCluster) int64 {
	seg := cluster.Spec.Segments
	if seg.Count <= 0 || seg.Storage.Size == "" {
		return 0
	}

	qty, err := resource.ParseQuantity(seg.Storage.Size)
	if err != nil {
		return 0
	}
	perVolumeBytes, ok := qty.AsInt64()
	if !ok || perVolumeBytes <= 0 {
		return 0
	}

	volumeCount := int64(seg.Count)
	if seg.Mirroring != nil && seg.Mirroring.Enabled {
		volumeCount += int64(seg.Count)
	}

	return perVolumeBytes * volumeCount
}

// publishDiskUsage persists the measured disk usage percentage into the cluster
// status (S.1) and publishes the cloudberry_disk_usage_percent gauge FROM the
// SAME value (M.1), keeping metric == status as a single source of truth.
func (r *AdminReconciler) publishDiskUsage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	pct int32,
	logger *slog.Logger,
) {
	// S.1: status carries the CURRENT measured value (tracks growth, never sticky).
	cluster.Status.DiskUsagePercent = pct
	if perr := patchStatus(ctx, r.client, cluster); perr != nil {
		// Non-fatal: still publish the metric from the measured value below so
		// the gauge stays current even if the status patch transiently fails.
		logger.Warn("failed to persist disk usage status", "error", perr)
	}

	// M.1: publish FROM the measured value so metric == status (single source).
	r.metrics.SetDiskUsagePercent(cluster.Name, cluster.Namespace, float64(pct))
	logger.Debug("recorded disk usage", "percent", pct)
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
		// H-4: MERGE desired annotations into the live object instead of a
		// wholesale replace, so third-party / controller-runtime annotations
		// (e.g. kubectl.kubernetes.io/last-applied-configuration) on the live
		// ConfigMap survive the update. Operator-owned data is still fully
		// owned via existing.Data = desired.Data above.
		existing.Annotations = mergeAnnotations(existing.Annotations, desired.Annotations)
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

// mergeAnnotations copies every key/value from desired into existing,
// allocating the map when existing is nil, and returns the merged map. Keys
// present on existing but absent from desired are PRESERVED (H-4): this keeps
// third-party / controller-runtime annotations on the live object intact while
// still applying every operator-desired annotation. Desired values win on key
// collisions.
func mergeAnnotations(existing, desired map[string]string) map[string]string {
	if existing == nil {
		existing = make(map[string]string, len(desired))
	}
	for k, v := range desired {
		existing[k] = v
	}
	return existing
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
func (r *AdminReconciler) reconcileConfig(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileConfig")
	defer func() { end(err) }()

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

	return ctrl.Result{RequeueAfter: r.requeueDefault()}, nil
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

	if err := util.PatchStatefulSetRestartTrigger(ctx, r.client, namespace, name); err != nil {
		return err
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
) (result ctrl.Result, err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "handleMaintenance",
		attribute.String("maintenance.operation", maintenance))
	defer func() { end(err) }()

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
			return ctrl.Result{RequeueAfter: r.requeueDefault()}, nil
		}
	}

	// Fallback: create a maintenance Job for operations that failed via DB client
	// or when the DB factory is not available.
	timestamp := time.Now().Format(util.BackupTimestampLayout)
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

	return ctrl.Result{RequeueAfter: r.requeueDefault()}, nil
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
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=create;delete;get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=create;delete;get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=create;delete;get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=create;delete;get;list;watch;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=create;delete;get;list;watch;update;patch
func (r *AdminReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Owns(&batchv1.Job{}).
		Owns(&batchv1.CronJob{}).
		Named(adminControllerName).
		Complete(r)
}
