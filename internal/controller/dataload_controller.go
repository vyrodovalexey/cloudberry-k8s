package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// dataLoadJobTypeGpload is the gpload job-type discriminator (mirrors the
// builder's dataLoadTypeGpload), used to gate control-file ConfigMap creation.
const dataLoadJobTypeGpload = "gpload"

const (
	// dataLoadRowsMarkerPrefix is the stdout/termination-message prefix the
	// data-loading Job's load script emits with the INSERT rowcount. It mirrors
	// builder.dataLoadRowsMarker; the controller harvests it from the Job pod's
	// terminated container message to populate status + the rows metric.
	dataLoadRowsMarkerPrefix = "DATALOAD_ROWS="

	// dataLoadBytesMarkerPrefix is the stdout/termination-message prefix the
	// data-loading Job's load script emits with the loaded BYTE count (M.10). It
	// mirrors builder.dataLoadBytesMarker; the controller harvests it from the Job
	// pod's terminated container message to populate the bytes metric. It is
	// emitted by the builder ONLY when a real byte count was measured, so an
	// absent marker means the bytes metric stays honestly absent for that job.
	dataLoadBytesMarkerPrefix = "DATALOAD_BYTES="

	// dataLoadStatus* are the human-readable per-job terminal statuses written to
	// status.dataLoading.jobs[].lastStatus, mirroring the backupJobStatus values.
	dataLoadStatusSucceeded = "Succeeded"
	dataLoadStatusFailed    = "Failed"
	dataLoadStatusRunning   = "Running"
	dataLoadStatusPending   = "Pending"

	// pxfExtensionSetupTimeout bounds the best-effort PXF extension DB call so a
	// connection problem never stalls reconcile (mirrors setupExporterRole).
	pxfExtensionSetupTimeout = 10 * time.Second

	// gpfdistOp* are the bounded `operation` label values for the
	// cloudberry_gpfdist_reconcile_total control-plane counter (B-1). Each is
	// recorded ONLY on a real K8s write outcome (create/update/delete).
	gpfdistOpPVC        = "pvc"
	gpfdistOpDeployment = "deployment"
	gpfdistOpService    = "service"
	gpfdistOpDelete     = "delete"

	// metricResultSuccess / metricResultError / metricResultSkipped are the
	// bounded `result` label values shared by the control-plane outcome
	// counters (B-1). "skipped" marks an honest no-work outcome (e.g. the DB
	// was unavailable so the scan never ran).
	metricResultSuccess = "success"
	metricResultError   = "error"
	metricResultSkipped = "skipped"

	// pxfExtensionResult* are the bounded `result` label values for the
	// cloudberry_pxf_extension_setup_total counter (B-2): the install succeeded
	// (>=1 extension), the DB was reachable but nothing was installed, or a hard
	// setup/connectivity error occurred.
	pxfExtensionResultInstalled = "installed"
	pxfExtensionResultAbsent    = "absent"
	pxfExtensionResultError     = "error"
)

// gpfdistReconcileResult maps a K8s write error to the bounded result label.
func gpfdistReconcileResult(err error) string {
	if err != nil {
		return metricResultError
	}
	return metricResultSuccess
}

// pxfDataLoaderRole resolves the dedicated data-loading DB role from the PXF
// spec (SE.6), defaulting to the cluster admin (gpadmin) for back-compat when
// PxfSpec.DataLoaderRole is empty. The default keeps the existing RP.11 grant
// behavior unchanged; a non-default role triggers EnsureDataLoaderRole.
func pxfDataLoaderRole(pxf *cbv1alpha1.PxfSpec) string {
	if pxf != nil && pxf.DataLoaderRole != "" {
		return pxf.DataLoaderRole
	}
	return util.DefaultAdminUser
}

// setupPXFExtensions best-effort installs the PXF client extensions when PXF is
// enabled, modeled on setupExporterRole. It is gated on the
// AnnotationPXFExtensionsReady idempotency flag (skipped once set) and is
// strictly NON-FATAL: a missing dbFactory, a connection failure or a
// SetupPXFExtensions error all only log a warning and return — the pxf agent is
// absent in cloudberry-official:2.1.0, so an unavailable extension is expected
// and must never fail the reconcile.
//
// The AnnotationPXFExtensionsReady flag is only set once at least one extension
// was actually CREATE EXTENSIONed (installed >= 1). When SetupPXFExtensions
// reports a reachable DB but ZERO extensions installed (DB in recovery, or pxf
// genuinely absent in this image), the annotation is intentionally LEFT UNSET so
// the install is retried on a subsequent reconcile once the extension becomes
// available. Retries are naturally bounded: the admin reconcile is periodic and
// each DB call is capped by pxfExtensionSetupTimeout, so this never hammers.
func (r *AdminReconciler) setupPXFExtensions(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	logger *slog.Logger,
) {
	// Wrap the extension-setup work in a dedicated span so the downstream
	// db.SetupPXFExtensions / db.EnsureDataLoaderRole spans nest under a
	// "setupPXFExtensions" parent rather than the reconcileDataLoading root,
	// keeping the extension-setup DB calls distinguishable in a trace. The
	// function is non-fatal and returns no error, so the span always ends nil.
	ctx, end := startControllerSpan(ctx, adminControllerName, "setupPXFExtensions")
	defer func() { end(nil) }()

	pxf := cluster.Spec.DataLoading.Pxf
	if pxf == nil || !pxf.Enabled {
		return
	}
	// Idempotent: once the annotation is set, skip the DB round-trip entirely.
	if cluster.Annotations[util.AnnotationPXFExtensionsReady] == "true" {
		return
	}
	if r.dbFactory == nil {
		logger.Debug("skipping PXF extension setup: no dbFactory configured")
		return
	}

	dbCtx, cancel := context.WithTimeout(ctx, pxfExtensionSetupTimeout)
	defer cancel()

	dbClient, err := r.dbFactory.NewClient(dbCtx, cluster)
	if err != nil {
		// A real setup attempt that failed at the connectivity boundary: count it
		// honestly as an error so a persistently-unreachable DB is visible.
		r.metrics.RecordPXFExtensionSetup(cluster.Name, cluster.Namespace, pxfExtensionResultError)
		logger.Warn("skipping PXF extension setup: DB not available (will retry)", "error", err)
		return
	}
	defer dbClient.Close()

	installed, setupErr := dbClient.SetupPXFExtensions(dbCtx)
	if setupErr != nil {
		// Non-fatal: only a hard connectivity error surfaces here; an absent pxf
		// extension returns (count, nil) from SetupPXFExtensions. Log and move on
		// WITHOUT setting the annotation, so it is retried on the next reconcile.
		r.metrics.RecordPXFExtensionSetup(cluster.Name, cluster.Namespace, pxfExtensionResultError)
		logger.Warn("PXF extension setup did not complete (non-fatal, will retry)", "error", setupErr)
		return
	}

	// SE.6: when a dedicated minimal-privilege data-loader role is configured
	// (non-empty and not the gpadmin fallback), ensure it exists and is GRANTed
	// ONLY the pxf protocol privileges. This is ADDITIVE: SetupPXFExtensions has
	// already applied the gpadmin RP.11 GRANT, so existing loads keep working;
	// the dedicated role merely adds a least-privilege alternative. The call is
	// best-effort/non-fatal (a no-op for the gpadmin default).
	if role := pxfDataLoaderRole(pxf); role != util.DefaultAdminUser {
		if roleErr := dbClient.EnsureDataLoaderRole(dbCtx, role); roleErr != nil {
			r.metrics.RecordDataLoaderRoleSetup(cluster.Name, cluster.Namespace, metricResultError)
			logger.Warn("ensuring PXF data-loader role did not complete (non-fatal)",
				"role", role, "error", roleErr)
		} else {
			r.metrics.RecordDataLoaderRoleSetup(cluster.Name, cluster.Namespace, metricResultSuccess)
		}
	}

	// A reachable DB with ZERO extensions installed means pxf is unavailable in
	// this image OR the DB is not yet ready (e.g. in recovery). Do NOT mark ready
	// — leave the annotation unset so the install retries when pxf appears.
	if installed < 1 {
		// Reachable DB, but nothing to install: the honest steady state when pxf
		// is image-blocked. Count as "absent" (distinct from a real error).
		r.metrics.RecordPXFExtensionSetup(cluster.Name, cluster.Namespace, pxfExtensionResultAbsent)
		logger.Info("PXF extension setup found no extensions to install (non-fatal, will retry)",
			"installed", installed)
		return
	}

	// >=1 extension was actually CREATE EXTENSIONed: the real success outcome.
	r.metrics.RecordPXFExtensionSetup(cluster.Name, cluster.Namespace, pxfExtensionResultInstalled)
	logger.Info("PXF client extensions ensured (best-effort)", "installed", installed)
	if setErr := setAnnotationPatch(ctx, r.client, cluster,
		util.AnnotationPXFExtensionsReady, "true"); setErr != nil {
		logger.Warn("failed to set pxf-extensions-ready annotation", "error", setErr)
	}
}

// reconcileDataLoadingJobs builds and launches the per-job data-loading
// Job/CronJob for every ENABLED job, idempotently creates them
// (deterministic-name get-or-create like createValidationJob), then lists the
// owned dataload Jobs, maps each to a terminal state, harvests the DATALOAD_ROWS
// marker and enriches status.dataLoading.jobs[] with the real execution status
// (lastRun/lastStatus/rowsLoaded/duration). Terminal-only metrics are recorded
// alongside. It is NON-FATAL on the happy path: a failure to create a Job is
// surfaced, but observed Job state never errors reconcile.
func (r *AdminReconciler) reconcileDataLoadingJobs(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileDataLoadingJobs")
	defer func() { end(err) }()

	jobs := cluster.Spec.DataLoading.Jobs
	if len(jobs) == 0 {
		return nil
	}

	if err := r.ensureDataLoadingWorkloads(ctx, cluster); err != nil {
		return err
	}

	// List the owned dataload Jobs and index them by the per-job NAME label so we
	// can correlate each spec job to its most recent Job.
	owned := &batchv1.JobList{}
	if err := r.client.List(ctx, owned,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentDataLoad,
		},
	); err != nil {
		return fmt.Errorf("listing data loading jobs: %w", err)
	}

	latestByJob := latestDataLoadJobByName(owned.Items)
	r.enrichDataLoadingStatus(ctx, cluster, latestByJob)
	return nil
}

// ensureDataLoadingWorkloads creates the Job (one-off) or CronJob (scheduled)
// for every ENABLED job, idempotently. Disabled jobs are skipped (no workload
// created). Existing workloads are left untouched (mirrors createValidationJob
// semantics: get-or-create, never recreate a running workload).
func (r *AdminReconciler) ensureDataLoadingWorkloads(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	for i := range cluster.Spec.DataLoading.Jobs {
		job := cluster.Spec.DataLoading.Jobs[i]
		if !job.Enabled {
			continue
		}
		// A gpload job is delivered via a control-file ConfigMap mounted into the
		// Job/CronJob pod; ensure it exists BEFORE the workload so the mount
		// resolves. PXF jobs carry no control file and skip this.
		if job.Type == dataLoadJobTypeGpload {
			if err := r.ensureGploadControlFileConfigMap(ctx, cluster, job); err != nil {
				return err
			}
		}
		if job.Schedule != "" {
			if err := r.ensureDataLoadCronJob(ctx, cluster, job); err != nil {
				return err
			}
			continue
		}
		if err := r.ensureDataLoadJob(ctx, cluster, job); err != nil {
			return err
		}
	}
	return nil
}

// ensureDataLoadJob creates a one-off data-loading Job for a job spec when one
// does not already exist (deterministic name <cluster>-dataload-<job>).
func (r *AdminReconciler) ensureDataLoadJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) error {
	name := util.DataLoadJobName(cluster.Name, job.Name)
	existing := &batchv1.Job{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return nil // already exists: idempotent no-op (do not recreate).
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting data loading job %s: %w", name, err)
	}

	desired := r.builder.BuildDataLoadJob(cluster, job)
	if desired == nil {
		// Mis-configured job: skip rather than error the whole reconcile.
		util.LoggerFromContext(ctx).Warn("skipping mis-configured data loading job",
			"job", job.Name, "type", job.Type)
		return nil
	}
	if createErr := r.client.Create(ctx, desired); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			return nil
		}
		return fmt.Errorf("creating data loading job %s: %w", name, createErr)
	}
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonDataLoadingReconciled,
		fmt.Sprintf("Data loading Job created for job %q", job.Name))
	return nil
}

// ensureDataLoadCronJob creates a scheduled data-loading CronJob for a job spec
// when one does not already exist (deterministic name <cluster>-dataload-<job>).
func (r *AdminReconciler) ensureDataLoadCronJob(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) error {
	name := util.DataLoadJobName(cluster.Name, job.Name)
	existing := &batchv1.CronJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return nil // already exists: idempotent no-op.
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting data loading cronjob %s: %w", name, err)
	}

	desired := r.builder.BuildDataLoadCronJob(cluster, job)
	if desired == nil {
		util.LoggerFromContext(ctx).Warn("skipping mis-configured data loading cronjob",
			"job", job.Name, "type", job.Type)
		return nil
	}
	if createErr := r.client.Create(ctx, desired); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			return nil
		}
		return fmt.Errorf("creating data loading cronjob %s: %w", name, createErr)
	}
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonDataLoadingReconciled,
		fmt.Sprintf("Data loading CronJob created for job %q", job.Name))
	return nil
}

// latestDataLoadJobByName indexes the observed dataload Jobs by their per-job
// NAME label, keeping the most recently created Job per name (so a re-run
// supersedes a prior run for status/metric purposes).
func latestDataLoadJobByName(jobs []batchv1.Job) map[string]*batchv1.Job {
	out := make(map[string]*batchv1.Job)
	for i := range jobs {
		jobName := jobs[i].Labels[util.LabelDataLoadJob]
		if jobName == "" {
			continue
		}
		if cur, ok := out[jobName]; !ok || jobs[i].CreationTimestamp.After(cur.CreationTimestamp.Time) {
			out[jobName] = &jobs[i]
		}
	}
	return out
}

// enrichDataLoadingStatus walks the spec jobs, correlates each to its latest
// observed Job (by the sanitized NAME label), and enriches the corresponding
// status.dataLoading.jobs[] entry with the real execution status. It also
// records the terminal-only metrics. The spec-derived {name,enabled} entries are
// preserved (and remain for jobs with no run yet); only the execution fields are
// added. The 5 metrics are emitted from Job status / the DATALOAD_ROWS marker
// only (never synthesized).
func (r *AdminReconciler) enrichDataLoadingStatus(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	latestByJob map[string]*batchv1.Job,
) {
	if cluster.Status.DataLoading == nil {
		return
	}
	statuses := cluster.Status.DataLoading.Jobs
	for i := range statuses {
		specJob, ok := dataLoadJobByName(cluster.Spec.DataLoading.Jobs, statuses[i].Name)
		if !ok {
			continue
		}
		job, ok := latestByJob[util.SanitizeK8sName(statuses[i].Name)]
		if !ok {
			continue
		}
		// Capture the previously recorded terminal status BEFORE it is overwritten
		// so the health-check failure Event can de-duplicate on a real transition
		// into Failed (mirrors emitBackupFailureEvent's prevStatus gate).
		prevStatus := statuses[i].LastStatus
		// Harvest the DATALOAD_ROWS and DATALOAD_BYTES markers ONCE; the status
		// enrichment + rows metric consume the rows value and the bytes metric the
		// bytes value (honest: each is consumed only when its marker was present,
		// never synthesized). Both are harvested only on a successful run.
		rows, haveRows := int64(0), false
		bytes, haveBytes := int64(0), false
		if dataLoadJobStatusCode(job) == backupJobStatusSucceeded {
			rows, haveRows = r.harvestDataLoadRows(ctx, cluster, job)
			bytes, haveBytes = r.harvestDataLoadBytes(ctx, cluster, job)
		}
		applyDataLoadJobStatus(&statuses[i], job, rows, haveRows)
		r.recordDataLoadJobMetrics(cluster, specJob, job, rows, haveRows, bytes, haveBytes)
		r.emitDataLoadHealthCheckFailureEvent(ctx, cluster, specJob, job, prevStatus)
	}
	cluster.Status.DataLoading.Jobs = statuses
}

// applyDataLoadJobStatus enriches a single status entry from its observed Job:
// LastRun (start time), LastStatus (terminal mapping), Duration (start→completion)
// and, on a successful run with a harvested marker, RowsLoaded. rows/haveRows are
// the pre-harvested DATALOAD_ROWS marker value.
func applyDataLoadJobStatus(
	status *cbv1alpha1.DataLoadingJobStatus,
	job *batchv1.Job,
	rows int64,
	haveRows bool,
) {
	status.LastStatus = dataLoadJobStatusString(job)
	if job.Status.StartTime != nil {
		status.LastRun = job.Status.StartTime.DeepCopy()
	}
	if d := dataLoadJobDuration(job); d != "" {
		status.Duration = d
	}
	if haveRows {
		r := rows
		status.RowsLoaded = &r
	}
}

// recordDataLoadJobMetrics emits the 5 honest data-loading metrics for an
// observed Job, gated to terminal states (mirrors recordBackupJobMetrics) so
// re-reconciles never double-count. source_type is derived from the spec job;
// the rows value is the pre-harvested DATALOAD_ROWS marker (never synthesized).
func (r *AdminReconciler) recordDataLoadJobMetrics(
	cluster *cbv1alpha1.CloudberryCluster,
	specJob cbv1alpha1.DataLoadingJob,
	job *batchv1.Job,
	rows int64,
	haveRows bool,
	bytes int64,
	haveBytes bool,
) {
	code := dataLoadJobStatusCode(job)
	// Status gauge is always set to the current code.
	r.metrics.SetDataLoadingJobStatus(cluster.Name, cluster.Namespace, specJob.Name, code)

	switch code {
	case backupJobStatusSucceeded:
		if job.Status.CompletionTime != nil {
			r.metrics.SetDataLoadingJobLastSuccess(cluster.Name, cluster.Namespace,
				specJob.Name, float64(job.Status.CompletionTime.Unix()))
		}
		if d := dataLoadJobDurationValue(job); d > 0 {
			r.metrics.ObserveDataLoadingJobDuration(cluster.Name, cluster.Namespace, specJob.Name, d)
		}
		if haveRows {
			r.metrics.RecordDataLoadingRows(cluster.Name, cluster.Namespace,
				specJob.Name, dataLoadSourceType(specJob), float64(rows))
		}
		// M.10: record bytes ONLY when the DATALOAD_BYTES marker was actually
		// harvested — never synthesized. source_type matches the rows metric.
		if haveBytes {
			r.metrics.RecordDataLoadingBytes(cluster.Name, cluster.Namespace,
				specJob.Name, dataLoadSourceType(specJob), float64(bytes))
		}
	case backupJobStatusFailed:
		r.metrics.RecordDataLoadingErrors(cluster.Name, cluster.Namespace, specJob.Name)
	default:
		// running/pending: only the status gauge is updated (no terminal metric).
	}
}

// dataLoadHealthCheckInitName mirrors builder.dataLoadHealthCheckInitName (the
// name of the pre-load health-check init container). It is duplicated here
// because the builder constant is unexported; both must stay in sync so the
// controller can attribute an init-container failure to the health checks and
// name it in the Event message.
const dataLoadHealthCheckInitName = "dataload-healthcheck"

// emitDataLoadHealthCheckFailureEvent emits a single de-duplicated Warning Event
// when a data-loading Job is observed Failed AND the failure is attributable to
// the pre-load health-check init container (honest attribution, §1.4):
//
//   - It fires ONLY on a real transition into Failed for this Job: when the
//     previously recorded status was already "Failed" the event is skipped, so
//     periodic reconciles of an unchanged failed Job never storm.
//   - Attribution is derived from the Job's pod(s): the event is emitted only
//     when the `dataload-healthcheck` INIT container terminated non-zero. A Job
//     that failed in the MAIN container (a real load error) does NOT get an HC
//     event — the existing generic failed-job handling (metrics/status) applies.
//   - When the init-container status is NOT derivable from observed state (no
//     pod, no init status yet), no HC-specific event is emitted (the failure is
//     still surfaced via status=Failed + errors_total; the specific failed check
//     is in the Job pod logs).
func (r *AdminReconciler) emitDataLoadHealthCheckFailureEvent(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	specJob cbv1alpha1.DataLoadingJob,
	job *batchv1.Job,
	prevStatus string,
) {
	if dataLoadJobStatusCode(job) != backupJobStatusFailed {
		return
	}
	// De-dup: skip when this Job was already recorded as Failed on a prior
	// reconcile (same idiom as emitBackupFailureEvent).
	if prevStatus == dataLoadStatusFailed {
		return
	}
	// Honest attribution: only emit when the health-check INIT container is
	// observed to have terminated non-zero. Otherwise stay silent (generic
	// status/metric handling already surfaced the failure).
	if !r.dataLoadHealthCheckInitFailed(ctx, cluster, job) {
		return
	}
	r.recorder.Event(cluster, corev1.EventTypeWarning,
		cbv1alpha1.EventReasonDataLoadingHealthCheckFailed,
		fmt.Sprintf("Data loading job %q failed pre-load health checks (init container %s)",
			specJob.Name, dataLoadHealthCheckInitName))
}

// dataLoadHealthCheckInitFailed reports whether the health-check INIT container
// of the latest data-loading Job terminated non-zero, inspected from the Job's
// pod(s) status.initContainerStatuses. It lists the Job's pods (by the
// well-known job-name label) and checks the `dataload-healthcheck` init
// container's terminated exit code. Non-fatal: a list error or absent status
// returns false (the controller then stays silent rather than mis-attributing).
func (r *AdminReconciler) dataLoadHealthCheckInitFailed(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) bool {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list data loading pods for health-check attribution",
			"job", job.Name, "error", err)
		return false
	}
	for i := range pods.Items {
		if podInitContainerFailed(&pods.Items[i], dataLoadHealthCheckInitName) {
			return true
		}
	}
	return false
}

// podInitContainerFailed reports whether the named init container in a pod
// terminated with a non-zero exit code.
func podInitContainerFailed(pod *corev1.Pod, name string) bool {
	for i := range pod.Status.InitContainerStatuses {
		st := pod.Status.InitContainerStatuses[i]
		if st.Name != name {
			continue
		}
		if term := st.State.Terminated; term != nil && term.ExitCode != 0 {
			return true
		}
	}
	return false
}

// dataLoadJobByName returns the spec job matching the given name.
func dataLoadJobByName(jobs []cbv1alpha1.DataLoadingJob, name string) (cbv1alpha1.DataLoadingJob, bool) {
	for i := range jobs {
		if jobs[i].Name == name {
			return jobs[i], true
		}
	}
	return cbv1alpha1.DataLoadingJob{}, false
}

// dataLoadJobStatusCode maps a data-loading Job's status to the spec-shared
// numeric code (0=pending, 1=running, 2=succeeded, 3=failed), reusing the backup
// mapping for consistency.
func dataLoadJobStatusCode(job *batchv1.Job) float64 {
	return backupJobStatusCode(job)
}

// dataLoadJobStatusString maps a Job's status to the human-readable per-job
// status written to status.dataLoading.jobs[].lastStatus.
func dataLoadJobStatusString(job *batchv1.Job) string {
	switch dataLoadJobStatusCode(job) {
	case backupJobStatusSucceeded:
		return dataLoadStatusSucceeded
	case backupJobStatusFailed:
		return dataLoadStatusFailed
	case backupJobStatusRunning:
		return dataLoadStatusRunning
	default:
		return dataLoadStatusPending
	}
}

// dataLoadJobDuration returns a human-readable duration for a Job, or "".
func dataLoadJobDuration(job *batchv1.Job) string {
	d := dataLoadJobDurationValue(job)
	if d <= 0 {
		return ""
	}
	return d.Round(time.Second).String()
}

// dataLoadJobDurationValue returns the elapsed time between Job start and
// completion (0 when either is unset).
func dataLoadJobDurationValue(job *batchv1.Job) time.Duration {
	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		return 0
	}
	return job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
}

// dataLoadJobTypePXF is the pxf job-type discriminator (mirrors the builder).
const dataLoadJobTypePXF = "pxf"

// dataLoadSourceType derives the rows_total source_type label from a spec job: a
// PXF job's profile head (e.g. "s3:parquet" -> "s3", "jdbc" -> "jdbc", "hive:orc"
// -> "hive"), else "gpfdist" for native gpload jobs. Honest: derived purely from
// the spec, one of s3/hdfs/hive/jdbc/hbase/gpfdist.
func dataLoadSourceType(job cbv1alpha1.DataLoadingJob) string {
	if job.Type == dataLoadJobTypePXF && job.PxfJob != nil {
		profile := job.PxfJob.Profile
		if idx := strings.IndexByte(profile, ':'); idx >= 0 {
			return strings.ToLower(profile[:idx])
		}
		if profile != "" {
			return strings.ToLower(profile)
		}
	}
	return "gpfdist"
}

// harvestDataLoadRows recovers the INSERT rowcount for a data-loading Job from
// its terminating pod, parsing the DATALOAD_ROWS=<n> marker from the terminated
// container's message (terminationMessagePath / FallbackToLogsOnError). It clones
// the backup retention-marker harvesting. Returns (count, true) when a count is
// recovered, or (0, false) otherwise. Non-fatal: list errors are logged and
// reported as not-found.
func (r *AdminReconciler) harvestDataLoadRows(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (int64, bool) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list data loading pods",
			"job", job.Name, "error", err)
		return 0, false
	}
	for i := range pods.Items {
		if n, ok := dataLoadRowsFromPod(&pods.Items[i]); ok {
			return n, true
		}
	}
	return 0, false
}

// dataLoadRowsFromPod extracts the rowcount from a data-loading pod's terminated
// container message, parsing the DATALOAD_ROWS=<n> marker.
func dataLoadRowsFromPod(pod *corev1.Pod) (int64, bool) {
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil || term.Message == "" {
			continue
		}
		if n, ok := parseDataLoadRowsMessage(term.Message); ok {
			return n, true
		}
	}
	return 0, false
}

// harvestDataLoadBytes recovers the loaded BYTE count for a data-loading Job from
// its terminating pod, parsing the DATALOAD_BYTES=<n> marker from the terminated
// container's message (M.10). It MIRRORS harvestDataLoadRows exactly. Returns
// (count, true) when a byte count is recovered, or (0, false) otherwise — an
// absent marker means the load could not measure real bytes, so the metric stays
// honestly absent. Non-fatal: list errors are logged and reported as not-found.
func (r *AdminReconciler) harvestDataLoadBytes(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (int64, bool) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list data loading pods for bytes harvest",
			"job", job.Name, "error", err)
		return 0, false
	}
	for i := range pods.Items {
		if n, ok := dataLoadBytesFromPod(&pods.Items[i]); ok {
			return n, true
		}
	}
	return 0, false
}

// dataLoadBytesFromPod extracts the byte count from a data-loading pod's
// terminated container message, parsing the DATALOAD_BYTES=<n> marker.
func dataLoadBytesFromPod(pod *corev1.Pod) (int64, bool) {
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil || term.Message == "" {
			continue
		}
		if n, ok := parseDataLoadBytesMessage(term.Message); ok {
			return n, true
		}
	}
	return 0, false
}

// ensureGploadControlFileConfigMap creates (or updates) the per-job gpload
// control-file ConfigMap ("<cluster>-gpload-<job>") that the gpload Job/CronJob
// mounts at /etc/gpload. It is idempotent: the ConfigMap is created when absent
// and its Data refreshed when present (so a spec change re-renders the control
// file). A mis-configured job (nil ConfigMap) is skipped, not errored.
func (r *AdminReconciler) ensureGploadControlFileConfigMap(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) error {
	desired := r.builder.BuildGploadControlFileConfigMap(cluster, job)
	if desired == nil {
		util.LoggerFromContext(ctx).Warn("skipping mis-configured gpload control-file configmap",
			"job", job.Name, "type", job.Type)
		return nil
	}
	name := desired.Name
	existing := &corev1.ConfigMap{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				return nil
			}
			return fmt.Errorf("creating gpload control-file configmap %s: %w", name, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("getting gpload control-file configmap %s: %w", name, err)
	default:
		existing.Data = desired.Data
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating gpload control-file configmap %s: %w", name, updateErr)
		}
		return nil
	}
}

// reconcileGpfdist creates/updates (or, when disabled, best-effort GCs) the
// gpfdist file-server PVC + Deployment + Service (GP.2-GP.5). It is gated on
// dataLoading.gpfdist.enabled: when enabled the three objects are ensured
// (get-or-create/update with ownerRefs); when not enabled they are deleted if
// present (the cluster ownerRef would also GC them, but an explicit best-effort
// delete reclaims them promptly when gpfdist flips off). It is NON-FATAL: object
// errors are returned to the caller, which logs and continues.
func (r *AdminReconciler) reconcileGpfdist(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	ctx, end := startControllerSpan(ctx, adminControllerName, "reconcileGpfdist")
	defer func() { end(err) }()

	dl := cluster.Spec.DataLoading
	enabled := dl != nil && dl.Gpfdist != nil && dl.Gpfdist.Enabled
	if !enabled {
		r.deleteGpfdistResources(ctx, cluster)
		return nil
	}
	if pvcErr := r.ensureGpfdistPVC(ctx, cluster); pvcErr != nil {
		return pvcErr
	}
	if depErr := r.ensureGpfdistDeployment(ctx, cluster); depErr != nil {
		return depErr
	}
	return r.ensureGpfdistService(ctx, cluster)
}

// ensureGpfdistPVC creates the gpfdist data PVC when absent. A PVC spec is
// immutable in most fields, so an existing PVC is left untouched (no update).
func (r *AdminReconciler) ensureGpfdistPVC(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	name := util.GpfdistDataPVCName(cluster.Name)
	existing := &corev1.PersistentVolumeClaim{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildGpfdistPVC(cluster)
		createErr := r.client.Create(ctx, desired)
		if apierrors.IsAlreadyExists(createErr) {
			createErr = nil // concurrent create won the race: a no-op success.
		}
		// Honest: record the real PVC create write outcome only.
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpPVC, gpfdistReconcileResult(createErr))
		if createErr != nil {
			return fmt.Errorf("creating gpfdist pvc %s: %w", name, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("getting gpfdist pvc %s: %w", name, err)
	default:
		return nil // PVC exists: spec is largely immutable, leave it (no write).
	}
}

// ensureGpfdistDeployment creates or updates the gpfdist Deployment.
func (r *AdminReconciler) ensureGpfdistDeployment(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	name := util.SanitizeK8sName(cluster.Name + "-gpfdist")
	existing := &appsv1.Deployment{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildGpfdistDeployment(cluster)
		createErr := r.client.Create(ctx, desired)
		if apierrors.IsAlreadyExists(createErr) {
			createErr = nil // concurrent create won the race: a no-op success.
		}
		// Honest: record the real Deployment create write outcome only.
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpDeployment, gpfdistReconcileResult(createErr))
		if createErr != nil {
			return fmt.Errorf("creating gpfdist deployment %s: %w", name, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("getting gpfdist deployment %s: %w", name, err)
	default:
		desired := r.builder.BuildGpfdistDeployment(cluster)
		existing.Spec = desired.Spec
		updateErr := r.client.Update(ctx, existing)
		// Honest: record the real Deployment update write outcome only.
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpDeployment, gpfdistReconcileResult(updateErr))
		if updateErr != nil {
			return fmt.Errorf("updating gpfdist deployment %s: %w", name, updateErr)
		}
		return nil
	}
}

// ensureGpfdistService creates or updates the gpfdist Service.
func (r *AdminReconciler) ensureGpfdistService(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	name := util.GpfdistServiceName2(cluster.Name)
	existing := &corev1.Service{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		desired := r.builder.BuildGpfdistService(cluster)
		createErr := r.client.Create(ctx, desired)
		if apierrors.IsAlreadyExists(createErr) {
			createErr = nil // concurrent create won the race: a no-op success.
		}
		// Honest: record the real Service create write outcome only.
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpService, gpfdistReconcileResult(createErr))
		if createErr != nil {
			return fmt.Errorf("creating gpfdist service %s: %w", name, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("getting gpfdist service %s: %w", name, err)
	default:
		desired := r.builder.BuildGpfdistService(cluster)
		// H-2 (defensive): copy ONLY the operator-owned Ports + Selector onto the
		// LIVE object, deliberately preserving the live, API-server-assigned
		// immutable fields (Spec.ClusterIP, Spec.Type). This is safe TODAY
		// because BuildGpfdistService uses ServiceTypeClusterIP, where the single
		// port carries no allocated NodePort. WARNING: if the gpfdist Service ever
		// becomes NodePort/LoadBalancer, this blind Ports overwrite would CLEAR
		// the allocated NodePort(s); that case must first merge desired ports onto
		// the live ports (preserving each live Port.NodePort by name/port) before
		// the Update.
		existing.Spec.Ports = desired.Spec.Ports
		existing.Spec.Selector = desired.Spec.Selector
		updateErr := r.client.Update(ctx, existing)
		// Honest: record the real Service update write outcome only.
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpService, gpfdistReconcileResult(updateErr))
		if updateErr != nil {
			return fmt.Errorf("updating gpfdist service %s: %w", name, updateErr)
		}
		return nil
	}
}

// deleteGpfdistResources best-effort deletes the gpfdist Deployment + Service +
// PVC when gpfdist is disabled. Deletions are best-effort: a NotFound is a no-op
// and any other error is logged (never fails reconcile), because the cluster
// ownerRef also GCs these objects on cluster deletion.
func (r *AdminReconciler) deleteGpfdistResources(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)
	ns := cluster.Namespace
	deps := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: util.SanitizeK8sName(cluster.Name + "-gpfdist"), Namespace: ns}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: util.GpfdistServiceName2(cluster.Name), Namespace: ns}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: util.GpfdistDataPVCName(cluster.Name), Namespace: ns}}
	for _, obj := range []client.Object{deps, svc, pvc} {
		err := r.client.Delete(ctx, obj)
		if apierrors.IsNotFound(err) {
			// Already absent: a no-op, not a real delete write — do not count it.
			continue
		}
		// Honest: record only a real delete write outcome (success or error).
		r.metrics.RecordGpfdistReconcile(cluster.Name, cluster.Namespace,
			gpfdistOpDelete, gpfdistReconcileResult(err))
		if err != nil {
			logger.Warn("best-effort gpfdist resource delete failed (non-fatal)",
				"name", obj.GetName(), "error", err)
		}
	}
}

// deleteDataLoadingWorkloads best-effort deletes ALL data-loading Jobs and
// CronJobs owned by the cluster (DIS.1 teardown), selected by the shared
// {LabelCluster=<name>, LabelComponent=ComponentDataLoad} label set the builder
// stamps on every dataload workload (dataLoadLabels). Foreground/Background
// propagation is forced to Background so the spawned pods are reaped. Deletions
// are best-effort: a NotFound is a no-op and any other error is logged (never
// fails reconcile, mirroring deleteGpfdistResources), because the cluster
// ownerRef also GCs these objects on cluster deletion. Label scoping ensures
// only THIS cluster's dataload objects are touched (foreign clusters/components
// are never matched).
func (r *AdminReconciler) deleteDataLoadingWorkloads(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)
	selector := client.MatchingLabels{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentDataLoad,
	}
	// Background propagation so the Job/CronJob pods are reaped on delete.
	delOpts := []client.DeleteOption{client.PropagationPolicy(metav1.DeletePropagationBackground)}

	jobs := &batchv1.JobList{}
	if err := r.client.List(ctx, jobs, client.InNamespace(cluster.Namespace), selector); err != nil {
		logger.Warn("listing data-loading jobs for teardown failed (non-fatal)", "error", err)
	} else {
		for i := range jobs.Items {
			if err := r.client.Delete(ctx, &jobs.Items[i], delOpts...); err != nil &&
				!apierrors.IsNotFound(err) {
				logger.Warn("best-effort data-loading Job delete failed (non-fatal)",
					"name", jobs.Items[i].Name, "error", err)
			}
		}
	}

	cronJobs := &batchv1.CronJobList{}
	if err := r.client.List(ctx, cronJobs, client.InNamespace(cluster.Namespace), selector); err != nil {
		logger.Warn("listing data-loading cronjobs for teardown failed (non-fatal)", "error", err)
	} else {
		for i := range cronJobs.Items {
			if err := r.client.Delete(ctx, &cronJobs.Items[i], delOpts...); err != nil &&
				!apierrors.IsNotFound(err) {
				logger.Warn("best-effort data-loading CronJob delete failed (non-fatal)",
					"name", cronJobs.Items[i].Name, "error", err)
			}
		}
	}
}

// deleteGploadControlFileConfigMaps best-effort deletes ALL gpload control-file
// ConfigMaps owned by the cluster (DIS.1 teardown). They carry the SAME
// dataload labels as the Jobs/CronJobs (gpload_builder.go BuildGploadControlFile
// ConfigMap stamps dataLoadLabels), so the same label-scoped list-and-delete
// reaches them even when spec.jobs is empty on the disabled path (where a
// name-derived delete would have nothing to iterate). Best-effort: NotFound is a
// no-op, other errors are logged (never fails reconcile). The PXF servers
// ConfigMap is NOT handled here: it is cluster-controller-owned and reaped by
// ensurePxfServersConfigMap when the sidecar is disabled.
func (r *AdminReconciler) deleteGploadControlFileConfigMaps(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	logger := util.LoggerFromContext(ctx)
	cms := &corev1.ConfigMapList{}
	if err := r.client.List(ctx, cms,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentDataLoad,
		},
	); err != nil {
		logger.Warn("listing gpload control-file configmaps for teardown failed (non-fatal)",
			"error", err)
		return
	}
	for i := range cms.Items {
		if err := r.client.Delete(ctx, &cms.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			logger.Warn("best-effort gpload control-file ConfigMap delete failed (non-fatal)",
				"name", cms.Items[i].Name, "error", err)
		}
	}
}

// parseDataLoadRowsMessage parses a data-loading container's termination message
// into a rowcount. It accepts the "DATALOAD_ROWS=<n>" marker (anywhere in the
// message, e.g. the FallbackToLogsOnError log tail). Returns (0, false) when no
// count is found.
func parseDataLoadRowsMessage(message string) (int64, bool) {
	return parseDataLoadCountMarker(message, dataLoadRowsMarkerPrefix)
}

// parseDataLoadBytesMessage parses a data-loading container's termination message
// into a BYTE count (M.10). It accepts the "DATALOAD_BYTES=<n>" marker (anywhere
// in the message). Returns (0, false) when no count is found, so an absent marker
// keeps the bytes metric honestly absent. Mirrors parseDataLoadRowsMessage.
func parseDataLoadBytesMessage(message string) (int64, bool) {
	return parseDataLoadCountMarker(message, dataLoadBytesMarkerPrefix)
}

// parseDataLoadCountMarker is the SHARED parser behind parseDataLoadRowsMessage
// and parseDataLoadBytesMessage: it finds the LAST occurrence of the given marker
// prefix and reads the immediately-following run of decimal digits into a
// non-negative int64. Returns (0, false) when the marker is absent or carries no
// valid count.
func parseDataLoadCountMarker(message, prefix string) (int64, bool) {
	idx := strings.LastIndex(message, prefix)
	if idx < 0 {
		return 0, false
	}
	rest := message[idx+len(prefix):]
	var digits strings.Builder
	for _, c := range rest {
		if c < '0' || c > '9' {
			break
		}
		digits.WriteRune(c)
	}
	if digits.Len() == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(digits.String(), 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
