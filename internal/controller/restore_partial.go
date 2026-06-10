package controller

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// Partial-restore (statistics-only failure) handling.
//
// gprestore exits with code 2 when ONLY the statistics restore fails (known
// upstream gpbackup bug: invalid bigint in statistics.sql) while the data
// restore succeeded. The restore Job script downgrades that exit code to 0 and
// writes the "GPRESTORE_PARTIAL=stats" marker to the pod's termination log
// (see internal/builder gprestoreStatsExitGuard). This file turns the marker
// into an observable outcome: the Job is annotated once
// (avsoft.io/restore-partial), a RestorePartial Warning Event is emitted on
// the cluster, and the restore metric records the result label "partial"
// instead of "success".

const (
	// restorePartialMarkerPrefix is the termination-message marker prefix the
	// restore Job script writes when gprestore exited with the statistics-only
	// failure code 2 (e.g. "GPRESTORE_PARTIAL=stats").
	restorePartialMarkerPrefix = "GPRESTORE_PARTIAL="

	// restoreStatusPartial is the `status` label recorded on the restore
	// metric for a success-with-warning (statistics-only failure) restore.
	restoreStatusPartial = "partial"
)

// jobRestorePartial reports whether the restore Job carries the
// restore-partial annotation (statistics restore failed, data succeeded).
func jobRestorePartial(job *batchv1.Job) bool {
	return job.Annotations[util.AnnotationRestorePartial] != ""
}

// restoreMetricStatus maps a restore Job's derived status to the metric label:
// a Succeeded Job carrying the restore-partial annotation records "partial"
// (success-with-warning); everything else records the lowercased status.
func restoreMetricStatus(job *batchv1.Job, status string) string {
	if status == backupStatusSuccess && jobRestorePartial(job) {
		return restoreStatusPartial
	}
	return strings.ToLower(status)
}

// reconcileRestorePartialAnnotations patches the avsoft.io/restore-partial
// annotation onto each Succeeded restore Job that does not yet carry it,
// reading the partial marker from the restore pod's terminated container
// message ("GPRESTORE_PARTIAL=<detail>"). The RestorePartial Warning Event is
// emitted exactly once per Job: only on the reconcile that performs the patch
// (the already-annotated check makes subsequent reconciles skip), mirroring
// the retention-cleanup annotation de-dup pattern. Non-fatal: pod/permission
// issues are logged and skipped so a single Job never blocks reconciliation.
func (r *AdminReconciler) reconcileRestorePartialAnnotations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	jobs []batchv1.Job,
) {
	logger := util.LoggerFromContext(ctx)
	for i := range jobs {
		job := &jobs[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationRestore {
			continue
		}
		if job.Status.Succeeded == 0 {
			continue
		}
		if jobRestorePartial(job) {
			// Annotation already set: idempotent skip (event already emitted).
			continue
		}
		detail, ok := r.readRestorePartialDetail(ctx, cluster, job)
		if !ok {
			// Marker not present (clean restore) or pod gone: skip without
			// error so a later reconcile can retry while the pod exists.
			continue
		}
		if err := r.patchRestorePartialAnnotation(ctx, job, detail); err != nil {
			logger.Warn("failed to patch restore-partial annotation",
				"job", job.Name, "error", err)
			continue
		}
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonRestorePartial,
			fmt.Sprintf("Restore job %s succeeded for data but the statistics restore failed "+
				"(gprestore exit code 2, %s); treating as success-with-warning", job.Name, detail))
	}
}

// patchRestorePartialAnnotation patches the restore Job with the
// avsoft.io/restore-partial annotation carrying the marker detail (e.g. "stats").
func (r *AdminReconciler) patchRestorePartialAnnotation(
	ctx context.Context,
	job *batchv1.Job,
	detail string,
) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[util.AnnotationRestorePartial] = detail
	if err := r.client.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("patching restore job %s annotation: %w", job.Name, err)
	}
	return nil
}

// readRestorePartialDetail recovers the partial-restore marker detail from the
// restore Job's pods. It lists the Job's pods by the job-name label and parses
// the "GPRESTORE_PARTIAL=<detail>" marker from the terminated container's
// message. Returns (detail, true) when the marker is found, or ("", false)
// when the pods or marker are not available. Non-fatal: list errors are
// logged and reported as not-found.
func (r *AdminReconciler) readRestorePartialDetail(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (string, bool) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list restore pods",
			"job", job.Name, "error", err)
		return "", false
	}
	for i := range pods.Items {
		if detail, ok := restorePartialFromPod(&pods.Items[i]); ok {
			return detail, true
		}
	}
	return "", false
}

// restorePartialFromPod extracts the partial-restore detail from a restore
// pod's terminated container messages, parsing the "GPRESTORE_PARTIAL=<detail>"
// marker. Returns ("", false) when no container terminated with the marker.
func restorePartialFromPod(pod *corev1.Pod) (string, bool) {
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil || term.Message == "" {
			continue
		}
		if detail, ok := parseRestorePartialMessage(term.Message); ok {
			return detail, true
		}
	}
	return "", false
}

// parseRestorePartialMessage parses a restore container's termination message
// for the "GPRESTORE_PARTIAL=<detail>" marker (anywhere in the message, e.g.
// the FallbackToLogsOnError log tail). The detail is the remainder of the
// marker token (alphanumerics, '-' and '_'), defaulting to "stats" when the
// marker is present but bare. Returns ("", false) when no marker is found.
func parseRestorePartialMessage(message string) (string, bool) {
	idx := strings.LastIndex(message, restorePartialMarkerPrefix)
	if idx < 0 {
		return "", false
	}
	rest := message[idx+len(restorePartialMarkerPrefix):]
	detail := strings.Builder{}
	for _, c := range rest {
		if !isRestorePartialDetailRune(c) {
			break
		}
		detail.WriteRune(c)
	}
	if detail.Len() == 0 {
		return "stats", true
	}
	return detail.String(), true
}

// isRestorePartialDetailRune reports whether the rune may appear in the
// partial-restore marker detail token.
func isRestorePartialDetailRune(c rune) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-' || c == '_':
		return true
	default:
		return false
	}
}
