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

// Real gpbackup-timestamp capture (the restore-by-timestamp correctness fix).
//
// For a FULL/incremental backup, gpbackup GENERATES ITS OWN 14-digit timestamp
// at runtime and offers no flag to pin it (only --from-timestamp for
// incrementals). The operator, however, pre-generates its own timestamp to NAME
// the backup Job, so the real S3 objects land under gpbackup's timestamp while
// the operator would otherwise record a DIFFERENT one (the Job-name /
// CompletionTime-derived value). A restore using the operator-recorded
// timestamp then passes `gprestore --timestamp <recorded>` and FAILS with a 404
// (the S3 prefix uses gpbackup's real timestamp).
//
// The backup Job script captures gpbackup's emitted "Backup Timestamp = <ts>"
// from its stdout and writes "BACKUP_TIMESTAMP=<ts>" to the pod's termination
// log (see internal/builder writeGpbackupTimestampCapture /
// writeCoordinatorBackupTimestampCapture, the same grep capture the migration
// Job uses). This file turns that marker into the avsoft.io/backup-timestamp
// annotation, which the status path PREFERS over the Job-name/CompletionTime
// value so a later restore-by-timestamp resolves the correct S3 prefix. It
// mirrors the retention-cleanup / restore-partial annotation patterns and is
// fully backward compatible: when the marker/annotation is absent the operator
// falls back to the previous behavior.

// backupTimestampFromAnnotation returns the REAL gpbackup timestamp captured on
// the backup Job's avsoft.io/backup-timestamp annotation, or "" when absent or
// not a valid 14-digit gpbackup timestamp (defensive: a malformed annotation
// must never poison status.lastBackupTimestamp).
func backupTimestampFromAnnotation(job *batchv1.Job) string {
	ts := job.Annotations[util.AnnotationBackupTimestamp]
	if util.IsGpbackupTimestamp(ts) {
		return ts
	}
	return ""
}

// reconcileBackupTimestampAnnotations patches the avsoft.io/backup-timestamp
// annotation onto each Succeeded backup-operation Job that does not yet carry
// it, reading the REAL gpbackup timestamp from the backup pod's terminated
// container message ("BACKUP_TIMESTAMP=<ts>"). Run BEFORE status is derived so
// the same reconcile records the true timestamp. It mirrors the
// reconcileRestorePartialAnnotations de-dup pattern and is non-fatal:
// pod/permission/parse issues are logged and skipped so a single Job never
// blocks reconciliation. Backward compatible: a Job without the marker is left
// un-annotated and the status path falls back to the Job-name/CompletionTime
// value.
func (r *AdminReconciler) reconcileBackupTimestampAnnotations(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	jobs []batchv1.Job,
) {
	logger := util.LoggerFromContext(ctx)
	for i := range jobs {
		job := &jobs[i]
		if job.Labels[util.LabelBackupOperation] != util.BackupOperationBackup {
			continue
		}
		if job.Status.Succeeded == 0 {
			continue
		}
		if backupTimestampFromAnnotation(job) != "" {
			// Annotation already set with a valid timestamp: idempotent skip.
			continue
		}
		ts, ok := r.readBackupRealTimestamp(ctx, cluster, job)
		if !ok {
			// Marker not present yet (pod gone / older Job): skip without error
			// so a later reconcile can retry while the pod exists.
			continue
		}
		if err := r.patchBackupTimestampAnnotation(ctx, job, ts); err != nil {
			logger.Warn("failed to patch backup-timestamp annotation",
				"job", job.Name, "error", err)
		}
	}
}

// patchBackupTimestampAnnotation patches the backup Job with the
// avsoft.io/backup-timestamp annotation carrying the REAL gpbackup timestamp.
func (r *AdminReconciler) patchBackupTimestampAnnotation(
	ctx context.Context,
	job *batchv1.Job,
	timestamp string,
) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[util.AnnotationBackupTimestamp] = timestamp
	if err := r.client.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("patching backup job %s timestamp annotation: %w", job.Name, err)
	}
	return nil
}

// readBackupRealTimestamp recovers the REAL gpbackup timestamp from a backup
// Job's pods. It lists the Job's pods by the job-name label and parses the
// "BACKUP_TIMESTAMP=<ts>" marker from the terminated container's message
// (terminationMessagePath / FallbackToLogsOnError). Returns (ts, true) when a
// valid 14-digit timestamp is recovered, or ("", false) when the pod or marker
// is not available. Non-fatal: list errors are logged and reported as
// not-found.
func (r *AdminReconciler) readBackupRealTimestamp(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	job *batchv1.Job,
) (string, bool) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{batchJobNameLabel: job.Name},
	); err != nil {
		util.LoggerFromContext(ctx).Warn("failed to list backup pods",
			"job", job.Name, "error", err)
		return "", false
	}
	for i := range pods.Items {
		if ts, ok := backupRealTimestampFromPod(&pods.Items[i]); ok {
			return ts, true
		}
	}
	return "", false
}

// backupRealTimestampFromPod extracts the REAL gpbackup timestamp from a backup
// pod's terminated container messages, parsing the "BACKUP_TIMESTAMP=<ts>"
// marker. Returns ("", false) when no container terminated with a valid marker.
func backupRealTimestampFromPod(pod *corev1.Pod) (string, bool) {
	for i := range pod.Status.ContainerStatuses {
		term := pod.Status.ContainerStatuses[i].State.Terminated
		if term == nil || term.Message == "" {
			continue
		}
		if ts, ok := parseBackupTimestampMessage(term.Message); ok {
			return ts, true
		}
	}
	return "", false
}

// parseBackupTimestampMessage parses a backup container's termination message
// for the "BACKUP_TIMESTAMP=<ts>" marker (anywhere in the message, e.g. the
// FallbackToLogsOnError log tail). It extracts the 14-digit timestamp that
// follows the marker and validates it as a gpbackup timestamp. Returns
// ("", false) when no valid marker is found.
func parseBackupTimestampMessage(message string) (string, bool) {
	idx := strings.LastIndex(message, backupTimestampMarkerPrefix)
	if idx < 0 {
		return "", false
	}
	rest := message[idx+len(backupTimestampMarkerPrefix):]
	digits := strings.Builder{}
	for _, c := range rest {
		if c < '0' || c > '9' {
			break
		}
		digits.WriteRune(c)
	}
	ts := digits.String()
	if !util.IsGpbackupTimestamp(ts) {
		return "", false
	}
	return ts, true
}
