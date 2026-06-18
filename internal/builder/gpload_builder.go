// Package builder: gpload_builder.go renders the gpload YAML CONTROL FILE
// (GL.1-GL.7) from a GploadJobSpec and builds the per-job ConfigMap that
// delivers it, plus the Job/CronJob that mounts the ConfigMap at /etc/gpload and
// runs `gpload -f /etc/gpload/<job>.yml` (J.25).
//
// The control file is DETERMINISTIC / byte-stable: blocks and keys are emitted
// in a FIXED order so the output is golden-testable. For a gpfdist source the
// SOURCE block emits LOCAL_HOSTNAME/PORT (host/port from InputSource or the
// cluster gpfdist Service) plus a FILE list of LOCAL paths (relative to the
// external gpfdist server's data dir, NO gpfdist:// prefix — gpload itself
// connects to that gpfdist over the network). A local source emits only the
// FILE list with each path verbatim.
//
// HONESTY NOTE: the gpload Job wraps the real `gpload -f` invocation and harvests
// gpload's own summary rowcount best-effort into the existing DATALOAD_ROWS
// marker. If the summary cannot be parsed the marker is OMITTED — no rowcount is
// synthesized (job_status/duration/errors remain real).
package builder

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// gploadControlFileVersion is the fixed gpload control-file VERSION (GL.1).
	gploadControlFileVersion = "1.0.0.1"

	// gploadDefaultDelimiter is the INPUT DELIMITER used when unset (GL.3 / J.31).
	gploadDefaultDelimiter = ","

	// gploadDefaultEncoding is the INPUT ENCODING used when unset (GL.3 / J.33).
	gploadDefaultEncoding = "UTF-8"

	// gploadFormatCSV / gploadFormatText are the gpload INPUT FORMAT values
	// (GL.3 / J.30). CSV is the default when Format is unset.
	gploadFormatCSV  = "csv"
	gploadFormatText = "text"

	// gploadModeInsert is the default OUTPUT MODE (GL.5 / J.35) when Mode unset.
	gploadModeInsert = "insert"
	gploadModeUpdate = "update"
	gploadModeMerge  = "merge"

	// gploadInputTypeLocal / gploadInputTypeGpfdist are the InputSource.Type
	// values (J.27). gpfdist is the default when unset.
	gploadInputTypeLocal   = "local"
	gploadInputTypeGpfdist = "gpfdist"

	// gploadControlMountPath is where the control-file ConfigMap is mounted in
	// the gpload pod; the control file lives at <mount>/<job>.yml.
	gploadControlMountPath = "/etc/gpload"

	// gploadControlVolumeName is the control-file ConfigMap volume name.
	gploadControlVolumeName = "gpload-control"

	// gploadContainerName is the gpload Job container name.
	gploadContainerName = "gpload"
)

// gploadControlFileKey returns the ConfigMap data key (and mounted file name)
// for a job's control file ("<job>.yml").
func gploadControlFileKey(jobName string) string {
	return util.SanitizeK8sName(jobName) + ".yml"
}

// BuildGploadControlFile renders the gpload YAML control file (GL.1-GL.7) for a
// gpload job. The output is byte-stable for a given input (blocks/keys emitted
// in a fixed order). It errors when the job is mis-configured (nil gploadJob or
// empty targetTable) so callers never emit an invalid control file.
func (b *DefaultBuilder) BuildGploadControlFile(
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) (string, error) {
	gp := job.GploadJob
	if gp == nil {
		return "", fmt.Errorf("building gpload control file for job %q: gploadJob is nil", job.Name)
	}
	if gp.TargetTable == "" {
		return "", fmt.Errorf("building gpload control file for job %q: targetTable is required", job.Name)
	}

	var s strings.Builder
	writeGploadHeader(&s, cluster)
	s.WriteString("GPLOAD:\n")
	writeGploadInput(&s, cluster, gp)
	writeGploadOutput(&s, gp)
	writeGploadPreload(&s, gp)
	writeGploadSQL(&s, gp)
	return s.String(), nil
}

// writeGploadHeader emits the GL.1 header block: VERSION/DATABASE/USER/HOST/PORT.
func writeGploadHeader(s *strings.Builder, cluster *cbv1alpha1.CloudberryCluster) {
	fmt.Fprintf(s, "VERSION: %s\n", gploadControlFileVersion)
	fmt.Fprintf(s, "DATABASE: %s\n", defaultCoordinatorDatabase)
	fmt.Fprintf(s, "USER: %s\n", util.DefaultAdminUser)
	fmt.Fprintf(s, "HOST: %s\n", util.CoordinatorServiceName(cluster.Name))
	fmt.Fprintf(s, "PORT: %d\n", resolvePort(cluster))
}

// writeGploadInput emits the GL.2/GL.3/GL.4 INPUT block: SOURCE (LOCAL_HOSTNAME/
// PORT for a gpfdist source plus the local FILE list), FORMAT, DELIMITER,
// optional HEADER, ENCODING and the optional error-handling keys (ERROR_LIMIT /
// LOG_ERRORS).
func writeGploadInput(
	s *strings.Builder,
	cluster *cbv1alpha1.CloudberryCluster,
	gp *cbv1alpha1.GploadJobSpec,
) {
	s.WriteString("  INPUT:\n")
	s.WriteString("    - SOURCE:\n")
	if gploadInputType(gp.InputSource) == gploadInputTypeGpfdist {
		fmt.Fprintf(s, "        LOCAL_HOSTNAME:\n          - %s\n",
			gploadGpfdistHost(cluster, gp.InputSource))
		fmt.Fprintf(s, "        PORT: %d\n", gploadGpfdistPort(cluster, gp.InputSource))
	}
	s.WriteString("        FILE:\n")
	for _, entry := range gploadFileEntries(gp) {
		fmt.Fprintf(s, "          - %s\n", entry)
	}
	fmt.Fprintf(s, "    - FORMAT: %s\n", gploadFormat(gp.Format))
	fmt.Fprintf(s, "    - DELIMITER: '%s'\n", gploadDelimiter(gp.Delimiter))
	if gp.Header != nil && *gp.Header {
		s.WriteString("    - HEADER: true\n")
	}
	fmt.Fprintf(s, "    - ENCODING: %s\n", gploadEncoding(gp.Encoding))
	if eh := gp.ErrorHandling; eh != nil {
		if eh.SegmentRejectLimit > 0 {
			fmt.Fprintf(s, "    - ERROR_LIMIT: %d\n", eh.SegmentRejectLimit)
		}
		if eh.LogErrors != nil && *eh.LogErrors {
			s.WriteString("    - LOG_ERRORS: true\n")
		}
	}
}

// writeGploadOutput emits the GL.5 OUTPUT block: TABLE, MODE and (for
// update/merge) the optional MATCH_COLUMNS / UPDATE_COLUMNS lists.
func writeGploadOutput(s *strings.Builder, gp *cbv1alpha1.GploadJobSpec) {
	s.WriteString("  OUTPUT:\n")
	fmt.Fprintf(s, "    - TABLE: %s\n", gp.TargetTable)
	mode := gploadMode(gp.Mode)
	fmt.Fprintf(s, "    - MODE: %s\n", strings.ToUpper(mode))
	if mode == gploadModeUpdate || mode == gploadModeMerge {
		if len(gp.MatchColumns) > 0 {
			fmt.Fprintf(s, "    - MATCH_COLUMNS: [ %s ]\n", strings.Join(gp.MatchColumns, ", "))
		}
		if len(gp.UpdateColumns) > 0 {
			fmt.Fprintf(s, "    - UPDATE_COLUMNS: [ %s ]\n", strings.Join(gp.UpdateColumns, ", "))
		}
	}
}

// writeGploadPreload emits the GL.6 PRELOAD block when truncate is requested.
func writeGploadPreload(s *strings.Builder, gp *cbv1alpha1.GploadJobSpec) {
	if gp.Preload == nil || gp.Preload.Truncate == nil || !*gp.Preload.Truncate {
		return
	}
	s.WriteString("  PRELOAD:\n")
	s.WriteString("    - TRUNCATE: true\n")
}

// writeGploadSQL emits the GL.7 SQL.AFTER block, one AFTER entry per PostActions
// element.
func writeGploadSQL(s *strings.Builder, gp *cbv1alpha1.GploadJobSpec) {
	if len(gp.PostActions) == 0 {
		return
	}
	s.WriteString("  SQL:\n")
	for _, action := range gp.PostActions {
		fmt.Fprintf(s, "    - AFTER: %q\n", action)
	}
}

// gploadFileEntries composes the SOURCE.FILE list (GL.2 / J.26-J.29) of LOCAL
// file paths. For a gpfdist source each glob is a path relative to the external
// gpfdist server's data dir (ensureLeadingSlash, NO gpfdist:// prefix — gpload
// reaches that gpfdist via the SOURCE LOCAL_HOSTNAME/PORT lines). For a local
// source each path is used verbatim. Empty/whitespace paths are skipped.
func gploadFileEntries(gp *cbv1alpha1.GploadJobSpec) []string {
	local := gploadInputType(gp.InputSource) == gploadInputTypeLocal
	out := make([]string, 0, len(gp.FilePaths))
	for _, p := range gp.FilePaths {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		if local {
			out = append(out, p)
			continue
		}
		out = append(out, ensureLeadingSlash(p))
	}
	return out
}

// gploadInputType resolves the InputSource.Type, defaulting to "gpfdist".
func gploadInputType(src *cbv1alpha1.GploadInputSourceSpec) string {
	if src != nil && strings.EqualFold(src.Type, gploadInputTypeLocal) {
		return gploadInputTypeLocal
	}
	return gploadInputTypeGpfdist
}

// gploadGpfdistHost resolves the gpfdist host for a gpfdist source: the explicit
// InputSource.Host, else the cluster gpfdist Service "<cluster>-gpfdist-svc".
func gploadGpfdistHost(
	cluster *cbv1alpha1.CloudberryCluster,
	src *cbv1alpha1.GploadInputSourceSpec,
) string {
	if src != nil && strings.TrimSpace(src.Host) != "" {
		return strings.TrimSpace(src.Host)
	}
	return util.GpfdistServiceName2(cluster.Name)
}

// gploadGpfdistPort resolves the gpfdist port for a gpfdist source: the explicit
// InputSource.Port, else the gpfdist spec port (or 8080 via gpfdistPort).
func gploadGpfdistPort(
	cluster *cbv1alpha1.CloudberryCluster,
	src *cbv1alpha1.GploadInputSourceSpec,
) int32 {
	if src != nil && src.Port > 0 {
		return src.Port
	}
	return gpfdistPort(cluster)
}

// gploadFormat resolves the INPUT FORMAT, defaulting to csv.
func gploadFormat(format string) string {
	if strings.EqualFold(strings.TrimSpace(format), gploadFormatText) {
		return gploadFormatText
	}
	return gploadFormatCSV
}

// gploadDelimiter resolves the INPUT DELIMITER, defaulting to ",".
func gploadDelimiter(delimiter string) string {
	if delimiter == "" {
		return gploadDefaultDelimiter
	}
	return delimiter
}

// gploadEncoding resolves the INPUT ENCODING, defaulting to UTF-8.
func gploadEncoding(encoding string) string {
	if strings.TrimSpace(encoding) == "" {
		return gploadDefaultEncoding
	}
	return encoding
}

// gploadMode resolves the OUTPUT MODE (lower-cased), defaulting to insert.
func gploadMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case gploadModeUpdate:
		return gploadModeUpdate
	case gploadModeMerge:
		return gploadModeMerge
	default:
		return gploadModeInsert
	}
}

// BuildGploadControlFileConfigMap builds the per-job control-file ConfigMap
// ("<cluster>-gpload-<job>") whose "<job>.yml" data key carries the rendered
// gpload control file. Returns nil when the control file cannot be rendered (a
// nil ConfigMap is safer than one with no usable control file).
func (b *DefaultBuilder) BuildGploadControlFileConfigMap(
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) *corev1.ConfigMap {
	controlFile, err := b.BuildGploadControlFile(cluster, job)
	if err != nil {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.GploadControlFileConfigMapName(cluster.Name, job.Name),
			Namespace:       cluster.Namespace,
			Labels:          dataLoadLabels(cluster.Name, job.Name),
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Data: map[string]string{
			gploadControlFileKey(job.Name): controlFile,
		},
	}
}

// BuildGploadJob builds a ONE-OFF gpload Job (no schedule) that mounts the
// control-file ConfigMap at /etc/gpload (read-only) and runs `gpload -f
// /etc/gpload/<job>.yml`, harvesting gpload's summary rowcount best-effort into
// the DATALOAD_ROWS marker. Returns nil when the control file cannot be rendered.
func (b *DefaultBuilder) BuildGploadJob(
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) *batchv1.Job {
	podSpec := b.buildGploadPodSpec(cluster, job)
	if podSpec == nil {
		return nil
	}
	labels := dataLoadLabels(cluster.Name, job.Name)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.DataLoadJobName(cluster.Name, job.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: b.buildDataLoadJobSpec(cluster, labels, podSpec, job),
	}
}

// BuildGploadCronJob builds a SCHEDULED gpload CronJob (J.25) when the job has a
// Schedule, mirroring the backup CronJob (ForbidConcurrent + history limits).
// Returns nil when the job has no Schedule or the control file cannot be rendered.
func (b *DefaultBuilder) BuildGploadCronJob(
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) *batchv1.CronJob {
	if job.Schedule == "" {
		return nil
	}
	podSpec := b.buildGploadPodSpec(cluster, job)
	if podSpec == nil {
		return nil
	}
	labels := dataLoadLabels(cluster.Name, job.Name)
	historyLimit := int32(3)
	concurrency := batchv1.ForbidConcurrent

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.DataLoadJobName(cluster.Name, job.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   job.Schedule,
			ConcurrencyPolicy:          concurrency,
			SuccessfulJobsHistoryLimit: &historyLimit,
			FailedJobsHistoryLimit:     &historyLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       b.buildDataLoadJobSpec(cluster, labels, podSpec, job),
			},
		},
	}
}

// buildGploadPodSpec builds the gpload Job pod spec: a /bin/bash -c container on
// the cluster (data-loader) image running the gpload wrapper script, with the
// PG* env, the control-file ConfigMap mounted read-only at /etc/gpload,
// RestartPolicy Never and the DataLoadingJobTemplate overrides. Returns nil when
// the control file cannot be rendered (so the job is skipped, not broken).
func (b *DefaultBuilder) buildGploadPodSpec(
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) *corev1.PodSpec {
	if _, err := b.BuildGploadControlFile(cluster, job); err != nil {
		return nil
	}
	script := buildGploadScript(job)

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      gploadControlVolumeName,
			MountPath: gploadControlMountPath,
			ReadOnly:  true,
		},
	}
	container := corev1.Container{
		Name:                     gploadContainerName,
		Image:                    dataLoaderImage(cluster),
		Command:                  []string{shellCommand, shellFlag},
		Args:                     []string{script},
		Env:                      buildDataLoadEnv(cluster),
		VolumeMounts:             volumeMounts,
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}

	volumes := []corev1.Volume{
		{
			Name: gploadControlVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.GploadControlFileConfigMapName(cluster.Name, job.Name),
					},
				},
			},
		},
	}

	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
	}

	// Pre-load health checks (Scenario 104): when enabled, prepend the
	// dataload-healthcheck init container and add the shared scratch volume +
	// its mount on the gpload container (HC.5), so HC.2 (target table), HC.4
	// (gpfdist reachability — the gpload-specific check) and HC.5 (disk space)
	// run before the gpload Job. Gated identically to the PXF/native path.
	if healthChecksEnabled(cluster) {
		container.VolumeMounts = append(container.VolumeMounts, dataLoadScratchMount())
		volumes = append(volumes, dataLoadScratchVolume(cluster))
		podSpec.InitContainers = []corev1.Container{
			buildDataLoadHealthCheckInitContainer(cluster, job),
		}
	}

	applyDataLoadTemplateContainer(cluster, &container)
	podSpec.Containers = []corev1.Container{container}
	podSpec.Volumes = volumes
	applyDataLoadTemplatePod(cluster, &podSpec)
	return &podSpec
}

// buildGploadScript renders the bash wrapper that runs `gpload -f
// /etc/gpload/<job>.yml`, tees the output, and best-effort harvests gpload's
// summary rowcount into the DATALOAD_ROWS marker. gpload prints a summary line
// like "<n> rows were inserted/updated"; the awk extracts the leading integer.
// When no count can be parsed the marker is OMITTED (no synthesized rowcount) so
// only honest job_status/duration/errors are reported.
//
// M.10: when the gpload source is a LOCAL file source the staged input files are
// present in the pod, so their REAL size is measured via `wc -c` and emitted as
// the DATALOAD_BYTES marker. For a gpfdist (remote) source the files are NOT
// local to the pod, so no real byte count is available and the bytes marker is
// OMITTED — the data_loading_bytes_total metric stays honestly absent (never
// synthesized). Both markers are written to /dev/termination-log together so the
// controller can harvest each independently.
func buildGploadScript(job cbv1alpha1.DataLoadingJob) string {
	controlFile := gploadControlMountPath + "/" + gploadControlFileKey(job.Name)

	var s strings.Builder
	s.WriteString("set -euo pipefail\n")
	s.WriteString(gpEnvPreamble)
	// Run gpload, capturing combined output so the summary line can be parsed
	// for the rowcount AFTER the load completes.
	out := "/tmp/gpload.out"
	fmt.Fprintf(&s, "gpload -f %s 2>&1 | tee %s\n", controlFile, out)
	// Best-effort rowcount: match gpload's "<n> rows were inserted/updated"
	// summary line and capture the leading integer (empty when not parsed).
	fmt.Fprintf(&s,
		"rows=$(grep -Eio '[0-9]+ rows were (inserted|updated)' %s | "+
			"grep -Eo '^[0-9]+' | tail -n1 || true)\n", out)
	// M.10: measure the REAL staged-input byte count via `wc -c` over the LOCAL
	// source files (only emitted for a local source; empty otherwise).
	writeGploadBytesMeasurement(&s, job)
	// Emit each marker to stdout and (together) to /dev/termination-log, but ONLY
	// when its value was actually measured — an unmeasured marker is omitted so
	// the corresponding metric stays honestly absent.
	s.WriteString("> /dev/termination-log 2>/dev/null || true\n")
	s.WriteString("if [ -n \"${rows:-}\" ]; then\n")
	fmt.Fprintf(&s, "  echo \"%s${rows}\"\n", dataLoadRowsMarker)
	fmt.Fprintf(&s,
		"  printf '%%s%%s\\n' '%s' \"${rows}\" >> /dev/termination-log 2>/dev/null || true\n",
		dataLoadRowsMarker)
	s.WriteString("fi\n")
	s.WriteString("if [ -n \"${bytes:-}\" ]; then\n")
	fmt.Fprintf(&s, "  echo \"%s${bytes}\"\n", dataLoadBytesMarker)
	fmt.Fprintf(&s,
		"  printf '%%s%%s\\n' '%s' \"${bytes}\" >> /dev/termination-log 2>/dev/null || true\n",
		dataLoadBytesMarker)
	s.WriteString("fi\n")
	return s.String()
}

// writeGploadBytesMeasurement emits the shell that measures the REAL total size
// (bytes) of the LOCAL staged input files via `wc -c`, assigning it to `bytes`
// for the M.10 DATALOAD_BYTES marker. It is emitted ONLY for a LOCAL gpload
// source (the files are present in the pod); for a gpfdist (remote) source no
// honest byte count is available, so `bytes` is left UNSET and the marker is
// omitted downstream — the metric stays honestly absent. The file list is the
// SAME verbatim local paths the control file's SOURCE.FILE block carries.
func writeGploadBytesMeasurement(s *strings.Builder, job cbv1alpha1.DataLoadingJob) {
	gp := job.GploadJob
	if gp == nil || gploadInputType(gp.InputSource) != gploadInputTypeLocal {
		return
	}
	files := gploadFileEntries(gp)
	if len(files) == 0 {
		return
	}
	// Sum the byte size of every existing local file. `wc -c` over the file set
	// yields a trailing "total" line for multiple files; tail -n1 + awk extracts
	// the final byte count. Missing files are skipped (best-effort), and any
	// failure leaves `bytes` unset so the marker is omitted (honest absence).
	s.WriteString("bytes=$(wc -c")
	for _, f := range files {
		fmt.Fprintf(s, " %s", shellQuote(f))
	}
	s.WriteString(" 2>/dev/null | tail -n1 | awk '{print $1}' || true)\n")
	// Guard: only keep a clean integer (drop empty/non-numeric so a partial read
	// never produces a fabricated marker).
	s.WriteString("case \"${bytes:-}\" in ''|*[!0-9]*) bytes=\"\" ;; esac\n")
}
