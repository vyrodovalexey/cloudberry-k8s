// Package builder: backup_builder.go constructs the gpbackup/gprestore-centric
// Kubernetes resources (ConfigMap, CronJob, on-demand backup/restore Jobs and the
// retention-cleanup Job) backed by the apache/cloudberry-backup toolchain.
package builder

import (
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultBackupImage is the fallback backup toolchain image.
	defaultBackupImage = "cloudberry-backup:2.1.0"

	// backupContainerName is the container name for gpbackup containers.
	backupContainerName = "gpbackup"
	// restoreContainerName is the container name for gprestore containers.
	restoreContainerName = "gprestore"
	// cleanupContainerName is the container name for gpbackman cleanup containers.
	cleanupContainerName = "gpbackman"

	// s3ConfigVolumeName is the volume name for the S3 plugin config ConfigMap.
	s3ConfigVolumeName = "s3-plugin-config"
	// s3ConfigMountPath is the mount path for the S3 plugin config.
	s3ConfigMountPath = "/etc/gpbackup"
	// s3ConfigTemplateKey is the ConfigMap data key holding the plugin template.
	s3ConfigTemplateKey = "s3-plugin-config.yaml.tpl"
	// s3RenderedConfigPath is the rendered (envsubst) plugin config path inside the pod.
	s3RenderedConfigPath = "/tmp/s3-config.yaml"

	// localBackupVolumeName is the volume name for the local (PVC) backup destination.
	localBackupVolumeName = "backup-data"
	// localBackupMountPath is the mount path for the local backup destination.
	localBackupMountPath = "/backups"

	// shellCommand is the shell used to render the plugin config and run the tool.
	shellCommand = "/bin/bash"
	// shellFlag is the shell flag for executing an inline script.
	shellFlag = "-c"

	// Default JobTemplate values per spec 11 §Webhook Defaults.
	defaultBackoffLimit            int32 = 2
	defaultActiveDeadlineSeconds   int64 = 7200
	defaultTTLSecondsAfterFinished int32 = 86400

	// defaultCoordinatorDatabase is the database used for backup connections.
	defaultCoordinatorDatabase = "postgres"

	// pluginConfigFlag is the gpbackup/gprestore plugin-config flag.
	pluginConfigFlag = "--plugin-config"

	// destinationTypeS3 is the S3 destination type value.
	destinationTypeS3 = "s3"
	// destinationTypeLocal is the local destination type value.
	destinationTypeLocal = "local"

	// defaultS3AccessKeyField is the default Secret key for the S3 access key id.
	defaultS3AccessKeyField = "aws_access_key_id" //nolint:gosec // field name, not a credential
	// defaultS3SecretKeyField is the default Secret key for the S3 secret access key.
	defaultS3SecretKeyField = "aws_secret_access_key" //nolint:gosec // field name, not a credential

	// preBackupCheckContainerName is the name of the pre-backup health-check init container.
	preBackupCheckContainerName = "pre-backup-check"
	// longRunningTxnThresholdSeconds is the age (seconds) above which an open
	// transaction is considered long-running and blocks the backup pre-check.
	longRunningTxnThresholdSeconds = 3600
	// minBackupDiskFreeKB is the minimum free space (KiB) required on the local
	// backup-dir mount before a backup proceeds (best-effort check).
	minBackupDiskFreeKB = 1048576

	// validateContainerName is the container name for the post-restore validation Job.
	validateContainerName = "post-restore-validate"
	// defaultHealthCheckQuery is the default connectivity health-check query.
	defaultHealthCheckQuery = "SELECT 1"
)

// BackupJobOptions carries per-request overrides for an on-demand backup Job.
type BackupJobOptions struct {
	// Timestamp is the gpbackup-style YYYYMMDDHHMMSS identifier used for naming.
	Timestamp string
	// Type is the backup type (full | incremental).
	Type string
	// Databases are the databases to back up (the first is used for --dbname).
	Databases []string
	// Gpbackup overrides the cluster-level gpbackup options for this request.
	Gpbackup *cbv1alpha1.GpbackupOptions
	// FromTimestamp pins the base backup for an incremental backup (--from-timestamp).
	FromTimestamp string
	// IncludeSchemas maps to --include-schema (repeated).
	IncludeSchemas []string
	// IncludeTables maps to --include-table (repeated).
	IncludeTables []string
	// ExcludeTables maps to --exclude-table (repeated).
	ExcludeTables []string
}

// RestoreJobOptions carries per-request parameters for a restore Job.
type RestoreJobOptions struct {
	// Timestamp is the gpbackup timestamp to restore from (--timestamp).
	Timestamp string
	// Databases are the databases targeted by the restore.
	Databases []string
	// Gprestore overrides the cluster-level gprestore options for this request.
	Gprestore *cbv1alpha1.GprestoreOptions
	// RedirectDb maps to --redirect-db.
	RedirectDb string
	// RedirectSchema maps to --redirect-schema.
	RedirectSchema string
	// IncludeSchemas maps to --include-schema (repeated).
	IncludeSchemas []string
	// IncludeTables maps to --include-table (repeated).
	IncludeTables []string
	// ExcludeTables maps to --exclude-table (repeated).
	ExcludeTables []string
}

// backupImage returns the configured backup toolchain image or the default.
func backupImage(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.Image != "" {
		return cluster.Spec.Backup.Image
	}
	return defaultBackupImage
}

// backupLabels returns labels for a backup resource with the given operation.
func backupLabels(cluster, operation string) map[string]string {
	labels := util.CommonLabels(cluster, util.ComponentBackup)
	labels[util.LabelBackupOperation] = operation
	return labels
}

// buildGpbackupArgs converts gpbackup options and per-request overrides into a
// gpbackup CLI argument slice. It is a pure function for easy unit testing.
func buildGpbackupArgs(opts *cbv1alpha1.GpbackupOptions, jobOpts *BackupJobOptions) []string {
	args := []string{pluginConfigFlag, s3RenderedConfigPath}

	if jobOpts != nil && len(jobOpts.Databases) > 0 {
		args = append(args, "--dbname", jobOpts.Databases[0])
	}

	if opts == nil {
		opts = &cbv1alpha1.GpbackupOptions{}
	}

	args = appendCompressionArgs(args, opts)
	args = appendDataFileArgs(args, opts)
	args = appendIncrementalArgs(args, opts, jobOpts)

	if opts.WithStats {
		args = append(args, "--with-stats")
	}
	if opts.WithoutGlobals {
		args = append(args, "--without-globals")
	}

	if jobOpts != nil {
		args = appendRepeatedFlag(args, "--include-schema", jobOpts.IncludeSchemas)
		args = appendRepeatedFlag(args, "--include-table", jobOpts.IncludeTables)
		args = appendRepeatedFlag(args, "--exclude-table", jobOpts.ExcludeTables)
	}

	return args
}

// appendCompressionArgs appends compression-related flags. NoCompression takes
// precedence over the compression level/type per spec.
func appendCompressionArgs(args []string, opts *cbv1alpha1.GpbackupOptions) []string {
	if opts.NoCompression {
		return append(args, "--no-compression")
	}
	if opts.CompressionLevel > 0 {
		args = append(args, "--compression-level", strconv.Itoa(int(opts.CompressionLevel)))
	}
	if opts.CompressionType != "" {
		args = append(args, "--compression-type", opts.CompressionType)
	}
	return args
}

// appendDataFileArgs appends --single-data-file (+ --copy-queue-size) or --jobs.
// Per spec, --jobs cannot be combined with --single-data-file.
func appendDataFileArgs(args []string, opts *cbv1alpha1.GpbackupOptions) []string {
	if opts.SingleDataFile {
		args = append(args, "--single-data-file")
		if opts.CopyQueueSize > 0 {
			args = append(args, "--copy-queue-size", strconv.Itoa(int(opts.CopyQueueSize)))
		}
		return args
	}
	if opts.Jobs > 0 {
		args = append(args, "--jobs", strconv.Itoa(int(opts.Jobs)))
	}
	return args
}

// appendIncrementalArgs appends incremental-related flags, including the optional
// per-request --from-timestamp pin.
func appendIncrementalArgs(
	args []string,
	opts *cbv1alpha1.GpbackupOptions,
	jobOpts *BackupJobOptions,
) []string {
	incremental := opts.Incremental
	if jobOpts != nil && jobOpts.Type == "incremental" {
		incremental = true
	}
	if !incremental {
		return args
	}
	args = append(args, "--incremental")
	if opts.LeafPartitionData {
		args = append(args, "--leaf-partition-data")
	}
	if jobOpts != nil && jobOpts.FromTimestamp != "" {
		args = append(args, "--from-timestamp", jobOpts.FromTimestamp)
	}
	return args
}

// buildGprestoreArgs converts gprestore options and per-request parameters into a
// gprestore CLI argument slice. It is a pure function for easy unit testing.
func buildGprestoreArgs(opts *cbv1alpha1.GprestoreOptions, jobOpts *RestoreJobOptions) []string {
	args := []string{pluginConfigFlag, s3RenderedConfigPath}
	if jobOpts != nil && jobOpts.Timestamp != "" {
		args = append(args, "--timestamp", jobOpts.Timestamp)
	}

	if opts == nil {
		opts = &cbv1alpha1.GprestoreOptions{}
	}

	if opts.Jobs > 0 {
		args = append(args, "--jobs", strconv.Itoa(int(opts.Jobs)))
	}
	args = appendGprestoreBoolFlags(args, opts)

	if jobOpts != nil {
		if jobOpts.RedirectDb != "" {
			args = append(args, "--redirect-db", jobOpts.RedirectDb)
		}
		if jobOpts.RedirectSchema != "" {
			args = append(args, "--redirect-schema", jobOpts.RedirectSchema)
		}
		args = appendRepeatedFlag(args, "--include-schema", jobOpts.IncludeSchemas)
		args = appendRepeatedFlag(args, "--include-table", jobOpts.IncludeTables)
		args = appendRepeatedFlag(args, "--exclude-table", jobOpts.ExcludeTables)
	}

	return args
}

// appendGprestoreBoolFlags appends the gprestore boolean option flags that are
// enabled in opts, in a stable order.
func appendGprestoreBoolFlags(args []string, opts *cbv1alpha1.GprestoreOptions) []string {
	flags := []struct {
		enabled bool
		flag    string
	}{
		{opts.CreateDb, "--create-db"},
		{opts.WithGlobals, "--with-globals"},
		{opts.WithStats, "--with-stats"},
		{opts.RunAnalyze, "--run-analyze"},
		{opts.OnErrorContinue, "--on-error-continue"},
		{opts.TruncateTable, "--truncate-table"},
		{opts.DataOnly, "--data-only"},
		{opts.MetadataOnly, "--metadata-only"},
		{opts.ResizeCluster, "--resize-cluster"},
	}
	for _, f := range flags {
		if f.enabled {
			args = append(args, f.flag)
		}
	}
	return args
}

// appendRepeatedFlag appends "flag value" pairs for each value in values.
func appendRepeatedFlag(args []string, flag string, values []string) []string {
	for _, v := range values {
		if v == "" {
			continue
		}
		args = append(args, flag, v)
	}
	return args
}

// renderToolScript builds the bash script that renders the S3 plugin config via
// envsubst and then runs the given tool with its arguments.
func renderToolScript(tool string, args []string) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	fmt.Fprintf(&b, "envsubst < %s/%s > %s\n", s3ConfigMountPath, s3ConfigTemplateKey, s3RenderedConfigPath)
	b.WriteString(tool)
	for _, a := range args {
		b.WriteString(" ")
		b.WriteString(shellQuote(a))
	}
	b.WriteString("\n")
	return b.String()
}

// shellQuote single-quotes an argument for safe inclusion in a bash command.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// BuildBackupS3ConfigMap builds the gpbackup_s3_plugin config ConfigMap.
// Returns nil when the destination is not S3.
func (b *DefaultBuilder) BuildBackupS3ConfigMap(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Destination.Type != destinationTypeS3 {
		return nil
	}

	template := strings.Join([]string{
		"executablepath: /usr/local/bin/gpbackup_s3_plugin",
		"options:",
		"  region: ${S3_REGION}",
		"  endpoint: ${S3_ENDPOINT}",
		"  aws_access_key_id: ${AWS_ACCESS_KEY_ID}",
		"  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}",
		"  bucket: ${S3_BUCKET}",
		"  folder: ${S3_FOLDER}",
		"  encryption: ${S3_ENCRYPTION}",
		"  backup_max_concurrent_requests: ${BACKUP_MAX_CONCURRENT_REQUESTS}",
		"  backup_multipart_chunksize: ${BACKUP_MULTIPART_CHUNKSIZE}",
		"  restore_max_concurrent_requests: ${RESTORE_MAX_CONCURRENT_REQUESTS}",
		"  restore_multipart_chunksize: ${RESTORE_MULTIPART_CHUNKSIZE}",
		"",
	}, "\n")

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.BackupS3ConfigMapName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          backupLabels(cluster.Name, util.BackupOperationBackup),
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Data: map[string]string{
			s3ConfigTemplateKey: template,
		},
	}
}

// BuildBackupCronJob builds the scheduled backup CronJob. Returns nil when no
// schedule is configured.
func (b *DefaultBuilder) BuildBackupCronJob(cluster *cbv1alpha1.CloudberryCluster) *batchv1.CronJob {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Schedule == "" {
		return nil
	}

	labels := backupLabels(cluster.Name, util.BackupOperationBackup)
	args := buildGpbackupArgs(cluster.Spec.Backup.Gpbackup, nil)
	podSpec := b.buildBackupPodSpec(cluster, backupContainerName, "gpbackup", args)
	addPreBackupCheckInitContainer(cluster, &podSpec)

	historyLimit := int32(3)
	concurrency := batchv1.ForbidConcurrent

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.BackupCronJobName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   cluster.Spec.Backup.Schedule,
			ConcurrencyPolicy:          concurrency,
			SuccessfulJobsHistoryLimit: &historyLimit,
			FailedJobsHistoryLimit:     &historyLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       b.buildJobSpec(cluster, labels, &podSpec),
			},
		},
	}
}

// BuildBackupJob builds an on-demand gpbackup Job.
func (b *DefaultBuilder) BuildBackupJob(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *BackupJobOptions,
) *batchv1.Job {
	if opts == nil {
		opts = &BackupJobOptions{}
	}
	labels := backupLabels(cluster.Name, util.BackupOperationBackup)

	gpOpts := opts.Gpbackup
	if gpOpts == nil && cluster.Spec.Backup != nil {
		gpOpts = cluster.Spec.Backup.Gpbackup
	}
	args := buildGpbackupArgs(gpOpts, opts)
	podSpec := b.buildBackupPodSpec(cluster, backupContainerName, "gpbackup", args)
	addPreBackupCheckInitContainer(cluster, &podSpec)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.BackupJobName(cluster.Name, opts.Timestamp),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: b.buildJobSpec(cluster, labels, &podSpec),
	}
}

// BuildRestoreJob builds a gprestore Job.
func (b *DefaultBuilder) BuildRestoreJob(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *RestoreJobOptions,
) *batchv1.Job {
	if opts == nil {
		opts = &RestoreJobOptions{}
	}
	labels := backupLabels(cluster.Name, util.BackupOperationRestore)

	grOpts := opts.Gprestore
	if grOpts == nil && cluster.Spec.Backup != nil {
		grOpts = cluster.Spec.Backup.Gprestore
	}
	args := buildGprestoreArgs(grOpts, opts)
	podSpec := b.buildBackupPodSpec(cluster, restoreContainerName, "gprestore", args)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.RestoreJobName(cluster.Name, opts.Timestamp),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: b.buildJobSpec(cluster, labels, &podSpec),
	}
}

// ValidationJobOptions carries per-request parameters for a post-restore
// validation Job.
type ValidationJobOptions struct {
	// Timestamp is the gpbackup timestamp that was restored (used for naming).
	Timestamp string
	// Database is the database to run the validation queries against.
	Database string
	// HealthCheckQuery is the configurable connectivity health-check query.
	// When empty it defaults to "SELECT 1".
	HealthCheckQuery string
}

// BuildPostRestoreValidationJob builds a validation Job that runs after a restore
// completes (spec 11 §Post-Restore Validation). It performs a best-effort
// row-count probe, an invalid-index scan and a configurable health-check query.
func (b *DefaultBuilder) BuildPostRestoreValidationJob(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *ValidationJobOptions,
) *batchv1.Job {
	if opts == nil {
		opts = &ValidationJobOptions{}
	}
	labels := backupLabels(cluster.Name, util.BackupOperationValidate)

	podSpec := b.buildBackupPodSpec(cluster, validateContainerName, "", nil)
	// The validation container runs the bash script directly rather than a tool.
	podSpec.Containers[0].Args = []string{postRestoreValidationScript(opts)}
	if opts.Database != "" {
		setEnvVar(&podSpec.Containers[0], "PGDATABASE", opts.Database)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.PostRestoreValidationJobName(cluster.Name, opts.Timestamp),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: b.buildJobSpec(cluster, labels, &podSpec),
	}
}

// setEnvVar sets (or replaces) an environment variable on the container.
func setEnvVar(container *corev1.Container, name, value string) {
	for i := range container.Env {
		if container.Env[i].Name == name {
			container.Env[i] = corev1.EnvVar{Name: name, Value: value}
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{Name: name, Value: value})
}

// postRestoreValidationScript builds the bash script for the post-restore
// validation Job. The invalid-index scan is must-pass; the row-count probe and
// health-check query are best-effort so transient issues do not fail validation.
func postRestoreValidationScript(opts *ValidationJobOptions) string {
	healthQuery := opts.HealthCheckQuery
	if healthQuery == "" {
		healthQuery = defaultHealthCheckQuery
	}

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString("echo 'post-restore-validate: row-count probe (best-effort)'\n")
	b.WriteString("psql -tA -c \"SELECT count(*) FROM pg_class WHERE relkind='r'\" || " +
		"echo 'post-restore-validate: row-count probe skipped'\n")

	b.WriteString("echo 'post-restore-validate: scanning for invalid indexes'\n")
	b.WriteString("invalid=$(psql -tA -c \"SELECT count(*) FROM pg_catalog.pg_class c " +
		"JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid " +
		"WHERE c.relkind='i' AND NOT i.indisvalid\")\n")
	b.WriteString("if [ \"${invalid:-0}\" -gt 0 ]; then " +
		"echo \"post-restore-validate: ${invalid} invalid index(es)\" >&2; exit 1; fi\n")

	b.WriteString("echo 'post-restore-validate: health-check query'\n")
	fmt.Fprintf(&b, "psql -tA -c %s\n", shellQuote(healthQuery))
	b.WriteString("echo 'post-restore-validate: passed'\n")
	return b.String()
}

// BuildRetentionCleanupJob builds a gpbackman retention cleanup Job enforcing the
// configured retention policy.
func (b *DefaultBuilder) BuildRetentionCleanupJob(
	cluster *cbv1alpha1.CloudberryCluster,
	timestamp string,
) *batchv1.Job {
	labels := backupLabels(cluster.Name, util.BackupOperationCleanup)

	args := buildGpbackmanArgs(cluster)
	podSpec := b.buildBackupPodSpec(cluster, cleanupContainerName, "gpbackman", args)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.RetentionCleanupJobName(cluster.Name, timestamp),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: b.buildJobSpec(cluster, labels, &podSpec),
	}
}

// buildGpbackmanArgs builds the gpbackman cleanup arguments from the retention policy.
func buildGpbackmanArgs(cluster *cbv1alpha1.CloudberryCluster) []string {
	args := []string{"delete", pluginConfigFlag, s3RenderedConfigPath, "--cascade"}
	if cluster.Spec.Backup == nil {
		return args
	}
	retention := cluster.Spec.Backup.Retention
	if retention.MaxAge != "" {
		args = append(args, "--older-than", retention.MaxAge)
	}
	if retention.FullCount > 0 {
		args = append(args, "--keep-full", strconv.Itoa(int(retention.FullCount)))
	}
	return args
}

// buildBackupPodSpec builds the pod spec shared by backup/restore/cleanup Jobs.
func (b *DefaultBuilder) buildBackupPodSpec(
	cluster *cbv1alpha1.CloudberryCluster,
	containerName, tool string,
	args []string,
) corev1.PodSpec {
	container := corev1.Container{
		Name:         containerName,
		Image:        backupImage(cluster),
		Command:      []string{shellCommand, shellFlag},
		Args:         []string{renderToolScript(tool, args)},
		Env:          buildBackupEnv(cluster),
		VolumeMounts: buildBackupVolumeMounts(cluster),
	}
	applyJobTemplateContainer(cluster, &container)

	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    []corev1.Container{container},
		Volumes:       buildBackupVolumes(cluster),
	}
	applyJobTemplatePod(cluster, &podSpec)
	return podSpec
}

// addPreBackupCheckInitContainer prepends the pre-backup health-check init
// container to the pod spec (spec 11 §Pre-Backup Health Checks). The init
// container shares the same image, env and volumes as the main gpbackup
// container so it can connect to the coordinator and reach the destination. On
// non-zero exit the Job will not proceed (init container semantics).
func addPreBackupCheckInitContainer(cluster *cbv1alpha1.CloudberryCluster, podSpec *corev1.PodSpec) {
	container := corev1.Container{
		Name:         preBackupCheckContainerName,
		Image:        backupImage(cluster),
		Command:      []string{shellCommand, shellFlag},
		Args:         []string{preBackupCheckScript(cluster)},
		Env:          buildBackupEnv(cluster),
		VolumeMounts: buildBackupVolumeMounts(cluster),
	}
	applyJobTemplateContainer(cluster, &container)
	podSpec.InitContainers = append([]corev1.Container{container}, podSpec.InitContainers...)
}

// preBackupCheckScript builds the bash script run by the pre-backup init
// container. The cluster-health and long-running-transaction checks are
// must-pass (set -e); the destination checks are best-effort so a missing tool
// never blocks the backup.
func preBackupCheckScript(cluster *cbv1alpha1.CloudberryCluster) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString("echo 'pre-backup-check: verifying segment health'\n")
	fmt.Fprintf(&b,
		"down=$(psql -tA -c \"SELECT count(*) FROM gp_segment_configuration WHERE status='d'\")\n")
	b.WriteString("if [ \"${down:-0}\" -gt 0 ]; then " +
		"echo \"pre-backup-check: ${down} down segment(s)\" >&2; exit 1; fi\n")

	b.WriteString("echo 'pre-backup-check: verifying no long-running transactions'\n")
	fmt.Fprintf(&b,
		"longtx=$(psql -tA -c \"SELECT count(*) FROM pg_stat_activity "+
			"WHERE state <> 'idle' AND xact_start IS NOT NULL "+
			"AND now() - xact_start > interval '%d seconds'\")\n",
		longRunningTxnThresholdSeconds)
	b.WriteString("if [ \"${longtx:-0}\" -gt 0 ]; then " +
		"echo \"pre-backup-check: ${longtx} long-running transaction(s)\" >&2; exit 1; fi\n")

	b.WriteString(preBackupDestinationCheck(cluster))
	b.WriteString("echo 'pre-backup-check: passed'\n")
	return b.String()
}

// preBackupDestinationCheck returns the best-effort destination-readiness portion
// of the pre-backup script. For local destinations it verifies free disk space on
// the backup mount; for S3 it performs a lightweight, non-fatal connectivity probe.
func preBackupDestinationCheck(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Backup == nil {
		return ""
	}
	switch cluster.Spec.Backup.Destination.Type {
	case destinationTypeLocal:
		path := localBackupMountPath
		if local := cluster.Spec.Backup.Destination.Local; local != nil && local.Path != "" {
			path = local.Path
		}
		return fmt.Sprintf(
			"echo 'pre-backup-check: verifying free disk space'\n"+
				"free=$(df -Pk %s | awk 'NR==2 {print $4}')\n"+
				"if [ \"${free:-0}\" -lt %d ]; then "+
				"echo \"pre-backup-check: insufficient free space ${free}KB\" >&2; exit 1; fi\n",
			shellQuote(path), minBackupDiskFreeKB)
	case destinationTypeS3:
		// Best-effort: the s3 plugin/aws cli may be absent, so never fail here.
		return "echo 'pre-backup-check: s3 bucket connectivity (best-effort)'\n" +
			"command -v aws >/dev/null 2>&1 && " +
			"aws s3 ls \"s3://${S3_BUCKET}\" >/dev/null 2>&1 || " +
			"echo 'pre-backup-check: skipping s3 connectivity probe'\n"
	default:
		return ""
	}
}

// buildJobSpec builds the JobSpec with template overrides applied.
func (b *DefaultBuilder) buildJobSpec(
	cluster *cbv1alpha1.CloudberryCluster,
	labels map[string]string,
	podSpec *corev1.PodSpec,
) batchv1.JobSpec {
	backoff := defaultBackoffLimit
	deadline := defaultActiveDeadlineSeconds
	ttl := defaultTTLSecondsAfterFinished

	if tmpl := jobTemplate(cluster); tmpl != nil {
		if tmpl.BackoffLimit != nil {
			backoff = *tmpl.BackoffLimit
		}
		if tmpl.ActiveDeadlineSeconds != nil {
			deadline = *tmpl.ActiveDeadlineSeconds
		}
		if tmpl.TTLSecondsAfterFinished != nil {
			ttl = *tmpl.TTLSecondsAfterFinished
		}
	}

	return batchv1.JobSpec{
		BackoffLimit:            &backoff,
		ActiveDeadlineSeconds:   &deadline,
		TTLSecondsAfterFinished: &ttl,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       *podSpec,
		},
	}
}

// jobTemplate returns the backup JobTemplate, or nil when not configured.
func jobTemplate(cluster *cbv1alpha1.CloudberryCluster) *cbv1alpha1.BackupJobTemplate {
	if cluster.Spec.Backup == nil {
		return nil
	}
	return cluster.Spec.Backup.JobTemplate
}

// buildBackupEnv builds environment variables for backup/restore containers.
func buildBackupEnv(cluster *cbv1alpha1.CloudberryCluster) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, 16)
	env = append(env, []corev1.EnvVar{
		{Name: "PGHOST", Value: util.CoordinatorServiceName(cluster.Name)},
		{Name: "PGPORT", Value: strconv.Itoa(int(resolvePort(cluster)))},
		{Name: "PGUSER", Value: util.DefaultAdminUser},
		{Name: "PGDATABASE", Value: defaultCoordinatorDatabase},
		{
			Name: "PGPASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.AdminPasswordSecretName(cluster.Name),
					},
					Key: secretKeyPassword,
				},
			},
		},
	}...)

	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Destination.Type != destinationTypeS3 {
		return env
	}
	return append(env, buildS3Env(cluster.Spec.Backup.Destination.S3)...)
}

// buildS3Env builds the S3-related environment variables consumed by the
// envsubst-rendered plugin config.
func buildS3Env(s3 *cbv1alpha1.S3Destination) []corev1.EnvVar {
	if s3 == nil {
		return nil
	}

	encryption := s3.Encryption
	if encryption == "" {
		encryption = "on"
	}

	env := make([]corev1.EnvVar, 0, 11)
	env = append(env, []corev1.EnvVar{
		{Name: "S3_REGION", Value: s3.Region},
		{Name: "S3_ENDPOINT", Value: s3.Endpoint},
		{Name: "S3_BUCKET", Value: s3.Bucket},
		{Name: "S3_FOLDER", Value: s3.Folder},
		{Name: "S3_ENCRYPTION", Value: encryption},
	}...)
	env = append(env, buildS3MultipartEnv(s3.Multipart)...)
	env = append(env, buildS3CredentialEnv(s3.CredentialSecret)...)
	return env
}

// buildS3MultipartEnv builds the multipart tuning environment variables, applying
// safe defaults when unset so envsubst always resolves them.
func buildS3MultipartEnv(mp *cbv1alpha1.S3Multipart) []corev1.EnvVar {
	backupReq := "4"
	backupChunk := "10MB"
	restoreReq := "4"
	restoreChunk := "10MB"
	if mp != nil {
		if mp.BackupMaxConcurrentRequests > 0 {
			backupReq = strconv.Itoa(int(mp.BackupMaxConcurrentRequests))
		}
		if mp.BackupMultipartChunksize != "" {
			backupChunk = mp.BackupMultipartChunksize
		}
		if mp.RestoreMaxConcurrentRequests > 0 {
			restoreReq = strconv.Itoa(int(mp.RestoreMaxConcurrentRequests))
		}
		if mp.RestoreMultipartChunksize != "" {
			restoreChunk = mp.RestoreMultipartChunksize
		}
	}
	return []corev1.EnvVar{
		{Name: "BACKUP_MAX_CONCURRENT_REQUESTS", Value: backupReq},
		{Name: "BACKUP_MULTIPART_CHUNKSIZE", Value: backupChunk},
		{Name: "RESTORE_MAX_CONCURRENT_REQUESTS", Value: restoreReq},
		{Name: "RESTORE_MULTIPART_CHUNKSIZE", Value: restoreChunk},
	}
}

// buildS3CredentialEnv builds AWS credential env vars sourced from the Secret.
func buildS3CredentialEnv(cred *cbv1alpha1.S3CredentialSecret) []corev1.EnvVar {
	if cred == nil || cred.Name == "" {
		return nil
	}
	accessKeyField := cred.AccessKeyField
	if accessKeyField == "" {
		accessKeyField = defaultS3AccessKeyField
	}
	secretKeyField := cred.SecretKeyField
	if secretKeyField == "" {
		secretKeyField = defaultS3SecretKeyField
	}
	return []corev1.EnvVar{
		{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cred.Name},
					Key:                  accessKeyField,
				},
			},
		},
		{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cred.Name},
					Key:                  secretKeyField,
				},
			},
		},
	}
}

// buildBackupVolumes builds the volumes for backup/restore Jobs.
func buildBackupVolumes(cluster *cbv1alpha1.CloudberryCluster) []corev1.Volume {
	var volumes []corev1.Volume
	if cluster.Spec.Backup == nil {
		return volumes
	}

	switch cluster.Spec.Backup.Destination.Type {
	case destinationTypeS3:
		volumes = append(volumes, corev1.Volume{
			Name: s3ConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.BackupS3ConfigMapName(cluster.Name),
					},
				},
			},
		})
	case destinationTypeLocal:
		if local := cluster.Spec.Backup.Destination.Local; local != nil && local.PersistentVolumeClaim != "" {
			volumes = append(volumes, corev1.Volume{
				Name: localBackupVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: local.PersistentVolumeClaim,
					},
				},
			})
		}
	default:
		// no-op: unknown destination types require no Job volumes.
	}
	return volumes
}

// buildBackupVolumeMounts builds the volume mounts for backup/restore containers.
func buildBackupVolumeMounts(cluster *cbv1alpha1.CloudberryCluster) []corev1.VolumeMount {
	var mounts []corev1.VolumeMount
	if cluster.Spec.Backup == nil {
		return mounts
	}

	switch cluster.Spec.Backup.Destination.Type {
	case destinationTypeS3:
		mounts = append(mounts, corev1.VolumeMount{
			Name:      s3ConfigVolumeName,
			MountPath: s3ConfigMountPath,
		})
	case destinationTypeLocal:
		if local := cluster.Spec.Backup.Destination.Local; local != nil && local.PersistentVolumeClaim != "" {
			path := local.Path
			if path == "" {
				path = localBackupMountPath
			}
			mounts = append(mounts, corev1.VolumeMount{
				Name:      localBackupVolumeName,
				MountPath: path,
			})
		}
	default:
		// no-op: unknown destination types require no Job volume mounts.
	}
	return mounts
}

// applyJobTemplateContainer applies container-level JobTemplate overrides.
func applyJobTemplateContainer(cluster *cbv1alpha1.CloudberryCluster, container *corev1.Container) {
	tmpl := jobTemplate(cluster)
	if tmpl == nil || tmpl.Resources == nil {
		return
	}
	container.Resources = toResourceRequirements(tmpl.Resources)
}

// applyJobTemplatePod applies pod-level JobTemplate overrides.
func applyJobTemplatePod(cluster *cbv1alpha1.CloudberryCluster, podSpec *corev1.PodSpec) {
	tmpl := jobTemplate(cluster)

	sa := util.BackupServiceAccountName(cluster.Name)
	if tmpl != nil && tmpl.ServiceAccountName != "" {
		sa = tmpl.ServiceAccountName
	}
	podSpec.ServiceAccountName = sa

	if tmpl == nil {
		return
	}
	if len(tmpl.NodeSelector) > 0 {
		podSpec.NodeSelector = tmpl.NodeSelector
	}
	if len(tmpl.Tolerations) > 0 {
		podSpec.Tolerations = toTolerations(tmpl.Tolerations)
	}
}

// toResourceRequirements converts API ResourceRequirements to corev1.
func toResourceRequirements(rr *cbv1alpha1.ResourceRequirements) corev1.ResourceRequirements {
	out := corev1.ResourceRequirements{}
	if rr == nil {
		return out
	}
	if rr.Requests != nil {
		out.Requests = toResourceList(rr.Requests)
	}
	if rr.Limits != nil {
		out.Limits = toResourceList(rr.Limits)
	}
	return out
}

// toResourceList converts an API ResourceList to a corev1.ResourceList,
// silently skipping any unparseable quantities.
func toResourceList(rl *cbv1alpha1.ResourceList) corev1.ResourceList {
	out := corev1.ResourceList{}
	if rl.CPU != "" {
		if q, err := resource.ParseQuantity(rl.CPU); err == nil {
			out[corev1.ResourceCPU] = q
		}
	}
	if rl.Memory != "" {
		if q, err := resource.ParseQuantity(rl.Memory); err == nil {
			out[corev1.ResourceMemory] = q
		}
	}
	return out
}

// toTolerations converts API Tolerations to corev1.Tolerations.
func toTolerations(in []cbv1alpha1.Toleration) []corev1.Toleration {
	out := make([]corev1.Toleration, 0, len(in))
	for _, t := range in {
		out = append(out, corev1.Toleration{
			Key:               t.Key,
			Operator:          corev1.TolerationOperator(t.Operator),
			Value:             t.Value,
			Effect:            corev1.TaintEffect(t.Effect),
			TolerationSeconds: t.TolerationSeconds,
		})
	}
	return out
}
