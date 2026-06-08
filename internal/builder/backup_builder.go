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

	// backupHistoryVolumeName is the emptyDir volume that backs the writable
	// COORDINATOR_DATA_DIRECTORY used by gpbackup for its history database.
	backupHistoryVolumeName = "backup-history"
	// backupHistoryMountPath is where the backup-history emptyDir is mounted; it
	// is exported as COORDINATOR_DATA_DIRECTORY so gpbackup writes its history DB
	// to a writable path (the standalone Job pod lacks the coordinator PGDATA).
	backupHistoryMountPath = "/var/lib/gpbackup"

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

	// envCBDBDatabase is the env key holding the backup target database. It is
	// informational/inspectable (spec 11): the gpbackup invocation continues to
	// pass the database via the --dbname CLI arg (see buildGpbackupArgs).
	envCBDBDatabase = "CBDB_DATABASE"
	// envCompressionLevel is the env key holding the gpbackup compression level.
	envCompressionLevel = "COMPRESSION_LEVEL"
	// envCompressionType is the env key holding the gpbackup compression type.
	envCompressionType = "COMPRESSION_TYPE"
	// envBackupJobs is the env key holding the gpbackup parallel job count.
	envBackupJobs = "BACKUP_JOBS"

	// defaultCompressionLevel is the COMPRESSION_LEVEL env default when unset.
	defaultCompressionLevel = "1"
	// defaultCompressionType is the COMPRESSION_TYPE env default when unset.
	defaultCompressionType = "gzip"
	// defaultBackupJobs is the BACKUP_JOBS env default when unset.
	defaultBackupJobs = "1"

	// pluginConfigFlag is the gpbackup/gprestore plugin-config flag.
	pluginConfigFlag = "--plugin-config"

	// destinationTypeS3 is the S3 destination type value.
	destinationTypeS3 = "s3"
	// destinationTypeLocal is the local destination type value.
	destinationTypeLocal = "local"

	// defaultS3AccessKeyField is the default Secret key for the S3 access key id.
	defaultS3AccessKeyField = util.DefaultS3AccessKeyField
	// defaultS3SecretKeyField is the default Secret key for the S3 secret access key.
	defaultS3SecretKeyField = util.DefaultS3SecretKeyField

	// NOTE: aws_signature_version was removed from the S3 plugin config template
	// because the version-matched gpbackup_s3_plugin (2.1.0-incubating) does not
	// recognize this option. SigV4 is the default for both AWS and MinIO.

	// preBackupCheckContainerName is the name of the pre-backup health-check init container.
	preBackupCheckContainerName = "pre-backup-check"
	// longRunningTxnThresholdSeconds is the age (seconds) above which an open
	// transaction is considered long-running and blocks the backup pre-check.
	longRunningTxnThresholdSeconds = 3600
	// minBackupDiskFreeKB is the minimum free space (KiB) required on the local
	// backup-dir mount before a backup proceeds (best-effort check).
	minBackupDiskFreeKB = 1048576

	// gpEnvPreamble is prepended to every backup/restore script to ensure
	// GPHOME/bin is on PATH and LD_LIBRARY_PATH includes GPHOME/lib.  It
	// sources the Cloudberry env file when present (official RPM images ship
	// cloudberry-env.sh instead of greenplum_path.sh) and falls back to a
	// manual export so the script works with any image layout.
	//
	// The whole block is a no-op when GPHOME is unset/empty: the
	// cloudberry-backup:2.1.0 runtime image does NOT set GPHOME and ships the
	// gpbackup/gprestore/gpbackup_s3_plugin binaries on the default PATH
	// (/usr/local/bin per Dockerfile.cloudberry-backup), so touching GPHOME
	// paths there is unnecessary.  Every reference uses ${GPHOME:-} and the
	// outer guard checks for a non-empty value so the script stays safe under
	// `set -u` (a bare ${GPHOME} would abort with "GPHOME: unbound variable").
	gpEnvPreamble = "if [ -n \"${GPHOME:-}\" ]; then " +
		"if [ -f \"${GPHOME:-}/cloudberry-env.sh\" ]; then source \"${GPHOME:-}/cloudberry-env.sh\"; " +
		"elif [ -f \"${GPHOME:-}/greenplum_path.sh\" ]; then source \"${GPHOME:-}/greenplum_path.sh\"; " +
		"else export PATH=\"${GPHOME:-}/bin:${PATH}\"; " +
		"export LD_LIBRARY_PATH=\"${GPHOME:-}/lib:${GPHOME:-}/lib64:${LD_LIBRARY_PATH:-}\"; fi; fi\n"

	// validateContainerName is the container name for the post-restore validation Job.
	validateContainerName = "post-restore-validate"
	// defaultHealthCheckQuery is the default connectivity health-check query.
	defaultHealthCheckQuery = "SELECT 1"

	// sshSetupPreamble installs the cluster-wide shared gpadmin SSH identity (the
	// operator mounts it read-only at /etc/cloudberry/ssh) into ~/.ssh with the
	// strict permissions sshd/ssh require, and writes a SILENT ssh client config.
	// gpbackup/gprestore dispatch over SSH to every segment to create per-segment
	// backup directories and run gpbackup_helper, so the Job needs the same
	// identity the cluster pods trust. The client config disables host-key
	// warnings (StrictHostKeyChecking/UserKnownHostsFile) which would otherwise
	// be treated as failure (exit 254) by gpbackup's command-runner. The whole
	// block is a guarded no-op when the shared keys are absent (e.g. local runs).
	sshSetupPreamble = "if [ -f /etc/cloudberry/ssh/id_ed25519 ]; then " +
		"mkdir -p \"${HOME}/.ssh\" && chmod 700 \"${HOME}/.ssh\"; " +
		"install -m 600 /etc/cloudberry/ssh/id_ed25519 \"${HOME}/.ssh/id_ed25519\"; " +
		"install -m 644 /etc/cloudberry/ssh/id_ed25519.pub \"${HOME}/.ssh/id_ed25519.pub\"; " +
		"if [ -f /etc/cloudberry/ssh/authorized_keys ]; then " +
		"install -m 600 /etc/cloudberry/ssh/authorized_keys \"${HOME}/.ssh/authorized_keys\"; " +
		"else install -m 600 /etc/cloudberry/ssh/id_ed25519.pub \"${HOME}/.ssh/authorized_keys\"; fi; " +
		"printf 'Host *\\n  StrictHostKeyChecking no\\n  UserKnownHostsFile /dev/null\\n" +
		"  LogLevel ERROR\\n  BatchMode yes\\n' > \"${HOME}/.ssh/config\"; " +
		"chmod 600 \"${HOME}/.ssh/config\"; touch \"${HOME}/.hushlogin\"; fi\n"

	// gpbackupPluginPathPreamble guarantees /usr/local/bin/gpbackup_s3_plugin
	// (the path pinned in the plugin config template) exists at runtime. On the
	// cloudberry-backup image the binary is already there. On the
	// cloudberry-official image it lives at $GPHOME/bin, so we symlink it into
	// /usr/local/bin (via sudo when needed). The block is a best-effort no-op
	// when the binary is already in place or cannot be linked.
	gpbackupPluginPathPreamble = "if [ ! -x /usr/local/bin/gpbackup_s3_plugin ] && " +
		"[ -n \"${GPHOME:-}\" ] && [ -x \"${GPHOME:-}/bin/gpbackup_s3_plugin\" ]; then " +
		"ln -sf \"${GPHOME}/bin/gpbackup_s3_plugin\" /usr/local/bin/gpbackup_s3_plugin 2>/dev/null || " +
		"sudo ln -sf \"${GPHOME}/bin/gpbackup_s3_plugin\" /usr/local/bin/gpbackup_s3_plugin 2>/dev/null || " +
		"true; fi\n"
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
		// gprestore enforces that --include-schema and --include-table are
		// MUTUALLY EXCLUSIVE ("flags may not be specified together:
		// include-schema, include-table"). When both filters are supplied on a
		// restore we emit the more specific --include-table (table-level
		// precedence) and OMIT --include-schema, so the gprestore invocation
		// stays valid. When only one is set, emit that one as-is.
		if len(jobOpts.IncludeTables) > 0 {
			args = appendRepeatedFlag(args, "--include-table", jobOpts.IncludeTables)
		} else {
			args = appendRepeatedFlag(args, "--include-schema", jobOpts.IncludeSchemas)
		}
		args = appendRepeatedFlag(args, "--exclude-table", jobOpts.ExcludeTables)
	}

	return args
}

// appendGprestoreBoolFlags appends the gprestore boolean option flags that are
// enabled in opts, in a stable order.
//
// gprestore enforces that --run-analyze and --with-stats are MUTUALLY EXCLUSIVE
// ("flags may not be specified together: run-analyze, with-stats"). When BOTH
// are requested we emit --run-analyze (run-analyze precedence: recomputing
// planner statistics via ANALYZE supersedes restoring the backed-up stats) and
// OMIT --with-stats so the gprestore invocation stays valid. When only one is
// set, that one is emitted as-is.
func appendGprestoreBoolFlags(args []string, opts *cbv1alpha1.GprestoreOptions) []string {
	effectiveWithStats := opts.WithStats && !opts.RunAnalyze

	flags := []struct {
		enabled bool
		flag    string
	}{
		{opts.CreateDb, "--create-db"},
		{opts.WithGlobals, "--with-globals"},
		{effectiveWithStats, "--with-stats"},
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

// renderToolScript builds the bash script that renders the S3 plugin config
// (substituting environment variables) and then runs the given tool with its
// arguments.  It prefers envsubst when available but falls back to a
// POSIX-compatible eval+heredoc so the script works on minimal images that
// lack the gettext package.
func renderToolScript(tool string, args []string) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)
	b.WriteString(sshSetupPreamble)
	// Resolve the gpbackup_s3_plugin path for THIS image and export it so the
	// envsubst-rendered plugin config (executablepath: ${GPBACKUP_PLUGIN_PATH})
	// points at the real binary on either the cloudberry-backup image
	// (/usr/local/bin) or the cloudberry-official image ($GPHOME/bin).
	b.WriteString(gpbackupPluginPathPreamble)
	// Render the S3 plugin config template, substituting env vars.
	// Use envsubst if present; otherwise fall back to eval with heredoc.
	fmt.Fprintf(&b,
		"if command -v envsubst >/dev/null 2>&1; then "+
			"envsubst < %[1]s/%[2]s > %[3]s; "+
			"else eval \"cat <<_ENVSUBST_EOF_\n$(cat %[1]s/%[2]s)\n_ENVSUBST_EOF_\" > %[3]s; fi\n",
		s3ConfigMountPath, s3ConfigTemplateKey, s3RenderedConfigPath)
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

	// The gpbackup_s3_plugin uses path-style addressing automatically whenever a
	// custom (non-AWS) "endpoint" is set, which is exactly the MinIO case, so
	// emitting the endpoint already satisfies ForcePathStyle at the wire level.
	// The upstream plugin does not expose a dedicated force_path_style option, so
	// we do NOT invent an unsupported template key. We surface
	// S3_FORCE_PATH_STYLE as an env var (see buildS3Env) for explicitness and
	// observability.
	//
	// NOTE: aws_signature_version is intentionally NOT included in the template.
	// The version-matched gpbackup_s3_plugin (2.1.0-incubating) does not
	// recognize this option and rejects it with "field aws_signature_version not
	// found in type s3plugin.PluginOptions". SigV4 is the default for both AWS
	// and MinIO, so omitting it is safe.
	template := strings.Join([]string{
		// The gpbackup_s3_plugin lives at different paths per image: the
		// cloudberry-backup:2.1.0 image installs it at /usr/local/bin (and does
		// NOT set GPHOME), while the cloudberry-official:2.1.0 image bundles it
		// at $GPHOME/bin.  The executablepath is pinned to the canonical
		// /usr/local/bin/gpbackup_s3_plugin; the tool script guarantees that path
		// exists at runtime (symlinking to $GPHOME/bin/gpbackup_s3_plugin on the
		// official image) so the same template works on either image.
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
	// The CronJob's backup target databases are resolved at runtime, so
	// CBDB_DATABASE is emitted empty (still inspectable) per spec 11.
	applyBackupGpbackupEnv(&podSpec, "", cluster.Spec.Backup.Gpbackup)
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
	applyBackupGpbackupEnv(&podSpec, firstDatabase(opts.Databases), gpOpts)
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
	// The restore Job carries the same informational gpbackup env (spec 11) so
	// the container env is inspectable; compression/jobs come from the cluster's
	// gpbackupOptions and the database from the restore request.
	var gpOpts *cbv1alpha1.GpbackupOptions
	if cluster.Spec.Backup != nil {
		gpOpts = cluster.Spec.Backup.Gpbackup
	}
	applyBackupGpbackupEnv(&podSpec, firstDatabase(opts.Databases), gpOpts)

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
	b.WriteString(gpEnvPreamble)
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
	// gpbackup/gprestore dispatch over SSH to every segment to create per-segment
	// backup directories and run gpbackup_helper, even when streaming to S3. The
	// Job therefore needs the SHARED cluster SSH identity so it can reach the
	// segments listed in gp_segment_configuration.
	addBackupSSHIdentity(cluster, &podSpec)
	applyJobTemplatePod(cluster, &podSpec)
	return podSpec
}

// addBackupSSHIdentity mounts the cluster-wide shared SSH keypair Secret into the
// backup/restore Job pod and provides a writable scratch
// COORDINATOR_DATA_DIRECTORY for the gpbackup history database. The shared SSH
// identity lets the Job's gpbackup/gprestore dispatch to every segment over SSH
// using the same key the cluster pods install (see ssh_builder.go and the
// entrypoint start_sshd). The history DB needs a writable path because the Job
// pod is a standalone backup pod that does not carry the coordinator's PGDATA.
func addBackupSSHIdentity(cluster *cbv1alpha1.CloudberryCluster, podSpec *corev1.PodSpec) {
	podSpec.Volumes = append(podSpec.Volumes,
		sshSecretVolume(cluster),
		corev1.Volume{
			Name: backupHistoryVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	)
	for i := range podSpec.Containers {
		podSpec.Containers[i].VolumeMounts = append(
			podSpec.Containers[i].VolumeMounts,
			sshSecretVolumeMount(),
			corev1.VolumeMount{Name: backupHistoryVolumeName, MountPath: backupHistoryMountPath},
		)
		setEnvVar(&podSpec.Containers[i], "COORDINATOR_DATA_DIRECTORY", backupHistoryMountPath)
	}
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
	b.WriteString(gpEnvPreamble)
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

// applyBackupGpbackupEnv sets the informational/inspectable gpbackup env vars
// (CBDB_DATABASE, COMPRESSION_LEVEL, COMPRESSION_TYPE, BACKUP_JOBS) on the first
// container of the pod spec. These mirror the existing gpbackup CLI args (which
// remain the source of truth for the actual invocation) so the Job container env
// is inspectable per spec 11. database is the resolved backup target ("" when
// none, e.g. the CronJob path whose databases are resolved at runtime); gpOpts
// supplies compression/jobs (defaults 1/gzip/1 apply when unset).
func applyBackupGpbackupEnv(
	podSpec *corev1.PodSpec,
	database string,
	gpOpts *cbv1alpha1.GpbackupOptions,
) {
	if len(podSpec.Containers) == 0 {
		return
	}
	container := &podSpec.Containers[0]

	compressionLevel := defaultCompressionLevel
	compressionType := defaultCompressionType
	backupJobs := defaultBackupJobs
	if gpOpts != nil {
		if gpOpts.CompressionLevel > 0 {
			compressionLevel = strconv.Itoa(int(gpOpts.CompressionLevel))
		}
		if gpOpts.CompressionType != "" {
			compressionType = gpOpts.CompressionType
		}
		if gpOpts.Jobs > 0 {
			backupJobs = strconv.Itoa(int(gpOpts.Jobs))
		}
	}

	setEnvVar(container, envCBDBDatabase, database)
	setEnvVar(container, envCompressionLevel, compressionLevel)
	setEnvVar(container, envCompressionType, compressionType)
	setEnvVar(container, envBackupJobs, backupJobs)
}

// firstDatabase returns the backup target database (the first entry), or "" when
// none is configured (e.g. the scheduled CronJob whose databases are resolved at
// runtime).
func firstDatabase(databases []string) string {
	if len(databases) > 0 {
		return databases[0]
	}
	return ""
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
	return append(env, buildS3Env(cluster, cluster.Spec.Backup.Destination.S3)...)
}

// buildS3Env builds the S3-related environment variables consumed by the
// envsubst-rendered plugin config.
func buildS3Env(cluster *cbv1alpha1.CloudberryCluster, s3 *cbv1alpha1.S3Destination) []corev1.EnvVar {
	if s3 == nil {
		return nil
	}

	encryption := s3.Encryption
	if encryption == "" {
		encryption = "on"
	}

	env := make([]corev1.EnvVar, 0, 12)
	env = append(env, []corev1.EnvVar{
		{Name: "S3_REGION", Value: s3.Region},
		{Name: "S3_ENDPOINT", Value: s3.Endpoint},
		{Name: "S3_BUCKET", Value: s3.Bucket},
		{Name: "S3_FOLDER", Value: s3.Folder},
		{Name: "S3_ENCRYPTION", Value: encryption},
		// S3_FORCE_PATH_STYLE is surfaced for explicitness/observability. The s3
		// plugin itself derives path-style addressing from a custom endpoint
		// (MinIO), so this env var documents intent and lets future plugin
		// versions or tooling consume it without changing the Job shape.
		{Name: "S3_FORCE_PATH_STYLE", Value: strconv.FormatBool(s3.ForcePathStyle)},
	}...)
	env = append(env, buildS3MultipartEnv(s3.Multipart)...)
	name, accessKeyField, secretKeyField := resolveS3CredentialSource(cluster, s3)
	env = append(env, buildS3CredentialEnv(name, accessKeyField, secretKeyField)...)
	return env
}

// resolveS3CredentialSource selects the Kubernetes Secret name and field keys
// that supply the AWS credentials for the S3 plugin. When a CredentialSecret is
// configured it is used directly; otherwise, when a VaultSecret is configured,
// the operator-materialized Secret (BackupS3VaultCredentialsSecretName) is used
// with the default field names so the Job spec always references a Secret and
// never embeds plaintext credentials. Returns empty name when neither is set.
func resolveS3CredentialSource(
	cluster *cbv1alpha1.CloudberryCluster,
	s3 *cbv1alpha1.S3Destination,
) (name, accessKeyField, secretKeyField string) {
	if cred := s3.CredentialSecret; cred != nil && cred.Name != "" {
		accessKeyField = cred.AccessKeyField
		if accessKeyField == "" {
			accessKeyField = defaultS3AccessKeyField
		}
		secretKeyField = cred.SecretKeyField
		if secretKeyField == "" {
			secretKeyField = defaultS3SecretKeyField
		}
		return cred.Name, accessKeyField, secretKeyField
	}
	if vs := s3.VaultSecret; vs != nil && vs.Path != "" {
		// The Vault-sourced credentials are materialized into a Secret with the
		// canonical default field names (see ensureBackupS3VaultCredentials).
		return util.BackupS3VaultCredentialsSecretName(cluster.Name),
			defaultS3AccessKeyField, defaultS3SecretKeyField
	}
	return "", "", ""
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

// buildS3CredentialEnv builds AWS credential env vars sourced from the named
// Secret using the given field keys. It is a pure function: the caller resolves
// the Secret name and field names (see resolveS3CredentialSource). When name is
// empty no credential env vars are emitted.
func buildS3CredentialEnv(name, accessKeyField, secretKeyField string) []corev1.EnvVar {
	if name == "" {
		return nil
	}
	return []corev1.EnvVar{
		{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
					Key:                  accessKeyField,
				},
			},
		},
		{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
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
