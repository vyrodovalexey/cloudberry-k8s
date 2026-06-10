// Package builder: backup_builder.go constructs the gpbackup/gprestore-centric
// Kubernetes resources (ConfigMap, CronJob, on-demand backup/restore Jobs and the
// retention-cleanup Job) backed by the apache/cloudberry-backup toolchain.
package builder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultBackupImage is the fallback backup toolchain image (kept in sync
	// with the mutating-webhook default via util.DefaultBackupImage).
	defaultBackupImage = util.DefaultBackupImage

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

	// backupHistoryVolumeName is the emptyDir volume mounted on the backup/restore
	// Job pod at backupHistoryMountPath. It is retained for the local-destination
	// gpbackman retention path and as an inspectable scratch COORDINATOR_DATA_DIRECTORY.
	//
	// NOTE: for the S3 destination the real gpbackup/gprestore run happens INSIDE
	// the coordinator pod via kubectl exec (the coordinator-exec model, see
	// coordinatorExecScript and spec 11 §MPP Dispatch). gpbackup, once connected
	// to the coordinator, reads gp_segment_configuration and writes its
	// coordinator metadata + history DB to the CATALOG coordinator data directory
	// (/data/pgdata/gpseg-1) and dispatches to each segment to create the
	// per-segment backup dirs — none of which exist in a standalone Job pod, so
	// the Job delegates to the coordinator pod where they do. This emptyDir is
	// therefore not the authoritative history-DB path for S3.
	backupHistoryVolumeName = "backup-history"
	// backupHistoryMountPath is where the backup-history emptyDir is mounted and
	// exported as COORDINATOR_DATA_DIRECTORY (inspectable). For the local
	// destination it backs the gpbackman --history-db path; for S3 the real run
	// is delegated to the coordinator pod (see coordinatorExecScript).
	backupHistoryMountPath = "/var/lib/gpbackup"
	// gpbackupHistoryDBPath is the SQLite history database path used by the
	// gpbackman retention script via --history-db for
	// backup-info/backup-delete/backup-clean.
	gpbackupHistoryDBPath = backupHistoryMountPath + "/gpbackup_history.db"

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
	// backupDirFlag is the gpbackup/gprestore/gpbackman backup-dir flag used for
	// the local destination (no S3 plugin).
	backupDirFlag = "--backup-dir"

	// kubectlBin is the kubectl binary used by the coordinator-exec wrapper to
	// run gpbackup/gprestore inside the coordinator pod. The cloudberry-backup
	// image ships kubectl on PATH; the wrapper falls back to /usr/local/bin and
	// /usr/bin so a non-PATH install still resolves.
	kubectlBin = "kubectl"
	// coordExecScratchDir is the writable directory inside the COORDINATOR pod
	// where the per-run rendered S3 plugin config is written before gpbackup is
	// invoked there. The coordinator pod's /tmp is always writable.
	coordExecScratchDir = "/tmp"

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
	// defaultS3SigningRegion is the SigV4 region used for the S3 reachability
	// HEAD when S3_REGION is empty. MinIO accepts us-east-1 by default and it
	// matches the signing region used in test/docker-compose/scripts/setup-minio.sh.
	defaultS3SigningRegion = "us-east-1"
	// s3ReachabilityMaxTimeSeconds bounds the SigV4 HEAD request so a hung/unreachable
	// endpoint fails closed (curl --max-time) instead of stalling the init container.
	s3ReachabilityMaxTimeSeconds = 15

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

	// validateMarkerPrefix prefixes every parsable marker the post-restore
	// validation script emits so its log lines can be matched deterministically.
	validateMarkerPrefix = "post-restore-validate: "

	// gprestoreTool is the gprestore tool name used to gate the statistics
	// exit-code tolerance wrapper on restore Jobs only.
	gprestoreTool = "gprestore"
	// withStatsFlag is the gpbackup/gprestore --with-stats flag.
	withStatsFlag = "--with-stats"
	// restorePartialMarker is written to the restore Job pod's termination log
	// (and stdout) when gprestore exits with code 2 — the known upstream
	// gpbackup bug where ONLY the statistics restore fails (invalid bigint in
	// statistics.sql) while the data restore succeeded. The admin controller
	// parses this marker to annotate the Job and emit the RestorePartial
	// Warning Event with the "partial" metric result.
	restorePartialMarker = "GPRESTORE_PARTIAL=stats"
	// gprestoreStatsExitGuard converts a gprestore exit code 2 (statistics-only
	// failure) into success-with-warning: it logs the partial marker, writes it
	// to /dev/termination-log for the controller to pick up, and clears rc so
	// the Job succeeds. Any other non-zero rc still fails the Job.
	gprestoreStatsExitGuard = "if [ \"${rc}\" -eq 2 ]; then " +
		"echo 'gprestore-partial: statistics restore failed (exit code 2); " +
		"data restore succeeded'; " +
		"printf '%s' '" + restorePartialMarker + "' > /dev/termination-log 2>/dev/null || true; " +
		"rc=0; fi\n"

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

// gpbackupOrSpec resolves the effective gpbackup options for the request: the
// per-request override when set, else the cluster spec's gpbackup options. It is
// the single source of truth shared by BuildBackupJob (for the rendered args) and
// effectiveBackupType (for the backup-type label) so the label always matches the
// args. Safe on a nil receiver.
func (o *BackupJobOptions) gpbackupOrSpec(
	cluster *cbv1alpha1.CloudberryCluster,
) *cbv1alpha1.GpbackupOptions {
	if o != nil && o.Gpbackup != nil {
		return o.Gpbackup
	}
	if cluster.Spec.Backup != nil {
		return cluster.Spec.Backup.Gpbackup
	}
	return nil
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
	// S3FolderOverride, when non-empty, overrides the S3 plugin config "folder:"
	// (the S3_FOLDER env) for this restore Job instead of using the cluster's own
	// spec.backup.destination.s3.folder. This is REQUIRED for cross-cluster
	// migration (spec 11 §Cross-Cluster Migration): the target restore Job must
	// read the backup from the SOURCE cluster's folder, since gpbackup wrote it
	// there. Without it the restore would look under the target's own folder and
	// fail with a NotFound (the data is under a different folder).
	S3FolderOverride string
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

// effectiveBackupType resolves the backup type that will actually run for a
// backup Job: "incremental" when the cluster spec enables incremental backups OR
// the per-request options set Type=="incremental", else "full". opts may be nil
// (e.g. the CronJob jobTemplate path), in which case only the spec is consulted.
func effectiveBackupType(cluster *cbv1alpha1.CloudberryCluster, opts *BackupJobOptions) string {
	// The label MUST match the gpbackup args actually rendered (i.e. whether
	// --incremental is emitted by isEffectivelyIncremental), so M1/M2 metric
	// routing and the rendered Job agree. The args resolve the gpbackup options
	// the same way BuildBackupJob does: per-request opts.Gpbackup, else the
	// cluster spec's Gpbackup. A per-request opts.Type=="incremental" also forces
	// incremental. A bare opts.Type=="full" does NOT suppress a cluster-level
	// incremental default (gpbackup still runs incremental), so the label stays
	// incremental to match the args.
	gpOpts := opts.gpbackupOrSpec(cluster)
	if isEffectivelyIncremental(gpOpts, opts) {
		return util.BackupTypeIncremental
	}
	return util.BackupTypeFull
}

// localBackupDir resolves the on-pod backup directory for a local destination:
// the configured Local.Path when set, otherwise the default localBackupMountPath
// (/backups). It is the single source of truth for the local backup path shared
// by the gpbackup/gprestore args, the volume mount and the retention script.
func localBackupDir(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Backup != nil {
		if local := cluster.Spec.Backup.Destination.Local; local != nil && local.Path != "" {
			return local.Path
		}
	}
	return localBackupMountPath
}

// isLocalDestination reports whether the cluster's backup destination is local.
func isLocalDestination(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.Backup != nil &&
		cluster.Spec.Backup.Destination.Type == destinationTypeLocal
}

// backupDestinationArgs returns the leading gpbackup/gprestore args for the
// cluster's backup destination:
//   - local -> ["--backup-dir", <Local.Path or /backups>]
//   - s3 (or nil/unknown, preserving existing behavior) ->
//     ["--plugin-config", "/tmp/s3-config.yaml"]
//
// NOTE: gpbackup REJECTS --plugin-config together with --backup-dir ("The
// following flags may not be specified together: plugin-config, backup-dir"), so
// the S3 path uses ONLY --plugin-config. For the S3 plugin, the per-segment DATA
// files are streamed to S3 by each segment's gpbackup_s3_plugin while the
// coordinator metadata + gpbackup history database are written under the
// coordinator data directory; the backup Job exports COORDINATOR_DATA_DIRECTORY
// (= backupHistoryMountPath, a writable emptyDir) so gpbackup writes its history
// DB there instead of the non-existent catalog path inside the standalone Job
// pod. The per-segment backup-directory creation requires the backup to run with
// coordinator+segment reachability (see addBackupSSHIdentity / the cluster image
// SSH wiring).
//
// Defaulting nil/empty/unknown destinations to the S3 leading args keeps every
// existing S3 caller's plugin wiring intact.
func backupDestinationArgs(cluster *cbv1alpha1.CloudberryCluster) []string {
	if isLocalDestination(cluster) {
		return []string{backupDirFlag, localBackupDir(cluster)}
	}
	return []string{pluginConfigFlag, s3RenderedConfigPath}
}

// buildGpbackupArgs converts gpbackup options and per-request overrides into a
// gpbackup CLI argument slice. It is a pure function for easy unit testing. The
// leading args are destination-aware (see backupDestinationArgs).
//
// gpbackup hard-requires --dbname (it aborts with `required flag(s) "dbname"
// not set`), so an empty target database is a build-time ERROR here instead of
// a silently broken command that only fails when the Job pod runs. Callers
// resolve the database first (see withDefaultBackupDatabase); the REST API
// additionally rejects database-less create-backup requests with 400.
func buildGpbackupArgs(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *cbv1alpha1.GpbackupOptions,
	jobOpts *BackupJobOptions,
) ([]string, error) {
	var dbname string
	if jobOpts != nil {
		dbname = firstDatabase(jobOpts.Databases)
	}
	if dbname == "" {
		return nil, fmt.Errorf(
			"building gpbackup args: no target database specified (gpbackup requires --dbname)")
	}

	args := backupDestinationArgs(cluster)
	args = append(args, "--dbname", dbname)

	if opts == nil {
		opts = &cbv1alpha1.GpbackupOptions{}
	}

	args = appendCompressionArgs(args, opts)
	args = appendDataFileArgs(args, opts)
	args = appendIncrementalArgs(args, opts, jobOpts)
	args = appendLeafPartitionDataArgs(args, opts, jobOpts)

	// WithStats is a *bool: nil means "unset" and follows the webhook default of
	// true; a non-nil value is honored (false => omit the flag).
	if util.DerefOr(opts.WithStats, true) {
		args = append(args, withStatsFlag)
	}
	if opts.WithoutGlobals {
		args = append(args, "--without-globals")
	}

	if jobOpts != nil {
		args = appendRepeatedFlag(args, "--include-schema", jobOpts.IncludeSchemas)
		args = appendRepeatedFlag(args, "--include-table", jobOpts.IncludeTables)
		args = appendRepeatedFlag(args, "--exclude-table", jobOpts.ExcludeTables)
	}

	return args, nil
}

// withDefaultBackupDatabase returns opts with Databases defaulted to the
// coordinator maintenance database (postgres, matching the Job's PGDATABASE
// connection default) when none is specified, without mutating the caller's
// value. It is nil-safe.
//
// Rationale (documented behavior): gpbackup hard-requires --dbname and the
// CRD's BackupSpec declares NO databases/defaultDatabase field, so the
// scheduled CronJob path has no user-supplied database to render — defaulting
// here keeps every rendered gpbackup command valid. User-facing on-demand
// requests are stricter: the REST API rejects an empty databases list with
// 400, so this fallback only serves the CronJob path and direct builder
// callers.
func withDefaultBackupDatabase(opts *BackupJobOptions) *BackupJobOptions {
	if opts == nil {
		opts = &BackupJobOptions{}
	}
	if len(opts.Databases) > 0 {
		return opts
	}
	out := *opts
	out.Databases = []string{defaultCoordinatorDatabase}
	return &out
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

// isEffectivelyIncremental reports whether the backup that will actually run is
// incremental: the gpbackup options set Incremental, OR the per-request job
// options pin Type=="incremental". It is the single source of truth shared by
// appendIncrementalArgs and appendLeafPartitionDataArgs so the incremental vs
// full decision (and therefore where --leaf-partition-data is emitted) stays
// consistent and never double-emits the flag.
func isEffectivelyIncremental(opts *cbv1alpha1.GpbackupOptions, jobOpts *BackupJobOptions) bool {
	if opts != nil && opts.Incremental {
		return true
	}
	return jobOpts != nil && jobOpts.Type == util.BackupTypeIncremental
}

// appendIncrementalArgs appends incremental-related flags, including the optional
// per-request --from-timestamp pin.
func appendIncrementalArgs(
	args []string,
	opts *cbv1alpha1.GpbackupOptions,
	jobOpts *BackupJobOptions,
) []string {
	if !isEffectivelyIncremental(opts, jobOpts) {
		return args
	}
	// gpbackup REQUIRES --leaf-partition-data for incremental backups, so force
	// it whenever an incremental is effective. opts.LeafPartitionData is already
	// implied here; emitting it unconditionally (and only once) avoids both a
	// missing flag when LeafPartitionData is unset and a duplicate when it is
	// set. Full backups are handled by appendLeafPartitionDataArgs: this branch
	// only runs for incrementals.
	args = append(args, "--incremental", "--leaf-partition-data")
	if jobOpts != nil && jobOpts.FromTimestamp != "" {
		args = append(args, "--from-timestamp", jobOpts.FromTimestamp)
	}
	return args
}

// appendLeafPartitionDataArgs emits --leaf-partition-data for a FULL backup when
// opts.LeafPartitionData is requested. --leaf-partition-data is valid and
// meaningful for full backups (it backs up leaf-partition data as separate files
// rather than the whole partitioned table), so a requested LeafPartitionData
// must be honored even when the backup is not incremental.
//
// The incremental path (appendIncrementalArgs) already force-emits
// --leaf-partition-data EXACTLY once, so this helper guards on
// !isEffectivelyIncremental to preserve that invariant and never duplicate the
// flag. Net behavior:
//   - full + LeafPartitionData=false   => no --leaf-partition-data
//   - full + LeafPartitionData=true    => exactly one --leaf-partition-data
//   - incremental (any LeafPartitionData) => exactly one (from the incr path)
func appendLeafPartitionDataArgs(
	args []string,
	opts *cbv1alpha1.GpbackupOptions,
	jobOpts *BackupJobOptions,
) []string {
	if opts.LeafPartitionData && !isEffectivelyIncremental(opts, jobOpts) {
		args = append(args, "--leaf-partition-data")
	}
	return args
}

// buildGprestoreArgs converts gprestore options and per-request parameters into a
// gprestore CLI argument slice. It is a pure function for easy unit testing. The
// leading args are destination-aware (see backupDestinationArgs).
func buildGprestoreArgs(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *cbv1alpha1.GprestoreOptions,
	jobOpts *RestoreJobOptions,
) []string {
	args := backupDestinationArgs(cluster)
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
	// WithStats is a *bool: nil means "unset" and follows the webhook default of
	// FALSE (restores skip statistics unless explicitly requested — a known
	// upstream gpbackup bug can make a statistics-only restore fail with exit
	// code 2 while the data restore succeeded); a non-nil value is honored.
	// run-analyze still takes precedence so the gprestore invocation stays valid.
	effectiveWithStats := util.DerefOr(opts.WithStats, false) && !opts.RunAnalyze

	flags := []struct {
		enabled bool
		flag    string
	}{
		{opts.CreateDb, "--create-db"},
		{opts.WithGlobals, "--with-globals"},
		{effectiveWithStats, withStatsFlag},
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

// renderToolScript builds the bash script the backup/restore Job container runs.
//
// S3 destination — coordinator-exec (the real-bug fix). gpbackup/gprestore are
// MPP orchestrators: once connected to the coordinator they read
// gp_segment_configuration and (a) write the coordinator metadata + history DB
// to the CATALOG coordinator data directory (/data/pgdata/gpseg-1) and (b)
// dispatch over SSH to EVERY segment to create the per-segment backup dirs.
// A standalone Job pod connected only via PGHOST has neither /data/pgdata/gpseg-1
// nor the segment-local backup paths, so gpbackup fails with "Unable to create
// backup directories on N segments" and "open .../gpbackup_history.db: no such
// file or directory". The supported model (spec 11 §MPP Dispatch and the
// Coordinator-Exec Data Cycle) is to run the tool INSIDE the coordinator pod.
// For S3 the Job therefore renders the plugin config and `kubectl exec`s the
// tool into <cluster>-coordinator-0 (see coordinatorExecScript), where the
// catalog datadir, the writable filesystem and the segment SSH topology all
// exist. NOTE: --backup-dir is intentionally NOT combined with --plugin-config
// (gpbackup rejects "plugin-config, backup-dir" together); the S3 args carry
// only --plugin-config (see backupDestinationArgs).
//
// LOCAL destination — standalone --backup-dir (unchanged operator behavior,
// spec 11 §Local Backup Destination). There is no S3 ConfigMap mounted at
// /etc/gpbackup, so the S3-config render block is skipped (reading the missing
// template under `set -euo pipefail` would abort the Job) and the tool runs
// in-pod with --backup-dir; the live local data cycle is exercised via the
// coordinator-exec path by the e2e script (spec 11 §MPP per-segment note).
//
// An empty tool (the validate/cleanup Jobs, which overwrite Args[0] with their
// own script afterward) renders only the preambles so no tool is invoked here.
func renderToolScript(cluster *cbv1alpha1.CloudberryCluster, tool string, args []string) string {
	if tool != "" && !isLocalDestination(cluster) {
		return coordinatorExecScript(cluster, tool, args)
	}

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)
	b.WriteString(sshSetupPreamble)
	// Resolve the gpbackup_s3_plugin path for THIS image and export it so the
	// envsubst-rendered plugin config (executablepath: ${GPBACKUP_PLUGIN_PATH})
	// points at the real binary on either the cloudberry-backup image
	// (/usr/local/bin) or the cloudberry-official image ($GPHOME/bin).
	b.WriteString(gpbackupPluginPathPreamble)
	if tool == "" {
		return b.String()
	}
	if statsPartialTolerated(tool, args) {
		// gprestore with statistics restore requested: capture the exit code so
		// a statistics-only failure (exit 2) is downgraded to success-with-warning
		// while any other failure still fails the Job.
		b.WriteString("rc=0\n")
		writeToolInvocation(&b, tool, args)
		b.WriteString(" || rc=$?\n")
		b.WriteString(gprestoreStatsExitGuard)
		b.WriteString("exit \"${rc}\"\n")
		return b.String()
	}
	writeToolInvocation(&b, tool, args)
	b.WriteString("\n")
	return b.String()
}

// writeToolInvocation writes "<tool> 'arg1' 'arg2' ..." (no trailing newline).
func writeToolInvocation(b *strings.Builder, tool string, args []string) {
	b.WriteString(tool)
	for _, a := range args {
		b.WriteString(" ")
		b.WriteString(shellQuote(a))
	}
}

// statsPartialTolerated reports whether the tool invocation is a gprestore run
// that requested statistics restore (--with-stats) and therefore tolerates the
// statistics-only exit code 2 as success-with-warning (the known upstream
// gpbackup bug: invalid bigint in statistics.sql fails ONLY the stats restore
// while the data restore succeeded).
func statsPartialTolerated(tool string, args []string) bool {
	if tool != gprestoreTool {
		return false
	}
	for _, a := range args {
		if a == withStatsFlag {
			return true
		}
	}
	return false
}

// coordinatorExecScript builds the coordinator-exec wrapper the S3 backup/restore
// Job container runs. It:
//
//  1. Renders the S3 plugin config from the Job container env (envsubst, with a
//     POSIX eval+heredoc fallback) into a per-run file in the Job pod.
//  2. base64-pipes the rendered config into the coordinator pod's /tmp via
//     `kubectl exec` (base64 avoids any quoting/here-doc hazard over the exec
//     boundary — the lesson from the e2e scenarios).
//  3. base64-pipes the inner tool script (sources the Cloudberry env, exports the
//     PG*/AWS* connection vars, then runs `<tool> --plugin-config <coord-cfg>
//     <args>`) into a second `kubectl exec` so the MPP run happens inside the
//     coordinator pod (segment -1) with the catalog datadir, writable filesystem
//     and segment SSH topology all present.
//
// A unique per-run timestamp ($RANDOM + date) names the coordinator-side config
// so concurrent Jobs never collide (spec 11 §38). The PG password reaches the
// coordinator only over the in-cluster exec channel, never on its disk or in the
// process table beyond the transient gpbackup invocation.
func coordinatorExecScript(cluster *cbv1alpha1.CloudberryCluster, tool string, args []string) string {
	coordPod := util.CoordinatorPodName(cluster.Name)

	// Drop the leading "--plugin-config <Job-pod path>" pair from backupDestinationArgs:
	// the coordinator-exec run substitutes the coordinator-side ${COORD_CFG}.
	toolArgs := dropLeadingPluginConfig(args)

	// innerTool is the tool script that runs INSIDE the coordinator pod. It is
	// emitted READABLY (via a quoted heredoc) so the gpbackup/gprestore flags
	// stay visible in the Job container's args[0] — the e2e suites inspect
	// `kubectl get job -o jsonpath=...args[0]` for `--include-schema`,
	// `--incremental`, `--resize-cluster`, etc.
	//
	// The connection env (COORD_CFG/PG*) is delivered as base64-encoded POSITIONAL
	// arguments to the remote bash (env-safe for any password / shell
	// metacharacter; kubectl exec forwards argv verbatim but does NOT forward
	// env). innerTool decodes them into $1..$6, sources the Cloudberry env so
	// gpbackup/gprestore + the plugin are on PATH, then runs the tool against the
	// per-run plugin config staged at ${COORD_CFG}. No secret value is embedded
	// in this rendered Go string (the secret arrives at runtime from the Job
	// pod's own env via the positional args).
	var innerTool strings.Builder
	innerTool.WriteString("set -euo pipefail\n")
	innerTool.WriteString("export COORD_CFG=$(printf '%s' \"$1\" | base64 -d)\n")
	innerTool.WriteString("export PGHOST=$(printf '%s' \"$2\" | base64 -d)\n")
	innerTool.WriteString("export PGPORT=$(printf '%s' \"$3\" | base64 -d)\n")
	innerTool.WriteString("export PGUSER=$(printf '%s' \"$4\" | base64 -d)\n")
	innerTool.WriteString("export PGDATABASE=$(printf '%s' \"$5\" | base64 -d)\n")
	innerTool.WriteString("export PGPASSWORD=$(printf '%s' \"$6\" | base64 -d)\n")
	// `ls` of the (possibly missing) env file returns non-zero; with `pipefail`
	// + `set -e` that would abort, so guard with `|| true`. Likewise the
	// `[ -n ] && source` test must not abort when GPENV is empty.
	innerTool.WriteString("GPENV=$(ls \"${GPHOME:-/usr/local/cloudberry-db}\"/greenplum_path.sh " +
		"\"${GPHOME:-/usr/local/cloudberry-db}\"/cloudberry-env.sh 2>/dev/null | head -1 || true)\n")
	innerTool.WriteString("if [ -n \"${GPENV}\" ]; then . \"${GPENV}\"; fi\n")
	innerTool.WriteString("export PATH=\"${GPHOME:-/usr/local/cloudberry-db}/bin:${PATH}\"\n")
	innerTool.WriteString(tool)
	innerTool.WriteString(" " + pluginConfigFlag + " \"${COORD_CFG}\"")
	for _, a := range toolArgs {
		innerTool.WriteString(" ")
		innerTool.WriteString(shellQuote(a))
	}
	innerTool.WriteString("\n")

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)
	// Render the S3 plugin config (envsubst with POSIX fallback) in the Job pod.
	fmt.Fprintf(&b,
		"if command -v envsubst >/dev/null 2>&1; then "+
			"envsubst < %[1]s/%[2]s > %[3]s; "+
			"else eval \"cat <<_ENVSUBST_EOF_\n$(cat %[1]s/%[2]s)\n_ENVSUBST_EOF_\" > %[3]s; fi\n",
		s3ConfigMountPath, s3ConfigTemplateKey, s3RenderedConfigPath)
	// Resolve a kubectl binary (PATH, then common install dirs).
	b.WriteString("KUBECTL=" + kubectlBin + "\n")
	b.WriteString("command -v \"${KUBECTL}\" >/dev/null 2>&1 || " +
		"{ for p in /usr/local/bin/kubectl /usr/bin/kubectl; do " +
		"[ -x \"$p\" ] && KUBECTL=\"$p\" && break; done; }\n")
	fmt.Fprintf(&b, "COORD_POD=%s\n", shellQuote(coordPod))
	fmt.Fprintf(&b, "COORD_CFG=%s/cbk-$(date -u +%%Y%%m%%d%%H%%M%%S)-${RANDOM:-0}-s3-config.yaml\n",
		coordExecScratchDir)
	// Stage the rendered plugin config inside the coordinator pod (base64 piped
	// so no quoting/here-doc hazard crosses the exec boundary). The remote shell
	// reads the target path from its own argv ($0) to keep the Job pod's value.
	fmt.Fprintf(&b,
		"base64 < %[1]s | \"${KUBECTL}\" exec -i \"${COORD_POD}\" -- "+
			"bash -c 'base64 -d > \"$0\"' \"${COORD_CFG}\"\n",
		s3RenderedConfigPath)
	// Materialize the (readable) inner tool script in the Job pod via a QUOTED
	// heredoc (delimiter quoted => NO expansion here; ${COORD_CFG}/$1.. expand
	// inside the coordinator at run time), keeping the gpbackup/gprestore flags
	// visible in args[0] for the e2e Job-arg assertions.
	b.WriteString("INNER_TOOL=$(cat <<'_CBK_INNER_EOF_'\n")
	b.WriteString(innerTool.String())
	b.WriteString("_CBK_INNER_EOF_\n)\n")
	// base64-encode THIS pod's live connection values (env-safe for any password)
	// and pass them as POSITIONAL args to the remote bash, which runs the inner
	// tool script (itself piped on stdin as base64 so no quoting/expansion hazard
	// crosses the exec boundary).
	b.WriteString("CFG_B64=$(printf '%s' \"${COORD_CFG}\" | base64 | tr -d '\\n')\n")
	b.WriteString("HOST_B64=$(printf '%s' \"${PGHOST:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("PORT_B64=$(printf '%s' \"${PGPORT:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("USER_B64=$(printf '%s' \"${PGUSER:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("DB_B64=$(printf '%s' \"${PGDATABASE:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("PASS_B64=$(printf '%s' \"${PGPASSWORD:-}\" | base64 | tr -d '\\n')\n")
	// The remote bootstrap decodes the inner tool script from stdin and runs it
	// with `bash -c <inner> _ <cfg> <host> <port> <user> <db> <pass>` so the
	// base64 values land as $1..$6 inside innerTool.
	remoteExec := "printf '%s' \"${INNER_TOOL}\" | base64 | " +
		"\"${KUBECTL}\" exec -i \"${COORD_POD}\" -- bash -c '" +
		"INNER=$(base64 -d); " +
		"bash -c \"${INNER}\" _ \"$0\" \"$1\" \"$2\" \"$3\" \"$4\" \"$5\"' " +
		"\"${CFG_B64}\" \"${HOST_B64}\" \"${PORT_B64}\" " +
		"\"${USER_B64}\" \"${DB_B64}\" \"${PASS_B64}\""
	// Best-effort cleanup of the staged config inside the coordinator pod.
	cleanup := "\"${KUBECTL}\" exec -i \"${COORD_POD}\" -- " +
		"bash -c 'rm -f \"$0\"' \"${COORD_CFG}\" 2>/dev/null || true\n"
	if statsPartialTolerated(tool, args) {
		// gprestore with statistics restore requested: capture the remote exit
		// code (kubectl exec propagates it) so a statistics-only failure
		// (exit 2) is downgraded to success-with-warning in the Job pod, while
		// any other failure still fails the Job. Cleanup always runs.
		b.WriteString("rc=0\n")
		b.WriteString(remoteExec + " || rc=$?\n")
		b.WriteString(cleanup)
		b.WriteString(gprestoreStatsExitGuard)
		b.WriteString("exit \"${rc}\"\n")
		return b.String()
	}
	b.WriteString(remoteExec + "\n")
	b.WriteString(cleanup)
	return b.String()
}

// dropLeadingPluginConfig returns args with a leading
// "--plugin-config <path>" pair removed (the S3 destination's leading pair from
// backupDestinationArgs). The coordinator-exec run substitutes the
// coordinator-side plugin config, so the Job-pod path must not be re-emitted.
func dropLeadingPluginConfig(args []string) []string {
	if len(args) >= 2 && args[0] == pluginConfigFlag {
		return args[2:]
	}
	return args
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
	// Record the effective backup type so the controller can derive status from
	// the spawned Job's label (spec-driven for the CronJob path; nil opts).
	labels[util.LabelBackupType] = effectiveBackupType(cluster, nil)
	// The CRD declares no per-schedule database list, so scheduled backups
	// target the coordinator maintenance database (postgres): gpbackup
	// hard-requires --dbname, and rendering the CronJob without it produced
	// Jobs that always failed at runtime (`required flag(s) "dbname" not set`).
	jobOpts := withDefaultBackupDatabase(nil)
	args, err := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup, jobOpts)
	if err != nil {
		// Defense in depth: unreachable — withDefaultBackupDatabase always
		// resolves a database — but no CronJob is safer than a broken one.
		return nil
	}
	podSpec := b.buildBackupPodSpec(cluster, backupContainerName, "gpbackup", args)
	// CBDB_DATABASE mirrors the rendered --dbname (spec 11: the env stays
	// informational/inspectable and must match the CLI args).
	applyBackupGpbackupEnv(&podSpec, firstDatabase(jobOpts.Databases), cluster.Spec.Backup.Gpbackup)
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

// BuildBackupJob builds an on-demand gpbackup Job. When opts carries no
// database the backup targets the coordinator maintenance database (see
// withDefaultBackupDatabase) so the rendered gpbackup command is always valid;
// the REST API layer additionally rejects database-less requests with 400, so
// user-facing requests are always explicit.
func (b *DefaultBuilder) BuildBackupJob(
	cluster *cbv1alpha1.CloudberryCluster,
	opts *BackupJobOptions,
) *batchv1.Job {
	opts = withDefaultBackupDatabase(opts)
	labels := backupLabels(cluster.Name, util.BackupOperationBackup)
	// Record the effective backup type (per-request Type/Gpbackup override or the
	// cluster spec) so the controller derives status from THIS Job's label.
	labels[util.LabelBackupType] = effectiveBackupType(cluster, opts)

	gpOpts := opts.gpbackupOrSpec(cluster)
	args, err := buildGpbackupArgs(cluster, gpOpts, opts)
	if err != nil {
		// Defense in depth: unreachable — withDefaultBackupDatabase always
		// resolves a database — but no Job is safer than a broken one.
		return nil
	}
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
	args := buildGprestoreArgs(cluster, grOpts, opts)
	podSpec := b.buildBackupPodSpec(cluster, restoreContainerName, "gprestore", args)
	// The restore Job carries the same informational gpbackup env (spec 11) so
	// the container env is inspectable; compression/jobs come from the cluster's
	// gpbackupOptions and the database from the restore request.
	var gpOpts *cbv1alpha1.GpbackupOptions
	if cluster.Spec.Backup != nil {
		gpOpts = cluster.Spec.Backup.Gpbackup
	}
	applyBackupGpbackupEnv(&podSpec, firstDatabase(opts.Databases), gpOpts)
	// For a cross-cluster migration the backup lives under the SOURCE cluster's
	// S3 folder; point this target restore Job at that folder so gprestore finds
	// it (spec 11 §Cross-Cluster Migration: "Both reference the same S3
	// bucket/folder"). No-op for ordinary restores where the override is empty.
	applyS3FolderOverride(&podSpec, opts.S3FolderOverride)

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
	// ExpectedRowCounts maps a fully-qualified table (schema.table) to the row
	// count recorded for it in the gpbackup history metadata of the restored
	// timestamp. When non-empty the validation script compares the actual
	// restored per-table count against the expected count and FAILS (exit 1) on
	// any mismatch (the headline row-count-vs-history check). When empty the
	// script falls back to a best-effort total-table probe that never fails (no
	// expected data to compare).
	ExpectedRowCounts map[string]int64
	// RunAnalyze, when true, makes the validation script run a database-wide
	// ANALYZE to refresh planner statistics before the row-count compare. It is
	// driven from the cluster's gprestore run-analyze intent (or the optional
	// validation config) so post-restore planner stats are confirmed fresh.
	RunAnalyze bool
	// S3FolderOverride, when non-empty, overrides the S3 plugin config "folder:"
	// (the S3_FOLDER env) for this validation Job, mirroring the restore Job. For
	// a cross-cluster migration (spec 11 §Cross-Cluster Migration) the validation
	// Job runs on the TARGET cluster but inspects the backup written under the
	// SOURCE folder, so its S3 env must point at the source folder to stay
	// consistent with the restore it validates.
	S3FolderOverride string
}

// BuildPostRestoreValidationJob builds a validation Job that runs after a restore
// completes (spec 11 §Post-Restore Validation). It optionally refreshes planner
// stats (ANALYZE when RunAnalyze is set), compares actual restored per-table row
// counts against the expected counts captured from the gpbackup history metadata
// (failing on any mismatch when ExpectedRowCounts is non-empty, or a best-effort
// total-table probe otherwise), runs an invalid-index scan (must-pass) and a
// configurable health-check query.
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
	// Mirror the restore Job's S3 folder for a cross-cluster migration so this
	// validation Job's S3 env points at the SOURCE folder it inspects. No-op when
	// the override is empty (ordinary single-cluster validation).
	applyS3FolderOverride(&podSpec, opts.S3FolderOverride)

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

// applyS3FolderOverride overrides the S3_FOLDER env (and thus the rendered S3
// plugin config "folder:") on every container and init container of the pod
// spec. It only acts when an override is set AND the container already carries an
// S3_FOLDER env (i.e. the cluster uses an S3 destination); it does not introduce
// S3 env onto a non-S3 (local) pod. Used by the migration restore/validation
// path so the target Job reads the backup from the source cluster's folder.
func applyS3FolderOverride(podSpec *corev1.PodSpec, folder string) {
	if folder == "" {
		return
	}
	overrideContainerS3Folder := func(containers []corev1.Container) {
		for i := range containers {
			for j := range containers[i].Env {
				if containers[i].Env[j].Name == "S3_FOLDER" {
					containers[i].Env[j].Value = folder
				}
			}
		}
	}
	overrideContainerS3Folder(podSpec.Containers)
	overrideContainerS3Folder(podSpec.InitContainers)
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
// validation Job. The ordering is: env preamble -> optional ANALYZE (when
// RunAnalyze) -> row-count compare -> invalid-index scan -> health-check ->
// "passed". The invalid-index scan and a non-empty row-count compare are
// must-pass (exit 1 on failure); the health-check query and the empty-map
// best-effort total probe never fail so transient/absent data does not break
// validation. psql connects via the Job's buildBackupEnv PGHOST and the
// PGDATABASE set from opts.Database.
func postRestoreValidationScript(opts *ValidationJobOptions) string {
	healthQuery := opts.HealthCheckQuery
	if healthQuery == "" {
		healthQuery = defaultHealthCheckQuery
	}

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)

	writeAnalyzeStep(&b, opts.RunAnalyze)
	writeRowCountStep(&b, opts.ExpectedRowCounts)
	writeInvalidIndexStep(&b)

	fmt.Fprintf(&b, "echo %s\n", shellQuote(validateMarkerPrefix+"health-check query"))
	fmt.Fprintf(&b, "psql -tA -c %s\n", shellQuote(healthQuery))
	fmt.Fprintf(&b, "echo %s\n", shellQuote(validateMarkerPrefix+"passed"))
	return b.String()
}

// writeAnalyzeStep appends the optional GAP-B ANALYZE step. When runAnalyze is
// set it runs a database-wide ANALYZE to refresh planner statistics and emits the
// ANALYZE_OK marker. ANALYZE failure must not pass silently: the explicit
// `set -euo pipefail` makes a failing psql abort the Job (the user explicitly
// asked for fresh stats), and stderr carries the psql error.
func writeAnalyzeStep(b *strings.Builder, runAnalyze bool) {
	if !runAnalyze {
		return
	}
	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"run-analyze (refreshing planner stats)"))
	b.WriteString("psql -c \"ANALYZE\"\n")
	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"ANALYZE_OK"))
}

// writeInvalidIndexStep appends the must-pass invalid-index scan. It is kept
// AS-IS (relkind='i' AND NOT indisvalid -> exit 1) so a restored database with
// any invalid index fails validation.
func writeInvalidIndexStep(b *strings.Builder) {
	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"scanning for invalid indexes"))
	b.WriteString("invalid=$(psql -tA -c \"SELECT count(*) FROM pg_catalog.pg_class c " +
		"JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid " +
		"WHERE c.relkind='i' AND NOT i.indisvalid\")\n")
	b.WriteString("if [ \"${invalid:-0}\" -gt 0 ]; then " +
		"echo \"" + validateMarkerPrefix + "${invalid} invalid index(es)\" >&2; exit 1; fi\n")
}

// writeRowCountStep appends the GAP-A row-count step. When expected is non-empty
// it renders a deterministic per-table compare loop: for each table it runs
// `psql -tA -c "SELECT count(*) FROM <table>"`, compares the actual count to the
// expected one, emits a parsable ROW_COUNT_MATCH/ROW_COUNT_MISMATCH marker and,
// if ANY table mismatched, exits 1 (the headline failing check; this also catches
// the data-only-into-prepopulated case where actual > expected). When expected is
// empty it keeps the legacy best-effort total-table probe which never fails (no
// expected data to compare).
func writeRowCountStep(b *strings.Builder, expected map[string]int64) {
	if len(expected) == 0 {
		fmt.Fprintf(b, "echo %s\n",
			shellQuote(validateMarkerPrefix+"row-count probe (best-effort, no expected counts)"))
		b.WriteString("psql -tA -c \"SELECT count(*) FROM pg_class WHERE relkind='r'\" || " +
			"echo \"" + validateMarkerPrefix + "ROW_COUNT_PROBE_SKIPPED\"\n")
		return
	}

	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"row-count compare vs gpbackup history"))
	b.WriteString("rowcount_mismatch=0\n")

	// Iterate the expected map in a stable (sorted) order so the rendered script
	// is deterministic for the same input (testable, reproducible Jobs).
	tables := make([]string, 0, len(expected))
	for table := range expected {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		writeRowCountTableCompare(b, table, expected[table])
	}

	b.WriteString("if [ \"${rowcount_mismatch}\" -gt 0 ]; then " +
		"echo \"" + validateMarkerPrefix + "${rowcount_mismatch} table(s) with row-count mismatch\" >&2; " +
		"exit 1; fi\n")
}

// writeRowCountTableCompare appends the compare block for a single expected
// table. The table identifier and the expected count are shell-quoted, so there
// is no injection surface, and the actual count is read with psql -tA. On
// mismatch it prints ROW_COUNT_MISMATCH (to stderr) and bumps rowcount_mismatch;
// on match it prints ROW_COUNT_MATCH.
func writeRowCountTableCompare(b *strings.Builder, table string, expectedCount int64) {
	expectedStr := strconv.FormatInt(expectedCount, 10)
	// The SELECT references the table with its schema-qualified identifier. The
	// identifier is single-quoted for the shell (psql receives it inside the
	// double-quoted -c string); operator-supplied table names are not
	// user-controlled free text (they come from gpbackup history keys).
	query := fmt.Sprintf("SELECT count(*) FROM %s", table)
	fmt.Fprintf(b, "actual=$(psql -tA -c %s)\n", shellQuote(query))
	fmt.Fprintf(b, "expected=%s\n", shellQuote(expectedStr))
	fmt.Fprintf(b, "table=%s\n", shellQuote(table))
	b.WriteString("if [ \"${actual:-}\" != \"${expected}\" ]; then\n")
	b.WriteString("  echo \"" + validateMarkerPrefix +
		"ROW_COUNT_MISMATCH table=${table} expected=${expected} actual=${actual:-0}\" >&2\n")
	b.WriteString("  rowcount_mismatch=$((rowcount_mismatch + 1))\n")
	b.WriteString("else\n")
	b.WriteString("  echo \"" + validateMarkerPrefix +
		"ROW_COUNT_MATCH table=${table} count=${actual}\"\n")
	b.WriteString("fi\n")
}

// BuildRetentionCleanupJob builds a gpbackman retention cleanup Job enforcing the
// configured retention policy (count-based via backup-info/backup-delete and
// time-based via backup-clean). The cleanup container runs a self-contained POSIX
// sh script (see buildGpbackmanRetentionScript) and reports the number of deleted
// backups both on stdout (the "RETENTION_DELETED=<n>" marker) and via the
// container terminationMessagePath, so the controller can patch the
// avsoft.io/backup-retention-deleted annotation that drives the retention metric.
func (b *DefaultBuilder) BuildRetentionCleanupJob(
	cluster *cbv1alpha1.CloudberryCluster,
	timestamp string,
) *batchv1.Job {
	labels := backupLabels(cluster.Name, util.BackupOperationCleanup)

	// The cleanup container runs the retention script directly (like the
	// post-restore validation Job) rather than a single tool invocation, so it
	// can enumerate, delete the oldest-excess backups and emit the deletion count.
	podSpec := b.buildBackupPodSpec(cluster, cleanupContainerName, "", nil)
	container := &podSpec.Containers[0]
	container.Args = []string{buildGpbackmanRetentionScript(cluster)}
	// FallbackToLogsOnError makes the deletion count recoverable from the pod log
	// (the "RETENTION_DELETED=<n>" marker) if the /dev/termination-log write is
	// missed; the script writes the count to the termination message directly.
	container.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError

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

// retentionDeletedMarker is the stdout/termination-message prefix the cleanup
// script emits with the total number of deleted backups; the controller parses it.
const retentionDeletedMarker = "RETENTION_DELETED="

// parseMaxAgeDays converts a retention MaxAge expression into a whole number of
// days for gpbackman's backup-clean --older-than-days. It accepts a "Nd" day
// form ("30d" -> 30), a "Nw" week form ("4w" -> 28), a Go duration ("720h" ->
// 30, "25h" -> 1), and a bare integer number of days ("30" -> 30). Sub-day
// durations truncate toward zero (a positive duration under 24h yields 1 day so
// it still enforces some retention; exactly zero yields 0). It returns (0,
// false) when the value is empty or unparseable so the caller can skip the
// time-based step.
func parseMaxAgeDays(maxAge string) (int, bool) {
	s := strings.TrimSpace(maxAge)
	if s == "" {
		return 0, false
	}
	// Bare integer number of days.
	if n, err := strconv.Atoi(s); err == nil {
		if n < 0 {
			return 0, false
		}
		return n, true
	}
	// "Nd" (days) and "Nw" (weeks) shorthands are not Go durations.
	if days, ok := parseDaysSuffix(s); ok {
		return days, true
	}
	// Go duration (e.g. "720h", "25h", "43200m").
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		days := int(d / (24 * time.Hour))
		if days == 0 {
			// A positive sub-day duration still implies retention; round up to 1
			// so backup-clean removes anything older than a day.
			days = 1
		}
		return days, true
	}
	return 0, false
}

// parseDaysSuffix parses the "Nd" (days) and "Nw" (weeks) retention shorthands
// that are not valid Go durations, returning the equivalent whole days.
func parseDaysSuffix(s string) (int, bool) {
	if len(s) < 2 {
		return 0, false
	}
	unit := s[len(s)-1]
	num, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || num < 0 {
		return 0, false
	}
	switch unit {
	case 'd', 'D':
		return num, true
	case 'w', 'W':
		return num * 7, true
	default:
		return 0, false
	}
}

// buildGpbackmanRetentionScript renders the POSIX sh script that enforces all
// retention policies for the cleanup Job using the real gpbackman CLI:
//   - count-based full retention: backup-info --type full lists the Success
//     full timestamps (newest first); the oldest excess beyond FullCount are
//     removed with backup-delete --timestamp <ts> --cascade.
//   - count-based incremental retention: same with --type incremental, guarding
//     against rows already removed by a cascaded full delete (re-checks the
//     current backup-info output before each delete).
//   - time-based retention (maxAge): backup-clean --older-than-days N --cascade.
//
// All interpolated values are single-quoted (shellQuote) so there is no
// injection surface, and the script is bash-3.2 / POSIX-sh safe (no associative
// arrays). It maintains a DELETED counter, prints the "RETENTION_DELETED=<n>"
// marker to stdout and writes <n> to the container terminationMessagePath so the
// operator can recover the count and patch the retention annotation.
func buildGpbackmanRetentionScript(cluster *cbv1alpha1.CloudberryCluster) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)
	b.WriteString(sshSetupPreamble)
	b.WriteString(gpbackupPluginPathPreamble)
	// DEST_FLAGS carries the destination selector passed to every gpbackman
	// command: for a LOCAL destination there is no S3 ConfigMap mounted at
	// /etc/gpbackup, so we use `--backup-dir <path>` and skip the S3 render
	// (reading the missing template under `set -euo pipefail` would abort the
	// Job). For S3 we render the plugin config (same preamble as
	// renderToolScript) and use `--plugin-config <rendered>`.
	if isLocalDestination(cluster) {
		fmt.Fprintf(&b, "DEST_FLAGS=%s\n",
			shellQuote(backupDirFlag+" "+localBackupDir(cluster)))
	} else {
		fmt.Fprintf(&b,
			"if command -v envsubst >/dev/null 2>&1; then "+
				"envsubst < %[1]s/%[2]s > %[3]s; "+
				"else eval \"cat <<_ENVSUBST_EOF_\n$(cat %[1]s/%[2]s)\n_ENVSUBST_EOF_\" > %[3]s; fi\n",
			s3ConfigMountPath, s3ConfigTemplateKey, s3RenderedConfigPath)
		fmt.Fprintf(&b, "DEST_FLAGS=%s\n",
			shellQuote(pluginConfigFlag+" "+s3RenderedConfigPath))
	}

	fmt.Fprintf(&b, "HISTORY_DB=%s\n", shellQuote(gpbackupHistoryDBPath))
	b.WriteString("DELETED=0\n")
	b.WriteString(retentionScriptHelpers())

	retention := retentionPolicy(cluster)
	if retention.FullCount > 0 {
		b.WriteString(retentionCountBlock("full", retention.FullCount))
	}
	if retention.IncrementalCount > 0 {
		b.WriteString(retentionCountBlock("incremental", retention.IncrementalCount))
	}
	if days, ok := parseMaxAgeDays(retention.MaxAge); ok {
		b.WriteString(retentionMaxAgeBlock(days))
	}

	// Report the total deletions on stdout and via the termination message.
	fmt.Fprintf(&b, "echo \"%s${DELETED}\"\n", retentionDeletedMarker)
	b.WriteString("printf '%s' \"${DELETED}\" > /dev/termination-log 2>/dev/null || true\n")
	b.WriteString("exit 0\n")
	return b.String()
}

// retentionPolicy returns the cluster's retention policy, or a zero policy when
// no backup is configured.
func retentionPolicy(cluster *cbv1alpha1.CloudberryCluster) cbv1alpha1.BackupRetention {
	if cluster.Spec.Backup == nil {
		return cbv1alpha1.BackupRetention{}
	}
	return cluster.Spec.Backup.Retention
}

// retentionScriptHelpers emits shell helper functions shared by the retention
// blocks: _gpbackman_timestamps lists Success backup timestamps (newest first)
// for a given --type, and _gpbackman_delete deletes one timestamp (cascade) and
// increments DELETED on success.
//
// _gpbackman_timestamps parses gpbackman backup-info output: it keeps only rows
// whose first field is a 14-digit timestamp AND whose row reports STATUS=Success
// (gpbackman renders a "Success" status column for completed backups; rows that
// are deleted/failed are skipped). The result is emitted newest-first because
// gpbackman backup-info already orders newest first; the helper preserves order.
func retentionScriptHelpers() string {
	return "_gpbackman_timestamps() {\n" +
		"  gpbackman backup-info --type \"$1\" --history-db \"${HISTORY_DB}\" 2>/dev/null | " +
		"awk '/[Ss]uccess/ { for (i = 1; i <= NF; i++) " +
		"if ($i ~ /^[0-9]{14}$/) { print $i; break } }' || true\n" +
		"}\n" +
		"_gpbackman_delete() {\n" +
		"  if gpbackman backup-delete --timestamp \"$1\" --cascade " +
		"${DEST_FLAGS} --history-db \"${HISTORY_DB}\"; then\n" +
		"    DELETED=$((DELETED + 1))\n" +
		"  fi\n" +
		"}\n"
}

// retentionCountBlock emits a shell block that enforces count-based retention for
// the given backup type. It re-enumerates the current Success timestamps before
// each delete so cascaded removals (a deleted full taking its incrementals with
// it) do not cause under/over-deletion: the loop deletes the oldest backup while
// the count exceeds keep, re-listing after every delete and stopping once the
// retained set is within the limit.
func retentionCountBlock(backupType string, keep int32) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# count-based retention for %s backups (keep newest %d)\n",
		backupType, keep)
	fmt.Fprintf(&b, "KEEP=%d\n", keep)
	b.WriteString("while :; do\n")
	fmt.Fprintf(&b, "  TS_LIST=\"$(_gpbackman_timestamps %s)\"\n", shellQuote(backupType))
	b.WriteString("  COUNT=$(printf '%s\\n' \"${TS_LIST}\" | grep -c '[0-9]' || true)\n")
	b.WriteString("  if [ \"${COUNT:-0}\" -le \"${KEEP}\" ]; then break; fi\n")
	// backup-info is newest-first, so the oldest Success timestamp is the last line.
	b.WriteString("  OLDEST=$(printf '%s\\n' \"${TS_LIST}\" | grep '[0-9]' | tail -n 1)\n")
	b.WriteString("  if [ -z \"${OLDEST}\" ]; then break; fi\n")
	b.WriteString("  _gpbackman_delete \"${OLDEST}\"\n")
	b.WriteString("done\n")
	return b.String()
}

// retentionMaxAgeBlock emits the time-based retention step: gpbackman backup-clean
// --older-than-days <days> removes every backup older than the window (cascade so
// dependent incrementals go with their full). The deleted count for this step is
// approximated by re-counting the total Success backups before and after so the
// reported DELETED stays accurate even though backup-clean removes many at once.
func retentionMaxAgeBlock(days int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# time-based retention (older than %d day(s))\n", days)
	b.WriteString("BEFORE_CLEAN=$(( $(_gpbackman_timestamps full | grep -c '[0-9]' || true) + " +
		"$(_gpbackman_timestamps incremental | grep -c '[0-9]' || true) ))\n")
	fmt.Fprintf(&b,
		"gpbackman backup-clean --older-than-days %d ${DEST_FLAGS} "+
			"--cascade --history-db \"${HISTORY_DB}\" || true\n", days)
	b.WriteString("AFTER_CLEAN=$(( $(_gpbackman_timestamps full | grep -c '[0-9]' || true) + " +
		"$(_gpbackman_timestamps incremental | grep -c '[0-9]' || true) ))\n")
	b.WriteString("if [ \"${BEFORE_CLEAN}\" -gt \"${AFTER_CLEAN}\" ]; then " +
		"DELETED=$((DELETED + BEFORE_CLEAN - AFTER_CLEAN)); fi\n")
	return b.String()
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
		Args:         []string{renderToolScript(cluster, tool, args)},
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

// preBackupDestinationCheck returns the destination-readiness portion of the
// pre-backup script. For local destinations it verifies free disk space on the
// backup mount (fail-closed below minBackupDiskFreeKB). For S3 it performs a
// real, fail-closed SigV4-signed HEAD request against the bucket so wrong
// credentials, a missing bucket or an unreachable endpoint block the backup.
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
		return s3ReachabilityCheckScript()
	default:
		return ""
	}
}

// s3ReachabilityCheckScript builds a self-contained POSIX-sh snippet that
// performs a fail-closed S3 bucket reachability check. It issues a SigV4-signed
// HTTP HEAD request (path-style, e.g. MinIO) against ${S3_ENDPOINT}/${S3_BUCKET}
// using the AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY already injected into the
// init container, mirroring the openssl-HMAC SigV4 signing used in
// test/docker-compose/scripts/setup-minio.sh. A 2xx/3xx response means the
// bucket is reachable; any other response (403/404/000/connection refused)
// causes "exit 1" so the backup is blocked. The request is bounded by
// curl --max-time so an unreachable endpoint fails closed instead of hanging.
//
// The snippet is built from constant pieces only (no untrusted interpolation):
// all S3 values are read at runtime from the container env vars, so there is no
// shell-injection surface. The few %-format verbs below substitute the
// signing region default and the curl timeout constants.
func s3ReachabilityCheckScript() string {
	return fmt.Sprintf(`echo 'pre-backup-check: verifying s3 bucket reachability'
_s3_region="${S3_REGION:-%[1]s}"
_s3_service="s3"
# Strip a trailing slash from the endpoint so ${endpoint}/${bucket} is clean.
_s3_endpoint="${S3_ENDPOINT%%/}"
# Derive host[:port] from the endpoint for the canonical Host header.
_s3_hostport="${_s3_endpoint#*://}"
_s3_hostport="${_s3_hostport%%/*}"
# openssl-based SigV4 helpers (mirror setup-minio.sh).
_hmac_hex() { printf '%%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" | sed 's/^.*= //'; }
_sha256_hex() { printf '%%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }
_amzdate="$(date -u +%%Y%%m%%dT%%H%%M%%SZ)"
_datestamp="$(date -u +%%Y%%m%%d)"
_payload_hash="$(_sha256_hex '')"
# Canonical request for HEAD /<bucket> (path-style, empty query, empty body).
_canonical_headers="host:${_s3_hostport}
x-amz-content-sha256:${_payload_hash}
x-amz-date:${_amzdate}
"
_signed_headers="host;x-amz-content-sha256;x-amz-date"
_canonical_request="$(printf '%%s\n%%s\n%%s\n%%s\n%%s\n%%s' \
  "HEAD" "/${S3_BUCKET}" "" "${_canonical_headers}" "${_signed_headers}" "${_payload_hash}")"
_scope="${_datestamp}/${_s3_region}/${_s3_service}/aws4_request"
_string_to_sign="$(printf '%%s\n%%s\n%%s\n%%s' \
  "AWS4-HMAC-SHA256" "${_amzdate}" "${_scope}" "$(_sha256_hex "${_canonical_request}")")"
# Derive the SigV4 signing key (HMAC chain seeded with "AWS4"+secret).
_k_secret_hex="$(printf 'AWS4%%s' "${AWS_SECRET_ACCESS_KEY}" | od -An -tx1 | tr -d ' \n')"
_k_date="$(_hmac_hex "${_k_secret_hex}" "${_datestamp}")"
_k_region="$(_hmac_hex "${_k_date}" "${_s3_region}")"
_k_service="$(_hmac_hex "${_k_region}" "${_s3_service}")"
_k_signing="$(_hmac_hex "${_k_service}" "aws4_request")"
_signature="$(_hmac_hex "${_k_signing}" "${_string_to_sign}")"
_authz="AWS4-HMAC-SHA256 Credential=${AWS_ACCESS_KEY_ID}/${_scope}, "
_authz="${_authz}SignedHeaders=${_signed_headers}, Signature=${_signature}"
# Fail closed: any curl error yields code 000 (-> exit 1 below).
_code="$(curl -s -o /dev/null -w '%%{http_code}' --max-time %[2]d \
  -I -X HEAD "${_s3_endpoint}/${S3_BUCKET}" \
  -H "Host: ${_s3_hostport}" \
  -H "x-amz-content-sha256: ${_payload_hash}" \
  -H "x-amz-date: ${_amzdate}" \
  -H "Authorization: ${_authz}" 2>/dev/null || echo 000)"
case "${_code}" in
  2??|3??) echo "pre-backup-check: s3 bucket reachable (http ${_code})" ;;
  *) echo "pre-backup-check: s3 bucket unreachable (http ${_code})" >&2; exit 1 ;;
esac
`, defaultS3SigningRegion, s3ReachabilityMaxTimeSeconds)
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
			Name: envPGPassword,
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
	if len(tmpl.ImagePullSecrets) > 0 {
		addImagePullSecrets(podSpec, tmpl.ImagePullSecrets)
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
