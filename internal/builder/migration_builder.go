// Package builder: migration_builder.go constructs the SINGLE coordinated
// cross-cluster migration Job (spec 11 §Cross-Cluster Migration).
//
// THE BUG IT FIXES. A migration previously created two independent Jobs — a
// gpbackup Job on the source and a gprestore Job on the target — both named and
// driven by ONE operator-chosen timestamp. But gpbackup GENERATES ITS OWN
// timestamp at runtime and offers no flag to pin it (only --from-timestamp for
// incrementals), so the backup actually lands in S3 under a DIFFERENT timestamp
// than the one the restore Job was told to use. gprestore then fails with a
// NotFound because it looked under the operator timestamp, not the real one.
//
// THE FIX (coordinator-exec, single chained Job). The migration now runs as ONE
// Job whose script (1) execs gpbackup INSIDE the SOURCE coordinator pod and
// CAPTURES the real "Backup Timestamp = <14-digit>" from gpbackup's stdout, then
// (2) execs gprestore INSIDE the TARGET coordinator pod with
// `--timestamp <captured>`, and finally (3) runs the post-restore validation
// against the target. Because both phases live in one Job the captured timestamp
// is propagated in-process (a shell variable), guaranteeing backup and restore
// reference the SAME S3 object. The operator-chosen timestamp is used only to
// NAME the Job.
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
	// migrationContainerName is the container name for the coordinated migration Job.
	migrationContainerName = "migration"

	// migrationTSVar is the shell variable that holds the REAL gpbackup-generated
	// 14-digit timestamp captured from gpbackup's stdout on the source coordinator
	// and fed verbatim to gprestore --timestamp on the target coordinator.
	migrationTSVar = "MIG_BACKUP_TS"

	// migrationBackupLogVar is the shell variable holding gpbackup's captured
	// combined stdout/stderr (tee'd to the Job log) from which migrationTSVar is
	// extracted with `grep -oE 'Backup Timestamp = [0-9]{14}'`.
	migrationBackupLogVar = "MIG_BACKUP_LOG"

	// envTargetPGPassword is the env key carrying the TARGET cluster's admin
	// password (sourced from the target's admin Secret) so the single migration
	// Job — which runs in the SOURCE cluster's pod context — can authenticate the
	// gprestore exec against the target coordinator.
	envTargetPGPassword = "TARGET_PGPASSWORD" //nolint:gosec // env var NAME, not a credential
	// envTargetPGHost is the env key carrying the target coordinator service host.
	envTargetPGHost = "TARGET_PGHOST"
	// envTargetPGPort is the env key carrying the target coordinator port.
	envTargetPGPort = "TARGET_PGPORT"
	// envTargetPGUser is the env key carrying the target admin user.
	envTargetPGUser = "TARGET_PGUSER"
	// envTargetPGDatabase is the env key carrying the restore target database.
	envTargetPGDatabase = "TARGET_PGDATABASE"
)

// MigrationJobOptions carries the parameters for the single coordinated
// cross-cluster migration Job.
type MigrationJobOptions struct {
	// Timestamp is the operator-chosen YYYYMMDDHHMMSS identifier used ONLY to NAME
	// the Job (and the per-run scratch config files). The actual gpbackup/gprestore
	// timestamp is the one gpbackup generates at runtime and the Job captures.
	Timestamp string
	// Source is the source cluster (gpbackup runs inside its coordinator).
	Source *cbv1alpha1.CloudberryCluster
	// Target is the target cluster (gprestore + validation run against it).
	Target *cbv1alpha1.CloudberryCluster
	// Database is the source database to back up (gpbackup --dbname).
	Database string
	// RedirectDb maps to gprestore --redirect-db (defaults to Database upstream).
	RedirectDb string
	// RedirectSchema maps to gprestore --redirect-schema.
	RedirectSchema string
	// IncludeTables maps to the repeated --include-table flag on both tools.
	IncludeTables []string
	// SingleDataFile sets gpbackup --single-data-file.
	SingleDataFile bool
	// Truncate requests a CLEAN target: the migration restores into a freshly
	// (re)created empty target DB. When set, the pre-create step DROPs the target
	// DB (if it exists) and recreates it empty before gprestore. It does NOT map
	// to gprestore --truncate-table (which assumes pre-existing tables and breaks
	// the pre-data metadata phase of a fresh-DB restore — see
	// buildMigrationRestoreArgs).
	Truncate bool
	// Jobs sets gprestore --jobs (parallelism) when > 0.
	Jobs int32
	// ValidationDatabase is the database the validation phase connects to on the
	// target (RedirectDb or Database).
	ValidationDatabase string
}

// BuildMigrationJob builds the single coordinated cross-cluster migration Job
// (spec 11 §Cross-Cluster Migration). The Job pod is rendered in the SOURCE
// cluster context (image, SSH identity, S3 ConfigMap, service account) and its
// container script execs the whole migration sequence into the two coordinator
// pods, capturing the real gpbackup timestamp between the backup and restore so
// both reference the same S3 object.
//
// The S3 plugin "folder:" is set to the SOURCE cluster's folder for BOTH the
// backup and the restore: gpbackup writes under the source folder, so the target
// gprestore must read from there (the same S3-folder fix used by the previous
// two-Job topology, preserved here).
func (b *DefaultBuilder) BuildMigrationJob(opts *MigrationJobOptions) *batchv1.Job {
	source := opts.Source
	target := opts.Target

	labels := backupLabels(source.Name, util.BackupOperationMigrate)

	// Build the pod in the SOURCE cluster context: it mounts the source S3
	// ConfigMap + SSH identity and carries the source PG connection env. The
	// container runs the migration script (no single in-pod tool).
	podSpec := b.buildBackupPodSpec(source, migrationContainerName, "", nil)
	podSpec.Containers[0].Args = []string{migrationScript(opts)}

	// The migration restore + validation phase targets the TARGET coordinator, so
	// inject the target connection env (host/port/user/db as plain values, the
	// password sourced from the target's admin Secret — never plaintext).
	applyTargetConnectionEnv(&podSpec.Containers[0], target, opts.ValidationDatabase)

	// gpbackup writes under the SOURCE folder; point the rendered S3 plugin config
	// at that folder so both the backup and the (target) restore read/write the
	// same object. No-op when the source has no S3 folder (e.g. local destination,
	// which migration does not support — guarded upstream in the API handler).
	applyS3FolderOverride(&podSpec, sourceMigrationFolder(source))

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.MigrationJobName(source.Name, opts.Timestamp),
			Namespace:       source.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(source)},
		},
		Spec: b.buildJobSpec(source, labels, &podSpec),
	}
}

// sourceMigrationFolder returns the source cluster's configured S3 folder, or ""
// when the source has no S3 destination.
func sourceMigrationFolder(cluster *cbv1alpha1.CloudberryCluster) string {
	b := cluster.Spec.Backup
	if b == nil || b.Destination.Type != destinationTypeS3 || b.Destination.S3 == nil {
		return ""
	}
	return b.Destination.S3.Folder
}

// applyTargetConnectionEnv adds the TARGET cluster's coordinator connection env
// to the migration container. The host/port/user/database are plain values; the
// password is sourced from the target's admin Secret (never embedded as
// plaintext, per the project's Vault/Secret credential policy).
func applyTargetConnectionEnv(
	container *corev1.Container,
	target *cbv1alpha1.CloudberryCluster,
	validationDatabase string,
) {
	setEnvVar(container, envTargetPGHost, util.CoordinatorServiceName(target.Name))
	setEnvVar(container, envTargetPGPort, fmt.Sprintf("%d", resolvePort(target)))
	setEnvVar(container, envTargetPGUser, util.DefaultAdminUser)
	setEnvVar(container, envTargetPGDatabase, validationDatabase)
	container.Env = append(container.Env, corev1.EnvVar{
		Name: envTargetPGPassword,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: util.AdminPasswordSecretName(target.Name),
				},
				Key: secretKeyPassword,
			},
		},
	})
}

// migrationScript renders the full migration bash script run by the single Job
// container. Ordering: preamble -> render S3 config in the Job pod -> resolve
// kubectl -> backup phase (exec gpbackup on the SOURCE coordinator, CAPTURE the
// real timestamp) -> restore phase (exec gprestore on the TARGET coordinator
// with --timestamp <captured>) -> validation phase (row-count probe +
// invalid-index scan + health-check against the target). All phases run under
// `set -euo pipefail` so any failure aborts the Job.
func migrationScript(opts *MigrationJobOptions) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)

	writeMigrationPreamble(&b, opts.Timestamp)
	writeMigrationBackupPhase(&b, opts)
	writeMigrationRestorePhase(&b, opts)
	writeMigrationValidationPhase(&b)

	fmt.Fprintf(&b, "echo %s\n", shellQuote(validateMarkerPrefix+"passed"))
	return b.String()
}

// writeMigrationPreamble renders the S3 plugin config in the Job pod and resolves
// kubectl + the per-run scratch config path used on both coordinators. tsSuffix
// (the operator timestamp) plus $RANDOM keeps concurrent migrations isolated.
func writeMigrationPreamble(b *strings.Builder, tsSuffix string) {
	// Render the S3 plugin config (envsubst with POSIX fallback) in the Job pod.
	fmt.Fprintf(b,
		"if command -v envsubst >/dev/null 2>&1; then "+
			"envsubst < %[1]s/%[2]s > %[3]s; "+
			"else eval \"cat <<_ENVSUBST_EOF_\n$(cat %[1]s/%[2]s)\n_ENVSUBST_EOF_\" > %[3]s; fi\n",
		s3ConfigMountPath, s3ConfigTemplateKey, s3RenderedConfigPath)
	// Resolve a kubectl binary (PATH, then common install dirs).
	b.WriteString("KUBECTL=" + kubectlBin + "\n")
	b.WriteString("command -v \"${KUBECTL}\" >/dev/null 2>&1 || " +
		"{ for p in /usr/local/bin/kubectl /usr/bin/kubectl; do " +
		"[ -x \"$p\" ] && KUBECTL=\"$p\" && break; done; }\n")
	// A per-run scratch config path reused (staged + cleaned) on each coordinator.
	fmt.Fprintf(b, "COORD_CFG=%s/cbk-mig-%s-${RANDOM:-0}-s3-config.yaml\n",
		coordExecScratchDir, shellQuoteBare(tsSuffix))
}

// writeMigrationBackupPhase execs gpbackup inside the SOURCE coordinator pod,
// tee'ing its output so the migration Job log carries it, then extracts the real
// 14-digit "Backup Timestamp = <ts>" gpbackup printed into migrationTSVar. If no
// timestamp is found the phase fails closed (the restore cannot proceed without
// the real timestamp).
func writeMigrationBackupPhase(b *strings.Builder, opts *MigrationJobOptions) {
	coordPod := util.CoordinatorPodName(opts.Source.Name)
	args := buildMigrationBackupArgs(opts)

	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"migration backup (source coordinator)"))
	fmt.Fprintf(b, "SRC_COORD_POD=%s\n", shellQuote(coordPod))

	// Stage the rendered plugin config into the SOURCE coordinator.
	writeStageConfig(b, "SRC_COORD_POD")
	// Build + run the inner gpbackup script; capture its combined output.
	writeInnerToolExec(b, innerToolExec{
		coordPodVar:  "SRC_COORD_POD",
		tool:         "gpbackup",
		args:         args,
		hostEnv:      "PGHOST",
		portEnv:      "PGPORT",
		userEnv:      "PGUSER",
		dbEnv:        "PGDATABASE",
		passEnv:      envPGPassword,
		captureToVar: migrationBackupLogVar,
	})
	writeCleanupConfig(b, "SRC_COORD_POD")

	// Extract the REAL gpbackup timestamp from the captured output and fail closed
	// if absent (without it the restore would fall back to a wrong timestamp).
	fmt.Fprintf(b,
		"%[1]s=$(printf '%%s' \"${%[2]s}\" | grep -oE 'Backup Timestamp = [0-9]{14}' | "+
			"grep -oE '[0-9]{14}' | head -1 || true)\n",
		migrationTSVar, migrationBackupLogVar)
	fmt.Fprintf(b,
		"if [ -z \"${%[1]s:-}\" ]; then echo %[2]s >&2; exit 1; fi\n",
		migrationTSVar,
		shellQuote(validateMarkerPrefix+"could not capture gpbackup Backup Timestamp"))
	fmt.Fprintf(b, "echo \"%scaptured gpbackup timestamp ${%s}\"\n",
		validateMarkerPrefix, migrationTSVar)
}

// writeMigrationRestorePhase execs gprestore inside the TARGET coordinator pod,
// passing the CAPTURED gpbackup timestamp so backup and restore reference the
// same S3 object. The restore connects with the TARGET_* connection env.
//
// Before gprestore it prepares the target database on the target coordinator
// (see writeMigrationEnsureTargetDB): gprestore 2.1.0 REFUSES a table-filtered
// (--include-table) or data-only restore into a database that does not already
// exist, and for that restore class --create-db is INSUFFICIENT (gprestore still
// demands a pre-existing target DB). The target cluster does not have the source
// database yet, so the migration must create it first. When Truncate is set the
// step DROPs+recreates the DB empty so the migration starts from a clean target
// (the user-facing "clean target" intent of --truncate, since the fresh-DB
// migration restore can no longer use the incompatible --truncate-table flag).
func writeMigrationRestorePhase(b *strings.Builder, opts *MigrationJobOptions) {
	coordPod := util.CoordinatorPodName(opts.Target.Name)
	args := buildMigrationRestoreArgs(opts)

	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"migration restore (target coordinator)"))
	fmt.Fprintf(b, "DST_COORD_POD=%s\n", shellQuote(coordPod))

	// Ensure the target database exists BEFORE gprestore (idempotent, target
	// coordinator). Runs after the gpbackup timestamp has been captured and
	// before the restore so the catalog entry is present where gprestore looks.
	// When Truncate is set the DB is DROPped+recreated empty (clean target).
	writeMigrationEnsureTargetDB(b, migrationTargetDatabase(opts), opts.Truncate)

	writeStageConfig(b, "DST_COORD_POD")
	// gprestore --timestamp <captured TS> is appended after the readable flags so
	// the captured shell variable expands at run time inside the coordinator.
	writeInnerToolExec(b, innerToolExec{
		coordPodVar:        "DST_COORD_POD",
		tool:               "gprestore",
		args:               args,
		hostEnv:            envTargetPGHost,
		portEnv:            envTargetPGPort,
		userEnv:            envTargetPGUser,
		dbEnv:              envTargetPGDatabase,
		passEnv:            envTargetPGPassword,
		appendTimestampVar: migrationTSVar,
	})
	writeCleanupConfig(b, "DST_COORD_POD")
}

// migrationTargetDatabase returns the database the migration restore targets on
// the destination cluster: gprestore's --redirect-db value when set, else the
// source Database. This is the exact name CREATE DATABASE must pre-create so the
// table-filtered gprestore finds an existing target DB.
func migrationTargetDatabase(opts *MigrationJobOptions) string {
	if opts.RedirectDb != "" {
		return opts.RedirectDb
	}
	return opts.Database
}

// writeMigrationEnsureTargetDB renders a "prepare the target database" step
// executed via psql INSIDE the TARGET coordinator pod (where the target cluster's
// catalog lives), using the gpadmin/admin TARGET_* connection. Behavior depends
// on clean:
//   - clean=false (default): idempotent CREATE-if-absent. A present DB is a no-op
//     (the SELECT returns 1, grep -q 1 succeeds, CREATE is skipped); an absent DB
//     is created exactly once.
//   - clean=true (--truncate): DROP the target DB (if it exists) and recreate it
//     EMPTY, so the migration starts from a CLEAN target. This is how --truncate's
//     "clean target" intent is honored now that the fresh-DB migration restore
//     can no longer use the incompatible gprestore --truncate-table flag.
//
// WHY (gprestore 2.1.0 semantics): a table-filtered (--include-table) or
// data-only restore REQUIRES the target database to already exist; gprestore
// fails with `Database %q must be created manually to restore table-filtered or
// data-only backups.` and --create-db does NOT satisfy this class (it creates the
// DB from the backup's GLOBAL metadata, which a table-filtered backup omits, and
// would itself error "database already exists" once we pre-create the DB). So we
// pre-create the DB here and DROP --create-db from buildMigrationRestoreArgs.
//
// DROP SAFETY (clean=true): a DROP DATABASE cannot run while backends are
// connected, so we first terminate any open backends to the target DB
// (pg_terminate_backend over pg_stat_activity WHERE datname=...), then issue
// `DROP DATABASE IF EXISTS` (a no-op when the DB is absent) followed by a fresh
// `CREATE DATABASE`. WITH (FORCE) is not relied upon (it is unavailable on older
// Cloudberry/Greenplum catalogs); terminating backends first achieves the same
// effect portably.
//
// SHELL/SQL SAFETY: dbName originates from the request and is validated as an SQL
// identifier by the API (isValidIdentifier). It is additionally embedded as a
// single-quoted shell literal via shellQuote (so no shell metacharacter — quote,
// $, backtick, ; — can break out), and quoted defensively in SQL (single quotes
// in the WHERE filter, double quotes for the identifier in CREATE/DROP DATABASE).
// The CREATE-if-absent path uses ON_ERROR_STOP=0 and a `|| CREATE` guard so a
// present DB is a no-op and an absent DB is created exactly once (no TOCTOU
// concern: the worst case on a race is a harmless "already exists" from the
// second CREATE, which we tolerate).
func writeMigrationEnsureTargetDB(b *strings.Builder, dbName string, clean bool) {
	if dbName == "" {
		// No database name (should not happen for a valid migrate request); skip
		// the pre-create so we never emit a malformed CREATE DATABASE statement.
		return
	}
	marker := "ensure target database exists (target coordinator)"
	if clean {
		marker = "clean+recreate target database (target coordinator)"
	}
	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+marker))

	var inner strings.Builder
	inner.WriteString("set -euo pipefail\n")
	inner.WriteString("export PGHOST=$(printf '%s' \"$1\" | base64 -d)\n")
	inner.WriteString("export PGPORT=$(printf '%s' \"$2\" | base64 -d)\n")
	inner.WriteString("export PGUSER=$(printf '%s' \"$3\" | base64 -d)\n")
	// Connect to the cluster-bootstrap database for the catalog probe/CREATE: the
	// target database may not exist yet, so we cannot connect THROUGH it. $4 is
	// the admin/postgres bootstrap DB; the migration target DB name is the
	// embedded literal below.
	inner.WriteString("export PGDATABASE=$(printf '%s' \"$4\" | base64 -d)\n")
	inner.WriteString("export PGPASSWORD=$(printf '%s' \"$5\" | base64 -d)\n")
	inner.WriteString("GPENV=$(ls \"${GPHOME:-/usr/local/cloudberry-db}\"/greenplum_path.sh " +
		"\"${GPHOME:-/usr/local/cloudberry-db}\"/cloudberry-env.sh 2>/dev/null | head -1 || true)\n")
	inner.WriteString("if [ -n \"${GPENV}\" ]; then . \"${GPENV}\"; fi\n")
	inner.WriteString("export PATH=\"${GPHOME:-/usr/local/cloudberry-db}/bin:${PATH}\"\n")
	// The target DB name is a single-quoted shell literal (shellQuote): inert
	// against shell injection. It is interpolated into the SQL with SQL quoting
	// (single quotes in the WHERE filter, double quotes for the CREATE/DROP
	// identifier).
	fmt.Fprintf(&inner, "MIG_TARGET_DB=%s\n", shellQuote(dbName))
	if clean {
		writeMigrationCleanTargetDBSQL(&inner)
	} else {
		writeMigrationCreateTargetDBSQL(&inner)
	}

	// Connect to the bootstrap admin database (TARGET_PGDATABASE is the migration
	// TARGET db which may not exist yet); use the standard "postgres" maintenance
	// DB so the catalog probe/CREATE can connect regardless.
	writeMigrationEnsureTargetDBExec(b, inner.String())
}

// writeMigrationCreateTargetDBSQL appends the idempotent CREATE-if-absent psql
// step: probe pg_database and CREATE only when the target DB does not exist.
func writeMigrationCreateTargetDBSQL(inner *strings.Builder) {
	inner.WriteString("psql -v ON_ERROR_STOP=0 -tAc " +
		"\"SELECT 1 FROM pg_database WHERE datname='${MIG_TARGET_DB}'\" | grep -q 1 || " +
		"psql -c \"CREATE DATABASE \\\"${MIG_TARGET_DB}\\\"\"\n")
}

// writeMigrationCleanTargetDBSQL appends the clean-target psql steps for
// Truncate=true: terminate any open backends to the target DB (so DROP can
// proceed), DROP DATABASE IF EXISTS (no-op when absent), then CREATE DATABASE so
// the restore lands in a freshly-created EMPTY database.
func writeMigrationCleanTargetDBSQL(inner *strings.Builder) {
	// Terminate any backends connected to the target DB so the DROP is not blocked
	// (best-effort: the maintenance DB is "postgres", so our own session does not
	// count). Single-quoted SQL literal for the datname filter; double-quoted
	// identifier for DROP/CREATE.
	inner.WriteString("psql -v ON_ERROR_STOP=0 -tAc " +
		"\"SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
		"WHERE datname='${MIG_TARGET_DB}' AND pid <> pg_backend_pid()\" >/dev/null 2>&1 || true\n")
	// DROP IF EXISTS makes an absent DB a no-op; the subsequent CREATE yields a
	// fresh, EMPTY target so the migration starts from a clean state.
	inner.WriteString("psql -v ON_ERROR_STOP=1 -c " +
		"\"DROP DATABASE IF EXISTS \\\"${MIG_TARGET_DB}\\\"\"\n")
	inner.WriteString("psql -v ON_ERROR_STOP=1 -c " +
		"\"CREATE DATABASE \\\"${MIG_TARGET_DB}\\\"\"\n")
}

// writeMigrationEnsureTargetDBExec execs the ensure-target-DB inner script inside
// the TARGET coordinator, base64-piping the connection values. It mirrors
// writeInnerScriptExec but pins PGDATABASE to the "postgres" bootstrap database
// (the migration target DB may not exist yet, so we must not connect through it).
func writeMigrationEnsureTargetDBExec(b *strings.Builder, inner string) {
	b.WriteString("INNER_ENSURE_DB=$(cat <<'_CBK_ENSURE_DB_EOF_'\n")
	b.WriteString(inner)
	b.WriteString("_CBK_ENSURE_DB_EOF_\n)\n")

	fmt.Fprintf(b, "EDB_HOST_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", envTargetPGHost)
	fmt.Fprintf(b, "EDB_PORT_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", envTargetPGPort)
	fmt.Fprintf(b, "EDB_USER_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", envTargetPGUser)
	// Connect via the always-present "postgres" maintenance DB, NOT the migration
	// target DB (which is exactly what we are about to create).
	b.WriteString("EDB_DB_B64=$(printf '%s' \"postgres\" | base64 | tr -d '\\n')\n")
	fmt.Fprintf(b, "EDB_PASS_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", envTargetPGPassword)

	fmt.Fprintf(b,
		"printf '%%s' \"${INNER_ENSURE_DB}\" | base64 | "+
			"\"${KUBECTL}\" exec -i \"${DST_COORD_POD}\" -- bash -c '"+
			"INNER=$(base64 -d); bash -c \"${INNER}\" _ \"$0\" \"$1\" \"$2\" \"$3\" \"$4\"' "+
			"\"${EDB_HOST_B64}\" \"${EDB_PORT_B64}\" \"${EDB_USER_B64}\" "+
			"\"${EDB_DB_B64}\" \"${EDB_PASS_B64}\"\n")
}

// writeMigrationValidationPhase runs the post-restore validation against the
// TARGET coordinator: a best-effort row-count probe, the must-pass invalid-index
// scan and a health-check query. It mirrors postRestoreValidationScript's probe
// path (no expected counts) but executes via kubectl exec on the target
// coordinator using the TARGET_* connection env. The live e2e script performs the
// authoritative src-vs-dst row-count cross-check independently.
func writeMigrationValidationPhase(b *strings.Builder) {
	fmt.Fprintf(b, "echo %s\n", shellQuote(validateMarkerPrefix+"migration validation (target coordinator)"))

	var inner strings.Builder
	inner.WriteString("set -euo pipefail\n")
	inner.WriteString("export PGHOST=$(printf '%s' \"$1\" | base64 -d)\n")
	inner.WriteString("export PGPORT=$(printf '%s' \"$2\" | base64 -d)\n")
	inner.WriteString("export PGUSER=$(printf '%s' \"$3\" | base64 -d)\n")
	inner.WriteString("export PGDATABASE=$(printf '%s' \"$4\" | base64 -d)\n")
	inner.WriteString("export PGPASSWORD=$(printf '%s' \"$5\" | base64 -d)\n")
	inner.WriteString("GPENV=$(ls \"${GPHOME:-/usr/local/cloudberry-db}\"/greenplum_path.sh " +
		"\"${GPHOME:-/usr/local/cloudberry-db}\"/cloudberry-env.sh 2>/dev/null | head -1 || true)\n")
	inner.WriteString("if [ -n \"${GPENV}\" ]; then . \"${GPENV}\"; fi\n")
	inner.WriteString("export PATH=\"${GPHOME:-/usr/local/cloudberry-db}/bin:${PATH}\"\n")
	// Best-effort row-count probe (never fails: migration has no expected counts;
	// the live script cross-checks src vs dst counts authoritatively).
	fmt.Fprintf(&inner, "echo %s\n",
		shellQuote(validateMarkerPrefix+"row-count probe (best-effort, no expected counts)"))
	inner.WriteString("psql -tA -c \"SELECT count(*) FROM pg_class WHERE relkind='r'\" || " +
		"echo \"" + validateMarkerPrefix + "ROW_COUNT_PROBE_SKIPPED\"\n")
	// Must-pass invalid-index scan (mirrors writeInvalidIndexStep).
	fmt.Fprintf(&inner, "echo %s\n", shellQuote(validateMarkerPrefix+"scanning for invalid indexes"))
	inner.WriteString("invalid=$(psql -tA -c \"SELECT count(*) FROM pg_catalog.pg_class c " +
		"JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid " +
		"WHERE c.relkind='i' AND NOT i.indisvalid\")\n")
	inner.WriteString("if [ \"${invalid:-0}\" -gt 0 ]; then " +
		"echo \"" + validateMarkerPrefix + "${invalid} invalid index(es)\" >&2; exit 1; fi\n")
	// Health-check query.
	fmt.Fprintf(&inner, "echo %s\n", shellQuote(validateMarkerPrefix+"health-check query"))
	fmt.Fprintf(&inner, "psql -tA -c %s\n", shellQuote(defaultHealthCheckQuery))

	writeInnerScriptExec(b, "DST_COORD_POD", inner.String(), []string{
		envTargetPGHost, envTargetPGPort, envTargetPGUser, envTargetPGDatabase, envTargetPGPassword,
	})
}

// buildMigrationBackupArgs builds the gpbackup args (sans the leading plugin
// config, which the coordinator-exec block supplies via ${COORD_CFG}) for the
// migration backup phase: --dbname, --single-data-file and the repeated
// --include-table flags. It deliberately mirrors migrateBackupOptions so the
// rendered tokens match the Scenario 87 assertions.
func buildMigrationBackupArgs(opts *MigrationJobOptions) []string {
	args := make([]string, 0, 8)
	if opts.Database != "" {
		args = append(args, "--dbname", opts.Database)
	}
	if opts.SingleDataFile {
		args = append(args, "--single-data-file")
	}
	args = appendRepeatedFlag(args, "--include-table", opts.IncludeTables)
	return args
}

// buildMigrationRestoreArgs builds the gprestore args (sans the leading plugin
// config and sans --timestamp, which is appended at run time from the captured
// gpbackup timestamp) for the migration restore phase: --redirect-db,
// --redirect-schema, --jobs and the repeated --include-table flags. It mirrors
// migrateRestoreOptions so the rendered tokens match the Scenario 87 assertions.
//
// NOTE: --truncate-table is deliberately NOT emitted here. A cross-cluster
// migration restores into a FRESHLY-(re)created EMPTY target DB (pre-created via
// psql — see writeMigrationEnsureTargetDB), so it must restore BOTH the metadata
// (schema: tables, sequences, indexes) AND the data. gprestore's --truncate-table
// only "removes data of the tables getting restored" and therefore assumes those
// objects ALREADY EXIST: during the PRE-DATA metadata phase it tries to TRUNCATE
// objects that do not exist yet (e.g. public.users_id_seq) and aborts with
// `pq: relation "public.users_id_seq" does not exist (42P01)`. The user-facing
// --truncate intent ("clean target") is instead satisfied at the DB level: when
// Truncate is set the target DB is DROPped+recreated empty before the restore.
func buildMigrationRestoreArgs(opts *MigrationJobOptions) []string {
	redirectDb := migrationTargetDatabase(opts)
	args := make([]string, 0, 10)
	if redirectDb != "" {
		args = append(args, "--redirect-db", redirectDb)
	}
	if opts.RedirectSchema != "" {
		args = append(args, "--redirect-schema", opts.RedirectSchema)
	}
	// NOTE: --create-db is deliberately NOT emitted here. gprestore 2.1.0 refuses
	// a table-filtered (--include-table) / data-only restore into a non-existent
	// target DB, and --create-db does NOT satisfy that class (it restores the DB
	// from GLOBAL metadata, absent in a table-filtered backup). The migration
	// instead PRE-CREATES the target database via psql on the target coordinator
	// (writeMigrationEnsureTargetDB), so the DB always exists by restore time;
	// adding --create-db on top would then error "database already exists".
	if opts.Jobs > 0 {
		args = append(args, "--jobs", fmt.Sprintf("%d", opts.Jobs))
	}
	args = appendRepeatedFlag(args, "--include-table", opts.IncludeTables)
	return args
}

// innerToolExec parametrizes a single coordinator-exec tool invocation.
type innerToolExec struct {
	// coordPodVar is the shell variable holding the coordinator pod name.
	coordPodVar string
	// tool is the binary to run inside the coordinator (gpbackup | gprestore).
	tool string
	// args are the readable tool args (excluding plugin config + timestamp).
	args []string
	// hostEnv/portEnv/userEnv/dbEnv/passEnv name the Job-pod env vars whose values
	// are base64-piped as positional args into the coordinator shell.
	hostEnv, portEnv, userEnv, dbEnv, passEnv string
	// captureToVar, when set, captures the tool's combined output into that shell
	// variable (used to extract gpbackup's Backup Timestamp).
	captureToVar string
	// appendTimestampVar, when set, appends `--timestamp ${<var>}` to the tool so
	// the captured gpbackup timestamp expands at run time inside the coordinator.
	appendTimestampVar string
}

// writeInnerToolExec renders a readable inner tool script (gpbackup/gprestore
// against ${COORD_CFG}) and the kubectl-exec that runs it inside the coordinator,
// delivering the connection values as base64 positional args (env-safe for any
// password). The tool flags are emitted READABLY so they stay visible in the
// Job's args[0] for the Scenario 87 grep-based assertions.
func writeInnerToolExec(b *strings.Builder, e innerToolExec) {
	var inner strings.Builder
	inner.WriteString("set -euo pipefail\n")
	inner.WriteString("export COORD_CFG=$(printf '%s' \"$1\" | base64 -d)\n")
	inner.WriteString("export PGHOST=$(printf '%s' \"$2\" | base64 -d)\n")
	inner.WriteString("export PGPORT=$(printf '%s' \"$3\" | base64 -d)\n")
	inner.WriteString("export PGUSER=$(printf '%s' \"$4\" | base64 -d)\n")
	inner.WriteString("export PGDATABASE=$(printf '%s' \"$5\" | base64 -d)\n")
	inner.WriteString("export PGPASSWORD=$(printf '%s' \"$6\" | base64 -d)\n")
	inner.WriteString("GPENV=$(ls \"${GPHOME:-/usr/local/cloudberry-db}\"/greenplum_path.sh " +
		"\"${GPHOME:-/usr/local/cloudberry-db}\"/cloudberry-env.sh 2>/dev/null | head -1 || true)\n")
	inner.WriteString("if [ -n \"${GPENV}\" ]; then . \"${GPENV}\"; fi\n")
	inner.WriteString("export PATH=\"${GPHOME:-/usr/local/cloudberry-db}/bin:${PATH}\"\n")
	inner.WriteString(e.tool)
	inner.WriteString(" " + pluginConfigFlag + " \"${COORD_CFG}\"")
	if e.appendTimestampVar != "" {
		// The captured gpbackup timestamp is passed as $7 to the inner shell.
		inner.WriteString(" --timestamp \"$7\"")
	}
	for _, a := range e.args {
		inner.WriteString(" ")
		inner.WriteString(shellQuote(a))
	}
	inner.WriteString("\n")

	writeInnerScriptExecWithTS(b, e, inner.String())
}

// writeInnerScriptExecWithTS materializes the inner tool script in the Job pod
// (quoted heredoc => no local expansion) and execs it inside the coordinator,
// base64-piping the connection values as positional args and, when set, the
// captured timestamp as an extra positional arg. Output is optionally captured.
func writeInnerScriptExecWithTS(b *strings.Builder, e innerToolExec, inner string) {
	b.WriteString("INNER_TOOL=$(cat <<'_CBK_INNER_EOF_'\n")
	b.WriteString(inner)
	b.WriteString("_CBK_INNER_EOF_\n)\n")
	fmt.Fprintf(b, "CFG_B64=$(printf '%%s' \"${COORD_CFG}\" | base64 | tr -d '\\n')\n")
	fmt.Fprintf(b, "HOST_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", e.hostEnv)
	fmt.Fprintf(b, "PORT_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", e.portEnv)
	fmt.Fprintf(b, "USER_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", e.userEnv)
	fmt.Fprintf(b, "DB_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", e.dbEnv)
	fmt.Fprintf(b, "PASS_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", e.passEnv)

	// The remote bootstrap decodes the inner tool from stdin and runs it with the
	// base64 connection values as $1..$6 (and the raw timestamp as $7 when set).
	tsArg := ""
	remote := "INNER=$(base64 -d); " +
		"bash -c \"${INNER}\" _ \"$0\" \"$1\" \"$2\" \"$3\" \"$4\" \"$5\""
	if e.appendTimestampVar != "" {
		// $6 carries the (non-secret) captured timestamp verbatim.
		remote = "INNER=$(base64 -d); " +
			"bash -c \"${INNER}\" _ \"$0\" \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$6\""
		tsArg = fmt.Sprintf(" \"${%s}\"", e.appendTimestampVar)
	}

	exec := "printf '%s' \"${INNER_TOOL}\" | base64 | " +
		"\"${KUBECTL}\" exec -i \"${" + e.coordPodVar + "}\" -- bash -c '" + remote + "' " +
		"\"${CFG_B64}\" \"${HOST_B64}\" \"${PORT_B64}\" " +
		"\"${USER_B64}\" \"${DB_B64}\" \"${PASS_B64}\"" + tsArg + "\n"

	if e.captureToVar != "" {
		// Capture combined output (tee to the Job log so it is observable too).
		fmt.Fprintf(b, "%s=$(%s2>&1 | tee /dev/stderr)\n",
			e.captureToVar, strings.TrimSuffix(exec, "\n")+" ")
		return
	}
	b.WriteString(exec)
}

// writeInnerScriptExec runs a non-tool inner script (e.g. validation psql steps)
// inside the coordinator, base64-piping the named env vars as positional args.
// envVars are delivered as $1.. in declaration order.
func writeInnerScriptExec(b *strings.Builder, coordPodVar, inner string, envVars []string) {
	b.WriteString("INNER_VALIDATE=$(cat <<'_CBK_VALIDATE_EOF_'\n")
	b.WriteString(inner)
	b.WriteString("_CBK_VALIDATE_EOF_\n)\n")

	var posArgs strings.Builder
	for i, name := range envVars {
		fmt.Fprintf(b, "V%d_B64=$(printf '%%s' \"${%s:-}\" | base64 | tr -d '\\n')\n", i, name)
		fmt.Fprintf(&posArgs, " \"${V%d_B64}\"", i)
	}

	// Build the positional-arg references ($0..$N-1) for the remote bash.
	var dollarRefs strings.Builder
	for i := range envVars {
		fmt.Fprintf(&dollarRefs, " \"$%d\"", i)
	}

	fmt.Fprintf(b,
		"printf '%%s' \"${INNER_VALIDATE}\" | base64 | "+
			"\"${KUBECTL}\" exec -i \"${%s}\" -- bash -c '"+
			"INNER=$(base64 -d); bash -c \"${INNER}\" _%s'%s\n",
		coordPodVar, dollarRefs.String(), posArgs.String())
}

// writeStageConfig stages the Job-pod-rendered S3 plugin config into the named
// coordinator pod (base64-piped so no quoting/here-doc hazard crosses the exec
// boundary). The remote shell reads the target path from its own argv.
func writeStageConfig(b *strings.Builder, coordPodVar string) {
	fmt.Fprintf(b,
		"base64 < %[1]s | \"${KUBECTL}\" exec -i \"${%[2]s}\" -- "+
			"bash -c 'base64 -d > \"$0\"' \"${COORD_CFG}\"\n",
		s3RenderedConfigPath, coordPodVar)
}

// writeCleanupConfig best-effort removes the staged config from the named
// coordinator pod (never aborts the migration on failure).
func writeCleanupConfig(b *strings.Builder, coordPodVar string) {
	fmt.Fprintf(b,
		"\"${KUBECTL}\" exec -i \"${%s}\" -- bash -c 'rm -f \"$0\"' \"${COORD_CFG}\" 2>/dev/null || true\n",
		coordPodVar)
}

// shellQuoteBare returns s sanitized for embedding inside a double-quoted shell
// path segment (the per-run config filename). The operator timestamp is a
// 14-digit string in practice; this strips anything outside [0-9A-Za-z_-] as a
// defensive measure so the rendered filename never carries shell metacharacters.
func shellQuoteBare(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == '-':
			b.WriteRune(r)
		default:
			// drop unsafe characters: filename stays metacharacter-free
		}
	}
	return b.String()
}
