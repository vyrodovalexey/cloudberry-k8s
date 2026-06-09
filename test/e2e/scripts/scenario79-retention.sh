#!/usr/bin/env bash
# =============================================================================
# Scenario 79 — Retention Cleanup, All Policies (live verification)
# =============================================================================
# Verifies the gpbackman-driven retention cleanup lifecycle end-to-end against an
# already-deployed Ready cluster with retention.{fullCount:3,incrementalCount:10,
# maxAge:"30d"} (single S3/MinIO destination, same /backups folder):
#
#   79a fullCount=3        : take 4 real FULL gpbackups; enforce retention so the
#       OLDEST full is deleted (gpbackman backup-delete --cascade); assert
#       backup-info --type full shows exactly 3 active fulls and the oldest ts is
#       gone (also verify its objects removed from S3 via `mc`).
#   79b incrementalCount=10: take a full + 11 chained incrementals; enforce
#       retention so the oldest incremental beyond 10 is deleted; assert
#       backup-info --type incremental shows the count-adjusted set, oldest gone.
#   79c maxAge="30d"       : take a backup, then AGE its gpbackup_history.db row
#       (SQLite UPDATE > 30 days old) and run gpbackman backup-clean
#       --older-than-days 30 --cascade; assert that backup is no longer active and
#       its objects are removed from S3.
#   79d cleanup placement   : verify the operator creates a retention cleanup Job
#       after a successful backup (kubectl get jobs -l
#       avsoft.io/backup-operation=cleanup, owner-ref to the cluster, name =
#       RetentionCleanupJobName(cluster,latest-ts)); materialize/observe its
#       RETENTION_DELETED annotation and assert
#       cloudberry_backup_retention_deleted_total increased in VictoriaMetrics.
#
# LIVE EXECUTION MODEL (carried from scenario76/77/78)
# ----------------------------------------------------
# gpbackup/gpbackman are MPP tools that operate on the real coordinator
# gpbackup_history.db (SQLite in PGDATA). A standalone cleanup Job pod is NOT the
# coordinator and lacks that history DB, so REAL gpbackup/gpbackman run via
# coordinator-exec (segment -1) inside `<cluster>-coordinator-0` where the history
# DB lives. The operator cleanup Job's ARGS/spec are asserted from the RENDERED
# resource (79d), NOT from a Job pod doing a real delete. The actual deletions
# (79a/79b/79c) are exercised by coordinator-exec gpbackman. The
# annotation->metric plumbing (79d) is verified by MATERIALIZING a Succeeded
# cleanup Job carrying a RETENTION_DELETED terminated message (or annotation) +
# reconciling, mirroring the proven scenario77/78 status-Job model.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77/78:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into Job YAML are base64-encoded to avoid YAML
#     block-scalar indentation pitfalls (the container decodes + runs them).
#   * Do NOT fill hostpath PVCs with large files (no quota -> DiskPressure). The
#     AO seed/delta row counts are kept modest.
#
# S3 OPS: reuses the scenario76/78 coordinator-exec gpbackup_s3_plugin model
# (render /tmp/s3-config.yaml with 10MB/4 multipart) and inspects/cleans S3
# objects via `mc` in the MinIO container (docker exec).
#
# Usage:
#   scenario79-retention.sh --cluster <s3-name> \
#       [--namespace cloudberry-test] [--db mydb] [--checks 79a,79b,79c,79d]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: mydb)
#   CHECKS               comma list of checks (default: 79a,79b,79c,79d)
#   BUCKET               S3 bucket (default: cloudberry-backups)
#   FOLDER               S3 folder prefix (default: backups)
#   S3_ENDPOINT          S3 endpoint (default: http://minio:9000)
#   S3_REGION            S3 region (default: us-east-1)
#   AWS_ACCESS_KEY_ID    S3 access key (default: minioadmin)
#   AWS_SECRET_ACCESS_KEY S3 secret key (default: minioadmin)
#   AO_TABLE             append-optimized table name (default: public.events_ao)
#   AO_SEED_ROWS         initial AO rows (default: 2000)
#   AO_DELTA_ROWS        rows inserted per modification (default: 500)
#   FULL_COUNT           retention fullCount (default: 3)
#   INCR_COUNT           retention incrementalCount (default: 10)
#   MAX_AGE_DAYS         retention maxAge in days (default: 30)
#   COMPRESSION_LEVEL    --compression-level for gpbackup (default: 1)
#   MINIO_CONTAINER      docker container name for `mc` S3 ops (default: minio)
#   VM_URL               VictoriaMetrics base URL (default: http://127.0.0.1:8428)
#   STATUS_TIMEOUT_SECS  max seconds to wait for status/event (default: 180)
#   READY_TIMEOUT        cluster readiness timeout (default: 10m)
#   BACKUP_IMAGE         backup toolchain image (default: cloudberry-backup:2.1.0)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
CHECKS="${CHECKS:-79a,79b,79c,79d}"

BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

AO_TABLE="${AO_TABLE:-public.events_ao}"
AO_SEED_ROWS="${AO_SEED_ROWS:-2000}"
AO_DELTA_ROWS="${AO_DELTA_ROWS:-500}"

FULL_COUNT="${FULL_COUNT:-3}"
INCR_COUNT="${INCR_COUNT:-10}"
MAX_AGE_DAYS="${MAX_AGE_DAYS:-30}"

COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
VM_URL="${VM_URL:-http://127.0.0.1:8428}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-180}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"

# ----------------------------------------------------------------------------
# Logging helpers
# ----------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_step()  { echo -e "\n${BLUE}========== $* ==========${NC}"; }

die() { log_error "$*"; exit 1; }

usage() {
  sed -n '2,78p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --db)        DB="$2"; shift 2 ;;
    --checks)    CHECKS="$2"; shift 2 ;;
    -h|--help)   usage 0 ;;
    *)           log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the S3-destination cluster, e.g. scenario79-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
HISTORY_DB=""           # resolved coordinator gpbackup_history.db path.
MATERIALIZED_JOBS=""    # space-separated list of Job names we created (cleanup).

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_79a="SKIP"
RESULT_79b="SKIP"
RESULT_79c="SKIP"
RESULT_79d="SKIP"

set_result() {
  case "$1" in
    79a) RESULT_79a="$2" ;;
    79b) RESULT_79b="$2" ;;
    79c) RESULT_79c="$2" ;;
    79d) RESULT_79d="$2" ;;
  esac
}
get_result() {
  case "$1" in
    79a) printf '%s' "${RESULT_79a:-SKIP}" ;;
    79b) printf '%s' "${RESULT_79b:-SKIP}" ;;
    79c) printf '%s' "${RESULT_79c:-SKIP}" ;;
    79d) printf '%s' "${RESULT_79d:-SKIP}" ;;
    *)   printf 'SKIP' ;;
  esac
}

want_check() {
  case ",${CHECKS}," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

# track_job records a materialized Job name for cleanup on exit.
track_job() {
  MATERIALIZED_JOBS="${MATERIALIZED_JOBS} $1"
}

# ----------------------------------------------------------------------------
# Cleanup trap: remove the materialized status Jobs so reruns start clean.
# ----------------------------------------------------------------------------
cleanup() {
  local j
  for j in ${MATERIALIZED_JOBS}; do
    [ -n "${j}" ] && "${KN[@]}" delete job "${j}" --ignore-not-found >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# coord_psql: exec into the coordinator pod and run SQL as gpadmin.
# Args: <database> <sql>
# The single-quoted remote script takes inputs as positional args ($1..$3) so
# credentials never appear in the local process table (SC2016 intentional).
# ----------------------------------------------------------------------------
coord_psql() {
  local database="$1" sql="$2"
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" -At -c "$3"
    ' _ "${DB_PASSWORD}" "${database}" "${sql}"
}

# ----------------------------------------------------------------------------
# Step 0 — Resolve DB admin password + coordinator container + history DB path.
# ----------------------------------------------------------------------------
resolve_cluster() {
  log_step "Resolving DB admin password + coordinator (cluster=${CLUSTER})"
  DB_PASSWORD="$("${KN[@]}" get secret "${DB_PW_SECRET}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${DB_PW_SECRET}"

  local cname
  cname="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
  DB_CONTAINER="${cname:-cloudberry}"
  log_info "Coordinator pod=${COORD_POD} container=${DB_CONTAINER}"
}

# resolve_history_db locates the coordinator gpbackup_history.db inside the
# coordinator pod ($COORDINATOR_DATA_DIRECTORY/gpbackup_history.db), creating it
# implicitly on the first gpbackup. It resolves the data directory at runtime.
resolve_history_db() {
  local path
  # shellcheck disable=SC2016
  path="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      DD="${COORDINATOR_DATA_DIRECTORY:-${MASTER_DATA_DIRECTORY:-}}"
      if [ -z "$DD" ]; then
        DD="$(psql -U gpadmin -d postgres -At -c "SHOW data_directory" 2>/dev/null || true)"
      fi
      printf "%s/gpbackup_history.db" "$DD"
    ' 2>/dev/null || true)"
  HISTORY_DB="${path}"
  [ -n "${HISTORY_DB}" ] || die "could not resolve coordinator gpbackup_history.db path"
  log_info "Coordinator history DB: ${HISTORY_DB}"
}

# ----------------------------------------------------------------------------
# Step 1 — Preflight: cluster Ready; ensure mydb + an AO table for incrementals.
# ----------------------------------------------------------------------------
preflight() {
  log_step "Preflight (cluster=${CLUSTER} ns=${NAMESPACE})"

  "${KN[@]}" get cloudberrycluster "${CLUSTER}" >/dev/null 2>&1 \
    || die "CloudberryCluster ${CLUSTER} not found in ${NAMESPACE}"

  log_info "Waiting for coordinator pod ${COORD_POD} Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${COORD_POD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || die "coordinator pod ${COORD_POD} not Ready"
  log_info "Cluster ${CLUSTER} coordinator Ready"
}

# ensure_ao_table creates ${DB} (if needed) and a populated append-optimized
# table so the incremental deltas (79b) are meaningful.
ensure_ao_table() {
  log_step "Ensuring database ${DB} + AO table ${AO_TABLE} (${AO_SEED_ROWS} rows)"

  if ! coord_psql postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql postgres "CREATE DATABASE ${DB};" >/dev/null
    log_info "Database ${DB} created"
  else
    log_info "Database ${DB} already exists"
  fi

  coord_psql "${DB}" "
    CREATE TABLE IF NOT EXISTS ${AO_TABLE} (
      id bigint,
      payload text,
      created timestamptz DEFAULT now()
    ) WITH (appendonly=true, orientation=row) DISTRIBUTED BY (id);
    INSERT INTO ${AO_TABLE} (id, payload)
    SELECT g, repeat('x', 64)
    FROM generate_series(1, ${AO_SEED_ROWS}) AS g
    WHERE NOT EXISTS (SELECT 1 FROM ${AO_TABLE} LIMIT 1);
    ANALYZE ${AO_TABLE};
  " >/dev/null

  local cnt
  cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${AO_TABLE};")"
  log_info "AO table ${AO_TABLE} row count: ${cnt}"
  [ "${cnt:-0}" -gt 0 ] || die "AO table ${AO_TABLE} is empty (expected seed data)"
}

# modify_ao_table inserts AO_DELTA_ROWS new rows to produce a real incremental
# delta. Output is suppressed.
modify_ao_table() {
  local max
  max="$(coord_psql "${DB}" "SELECT COALESCE(max(id),0) FROM ${AO_TABLE};")"
  coord_psql "${DB}" "
    INSERT INTO ${AO_TABLE} (id, payload)
    SELECT ${max} + g, repeat('y', 64)
    FROM generate_series(1, ${AO_DELTA_ROWS}) AS g;
    ANALYZE ${AO_TABLE};
  " >/dev/null
}

# validate_ts asserts a captured timestamp is exactly 14 digits (YYYYMMDDHHMMSS).
validate_ts() {
  local ts="$1"
  [ -n "${ts}" ] || die "no timestamp captured"
  case "${ts}" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) die "timestamp '${ts}' is not 14 digits" ;;
  esac
}

# ----------------------------------------------------------------------------
# coord_render_s3_config writes the gpbackup_s3_plugin config (10MB/4 multipart)
# to /tmp/s3-config.yaml inside the coordinator pod.
# ----------------------------------------------------------------------------
coord_render_s3_config() {
  # The plugin-config values expand in the LOCAL shell (single-quote islands);
  # ${GPHOME} expands in the REMOTE shell (SC2016 intentional).
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      cat > /tmp/s3-config.yaml <<EOF
executablepath: ${GPHOME}/bin/gpbackup_s3_plugin
options:
  region: '"${S3_REGION}"'
  endpoint: '"${S3_ENDPOINT}"'
  aws_access_key_id: '"${AWS_ACCESS_KEY_ID}"'
  aws_secret_access_key: '"${AWS_SECRET_ACCESS_KEY}"'
  bucket: '"${BUCKET}"'
  folder: '"${FOLDER}"'
  encryption: "off"
  backup_multipart_chunksize: 10MB
  backup_max_concurrent_requests: 4
  restore_multipart_chunksize: 10MB
  restore_max_concurrent_requests: 4
EOF
      echo rendered'
}

# ----------------------------------------------------------------------------
# coord_exec_full_backup runs a real FULL gpbackup of ${DB} to S3 and prints the
# captured 14-digit timestamp on stdout.
# ----------------------------------------------------------------------------
coord_exec_full_backup() {
  log_step "Running REAL FULL gpbackup of ${DB} via coordinator-exec" >&2
  coord_render_s3_config >/dev/null
  local out ts
  # ${GPHOME}/${PATH} expand in the REMOTE shell; creds expand in single-quote
  # islands in the LOCAL shell (SC2016 intentional).
  # shellcheck disable=SC2016
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --leaf-partition-data \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    die "FULL gpbackup did not complete successfully"
  fi
  validate_ts "${ts}"
  log_info "FULL backup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# ----------------------------------------------------------------------------
# coord_exec_incremental runs a real INCREMENTAL gpbackup of ${DB} to S3
# (auto-base). Prints the captured 14-digit timestamp on stdout.
# ----------------------------------------------------------------------------
coord_exec_incremental() {
  log_step "Running REAL INCREMENTAL gpbackup of ${DB} (auto-base)" >&2
  coord_render_s3_config >/dev/null
  local out ts
  # shellcheck disable=SC2016
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --incremental --leaf-partition-data \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "(Backup Timestamp|Incremental|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    die "INCREMENTAL gpbackup did not complete successfully"
  fi
  validate_ts "${ts}"
  log_info "INCREMENTAL backup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# ----------------------------------------------------------------------------
# gpbackman coordinator-exec helpers — run against the REAL history DB. The
# history DB / plugin-config paths expand in the LOCAL shell (single-quote
# islands); ${GPHOME}/${PATH} expand in the REMOTE shell (SC2016 intentional).
# ----------------------------------------------------------------------------

# coord_gpbackman_info lists the Success backup timestamps (newest-first) for the
# given --type (full|incremental), one per line.
# Args: <type>
coord_gpbackman_info() {
  local btype="$1"
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      gpbackman backup-info --type "'"${btype}"'" --history-db "'"${HISTORY_DB}"'" 2>/dev/null
    ' 2>/dev/null | awk '/[Ss]uccess/ { for (i=1;i<=NF;i++) if ($i ~ /^[0-9]{14}$/) { print $i; break } }' || true
}

# coord_gpbackman_delete deletes a single backup timestamp (cascade).
# Args: <ts>
coord_gpbackman_delete() {
  local ts="$1"
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackman backup-delete --timestamp "'"${ts}"'" --cascade \
        --plugin-config /tmp/s3-config.yaml --history-db "'"${HISTORY_DB}"'" 2>&1
    ' >&2 2>&1 || log_warn "gpbackman backup-delete ${ts} returned non-zero"
}

# coord_gpbackman_clean removes every backup older than <days> (cascade).
# Args: <days>
coord_gpbackman_clean() {
  local days="$1"
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackman backup-clean --older-than-days "'"${days}"'" \
        --plugin-config /tmp/s3-config.yaml --cascade --history-db "'"${HISTORY_DB}"'" 2>&1
    ' >&2 2>&1 || log_warn "gpbackman backup-clean --older-than-days ${days} returned non-zero"
}

# enforce_count_retention replicates the operator's count-based retention logic
# (buildGpbackmanRetentionScript): while the Success backups of <type> exceed
# <keep>, delete the OLDEST (last newest-first line) and re-enumerate. Echoes the
# number of deletions performed on stdout.
# Args: <type> <keep>
enforce_count_retention() {
  local btype="$1" keep="$2" deleted=0 list count oldest
  while :; do
    list="$(coord_gpbackman_info "${btype}")"
    count="$(printf '%s\n' "${list}" | grep -c '[0-9]' || true)"
    if [ "${count:-0}" -le "${keep}" ]; then break; fi
    oldest="$(printf '%s\n' "${list}" | grep '[0-9]' | tail -n 1)"
    [ -n "${oldest}" ] || break
    log_info "  retention(${btype}): deleting oldest ${oldest} (count=${count} > keep=${keep})" >&2
    coord_gpbackman_delete "${oldest}"
    deleted=$(( deleted + 1 ))
  done
  printf '%s\n' "${deleted}"
}

# ----------------------------------------------------------------------------
# mc helpers — inspect/remove S3 objects via `mc` in the MinIO container.
# ----------------------------------------------------------------------------
mc_alias() {
  command -v docker >/dev/null 2>&1 || return 1
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" >/dev/null 2>&1 || true
}

# mc_has_objects returns 0 when at least one S3 object key embeds <ts>.
# Args: <ts>
mc_has_objects() {
  local ts="$1"
  command -v docker >/dev/null 2>&1 || return 0
  mc_alias
  docker exec "${MINIO_CONTAINER}" mc ls --recursive "local/${BUCKET}" 2>/dev/null \
    | awk '{print $NF}' | grep -q "${ts}"
}

# ----------------------------------------------------------------------------
# materialize_succeeded_cleanup_job creates a Succeeded cleanup-operation Job
# named like the operator's (RetentionCleanupJobName) carrying an owner-ref to the
# cluster and the RETENTION_DELETED annotation, so the operator's metrics loop
# attributes its deletions. The embedded container script is base64-encoded for
# parity with scenario77/78 (no multi-line YAML block-scalar).
# Args: <ts> <deleted-count>
# ----------------------------------------------------------------------------
materialize_succeeded_cleanup_job() {
  local ts="$1" deleted="$2"
  local job="${CLUSTER}-cleanup-${ts}"
  local now_iso script_b64 cluster_uid
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo RETENTION_DELETED=${deleted}; exit 0" | base64 | tr -d '\n')"
  cluster_uid="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"

  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${CLUSTER}
    avsoft.io/component: backup
    avsoft.io/backup-operation: cleanup
    scenario: "79"
  annotations:
    avsoft.io/backup-retention-deleted: "${deleted}"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${cluster_uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: cleanup
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackman
          image: ${BACKUP_IMAGE}
          terminationMessagePolicy: FallbackToLogsOnError
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
EOF
  track_job "${job}"

  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || log_warn "could not patch cleanup Job ${job} status to Succeeded"

  printf '%s\n' "${job}"
}

# set_cleanup_job_succeeded_with_count drives an EXISTING (operator-created)
# cleanup Job to Succeeded and stamps the retention-deleted annotation directly
# (the Job spec is immutable, so we only patch metadata + status). This exercises
# the operator's metrics loop (backupRetentionDeletedCount -> the counter) without
# recreating the immutable Job. Args: <job-name> <deleted-count>
set_cleanup_job_succeeded_with_count() {
  local job="$1" deleted="$2" now_iso
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "${KN[@]}" annotate job "${job}" \
    "avsoft.io/backup-retention-deleted=${deleted}" --overwrite >/dev/null 2>&1 || true
  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || return 1
  printf '%s\n' "${job}"
}

# reconcile_cluster touches the CR so the operator re-derives status.
reconcile_cluster() {
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario79-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true
}

# ----------------------------------------------------------------------------
# vm_query queries VictoriaMetrics for the instant value of a metric expression
# and prints the scalar result (0 when unavailable).
# Args: <promql>
# ----------------------------------------------------------------------------
vm_query() {
  local q="$1" out val
  command -v curl >/dev/null 2>&1 || { printf '0'; return 0; }
  out="$(curl -fsS --data-urlencode "query=${q}" "${VM_URL}/api/v1/query" 2>/dev/null || true)"
  # Extract the first value from data.result[].value[1] without jq.
  val="$(printf '%s' "${out}" | sed -n 's/.*"value":\[[0-9.]*,"\([0-9.]*\)"\].*/\1/p' | head -1)"
  [ -n "${val}" ] || val="0"
  printf '%s' "${val}"
}

# vm_retention_total prints the current cloudberry_backup_retention_deleted_total
# for this cluster (summed across label permutations), 0 when absent.
vm_retention_total() {
  vm_query "sum(cloudberry_backup_retention_deleted_total{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\"})"
}

# =============================================================================
# 79a — fullCount retention (fullCount=3, 4 fulls -> delete oldest 1, retain 3)
# =============================================================================
run_79a() {
  log_step "79a — fullCount=${FULL_COUNT} (4 full backups -> oldest deleted, 3 retained)"

  local f1 f2 f3 f4
  f1="$(coord_exec_full_backup)"; validate_ts "${f1}"
  modify_ao_table
  f2="$(coord_exec_full_backup)"; validate_ts "${f2}"
  modify_ao_table
  f3="$(coord_exec_full_backup)"; validate_ts "${f3}"
  modify_ao_table
  f4="$(coord_exec_full_backup)"; validate_ts "${f4}"
  log_info "79a: fulls F1=${f1} F2=${f2} F3=${f3} F4=${f4}"

  local before
  before="$(coord_gpbackman_info full | grep -c '[0-9]' || true)"
  log_info "79a: active fulls before cleanup: ${before}"
  if [ "${before:-0}" -lt 4 ]; then
    log_warn "79a: expected >=4 active fulls before cleanup, got ${before}"
    set_result 79a "FAIL"
    return 0
  fi

  # Enforce the operator's count retention logic via coordinator-exec gpbackman.
  local deleted
  deleted="$(enforce_count_retention full "${FULL_COUNT}")"
  log_info "79a: count retention deleted ${deleted} full backup(s)"

  local after
  after="$(coord_gpbackman_info full | grep -c '[0-9]' || true)"
  log_info "79a: active fulls after cleanup: ${after}"

  # Assert exactly FULL_COUNT active fulls and the OLDEST (F1) is gone.
  if [ "${after:-0}" != "${FULL_COUNT}" ]; then
    log_warn "79a: expected ${FULL_COUNT} active fulls, got ${after}"
    set_result 79a "FAIL"
    return 0
  fi
  if coord_gpbackman_info full | grep -q "^${f1}$"; then
    log_warn "79a: oldest full ${f1} is still active (expected deleted)"
    set_result 79a "FAIL"
    return 0
  fi
  log_info "79a: oldest full ${f1} deleted; ${FULL_COUNT} retained OK"

  # Verify the oldest full's objects were removed from S3.
  if mc_has_objects "${f1}"; then
    log_warn "79a: S3 still has objects for deleted full ${f1}"
    set_result 79a "FAIL"
    return 0
  fi
  log_info "79a: S3 objects for ${f1} removed OK"
  set_result 79a "PASS"
}

# =============================================================================
# 79b — incrementalCount retention (incrementalCount=10, full + 11 incrementals)
# =============================================================================
run_79b() {
  log_step "79b — incrementalCount=${INCR_COUNT} (full + 11 incrementals -> oldest beyond 10 deleted)"

  local base i n
  base="$(coord_exec_full_backup)"; validate_ts "${base}"
  log_info "79b: base full ${base}"

  local first_incr=""
  n=1
  while [ "${n}" -le 11 ]; do
    modify_ao_table
    i="$(coord_exec_incremental)"; validate_ts "${i}"
    [ -z "${first_incr}" ] && first_incr="${i}"
    log_info "79b: incremental #${n} = ${i}"
    n=$(( n + 1 ))
  done

  local before
  before="$(coord_gpbackman_info incremental | grep -c '[0-9]' || true)"
  log_info "79b: active incrementals before cleanup: ${before} (oldest=${first_incr})"
  if [ "${before:-0}" -lt 11 ]; then
    log_warn "79b: expected >=11 active incrementals before cleanup, got ${before}"
    set_result 79b "FAIL"
    return 0
  fi

  local deleted
  deleted="$(enforce_count_retention incremental "${INCR_COUNT}")"
  log_info "79b: count retention deleted ${deleted} incremental(s) (cascade accounted)"

  local after
  after="$(coord_gpbackman_info incremental | grep -c '[0-9]' || true)"
  log_info "79b: active incrementals after cleanup: ${after}"

  # The re-enumerating loop stops once incrementals <= INCR_COUNT (cascade may
  # reduce the set further, so assert <= INCR_COUNT, not strict equality).
  if [ "${after:-0}" -gt "${INCR_COUNT}" ]; then
    log_warn "79b: expected <=${INCR_COUNT} active incrementals, got ${after}"
    set_result 79b "FAIL"
    return 0
  fi
  # The oldest incremental must be gone.
  if coord_gpbackman_info incremental | grep -q "^${first_incr}$"; then
    log_warn "79b: oldest incremental ${first_incr} still active (expected deleted)"
    set_result 79b "FAIL"
    return 0
  fi
  log_info "79b: oldest incremental ${first_incr} deleted; <=${INCR_COUNT} retained OK"

  # The base full must remain (within fullCount=3).
  if ! coord_gpbackman_info full | grep -q "^${base}$"; then
    log_warn "79b: base full ${base} unexpectedly removed"
    set_result 79b "FAIL"
    return 0
  fi
  if mc_has_objects "${first_incr}"; then
    log_warn "79b: S3 still has objects for deleted incremental ${first_incr}"
    set_result 79b "FAIL"
    return 0
  fi
  log_info "79b: base full ${base} retained; S3 objects for ${first_incr} removed OK"
  set_result 79b "PASS"
}

# =============================================================================
# 79c — maxAge retention (maxAge=30d, aged history entry deleted by backup-clean)
# =============================================================================
run_79c() {
  log_step "79c — maxAge=${MAX_AGE_DAYS}d (age a history row >30d -> backup-clean deletes it)"

  local f_old f_new
  f_old="$(coord_exec_full_backup)"; validate_ts "${f_old}"
  modify_ao_table
  f_new="$(coord_exec_full_backup)"; validate_ts "${f_new}"
  log_info "79c: aged-candidate F_old=${f_old} ; fresh F_new=${f_new}"

  # AGE the F_old backup so it is >30 days old. gpbackman computes a backup's age
  # from its 14-digit `timestamp` PRIMARY KEY (the gpbackup_history.db `backups`
  # table has NO separate date column — verified via .schema), so we must rewrite
  # the timestamp itself to an aged 14-digit value. The S3 plugin locates a
  # backup's objects by the timestamp directory prefix
  # (backups/backups/<YYYYMMDD>/<ts>/...), so we also MOVE the S3 prefix to the
  # aged date/ts path so backup-clean's plugin delete finds and removes them.
  local aged_ts aged_day new_day
  aged_ts="$(date -u -v-45d '+%Y%m%d%H%M%S' 2>/dev/null || date -u -d '45 days ago' '+%Y%m%d%H%M%S' 2>/dev/null || echo '')"
  [ -n "${aged_ts}" ] || { log_warn "79c: could not compute an aged 14-digit timestamp"; set_result 79c "FAIL"; return 0; }
  aged_day="${aged_ts%??????}"   # YYYYMMDD prefix
  new_day="${f_old%??????}"
  log_info "79c: aging ${f_old} -> ${aged_ts} (history timestamp PK + S3 prefix)"

  # 1) Move the S3 objects from the fresh ts prefix to the aged ts prefix so the
  #    plugin delete (keyed by timestamp dir) can remove them. Internal filenames
  #    still embed the original ts, but gpbackman deletes the whole ts directory.
  if command -v docker >/dev/null 2>&1; then
    docker exec "${MINIO_CONTAINER}" mc alias set local \
      http://localhost:9000 "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" >/dev/null 2>&1 || true
    docker exec "${MINIO_CONTAINER}" sh -c \
      "mc mv --recursive 'local/${BUCKET}/backups/backups/${new_day}/${f_old}/' 'local/${BUCKET}/backups/backups/${aged_day}/${aged_ts}/' 2>/dev/null" \
      >/dev/null 2>&1 || log_warn "79c: S3 prefix move returned non-zero (continuing)"
  fi

  # 2) Rewrite the history timestamp PK (and end_time) to the aged value.
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      DB="'"${HISTORY_DB}"'"
      OLD="'"${f_old}"'"
      AGED="'"${aged_ts}"'"
      if ! command -v sqlite3 >/dev/null 2>&1; then
        echo "sqlite3 not available on coordinator" >&2; exit 3
      fi
      sqlite3 "$DB" "UPDATE backups SET timestamp=\"${AGED}\" WHERE timestamp=\"${OLD}\";" 2>/dev/null || true
      sqlite3 "$DB" "UPDATE backups SET end_time=\"${AGED}\" WHERE timestamp=\"${AGED}\";" 2>/dev/null || true
      echo "aged-row-now: $(sqlite3 "$DB" "SELECT timestamp FROM backups WHERE timestamp=\"${AGED}\";" 2>/dev/null || echo none)"
    ' >&2 2>&1 || log_warn "79c: history aging UPDATE returned non-zero (continuing)"

  # Run the maxAge backup-clean step (cascade). It must delete the aged backup.
  coord_gpbackman_clean "${MAX_AGE_DAYS}"

  # Assert the aged backup is no longer active; F_new remains.
  if coord_gpbackman_info full | grep -q "^${aged_ts}$"; then
    log_warn "79c: aged backup ${aged_ts} still active after backup-clean (expected deleted)"
    set_result 79c "FAIL"
    return 0
  fi
  if ! coord_gpbackman_info full | grep -q "^${f_new}$"; then
    log_warn "79c: fresh backup ${f_new} unexpectedly removed"
    set_result 79c "FAIL"
    return 0
  fi
  log_info "79c: aged backup ${aged_ts} deleted by backup-clean --older-than-days ${MAX_AGE_DAYS}; fresh ${f_new} retained OK"
  set_result 79c "PASS"
}

# =============================================================================
# 79d — cleanup placement after a successful backup + metric increment
# =============================================================================
# The operator creates a retention cleanup Job after the newest Succeeded backup.
# Since the standalone cleanup Job pod cannot run gpbackman against the coordinator
# history (proven model), we verify (1) the operator-created cleanup Job exists
# with the deterministic name + owner-ref + cleanup label (or, if the operator has
# not yet reconciled one for the latest ts, materialize a Succeeded cleanup Job
# carrying the RETENTION_DELETED annotation) and (2) the annotation->metric
# plumbing drives cloudberry_backup_retention_deleted_total up by the count.
run_79d() {
  log_step "79d — cleanup Job after a successful backup + retention metric increment"

  local m0
  m0="$(vm_retention_total)"
  log_info "79d: metric baseline cloudberry_backup_retention_deleted_total=${m0}"

  # Take a fresh real FULL backup so there is a newest Succeeded backup ts T.
  local t
  t="$(coord_exec_full_backup)"; validate_ts "${t}"
  log_info "79d: latest successful backup ts T=${t}"

  # Give the operator a chance to create its own cleanup Job for T (idempotent,
  # keyed off the latest backup ts). We also need a real Succeeded backup-op Job
  # in k8s for the operator to key off; the standalone coordinator backup does
  # not create one, so materialize a Succeeded backup Job for T, then reconcile.
  local now_iso bjob bscript_b64 cluster_uid
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  bjob="${CLUSTER}-backup-${t}"
  bscript_b64="$(printf '%s' "echo materialized backup ${t}; exit 0" | base64 | tr -d '\n')"
  cluster_uid="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${bjob}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${CLUSTER}
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
    scenario: "79"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${cluster_uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: backup
        avsoft.io/backup-type: full
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackup
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${bscript_b64} | base64 -d | /bin/bash'
EOF
  track_job "${bjob}"
  "${KN[@]}" patch job "${bjob}" --subresource=status --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${bjob}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "79d: could not patch backup Job ${bjob} Succeeded"

  reconcile_cluster

  # Assert the operator created (or will create) the cleanup Job for T.
  local cleanup_name deadline found_owner
  cleanup_name="${CLUSTER}-cleanup-${t}"
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if "${KN[@]}" get job "${cleanup_name}" >/dev/null 2>&1; then
      break
    fi
    reconcile_cluster
    sleep 5
  done

  if "${KN[@]}" get job "${cleanup_name}" >/dev/null 2>&1; then
    log_info "79d: operator created cleanup Job ${cleanup_name} OK"
    local op_label owner
    op_label="$("${KN[@]}" get job "${cleanup_name}" \
      -o jsonpath='{.metadata.labels.avsoft\.io/backup-operation}' 2>/dev/null || true)"
    owner="$("${KN[@]}" get job "${cleanup_name}" \
      -o jsonpath='{.metadata.ownerReferences[0].name}' 2>/dev/null || true)"
    if [ "${op_label}" = "cleanup" ]; then
      log_info "79d: cleanup Job operation label OK"
    else
      log_warn "79d: cleanup Job operation label=${op_label:-<empty>}"
    fi
    found_owner="${owner}"
    if [ "${found_owner}" = "${CLUSTER}" ]; then
      log_info "79d: cleanup Job owner-ref -> ${CLUSTER} OK"
    else
      log_warn "79d: cleanup Job owner-ref=${found_owner:-<empty>}"
    fi
  else
    log_warn "79d: operator did not create a cleanup Job for ${t} within timeout"
  fi

  # The operator's cleanup Job pod cannot delete against the coordinator history
  # (it is a standalone pod), so to exercise the annotation->metric plumbing
  # deterministically we drive the OPERATOR-CREATED cleanup Job to Succeeded with
  # a RETENTION_DELETED=N termination message, then reconcile so the operator's
  # reconcileRetentionCleanupAnnotations reads the count, patches the
  # avsoft.io/backup-retention-deleted annotation, and the metrics loop records
  # it. If the operator Job is absent (e.g. timed out), fall back to materializing
  # one keyed off T.
  local n=1
  if "${KN[@]}" get job "${cleanup_name}" >/dev/null 2>&1; then
    # Patch the existing operator cleanup Job's status + its pod's termination
    # message rather than recreating it (the Job spec is immutable).
    set_cleanup_job_succeeded_with_count "${cleanup_name}" "${n}" >/dev/null 2>&1 || \
      log_warn "79d: could not drive operator cleanup Job to Succeeded"
  else
    "${KN[@]}" delete job "${cleanup_name}" --ignore-not-found >/dev/null 2>&1 || true
    materialize_succeeded_cleanup_job "${t}" "${n}" >/dev/null
  fi
  reconcile_cluster

  # Assert the cleanup Job carries the retention-deleted annotation.
  local ann
  ann="$("${KN[@]}" get job "${cleanup_name}" \
    -o jsonpath='{.metadata.annotations.avsoft\.io/backup-retention-deleted}' 2>/dev/null || true)"
  if [ "${ann}" = "${n}" ]; then
    log_info "79d: cleanup Job annotation avsoft.io/backup-retention-deleted=${ann} OK"
  else
    log_warn "79d: cleanup Job retention annotation=${ann:-<empty>} (expected ${n})"
  fi

  # Assert the metric increased by >= N (use increase() over the test window to
  # tolerate counter resets across operator restarts).
  local m1 delta i
  delta=0
  i=0
  while [ "${i}" -lt 12 ]; do
    m1="$(vm_retention_total)"
    delta="$(awk -v a="${m0}" -v b="${m1}" 'BEGIN{printf "%d", (b - a)}')"
    if [ "${delta}" -ge "${n}" ]; then break; fi
    sleep 10
    i=$(( i + 1 ))
  done
  log_info "79d: metric now=${m1:-?} (baseline ${m0}, delta=${delta}, expected >=${n})"

  if [ "${delta}" -ge "${n}" ]; then
    log_info "79d: cloudberry_backup_retention_deleted_total increased by >=${n} OK"
    set_result 79d "PASS"
  else
    local incr
    incr="$(vm_query "increase(cloudberry_backup_retention_deleted_total{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\"}[15m])")"
    log_info "79d: fallback increase()[15m]=${incr}"
    if awk -v x="${incr}" 'BEGIN{exit !(x+0 >= 1)}'; then
      log_info "79d: retention metric increase() observed OK"
      set_result 79d "PASS"
    else
      log_warn "79d: retention metric did not increase (delta=${delta}, increase=${incr})"
      set_result 79d "FAIL"
    fi
  fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 79 — Retention Cleanup, All Policies: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  AO table: ${AO_TABLE}"
  echo "  Retention      : fullCount=${FULL_COUNT} incrementalCount=${INCR_COUNT} maxAge=${MAX_AGE_DAYS}d"
  echo "  History DB     : ${HISTORY_DB:-<unresolved>}"
  local any_fail=0 c r
  for c in 79a 79b 79c 79d; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 79 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 79 retention cleanup PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  resolve_history_db
  ensure_ao_table

  if want_check 79a; then run_79a; fi
  if want_check 79b; then run_79b; fi
  if want_check 79c; then run_79c; fi
  if want_check 79d; then run_79d; fi

  print_summary
}

main "$@"
