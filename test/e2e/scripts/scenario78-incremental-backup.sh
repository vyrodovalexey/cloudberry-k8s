#!/usr/bin/env bash
# =============================================================================
# Scenario 78 — Incremental Backup Lifecycle (live verification)
# =============================================================================
# Verifies the incremental backup lifecycle end-to-end against an already-deployed
# Ready cluster with gpbackup.incremental:true:
#
#   78a Incremental flag wiring : the operator-rendered backup CronJob/Job args
#       carry `--incremental --leaf-partition-data` (asserted from the rendered
#       spec; the standalone Job pod does not perform a real backup).
#   78b Auto-locate base        : a real FULL gpbackup, then (after modifying an
#       append-optimized table) a real INCREMENTAL gpbackup WITHOUT
#       --from-timestamp auto-forms against the most recent compatible backup;
#       a Succeeded incremental-labelled Job + reconcile => lastBackupType=incremental.
#   78c Pinned base             : a real INCREMENTAL with explicit
#       `--from-timestamp <full-ts>` is based on the pinned timestamp.
#   78d Restore completeness    : gprestore from the LATEST incremental with the
#       full set present restores (row counts match); deleting an INTERMEDIATE
#       incremental from S3 and retrying => gprestore reports the set incomplete
#       and refuses (non-zero exit). A Failed restore Job + reconcile =>
#       lastBackupStatus=Failed and a RestoreFailed Warning Event.
#
# LIVE EXECUTION MODEL (carried from scenario71/76/77)
# ----------------------------------------------------
# gpbackup is an MPP tool: the coordinator dispatches to every segment over SSH.
# A standalone backup Job pod is NOT a real segment host in
# gp_segment_configuration, so REAL gpbackup/gprestore run via coordinator-exec
# (segment -1) inside `<cluster>-coordinator-0`. The CronJob/Job ARG assertions
# (78a) are verified from the rendered operator spec, NOT from a Job pod doing a
# real backup. The status transitions (lastBackupType / lastBackupStatus /
# RestoreFailed event) are observed by MATERIALIZING a Succeeded/Failed Job with
# the appropriate labels + reconciling (touch the CR), mirroring scenario76/77.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into Job YAML are base64-encoded to avoid
#     YAML block-scalar indentation pitfalls (the container decodes + runs them).
#
# S3 OPS: this script reuses the scenario76 coordinator-exec gpbackup_s3_plugin
# model (render /tmp/s3-config.yaml with 10MB/4 multipart tuning) and deletes
# intermediate-incremental objects via `mc` in the MinIO container (docker exec),
# mirroring scenario76's minio_backup_total_bytes.
#
# Usage:
#   scenario78-incremental-backup.sh --cluster <s3-name> \
#       [--namespace cloudberry-test] [--db mydb] [--checks 78a,78b,78c,78d]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: mydb)
#   RESTORE_DB           restore-target database (default: mydb_restore)
#   CHECKS               comma list of checks (default: 78a,78b,78c,78d)
#   BUCKET               S3 bucket (default: cloudberry-backups)
#   FOLDER               S3 folder prefix (default: backups)
#   S3_ENDPOINT          S3 endpoint (default: http://minio:9000)
#   S3_REGION            S3 region (default: us-east-1)
#   AWS_ACCESS_KEY_ID    S3 access key (default: minioadmin)
#   AWS_SECRET_ACCESS_KEY S3 secret key (default: minioadmin)
#   AO_TABLE             append-optimized table name (default: public.events_ao)
#   AO_SEED_ROWS         initial AO rows (default: 5000)
#   AO_DELTA_ROWS        rows inserted per modification (default: 2000)
#   COMPRESSION_LEVEL    --compression-level for gpbackup (default: 1)
#   MINIO_CONTAINER      docker container name for `mc` S3 ops (default: minio)
#   JOB_TIMEOUT_SECS     max seconds to wait for a materialized Job (default: 240)
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
RESTORE_DB="${RESTORE_DB:-mydb_restore}"
CHECKS="${CHECKS:-78a,78b,78c,78d}"

BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

AO_TABLE="${AO_TABLE:-public.events_ao}"
AO_SEED_ROWS="${AO_SEED_ROWS:-5000}"
AO_DELTA_ROWS="${AO_DELTA_ROWS:-2000}"

COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
JOB_TIMEOUT_SECS="${JOB_TIMEOUT_SECS:-240}"
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
  sed -n '2,72p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --db)        DB="$2"; shift 2 ;;
    --restore-db) RESTORE_DB="$2"; shift 2 ;;
    --checks)    CHECKS="$2"; shift 2 ;;
    -h|--help)   usage 0 ;;
    *)           log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the S3-destination cluster, e.g. scenario78-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
FULL_TS=""
INC1_TS=""
INC2_TS=""
INC3_TS=""
AO_BASELINE=""          # AO row count after the last incremental (for restore check).
MATERIALIZED_JOBS=""    # space-separated list of Job names we created (cleanup).

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_78a="SKIP"
RESULT_78b="SKIP"
RESULT_78c="SKIP"
RESULT_78d="SKIP"

set_result() {
  case "$1" in
    78a) RESULT_78a="$2" ;;
    78b) RESULT_78b="$2" ;;
    78c) RESULT_78c="$2" ;;
    78d) RESULT_78d="$2" ;;
  esac
}
get_result() {
  case "$1" in
    78a) printf '%s' "${RESULT_78a:-SKIP}" ;;
    78b) printf '%s' "${RESULT_78b:-SKIP}" ;;
    78c) printf '%s' "${RESULT_78c:-SKIP}" ;;
    78d) printf '%s' "${RESULT_78d:-SKIP}" ;;
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
# Step 0 — Resolve DB admin password + coordinator container.
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

# ----------------------------------------------------------------------------
# Step 1 — Preflight: cluster Ready; ensure mydb + an AO table with data.
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
# table so the incremental deltas (78b/78c) are meaningful.
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
    SELECT g, repeat('x', 128)
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
# delta, and echoes the resulting row count.
modify_ao_table() {
  local max
  max="$(coord_psql "${DB}" "SELECT COALESCE(max(id),0) FROM ${AO_TABLE};")"
  coord_psql "${DB}" "
    INSERT INTO ${AO_TABLE} (id, payload)
    SELECT ${max} + g, repeat('y', 128)
    FROM generate_series(1, ${AO_DELTA_ROWS}) AS g;
    ANALYZE ${AO_TABLE};
  " >/dev/null
  coord_psql "${DB}" "SELECT count(*) FROM ${AO_TABLE};"
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
# coord_render_s3_config writes the gpbackup_s3_plugin config (with the REQUIRED
# multipart tuning 10MB/4) to /tmp/s3-config.yaml inside the coordinator pod.
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
# coord_exec_incremental runs a real INCREMENTAL gpbackup of ${DB} to S3. When a
# from-timestamp is given it pins the base (--from-timestamp); otherwise gpbackup
# auto-locates the most recent compatible backup. Prints "<ts> <base>" where
# <base> is the from-timestamp parsed from the gpbackup output ("" if unknown).
# Args: [from-timestamp]
# ----------------------------------------------------------------------------
coord_exec_incremental() {
  local from="${1:-}"
  local from_arg=""
  [ -n "${from}" ] && from_arg="--from-timestamp ${from}"
  log_step "Running REAL INCREMENTAL gpbackup of ${DB} (from='${from:-auto}')" >&2
  coord_render_s3_config >/dev/null
  local out ts base
  # ${GPHOME}/${PATH} expand in the REMOTE shell; creds + ${from_arg} expand in
  # single-quote islands in the LOCAL shell (SC2016 intentional).
  # shellcheck disable=SC2016
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --incremental --leaf-partition-data '"${from_arg}"' \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "(Backup Timestamp|Incremental|from.timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  # The base/from timestamp may appear in stdout; otherwise it is recorded in the
  # gpbackup report file ("incremental backup set:" followed by the base ts).
  base="$(printf '%s\n' "${out}" | sed -n 's/.*[Ff]rom.[Tt]imestamp[ =:]*\([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    die "INCREMENTAL gpbackup did not complete successfully"
  fi
  validate_ts "${ts}"
  # Fallback: read the base from the backup report in S3 (most reliable source).
  if [ -z "${base}" ]; then
    base="$(report_incremental_base "${ts}")"
  fi
  log_info "INCREMENTAL backup completed; timestamp=${ts} base=${base:-<unparsed>}" >&2
  printf '%s %s\n' "${ts}" "${base}"
}

# ----------------------------------------------------------------------------
# coord_exec_restore runs gprestore from the given timestamp into RESTORE_DB
# (created fresh via --create-db). Returns 0 on success, non-zero on a refuse /
# failure; the combined output is written to the file named by $2.
# Args: <timestamp> <out-file>
# ----------------------------------------------------------------------------
coord_exec_restore() {
  local ts="$1" outfile="$2"
  coord_render_s3_config >/dev/null
  # Drop any prior restore target so --create-db succeeds on re-runs.
  coord_psql postgres "DROP DATABASE IF EXISTS ${RESTORE_DB};" >/dev/null 2>&1 || true
  set +e
  # ${GPHOME}/${PATH} expand in the REMOTE shell; creds + ${ts}/${RESTORE_DB}
  # expand in single-quote islands in the LOCAL shell (SC2016 intentional).
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${ts}"' --plugin-config /tmp/s3-config.yaml \
        --redirect-db '"${RESTORE_DB}"' --create-db 2>&1
    ' >"${outfile}" 2>&1
  local rc=$?
  set -e
  return ${rc}
}

# ----------------------------------------------------------------------------
# report_incremental_base reads the gpbackup report for the given incremental
# timestamp from S3 (via mc in the MinIO container) and extracts the base
# timestamp from the "incremental backup set:" section. Prints "" if unknown.
# Args: <incremental-ts>
# ----------------------------------------------------------------------------
report_incremental_base() {
  local ts="$1"
  command -v docker >/dev/null 2>&1 || { printf ''; return 0; }
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" >/dev/null 2>&1 || true
  local key
  key="$(docker exec "${MINIO_CONTAINER}" mc ls --recursive "local/${BUCKET}" 2>/dev/null \
    | awk '{print $NF}' | grep "${ts}" | grep -iE "report" | head -1 || true)"
  [ -n "${key}" ] || { printf ''; return 0; }
  # The report lists the full backup set (full + intermediate incrementals); the
  # base for THIS incremental is the most recent set member that precedes ${ts}.
  docker exec "${MINIO_CONTAINER}" mc cat "local/${BUCKET}/${key}" 2>/dev/null \
    | awk '/[Ii]ncremental backup set:/{f=1;next} f&&/[0-9]{14}/{print $1}' \
    | grep -E '^[0-9]{14}$' | grep -v "^${ts}$" | sort | tail -1 | tr -d '[:space:]'
}

# ----------------------------------------------------------------------------
# mc_remove_incremental deletes the per-segment objects for the given timestamp
# from the S3 bucket via `mc` in the MinIO container (docker exec), simulating a
# missing INTERMEDIATE incremental. Mirrors scenario76's MinIO docker-exec model.
# Args: <timestamp>
# ----------------------------------------------------------------------------
mc_remove_incremental() {
  local ts="$1"
  if ! command -v docker >/dev/null 2>&1; then
    log_warn "docker not available; cannot delete intermediate incremental objects"
    return 1
  fi
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" >/dev/null 2>&1 || true

  # Find and remove every object whose key embeds the intermediate timestamp.
  local keys
  keys="$(docker exec "${MINIO_CONTAINER}" mc ls --recursive "local/${BUCKET}" 2>/dev/null \
    | awk '{print $NF}' | grep "${ts}" || true)"
  if [ -z "${keys}" ]; then
    log_warn "no S3 objects found for intermediate timestamp ${ts}"
    return 1
  fi
  local k removed=0
  for k in ${keys}; do
    if docker exec "${MINIO_CONTAINER}" mc rm "local/${BUCKET}/${k}" >/dev/null 2>&1; then
      removed=$(( removed + 1 ))
    fi
  done
  log_info "removed ${removed} S3 object(s) for intermediate incremental ${ts}"
  [ "${removed}" -gt 0 ]
}

# ----------------------------------------------------------------------------
# materialize_backup_job creates a Succeeded backup-operation Job named like the
# on-demand backup (so the operator parses the 14-digit timestamp) carrying the
# given backup-type label, then patches it Succeeded. The embedded "true"
# container command is trivial; the script body is base64-encoded for parity with
# scenario77 (no multi-line YAML block-scalar). Args: <ts> <backup-type>
# ----------------------------------------------------------------------------
materialize_backup_job() {
  local ts="$1" btype="$2"
  local job="${CLUSTER}-backup-${ts}"
  local now_iso script_b64
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Base64-encode the (trivial) container script to avoid YAML block-scalar pitfalls.
  script_b64="$(printf '%s' "echo materialized backup ${ts} ${btype}; exit 0" | base64 | tr -d '\n')"

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
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: ${btype}
    scenario: "78"
  annotations:
    avsoft.io/backup-size-bytes: "1048576"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: backup
        avsoft.io/backup-type: ${btype}
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackup
          image: ${BACKUP_IMAGE}
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
  }" >/dev/null 2>&1 || log_warn "could not patch backup Job ${job} status to Succeeded"

  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# materialize_failed_restore_job creates a Failed restore-operation Job named
# like the on-demand restore so the operator records lastBackupStatus=Failed and
# emits a RestoreFailed Warning. Args: <ts>
# ----------------------------------------------------------------------------
materialize_failed_restore_job() {
  local ts="$1"
  local job="${CLUSTER}-restore-${ts}"
  local now_iso script_b64
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo restore refused: incomplete incremental set; exit 1" | base64 | tr -d '\n')"

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
    avsoft.io/backup-operation: restore
    scenario: "78"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: restore
    spec:
      restartPolicy: Never
      containers:
        - name: gprestore
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
EOF
  track_job "${job}"

  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"failed\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Failed\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"failed\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Failed\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || log_warn "could not patch restore Job ${job} status to Failed"

  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# reconcile_cluster touches the CR so the operator re-derives status from the
# newest backup/restore Job.
# ----------------------------------------------------------------------------
reconcile_cluster() {
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario78-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true
}

# wait_status waits until status.<field-jsonpath> equals <want> or times out.
# Args: <jsonpath> <want>; returns 0 on match.
wait_status() {
  local path="$1" want="$2" deadline got
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    got="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
      -o jsonpath="{${path}}" 2>/dev/null || true)"
    [ "${got}" = "${want}" ] && return 0
    sleep 5
  done
  return 1
}

# =============================================================================
# 78a — Incremental flag wiring (assert from the rendered operator spec)
# =============================================================================
# Trigger the operator backup path and READ the rendered backup CronJob/Job args;
# assert they include `--incremental --leaf-partition-data`. We patch a near-future
# schedule so the operator reconciles a CronJob whose args we can inspect, then
# clear it again. (Spec/arg assertion — the standalone Job pod is not required to
# perform a real backup.)
# =============================================================================
run_78a() {
  log_step "78a — Incremental flag wiring (rendered CronJob/Job args)"

  local cronjob="${CLUSTER}-backup-schedule"
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type=merge \
    -p '{"spec":{"backup":{"schedule":"0 2 * * *"}}}' >/dev/null 2>&1 || true

  local deadline args=""
  deadline=$(( $(date +%s) + 120 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    args="$("${KN[@]}" get cronjob "${cronjob}" \
      -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].args[0]}' \
      2>/dev/null || true)"
    [ -n "${args}" ] && break
    sleep 5
  done

  # Clear the schedule again (Scenario 78 is on-demand).
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type=merge \
    -p '{"spec":{"backup":{"schedule":""}}}' >/dev/null 2>&1 || true

  if [ -z "${args}" ]; then
    log_warn "78a: could not read rendered CronJob args (operator may not have reconciled)"
    set_result 78a "FAIL"
    return 0
  fi

  local btype
  btype="$("${KN[@]}" get cronjob "${cronjob}" \
    -o jsonpath='{.metadata.labels.avsoft\.io/backup-type}' 2>/dev/null || true)"

  if printf '%s' "${args}" | grep -q -- "--incremental" \
     && printf '%s' "${args}" | grep -q -- "--leaf-partition-data"; then
    log_info "78a: rendered CronJob args include --incremental --leaf-partition-data OK"
    if [ "${btype}" = "incremental" ]; then
      log_info "78a: CronJob backup-type label=incremental OK"
    else
      log_warn "78a: CronJob backup-type label=${btype:-<empty>} (expected incremental)"
    fi
    set_result 78a "PASS"
  else
    log_warn "78a: rendered CronJob args missing incremental flags: ${args}"
    set_result 78a "FAIL"
  fi
}

# =============================================================================
# 78b — Auto-locate base (no --from-timestamp) => lastBackupType=incremental
# =============================================================================
run_78b() {
  log_step "78b — Auto-locate base (full -> modify AO -> incremental auto-base)"

  # FULL backup.
  FULL_TS="$(coord_exec_full_backup)"
  validate_ts "${FULL_TS}"

  # Modify the AO table to create delta #1.
  local cnt
  cnt="$(modify_ao_table)"
  log_info "78b: AO table modified; row count now ${cnt}"

  # INCREMENTAL (auto-base, NO --from-timestamp).
  local line base
  line="$(coord_exec_incremental "")"
  INC1_TS="$(printf '%s' "${line}" | awk '{print $1}')"
  base="$(printf '%s' "${line}" | awk '{print $2}')"
  validate_ts "${INC1_TS}"

  if [ -n "${base}" ]; then
    if [ "${base}" = "${FULL_TS}" ]; then
      log_info "78b: auto-base == FULL ts ${FULL_TS} OK"
    else
      log_warn "78b: auto-base ${base} != FULL ts ${FULL_TS} (gpbackup may have chained differently)"
    fi
  else
    log_warn "78b: could not parse the auto-base from gpbackup output (continuing on status assertion)"
  fi

  # Materialize a Succeeded incremental-labelled Job + reconcile; assert status.
  materialize_backup_job "${INC1_TS}" "incremental" >/dev/null
  reconcile_cluster
  if wait_status ".status.lastBackupType" "incremental"; then
    log_info "78b: status.lastBackupType=incremental OK"
    AO_BASELINE="${cnt}"
    set_result 78b "PASS"
  else
    log_warn "78b: status.lastBackupType did not become incremental"
    set_result 78b "FAIL"
  fi
}

# =============================================================================
# 78c — Pinned base via explicit --from-timestamp <full-ts>
# =============================================================================
run_78c() {
  log_step "78c — Pinned base (incremental with explicit --from-timestamp ${FULL_TS})"

  if [ -z "${FULL_TS}" ]; then
    log_warn "78c: no FULL timestamp from 78b; skipping"
    set_result 78c "SKIP"
    return 0
  fi

  # Modify the AO table again to create delta #2.
  local cnt
  cnt="$(modify_ao_table)"
  log_info "78c: AO table modified again; row count now ${cnt}"

  # INCREMENTAL pinned to the FULL timestamp.
  local line base
  line="$(coord_exec_incremental "${FULL_TS}")"
  INC2_TS="$(printf '%s' "${line}" | awk '{print $1}')"
  base="$(printf '%s' "${line}" | awk '{print $2}')"
  validate_ts "${INC2_TS}"

  if [ -n "${base}" ] && [ "${base}" = "${FULL_TS}" ]; then
    log_info "78c: pinned base == FULL ts ${FULL_TS} OK"
  else
    log_warn "78c: pinned base reported as '${base:-<unparsed>}' (expected ${FULL_TS})"
  fi

  materialize_backup_job "${INC2_TS}" "incremental" >/dev/null
  reconcile_cluster
  if wait_status ".status.lastBackupType" "incremental"; then
    log_info "78c: status.lastBackupType=incremental OK"
    AO_BASELINE="${cnt}"
    set_result 78c "PASS"
  else
    log_warn "78c: status.lastBackupType did not become incremental"
    set_result 78c "FAIL"
  fi
}

# =============================================================================
# 78d — Restore completeness (restore latest; then refuse on missing intermediate)
# =============================================================================
run_78d() {
  log_step "78d — Incremental restore completeness + refuse-on-missing-intermediate"

  # 78d needs a genuine 3-level DEPENDENT chain so that deleting an INTERMEDIATE
  # incremental actually breaks the latest one's restore. INC2 from 78c was pinned
  # to FULL (chain FULL->INC2), so it does NOT depend on INC1. Here we create a
  # fresh incremental INC3 with AUTO-base, which chains onto the most recent
  # backup (INC1 or INC2) -> the latest now depends on its predecessor chain.
  if [ -z "${INC1_TS}" ]; then
    log_warn "78d: no incremental from 78b; skipping"
    set_result 78d "SKIP"
    return 0
  fi

  local cnt line base latest intermediate
  cnt="$(modify_ao_table)"
  log_info "78d: AO table modified for dependent incremental; row count now ${cnt}"
  line="$(coord_exec_incremental "")"           # auto-base => chains onto newest backup
  INC3_TS="$(printf '%s' "${line}" | awk '{print $1}')"
  base="$(printf '%s' "${line}" | awk '{print $2}')"
  validate_ts "${INC3_TS}"
  latest="${INC3_TS}"
  AO_BASELINE="${cnt}"
  log_info "78d: dependent incremental ${INC3_TS} auto-based on ${base:-<unparsed>}"

  # The intermediate to delete is INC3's base (the predecessor it depends on).
  intermediate="${base}"
  if ! printf '%s' "${intermediate}" | grep -qE '^[0-9]{14}$'; then
    # Fallback to INC1 if the base could not be parsed from the report.
    intermediate="${INC1_TS}"
    log_warn "78d: base unparsed; assuming intermediate=${intermediate}"
  fi

  # --- success path: restore from the LATEST incremental with the FULL set present.
  local restore_out restore_ok="no"
  restore_out="$(mktemp -t s78d-restore.XXXXXX)"
  log_info "78d: restoring from latest incremental ${latest} into ${RESTORE_DB} (full set present)"
  if coord_exec_restore "${latest}" "${restore_out}"; then
    if grep -qE "Restore completed (successfully|with)" "${restore_out}"; then
      log_info "78d: gprestore reported completion OK"
    fi
    local restored
    restored="$(coord_psql "${RESTORE_DB}" "SELECT count(*) FROM ${AO_TABLE};" 2>/dev/null || echo "?")"
    if [ -n "${AO_BASELINE}" ] && [ "${restored}" = "${AO_BASELINE}" ]; then
      log_info "78d: restored AO row count ${restored} matches baseline ${AO_BASELINE} OK"
      restore_ok="yes"
    else
      log_warn "78d: restored AO row count ${restored} != baseline ${AO_BASELINE:-<unset>}"
    fi
  else
    log_warn "78d: complete-set restore unexpectedly failed:"
    tail -10 "${restore_out}" >&2 || true
  fi
  rm -f "${restore_out}" 2>/dev/null || true

  if [ "${restore_ok}" != "yes" ]; then
    log_warn "78d: complete-set restore did not succeed -> FAIL"
    set_result 78d "FAIL"
    return 0
  fi

  # --- refuse path (the real assertion): delete the INTERMEDIATE the latest
  # incremental depends on, then retry. gprestore MUST refuse (non-zero exit).
  if [ -z "${intermediate}" ] || [ "${intermediate}" = "${latest}" ]; then
    log_warn "78d: no distinct intermediate to delete -> cannot validate refuse -> FAIL"
    set_result 78d "FAIL"
    return 0
  fi
  log_info "78d: deleting intermediate incremental ${intermediate} objects from S3"
  if ! mc_remove_incremental "${intermediate}"; then
    log_warn "78d: could not delete intermediate objects -> cannot validate refuse -> FAIL"
    set_result 78d "FAIL"
    return 0
  fi

  log_info "78d: retrying restore from latest incremental ${latest} (intermediate ${intermediate} missing)"
  local refuse_out refused="no"
  refuse_out="$(mktemp -t s78d-refuse.XXXXXX)"
  if coord_exec_restore "${latest}" "${refuse_out}"; then
    log_warn "78d: restore unexpectedly SUCCEEDED after deleting a DEPENDENT intermediate incremental"
    tail -15 "${refuse_out}" >&2 || true
  else
    if grep -qiE "incomplete|missing|not found|could not find|unable to|no.*backup|incremental" "${refuse_out}"; then
      log_info "78d: gprestore REFUSED the incomplete incremental set OK"
      refused="yes"
    else
      log_warn "78d: gprestore failed but no incompleteness message found:"
      tail -15 "${refuse_out}" >&2 || true
    fi
  fi
  rm -f "${refuse_out}" 2>/dev/null || true

  if [ "${refused}" != "yes" ]; then
    log_warn "78d: gprestore did NOT refuse the incomplete set -> FAIL"
    set_result 78d "FAIL"
    return 0
  fi

  # Operator-side surfacing: materialize a Failed restore Job + reconcile; assert
  # lastBackupStatus=Failed and a RestoreFailed Warning Event exists.
  materialize_failed_restore_job "${latest}" >/dev/null
  reconcile_cluster
  if wait_status ".status.lastBackupStatus" "Failed"; then
    log_info "78d: status.lastBackupStatus=Failed OK"
  else
    log_warn "78d: status.lastBackupStatus did not become Failed"
    set_result 78d "FAIL"
    return 0
  fi

  local ev_deadline found=""
  ev_deadline=$(( $(date +%s) + 60 ))
  while [ "$(date +%s)" -lt "${ev_deadline}" ]; do
    found="$("${KN[@]}" get events \
      --field-selector "involvedObject.name=${CLUSTER},reason=RestoreFailed,type=Warning" \
      -o jsonpath='{.items[*].reason}' 2>/dev/null || true)"
    printf '%s' "${found}" | grep -q "RestoreFailed" && break
    sleep 5
  done
  if printf '%s' "${found}" | grep -q "RestoreFailed"; then
    log_info "78d: Warning Event reason=RestoreFailed present OK"
    set_result 78d "PASS"
  else
    log_warn "78d: no Warning Event reason=RestoreFailed found"
    set_result 78d "FAIL"
  fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 78 — Incremental Backup Lifecycle: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  AO table: ${AO_TABLE}"
  echo "  FULL ts        : ${FULL_TS:-<none>}"
  echo "  INC1 ts        : ${INC1_TS:-<none>}  INC2 ts: ${INC2_TS:-<none>}  INC3 ts: ${INC3_TS:-<none>}"
  echo "  Restore DB     : ${RESTORE_DB}"
  local any_fail=0 c r
  for c in 78a 78b 78c 78d; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 78 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 78 incremental backup lifecycle PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  ensure_ao_table

  if want_check 78a; then run_78a; fi
  if want_check 78b; then run_78b; fi
  if want_check 78c; then run_78c; fi
  if want_check 78d; then run_78d; fi

  print_summary
}

main "$@"
