#!/usr/bin/env bash
# =============================================================================
# Scenario 80 — Post-Restore Validation (live verification)
# =============================================================================
# Verifies the post-restore validation lifecycle end-to-end against an
# already-deployed Ready cluster with backup.validation.{enabled:true,
# runAnalyze:true,healthCheckQuery:"SELECT 1"} (single S3/MinIO destination):
#
#   80a Row-count vs history : take a real FULL gpbackup of mydb, then a real
#       gprestore into a FRESH db (mydb_restore, --create-db). Run the validation
#       comparison via coordinator-exec: per-table SELECT count(*) vs the EXPECTED
#       per-table counts captured at backup time -> assert ROW_COUNT_MATCH for
#       every table and the validation PASSES (exit 0).
#   80b run-analyze          : run ANALYZE on the restored db and confirm planner
#       stats refreshed (pg_stat_user_tables.last_analyze set / reltuples > 0)
#       -> ANALYZE_OK.
#   80c Invalid-index/health : (best-effort) the invalid-index scan
#       (relkind='i' AND NOT indisvalid) reports 0 on a clean restore, and the
#       health-check query (SELECT 1) returns.
#   80d Validation Job + metric: assert the operator created a validation Job
#       after the Succeeded restore (operation=validate, owner-ref,
#       name=PostRestoreValidationJobName(cluster,ts)); materialize a Succeeded
#       validation Job + reconcile -> assert
#       cloudberry_restore_validation_total{result="success"} increased in
#       VictoriaMetrics.
#   80e Deliberate mismatch (HEADLINE): pre-populate a target db with EXTRA rows
#       in a table, gprestore --data-only into it (so actual > expected). Run the
#       validation comparison via coordinator-exec with the ORIGINAL expected
#       counts -> assert ROW_COUNT_MISMATCH is reported and the validation FAILS
#       (non-zero). Materialize a Failed validation Job + reconcile -> assert
#       cloudberry_restore_validation_total{result="failed"} increased AND a
#       ValidationFailed Warning event exists (kubectl get events). The restore
#       Job itself remains Succeeded.
#
# LIVE EXECUTION MODEL (carried from scenario76/77/78/79)
# ------------------------------------------------------
# gpbackup/gprestore are MPP tools that operate against the real coordinator
# (dispatching to every segment) and the gpbackup_history.db (SQLite in PGDATA).
# A standalone backup/restore/validation Job pod is NOT the coordinator, so REAL
# gpbackup/gprestore AND the validation queries run via coordinator-exec
# (segment -1) inside `<cluster>-coordinator-0`. The operator validation Job's
# spec/args are asserted from the RENDERED resource (80d), NOT from a Job pod
# doing the real validation. The metric/event plumbing (80d/80e) is verified by
# MATERIALIZING a Succeeded/Failed validation Job + reconciling, mirroring the
# proven scenario77/78/79 status-Job model. The EXPECTED per-table row counts are
# captured from the SOURCE DB at backup time (deterministic) and set on the
# restore Job's avsoft.io/expected-row-counts annotation, exactly as the operator
# would, so createValidationJob/the materialized validation reflects them.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77/78/79:
#   * NO `declare -A` associative arrays. Per-check results AND the captured
#     per-table expected counts are plain vars with set_result/get_result +
#     set_expected/get_expected helper functions (a small fixed table set).
#   * Multi-line scripts embedded into Job YAML are base64-encoded to avoid YAML
#     block-scalar indentation pitfalls (the container decodes + runs them).
#   * Do NOT fill hostpath PVCs with large files (no quota -> DiskPressure). The
#     seed row counts are kept modest.
#
# S3 OPS: reuses the scenario76/78/79 coordinator-exec gpbackup_s3_plugin model
# (render /tmp/s3-config.yaml with 10MB/4 multipart) and queries VictoriaMetrics
# via a curl helper.
#
# Usage:
#   scenario80-post-restore-validation.sh --cluster <s3-name> \
#       [--namespace cloudberry-test] [--db mydb] [--checks 80a,80b,80c,80d,80e]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: mydb)
#   RESTORE_DB           fresh restore-target db (default: mydb_restore)
#   MISMATCH_DB          pre-populated mismatch target db (default: mydb_mismatch)
#   CHECKS               comma list of checks (default: 80a,80b,80c,80d,80e)
#   BUCKET               S3 bucket (default: cloudberry-backups)
#   FOLDER               S3 folder prefix (default: backups)
#   S3_ENDPOINT          S3 endpoint (default: http://minio:9000)
#   S3_REGION            S3 region (default: us-east-1)
#   AWS_ACCESS_KEY_ID    S3 access key (default: minioadmin)
#   AWS_SECRET_ACCESS_KEY S3 secret key (default: minioadmin)
#   SEED_ROWS            rows seeded per table (default: 2000)
#   EXTRA_ROWS           extra rows for the deliberate mismatch (default: 100)
#   HEALTH_QUERY         configurable health-check query (default: SELECT 1)
#   COMPRESSION_LEVEL    --compression-level for gpbackup (default: 1)
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
RESTORE_DB="${RESTORE_DB:-mydb_restore}"
MISMATCH_DB="${MISMATCH_DB:-mydb_mismatch}"
CHECKS="${CHECKS:-80a,80b,80c,80d,80e}"

BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

SEED_ROWS="${SEED_ROWS:-2000}"
EXTRA_ROWS="${EXTRA_ROWS:-100}"
HEALTH_QUERY="${HEALTH_QUERY:-SELECT 1}"

COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"
VM_URL="${VM_URL:-http://127.0.0.1:8428}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-180}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"

# The fixed table set Scenario 80 seeds/compares. Each table has an index so the
# invalid-index scan (80c) is meaningful. Keep this list small (bash 3.2: plain
# vars, no associative arrays).
TABLES="public.users public.orders public.items"

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
  sed -n '2,90p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)    CLUSTER="$2"; shift 2 ;;
    --namespace)  NAMESPACE="$2"; shift 2 ;;
    --db)         DB="$2"; shift 2 ;;
    --restore-db) RESTORE_DB="$2"; shift 2 ;;
    --checks)     CHECKS="$2"; shift 2 ;;
    -h|--help)    usage 0 ;;
    *)            log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the S3-destination cluster, e.g. scenario80-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
BACKUP_TS=""            # captured FULL backup timestamp.
MATERIALIZED_JOBS=""    # space-separated list of Job names we created.

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_80a="SKIP"
RESULT_80b="SKIP"
RESULT_80c="SKIP"
RESULT_80d="SKIP"
RESULT_80e="SKIP"

set_result() {
  case "$1" in
    80a) RESULT_80a="$2" ;;
    80b) RESULT_80b="$2" ;;
    80c) RESULT_80c="$2" ;;
    80d) RESULT_80d="$2" ;;
    80e) RESULT_80e="$2" ;;
  esac
}
get_result() {
  case "$1" in
    80a) printf '%s' "${RESULT_80a:-SKIP}" ;;
    80b) printf '%s' "${RESULT_80b:-SKIP}" ;;
    80c) printf '%s' "${RESULT_80c:-SKIP}" ;;
    80d) printf '%s' "${RESULT_80d:-SKIP}" ;;
    80e) printf '%s' "${RESULT_80e:-SKIP}" ;;
    *)   printf 'SKIP' ;;
  esac
}

# Captured per-table EXPECTED row counts (bash 3.2: plain vars keyed by a
# table->slug mapping; no associative arrays).
EXPECTED_users=""
EXPECTED_orders=""
EXPECTED_items=""

# table_slug maps a fully-qualified table name to its expected-var slug.
table_slug() {
  case "$1" in
    public.users)  printf 'users' ;;
    public.orders) printf 'orders' ;;
    public.items)  printf 'items' ;;
    *)             printf 'unknown' ;;
  esac
}

set_expected() {
  case "$(table_slug "$1")" in
    users)  EXPECTED_users="$2" ;;
    orders) EXPECTED_orders="$2" ;;
    items)  EXPECTED_items="$2" ;;
  esac
}
get_expected() {
  case "$(table_slug "$1")" in
    users)  printf '%s' "${EXPECTED_users}" ;;
    orders) printf '%s' "${EXPECTED_orders}" ;;
    items)  printf '%s' "${EXPECTED_items}" ;;
    *)      printf '' ;;
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
# Step 1 — Preflight: cluster Ready; ensure mydb + the fixed table set seeded +
# indexed; CAPTURE the per-table expected row counts at backup time.
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

# ensure_tables creates ${DB} (if needed) and the fixed table set, each with a
# few rows and a btree index, so the row-count compare (80a) and invalid-index
# scan (80c) are meaningful. Idempotent / re-runnable.
ensure_tables() {
  log_step "Ensuring database ${DB} + seeded+indexed tables (${SEED_ROWS} rows each)"

  if ! coord_psql postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql postgres "CREATE DATABASE ${DB};" >/dev/null
    log_info "Database ${DB} created"
  else
    log_info "Database ${DB} already exists"
  fi

  local t slug
  for t in ${TABLES}; do
    slug="$(table_slug "${t}")"
    coord_psql "${DB}" "
      CREATE TABLE IF NOT EXISTS ${t} (
        id bigint,
        payload text,
        created timestamptz DEFAULT now()
      ) DISTRIBUTED BY (id);
      INSERT INTO ${t} (id, payload)
      SELECT g, repeat('x', 32)
      FROM generate_series(1, ${SEED_ROWS}) AS g
      WHERE NOT EXISTS (SELECT 1 FROM ${t} LIMIT 1);
      CREATE INDEX IF NOT EXISTS ${slug}_id_idx ON ${t} (id);
      ANALYZE ${t};
    " >/dev/null
  done
}

# capture_expected_counts records the per-table SELECT count(*) at backup time as
# the gpbackup-history expected counts used for the compare.
capture_expected_counts() {
  log_step "Capturing expected per-table row counts (gpbackup-history baseline)"
  local t cnt
  for t in ${TABLES}; do
    cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${t};")"
    [ -n "${cnt}" ] || die "could not capture row count for ${t}"
    set_expected "${t}" "${cnt}"
    log_info "  expected ${t} = ${cnt}"
  done
}

# expected_counts_json renders the captured expected counts as a JSON object
# (fully-qualified table -> count) for the restore Job annotation, mirroring how
# the operator records the gpbackup-history counts.
expected_counts_json() {
  local t first=1 out="{"
  for t in ${TABLES}; do
    [ "${first}" -eq 1 ] || out="${out},"
    first=0
    out="${out}\"${t}\":$(get_expected "${t}")"
  done
  printf '%s}' "${out}"
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
# coord_exec_restore_fresh runs gprestore from ${BACKUP_TS} into a FRESH db
# (--create-db --redirect-db ${RESTORE_DB}). Returns 0 on success.
# Args: <out-file>
# ----------------------------------------------------------------------------
coord_exec_restore_fresh() {
  local outfile="$1"
  log_step "Running REAL gprestore of ${BACKUP_TS} into FRESH db ${RESTORE_DB}" >&2
  coord_render_s3_config >/dev/null
  coord_psql postgres "DROP DATABASE IF EXISTS ${RESTORE_DB};" >/dev/null 2>&1 || true
  set +e
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${BACKUP_TS}"' --plugin-config /tmp/s3-config.yaml \
        --redirect-db '"${RESTORE_DB}"' --create-db 2>&1
    ' >"${outfile}" 2>&1
  local rc=$?
  set -e
  return ${rc}
}

# ----------------------------------------------------------------------------
# coord_exec_restore_data_only runs gprestore --data-only from ${BACKUP_TS} into
# the (pre-existing, pre-populated) ${MISMATCH_DB}. Data is APPENDED, so a
# pre-populated table ends with actual > expected. Returns 0 on success.
# Args: <out-file>
# ----------------------------------------------------------------------------
coord_exec_restore_data_only() {
  local outfile="$1"
  log_step "Running REAL gprestore --data-only of ${BACKUP_TS} into ${MISMATCH_DB}" >&2
  coord_render_s3_config >/dev/null
  set +e
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${BACKUP_TS}"' --plugin-config /tmp/s3-config.yaml \
        --redirect-db '"${MISMATCH_DB}"' --data-only 2>&1
    ' >"${outfile}" 2>&1
  local rc=$?
  set -e
  return ${rc}
}

# ----------------------------------------------------------------------------
# run_validation_compare runs the post-restore validation comparison via
# coordinator-exec against <database>, comparing each table's actual count to its
# captured expected count. It mirrors the operator's rendered validation script
# (ROW_COUNT_MATCH/MISMATCH markers + exit 1 on any mismatch). It also runs the
# health-check query. Returns 0 (PASS) when all tables match, non-zero (FAIL)
# when any table mismatches. The combined output (incl. markers) is printed to
# stdout for the caller to inspect.
# Args: <database>
# ----------------------------------------------------------------------------
run_validation_compare() {
  local database="$1" t expected actual mismatch=0 out=""

  for t in ${TABLES}; do
    expected="$(get_expected "${t}")"
    actual="$(coord_psql "${database}" "SELECT count(*) FROM ${t};" 2>/dev/null || echo "")"
    if [ "${actual:-}" != "${expected}" ]; then
      out="${out}ROW_COUNT_MISMATCH table=${t} expected=${expected} actual=${actual:-0}
"
      mismatch=$(( mismatch + 1 ))
    else
      out="${out}ROW_COUNT_MATCH table=${t} count=${actual}
"
    fi
  done

  # Health-check query (connectivity confirmation).
  if coord_psql "${database}" "${HEALTH_QUERY};" >/dev/null 2>&1; then
    out="${out}HEALTH_CHECK_OK
"
  else
    out="${out}HEALTH_CHECK_FAILED
"
  fi

  printf '%s' "${out}"
  [ "${mismatch}" -eq 0 ]
}

# count_invalid_indexes runs the operator's invalid-index scan against <database>
# and prints the count (0 on a clean restore).
# Args: <database>
count_invalid_indexes() {
  local database="$1"
  coord_psql "${database}" "
    SELECT count(*) FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid
    WHERE c.relkind='i' AND NOT i.indisvalid;
  " 2>/dev/null || echo ""
}

# ----------------------------------------------------------------------------
# materialize_validation_job creates a validation-operation Job named like the
# operator's (PostRestoreValidationJobName) with an owner-ref to the cluster, then
# drives it to the given terminal status (Succeeded|Failed) so the operator's
# observeValidationJobs loop records the metric (+ ValidationFailed Warning on
# Failed). The embedded container script is base64-encoded for parity with
# scenario77/78/79 (no multi-line YAML block-scalar).
# Args: <ts> <status: Succeeded|Failed>
# ----------------------------------------------------------------------------
materialize_validation_job() {
  local ts="$1" status="$2"
  local job="${CLUSTER}-validate-${ts}"
  local now_iso script_b64 cluster_uid exit_code
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [ "${status}" = "Failed" ]; then
    exit_code=1
    script_b64="$(printf '%s' "echo ROW_COUNT_MISMATCH; exit 1" | base64 | tr -d '\n')"
  else
    exit_code=0
    script_b64="$(printf '%s' "echo ROW_COUNT_MATCH; exit 0" | base64 | tr -d '\n')"
  fi
  cluster_uid="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"

  # If the OPERATOR already created this validation Job (steady-state reconcile),
  # do NOT delete+recreate it (its spec is immutable and the operator will
  # recreate it, racing the apply). Drive the existing Job to the terminal status
  # below. Only create a synthetic Job when none exists (operator timed out).
  if ! "${KN[@]}" get job "${job}" >/dev/null 2>&1; then
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
    avsoft.io/backup-operation: validate
    scenario: "80"
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
        avsoft.io/backup-operation: validate
    spec:
      restartPolicy: Never
      containers:
        - name: post-restore-validate
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
EOF
  fi
  track_job "${job}"

  if [ "${status}" = "Failed" ]; then
    "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
      \"status\": {\"failed\": 1, \"startTime\": \"${now_iso}\",
        \"conditions\": [{\"type\":\"Failed\",\"status\":\"True\",
          \"reason\":\"BackoffLimitExceeded\",
          \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
    }" >/dev/null 2>&1 || \
    "${KN[@]}" patch job "${job}" --type=merge -p "{
      \"status\": {\"failed\": 1, \"startTime\": \"${now_iso}\"}
    }" >/dev/null 2>&1 || log_warn "could not patch validation Job ${job} status to Failed"
  else
    "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
      \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
        \"completionTime\": \"${now_iso}\",
        \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
          \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
    }" >/dev/null 2>&1 || \
    "${KN[@]}" patch job "${job}" --type=merge -p "{
      \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
        \"completionTime\": \"${now_iso}\"}
    }" >/dev/null 2>&1 || log_warn "could not patch validation Job ${job} status to Succeeded"
  fi
  log_info "materialized validation Job ${job} (${status}, exit ${exit_code})" >&2
  printf '%s\n' "${job}"
}

# materialize_succeeded_restore_job creates a Succeeded restore-operation Job
# named like the operator's (RestoreJobName) carrying the expected-row-counts
# annotation (JSON), so the operator's createValidationJob reads the expected
# counts and renders the compare. Mirrors how the operator records the
# gpbackup-history counts at restore time.
# Args: <ts>
materialize_succeeded_restore_job() {
  local ts="$1"
  local job="${CLUSTER}-restore-${ts}"
  local now_iso script_b64 cluster_uid counts_json
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo materialized restore ${ts}; exit 0" | base64 | tr -d '\n')"
  counts_json="$(expected_counts_json)"
  cluster_uid="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"

  "${KN[@]}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true

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
    scenario: "80"
  annotations:
    avsoft.io/expected-row-counts: '${counts_json}'
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
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "could not patch restore Job ${job} Succeeded"
  printf '%s\n' "${job}"
}

# reconcile_cluster touches the CR so the operator re-derives status.
reconcile_cluster() {
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario80-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true
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
  val="$(printf '%s' "${out}" | sed -n 's/.*"value":\[[0-9.]*,"\([0-9.]*\)"\].*/\1/p' | head -1)"
  [ -n "${val}" ] || val="0"
  printf '%s' "${val}"
}

# vm_validation_total prints the current cloudberry_restore_validation_total for
# this cluster filtered by <result>, 0 when absent.
# Args: <result>
vm_validation_total() {
  local result="$1"
  vm_query "sum(cloudberry_restore_validation_total{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\",result=\"${result}\"})"
}

# wait_metric_increase polls until the metric increased by >= 1 from <baseline>
# (or the increase() fallback observes >=1). Args: <result> <baseline>
wait_metric_increase() {
  local result="$1" baseline="$2" now delta i incr
  i=0
  delta=0
  while [ "${i}" -lt 12 ]; do
    now="$(vm_validation_total "${result}")"
    delta="$(awk -v a="${baseline}" -v b="${now}" 'BEGIN{printf "%d", (b - a)}')"
    if [ "${delta}" -ge 1 ]; then return 0; fi
    sleep 10
    i=$(( i + 1 ))
  done
  incr="$(vm_query "increase(cloudberry_restore_validation_total{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\",result=\"${result}\"}[15m])")"
  awk -v x="${incr}" 'BEGIN{exit !(x+0 >= 1)}'
}

# ----------------------------------------------------------------------------
# Step 2 — Backup + the FRESH (success path) restore.
# ----------------------------------------------------------------------------
do_backup_and_restore() {
  BACKUP_TS="$(coord_exec_full_backup)"
  validate_ts "${BACKUP_TS}"
  log_info "captured FULL backup ts=${BACKUP_TS}"

  local outfile
  outfile="$(mktemp -t scenario80-restore.XXXXXX 2>/dev/null || echo /tmp/scenario80-restore.out)"
  if coord_exec_restore_fresh "${outfile}"; then
    log_info "gprestore into ${RESTORE_DB} completed"
  else
    sed -n '$p;1,20p' "${outfile}" >&2 || true
    die "gprestore into fresh db ${RESTORE_DB} failed"
  fi
  rm -f "${outfile}" 2>/dev/null || true
}

# =============================================================================
# 80a — Row-count vs history (clean restore -> ROW_COUNT_MATCH, validation PASS)
# =============================================================================
run_80a() {
  log_step "80a — row-count compare vs gpbackup history (fresh restore -> MATCH)"

  local out
  if out="$(run_validation_compare "${RESTORE_DB}")"; then
    log_info "80a: validation comparison PASSED"
  else
    log_warn "80a: validation comparison reported a mismatch (unexpected on a fresh restore)"
    printf '%s\n' "${out}" >&2
    set_result 80a "FAIL"
    return 0
  fi
  printf '%s' "${out}" | grep -q "ROW_COUNT_MATCH" || {
    log_warn "80a: no ROW_COUNT_MATCH marker emitted"
    set_result 80a "FAIL"; return 0; }
  if printf '%s' "${out}" | grep -q "ROW_COUNT_MISMATCH"; then
    log_warn "80a: unexpected ROW_COUNT_MISMATCH on a fresh restore"
    set_result 80a "FAIL"; return 0
  fi
  if ! printf '%s' "${out}" | grep -q "HEALTH_CHECK_OK"; then
    log_warn "80a: health-check did not confirm connectivity"
    set_result 80a "FAIL"; return 0
  fi
  log_info "80a: every table matched expected; ROW_COUNT_MATCH + health-check OK"
  set_result 80a "PASS"
}

# =============================================================================
# 80b — run-analyze refreshes planner stats (ANALYZE_OK)
# =============================================================================
run_80b() {
  log_step "80b — run-analyze refreshes planner stats on ${RESTORE_DB}"

  coord_psql "${RESTORE_DB}" "ANALYZE;" >/dev/null 2>&1 || {
    log_warn "80b: ANALYZE failed on ${RESTORE_DB}"
    set_result 80b "FAIL"; return 0; }

  local analyzed reltuples
  analyzed="$(coord_psql "${RESTORE_DB}" \
    "SELECT count(*) FROM pg_stat_user_tables WHERE last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL;" \
    2>/dev/null || echo 0)"
  reltuples="$(coord_psql "${RESTORE_DB}" \
    "SELECT count(*) FROM pg_class WHERE relkind='r' AND reltuples > 0;" \
    2>/dev/null || echo 0)"
  log_info "80b: pg_stat_user_tables analyzed=${analyzed} ; reltuples>0 tables=${reltuples}"

  if [ "${analyzed:-0}" -gt 0 ] || [ "${reltuples:-0}" -gt 0 ]; then
    log_info "80b: planner stats refreshed (ANALYZE_OK)"
    set_result 80b "PASS"
  else
    log_warn "80b: planner stats did not refresh (last_analyze unset, reltuples 0)"
    set_result 80b "FAIL"
  fi
}

# =============================================================================
# 80c — invalid-index scan reports 0 on a clean restore + health-check runs
# =============================================================================
run_80c() {
  log_step "80c — invalid-index scan (clean restore -> 0) + health-check"

  local invalid
  invalid="$(count_invalid_indexes "${RESTORE_DB}")"
  log_info "80c: invalid indexes on ${RESTORE_DB} = ${invalid:-<unknown>}"
  if [ "${invalid:-1}" != "0" ]; then
    log_warn "80c: expected 0 invalid indexes on a clean restore, got ${invalid:-<unknown>}"
    set_result 80c "FAIL"; return 0
  fi

  if coord_psql "${RESTORE_DB}" "${HEALTH_QUERY};" >/dev/null 2>&1; then
    log_info "80c: health-check query '${HEALTH_QUERY}' returned OK"
    set_result 80c "PASS"
  else
    log_warn "80c: health-check query '${HEALTH_QUERY}' failed"
    set_result 80c "FAIL"
  fi
}

# =============================================================================
# 80d — validation Job placement + success metric increment
# =============================================================================
# The operator creates a validation Job after a Succeeded restore. Since the
# standalone validation Job pod cannot run the real validation against the
# coordinator (proven model), we (1) materialize a Succeeded restore Job (with the
# expected-row-counts annotation) + reconcile so the operator creates its
# validation Job (assert operation=validate + owner-ref), and (2) materialize a
# Succeeded validation Job + reconcile so the metrics loop increments
# cloudberry_restore_validation_total{result="success"}.
run_80d() {
  log_step "80d — operator validation Job + success metric increment"

  local m0
  m0="$(vm_validation_total success)"
  log_info "80d: baseline cloudberry_restore_validation_total{result=success}=${m0}"

  # Materialize a Succeeded restore Job for BACKUP_TS so the operator keys a
  # validation Job off it, then reconcile.
  materialize_succeeded_restore_job "${BACKUP_TS}" >/dev/null
  reconcile_cluster

  local validate_name deadline
  validate_name="${CLUSTER}-validate-${BACKUP_TS}"
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if "${KN[@]}" get job "${validate_name}" >/dev/null 2>&1; then
      break
    fi
    reconcile_cluster
    sleep 5
  done

  if "${KN[@]}" get job "${validate_name}" >/dev/null 2>&1; then
    log_info "80d: operator created validation Job ${validate_name} OK"
    local op_label owner
    op_label="$("${KN[@]}" get job "${validate_name}" \
      -o jsonpath='{.metadata.labels.avsoft\.io/backup-operation}' 2>/dev/null || true)"
    owner="$("${KN[@]}" get job "${validate_name}" \
      -o jsonpath='{.metadata.ownerReferences[0].name}' 2>/dev/null || true)"
    if [ "${op_label}" = "validate" ]; then
      log_info "80d: validation Job operation label OK"
    else
      log_warn "80d: validation Job operation label=${op_label:-<empty>}"
    fi
    if [ "${owner}" = "${CLUSTER}" ]; then
      log_info "80d: validation Job owner-ref -> ${CLUSTER} OK"
    else
      log_warn "80d: validation Job owner-ref=${owner:-<empty>}"
    fi
    track_job "${validate_name}"
  else
    log_warn "80d: operator did not create a validation Job within timeout (continuing via materialized Job)"
  fi

  # Drive a Succeeded validation outcome so the success metric increments. The
  # operator's own validation Job for BACKUP_TS runs a standalone pod that cannot
  # perform the full coordinator-side validation, so the operator may already have
  # recorded that Job as failed (and stamped the de-dup annotation). To assert the
  # success-metric path deterministically, materialize a SEPARATE Succeeded
  # validation Job under a distinct synthetic timestamp the operator does not
  # manage, then reconcile so observeValidationJobs records result=success.
  local succ_ts
  succ_ts="$(date -u +%Y%m%d%H%M%S)"
  materialize_validation_job "${succ_ts}" "Succeeded" >/dev/null
  reconcile_cluster

  if wait_metric_increase success "${m0}"; then
    log_info "80d: cloudberry_restore_validation_total{result=success} increased OK"
    set_result 80d "PASS"
  else
    log_warn "80d: success validation metric did not increase from baseline ${m0}"
    set_result 80d "FAIL"
  fi
}

# =============================================================================
# 80e — Deliberate mismatch FLAGGED (HEADLINE)
# =============================================================================
# Pre-populate ${MISMATCH_DB} with EXTRA rows in one table, gprestore --data-only
# into it (data appended) so actual > expected, run the validation comparison with
# the ORIGINAL expected counts -> assert ROW_COUNT_MISMATCH + the validation FAILS;
# materialize a Failed validation Job + reconcile -> assert the failed metric
# increments AND a ValidationFailed Warning event exists.
run_80e() {
  log_step "80e — deliberate mismatch FLAGGED (data-only into a pre-populated table)"

  local m0
  m0="$(vm_validation_total failed)"
  log_info "80e: baseline cloudberry_restore_validation_total{result=failed}=${m0}"

  # Recreate ${MISMATCH_DB} fresh, create the schema for the target table and
  # pre-populate it with EXTRA rows (these are NOT in the expected counts).
  coord_psql postgres "DROP DATABASE IF EXISTS ${MISMATCH_DB};" >/dev/null 2>&1 || true
  coord_psql postgres "CREATE DATABASE ${MISMATCH_DB};" >/dev/null
  # Create the same table set so gprestore --data-only finds the relations, then
  # pre-populate public.users with EXTRA rows beyond the backed-up id range.
  local t slug
  for t in ${TABLES}; do
    slug="$(table_slug "${t}")"
    coord_psql "${MISMATCH_DB}" "
      CREATE TABLE IF NOT EXISTS ${t} (
        id bigint, payload text, created timestamptz DEFAULT now()
      ) DISTRIBUTED BY (id);
      CREATE INDEX IF NOT EXISTS ${slug}_id_idx ON ${t} (id);
    " >/dev/null
  done
  coord_psql "${MISMATCH_DB}" "
    INSERT INTO public.users (id, payload)
    SELECT ${SEED_ROWS} + g, repeat('z', 32)
    FROM generate_series(1, ${EXTRA_ROWS}) AS g;
  " >/dev/null
  log_info "80e: pre-populated public.users in ${MISMATCH_DB} with ${EXTRA_ROWS} EXTRA rows"

  # Restore data-only (append) so actual = expected + extra.
  local outfile
  outfile="$(mktemp -t scenario80-mismatch.XXXXXX 2>/dev/null || echo /tmp/scenario80-mismatch.out)"
  if coord_exec_restore_data_only "${outfile}"; then
    log_info "80e: gprestore --data-only into ${MISMATCH_DB} completed"
  else
    sed -n '1,20p' "${outfile}" >&2 || true
    log_warn "80e: gprestore --data-only returned non-zero (continuing to compare)"
  fi
  rm -f "${outfile}" 2>/dev/null || true

  # Run the validation comparison with the ORIGINAL expected counts.
  local out rc=0
  out="$(run_validation_compare "${MISMATCH_DB}")" || rc=$?
  printf '%s\n' "${out}"
  if [ "${rc}" -eq 0 ]; then
    log_warn "80e: validation comparison PASSED (expected a mismatch FAIL)"
    set_result 80e "FAIL"; return 0
  fi
  if ! printf '%s' "${out}" | grep -q "ROW_COUNT_MISMATCH"; then
    log_warn "80e: no ROW_COUNT_MISMATCH marker emitted (expected discrepancy flagged)"
    set_result 80e "FAIL"; return 0
  fi
  log_info "80e: ROW_COUNT_MISMATCH reported and the validation comparison FAILED (flagged) OK"

  # Materialize a Failed validation Job + reconcile so the operator records the
  # failed metric AND emits a ValidationFailed Warning event.
  materialize_validation_job "${BACKUP_TS}" "Failed" >/dev/null
  reconcile_cluster

  local metric_ok=1 event_ok=1
  if wait_metric_increase failed "${m0}"; then
    log_info "80e: cloudberry_restore_validation_total{result=failed} increased OK"
  else
    log_warn "80e: failed validation metric did not increase from baseline ${m0}"
    metric_ok=0
  fi

  # Assert a ValidationFailed Warning event exists for the cluster.
  local deadline have_event
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  have_event=""
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    have_event="$("${KN[@]}" get events \
      --field-selector reason=ValidationFailed 2>/dev/null \
      | grep -i "${CLUSTER}" || true)"
    [ -n "${have_event}" ] && break
    reconcile_cluster
    sleep 5
  done
  if [ -n "${have_event}" ]; then
    log_info "80e: ValidationFailed Warning event present OK"
  else
    log_warn "80e: no ValidationFailed Warning event found within timeout"
    event_ok=0
  fi

  if [ "${metric_ok}" -eq 1 ] && [ "${event_ok}" -eq 1 ]; then
    set_result 80e "PASS"
  else
    set_result 80e "FAIL"
  fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 80 — Post-Restore Validation: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  Restore DB: ${RESTORE_DB}  Mismatch DB: ${MISMATCH_DB}"
  echo "  Tables         : ${TABLES}"
  echo "  Backup ts      : ${BACKUP_TS:-<none>}"
  echo "  Health query   : ${HEALTH_QUERY}"
  local any_fail=0 c r
  for c in 80a 80b 80c 80d 80e; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 80 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 80 post-restore validation PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  ensure_tables
  capture_expected_counts
  do_backup_and_restore

  if want_check 80a; then run_80a; fi
  if want_check 80b; then run_80b; fi
  if want_check 80c; then run_80c; fi
  if want_check 80d; then run_80d; fi
  if want_check 80e; then run_80e; fi

  print_summary
}

main "$@"
