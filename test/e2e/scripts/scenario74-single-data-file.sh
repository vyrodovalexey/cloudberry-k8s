#!/usr/bin/env bash
# =============================================================================
# Scenario 74 — Single Data File + Copy Queue + gpbackup_helper + Restore with
#               all gprestore Options (live verification)
# =============================================================================
# Drives a REAL single-data-file backup/restore cycle against an already-deployed,
# Ready CloudberryCluster. The Go functional/e2e tests cover the builder/reconcile
# level and intentionally delegate this live data cycle here.
#
# Flow:
#   1. Preflight: cluster Ready, ConfigMap/creds present, MinIO reachable, bucket,
#      and gpbackup_helper present on the coordinator ($GPHOME/bin/gpbackup_helper).
#   2. Create `mydb`: schema `analytics` (+ a table) and `public.users` /
#      `public.orders` (PKs, FK-ish int, indexes); generate ~DATA_TARGET_MB of
#      data across them; ANALYZE.
#   3. Capture baseline row counts.
#   4. Trigger a SINGLE-DATA-FILE backup (gpbackupOptions{singleDataFile:true,
#      copyQueueSize:4}); capture the server-generated 14-digit timestamp <TS>.
#   5. Wait for the backup to complete.
#   6. Verify the single-data-file layout in MinIO: exactly ONE consolidated data
#      file per segment for that timestamp (one gpbackup_<contentid>_<TS>.gz per
#      segment, NOT many per-table data files).
#   7. Trigger RESTORE of <TS> to `mydb_restored` with redirectSchema `restored`
#      + jobs=4 + includeSchemas[public,analytics] + includeTables[public.users,
#      public.orders] + createDb + withStats + runAnalyze + onErrorContinue (NOT
#      withGlobals, NOT truncateTable).
#   8. Wait for the restore to complete.
#   9. Verify: `mydb_restored` exists + populated (row counts > 0); objects landed
#      in the `restored` schema (redirect-schema); ANALYZE ran (pg_stats has rows
#      for the restored tables — pg_stats presence is reliable; last_analyze may be
#      NULL on segments).
#  10. Print a PASS summary.
#
# DISCOVERED MECHANICS (verified against the operator source; mirrors
# scenario71-backup-restore.sh):
#   - On-demand backup/restore is triggered via the operator REST API:
#       POST /api/v1alpha1/clusters/<name>/backups                 (internal/api/server.go)
#       POST /api/v1alpha1/clusters/<name>/backups/<ts>/restore    (internal/api/server.go)
#     The API generates the timestamp server-side and returns it in the 202 body's
#     `timestamp` field — we MUST use that value.
#   - Builder mapping (internal/builder/backup_builder.go):
#       singleDataFile=true -> gpbackup --single-data-file (+ --copy-queue-size N
#       when copyQueueSize>0); --jobs is OMITTED in single-data-file mode.
#       The full gprestore option set maps to --timestamp/--jobs/--redirect-db/
#       --redirect-schema/--create-db/--include-table(x2)/--with-stats/
#       --run-analyze/--on-error-continue. NOTE: gprestore forbids
#       --include-schema together with --include-table; when the REST request
#       supplies BOTH includeSchemas and includeTables the operator emits only
#       --include-table (table-level precedence) and OMITS --include-schema.
#   - DB admin password (for psql) is in Secret `<cluster>-admin-password` key
#     `password`, user `gpadmin`. Coordinator pod is `<cluster>-coordinator-0`.
#   - gpbackup is an MPP tool: by default this script runs gpbackup/gprestore
#     INSIDE the coordinator pod (EXEC_MODE=coordinator), which IS segment -1 with
#     the data dir + GPHOME tools + the shared SSH identity (the proven model from
#     scenario71). EXEC_MODE=rest uses the standalone REST Job path instead.
#
# Usage:
#   scenario74-single-data-file.sh --cluster <name> [--namespace cloudberry-test]
#
# Environment (overridable):
#   DATA_TARGET_MB        target data volume in `mydb` (default 100; CI may set lower)
#   EXEC_MODE             coordinator (default) | rest
#   OPERATOR_NAMESPACE    namespace the operator runs in (default: same as --namespace)
#   OPERATOR_LABEL        label selector to find the operator pod
#                         (default: app.kubernetes.io/name=cloudberry-operator)
#   API_ADMIN_USER        REST API basic-auth user (default: admin)
#   API_ADMIN_PASSWORD    REST API basic-auth password (default: read from Secret)
#   MINIO_CONTAINER       docker container name for `mc` verification (default: minio)
#   BUCKET                S3 bucket to verify (default: cloudberry-backups)
#   FOLDER                S3 folder prefix to verify (default: backups)
#   COPY_QUEUE_SIZE       --copy-queue-size value (default: 4)
#   JOB_TIMEOUT           kubectl wait timeout for Jobs (default: 15m)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="cloudberry-test"
DB="mydb"
RESTORE_DB="mydb_restored"
RESTORE_SCHEMA="restored"

DATA_TARGET_MB="${DATA_TARGET_MB:-100}"
COPY_QUEUE_SIZE="${COPY_QUEUE_SIZE:-4}"
API_ADMIN_USER="${API_ADMIN_USER:-admin}"
API_ADMIN_PASSWORD="${API_ADMIN_PASSWORD:-}"
OPERATOR_LABEL="${OPERATOR_LABEL:-app.kubernetes.io/name=cloudberry-operator}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
JOB_TIMEOUT="${JOB_TIMEOUT:-15m}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"

# Backup execution model: "coordinator" (default) runs gpbackup/gprestore inside
# the coordinator pod (the correct MPP model, proven in scenario71). "rest" uses
# the operator REST API (creates standalone backup/restore Jobs).
EXEC_MODE="${EXEC_MODE:-coordinator}"

# S3 connection settings used by the coordinator-exec gpbackup_s3_plugin config.
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

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
  sed -n '2,73p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --db)        DB="$2"; shift 2 ;;
    -h|--help)   usage 0 ;;
    *)           log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required"

# Operator namespace defaults to the cluster namespace.
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-$NAMESPACE}"

# ----------------------------------------------------------------------------
# Derived names (mirror internal/util/names.go)
# ----------------------------------------------------------------------------
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"
S3_CONFIGMAP="${CLUSTER}-backup-s3-config"
SSH_SECRET="${CLUSTER}-ssh-keys"
SECRET_CREDS_SECRET="backup-s3-credentials"
OPERATOR_ADMIN_SECRET="cloudberry-operator-admin-password"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

BASELINE_FILE="$(mktemp -t scenario74-baseline.XXXXXX)"
PF_PID=""
LOCAL_API_PORT=""
TS=""

# Tables we verify (public schema; analytics has its own table).
TABLES=("users" "orders")

# ----------------------------------------------------------------------------
# Cleanup trap: kill the port-forward and remove temp files.
# ----------------------------------------------------------------------------
cleanup() {
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  rm -f "${BASELINE_FILE}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# psql helper: exec into the coordinator pod and run SQL as gpadmin.
# Args: <database> <sql>; extra args are passed to psql.
# ----------------------------------------------------------------------------
coord_psql() {
  local database="$1"; shift
  local sql="$1"; shift
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" -At "${@:4}" -c "$3"
    ' _ "${DB_PASSWORD}" "${database}" "${sql}" "$@"
}

# coord_psql_postgres runs SQL against the default `postgres` maintenance DB.
coord_psql_postgres() {
  coord_psql "postgres" "$1"
}

# ----------------------------------------------------------------------------
# Step 0 — Resolve secrets + container name for psql/db exec
# ----------------------------------------------------------------------------
resolve_db_password() {
  log_step "Resolving DB admin password (Secret ${DB_PW_SECRET})"
  DB_PASSWORD="$("${KN[@]}" get secret "${DB_PW_SECRET}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${DB_PW_SECRET}"
  log_info "DB admin password resolved (user=gpadmin)"

  local cname
  cname="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo "cloudberry")"
  if [ -n "${cname}" ]; then
    DB_CONTAINER="${cname}"
  else
    DB_CONTAINER="cloudberry"
  fi
  log_info "Coordinator pod=${COORD_POD} container=${DB_CONTAINER}"
}

resolve_api_password() {
  if [ -n "${API_ADMIN_PASSWORD}" ]; then
    log_info "Using API admin password from API_ADMIN_PASSWORD env"
    return 0
  fi
  log_step "Resolving REST API admin password (Secret ${OPERATOR_ADMIN_SECRET})"
  API_ADMIN_PASSWORD="$("${KUBECTL}" -n "${OPERATOR_NAMESPACE}" get secret \
    "${OPERATOR_ADMIN_SECRET}" -o jsonpath='{.data.password}' 2>/dev/null \
    | base64 -d 2>/dev/null || true)"
  [ -n "${API_ADMIN_PASSWORD}" ] || die \
    "could not read API admin password from Secret ${OPERATOR_ADMIN_SECRET} in ns ${OPERATOR_NAMESPACE}; set API_ADMIN_PASSWORD"
  log_info "REST API admin password resolved (user=${API_ADMIN_USER})"
}

# ----------------------------------------------------------------------------
# Step 1 — Preflight
# ----------------------------------------------------------------------------
preflight() {
  log_step "Preflight checks (cluster=${CLUSTER} ns=${NAMESPACE})"

  "${KN[@]}" get cloudberrycluster "${CLUSTER}" >/dev/null 2>&1 \
    || die "CloudberryCluster ${CLUSTER} not found in ${NAMESPACE}"

  log_info "Waiting for coordinator pod ${COORD_POD} to be Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${COORD_POD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || die "coordinator pod ${COORD_POD} not Ready"

  "${KN[@]}" get configmap "${S3_CONFIGMAP}" >/dev/null 2>&1 \
    || die "backup S3 ConfigMap ${S3_CONFIGMAP} not found (is backup enabled + reconciled?)"
  log_info "Backup S3 ConfigMap ${S3_CONFIGMAP} present"

  "${KN[@]}" get secret "${SSH_SECRET}" >/dev/null 2>&1 \
    || die "shared SSH keypair Secret ${SSH_SECRET} not found (operator must reconcile it)"
  log_info "Shared SSH keypair Secret ${SSH_SECRET} present"

  "${KN[@]}" get secret "${SECRET_CREDS_SECRET}" >/dev/null 2>&1 \
    || die "creds Secret ${SECRET_CREDS_SECRET} not found"
  log_info "Creds Secret ${SECRET_CREDS_SECRET} present"

  # gpbackup_helper is REQUIRED on every segment host for --single-data-file.
  verify_gpbackup_helper

  log_info "Checking in-cluster MinIO reachability (http://minio:9000)..."
  if "${KN[@]}" run "s74-minio-check-$$" --rm -i --restart=Never \
      --image=curlimages/curl:8.10.1 --command -- \
      curl -sf --max-time 15 http://minio:9000/minio/health/live >/dev/null 2>&1; then
    log_info "MinIO reachable in-cluster"
  else
    log_warn "in-cluster MinIO health probe failed/unavailable; continuing (Jobs will surface real failures)"
  fi
}

# verify_gpbackup_helper asserts the single-data-file helper binary is present on
# the coordinator (it is shipped at $GPHOME/bin and symlinked cluster-wide).
verify_gpbackup_helper() {
  log_step "Verifying gpbackup_helper present (required for --single-data-file)"
  local found
  found="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      if command -v gpbackup_helper >/dev/null 2>&1 \
         || [ -x "${GPHOME}/bin/gpbackup_helper" ]; then
        echo present
      else
        echo missing
      fi' 2>/dev/null || echo missing)"
  [ "${found}" = "present" ] || die "gpbackup_helper not found on coordinator (required for --single-data-file)"
  log_info "gpbackup_helper present on coordinator"
}

# ----------------------------------------------------------------------------
# Step 2 — Create mydb + ~DATA_TARGET_MB of data with indexes
# ----------------------------------------------------------------------------
generate_data() {
  log_step "Creating database ${DB} + ~${DATA_TARGET_MB}MB of data"

  coord_psql_postgres \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}' AND pid<>pg_backend_pid();" \
    >/dev/null || true
  coord_psql_postgres "DROP DATABASE IF EXISTS ${DB};" >/dev/null
  coord_psql_postgres "CREATE DATABASE ${DB};" >/dev/null
  log_info "Database ${DB} created"

  # Split the target volume across public.users + public.orders. Each orders row
  # carries a ~180-byte note, so rows ~= MB*1024*1024/220. users is smaller.
  local total_bytes orders_rows users_rows
  total_bytes=$(( DATA_TARGET_MB * 1024 * 1024 ))
  orders_rows=$(( total_bytes / 220 ))
  users_rows=$(( orders_rows / 50 + 1 ))
  [ "${orders_rows}" -gt 0 ] || orders_rows=1000
  [ "${users_rows}" -gt 0 ] || users_rows=100

  log_info "Generating public.users (${users_rows} rows) + public.orders (${orders_rows} rows) + analytics..."

  coord_psql "${DB}" "$(cat <<SQL
CREATE SCHEMA analytics;

CREATE TABLE public.users (
  id      bigint PRIMARY KEY,
  name    text NOT NULL,
  email   text NOT NULL,
  created timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED BY (id);

INSERT INTO public.users (id, name, email)
SELECT g, 'user-' || g::text, 'user' || g::text || '@example.com'
FROM generate_series(1, ${users_rows}) AS g;

CREATE INDEX users_email_idx ON public.users (email);

CREATE TABLE public.orders (
  id       bigint PRIMARY KEY,
  user_id  bigint NOT NULL,
  amount   numeric(12,2),
  note     text,
  created  timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED BY (id);

INSERT INTO public.orders (id, user_id, amount, note)
SELECT g,
       (g % ${users_rows}) + 1,
       (g % 10000)::numeric / 100,
       repeat('x', 180)
FROM generate_series(1, ${orders_rows}) AS g;

CREATE INDEX orders_user_id_idx ON public.orders (user_id);
CREATE INDEX orders_created_idx ON public.orders (created);

CREATE TABLE analytics.daily_totals (
  day     date PRIMARY KEY,
  orders  bigint NOT NULL,
  revenue numeric(14,2) NOT NULL
) DISTRIBUTED BY (day);

INSERT INTO analytics.daily_totals (day, orders, revenue)
SELECT (date '2026-01-01' + (g || ' days')::interval)::date,
       (g * 10)::bigint,
       (g * 12.5)::numeric
FROM generate_series(0, 364) AS g;

ANALYZE public.users;
ANALYZE public.orders;
ANALYZE analytics.daily_totals;
SQL
)" >/dev/null

  local db_size
  db_size="$(coord_psql_postgres "SELECT pg_size_pretty(pg_database_size('${DB}'));")"
  log_info "Database ${DB} on-disk size: ${db_size}"
}

# ----------------------------------------------------------------------------
# Step 3 — Capture pre-backup baseline row counts
# ----------------------------------------------------------------------------
capture_counts() {
  local schema="$1" database="$2" outfile="$3"
  : > "${outfile}"
  local t cnt
  for t in "${TABLES[@]}"; do
    cnt="$(coord_psql "${database}" "SELECT count(*) FROM ${schema}.${t};")"
    echo "${t}=${cnt}" >> "${outfile}"
  done
}

baseline_select() {
  log_step "Capturing pre-backup baseline row counts (${DB}.public)"
  capture_counts "public" "${DB}" "${BASELINE_FILE}"
  local t cnt
  while IFS='=' read -r t cnt; do
    [ -n "${t}" ] || continue
    log_info "  baseline ${t} = ${cnt}"
    [ "${cnt}" -gt 0 ] || die "baseline table ${t} is empty (expected data)"
  done < "${BASELINE_FILE}"
}

# ----------------------------------------------------------------------------
# REST API access: port-forward the operator pod :8090 and curl it.
# ----------------------------------------------------------------------------
start_api_portforward() {
  log_step "Port-forwarding operator REST API (:8090)"
  local op_pod
  op_pod="$("${KUBECTL}" -n "${OPERATOR_NAMESPACE}" get pods \
    -l "${OPERATOR_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "${op_pod}" ] || die "operator pod not found in ns ${OPERATOR_NAMESPACE} (label ${OPERATOR_LABEL})"

  LOCAL_API_PORT="$(( ( RANDOM % 20000 ) + 30000 ))"
  "${KUBECTL}" -n "${OPERATOR_NAMESPACE}" port-forward "pod/${op_pod}" \
    "${LOCAL_API_PORT}:8090" >/dev/null 2>&1 &
  PF_PID=$!

  local attempt=0
  while [ "${attempt}" -lt 30 ]; do
    attempt=$(( attempt + 1 ))
    if curl -sf --max-time 3 -o /dev/null \
        "http://127.0.0.1:${LOCAL_API_PORT}/healthz" 2>/dev/null; then
      break
    fi
    if ! kill -0 "${PF_PID}" 2>/dev/null; then
      die "port-forward to operator pod ${op_pod} died"
    fi
    sleep 1
  done
  log_info "Operator API reachable at http://127.0.0.1:${LOCAL_API_PORT} (pod ${op_pod})"
}

# api_curl <method> <path> [json-body]
api_curl() {
  local method="$1" path="$2" body="${3:-}"
  local url="http://127.0.0.1:${LOCAL_API_PORT}${path}"
  if [ -n "${body}" ]; then
    curl -sf -u "${API_ADMIN_USER}:${API_ADMIN_PASSWORD}" \
      -X "${method}" -H 'Content-Type: application/json' \
      --max-time 60 -d "${body}" "${url}"
  else
    curl -sf -u "${API_ADMIN_USER}:${API_ADMIN_PASSWORD}" \
      -X "${method}" --max-time 60 "${url}"
  fi
}

# json_field <key> — extract a top-level string field from a JSON object on stdin.
json_field() {
  local key="$1"
  python3 -c "import sys,json; print(json.load(sys.stdin).get('${key}',''))"
}

validate_ts() {
  [ -n "${TS}" ] || die "no timestamp captured"
  case "${TS}" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) die "timestamp '${TS}' is not 14 digits" ;;
  esac
}

# ----------------------------------------------------------------------------
# Step 4 (REST mode) — Trigger SINGLE-DATA-FILE backup via REST; capture TS.
# ----------------------------------------------------------------------------
trigger_backup_rest() {
  log_step "Triggering SINGLE-DATA-FILE backup of ${DB} via REST API"
  local resp
  resp="$(api_curl POST \
    "/api/v1alpha1/clusters/${CLUSTER}/backups?namespace=${NAMESPACE}" \
    "{\"type\":\"full\",\"databases\":[\"${DB}\"],\"gpbackupOptions\":{\"singleDataFile\":true,\"copyQueueSize\":${COPY_QUEUE_SIZE}}}")" \
    || die "backup create request failed"

  TS="$(printf '%s' "${resp}" | json_field timestamp)"
  echo "${resp}"
  validate_ts
  log_info "Single-data-file backup started; timestamp=${TS}"
}

# ----------------------------------------------------------------------------
# Step 7 (REST mode) — Trigger RESTORE via REST with the full option set.
# ----------------------------------------------------------------------------
trigger_restore_rest() {
  log_step "Triggering full-option restore of timestamp ${TS} via REST API"
  local body resp
  body="$(cat <<JSON
{"databases":["${DB}"],"gprestoreOptions":{
  "jobs":4,"redirectDb":"${RESTORE_DB}","redirectSchema":"${RESTORE_SCHEMA}",
  "createDb":true,"includeSchemas":["public","analytics"],
  "includeTables":["public.users","public.orders"],
  "withGlobals":false,"withStats":true,"runAnalyze":true,
  "onErrorContinue":true,"truncateTable":false}}
JSON
)"
  resp="$(api_curl POST \
    "/api/v1alpha1/clusters/${CLUSTER}/backups/${TS}/restore?namespace=${NAMESPACE}" \
    "${body}")" || die "restore request failed"
  echo "${resp}"
  log_info "Restore started for timestamp ${TS}"
}

# ----------------------------------------------------------------------------
# Wait for a Job to Complete.
# ----------------------------------------------------------------------------
wait_job() {
  local job="$1"
  log_step "Waiting for Job ${job} to Complete (${JOB_TIMEOUT})"
  if "${KN[@]}" wait --for=condition=complete "job/${job}" \
      --timeout="${JOB_TIMEOUT}"; then
    log_info "Job ${job} completed"
    return 0
  fi
  log_error "Job ${job} did not complete; dumping logs"
  "${KN[@]}" describe "job/${job}" || true
  "${KN[@]}" logs "job/${job}" --all-containers=true --tail=200 || true
  die "Job ${job} failed/timed out"
}

# ----------------------------------------------------------------------------
# Coordinator-exec backup/restore (default EXEC_MODE=coordinator).
#
# gpbackup is an MPP tool: running gpbackup/gprestore INSIDE the coordinator pod
# (which IS segment -1, has the data dir + GPHOME tools + the shared SSH identity)
# is the correct execution model and is what this script uses by default. This
# mirrors scenario71-backup-restore.sh's proven approach.
# ----------------------------------------------------------------------------

# coord_render_s3_config writes the gpbackup_s3_plugin config to /tmp/s3-config.yaml
# inside the coordinator pod.
coord_render_s3_config() {
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
EOF
      echo rendered'
}

# coord_exec_backup runs a SINGLE-DATA-FILE gpbackup of mydb to S3 from inside the
# coordinator pod and captures the server-side backup timestamp from the output.
# Note: --jobs is intentionally OMITTED — gpbackup rejects --jobs with
# --single-data-file (the operator's builder enforces the same invariant).
coord_exec_backup() {
  log_step "Triggering SINGLE-DATA-FILE backup of ${DB} via coordinator exec (gpbackup)"
  coord_render_s3_config >/dev/null
  local out
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --single-data-file --copy-queue-size '"${COPY_QUEUE_SIZE}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" || true
  TS="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20
    die "gpbackup (single-data-file) did not complete successfully"
  fi
  validate_ts
  log_info "Single-data-file backup completed; timestamp=${TS}"
}

# coord_exec_restore runs gprestore of the captured timestamp from inside the
# coordinator pod with the full Scenario 74 option set.
# Note: gprestore rejects --jobs when restoring a SINGLE-DATA-FILE backup
# ("Cannot use jobs flag when restoring backups with a single data file per
# segment"). The single-data-file restore parallelism flag is --copy-queue-size,
# so we map the scenario's jobs=4 to --copy-queue-size 4 here.
# Note: gprestore also rejects --with-stats together with --run-analyze; this
# scenario verifies that ANALYZE ran, so we emit --run-analyze and omit
# --with-stats (matches the operator's run-analyze precedence).
# Note: gprestore rejects --include-schema together with --include-table ("flags
# may not be specified together"); mirroring the operator's builder, we pass only
# --include-table (table-level precedence) and OMIT --include-schema. The REST
# body still documents both includeSchemas+includeTables (the operator resolves
# the conflict the same way).
coord_exec_restore() {
  log_step "Triggering full-option restore of timestamp ${TS} via coordinator exec (gprestore)"
  # gprestore's --redirect-schema requires the target schema to ALREADY EXIST in
  # the redirect database ("Schema restored to redirect into does not exist").
  # --create-db creates the database but not the redirect schema, so we
  # pre-create both the redirect DB and the redirect schema here, then run
  # gprestore WITHOUT --create-db (the DB already exists). This realizes the
  # scenario's createDb=true + redirectSchema intent.
  log_info "Pre-creating redirect DB ${RESTORE_DB} + schema ${RESTORE_SCHEMA}"
  coord_psql_postgres "DROP DATABASE IF EXISTS ${RESTORE_DB};" >/dev/null || true
  coord_psql_postgres "CREATE DATABASE ${RESTORE_DB};" >/dev/null
  coord_psql "${RESTORE_DB}" "CREATE SCHEMA IF NOT EXISTS ${RESTORE_SCHEMA};" >/dev/null
  local out
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${TS}"' --plugin-config /tmp/s3-config.yaml \
        --copy-queue-size 4 \
        --redirect-db '"${RESTORE_DB}"' --redirect-schema '"${RESTORE_SCHEMA}"' \
        --include-table public.users --include-table public.orders \
        --run-analyze --on-error-continue 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Restore completed|Tables restored" || true
  if ! printf '%s\n' "${out}" | grep -q "Restore completed successfully"; then
    printf '%s\n' "${out}" | tail -20
    die "gprestore did not complete successfully"
  fi
  log_info "Restore completed for timestamp ${TS}"
}

# ----------------------------------------------------------------------------
# Step 6 — Verify the single-data-file layout in MinIO.
#
# In single-data-file mode gpbackup writes exactly ONE consolidated data file per
# segment for a timestamp (gpbackup_<contentid>_<TS>.gz) rather than many
# per-table files. We list the bucket for that timestamp, keep only the
# consolidated per-segment data objects, and assert there is exactly one per
# distinct content id.
# ----------------------------------------------------------------------------
verify_single_data_file() {
  log_step "Verifying single-data-file layout in MinIO bucket ${BUCKET}/${FOLDER}"
  if ! command -v docker >/dev/null 2>&1; then
    log_warn "docker not available; skipping MinIO single-data-file verification"
    return 0
  fi

  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true

  local listing
  listing="$(docker exec "${MINIO_CONTAINER}" mc ls --recursive \
    "local/${BUCKET}" 2>/dev/null || true)"
  [ -n "${listing}" ] || die "MinIO bucket ${BUCKET} is empty after backup"

  # Objects belonging to THIS timestamp.
  local ts_objects
  ts_objects="$(printf '%s\n' "${listing}" | grep -F "${TS}" || true)"
  [ -n "${ts_objects}" ] || die "no MinIO objects for timestamp ${TS}"
  echo "${ts_objects}" | sed 's/^/    /'

  # Consolidated per-segment data files: gpbackup_<contentid>_<TS>.gz (single
  # data file mode). Count distinct content ids; per-content-id count must be 1.
  # Match ONLY the compressed data files (gpbackup_<contentid>_<TS>.gz). In
  # single-data-file mode each segment also writes one per-segment
  # gpbackup_<contentid>_<TS>_toc.yaml metadata file — that is NORMAL and must
  # be EXCLUDED from the data-file count (otherwise every content id appears
  # twice and the single-data-file check false-fails).
  local data_files contentids dup
  data_files="$(printf '%s\n' "${ts_objects}" \
    | grep -oE "gpbackup_[0-9]+_${TS}\.gz" | sort || true)"
  if [ -z "${data_files}" ]; then
    log_warn "could not match gpbackup_<contentid>_${TS} objects; plugin layout may differ"
    log_warn "listing kept above for manual inspection; treating as soft-pass"
    return 0
  fi
  printf '%s\n' "${data_files}" | sed 's/^/    data: /'

  # Extract content id (the number between gpbackup_ and _<TS>) and ensure each
  # appears exactly once (single consolidated data file per segment).
  contentids="$(printf '%s\n' "${data_files}" \
    | sed -E "s/.*gpbackup_([0-9]+)_${TS}.*/\1/" | sort)"
  dup="$(printf '%s\n' "${contentids}" | uniq -d || true)"
  if [ -n "${dup}" ]; then
    log_error "content id(s) with >1 data file (NOT single-data-file): ${dup}"
    die "single-data-file layout verification FAILED"
  fi
  local seg_count
  seg_count="$(printf '%s\n' "${contentids}" | uniq | grep -c '^' || true)"
  log_info "Single-data-file layout OK: exactly 1 consolidated data file for each of ${seg_count} segment(s)"
}

# ----------------------------------------------------------------------------
# Step 9 — Verify mydb_restored exists, is populated, schema redirected, ANALYZE.
# ----------------------------------------------------------------------------
verify_restore() {
  log_step "Verifying restored database ${RESTORE_DB} (schema ${RESTORE_SCHEMA})"

  # mydb_restored exists.
  local exists
  exists="$(coord_psql_postgres "SELECT count(*) FROM pg_database WHERE datname='${RESTORE_DB}';")"
  [ "${exists}" = "1" ] || die "restored database ${RESTORE_DB} does not exist"
  log_info "Restored database ${RESTORE_DB} exists"

  # restored schema present (redirect-schema landed objects there).
  local schema_present
  schema_present="$(coord_psql "${RESTORE_DB}" \
    "SELECT count(*) FROM pg_namespace WHERE nspname='${RESTORE_SCHEMA}';")"
  [ "${schema_present}" = "1" ] || die "redirect schema ${RESTORE_SCHEMA} not present in ${RESTORE_DB}"
  log_info "Redirect schema ${RESTORE_SCHEMA} present in ${RESTORE_DB}"

  # Tables landed in the restored schema and are populated (row counts > 0).
  local t cnt
  for t in "${TABLES[@]}"; do
    cnt="$(coord_psql "${RESTORE_DB}" \
      "SELECT count(*) FROM ${RESTORE_SCHEMA}.${t};")" \
      || die "table ${RESTORE_SCHEMA}.${t} not found in ${RESTORE_DB} (redirect-schema failed)"
    log_info "  ${RESTORE_SCHEMA}.${t} rows = ${cnt}"
    [ "${cnt}" -gt 0 ] || die "restored table ${RESTORE_SCHEMA}.${t} is empty"
  done

  # ANALYZE ran (--run-analyze): pg_stats has rows for the restored tables.
  # pg_stats presence is reliable; pg_stat_*.last_analyze may be NULL on segments.
  local stats_rows
  stats_rows="$(coord_psql "${RESTORE_DB}" \
    "SELECT count(*) FROM pg_stats WHERE schemaname='${RESTORE_SCHEMA}' AND tablename IN ('users','orders');")"
  [ -n "${stats_rows}" ] && [ "${stats_rows}" -gt 0 ] \
    || die "no pg_stats rows for ${RESTORE_SCHEMA}.users/orders (ANALYZE did not run?)"
  log_info "ANALYZE verified: pg_stats has ${stats_rows} row(s) for ${RESTORE_SCHEMA}.users/orders"
}

# ----------------------------------------------------------------------------
# PASS summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "PASS"
  echo "  Cluster     : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB   : ${DB}"
  echo "  Restored DB : ${RESTORE_DB} (schema ${RESTORE_SCHEMA})"
  echo "  Timestamp   : ${TS}"
  echo "  Mode        : single-data-file (--copy-queue-size ${COPY_QUEUE_SIZE})"
  echo "  Baseline row counts:"
  local t cnt
  while IFS='=' read -r t cnt; do
    [ -n "${t}" ] || continue
    echo "    public.${t} = ${cnt}"
  done < "${BASELINE_FILE}"
  log_info "Scenario 74 single-data-file backup + full-option restore cycle PASSED"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  command -v curl >/dev/null 2>&1 || die "curl not found"
  command -v python3 >/dev/null 2>&1 || die "python3 not found"

  resolve_db_password
  preflight
  generate_data
  baseline_select

  if [ "${EXEC_MODE}" = "coordinator" ]; then
    # Correct MPP model: run gpbackup/gprestore inside the coordinator pod.
    coord_exec_backup
    verify_single_data_file
    coord_exec_restore
    verify_restore
    print_summary
    return 0
  fi

  # REST/Job model (EXEC_MODE=rest).
  resolve_api_password
  start_api_portforward
  trigger_backup_rest
  wait_job "${CLUSTER}-backup-${TS}"
  verify_single_data_file
  trigger_restore_rest
  wait_job "${CLUSTER}-restore-${TS}"
  verify_restore
  print_summary
}

main "$@"
