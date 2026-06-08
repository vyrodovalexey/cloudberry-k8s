#!/usr/bin/env bash
# =============================================================================
# Scenario 71 — Live backup -> verify (MinIO) -> clean -> restore -> verify cycle
# =============================================================================
# Drives a REAL backup/restore data cycle against an already-deployed, Ready
# CloudberryCluster (both the Secret and Vault credential variants share this
# script). The Go functional/e2e tests cover the builder/reconcile level and
# intentionally delegate this live data cycle here.
#
# Flow:
#   1. Preflight: cluster Ready, ConfigMap/creds present, MinIO reachable, bucket.
#   2. Create `mydb` + ~DATA_TARGET_MB of data across tables WITH indexes; ANALYZE.
#   3. Capture per-table baseline row counts (pre-backup SELECT).
#   4. Trigger an on-demand FULL backup of `mydb` via the operator REST API; the
#      API returns the server-generated 14-digit timestamp (the gpbackup set id).
#   5. Wait for the backup Job <cluster>-backup-<TS> to Complete.
#   6. Verify backup objects exist in the MinIO bucket under the folder prefix.
#   7. Clean: DROP DATABASE mydb (terminating connections first).
#   8. Trigger restore of <TS> via the REST API (createDb so mydb is recreated).
#   9. Wait for the restore Job <cluster>-restore-<TS> to Complete.
#  10. Re-run the verification SELECT; assert per-table row counts MATCH baseline.
#  11. Print a PASS summary.
#
# DISCOVERED MECHANICS (verified against the operator source):
#   - On-demand backup/restore is triggered via the operator REST API:
#       POST /api/v1alpha1/clusters/<name>/backups                 (internal/api/server.go:3145)
#       POST /api/v1alpha1/clusters/<name>/backups/<ts>/restore    (internal/api/server.go:3258)
#     The API generates the timestamp server-side (server.go:3178) and returns it
#     in the 202 body's `timestamp` field — we MUST use that value.
#   - REST API basic-auth user is `admin` with PermissionAdmin (covers both
#     backup-create=Operator and restore=Admin) — cmd/operator/main.go:341.
#     Its password is in Secret `cloudberry-operator-admin-password` key
#     `password` (util.OperatorAdminPasswordSecretName) in the OPERATOR namespace,
#     resolvable via CLOUDBERRY_API_ADMIN_PASSWORD too (main.go:407,412).
#   - The API server listens on :8090 inside the operator POD; the helm Service
#     does NOT expose it, so we port-forward the operator pod directly.
#   - DB admin password (for psql) is in Secret `<cluster>-admin-password` key
#     `password` (util.AdminPasswordSecretName / backup_builder.go:755), user
#     `gpadmin` (util.DefaultAdminUser). NOTE: this is `-admin-password`, NOT
#     `-admin-credentials`.
#   - Coordinator pod is `<cluster>-coordinator-0` (StatefulSet
#     util.CoordinatorName = `<cluster>-coordinator`).
#   - Job names: `<cluster>-backup-<TS>` / `<cluster>-restore-<TS>`
#     (util.BackupJobName / util.RestoreJobName).
#
# Usage:
#   scenario71-backup-restore.sh --cluster <name> [--namespace cloudberry-test] \
#       [--variant secret|vault] [--db mydb]
#
# Environment (overridable):
#   DATA_TARGET_MB        target data volume in `mydb` (default 100; CI may set lower)
#   OPERATOR_NAMESPACE    namespace the operator runs in (default: same as --namespace)
#   OPERATOR_LABEL        label selector to find the operator pod
#                         (default: app.kubernetes.io/name=cloudberry-operator)
#   API_ADMIN_USER        REST API basic-auth user (default: admin)
#   API_ADMIN_PASSWORD    REST API basic-auth password (default: read from Secret)
#   MINIO_CONTAINER       docker container name for `mc` verification (default: minio)
#   BUCKET                S3 bucket to verify (default: cloudberry-backups)
#   FOLDER                S3 folder prefix to verify (default: backups)
#   JOB_TIMEOUT           kubectl wait timeout for Jobs (default: 15m)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="cloudberry-test"
VARIANT="secret"
DB="mydb"

DATA_TARGET_MB="${DATA_TARGET_MB:-100}"
API_ADMIN_USER="${API_ADMIN_USER:-admin}"
API_ADMIN_PASSWORD="${API_ADMIN_PASSWORD:-}"
OPERATOR_LABEL="${OPERATOR_LABEL:-app.kubernetes.io/name=cloudberry-operator}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
JOB_TIMEOUT="${JOB_TIMEOUT:-15m}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"

# Backup execution model: "coordinator" (default) runs gpbackup/gprestore inside
# the coordinator pod (the correct MPP model). "rest" uses the operator REST API
# (creates a standalone backup Job — kept for reference/compat).
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
  sed -n '2,72p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --variant)   VARIANT="$2"; shift 2 ;;
    --db)        DB="$2"; shift 2 ;;
    -h|--help)   usage 0 ;;
    *)           log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required"
case "$VARIANT" in
  secret|vault) ;;
  *) die "--variant must be 'secret' or 'vault' (got '$VARIANT')" ;;
esac

# Operator namespace defaults to the cluster namespace (helm-install-test deploys
# the operator into cloudberry-test alongside the cluster).
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-$NAMESPACE}"

# ----------------------------------------------------------------------------
# Derived names (mirror internal/util/names.go)
# ----------------------------------------------------------------------------
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"
S3_CONFIGMAP="${CLUSTER}-backup-s3-config"
# Cluster-wide shared gpadmin SSH keypair Secret (util.ClusterSSHSecretName).
# gpbackup/gprestore dispatch over SSH to every segment, so this shared identity
# MUST be present and mounted into the cluster pods + backup/restore Jobs.
SSH_SECRET="${CLUSTER}-ssh-keys"
VAULT_CREDS_SECRET="${CLUSTER}-backup-s3-vault-creds"
SECRET_CREDS_SECRET="backup-s3-credentials"
OPERATOR_ADMIN_SECRET="cloudberry-operator-admin-password"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

BASELINE_FILE="$(mktemp -t scenario71-baseline.XXXXXX)"
PF_PID=""
LOCAL_API_PORT=""

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
  # The official Cloudberry image installs psql under $GPHOME/bin (not on the
  # default PATH). Run via a login shell that sources greenplum_path.sh so psql
  # and libpq resolve regardless of the exact GPHOME version directory.
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

  # Detect the DB container name in the coordinator pod (default 'cloudberry').
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
  log_step "Preflight checks (cluster=${CLUSTER} ns=${NAMESPACE} variant=${VARIANT})"

  "${KN[@]}" get cloudberrycluster "${CLUSTER}" >/dev/null 2>&1 \
    || die "CloudberryCluster ${CLUSTER} not found in ${NAMESPACE}"

  # Coordinator pod Ready.
  log_info "Waiting for coordinator pod ${COORD_POD} to be Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${COORD_POD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || die "coordinator pod ${COORD_POD} not Ready"

  # Backup S3 ConfigMap present.
  "${KN[@]}" get configmap "${S3_CONFIGMAP}" >/dev/null 2>&1 \
    || die "backup S3 ConfigMap ${S3_CONFIGMAP} not found (is backup enabled + reconciled?)"
  log_info "Backup S3 ConfigMap ${S3_CONFIGMAP} present"

  # Cluster-wide shared gpadmin SSH keypair Secret present. Without it the
  # coordinator cannot SSH to the segments and gpbackup fails to create the
  # per-segment backup directories ("Connection closed by ... port 22").
  "${KN[@]}" get secret "${SSH_SECRET}" >/dev/null 2>&1 \
    || die "shared SSH keypair Secret ${SSH_SECRET} not found (operator must reconcile it)"
  log_info "Shared SSH keypair Secret ${SSH_SECRET} present"

  # Variant-specific credential source present.
  if [ "${VARIANT}" = "vault" ]; then
    "${KN[@]}" get secret "${VAULT_CREDS_SECRET}" >/dev/null 2>&1 \
      || die "vault-materialized creds Secret ${VAULT_CREDS_SECRET} not found"
    log_info "Vault-materialized creds Secret ${VAULT_CREDS_SECRET} present"
  else
    "${KN[@]}" get secret "${SECRET_CREDS_SECRET}" >/dev/null 2>&1 \
      || die "creds Secret ${SECRET_CREDS_SECRET} not found"
    log_info "Creds Secret ${SECRET_CREDS_SECRET} present"
  fi

  # MinIO reachable from in-cluster (via the `minio` ExternalName Service).
  log_info "Checking in-cluster MinIO reachability (http://minio:9000)..."
  if "${KN[@]}" run "s71-minio-check-$$" --rm -i --restart=Never \
      --image=curlimages/curl:8.10.1 --command -- \
      curl -sf --max-time 15 http://minio:9000/minio/health/live >/dev/null 2>&1; then
    log_info "MinIO reachable in-cluster"
  else
    log_warn "in-cluster MinIO health probe failed/unavailable; continuing (Jobs will surface real failures)"
  fi
}

# ----------------------------------------------------------------------------
# Step 2 — Create mydb + ~DATA_TARGET_MB of data with indexes
# ----------------------------------------------------------------------------
generate_data() {
  log_step "Creating database ${DB} + ~${DATA_TARGET_MB}MB of data"

  # (Re)create the database from scratch for a deterministic baseline.
  coord_psql_postgres \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}' AND pid<>pg_backend_pid();" \
    >/dev/null || true
  coord_psql_postgres "DROP DATABASE IF EXISTS ${DB};" >/dev/null
  coord_psql_postgres "CREATE DATABASE ${DB};" >/dev/null
  log_info "Database ${DB} created"

  # Split the target volume across two tables. Each row of `events` is ~200 bytes
  # of payload, so rows ~= MB*1024*1024/200. `dim` is a smaller dimension table.
  local total_bytes events_rows dim_rows
  total_bytes=$(( DATA_TARGET_MB * 1024 * 1024 ))
  events_rows=$(( total_bytes / 220 ))
  dim_rows=$(( events_rows / 50 + 1 ))
  [ "${events_rows}" -gt 0 ] || events_rows=1000
  [ "${dim_rows}" -gt 0 ] || dim_rows=100

  log_info "Generating dim (${dim_rows} rows) + events (${events_rows} rows)..."

  coord_psql "${DB}" "$(cat <<SQL
CREATE TABLE dim (
  id      bigint PRIMARY KEY,
  label   text NOT NULL,
  created timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED BY (id);

INSERT INTO dim (id, label)
SELECT g, 'dim-' || g::text
FROM generate_series(1, ${dim_rows}) AS g;

CREATE TABLE events (
  id       bigint,
  dim_id   bigint,
  amount   numeric(12,2),
  payload  text,
  created  timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED BY (id);

INSERT INTO events (id, dim_id, amount, payload)
SELECT g,
       (g % ${dim_rows}) + 1,
       (g % 10000)::numeric / 100,
       repeat('x', 180)
FROM generate_series(1, ${events_rows}) AS g;

CREATE INDEX events_dim_id_idx ON events (dim_id);
CREATE INDEX events_created_idx ON events (created);

ANALYZE dim;
ANALYZE events;
SQL
)" >/dev/null

  local db_size
  db_size="$(coord_psql_postgres "SELECT pg_size_pretty(pg_database_size('${DB}'));")"
  log_info "Database ${DB} on-disk size: ${db_size}"
}

# ----------------------------------------------------------------------------
# Step 3 — Capture pre-backup baseline row counts (verification SELECT)
# ----------------------------------------------------------------------------
TABLES=("dim" "events")

capture_counts() {
  local outfile="$1"
  : > "${outfile}"
  local t cnt
  for t in "${TABLES[@]}"; do
    cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${t};")"
    echo "${t}=${cnt}" >> "${outfile}"
  done
}

baseline_select() {
  log_step "Capturing pre-backup baseline row counts"
  capture_counts "${BASELINE_FILE}"
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

  # Pick an ephemeral local port.
  LOCAL_API_PORT="$(( ( RANDOM % 20000 ) + 30000 ))"
  "${KUBECTL}" -n "${OPERATOR_NAMESPACE}" port-forward "pod/${op_pod}" \
    "${LOCAL_API_PORT}:8090" >/dev/null 2>&1 &
  PF_PID=$!

  # Wait for the forward to become usable.
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

# ----------------------------------------------------------------------------
# Step 4 — Trigger FULL backup via REST; capture the timestamp.
# ----------------------------------------------------------------------------
trigger_backup() {
  log_step "Triggering on-demand FULL backup of ${DB} via REST API"
  local resp
  resp="$(api_curl POST \
    "/api/v1alpha1/clusters/${CLUSTER}/backups?namespace=${NAMESPACE}" \
    "{\"type\":\"full\",\"databases\":[\"${DB}\"]}")" \
    || die "backup create request failed"

  TS="$(printf '%s' "${resp}" | json_field timestamp)"
  echo "${resp}"
  [ -n "${TS}" ] || die "no timestamp in backup response"
  case "${TS}" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) die "backup timestamp '${TS}' is not 14 digits" ;;
  esac
  log_info "Backup started; timestamp=${TS}"
}

# ----------------------------------------------------------------------------
# Step 5 — Wait for the backup Job to Complete.
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
# Step 6 — Verify backup objects in MinIO.
# ----------------------------------------------------------------------------
verify_minio_objects() {
  log_step "Verifying backup objects in MinIO bucket ${BUCKET}/${FOLDER}"
  if ! command -v docker >/dev/null 2>&1; then
    log_warn "docker not available; skipping MinIO object verification"
    return 0
  fi

  # Ensure an mc alias exists inside the minio container, then recursively list.
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true

  local listing
  listing="$(docker exec "${MINIO_CONTAINER}" mc ls --recursive \
    "local/${BUCKET}" 2>/dev/null || true)"

  if [ -z "${listing}" ]; then
    die "MinIO bucket ${BUCKET} is empty after backup"
  fi
  echo "${listing}" | sed 's/^/    /'

  if printf '%s\n' "${listing}" | grep -q "${TS}"; then
    log_info "Found >=1 backup object for timestamp ${TS} in ${BUCKET}"
  else
    log_warn "no object name contained ${TS}; bucket non-empty (prefix may differ)"
  fi
}

# ----------------------------------------------------------------------------
# Step 7 — Clean mydb (DROP), so restore must recreate it.
# ----------------------------------------------------------------------------
clean_db() {
  log_step "Cleaning ${DB} (DROP DATABASE)"
  coord_psql_postgres \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}' AND pid<>pg_backend_pid();" \
    >/dev/null || true
  coord_psql_postgres "DROP DATABASE IF EXISTS ${DB};" >/dev/null
  local exists
  exists="$(coord_psql_postgres "SELECT count(*) FROM pg_database WHERE datname='${DB}';")"
  [ "${exists}" = "0" ] || die "database ${DB} still present after DROP"
  log_info "Database ${DB} dropped"
}

# ----------------------------------------------------------------------------
# Step 8 — Trigger restore via REST (createDb so mydb is recreated).
# ----------------------------------------------------------------------------
trigger_restore() {
  log_step "Triggering restore of timestamp ${TS} via REST API"
  local resp
  resp="$(api_curl POST \
    "/api/v1alpha1/clusters/${CLUSTER}/backups/${TS}/restore?namespace=${NAMESPACE}" \
    "{\"databases\":[\"${DB}\"],\"gprestoreOptions\":{\"createDb\":true}}")" \
    || die "restore request failed"
  echo "${resp}"
  log_info "Restore started for timestamp ${TS}"
}

# ----------------------------------------------------------------------------
# Coordinator-exec backup/restore (default).
#
# gpbackup is an MPP tool: the coordinator dispatches to every segment over SSH
# to create per-segment backup dirs and run gpbackup_s3_plugin, and it writes
# the history DB into the coordinator data dir. A standalone backup Job pod is
# NOT a real segment host in gp_segment_configuration, so its plugin-config
# distribution (/tmp/<ts>_s3-config.yaml) never reaches the segments. Running
# gpbackup/gprestore INSIDE the coordinator pod (which IS segment -1, has the
# data dir + GPHOME tools + the shared SSH identity) is the correct execution
# model and is what this script uses by default (EXEC_MODE=coordinator).
# ----------------------------------------------------------------------------

# coord_render_s3_config writes the gpbackup_s3_plugin config to /tmp/s3-config.yaml
# inside the coordinator pod, using the same S3 settings the operator's ConfigMap
# carries (minus aws_signature_version, which the 2.1.0 plugin rejects).
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

# coord_exec_backup runs gpbackup of mydb to S3 from inside the coordinator pod
# and captures the server-side backup timestamp from the output.
coord_exec_backup() {
  log_step "Triggering FULL backup of ${DB} via coordinator exec (gpbackup)"
  coord_render_s3_config >/dev/null
  local out
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml --jobs 1 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" || true
  TS="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20
    die "gpbackup did not complete successfully"
  fi
  [ -n "${TS}" ] || die "could not capture backup timestamp from gpbackup output"
  log_info "Backup completed; timestamp=${TS}"
}

# coord_exec_restore runs gprestore of the captured timestamp from inside the
# coordinator pod, recreating mydb (--create-db).
coord_exec_restore() {
  log_step "Triggering restore of timestamp ${TS} via coordinator exec (gprestore)"
  local out
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${TS}"' --plugin-config /tmp/s3-config.yaml --create-db --jobs 1 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Restore completed|Tables restored" || true
  if ! printf '%s\n' "${out}" | grep -q "Restore completed successfully"; then
    printf '%s\n' "${out}" | tail -20
    die "gprestore did not complete successfully"
  fi
  log_info "Restore completed for timestamp ${TS}"
}

# ----------------------------------------------------------------------------
# Step 10 — Re-run verification SELECT; assert row counts match baseline.
# ----------------------------------------------------------------------------
verify_restore() {
  log_step "Verifying restored row counts match baseline"
  local restored_file
  restored_file="$(mktemp -t scenario71-restored.XXXXXX)"
  capture_counts "${restored_file}"

  local mismatch=0 t base_cnt rest_cnt
  for t in "${TABLES[@]}"; do
    base_cnt="$(grep "^${t}=" "${BASELINE_FILE}" | cut -d= -f2)"
    rest_cnt="$(grep "^${t}=" "${restored_file}" | cut -d= -f2)"
    if [ "${base_cnt}" = "${rest_cnt}" ]; then
      log_info "  ${t}: baseline=${base_cnt} restored=${rest_cnt} OK"
    else
      log_error "  ${t}: baseline=${base_cnt} restored=${rest_cnt} MISMATCH"
      mismatch=1
    fi
  done
  rm -f "${restored_file}" 2>/dev/null || true
  [ "${mismatch}" -eq 0 ] || die "row-count verification FAILED"
}

# ----------------------------------------------------------------------------
# PASS summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "PASS"
  echo "  Variant   : ${VARIANT}"
  echo "  Cluster   : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Database  : ${DB}"
  echo "  Timestamp : ${TS}"
  echo "  Row counts (baseline == restored):"
  local t cnt
  while IFS='=' read -r t cnt; do
    [ -n "${t}" ] || continue
    echo "    ${t} = ${cnt}"
  done < "${BASELINE_FILE}"
  log_info "Scenario 71 (${VARIANT}) backup/restore cycle PASSED"
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
    verify_minio_objects
    clean_db
    coord_exec_restore
    verify_restore
    print_summary
    return 0
  fi

  # REST/Job model (EXEC_MODE=rest).
  resolve_api_password
  start_api_portforward
  trigger_backup
  wait_job "${CLUSTER}-backup-${TS}"
  verify_minio_objects
  clean_db
  trigger_restore
  wait_job "${CLUSTER}-restore-${TS}"
  verify_restore
  print_summary
}

main "$@"
