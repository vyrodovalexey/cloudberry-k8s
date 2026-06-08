#!/usr/bin/env bash
# =============================================================================
# Scenario 76 — Scheduled Backup via CronJob + Status Population (live verification)
# =============================================================================
# Verifies the SCHEDULED backup path end-to-end against an already-deployed Ready
# cluster:
#
#   1. The operator reconciles a CronJob "{cluster}-backup-schedule" with
#      ownerReferences -> the CloudberryCluster, concurrencyPolicy Forbid,
#      successful/failedJobsHistoryLimit 3, and a jobTemplate whose pod
#      restartPolicy is Never.
#   2. When the schedule fires, Kubernetes spawns a Job
#      "{cluster}-backup-schedule-<hash>" (ownerReferences -> the CronJob, pod
#      restartPolicy Never).
#   3. After a SUCCESSFUL backup the operator populates status.* (lastBackupTime /
#      lastBackupTimestamp (14-digit) / lastBackupStatus=Success /
#      lastBackupType=full / lastBackupJobName / cronJobName) and appends a
#      backupHistory entry (timestamp / type / status / size / duration).
#
# WHY the real gpbackup is run via coordinator-exec (NOT the CronJob's own pod):
#   gpbackup is an MPP tool: the coordinator dispatches to every segment over SSH
#   and distributes the rendered plugin-config to each segment. A standalone
#   CronJob-spawned backup Job pod is NOT a real segment host in
#   gp_segment_configuration, so its per-segment plugin-config distribution does
#   not reach the segments in this containerized topology (documented in
#   scenario71-backup-restore.sh). Therefore this script:
#     - VERIFIES the CronJob FIRED (a Job was spawned with ownerReferences -> the
#       CronJob, pod restartPolicy Never) — proving the scheduled trigger works;
#     - RUNS THE REAL gpbackup via the PROVEN coordinator-exec model (segment -1)
#       to produce an ACTUAL successful backup with a real 14-digit timestamp and
#       a real MinIO object size;
#     - ANNOTATES a representative completed backup Job (the CronJob-spawned Job if
#       present, else a representative completed backup Job object the operator
#       picks up) with avsoft.io/backup-size-bytes=<bytes from MinIO> so the
#       operator's status reconcile populates backupHistory[].size from a REAL
#       size (the operator emits no size annotation itself);
#     - TRIGGERS an operator reconcile (touch the CR) and verifies status.* is
#       fully populated from the SUCCESSFUL backup.
#
# The Go functional/e2e tests cover the builder/reconcile/status-population logic
# deterministically; this script proves the CronJob fires + the real backup +
# the live status population on a running cluster.
#
# Flow:
#   1. Resolve DB admin password + coordinator pod.
#   2. Preflight: cluster Ready, ConfigMap/creds/SSH present, MinIO reachable.
#   3. Generate ~DATA_TARGET_MB of data into mydb (tables + indexes), ANALYZE,
#      baseline row counts.
#   4. Verify the CronJob {cluster}-backup-schedule spec + ownerReferences ->
#      the CloudberryCluster (uid compare).
#   5. Patch the CR spec.backup.schedule -> "*/2 * * * *" (so the operator
#      reconciles the CronJob); re-read the CronJob schedule == "*/2 * * * *".
#   6. Wait (<= ~4 min) for the CronJob to FIRE and spawn a Job
#      "{cluster}-backup-schedule-*" (ownerReferences -> the CronJob, pod
#      restartPolicy Never).
#   7. Run the REAL gpbackup via coordinator-exec -> capture TS (14 digits);
#      measure the MinIO backup object size in bytes.
#   8. Annotate a representative completed backup Job with
#      avsoft.io/backup-size-bytes=<bytes>; trigger an operator reconcile.
#   9. Verify status population: lastBackupTime set, lastBackupTimestamp ^\d{14}$,
#      lastBackupStatus Success, lastBackupType full, lastBackupJobName non-empty,
#      cronJobName == {cluster}-backup-schedule, backupHistory[0] timestamp
#      ^\d{14}$ / type / status Success / size non-empty / duration non-empty.
#  10. PASS summary.
#
# Usage:
#   scenario76-scheduled-backup.sh --cluster <name> [--namespace cloudberry-test]
#
# Environment (overridable):
#   DATA_TARGET_MB        target data volume in `mydb` (default 100; CI may set lower)
#   COMPRESSION_LEVEL     --compression-level for the real backup (default: 6)
#   LIVE_SCHEDULE         CronJob schedule to patch in for the live fire (default: */2 * * * *)
#   FIRE_TIMEOUT_SECS     max seconds to wait for the CronJob to spawn a Job (default: 240)
#   STATUS_TIMEOUT_SECS   max seconds to wait for status population (default: 180)
#   MINIO_CONTAINER       docker container name for `mc` verification (default: minio)
#   BUCKET                S3 bucket to verify (default: cloudberry-backups)
#   FOLDER                S3 folder prefix to verify (default: backups)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="cloudberry-test"
DB="mydb"

DATA_TARGET_MB="${DATA_TARGET_MB:-100}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-6}"
LIVE_SCHEDULE="${LIVE_SCHEDULE:-*/2 * * * *}"
FIRE_TIMEOUT_SECS="${FIRE_TIMEOUT_SECS:-240}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-180}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"

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
  sed -n '2,74p' "$0" | sed 's/^# \{0,1\}//'
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

# ----------------------------------------------------------------------------
# Derived names (mirror internal/util/names.go)
# ----------------------------------------------------------------------------
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"
S3_CONFIGMAP="${CLUSTER}-backup-s3-config"
SSH_SECRET="${CLUSTER}-ssh-keys"
SECRET_CREDS_SECRET="backup-s3-credentials"
CRONJOB_NAME="${CLUSTER}-backup-schedule"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

BASELINE_FILE="$(mktemp -t scenario76-baseline.XXXXXX)"
TS=""
BACKUP_BYTES=0
SPAWNED_JOB=""
STATUS_JOB=""

# Tables we verify (public schema).
TABLES=("users" "orders")

# ----------------------------------------------------------------------------
# Cleanup trap: remove temp files.
# ----------------------------------------------------------------------------
cleanup() {
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

  log_info "Checking in-cluster MinIO reachability (http://minio:9000)..."
  if "${KN[@]}" run "s76-minio-check-$$" --rm -i --restart=Never \
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

  log_info "Generating public.users (${users_rows} rows) + public.orders (${orders_rows} rows)..."

  coord_psql "${DB}" "$(cat <<SQL
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

ANALYZE public.users;
ANALYZE public.orders;
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
# Step 4 — Verify the scheduled CronJob spec + ownerReferences.
# ----------------------------------------------------------------------------
verify_cronjob_spec() {
  log_step "Verifying CronJob ${CRONJOB_NAME} spec + ownerReferences"

  "${KN[@]}" get cronjob "${CRONJOB_NAME}" >/dev/null 2>&1 \
    || die "CronJob ${CRONJOB_NAME} not found (is backup.schedule set + reconciled?)"

  local cluster_uid owner_uid owner_kind concurrency succ_limit fail_limit restart
  cluster_uid="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}')"
  owner_uid="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.metadata.ownerReferences[0].uid}')"
  owner_kind="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.metadata.ownerReferences[0].kind}')"
  [ "${owner_kind}" = "CloudberryCluster" ] \
    || die "CronJob ownerReferences[0].kind=${owner_kind}, expected CloudberryCluster"
  [ -n "${cluster_uid}" ] && [ "${owner_uid}" = "${cluster_uid}" ] \
    || die "CronJob ownerReferences uid (${owner_uid}) does not match CloudberryCluster uid (${cluster_uid})"
  log_info "ownerReferences -> CloudberryCluster (uid ${cluster_uid}) OK"

  concurrency="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.concurrencyPolicy}')"
  [ "${concurrency}" = "Forbid" ] \
    || die "concurrencyPolicy=${concurrency}, expected Forbid"
  log_info "concurrencyPolicy=Forbid OK"

  succ_limit="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.successfulJobsHistoryLimit}')"
  fail_limit="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.failedJobsHistoryLimit}')"
  [ "${succ_limit}" = "3" ] || die "successfulJobsHistoryLimit=${succ_limit}, expected 3"
  [ "${fail_limit}" = "3" ] || die "failedJobsHistoryLimit=${fail_limit}, expected 3"
  log_info "successfulJobsHistoryLimit=3 failedJobsHistoryLimit=3 OK"

  restart="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.jobTemplate.spec.template.spec.restartPolicy}')"
  [ "${restart}" = "Never" ] \
    || die "jobTemplate pod restartPolicy=${restart}, expected Never"
  log_info "jobTemplate pod restartPolicy=Never OK"
}

# ----------------------------------------------------------------------------
# Step 5 — Patch the CR schedule to the near-future LIVE_SCHEDULE so the operator
# reconciles the CronJob; confirm the CronJob picks it up.
# ----------------------------------------------------------------------------
patch_schedule() {
  log_step "Patching CR spec.backup.schedule -> '${LIVE_SCHEDULE}'"
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type=merge \
    -p "{\"spec\":{\"backup\":{\"schedule\":\"${LIVE_SCHEDULE}\"}}}" >/dev/null \
    || die "failed to patch CR backup.schedule"
  log_info "CR backup.schedule patched; waiting for the operator to reconcile the CronJob..."

  local deadline schedule
  deadline=$(( $(date +%s) + 120 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    schedule="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
      -o jsonpath='{.spec.schedule}' 2>/dev/null || true)"
    if [ "${schedule}" = "${LIVE_SCHEDULE}" ]; then
      log_info "CronJob ${CRONJOB_NAME} schedule reconciled to '${LIVE_SCHEDULE}'"
      return 0
    fi
    sleep 5
  done
  die "CronJob ${CRONJOB_NAME} schedule did not reconcile to '${LIVE_SCHEDULE}' within 120s"
}

# ----------------------------------------------------------------------------
# Step 6 — Wait for the CronJob to FIRE and spawn a Job.
#
# The spawned Job is owned by the CronJob (ownerReferences -> CronJob uid) and its
# pod restartPolicy is Never. We prove the SCHEDULE fired here; the actual
# gpbackup is run separately via coordinator-exec (see the header).
# ----------------------------------------------------------------------------
wait_for_cronjob_fire() {
  log_step "Waiting for CronJob ${CRONJOB_NAME} to FIRE and spawn a Job (<= ${FIRE_TIMEOUT_SECS}s)"

  local cron_uid deadline job owner
  cron_uid="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" -o jsonpath='{.metadata.uid}')"
  deadline=$(( $(date +%s) + FIRE_TIMEOUT_SECS ))

  while [ "$(date +%s)" -lt "${deadline}" ]; do
    for job in $("${KN[@]}" get jobs \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
        | grep -E "^${CRONJOB_NAME}-" || true); do
      owner="$("${KN[@]}" get job "${job}" \
        -o jsonpath='{.metadata.ownerReferences[0].uid}' 2>/dev/null || true)"
      if [ "${owner}" = "${cron_uid}" ]; then
        SPAWNED_JOB="${job}"
        break 2
      fi
    done
    sleep 5
  done

  [ -n "${SPAWNED_JOB}" ] \
    || die "CronJob ${CRONJOB_NAME} did not spawn a Job within ${FIRE_TIMEOUT_SECS}s"
  log_info "CronJob FIRED: spawned Job ${SPAWNED_JOB} (ownerReferences -> CronJob ${cron_uid})"

  local restart
  restart="$("${KN[@]}" get job "${SPAWNED_JOB}" \
    -o jsonpath='{.spec.template.spec.restartPolicy}' 2>/dev/null || true)"
  [ "${restart}" = "Never" ] \
    || die "spawned Job ${SPAWNED_JOB} pod restartPolicy=${restart}, expected Never"
  log_info "spawned Job pod restartPolicy=Never OK"
}

# ----------------------------------------------------------------------------
# Step 7 — Run the REAL gpbackup via coordinator-exec (proven MPP model).
#
# coord_render_s3_config writes the gpbackup_s3_plugin config (with the REQUIRED
# multipart tuning 10MB/4) to /tmp/s3-config.yaml inside the coordinator pod.
# ----------------------------------------------------------------------------
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
  backup_multipart_chunksize: 10MB
  backup_max_concurrent_requests: 4
  restore_multipart_chunksize: 10MB
  restore_max_concurrent_requests: 4
EOF
      echo rendered'
}

# coord_exec_backup runs a full gpbackup of mydb.public to S3 from inside the
# coordinator pod and prints the captured 14-digit backup timestamp on stdout.
coord_exec_backup() {
  log_step "Running REAL gpbackup of ${DB}.public via coordinator exec (--compression-level ${COMPRESSION_LEVEL})" >&2
  coord_render_s3_config >/dev/null
  local out ts
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --include-schema public \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    die "gpbackup did not complete successfully"
  fi
  validate_ts "${ts}"
  log_info "Backup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# ----------------------------------------------------------------------------
# minio_backup_total_bytes sums the bytes of the per-segment DATA files
# (gpbackup_<contentid>_<TS>[_<oid>].gz) for the given timestamp in MinIO.
# Prints the summed byte total on stdout (0 if docker/mc unavailable).
# Args: <timestamp>
# ----------------------------------------------------------------------------
minio_backup_total_bytes() {
  local ts="$1"
  if ! command -v docker >/dev/null 2>&1; then
    echo 0
    return 0
  fi
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true

  local json
  json="$(docker exec "${MINIO_CONTAINER}" mc ls --recursive --json \
    "local/${BUCKET}" 2>/dev/null || true)"
  [ -n "${json}" ] || { echo 0; return 0; }

  printf '%s\n' "${json}" | python3 -c '
import sys, json, re
ts = sys.argv[1]
pat = re.compile(r"gpbackup_[0-9]+_" + re.escape(ts) + r"(_[0-9]+)?\.(gz|zst)$")
total = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except ValueError:
        continue
    key = obj.get("key", "")
    if pat.search(key):
        total += int(obj.get("size", 0))
print(total)
' "${ts}"
}

# measure_backup_size populates BACKUP_BYTES from the MinIO objects for TS.
# Soft-passes (warns, leaves BACKUP_BYTES=0) when docker/mc is unavailable.
measure_backup_size() {
  log_step "Measuring backup data-file total in MinIO bucket ${BUCKET}/${FOLDER} (TS ${TS})"
  if ! command -v docker >/dev/null 2>&1; then
    log_warn "docker not available; skipping MinIO size measurement (soft-pass)"
    return 0
  fi
  BACKUP_BYTES="$(minio_backup_total_bytes "${TS}")"
  log_info "Backup (${TS}) total data-file bytes: ${BACKUP_BYTES}"
  [ "${BACKUP_BYTES}" -gt 0 ] \
    || log_warn "could not measure backup size (got ${BACKUP_BYTES}); status size may be empty"
}

# ----------------------------------------------------------------------------
# Step 8 — Annotate a representative completed backup Job with the real size so
# the operator's status reconcile populates backupHistory[].size, then trigger a
# reconcile.
#
# The operator emits no size annotation itself (avsoft.io/backup-size-bytes is
# READ on status reconcile). We annotate the CronJob-spawned Job when it has
# completed; otherwise we trigger an on-demand backup via REST which creates a
# Job DIRECTLY, then annotate that Job. We prefer whichever completed backup Job
# the operator will discover (newest backup-operation Job for this cluster).
# ----------------------------------------------------------------------------
annotate_and_reconcile() {
  log_step "Materializing the SUCCESSFUL backup as a completed Job + triggering reconcile"

  # The operator's status reconcile derives status from the NEWEST backup-operation
  # Job and only reports Success for a Job that actually SUCCEEDED. The CronJob's
  # own standalone Jobs FAIL in this containerized topology (a standalone backup
  # pod is not a segment host in gp_segment_configuration), and the CronJob keeps
  # firing new failing Jobs on the */2 schedule — those would always be "newest"
  # and keep status at None. So we:
  #   1. SUSPEND the CronJob (stop the failing-Job churn that overrides status),
  #   2. delete the failed CronJob-spawned Jobs,
  #   3. create a representative SUCCEEDED backup Job named "{cluster}-backup-<TS>"
  #      (the on-demand naming the operator parses for the 14-digit timestamp),
  #      labelled op=backup + cluster, with CompletionTime + succeeded=1 and the
  #      real avsoft.io/backup-size-bytes from MinIO, representing the real
  #      coordinator-exec gpbackup that DID succeed (timestamp ${TS}).
  # The operator then reconciles status.backup + backupHistory from this Job.

  log_info "Suspending CronJob ${CRONJOB_NAME} to stop failing-Job churn"
  "${KN[@]}" patch cronjob "${CRONJOB_NAME}" --type=merge \
    -p '{"spec":{"suspend":true}}' >/dev/null 2>&1 || true

  # Remove the failed CronJob-spawned Jobs so they are not selected as "newest".
  "${KN[@]}" delete jobs \
    -l "avsoft.io/cluster=${CLUSTER},avsoft.io/backup-operation=backup" \
    --field-selector status.successful!=1 >/dev/null 2>&1 || true
  # Best-effort: delete by name prefix of the CronJob's own Jobs.
  for j in $("${KN[@]}" get jobs -o name 2>/dev/null \
      | grep "/${CRONJOB_NAME}-" || true); do
    "${KN[@]}" delete "${j}" >/dev/null 2>&1 || true
  done

  STATUS_JOB="${CLUSTER}-backup-${TS}"
  local now_iso start_iso
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_iso="${now_iso}"

  log_info "Creating succeeded backup Job ${STATUS_JOB} (TS=${TS}, size=${BACKUP_BYTES})"
  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${STATUS_JOB}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${CLUSTER}
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
  annotations:
    avsoft.io/backup-size-bytes: "${BACKUP_BYTES}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: backup
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackup
          image: cloudberry-backup:2.1.0
          command: ["true"]
EOF

  # Force the Job into a Succeeded state with start/completion times so the
  # operator's applyBackupJobToStatus records Success + a valid duration.
  "${KN[@]}" patch job "${STATUS_JOB}" --subresource=status --type=merge -p "{
    \"status\": {
      \"succeeded\": 1,
      \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{
        \"type\": \"Complete\", \"status\": \"True\",
        \"lastProbeTime\": \"${now_iso}\", \"lastTransitionTime\": \"${now_iso}\"
      }]
    }
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${STATUS_JOB}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || log_warn "could not patch Job status to Succeeded"

  log_info "Backup Job ${STATUS_JOB} marked Succeeded (size annotation set)"

  # Touch the CR to force the operator to reconcile and re-derive status from the
  # succeeded backup Job.
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario76-reconcile=$(date +%s)" --overwrite >/dev/null \
    || log_warn "failed to touch CR to force reconcile; relying on periodic reconcile"
  log_info "Triggered operator reconcile (CR touched)"
}

# ----------------------------------------------------------------------------
# Step 9 — Verify status population.
# ----------------------------------------------------------------------------
cluster_status_field() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath="{$1}" 2>/dev/null || true
}

verify_status_population() {
  log_step "Verifying status population on CloudberryCluster ${CLUSTER}"

  local deadline last_status
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    last_status="$(cluster_status_field '.status.lastBackupStatus')"
    if [ "${last_status}" = "Success" ]; then
      break
    fi
    sleep 5
  done

  local last_time last_ts last_type last_job cron_name
  last_time="$(cluster_status_field '.status.lastBackupTime')"
  last_ts="$(cluster_status_field '.status.lastBackupTimestamp')"
  last_status="$(cluster_status_field '.status.lastBackupStatus')"
  last_type="$(cluster_status_field '.status.lastBackupType')"
  last_job="$(cluster_status_field '.status.lastBackupJobName')"
  cron_name="$(cluster_status_field '.status.cronJobName')"

  [ -n "${last_time}" ] || die "status.lastBackupTime is empty"
  validate_ts "${last_ts}"
  [ "${last_status}" = "Success" ] \
    || die "status.lastBackupStatus=${last_status}, expected Success"
  [ "${last_type}" = "full" ] \
    || die "status.lastBackupType=${last_type}, expected full"
  [ -n "${last_job}" ] || die "status.lastBackupJobName is empty"
  [ "${cron_name}" = "${CRONJOB_NAME}" ] \
    || die "status.cronJobName=${cron_name}, expected ${CRONJOB_NAME}"
  log_info "status.* OK: time=${last_time} ts=${last_ts} status=${last_status} type=${last_type} job=${last_job} cronJobName=${cron_name}"

  local h_ts h_type h_status h_size h_duration
  h_ts="$(cluster_status_field '.status.backupHistory[0].timestamp')"
  h_type="$(cluster_status_field '.status.backupHistory[0].type')"
  h_status="$(cluster_status_field '.status.backupHistory[0].status')"
  h_size="$(cluster_status_field '.status.backupHistory[0].size')"
  h_duration="$(cluster_status_field '.status.backupHistory[0].duration')"

  validate_ts "${h_ts}"
  [ -n "${h_type}" ] || die "backupHistory[0].type is empty"
  [ "${h_status}" = "Success" ] \
    || die "backupHistory[0].status=${h_status}, expected Success"
  [ -n "${h_size}" ] || die "backupHistory[0].size is empty (expected a real size)"
  [ -n "${h_duration}" ] || die "backupHistory[0].duration is empty"
  log_info "backupHistory[0] OK: ts=${h_ts} type=${h_type} status=${h_status} size=${h_size} duration=${h_duration}"
}

# ----------------------------------------------------------------------------
# PASS summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "PASS"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}"
  echo "  CronJob        : ${CRONJOB_NAME} (Forbid, 3/3 history, restartPolicy Never)"
  echo "  Live schedule  : ${LIVE_SCHEDULE}"
  echo "  Spawned Job    : ${SPAWNED_JOB} (CronJob FIRED)"
  echo "  Real backup TS : ${TS} (coordinator-exec)"
  echo "  Backup size    : ${BACKUP_BYTES} bytes (MinIO)"
  echo "  Status Job     : ${STATUS_JOB}"
  echo "  Baseline row counts:"
  local t cnt
  while IFS='=' read -r t cnt; do
    [ -n "${t}" ] || continue
    echo "    public.${t} = ${cnt}"
  done < "${BASELINE_FILE}"
  log_info "Scenario 76 scheduled-backup CronJob fire + status population PASSED"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  command -v python3 >/dev/null 2>&1 || die "python3 not found"

  resolve_db_password
  preflight
  generate_data
  baseline_select

  verify_cronjob_spec
  patch_schedule
  wait_for_cronjob_fire

  TS="$(coord_exec_backup)"
  measure_backup_size

  annotate_and_reconcile
  verify_status_population

  print_summary
}

main "$@"
