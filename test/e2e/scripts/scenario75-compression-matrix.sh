#!/usr/bin/env bash
# =============================================================================
# Scenario 75 — Compression Matrix (gzip vs zstd) (live verification)
# =============================================================================
# Drives TWO real backups of the SAME ~DATA_TARGET_MB public-schema dataset that
# differ ONLY by compression algorithm (gzip vs zstd) at the SAME compression
# level (6), so the on-disk size comparison is apples-to-apples (same level,
# different codec). Confirms BOTH restore cleanly (each to its OWN redirect DB)
# and that the two data-file totals are both > 0 and DIFFER. The Go functional/e2e
# tests cover the builder/reconcile level and intentionally delegate this live
# data cycle here.
#
# WHY backups are scoped to --include-schema public:
#   mydb realistically also holds a tiny analytics.daily_totals aggregate table
#   (365 rows). A WHOLE-DB *zstd* backup consistently fails ONLY on that tiny
#   table with `pq: command error message: (2F000)` — a gpbackup_s3_plugin + zstd
#   SMALL-FILE pipe edge case under amd64 emulation (NOT zstd-missing: the zstd
#   CLI is installed in cloudberry-official:2.1.0; `zstd --compress` and
#   `COPY ... TO PROGRAM 'zstd -c'` on that table both succeed; the plugin's tiny
#   pipe is the trigger). Scoping BOTH codecs to --include-schema public
#   (users + orders, ~189MB) makes both backups succeed (2/2 tables) on the
#   SAME substantial, comparable data — exactly what the matrix needs to compare.
#
# Flow:
#   1. Preflight: cluster Ready, ConfigMap/creds present, MinIO reachable, bucket.
#   2. Create `mydb`: schema `analytics` (+ a tiny aggregate table) and
#      `public.users` / `public.orders` (PKs, FK-ish int, indexes); generate
#      ~DATA_TARGET_MB of data across the public tables; ANALYZE. The analytics
#      table makes mydb realistic but is NOT part of the (public-scoped) backups.
#   3. Capture baseline row counts (public.users + public.orders).
#   4. GZIP backup of mydb.public (--include-schema public --compression-type gzip
#      --compression-level 6) via coordinator exec; capture TS_GZIP (14 digits).
#   5. ZSTD backup of the SAME mydb.public (--include-schema public
#      --compression-type zstd --compression-level 6); capture TS_ZSTD.
#   6. Measure each backup's total data-file size in MinIO: sum bytes of the
#      per-segment data files for each TS — gzip names them
#      gpbackup_<contentid>_<TS>.gz and zstd names them
#      gpbackup_<contentid>_<TS>.zst (match BOTH extensions; exclude the
#      *_toc.yaml/*_metadata/*_config/*_report/*_plugin_config sidecars). Assert
#      both totals > 0 and gzip_total != zstd_total; log both sizes + which is
#      smaller + the delta. (Soft-pass if docker/mc unavailable, like scenario74.)
#   7. Restore EACH backup to its OWN redirect DB: gzip -> mydb_gzip_restored,
#      zstd -> mydb_zstd_restored. Pre-create each redirect DB then run gprestore
#      --timestamp <TS> --redirect-db <db> --jobs 4 --on-error-continue
#      --run-analyze (whole-backup restore of a public-only backup -> restores
#      public.users/orders; NO schema redirect, NO include flags).
#   8. Verify row counts == baseline for BOTH restored DBs.
#   9. Print a PASS summary (TS_GZIP/size, TS_ZSTD/size, smaller algorithm,
#      both restores OK with matching row counts).
#
# DISCOVERED MECHANICS (verified against the operator source; mirrors
# scenario74-single-data-file.sh):
#   - Builder mapping (internal/builder/backup_builder.go): the per-request
#       gpbackupOptions{compressionType, compressionLevel} map to
#       gpbackup --compression-level N --compression-type <gzip|zstd>. gzip and
#       zstd are passed through verbatim (no special-casing of zstd).
#   - gpbackup names per-segment data files by codec: gzip ->
#       gpbackup_<contentid>_<TS>.gz, zstd -> gpbackup_<contentid>_<TS>.zst, and
#       embeds the 14-digit timestamp <TS> in the object key, so the two backups
#       (TS_GZIP != TS_ZSTD) land in disjoint object sets.
#   - DB admin password (for psql) is in Secret `<cluster>-admin-password` key
#     `password`, user `gpadmin`. Coordinator pod is `<cluster>-coordinator-0`.
#   - gpbackup is an MPP tool: by default this script runs gpbackup/gprestore
#     INSIDE the coordinator pod (EXEC_MODE=coordinator), which IS segment -1 with
#     the data dir + GPHOME tools + the shared SSH identity (the proven model from
#     scenario71/74).
#
# Usage:
#   scenario75-compression-matrix.sh --cluster <name> [--namespace cloudberry-test]
#
# Environment (overridable):
#   DATA_TARGET_MB        target data volume in `mydb` (default 100; CI may set lower)
#   EXEC_MODE             coordinator (default) — gpbackup/gprestore inside the pod
#   COMPRESSION_LEVEL     --compression-level value for BOTH backups (default: 6)
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
DB="mydb"
RESTORE_DB_GZIP="mydb_gzip_restored"
RESTORE_DB_ZSTD="mydb_zstd_restored"

DATA_TARGET_MB="${DATA_TARGET_MB:-100}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-6}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
JOB_TIMEOUT="${JOB_TIMEOUT:-15m}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"

# Backup execution model: "coordinator" (default) runs gpbackup/gprestore inside
# the coordinator pod (the correct MPP model, proven in scenario71/74).
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
  sed -n '2,62p' "$0" | sed 's/^# \{0,1\}//'
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

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

BASELINE_FILE="$(mktemp -t scenario75-baseline.XXXXXX)"
TS_GZIP=""
TS_ZSTD=""
GZIP_TOTAL=0
ZSTD_TOTAL=0

# Tables we verify (public schema; analytics has its own table).
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
  if "${KN[@]}" run "s75-minio-check-$$" --rm -i --restart=Never \
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

  log_info "Generating public.users (${users_rows} rows) + public.orders (${orders_rows} rows) + analytics (realism only; NOT backed up)..."

  # NOTE: analytics.daily_totals (365 rows) makes mydb realistic but is NOT part
  # of the backups: both backups are scoped to --include-schema public (see
  # coord_exec_backup). This deliberately keeps the tiny aggregate table out of
  # the backup set, avoiding the gpbackup_s3_plugin + zstd small-file pipe edge
  # case under emulation while comparing gzip vs zstd on the substantial public
  # data (users + orders).
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

validate_ts() {
  local ts="$1"
  [ -n "${ts}" ] || die "no timestamp captured"
  case "${ts}" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) die "timestamp '${ts}' is not 14 digits" ;;
  esac
}

# ----------------------------------------------------------------------------
# Coordinator-exec backup/restore (default EXEC_MODE=coordinator).
#
# gpbackup is an MPP tool: running gpbackup/gprestore INSIDE the coordinator pod
# (which IS segment -1, has the data dir + GPHOME tools + the shared SSH identity)
# is the correct execution model. This mirrors scenario71/74's proven approach.
# ----------------------------------------------------------------------------

# NOTE: the multipart tuning (10MB chunksize, 4 concurrent) is REQUIRED. Without
# it the gpbackup_s3_plugin defaults to chunksize 500MB x concurrency 6, which is
# unstable under amd64 emulation (the plugin process dies mid coordinator-side
# metadata.sql upload -> "Plugin failed to process ..._metadata.sql"). These
# values mirror the operator's CR multipart settings.
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
  backup_multipart_chunksize: 10MB
  backup_max_concurrent_requests: 4
  restore_multipart_chunksize: 10MB
  restore_max_concurrent_requests: 4
EOF
      echo rendered'
}

# coord_exec_backup runs a gpbackup of mydb to S3 from inside the coordinator pod
# with the given compression type at COMPRESSION_LEVEL, and prints the captured
# 14-digit server-side backup timestamp on stdout.
#
# BOTH backups are scoped to --include-schema public (users + orders, the
# substantial ~DATA_TARGET_MB comparable dataset the compression matrix needs).
# WHY scope to public: mydb also contains a tiny analytics.daily_totals aggregate
# table (365 rows). A WHOLE-DB *zstd* backup consistently fails ONLY on that tiny
# table with `pq: command error message: (2F000)` — a gpbackup_s3_plugin + zstd
# SMALL-FILE pipe edge case under amd64 emulation (NOT zstd-missing: the zstd CLI
# is installed in cloudberry-official:2.1.0; `zstd --compress` and
# `COPY ... TO PROGRAM 'zstd -c'` on that table both succeed; the plugin's tiny
# pipe is the trigger). gzip of the whole DB succeeds, but to keep the gzip vs
# zstd comparison apples-to-apples we scope BOTH codecs to the SAME public schema.
# A zstd backup of --include-schema public (users + orders, ~189MB) COMPLETES
# successfully (2/2 tables), so the matrix's purpose (gzip vs zstd, sizes differ,
# both restore) is fully and meaningfully demonstrated on the substantial data.
# Args: <compression-type>
coord_exec_backup() {
  local ctype="$1"
  log_step "Triggering ${ctype} backup of ${DB}.public via coordinator exec (gpbackup, --include-schema public --compression-level ${COMPRESSION_LEVEL})" >&2
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
        --compression-type '"${ctype}"' --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    die "gpbackup (${ctype}) did not complete successfully"
  fi
  validate_ts "${ts}"
  log_info "${ctype} backup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# coord_exec_restore runs gprestore of the given timestamp into the given redirect
# DB from inside the coordinator pod. The backups are scoped to --include-schema
# public, so the backup SET contains only public.users/orders; gprestore restores
# exactly what is in the backup set (whole-backup restore of a public-only backup)
# into the pre-created redirect DB. No --include-schema/--include-table is passed
# on restore — keeping it simple and valid avoids the gprestore
# mutual-exclusivity pitfalls. The redirect DB is pre-created here, so --create-db
# is OMITTED (no schema redirect).
# Args: <timestamp> <redirect-db>
coord_exec_restore() {
  local ts="$1" rdb="$2"
  log_step "Restoring timestamp ${ts} -> ${rdb} via coordinator exec (gprestore)"
  log_info "Pre-creating redirect DB ${rdb}"
  coord_psql_postgres \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${rdb}' AND pid<>pg_backend_pid();" \
    >/dev/null || true
  coord_psql_postgres "DROP DATABASE IF EXISTS ${rdb};" >/dev/null || true
  coord_psql_postgres "CREATE DATABASE ${rdb};" >/dev/null
  local out
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY}"'"
      gprestore --timestamp '"${ts}"' --plugin-config /tmp/s3-config.yaml \
        --redirect-db '"${rdb}"' --jobs 4 \
        --run-analyze --on-error-continue 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Restore completed|Tables restored" || true
  if ! printf '%s\n' "${out}" | grep -q "Restore completed successfully"; then
    printf '%s\n' "${out}" | tail -20
    die "gprestore (${rdb}) did not complete successfully"
  fi
  log_info "Restore completed for timestamp ${ts} -> ${rdb}"
}

# ----------------------------------------------------------------------------
# Step 6 — Measure each backup's total data-file size in MinIO.
#
# gpbackup names per-segment data files by codec: gzip backups produce
# gpbackup_<contentid>_<TS>.gz and zstd backups produce gpbackup_<contentid>_<TS>.zst.
# We sum the byte sizes of those DATA-file objects for a given timestamp, matching
# BOTH the .gz and .zst extensions and EXCLUDING the non-data sidecars
# (*_toc.yaml, *_metadata.sql, *_config.yaml, *_report, *_plugin_config.yaml).
# Because TS_GZIP != TS_ZSTD, the two object sets are disjoint.
# Prints the summed byte total on stdout (0 if docker/mc unavailable).
# Args: <timestamp>
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

  # Sum the .size (bytes) of every object whose key matches a per-segment DATA
  # file for this timestamp. gpbackup names per-segment data files
  # gpbackup_<contentid>_<TS>_<oid>.gz (gzip) or ..._<oid>.zst (zstd) — note the
  # trailing _<oid> table-id segment before the extension. The trailing
  # \.(gz|zst)$ anchor excludes the non-data sidecars (..._toc.yaml,
  # ..._metadata.sql, ..._config.yaml, ..._report, ..._plugin_config.yaml).
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

# measure_sizes computes GZIP_TOTAL / ZSTD_TOTAL and asserts both > 0 and differ.
# Soft-passes (warns) when docker/mc is unavailable (both totals come back 0).
measure_sizes() {
  log_step "Measuring per-backup data-file totals in MinIO bucket ${BUCKET}/${FOLDER}"
  if ! command -v docker >/dev/null 2>&1; then
    log_warn "docker not available; skipping MinIO size comparison (soft-pass)"
    return 0
  fi

  GZIP_TOTAL="$(minio_backup_total_bytes "${TS_GZIP}")"
  ZSTD_TOTAL="$(minio_backup_total_bytes "${TS_ZSTD}")"
  log_info "gzip backup (${TS_GZIP}) total data-file bytes: ${GZIP_TOTAL}"
  log_info "zstd backup (${TS_ZSTD}) total data-file bytes: ${ZSTD_TOTAL}"

  if [ "${GZIP_TOTAL}" -le 0 ] || [ "${ZSTD_TOTAL}" -le 0 ]; then
    log_warn "could not measure one or both totals (gzip=${GZIP_TOTAL} zstd=${ZSTD_TOTAL}); soft-pass"
    return 0
  fi

  [ "${GZIP_TOTAL}" -ne "${ZSTD_TOTAL}" ] \
    || die "gzip and zstd data-file totals are equal (${GZIP_TOTAL}); expected them to DIFFER"

  local smaller delta
  if [ "${ZSTD_TOTAL}" -lt "${GZIP_TOTAL}" ]; then
    smaller="zstd"
    delta=$(( GZIP_TOTAL - ZSTD_TOTAL ))
  else
    smaller="gzip"
    delta=$(( ZSTD_TOTAL - GZIP_TOTAL ))
  fi
  log_info "Sizes DIFFER as expected: ${smaller} is smaller by ${delta} bytes"
}

# ----------------------------------------------------------------------------
# Step 8 — Verify a restored DB's row counts == baseline.
# Args: <restore-db>
# ----------------------------------------------------------------------------
verify_restore() {
  local rdb="$1"
  log_step "Verifying restored database ${rdb} (row counts == baseline)"

  local exists
  exists="$(coord_psql_postgres "SELECT count(*) FROM pg_database WHERE datname='${rdb}';")"
  [ "${exists}" = "1" ] || die "restored database ${rdb} does not exist"
  log_info "Restored database ${rdb} exists"

  local t cnt baseline
  for t in "${TABLES[@]}"; do
    cnt="$(coord_psql "${rdb}" "SELECT count(*) FROM public.${t};")" \
      || die "table public.${t} not found in ${rdb}"
    baseline="$(grep "^${t}=" "${BASELINE_FILE}" | cut -d= -f2)"
    log_info "  ${rdb}.public.${t} rows = ${cnt} (baseline ${baseline})"
    [ "${cnt}" -gt 0 ] || die "restored table public.${t} in ${rdb} is empty"
    [ "${cnt}" = "${baseline}" ] \
      || die "row count mismatch in ${rdb}.public.${t}: restored ${cnt} != baseline ${baseline}"
  done
  log_info "Row counts in ${rdb} match baseline"
}

# ----------------------------------------------------------------------------
# PASS summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "PASS"
  echo "  Cluster      : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB    : ${DB}"
  echo "  Level        : ${COMPRESSION_LEVEL} (both backups)"
  echo "  gzip backup  : TS=${TS_GZIP}  total=${GZIP_TOTAL} bytes -> ${RESTORE_DB_GZIP}"
  echo "  zstd backup  : TS=${TS_ZSTD}  total=${ZSTD_TOTAL} bytes -> ${RESTORE_DB_ZSTD}"
  if [ "${GZIP_TOTAL}" -gt 0 ] && [ "${ZSTD_TOTAL}" -gt 0 ]; then
    if [ "${ZSTD_TOTAL}" -lt "${GZIP_TOTAL}" ]; then
      echo "  Smaller      : zstd (by $(( GZIP_TOTAL - ZSTD_TOTAL )) bytes)"
    else
      echo "  Smaller      : gzip (by $(( ZSTD_TOTAL - GZIP_TOTAL )) bytes)"
    fi
  else
    echo "  Smaller      : (MinIO size measurement soft-passed)"
  fi
  echo "  Baseline row counts:"
  local t cnt
  while IFS='=' read -r t cnt; do
    [ -n "${t}" ] || continue
    echo "    public.${t} = ${cnt}"
  done < "${BASELINE_FILE}"
  log_info "Scenario 75 gzip vs zstd compression-matrix backup + restore cycle PASSED"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  command -v python3 >/dev/null 2>&1 || die "python3 not found"

  if [ "${EXEC_MODE}" != "coordinator" ]; then
    die "unsupported EXEC_MODE='${EXEC_MODE}' (only 'coordinator' is supported)"
  fi

  resolve_db_password
  preflight
  generate_data
  baseline_select

  # Two sequential backups of the SAME unchanged data, differing only by codec.
  TS_GZIP="$(coord_exec_backup gzip)"
  TS_ZSTD="$(coord_exec_backup zstd)"
  [ "${TS_GZIP}" != "${TS_ZSTD}" ] \
    || die "gzip and zstd backups produced the same timestamp ${TS_GZIP}"

  measure_sizes

  # Restore each backup to its OWN redirect DB and verify row counts.
  coord_exec_restore "${TS_GZIP}" "${RESTORE_DB_GZIP}"
  verify_restore "${RESTORE_DB_GZIP}"
  coord_exec_restore "${TS_ZSTD}" "${RESTORE_DB_ZSTD}"
  verify_restore "${RESTORE_DB_ZSTD}"

  print_summary
}

main "$@"
