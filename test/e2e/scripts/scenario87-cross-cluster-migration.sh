#!/usr/bin/env bash
# =============================================================================
# Scenario 87 — Cross-Cluster Migration (cloudberry-ctl migrate) live verify
# =============================================================================
# Drives `cloudberry-ctl migrate --source-cluster <src> --target-cluster <dst>`
# against an already deployed environment with TWO Ready clusters that share one
# S3 bucket, through the operator REST API over an OIDC ADMIN bearer token, and
# asserts: the source backup Job args, the target restore Job args, that both
# clusters share the bucket, and that the post-migration validation Job is
# created, completes, reports "post-restore-validate: passed" with no row-count
# mismatch / invalid-index, AND that the target's row counts equal the source's.
#
# The migrate flow (cobra path -> operator REST request -> ONE coordinated Job):
#   87a migrate ...        -> POST /clusters/{src}/migrate (202 envelope)
#   87b backup phase       -> gpbackup: --include-table x2 + --single-data-file +
#                             --plugin-config + --dbname; CAPTURES the real
#                             gpbackup "Backup Timestamp = <14>" from stdout
#   87c restore phase      -> gprestore: --timestamp <captured> + --redirect-db +
#                             --plugin-config + --include-table (NO --truncate-table:
#                             a fresh-DB restore of metadata+data; --truncate cleans
#                             the target at the DB level — DROP+CREATE the empty DB)
#   87d same bucket        -> src/dst .spec.backup.destination.s3.bucket equal
#   87e validation phase   -> migration Job logs markers post-restore-validate:/
#                             row-count/invalid/SELECT 1 and "post-restore-validate:
#                             passed"; target counts == source counts.
#
# THE FINAL CROSS-CLUSTER FIX. The migration now runs as a SINGLE coordinated Job
# (<src>-migration-<ts>, operation=migrate) that execs gpbackup inside the SOURCE
# coordinator, CAPTURES gpbackup's real run-time timestamp, then execs gprestore
# `--timestamp <captured>` inside the TARGET coordinator and validates. gpbackup
# generates its own timestamp and offers no flag to pin it, so the operator's
# pre-chosen timestamp can ONLY name the Job — the restore must use the captured
# one or it fails with a NotFound. All three phases live in one Job so the
# captured timestamp is propagated in-process.
#
# BUILD + OIDC + PORT-FORWARD + CLI-CONFIG MODEL (mirrors scenario86)
# ------------------------------------------------------------------
# 1. BUILD the CLI: `go build -o ${CTL_BIN} ./cmd/cloudberry-ctl` (or make build).
# 2. OIDC token: Keycloak password grant for an ADMIN user (/migrate is
#    Admin-gated), with the Host-header iss trick so the minted token's `iss`
#    matches the operator's issuerURL; client_credentials fallback.
# 3. PORT-FORWARD the operator REST API Service to a local ephemeral port.
# 4. POINT the CLI at the API via CLOUDBERRY_OPERATOR_URL + bearer token. We do
#    NOT export CLOUDBERRY_CLUSTER (there are two clusters); --source-cluster and
#    --target-cluster are passed explicitly.
#
# BASH 3.2 COMPATIBILITY (macOS default bash):
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helpers.
#   * Use ENV for every tunable (NO hardcode). Run with ENV.
#   * Idempotent + re-runnable: an EXIT trap kills the port-forward and removes
#     temp files; seeding uses IF NOT EXISTS / WHERE NOT EXISTS guards.
#   * Every `set -euo pipefail` command substitution that may produce no output
#     is guarded with `|| true` so an empty result never aborts the script.
#
# Usage:
#   scenario87-cross-cluster-migration.sh --source <name> --target <name> \
#       [--namespace cloudberry-test] [--db mydb] \
#       [--tables "public.users,public.orders"] \
#       [--checks preflight,seed,87a,87b,87c,87d,87e]
#
# Environment (overridable; NO hardcode):
#   NAMESPACE             target namespace (default: cloudberry-test)
#   DB                    source database (default: mydb)
#   REDIRECT_DB           gprestore --redirect-db (default: empty => DB)
#   TABLES                comma list of tables (default: public.users,public.orders)
#   TRUNCATE              pass --truncate (default: 1)
#   CHECKS                comma list of checks (default: all)
#   CTL_BIN               cloudberry-ctl binary path (default: /tmp/cloudberry-ctl)
#   CTL_BUILD             1 to build the CLI, 0 to reuse CTL_BIN (default: 1)
#   USE_MAKE              1 to `make build` instead of go build (default: 0)
#   OPERATOR_NAMESPACE    operator namespace (default: ${NAMESPACE})
#   OPERATOR_LABEL        operator pod/service selector
#                         (default: app.kubernetes.io/name=cloudberry-operator)
#   API_SERVICE           operator REST API Service name (default: auto-discover)
#   API_PORT              operator REST API port (default: 8090)
#   KEYCLOAK_URL          Keycloak base URL (default: http://127.0.0.1:8090)
#   ISSUER_HOST           Host header for the token request (default: host.docker.internal:8090)
#   OIDC_REALM            Keycloak realm (default: test)
#   OIDC_CLIENT_ID        OIDC client id (default: cloudberry-operator)
#   OIDC_CLIENT_SECRET    OIDC client secret (default: some-secret)
#   OIDC_ADMIN_USER       admin-role user (default: adminuser)
#   OIDC_ADMIN_PASS       admin-role password (default: adminpass)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
#   VALIDATE_TIMEOUT      validation Job completion timeout (default: 5m)
#   SEED_ROWS             rows seeded into each table on the source (default: 200)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing (ENV-driven; NO hardcode)
# ----------------------------------------------------------------------------
SOURCE=""
TARGET=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
REDIRECT_DB="${REDIRECT_DB:-}"
TABLES="${TABLES:-public.users,public.orders}"
TRUNCATE="${TRUNCATE:-1}"
CHECKS="${CHECKS:-preflight,seed,87a,87b,87c,87d,87e}"

CTL_BIN="${CTL_BIN:-/tmp/cloudberry-ctl}"
CTL_BUILD="${CTL_BUILD:-1}"
USE_MAKE="${USE_MAKE:-0}"

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-${NAMESPACE}}"
OPERATOR_LABEL="${OPERATOR_LABEL:-app.kubernetes.io/name=cloudberry-operator}"
API_SERVICE="${API_SERVICE:-}"
API_PORT="${API_PORT:-8090}"

KEYCLOAK_URL="${KEYCLOAK_URL:-http://127.0.0.1:8090}"
ISSUER_HOST="${ISSUER_HOST:-host.docker.internal:8090}"
OIDC_REALM="${OIDC_REALM:-test}"
OIDC_CLIENT_ID="${OIDC_CLIENT_ID:-cloudberry-operator}"
OIDC_CLIENT_SECRET="${OIDC_CLIENT_SECRET:-some-secret}"
OIDC_ADMIN_USER="${OIDC_ADMIN_USER:-adminuser}"
OIDC_ADMIN_PASS="${OIDC_ADMIN_PASS:-adminpass}"

READY_TIMEOUT="${READY_TIMEOUT:-10m}"
VALIDATE_TIMEOUT="${VALIDATE_TIMEOUT:-5m}"
SEED_ROWS="${SEED_ROWS:-200}"

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
  sed -n '2,80p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --source)     SOURCE="$2"; shift 2 ;;
    --target)     TARGET="$2"; shift 2 ;;
    --namespace)  NAMESPACE="$2"; shift 2 ;;
    --db)         DB="$2"; shift 2 ;;
    --tables)     TABLES="$2"; shift 2 ;;
    --checks)     CHECKS="$2"; shift 2 ;;
    -h|--help)    usage 0 ;;
    *)            log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$SOURCE" ] || die "--source is required (the source cluster, e.g. scenario87-src)"
[ -n "$TARGET" ] || die "--target is required (the target cluster, e.g. scenario87-dst)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Coordinator pods (handleMigrate resolves clusters by name; this repo keeps both
# clusters in the same namespace).
SRC_COORD="${SOURCE}-coordinator-0"
DST_COORD="${TARGET}-coordinator-0"
SRC_DB_PW_SECRET="${SOURCE}-admin-password"
DST_DB_PW_SECRET="${TARGET}-admin-password"

# Runtime state.
SRC_DB_PASSWORD=""
DST_DB_PASSWORD=""
SRC_DB_CONTAINER="cloudberry"
DST_DB_CONTAINER="cloudberry"
ACCESS_TOKEN=""
LOCAL_API_PORT=""
PF_PID=""
OPERATOR_URL=""
TMPDIR_S87=""

# Migration envelope state. The migration is ONE coordinated Job; MIGRATION_JOB
# holds its name. BACKUP_JOB/RESTORE_JOB/VALIDATION_JOB are kept for envelope
# back-compat and all resolve to the same single Job (it performs all phases).
MIG_TS=""
MIGRATION_JOB=""
BACKUP_JOB=""
RESTORE_JOB=""
VALIDATION_JOB=""
SRC_USERS_COUNT=""
SRC_ORDERS_COUNT=""
DBT="${DB}"   # the target restore database (REDIRECT_DB or DB).

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_PREFLIGHT="SKIP"
RESULT_SEED="SKIP"
RESULT_87A="SKIP"
RESULT_87B="SKIP"
RESULT_87C="SKIP"
RESULT_87D="SKIP"
RESULT_87E="SKIP"

set_result() {
  case "$1" in
    preflight) RESULT_PREFLIGHT="$2" ;;
    seed)      RESULT_SEED="$2" ;;
    87a)       RESULT_87A="$2" ;;
    87b)       RESULT_87B="$2" ;;
    87c)       RESULT_87C="$2" ;;
    87d)       RESULT_87D="$2" ;;
    87e)       RESULT_87E="$2" ;;
  esac
}
get_result() {
  case "$1" in
    preflight) printf '%s' "${RESULT_PREFLIGHT:-SKIP}" ;;
    seed)      printf '%s' "${RESULT_SEED:-SKIP}" ;;
    87a)       printf '%s' "${RESULT_87A:-SKIP}" ;;
    87b)       printf '%s' "${RESULT_87B:-SKIP}" ;;
    87c)       printf '%s' "${RESULT_87C:-SKIP}" ;;
    87d)       printf '%s' "${RESULT_87D:-SKIP}" ;;
    87e)       printf '%s' "${RESULT_87E:-SKIP}" ;;
    *)         printf 'SKIP' ;;
  esac
}

want_check() {
  case ",${CHECKS}," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

ASSERT_FAIL=0
assert_contains() {
  local hay="$1" needle="$2" label="$3"
  case "${hay}" in
    *"${needle}"*) log_info "  ${label}: contains '${needle}' OK" ;;
    *) log_warn "  ${label}: MISSING '${needle}'"; ASSERT_FAIL=1 ;;
  esac
}
assert_absent() {
  local hay="$1" needle="$2" label="$3"
  case "${hay}" in
    *"${needle}"*) log_warn "  ${label}: unexpected '${needle}'"; ASSERT_FAIL=1 ;;
    *) log_info "  ${label}: absent '${needle}' OK" ;;
  esac
}

# ----------------------------------------------------------------------------
# Cleanup trap: kill the port-forward + remove temp files so reruns start clean.
# ----------------------------------------------------------------------------
cleanup() {
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  [ -n "${TMPDIR_S87}" ] && rm -rf "${TMPDIR_S87}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# extract_json <key> reads a flat JSON object on stdin and prints the string
# value for <key> (jq when present, else a sed fallback). Guarded so an empty
# result never aborts the caller under `set -o pipefail`.
# ----------------------------------------------------------------------------
extract_json() {
  local key="$1" data
  data="$(cat)"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "${data}" | jq -r --arg k "${key}" '.[$k] // empty' 2>/dev/null || true
    return 0
  fi
  printf '%s' "${data}" \
    | sed -n 's/.*"'"${key}"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1 || true
}

# ----------------------------------------------------------------------------
# coord_psql <coord_pod> <container> <password> <database> <sql>: exec into a
# coordinator pod and run SQL as gpadmin. Inputs are positional so credentials
# never appear in the process table (SC2016 intentional).
# ----------------------------------------------------------------------------
coord_psql() {
  local pod="$1" container="$2" password="$3" database="$4" sql="$5"
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${pod}" -c "${container:-cloudberry}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" -At -c "$3"
    ' _ "${password}" "${database}" "${sql}"
}

# coord_psql_src / coord_psql_target are thin wrappers binding the per-cluster
# connection state captured by resolve_clusters.
coord_psql_src() {
  coord_psql "${SRC_COORD}" "${SRC_DB_CONTAINER}" "${SRC_DB_PASSWORD}" "$1" "$2"
}
coord_psql_target() {
  coord_psql "${DST_COORD}" "${DST_DB_CONTAINER}" "${DST_DB_PASSWORD}" "$1" "$2"
}

# job_args prints the container[0] args of a Job as a single string (guarded).
job_args() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true
}

# latest_job_by_label <operation> <cluster> auto-discovers the NEWEST Job by
# labels avsoft.io/cluster=<cluster> AND avsoft.io/backup-operation=<operation>,
# sorted by creationTimestamp. Guarded so an empty result never aborts.
latest_job_by_label() {
  local op="$1" cluster="$2"
  "${KN[@]}" get jobs \
    -l "avsoft.io/cluster=${cluster},avsoft.io/backup-operation=${op}" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true
}

# s3_bucket_of <cluster> prints the cluster's S3 destination bucket (guarded).
s3_bucket_of() {
  "${KN[@]}" get cloudberrycluster "$1" \
    -o jsonpath='{.spec.backup.destination.s3.bucket}' 2>/dev/null || true
}

# ----------------------------------------------------------------------------
# STEP 0 — Resolve DB admin passwords + coordinator container names.
# ----------------------------------------------------------------------------
resolve_clusters() {
  log_step "Resolving DB admin passwords + coordinator containers (src=${SOURCE} dst=${TARGET})"

  SRC_DB_PASSWORD="$("${KN[@]}" get secret "${SRC_DB_PW_SECRET}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${SRC_DB_PASSWORD}" ] || die "could not read password from Secret ${SRC_DB_PW_SECRET}"

  DST_DB_PASSWORD="$("${KN[@]}" get secret "${DST_DB_PW_SECRET}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${DST_DB_PASSWORD}" ] || die "could not read password from Secret ${DST_DB_PW_SECRET}"

  local sc dc
  sc="$("${KN[@]}" get pod "${SRC_COORD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
  SRC_DB_CONTAINER="${sc:-cloudberry}"
  dc="$("${KN[@]}" get pod "${DST_COORD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
  DST_DB_CONTAINER="${dc:-cloudberry}"

  log_info "Source coord=${SRC_COORD} container=${SRC_DB_CONTAINER}"
  log_info "Target coord=${DST_COORD} container=${DST_DB_CONTAINER}"

  # The restore target database (REDIRECT_DB wins, else DB).
  DBT="${REDIRECT_DB:-${DB}}"
}

# ----------------------------------------------------------------------------
# STEP 1 — Preflight: both clusters Ready; FAIL FAST if they do not share a
# bucket (same-bucket is a hard precondition for migration).
# ----------------------------------------------------------------------------
preflight() {
  log_step "Preflight (src=${SOURCE} dst=${TARGET} ns=${NAMESPACE})"

  "${KN[@]}" get cloudberrycluster "${SOURCE}" >/dev/null 2>&1 \
    || { set_result preflight FAIL; die "CloudberryCluster ${SOURCE} not found in ${NAMESPACE}"; }
  "${KN[@]}" get cloudberrycluster "${TARGET}" >/dev/null 2>&1 \
    || { set_result preflight FAIL; die "CloudberryCluster ${TARGET} not found in ${NAMESPACE}"; }

  log_info "Waiting for source coordinator ${SRC_COORD} Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${SRC_COORD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || { set_result preflight FAIL; die "source coordinator ${SRC_COORD} not Ready"; }
  log_info "Waiting for target coordinator ${DST_COORD} Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${DST_COORD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || { set_result preflight FAIL; die "target coordinator ${DST_COORD} not Ready"; }

  local src_b dst_b
  src_b="$(s3_bucket_of "${SOURCE}")"
  dst_b="$(s3_bucket_of "${TARGET}")"
  log_info "Source bucket=${src_b:-<none>}  Target bucket=${dst_b:-<none>}"
  if [ -z "${src_b}" ] || [ -z "${dst_b}" ]; then
    set_result preflight FAIL
    die "both clusters must be backup-enabled with an S3 destination"
  fi
  if [ "${src_b}" != "${dst_b}" ]; then
    set_result preflight FAIL
    die "source and target clusters must share the same S3 bucket (source=${src_b}, target=${dst_b})"
  fi
  log_info "Both clusters share the S3 bucket '${src_b}' OK"
  set_result preflight PASS
}

# ----------------------------------------------------------------------------
# STEP 2 — Seed public.users AND public.orders on the SOURCE with known counts.
# ----------------------------------------------------------------------------
seed_source() {
  log_step "Seeding ${DB} on source (public.users + public.orders, ${SEED_ROWS} rows each)"

  if ! coord_psql_src postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql_src postgres "CREATE DATABASE ${DB};" >/dev/null 2>&1 || true
    log_info "Database ${DB} created on source"
  else
    log_info "Database ${DB} already exists on source"
  fi

  coord_psql_src "${DB}" "
    CREATE TABLE IF NOT EXISTS public.users (
      id bigint,
      name text
    ) DISTRIBUTED BY (id);
    INSERT INTO public.users (id, name)
    SELECT g, 'user-' || g
    FROM generate_series(1, ${SEED_ROWS}) AS g
    WHERE NOT EXISTS (SELECT 1 FROM public.users LIMIT 1);
    CREATE INDEX IF NOT EXISTS users_id_idx ON public.users (id);

    CREATE TABLE IF NOT EXISTS public.orders (
      id bigint,
      amount numeric
    ) DISTRIBUTED BY (id);
    INSERT INTO public.orders (id, amount)
    SELECT g, (g * 1.5)
    FROM generate_series(1, ${SEED_ROWS}) AS g
    WHERE NOT EXISTS (SELECT 1 FROM public.orders LIMIT 1);

    ANALYZE public.users;
    ANALYZE public.orders;
  " >/dev/null 2>&1 || true

  SRC_USERS_COUNT="$(coord_psql_src "${DB}" "SELECT count(*) FROM public.users;" 2>/dev/null || true)"
  SRC_ORDERS_COUNT="$(coord_psql_src "${DB}" "SELECT count(*) FROM public.orders;" 2>/dev/null || true)"
  log_info "Source counts: public.users=${SRC_USERS_COUNT:-?}  public.orders=${SRC_ORDERS_COUNT:-?}"

  if [ -z "${SRC_USERS_COUNT}" ] || [ "${SRC_USERS_COUNT}" = "0" ] \
     || [ -z "${SRC_ORDERS_COUNT}" ] || [ "${SRC_ORDERS_COUNT}" = "0" ]; then
    log_warn "seed: source row counts are zero/unreadable"
    set_result seed FAIL
    return
  fi
  set_result seed PASS
}

# ----------------------------------------------------------------------------
# Build the cloudberry-ctl CLI.
# ----------------------------------------------------------------------------
build_cli() {
  log_step "Building cloudberry-ctl"
  if [ "${CTL_BUILD}" != "1" ] && [ -x "${CTL_BIN}" ]; then
    log_info "Reusing existing CLI binary ${CTL_BIN}"
    return 0
  fi
  command -v go >/dev/null 2>&1 || die "go not found (required to build cloudberry-ctl)"
  local script_dir repo_root
  script_dir="$(cd "$(dirname "$0")" && pwd)"
  repo_root="$(cd "${script_dir}/../../.." && pwd)"
  if [ "${USE_MAKE}" = "1" ] && [ -f "${repo_root}/Makefile" ]; then
    log_info "make build (in ${repo_root})"
    ( cd "${repo_root}" && make build ) || die "make build failed"
    local found
    # shellcheck disable=SC2012
    found="$(ls "${repo_root}"/bin/cloudberry-ctl "${repo_root}"/cloudberry-ctl 2>/dev/null | head -1 || true)"
    [ -n "${found}" ] && CTL_BIN="${found}"
  else
    log_info "go build -o ${CTL_BIN} ./cmd/cloudberry-ctl (in ${repo_root})"
    ( cd "${repo_root}" && go build -o "${CTL_BIN}" ./cmd/cloudberry-ctl ) \
      || die "go build cloudberry-ctl failed"
  fi
  [ -x "${CTL_BIN}" ] || die "cloudberry-ctl binary not found at ${CTL_BIN}"
  log_info "cloudberry-ctl built: ${CTL_BIN}"
}

# ----------------------------------------------------------------------------
# OIDC ADMIN token acquisition (Keycloak realm ${OIDC_REALM}) with the
# Host-header trick so the minted token's iss matches the operator's issuerURL.
# ----------------------------------------------------------------------------
obtain_oidc_token() {
  log_step "Obtaining OIDC admin token from Keycloak (realm ${OIDC_REALM}, user ${OIDC_ADMIN_USER})"
  command -v curl >/dev/null 2>&1 || die "curl not found (required for OIDC token)"

  local token_url resp
  token_url="${KEYCLOAK_URL}/realms/${OIDC_REALM}/protocol/openid-connect/token"

  resp="$(curl -sS --max-time 30 -X POST "${token_url}" \
    -H "Host: ${ISSUER_HOST}" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=password" \
    --data-urlencode "client_id=${OIDC_CLIENT_ID}" \
    --data-urlencode "client_secret=${OIDC_CLIENT_SECRET}" \
    --data-urlencode "username=${OIDC_ADMIN_USER}" \
    --data-urlencode "password=${OIDC_ADMIN_PASS}" \
    --data-urlencode "scope=openid profile email" 2>/dev/null || true)"
  ACCESS_TOKEN="$(printf '%s' "${resp}" | extract_json access_token)"

  if [ -z "${ACCESS_TOKEN}" ]; then
    log_warn "password grant produced no token; trying client_credentials"
    resp="$(curl -sS --max-time 30 -X POST "${token_url}" \
      -H "Host: ${ISSUER_HOST}" \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode "grant_type=client_credentials" \
      --data-urlencode "client_id=${OIDC_CLIENT_ID}" \
      --data-urlencode "client_secret=${OIDC_CLIENT_SECRET}" 2>/dev/null || true)"
    ACCESS_TOKEN="$(printf '%s' "${resp}" | extract_json access_token)"
  fi

  [ -n "${ACCESS_TOKEN}" ] || die "could not obtain an OIDC access_token from ${token_url}"
  log_info "OIDC access_token acquired (len ${#ACCESS_TOKEN})"
}

# ----------------------------------------------------------------------------
# Port-forward the operator REST API Service and point the CLI at it.
# ----------------------------------------------------------------------------
start_api_portforward() {
  log_step "Port-forwarding operator REST API (port ${API_PORT})"

  local svc="${API_SERVICE}"
  if [ -z "${svc}" ]; then
    svc="$("${KUBECTL}" -n "${OPERATOR_NAMESPACE}" get svc \
      -l "${OPERATOR_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  fi

  LOCAL_API_PORT="$(( ( RANDOM % 20000 ) + 30000 ))"

  if [ -n "${svc}" ]; then
    log_info "Operator API Service: ${svc} (ns ${OPERATOR_NAMESPACE})"
    "${KUBECTL}" -n "${OPERATOR_NAMESPACE}" port-forward "svc/${svc}" \
      "${LOCAL_API_PORT}:${API_PORT}" >/dev/null 2>&1 &
    PF_PID=$!
  else
    local op_pod
    op_pod="$("${KUBECTL}" -n "${OPERATOR_NAMESPACE}" get pods \
      -l "${OPERATOR_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    [ -n "${op_pod}" ] || die "operator API Service/pod not found in ns ${OPERATOR_NAMESPACE}"
    log_info "Operator API pod: ${op_pod} (ns ${OPERATOR_NAMESPACE})"
    "${KUBECTL}" -n "${OPERATOR_NAMESPACE}" port-forward "pod/${op_pod}" \
      "${LOCAL_API_PORT}:${API_PORT}" >/dev/null 2>&1 &
    PF_PID=$!
  fi

  OPERATOR_URL="http://127.0.0.1:${LOCAL_API_PORT}"

  local attempt=0
  while [ "${attempt}" -lt 30 ]; do
    attempt=$(( attempt + 1 ))
    if curl -sf --max-time 3 "${OPERATOR_URL}/healthz" >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "${PF_PID}" 2>/dev/null; then
      die "port-forward to operator API died"
    fi
    sleep 1
  done
  log_info "Operator API reachable at ${OPERATOR_URL}"
}

# configure_cli exports the env the CLI reads for the API URL + OIDC token. We do
# NOT export CLOUDBERRY_CLUSTER (there are two clusters); --source-cluster and
# --target-cluster are passed explicitly.
configure_cli() {
  log_step "Configuring cloudberry-ctl (API URL + OIDC bearer token)"
  export CLOUDBERRY_OPERATOR_URL="${OPERATOR_URL}"
  export CLOUDBERRY_AUTH_METHOD="oidc"
  export CLOUDBERRY_PASSWORD="${ACCESS_TOKEN}"
  export CLOUDBERRY_NAMESPACE="${NAMESPACE}"
  export CLOUDBERRY_OUTPUT="json"
  log_info "CLI -> ${CLOUDBERRY_OPERATOR_URL} (auth=oidc ns=${NAMESPACE})"
}

# ctl runs the CLI binary (env-configured); on failure (commonly a short-lived
# OIDC token expiring during a long suite, or rc=6 = 408/429 rate-limit), refresh
# the bearer token once and retry so late checks do not flake.
ctl() {
  if "${CTL_BIN}" "$@"; then
    return 0
  fi
  local rc=$?
  if obtain_oidc_token >/dev/null 2>&1; then
    export CLOUDBERRY_PASSWORD="${ACCESS_TOKEN}"
    "${CTL_BIN}" "$@"
    return $?
  fi
  return "${rc}"
}

# migration_job resolves the single coordinated migration Job name: the captured
# envelope value first, else auto-discovery by labels (operation=migrate on the
# SOURCE cluster). Guarded so an empty result never aborts.
migration_job() {
  if [ -n "${MIGRATION_JOB}" ]; then
    printf '%s' "${MIGRATION_JOB}"
    return 0
  fi
  latest_job_by_label migrate "${SOURCE}"
}

# resolve_ts re-resolves the OPERATOR (Job-naming) timestamp from live state: the
# captured envelope first, else the 14-digit token embedded in the migration Job
# name. NOTE: this is the operator timestamp that names the Job, NOT necessarily
# the real gpbackup timestamp the restore uses (that one is captured at run time
# inside the Job).
resolve_ts() {
  if [ -n "${MIG_TS}" ]; then
    printf '%s' "${MIG_TS}"
    return 0
  fi
  local job ts
  job="$(migration_job)"
  if [ -n "${job}" ]; then
    ts="$(printf '%s' "${job}" | grep -oE '[0-9]{14}' 2>/dev/null | head -1 || true)"
    [ -n "${ts}" ] && { printf '%s' "${ts}"; return 0; }
  fi
  printf ''
}

# =============================================================================
# 87a — run the migrate command and capture the 202 envelope.
# =============================================================================
run_87a() {
  log_step "87a — cloudberry-ctl migrate (src=${SOURCE} -> dst=${TARGET})"
  ASSERT_FAIL=0

  local trunc_args=()
  [ "${TRUNCATE}" = "1" ] && trunc_args+=("--truncate")
  local redirect_args=()
  [ -n "${REDIRECT_DB}" ] && redirect_args+=("--redirect-db" "${REDIRECT_DB}")

  # The migrate call may be transiently rate-limited (CLI rc=6 = 408/429) at the
  # tail of a long suite; retry with backoff so a healthy call eventually lands.
  local out="" rc=1 attempt=0
  while [ "${attempt}" -lt 6 ]; do
    attempt=$(( attempt + 1 ))
    out="$(ctl migrate --source-cluster "${SOURCE}" --target-cluster "${TARGET}" \
      --database "${DB}" --tables "${TABLES}" \
      ${trunc_args[@]+"${trunc_args[@]}"} ${redirect_args[@]+"${redirect_args[@]}"} 2>&1)" && rc=0 || rc=$?
    [ "${rc}" -eq 0 ] && break
    log_info "87a: migrate attempt ${attempt} rc=${rc}; retrying..."
    sleep $(( attempt * 3 ))
  done

  if [ "${rc}" -ne 0 ]; then
    log_warn "87a: migrate command failed (rc=${rc}): ${out}"
    set_result 87a FAIL
    return
  fi

  # Capture the 202 envelope fields (guard each with `|| true`). The migration is
  # ONE coordinated Job; backupJob/restoreJob/validationJob all reference it.
  MIG_TS="$(printf '%s' "${out}" | extract_json timestamp || true)"
  MIGRATION_JOB="$(printf '%s' "${out}" | extract_json migrationJob || true)"
  BACKUP_JOB="$(printf '%s' "${out}" | extract_json backupJob || true)"
  RESTORE_JOB="$(printf '%s' "${out}" | extract_json restoreJob || true)"
  VALIDATION_JOB="$(printf '%s' "${out}" | extract_json validationJob || true)"
  [ -n "${MIGRATION_JOB}" ] || MIGRATION_JOB="${BACKUP_JOB}"
  log_info "87a: ts=${MIG_TS:-<none>} migrationJob=${MIGRATION_JOB:-<none>}"

  assert_contains "${out}" "migration started" "87a"
  if [ -z "${MIG_TS}" ]; then
    log_warn "87a: no timestamp in envelope"; ASSERT_FAIL=1
  fi
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 87a PASS; else set_result 87a FAIL; fi
}

# =============================================================================
# 87b — backup phase args inside the single migration Job (--include-table x2 +
#       --single-data-file + --plugin-config + --dbname) and the real-timestamp
#       capture (grep for "Backup Timestamp = <14>").
# =============================================================================
run_87b() {
  log_step "87b — migration backup phase args"
  ASSERT_FAIL=0
  local job args
  job="$(migration_job)"
  if [ -z "${job}" ]; then
    log_warn "87b: could not find the migration Job"; set_result 87b FAIL; return
  fi
  log_info "87b: migration Job=${job}"
  args="$(job_args "${job}")"
  assert_contains "${args}" "--include-table" "87b"
  assert_contains "${args}" "public.users" "87b"
  assert_contains "${args}" "public.orders" "87b"
  assert_contains "${args}" "--single-data-file" "87b"
  assert_contains "${args}" "--plugin-config" "87b"
  assert_contains "${args}" "--dbname" "87b"
  # The FINAL fix: the backup phase captures the REAL gpbackup timestamp.
  assert_contains "${args}" "Backup Timestamp = " "87b"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 87b PASS; else set_result 87b FAIL; fi
}

# =============================================================================
# 87c — restore phase args inside the single migration Job (--timestamp from the
#       CAPTURED gpbackup ts + --redirect-db + --plugin-config + --include-table).
#       The restore must NOT pin the operator timestamp AND must NOT use
#       --truncate-table: the migration restores into a FRESH empty target DB
#       (metadata + data), where --truncate-table would TRUNCATE not-yet-existing
#       objects during the pre-data metadata phase and abort (42P01). The
#       user-facing --truncate "clean target" intent is honoured at the DB level
#       (the migration DROPs+recreates the empty target DB before gprestore), so
#       with TRUNCATE=1 we assert the migration creates a fresh target DB.
# =============================================================================
run_87c() {
  log_step "87c — migration restore phase args"
  ASSERT_FAIL=0
  local job args ts
  job="$(migration_job)"
  if [ -z "${job}" ]; then
    log_warn "87c: could not find the migration Job"; set_result 87c FAIL; return
  fi
  log_info "87c: migration Job=${job}"
  args="$(job_args "${job}")"
  assert_contains "${args}" "--timestamp" "87c"
  # gprestore is fed the CAPTURED gpbackup timestamp (the MIG_BACKUP_TS shell var
  # expanded at run time), NOT the operator timestamp that merely names the Job.
  ts="$(resolve_ts)"
  if [ -n "${ts}" ]; then
    assert_absent "${args}" "--timestamp '${ts}'" "87c"
  fi
  assert_contains "${args}" "--redirect-db" "87c"
  assert_contains "${args}" "--plugin-config" "87c"
  assert_contains "${args}" "--include-table" "87c"
  # The migration restore must NOT use --truncate-table (fresh-DB restore). The
  # migration instead cleans/creates the target at the DB level (CREATE DATABASE,
  # and DROP+CREATE when --truncate is passed).
  assert_absent "${args}" "--truncate-table" "87c"
  assert_contains "${args}" "CREATE DATABASE" "87c"
  if [ "${TRUNCATE}" = "1" ]; then
    # --truncate => clean target: the migration DROPs+recreates the empty DB.
    assert_contains "${args}" "DROP DATABASE IF EXISTS" "87c"
    assert_contains "${args}" "clean+recreate target database" "87c"
  fi
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 87c PASS; else set_result 87c FAIL; fi
}

# =============================================================================
# 87d — same bucket: src/dst share the S3 bucket; the single migration Job runs
#       both tools with --plugin-config and pins the SOURCE folder.
# =============================================================================
run_87d() {
  log_step "87d — same S3 bucket"
  ASSERT_FAIL=0
  local src_b dst_b job args
  src_b="$(s3_bucket_of "${SOURCE}")"
  dst_b="$(s3_bucket_of "${TARGET}")"
  log_info "87d: source bucket=${src_b:-<none>}  target bucket=${dst_b:-<none>}"
  if [ -z "${src_b}" ] || [ "${src_b}" != "${dst_b}" ]; then
    log_warn "87d: clusters do not share the S3 bucket"; ASSERT_FAIL=1
  else
    log_info "87d: shared bucket '${src_b}' OK"
  fi

  job="$(migration_job)"
  [ -n "${job}" ] && args="$(job_args "${job}")" || args=""
  # Both tool invocations use --plugin-config inside the single Job.
  assert_contains "${args}" "gpbackup --plugin-config" "87d"
  assert_contains "${args}" "gprestore --plugin-config" "87d"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 87d PASS; else set_result 87d FAIL; fi
}

# =============================================================================
# 87e — migration Job completes, logs "passed" (no mismatch / invalid index),
#       AND target counts == source counts.
# =============================================================================
run_87e() {
  log_step "87e — migration validation + row-count match"
  local vjob
  vjob="$(migration_job)"
  if [ -z "${vjob}" ]; then
    log_warn "87e: migration Job not found; SKIP"
    set_result 87e SKIP
    return
  fi
  log_info "87e: migration Job=${vjob}"

  # Wait for the migration Job (backup + restore + validation) to complete.
  "${KN[@]}" wait --for=condition=complete "job/${vjob}" \
    --timeout="${VALIDATE_TIMEOUT}" >/dev/null 2>&1 \
    || log_warn "87e: migration Job did not report Complete within ${VALIDATE_TIMEOUT}"

  ASSERT_FAIL=0
  local logs
  logs="$("${KN[@]}" logs "job/${vjob}" 2>/dev/null || true)"
  if [ -z "${logs}" ]; then
    log_warn "87e: migration Job logs were EMPTY"; ASSERT_FAIL=1
  else
    assert_contains "${logs}" "post-restore-validate:" "87e"
    assert_contains "${logs}" "post-restore-validate: passed" "87e"
    assert_absent   "${logs}" "ROW_COUNT_MISMATCH" "87e"
    # The validation script prints a benign status line "scanning for invalid
    # indexes" always; the FAILURE marker is "<N> invalid index(es)" (stderr,
    # exit 1). Assert absence of the failure marker, not the benign status text.
    assert_absent   "${logs}" "invalid index(es)" "87e"
    # The captured gpbackup timestamp marker proves the FINAL fix ran end-to-end.
    assert_contains "${logs}" "captured gpbackup timestamp" "87e"
  fi

  # Independent row-count cross-check: target counts must equal source counts.
  local dst_users dst_orders
  dst_users="$(coord_psql_target "${DBT}" "SELECT count(*) FROM public.users;" 2>/dev/null || true)"
  dst_orders="$(coord_psql_target "${DBT}" "SELECT count(*) FROM public.orders;" 2>/dev/null || true)"
  log_info "87e: target counts (db=${DBT}): public.users=${dst_users:-?}  public.orders=${dst_orders:-?}"
  log_info "87e: source counts: public.users=${SRC_USERS_COUNT:-?}  public.orders=${SRC_ORDERS_COUNT:-?}"

  if [ -n "${dst_users}" ] && [ "${dst_users}" = "${SRC_USERS_COUNT}" ]; then
    log_info "  87e: public.users counts match (${dst_users}) OK"
  else
    log_warn "  87e: public.users count mismatch (src=${SRC_USERS_COUNT:-?} dst=${dst_users:-?})"
    ASSERT_FAIL=1
  fi
  if [ -n "${dst_orders}" ] && [ "${dst_orders}" = "${SRC_ORDERS_COUNT}" ]; then
    log_info "  87e: public.orders counts match (${dst_orders}) OK"
  else
    log_warn "  87e: public.orders count mismatch (src=${SRC_ORDERS_COUNT:-?} dst=${dst_orders:-?})"
    ASSERT_FAIL=1
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 87e PASS; else set_result 87e FAIL; fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 87 — Cross-Cluster Migration: per-check summary"
  echo "  Source         : ${SOURCE} (ns ${NAMESPACE})"
  echo "  Target         : ${TARGET} (ns ${NAMESPACE})"
  echo "  Database       : ${DB}  Restore DB: ${DBT}"
  echo "  Tables         : ${TABLES}"
  echo "  CLI binary     : ${CTL_BIN}"
  echo "  Operator URL   : ${OPERATOR_URL}"
  echo "  OIDC realm     : ${OIDC_REALM}  user: ${OIDC_ADMIN_USER}"
  echo "  Migration TS   : ${MIG_TS:-<none>} (names the Job; gpbackup picks its own at run time)"
  echo "  Migration Job  : ${MIGRATION_JOB:-<none>} (single coordinated backup+restore+validate)"
  local any_fail=0 c r
  for c in preflight seed 87a 87b 87c 87d 87e; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 87 FAILED (one or more checks did not pass)"
    exit 1
  fi
  log_info "Scenario 87 cross-cluster migration PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  TMPDIR_S87="$(mktemp -d 2>/dev/null || mktemp -d -t s87)"

  resolve_clusters
  if want_check preflight; then preflight; fi
  if want_check seed; then seed_source; fi
  build_cli
  obtain_oidc_token
  start_api_portforward
  configure_cli

  if want_check 87a; then run_87a; fi
  if want_check 87b; then run_87b; fi
  if want_check 87c; then run_87c; fi
  if want_check 87d; then run_87d; fi
  if want_check 87e; then run_87e; fi

  print_summary
}

main "$@"
