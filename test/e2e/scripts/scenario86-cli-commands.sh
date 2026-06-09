#!/usr/bin/env bash
# =============================================================================
# Scenario 86 — All cloudberry-ctl backup CLI commands (live verification)
# =============================================================================
# Drives EVERY `cloudberry-ctl backup ...` command (86a-k) against an already
# deployed Ready S3-destination cluster (scenario86-s3: backup enabled +
# schedule + incremental enabled), through the operator REST API over an OIDC
# bearer token, and asserts the responses PLUS — for the Job-producing commands —
# the container args of the created backup/restore/cleanup Jobs and the CronJob
# schedule/suspend changes, and that `backup jobs logs` STREAMS real pod logs.
#
# The 11 commands (cobra path -> operator REST request):
#   86a backup create (x3 variants) -> POST /backups (gpbackupOptions flags)
#   86b backup list                 -> GET  /backups
#   86c backup status --timestamp   -> GET  /backups/{ts}
#   86d backup delete --timestamp   -> DELETE /backups/{ts}      (cleanup Job)
#   86e backup restore ...          -> POST /backups/{ts}/restore (--resize-cluster)
#   86f backup schedule             -> GET  /backups/schedule
#   86g backup schedule set --cron  -> PATCH /backups/schedule {schedule}
#   86h backup schedule suspend     -> PATCH /backups/schedule {suspend:true}
#   86i backup schedule resume      -> PATCH /backups/schedule {suspend:false}
#   86j backup jobs                 -> GET  /backups/jobs
#   86k backup jobs logs --job <n>  -> GET  /backups/jobs/{job}/logs (STREAMS)
#
# BUILD + OIDC + PORT-FORWARD + CLI-CONFIG MODEL
# ---------------------------------------------
# 1. BUILD the CLI: `go build -o ${CTL_BIN} ./cmd/cloudberry-ctl` (or `make
#    build`). ${CTL_BIN} defaults to /tmp/cloudberry-ctl.
# 2. OIDC token: POST ${KEYCLOAK_URL}/realms/${OIDC_REALM}/.../token with a
#    password grant for an ADMIN-role user (${OIDC_ADMIN_USER}). Keycloak mints
#    the token's `iss` from the request authority, and the operator validates
#    `iss` against its configured issuerURL (host.docker.internal:8090). So we
#    reach Keycloak at the host-reachable ${KEYCLOAK_URL} (127.0.0.1:8090) but
#    send `Host: ${ISSUER_HOST}` so the minted token's iss matches the operator.
# 3. PORT-FORWARD the operator REST API Service (ns cloudberry-test, port 8090)
#    to a local ephemeral port. The operator API serves PLAIN HTTP; auth is the
#    OIDC bearer token, not mTLS.
# 4. POINT the CLI at the API: the CLI takes the API URL via --operator-url /
#    CLOUDBERRY_OPERATOR_URL and the OIDC bearer token via --auth-method oidc +
#    --password (CLOUDBERRY_PASSWORD); with --auth-method oidc the CLI sends
#    `Authorization: Bearer <password>`. We export:
#      CLOUDBERRY_OPERATOR_URL=http://127.0.0.1:<localport>
#      CLOUDBERRY_AUTH_METHOD=oidc
#      CLOUDBERRY_PASSWORD=<access_token>
#      CLOUDBERRY_CLUSTER=scenario86-s3   CLOUDBERRY_NAMESPACE=cloudberry-test
#      CLOUDBERRY_OUTPUT=json
#
# A healthy REAL coordinator-exec gpbackup of mydb to S3 is also run so "all
# backups successful" holds and 86c has a real timestamp to read.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lessons from prior scenarios:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Any multi-line script embedded into a remote shell is base64-encoded
#     (the coordinator gpbackup / s3 config are run inline as in scenario85).
#   * Use ENV for every tunable (NO hardcode). Run with ENV.
#   * Idempotent + re-runnable: an EXIT trap kills the port-forward and removes
#     temp files; reruns start clean.
#
# Usage:
#   scenario86-cli-commands.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db mydb] \
#       [--checks 86a,86b,86c,86d,86e,86f,86g,86h,86i,86j,86k,backup]
#
# Environment (overridable; NO hardcode):
#   NAMESPACE             target namespace (default: cloudberry-test)
#   DB                    source database (default: mydb)
#   RESTORE_DB            restore target database (default: mydb_restored)
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
#   CRED_SECRET           the S3 credential Secret name (default: backup-s3-credentials)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
#   SEED_ROWS             rows seeded into mydb (default: 5000)
#   COMPRESSION_LEVEL     --compression-level for the healthy gpbackup (default: 1)
#   CRON_NEW              the cron set by 86g (default: "0 3 * * *")
#   S3_REGION/S3_ENDPOINT/S3_BUCKET/S3_FOLDER  S3 wiring (mirror the sample CR)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing (ENV-driven; NO hardcode)
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
RESTORE_DB="${RESTORE_DB:-mydb_restored}"
CHECKS="${CHECKS:-86a,86b,86c,86e,86f,86g,86h,86i,86j,86k,86d,backup}"

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

CRED_SECRET="${CRED_SECRET:-backup-s3-credentials}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
SEED_ROWS="${SEED_ROWS:-5000}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"
CRON_NEW="${CRON_NEW:-0 3 * * *}"

S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-backups}"
S3_FOLDER="${S3_FOLDER:-scenario86}"

# The table Scenario 86 seeds in mydb (restore uses public.users).
SEED_TABLE="public.users"

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
  sed -n '2,110p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)    CLUSTER="$2"; shift 2 ;;
    --namespace)  NAMESPACE="$2"; shift 2 ;;
    --db)         DB="$2"; shift 2 ;;
    --checks)     CHECKS="$2"; shift 2 ;;
    -h|--help)    usage 0 ;;
    *)            log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the S3 CLI cluster, e.g. scenario86-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"
# Operator names the schedule CronJob "<cluster>-backup-schedule"; auto-discover
# at runtime to stay resilient to naming changes (falls back to the default).
CRONJOB_NAME="${CLUSTER}-backup-schedule"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
AWS_ACCESS_KEY_ID_VALUE=""
AWS_SECRET_ACCESS_KEY_VALUE=""
ACCESS_TOKEN=""
LOCAL_API_PORT=""
PF_PID=""
OPERATOR_URL=""
KNOWN_TS=""            # a timestamp known to be in history (86c/86d/86e use it).
LOGS_JOB=""            # a backup Job name whose pod logs 86k streams.
TMPDIR_S86=""

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_86A="SKIP"
RESULT_86B="SKIP"
RESULT_86C="SKIP"
RESULT_86D="SKIP"
RESULT_86E="SKIP"
RESULT_86F="SKIP"
RESULT_86G="SKIP"
RESULT_86H="SKIP"
RESULT_86I="SKIP"
RESULT_86J="SKIP"
RESULT_86K="SKIP"
RESULT_BACKUP="SKIP"

set_result() {
  case "$1" in
    86a)    RESULT_86A="$2" ;;
    86b)    RESULT_86B="$2" ;;
    86c)    RESULT_86C="$2" ;;
    86d)    RESULT_86D="$2" ;;
    86e)    RESULT_86E="$2" ;;
    86f)    RESULT_86F="$2" ;;
    86g)    RESULT_86G="$2" ;;
    86h)    RESULT_86H="$2" ;;
    86i)    RESULT_86I="$2" ;;
    86j)    RESULT_86J="$2" ;;
    86k)    RESULT_86K="$2" ;;
    backup) RESULT_BACKUP="$2" ;;
  esac
}
get_result() {
  case "$1" in
    86a)    printf '%s' "${RESULT_86A:-SKIP}" ;;
    86b)    printf '%s' "${RESULT_86B:-SKIP}" ;;
    86c)    printf '%s' "${RESULT_86C:-SKIP}" ;;
    86d)    printf '%s' "${RESULT_86D:-SKIP}" ;;
    86e)    printf '%s' "${RESULT_86E:-SKIP}" ;;
    86f)    printf '%s' "${RESULT_86F:-SKIP}" ;;
    86g)    printf '%s' "${RESULT_86G:-SKIP}" ;;
    86h)    printf '%s' "${RESULT_86H:-SKIP}" ;;
    86i)    printf '%s' "${RESULT_86I:-SKIP}" ;;
    86j)    printf '%s' "${RESULT_86J:-SKIP}" ;;
    86k)    printf '%s' "${RESULT_86K:-SKIP}" ;;
    backup) printf '%s' "${RESULT_BACKUP:-SKIP}" ;;
    *)      printf 'SKIP' ;;
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
  [ -n "${TMPDIR_S86}" ] && rm -rf "${TMPDIR_S86}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# extract_json <key> reads a flat JSON object on stdin and prints the string
# value for <key> (jq when present, else a sed fallback).
# ----------------------------------------------------------------------------
extract_json() {
  local key="$1" data
  data="$(cat)"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "${data}" | jq -r --arg k "${key}" '.[$k] // empty' 2>/dev/null
    return 0
  fi
  printf '%s' "${data}" \
    | sed -n 's/.*"'"${key}"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
}

# extract_first_backup_ts reads a `backup list` JSON response (which nests
# timestamps under .backups[].timestamp, not at the top level) and returns the
# first backup's gpbackup timestamp. Falls back to the first 14-digit token.
extract_first_backup_ts() {
  local data
  data="$(cat)"
  if command -v jq >/dev/null 2>&1; then
    local ts
    ts="$(printf '%s' "${data}" | jq -r '.backups[0].timestamp // empty' 2>/dev/null)"
    [ -n "${ts}" ] && { printf '%s' "${ts}"; return 0; }
  fi
  # grep returns non-zero on no match; guard so the pipeline never fails the
  # caller under `set -o pipefail` (callers treat empty output as "no ts").
  printf '%s' "${data}" | grep -oE '[0-9]{14}' 2>/dev/null | head -1 || true
}

# ----------------------------------------------------------------------------
# coord_psql: exec into the coordinator pod and run SQL as gpadmin.
# Inputs are positional ($1..$3) so credentials never appear in the process
# table (SC2016 intentional).
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
# STEP 0 — Resolve DB admin password, coordinator container, S3 cred values.
# ----------------------------------------------------------------------------
resolve_cluster() {
  log_step "Resolving DB admin password + coordinator + cred secret (cluster=${CLUSTER})"
  DB_PASSWORD="$("${KN[@]}" get secret "${DB_PW_SECRET}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${DB_PW_SECRET}"

  local cname
  cname="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
  DB_CONTAINER="${cname:-cloudberry}"
  log_info "Coordinator pod=${COORD_POD} container=${DB_CONTAINER}"

  AWS_ACCESS_KEY_ID_VALUE="$("${KN[@]}" get secret "${CRED_SECRET}" \
    -o jsonpath='{.data.aws_access_key_id}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  AWS_SECRET_ACCESS_KEY_VALUE="$("${KN[@]}" get secret "${CRED_SECRET}" \
    -o jsonpath='{.data.aws_secret_access_key}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [ -n "${AWS_ACCESS_KEY_ID_VALUE}" ]; then
    log_info "Resolved credential Secret ${CRED_SECRET} (kept local)"
  else
    log_warn "Could not resolve creds from Secret ${CRED_SECRET} (healthy gpbackup limited)"
  fi
}

# ----------------------------------------------------------------------------
# STEP 0 — Preflight: cluster Ready; ensure mydb + public.users + rows + index.
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

  # Auto-discover the schedule CronJob name (resilient to naming changes).
  local discovered
  discovered="$("${KN[@]}" get cronjob \
    -l "cloudberry.avsoft.io/cluster=${CLUSTER}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -z "${discovered}" ]; then
    discovered="$("${KN[@]}" get cronjob -o name 2>/dev/null \
      | sed 's#^cronjob.batch/##' | grep "^${CLUSTER}-backup" | head -n1 || true)"
  fi
  if [ -n "${discovered}" ] && [ "${discovered}" != "${CRONJOB_NAME}" ]; then
    log_info "Discovered schedule CronJob: ${discovered} (was ${CRONJOB_NAME})"
    CRONJOB_NAME="${discovered}"
  fi
}

# ensure_database creates ${DB} with public.users + rows + a btree index.
ensure_database() {
  log_step "Ensuring database ${DB} + table ${SEED_TABLE} (${SEED_ROWS} rows + index)"

  if ! coord_psql postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql postgres "CREATE DATABASE ${DB};" >/dev/null
    log_info "Database ${DB} created"
  else
    log_info "Database ${DB} already exists"
  fi

  coord_psql "${DB}" "
    CREATE TABLE IF NOT EXISTS ${SEED_TABLE} (
      id bigint,
      payload text,
      created timestamptz DEFAULT now()
    ) DISTRIBUTED BY (id);
    INSERT INTO ${SEED_TABLE} (id, payload)
    SELECT g, repeat('a', 48)
    FROM generate_series(1, ${SEED_ROWS}) AS g
    WHERE NOT EXISTS (SELECT 1 FROM ${SEED_TABLE} LIMIT 1);
    CREATE INDEX IF NOT EXISTS users_id_idx ON ${SEED_TABLE} (id);
    ANALYZE ${SEED_TABLE};
  " >/dev/null
  local cnt
  cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${SEED_TABLE};" 2>/dev/null || echo '?')"
  log_info "Table ${SEED_TABLE}: ${cnt} rows"
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
  # Locate the repository root (this script lives in test/e2e/scripts/).
  local script_dir repo_root
  script_dir="$(cd "$(dirname "$0")" && pwd)"
  repo_root="$(cd "${script_dir}/../../.." && pwd)"
  if [ "${USE_MAKE}" = "1" ] && [ -f "${repo_root}/Makefile" ]; then
    log_info "make build (in ${repo_root})"
    ( cd "${repo_root}" && make build ) || die "make build failed"
    # Best-effort: locate a built cloudberry-ctl binary.
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
# OIDC token acquisition (Keycloak realm ${OIDC_REALM}) with the Host-header
# trick so the minted token's iss matches the operator's issuerURL.
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

  # Wait for the forward to become usable (the /healthz endpoint is unauthenticated).
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

# configure_cli exports the env the CLI reads for the API URL + OIDC token.
configure_cli() {
  log_step "Configuring cloudberry-ctl (API URL + OIDC bearer token)"
  export CLOUDBERRY_OPERATOR_URL="${OPERATOR_URL}"
  export CLOUDBERRY_AUTH_METHOD="oidc"
  export CLOUDBERRY_PASSWORD="${ACCESS_TOKEN}"
  export CLOUDBERRY_CLUSTER="${CLUSTER}"
  export CLOUDBERRY_NAMESPACE="${NAMESPACE}"
  export CLOUDBERRY_OUTPUT="json"
  log_info "CLI -> ${CLOUDBERRY_OPERATOR_URL} (auth=oidc cluster=${CLUSTER} ns=${NAMESPACE})"
}

# ctl runs the CLI binary (env-configured) and returns its exit code/output.
ctl() {
  # Run the CLI; on failure (commonly a short-lived OIDC token expiring during a
  # long suite), refresh the bearer token once and retry so late checks (86d) do
  # not flake. Token refresh is best-effort and silent on success.
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

# job_args prints the container[0] args of a Job as a single string.
job_args() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true
}

# latest_backup_job prints the most recently created backup-operation Job name.
latest_backup_job() {
  local op="${1:-backup}"
  "${KN[@]}" get jobs -l "avsoft.io/backup-operation=${op}" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true
}

# latest_finished_backup_job returns the newest backup Job that has finished
# (Complete OR Failed) — such Jobs' pods carry the full gpbackup output, which is
# what 86k needs to stream a recognizable backup log line. Falls back to the
# newest backup Job of any state.
latest_finished_backup_job() {
  local names cond name best=""
  names="$("${KN[@]}" get jobs -l 'avsoft.io/backup-operation=backup' \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  while IFS= read -r name; do
    [ -n "${name}" ] || continue
    cond="$("${KN[@]}" get job "${name}" \
      -o jsonpath='{.status.conditions[*].type}' 2>/dev/null || true)"
    case "${cond}" in
      *Complete*|*Failed*) best="${name}" ;;
    esac
  done <<EOF
${names}
EOF
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  latest_backup_job backup
}

# ----------------------------------------------------------------------------
# coord_render_s3_config writes the gpbackup_s3_plugin config inside the
# coordinator pod (10MB/4 multipart, mirrors the sample CR).
# ----------------------------------------------------------------------------
coord_render_s3_config() {
  # shellcheck disable=SC2016
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID_VALUE}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY_VALUE}" \
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      cat > /tmp/s3-config.yaml <<EOF
executablepath: ${GPHOME}/bin/gpbackup_s3_plugin
options:
  region: '"${S3_REGION}"'
  endpoint: '"${S3_ENDPOINT}"'
  aws_access_key_id: '"${AWS_ACCESS_KEY_ID_VALUE}"'
  aws_secret_access_key: '"${AWS_SECRET_ACCESS_KEY_VALUE}"'
  bucket: '"${S3_BUCKET}"'
  folder: '"${S3_FOLDER}"'
  encryption: "off"
  backup_multipart_chunksize: 10MB
  backup_max_concurrent_requests: 4
  restore_multipart_chunksize: 10MB
  restore_max_concurrent_requests: 4
EOF
      echo rendered'
}

# ----------------------------------------------------------------------------
# backup — a healthy REAL coordinator-exec gpbackup so "all backups successful".
# ----------------------------------------------------------------------------
run_healthy_backup() {
  log_step "backup — REAL coordinator-exec gpbackup of ${DB} to S3 (healthy backup)"
  if [ -z "${AWS_ACCESS_KEY_ID_VALUE}" ]; then
    log_warn "backup: S3 creds unavailable; SKIP healthy backup"; set_result backup SKIP; return
  fi
  coord_render_s3_config >/dev/null
  local out
  # shellcheck disable=SC2016
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID_VALUE}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY_VALUE}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  if printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    local ts
    ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
    [ -n "${ts}" ] && KNOWN_TS="${ts}"
    log_info "backup: healthy gpbackup completed; timestamp=${ts}"
    set_result backup PASS
  else
    printf '%s\n' "${out}" | tail -20 >&2
    log_warn "backup: healthy gpbackup did not complete (S3 reachable? creds?)"
    set_result backup FAIL
  fi
}

# =============================================================================
# 86a — backup create (3 variants) + Job arg assertions
# =============================================================================
run_86a() {
  log_step "86a — backup create (full / single-data-file / incremental)"
  ASSERT_FAIL=0

  # Variant 1: full (all primary flags).
  log_info "86a-1: create full (all primary flags)"
  if ! ctl backup create --cluster "${CLUSTER}" --database "${DB}" \
      --type full --compression-level 6 --compression-type zstd --jobs 4 \
      --include-schema public --exclude-table public.temp --with-stats --without-globals \
      >/dev/null 2>&1; then
    log_warn "86a-1: create full command failed"; ASSERT_FAIL=1
  fi
  sleep 2
  local job1 args1
  job1="$(latest_backup_job backup)"
  if [ -n "${job1}" ]; then
    LOGS_JOB="${job1}"   # most recent backup Job; 86k streams its logs.
    args1="$(job_args "${job1}")"
    assert_contains "${args1}" "--compression-level" "86a-1"
    assert_contains "${args1}" "--compression-type" "86a-1"
    assert_contains "${args1}" "zstd" "86a-1"
    assert_contains "${args1}" "--jobs" "86a-1"
    assert_contains "${args1}" "--include-schema" "86a-1"
    assert_contains "${args1}" "public" "86a-1"
    assert_contains "${args1}" "--exclude-table" "86a-1"
    assert_contains "${args1}" "public.temp" "86a-1"
    assert_contains "${args1}" "--with-stats" "86a-1"
    assert_contains "${args1}" "--without-globals" "86a-1"
    assert_absent   "${args1}" "--single-data-file" "86a-1"
    assert_absent   "${args1}" "--incremental" "86a-1"
  else
    log_warn "86a-1: could not find created backup Job"; ASSERT_FAIL=1
  fi

  # Variant 2: single-data-file.
  log_info "86a-2: create single-data-file"
  if ! ctl backup create --cluster "${CLUSTER}" --database "${DB}" \
      --type full --single-data-file --copy-queue-size 4 >/dev/null 2>&1; then
    log_warn "86a-2: create single-data-file command failed"; ASSERT_FAIL=1
  fi
  sleep 2
  local job2 args2
  job2="$(latest_backup_job backup)"
  if [ -n "${job2}" ] && [ "${job2}" != "${job1}" ]; then
    args2="$(job_args "${job2}")"
    assert_contains "${args2}" "--single-data-file" "86a-2"
    assert_contains "${args2}" "--copy-queue-size" "86a-2"
    assert_absent   "${args2}" "--jobs" "86a-2"
  else
    log_warn "86a-2: could not find a NEW single-data-file Job"; ASSERT_FAIL=1
  fi

  # Variant 3: incremental from the prior full timestamp.
  log_info "86a-3: create incremental"
  local from_ts="${KNOWN_TS}"
  [ -n "${from_ts}" ] || from_ts="$(ctl backup list 2>/dev/null | extract_json timestamp)"
  if [ -n "${from_ts}" ]; then
    if ! ctl backup create --cluster "${CLUSTER}" --database "${DB}" \
        --type incremental --incremental --from-timestamp "${from_ts}" \
        --leaf-partition-data >/dev/null 2>&1; then
      log_warn "86a-3: create incremental command failed"; ASSERT_FAIL=1
    fi
    sleep 2
    local job3 args3
    job3="$(latest_backup_job backup)"
    if [ -n "${job3}" ]; then
      args3="$(job_args "${job3}")"
      assert_contains "${args3}" "--incremental" "86a-3"
      assert_contains "${args3}" "--leaf-partition-data" "86a-3"
    else
      log_warn "86a-3: could not find incremental Job"; ASSERT_FAIL=1
    fi
  else
    log_warn "86a-3: no prior timestamp for --from-timestamp; skipping incremental"
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86a PASS; else set_result 86a FAIL; fi
}

# =============================================================================
# 86b — backup list -> shows backups
# =============================================================================
run_86b() {
  log_step "86b — backup list"
  local out
  out="$(ctl backup list --cluster "${CLUSTER}" 2>&1 || true)"
  ASSERT_FAIL=0
  assert_contains "${out}" "backups" "86b"
  # Capture a known timestamp for 86c/86d/86e from the nested backups[] array.
  local ts
  ts="$(printf '%s' "${out}" | extract_first_backup_ts)"
  [ -n "${ts}" ] && [ -z "${KNOWN_TS}" ] && KNOWN_TS="${ts}"
  [ -n "${KNOWN_TS}" ] && log_info "86b: a history timestamp = ${KNOWN_TS}"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86b PASS; else set_result 86b FAIL; fi
}

# =============================================================================
# 86c — backup status --timestamp <ts> -> one backup's detail
# =============================================================================
run_86c() {
  log_step "86c — backup status --timestamp"
  # Re-resolve a currently-valid timestamp from the live backup list so we never
  # depend on a stale value captured much earlier in the run.
  local ts
  ts="$(ctl backup list --cluster "${CLUSTER}" 2>/dev/null | extract_first_backup_ts || true)"
  [ -n "${ts}" ] || ts="${KNOWN_TS}"
  [ -n "${ts}" ] && KNOWN_TS="${ts}"
  if [ -z "${ts}" ]; then
    log_warn "86c: no known timestamp; SKIP"; set_result 86c SKIP; return
  fi
  local out
  out="$(ctl backup status --cluster "${CLUSTER}" --timestamp "${ts}" 2>&1 || true)"
  ASSERT_FAIL=0
  assert_contains "${out}" "${ts}" "86c"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86c PASS; else set_result 86c FAIL; fi
}

# =============================================================================
# 86e — backup restore (incl --resize-cluster) -> restore Job + arg assertions
# =============================================================================
run_86e() {
  log_step "86e — backup restore (--resize-cluster) + Job arg assertions"
  local ts="${KNOWN_TS}"
  [ -n "${ts}" ] || ts="20260101010101"

  if ! ctl backup restore --cluster "${CLUSTER}" --timestamp "${ts}" \
      --redirect-db "${RESTORE_DB}" --redirect-schema restored --create-db \
      --include-schema public --include-table public.users --jobs 4 \
      --with-stats --run-analyze --on-error-continue --truncate-table \
      --resize-cluster >/dev/null 2>&1; then
    log_warn "86e: restore command failed"; set_result 86e FAIL; return
  fi
  sleep 2
  local job args
  job="$(latest_backup_job restore)"
  if [ -z "${job}" ]; then
    log_warn "86e: could not find created restore Job"; set_result 86e FAIL; return
  fi
  log_info "86e: restore Job=${job}"
  args="$(job_args "${job}")"
  ASSERT_FAIL=0
  assert_contains "${args}" "--redirect-db" "86e"
  assert_contains "${args}" "${RESTORE_DB}" "86e"
  assert_contains "${args}" "--redirect-schema" "86e"
  assert_contains "${args}" "restored" "86e"
  assert_contains "${args}" "--create-db" "86e"
  assert_contains "${args}" "--run-analyze" "86e"
  assert_contains "${args}" "--on-error-continue" "86e"
  assert_contains "${args}" "--truncate-table" "86e"
  assert_contains "${args}" "--resize-cluster" "86e"
  assert_contains "${args}" "--timestamp" "86e"
  assert_contains "${args}" "${ts}" "86e"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86e PASS; else set_result 86e FAIL; fi
}

# =============================================================================
# 86f — backup schedule (show) -> prints CronJob status
# =============================================================================
run_86f() {
  log_step "86f — backup schedule (show)"
  local out
  out="$(ctl backup schedule --cluster "${CLUSTER}" 2>&1 || true)"
  ASSERT_FAIL=0
  assert_contains "${out}" "schedule" "86f"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86f PASS; else set_result 86f FAIL; fi
}

# =============================================================================
# 86g — backup schedule set --cron -> CronJob .spec.schedule updated
# =============================================================================
run_86g() {
  log_step "86g — backup schedule set --cron \"${CRON_NEW}\""
  if ! ctl backup schedule set --cluster "${CLUSTER}" --cron "${CRON_NEW}" >/dev/null 2>&1; then
    log_warn "86g: schedule set command failed"; set_result 86g FAIL; return
  fi
  # Allow the operator to reconcile spec.backup.schedule into the CronJob.
  local attempt=0 sched=""
  while [ "${attempt}" -lt 20 ]; do
    attempt=$(( attempt + 1 ))
    sched="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
      -o jsonpath='{.spec.schedule}' 2>/dev/null || true)"
    [ "${sched}" = "${CRON_NEW}" ] && break
    sleep 3
  done
  ASSERT_FAIL=0
  assert_contains "${sched}" "${CRON_NEW}" "86g"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86g PASS; else set_result 86g FAIL; fi
}

# =============================================================================
# 86h — backup schedule suspend -> CronJob .spec.suspend == true
# =============================================================================
run_86h() {
  log_step "86h — backup schedule suspend"
  if ! ctl backup schedule suspend --cluster "${CLUSTER}" >/dev/null 2>&1; then
    log_warn "86h: schedule suspend command failed"; set_result 86h FAIL; return
  fi
  sleep 2
  local suspend
  suspend="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.suspend}' 2>/dev/null || true)"
  ASSERT_FAIL=0
  assert_contains "${suspend}" "true" "86h"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86h PASS; else set_result 86h FAIL; fi
}

# =============================================================================
# 86i — backup schedule resume -> CronJob .spec.suspend == false
# =============================================================================
run_86i() {
  log_step "86i — backup schedule resume"
  if ! ctl backup schedule resume --cluster "${CLUSTER}" >/dev/null 2>&1; then
    log_warn "86i: schedule resume command failed"; set_result 86i FAIL; return
  fi
  sleep 2
  local suspend
  suspend="$("${KN[@]}" get cronjob "${CRONJOB_NAME}" \
    -o jsonpath='{.spec.suspend}' 2>/dev/null || true)"
  ASSERT_FAIL=0
  assert_contains "${suspend}" "false" "86i"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86i PASS; else set_result 86i FAIL; fi
}

# =============================================================================
# 86j — backup jobs -> lists backup/restore Jobs
# =============================================================================
run_86j() {
  log_step "86j — backup jobs"
  local out
  out="$(ctl backup jobs --cluster "${CLUSTER}" 2>&1 || true)"
  ASSERT_FAIL=0
  assert_contains "${out}" "jobs" "86j"
  # Capture a finished backup Job for 86k (its pod carries full gpbackup logs).
  if [ -z "${LOGS_JOB}" ]; then
    LOGS_JOB="$(latest_finished_backup_job)"
  fi
  [ -n "${LOGS_JOB}" ] && log_info "86j: logs candidate Job = ${LOGS_JOB}"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86j PASS; else set_result 86j FAIL; fi
}

# =============================================================================
# 86k — backup jobs logs --job <name> -> STREAMS the Job's pod logs
# =============================================================================
run_86k() {
  log_step "86k — backup jobs logs --job (streamed logs)"
  local job="${LOGS_JOB}"
  [ -n "${job}" ] || job="$(latest_finished_backup_job)"
  if [ -z "${job}" ]; then
    log_warn "86k: no backup Job available to stream logs; SKIP"; set_result 86k SKIP; return
  fi
  log_info "86k: streaming logs for Job=${job}"
  ASSERT_FAIL=0
  # The backup pod may be mid-(re)start when we first stream; retry a few times
  # to capture real gpbackup output instead of a transient empty/fallback read.
  local out="" attempt=0 matched=0
  while [ "${attempt}" -lt 8 ]; do
    attempt=$(( attempt + 1 ))
    # Re-pick a finished backup Job each attempt (the chosen one may be retrying).
    local cand="${job}"
    [ "${attempt}" -gt 1 ] && cand="$(latest_finished_backup_job)"
    [ -n "${cand}" ] || cand="${job}"
    out="$(ctl backup jobs logs --cluster "${CLUSTER}" --job "${cand}" 2>&1 || true)"
    case "${out}" in
      *gpbackup*|*"Backup "*|*"Restore "*|*"completed successfully"*)
        job="${cand}"; matched=1; break ;;
    esac
    sleep 4
  done
  if [ -z "${out}" ]; then
    log_warn "86k: streamed log output was EMPTY"; ASSERT_FAIL=1
  else
    log_info "86k: streamed $(printf '%s' "${out}" | wc -l | tr -d ' ') lines (Job=${job})"
    if [ "${matched}" -eq 1 ]; then
      log_info "86k: output contains a recognizable backup log line OK"
    else
      case "${out}" in
        *"kubectl logs"*)
          log_warn "86k: endpoint returned CLI fallback; no live pod logs"; ASSERT_FAIL=1 ;;
        *)
          log_warn "86k: output present but no recognizable backup log marker"; ASSERT_FAIL=1 ;;
      esac
    fi
  fi
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86k PASS; else set_result 86k FAIL; fi
}

# =============================================================================
# 86d — backup delete --timestamp <ts> -> cleanup Job created
# =============================================================================
run_86d() {
  log_step "86d — backup delete --timestamp (cleanup Job)"
  # Re-resolve a currently-valid timestamp from the live backup list (the cleanup
  # Job is named after it, so it must exist in history at delete time).
  local ts
  ts="$(ctl backup list --cluster "${CLUSTER}" 2>/dev/null | extract_first_backup_ts || true)"
  [ -n "${ts}" ] || ts="${KNOWN_TS}"
  [ -n "${ts}" ] || ts="20260101010101"

  # The delete may be transiently rate-limited (CLI rc=6 = 408/429) at the tail
  # of a long suite; retry with backoff so a healthy DELETE eventually lands.
  local del_out del_rc=1 d_attempt=0
  while [ "${d_attempt}" -lt 6 ]; do
    d_attempt=$(( d_attempt + 1 ))
    del_out="$(ctl backup delete --cluster "${CLUSTER}" --timestamp "${ts}" 2>&1)" \
      && del_rc=0 || del_rc=$?
    [ "${del_rc}" -eq 0 ] && break
    log_info "86d: delete attempt ${d_attempt} rc=${del_rc}; retrying..."
    sleep $(( d_attempt * 3 ))
  done
  if [ "${del_rc}" -ne 0 ]; then
    log_warn "86d: delete command failed (ts=${ts} rc=${del_rc}): ${del_out}"
    set_result 86d FAIL; return
  fi
  sleep 2
  local job op
  # Prefer the Job named after this exact timestamp; fall back to newest cleanup.
  job="$("${KN[@]}" get job "${CLUSTER}-cleanup-${ts}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || true)"
  [ -n "${job}" ] || job="$(latest_backup_job cleanup)"
  if [ -z "${job}" ]; then
    log_warn "86d: could not find created cleanup Job"; set_result 86d FAIL; return
  fi
  op="$("${KN[@]}" get job "${job}" \
    -o jsonpath='{.metadata.labels.avsoft\.io/backup-operation}' 2>/dev/null || true)"
  ASSERT_FAIL=0
  assert_contains "${op}" "cleanup" "86d"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 86d PASS; else set_result 86d FAIL; fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 86 — All CLI Commands: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  Restore DB: ${RESTORE_DB}"
  echo "  CLI binary     : ${CTL_BIN}"
  echo "  Operator URL   : ${OPERATOR_URL}"
  echo "  OIDC realm     : ${OIDC_REALM}  user: ${OIDC_ADMIN_USER}"
  echo "  Known TS       : ${KNOWN_TS:-<none>}  Logs Job: ${LOGS_JOB:-<none>}"
  local any_fail=0 c r
  for c in 86a 86b 86c 86d 86e 86f 86g 86h 86i 86j 86k backup; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 86 FAILED (one or more checks did not pass)"
    exit 1
  fi
  log_info "Scenario 86 CLI commands PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  TMPDIR_S86="$(mktemp -d 2>/dev/null || mktemp -d -t s86)"

  resolve_cluster
  preflight
  ensure_database
  build_cli
  obtain_oidc_token
  start_api_portforward
  configure_cli

  # The healthy real backup runs first so its timestamp lands in history for
  # 86c/86d/86e and a backup Job exists for 86a/86j/86k.
  if want_check backup; then run_healthy_backup; fi
  if want_check 86a; then run_86a; fi
  if want_check 86b; then run_86b; fi
  if want_check 86c; then run_86c; fi
  if want_check 86e; then run_86e; fi
  if want_check 86f; then run_86f; fi
  if want_check 86g; then run_86g; fi
  if want_check 86h; then run_86h; fi
  if want_check 86i; then run_86i; fi
  if want_check 86j; then run_86j; fi
  if want_check 86k; then run_86k; fi
  if want_check 86d; then run_86d; fi

  print_summary
}

main "$@"
