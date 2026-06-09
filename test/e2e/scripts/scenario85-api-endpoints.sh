#!/usr/bin/env bash
# =============================================================================
# Scenario 85 — All Backup REST API Endpoints (live verification)
# =============================================================================
# Drives ALL 7 operator backup REST API endpoints against an already-deployed
# Ready S3-destination cluster (scenario85-s3, backup enabled + a schedule for
# 85g), over the OIDC-authed TLS (Vault-PKI) REST API, and asserts the responses
# plus — for the two write endpoints — the args of the created backup/restore
# Jobs (and the cleanup Job for 85d).
#
# The 7 endpoints (base /api/v1alpha1/clusters/<name>/backups, RBAC in parens):
#   85a GET    /backups               (Basic)    -> list (status.BackupHistory).
#   85b POST   /backups               (Operator) -> Job whose args match the full
#                                                   gpbackupOptions, incl.
#                                                   --leaf-partition-data on a FULL
#                                                   backup (the GAP-B fix).
#   85c GET    /backups/{ts}          (Basic)    -> details for a timestamp.
#   85d DELETE /backups/{ts}          (Admin)    -> a cleanup Job (operation=cleanup).
#   85e POST   /backups/{ts}/restore  (Admin)    -> Job whose args match the full
#                                                   gprestoreOptions (--data-only/
#                                                   --resize-cluster/--redirect-db/...).
#   85f GET    /backups/jobs          (Basic)    -> backup/restore/cleanup statuses.
#   85g GET    /backups/schedule      (Basic)    -> CronJob status + nextScheduleTime.
#
# OIDC + PORT-FORWARD MODEL
# -------------------------
# The operator REST API is OIDC-authenticated and served over TLS (certificates
# issued from Vault PKI) on the API port. This script:
#   1. Obtains an OIDC bearer token from Keycloak: POST
#      ${KEYCLOAK_URL}/realms/${OIDC_REALM}/protocol/openid-connect/token with
#      client_id=${OIDC_CLIENT_ID} + client_secret=${OIDC_CLIENT_SECRET}, grant
#      grant_type=password for an ADMIN-role user (${OIDC_ADMIN_USER}/${OIDC_ADMIN_PASS})
#      so POST/DELETE/restore pass RBAC. Falls back to client_credentials if the
#      password grant is unavailable. Captures .access_token.
#   2. Discovers the operator REST API Service + its API port (default 8090) and
#      port-forwards it to a local ephemeral port.
#   3. Calls each endpoint over HTTPS with `Authorization: Bearer <token>`, using
#      the Vault-PKI CA (or `curl -k` for smoke).
#
# For 85b/85e: after the POST returns the Job name, the script fetches the Job
# (kubectl get job -o jsonpath='{.spec.template.spec.containers[0].args}') and
# asserts the container args contain the expected gpbackup/gprestore flags.
#
# A healthy real backup (coordinator-exec gpbackup to S3) is also run so "all
# backups successful" holds.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77..84:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into anything are base64-encoded (the only
#     embedded script here, the coordinator gpbackup, is run inline; the s3
#     config render mirrors scenario84). No YAML block-scalars are emitted.
#   * Use ENV for every tunable (NO hardcode). Run with ENV.
#   * Idempotent + re-runnable: an EXIT trap kills the port-forward and removes
#     temp files; reruns start clean.
#
# Usage:
#   scenario85-api-endpoints.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db mydb] \
#       [--checks 85a,85b,85c,85e,85d,85f,85g,backup]
#
# Environment (overridable; NO hardcode):
#   NAMESPACE             target namespace (default: cloudberry-test)
#   DB                    source database (default: mydb)
#   RESTORE_DB            restore target database (default: mydb_restore)
#   CHECKS                comma list of checks (default: all)
#   OPERATOR_NAMESPACE    operator namespace (default: cloudberry-system)
#   OPERATOR_LABEL        operator pod/service selector
#                         (default: app.kubernetes.io/name=cloudberry-operator)
#   API_SERVICE           operator REST API Service name (default: auto-discover)
#   API_PORT              operator REST API port (default: 8090)
#   API_SCHEME            https|http (default: https — Vault-PKI TLS)
#   CA_FILE               PEM CA bundle for the API TLS cert (default: empty -> curl -k)
#   KEYCLOAK_URL          Keycloak base URL
#                         (default: http://host.docker.internal:8090 ... see note)
#   OIDC_REALM            Keycloak realm (default: test)
#   OIDC_CLIENT_ID        OIDC client id (default: cloudberry-operator)
#   OIDC_CLIENT_SECRET    OIDC client secret (default: some-secret)
#   OIDC_ADMIN_USER       admin-role user (default: adminuser)
#   OIDC_ADMIN_PASS       admin-role password (default: adminpass)
#   CRED_SECRET           the S3 credential Secret name (default: backup-s3-credentials)
#   READY_TIMEOUT         cluster readiness timeout (default: 10m)
#   JOB_TIMEOUT           Job completion timeout (default: 10m)
#   SEED_ROWS             rows seeded into mydb (default: 5000)
#   COMPRESSION_LEVEL     --compression-level for the healthy gpbackup (default: 1)
#   S3_REGION/S3_ENDPOINT/S3_BUCKET/S3_FOLDER  S3 wiring (mirror the sample CR)
#
# NOTE on KEYCLOAK_URL: the operator's OIDC issuer in the sample CR is
#   http://host.docker.internal:8090/realms/test. From INSIDE the test host the
#   token endpoint is reached at the same authority; override KEYCLOAK_URL if
#   Keycloak is exposed elsewhere. The operator API port (also 8090 in-cluster)
#   is reached via the kubectl port-forward, NOT host.docker.internal, so the two
#   :8090 are distinct endpoints.
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing (ENV-driven; NO hardcode)
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
RESTORE_DB="${RESTORE_DB:-mydb_restore}"
CHECKS="${CHECKS:-85a,85b,85c,85e,85d,85f,85g,backup}"

# The operator is deployed into the test namespace in this environment
# (helm-install-test installs it in ${NAMESPACE}=cloudberry-test). Default the
# operator namespace to ${NAMESPACE}; override OPERATOR_NAMESPACE for a split
# (operator-in-cloudberry-system) production layout.
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-${NAMESPACE}}"
OPERATOR_LABEL="${OPERATOR_LABEL:-app.kubernetes.io/name=cloudberry-operator}"
API_SERVICE="${API_SERVICE:-}"
API_PORT="${API_PORT:-8090}"
# The operator REST API serves plain HTTP (api.StartServer uses ListenAndServe);
# auth is via the OIDC bearer token, not mTLS. Use http for the port-forwarded API.
API_SCHEME="${API_SCHEME:-http}"
CA_FILE="${CA_FILE:-}"

KEYCLOAK_URL="${KEYCLOAK_URL:-http://127.0.0.1:8090}"
OIDC_REALM="${OIDC_REALM:-test}"
OIDC_CLIENT_ID="${OIDC_CLIENT_ID:-cloudberry-operator}"
OIDC_CLIENT_SECRET="${OIDC_CLIENT_SECRET:-some-secret}"
OIDC_ADMIN_USER="${OIDC_ADMIN_USER:-adminuser}"
OIDC_ADMIN_PASS="${OIDC_ADMIN_PASS:-adminpass}"

CRED_SECRET="${CRED_SECRET:-backup-s3-credentials}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
JOB_TIMEOUT="${JOB_TIMEOUT:-10m}"
SEED_ROWS="${SEED_ROWS:-5000}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"

# S3 wiring (mirrors the scenario85-s3 sample CR). Used by the healthy gpbackup.
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-backups}"
S3_FOLDER="${S3_FOLDER:-scenario85}"

# The table Scenario 85 seeds in mydb.
SEED_TABLE="public.api_demo"

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
  sed -n '2,96p' "$0" | sed 's/^# \{0,1\}//'
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

[ -n "$CLUSTER" ] || die "--cluster is required (the S3 API cluster, e.g. scenario85-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
AWS_ACCESS_KEY_ID_VALUE=""
AWS_SECRET_ACCESS_KEY_VALUE=""
ACCESS_TOKEN=""
LOCAL_API_PORT=""
PF_PID=""
BASE=""
KNOWN_TS=""           # a timestamp known to be in history (85c/85d/85e use it).
CREATED_BACKUP_TS=""  # the ts produced by 85b (if it lands in history).
TMPDIR_S85=""

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_85A="SKIP"
RESULT_85B="SKIP"
RESULT_85C="SKIP"
RESULT_85D="SKIP"
RESULT_85E="SKIP"
RESULT_85F="SKIP"
RESULT_85G="SKIP"
RESULT_BACKUP="SKIP"

set_result() {
  case "$1" in
    85a)    RESULT_85A="$2" ;;
    85b)    RESULT_85B="$2" ;;
    85c)    RESULT_85C="$2" ;;
    85d)    RESULT_85D="$2" ;;
    85e)    RESULT_85E="$2" ;;
    85f)    RESULT_85F="$2" ;;
    85g)    RESULT_85G="$2" ;;
    backup) RESULT_BACKUP="$2" ;;
  esac
}
get_result() {
  case "$1" in
    85a)    printf '%s' "${RESULT_85A:-SKIP}" ;;
    85b)    printf '%s' "${RESULT_85B:-SKIP}" ;;
    85c)    printf '%s' "${RESULT_85C:-SKIP}" ;;
    85d)    printf '%s' "${RESULT_85D:-SKIP}" ;;
    85e)    printf '%s' "${RESULT_85E:-SKIP}" ;;
    85f)    printf '%s' "${RESULT_85F:-SKIP}" ;;
    85g)    printf '%s' "${RESULT_85G:-SKIP}" ;;
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

# ----------------------------------------------------------------------------
# Cleanup trap: kill the port-forward + remove temp files so reruns start clean
# (idempotent / re-runnable).
# ----------------------------------------------------------------------------
cleanup() {
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  [ -n "${TMPDIR_S85}" ] && rm -rf "${TMPDIR_S85}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# coord_psql: exec into the coordinator pod and run SQL as gpadmin.
# Args: <database> <sql>. Inputs are passed as positional args ($1..$3) so
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
# STEP 0 — Preflight: cluster Ready; ensure mydb + a table + rows + index.
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

# ensure_database creates ${DB} with a table + rows + a btree index. Idempotent.
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
    CREATE INDEX IF NOT EXISTS api_demo_id_idx ON ${SEED_TABLE} (id);
    ANALYZE ${SEED_TABLE};
  " >/dev/null
  local cnt
  cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${SEED_TABLE};" 2>/dev/null || echo '?')"
  log_info "Table ${SEED_TABLE}: ${cnt} rows"
}

# ----------------------------------------------------------------------------
# OIDC token acquisition (Keycloak realm ${OIDC_REALM}).
# ----------------------------------------------------------------------------
obtain_oidc_token() {
  log_step "Obtaining OIDC bearer token from Keycloak (realm ${OIDC_REALM}, client ${OIDC_CLIENT_ID})"
  command -v curl >/dev/null 2>&1 || die "curl not found (required for OIDC token + API calls)"

  local token_url resp
  token_url="${KEYCLOAK_URL}/realms/${OIDC_REALM}/protocol/openid-connect/token"

  # Keycloak mints the token's `iss` from the request authority. The operator
  # validates `iss` against its configured issuerURL (host.docker.internal:8090).
  # So reach Keycloak at the host-reachable KEYCLOAK_URL (127.0.0.1:8090) but send
  # a Host header of ${ISSUER_HOST} so the minted token's iss matches the operator.
  local issuer_host="${ISSUER_HOST:-host.docker.internal:8090}"

  # Primary: password grant for an admin-role user (so POST/DELETE/restore pass RBAC).
  resp="$(curl -sS --max-time 30 -X POST "${token_url}" \
    -H "Host: ${issuer_host}" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=password" \
    --data-urlencode "client_id=${OIDC_CLIENT_ID}" \
    --data-urlencode "client_secret=${OIDC_CLIENT_SECRET}" \
    --data-urlencode "username=${OIDC_ADMIN_USER}" \
    --data-urlencode "password=${OIDC_ADMIN_PASS}" \
    --data-urlencode "scope=openid profile email" 2>/dev/null || true)"
  ACCESS_TOKEN="$(printf '%s' "${resp}" | extract_json access_token)"

  # Fallback: client_credentials (if that client maps to admin).
  if [ -z "${ACCESS_TOKEN}" ]; then
    log_warn "password grant produced no token; trying client_credentials"
    resp="$(curl -sS --max-time 30 -X POST "${token_url}" \
      -H "Host: ${issuer_host}" \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode "grant_type=client_credentials" \
      --data-urlencode "client_id=${OIDC_CLIENT_ID}" \
      --data-urlencode "client_secret=${OIDC_CLIENT_SECRET}" 2>/dev/null || true)"
    ACCESS_TOKEN="$(printf '%s' "${resp}" | extract_json access_token)"
  fi

  [ -n "${ACCESS_TOKEN}" ] || die "could not obtain an OIDC access_token from ${token_url}"
  log_info "OIDC access_token acquired (len ${#ACCESS_TOKEN})"
}

# extract_json <key> reads a flat JSON object on stdin and prints the string
# value for <key> (best-effort; jq when present, else a sed fallback).
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

# ----------------------------------------------------------------------------
# Operator REST API access: discover the API Service + port, port-forward, set BASE.
# ----------------------------------------------------------------------------
start_api_portforward() {
  log_step "Port-forwarding operator REST API (TLS ${API_SCHEME}, port ${API_PORT})"

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
    # Fall back to the operator pod directly.
    local op_pod
    op_pod="$("${KUBECTL}" -n "${OPERATOR_NAMESPACE}" get pods \
      -l "${OPERATOR_LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    [ -n "${op_pod}" ] || die "operator API Service/pod not found in ns ${OPERATOR_NAMESPACE} (label ${OPERATOR_LABEL})"
    log_info "Operator API pod: ${op_pod} (ns ${OPERATOR_NAMESPACE})"
    "${KUBECTL}" -n "${OPERATOR_NAMESPACE}" port-forward "pod/${op_pod}" \
      "${LOCAL_API_PORT}:${API_PORT}" >/dev/null 2>&1 &
    PF_PID=$!
  fi

  BASE="${API_SCHEME}://127.0.0.1:${LOCAL_API_PORT}/api/v1alpha1/clusters/${CLUSTER}"

  # Wait for the forward to become usable (the /healthz endpoint is unauthenticated).
  local attempt=0 health="${API_SCHEME}://127.0.0.1:${LOCAL_API_PORT}/healthz"
  while [ "${attempt}" -lt 30 ]; do
    attempt=$(( attempt + 1 ))
    if curl_api -o /dev/null --max-time 3 "${health}" 2>/dev/null; then
      break
    fi
    if ! kill -0 "${PF_PID}" 2>/dev/null; then
      die "port-forward to operator API died"
    fi
    sleep 1
  done
  log_info "Operator API reachable at ${API_SCHEME}://127.0.0.1:${LOCAL_API_PORT} (BASE ${BASE})"
}

# curl_api wraps curl with the TLS CA (or -k) — does NOT add auth (for /healthz).
curl_api() {
  if [ -n "${CA_FILE}" ] && [ -f "${CA_FILE}" ]; then
    curl -sf --cacert "${CA_FILE}" "$@"
  else
    curl -sfk "$@"
  fi
}

# api_call <method> <path> [json-body]  -> prints "<http_code>\n<body>".
# Authenticated with the OIDC bearer token, over TLS (CA or -k).
api_call() {
  local method="$1" path="$2" body="${3:-}" url tls
  url="${BASE}${path}"
  if [ -n "${CA_FILE}" ] && [ -f "${CA_FILE}" ]; then
    tls=(--cacert "${CA_FILE}")
  else
    tls=(-k)
  fi
  if [ -n "${body}" ]; then
    curl -sS "${tls[@]}" -w $'\n%{http_code}' \
      -X "${method}" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
      -H 'Content-Type: application/json' --max-time 60 \
      -d "${body}" "${url}" 2>/dev/null || true
  else
    curl -sS "${tls[@]}" -w $'\n%{http_code}' \
      -X "${method}" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
      --max-time 60 "${url}" 2>/dev/null || true
  fi
}

# http_code / http_body split the api_call output (last line is the code).
http_code() { printf '%s' "$1" | tail -1; }
http_body() { printf '%s' "$1" | sed '$d'; }

# job_args prints the container[0] args of a Job as a single string.
job_args() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true
}

# assert_contains <haystack> <needle> <label> -> sets ASSERT_FAIL on miss.
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

# =============================================================================
# 85a — GET /backups -> 200 + backups/total
# =============================================================================
run_85a() {
  log_step "85a — GET /backups (list)"
  local out code body
  out="$(api_call GET /backups)"
  code="$(http_code "${out}")"; body="$(http_body "${out}")"
  if [ "${code}" != "200" ]; then
    log_warn "85a: expected 200, got ${code}: ${body}"; set_result 85a FAIL; return
  fi
  ASSERT_FAIL=0
  assert_contains "${body}" '"total"' "85a"
  assert_contains "${body}" '"backups"' "85a"
  # Capture a known timestamp from history (if any) for 85c/85d/85e.
  KNOWN_TS="$(printf '%s' "${body}" | extract_json timestamp)"
  [ -n "${KNOWN_TS}" ] && log_info "85a: a history timestamp = ${KNOWN_TS}"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 85a PASS; else set_result 85a FAIL; fi
}

# =============================================================================
# 85b — POST /backups (FULL) -> 202 + job; assert Job args
# =============================================================================
run_85b() {
  log_step "85b — POST /backups (full Create Backup Request) + Job arg assertions"
  local body out code resp job args
  body='{
    "type": "full",
    "databases": ["'"${DB}"'"],
    "gpbackupOptions": {
      "singleDataFile": true,
      "copyQueueSize": 8,
      "includeSchemas": ["public"],
      "excludeTables": ["public.tmp"],
      "leafPartitionData": true,
      "withStats": true,
      "withoutGlobals": true
    }
  }'
  out="$(api_call POST /backups "${body}")"
  code="$(http_code "${out}")"; resp="$(http_body "${out}")"
  if [ "${code}" != "202" ]; then
    log_warn "85b: expected 202, got ${code}: ${resp}"; set_result 85b FAIL; return
  fi
  job="$(printf '%s' "${resp}" | extract_json job)"
  CREATED_BACKUP_TS="$(printf '%s' "${resp}" | extract_json timestamp)"
  if [ -z "${job}" ]; then
    log_warn "85b: no job name in response: ${resp}"; set_result 85b FAIL; return
  fi
  log_info "85b: backup Job=${job} ts=${CREATED_BACKUP_TS}"

  # Fetch the created Job and assert its container args.
  args="$(job_args "${job}")"
  if [ -z "${args}" ]; then
    log_warn "85b: could not read args of Job ${job}"; set_result 85b FAIL; return
  fi
  ASSERT_FAIL=0
  assert_contains "${args}" "--single-data-file" "85b"
  assert_contains "${args}" "--copy-queue-size" "85b"
  assert_contains "${args}" "8" "85b"
  assert_contains "${args}" "--include-schema" "85b"
  assert_contains "${args}" "public" "85b"
  assert_contains "${args}" "--exclude-table" "85b"
  assert_contains "${args}" "public.tmp" "85b"
  assert_contains "${args}" "--leaf-partition-data" "85b"
  assert_contains "${args}" "--with-stats" "85b"
  assert_contains "${args}" "--without-globals" "85b"
  assert_contains "${args}" "--dbname" "85b"
  assert_contains "${args}" "${DB}" "85b"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 85b PASS; else set_result 85b FAIL; fi
}

# =============================================================================
# 85c — GET /backups/{ts} -> 200
# =============================================================================
run_85c() {
  log_step "85c — GET /backups/{ts} (details)"
  local ts out code body
  ts="${KNOWN_TS}"
  [ -n "${ts}" ] || ts="${CREATED_BACKUP_TS}"
  if [ -z "${ts}" ]; then
    log_warn "85c: no known timestamp (history empty + 85b ts not recorded yet); SKIP"
    set_result 85c SKIP; return
  fi
  out="$(api_call GET "/backups/${ts}")"
  code="$(http_code "${out}")"; body="$(http_body "${out}")"
  if [ "${code}" = "200" ]; then
    log_info "85c: GET /backups/${ts} -> 200 OK"
    set_result 85c PASS
  else
    log_warn "85c: expected 200 for ts ${ts}, got ${code}: ${body}"
    set_result 85c FAIL
  fi
}

# =============================================================================
# 85e — POST /backups/{ts}/restore (FULL) -> 202 + restore job; assert args.
#       Negative: dataOnly+metadataOnly -> 400.
# =============================================================================
run_85e() {
  log_step "85e — POST /backups/{ts}/restore (full Restore Request) + Job arg assertions"
  local ts out code resp job args negout negcode
  ts="${KNOWN_TS}"
  [ -n "${ts}" ] || ts="${CREATED_BACKUP_TS}"
  [ -n "${ts}" ] || ts="20260101010101"

  local body='{
    "databases": ["'"${DB}"'"],
    "gprestoreOptions": {
      "redirectDb": "'"${RESTORE_DB}"'",
      "createDb": true,
      "withGlobals": true,
      "runAnalyze": true,
      "onErrorContinue": true,
      "truncateTable": true,
      "dataOnly": true,
      "resizeCluster": true
    }
  }'
  out="$(api_call POST "/backups/${ts}/restore" "${body}")"
  code="$(http_code "${out}")"; resp="$(http_body "${out}")"
  if [ "${code}" != "202" ]; then
    log_warn "85e: expected 202, got ${code}: ${resp}"; set_result 85e FAIL; return
  fi
  job="$(printf '%s' "${resp}" | extract_json job)"
  if [ -z "${job}" ]; then
    log_warn "85e: no restore job name in response: ${resp}"; set_result 85e FAIL; return
  fi
  log_info "85e: restore Job=${job} ts=${ts}"

  args="$(job_args "${job}")"
  if [ -z "${args}" ]; then
    log_warn "85e: could not read args of Job ${job}"; set_result 85e FAIL; return
  fi
  ASSERT_FAIL=0
  assert_contains "${args}" "--redirect-db" "85e"
  assert_contains "${args}" "${RESTORE_DB}" "85e"
  assert_contains "${args}" "--create-db" "85e"
  assert_contains "${args}" "--with-globals" "85e"
  assert_contains "${args}" "--run-analyze" "85e"
  assert_contains "${args}" "--on-error-continue" "85e"
  assert_contains "${args}" "--truncate-table" "85e"
  assert_contains "${args}" "--data-only" "85e"
  assert_contains "${args}" "--resize-cluster" "85e"
  assert_contains "${args}" "--timestamp" "85e"
  assert_contains "${args}" "${ts}" "85e"
  assert_absent   "${args}" "--metadata-only" "85e"

  # Negative: dataOnly + metadataOnly -> 400.
  negout="$(api_call POST "/backups/${ts}/restore" \
    '{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}')"
  negcode="$(http_code "${negout}")"
  if [ "${negcode}" = "400" ]; then
    log_info "85e: dataOnly+metadataOnly -> 400 OK"
  else
    log_warn "85e: dataOnly+metadataOnly expected 400, got ${negcode}"; ASSERT_FAIL=1
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 85e PASS; else set_result 85e FAIL; fi
}

# =============================================================================
# 85d — DELETE /backups/{ts} -> 2xx + a cleanup Job (operation=cleanup)
# =============================================================================
run_85d() {
  log_step "85d — DELETE /backups/{ts} (cleanup) + cleanup Job assertion"
  local ts out code resp job op
  ts="${KNOWN_TS}"
  [ -n "${ts}" ] || ts="${CREATED_BACKUP_TS}"
  [ -n "${ts}" ] || ts="20260101010101"

  out="$(api_call DELETE "/backups/${ts}")"
  code="$(http_code "${out}")"; resp="$(http_body "${out}")"
  case "${code}" in
    2*) : ;;
    *) log_warn "85d: expected 2xx, got ${code}: ${resp}"; set_result 85d FAIL; return ;;
  esac
  job="$(printf '%s' "${resp}" | extract_json job)"
  if [ -z "${job}" ]; then
    log_warn "85d: no cleanup job name in response: ${resp}"; set_result 85d FAIL; return
  fi
  op="$("${KN[@]}" get job "${job}" \
    -o jsonpath='{.metadata.labels.avsoft\.io/backup-operation}' 2>/dev/null || true)"
  if [ "${op}" = "cleanup" ]; then
    log_info "85d: cleanup Job=${job} operation=cleanup OK"
    set_result 85d PASS
  else
    log_warn "85d: Job ${job} backup-operation='${op}' (expected cleanup)"
    set_result 85d FAIL
  fi
}

# =============================================================================
# 85f — GET /backups/jobs -> 200 + jobs list
# =============================================================================
run_85f() {
  log_step "85f — GET /backups/jobs (job statuses)"
  local out code body
  out="$(api_call GET /backups/jobs)"
  code="$(http_code "${out}")"; body="$(http_body "${out}")"
  if [ "${code}" != "200" ]; then
    log_warn "85f: expected 200, got ${code}: ${body}"; set_result 85f FAIL; return
  fi
  ASSERT_FAIL=0
  assert_contains "${body}" '"jobs"' "85f"
  assert_contains "${body}" '"total"' "85f"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 85f PASS; else set_result 85f FAIL; fi
}

# =============================================================================
# 85g — GET /backups/schedule -> 200 + schedule + nextScheduleTime
# =============================================================================
run_85g() {
  log_step "85g — GET /backups/schedule (CronJob status + nextScheduleTime)"
  local out code body
  out="$(api_call GET /backups/schedule)"
  code="$(http_code "${out}")"; body="$(http_body "${out}")"
  if [ "${code}" != "200" ]; then
    log_warn "85g: expected 200, got ${code}: ${body}"; set_result 85g FAIL; return
  fi
  ASSERT_FAIL=0
  assert_contains "${body}" '"scheduled"' "85g"
  assert_contains "${body}" '"schedule"' "85g"
  assert_contains "${body}" '"nextScheduleTime"' "85g"
  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 85g PASS; else set_result 85g FAIL; fi
}

# ----------------------------------------------------------------------------
# coord_render_s3_config writes the gpbackup_s3_plugin config to
# /tmp/s3-config.yaml inside the coordinator pod (10MB/4 multipart).
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

# =============================================================================
# backup — a healthy REAL coordinator-exec gpbackup so "all backups successful".
# =============================================================================
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

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 85 — All API Endpoints: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  Restore DB: ${RESTORE_DB}"
  echo "  API BASE       : ${BASE}"
  echo "  OIDC realm     : ${OIDC_REALM}  client: ${OIDC_CLIENT_ID}  user: ${OIDC_ADMIN_USER}"
  echo "  Known TS       : ${KNOWN_TS:-<none>}  Created TS: ${CREATED_BACKUP_TS:-<none>}"
  local any_fail=0 c r
  for c in 85a 85b 85c 85d 85e 85f 85g backup; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 85 FAILED (one or more checks did not pass)"
    exit 1
  fi
  log_info "Scenario 85 API endpoints PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  TMPDIR_S85="$(mktemp -d 2>/dev/null || mktemp -d -t s85)"

  resolve_cluster
  preflight
  ensure_database
  obtain_oidc_token
  start_api_portforward

  # 85b is run before 85c/85d/85e so a fresh timestamp exists; the healthy real
  # backup runs first so its timestamp lands in history for 85c.
  if want_check backup; then run_healthy_backup; fi
  if want_check 85a; then run_85a; fi
  if want_check 85b; then run_85b; fi
  if want_check 85c; then run_85c; fi
  if want_check 85e; then run_85e; fi
  if want_check 85d; then run_85d; fi
  if want_check 85f; then run_85f; fi
  if want_check 85g; then run_85g; fi

  print_summary
}

main "$@"
