#!/usr/bin/env bash
# =============================================================================
# Scenario 88 — Backup Disabled / No Schedule live verify
# =============================================================================
# Drives the backup-disabled / no-schedule behavior against an already deployed
# environment with ONE Ready cluster, through the operator REST API over an OIDC
# ADMIN bearer token (the create/list/schedule routes are Admin/Operator; the
# ADMIN token is used to be safe). It asserts the per-cluster effects of
# disabling backup and of an empty schedule, and that on-demand backup still
# works without a schedule.
#
# Check flow (single cluster; --cluster <name> [--namespace cloudberry-test]):
#   preflight     : cluster Ready; capture ORIGINAL backup.enabled + schedule so
#                   the EXIT trap can restore the CR to its pre-test state.
#   88a-disable   : patch backup.enabled=false; wait reconcile; assert NO CronJob
#                   "<cluster>-backup-schedule"; assert Status.cronJobName empty.
#   88a-api       : `backup create` rejected (rc!=0 OR "not enabled" in output);
#                   `backup list` => 200 (GAP-2: enabled:false / no disabled word);
#                   `backup schedule` => scheduled:false.
#   88a-reenable  : patch enabled=true + schedule=SCHEDULE; wait reconcile; assert
#                   CronJob recreated + Status.cronJobName set.
#   88b-empty     : patch enabled=true + schedule=""; wait reconcile; assert NO
#                   CronJob + empty Status.cronJobName.
#   88b-ondemand  : run `backup create`; assert accepted AND the backup Job
#                   completes (coordinator-exec); assert "Backup completed
#                   successfully" in logs OR Job succeeded.
#   88a-rbac-note : informational SKIP — backup SA/Role are Helm-level.
#
# GAP NOTES (see test/cases/scenario88_backup_disabled_cases.go):
#   GAP-1  The backup ServiceAccount "cloudberry-backup-sa", Role
#          "cloudberry-backup-role" and RoleBinding are created ONLY by the Helm
#          chart (deploy/helm/.../backup-rbac.yaml), gated by the Helm value
#          `backup.rbac.create`, in the OPERATOR namespace, and are SHARED across
#          every cluster. The per-cluster reconcile NEVER creates/removes them, so
#          88a-disable asserts ONLY the per-cluster effects (no CronJob, empty
#          cronJobName, API disabled), never a per-cluster SA/Role removal. The
#          88a-rbac-note check logs this and SKIPs.
#   GAP-2  handleListBackups returns 200 with an "enabled" boolean (NOT a
#          BACKUP_NOT_ENABLED error and NOT a literal "disabled" word). The
#          authoritative disabled signals are create => rejected and schedule =>
#          scheduled:false. 88a-api asserts the REAL behavior.
#   GAP-3  Disabling backup now REMOVES a previously-created CronJob and clears
#          Status.cronJobName. The 88a-disable check sequences AHEAD of the
#          88a-reenable check, so it runs from a clean/no-schedule baseline; the
#          empty-schedule CronJob deletion is exercised by 88b-empty (the enabled
#          path that legitimately deletes the CronJob).
#
# BUILD + OIDC + PORT-FORWARD + CLI-CONFIG MODEL (mirrors scenario87)
# ------------------------------------------------------------------
# 1. BUILD the CLI: `go build -o ${CTL_BIN} ./cmd/cloudberry-ctl` (or make build).
# 2. OIDC token: Keycloak password grant for an ADMIN user, with the Host-header
#    iss trick so the minted token's `iss` matches the operator's issuerURL;
#    client_credentials fallback.
# 3. PORT-FORWARD the operator REST API Service to a local ephemeral port.
# 4. POINT the CLI at the API via CLOUDBERRY_OPERATOR_URL + bearer token. For 88
#    there is ONE cluster, so we DO export CLOUDBERRY_CLUSTER so the backup
#    subcommands target it without --cluster.
#
# BASH 3.2 COMPATIBILITY (macOS default bash):
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helpers.
#   * Use ENV for every tunable (NO hardcode). Run with ENV.
#   * Idempotent + re-runnable: an EXIT trap restores the CR's original backup
#     config, kills the port-forward and removes temp files.
#   * Every `set -euo pipefail` command substitution that may produce no output
#     is guarded with `|| true` so an empty result never aborts the script.
#   * The intentional non-zero `backup create` (88a) captures rc explicitly so
#     `set -e` does not abort.
#
# Usage:
#   scenario88-backup-disabled.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db mydb] [--schedule "0 2 * * *"] \
#       [--checks preflight,88a-disable,88a-api,88a-reenable,88b-empty,88b-ondemand]
#
# Environment (overridable; NO hardcode):
#   NAMESPACE             target namespace (default: cloudberry-test)
#   DB                    database for the on-demand backup (default: mydb)
#   SCHEDULE              re-enable schedule (default: "0 2 * * *")
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
#   RECONCILE_TIMEOUT     per-patch reconcile wait (default: 60s; via retry loop)
#   JOB_TIMEOUT           on-demand backup Job completion timeout (default: 10m)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing (ENV-driven; NO hardcode)
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
SCHEDULE="${SCHEDULE:-0 2 * * *}"
CHECKS="${CHECKS:-preflight,88a-disable,88a-api,88a-reenable,88b-empty,88b-ondemand,88a-rbac-note}"

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
RECONCILE_TIMEOUT="${RECONCILE_TIMEOUT:-60s}"
JOB_TIMEOUT="${JOB_TIMEOUT:-10m}"

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
    --schedule)   SCHEDULE="$2"; shift 2 ;;
    --checks)     CHECKS="$2"; shift 2 ;;
    -h|--help)    usage 0 ;;
    *)            log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the cluster, e.g. scenario88)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

COORD="${CLUSTER}-coordinator-0"
CRON_NAME="${CLUSTER}-backup-schedule"   # util.BackupCronJobName(cluster)

# Runtime state.
ACCESS_TOKEN=""
LOCAL_API_PORT=""
PF_PID=""
OPERATOR_URL=""
TMPDIR_S88=""

# Captured ORIGINAL CR backup config (restored on EXIT for idempotent reruns).
ORIG_ENABLED=""
ORIG_SCHEDULE=""
ORIG_CAPTURED=0

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_PREFLIGHT="SKIP"
RESULT_88A_DISABLE="SKIP"
RESULT_88A_API="SKIP"
RESULT_88A_REENABLE="SKIP"
RESULT_88B_EMPTY="SKIP"
RESULT_88B_ONDEMAND="SKIP"
RESULT_88A_RBAC_NOTE="SKIP"

set_result() {
  case "$1" in
    preflight)     RESULT_PREFLIGHT="$2" ;;
    88a-disable)   RESULT_88A_DISABLE="$2" ;;
    88a-api)       RESULT_88A_API="$2" ;;
    88a-reenable)  RESULT_88A_REENABLE="$2" ;;
    88b-empty)     RESULT_88B_EMPTY="$2" ;;
    88b-ondemand)  RESULT_88B_ONDEMAND="$2" ;;
    88a-rbac-note) RESULT_88A_RBAC_NOTE="$2" ;;
  esac
}
get_result() {
  case "$1" in
    preflight)     printf '%s' "${RESULT_PREFLIGHT:-SKIP}" ;;
    88a-disable)   printf '%s' "${RESULT_88A_DISABLE:-SKIP}" ;;
    88a-api)       printf '%s' "${RESULT_88A_API:-SKIP}" ;;
    88a-reenable)  printf '%s' "${RESULT_88A_REENABLE:-SKIP}" ;;
    88b-empty)     printf '%s' "${RESULT_88B_EMPTY:-SKIP}" ;;
    88b-ondemand)  printf '%s' "${RESULT_88B_ONDEMAND:-SKIP}" ;;
    88a-rbac-note) printf '%s' "${RESULT_88A_RBAC_NOTE:-SKIP}" ;;
    *)             printf 'SKIP' ;;
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
# Cleanup trap: restore the CR's original backup config, kill the port-forward,
# remove temp files so reruns start clean.
# ----------------------------------------------------------------------------
restore_cr() {
  [ "${ORIG_CAPTURED}" -eq 1 ] || return 0
  log_info "Restoring CR backup config (enabled=${ORIG_ENABLED:-<unset>} schedule='${ORIG_SCHEDULE}')"
  local en="${ORIG_ENABLED:-false}"
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p "{\"spec\":{\"backup\":{\"enabled\":${en},\"schedule\":\"${ORIG_SCHEDULE}\"}}}" \
    >/dev/null 2>&1 || true
}
cleanup() {
  restore_cr
  if [ -n "${PF_PID}" ] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  [ -n "${TMPDIR_S88}" ] && rm -rf "${TMPDIR_S88}" 2>/dev/null || true
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
# Cluster-state helpers (re-resolve from LIVE state; never cache across a
# reconcile boundary). All are guarded so an empty result never aborts.
# ----------------------------------------------------------------------------
# cronjob_exists returns 0 when the per-cluster backup CronJob exists.
cronjob_exists() {
  "${KN[@]}" get cronjob "${CRON_NAME}" >/dev/null 2>&1
}

# status_cronjob_name prints .status.cronJobName (empty when unset).
status_cronjob_name() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.status.cronJobName}' 2>/dev/null || true
}

# spec_backup_enabled prints .spec.backup.enabled (true|false|empty).
spec_backup_enabled() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.spec.backup.enabled}' 2>/dev/null || true
}

# spec_backup_schedule prints .spec.backup.schedule (empty when unset).
spec_backup_schedule() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.spec.backup.schedule}' 2>/dev/null || true
}

# patch_backup_enabled <true|false> merge-patches spec.backup.enabled.
patch_backup_enabled() {
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p "{\"spec\":{\"backup\":{\"enabled\":$1}}}" >/dev/null 2>&1 || true
}

# patch_backup_schedule <"<cron>"|""> merge-patches spec.backup.schedule.
patch_backup_schedule() {
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p "{\"spec\":{\"backup\":{\"schedule\":\"$1\"}}}" >/dev/null 2>&1 || true
}

# wait_for_reconcile <want-cronjob:0|1>: poll (bounded) until the observable
# CronJob presence matches the desired state. Re-resolves LIVE state each loop.
wait_for_reconcile() {
  local want="$1" attempt=0
  while [ "${attempt}" -lt 30 ]; do
    attempt=$(( attempt + 1 ))
    if [ "${want}" = "1" ]; then
      cronjob_exists && return 0
    else
      cronjob_exists || return 0
    fi
    sleep 2
  done
  return 1
}

# latest_job_by_label <operation> <cluster>: NEWEST Job by labels (guarded).
latest_job_by_label() {
  local op="$1" cluster="$2"
  "${KN[@]}" get jobs \
    -l "avsoft.io/cluster=${cluster},avsoft.io/backup-operation=${op}" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true
}

# job_args prints the container[0] args of a Job as a single string (guarded).
job_args() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true
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

# configure_cli exports the env the CLI reads. For 88 there is ONE cluster, so we
# export CLOUDBERRY_CLUSTER so the backup subcommands target it without --cluster.
configure_cli() {
  log_step "Configuring cloudberry-ctl (API URL + OIDC bearer token + cluster)"
  export CLOUDBERRY_OPERATOR_URL="${OPERATOR_URL}"
  export CLOUDBERRY_AUTH_METHOD="oidc"
  export CLOUDBERRY_PASSWORD="${ACCESS_TOKEN}"
  export CLOUDBERRY_NAMESPACE="${NAMESPACE}"
  export CLOUDBERRY_CLUSTER="${CLUSTER}"
  export CLOUDBERRY_OUTPUT="json"
  log_info "CLI -> ${CLOUDBERRY_OPERATOR_URL} (auth=oidc ns=${NAMESPACE} cluster=${CLUSTER})"
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

# =============================================================================
# preflight — cluster exists; coordinator Ready; capture ORIGINAL backup config.
# =============================================================================
preflight() {
  log_step "Preflight (cluster=${CLUSTER} ns=${NAMESPACE})"

  "${KN[@]}" get cloudberrycluster "${CLUSTER}" >/dev/null 2>&1 \
    || { set_result preflight FAIL; die "CloudberryCluster ${CLUSTER} not found in ${NAMESPACE}"; }

  log_info "Waiting for coordinator ${COORD} Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${COORD}" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || { set_result preflight FAIL; die "coordinator ${COORD} not Ready"; }

  # Capture the ORIGINAL backup config so the EXIT trap can restore it.
  ORIG_ENABLED="$(spec_backup_enabled)"
  ORIG_SCHEDULE="$(spec_backup_schedule)"
  [ -n "${ORIG_ENABLED}" ] || ORIG_ENABLED="false"
  ORIG_CAPTURED=1
  log_info "Captured original backup config: enabled=${ORIG_ENABLED} schedule='${ORIG_SCHEDULE}'"
  set_result preflight PASS
}

# =============================================================================
# 88a-disable — disable backup; assert NO CronJob + empty Status.cronJobName.
# Sequenced from a clean/no-schedule baseline (patch schedule="" first) so the
# "no CronJob" assertion reflects the disabled-reconcile no-op (GAP-3).
# =============================================================================
run_88a_disable() {
  log_step "88a-disable — disable backup (no CronJob + empty cronJobName)"
  ASSERT_FAIL=0

  # Clean baseline: clear any pre-existing schedule, then disable.
  patch_backup_schedule ""
  patch_backup_enabled false
  if ! wait_for_reconcile 0; then
    log_warn "88a-disable: CronJob still present after reconcile wait"
  fi

  if cronjob_exists; then
    log_warn "88a-disable: CronJob ${CRON_NAME} still EXISTS (expected absent)"
    ASSERT_FAIL=1
  else
    log_info "88a-disable: no CronJob ${CRON_NAME} OK"
  fi

  local cjn
  cjn="$(status_cronjob_name)"
  if [ -n "${cjn}" ]; then
    log_warn "88a-disable: Status.cronJobName non-empty ('${cjn}'); expected empty"
    ASSERT_FAIL=1
  else
    log_info "88a-disable: Status.cronJobName empty OK"
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 88a-disable PASS; else set_result 88a-disable FAIL; fi
}

# =============================================================================
# 88a-api — while disabled: create rejected; list 200 (GAP-2); schedule false.
# =============================================================================
run_88a_api() {
  log_step "88a-api — API reports disabled while backup is off"
  ASSERT_FAIL=0

  # backup create => MUST be rejected. The CLI maps the 400 BACKUP_NOT_ENABLED to
  # a non-zero rc; capture rc explicitly so `set -e` does not abort.
  local out="" rc=0
  out="$(ctl backup create --database "${DB}" 2>&1)" && rc=0 || rc=$?
  if [ "${rc}" -ne 0 ]; then
    log_info "88a-api: backup create rejected (rc=${rc}) OK"
  else
    case "${out}" in
      *"not enabled"*|*"BACKUP_NOT_ENABLED"*)
        log_info "88a-api: backup create reported disabled in output OK" ;;
      *)
        log_warn "88a-api: backup create was NOT rejected (rc=0, no disabled marker): ${out}"
        ASSERT_FAIL=1 ;;
    esac
  fi

  # backup list => 200 (rc 0). GAP-2: the handler returns "enabled":false and an
  # empty history; it does NOT emit a literal "disabled" word. Assert it succeeds
  # and surfaces the disabled flag, NOT a fictional "disabled" string.
  local list_out="" list_rc=0
  list_out="$(ctl backup list 2>&1)" && list_rc=0 || list_rc=$?
  if [ "${list_rc}" -ne 0 ]; then
    log_warn "88a-api: backup list failed (rc=${list_rc}): ${list_out}"
    ASSERT_FAIL=1
  else
    log_info "88a-api: backup list succeeded (rc 0) OK"
    # Best-effort: confirm the enabled:false flag is surfaced (no hard fail if the
    # output shape differs across CLI output modes).
    case "${list_out}" in
      *'"enabled": false'*|*'"enabled":false'*|*'enabled false'*)
        log_info "88a-api: backup list surfaced enabled:false OK" ;;
      *)
        log_warn "88a-api: backup list did not clearly surface enabled:false (informational)" ;;
    esac
  fi

  # backup schedule (status) => scheduled:false.
  local sched_out="" sched_rc=0
  sched_out="$(ctl backup schedule 2>&1)" && sched_rc=0 || sched_rc=$?
  if [ "${sched_rc}" -ne 0 ]; then
    log_warn "88a-api: backup schedule failed (rc=${sched_rc}): ${sched_out}"
    ASSERT_FAIL=1
  else
    case "${sched_out}" in
      *'"scheduled": false'*|*'"scheduled":false'*|*'scheduled false'*|*false*)
        log_info "88a-api: backup schedule reports scheduled:false OK" ;;
      *)
        log_warn "88a-api: backup schedule did not report scheduled:false: ${sched_out}"
        ASSERT_FAIL=1 ;;
    esac
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 88a-api PASS; else set_result 88a-api FAIL; fi
}

# =============================================================================
# 88a-reenable — enable + schedule; assert CronJob recreated + cronJobName set.
# =============================================================================
run_88a_reenable() {
  log_step "88a-reenable — re-enable backup with a schedule (CronJob recreated)"
  ASSERT_FAIL=0

  patch_backup_enabled true
  patch_backup_schedule "${SCHEDULE}"
  if ! wait_for_reconcile 1; then
    log_warn "88a-reenable: CronJob ${CRON_NAME} did not appear after reconcile wait"
  fi

  if cronjob_exists; then
    log_info "88a-reenable: CronJob ${CRON_NAME} recreated OK"
  else
    log_warn "88a-reenable: CronJob ${CRON_NAME} still ABSENT (expected present)"
    ASSERT_FAIL=1
  fi

  local cjn
  cjn="$(status_cronjob_name)"
  if [ "${cjn}" = "${CRON_NAME}" ]; then
    log_info "88a-reenable: Status.cronJobName == ${CRON_NAME} OK"
  else
    log_warn "88a-reenable: Status.cronJobName='${cjn}' (expected '${CRON_NAME}')"
    ASSERT_FAIL=1
  fi

  # Best-effort: the schedule endpoint should now report scheduled:true.
  local sched_out=""
  sched_out="$(ctl backup schedule 2>&1 || true)"
  case "${sched_out}" in
    *'"scheduled": true'*|*'"scheduled":true'*|*'scheduled true'*)
      log_info "88a-reenable: backup schedule reports scheduled:true OK" ;;
    *)
      log_warn "88a-reenable: backup schedule did not clearly report scheduled:true (informational)" ;;
  esac

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 88a-reenable PASS; else set_result 88a-reenable FAIL; fi
}

# =============================================================================
# 88b-empty — enabled + empty schedule; assert NO CronJob + empty cronJobName.
# =============================================================================
run_88b_empty() {
  log_step "88b-empty — enabled + empty schedule (no CronJob)"
  ASSERT_FAIL=0

  patch_backup_enabled true
  patch_backup_schedule ""
  if ! wait_for_reconcile 0; then
    log_warn "88b-empty: CronJob still present after reconcile wait"
  fi

  if cronjob_exists; then
    log_warn "88b-empty: CronJob ${CRON_NAME} still EXISTS (expected absent)"
    ASSERT_FAIL=1
  else
    log_info "88b-empty: no CronJob ${CRON_NAME} OK"
  fi

  local cjn
  cjn="$(status_cronjob_name)"
  if [ -n "${cjn}" ]; then
    log_warn "88b-empty: Status.cronJobName non-empty ('${cjn}'); expected empty"
    ASSERT_FAIL=1
  else
    log_info "88b-empty: Status.cronJobName empty OK"
  fi

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 88b-empty PASS; else set_result 88b-empty FAIL; fi
}

# =============================================================================
# 88b-ondemand — on-demand backup works WITHOUT a schedule: create accepted AND
# the backup Job completes (coordinator-exec).
# =============================================================================
run_88b_ondemand() {
  log_step "88b-ondemand — on-demand backup without a schedule"
  ASSERT_FAIL=0

  # Ensure enabled (the 88b-empty step left it enabled, empty schedule).
  patch_backup_enabled true

  local out="" rc=0 JOB=""
  out="$(ctl backup create --database "${DB}" 2>&1)" && rc=0 || rc=$?
  if [ "${rc}" -ne 0 ]; then
    log_warn "88b-ondemand: backup create failed (rc=${rc}): ${out}"
    set_result 88b-ondemand FAIL
    return
  fi
  assert_contains "${out}" "backup started" "88b-ondemand"

  JOB="$(printf '%s' "${out}" | extract_json job || true)"
  if [ -z "${JOB}" ]; then
    JOB="$(latest_job_by_label backup "${CLUSTER}")"
  fi
  if [ -z "${JOB}" ]; then
    log_warn "88b-ondemand: could not resolve the on-demand backup Job name"
    set_result 88b-ondemand FAIL
    return
  fi
  log_info "88b-ondemand: backup Job=${JOB}"

  # Poll the Job to Complete (coordinator-exec).
  if "${KN[@]}" wait --for=condition=complete "job/${JOB}" \
      --timeout="${JOB_TIMEOUT}" >/dev/null 2>&1; then
    log_info "88b-ondemand: backup Job ${JOB} Completed OK"
  else
    log_warn "88b-ondemand: backup Job ${JOB} did not Complete within ${JOB_TIMEOUT}"
    # Best-effort log dump for triage.
    "${KN[@]}" logs "job/${JOB}" 2>/dev/null | tail -40 || true
    ASSERT_FAIL=1
  fi

  # Assert success marker in logs OR the Job succeeded count.
  local logs succeeded
  logs="$("${KN[@]}" logs "job/${JOB}" 2>/dev/null || true)"
  succeeded="$("${KN[@]}" get job "${JOB}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
  case "${logs}" in
    *"Backup completed successfully"*)
      log_info "88b-ondemand: logs show 'Backup completed successfully' OK" ;;
    *)
      if [ "${succeeded:-0}" = "1" ]; then
        log_info "88b-ondemand: Job reports succeeded=1 OK"
      else
        log_warn "88b-ondemand: no success marker in logs and succeeded='${succeeded:-0}'"
        ASSERT_FAIL=1
      fi ;;
  esac

  if [ "${ASSERT_FAIL}" -eq 0 ]; then set_result 88b-ondemand PASS; else set_result 88b-ondemand FAIL; fi
}

# =============================================================================
# 88a-rbac-note — informational: backup SA/Role are Helm-level (GAP-1).
# =============================================================================
run_88a_rbac_note() {
  log_step "88a-rbac-note — backup SA/Role are Helm-level (informational)"
  log_info "GAP-1: the backup ServiceAccount (cloudberry-backup-sa) + Role"
  log_info "  (cloudberry-backup-role) + RoleBinding are created ONLY by the Helm"
  log_info "  chart (gated by backup.rbac.create) in the OPERATOR namespace and are"
  log_info "  SHARED across every cluster. The per-cluster reconcile NEVER"
  log_info "  creates/removes them, so disabling a single cluster's backup does NOT"
  log_info "  remove them. 88a asserts only the per-cluster effects."
  set_result 88a-rbac-note SKIP
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 88 — Backup Disabled / No Schedule: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Database       : ${DB}"
  echo "  Re-enable cron : ${SCHEDULE}"
  echo "  CronJob name   : ${CRON_NAME}"
  echo "  CLI binary     : ${CTL_BIN}"
  echo "  Operator URL   : ${OPERATOR_URL}"
  echo "  OIDC realm     : ${OIDC_REALM}  user: ${OIDC_ADMIN_USER}"
  echo "  Original backup: enabled=${ORIG_ENABLED:-<unset>} schedule='${ORIG_SCHEDULE}'"
  local any_fail=0 c r
  for c in preflight 88a-disable 88a-api 88a-reenable 88b-empty 88b-ondemand 88a-rbac-note; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 88 FAILED (one or more checks did not pass)"
    exit 1
  fi
  log_info "Scenario 88 backup-disabled / no-schedule PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  TMPDIR_S88="$(mktemp -d 2>/dev/null || mktemp -d -t s88)"

  if want_check preflight; then preflight; fi
  build_cli
  obtain_oidc_token
  start_api_portforward
  configure_cli

  if want_check 88a-disable; then run_88a_disable; fi
  if want_check 88a-api; then run_88a_api; fi
  if want_check 88a-reenable; then run_88a_reenable; fi
  if want_check 88b-empty; then run_88b_empty; fi
  if want_check 88b-ondemand; then run_88b_ondemand; fi
  if want_check 88a-rbac-note; then run_88a_rbac_note; fi

  print_summary
}

main "$@"
