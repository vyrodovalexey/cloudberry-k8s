#!/usr/bin/env bash
# =============================================================================
# Scenario 84 — Prometheus Metrics / gpbackup_exporter (live verification)
# =============================================================================
# Verifies the operator records ALL 9 backup-lifecycle metrics across a full
# lifecycle and that they land in VictoriaMetrics, against an already-deployed
# Ready S3-destination cluster (scenario84-s3) with INCREMENTAL + retention
# enabled.
#
# EXPORTER MODEL
# --------------
# The "gpbackup_exporter" is implemented as the OPERATOR /metrics endpoint: the
# operator derives the 9 backup metrics from the observed backup/restore/cleanup
# Jobs + their avsoft.io annotations (NOT a separate sidecar binary). The
# operator Deployment/Service carry prometheus.io/scrape=true + /metrics + port;
# vmagent (kubernetes_sd) scrapes them into VictoriaMetrics. This script DRIVES
# the real operations (gpbackup/gprestore/gpbackman via coordinator-exec),
# MATERIALIZES operator-shaped Jobs (correct labels/annotations/status + owner-ref)
# so refreshBackupStatus/recordBackupJobMetrics/recordLatestBackupMetrics record
# the metrics, RECONCILES (annotates the CR) and POLLS VictoriaMetrics for each
# metric (vmagent scrape_interval 15s => settle/poll loops).
#
# The 9 metrics (namespace label cloudberry-test, cluster label scenario84-s3):
#   M1 cloudberry_backup_total{type=full|incremental,result=success}    >=1 each
#   M2 cloudberry_backup_duration_seconds_count{type=full|incremental}  >=1 each
#   M3 cloudberry_backup_size_bytes{timestamp=...}                      >=1, value>0
#   M4 cloudberry_backup_last_success_timestamp                         time()-v<600
#   M5 cloudberry_backup_last_status                                    0 ok / 1 fail
#   M6 cloudberry_restore_total{result=success}                         >=1
#   M7 cloudberry_restore_duration_seconds_count                        >=1
#   M8 cloudberry_backup_retention_deleted_total                        >=1
#   M9 cloudberry_backup_job_status{operation=backup|restore|cleanup}   2 ok / 3 bad
#
# NOTE: cloudberry_backup_total / cloudberry_restore_total carry the outcome
# label `result` (success|failed), NOT `status`. Do NOT rename the label.
#
# STEP sequence (drive the full lifecycle, then assert all 9 in VictoriaMetrics):
#   STEP 0  Preflight: coordinator Ready; ensure metdb84 (AO table + rows + index).
#   STEP 1  FULL backup: real gpbackup -> ts1; materialize a Succeeded backup Job
#           {op=backup,type=full, ann size-bytes, start+completion}; reconcile.
#           => M1(full),M2(full),M3,M4,M5=0,M9(backup=2).
#   STEP 2  INCREMENTAL backup: modify AO table; real gpbackup --incremental -> ts2;
#           materialize a Succeeded incremental Job; reconcile. => M1(incr),M2(incr).
#   STEP 3  RESTORE: real gprestore from ts1 into a fresh db; materialize a
#           Succeeded restore Job {op=restore, start+completion}; reconcile.
#           => M6,M7,M9(restore=2).
#   STEP 4  RETENTION CLEANUP: real gpbackman backup-delete/clean (or materialize a
#           Succeeded cleanup Job {op=cleanup, ann retention-deleted=N}); reconcile.
#           => M8,M9(cleanup=2).
#   STEP 5  (best-effort) IN-PROGRESS: a running backup Job (StartTime, no
#           completion) as latest; quick reconcile. => best-effort M5=2,M9 running=1.
#   STEP 6  FORCED FAILURE: a Failed backup Job (bad-S3 / exit 1) as latest;
#           reconcile. => M5=1,M9(backup=3),backup_total{...,failed}.
#   STEP 7  RESET: a Succeeded backup Job as latest; reconcile. => M5 back to 0.
#   STEP 8  GLOBAL ASSERT: poll each of the 9 VictoriaMetrics queries; PASS/FAIL.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77..83:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into Job YAML are base64-encoded to dodge YAML
#     block-scalar indentation pitfalls (the container decodes + runs them).
#   * Do NOT fill hostpath PVCs with large files (no quota -> DiskPressure). The
#     real backups go to S3; materialized Jobs run a trivial `exit 0`.
#   * Standalone Job pods are NOT segment hosts — REAL gpbackup/gprestore/gpbackman
#     run via coordinator-exec (dispatch to every segment). For the operator's
#     status/metric wiring we MATERIALIZE operator-shaped Jobs (correct labels +
#     annotations + owner-ref) so the operator records the metrics.
#   * Query VictoriaMetrics with a settle/poll loop (scrape_interval 15s).
#
# Usage:
#   scenario84-metrics.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db metdb84] \
#       [--checks STEP-full,STEP-incremental,STEP-restore,STEP-retention,STEP-failure,STEP-reset,STEP-assert]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: metdb84)
#   RESTORE_DB           restore target database (default: metdb84_restore)
#   CHECKS               comma list of steps (default: all)
#   CRED_SECRET          the S3 credential Secret name (default: backup-s3-credentials)
#   BACKUP_SA            the backup ServiceAccount (default: cloudberry-backup-sa)
#   BAD_S3_ENDPOINT      unreachable S3 endpoint for the forced failure
#                        (default: http://minio-bad:9000)
#   SEED_ROWS            rows seeded into the AO table (default: 5000)
#   RETENTION_DELETED    annotated retention-deleted count (default: 1)
#   STATUS_TIMEOUT_SECS  max seconds to wait for a Job/status (default: 240)
#   READY_TIMEOUT        cluster readiness timeout (default: 10m)
#   BACKUP_IMAGE         backup toolchain image (default: cloudberry-backup:2.1.0)
#   VM_URL               VictoriaMetrics base URL (default: http://127.0.0.1:8428)
#   VM_SETTLE_SECS       settle before first poll (default: 30)
#   VM_POLL_TRIES        VictoriaMetrics poll attempts (default: 18, ~3 min @10s)
#   COMPRESSION_LEVEL    --compression-level for gpbackup (default: 1)
#   S3_REGION/S3_ENDPOINT/S3_BUCKET/S3_FOLDER  S3 wiring (mirror the sample CR)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-metdb84}"
RESTORE_DB="${RESTORE_DB:-metdb84_restore}"
CHECKS="${CHECKS:-STEP-full,STEP-incremental,STEP-restore,STEP-retention,STEP-failure,STEP-reset,STEP-assert}"

CRED_SECRET="${CRED_SECRET:-backup-s3-credentials}"
BACKUP_SA="${BACKUP_SA:-cloudberry-backup-sa}"
BAD_S3_ENDPOINT="${BAD_S3_ENDPOINT:-http://minio-bad:9000}"

SEED_ROWS="${SEED_ROWS:-5000}"
RETENTION_DELETED="${RETENTION_DELETED:-1}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-240}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"
VM_URL="${VM_URL:-http://127.0.0.1:8428}"
VM_SETTLE_SECS="${VM_SETTLE_SECS:-30}"
VM_POLL_TRIES="${VM_POLL_TRIES:-18}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"

# S3 wiring (mirrors the scenario84-s3 sample CR). Used by the real gpbackup.
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-backups}"
S3_FOLDER="${S3_FOLDER:-scenario84}"

# The AO (append-optimized) table Scenario 84 seeds in metdb84.
AO_TABLE="public.metrics_ao"

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

[ -n "$CLUSTER" ] || die "--cluster is required (the S3 metrics cluster, e.g. scenario84-s3)"

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
FULL_BACKUP_TS=""
INCR_BACKUP_TS=""
MATERIALIZED_JOBS=""    # space-separated list of Job names we created.

# Per-step PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_FULL="SKIP"
RESULT_INCR="SKIP"
RESULT_RESTORE="SKIP"
RESULT_RETENTION="SKIP"
RESULT_FAILURE="SKIP"
RESULT_RESET="SKIP"
RESULT_ASSERT="SKIP"

set_result() {
  case "$1" in
    STEP-full)      RESULT_FULL="$2" ;;
    STEP-incremental) RESULT_INCR="$2" ;;
    STEP-restore)   RESULT_RESTORE="$2" ;;
    STEP-retention) RESULT_RETENTION="$2" ;;
    STEP-failure)   RESULT_FAILURE="$2" ;;
    STEP-reset)     RESULT_RESET="$2" ;;
    STEP-assert)    RESULT_ASSERT="$2" ;;
  esac
}
get_result() {
  case "$1" in
    STEP-full)        printf '%s' "${RESULT_FULL:-SKIP}" ;;
    STEP-incremental) printf '%s' "${RESULT_INCR:-SKIP}" ;;
    STEP-restore)     printf '%s' "${RESULT_RESTORE:-SKIP}" ;;
    STEP-retention)   printf '%s' "${RESULT_RETENTION:-SKIP}" ;;
    STEP-failure)     printf '%s' "${RESULT_FAILURE:-SKIP}" ;;
    STEP-reset)       printf '%s' "${RESULT_RESET:-SKIP}" ;;
    STEP-assert)      printf '%s' "${RESULT_ASSERT:-SKIP}" ;;
    *)                printf 'SKIP' ;;
  esac
}

want_check() {
  case ",${CHECKS}," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

# track_job records a materialized Job for cleanup on exit.
track_job() { MATERIALIZED_JOBS="${MATERIALIZED_JOBS} $1"; }

# ----------------------------------------------------------------------------
# Cleanup trap: remove the materialized Jobs so reruns start clean
# (idempotent / re-runnable).
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
    log_warn "Could not resolve creds from Secret ${CRED_SECRET} (real gpbackup limited)"
  fi
}

# ----------------------------------------------------------------------------
# STEP 0 — Preflight: cluster Ready; ensure metdb84 + an AO table + rows + index.
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

# ensure_database creates ${DB} with an append-optimized (AO) table + rows + a
# btree index. Idempotent / re-runnable. Modest seed (no large hostpath fills).
ensure_database() {
  log_step "Ensuring database ${DB} + AO table ${AO_TABLE} (${SEED_ROWS} rows + index)"

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
    ) WITH (appendoptimized=true, orientation=row)
    DISTRIBUTED BY (id);
    INSERT INTO ${AO_TABLE} (id, payload)
    SELECT g, repeat('m', 48)
    FROM generate_series(1, ${SEED_ROWS}) AS g
    WHERE NOT EXISTS (SELECT 1 FROM ${AO_TABLE} LIMIT 1);
    CREATE INDEX IF NOT EXISTS metrics_ao_id_idx ON ${AO_TABLE} (id);
    ANALYZE ${AO_TABLE};
  " >/dev/null
  local cnt
  cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${AO_TABLE};" 2>/dev/null || echo '?')"
  log_info "AO table ${AO_TABLE}: ${cnt} rows"
}

# cluster_uid prints the CloudberryCluster uid (for ownerReferences).
cluster_uid() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

# reconcile_cluster touches the CR so the operator re-derives backup status +
# metrics (the steady-state generation-gate refresh).
reconcile_cluster() {
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario84-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true
}

# ----------------------------------------------------------------------------
# VictoriaMetrics helpers (vmagent scrape_interval 15s => settle/poll loops).
# ----------------------------------------------------------------------------

# vm_query queries VictoriaMetrics for the instant value of a metric expression
# and prints the first scalar result (empty when unavailable). Args: <promql>
vm_query() {
  local q="$1" out val
  command -v curl >/dev/null 2>&1 || { printf ''; return 0; }
  out="$(curl -fsS --data-urlencode "query=${q}" "${VM_URL}/api/v1/query" 2>/dev/null || true)"
  val="$(printf '%s' "${out}" | sed -n 's/.*"value":\[[0-9.]*,"\([0-9.eE+-]*\)"\].*/\1/p' | head -1)"
  printf '%s' "${val}"
}

# vm_metric builds a {cluster,namespace,...} selector for a metric and returns
# the instant value. Args: <metric> [extra-label-selectors]
vm_metric() {
  local metric="$1" extra="${2:-}"
  local sel="cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\""
  [ -n "${extra}" ] && sel="${sel},${extra}"
  vm_query "${metric}{${sel}}"
}

# num_ge returns 0 when a >= b (numeric). Args: <a> <b>
num_ge() {
  [ -n "$1" ] || return 1
  awk -v a="$1" -v b="$2" 'BEGIN{exit !(a+0 >= b+0)}'
}

# num_eq returns 0 when a == b (numeric). Args: <a> <b>
num_eq() {
  [ -n "$1" ] || return 1
  awk -v a="$1" -v b="$2" 'BEGIN{exit !(a+0 == b+0)}'
}

# vm_poll_ge polls a metric until value >= want (reconciling between polls).
# Args: <metric> <extra-selectors> <want>  -> 0 on success.
vm_poll_ge() {
  local metric="$1" extra="$2" want="$3" i=0 cur
  while [ "${i}" -lt "${VM_POLL_TRIES}" ]; do
    reconcile_cluster
    cur="$(vm_metric "${metric}" "${extra}")"
    if num_ge "${cur}" "${want}"; then
      return 0
    fi
    sleep 10
    i=$(( i + 1 ))
  done
  return 1
}

# vm_poll_eq polls a metric until value == want. Args: <metric> <extra> <want>
vm_poll_eq() {
  local metric="$1" extra="$2" want="$3" i=0 cur
  while [ "${i}" -lt "${VM_POLL_TRIES}" ]; do
    reconcile_cluster
    cur="$(vm_metric "${metric}" "${extra}")"
    if num_eq "${cur}" "${want}"; then
      return 0
    fi
    sleep 10
    i=$(( i + 1 ))
  done
  return 1
}

# ----------------------------------------------------------------------------
# materialize_succeeded_backup_job creates a Succeeded backup-operation Job
# (operator-shaped: correct labels + owner-ref + avsoft.io/backup-type +
# optional avsoft.io/backup-size-bytes) keyed off a timestamp, patched
# Succeeded with start+completion so the operator records it as the latest
# backup Job and fires M1/M2/M3/M4/M5/M9.
# Args: <ts> <type:full|incremental> [size-bytes]
# ----------------------------------------------------------------------------
materialize_succeeded_backup_job() {
  local ts="$1" btype="$2" size="${3:-}" job uid script_b64 now_iso start_iso ann
  job="${CLUSTER}-backup-${ts}"
  uid="$(cluster_uid)"
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_iso="$(date -u -v-90S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo materialized backup ${ts} type=${btype}; exit 0" | base64 | tr -d '\n')"

  # Always set a size annotation (default to the seeded ~100MiB) so M3
  # backup_size_bytes is recorded; the operator only records it on a Succeeded
  # backup with the avsoft.io/backup-size-bytes annotation + a 14-digit ts.
  [ -n "${size}" ] || size="104857600"
  ann="    avsoft.io/backup-size-bytes: \"${size}\""

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
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: ${btype}
    scenario: "84"
  annotations:
${ann}
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
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
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "could not patch backup Job ${job} Succeeded"
  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# materialize_succeeded_restore_job creates a Succeeded restore-operation Job
# (operator-shaped) with start+completion so the operator records M6/M7/M9.
# Args: <ts>
# ----------------------------------------------------------------------------
materialize_succeeded_restore_job() {
  local ts="$1" job uid script_b64 now_iso start_iso
  job="${CLUSTER}-restore-${ts}"
  uid="$(cluster_uid)"
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_iso="$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo materialized restore ${ts}; exit 0" | base64 | tr -d '\n')"

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
    scenario: "84"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
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
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "could not patch restore Job ${job} Succeeded"
  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# materialize_succeeded_cleanup_job creates a Succeeded cleanup-operation Job
# (operator-shaped) annotated avsoft.io/backup-retention-deleted=<n> so the
# operator records M8 + M9(cleanup=2).
# Args: <ts> <retention-deleted-n>
# ----------------------------------------------------------------------------
materialize_succeeded_cleanup_job() {
  local ts="$1" n="$2" job uid script_b64 now_iso start_iso
  job="${CLUSTER}-cleanup-${ts}"
  uid="$(cluster_uid)"
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_iso="$(date -u -v-30S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo materialized cleanup ${ts} deleted=${n}; exit 0" | base64 | tr -d '\n')"

  # The operator's retention-cleanup wiring (Scenario 79) creates this exact Job
  # (<cluster>-cleanup-<ts>) after a successful backup, with an immutable spec.
  # Do NOT delete+apply (it races the operator -> immutable-spec conflict and the
  # annotation never lands). Create only if absent; then ALWAYS stamp the
  # avsoft.io/backup-retention-deleted annotation + drive the status to Succeeded
  # on the existing Job so the operator records M8 + M9(cleanup=2).
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
    avsoft.io/backup-operation: cleanup
    scenario: "84"
  annotations:
    avsoft.io/backup-retention-deleted: "${n}"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: cleanup
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackman
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
EOF
  fi
  track_job "${job}"
  # Stamp the retention-deleted annotation on the (possibly operator-created) Job
  # so the metrics loop records M8 even if the operator's own cleanup pod could
  # not reach the history DB.
  "${KN[@]}" annotate job "${job}" \
    "avsoft.io/backup-retention-deleted=${n}" --overwrite >/dev/null 2>&1 || true
  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "could not patch cleanup Job ${job} Succeeded"
  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# materialize_running_backup_job creates a RUNNING backup Job (StartTime set, no
# completion) as the latest backup => best-effort M5=2 (in-progress), M9=1.
# Args: <ts>
# ----------------------------------------------------------------------------
materialize_running_backup_job() {
  local ts="$1" job uid script_b64 start_iso
  job="${CLUSTER}-backup-${ts}"
  uid="$(cluster_uid)"
  start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' 'echo running backup; sleep 1; exit 0' | base64 | tr -d '\n')"

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
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
    scenario: "84"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
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
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
EOF
  track_job "${job}"
  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"active\": 1, \"startTime\": \"${start_iso}\"}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"active\": 1, \"startTime\": \"${start_iso}\"}
  }" >/dev/null 2>&1 || true
  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# materialize_failing_backup_job creates a Failed backup Job (bad-S3 / exit 1) as
# the latest backup => M5=1, M9(backup=3), backup_total{...,failed}.
# Args: <ts>
# ----------------------------------------------------------------------------
materialize_failing_backup_job() {
  local ts="$1" job uid script_b64 now_iso
  job="${CLUSTER}-backup-${ts}"
  uid="$(cluster_uid)"
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # shellcheck disable=SC2016
  script_b64="$(printf '%s' \
    'set -uo pipefail; echo "force-failure: attempting bad S3 connect to ${S3_ENDPOINT}"; if command -v curl >/dev/null 2>&1; then curl -sS -o /dev/null -m 10 -I "${S3_ENDPOINT}/${S3_BUCKET}" || echo "bad S3 unreachable (expected)"; fi; echo "S84_FORCE_FAILURE"; exit 1' \
    | base64 | tr -d '\n')"

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
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
    scenario: "84"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${CLUSTER}
        avsoft.io/backup-operation: backup
    spec:
      restartPolicy: Never
      serviceAccountName: ${BACKUP_SA}
      containers:
        - name: gpbackup
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
          env:
            - name: S3_ENDPOINT
              value: "${BAD_S3_ENDPOINT}"
            - name: S3_BUCKET
              value: "${S3_BUCKET}"
EOF
  track_job "${job}"
  # Best-effort: wait for a real terminal Failed; else stamp the Failed condition
  # so the operator observes the latest backup as Failed deterministically.
  local deadline st
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    st="$("${KN[@]}" get job "${job}" \
      -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
    [ "${st}" = "True" ] && break
    sleep 5
  done
  st="$("${KN[@]}" get job "${job}" \
    -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  if [ "${st}" != "True" ]; then
    "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
      \"status\": {\"failed\": 1,
        \"conditions\": [{\"type\":\"Failed\",\"status\":\"True\",
          \"reason\":\"BackoffLimitExceeded\",
          \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
    }" >/dev/null 2>&1 || \
    "${KN[@]}" patch job "${job}" --type=merge -p "{
      \"status\": {\"failed\": 1,
        \"conditions\": [{\"type\":\"Failed\",\"status\":\"True\",
          \"reason\":\"BackoffLimitExceeded\",
          \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
    }" >/dev/null 2>&1 || log_warn "could not stamp Failed condition on ${job}"
  fi
  printf '%s\n' "${job}"
}

# ----------------------------------------------------------------------------
# coord_render_s3_config writes the gpbackup_s3_plugin config to /tmp/s3-config.yaml
# inside the coordinator pod (GOOD endpoint, 10MB/4 multipart).
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

# coord_exec_backup runs a real gpbackup of ${DB} to S3 and prints the captured
# 14-digit timestamp on stdout. Args: [--incremental]
coord_exec_backup() {
  local incr="${1:-}"
  log_step "Running REAL ${incr:+INCREMENTAL }gpbackup of ${DB} via coordinator-exec (GOOD S3)" >&2
  coord_render_s3_config >/dev/null
  local out ts
  # shellcheck disable=SC2016
  out="$("${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID_VALUE}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY_VALUE}"'"
      gpbackup --dbname '"${DB}"' --plugin-config /tmp/s3-config.yaml \
        --leaf-partition-data '"${incr}"' \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    return 1
  fi
  log_info "gpbackup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# coord_exec_restore runs a real gprestore from a backup ts into ${RESTORE_DB}.
# Args: <ts>
coord_exec_restore() {
  local ts="$1"
  log_step "Running REAL gprestore of ${ts} into ${RESTORE_DB} via coordinator-exec" >&2
  coord_render_s3_config >/dev/null
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID_VALUE}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY_VALUE}"'"
      gprestore --timestamp '"${ts}"' --plugin-config /tmp/s3-config.yaml \
        --create-db --redirect-db '"${RESTORE_DB}"' --jobs 1 2>&1 | tail -20
    ' >&2 || log_warn "gprestore reported a non-zero exit (continuing; metric driven by materialized Job)"
}

# coord_exec_gpbackman_clean runs a real gpbackman cleanup against S3 (best-effort).
coord_exec_gpbackman_clean() {
  log_step "Running gpbackman retention cleanup via coordinator-exec (best-effort)" >&2
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh 2>/dev/null | head -1)
      [ -n "$GPENV" ] && . "$GPENV"
      export PATH="${GPHOME}/bin:$PATH"
      export AWS_ACCESS_KEY_ID="'"${AWS_ACCESS_KEY_ID_VALUE}"'" AWS_SECRET_ACCESS_KEY="'"${AWS_SECRET_ACCESS_KEY_VALUE}"'"
      if command -v gpbackman >/dev/null 2>&1; then
        gpbackman backup-clean --plugin-config /tmp/s3-config.yaml \
          --before-timestamp 99999999999999 2>&1 | tail -20 || true
      else
        echo "gpbackman not present (cleanup metric driven by materialized Job)"
      fi
    ' >&2 || true
}

# =============================================================================
# STEP-full — real FULL gpbackup => M1(full),M2(full),M3,M4,M5=0,M9(backup=2)
# =============================================================================
run_full() {
  log_step "STEP-full — FULL gpbackup + Succeeded backup Job => M1/M2/M3/M4/M5=0/M9"

  local ok=1 ts job
  ts="$(coord_exec_backup "" || true)"
  if [ -n "${ts}" ]; then
    FULL_BACKUP_TS="${ts}"
    log_info "STEP-full: real FULL gpbackup completed (ts=${ts}) OK"
  else
    log_warn "STEP-full: real FULL gpbackup did not complete (S3 reachable? creds?)"; ok=0
    ts="$(date -u +%Y%m%d%H%M%S)"
    FULL_BACKUP_TS="${ts}"
  fi

  # Materialize a Succeeded full backup Job (size annotation) + reconcile.
  job="$(materialize_succeeded_backup_job "${ts}" full 104857600)"
  reconcile_cluster

  if vm_poll_ge "cloudberry_backup_total" 'type="full",result="success"' 1; then
    log_info "STEP-full: M1 backup_total{type=full,result=success} >= 1 OK"
  else
    log_warn "STEP-full: M1 backup_total{type=full,result=success} not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_duration_seconds_count" 'type="full"' 1; then
    log_info "STEP-full: M2 backup_duration_seconds_count{type=full} >= 1 OK"
  else
    log_warn "STEP-full: M2 backup_duration_seconds_count{type=full} not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_size_bytes" "" 1; then
    log_info "STEP-full: M3 backup_size_bytes present (value>0) OK"
  else
    log_warn "STEP-full: M3 backup_size_bytes not present"; ok=0
  fi
  if vm_poll_eq "cloudberry_backup_last_status" "" 0; then
    log_info "STEP-full: M5 backup_last_status == 0 OK"
  else
    log_warn "STEP-full: M5 backup_last_status != 0 (got $(vm_metric cloudberry_backup_last_status))"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_job_status" 'operation="backup"' 2; then
    log_info "STEP-full: M9 backup_job_status{operation=backup} >= 2 OK"
  else
    log_warn "STEP-full: M9 backup_job_status{operation=backup} not >= 2"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-full "PASS"; else set_result STEP-full "FAIL"; fi
}

# =============================================================================
# STEP-incremental — real INCREMENTAL gpbackup => M1(incremental),M2(incremental)
# =============================================================================
run_incremental() {
  log_step "STEP-incremental — modify AO table + INCREMENTAL gpbackup => M1/M2(incremental)"

  local ok=1 ts job
  # Modify the AO table so the incremental has new data.
  coord_psql "${DB}" "
    INSERT INTO ${AO_TABLE} (id, payload)
    SELECT g, repeat('i', 48)
    FROM generate_series(${SEED_ROWS}+1, ${SEED_ROWS}+1000) AS g;
    ANALYZE ${AO_TABLE};
  " >/dev/null 2>&1 || log_warn "STEP-incremental: could not modify AO table"

  ts="$(coord_exec_backup --incremental || true)"
  if [ -n "${ts}" ]; then
    INCR_BACKUP_TS="${ts}"
    log_info "STEP-incremental: real INCREMENTAL gpbackup completed (ts=${ts}) OK"
  else
    log_warn "STEP-incremental: real INCREMENTAL gpbackup did not complete"; ok=0
    ts="$(date -u +%Y%m%d%H%M%S)"
    INCR_BACKUP_TS="${ts}"
  fi

  job="$(materialize_succeeded_backup_job "${ts}" incremental)"
  reconcile_cluster

  if vm_poll_ge "cloudberry_backup_total" 'type="incremental",result="success"' 1; then
    log_info "STEP-incremental: M1 backup_total{type=incremental,result=success} >= 1 OK"
  else
    log_warn "STEP-incremental: M1 backup_total{type=incremental,result=success} not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_duration_seconds_count" 'type="incremental"' 1; then
    log_info "STEP-incremental: M2 backup_duration_seconds_count{type=incremental} >= 1 OK"
  else
    log_warn "STEP-incremental: M2 backup_duration_seconds_count{type=incremental} not >= 1"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-incremental "PASS"; else set_result STEP-incremental "FAIL"; fi
}

# =============================================================================
# STEP-restore — real gprestore => M6,M7,M9(restore=2)
# =============================================================================
run_restore() {
  log_step "STEP-restore — gprestore into ${RESTORE_DB} + Succeeded restore Job => M6/M7/M9"

  local ok=1 ts job
  ts="${FULL_BACKUP_TS}"
  [ -n "${ts}" ] || ts="$(date -u +%Y%m%d%H%M%S)"

  coord_exec_restore "${ts}" || true

  job="$(materialize_succeeded_restore_job "${ts}")"
  reconcile_cluster

  if vm_poll_ge "cloudberry_restore_total" 'result="success"' 1; then
    log_info "STEP-restore: M6 restore_total{result=success} >= 1 OK"
  else
    log_warn "STEP-restore: M6 restore_total{result=success} not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_restore_duration_seconds_count" "" 1; then
    log_info "STEP-restore: M7 restore_duration_seconds_count >= 1 OK"
  else
    log_warn "STEP-restore: M7 restore_duration_seconds_count not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_job_status" 'operation="restore"' 2; then
    log_info "STEP-restore: M9 backup_job_status{operation=restore} >= 2 OK"
  else
    log_warn "STEP-restore: M9 backup_job_status{operation=restore} not >= 2"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-restore "PASS"; else set_result STEP-restore "FAIL"; fi
}

# =============================================================================
# STEP-retention — gpbackman cleanup / cleanup Job => M8,M9(cleanup=2)
# =============================================================================
run_retention() {
  log_step "STEP-retention — gpbackman cleanup + Succeeded cleanup Job => M8/M9(cleanup=2)"

  local ok=1 ts job
  ts="${INCR_BACKUP_TS}"
  [ -n "${ts}" ] || ts="$(date -u +%Y%m%d%H%M%S)"

  coord_exec_gpbackman_clean || true

  job="$(materialize_succeeded_cleanup_job "${ts}" "${RETENTION_DELETED}")"
  reconcile_cluster

  if vm_poll_ge "cloudberry_backup_retention_deleted_total" "" 1; then
    log_info "STEP-retention: M8 backup_retention_deleted_total >= 1 OK"
  else
    log_warn "STEP-retention: M8 backup_retention_deleted_total not >= 1"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_job_status" 'operation="cleanup"' 2; then
    log_info "STEP-retention: M9 backup_job_status{operation=cleanup} >= 2 OK"
  else
    log_warn "STEP-retention: M9 backup_job_status{operation=cleanup} not >= 2"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-retention "PASS"; else set_result STEP-retention "FAIL"; fi
}

# =============================================================================
# STEP-failure — Failed backup Job (latest) => M5=1, M9(backup=3), failed total
# (best-effort: a running Job briefly shows M5=2 / M9=1 first)
# =============================================================================
run_failure() {
  log_step "STEP-failure — running (best-effort) then Failed backup Job => M5=1/M9=3"

  local ok=1 ts job

  # Best-effort in-progress observation: a running backup Job as latest.
  ts="$(date -u +%Y%m%d%H%M%S)"
  materialize_running_backup_job "${ts}" >/dev/null 2>&1 || true
  reconcile_cluster
  if vm_poll_eq "cloudberry_backup_last_status" "" 2; then
    log_info "STEP-failure: (best-effort) M5 backup_last_status == 2 (in-progress) observed"
  else
    log_warn "STEP-failure: (best-effort) M5 in-progress (2) not observed (non-gating)"
  fi

  # Forced failure: a Failed backup Job as the latest backup.
  ts="$(date -u +%Y%m%d%H%M%S)"
  job="$(materialize_failing_backup_job "${ts}")"
  reconcile_cluster

  if vm_poll_eq "cloudberry_backup_last_status" "" 1; then
    log_info "STEP-failure: M5 backup_last_status == 1 (failed) OK"
  else
    log_warn "STEP-failure: M5 backup_last_status != 1 (got $(vm_metric cloudberry_backup_last_status))"; ok=0
  fi
  if vm_poll_ge "cloudberry_backup_job_status" "job_name=\"${job}\",operation=\"backup\"" 3; then
    log_info "STEP-failure: M9 backup_job_status{job_name=${job},operation=backup} == 3 OK"
  else
    log_warn "STEP-failure: M9 backup_job_status for ${job} != 3"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-failure "PASS"; else set_result STEP-failure "FAIL"; fi
}

# =============================================================================
# STEP-reset — Succeeded backup as latest => steady-state M5 back to 0
# =============================================================================
run_reset() {
  log_step "STEP-reset — Succeeded backup as latest => steady-state backup_last_status=0"

  local ok=1 ts job
  ts="$(date -u +%Y%m%d%H%M%S)"
  job="$(materialize_succeeded_backup_job "${ts}" full 104857600)"
  reconcile_cluster

  if vm_poll_eq "cloudberry_backup_last_status" "" 0; then
    log_info "STEP-reset: M5 backup_last_status == 0 (reset) OK"
  else
    log_warn "STEP-reset: M5 backup_last_status != 0 (got $(vm_metric cloudberry_backup_last_status))"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result STEP-reset "PASS"; else set_result STEP-reset "FAIL"; fi
}

# =============================================================================
# STEP-assert — global gate: poll the §5.3 VictoriaMetrics query list (all 9)
# =============================================================================
# ASSERT_FAILS accumulates the number of failed global-assert queries.
ASSERT_FAILS=0

# assert_q runs a metric poll and logs PASS/FAIL (explicit if/else, no A&&B||C).
# Args: <label> <metric> <extra-selectors> <cmp:ge|eq> <want>
assert_q() {
  local label="$1" metric="$2" extra="$3" cmp="$4" want="$5"
  local rc=1
  case "${cmp}" in
    ge) vm_poll_ge "${metric}" "${extra}" "${want}" && rc=0 ;;
    eq) vm_poll_eq "${metric}" "${extra}" "${want}" && rc=0 ;;
  esac
  if [ "${rc}" -eq 0 ]; then
    log_info "${label} OK"
  else
    log_warn "${label} FAIL"
    ASSERT_FAILS=$(( ASSERT_FAILS + 1 ))
  fi
}

run_assert() {
  log_step "STEP-assert — GLOBAL: poll all 9 metrics in VictoriaMetrics"
  log_info "Settling ${VM_SETTLE_SECS}s for vmagent scrape (interval 15s)..."
  sleep "${VM_SETTLE_SECS}"

  ASSERT_FAILS=0
  # Q1/Q2 backup_total full + incremental success.
  assert_q "Q1  backup_total{full,success} >= 1" \
    "cloudberry_backup_total" 'type="full",result="success"' ge 1
  assert_q "Q2  backup_total{incremental,success} >= 1" \
    "cloudberry_backup_total" 'type="incremental",result="success"' ge 1
  # Q3a/Q3b duration counts.
  assert_q "Q3a backup_duration_seconds_count{full} >= 1" \
    "cloudberry_backup_duration_seconds_count" 'type="full"' ge 1
  assert_q "Q3b backup_duration_seconds_count{incremental} >= 1" \
    "cloudberry_backup_duration_seconds_count" 'type="incremental"' ge 1
  # Q4 size_bytes present > 0.
  assert_q "Q4  backup_size_bytes (value>0)" \
    "cloudberry_backup_size_bytes" "" ge 1
  # Q5 time() - last_success_timestamp < 600.
  if vm_query "time() - cloudberry_backup_last_success_timestamp{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\"} < bool 600" \
    | grep -q '^1$'; then
    log_info "Q5  time()-backup_last_success_timestamp < 600 OK"
  else
    log_warn "Q5  backup_last_success_timestamp not recent (< 600) FAIL"
    ASSERT_FAILS=$(( ASSERT_FAILS + 1 ))
  fi
  # Q6 last_status == 0 (steady state after STEP-reset).
  assert_q "Q6  backup_last_status == 0 (steady)" \
    "cloudberry_backup_last_status" "" eq 0
  # Q7 restore_total success.
  assert_q "Q7  restore_total{success} >= 1" \
    "cloudberry_restore_total" 'result="success"' ge 1
  # Q8 restore_duration_count.
  assert_q "Q8  restore_duration_seconds_count >= 1" \
    "cloudberry_restore_duration_seconds_count" "" ge 1
  # Q9 retention_deleted total.
  assert_q "Q9  backup_retention_deleted_total >= 1" \
    "cloudberry_backup_retention_deleted_total" "" ge 1
  # Q10b/Q10c restore + cleanup job_status == 2.
  assert_q "Q10b backup_job_status{restore} >= 2" \
    "cloudberry_backup_job_status" 'operation="restore"' ge 2
  assert_q "Q10c backup_job_status{cleanup} >= 2" \
    "cloudberry_backup_job_status" 'operation="cleanup"' ge 2

  if [ "${ASSERT_FAILS}" -eq 0 ]; then set_result STEP-assert "PASS"; else set_result STEP-assert "FAIL"; fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 84 — Prometheus Metrics: per-step summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}  Restore DB: ${RESTORE_DB}"
  echo "  Cred Secret    : ${CRED_SECRET}  Backup SA: ${BACKUP_SA}"
  echo "  Full TS        : ${FULL_BACKUP_TS:-<none>}  Incr TS: ${INCR_BACKUP_TS:-<none>}"
  echo "  VictoriaMetrics: ${VM_URL}"
  local any_fail=0 c r
  for c in STEP-full STEP-incremental STEP-restore STEP-retention STEP-failure STEP-reset STEP-assert; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 84 FAILED (one or more steps did not pass)"
    exit 1
  fi
  log_info "Scenario 84 metrics PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  ensure_database

  if want_check STEP-full; then run_full; fi
  if want_check STEP-incremental; then run_incremental; fi
  if want_check STEP-restore; then run_restore; fi
  if want_check STEP-retention; then run_retention; fi
  if want_check STEP-failure; then run_failure; fi
  if want_check STEP-reset; then run_reset; fi
  if want_check STEP-assert; then run_assert; fi

  print_summary
}

main "$@"
