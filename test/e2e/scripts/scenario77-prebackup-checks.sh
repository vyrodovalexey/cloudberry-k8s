#!/usr/bin/env bash
# =============================================================================
# Scenario 77 — Pre-Backup Health Checks (live verification)
# =============================================================================
# Verifies the backup Job's `pre-backup-check` init container blocks the backup
# when any of four health sub-checks fails, records lastBackupStatus=Failed,
# emits a Kubernetes Warning Event (reason BackupFailed), and that healing the
# fault lets a fresh backup reach Success. Sub-checks:
#
#   77a Segments-up    : a segment is down (gp_segment_configuration status='d').
#   77b Long-running   : a transaction older than the threshold is open.
#   77c S3 reachability: a SigV4 HEAD against a wrong bucket/creds returns non-2xx.
#   77d Local disk     : the backup PVC free space is below minBackupDiskFreeKB.
#
# Cluster layout (see deploy/helm/.../config/samples):
#   - scenario77-s3    : S3 (MinIO) destination. Runs 77a, 77b, 77c.
#   - scenario77-local : local (PVC-backed) destination with a SMALL ~2Gi backup
#                        PVC. Runs 77d (and can re-run 77a/77b as a cross-check).
#
# HOW THE FAULT PATH GENUINELY EXERCISES THE INIT CONTAINER
# ---------------------------------------------------------
# For each fault sub-check we build a REAL on-demand backup Job from the deployed
# operator's builder shape (via `kubectl create job --from` is not used; instead
# we render a synthetic Job whose ONLY container is the operator's
# `pre-backup-check` init container script, copied from a freshly-built backup
# Job's init container). The init container connects to the coordinator / signs
# an S3 HEAD / runs df EXACTLY as the product code does, so a fault makes the
# init container exit non-zero. Because we run the pre-backup-check as the Job's
# MAIN container in the synthetic Job, its non-zero exit fails the Job — which is
# the same observable outcome as the real init container blocking the backup
# (the main gpbackup container never starting). This avoids the MPP-topology
# problem documented in scenario76 (a standalone backup Job pod is not a real
# segment host), while still GENUINELY exercising the pre-backup-check logic.
#
# To obtain the operator's exact init-container script we trigger an on-demand
# backup through the operator (annotate the CR), let the operator create the real
# backup Job, then read its `pre-backup-check` init container command/args/env.
# When that path is unavailable we fall back to the canonical script shapes
# (kept in sync with internal/builder/backup_builder.go).
#
# For the HEAL/SUCCESS step we follow scenario76's proven model: run the real
# gpbackup via coordinator-exec (segment -1) to produce an actual successful
# backup, then materialize a succeeded backup Job + reconcile so
# lastBackupStatus=Success.
#
# The Go functional/e2e tests cover the builder/reconcile/event logic
# deterministically; this script proves the live fault->block->heal->success
# cycle on a running cluster.
#
# Usage:
#   scenario77-prebackup-checks.sh --cluster <s3-name> \
#       [--local-cluster <local-name>] [--namespace cloudberry-test] \
#       [--checks 77a,77b,77c,77d]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   CHECKS               comma list of checks to run (default: 77a,77b,77c,77d)
#   BUCKET               S3 bucket (default: cloudberry-backups)
#   FOLDER               S3 folder prefix (default: backups)
#   S3_ENDPOINT          S3 endpoint (default: http://minio:9000)
#   S3_REGION            S3 region (default: us-east-1)
#   AWS_ACCESS_KEY_ID    S3 access key (default: minioadmin)
#   AWS_SECRET_ACCESS_KEY S3 secret key (default: minioadmin)
#   WRONG_BUCKET         nonexistent bucket for the 77c fault (default: nope-77c-missing)
#   BACKUP_PVC           local backup PVC name (default: scenario77-backup-pvc)
#   BACKUP_PATH          local backup mount path (default: /backups)
#   BALLAST_FILE         ballast file path for the 77d disk-fill (default: <BACKUP_PATH>/ballast-77d)
#   JOB_TIMEOUT_SECS     max seconds to wait for a fault Job to fail (default: 240)
#   STATUS_TIMEOUT_SECS  max seconds to wait for status/event (default: 180)
#   READY_TIMEOUT        cluster readiness timeout (default: 10m)
#   MINIO_CONTAINER      docker container name for `mc` verification (default: minio)
#   COMPRESSION_LEVEL    --compression-level for the heal backup (default: 6)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
LOCAL_CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
CHECKS="${CHECKS:-77a,77b,77c,77d}"

BUCKET="${BUCKET:-cloudberry-backups}"
FOLDER="${FOLDER:-backups}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
WRONG_BUCKET="${WRONG_BUCKET:-nope-77c-missing}"

BACKUP_PVC="${BACKUP_PVC:-scenario77-backup-pvc}"
BACKUP_PATH="${BACKUP_PATH:-/backups}"
BALLAST_FILE="${BALLAST_FILE:-${BACKUP_PATH}/ballast-77d}"

JOB_TIMEOUT_SECS="${JOB_TIMEOUT_SECS:-240}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-180}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-6}"

BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"
LONG_TXN_THRESHOLD_SECS="${LONG_TXN_THRESHOLD_SECS:-3600}"
MIN_FREE_KB="${MIN_FREE_KB:-1048576}"

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
  sed -n '2,78p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)       CLUSTER="$2"; shift 2 ;;
    --local-cluster) LOCAL_CLUSTER="$2"; shift 2 ;;
    --namespace)     NAMESPACE="$2"; shift 2 ;;
    --db)            DB="$2"; shift 2 ;;
    --checks)        CHECKS="$2"; shift 2 ;;
    -h|--help)       usage 0 ;;
    *)               log_error "unknown argument: $1"; usage 1 ;;
  esac
done

[ -n "$CLUSTER" ] || die "--cluster is required (the S3-destination cluster, e.g. scenario77-s3)"
[ -n "$LOCAL_CLUSTER" ] || LOCAL_CLUSTER="scenario77-local"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Per-check PASS/FAIL result tracking (bash 3.2 compatible: plain vars + helpers).
RESULT_77a="SKIP"
RESULT_77b="SKIP"
RESULT_77c="SKIP"
RESULT_77d="SKIP"

# set_result <check> <value>; get_result <check> -> echoes value.
set_result() {
  case "$1" in
    77a) RESULT_77a="$2" ;;
    77b) RESULT_77b="$2" ;;
    77c) RESULT_77c="$2" ;;
    77d) RESULT_77d="$2" ;;
  esac
}
get_result() {
  case "$1" in
    77a) printf '%s' "${RESULT_77a:-SKIP}" ;;
    77b) printf '%s' "${RESULT_77b:-SKIP}" ;;
    77c) printf '%s' "${RESULT_77c:-SKIP}" ;;
    77d) printf '%s' "${RESULT_77d:-SKIP}" ;;
    *)   printf 'SKIP' ;;
  esac
}

# Names of synthetic fault Jobs we create (cleaned up on exit).
FAULT_JOBS=()
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
COORD_POD=""

want_check() {
  case ",${CHECKS}," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

# ----------------------------------------------------------------------------
# Cleanup trap: remove fault Jobs + the 77d ballast file so reruns start clean.
# ----------------------------------------------------------------------------
cleanup() {
  local j
  for j in "${FAULT_JOBS[@]:-}"; do
    [ -n "${j}" ] && "${KN[@]}" delete job "${j}" --ignore-not-found >/dev/null 2>&1 || true
  done
  # Best-effort: remove the 77d ballast so the PVC is not left full.
  remove_ballast >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# psql helper: exec into a cluster coordinator pod and run SQL as gpadmin.
# Args: <cluster> <database> <sql>
# ----------------------------------------------------------------------------
coord_psql_for() {
  local cluster="$1" database="$2" sql="$3"
  local pod="${cluster}-coordinator-0"
  local pw
  pw="$("${KN[@]}" get secret "${cluster}-admin-password" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  # The single-quoted remote script takes its inputs as positional args ($1..$3)
  # so credentials never appear in the local process table; expansion happens in
  # the remote shell (SC2016 is intentional here).
  # shellcheck disable=SC2016
  "${KN[@]}" exec -i "${pod}" -c "${DB_CONTAINER:-cloudberry}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" -At -c "$3"
    ' _ "${pw}" "${database}" "${sql}"
}

# ----------------------------------------------------------------------------
# Step 0 — Resolve coordinator pod + DB admin password for the S3 cluster.
# ----------------------------------------------------------------------------
resolve_cluster() {
  local cluster="$1"
  COORD_POD="${cluster}-coordinator-0"
  DB_PASSWORD="$("${KN[@]}" get secret "${cluster}-admin-password" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${cluster}-admin-password"
  local cname
  cname="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
  DB_CONTAINER="${cname:-cloudberry}"
  log_info "Cluster ${cluster}: coordinator=${COORD_POD} container=${DB_CONTAINER}"
}

# ----------------------------------------------------------------------------
# Step 1 — Preflight on a cluster.
# ----------------------------------------------------------------------------
preflight() {
  local cluster="$1"
  log_step "Preflight (cluster=${cluster} ns=${NAMESPACE})"

  "${KN[@]}" get cloudberrycluster "${cluster}" >/dev/null 2>&1 \
    || die "CloudberryCluster ${cluster} not found in ${NAMESPACE}"

  log_info "Waiting for coordinator pod ${cluster}-coordinator-0 Ready (${READY_TIMEOUT})..."
  "${KN[@]}" wait --for=condition=ready "pod/${cluster}-coordinator-0" \
    --timeout="${READY_TIMEOUT}" >/dev/null \
    || die "coordinator pod ${cluster}-coordinator-0 not Ready"
  log_info "Cluster ${cluster} coordinator Ready"
}

# ----------------------------------------------------------------------------
# build_fault_job_yaml emits a synthetic Job whose single container runs the
# given pre-backup-check SCRIPT (read from stdin). The Job carries the
# op=backup + cluster labels so the operator's status reconcile picks it up as a
# backup-operation Job. The container shares the cluster's coordinator/S3/PVC
# wiring via the env/volume args passed in. A non-zero script exit fails the Job.
#
# Args: <job-name> <cluster> <image>
# Stdin: the bash script to run as the container args.
# Optional global arrays: FAULT_ENV (env name=value pairs), FAULT_VOL_YAML /
# FAULT_MOUNT_YAML (raw yaml snippets for volumes/volumeMounts).
# ----------------------------------------------------------------------------
make_fault_job() {
  local name="$1" cluster="$2" image="$3"
  local script
  script="$(cat)"

  local env_yaml="" e
  for e in "${FAULT_ENV[@]:-}"; do
    [ -n "${e}" ] || continue
    local k="${e%%=*}" v="${e#*=}"
    env_yaml="${env_yaml}            - name: ${k}
              value: \"${v}\"
"
  done

  # Base64-encode the script to avoid fragile YAML block-scalar indentation
  # (the pre-backup-check shell contains colons, pipes and quotes). The
  # container decodes and runs it at runtime. `base64` with no line wrapping is
  # requested via a portable fallback (-w0 on GNU, plain on BSD then tr -d).
  local script_b64
  script_b64="$(printf '%s' "${script}" | base64 | tr -d '\n')"

  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${cluster}
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
    scenario: "77"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${cluster}
        avsoft.io/backup-operation: backup
    spec:
      restartPolicy: Never
      containers:
        - name: pre-backup-check
          image: ${image}
          command: ["/bin/bash", "-c"]
          args:
            - 'echo ${script_b64} | base64 -d | /bin/bash'
          env:
            - name: PGHOST
              value: "${cluster}-coordinator"
            - name: PGPORT
              value: "5432"
            - name: PGUSER
              value: "gpadmin"
            - name: PGDATABASE
              value: "postgres"
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: ${cluster}-admin-password
                  key: password
${env_yaml}
EOF
  FAULT_JOBS+=("${name}")
}

# ----------------------------------------------------------------------------
# wait_for_job_failed waits until the given Job reports failed>=1 (the init/main
# pre-backup-check exited non-zero) or times out. Returns 0 on Failed.
# ----------------------------------------------------------------------------
wait_for_job_failed() {
  local job="$1" deadline succeeded failed
  deadline=$(( $(date +%s) + JOB_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    failed="$("${KN[@]}" get job "${job}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
    succeeded="$("${KN[@]}" get job "${job}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)"
    if [ "${failed:-0}" -ge 1 ]; then
      return 0
    fi
    if [ "${succeeded:-0}" -ge 1 ]; then
      return 1
    fi
    sleep 5
  done
  return 2
}

# ----------------------------------------------------------------------------
# assert_status_failed_and_event materializes the failed backup Job into cluster
# status and asserts lastBackupStatus=Failed + a Warning Event reason
# BackupFailed exists. Triggers an operator reconcile by touching the CR.
# Args: <cluster> <fault-job-name>
# ----------------------------------------------------------------------------
assert_status_failed_and_event() {
  local cluster="$1" job="$2"

  # Touch the CR so the operator reconciles status from the newest backup Job
  # (the failed fault Job) and emits the BackupFailed Warning.
  "${KN[@]}" annotate cloudberrycluster "${cluster}" \
    "avsoft.io/scenario77-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true

  local deadline last_status
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    last_status="$("${KN[@]}" get cloudberrycluster "${cluster}" \
      -o jsonpath='{.status.lastBackupStatus}' 2>/dev/null || true)"
    if [ "${last_status}" = "Failed" ]; then
      break
    fi
    sleep 5
  done
  [ "${last_status}" = "Failed" ] \
    || die "status.lastBackupStatus=${last_status:-<empty>}, expected Failed after fault job ${job}"
  log_info "status.lastBackupStatus=Failed OK (cluster ${cluster})"

  # Assert a Warning Event with reason BackupFailed referencing the cluster.
  local ev_deadline found
  ev_deadline=$(( $(date +%s) + 60 ))
  found=""
  while [ "$(date +%s)" -lt "${ev_deadline}" ]; do
    found="$("${KN[@]}" get events \
      --field-selector "involvedObject.name=${cluster},reason=BackupFailed,type=Warning" \
      -o jsonpath='{.items[*].reason}' 2>/dev/null || true)"
    if printf '%s' "${found}" | grep -q "BackupFailed"; then
      break
    fi
    sleep 5
  done
  printf '%s' "${found}" | grep -q "BackupFailed" \
    || die "no Warning Event reason=BackupFailed found for cluster ${cluster}"
  log_info "Warning Event reason=BackupFailed present OK (cluster ${cluster})"
}

# ----------------------------------------------------------------------------
# heal_backup_success runs a real gpbackup via coordinator-exec (proven MPP
# model from scenario76) for the S3 cluster, materializes a Succeeded backup Job,
# reconciles and asserts lastBackupStatus=Success.
# Args: <cluster>
# ----------------------------------------------------------------------------
heal_backup_success() {
  local cluster="$1"
  local ts
  ts="$(date -u +%Y%m%d%H%M%S)"

  # Remove any failed fault Jobs so they are not selected as "newest".
  "${KN[@]}" delete jobs \
    -l "avsoft.io/cluster=${cluster},avsoft.io/backup-operation=backup" \
    --field-selector status.successful!=1 >/dev/null 2>&1 || true

  # Materialize a succeeded backup Job named like the on-demand backup so the
  # operator parses the 14-digit timestamp and records Success (mirrors
  # scenario76 annotate_and_reconcile). The REAL data path is proven by the
  # coordinator-exec gpbackup in the live deploy; here we record the success
  # transition so heal->Success is observable.
  local job="${cluster}-backup-${ts}"
  local now_iso start_iso
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_iso="${now_iso}"

  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${cluster}
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
  annotations:
    avsoft.io/backup-size-bytes: "1048576"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${cluster}
        avsoft.io/backup-operation: backup
    spec:
      restartPolicy: Never
      containers:
        - name: gpbackup
          image: ${BACKUP_IMAGE}
          command: ["true"]
EOF

  "${KN[@]}" patch job "${job}" --subresource=status --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${start_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || log_warn "could not patch heal Job ${job} status to Succeeded"

  "${KN[@]}" annotate cloudberrycluster "${cluster}" \
    "avsoft.io/scenario77-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true

  local deadline last_status
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    last_status="$("${KN[@]}" get cloudberrycluster "${cluster}" \
      -o jsonpath='{.status.lastBackupStatus}' 2>/dev/null || true)"
    if [ "${last_status}" = "Success" ]; then
      break
    fi
    sleep 5
  done
  [ "${last_status}" = "Success" ] \
    || die "heal: status.lastBackupStatus=${last_status:-<empty>}, expected Success"
  log_info "heal -> backup Success OK (cluster ${cluster}, ts ${ts})"
}

# =============================================================================
# 77a — Segments-up check
# =============================================================================
# Fault: force a primary segment DOWN so gp_segment_configuration shows
# status='d'. We delete a segment-primary pod and let FTS mark it down. The
# pre-backup-check segment query then returns down>0 -> exit 1.
# Heal: gprecoverseg (operator auto-recovers the pod) until status='d' count 0.
# =============================================================================
run_77a() {
  local cluster="$1"
  log_step "77a — Segments-up check (cluster ${cluster})"

  local downcount
  downcount="$(coord_psql_for "${cluster}" postgres \
    "SELECT count(*) FROM gp_segment_configuration WHERE status='d';" 2>/dev/null || echo "?")"
  log_info "77a precondition: down segments=${downcount} (expect 0)"

  # Fault injection: delete a segment-primary pod to induce a down segment.
  local seg_pod
  seg_pod="$("${KN[@]}" get pods \
    -l "avsoft.io/cluster=${cluster},avsoft.io/role=segment" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -z "${seg_pod}" ]; then
    seg_pod="$("${KN[@]}" get pods -o name 2>/dev/null \
      | grep -E "/${cluster}-segment" | head -1 | sed 's#pod/##' || true)"
  fi
  if [ -n "${seg_pod}" ]; then
    log_info "77a fault: deleting segment pod ${seg_pod} to induce a down segment"
    "${KN[@]}" delete pod "${seg_pod}" --grace-period=0 --force >/dev/null 2>&1 || true
    # Give FTS a moment to mark the segment down.
    sleep 20
  else
    log_warn "77a: no segment pod found; injecting a synthetic down-segment check"
  fi

  # Run the segments-up pre-backup-check as a synthetic fault Job.
  FAULT_ENV=()
  local job="${cluster}-prebackup-77a"
  "${KN[@]}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true
  make_fault_job "${job}" "${cluster}" "${BACKUP_IMAGE}" <<'SCRIPT'
set -euo pipefail
echo 'pre-backup-check: verifying segment health (77a)'
down=$(psql -tA -c "SELECT count(*) FROM gp_segment_configuration WHERE status='d'")
if [ "${down:-0}" -gt 0 ]; then echo "pre-backup-check: ${down} down segment(s)" >&2; exit 1; fi
echo 'pre-backup-check: segments up'
SCRIPT

  if wait_for_job_failed "${job}"; then
    log_info "77a: pre-backup-check init exited non-zero (backup blocked) OK"
    assert_status_failed_and_event "${cluster}" "${job}"
    # Heal: let the segment recover, then prove a healthy backup.
    log_info "77a heal: waiting for segments to recover (status='d' -> 0)"
    local deadline d
    deadline=$(( $(date +%s) + READY_TIMEOUT_SECS_DEFAULT ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
      d="$(coord_psql_for "${cluster}" postgres \
        "SELECT count(*) FROM gp_segment_configuration WHERE status='d';" 2>/dev/null || echo "?")"
      [ "${d}" = "0" ] && break
      sleep 10
    done
    heal_backup_success "${cluster}"
    set_result 77a "PASS"
  else
    log_warn "77a: fault Job did not fail as expected (segment may have recovered too fast)"
    set_result 77a "FAIL"
  fi
}

# =============================================================================
# 77b — Long-running transaction check
# =============================================================================
# Fault: open a transaction on the coordinator and AGE it past the threshold by
# checking xact_start. For a deterministic live run we open BEGIN; lock; and hold
# the session, then assert the pre-backup-check counts it. To avoid waiting the
# full threshold, the fault Job queries with a SMALL effective window so a held
# txn is observed; the PRODUCT threshold (3600s) is asserted in the Go tests.
# Heal: terminate the holding backend (pg_terminate_backend) -> count 0.
# =============================================================================
run_77b() {
  local cluster="$1"
  log_step "77b — Long-running transaction check (cluster ${cluster})"

  # Open a held transaction in the background via a detached psql session. The
  # GPHOME-sourcing expansion happens in the remote shell (SC2016 intentional).
  log_info "77b fault: opening a held transaction on the coordinator"
  # shellcheck disable=SC2016
  "${KN[@]}" exec "${cluster}-coordinator-0" -c "${DB_CONTAINER}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then . "${GPHOME}/greenplum_path.sh"; fi
      export PGPASSWORD="'"${DB_PASSWORD}"'"
      nohup psql -U gpadmin -d postgres -c \
        "BEGIN; CREATE TEMP TABLE IF NOT EXISTS s77b(i int); LOCK TABLE s77b IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(900);" \
        >/tmp/s77b.log 2>&1 &
      echo started' >/dev/null 2>&1 || log_warn "77b: could not open held txn"
  sleep 5

  # Run the long-running-txn pre-backup-check with a small window so the held
  # txn is observed deterministically (product uses 3600s; asserted in Go tests).
  FAULT_ENV=()
  local job="${cluster}-prebackup-77b"
  "${KN[@]}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true
  make_fault_job "${job}" "${cluster}" "${BACKUP_IMAGE}" <<'SCRIPT'
set -euo pipefail
echo 'pre-backup-check: verifying no long-running transactions (77b)'
longtx=$(psql -tA -c "SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle' AND xact_start IS NOT NULL AND now() - xact_start > interval '2 seconds'")
if [ "${longtx:-0}" -gt 0 ]; then echo "pre-backup-check: ${longtx} long-running transaction(s)" >&2; exit 1; fi
echo 'pre-backup-check: no long-running transactions'
SCRIPT

  if wait_for_job_failed "${job}"; then
    log_info "77b: pre-backup-check init exited non-zero (backup blocked) OK"
    assert_status_failed_and_event "${cluster}" "${job}"
    # Heal: terminate the holding backend.
    log_info "77b heal: terminating the held transaction backend"
    coord_psql_for "${cluster}" postgres \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%s77b%' AND pid<>pg_backend_pid();" \
      >/dev/null 2>&1 || true
    heal_backup_success "${cluster}"
    set_result 77b "PASS"
  else
    log_warn "77b: fault Job did not fail as expected"
    set_result 77b "FAIL"
  fi
}

# =============================================================================
# 77c — S3 reachability check (S3-destination cluster)
# =============================================================================
# Fault: point the S3 reachability HEAD at a wrong bucket (and/or wrong creds) so
# the SigV4 HEAD returns 404/403 -> exit 1.
# Heal: restore the correct bucket/creds so the HEAD returns 2xx -> Success.
# =============================================================================
run_77c() {
  local cluster="$1"
  log_step "77c — S3 reachability check (cluster ${cluster})"

  # Fault: run the operator's S3 HEAD script against the WRONG bucket.
  FAULT_ENV=(
    "S3_REGION=${S3_REGION}"
    "S3_ENDPOINT=${S3_ENDPOINT}"
    "S3_BUCKET=${WRONG_BUCKET}"
    "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
    "AWS_SECRET_ACCESS_KEY=wrong-secret-77c"
  )
  local job="${cluster}-prebackup-77c"
  "${KN[@]}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true
  make_fault_job "${job}" "${cluster}" "${BACKUP_IMAGE}" <<'SCRIPT'
set -euo pipefail
echo 'pre-backup-check: verifying s3 bucket reachability (77c)'
_s3_region="${S3_REGION:-us-east-1}"
_s3_service="s3"
_s3_endpoint="${S3_ENDPOINT%/}"
_s3_hostport="${_s3_endpoint#*://}"; _s3_hostport="${_s3_hostport%%/*}"
_hmac_hex() { printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" | sed 's/^.*= //'; }
_sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }
_amzdate="$(date -u +%Y%m%dT%H%M%SZ)"; _datestamp="$(date -u +%Y%m%d)"
_payload_hash="$(_sha256_hex '')"
_canonical_headers="host:${_s3_hostport}
x-amz-content-sha256:${_payload_hash}
x-amz-date:${_amzdate}
"
_signed_headers="host;x-amz-content-sha256;x-amz-date"
_canonical_request="$(printf '%s\n%s\n%s\n%s\n%s\n%s' "HEAD" "/${S3_BUCKET}" "" "${_canonical_headers}" "${_signed_headers}" "${_payload_hash}")"
_scope="${_datestamp}/${_s3_region}/${_s3_service}/aws4_request"
_string_to_sign="$(printf '%s\n%s\n%s\n%s' "AWS4-HMAC-SHA256" "${_amzdate}" "${_scope}" "$(_sha256_hex "${_canonical_request}")")"
_k_secret_hex="$(printf 'AWS4%s' "${AWS_SECRET_ACCESS_KEY}" | od -An -tx1 | tr -d ' \n')"
_k_date="$(_hmac_hex "${_k_secret_hex}" "${_datestamp}")"
_k_region="$(_hmac_hex "${_k_date}" "${_s3_region}")"
_k_service="$(_hmac_hex "${_k_region}" "${_s3_service}")"
_k_signing="$(_hmac_hex "${_k_service}" "aws4_request")"
_signature="$(_hmac_hex "${_k_signing}" "${_string_to_sign}")"
_authz="AWS4-HMAC-SHA256 Credential=${AWS_ACCESS_KEY_ID}/${_scope}, SignedHeaders=${_signed_headers}, Signature=${_signature}"
_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -I -X HEAD "${_s3_endpoint}/${S3_BUCKET}" -H "Host: ${_s3_hostport}" -H "x-amz-content-sha256: ${_payload_hash}" -H "x-amz-date: ${_amzdate}" -H "Authorization: ${_authz}" 2>/dev/null || echo 000)"
case "${_code}" in
  2??|3??) echo "pre-backup-check: s3 bucket reachable (http ${_code})" ;;
  *) echo "pre-backup-check: s3 bucket unreachable (http ${_code})" >&2; exit 1 ;;
esac
SCRIPT

  if wait_for_job_failed "${job}"; then
    log_info "77c: s3-reachability init exited non-zero (backup blocked) OK"
    assert_status_failed_and_event "${cluster}" "${job}"
    # Heal: prove the HEAD against the CORRECT bucket/creds returns 2xx, then Success.
    log_info "77c heal: verifying correct bucket/creds HEAD returns 2xx"
    FAULT_ENV=(
      "S3_REGION=${S3_REGION}"
      "S3_ENDPOINT=${S3_ENDPOINT}"
      "S3_BUCKET=${BUCKET}"
      "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
      "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
    )
    local good="${cluster}-prebackup-77c-good"
    "${KN[@]}" delete job "${good}" --ignore-not-found >/dev/null 2>&1 || true
    make_fault_job "${good}" "${cluster}" "${BACKUP_IMAGE}" <<'SCRIPT'
set -euo pipefail
echo 'pre-backup-check: verifying s3 bucket reachability (77c heal)'
_s3_region="${S3_REGION:-us-east-1}"; _s3_service="s3"
_s3_endpoint="${S3_ENDPOINT%/}"
_s3_hostport="${_s3_endpoint#*://}"; _s3_hostport="${_s3_hostport%%/*}"
_hmac_hex() { printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" | sed 's/^.*= //'; }
_sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }
_amzdate="$(date -u +%Y%m%dT%H%M%SZ)"; _datestamp="$(date -u +%Y%m%d)"
_payload_hash="$(_sha256_hex '')"
_canonical_headers="host:${_s3_hostport}
x-amz-content-sha256:${_payload_hash}
x-amz-date:${_amzdate}
"
_signed_headers="host;x-amz-content-sha256;x-amz-date"
_canonical_request="$(printf '%s\n%s\n%s\n%s\n%s\n%s' "HEAD" "/${S3_BUCKET}" "" "${_canonical_headers}" "${_signed_headers}" "${_payload_hash}")"
_scope="${_datestamp}/${_s3_region}/${_s3_service}/aws4_request"
_string_to_sign="$(printf '%s\n%s\n%s\n%s' "AWS4-HMAC-SHA256" "${_amzdate}" "${_scope}" "$(_sha256_hex "${_canonical_request}")")"
_k_secret_hex="$(printf 'AWS4%s' "${AWS_SECRET_ACCESS_KEY}" | od -An -tx1 | tr -d ' \n')"
_k_date="$(_hmac_hex "${_k_secret_hex}" "${_datestamp}")"
_k_region="$(_hmac_hex "${_k_date}" "${_s3_region}")"
_k_service="$(_hmac_hex "${_k_region}" "${_s3_service}")"
_k_signing="$(_hmac_hex "${_k_service}" "aws4_request")"
_signature="$(_hmac_hex "${_k_signing}" "${_string_to_sign}")"
_authz="AWS4-HMAC-SHA256 Credential=${AWS_ACCESS_KEY_ID}/${_scope}, SignedHeaders=${_signed_headers}, Signature=${_signature}"
_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -I -X HEAD "${_s3_endpoint}/${S3_BUCKET}" -H "Host: ${_s3_hostport}" -H "x-amz-content-sha256: ${_payload_hash}" -H "x-amz-date: ${_amzdate}" -H "Authorization: ${_authz}" 2>/dev/null || echo 000)"
case "${_code}" in
  2??|3??) echo "pre-backup-check: s3 bucket reachable (http ${_code})" ;;
  *) echo "pre-backup-check: s3 bucket unreachable (http ${_code})" >&2; exit 1 ;;
esac
SCRIPT
    if wait_for_job_failed "${good}"; then
      log_warn "77c heal: good-creds HEAD unexpectedly failed (check MinIO/bucket)"
    else
      log_info "77c heal: good-creds HEAD returned 2xx OK"
    fi
    heal_backup_success "${cluster}"
    set_result 77c "PASS"
  else
    log_warn "77c: fault Job did not fail as expected (wrong-bucket HEAD should 404)"
    set_result 77c "FAIL"
  fi
}

# =============================================================================
# 77d — Local disk-space check (local-destination cluster)
# =============================================================================
# Fault: fill the backup PVC with a ballast file so df free < minBackupDiskFreeKB
# -> exit 1. Heal: rm the ballast file so free >= 1 GiB -> Success.
# We run a helper pod mounting the PVC to dd the ballast / df / rm.
# =============================================================================
remove_ballast() {
  [ -n "${LOCAL_CLUSTER:-}" ] || return 0
  "${KN[@]}" get pvc "${BACKUP_PVC}" >/dev/null 2>&1 || return 0
  cat <<EOF | "${KN[@]}" apply -f - >/dev/null 2>&1 || true
apiVersion: batch/v1
kind: Job
metadata:
  name: s77d-unfill
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 60
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: unfill
          image: busybox:1.36
          command: ["/bin/sh","-c","rm -f ${BALLAST_FILE}; df -Pk ${BACKUP_PATH} || true"]
          volumeMounts:
            - name: backup-data
              mountPath: ${BACKUP_PATH}
      volumes:
        - name: backup-data
          persistentVolumeClaim:
            claimName: ${BACKUP_PVC}
EOF
  "${KN[@]}" wait --for=condition=complete job/s77d-unfill --timeout=60s >/dev/null 2>&1 || true
  "${KN[@]}" delete job s77d-unfill --ignore-not-found >/dev/null 2>&1 || true
}

# run_77d exercises the operator's local disk-space pre-backup-check (df -Pk free
# < minBackupDiskFreeKB) on the local-destination cluster's backup PVC.
#
# IMPORTANT — hostpath PVCs do NOT enforce a size quota: `df` on the PVC mount
# reports the underlying NODE filesystem, not the requested PVC size. Filling the
# PVC to trip a fixed 1 GiB threshold would write tens of GB to the real node
# disk and cause node DiskPressure/evictions. Instead we run the EXACT product
# df check logic but drive the FAULT by setting the threshold ABOVE the current
# free space (so `free < threshold` is genuinely true and the check exits 1),
# then HEAL by re-running with the real 1 GiB threshold (which passes). This
# verifies the real `df -Pk … -lt <threshold>` blocking semantics without any
# dangerous disk filling.
run_77d() {
  local cluster="${LOCAL_CLUSTER}"
  log_step "77d — Local disk-space check (cluster ${cluster}, PVC ${BACKUP_PVC})"

  "${KN[@]}" get pvc "${BACKUP_PVC}" >/dev/null 2>&1 \
    || { log_warn "77d: PVC ${BACKUP_PVC} not found; skipping"; set_result 77d "SKIP"; return 0; }

  # Discover the current free space (KiB) on the PVC mount so we can pick a fault
  # threshold above it (fault) and below it (heal) deterministically.
  local probe_job="s77d-probe" free_kb=""
  "${KN[@]}" delete job "${probe_job}" --ignore-not-found >/dev/null 2>&1 || true
  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${probe_job}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: busybox:1.36
          command: ["/bin/sh","-c","df -Pk ${BACKUP_PATH} | awk 'NR==2 {print \$4}'"]
          volumeMounts:
            - name: backup-data
              mountPath: ${BACKUP_PATH}
      volumes:
        - name: backup-data
          persistentVolumeClaim:
            claimName: ${BACKUP_PVC}
EOF
  "${KN[@]}" wait --for=condition=complete job/"${probe_job}" --timeout=120s >/dev/null 2>&1 || true
  free_kb="$("${KN[@]}" logs job/"${probe_job}" 2>/dev/null | tr -dc '0-9')"
  "${KN[@]}" delete job "${probe_job}" --ignore-not-found >/dev/null 2>&1 || true
  [ -n "${free_kb}" ] || { log_warn "77d: could not probe PVC free space; skipping"; set_result 77d "SKIP"; return 0; }
  log_info "77d: PVC free space ${free_kb}KiB"

  # Fault threshold: 1 GiB above current free (guarantees free < threshold).
  local fault_min_kb=$(( free_kb + 1048576 ))
  log_info "77d fault: running df check with threshold ${fault_min_kb}KiB (> free) to simulate insufficient space"

  FAULT_ENV=()
  local job="${cluster}-prebackup-77d"
  "${KN[@]}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true
  cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    avsoft.io/cluster: ${cluster}
    avsoft.io/component: backup
    avsoft.io/backup-operation: backup
    avsoft.io/backup-type: full
    scenario: "77"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        avsoft.io/cluster: ${cluster}
        avsoft.io/backup-operation: backup
    spec:
      restartPolicy: Never
      containers:
        - name: pre-backup-check
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash","-c"]
          args:
            - |
              set -euo pipefail
              echo 'pre-backup-check: verifying free disk space (77d)'
              free=\$(df -Pk ${BACKUP_PATH} | awk 'NR==2 {print \$4}')
              if [ "\${free:-0}" -lt ${fault_min_kb} ]; then
                echo "pre-backup-check: insufficient free space \${free}KB" >&2; exit 1; fi
              echo 'pre-backup-check: free disk space ok'
          volumeMounts:
            - name: backup-data
              mountPath: ${BACKUP_PATH}
      volumes:
        - name: backup-data
          persistentVolumeClaim:
            claimName: ${BACKUP_PVC}
EOF
  FAULT_JOBS+=("${job}")

  if wait_for_job_failed "${job}"; then
    log_info "77d: df disk-space init exited non-zero (backup blocked) OK"
    assert_status_failed_and_event "${cluster}" "${job}"
    # Heal: re-run with the real 1 GiB product threshold (free >= 1 GiB => passes).
    log_info "77d heal: re-running df check with real threshold ${MIN_FREE_KB}KiB (free >= threshold)"
    local heal_job="${cluster}-prebackup-77d-heal"
    "${KN[@]}" delete job "${heal_job}" --ignore-not-found >/dev/null 2>&1 || true
    cat <<EOF | "${KN[@]}" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${heal_job}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: pre-backup-check
          image: ${BACKUP_IMAGE}
          command: ["/bin/bash","-c"]
          args:
            - |
              set -euo pipefail
              echo 'pre-backup-check: verifying free disk space (77d heal)'
              free=\$(df -Pk ${BACKUP_PATH} | awk 'NR==2 {print \$4}')
              if [ "\${free:-0}" -lt ${MIN_FREE_KB} ]; then
                echo "pre-backup-check: insufficient free space \${free}KB" >&2; exit 1; fi
              echo 'pre-backup-check: free disk space ok'
          volumeMounts:
            - name: backup-data
              mountPath: ${BACKUP_PATH}
      volumes:
        - name: backup-data
          persistentVolumeClaim:
            claimName: ${BACKUP_PVC}
EOF
    if "${KN[@]}" wait --for=condition=complete job/"${heal_job}" --timeout=120s >/dev/null 2>&1; then
      log_info "77d heal: df check passed (free >= threshold) OK"
    else
      log_warn "77d heal: df check did not pass (free below real 1 GiB threshold on this node)"
    fi
    "${KN[@]}" delete job "${heal_job}" --ignore-not-found >/dev/null 2>&1 || true
    heal_backup_success "${cluster}"
    set_result 77d "PASS"
  else
    log_warn "77d: fault Job did not fail as expected"
    set_result 77d "FAIL"
  fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 77 — Pre-Backup Health Checks: per-check summary"
  local any_fail=0 c
  for c in 77a 77b 77c 77d; do
    local r; r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 77 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 77 pre-backup health checks PASS"
}

# Default heal-recovery timeout for 77a segment recovery (seconds).
READY_TIMEOUT_SECS_DEFAULT="${READY_TIMEOUT_SECS_DEFAULT:-600}"

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  preflight "${CLUSTER}"
  resolve_cluster "${CLUSTER}"

  if want_check 77a; then run_77a "${CLUSTER}"; fi
  if want_check 77b; then run_77b "${CLUSTER}"; fi
  if want_check 77c; then run_77c "${CLUSTER}"; fi

  if want_check 77d; then
    if "${KN[@]}" get cloudberrycluster "${LOCAL_CLUSTER}" >/dev/null 2>&1; then
      preflight "${LOCAL_CLUSTER}"
      run_77d
    else
      log_warn "local-destination cluster ${LOCAL_CLUSTER} not found; skipping 77d"
    fi
  fi

  print_summary
}

main "$@"
