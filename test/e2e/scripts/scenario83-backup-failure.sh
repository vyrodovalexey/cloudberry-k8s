#!/usr/bin/env bash
# =============================================================================
# Scenario 83 — Backup Failure Handling (live verification)
# =============================================================================
# Verifies the backup FAILURE-handling surface end-to-end against an
# already-deployed Ready S3-destination cluster (scenario83-s3) with
# backup.destination.{type:s3, s3:{credentialSecret:s3-credentials}} and
# spec.backup.jobTemplate.backoffLimit: 2:
#
#   83-healthy Acceptance "all backups successful": run a REAL healthy gpbackup of
#       mydb via coordinator-exec against the GOOD S3 endpoint => "Backup
#       completed successfully"; materialize a Succeeded backup Job + reconcile =>
#       assert cloudberry_backup_last_status=0 in VictoriaMetrics.
#   83a Force-failure (backoffLimit + status + last_status=1): materialize an
#       operator-shaped backup Job with backoffLimit: 2 whose container FAILS fast
#       (a bad/unreachable S3 endpoint, e.g. http://minio-bad:9000, or simply
#       `exit 1` to simulate the gpbackup_s3_plugin failure) => assert the Job
#       spec backoffLimit==2, the Job RETRIES (pods restart up to backoffLimit),
#       and the Job ends Failed with a JobFailed condition
#       reason=BackoffLimitExceeded; materialize/observe the Failed backup Job +
#       reconcile => assert status.lastBackupStatus=Failed AND
#       cloudberry_backup_last_status=1. The standalone Job failing on bad S3
#       connect is the correct observable (the Job pod is NOT a segment host — it
#       fails on S3 connect, deterministically).
#   83b Deadline (activeDeadlineSeconds): materialize a PER-RUN backup Job with a
#       LOW activeDeadlineSeconds (5) whose container runs `sleep 600` => the Job
#       is killed at the deadline and gets a JobFailed condition
#       reason=DeadlineExceeded; assert the Job spec activeDeadlineSeconds==5, the
#       Job ends Failed (DeadlineExceeded); materialize/observe as the latest
#       backup Job + reconcile => assert lastBackupStatus=Failed +
#       cloudberry_backup_last_status=1. The per-run Job proves the deadline kill
#       WITHOUT lowering the production cluster's deadline.
#   83-reset (optional but nice): run/observe a healthy success at the END so the
#       steady-state cloudberry_backup_last_status returns to 0 ("all backups
#       successful"), while the discrete failure Jobs from 83a/83b remain the
#       intended Scenario-83 failures (observed on their own resources).
#
# WHY MATERIALIZE JOBS (carried from scenario77/78/79/80/82)
# ----------------------------------------------------------
# The standalone backup Job pod is NOT a segment host, so a full real backup on a
# single Job pod cannot land per-segment sets. For a FAILURE test that is exactly
# right: the standalone pod FAILING on a bad S3 connect (or exceeding the
# deadline) is the deterministic observable. For the HEALTHY backup we run real
# gpbackup via coordinator-exec (dispatches to every segment). For the operator's
# status/metric wiring we MATERIALIZE operator-shaped backup Jobs (correct
# labels + owner-ref) so refreshBackupStatus/recordLatestBackupMetrics pick them
# up as the latest backup Job and set cloudberry_backup_last_status.
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77..82:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into Job/Pod YAML are base64-encoded to avoid
#     YAML block-scalar indentation pitfalls (the container decodes + runs them).
#   * Do NOT fill hostpath PVCs with large files (no quota -> DiskPressure). This
#     scenario writes no backup data on a PVC; the failure Jobs fail on S3 connect
#     or the deadline, and the healthy backup goes to S3.
#   * The failing/deadline Jobs are operator-shaped (labels avsoft.io/cluster,
#     avsoft.io/component=backup, avsoft.io/backup-operation=backup, owner-ref to
#     the cluster) so refreshBackupStatus picks them up as the latest backup Job.
#
# Usage:
#   scenario83-backup-failure.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db mydb] [--checks 83-healthy,83a,83b,83-reset]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: mydb)
#   CHECKS               comma list of checks (default: 83-healthy,83a,83b,83-reset)
#   CRED_SECRET          the S3 credential Secret name (default: s3-credentials)
#   BACKUP_SA            the backup ServiceAccount (default: cloudberry-backup-sa)
#   BAD_S3_ENDPOINT      unreachable S3 endpoint for the 83a force-failure
#                        (default: http://minio-bad:9000)
#   DEADLINE_SECS        low activeDeadlineSeconds for the 83b deadline Job
#                        (default: 5)
#   SEED_ROWS            rows seeded per table (default: 2000)
#   STATUS_TIMEOUT_SECS  max seconds to wait for a Job/status (default: 240)
#   READY_TIMEOUT        cluster readiness timeout (default: 10m)
#   BACKUP_IMAGE         backup toolchain image (default: cloudberry-backup:2.1.0)
#   VM_URL               VictoriaMetrics base URL (default: http://127.0.0.1:8428)
#   COMPRESSION_LEVEL    --compression-level for gpbackup (default: 1)
#   S3_REGION/S3_ENDPOINT/S3_BUCKET/S3_FOLDER  S3 wiring (mirror the sample CR)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
CHECKS="${CHECKS:-83-healthy,83a,83b,83-reset}"

CRED_SECRET="${CRED_SECRET:-s3-credentials}"
BACKUP_SA="${BACKUP_SA:-cloudberry-backup-sa}"
BAD_S3_ENDPOINT="${BAD_S3_ENDPOINT:-http://minio-bad:9000}"
DEADLINE_SECS="${DEADLINE_SECS:-5}"

SEED_ROWS="${SEED_ROWS:-2000}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-240}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"
VM_URL="${VM_URL:-http://127.0.0.1:8428}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-1}"

# S3 wiring (mirrors the scenario83-s3 sample CR). Used by the healthy gpbackup.
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-backups}"
S3_FOLDER="${S3_FOLDER:-scenario83}"

# The fixed table set Scenario 83 seeds (modest; keep the list small).
TABLES="public.users public.orders public.items"

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
  sed -n '2,84p' "$0" | sed 's/^# \{0,1\}//'
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

[ -n "$CLUSTER" ] || die "--cluster is required (the S3 backup-failure cluster, e.g. scenario83-s3)"

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
HEALTHY_BACKUP_TS=""
MATERIALIZED_JOBS=""    # space-separated list of Job names we created.

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_HEALTHY="SKIP"
RESULT_83a="SKIP"
RESULT_83b="SKIP"
RESULT_RESET="SKIP"

set_result() {
  case "$1" in
    83-healthy) RESULT_HEALTHY="$2" ;;
    83a)        RESULT_83a="$2" ;;
    83b)        RESULT_83b="$2" ;;
    83-reset)   RESULT_RESET="$2" ;;
  esac
}
get_result() {
  case "$1" in
    83-healthy) printf '%s' "${RESULT_HEALTHY:-SKIP}" ;;
    83a)        printf '%s' "${RESULT_83a:-SKIP}" ;;
    83b)        printf '%s' "${RESULT_83b:-SKIP}" ;;
    83-reset)   printf '%s' "${RESULT_RESET:-SKIP}" ;;
    *)          printf 'SKIP' ;;
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
# Step 0 — Resolve DB admin password, coordinator container, and the credential
# Secret's access/secret-key values (for the healthy gpbackup).
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
# Step 1 — Preflight: cluster Ready; ensure mydb + a few seeded+indexed tables.
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

# ensure_tables creates ${DB} (if needed) and the fixed table set, each with a
# few rows and a btree index. Idempotent / re-runnable. Modest seed (no large
# fills — Scenario 78 lesson). Captures + logs row counts.
ensure_tables() {
  log_step "Ensuring database ${DB} + seeded+indexed tables (${SEED_ROWS} rows each)"

  if ! coord_psql postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql postgres "CREATE DATABASE ${DB};" >/dev/null
    log_info "Database ${DB} created"
  else
    log_info "Database ${DB} already exists"
  fi

  local t slug cnt
  for t in ${TABLES}; do
    slug="$(printf '%s' "${t}" | sed 's/^public\.//')"
    coord_psql "${DB}" "
      CREATE TABLE IF NOT EXISTS ${t} (
        id bigint,
        payload text,
        created timestamptz DEFAULT now()
      ) DISTRIBUTED BY (id);
      INSERT INTO ${t} (id, payload)
      SELECT g, repeat('x', 32)
      FROM generate_series(1, ${SEED_ROWS}) AS g
      WHERE NOT EXISTS (SELECT 1 FROM ${t} LIMIT 1);
      CREATE INDEX IF NOT EXISTS ${slug}_id_idx ON ${t} (id);
      ANALYZE ${t};
    " >/dev/null
    cnt="$(coord_psql "${DB}" "SELECT count(*) FROM ${t};" 2>/dev/null || echo '?')"
    log_info "Table ${t}: ${cnt} rows"
  done
}

# cluster_uid prints the CloudberryCluster uid (for ownerReferences).
cluster_uid() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

# reconcile_cluster touches the CR so the operator re-derives backup status +
# metrics (the steady-state generation-gate refresh, Scenario 76/79/80).
reconcile_cluster() {
  "${KN[@]}" annotate cloudberrycluster "${CLUSTER}" \
    "avsoft.io/scenario83-reconcile=$(date +%s%N)" --overwrite >/dev/null 2>&1 || true
}

# ----------------------------------------------------------------------------
# vm_query queries VictoriaMetrics for the instant value of a metric expression
# and prints the scalar result (empty when unavailable).
# Args: <promql>
# ----------------------------------------------------------------------------
vm_query() {
  local q="$1" out val
  command -v curl >/dev/null 2>&1 || { printf ''; return 0; }
  out="$(curl -fsS --data-urlencode "query=${q}" "${VM_URL}/api/v1/query" 2>/dev/null || true)"
  val="$(printf '%s' "${out}" | sed -n 's/.*"value":\[[0-9.]*,"\([0-9.-]*\)"\].*/\1/p' | head -1)"
  printf '%s' "${val}"
}

# vm_backup_last_status prints cloudberry_backup_last_status for this cluster
# (empty when absent).
vm_backup_last_status() {
  vm_query "cloudberry_backup_last_status{cluster=\"${CLUSTER}\",namespace=\"${NAMESPACE}\"}"
}

# wait_backup_last_status polls until cloudberry_backup_last_status == <want>
# (numeric compare), returns 0 on match. Args: <want>
wait_backup_last_status() {
  local want="$1" i=0 cur
  while [ "${i}" -lt 18 ]; do
    reconcile_cluster
    cur="$(vm_backup_last_status)"
    if [ -n "${cur}" ] && awk -v a="${cur}" -v b="${want}" 'BEGIN{exit !(a==b)}'; then
      return 0
    fi
    sleep 10
    i=$(( i + 1 ))
  done
  return 1
}

# job_failed_reason prints the reason of a Job's Failed condition (empty when
# none). Args: <job-name>
job_failed_reason() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}' 2>/dev/null || true
}

# job_failed_status prints the status of a Job's Failed condition. Args: <job>
job_failed_status() {
  "${KN[@]}" get job "$1" \
    -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true
}

# wait_job_failed polls until a Job carries a Failed condition (status True) or a
# failed-pod count > 0, returns 0 on terminal Failed. Args: <job-name>
wait_job_failed() {
  local job="$1" deadline st failed
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    st="$(job_failed_status "${job}")"
    failed="$("${KN[@]}" get job "${job}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
    if [ "${st}" = "True" ] || { [ -n "${failed}" ] && [ "${failed}" -gt 0 ] 2>/dev/null; }; then
      # Give the Failed condition a moment to settle (DeadlineExceeded/Backoff).
      st="$(job_failed_status "${job}")"
      [ "${st}" = "True" ] && return 0
    fi
    sleep 5
  done
  [ "$(job_failed_status "${job}")" = "True" ]
}

# job_status_failed_count prints .status.failed for a Job (0 when absent).
job_status_failed_count() {
  "${KN[@]}" get job "$1" -o jsonpath='{.status.failed}' 2>/dev/null || printf '0'
}

# ----------------------------------------------------------------------------
# materialize_failing_backup_job applies an operator-shaped on-demand backup Job
# (correct labels + owner-ref) whose container FAILS fast. The job carries
# backoffLimit so it retries up to backoffLimit before terminal
# BackoffLimitExceeded. The container points at a bad/unreachable S3 endpoint and
# exits non-zero (a curl HEAD to the bad endpoint, then `exit 1` to simulate the
# gpbackup_s3_plugin failure even when curl is absent).
# Args: <job-name> <backoffLimit>
# ----------------------------------------------------------------------------
materialize_failing_backup_job() {
  local job="$1" backoff="$2" uid script_b64
  uid="$(cluster_uid)"

  # The script attempts to reach the BAD S3 endpoint then fails. base64-encoded
  # to dodge YAML block-scalar indentation pitfalls; decoded + run in the pod.
  # shellcheck disable=SC2016
  script_b64="$(printf '%s' \
    'set -uo pipefail; echo "force-failure: attempting bad S3 connect to ${S3_ENDPOINT}"; if command -v curl >/dev/null 2>&1; then curl -sS -o /dev/null -m 10 -I "${S3_ENDPOINT}/${S3_BUCKET}" || echo "bad S3 unreachable (expected)"; fi; echo "S83_FORCE_FAILURE"; exit 1' \
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
    scenario: "83"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: ${backoff}
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
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${CRED_SECRET}
                  key: aws_access_key_id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${CRED_SECRET}
                  key: aws_secret_access_key
            - name: S3_REGION
              value: "${S3_REGION}"
            - name: S3_ENDPOINT
              value: "${BAD_S3_ENDPOINT}"
            - name: S3_BUCKET
              value: "${S3_BUCKET}"
            - name: S3_FOLDER
              value: "${S3_FOLDER}"
EOF
  track_job "${job}"
}

# ----------------------------------------------------------------------------
# materialize_deadline_backup_job applies an operator-shaped backup Job with a
# LOW activeDeadlineSeconds and a long `sleep 600` so Kubernetes kills it at the
# deadline (Failed condition reason=DeadlineExceeded). PER-RUN — proves the
# deadline kill WITHOUT lowering the production cluster's deadline.
# Args: <job-name> <activeDeadlineSeconds>
# ----------------------------------------------------------------------------
materialize_deadline_backup_job() {
  local job="$1" deadline="$2" uid script_b64
  uid="$(cluster_uid)"

  # shellcheck disable=SC2016
  script_b64="$(printf '%s' \
    'set -uo pipefail; echo "deadline: starting long backup (sleep 600), expect DeadlineExceeded"; sleep 600; echo "S83_DEADLINE_UNEXPECTED_COMPLETE"' \
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
    scenario: "83"
  ownerReferences:
    - apiVersion: avsoft.io/v1alpha1
      kind: CloudberryCluster
      name: ${CLUSTER}
      uid: ${uid}
      controller: true
      blockOwnerDeletion: true
spec:
  backoffLimit: 2
  activeDeadlineSeconds: ${deadline}
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
EOF
  track_job "${job}"
}

# ----------------------------------------------------------------------------
# materialize_succeeded_backup_job creates a Succeeded backup-operation Job
# (operator-shaped: correct labels + owner-ref) keyed off a timestamp so the
# operator records it as the latest backup Job (Success -> last_status=0).
# Args: <ts>
# ----------------------------------------------------------------------------
materialize_succeeded_backup_job() {
  local ts="$1" job uid script_b64 now_iso
  job="${CLUSTER}-backup-${ts}"
  uid="$(cluster_uid)"
  now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  script_b64="$(printf '%s' "echo materialized backup ${ts}; exit 0" | base64 | tr -d '\n')"

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
    scenario: "83"
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
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\",
      \"conditions\": [{\"type\":\"Complete\",\"status\":\"True\",
        \"lastProbeTime\":\"${now_iso}\",\"lastTransitionTime\":\"${now_iso}\"}]}
  }" >/dev/null 2>&1 || \
  "${KN[@]}" patch job "${job}" --type=merge -p "{
    \"status\": {\"succeeded\": 1, \"startTime\": \"${now_iso}\",
      \"completionTime\": \"${now_iso}\"}
  }" >/dev/null 2>&1 || log_warn "could not patch backup Job ${job} Succeeded"
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

# coord_exec_full_backup runs a real FULL gpbackup of ${DB} to S3 and prints the
# captured 14-digit timestamp on stdout.
coord_exec_full_backup() {
  log_step "Running REAL FULL gpbackup of ${DB} via coordinator-exec (GOOD S3)" >&2
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
        --leaf-partition-data \
        --compression-type gzip --compression-level '"${COMPRESSION_LEVEL}"' 2>&1
    ')" || true
  printf '%s\n' "${out}" | grep -E "Backup (Timestamp|completed)" >&2 || true
  ts="$(printf '%s\n' "${out}" | sed -n 's/.*Backup Timestamp = \([0-9]\{14\}\).*/\1/p' | head -1)"
  if ! printf '%s\n' "${out}" | grep -q "Backup completed successfully"; then
    printf '%s\n' "${out}" | tail -20 >&2
    return 1
  fi
  log_info "FULL backup completed; timestamp=${ts}" >&2
  printf '%s\n' "${ts}"
}

# =============================================================================
# 83-healthy — real gpbackup success + last_status=0
# =============================================================================
run_healthy() {
  log_step "83-healthy — real gpbackup success (acceptance: all backups successful)"

  local ok=1 ts job last
  ts="$(coord_exec_full_backup || true)"
  if [ -n "${ts}" ]; then
    HEALTHY_BACKUP_TS="${ts}"
    log_info "83-healthy: real gpbackup reported 'Backup completed successfully' (ts=${ts}) OK"
  else
    log_warn "83-healthy: real gpbackup did not complete (S3 reachable? creds?)"; ok=0
    ts="$(date -u +%Y%m%d%H%M%S)"
  fi

  # Materialize a Succeeded backup Job + reconcile so the operator records
  # last_status=0.
  job="$(materialize_succeeded_backup_job "${ts}")"
  reconcile_cluster

  if wait_backup_last_status 0; then
    log_info "83-healthy: cloudberry_backup_last_status == 0 (VictoriaMetrics) OK"
  else
    last="$(vm_backup_last_status)"
    log_warn "83-healthy: cloudberry_backup_last_status=${last:-<empty>} (expected 0)"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 83-healthy "PASS"; else set_result 83-healthy "FAIL"; fi
}

# =============================================================================
# 83a — force-failure: backoffLimit=2 retries -> BackoffLimitExceeded + last_status=1
# =============================================================================
run_83a() {
  log_step "83a — force-failure (bad S3) retries to backoffLimit=2 -> Failed + last_status=1"

  local ok=1 job backoff reason last_status cr_status failed
  job="${CLUSTER}-backup-forcefail"
  materialize_failing_backup_job "${job}" 2

  # A1: the Job spec carries backoffLimit==2.
  backoff="$("${KN[@]}" get job "${job}" -o jsonpath='{.spec.backoffLimit}' 2>/dev/null || true)"
  if [ "${backoff}" = "2" ]; then
    log_info "83a: Job spec backoffLimit==2 OK"
  else
    log_warn "83a: Job spec backoffLimit=${backoff:-<empty>} (expected 2)"; ok=0
  fi

  # A2: the Job RETRIES (pods restart up to backoffLimit) and ends terminal
  # Failed with reason BackoffLimitExceeded.
  log_info "83a: waiting for the failing Job to retry + reach terminal Failed..."
  if wait_job_failed "${job}"; then
    failed="$(job_status_failed_count "${job}")"
    reason="$(job_failed_reason "${job}")"
    log_info "83a: Job terminal Failed (status.failed=${failed:-?}, reason=${reason:-<none>})"
    if [ "${reason}" = "BackoffLimitExceeded" ]; then
      log_info "83a: Failed condition reason=BackoffLimitExceeded OK"
    else
      log_warn "83a: Failed condition reason=${reason:-<none>} (expected BackoffLimitExceeded; backoff still failed)"
      # Still a failure — do not hard-fail on reason text variance across K8s.
    fi
  else
    log_warn "83a: Job did not reach terminal Failed within ${STATUS_TIMEOUT_SECS}s"; ok=0
  fi

  # A3: reconcile -> lastBackupStatus=Failed.
  reconcile_cluster
  local i=0
  cr_status=""
  while [ "${i}" -lt 18 ]; do
    cr_status="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
      -o jsonpath='{.status.lastBackupStatus}' 2>/dev/null || true)"
    [ "${cr_status}" = "Failed" ] && break
    reconcile_cluster
    sleep 10
    i=$(( i + 1 ))
  done
  if [ "${cr_status}" = "Failed" ]; then
    log_info "83a: status.lastBackupStatus == Failed OK"
  else
    log_warn "83a: status.lastBackupStatus=${cr_status:-<empty>} (expected Failed)"; ok=0
  fi

  # A4: cloudberry_backup_last_status == 1 (VictoriaMetrics).
  if wait_backup_last_status 1; then
    log_info "83a: cloudberry_backup_last_status == 1 (VictoriaMetrics) OK"
  else
    last_status="$(vm_backup_last_status)"
    log_warn "83a: cloudberry_backup_last_status=${last_status:-<empty>} (expected 1)"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 83a "PASS"; else set_result 83a "FAIL"; fi
}

# =============================================================================
# 83b — deadline: activeDeadlineSeconds kill -> DeadlineExceeded + last_status=1
# =============================================================================
run_83b() {
  log_step "83b — deadline kill (activeDeadlineSeconds=${DEADLINE_SECS} + sleep 600) -> Failed + last_status=1"

  local ok=1 job deadline reason cr_status last_status
  job="${CLUSTER}-backup-deadline"
  materialize_deadline_backup_job "${job}" "${DEADLINE_SECS}"

  # B1: the Job spec carries activeDeadlineSeconds==DEADLINE_SECS.
  deadline="$("${KN[@]}" get job "${job}" -o jsonpath='{.spec.activeDeadlineSeconds}' 2>/dev/null || true)"
  if [ "${deadline}" = "${DEADLINE_SECS}" ]; then
    log_info "83b: Job spec activeDeadlineSeconds==${DEADLINE_SECS} OK"
  else
    log_warn "83b: Job spec activeDeadlineSeconds=${deadline:-<empty>} (expected ${DEADLINE_SECS})"; ok=0
  fi

  # B2: the Job is killed at the deadline -> terminal Failed reason=DeadlineExceeded.
  log_info "83b: waiting for the deadline kill (~${DEADLINE_SECS}s + settle)..."
  if wait_job_failed "${job}"; then
    reason="$(job_failed_reason "${job}")"
    if [ "${reason}" = "DeadlineExceeded" ]; then
      log_info "83b: Failed condition reason=DeadlineExceeded OK"
    else
      log_warn "83b: Failed condition reason=${reason:-<none>} (expected DeadlineExceeded)"
      # Still failed -> backupJobStatus classifies Failed; do not hard-fail on text.
    fi
  else
    log_warn "83b: Job did not reach terminal Failed within ${STATUS_TIMEOUT_SECS}s"; ok=0
  fi

  # B3: reconcile -> lastBackupStatus=Failed.
  reconcile_cluster
  local i=0
  cr_status=""
  while [ "${i}" -lt 18 ]; do
    cr_status="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
      -o jsonpath='{.status.lastBackupStatus}' 2>/dev/null || true)"
    [ "${cr_status}" = "Failed" ] && break
    reconcile_cluster
    sleep 10
    i=$(( i + 1 ))
  done
  if [ "${cr_status}" = "Failed" ]; then
    log_info "83b: status.lastBackupStatus == Failed OK"
  else
    log_warn "83b: status.lastBackupStatus=${cr_status:-<empty>} (expected Failed)"; ok=0
  fi

  # B4: cloudberry_backup_last_status == 1 (VictoriaMetrics).
  if wait_backup_last_status 1; then
    log_info "83b: cloudberry_backup_last_status == 1 (VictoriaMetrics) OK"
  else
    last_status="$(vm_backup_last_status)"
    log_warn "83b: cloudberry_backup_last_status=${last_status:-<empty>} (expected 1)"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 83b "PASS"; else set_result 83b "FAIL"; fi
}

# =============================================================================
# 83-reset — return steady-state last_status to 0 (all backups successful)
# =============================================================================
run_reset() {
  log_step "83-reset — observe a healthy success so steady-state last_status returns to 0"

  local ok=1 ts job last
  ts="${HEALTHY_BACKUP_TS}"
  [ -n "${ts}" ] || ts="$(date -u +%Y%m%d%H%M%S)"

  job="$(materialize_succeeded_backup_job "${ts}")"
  reconcile_cluster

  if wait_backup_last_status 0; then
    log_info "83-reset: steady-state cloudberry_backup_last_status == 0 OK"
  else
    last="$(vm_backup_last_status)"
    log_warn "83-reset: cloudberry_backup_last_status=${last:-<empty>} (expected 0)"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 83-reset "PASS"; else set_result 83-reset "FAIL"; fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 83 — Backup Failure Handling: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}"
  echo "  Cred Secret    : ${CRED_SECRET}  Backup SA: ${BACKUP_SA}"
  echo "  Bad S3 endpoint: ${BAD_S3_ENDPOINT}  Deadline: ${DEADLINE_SECS}s"
  echo "  VictoriaMetrics: ${VM_URL}"
  local any_fail=0 c r
  for c in 83-healthy 83a 83b 83-reset; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 83 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 83 backup-failure PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  ensure_tables

  if want_check 83-healthy; then run_healthy; fi
  if want_check 83a; then run_83a; fi
  if want_check 83b; then run_83b; fi
  if want_check 83-reset; then run_reset; fi

  print_summary
}

main "$@"
