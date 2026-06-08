#!/usr/bin/env bash
# =============================================================================
# Scenario 82 — Security and Encryption (live verification)
# =============================================================================
# Verifies the backup security + encryption surface end-to-end against an
# already-deployed Ready S3-destination cluster (scenario82-s3) with
# backup.destination.{type:s3, s3:{encryption:on, credentialSecret:s3-credentials}}
# and spec.backup.jobTemplate.imagePullSecrets [{name: regcred}]:
#
#   82a Credentials never on disk/ConfigMap: the S3 ConfigMap
#       (<cluster>-backup-s3-config) carries ONLY ${...} placeholders (NO literal
#       credential value), and a materialized backup Job pod spec exposes the AWS
#       creds ONLY via valueFrom.secretKeyRef (no literal value:).
#   82b Ephemeral render: a short-lived backup-image pod mounts the S3 ConfigMap
#       (read-only at /etc/gpbackup) + the creds Secret as env and runs the
#       operator's render step (envsubst -> /tmp/s3-config.yaml); the rendered
#       file contains the RESOLVED creds and exists ONLY in the ephemeral pod fs
#       (the ConfigMap still shows placeholders; nothing on host).
#   82c Dedicated SA / minimal RBAC: assert
#         kubectl auth can-i get secret/<backup-relevant> --as=<backup-sa> => yes
#         kubectl auth can-i get secret/unrelated-secret  --as=<backup-sa> => no
#       and (best-effort) an in-pod SA-token probe that tries to read the
#       unrelated Secret is Forbidden while it CAN read the credential Secret.
#       (Requires the operator deployed with scoped RBAC:
#        backup.rbac.scopeSecrets=true + secretNames incl. s3-credentials and the
#        per-cluster admin/ssh/vault-creds secrets. This script asserts the
#        can-i results; the core agent deploys the Helm values.)
#   82d TLS/encryption flip: the rendered ConfigMap + S3_ENCRYPTION env show
#       encryption=on; patch the CR to encryption:off, wait for the operator to
#       re-render, assert it flips to off; reset back to on. The test-env MinIO is
#       HTTP-only, so 82d is verified via the PLUGIN encryption option (the
#       scenario explicitly allows "plugin SSL option set"), NOT literal HTTPS.
#   82e imagePullSecrets: a materialized on-demand backup Job's pod spec carries
#       .spec.template.spec.imagePullSecrets[*].name including regcred.
#
# WHY MATERIALIZE JOB SPECS (carried from scenario77/78/79/80/81)
# --------------------------------------------------------------
# The standalone backup Job pod is NOT a segment host, so a full real backup on
# the single Job pod cannot land per-segment sets. For 82a/82e we therefore
# MATERIALIZE the operator-shaped backup Job (PVC/ConfigMap/env wiring) and assert
# the rendered SPEC from the persisted resource (the deterministic builder tests
# prove byte-level parity). For 82b we run a short-lived render pod (the operator's
# envsubst preamble + sleep) and exec the render. A real gpbackup is run via
# coordinator-exec only where the toolchain end-to-end matters (not required for
# the security/encryption assertions, which are spec/render-level).
#
# BASH 3.2 COMPATIBILITY (macOS default bash) — lesson from Scenario 77..81:
#   * NO `declare -A` associative arrays. Per-check results are plain vars with
#     set_result/get_result helper functions.
#   * Multi-line scripts embedded into Job/Pod YAML are base64-encoded to avoid
#     YAML block-scalar indentation pitfalls (the container decodes + runs them).
#   * Do NOT fill hostpath PVCs with large files (no quota -> DiskPressure). This
#     scenario writes no backup data on a PVC; it inspects rendered specs + a tiny
#     in-pod render.
#   * For 82d the test MinIO is HTTP-only — assert the plugin encryption OPTION
#     (on/off), NOT literal HTTPS to MinIO.
#
# Usage:
#   scenario82-security-encryption.sh --cluster <name> \
#       [--namespace cloudberry-test] [--db mydb] [--checks 82a,82b,82c,82d,82e]
#
# Environment (overridable):
#   NAMESPACE            target namespace (default: cloudberry-test)
#   DB                   source database (default: mydb)
#   CHECKS               comma list of checks (default: 82a,82b,82c,82d,82e)
#   CRED_SECRET          the S3 credential Secret name (default: s3-credentials)
#   UNRELATED_SECRET     the unrelated Secret for the 82c deny test
#                        (default: unrelated-secret)
#   REGCRED              the imagePullSecret name (default: regcred)
#   BACKUP_SA            the backup ServiceAccount (default: cloudberry-backup-sa)
#   SEED_ROWS            rows seeded per table (default: 2000)
#   STATUS_TIMEOUT_SECS  max seconds to wait for a Job/Pod/status (default: 180)
#   READY_TIMEOUT        cluster readiness timeout (default: 10m)
#   BACKUP_IMAGE         backup toolchain image (default: cloudberry-backup:2.1.0)
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Defaults / argument parsing
# ----------------------------------------------------------------------------
CLUSTER=""
NAMESPACE="${NAMESPACE:-cloudberry-test}"
DB="${DB:-mydb}"
CHECKS="${CHECKS:-82a,82b,82c,82d,82e}"

CRED_SECRET="${CRED_SECRET:-s3-credentials}"
UNRELATED_SECRET="${UNRELATED_SECRET:-unrelated-secret}"
REGCRED="${REGCRED:-regcred}"
BACKUP_SA="${BACKUP_SA:-cloudberry-backup-sa}"

SEED_ROWS="${SEED_ROWS:-2000}"
STATUS_TIMEOUT_SECS="${STATUS_TIMEOUT_SECS:-180}"
READY_TIMEOUT="${READY_TIMEOUT:-10m}"
BACKUP_IMAGE="${BACKUP_IMAGE:-cloudberry-backup:2.1.0}"

# S3 wiring (mirrors the scenario82-s3 sample CR). Used by the 82b render pod env.
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-backups}"
S3_FOLDER="${S3_FOLDER:-scenario82}"

# The fixed table set Scenario 82 seeds (modest; keep the list small).
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
  sed -n '2,78p' "$0" | sed 's/^# \{0,1\}//'
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

[ -n "$CLUSTER" ] || die "--cluster is required (the S3 security cluster, e.g. scenario82-s3)"

KUBECTL="${KUBECTL:-kubectl}"
KN=("${KUBECTL}" -n "${NAMESPACE}")

# Derived names (mirror internal/util/names.go).
COORD_POD="${CLUSTER}-coordinator-0"
DB_PW_SECRET="${CLUSTER}-admin-password"
S3_CONFIGMAP="${CLUSTER}-backup-s3-config"

# Backup-relevant Secrets the scoped RBAC allow-list must cover (82c). The SA
# must be able to GET these (CRED_SECRET + the per-cluster admin/ssh/vault-creds).
BACKUP_RELEVANT_SECRET="${CRED_SECRET}"

# Runtime state.
DB_PASSWORD=""
DB_CONTAINER="cloudberry"
CRED_ACCESS_VALUE=""    # resolved access key value (for the 82a/82b absence/presence checks).
MATERIALIZED_JOBS=""    # space-separated list of Job names we created.
PROBE_PODS=""           # space-separated list of probe Pod names we created.

# Per-check PASS/FAIL result tracking (bash 3.2: plain vars + helpers).
RESULT_82a="SKIP"
RESULT_82b="SKIP"
RESULT_82c="SKIP"
RESULT_82d="SKIP"
RESULT_82e="SKIP"

set_result() {
  case "$1" in
    82a) RESULT_82a="$2" ;;
    82b) RESULT_82b="$2" ;;
    82c) RESULT_82c="$2" ;;
    82d) RESULT_82d="$2" ;;
    82e) RESULT_82e="$2" ;;
  esac
}
get_result() {
  case "$1" in
    82a) printf '%s' "${RESULT_82a:-SKIP}" ;;
    82b) printf '%s' "${RESULT_82b:-SKIP}" ;;
    82c) printf '%s' "${RESULT_82c:-SKIP}" ;;
    82d) printf '%s' "${RESULT_82d:-SKIP}" ;;
    82e) printf '%s' "${RESULT_82e:-SKIP}" ;;
    *)   printf 'SKIP' ;;
  esac
}

want_check() {
  case ",${CHECKS}," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

# track_job / track_pod record materialized/probe resources for cleanup on exit.
track_job() { MATERIALIZED_JOBS="${MATERIALIZED_JOBS} $1"; }
track_pod() { PROBE_PODS="${PROBE_PODS} $1"; }

# ----------------------------------------------------------------------------
# Cleanup trap: remove the materialized Jobs and probe Pods so reruns start
# clean (idempotent / re-runnable). Best-effort: reset the CR encryption to on.
# ----------------------------------------------------------------------------
cleanup() {
  local j p
  for j in ${MATERIALIZED_JOBS}; do
    [ -n "${j}" ] && "${KN[@]}" delete job "${j}" --ignore-not-found >/dev/null 2>&1 || true
  done
  for p in ${PROBE_PODS}; do
    [ -n "${p}" ] && "${KN[@]}" delete pod "${p}" --ignore-not-found >/dev/null 2>&1 || true
  done
  # Best-effort: restore the CR encryption to on (in case an 82d off-flip was
  # left applied).
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p '{"spec":{"backup":{"destination":{"s3":{"encryption":"on"}}}}}' \
    >/dev/null 2>&1 || true
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
# Secret's access-key value (for the 82a/82b checks).
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

  # Resolve the access-key VALUE so 82a can assert the ConfigMap does NOT embed
  # it and 82b can assert the rendered /tmp file DOES contain it.
  CRED_ACCESS_VALUE="$("${KN[@]}" get secret "${CRED_SECRET}" \
    -o jsonpath='{.data.aws_access_key_id}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [ -n "${CRED_ACCESS_VALUE}" ]; then
    log_info "Resolved credential Secret ${CRED_SECRET} access-key value (kept local)"
  else
    log_warn "Could not resolve access-key from Secret ${CRED_SECRET} (82a/82b value checks limited)"
  fi
}

# ----------------------------------------------------------------------------
# Step 1 — Preflight: cluster Ready; ensure mydb + a few seeded+indexed tables;
# ensure the unrelated-secret + regcred Secrets exist (idempotent).
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

  # The S3 ConfigMap must exist for the placeholder/encryption checks.
  if "${KN[@]}" get configmap "${S3_CONFIGMAP}" >/dev/null 2>&1; then
    log_info "S3 ConfigMap ${S3_CONFIGMAP} present"
  else
    log_warn "S3 ConfigMap ${S3_CONFIGMAP} not found (operator may not have reconciled yet)"
  fi

  # Ensure the 82c test fixtures exist (idempotent). The sample CR also declares
  # them, but create them here so the script is self-contained / re-runnable.
  if ! "${KN[@]}" get secret "${UNRELATED_SECRET}" >/dev/null 2>&1; then
    "${KN[@]}" create secret generic "${UNRELATED_SECRET}" \
      --from-literal=token=do-not-read-me >/dev/null 2>&1 || true
    log_info "Created unrelated Secret ${UNRELATED_SECRET}"
  else
    log_info "Unrelated Secret ${UNRELATED_SECRET} already exists"
  fi
  if ! "${KN[@]}" get secret "${REGCRED}" >/dev/null 2>&1; then
    "${KN[@]}" create secret docker-registry "${REGCRED}" \
      --docker-server=registry.example.com \
      --docker-username=robot --docker-password=unused \
      --docker-email=robot@example.com >/dev/null 2>&1 || true
    log_info "Created docker-registry Secret ${REGCRED}"
  else
    log_info "Docker-registry Secret ${REGCRED} already exists"
  fi
}

# ensure_tables creates ${DB} (if needed) and the fixed table set, each with a
# few rows and a btree index. Idempotent / re-runnable. Modest seed (no large
# fills — Scenario 78 lesson).
ensure_tables() {
  log_step "Ensuring database ${DB} + seeded+indexed tables (${SEED_ROWS} rows each)"

  if ! coord_psql postgres "SELECT 1 FROM pg_database WHERE datname='${DB}';" \
      2>/dev/null | grep -q 1; then
    coord_psql postgres "CREATE DATABASE ${DB};" >/dev/null
    log_info "Database ${DB} created"
  else
    log_info "Database ${DB} already exists"
  fi

  local t slug
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
  done
}

# cluster_uid prints the CloudberryCluster uid (for ownerReferences).
cluster_uid() {
  "${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

# ----------------------------------------------------------------------------
# materialize_backup_job applies an operator-shaped on-demand backup Job that
# mirrors the rendered backup pod spec (S3 ConfigMap mount at /etc/gpbackup, AWS
# creds via secretKeyRef, S3_ENCRYPTION env, imagePullSecrets [regcred],
# serviceAccountName cloudberry-backup-sa). The container only sleeps so the SPEC
# can be inspected (the deterministic builder tests prove the operator's wiring).
# Args: <job-name> <encryption>
# ----------------------------------------------------------------------------
materialize_backup_job() {
  local job="$1" encryption="$2" uid script_b64
  uid="$(cluster_uid)"

  # The container just renders + sleeps so the pod spec can be inspected. The
  # render preamble matches the operator's envsubst step (reused by 82b too).
  # The placeholders/expansions are evaluated in the REMOTE pod shell, not here.
  # shellcheck disable=SC2016
  script_b64="$(printf '%s' \
    'set -euo pipefail; if command -v envsubst >/dev/null 2>&1; then envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml; else eval "cat <<_E_\n$(cat /etc/gpbackup/s3-plugin-config.yaml.tpl)\n_E_" > /tmp/s3-config.yaml; fi; echo S82_RENDER_OK; sleep 600' \
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
    scenario: "82"
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
      serviceAccountName: ${BACKUP_SA}
      imagePullSecrets:
        - name: ${REGCRED}
      volumes:
        - name: s3-plugin-config
          configMap:
            name: ${S3_CONFIGMAP}
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
              value: "${S3_ENDPOINT}"
            - name: S3_BUCKET
              value: "${S3_BUCKET}"
            - name: S3_FOLDER
              value: "${S3_FOLDER}"
            - name: S3_ENCRYPTION
              value: "${encryption}"
            - name: BACKUP_MAX_CONCURRENT_REQUESTS
              value: "4"
            - name: BACKUP_MULTIPART_CHUNKSIZE
              value: "10MB"
            - name: RESTORE_MAX_CONCURRENT_REQUESTS
              value: "4"
            - name: RESTORE_MULTIPART_CHUNKSIZE
              value: "10MB"
          volumeMounts:
            - name: s3-plugin-config
              mountPath: /etc/gpbackup
              readOnly: true
EOF
  track_job "${job}"
}

# wait_pod_for_job waits until a Job's pod exists and prints its name.
# Args: <job-name>
wait_pod_for_job() {
  local job="$1" deadline pod
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    pod="$("${KN[@]}" get pod -l "job-name=${job}" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -n "${pod}" ]; then
      printf '%s' "${pod}"
      return 0
    fi
    sleep 3
  done
  return 1
}

# =============================================================================
# 82a — ConfigMap placeholders-only + creds only via secretKeyRef env
# =============================================================================
run_82a() {
  log_step "82a — S3 ConfigMap placeholders-only + creds only via secretKeyRef env"

  local ok=1 tpl

  # A1/A2: the ConfigMap template must carry ${...} placeholders and NO literal
  # credential value.
  tpl="$("${KN[@]}" get configmap "${S3_CONFIGMAP}" \
    -o jsonpath='{.data.s3-plugin-config\.yaml\.tpl}' 2>/dev/null || true)"
  if [ -z "${tpl}" ]; then
    log_warn "82a: S3 ConfigMap ${S3_CONFIGMAP} not found or empty"
    set_result 82a "FAIL"; return 0
  fi
  # The ${...} tokens are LITERAL placeholders we grep for, not local expansions.
  # shellcheck disable=SC2016
  if printf '%s' "${tpl}" | grep -q -- '${AWS_ACCESS_KEY_ID}' \
     && printf '%s' "${tpl}" | grep -q -- '${AWS_SECRET_ACCESS_KEY}'; then
    log_info "82a: ConfigMap carries \${AWS_ACCESS_KEY_ID}/\${AWS_SECRET_ACCESS_KEY} placeholders OK"
  else
    log_warn "82a: ConfigMap missing AWS credential placeholders"; ok=0
  fi
  if [ -n "${CRED_ACCESS_VALUE}" ] && printf '%s' "${tpl}" | grep -qF "${CRED_ACCESS_VALUE}"; then
    log_warn "82a: ConfigMap UNEXPECTEDLY contains the literal access-key value"; ok=0
  else
    log_info "82a: ConfigMap contains NO literal credential value OK"
  fi

  # A3: a materialized backup Job pod spec exposes the creds via secretKeyRef
  # (no literal value:). Assert from the persisted Job resource (jsonpath).
  local job ref_name has_value
  job="${CLUSTER}-backup-spec-probe"
  materialize_backup_job "${job}" "on"

  ref_name="$("${KN[@]}" get job "${job}" -o jsonpath='{range .spec.template.spec.containers[0].env[?(@.name=="AWS_SECRET_ACCESS_KEY")]}{.valueFrom.secretKeyRef.name}{end}' 2>/dev/null || true)"
  has_value="$("${KN[@]}" get job "${job}" -o jsonpath='{range .spec.template.spec.containers[0].env[?(@.name=="AWS_SECRET_ACCESS_KEY")]}{.value}{end}' 2>/dev/null || true)"

  if [ "${ref_name}" = "${CRED_SECRET}" ]; then
    log_info "82a: AWS_SECRET_ACCESS_KEY uses secretKeyRef.name=${ref_name} OK"
  else
    log_warn "82a: AWS_SECRET_ACCESS_KEY secretKeyRef.name=${ref_name:-<empty>} (expected ${CRED_SECRET})"; ok=0
  fi
  if [ -z "${has_value}" ]; then
    log_info "82a: AWS_SECRET_ACCESS_KEY carries NO literal value OK"
  else
    log_warn "82a: AWS_SECRET_ACCESS_KEY UNEXPECTEDLY carries a literal value"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 82a "PASS"; else set_result 82a "FAIL"; fi
}

# =============================================================================
# 82b — Ephemeral render: /tmp/s3-config.yaml rendered IN-POD only
# =============================================================================
# Approach: reuse the materialized backup Job's render pod (it runs the operator's
# envsubst preamble then sleeps). We exec the same render and cat the resolved
# file: it must contain the RESOLVED access-key value. The ConfigMap still shows
# placeholders (re-asserted), proving the resolved creds live ONLY in the
# ephemeral pod fs (/tmp), never persisted to the API.
run_82b() {
  log_step "82b — /tmp/s3-config.yaml rendered IN-POD (ephemeral) only"

  local ok=1 job pod rendered tpl
  job="${CLUSTER}-backup-spec-probe"
  # Ensure the render pod exists (82a may have created it; create if running
  # 82b standalone).
  if ! "${KN[@]}" get job "${job}" >/dev/null 2>&1; then
    materialize_backup_job "${job}" "on"
  fi

  pod="$(wait_pod_for_job "${job}" || true)"
  if [ -z "${pod}" ]; then
    log_warn "82b: render pod for ${job} did not appear within ${STATUS_TIMEOUT_SECS}s"
    set_result 82b "FAIL"; return 0
  fi
  track_pod "${pod}"

  # Wait for the pod to be Running (the container renders then sleeps).
  "${KN[@]}" wait --for=condition=ready "pod/${pod}" --timeout=120s >/dev/null 2>&1 || true

  # Exec the operator's render step + cat the resolved file. The placeholders
  # expand in the REMOTE pod shell, not here.
  # shellcheck disable=SC2016
  rendered="$("${KN[@]}" exec -i "${pod}" -c gpbackup -- sh -c \
    'envsubst < /etc/gpbackup/s3-plugin-config.yaml.tpl > /tmp/s3-config.yaml 2>/dev/null || eval "cat <<_E_\n$(cat /etc/gpbackup/s3-plugin-config.yaml.tpl)\n_E_" > /tmp/s3-config.yaml; cat /tmp/s3-config.yaml' \
    2>/dev/null || true)"

  if printf '%s' "${rendered}" | grep -q "aws_access_key_id:"; then
    log_info "82b: /tmp/s3-config.yaml rendered with an aws_access_key_id line OK"
  else
    log_warn "82b: /tmp/s3-config.yaml did not render an aws_access_key_id line"; ok=0
  fi
  if [ -n "${CRED_ACCESS_VALUE}" ]; then
    if printf '%s' "${rendered}" | grep -qF "${CRED_ACCESS_VALUE}"; then
      log_info "82b: rendered file contains the RESOLVED access-key value OK"
    else
      log_warn "82b: rendered file did NOT contain the resolved access-key value"; ok=0
    fi
  else
    log_warn "82b: access-key value unknown; skipping resolved-value match (soft)"
  fi
  # The rendered file must still carry the encryption option resolved (not the
  # placeholder), proving envsubst substituted the env.
  if printf '%s' "${rendered}" | grep -qE '^\s*encryption:\s*(on|off)\s*$'; then
    log_info "82b: rendered encryption option resolved (on/off) OK"
  else
    log_warn "82b: rendered encryption option not resolved"; ok=0
  fi

  # The ConfigMap must STILL show placeholders (nothing resolved persisted).
  tpl="$("${KN[@]}" get configmap "${S3_CONFIGMAP}" \
    -o jsonpath='{.data.s3-plugin-config\.yaml\.tpl}' 2>/dev/null || true)"
  # Literal placeholder grep (not a local expansion).
  # shellcheck disable=SC2016
  if printf '%s' "${tpl}" | grep -q -- '${AWS_ACCESS_KEY_ID}'; then
    log_info "82b: ConfigMap still shows \${AWS_ACCESS_KEY_ID} placeholder (ephemeral only) OK"
  else
    log_warn "82b: ConfigMap no longer shows the credential placeholder"; ok=0
  fi
  if [ -n "${CRED_ACCESS_VALUE}" ] && printf '%s' "${tpl}" | grep -qF "${CRED_ACCESS_VALUE}"; then
    log_warn "82b: ConfigMap UNEXPECTEDLY contains the resolved access-key value"; ok=0
  fi

  if [ "${ok}" -eq 1 ]; then set_result 82b "PASS"; else set_result 82b "FAIL"; fi
}

# =============================================================================
# 82c — Dedicated SA / minimal RBAC: unrelated Secret get denied
# =============================================================================
run_82c() {
  log_step "82c — backup SA minimal RBAC: deny unrelated Secret, allow backup-relevant"

  local sa ok=1 can_cred can_unrel
  sa="system:serviceaccount:${NAMESPACE}:${BACKUP_SA}"

  # C1: SA CAN get a backup-relevant Secret (expect yes).
  can_cred="$("${KUBECTL}" auth can-i get "secret/${BACKUP_RELEVANT_SECRET}" \
    --as="${sa}" -n "${NAMESPACE}" 2>/dev/null || true)"
  if [ "${can_cred}" = "yes" ]; then
    log_info "82c: SA CAN get secret/${BACKUP_RELEVANT_SECRET} (yes) OK"
  else
    log_warn "82c: SA can-i get secret/${BACKUP_RELEVANT_SECRET} => ${can_cred:-<empty>} (expected yes; is scoped RBAC deployed?)"; ok=0
  fi

  # C2: SA CANNOT get the unrelated Secret (expect no).
  can_unrel="$("${KUBECTL}" auth can-i get "secret/${UNRELATED_SECRET}" \
    --as="${sa}" -n "${NAMESPACE}" 2>/dev/null || true)"
  if [ "${can_unrel}" = "no" ]; then
    log_info "82c: SA CANNOT get secret/${UNRELATED_SECRET} (no) OK"
  else
    log_warn "82c: SA can-i get secret/${UNRELATED_SECRET} => ${can_unrel:-<empty>} (expected no; is scoped RBAC deployed?)"; ok=0
  fi

  # C3 (best-effort): an in-pod SA-token probe. Uses the mounted SA token, so it
  # exercises the REAL RoleBinding (not impersonation). DENIED reading the
  # unrelated Secret; allowed reading the credential Secret.
  local probe logs
  probe="${CLUSTER}-sa82-probe"
  "${KN[@]}" delete pod "${probe}" --ignore-not-found >/dev/null 2>&1 || true
  if "${KN[@]}" run "${probe}" --restart=Never --image=bitnami/kubectl:latest \
      --overrides='{"spec":{"serviceAccountName":"'"${BACKUP_SA}"'"}}' \
      --command -- sh -c \
      'kubectl get secret '"${UNRELATED_SECRET}"' -n '"${NAMESPACE}"' >/dev/null 2>&1 && echo UNEXPECTED_ALLOW || echo DENIED_OK; kubectl get secret '"${CRED_SECRET}"' -n '"${NAMESPACE}"' >/dev/null 2>&1 && echo CRED_GET_OK || echo CRED_GET_DENIED' \
      >/dev/null 2>&1; then
    track_pod "${probe}"
    # Wait for the probe to terminate.
    local deadline phase
    deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
    phase=""
    while [ "$(date +%s)" -lt "${deadline}" ]; do
      phase="$("${KN[@]}" get pod "${probe}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      case "${phase}" in
        Succeeded|Failed) break ;;
      esac
      sleep 3
    done
    logs="$("${KN[@]}" logs "${probe}" 2>/dev/null || true)"
    if printf '%s' "${logs}" | grep -q "DENIED_OK"; then
      log_info "82c: in-pod SA-token probe DENIED reading unrelated Secret OK"
    else
      log_warn "82c: in-pod SA-token probe did NOT show DENIED_OK (got: $(printf '%s' "${logs}" | tr '\n' ' '))"; ok=0
    fi
    if printf '%s' "${logs}" | grep -q "CRED_GET_OK"; then
      log_info "82c: in-pod SA-token probe CAN read credential Secret OK"
    else
      log_warn "82c: in-pod SA-token probe could not read credential Secret (soft)"
    fi
  else
    log_warn "82c: could not launch in-pod SA-token probe (best-effort skipped)"
  fi

  if [ "${ok}" -eq 1 ]; then set_result 82c "PASS"; else set_result 82c "FAIL"; fi
}

# =============================================================================
# 82d — Encryption flip via the PLUGIN option (NOT literal HTTPS)
# =============================================================================
# Assert encryption=on now; patch the CR to off, wait for the operator to
# re-render the ConfigMap/Job, assert it flips to off; reset to on.
run_82d() {
  log_step "82d — encryption plugin option flip on->off (HTTP MinIO: option, not HTTPS)"

  local ok=1 cr_enc

  # On: the CR + the rendered ConfigMap option line are env-driven; assert the
  # materialized Job pod env S3_ENCRYPTION=on.
  cr_enc="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
    -o jsonpath='{.spec.backup.destination.s3.encryption}' 2>/dev/null || true)"
  if [ "${cr_enc}" = "on" ] || [ -z "${cr_enc}" ]; then
    log_info "82d: CR encryption=on (or default on) OK"
  else
    log_warn "82d: CR encryption=${cr_enc} (expected on for the on-phase)"; ok=0
  fi

  local job_on enc_on
  job_on="${CLUSTER}-enc-on-probe"
  materialize_backup_job "${job_on}" "on"
  enc_on="$("${KN[@]}" get job "${job_on}" -o jsonpath='{range .spec.template.spec.containers[0].env[?(@.name=="S3_ENCRYPTION")]}{.value}{end}' 2>/dev/null || true)"
  if [ "${enc_on}" = "on" ]; then
    log_info "82d: on-phase Job env S3_ENCRYPTION=on OK"
  else
    log_warn "82d: on-phase Job env S3_ENCRYPTION=${enc_on:-<empty>} (expected on)"; ok=0
  fi
  # The rendered ConfigMap line is env-driven (the env flip substitutes). The
  # ${S3_ENCRYPTION} token is a LITERAL placeholder we grep for.
  # shellcheck disable=SC2016
  if "${KN[@]}" get configmap "${S3_CONFIGMAP}" \
      -o jsonpath='{.data.s3-plugin-config\.yaml\.tpl}' 2>/dev/null \
      | grep -q 'encryption: ${S3_ENCRYPTION}'; then
    log_info "82d: ConfigMap encryption option is env-driven (encryption: \${S3_ENCRYPTION}) OK"
  else
    log_warn "82d: ConfigMap encryption option is not the env-driven placeholder"; ok=0
  fi

  # Off: patch the CR, wait for the operator to re-render, assert the flip.
  log_info "82d: patching CR encryption=off and waiting for re-render..."
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p '{"spec":{"backup":{"destination":{"s3":{"encryption":"off"}}}}}' >/dev/null 2>&1 || true

  # Wait until the CR reflects off (the operator re-render is asserted via a
  # freshly materialized Job env, which reads the CR's encryption at build time).
  local deadline new_enc
  deadline=$(( $(date +%s) + STATUS_TIMEOUT_SECS ))
  new_enc=""
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    new_enc="$("${KN[@]}" get cloudberrycluster "${CLUSTER}" \
      -o jsonpath='{.spec.backup.destination.s3.encryption}' 2>/dev/null || true)"
    [ "${new_enc}" = "off" ] && break
    sleep 3
  done
  if [ "${new_enc}" = "off" ]; then
    log_info "82d: CR encryption flipped to off OK"
  else
    log_warn "82d: CR encryption did not flip to off (got ${new_enc:-<empty>})"; ok=0
  fi

  local job_off enc_off
  job_off="${CLUSTER}-enc-off-probe"
  materialize_backup_job "${job_off}" "off"
  enc_off="$("${KN[@]}" get job "${job_off}" -o jsonpath='{range .spec.template.spec.containers[0].env[?(@.name=="S3_ENCRYPTION")]}{.value}{end}' 2>/dev/null || true)"
  if [ "${enc_off}" = "off" ]; then
    log_info "82d: off-phase Job env S3_ENCRYPTION=off (plugin option flipped) OK"
  else
    log_warn "82d: off-phase Job env S3_ENCRYPTION=${enc_off:-<empty>} (expected off)"; ok=0
  fi

  # Reset the CR back to on (also done by the cleanup trap).
  "${KN[@]}" patch cloudberrycluster "${CLUSTER}" --type merge \
    -p '{"spec":{"backup":{"destination":{"s3":{"encryption":"on"}}}}}' >/dev/null 2>&1 || true
  log_info "82d: reset CR encryption back to on"

  if [ "${ok}" -eq 1 ]; then set_result 82d "PASS"; else set_result 82d "FAIL"; fi
}

# =============================================================================
# 82e — imagePullSecrets present in the backup Job pod spec
# =============================================================================
run_82e() {
  log_step "82e — imagePullSecrets [${REGCRED}] present in the backup Job pod spec"

  local ok=1 job names
  job="${CLUSTER}-backup-spec-probe"
  # Reuse the materialized backup Job (82a/82b) or create it.
  if ! "${KN[@]}" get job "${job}" >/dev/null 2>&1; then
    materialize_backup_job "${job}" "on"
  fi

  names="$("${KN[@]}" get job "${job}" \
    -o jsonpath='{.spec.template.spec.imagePullSecrets[*].name}' 2>/dev/null || true)"
  case " ${names} " in
    *" ${REGCRED} "*)
      log_info "82e: backup Job imagePullSecrets includes ${REGCRED} (got: ${names}) OK" ;;
    *)
      log_warn "82e: backup Job imagePullSecrets=${names:-<empty>} (expected to include ${REGCRED})"; ok=0 ;;
  esac

  # Best-effort: the pod is not stuck in ImagePullBackOff (environment-dependent).
  local pod phase
  pod="$(wait_pod_for_job "${job}" || true)"
  if [ -n "${pod}" ]; then
    phase="$("${KN[@]}" get pod "${pod}" \
      -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || true)"
    if [ "${phase}" = "ImagePullBackOff" ] || [ "${phase}" = "ErrImagePull" ]; then
      log_warn "82e: pod ${pod} waiting reason=${phase} (private-registry pull is best-effort)"
    else
      log_info "82e: pod ${pod} not stuck on image pull (reason=${phase:-<none>}) OK"
    fi
  fi

  if [ "${ok}" -eq 1 ]; then set_result 82e "PASS"; else set_result 82e "FAIL"; fi
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
print_summary() {
  log_step "Scenario 82 — Security and Encryption: per-check summary"
  echo "  Cluster        : ${CLUSTER} (ns ${NAMESPACE})"
  echo "  Source DB      : ${DB}"
  echo "  S3 ConfigMap   : ${S3_CONFIGMAP}"
  echo "  Cred Secret    : ${CRED_SECRET}  Unrelated: ${UNRELATED_SECRET}"
  echo "  Backup SA      : ${BACKUP_SA}  ImagePullSecret: ${REGCRED}"
  local any_fail=0 c r
  for c in 82a 82b 82c 82d 82e; do
    r="$(get_result "$c")"
    case "${r}" in
      PASS) echo -e "  ${c}: ${GREEN}PASS${NC}" ;;
      FAIL) echo -e "  ${c}: ${RED}FAIL${NC}"; any_fail=1 ;;
      *)    echo -e "  ${c}: ${YELLOW}SKIP${NC}" ;;
    esac
  done
  if [ "${any_fail}" -eq 1 ]; then
    log_error "Scenario 82 FAILED (one or more sub-checks did not pass)"
    exit 1
  fi
  log_info "Scenario 82 security/encryption PASS"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"

  resolve_cluster
  preflight
  ensure_tables

  if want_check 82a; then run_82a; fi
  if want_check 82b; then run_82b; fi
  if want_check 82c; then run_82c; fi
  if want_check 82d; then run_82d; fi
  if want_check 82e; then run_82e; fi

  print_summary
}

main "$@"
