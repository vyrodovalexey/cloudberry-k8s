#!/usr/bin/env bash
# =============================================================================
# Phase 7f — PXF external WRITABLE/READABLE tables acceptance (data round-trip)
# =============================================================================
# Drives the Phase 7f acceptance against the live, Running acceptance-test
# Cloudberry cluster in local Kubernetes:
#
#   For each of 7 sources (s3:text, s3:parquet, s3:avro, hdfs:text,
#   hdfs:parquet, hdfs:avro, hdfs:SequenceFile):
#     1. (Re)build a ~10MB internal staging table.
#     2. CREATE WRITABLE EXTERNAL TABLE -> INSERT (writes ~10MB to S3/HDFS via PXF).
#     3. CREATE READABLE EXTERNAL TABLE over the SAME path -> SELECT count + sample.
#     4. PROVE the bytes landed by listing MinIO (mc) / HDFS (hdfs dfs).
#
# Connection method mirrors test/e2e/scripts/phase7d-dataload-mydb.sh and
# scenario71-backup-restore.sh: kubectl exec into the coordinator pod, source
# the Cloudberry env, run psql as gpadmin over the local socket.
#
# SequenceFile prerequisite: the hdfs:SequenceFile profile needs a user-supplied
# Java Writable class (Phase7fRecord) on the PXF classpath. This script compiles
# it (host javac, --release 11) against the Hadoop jars bundled in the PXF app
# jar and deploys Phase7fRecord.class into /pxf-base/lib on every segment-primary
# PXF sidecar, then restarts PXF so the class is picked up. Set
# SKIP_SEQFILE_DEPLOY=1 to skip if the class is already present.
#
# All configuration is via ENV (no hardcoded namespace / cluster / password):
#   NAMESPACE            (default cloudberry-test)
#   CLUSTER              (default acceptance-test)
#   COORD_POD            (default ${CLUSTER}-coordinator-0)
#   DB_CONTAINER         (default cloudberry; auto-detected if unset)
#   DB_PW_SECRET         (default ${CLUSTER}-admin-password)
#   DB                   (default extdb; the DB that has the pxf extension)
#   ROWS                 (default 175000; ~10MB per source in the text encoding,
#                         ~20MB heap staging; parquet/avro compress to ~5-8MB)
#   S3_BUCKET            (default cloudberry-data)
#   MINIO_CONTAINER      (default minio)
#   NAMENODE_CONTAINER   (default namenode)
#   PXF_PRIMARY_LABEL    label selector for segment-primary pods carrying the
#                        PXF sidecar (default cloudberry.role=segment-primary;
#                        falls back to enumerating ${CLUSTER}-segment-primary-N)
#   SKIP_SEQFILE_DEPLOY  (default 0; set 1 to skip the Writable class deploy)
#   KUBECTL              (default kubectl)
# =============================================================================
set -euo pipefail

NAMESPACE="${NAMESPACE:-cloudberry-test}"
CLUSTER="${CLUSTER:-acceptance-test}"
COORD_POD="${COORD_POD:-${CLUSTER}-coordinator-0}"
DB_PW_SECRET="${DB_PW_SECRET:-${CLUSTER}-admin-password}"
DB="${DB:-extdb}"
ROWS="${ROWS:-175000}"
S3_BUCKET="${S3_BUCKET:-cloudberry-data}"
MINIO_CONTAINER="${MINIO_CONTAINER:-minio}"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
SKIP_SEQFILE_DEPLOY="${SKIP_SEQFILE_DEPLOY:-0}"
KUBECTL="${KUBECTL:-kubectl}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASES_DIR="${CASES_DIR:-${SCRIPT_DIR}/../../cases}"
SQL_FILE="${CASES_DIR}/phase7f_external_tables.sql"
JAVA_SRC="${CASES_DIR}/Phase7fRecord.java"

KN=("${KUBECTL}" -n "${NAMESPACE}")

# Per-source bookkeeping for the final summary.
HDFS_PREFIX="/phase7f"
S3_PREFIX="phase7f"

# Scratch dirs created during the run; cleaned up by the EXIT trap.
WORK_DIRS=()
cleanup() {
  local d
  for d in "${WORK_DIRS[@]:-}"; do
    [ -n "${d}" ] && rm -rf "${d}" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
log_step() { printf '\n%b========== %s ==========%b\n' "${BLUE}" "$*" "${NC}"; }
die()  { printf '%b[ERROR]%b %s\n' "${RED}" "${NC}" "$*" >&2; exit 1; }

# ----------------------------------------------------------------------------
# Resolve DB password + coordinator container (ENV-driven, never hardcoded).
# ----------------------------------------------------------------------------
DB_PASSWORD="$("${KN[@]}" get secret "${DB_PW_SECRET}" \
  -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
[ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${DB_PW_SECRET}"

if [ -z "${DB_CONTAINER:-}" ]; then
  DB_CONTAINER="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
fi
DB_CONTAINER="${DB_CONTAINER:-cloudberry}"

# coord_env emits a shell snippet that sources the Cloudberry env regardless of
# whether the image ships greenplum_path.sh or cloudberry-env.sh.
coord_env='GPENV=$(ls ${GPHOME}/greenplum_path.sh ${GPHOME}/cloudberry-env.sh /usr/local/cloudberry-db/greenplum_path.sh 2>/dev/null | head -1); [ -n "$GPENV" ] && . "$GPENV"'

# coord_psql <sql> [extra psql args...]
coord_psql() {
  local sql="$1"; shift
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER}" -- \
    bash -lc "${coord_env}"'
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" "${@:4}" -c "$3"
    ' _ "${DB_PASSWORD}" "${DB}" "${sql}" "$@"
}

# coord_psql_file — stream the phase7f SQL file into psql, passing -v rows=ROWS.
coord_psql_file() {
  local file="$1"
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER}" -- \
    bash -lc "${coord_env}"'
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -v rows="$3" -U gpadmin -d "$2" -f -
    ' _ "${DB_PASSWORD}" "${DB}" "${ROWS}" < "${file}"
}

# scalar <sql> — return a single scalar value from the DB.
scalar() { coord_psql "$1" -At; }

# ----------------------------------------------------------------------------
# Discover the segment-primary pods that carry the PXF sidecar.
# ----------------------------------------------------------------------------
discover_pxf_pods() {
  local pods
  pods="$("${KN[@]}" get pods -l "${PXF_PRIMARY_LABEL:-cloudberry.role=segment-primary}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  if [ -z "${pods}" ]; then
    # Fallback: enumerate ${CLUSTER}-segment-primary-N until one is missing.
    local i=0 name
    pods=""
    while :; do
      name="${CLUSTER}-segment-primary-${i}"
      "${KN[@]}" get pod "${name}" >/dev/null 2>&1 || break
      pods="${pods}${name}"$'\n'
      i=$((i + 1))
    done
  fi
  printf '%s' "${pods}" | sed '/^$/d'
}

# ----------------------------------------------------------------------------
# Compile + deploy the Phase7fRecord Writable class for hdfs:SequenceFile.
# ----------------------------------------------------------------------------
deploy_seqfile_class() {
  log_step "Deploying Phase7fRecord Writable class for hdfs:SequenceFile"
  [ -f "${JAVA_SRC}" ] || die "missing ${JAVA_SRC}"
  command -v javac >/dev/null 2>&1 || die "javac not found on host (needed to compile Phase7fRecord)"

  local pods
  pods="$(discover_pxf_pods)"
  [ -n "${pods}" ] || die "no segment-primary PXF pods discovered"
  log "PXF sidecar pods: $(echo "${pods}" | tr '\n' ' ')"

  local first_pod
  first_pod="$(echo "${pods}" | head -1)"

  # mktemp scratch dir; registered for cleanup via the script-level EXIT trap so
  # we never reference it after the function returns (avoids set -u surprises).
  local work; work="$(mktemp -d -t phase7f.XXXXXX)"
  WORK_DIRS+=("${work}")

  # Pull the PXF app jar and extract the Hadoop jars to compile against.
  log "Extracting Hadoop jars from the PXF app jar (for compile classpath)..."
  local appjar
  appjar="$("${KN[@]}" exec "${first_pod}" -c pxf -- bash -lc \
    'ls /usr/local/cloudberry-pxf/application/pxf-app-*.jar 2>/dev/null | head -1' 2>/dev/null || true)"
  [ -n "${appjar}" ] || die "could not locate pxf-app jar in ${first_pod}"
  "${KN[@]}" cp -c pxf "${first_pod}:${appjar}" "${work}/pxf-app.jar" >/dev/null 2>&1 \
    || die "failed to copy ${appjar} from ${first_pod}"

  ( cd "${work}" && jar xf pxf-app.jar \
      BOOT-INF/lib/hadoop-common-2.10.2.jar \
      BOOT-INF/lib/hadoop-mapreduce-client-core-2.10.2.jar ) \
    || die "failed to extract hadoop jars (version mismatch? inspect ${work})"

  local cp="${work}/BOOT-INF/lib/hadoop-common-2.10.2.jar:${work}/BOOT-INF/lib/hadoop-mapreduce-client-core-2.10.2.jar"
  log "Compiling Phase7fRecord.java with --release 11..."
  mkdir -p "${work}/classes"
  javac --release 11 -cp "${cp}" -d "${work}/classes" "${JAVA_SRC}" \
    || die "javac failed for ${JAVA_SRC}"
  [ -f "${work}/classes/Phase7fRecord.class" ] || die "Phase7fRecord.class not produced"

  # Deploy to /pxf-base/lib (on the PXF Spring Boot LOADER_PATH) on every pod.
  local p
  while IFS= read -r p; do
    [ -n "${p}" ] || continue
    "${KN[@]}" cp -c pxf "${work}/classes/Phase7fRecord.class" \
      "${p}:/pxf-base/lib/Phase7fRecord.class" >/dev/null 2>&1 \
      || die "failed to deploy class to ${p}"
    log "  deployed Phase7fRecord.class -> ${p}:/pxf-base/lib/"
  done <<< "${pods}"

  # Restart PXF so the new classpath entry is loaded, then wait for health.
  log "Restarting PXF on each sidecar (background) to load the class..."
  while IFS= read -r p; do
    [ -n "${p}" ] || continue
    "${KN[@]}" exec "${p}" -c pxf -- bash -lc \
      'export PXF_BASE=/pxf-base; nohup /usr/local/cloudberry-pxf/bin/pxf restart >/tmp/pxf-restart.log 2>&1 & echo restart-dispatched' \
      >/dev/null 2>&1 || true
  done <<< "${pods}"

  log "Waiting for PXF /actuator/health UP on all sidecars..."
  local attempt up total
  total="$(echo "${pods}" | wc -l | tr -d ' ')"
  for attempt in $(seq 1 40); do
    up=0
    while IFS= read -r p; do
      [ -n "${p}" ] || continue
      if "${KN[@]}" exec "${p}" -c pxf -- bash -lc \
          'curl -s --max-time 5 http://localhost:5888/actuator/health 2>/dev/null' 2>/dev/null \
          | grep -q '"status":"UP"'; then
        up=$((up + 1))
      fi
    done <<< "${pods}"
    log "  health: ${up}/${total} UP (attempt ${attempt})"
    [ "${up}" -eq "${total}" ] && { log "All PXF sidecars UP"; return 0; }
    sleep 8
  done
  die "PXF sidecars did not all report UP after restart"
}

# ----------------------------------------------------------------------------
# Preflight.
# ----------------------------------------------------------------------------
preflight() {
  log_step "Preflight (cluster=${CLUSTER} ns=${NAMESPACE} db=${DB} rows=${ROWS})"
  command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found"
  command -v docker >/dev/null 2>&1 || die "docker not found (needed for MinIO/HDFS evidence)"
  [ -f "${SQL_FILE}" ] || die "missing SQL file ${SQL_FILE}"

  "${KN[@]}" get cloudberrycluster "${CLUSTER}" >/dev/null 2>&1 \
    || die "CloudberryCluster ${CLUSTER} not found in ${NAMESPACE}"
  "${KN[@]}" wait --for=condition=ready "pod/${COORD_POD}" --timeout=300s >/dev/null \
    || die "coordinator pod ${COORD_POD} not Ready"

  # The target DB must have the pxf extension.
  local ext
  ext="$(scalar "SELECT count(*) FROM pg_extension WHERE extname='pxf';" || true)"
  if [ "${ext}" != "1" ]; then
    log "pxf extension not present in ${DB}; creating it..."
    coord_psql "CREATE EXTENSION IF NOT EXISTS pxf;" -At >/dev/null \
      || die "failed to CREATE EXTENSION pxf in ${DB}"
  fi
  log "pxf extension present in ${DB}"

  # Ensure a clean slate for the phase7f paths so byte accounting is exact.
  log "Cleaning any prior phase7f data in MinIO + HDFS..."
  docker exec "${MINIO_CONTAINER}" mc alias set local \
    http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true
  docker exec "${MINIO_CONTAINER}" mc rm --recursive --force \
    "local/${S3_BUCKET}/${S3_PREFIX}/" >/dev/null 2>&1 || true
  docker exec "${NAMENODE_CONTAINER}" hdfs dfs -rm -r -f "${HDFS_PREFIX}" >/dev/null 2>&1 || true
}

# ----------------------------------------------------------------------------
# Run the round-trip SQL (writes + reads for all 7 sources).
# ----------------------------------------------------------------------------
run_sql() {
  log_step "Running Phase 7f round-trip SQL (${SQL_FILE})"
  coord_psql_file "${SQL_FILE}"
}

# ----------------------------------------------------------------------------
# Evidence: list MinIO / HDFS and compute bytes landed per source.
# ----------------------------------------------------------------------------
s3_du() {
  # $1 = sub-path under the bucket. `mc du` prints e.g. "5.5MiB\t2 objects\t..".
  # Echo "<size> (<N> objects)".
  local sub="$1" out
  out="$(docker exec "${MINIO_CONTAINER}" mc du "local/${S3_BUCKET}/${sub}" 2>/dev/null | tail -1 || true)"
  [ -n "${out}" ] || { echo "(none)"; return; }
  echo "${out}" | awk -F'\t' '{printf "%s (%s)", $1, $2}'
}

hdfs_du() {
  # $1 = HDFS path. `hdfs dfs -du -s -h` prints "5.5 M  5.5 M  <path>" where the
  # first two columns are the (human) size. Echo "5.5 M".
  local out
  out="$(docker exec "${NAMENODE_CONTAINER}" hdfs dfs -du -s -h "$1" 2>/dev/null | tail -1 || true)"
  [ -n "${out}" ] || { echo "(none)"; return; }
  echo "${out}" | awk '{printf "%s %s", $1, $2}'
}

# ----------------------------------------------------------------------------
# Build the per-source summary table.
# ----------------------------------------------------------------------------
declare -a SRC_NAMES=(
  "s3:text" "s3:parquet" "s3:avro"
  "hdfs:text" "hdfs:parquet" "hdfs:avro" "hdfs:SequenceFile"
)
# parallel arrays: read-table name and backend listing path.
declare -a RTAB=(
  rext_s3_text rext_s3_parquet rext_s3_avro
  rext_hdfs_text rext_hdfs_parquet rext_hdfs_avro rext_hdfs_sequencefile
)
declare -a BACKEND=( s3 s3 s3 hdfs hdfs hdfs hdfs )
declare -a PATHS=(
  "${S3_PREFIX}/s3_text" "${S3_PREFIX}/s3_parquet" "${S3_PREFIX}/s3_avro"
  "${HDFS_PREFIX}/hdfs_text" "${HDFS_PREFIX}/hdfs_parquet"
  "${HDFS_PREFIX}/hdfs_avro" "${HDFS_PREFIX}/hdfs_sequencefile"
)

summary() {
  log_step "Phase 7f per-source acceptance summary"
  local staging_rows
  staging_rows="$(scalar "SELECT count(*) FROM p7f_staging;" || echo '?')"

  printf '\n%-20s | %-9s | %-11s | %-26s | %s\n' \
    "source" "writable" "rows" "bytes landed" "readable SELECT count"
  printf '%s\n' "---------------------+-----------+-------------+----------------------------+----------------------"

  local i fail=0
  for i in "${!SRC_NAMES[@]}"; do
    local src="${SRC_NAMES[$i]}" rtab="${RTAB[$i]}" be="${BACKEND[$i]}" p="${PATHS[$i]}"
    local rcount bytes wcreated

    rcount="$(scalar "SELECT count(*) FROM ${rtab};" 2>/dev/null || echo 'ERR')"
    wcreated="$(scalar "SELECT count(*) FROM pg_class WHERE relname='wext_${rtab#rext_}';" 2>/dev/null || echo 0)"
    [ "${wcreated}" = "1" ] && wcreated="Y" || wcreated="N"

    if [ "${be}" = "s3" ]; then
      bytes="$(s3_du "${p}")"
    else
      bytes="$(hdfs_du "${p}")"
    fi
    [ -n "${bytes## }" ] || bytes="(none)"

    printf '%-20s | %-9s | %-11s | %-26s | %s\n' \
      "${src}" "${wcreated}" "${rcount}" "${bytes}" "${rcount}"

    if [ "${wcreated}" != "Y" ] || [ "${rcount}" = "ERR" ] || [ "${rcount}" = "0" ]; then
      fail=$((fail + 1))
    fi
  done

  echo
  log "staging rows = ${staging_rows} (each source written from this table)"
  echo
  log_step "Backend listing evidence"
  echo "--- MinIO: ${S3_BUCKET}/${S3_PREFIX}/ ---"
  docker exec "${MINIO_CONTAINER}" mc ls --recursive "local/${S3_BUCKET}/${S3_PREFIX}/" 2>/dev/null || true
  echo "--- HDFS: ${HDFS_PREFIX} ---"
  docker exec "${NAMENODE_CONTAINER}" hdfs dfs -du -h "${HDFS_PREFIX}" 2>/dev/null || true

  echo
  if [ "${fail}" -eq 0 ]; then
    printf '%b[PASS]%b all 7 sources: writable created + data written + readable SELECT returned rows\n' \
      "${GREEN}" "${NC}"
  else
    printf '%b[FAIL]%b %d source(s) did not satisfy acceptance\n' "${RED}" "${NC}" "${fail}"
    exit 1
  fi
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  log "Phase 7f — PXF external tables acceptance (pod=${COORD_POD} container=${DB_CONTAINER})"
  preflight
  if [ "${SKIP_SEQFILE_DEPLOY}" != "1" ]; then
    deploy_seqfile_class
  else
    log "SKIP_SEQFILE_DEPLOY=1 — assuming Phase7fRecord.class already on PXF classpath"
  fi
  run_sql
  summary
  log "Phase 7f acceptance finished."
}

main "$@"
