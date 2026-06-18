#!/usr/bin/env bash
# =============================================================================
# Scenario 99 — Writable External Tables / Data Export
# Export-Target Preparation
# =============================================================================
# Prepares the EXPORT TARGETS the Scenario 99 writable-export tests write TO (and
# a small SOURCE dataset the e2e can export from). The writable-export machinery
# (FORMATTER='pxfwritable_export' + the reversed INSERT) already ships; this
# script only stages the destinations + grants so the live export LANDS:
#
#   FE.11  JDBC EXPORT TARGET (in pgsource sourcedb):
#     - CREATE TABLE IF NOT EXISTS export_target (id int, region text,
#       amount numeric), OWNED BY pxfuser, GRANT ALL to pxfuser. This is the
#       writable JDBC target the operator's jdbc export INSERTs INTO; the e2e
#       proves rows LAND via SELECT count(*) FROM export_target > 0. Truncated
#       here so the post-export count is deterministic.
#
#   FE.9/WE.1  S3 EXPORT PREFIX (MinIO):
#     - ensure the writable bucket cloudberry-warehouse exists + an exports/
#       prefix (a 0-byte marker object so the prefix is listable). The operator's
#       s3:text writable export writes objects under cloudberry-warehouse/exports/.
#
#   FE.10  HDFS EXPORT PREFIX (WebHDFS):
#     - ensure /data-lake/exports (and /data-lake/exports/hdfs) exist + are
#       writable (perm 1777). /data-lake is normally created by the env; this
#       script is idempotent and (re)creates the exports subtree.
#
#   SOURCE dataset (optional): a small export SOURCE the e2e exports from. The
#   e2e creates public.export_src in the CLUSTER itself, so this script only
#   DOCUMENTS the shape (id int, region text, amount numeric; region has a
#   selective value 'us-east' so the SF.1 filtered export lands FEWER rows).
#
# The script is IDEMPOTENT / re-runnable and logs which targets it PRODUCED and
# which are CONFIG-ONLY (e.g. when MinIO / WebHDFS / pgsource is down).
#
# HONESTY: no target here implies a bytes_transferred metric (it stays PLANNED).
# The honest "data lands" proofs are object landing (S3), file landing (HDFS
# LISTSTATUS) and ROW landing (JDBC count(*)), all asserted by the Scenario 99
# e2e Part B. The ALWAYS-true DDL signal is FORMATTER='pxfwritable_export'.
#
# Usage:
#   bash gen-export-targets.sh [--verify]
#
# Environment (documented defaults; use ENV, no hardcode):
#   MINIO_ADDR        - MinIO address          (default: http://127.0.0.1:9000)
#   MINIO_ACCESS_KEY  - MinIO access key        (default: minioadmin)
#   MINIO_SECRET_KEY  - MinIO secret key        (default: minioadmin)
#   MINIO_REGION      - S3 signing region       (default: us-east-1)
#   WEBHDFS_ADDR      - WebHDFS base URL         (default: http://127.0.0.1:9870)
#   PG_CONTAINER      - pgsource container       (default: pgsource)
#   PG_USER / PG_DB   - pgsource creds           (default: pxfuser / sourcedb)
#   WAREHOUSE_BUCKET  - S3 export bucket         (default: cloudberry-warehouse)
#   HDFS_EXPORT_DIR   - HDFS export dir          (default: /data-lake/exports)
# =============================================================================

set -euo pipefail

MINIO_ADDR="${MINIO_ADDR:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
WEBHDFS_ADDR="${WEBHDFS_ADDR:-http://127.0.0.1:9870}"

PG_CONTAINER="${PG_CONTAINER:-pgsource}"
PG_USER="${PG_USER:-pxfuser}"
PG_DB="${PG_DB:-sourcedb}"

WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-cloudberry-warehouse}"
HDFS_EXPORT_DIR="${HDFS_EXPORT_DIR:-/data-lake/exports}"

VERIFY_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
  esac
done

# Track produced vs config-only targets for the final summary.
PRODUCED=()
CONFIG_ONLY=()

log() { echo "[gen-export-targets] $*"; }

# ---------------------------------------------------------------------------
# SigV4 helpers (mirror gen-pushdown-samples.sh / gen-objstore-samples.sh).
# ---------------------------------------------------------------------------
_hmac_sha256_hex() { # $1=hexkey $2=data -> hex digest
  printf '%s' "$2" \
    | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" \
    | sed 's/^.*= //'
}
_sha256_hex_file() { openssl dgst -sha256 "$1" | sed 's/^.*= //'; }
_sha256_hex_str()  { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }

# create_bucket <bucket> — idempotent PUT /<bucket> (200 create / 409 exists).
create_bucket() {
  local bucket="$1"
  local scheme hostport host port
  scheme="${MINIO_ADDR%%://*}"
  hostport="${MINIO_ADDR#*://}"; hostport="${hostport%%/*}"
  host="${hostport%%:*}"; port="${hostport##*:}"
  [ "$port" = "$host" ] && port=9000

  local amzdate datestamp payload_hash
  amzdate="$(date -u +%Y%m%dT%H%M%SZ)"
  datestamp="$(date -u +%Y%m%d)"
  payload_hash="$(_sha256_hex_str "")"

  local canonical_headers="host:${host}:${port}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${amzdate}\n"
  local signed_headers="host;x-amz-content-sha256;x-amz-date"
  local canonical_request
  canonical_request="$(printf '%s\n%s\n%s\n%b\n%s\n%s' \
    "PUT" "/${bucket}" "" "$canonical_headers" "$signed_headers" "$payload_hash")"

  local credential_scope="${datestamp}/${MINIO_REGION}/s3/aws4_request"
  local string_to_sign
  string_to_sign="$(printf '%s\n%s\n%s\n%s' \
    "AWS4-HMAC-SHA256" "$amzdate" "$credential_scope" \
    "$(_sha256_hex_str "$canonical_request")")"

  local k_secret_hex k_date k_region k_service k_signing signature
  k_secret_hex="$(printf 'AWS4%s' "${MINIO_SECRET_KEY}" | xxd -p -c 256 | tr -d '\n')"
  k_date="$(_hmac_sha256_hex "$k_secret_hex" "$datestamp")"
  k_region="$(_hmac_sha256_hex "$k_date" "$MINIO_REGION")"
  k_service="$(_hmac_sha256_hex "$k_region" "s3")"
  k_signing="$(_hmac_sha256_hex "$k_service" "aws4_request")"
  signature="$(_hmac_sha256_hex "$k_signing" "$string_to_sign")"

  local authorization="AWS4-HMAC-SHA256 Credential=${MINIO_ACCESS_KEY}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -X PUT "${scheme}://${host}:${port}/${bucket}" \
    -H "Host: ${host}:${port}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amzdate}" \
    -H "Authorization: ${authorization}" 2>/dev/null || echo "000")"
  echo "$code"
}

# s3_put_file <bucket> <key> <localfile> [content-type] -> echoes HTTP code.
s3_put_file() {
  local bucket="$1" key="$2" file="$3" ctype="${4:-application/octet-stream}"

  local scheme hostport host port
  scheme="${MINIO_ADDR%%://*}"
  hostport="${MINIO_ADDR#*://}"; hostport="${hostport%%/*}"
  host="${hostport%%:*}"; port="${hostport##*:}"
  [ "$port" = "$host" ] && port=9000

  local amzdate datestamp payload_hash
  amzdate="$(date -u +%Y%m%dT%H%M%SZ)"
  datestamp="$(date -u +%Y%m%d)"
  payload_hash="$(_sha256_hex_file "$file")"

  local canonical_headers="host:${host}:${port}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${amzdate}\n"
  local signed_headers="host;x-amz-content-sha256;x-amz-date"
  local canonical_request
  canonical_request="$(printf '%s\n%s\n%s\n%b\n%s\n%s' \
    "PUT" "/${bucket}/${key}" "" \
    "$canonical_headers" "$signed_headers" "$payload_hash")"

  local credential_scope="${datestamp}/${MINIO_REGION}/s3/aws4_request"
  local string_to_sign
  string_to_sign="$(printf '%s\n%s\n%s\n%s' \
    "AWS4-HMAC-SHA256" "$amzdate" "$credential_scope" \
    "$(_sha256_hex_str "$canonical_request")")"

  local k_secret_hex k_date k_region k_service k_signing signature
  k_secret_hex="$(printf 'AWS4%s' "${MINIO_SECRET_KEY}" | xxd -p -c 256 | tr -d '\n')"
  k_date="$(_hmac_sha256_hex "$k_secret_hex" "$datestamp")"
  k_region="$(_hmac_sha256_hex "$k_date" "$MINIO_REGION")"
  k_service="$(_hmac_sha256_hex "$k_region" "s3")"
  k_signing="$(_hmac_sha256_hex "$k_service" "aws4_request")"
  signature="$(_hmac_sha256_hex "$k_signing" "$string_to_sign")"

  local authorization="AWS4-HMAC-SHA256 Credential=${MINIO_ACCESS_KEY}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -X PUT "${scheme}://${host}:${port}/${bucket}/${key}" \
    -H "Host: ${host}:${port}" \
    -H "Content-Type: ${ctype}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amzdate}" \
    -H "Authorization: ${authorization}" \
    --data-binary "@${file}" 2>/dev/null || echo "000")"
  echo "$code"
}

# webhdfs_mkdir <path> [perm] — idempotent MKDIRS (mirror gen-hadoop-samples.sh).
webhdfs_mkdir() {
  local path="$1" perm="${2:-1777}"
  curl -s -o /dev/null -w '%{http_code}' -X PUT \
    "${WEBHDFS_ADDR}/webhdfs/v1${path}?op=MKDIRS&permission=${perm}&user.name=hive" \
    2>/dev/null || echo "000"
}

minio_reachable() { curl -sf "${MINIO_ADDR}/minio/health/live" > /dev/null 2>&1; }
webhdfs_reachable() {
  curl -sf "${WEBHDFS_ADDR}/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1
}
pg_reachable() {
  command -v docker > /dev/null 2>&1 &&
    docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${PG_CONTAINER}$"
}

log "=== Scenario 99 export-target preparation ==="
log "MinIO: ${MINIO_ADDR} | WebHDFS: ${WEBHDFS_ADDR} | pgsource: ${PG_CONTAINER}/${PG_DB}"
log "S3 export bucket: ${WAREHOUSE_BUCKET} (prefix exports/) | HDFS export dir: ${HDFS_EXPORT_DIR}"

if [ "$VERIFY_ONLY" = true ]; then
  log "Verify mode: probing export targets..."
  minio_reachable   && log "  MinIO reachable"   || log "  MinIO NOT reachable"
  webhdfs_reachable && log "  WebHDFS reachable" || log "  WebHDFS NOT reachable"
  pg_reachable      && log "  pgsource reachable" || log "  pgsource NOT reachable"
  exit 0
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# ---------------------------------------------------------------------------
# FE.11 — JDBC EXPORT TARGET in pgsource sourcedb.
# Pre-create export_target (id int, region text, amount numeric), owned by
# pxfuser, GRANT ALL to pxfuser. TRUNCATE so the post-export count is
# deterministic. SCHEMA: matches the export SOURCE (public.export_src) the e2e
# seeds, so the reversed INSERT INTO export_target SELECT * FROM export_src works.
# ---------------------------------------------------------------------------
if pg_reachable; then
  log "FE.11 JDBC: preparing pgsource ${PG_DB}.export_target (id int, region text, amount numeric)..."
  if docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c "
      CREATE TABLE IF NOT EXISTS export_target (id int, region text, amount numeric);
      ALTER TABLE export_target OWNER TO ${PG_USER};
      GRANT ALL ON TABLE export_target TO ${PG_USER};
      TRUNCATE export_target;
    " > /dev/null 2>&1; then
    cnt="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c \
      "SELECT count(*) FROM export_target;" 2>/dev/null || echo "?")"
    log "  export_target ready (rows after truncate: ${cnt}); owner=${PG_USER}, GRANT ALL"
    PRODUCED+=("jdbc-export-target (FE.11: pgsource ${PG_DB}.export_target, GRANT ALL ${PG_USER})")
  else
    log "  could not prepare export_target (psql failed); FE.11 is [CONFIG-ONLY]"
    CONFIG_ONLY+=("jdbc-export-target (FE.11)")
  fi
else
  log "FE.11 JDBC: container '${PG_CONTAINER}' not running — start the env first; FE.11 [CONFIG-ONLY]"
  CONFIG_ONLY+=("jdbc-export-target (FE.11)")
fi

# ---------------------------------------------------------------------------
# FE.9 / WE.1 — S3 EXPORT PREFIX in MinIO (cloudberry-warehouse + exports/).
# Create the bucket (idempotent) and a 0-byte marker so the exports/ prefix is
# listable. The operator's s3:text writable export writes objects under it.
# ---------------------------------------------------------------------------
if minio_reachable; then
  log "FE.9/WE.1 S3: ensuring bucket ${WAREHOUSE_BUCKET} + exports/ prefix..."
  bcode="$(create_bucket "${WAREHOUSE_BUCKET}")"
  case "$bcode" in
    200|409) log "  bucket '${WAREHOUSE_BUCKET}' ready (HTTP ${bcode})" ;;
    *)       log "  WARNING: bucket '${WAREHOUSE_BUCKET}' create returned HTTP ${bcode}" ;;
  esac
  # 0-byte marker so the exports/ prefix exists + is listable (writable proof).
  : > "${WORK_DIR}/.keep"
  mcode="$(s3_put_file "${WAREHOUSE_BUCKET}" "exports/s3/.keep" "${WORK_DIR}/.keep" "text/plain")"
  case "$mcode" in
    200|201|204) log "  exports/s3/ prefix writable (marker uploaded, HTTP ${mcode})"
                 PRODUCED+=("s3-export-prefix (FE.9/WE.1: ${WAREHOUSE_BUCKET}/exports/s3/)") ;;
    *)           log "  WARNING: could not write exports/ marker (HTTP ${mcode}); [CONFIG-ONLY]"
                 CONFIG_ONLY+=("s3-export-prefix (FE.9/WE.1)") ;;
  esac
else
  log "FE.9/WE.1 S3: MinIO ${MINIO_ADDR} not reachable; S3 export prefix [CONFIG-ONLY]"
  CONFIG_ONLY+=("s3-export-prefix (FE.9/WE.1)")
fi

# ---------------------------------------------------------------------------
# FE.10 — HDFS EXPORT PREFIX (/data-lake/exports + /data-lake/exports/hdfs).
# /data-lake is normally created by the env; (re)create the exports subtree
# writable (perm 1777) so the hdfs:text writable export can write part files.
# ---------------------------------------------------------------------------
if webhdfs_reachable; then
  log "FE.10 HDFS: ensuring ${HDFS_EXPORT_DIR} (+ /hdfs) writable (1777)..."
  c1="$(webhdfs_mkdir "/data-lake" 1777)"
  c2="$(webhdfs_mkdir "${HDFS_EXPORT_DIR}" 1777)"
  c3="$(webhdfs_mkdir "${HDFS_EXPORT_DIR}/hdfs" 1777)"
  log "  MKDIRS /data-lake=${c1} ${HDFS_EXPORT_DIR}=${c2} ${HDFS_EXPORT_DIR}/hdfs=${c3}"
  case "$c2" in
    200) PRODUCED+=("hdfs-export-prefix (FE.10: ${HDFS_EXPORT_DIR}/hdfs, perm 1777)") ;;
    *)   CONFIG_ONLY+=("hdfs-export-prefix (FE.10)") ;;
  esac
else
  log "FE.10 HDFS: WebHDFS ${WEBHDFS_ADDR} not reachable; HDFS export prefix [CONFIG-ONLY]"
  CONFIG_ONLY+=("hdfs-export-prefix (FE.10)")
fi

# ---------------------------------------------------------------------------
# SOURCE dataset (DOCUMENTED): the e2e creates public.export_src in the CLUSTER.
# Shape: id int, region text, amount numeric. region carries a selective value
# 'us-east' (≈1/4 of rows) so SF.1's WHERE region='us-east' lands FEWER rows than
# the unfiltered baseline (a deterministic subset<total proof).
# ---------------------------------------------------------------------------
log "SOURCE dataset: the e2e seeds public.export_src (id int, region text, amount numeric)"
log "                in the cluster; region='us-east' is the SF.1 selective subset (~1/4)."

# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------
log ""
log "=== Export-target preparation complete ==="
log "PRODUCED:       ${PRODUCED[*]:-(none)}"
log "CONFIG-ONLY:    ${CONFIG_ONLY[*]:-(none)}"
log ""
log "FE.11  jdbc export target  -> pgsource ${PG_DB}.export_target (id int, region text, amount numeric)"
log "FE.9   s3 export prefix    -> ${WAREHOUSE_BUCKET}/exports/s3/ (MinIO; minioadmin/minioadmin)"
log "FE.10  hdfs export prefix  -> ${HDFS_EXPORT_DIR}/hdfs (WebHDFS; perm 1777)"
log "SF.1   filter column       -> region='us-east' selects a strict subset"
log ""
log "HONESTY: these targets prove writable export via object/file/row LANDING +"
log "the FORMATTER='pxfwritable_export' DDL clause. NO bytes_transferred metric is"
log "implied (it stays PLANNED)."
