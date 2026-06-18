#!/usr/bin/env bash
# =============================================================================
# Scenario 96 — Object Store Sample Data Generator
# =============================================================================
# Generates sample datasets into the MinIO buckets used by Scenario 96
# (Object Store Profiles & Format Write-Capability):
#
#   - cloudberry-data       (s3-datalake server, AWS-style)
#   - cloudberry-warehouse  (minio-warehouse server, path-style)
#
# Formats:
#   - text / CSV : generated NATIVELY (printf) — always produced.
#   - json       : generated NATIVELY (printf, JSON-lines) — always produced.
#   - parquet    : generated via a small python tooling container
#                  (pandas + pyarrow) when docker is available; otherwise
#                  reported as [CONFIG-ONLY].
#   - avro       : generated via the same python container (fastavro) when
#                  docker is available; otherwise reported as [CONFIG-ONLY].
#   - orc        : [CONFIG-ONLY] — no easy local tool, never generated.
#
# The script is IDEMPOTENT / re-runnable: it (re)creates the buckets (idempotent)
# and overwrites the sample objects in place. It logs clearly which formats it
# PRODUCED and which are CONFIG-ONLY.
#
# Usage:
#   bash gen-objstore-samples.sh [--verify] [--no-docker] [--rows N]
#
# Environment (documented MinIO defaults):
#   MINIO_ADDR        - MinIO address           (default: http://127.0.0.1:9000)
#   MINIO_ACCESS_KEY  - MinIO access key         (default: minioadmin)
#   MINIO_SECRET_KEY  - MinIO secret key         (default: minioadmin)
#   MINIO_REGION      - S3 signing region        (default: us-east-1)
#   OBJSTORE_ROWS     - sample row count         (default: 1000)
#   PYTHON_IMAGE      - tooling image for parquet/avro
#                       (default: python:3.11-slim)
# =============================================================================

set -euo pipefail

MINIO_ADDR="${MINIO_ADDR:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
OBJSTORE_ROWS="${OBJSTORE_ROWS:-1000}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.11-slim}"

DATA_BUCKET="cloudberry-data"
WAREHOUSE_BUCKET="cloudberry-warehouse"

VERIFY_ONLY=false
USE_DOCKER=true

for arg in "$@"; do
  case "$arg" in
    --verify)    VERIFY_ONLY=true ;;
    --no-docker) USE_DOCKER=false ;;
    --rows)      shift; OBJSTORE_ROWS="${1:-1000}" ;;
  esac
done

# Track produced vs config-only formats for the final summary.
PRODUCED=()
CONFIG_ONLY=()

log()  { echo "[gen-objstore] $*"; }

# ---------------------------------------------------------------------------
# SigV4 helpers (mirrors setup-minio.sh) — used to PUT objects via curl when
# `mc` is not available.
# ---------------------------------------------------------------------------
_hmac_sha256_hex() { # $1=hexkey $2=data -> hex digest
  printf '%s' "$2" \
    | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" \
    | sed 's/^.*= //'
}
_sha256_hex_file() { openssl dgst -sha256 "$1" | sed 's/^.*= //'; }
_sha256_hex_str()  { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }

# s3_put_file <bucket> <key> <localfile> [content-type]
# Uploads a local file to MinIO via a SigV4-signed PUT. Returns non-zero on a
# non-2xx response.
s3_put_file() {
  local bucket="$1" key="$2" file="$3" ctype="${4:-application/octet-stream}"

  local scheme hostport host port
  scheme="${MINIO_ADDR%%://*}"
  hostport="${MINIO_ADDR#*://}"; hostport="${hostport%%/*}"
  host="${hostport%%:*}"; port="${hostport##*:}"
  [ "$port" = "$host" ] && port=9000

  local service="s3"
  local amzdate datestamp
  amzdate="$(date -u +%Y%m%dT%H%M%SZ)"
  datestamp="$(date -u +%Y%m%d)"

  local payload_hash
  payload_hash="$(_sha256_hex_file "$file")"

  local method="PUT"
  local canonical_uri="/${bucket}/${key}"
  local canonical_headers="host:${host}:${port}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${amzdate}\n"
  local signed_headers="host;x-amz-content-sha256;x-amz-date"
  local canonical_request
  canonical_request="$(printf '%s\n%s\n%s\n%b\n%s\n%s' \
    "$method" "$canonical_uri" "" \
    "$canonical_headers" "$signed_headers" "$payload_hash")"

  local algorithm="AWS4-HMAC-SHA256"
  local credential_scope="${datestamp}/${MINIO_REGION}/${service}/aws4_request"
  local string_to_sign
  string_to_sign="$(printf '%s\n%s\n%s\n%s' \
    "$algorithm" "$amzdate" "$credential_scope" \
    "$(_sha256_hex_str "$canonical_request")")"

  local k_secret_hex k_date k_region k_service k_signing signature
  k_secret_hex="$(printf 'AWS4%s' "${MINIO_SECRET_KEY}" | xxd -p -c 256 | tr -d '\n')"
  k_date="$(_hmac_sha256_hex "$k_secret_hex" "$datestamp")"
  k_region="$(_hmac_sha256_hex "$k_date" "$MINIO_REGION")"
  k_service="$(_hmac_sha256_hex "$k_region" "$service")"
  k_signing="$(_hmac_sha256_hex "$k_service" "aws4_request")"
  signature="$(_hmac_sha256_hex "$k_signing" "$string_to_sign")"

  local authorization="${algorithm} Credential=${MINIO_ACCESS_KEY}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}"

  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -X "$method" "${scheme}://${host}:${port}/${bucket}/${key}" \
    -H "Host: ${host}:${port}" \
    -H "Content-Type: ${ctype}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amzdate}" \
    -H "Authorization: ${authorization}" \
    --data-binary "@${file}" 2>/dev/null || echo "000")"

  case "$code" in
    200|201|204) return 0 ;;
    *) log "  WARNING: upload ${bucket}/${key} failed (HTTP ${code})"; return 1 ;;
  esac
}

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
  case "$code" in
    200|409) log "  bucket '${bucket}' ready (HTTP ${code})" ;;
    *) log "  WARNING: could not create bucket '${bucket}' (HTTP ${code})" ;;
  esac
}

# ---------------------------------------------------------------------------
# Wait for MinIO.
# ---------------------------------------------------------------------------
log "=== Scenario 96 object-store sample generator ==="
log "MinIO: ${MINIO_ADDR} | rows: ${OBJSTORE_ROWS}"

log "Waiting for MinIO..."
for i in $(seq 1 30); do
  if curl -sf "${MINIO_ADDR}/minio/health/live" > /dev/null 2>&1; then
    log "MinIO is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    log "ERROR: MinIO not ready after 30 attempts"
    exit 1
  fi
  sleep 2
done

if [ "$VERIFY_ONLY" = true ]; then
  log "Verify mode: MinIO is healthy."
  exit 0
fi

create_bucket "$DATA_BUCKET"
create_bucket "$WAREHOUSE_BUCKET"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# ---------------------------------------------------------------------------
# Native text/CSV + JSON generation.
# Single-column "id,name,value" CSV rows + a parallel JSON-lines file. The same
# logical dataset is staged under both buckets so s3-datalake and minio-warehouse
# can both be read live.
# ---------------------------------------------------------------------------
gen_csv() { # $1=outfile
  {
    for ((r = 1; r <= OBJSTORE_ROWS; r++)); do
      printf '%d,item-%d,%d\n' "$r" "$r" $((r * 10))
    done
  } > "$1"
}

gen_json() { # $1=outfile (JSON-lines)
  {
    for ((r = 1; r <= OBJSTORE_ROWS; r++)); do
      printf '{"id":%d,"name":"item-%d","value":%d}\n' "$r" "$r" $((r * 10))
    done
  } > "$1"
}

log "Generating native text/CSV + JSON (${OBJSTORE_ROWS} rows)..."
gen_csv  "${WORK_DIR}/data.csv"
gen_json "${WORK_DIR}/data.json"

for bucket in "$DATA_BUCKET" "$WAREHOUSE_BUCKET"; do
  s3_put_file "$bucket" "text/data.csv"  "${WORK_DIR}/data.csv"  "text/csv"        && \
    log "  uploaded ${bucket}/text/data.csv"
  s3_put_file "$bucket" "json/data.json" "${WORK_DIR}/data.json" "application/json" && \
    log "  uploaded ${bucket}/json/data.json"
done
PRODUCED+=("text/csv" "json")

# ---------------------------------------------------------------------------
# parquet / avro via a python tooling container (pandas+pyarrow+fastavro).
# ---------------------------------------------------------------------------
gen_parquet_avro_with_docker() {
  if ! command -v docker > /dev/null 2>&1; then
    log "docker not found; parquet/avro are [CONFIG-ONLY] this run"
    CONFIG_ONLY+=("parquet" "avro")
    return 0
  fi

  log "Generating parquet + avro via ${PYTHON_IMAGE} (pandas/pyarrow/fastavro)..."
  cat > "${WORK_DIR}/gen.py" <<'PYEOF'
import os
rows = int(os.environ.get("OBJSTORE_ROWS", "1000"))
data = {
    "id":   list(range(1, rows + 1)),
    "name": [f"item-{i}" for i in range(1, rows + 1)],
    "value":[i * 10 for i in range(1, rows + 1)],
}
ok = []
# parquet
try:
    import pandas as pd
    pd.DataFrame(data).to_parquet("/out/data.parquet", index=False)
    ok.append("parquet")
except Exception as e:
    print(f"PARQUET_FAILED: {e}")
# avro
try:
    import fastavro
    schema = {
        "type": "record", "name": "Sample",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "long"},
        ],
    }
    records = [
        {"id": data["id"][i], "name": data["name"][i], "value": data["value"][i]}
        for i in range(rows)
    ]
    with open("/out/data.avro", "wb") as f:
        fastavro.writer(f, schema, records)
    ok.append("avro")
except Exception as e:
    print(f"AVRO_FAILED: {e}")
print("PRODUCED:" + ",".join(ok))
PYEOF

  # Run the container: install deps quietly, then generate into the mounted /out.
  local rc=0
  docker run --rm \
    -e OBJSTORE_ROWS="${OBJSTORE_ROWS}" \
    -v "${WORK_DIR}:/out" \
    "${PYTHON_IMAGE}" \
    bash -lc "pip install --quiet --no-cache-dir pandas pyarrow fastavro >/dev/null 2>&1 && python /out/gen.py" \
    || rc=$?

  if [ "$rc" -ne 0 ]; then
    log "  python tooling container failed (rc=${rc}); parquet/avro are [CONFIG-ONLY]"
    CONFIG_ONLY+=("parquet" "avro")
    return 0
  fi

  if [ -f "${WORK_DIR}/data.parquet" ]; then
    for bucket in "$DATA_BUCKET" "$WAREHOUSE_BUCKET"; do
      s3_put_file "$bucket" "parquet/data.parquet" "${WORK_DIR}/data.parquet" \
        "application/octet-stream" && log "  uploaded ${bucket}/parquet/data.parquet"
    done
    PRODUCED+=("parquet")
  else
    log "  parquet not produced; [CONFIG-ONLY]"
    CONFIG_ONLY+=("parquet")
  fi

  if [ -f "${WORK_DIR}/data.avro" ]; then
    for bucket in "$DATA_BUCKET" "$WAREHOUSE_BUCKET"; do
      s3_put_file "$bucket" "avro/data.avro" "${WORK_DIR}/data.avro" \
        "application/octet-stream" && log "  uploaded ${bucket}/avro/data.avro"
    done
    PRODUCED+=("avro")
  else
    log "  avro not produced; [CONFIG-ONLY]"
    CONFIG_ONLY+=("avro")
  fi
}

if [ "$USE_DOCKER" = true ]; then
  gen_parquet_avro_with_docker
else
  log "--no-docker: parquet/avro are [CONFIG-ONLY] this run"
  CONFIG_ONLY+=("parquet" "avro")
fi

# ORC: no easy local tool — always config-only.
CONFIG_ONLY+=("orc")

# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------
log ""
log "=== Sample generation complete ==="
log "Buckets:        ${DATA_BUCKET}, ${WAREHOUSE_BUCKET}"
log "PRODUCED:       ${PRODUCED[*]:-(none)}"
log "CONFIG-ONLY:    ${CONFIG_ONLY[*]:-(none)}"
log ""
log "CONFIG-ONLY formats are NOT synthesized locally (orc: no easy tool;"
log "parquet/avro require docker + the python tooling image). Tests assert"
log "DDL/LOCATION/server-config correctness for config-only formats."
