#!/usr/bin/env bash
# =============================================================================
# Scenario 98 — Filter Pushdown / Column Projection / Per-Row Error Handling
# Sample Data Generator
# =============================================================================
# Generates the datasets the Scenario 98 tests need:
#
#   FE.1 / FE.4  WIDE + FILTERABLE Parquet (into MinIO bucket cloudberry-data):
#     - many columns (so column projection is observable) + a FILTER column
#       (`region`, `year`) and enough rows that a WHERE filter measurably reduces
#       the row count (filtered ≪ total). Produced via a python:3.11-slim
#       container (pandas + pyarrow), like gen-objstore-samples.sh. When docker /
#       the tooling image is unavailable → [CONFIG-ONLY].
#
#   FE.5  WIDE ORC: same wide table written as ORC via pyarrow.orc when available,
#         else [CONFIG-ONLY].
#
#   FE.2  FILTERABLE JDBC: reuses jdbc_test_data (seeded with 10000 rows by
#         setup-jdbc-sources.sh) on pgsource + mysql. The documented filter column
#         is `category` (5 selective values: electronics/clothing/food/books/
#         tools), so `WHERE category='electronics'` lands ≈1/5 of the rows. When
#         pgsource is reachable, JDBC source query logging is enabled best-effort
#         (ALTER SYSTEM SET log_statement='all') so the pushed predicate is
#         visible in the source log; else the proof relies on row-count + EXPLAIN.
#
#   FE.3  FILTERABLE Hive: reuses warehouse.fact_sales (created by
#         gen-hadoop-samples.sh). Documented filter column noted; [CONFIG-ONLY]
#         when no live Hive backing.
#
#   FE.12a/b  MALFORMED-ROW source (into MinIO cloudberry-data/errors/):
#     - a CSV with K VALID rows + M MALFORMED rows (wrong column count / bad
#       types). M is fixed at MALFORMED_BAD_ROWS (default 5) so a load with
#       SEGMENT REJECT LIMIT 10 (> 5) is TOLERATED (FE.12a → Completed) and a load
#       with SEGMENT REJECT LIMIT 2 (< 5) FAILS (FE.12b → Failed). The known
#       bad-row count straddles the two thresholds.
#
# The script is IDEMPOTENT / re-runnable: it (re)creates the bucket (idempotent)
# and overwrites the sample objects in place. It logs clearly which datasets it
# PRODUCED and which are CONFIG-ONLY.
#
# HONESTY: no dataset here implies a bytes_transferred metric. The honest proofs
# are row-count reduction (filtered<baseline), EXPLAIN, source query logs, and
# the load Job status — all asserted by the Scenario 98 e2e suite.
#
# Usage:
#   bash gen-pushdown-samples.sh [--verify] [--no-docker] [--rows N]
#
# Environment (documented defaults):
#   MINIO_ADDR        - MinIO address           (default: http://127.0.0.1:9000)
#   MINIO_ACCESS_KEY  - MinIO access key         (default: minioadmin)
#   MINIO_SECRET_KEY  - MinIO secret key         (default: minioadmin)
#   MINIO_REGION      - S3 signing region        (default: us-east-1)
#   PUSHDOWN_ROWS     - wide/filterable row count (default: 20000)
#   WIDE_COLS         - number of payload columns (default: 40)
#   MALFORMED_GOOD    - valid rows in the error CSV (default: 100)
#   MALFORMED_BAD_ROWS- malformed rows in the error CSV (default: 5)
#   PYTHON_IMAGE      - tooling image            (default: python:3.11-slim)
#   PG_CONTAINER      - postgres-source container (default: pgsource)
#   PG_USER / PG_DB   - pgsource creds            (default: pxfuser / sourcedb)
# =============================================================================

set -euo pipefail

MINIO_ADDR="${MINIO_ADDR:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
PUSHDOWN_ROWS="${PUSHDOWN_ROWS:-20000}"
WIDE_COLS="${WIDE_COLS:-40}"
MALFORMED_GOOD="${MALFORMED_GOOD:-100}"
MALFORMED_BAD_ROWS="${MALFORMED_BAD_ROWS:-5}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.11-slim}"

PG_CONTAINER="${PG_CONTAINER:-pgsource}"
PG_USER="${PG_USER:-pxfuser}"
PG_DB="${PG_DB:-sourcedb}"

DATA_BUCKET="cloudberry-data"

VERIFY_ONLY=false
USE_DOCKER=true

for arg in "$@"; do
  case "$arg" in
    --verify)    VERIFY_ONLY=true ;;
    --no-docker) USE_DOCKER=false ;;
    --rows)      shift; PUSHDOWN_ROWS="${1:-20000}" ;;
  esac
done

# Track produced vs config-only datasets for the final summary.
PRODUCED=()
CONFIG_ONLY=()

log() { echo "[gen-pushdown] $*"; }

# ---------------------------------------------------------------------------
# SigV4 helpers (mirror gen-objstore-samples.sh / setup-minio.sh).
# ---------------------------------------------------------------------------
_hmac_sha256_hex() { # $1=hexkey $2=data -> hex digest
  printf '%s' "$2" \
    | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" \
    | sed 's/^.*= //'
}
_sha256_hex_file() { openssl dgst -sha256 "$1" | sed 's/^.*= //'; }
_sha256_hex_str()  { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }

# s3_put_file <bucket> <key> <localfile> [content-type]
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
log "=== Scenario 98 pushdown/projection/error sample generator ==="
log "MinIO: ${MINIO_ADDR} | wide rows: ${PUSHDOWN_ROWS} | wide cols: ${WIDE_COLS}"
log "malformed CSV: ${MALFORMED_GOOD} good + ${MALFORMED_BAD_ROWS} bad rows"

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

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# ---------------------------------------------------------------------------
# FE.12a/b — malformed-row CSV (native, always produced).
# A header-less CSV with the schema (id,region,year,value). The first
# MALFORMED_GOOD rows are VALID; then MALFORMED_BAD_ROWS rows are MALFORMED:
# they have the wrong column count and a non-numeric value, so the engine rejects
# exactly MALFORMED_BAD_ROWS rows. SEGMENT REJECT LIMIT 10 tolerates them
# (FE.12a), SEGMENT REJECT LIMIT 2 fails (FE.12b).
# ---------------------------------------------------------------------------
gen_malformed_csv() { # $1=outfile
  {
    local r
    for ((r = 1; r <= MALFORMED_GOOD; r++)); do
      printf '%d,us-east,2024,%d\n' "$r" $((r * 10))
    done
    # MALFORMED_BAD_ROWS bad rows: too few columns + a non-numeric value.
    for ((r = 1; r <= MALFORMED_BAD_ROWS; r++)); do
      printf 'BAD-%d,not_a_number\n' "$r"
    done
  } > "$1"
}

log "Generating malformed-row CSV (${MALFORMED_GOOD} valid + ${MALFORMED_BAD_ROWS} malformed)..."
gen_malformed_csv "${WORK_DIR}/malformed.csv"
if s3_put_file "$DATA_BUCKET" "errors/malformed.csv" "${WORK_DIR}/malformed.csv" "text/csv"; then
  log "  uploaded ${DATA_BUCKET}/errors/malformed.csv"
  PRODUCED+=("malformed-csv (FE.12a/b)")
else
  CONFIG_ONLY+=("malformed-csv (FE.12a/b)")
fi

# ---------------------------------------------------------------------------
# FE.1 / FE.4 / FE.5 — WIDE + FILTERABLE Parquet (and ORC) via python tooling.
# ---------------------------------------------------------------------------
gen_wide_with_docker() {
  if ! command -v docker > /dev/null 2>&1; then
    log "docker not found; wide parquet/orc are [CONFIG-ONLY] this run"
    CONFIG_ONLY+=("wide-parquet (FE.1/FE.4)" "wide-orc (FE.5)")
    return 0
  fi

  log "Generating WIDE+FILTERABLE parquet (+orc) via ${PYTHON_IMAGE} (pandas/pyarrow)..."
  cat > "${WORK_DIR}/gen_wide.py" <<'PYEOF'
import os
rows = int(os.environ.get("PUSHDOWN_ROWS", "20000"))
cols = int(os.environ.get("WIDE_COLS", "40"))
regions = ["us-east", "us-west", "eu-central", "ap-south", "sa-east"]
years = [2021, 2022, 2023, 2024]

data = {
    "id":     list(range(1, rows + 1)),
    # FILTER columns: region (5 selective values) + year (4 values).
    "region": [regions[i % len(regions)] for i in range(rows)],
    "year":   [years[i % len(years)] for i in range(rows)],
    # A couple of named projection columns the EXPLAIN test selects.
    "col_a":  [i for i in range(rows)],
    "col_b":  [f"b-{i}" for i in range(rows)],
}
# Many extra payload columns so column projection is observable.
for c in range(cols):
    data[f"payload_{c:02d}"] = [f"p{c}-{i}" for i in range(rows)]

ok = []
try:
    import pandas as pd
    df = pd.DataFrame(data)
    df.to_parquet("/out/wide.parquet", index=False)
    ok.append("parquet")
except Exception as e:
    print(f"PARQUET_FAILED: {e}")

try:
    import pyarrow as pa
    import pyarrow.orc as orc
    import pandas as pd
    table = pa.Table.from_pandas(pd.DataFrame(data))
    orc.write_table(table, "/out/wide.orc")
    ok.append("orc")
except Exception as e:
    print(f"ORC_FAILED: {e}")

print("PRODUCED:" + ",".join(ok))
PYEOF

  local rc=0
  docker run --rm \
    -e PUSHDOWN_ROWS="${PUSHDOWN_ROWS}" \
    -e WIDE_COLS="${WIDE_COLS}" \
    -v "${WORK_DIR}:/out" \
    "${PYTHON_IMAGE}" \
    bash -lc "pip install --quiet --no-cache-dir pandas pyarrow >/dev/null 2>&1 && python /out/gen_wide.py" \
    || rc=$?

  if [ "$rc" -ne 0 ]; then
    log "  python tooling container failed (rc=${rc}); wide parquet/orc are [CONFIG-ONLY]"
    CONFIG_ONLY+=("wide-parquet (FE.1/FE.4)" "wide-orc (FE.5)")
    return 0
  fi

  if [ -f "${WORK_DIR}/wide.parquet" ]; then
    s3_put_file "$DATA_BUCKET" "wide/data.parquet" "${WORK_DIR}/wide.parquet" \
      "application/octet-stream" && log "  uploaded ${DATA_BUCKET}/wide/data.parquet"
    PRODUCED+=("wide-parquet (FE.1/FE.4)")
  else
    log "  wide parquet not produced; [CONFIG-ONLY]"
    CONFIG_ONLY+=("wide-parquet (FE.1/FE.4)")
  fi

  if [ -f "${WORK_DIR}/wide.orc" ]; then
    s3_put_file "$DATA_BUCKET" "wide/data.orc" "${WORK_DIR}/wide.orc" \
      "application/octet-stream" && log "  uploaded ${DATA_BUCKET}/wide/data.orc"
    PRODUCED+=("wide-orc (FE.5)")
  else
    log "  wide orc not produced; [CONFIG-ONLY]"
    CONFIG_ONLY+=("wide-orc (FE.5)")
  fi
}

if [ "$USE_DOCKER" = true ]; then
  gen_wide_with_docker
else
  log "--no-docker: wide parquet/orc are [CONFIG-ONLY] this run"
  CONFIG_ONLY+=("wide-parquet (FE.1/FE.4)" "wide-orc (FE.5)")
fi

# ---------------------------------------------------------------------------
# FE.2 — FILTERABLE JDBC: reuse jdbc_test_data (seeded by setup-jdbc-sources.sh).
# Document the filter column and enable source query logging best-effort.
# ---------------------------------------------------------------------------
configure_jdbc_logging() {
  if ! command -v docker > /dev/null 2>&1; then
    log "FE.2 JDBC: docker not found; jdbc_test_data reuse is CONFIG-noted "
    log "          (filter column 'category'); source query logging NOT enabled"
    CONFIG_ONLY+=("jdbc-source-log (FE.2)")
    PRODUCED+=("jdbc-filterable (FE.2: reuse jdbc_test_data, filter 'category')")
    return 0
  fi

  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${PG_CONTAINER}$"; then
    # Verify the seeded table + the filterable column exist.
    local cnt
    cnt="$(docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -tA -c \
      "SELECT count(*) FROM jdbc_test_data;" 2>/dev/null || echo "0")"
    log "FE.2 JDBC: pgsource jdbc_test_data has ${cnt} rows (filter column 'category')"

    # Enable source query logging so the pushed WHERE predicate is visible in the
    # postgres-source server log (best-effort; reload only).
    if docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -c \
        "ALTER SYSTEM SET log_statement='all';" > /dev/null 2>&1 && \
       docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -c \
        "SELECT pg_reload_conf();" > /dev/null 2>&1; then
      log "  enabled pgsource log_statement='all' — pushed predicate visible in the source log"
      PRODUCED+=("jdbc-source-log (FE.2: pgsource log_statement=all)")
    else
      log "  could not enable pgsource query logging; FE.2 relies on row-count + EXPLAIN"
      CONFIG_ONLY+=("jdbc-source-log (FE.2)")
    fi
    PRODUCED+=("jdbc-filterable (FE.2: reuse jdbc_test_data, filter 'category')")
  else
    log "FE.2 JDBC: container '${PG_CONTAINER}' not running — run setup-jdbc-sources.sh first"
    CONFIG_ONLY+=("jdbc-filterable (FE.2)")
  fi
}

configure_jdbc_logging

# ---------------------------------------------------------------------------
# FE.3 — FILTERABLE Hive: reuse warehouse.fact_sales (gen-hadoop-samples.sh).
# CONFIG-ONLY here; the Hive table + filterable column are documented.
# ---------------------------------------------------------------------------
log "FE.3 Hive: reuse warehouse.fact_sales (created by gen-hadoop-samples.sh) "
log "          as the filterable Hive table. Filter column: region/category as "
log "          present in fact_sales. [CONFIG-ONLY] without a live Hive backing."
CONFIG_ONLY+=("hive-filterable (FE.3: reuse warehouse.fact_sales)")

# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------
log ""
log "=== Sample generation complete ==="
log "Bucket:         ${DATA_BUCKET}"
log "PRODUCED:       ${PRODUCED[*]:-(none)}"
log "CONFIG-ONLY:    ${CONFIG_ONLY[*]:-(none)}"
log ""
log "FE.1/FE.4  wide+filterable parquet  -> ${DATA_BUCKET}/wide/data.parquet"
log "FE.5       wide orc                 -> ${DATA_BUCKET}/wide/data.orc (config-only w/o pyarrow-orc)"
log "FE.2       filterable jdbc          -> jdbc_test_data (filter column 'category')"
log "FE.3       filterable hive          -> warehouse.fact_sales (config-only w/o live Hive)"
log "FE.12a/b   malformed-row CSV        -> ${DATA_BUCKET}/errors/malformed.csv"
log "           (${MALFORMED_GOOD} valid + ${MALFORMED_BAD_ROWS} bad; limit 10=tolerate, limit 2=fail)"
log ""
log "HONESTY: these datasets prove pushdown/projection/error-handling via"
log "row-count reduction, EXPLAIN, source query logs, and the load Job status."
log "NO bytes_transferred metric is implied (it stays PLANNED)."
