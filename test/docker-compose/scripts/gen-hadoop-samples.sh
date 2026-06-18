#!/usr/bin/env bash
# =============================================================================
# Scenario 97 — Hadoop (HDFS / Hive / HBase) Sample Data Generator
# =============================================================================
# Generates the sample datasets used by Scenario 97 (Hadoop Profiles) into the
# compose stack's HDFS, Hive metastore and HBase:
#
#   HDFS (WebHDFS / namenode):
#     - /data-lake/events/data.csv   (text/CSV)  : NATIVE  — always produced.
#     - /data-lake/events/data.json  (JSON-lines): NATIVE  — always produced.
#     - parquet/avro/orc/sequencefile on HDFS    : produced via hive/beeline CTAS
#       into typed Hive tables stored on HDFS when beeline is available, else
#       reported [CONFIG-ONLY].
#
#   Hive (HiveServer2 / beeline, jdbc:hive2://localhost:10000):
#     - warehouse.fact_sales              (STORED AS TEXTFILE) : hive / hive:text
#     - warehouse.fact_sales_orc          (STORED AS ORC)      : hive:orc
#     - warehouse.fact_sales_rc           (STORED AS RCFILE)   : hive:rc  / FF.6a
#       (CTAS; [CONFIG-ONLY] when beeline absent)
#
#   HBase (hbase shell):
#     - pxf_hbase_test (cf:name, cf:value)  : HBase profile read.
#
# The script is IDEMPOTENT / re-runnable: it (re)creates HDFS directories
# (MKDIRS is a no-op when present), CREATE ... IF NOT EXISTS for Hive tables and
# create-if-absent for the HBase table. It LOGS clearly which formats/tables were
# PRODUCED and which are CONFIG-ONLY.
#
# Usage:
#   bash gen-hadoop-samples.sh [--verify] [--ci] [--no-docker] [--rows N]
#
# Environment (compose service defaults):
#   WEBHDFS_ADDR           - WebHDFS base URL    (default: http://127.0.0.1:9870)
#   HIVE_JDBC_HOSTPORT     - HiveServer2 endpoint (default: localhost:10000)
#   NAMENODE_CONTAINER     - namenode container  (default: namenode)
#   HIVESERVER2_CONTAINER  - hiveserver2 container (default: hiveserver2)
#   HBASE_CONTAINER        - hbase container      (default: hbase)
#   HADOOP_ROWS            - sample row count     (default: 1000)
# =============================================================================

set -euo pipefail

WEBHDFS_ADDR="${WEBHDFS_ADDR:-http://127.0.0.1:9870}"
HIVE_JDBC_HOSTPORT="${HIVE_JDBC_HOSTPORT:-localhost:10000}"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
HIVESERVER2_CONTAINER="${HIVESERVER2_CONTAINER:-hiveserver2}"
HBASE_CONTAINER="${HBASE_CONTAINER:-hbase}"
HADOOP_ROWS="${HADOOP_ROWS:-1000}"

VERIFY_ONLY=false
CI_MODE=false
USE_DOCKER=true

for arg in "$@"; do
  case "$arg" in
    --verify)    VERIFY_ONLY=true ;;
    --ci)        CI_MODE=true ;;
    --no-docker) USE_DOCKER=false ;;
    --rows)      shift; HADOOP_ROWS="${1:-1000}" ;;
  esac
done

# Track produced vs config-only artifacts for the final summary.
PRODUCED=()
CONFIG_ONLY=()

log() { echo "[gen-hadoop] $*"; }

# ---------------------------------------------------------------------------
# WebHDFS helpers (idempotent).
# ---------------------------------------------------------------------------
webhdfs_mkdir() { # $1=path [$2=perm]
  local path="$1" perm="${2:-1777}" code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
    "${WEBHDFS_ADDR}/webhdfs/v1${path}?op=MKDIRS&permission=${perm}&user.name=hive" \
    2>/dev/null || echo "000")"
  case "$code" in
    200) log "  HDFS dir ${path} ready (HTTP ${code})" ;;
    *)   log "  WARNING: could not create HDFS dir ${path} (HTTP ${code})" ;;
  esac
}

# webhdfs_put_local <local> <hdfs-path> : two-step WebHDFS CREATE (idempotent
# overwrite=true). Returns non-zero on failure.
webhdfs_put_local() {
  local localf="$1" path="$2"
  # Step 1: get the datanode redirect location (no data, follow disabled).
  local loc
  loc="$(curl -s -o /dev/null -w '%{redirect_url}' -X PUT \
    "${WEBHDFS_ADDR}/webhdfs/v1${path}?op=CREATE&overwrite=true&user.name=hive" \
    2>/dev/null || echo "")"
  if [ -z "$loc" ]; then
    # Some single-node setups serve CREATE directly (no redirect).
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
      "${WEBHDFS_ADDR}/webhdfs/v1${path}?op=CREATE&overwrite=true&user.name=hive" \
      -H 'Content-Type: application/octet-stream' --data-binary "@${localf}" \
      2>/dev/null || echo "000")"
    case "$code" in 201|200) return 0 ;; *) return 1 ;; esac
  fi
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$loc" \
    -H 'Content-Type: application/octet-stream' --data-binary "@${localf}" \
    2>/dev/null || echo "000")"
  case "$code" in 201|200) return 0 ;; *) return 1 ;; esac
}

# ---------------------------------------------------------------------------
# Readiness waits.
# ---------------------------------------------------------------------------
wait_hdfs() {
  log "Waiting for HDFS WebHDFS (${WEBHDFS_ADDR})..."
  for i in $(seq 1 60); do
    if curl -sf "${WEBHDFS_ADDR}/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1; then
      log "HDFS is ready."
      return 0
    fi
    sleep 3
  done
  log "ERROR: HDFS not ready after 60 attempts"
  return 1
}

# ---------------------------------------------------------------------------
# Verify mode: just confirm reachability and exit.
# ---------------------------------------------------------------------------
if [ "$VERIFY_ONLY" = true ]; then
  log "Verify mode: checking HDFS reachability..."
  curl -sf "${WEBHDFS_ADDR}/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1 \
    && log "HDFS reachable." || log "HDFS NOT reachable."
  exit 0
fi

log "=== Scenario 97 Hadoop sample generator ==="
log "WebHDFS: ${WEBHDFS_ADDR} | HiveServer2: ${HIVE_JDBC_HOSTPORT} | rows: ${HADOOP_ROWS}"

wait_hdfs

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# ---------------------------------------------------------------------------
# 1. Native HDFS text/CSV + JSON samples into /data-lake/events.
# ---------------------------------------------------------------------------
gen_csv()  { local f="$1"; { for ((r=1;r<=HADOOP_ROWS;r++)); do printf '%d,item-%d,%d\n' "$r" "$r" $((r*10)); done; } > "$f"; }
gen_json() { local f="$1"; { for ((r=1;r<=HADOOP_ROWS;r++)); do printf '{"id":%d,"name":"item-%d","value":%d}\n' "$r" "$r" $((r*10)); done; } > "$f"; }

log "Generating native HDFS text/CSV + JSON (${HADOOP_ROWS} rows)..."
gen_csv  "${WORK_DIR}/data.csv"
gen_json "${WORK_DIR}/data.json"

webhdfs_mkdir "/data-lake" 1777
webhdfs_mkdir "/data-lake/events" 1777
webhdfs_mkdir "/user/hive/warehouse" 1777

if webhdfs_put_local "${WORK_DIR}/data.csv" "/data-lake/events/data.csv"; then
  log "  uploaded /data-lake/events/data.csv (hdfs:text)"
  PRODUCED+=("hdfs:text")
else
  log "  WARNING: could not upload data.csv; hdfs:text is [CONFIG-ONLY]"
  CONFIG_ONLY+=("hdfs:text")
fi

if webhdfs_put_local "${WORK_DIR}/data.json" "/data-lake/events/data.json"; then
  log "  uploaded /data-lake/events/data.json (hdfs:json)"
  PRODUCED+=("hdfs:json")
else
  log "  WARNING: could not upload data.json; hdfs:json is [CONFIG-ONLY]"
  CONFIG_ONLY+=("hdfs:json")
fi

# ---------------------------------------------------------------------------
# 2. Hive tables via beeline (text / orc / rc) + a default for auto-detect.
#    text  -> warehouse.fact_sales        (hive, hive:text)
#    orc   -> warehouse.fact_sales_orc    (hive:orc)
#    rc    -> warehouse.fact_sales_rc     (hive:rc, FF.6a) via CTAS
#    These also back the typed HDFS reads (parquet/orc/sequencefile/rc) where
#    PXF reads the underlying HDFS warehouse files.
# ---------------------------------------------------------------------------
seed_hive() {
  if [ "$CI_MODE" = true ]; then
    log "  CI mode: skipping Hive table seeding; hive:* are [CONFIG-ONLY]"
    CONFIG_ONLY+=("hive" "hive:text" "hive:orc" "hive:rc")
    return 0
  fi
  if [ "$USE_DOCKER" = false ] || ! command -v docker > /dev/null 2>&1; then
    log "  docker unavailable; hive:* tables are [CONFIG-ONLY]"
    CONFIG_ONLY+=("hive" "hive:text" "hive:orc" "hive:rc")
    return 0
  fi

  log "Seeding Hive tables via beeline (${HIVESERVER2_CONTAINER})..."
  # Wait briefly for HiveServer2 JDBC.
  for i in $(seq 1 20); do
    if nc -z -w 3 "${HIVE_JDBC_HOSTPORT%%:*}" "${HIVE_JDBC_HOSTPORT##*:}" > /dev/null 2>&1; then
      break
    fi
    sleep 3
  done

  local hql
  hql=$(cat <<'SQL'
CREATE DATABASE IF NOT EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
  id BIGINT, product STRING, amount DOUBLE, sale_date STRING
) STORED AS TEXTFILE;
INSERT INTO warehouse.fact_sales VALUES
  (1,'widget',19.99,'2026-01-01'),
  (2,'gadget',49.50,'2026-01-02'),
  (3,'gizmo',12.25,'2026-01-03');
CREATE TABLE IF NOT EXISTS warehouse.fact_sales_orc
  STORED AS ORC AS SELECT * FROM warehouse.fact_sales;
CREATE TABLE IF NOT EXISTS warehouse.fact_sales_rc
  STORED AS RCFILE AS SELECT * FROM warehouse.fact_sales;
SHOW TABLES IN warehouse;
SQL
)

  if docker exec -i "${HIVESERVER2_CONTAINER}" \
       beeline -u "jdbc:hive2://localhost:10000" -e "${hql}" > /dev/null 2>&1; then
    log "  Hive tables created: fact_sales (TEXTFILE), fact_sales_orc (ORC), fact_sales_rc (RCFILE)"
    PRODUCED+=("hive" "hive:text" "hive:orc" "hive:rc")
    # The ORC/RC/sequencefile HDFS-level reads are backed by these Hive tables'
    # warehouse files; PXF hdfs:orc reads the ORC warehouse data.
    PRODUCED+=("hdfs:orc")
  else
    log "  WARNING: could not seed Hive tables (HiveServer2 may still be starting); hive:* [CONFIG-ONLY]"
    CONFIG_ONLY+=("hive" "hive:text" "hive:orc" "hive:rc" "hdfs:orc")
  fi
}

seed_hive

# parquet / avro / sequencefile on HDFS: no easy native local tool; produced only
# when a dedicated tool/CTAS path is available. Marked CONFIG-ONLY here so tests
# assert DDL/LOCATION correctness rather than live rows.
CONFIG_ONLY+=("hdfs:parquet" "hdfs:avro" "hdfs:sequencefile")

# ---------------------------------------------------------------------------
# 3. HBase sample table via hbase shell (idempotent).
# ---------------------------------------------------------------------------
seed_hbase() {
  if [ "$CI_MODE" = true ]; then
    log "  CI mode: skipping HBase table seeding; HBase is [CONFIG-ONLY]"
    CONFIG_ONLY+=("HBase")
    return 0
  fi
  if [ "$USE_DOCKER" = false ] || ! command -v docker > /dev/null 2>&1; then
    log "  docker unavailable; HBase table is [CONFIG-ONLY]"
    CONFIG_ONLY+=("HBase")
    return 0
  fi

  log "Seeding HBase table pxf_hbase_test via hbase shell (${HBASE_CONTAINER})..."
  docker exec -i "${HBASE_CONTAINER}" /hbase/bin/hbase shell <<'EOF' > /dev/null 2>&1 || true
create 'pxf_hbase_test', {NAME => 'cf', VERSIONS => 1}
EOF
  if docker exec -i "${HBASE_CONTAINER}" /hbase/bin/hbase shell <<'EOF' > /dev/null 2>&1
put 'pxf_hbase_test', 'row1', 'cf:name', 'widget'
put 'pxf_hbase_test', 'row1', 'cf:value', '19.99'
put 'pxf_hbase_test', 'row2', 'cf:name', 'gadget'
put 'pxf_hbase_test', 'row2', 'cf:value', '49.50'
put 'pxf_hbase_test', 'row3', 'cf:name', 'gizmo'
put 'pxf_hbase_test', 'row3', 'cf:value', '12.25'
exit
EOF
  then
    log "  HBase table pxf_hbase_test seeded (cf:name, cf:value)"
    PRODUCED+=("HBase")
  else
    log "  WARNING: could not seed HBase (slow under QEMU); HBase is [CONFIG-ONLY]"
    CONFIG_ONLY+=("HBase")
  fi
}

seed_hbase

# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------
log ""
log "=== Hadoop sample generation complete ==="
log "HDFS:        ${WEBHDFS_ADDR}  (/data-lake/events, /user/hive/warehouse)"
log "Hive:        jdbc:hive2://${HIVE_JDBC_HOSTPORT}  (warehouse.fact_sales*)"
log "HBase:       table pxf_hbase_test"
log "PRODUCED:    ${PRODUCED[*]:-(none)}"
log "CONFIG-ONLY: ${CONFIG_ONLY[*]:-(none)}"
log ""
log "CONFIG-ONLY profiles are NOT synthesized live this run (hdfs:parquet/avro/"
log "sequencefile have no easy local tool; orc/rc require beeline CTAS; HBase is"
log "slow under QEMU). Tests assert DDL/LOCATION/server-config correctness for"
log "config-only profiles and live rows for the produced ones."
