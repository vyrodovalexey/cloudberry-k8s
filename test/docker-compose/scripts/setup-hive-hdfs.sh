#!/usr/bin/env bash
# =============================================================================
# Hive + HDFS Setup Script
# Prepares the test HDFS warehouse directory and a sample Hive table used by
# PXF data-loading tests (hdfs:* and hive:* profiles).
#
# Usage:
#   bash setup-hive-hdfs.sh [--verify] [--ci]
#
# Endpoints (match specifications/12-data-loading-spec.md PXF server config):
#   HDFS NameNode RPC   - hdfs://namenode:8020      (host: 127.0.0.1:8020)
#   HDFS NameNode UI    - http://namenode:9870      (host: 127.0.0.1:9870)
#   HDFS WebHDFS        - http://namenode:9870/webhdfs/v1
#   Hive Metastore      - thrift://hive-metastore:9083 (host: 127.0.0.1:9083)
#   HiveServer2 (JDBC)  - jdbc:hive2://hiveserver2:10000 (host: 127.0.0.1:10000)
#
# Environment:
#   WEBHDFS_ADDR    - WebHDFS base URL (default: http://127.0.0.1:9870)
#   METASTORE_PORT  - Hive Metastore Thrift port on host (default: 9083)
# =============================================================================

set -euo pipefail

WEBHDFS_ADDR="${WEBHDFS_ADDR:-http://127.0.0.1:9870}"
METASTORE_PORT="${METASTORE_PORT:-9083}"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
HIVESERVER2_CONTAINER="${HIVESERVER2_CONTAINER:-hiveserver2}"
VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== Hive + HDFS Setup ==="
echo "WebHDFS address:  ${WEBHDFS_ADDR}"
echo "Metastore port:   ${METASTORE_PORT}"

# ---------------------------------------------------------------------------
# Wait for HDFS NameNode (WebHDFS) to be ready.
# ---------------------------------------------------------------------------
echo "Waiting for HDFS NameNode (WebHDFS) to be ready..."
for i in $(seq 1 60); do
  if curl -sf "${WEBHDFS_ADDR}/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1; then
    echo "HDFS NameNode is ready!"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: HDFS NameNode not ready after 60 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for HDFS NameNode..."
  sleep 3
done

# ---------------------------------------------------------------------------
# Wait for the Hive Metastore Thrift port to be open.
# ---------------------------------------------------------------------------
echo "Waiting for Hive Metastore to be ready..."
for i in $(seq 1 60); do
  if nc -z -w 3 127.0.0.1 "${METASTORE_PORT}" > /dev/null 2>&1; then
    echo "Hive Metastore port is open!"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: Hive Metastore not ready after 60 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for Hive Metastore..."
  sleep 3
done

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking HDFS + Metastore connectivity..."
  curl -sf "${WEBHDFS_ADDR}/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1
  nc -z -w 3 127.0.0.1 "${METASTORE_PORT}" > /dev/null 2>&1
  echo "Hive + HDFS are reachable."
  exit 0
fi

# ---------------------------------------------------------------------------
# Create the Hive warehouse directory in HDFS (idempotent via WebHDFS).
# WebHDFS MKDIRS returns {"boolean":true} and is a no-op if it already exists.
# ---------------------------------------------------------------------------
create_hdfs_dir() {
  local path="$1"
  local perm="${2:-1777}"
  echo "Creating HDFS directory: ${path} (perm ${perm})"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
    "${WEBHDFS_ADDR}/webhdfs/v1${path}?op=MKDIRS&permission=${perm}&user.name=hive" 2>/dev/null || echo "000")"
  case "$code" in
    200) echo "  Directory '${path}' ready (HTTP ${code})" ;;
    *)   echo "  WARNING: Could not create '${path}' (HTTP ${code})" ;;
  esac
}

create_hdfs_dir "/user/hive/warehouse" 1777
create_hdfs_dir "/data-lake" 1777
create_hdfs_dir "/tmp" 1777

# ---------------------------------------------------------------------------
# Create a sample Hive database + table via HiveServer2 (beeline) so PXF
# `hive:*` profiles have something to read. Best-effort: skipped in CI (no
# docker) and never fails the script (HiveServer2 may still be warming up).
# ---------------------------------------------------------------------------
seed_hive_table() {
  if [ "$CI_MODE" = true ]; then
    echo ""
    echo "  CI mode: skipping HiveServer2 sample table seeding"
    return 0
  fi
  if ! command -v docker > /dev/null 2>&1; then
    echo "  docker not found; skipping HiveServer2 sample table seeding"
    return 0
  fi

  echo ""
  echo "=== Seeding sample Hive schema via HiveServer2 (beeline) ==="

  # Wait briefly for HiveServer2 JDBC (:10000) to accept connections.
  for i in $(seq 1 20); do
    if nc -z -w 3 127.0.0.1 10000 > /dev/null 2>&1; then break; fi
    sleep 3
  done

  local hql
  hql=$(cat <<'SQL'
CREATE DATABASE IF NOT EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
  id        BIGINT,
  product   STRING,
  amount    DOUBLE,
  sale_date STRING
) STORED AS ORC;
INSERT INTO warehouse.fact_sales VALUES
  (1, 'widget', 19.99, '2026-01-01'),
  (2, 'gadget', 49.50, '2026-01-02'),
  (3, 'gizmo',  12.25, '2026-01-03');
SHOW TABLES IN warehouse;
SQL
)

  if docker exec -i "${HIVESERVER2_CONTAINER}" \
      beeline -u "jdbc:hive2://localhost:10000" -e "${hql}" > /dev/null 2>&1; then
    echo "  Sample table warehouse.fact_sales created (ORC)"
  else
    echo "  WARNING: Could not seed warehouse.fact_sales (HiveServer2 may still be starting)"
  fi
}

seed_hive_table

echo ""
echo "=== Hive + HDFS Setup Complete ==="
echo "HDFS warehouse:   hdfs://namenode:8020/user/hive/warehouse"
echo "HDFS data lake:   hdfs://namenode:8020/data-lake"
echo "Metastore:        thrift://hive-metastore:9083"
echo "HiveServer2 JDBC: jdbc:hive2://127.0.0.1:10000"
echo "NameNode UI:      ${WEBHDFS_ADDR}"
