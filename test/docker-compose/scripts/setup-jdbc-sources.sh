#!/usr/bin/env bash
# =============================================================================
# JDBC Sources Setup Script
# Seeds MySQL (mysql-oltp) and PostgreSQL (postgres-source) databases with
# sample tables for PXF JDBC external-table data-loading tests (Scenario 93).
#
# Usage:
#   bash setup-jdbc-sources.sh [--verify] [--ci]
#
# The script:
#   1. Waits for MySQL (3306) and PostgreSQL pgsource (5432) to be ready.
#   2. Creates a sample table (jdbc_test_data) in both databases with an index
#      and seeds a configurable number of rows (default: 10 000 for fast setup;
#      the deploy phase can bulk-load ~100 MB later).
#   3. Creates k8s Secrets for JDBC credentials in the cloudberry-test namespace
#      (mysql-credentials, pg-source-credentials) — skipped in --ci mode.
#   4. Creates ExternalName Services so jdbc://mysql:3306 and
#      jdbc://pgsource:5432 resolve from in-cluster pods.
#
# Environment:
#   MYSQL_HOST       - MySQL host (default: 127.0.0.1)
#   MYSQL_PORT       - MySQL port (default: 3306)
#   MYSQL_ROOT_PASS  - MySQL root password (default: rootpass)
#   MYSQL_DB         - MySQL database (default: oltp)
#   MYSQL_USER       - MySQL user (default: pxfuser)
#   MYSQL_PASS       - MySQL password (default: pxfpass)
#   PG_HOST          - PostgreSQL host (default: 127.0.0.1)
#   PG_PORT          - PostgreSQL port (default: 5432)
#   PG_DB            - PostgreSQL database (default: sourcedb)
#   PG_USER          - PostgreSQL user (default: pxfuser)
#   PG_PASS          - PostgreSQL password (default: pxfpass)
#   SEED_ROWS        - Number of rows to seed (default: 10000)
#   K8S_NAMESPACE    - k8s namespace (default: cloudberry-test)
#   K8S_EXT_HOST     - ExternalName target (default: host.docker.internal)
# =============================================================================

set -euo pipefail

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_ROOT_PASS="${MYSQL_ROOT_PASS:-rootpass}"
MYSQL_DB="${MYSQL_DB:-oltp}"
MYSQL_USER="${MYSQL_USER:-pxfuser}"
MYSQL_PASS="${MYSQL_PASS:-pxfpass}"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-sourcedb}"
PG_USER="${PG_USER:-pxfuser}"
PG_PASS="${PG_PASS:-pxfpass}"

SEED_ROWS="${SEED_ROWS:-10000}"
K8S_NAMESPACE="${K8S_NAMESPACE:-cloudberry-test}"
K8S_EXT_HOST="${K8S_EXT_HOST:-host.docker.internal}"

VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== JDBC Sources Setup ==="
echo "MySQL:      ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB} (user: ${MYSQL_USER})"
echo "PostgreSQL: ${PG_HOST}:${PG_PORT}/${PG_DB} (user: ${PG_USER})"
echo "Seed rows:  ${SEED_ROWS}"

# ---------------------------------------------------------------------------
# Wait for MySQL
# ---------------------------------------------------------------------------
wait_for_mysql() {
  echo ""
  echo "Waiting for MySQL to be ready..."
  for i in $(seq 1 60); do
    if docker exec mysql mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PASS}" --silent 2>/dev/null; then
      echo "MySQL is ready!"
      return 0
    fi
    if [ "$i" -eq 60 ]; then
      echo "ERROR: MySQL not ready after 60 attempts"
      exit 1
    fi
    echo "Attempt $i: Waiting for MySQL..."
    sleep 3
  done
}

# ---------------------------------------------------------------------------
# Wait for PostgreSQL (pgsource)
# ---------------------------------------------------------------------------
wait_for_pgsource() {
  echo ""
  echo "Waiting for PostgreSQL (pgsource) to be ready..."
  for i in $(seq 1 30); do
    if docker exec pgsource pg_isready -U "${PG_USER}" -d "${PG_DB}" > /dev/null 2>&1; then
      echo "PostgreSQL (pgsource) is ready!"
      return 0
    fi
    if [ "$i" -eq 30 ]; then
      echo "ERROR: PostgreSQL (pgsource) not ready after 30 attempts"
      exit 1
    fi
    echo "Attempt $i: Waiting for PostgreSQL (pgsource)..."
    sleep 2
  done
}

# ---------------------------------------------------------------------------
# Seed MySQL
# ---------------------------------------------------------------------------
seed_mysql() {
  echo ""
  echo "Seeding MySQL database '${MYSQL_DB}'..."

  docker exec -i mysql mysql -uroot -p"${MYSQL_ROOT_PASS}" "${MYSQL_DB}" <<'EOSQL'
CREATE TABLE IF NOT EXISTS jdbc_test_data (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  value      DECIMAL(12,2) NOT NULL,
  category   VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  payload    VARCHAR(512) DEFAULT NULL,
  INDEX idx_category (category),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOSQL

  # Check if data already exists (idempotent).
  local existing
  existing=$(docker exec mysql mysql -uroot -p"${MYSQL_ROOT_PASS}" -N -e \
    "SELECT COUNT(*) FROM ${MYSQL_DB}.jdbc_test_data;" 2>/dev/null || echo "0")

  if [ "${existing}" -ge "${SEED_ROWS}" ]; then
    echo "  Table jdbc_test_data already has ${existing} rows (>= ${SEED_ROWS}), skipping seed."
    return 0
  fi

  echo "  Inserting ${SEED_ROWS} rows into jdbc_test_data..."
  # Batch-insert using a stored procedure for speed.
  docker exec -i mysql mysql -uroot -p"${MYSQL_ROOT_PASS}" "${MYSQL_DB}" <<EOSQL
DELIMITER //
DROP PROCEDURE IF EXISTS seed_jdbc_test_data //
CREATE PROCEDURE seed_jdbc_test_data(IN total INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE batch INT DEFAULT 1000;
  DECLARE remaining INT;
  SET remaining = total;
  WHILE remaining > 0 DO
    SET batch = LEAST(remaining, 1000);
    SET i = 0;
    START TRANSACTION;
    WHILE i < batch DO
      INSERT INTO jdbc_test_data (name, value, category, payload)
      VALUES (
        CONCAT('record_', FLOOR(RAND() * 1000000)),
        ROUND(RAND() * 10000, 2),
        ELT(1 + FLOOR(RAND() * 5), 'electronics', 'clothing', 'food', 'books', 'tools'),
        REPEAT('x', 200 + FLOOR(RAND() * 100))
      );
      SET i = i + 1;
    END WHILE;
    COMMIT;
    SET remaining = remaining - batch;
  END WHILE;
END //
DELIMITER ;
CALL seed_jdbc_test_data(${SEED_ROWS});
DROP PROCEDURE IF EXISTS seed_jdbc_test_data;
EOSQL

  local count
  count=$(docker exec mysql mysql -uroot -p"${MYSQL_ROOT_PASS}" -N -e \
    "SELECT COUNT(*) FROM ${MYSQL_DB}.jdbc_test_data;" 2>/dev/null || echo "?")
  echo "  MySQL jdbc_test_data: ${count} rows"
}

# ---------------------------------------------------------------------------
# Seed PostgreSQL (pgsource)
# ---------------------------------------------------------------------------
seed_pgsource() {
  echo ""
  echo "Seeding PostgreSQL database '${PG_DB}'..."

  docker exec -i pgsource psql -U "${PG_USER}" -d "${PG_DB}" <<'EOSQL'
CREATE TABLE IF NOT EXISTS jdbc_test_data (
  id         SERIAL PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  value      NUMERIC(12,2) NOT NULL,
  category   VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  payload    VARCHAR(512) DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_category ON jdbc_test_data (category);
CREATE INDEX IF NOT EXISTS idx_created_at ON jdbc_test_data (created_at);
EOSQL

  # Check if data already exists (idempotent).
  local existing
  existing=$(docker exec pgsource psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c \
    "SELECT COUNT(*) FROM jdbc_test_data;" 2>/dev/null || echo "0")

  if [ "${existing}" -ge "${SEED_ROWS}" ]; then
    echo "  Table jdbc_test_data already has ${existing} rows (>= ${SEED_ROWS}), skipping seed."
    return 0
  fi

  echo "  Inserting ${SEED_ROWS} rows into jdbc_test_data..."
  docker exec -i pgsource psql -U "${PG_USER}" -d "${PG_DB}" <<EOSQL
INSERT INTO jdbc_test_data (name, value, category, payload)
SELECT
  'record_' || (random() * 1000000)::int,
  round((random() * 10000)::numeric, 2),
  (ARRAY['electronics', 'clothing', 'food', 'books', 'tools'])[1 + floor(random() * 5)::int],
  repeat('x', 200 + floor(random() * 100)::int)
FROM generate_series(1, ${SEED_ROWS});
EOSQL

  local count
  count=$(docker exec pgsource psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c \
    "SELECT COUNT(*) FROM jdbc_test_data;" 2>/dev/null || echo "?")
  echo "  PostgreSQL jdbc_test_data: ${count} rows"
}

# ---------------------------------------------------------------------------
# Create k8s artifacts (Secrets + ExternalName Services)
# ---------------------------------------------------------------------------
setup_k8s_jdbc_artifacts() {
  if [ "$CI_MODE" = true ]; then
    echo ""
    echo "  CI mode: skipping k8s JDBC Secrets + ExternalName Services"
    return 0
  fi

  if ! command -v kubectl > /dev/null 2>&1; then
    echo "  kubectl not found; skipping k8s JDBC Secrets + ExternalName Services"
    return 0
  fi

  if ! kubectl cluster-info > /dev/null 2>&1; then
    echo "  No reachable Kubernetes cluster; skipping k8s JDBC Secrets + ExternalName Services"
    return 0
  fi

  echo ""
  echo "=== Creating k8s JDBC artifacts in namespace '${K8S_NAMESPACE}' ==="

  # Ensure the namespace exists (idempotent).
  kubectl create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null 2>&1 || true

  # 1. Secret: mysql-credentials
  echo "Creating Secret 'mysql-credentials'..."
  kubectl create secret generic mysql-credentials \
    --namespace "${K8S_NAMESPACE}" \
    --from-literal=username="${MYSQL_USER}" \
    --from-literal=password="${MYSQL_PASS}" \
    --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null
  echo "  Secret 'mysql-credentials' ready in '${K8S_NAMESPACE}'"

  # 2. Secret: pg-source-credentials
  echo "Creating Secret 'pg-source-credentials'..."
  kubectl create secret generic pg-source-credentials \
    --namespace "${K8S_NAMESPACE}" \
    --from-literal=username="${PG_USER}" \
    --from-literal=password="${PG_PASS}" \
    --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null
  echo "  Secret 'pg-source-credentials' ready in '${K8S_NAMESPACE}'"

  # 3. ExternalName Service: mysql -> host.docker.internal
  echo "Creating ExternalName Service 'mysql' -> ${K8S_EXT_HOST}:${MYSQL_PORT}..."
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: ${K8S_NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    app.kubernetes.io/component: jdbc-source
spec:
  type: ExternalName
  externalName: ${K8S_EXT_HOST}
  ports:
    - name: mysql
      port: ${MYSQL_PORT}
      targetPort: ${MYSQL_PORT}
      protocol: TCP
EOF
  echo "  Service 'mysql' ready in '${K8S_NAMESPACE}'"

  # 4. ExternalName Service: pgsource -> host.docker.internal
  echo "Creating ExternalName Service 'pgsource' -> ${K8S_EXT_HOST}:${PG_PORT}..."
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: pgsource
  namespace: ${K8S_NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    app.kubernetes.io/component: jdbc-source
spec:
  type: ExternalName
  externalName: ${K8S_EXT_HOST}
  ports:
    - name: postgresql
      port: ${PG_PORT}
      targetPort: ${PG_PORT}
      protocol: TCP
EOF
  echo "  Service 'pgsource' ready in '${K8S_NAMESPACE}'"
}

# ---------------------------------------------------------------------------
# Verify mode
# ---------------------------------------------------------------------------
verify() {
  echo ""
  echo "=== Verification ==="
  local errors=0

  # MySQL connectivity
  echo "Checking MySQL..."
  if docker exec mysql mysqladmin ping -h 127.0.0.1 -uroot -p"${MYSQL_ROOT_PASS}" --silent 2>/dev/null; then
    echo "  MySQL is healthy"
  else
    echo "  ERROR: MySQL is not reachable"
    errors=$((errors + 1))
  fi

  # MySQL table
  local mysql_count
  mysql_count=$(docker exec mysql mysql -uroot -p"${MYSQL_ROOT_PASS}" -N -e \
    "SELECT COUNT(*) FROM ${MYSQL_DB}.jdbc_test_data;" 2>/dev/null || echo "MISSING")
  echo "  MySQL jdbc_test_data rows: ${mysql_count}"
  if [ "${mysql_count}" = "MISSING" ]; then
    errors=$((errors + 1))
  fi

  # PostgreSQL connectivity
  echo "Checking PostgreSQL (pgsource)..."
  if docker exec pgsource pg_isready -U "${PG_USER}" -d "${PG_DB}" > /dev/null 2>&1; then
    echo "  PostgreSQL (pgsource) is healthy"
  else
    echo "  ERROR: PostgreSQL (pgsource) is not reachable"
    errors=$((errors + 1))
  fi

  # PostgreSQL table
  local pg_count
  pg_count=$(docker exec pgsource psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c \
    "SELECT COUNT(*) FROM jdbc_test_data;" 2>/dev/null || echo "MISSING")
  echo "  PostgreSQL jdbc_test_data rows: ${pg_count}"
  if [ "${pg_count}" = "MISSING" ]; then
    errors=$((errors + 1))
  fi

  if [ $errors -gt 0 ]; then
    echo ""
    echo "Verification FAILED with ${errors} error(s)"
    return 1
  fi

  echo ""
  echo "All JDBC source verifications PASSED"
  return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
wait_for_mysql
wait_for_pgsource

if [ "$VERIFY_ONLY" = true ]; then
  verify
  exit $?
fi

seed_mysql
seed_pgsource
setup_k8s_jdbc_artifacts

echo ""
echo "=== JDBC Sources Setup Complete ==="
echo "MySQL:      ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB} (table: jdbc_test_data)"
echo "PostgreSQL: ${PG_HOST}:${PG_PORT}/${PG_DB} (table: jdbc_test_data)"
