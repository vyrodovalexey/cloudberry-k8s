#!/usr/bin/env bash
# =============================================================================
# Phase 7d - Data-load acceptance against a live Cloudberry cluster.
#
# Creates database `mydb`, bulk-loads ~100MB of data across three MPP tables
# (customers / orders / line_items) with indexes, runs ANALYZE, then executes
# verification SELECTs (row counts, join+aggregation, EXPLAIN, segment skew).
#
# Connection method mirrors test/e2e/scripts/scenario71-backup-restore.sh:
#   kubectl exec into the coordinator pod, source greenplum_path.sh, run psql
#   as gpadmin over the local socket (SSL/sslmode not needed for local socket).
#
# All configuration is via ENV (no hardcoded namespace / cluster / password):
#   NAMESPACE          (default cloudberry-test)
#   CLUSTER            (default acceptance-test)
#   COORD_POD          (default ${CLUSTER}-coordinator-0)
#   DB_CONTAINER       (default cloudberry; auto-detected if unset)
#   DB_PW_SECRET       (default ${CLUSTER}-admin-password)
#   DB                 (default mydb)
#   KUBECTL            (default kubectl)
# =============================================================================
set -euo pipefail

NAMESPACE="${NAMESPACE:-cloudberry-test}"
CLUSTER="${CLUSTER:-acceptance-test}"
COORD_POD="${COORD_POD:-${CLUSTER}-coordinator-0}"
DB_PW_SECRET="${DB_PW_SECRET:-${CLUSTER}-admin-password}"
DB="${DB:-mydb}"
KUBECTL="${KUBECTL:-kubectl}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASES_DIR="${CASES_DIR:-${SCRIPT_DIR}/../../cases}"
DDL_SQL="${CASES_DIR}/phase7d_dataload_mydb.sql"
VERIFY_SQL="${CASES_DIR}/phase7d_dataload_verify.sql"

KN=("${KUBECTL}" -n "${NAMESPACE}")

log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

# --- Resolve password + container name (ENV-driven, never hardcoded) ---------
DB_PASSWORD="$("${KN[@]}" get secret "${DB_PW_SECRET}" \
  -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
[ -n "${DB_PASSWORD}" ] || die "could not read password from Secret ${DB_PW_SECRET}"

if [ -z "${DB_CONTAINER:-}" ]; then
  DB_CONTAINER="$("${KN[@]}" get pod "${COORD_POD}" \
    -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo cloudberry)"
fi
DB_CONTAINER="${DB_CONTAINER:-cloudberry}"
log "coordinator pod=${COORD_POD} container=${DB_CONTAINER} db=${DB}"

# --- psql helpers ------------------------------------------------------------
# coord_psql <database> <sql> [extra psql args...]
coord_psql() {
  local database="$1"; shift
  local sql="$1"; shift
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" "${@:4}" -c "$3"
    ' _ "${DB_PASSWORD}" "${database}" "${sql}" "$@"
}

# coord_psql_file <database> -- streams a .sql file into psql in the pod.
coord_psql_file() {
  local database="$1"; shift
  local file="$1"; shift
  "${KN[@]}" exec -i "${COORD_POD}" -c "${DB_CONTAINER}" -- \
    bash -lc '
      if [ -n "${GPHOME:-}" ] && [ -f "${GPHOME}/greenplum_path.sh" ]; then
        . "${GPHOME}/greenplum_path.sh"
      elif [ -f /usr/local/cloudberry-db/greenplum_path.sh ]; then
        . /usr/local/cloudberry-db/greenplum_path.sh
      fi
      export PGPASSWORD="$1"
      exec psql -v ON_ERROR_STOP=1 -U gpadmin -d "$2" -f -
    ' _ "${DB_PASSWORD}" "${database}" < "${file}"
}

# --- Step 1: (re)create database mydb ----------------------------------------
log "Creating database ${DB} (drop if exists)..."
coord_psql postgres \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}' AND pid<>pg_backend_pid();" \
  -At >/dev/null || true
coord_psql postgres "DROP DATABASE IF EXISTS ${DB};" -At >/dev/null
coord_psql postgres "CREATE DATABASE ${DB};" -At >/dev/null
log "Database ${DB} created."

# --- Step 2: DDL + bulk load + indexes + ANALYZE -----------------------------
log "Loading schema + data from ${DDL_SQL} (this takes a minute)..."
coord_psql_file "${DB}" "${DDL_SQL}"
log "Data load complete."

# --- Step 3: verification SELECTs --------------------------------------------
log "Running verification SELECTs from ${VERIFY_SQL}..."
coord_psql_file "${DB}" "${VERIFY_SQL}"

log "Phase 7d data-load acceptance finished OK."
