#!/bin/bash
# =============================================================================
# Cloudberry-compatible PostgreSQL entrypoint
# =============================================================================
# Wraps the standard PostgreSQL entrypoint with Cloudberry-specific
# initialization: catalog tables, FTS functions, and segment configuration.
# =============================================================================
set -e

# ---------------------------------------------------------------------------
# Environment variables (with defaults)
# ---------------------------------------------------------------------------
export POSTGRES_USER="${POSTGRES_USER:-gpadmin}"
export POSTGRES_DB="${POSTGRES_DB:-postgres}"
# POSTGRES_PASSWORD is expected from the operator secret

# ---------------------------------------------------------------------------
# If PGDATA is set by the operator, respect it
# ---------------------------------------------------------------------------
if [ -n "$PGDATA" ]; then
    echo "cloudberry-entrypoint: PGDATA=$PGDATA"
fi

# ---------------------------------------------------------------------------
# Execute the standard PostgreSQL entrypoint
# ---------------------------------------------------------------------------
exec docker-entrypoint.sh "$@"
