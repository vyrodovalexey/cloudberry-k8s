# =============================================================================
# Cloudberry-Compatible PostgreSQL Image
# =============================================================================
# PostgreSQL 16 with Cloudberry/Greenplum catalog simulation.
# Provides gp_segment_configuration, gp_request_fts_probe_scan(),
# gp_toolkit schema, and resource group tables for operator testing.
# =============================================================================

FROM postgres:16

# OCI labels
LABEL org.opencontainers.image.title="cloudberry-db-sim" \
      org.opencontainers.image.description="PostgreSQL 16 with Cloudberry catalog simulation for operator testing" \
      org.opencontainers.image.vendor="Cloudberry Contributors" \
      org.opencontainers.image.source="https://github.com/cloudberry-contrib/cloudberry-k8s" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="7.1.0-sim"

# Install only runtime dependencies (sorted alphanumerically)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy initialization SQL (executed automatically by postgres entrypoint)
COPY init-cloudberry-catalog.sql /docker-entrypoint-initdb.d/01-cloudberry-catalog.sql

# Copy custom entrypoint wrapper
COPY docker-entrypoint-cloudberry.sh /usr/local/bin/docker-entrypoint-cloudberry.sh
RUN chmod +x /usr/local/bin/docker-entrypoint-cloudberry.sh

# Default environment for the operator compatibility
ENV POSTGRES_USER=gpadmin \
    POSTGRES_DB=postgres

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U "${POSTGRES_USER:-gpadmin}" -d "${POSTGRES_DB:-postgres}" || exit 1

EXPOSE 5432

# Use the custom entrypoint that wraps the standard one
ENTRYPOINT ["docker-entrypoint-cloudberry.sh"]
CMD ["postgres"]
