#!/usr/bin/env bash
# =============================================================================
# MinIO Setup Script
# Creates test buckets for backup and data loading testing.
#
# Usage:
#   bash setup-minio.sh [--verify] [--ci]
#
# Environment:
#   MINIO_ADDR       - MinIO address (default: http://127.0.0.1:9000)
#   MINIO_ACCESS_KEY - MinIO access key (default: minioadmin)
#   MINIO_SECRET_KEY - MinIO secret key (default: minioadmin)
# =============================================================================

set -euo pipefail

MINIO_ADDR="${MINIO_ADDR:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== MinIO Setup ==="
echo "MinIO address: ${MINIO_ADDR}"

# Wait for MinIO to be ready.
echo "Waiting for MinIO to be ready..."
for i in $(seq 1 30); do
  if curl -sf "${MINIO_ADDR}/minio/health/live" > /dev/null 2>&1; then
    echo "MinIO is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: MinIO not ready after 30 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for MinIO..."
  sleep 2
done

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking MinIO health..."
  curl -sf "${MINIO_ADDR}/minio/health/live" > /dev/null 2>&1
  echo "MinIO is healthy."
  exit 0
fi

# Create buckets using mc (MinIO Client) via docker exec.
create_bucket() {
  local bucket="$1"
  echo "Creating bucket: ${bucket}"

  # Try docker exec with mc first (works in docker-compose mode).
  if docker exec minio mc alias set local http://localhost:9000 \
    "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" > /dev/null 2>&1; then
    if docker exec minio mc mb "local/${bucket}" --ignore-existing 2>/dev/null; then
      echo "  Bucket '${bucket}' ready (via mc)"
      return 0
    fi
  fi

  # Fallback: use mc binary if available on host.
  if command -v mc > /dev/null 2>&1; then
    mc alias set testminio "${MINIO_ADDR}" \
      "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" > /dev/null 2>&1 || true
    if mc mb "testminio/${bucket}" --ignore-existing 2>/dev/null; then
      echo "  Bucket '${bucket}' ready (via host mc)"
      return 0
    fi
  fi

  echo "  WARNING: Could not create bucket '${bucket}' (mc not available)"
}

create_bucket "cloudberry-backups"
create_bucket "cloudberry-data"

echo ""
echo "=== MinIO Setup Complete ==="
echo "Buckets: cloudberry-backups, cloudberry-data"
echo "Console: ${MINIO_ADDR/9000/9001}"
