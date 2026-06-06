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
#   K8S_NAMESPACE    - k8s namespace for the backup Secret + minio Service
#                      (default: cloudberry-test)
#   MINIO_K8S_HOST   - host the in-cluster `minio` Service points at
#                      (default: host.docker.internal)
# =============================================================================

set -euo pipefail

MINIO_ADDR="${MINIO_ADDR:-http://127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
K8S_NAMESPACE="${K8S_NAMESPACE:-cloudberry-test}"
MINIO_K8S_HOST="${MINIO_K8S_HOST:-host.docker.internal}"
MINIO_K8S_PORT="${MINIO_K8S_PORT:-9000}"
BACKUP_S3_SECRET_NAME="${BACKUP_S3_SECRET_NAME:-backup-s3-credentials}"
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

# ---------------------------------------------------------------------------
# Create the in-cluster k8s artifacts required by Scenario 71 backups:
#   1. Secret  backup-s3-credentials  (S3 access/secret keys for the Secret variant)
#   2. Service minio (ExternalName -> host MinIO) so the CR's
#      endpoint http://minio:9000 resolves from in-cluster backup/restore Jobs.
# Both are idempotent (apply with --dry-run=client | kubectl apply -f -) and are
# only attempted when kubectl is available, so docker-compose-only runs (CI mode
# or hosts without a cluster) do not fail.
# ---------------------------------------------------------------------------
setup_k8s_backup_artifacts() {
  if ! command -v kubectl > /dev/null 2>&1; then
    echo "  kubectl not found; skipping k8s backup-s3-credentials Secret + minio Service"
    return 0
  fi

  echo ""
  echo "=== Creating k8s backup artifacts in namespace '${K8S_NAMESPACE}' ==="

  # Ensure the namespace exists (idempotent).
  kubectl create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null 2>&1 || true

  # 1. Secret with S3 credentials (keys match the CR sample + Go test fixtures).
  echo "Creating Secret '${BACKUP_S3_SECRET_NAME}'..."
  kubectl create secret generic "${BACKUP_S3_SECRET_NAME}" \
    --namespace "${K8S_NAMESPACE}" \
    --from-literal=aws_access_key_id="${MINIO_ACCESS_KEY}" \
    --from-literal=aws_secret_access_key="${MINIO_SECRET_KEY}" \
    --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null
  echo "  Secret '${BACKUP_S3_SECRET_NAME}' ready in '${K8S_NAMESPACE}'"

  # 2. ExternalName Service 'minio' -> host MinIO. NOTE: ExternalName does NOT
  #    remap ports, so the host must expose MinIO on ${MINIO_K8S_PORT}
  #    (docker-compose maps 9000:9000). The Service makes the CR's unchanged
  #    endpoint http://minio:9000 resolvable inside the cluster.
  echo "Creating ExternalName Service 'minio' -> ${MINIO_K8S_HOST}:${MINIO_K8S_PORT}..."
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${K8S_NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    app.kubernetes.io/component: backup-s3-endpoint
spec:
  type: ExternalName
  externalName: ${MINIO_K8S_HOST}
  ports:
    - name: s3
      port: ${MINIO_K8S_PORT}
      targetPort: ${MINIO_K8S_PORT}
      protocol: TCP
EOF
  echo "  Service 'minio' ready in '${K8S_NAMESPACE}'"
}

setup_k8s_backup_artifacts

echo ""
echo "=== MinIO Setup Complete ==="
echo "Buckets: cloudberry-backups, cloudberry-data"
echo "Console: ${MINIO_ADDR/9000/9001}"
