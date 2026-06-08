#!/usr/bin/env bash
# =============================================================================
# MinIO Setup Script
# Creates test buckets for backup and data loading testing.
#
# Usage:
#   bash setup-minio.sh [--verify] [--ci]
#
# Buckets are created directly against the MinIO S3 API using curl with an
# AWS Signature V4 PUT /<bucket> request (no `mc` client required).
#
# Environment:
#   MINIO_ADDR       - MinIO address (default: http://127.0.0.1:9000)
#   MINIO_ACCESS_KEY - MinIO access key (default: minioadmin)
#   MINIO_SECRET_KEY - MinIO secret key (default: minioadmin)
#   MINIO_REGION     - S3 region used for request signing (default: us-east-1)
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

# Create buckets using curl against the MinIO S3 API (AWS Signature V4).
#
# A bucket is created with `PUT /<bucket>` on the S3 endpoint. The request must
# be signed with SigV4; we compute the signature with openssl. MinIO returns
# 200 on create and 409 (BucketAlreadyOwnedByYou) if it already exists — both
# are treated as success (idempotent).

# hmac_sha256 <hex-key|str:...> <data> -> hex.  Pass the key as a hex string;
# the helper below builds the signing key step by step.
_hmac_sha256_hex() { # $1=hexkey $2=data -> hex digest
  printf '%s' "$2" \
    | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" \
    | sed 's/^.*= //'
}

_sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 | sed 's/^.*= //'; }

create_bucket() {
  local bucket="$1"
  echo "Creating bucket: ${bucket}"

  local host port scheme
  scheme="${MINIO_ADDR%%://*}"
  local hostport="${MINIO_ADDR#*://}"
  hostport="${hostport%%/*}"
  host="${hostport%%:*}"
  port="${hostport##*:}"
  [ "$port" = "$host" ] && port=9000   # no explicit port in MINIO_ADDR

  local region="${MINIO_REGION:-us-east-1}"
  local service="s3"
  local amzdate datestamp
  amzdate="$(date -u +%Y%m%dT%H%M%SZ)"
  datestamp="$(date -u +%Y%m%d)"

  # Canonical request for `PUT /<bucket>` with an empty body.
  local method="PUT"
  local canonical_uri="/${bucket}"
  local canonical_querystring=""
  local payload_hash
  payload_hash="$(_sha256_hex "")"
  local canonical_headers="host:${host}:${port}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${amzdate}\n"
  local signed_headers="host;x-amz-content-sha256;x-amz-date"
  local canonical_request
  canonical_request="$(printf '%s\n%s\n%s\n%b\n%s\n%s' \
    "$method" "$canonical_uri" "$canonical_querystring" \
    "$canonical_headers" "$signed_headers" "$payload_hash")"

  # String to sign.
  local algorithm="AWS4-HMAC-SHA256"
  local credential_scope="${datestamp}/${region}/${service}/aws4_request"
  local hashed_canonical_request
  hashed_canonical_request="$(_sha256_hex "$canonical_request")"
  local string_to_sign
  string_to_sign="$(printf '%s\n%s\n%s\n%s' \
    "$algorithm" "$amzdate" "$credential_scope" "$hashed_canonical_request")"

  # Derive the SigV4 signing key (HMAC chain), seeded with "AWS4"+secret as a
  # literal-key HMAC, then continuing with hex keys.
  local k_secret_hex k_date k_region k_service k_signing signature
  k_secret_hex="$(printf 'AWS4%s' "${MINIO_SECRET_KEY}" | xxd -p -c 256 | tr -d '\n')"
  k_date="$(_hmac_sha256_hex "$k_secret_hex" "$datestamp")"
  k_region="$(_hmac_sha256_hex "$k_date" "$region")"
  k_service="$(_hmac_sha256_hex "$k_region" "$service")"
  k_signing="$(_hmac_sha256_hex "$k_service" "aws4_request")"
  signature="$(_hmac_sha256_hex "$k_signing" "$string_to_sign")"

  local authorization="${algorithm} Credential=${MINIO_ACCESS_KEY}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}"

  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -X "$method" "${scheme}://${host}:${port}/${bucket}" \
    -H "Host: ${host}:${port}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amzdate}" \
    -H "Authorization: ${authorization}" 2>/dev/null || echo "000")"

  case "$code" in
    200|409)
      echo "  Bucket '${bucket}' ready (via curl, HTTP ${code})"
      return 0
      ;;
    *)
      echo "  WARNING: Could not create bucket '${bucket}' (HTTP ${code})"
      return 0
      ;;
  esac
}

create_bucket "cloudberry-backups"
create_bucket "cloudberry-data"

# ---------------------------------------------------------------------------
# Create the in-cluster k8s artifacts required by Scenario 71 backups:
#   1. Secret  backup-s3-credentials  (S3 access/secret keys for the Secret variant)
#   2. Service minio (ExternalName -> host MinIO) so the CR's
#      endpoint http://minio:9000 resolves from in-cluster backup/restore Jobs.
# Both are idempotent (apply with --dry-run=client | kubectl apply -f -) and are
# only attempted when:
#   - not running in --ci mode (CI has no k8s cluster), AND
#   - kubectl is installed, AND
#   - a Kubernetes API server is actually reachable.
# In GitHub CI the kubectl binary exists but no cluster is reachable
# (localhost:8080 refused), so a `command -v kubectl` check alone is not enough;
# we additionally probe the API server and skip gracefully when absent.
# ---------------------------------------------------------------------------
setup_k8s_backup_artifacts() {
  if [ "$CI_MODE" = true ]; then
    echo ""
    echo "  CI mode: skipping k8s backup-s3-credentials Secret + minio Service"
    return 0
  fi

  if ! command -v kubectl > /dev/null 2>&1; then
    echo "  kubectl not found; skipping k8s backup-s3-credentials Secret + minio Service"
    return 0
  fi

  # The kubectl binary may exist without a reachable cluster (e.g. GitHub CI).
  # Probe the API server; skip the k8s artifacts when it is unreachable.
  if ! kubectl cluster-info > /dev/null 2>&1; then
    echo "  No reachable Kubernetes cluster; skipping k8s backup-s3-credentials Secret + minio Service"
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
