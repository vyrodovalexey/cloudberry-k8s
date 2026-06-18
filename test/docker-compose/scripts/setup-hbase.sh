#!/usr/bin/env bash
# =============================================================================
# HBase Setup Script
# Creates a sample HBase table with rows for PXF hbase:* external-table tests.
#
# Usage:
#   bash setup-hbase.sh [--verify] [--ci]
#
# Environment:
#   HBASE_CONTAINER  - HBase container name (default: hbase)
#   K8S_NAMESPACE    - k8s namespace (default: cloudberry-test)
#   K8S_EXT_HOST     - ExternalName target (default: host.docker.internal)
# =============================================================================

set -euo pipefail

HBASE_CONTAINER="${HBASE_CONTAINER:-hbase}"
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

echo "=== HBase Setup ==="
echo "HBase container: ${HBASE_CONTAINER}"

# Wait for HBase (ZooKeeper) to be ready.
echo "Waiting for HBase ZooKeeper to be ready..."
for i in $(seq 1 60); do
  if echo "ruok" | nc -w 3 127.0.0.1 2181 2>/dev/null | grep -q "imok"; then
    echo "HBase ZooKeeper is ready!"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: HBase ZooKeeper not ready after 60 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for HBase ZooKeeper..."
  sleep 3
done

# Wait for HBase Master to be ready (port 16010).
echo "Waiting for HBase Master to be ready..."
for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:16010/status/cluster" > /dev/null 2>&1; then
    echo "HBase Master is ready!"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "WARNING: HBase Master WebUI not ready after 60 attempts (continuing anyway)"
    break
  fi
  echo "Attempt $i: Waiting for HBase Master..."
  sleep 3
done

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking HBase connectivity..."
  echo "ruok" | nc -w 3 127.0.0.1 2181 2>/dev/null | grep -q "imok"
  echo "HBase ZooKeeper is healthy."
  exit 0
fi

# ---------------------------------------------------------------------------
# Create sample HBase table via hbase shell (idempotent).
# ---------------------------------------------------------------------------
seed_hbase_table() {
  if [ "$CI_MODE" = true ]; then
    echo ""
    echo "  CI mode: skipping HBase sample table seeding"
    return 0
  fi
  if ! command -v docker > /dev/null 2>&1; then
    echo "  docker not found; skipping HBase sample table seeding"
    return 0
  fi

  echo ""
  echo "=== Seeding sample HBase table via hbase shell ==="

  # Create table 'pxf_hbase_test' with column family 'cf' (idempotent).
  local hbase_cmds
  hbase_cmds=$(cat <<'HBASE'
create_if_not_exists = false
begin
  list.include?('pxf_hbase_test')
rescue
  create_if_not_exists = true
end
if !list.include?('pxf_hbase_test')
  create 'pxf_hbase_test', 'cf'
end
put 'pxf_hbase_test', 'row1', 'cf:name', 'widget'
put 'pxf_hbase_test', 'row1', 'cf:value', '19.99'
put 'pxf_hbase_test', 'row2', 'cf:name', 'gadget'
put 'pxf_hbase_test', 'row2', 'cf:value', '49.50'
put 'pxf_hbase_test', 'row3', 'cf:name', 'gizmo'
put 'pxf_hbase_test', 'row3', 'cf:value', '12.25'
exit
HBASE
)

  # Simpler approach: use create_namespace and create with if_not_exists
  if docker exec -i "${HBASE_CONTAINER}" /hbase/bin/hbase shell <<'EOF' 2>/dev/null
create 'pxf_hbase_test', {NAME => 'cf', VERSIONS => 1}
EOF
  then
    echo "  Table 'pxf_hbase_test' created"
  else
    echo "  Table 'pxf_hbase_test' may already exist (idempotent)"
  fi

  # Insert sample rows (puts are idempotent in HBase).
  docker exec -i "${HBASE_CONTAINER}" /hbase/bin/hbase shell <<'EOF' 2>/dev/null || true
put 'pxf_hbase_test', 'row1', 'cf:name', 'widget'
put 'pxf_hbase_test', 'row1', 'cf:value', '19.99'
put 'pxf_hbase_test', 'row2', 'cf:name', 'gadget'
put 'pxf_hbase_test', 'row2', 'cf:value', '49.50'
put 'pxf_hbase_test', 'row3', 'cf:name', 'gizmo'
put 'pxf_hbase_test', 'row3', 'cf:value', '12.25'
scan 'pxf_hbase_test'
exit
EOF
  echo "  Sample rows inserted into 'pxf_hbase_test'"
}

seed_hbase_table

# ---------------------------------------------------------------------------
# Create k8s ExternalName Service for hbase
# ---------------------------------------------------------------------------
setup_k8s_hbase_artifacts() {
  if [ "$CI_MODE" = true ]; then
    echo ""
    echo "  CI mode: skipping k8s hbase ExternalName Service"
    return 0
  fi

  if ! command -v kubectl > /dev/null 2>&1; then
    echo "  kubectl not found; skipping k8s hbase ExternalName Service"
    return 0
  fi

  if ! kubectl cluster-info > /dev/null 2>&1; then
    echo "  No reachable Kubernetes cluster; skipping k8s hbase ExternalName Service"
    return 0
  fi

  echo ""
  echo "=== Creating k8s HBase artifacts in namespace '${K8S_NAMESPACE}' ==="

  # Ensure the namespace exists (idempotent).
  kubectl create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml \
    | kubectl apply -f - > /dev/null 2>&1 || true

  # ExternalName Service: hbase -> host.docker.internal
  echo "Creating ExternalName Service 'hbase' -> ${K8S_EXT_HOST}..."
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: hbase
  namespace: ${K8S_NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    app.kubernetes.io/component: hbase-endpoint
spec:
  type: ExternalName
  externalName: ${K8S_EXT_HOST}
  ports:
    - name: zookeeper
      port: 2181
      targetPort: 2181
      protocol: TCP
    - name: hbase-master
      port: 16010
      targetPort: 16010
      protocol: TCP
    - name: hbase-regionserver
      port: 16020
      targetPort: 16020
      protocol: TCP
EOF
  echo "  Service 'hbase' ready in '${K8S_NAMESPACE}'"
}

setup_k8s_hbase_artifacts

echo ""
echo "=== HBase Setup Complete ==="
echo "ZooKeeper:  127.0.0.1:2181"
echo "Master UI:  http://127.0.0.1:16010"
echo "Table:      pxf_hbase_test (cf:name, cf:value)"
