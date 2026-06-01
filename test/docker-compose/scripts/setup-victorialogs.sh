#!/usr/bin/env bash
# =============================================================================
# VictoriaLogs Setup Script
# Waits for VictoriaLogs to be ready and verifies the UI endpoint.
#
# Usage:
#   bash setup-victorialogs.sh [--verify] [--ci]
#
# Environment:
#   VICTORIALOGS_ADDR - VictoriaLogs address (default: http://127.0.0.1:9428)
# =============================================================================

set -euo pipefail

VICTORIALOGS_ADDR="${VICTORIALOGS_ADDR:-http://127.0.0.1:9428}"
VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== VictoriaLogs Setup ==="
echo "VictoriaLogs address: ${VICTORIALOGS_ADDR}"

# Wait for VictoriaLogs to be ready.
echo "Waiting for VictoriaLogs to be ready..."
for i in $(seq 1 30); do
  if curl -sf "${VICTORIALOGS_ADDR}/health" > /dev/null 2>&1; then
    echo "VictoriaLogs is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: VictoriaLogs not ready after 30 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for VictoriaLogs..."
  sleep 2
done

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking VictoriaLogs health..."
  curl -sf "${VICTORIALOGS_ADDR}/health" > /dev/null 2>&1
  echo "VictoriaLogs is healthy."
  exit 0
fi

# Verify the UI endpoint is accessible.
echo "Checking VictoriaLogs UI endpoint..."
if curl -sf -o /dev/null "${VICTORIALOGS_ADDR}/select/vmui/"; then
  echo "  VictoriaLogs UI is accessible."
else
  echo "  WARNING: VictoriaLogs UI endpoint not accessible (non-fatal)."
fi

echo ""
echo "=== VictoriaLogs Setup Complete ==="
echo "Health: ${VICTORIALOGS_ADDR}/health"
echo "UI:     ${VICTORIALOGS_ADDR}/select/vmui/"
