#!/usr/bin/env bash
# =============================================================================
# RabbitMQ Setup Script
# Creates test vhost, queues, and exchanges for data loading testing.
#
# Usage:
#   bash setup-rabbitmq.sh [--verify] [--ci]
#
# Environment:
#   RABBITMQ_ADDR - RabbitMQ management API address (default: http://127.0.0.1:15672)
#   RABBITMQ_USER - RabbitMQ admin username (default: guest)
#   RABBITMQ_PASS - RabbitMQ admin password (default: guest)
# =============================================================================

set -euo pipefail

RABBITMQ_ADDR="${RABBITMQ_ADDR:-http://127.0.0.1:15672}"
RABBITMQ_USER="${RABBITMQ_USER:-guest}"
RABBITMQ_PASS="${RABBITMQ_PASS:-guest}"
VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== RabbitMQ Setup ==="
echo "RabbitMQ management: ${RABBITMQ_ADDR}"

# Wait for RabbitMQ management API to be ready.
echo "Waiting for RabbitMQ to be ready..."
for i in $(seq 1 30); do
  if curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
    "${RABBITMQ_ADDR}/api/overview" > /dev/null 2>&1; then
    echo "RabbitMQ is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: RabbitMQ not ready after 30 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for RabbitMQ..."
  sleep 3
done

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking RabbitMQ health..."
  curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
    "${RABBITMQ_ADDR}/api/overview" > /dev/null 2>&1
  echo "RabbitMQ is healthy."
  exit 0
fi

# Create vhost.
echo "Creating vhost: cloudberry"
curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
  -X PUT "${RABBITMQ_ADDR}/api/vhosts/cloudberry" \
  -H "Content-Type: application/json" \
  -d '{}' > /dev/null 2>&1 || echo "  WARNING: Could not create vhost"

# Set permissions for guest user on the new vhost.
echo "Setting permissions for user '${RABBITMQ_USER}' on vhost 'cloudberry'"
curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
  -X PUT "${RABBITMQ_ADDR}/api/permissions/cloudberry/${RABBITMQ_USER}" \
  -H "Content-Type: application/json" \
  -d '{"configure":".*","write":".*","read":".*"}' > /dev/null 2>&1 || \
  echo "  WARNING: Could not set permissions"

# Create exchange.
echo "Creating exchange: cloudberry-exchange"
curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
  -X PUT "${RABBITMQ_ADDR}/api/exchanges/cloudberry/cloudberry-exchange" \
  -H "Content-Type: application/json" \
  -d '{"type":"direct","durable":true}' > /dev/null 2>&1 || \
  echo "  WARNING: Could not create exchange"

# Create queue.
echo "Creating queue: cloudberry-test-data"
curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
  -X PUT "${RABBITMQ_ADDR}/api/queues/cloudberry/cloudberry-test-data" \
  -H "Content-Type: application/json" \
  -d '{"durable":true}' > /dev/null 2>&1 || \
  echo "  WARNING: Could not create queue"

# Bind queue to exchange.
echo "Binding queue to exchange"
curl -sf -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" \
  -X POST "${RABBITMQ_ADDR}/api/bindings/cloudberry/e/cloudberry-exchange/q/cloudberry-test-data" \
  -H "Content-Type: application/json" \
  -d '{"routing_key":"test-data"}' > /dev/null 2>&1 || \
  echo "  WARNING: Could not bind queue"

echo ""
echo "=== RabbitMQ Setup Complete ==="
echo "VHost: cloudberry"
echo "Exchange: cloudberry-exchange"
echo "Queue: cloudberry-test-data"
echo "Management UI: ${RABBITMQ_ADDR}"
