#!/usr/bin/env bash
# =============================================================================
# Kafka Setup Script
# Creates test topics for data loading testing.
#
# Usage:
#   bash setup-kafka.sh [--verify] [--ci]
#
# Environment:
#   KAFKA_BROKERS - Kafka broker address (default: 127.0.0.1:9094)
# =============================================================================

set -euo pipefail

KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9094}"
VERIFY_ONLY=false
CI_MODE=false

for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --ci)     CI_MODE=true ;;
  esac
done

echo "=== Kafka Setup ==="
echo "Kafka brokers: ${KAFKA_BROKERS}"

# Wait for Kafka to be ready.
echo "Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
  if echo "" | nc -w 3 127.0.0.1 9094 > /dev/null 2>&1; then
    echo "Kafka port is open!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Kafka not ready after 30 attempts"
    exit 1
  fi
  echo "Attempt $i: Waiting for Kafka..."
  sleep 3
done

# Additional wait for Kafka to fully initialize.
sleep 5

if [ "$VERIFY_ONLY" = true ]; then
  echo "Verification mode: checking Kafka connectivity..."
  echo "" | nc -w 3 127.0.0.1 9094 > /dev/null 2>&1
  echo "Kafka is reachable."
  exit 0
fi

# Create topics using docker exec if kafka container is available.
create_topic() {
  local topic="$1"
  local partitions="${2:-3}"
  echo "Creating topic: ${topic} (partitions: ${partitions})"

  if docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null; then
    echo "  Topic '${topic}' created successfully"
  else
    echo "  WARNING: Could not create topic '${topic}' (may already exist or container not available)"
  fi
}

create_topic "cloudberry-test-data" 3
create_topic "cloudberry-cdc" 3

echo ""
echo "=== Kafka Setup Complete ==="
echo "Topics created: cloudberry-test-data, cloudberry-cdc"
