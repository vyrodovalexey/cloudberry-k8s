#!/usr/bin/env bash
# =============================================================================
# Scenario 102 — Job 5 kafka-cdc (Continuous Streaming, Custom Connector)
# Kafka CDC Sample-Message Generation
# =============================================================================
# Produces the sample CDC messages the Scenario 102 kafka-cdc job consumes. The
# operator builds a continuous dataload Job that creates a pxf:// external table
# over the kafka topic (PROFILE=kafka&SERVER=kafka-connector, backed by the
# custom connector JAR) and streams rows into public.kafka_events.
#
# What this PRODUCES (task-breakdown §6.1):
#   JSON CDC envelopes published to the kafka topic cloudberry-cdc, e.g.
#     {"op":"c","ts":"2026-06-15T12:00:00Z","id":1,"event_type":"signup","payload":{"plan":"pro"}}
#     {"op":"u","ts":"2026-06-15T12:00:05Z","id":1,"event_type":"upgrade","payload":{"plan":"max"}}
#     {"op":"c","ts":"2026-06-15T12:00:10Z","id":2,"event_type":"signup","payload":{"plan":"free"}}
#
# TARGET DDL (created before the live consume by the live harness):
#   CREATE TABLE IF NOT EXISTS public.kafka_events
#     (id int, event_type text, payload jsonb, op text, ts timestamptz);
#
# CONNECTOR JAR (DevOps staging, task-breakdown §6.2):
#   The kafka-connector custom connector JAR is staged at
#     s3://cloudberry-data/connectors/kafka-connector.jar   (MinIO)
#   (in-cluster also http://minio:9000/cloudberry-data/connectors/kafka-connector.jar).
#   The operator's pxf-connector-init init container downloads it into
#   /pxf/lib/custom/kafka-connector.jar in the PXF sidecar (C.18).
#
# The script is IDEMPOTENT / re-runnable and LOGS what it PRODUCED.
#
# HONESTY: this only stages sample messages on the topic; NO metric is implied.
# The download + mount of the connector JAR is provable with ANY reachable
# artifact; the end-to-end kafka→table row landing needs a REAL Kafka→PXF
# connector JAR (CONFIG-ONLY if a placeholder is staged). A continuous consumer's
# steady state is cloudberry_data_loading_job_status=Running; NO new metric.
#
# Usage:
#   bash gen-kafka-cdc.sh [--verify] [--cat]
#
# Environment (use ENV, no hardcode):
#   KAFKA_BROKERS   - external Kafka listener  (default: 127.0.0.1:9094)
#   KAFKA_CONTAINER - kafka docker container   (default: kafka)
#   TOPIC           - the CDC topic            (default: cloudberry-cdc)
#   CONNECTOR_JAR   - the connector jarUrl     (default: s3://cloudberry-data/connectors/kafka-connector.jar)
# =============================================================================

set -euo pipefail

KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9094}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
TOPIC="${TOPIC:-cloudberry-cdc}"
CONNECTOR_JAR="${CONNECTOR_JAR:-s3://cloudberry-data/connectors/kafka-connector.jar}"
TARGET_TABLE="public.kafka_events"

VERIFY_ONLY=false
CAT_MSGS=false
for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY_ONLY=true ;;
    --cat)    CAT_MSGS=true ;;
  esac
done

log() { echo "[gen-kafka-cdc] $*"; }

# Sample CDC envelopes (task-breakdown §6.1). One JSON object per line — the
# kafka-console-producer publishes one message per line.
read -r -d '' CDC_MESSAGES <<'JSON' || true
{"op":"c","ts":"2026-06-15T12:00:00Z","id":1,"event_type":"signup","payload":{"plan":"pro"}}
{"op":"u","ts":"2026-06-15T12:00:05Z","id":1,"event_type":"upgrade","payload":{"plan":"max"}}
{"op":"c","ts":"2026-06-15T12:00:10Z","id":2,"event_type":"signup","payload":{"plan":"free"}}
{"op":"c","ts":"2026-06-15T12:00:15Z","id":3,"event_type":"signup","payload":{"plan":"pro"}}
{"op":"d","ts":"2026-06-15T12:00:20Z","id":2,"event_type":"churn","payload":{"reason":"price"}}
JSON

# Target DDL the live harness runs before the consume.
TARGET_DDL="CREATE TABLE IF NOT EXISTS ${TARGET_TABLE} (id int, event_type text, payload jsonb, op text, ts timestamptz);"

msg_count=$(printf '%s\n' "${CDC_MESSAGES}" | grep -c '^{' || true)

log "=== Scenario 102 kafka-cdc sample-message generation ==="
log "KAFKA_BROKERS=${KAFKA_BROKERS} | KAFKA_CONTAINER=${KAFKA_CONTAINER}"
log "TOPIC=${TOPIC} | TARGET_TABLE=${TARGET_TABLE}"
log "CONNECTOR JAR (jarUrl)=${CONNECTOR_JAR}"
log "  -> the operator's pxf-connector-init downloads this into /pxf/lib/custom/kafka-connector.jar (C.18)"
log ""
log "TARGET DDL:"
log "  ${TARGET_DDL}"

if [ "$CAT_MSGS" = true ]; then
  log ""
  log "----- sample CDC messages (${msg_count}) -----"
  printf '%s\n' "${CDC_MESSAGES}"
fi

# ---------------------------------------------------------------------------
# Produce the messages to the topic. Prefer a docker-exec console producer (the
# kafka container ships kafka-console-producer); fall back to logging only when
# the container is not available (so the script still runs in a CI dry-run).
# ---------------------------------------------------------------------------
produce_via_docker() {
  # docker exec kafka kafka-console-producer ...  reads stdin, one msg per line.
  printf '%s\n' "${CDC_MESSAGES}" | docker exec -i "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" 2>/dev/null
}

topic_reachable() {
  # The kafka container can list the topic on the internal listener.
  docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic "${TOPIC}" >/dev/null 2>&1
}

if [ "$VERIFY_ONLY" = true ]; then
  log ""
  log "Verification mode: checking topic ${TOPIC} reachability..."
  if command -v docker >/dev/null 2>&1 && topic_reachable; then
    log "Topic ${TOPIC} is present and reachable via container ${KAFKA_CONTAINER}."
    exit 0
  fi
  log "Topic ${TOPIC} NOT reachable (kafka container down or topic missing) — run setup-kafka.sh first."
  exit 0
fi

log ""
if command -v docker >/dev/null 2>&1; then
  if topic_reachable; then
    log "Producing ${msg_count} CDC message(s) to topic ${TOPIC} via container ${KAFKA_CONTAINER}..."
    if produce_via_docker; then
      log "PRODUCED ${msg_count} CDC message(s) to topic ${TOPIC}."
    else
      log "WARNING: could not produce to topic ${TOPIC} (producer failed); messages logged above."
    fi
  else
    log "Topic ${TOPIC} not reachable via container ${KAFKA_CONTAINER} — run setup-kafka.sh to create it."
    log "Sample messages were NOT produced (logged above for reference)."
  fi
else
  log "docker not available — sample messages logged above (not produced)."
fi

log ""
log "=== kafka-cdc sample-message generation complete ==="
log "Topic: ${TOPIC} | messages: ${msg_count}"
log "Connector JAR jarUrl: ${CONNECTOR_JAR}"
log "Next: deploy the kafka-test cluster (scenario102-kafka-test.yaml); the operator builds a"
log "  continuous dataload Job <cluster>-dataload-kafka-cdc (NOT a CronJob, J.46) that creates a"
log "  pxf://${TOPIC}?PROFILE=kafka&SERVER=kafka-connector external table and streams into ${TARGET_TABLE}."
log "HONESTY: end-to-end row landing needs a REAL Kafka->PXF connector JAR (CONFIG-ONLY otherwise)."
