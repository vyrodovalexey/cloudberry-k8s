#!/usr/bin/env bash
# =============================================================================
# Scenario 101 — gpfdist Deployment + Job 4 (gpload-csv)
# CSV Sample-Data Generation + PVC Seeding
# =============================================================================
# Generates the CSV sample data the Scenario 101 gpfdist+gpload tests load. The
# gpfdist Deployment serves /data/incoming/*.csv and the gpload CronJob loads
# them into public.raw_data via `gpload -f /etc/gpload/<job>.yml` (the control
# file the operator renders from the gpload-csv job).
#
# What this PRODUCES (spec §11):
#   incoming/raw_data_001.csv  (header: id,event_type,payload,created_at + rows)
#   incoming/raw_data_002.csv  (more rows, same schema — exercises the *.csv glob)
#
# The files are written to a host directory (DATA_DIR, default
# test/docker-compose/data/gpload/incoming) AND their content is emitted to the
# log so the live deploy can seed them into the gpfdist PVC's /data/incoming.
#
# TARGET DDL (created before the load by the live harness):
#   CREATE TABLE IF NOT EXISTS public.raw_data
#     (id int, event_type text, payload jsonb, created_at timestamptz);
#
# SEED APPROACH (gpfdist PVC is RWO; replicas:1 for the live run):
#   The gpfdist Deployment mounts <cluster>-gpfdist-data-pvc at /data. Seed the
#   CSVs into /data/incoming on that PVC by ONE of:
#     (a) RECOMMENDED — a seed Job that mounts the SAME PVC
#         (<cluster>-gpfdist-data-pvc) and writes the CSVs (reproducible,
#         kubectl-cp-free). Pass --emit-seed-job to print such a Job manifest.
#     (b) kubectl cp into the running gpfdist pod:
#         kubectl cp <csv> <ns>/<gpfdist-pod>:/data/incoming/<csv>
#     (c) kubectl exec writing the CSVs (what the e2e does as a fallback).
#   Verify: kubectl exec <gpfdist-pod> -- ls /data/incoming  (shows the CSVs)
#   before the gpload CronJob fires.
#
# The script is IDEMPOTENT / re-runnable and logs what it PRODUCED.
#
# HONESTY: this only stages sample data; no metric is implied. The honest "data
# loads" proof is SELECT count(*) FROM public.raw_data > 0 after the gpload Job
# runs (asserted by the Scenario 101 e2e Part B). cloudberry_gpfdist_* stay
# PLANNED; gpload reuses cloudberry_data_loading_*.
#
# Usage:
#   bash gen-gpload-csv.sh [--emit-seed-job] [--cat]
#
# Environment (use ENV, no hardcode):
#   DATA_DIR        - host output dir   (default: <repo>/test/docker-compose/data/gpload/incoming)
#   GPFDIST_PVC     - the gpfdist PVC    (default: gpfdist-test-gpfdist-data-pvc)
#   NAMESPACE       - k8s namespace      (default: cloudberry-test)
#   GPFDIST_IMAGE   - seed Job image     (default: cloudberry-gpfdist:2.1.0)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_DATA_DIR="${SCRIPT_DIR}/../data/gpload/incoming"

DATA_DIR="${DATA_DIR:-${DEFAULT_DATA_DIR}}"
GPFDIST_PVC="${GPFDIST_PVC:-gpfdist-test-gpfdist-data-pvc}"
NAMESPACE="${NAMESPACE:-cloudberry-test}"
GPFDIST_IMAGE="${GPFDIST_IMAGE:-cloudberry-gpfdist:2.1.0}"

EMIT_SEED_JOB=false
CAT_FILES=false
for arg in "$@"; do
  case "$arg" in
    --emit-seed-job) EMIT_SEED_JOB=true ;;
    --cat)           CAT_FILES=true ;;
  esac
done

log() { echo "[gen-gpload-csv] $*"; }

# CSV contents (spec §11). raw_data_001.csv carries the header (exercises J.32
# HEADER true); raw_data_002.csv carries more rows (same schema) so the *.csv
# glob (GL.2 / J.26) picks up multiple files. payload is JSON (jsonb target),
# double-quoted with doubled inner quotes per RFC-4180.
read -r -d '' RAW_DATA_001 <<'CSV' || true
id,event_type,payload,created_at
1,click,"{""x"":1}",2026-06-14T10:00:00Z
2,view,"{""y"":2}",2026-06-14T10:01:00Z
3,purchase,"{""amt"":9.99}",2026-06-14T10:02:00Z
CSV

read -r -d '' RAW_DATA_002 <<'CSV' || true
id,event_type,payload,created_at
4,click,"{""x"":4}",2026-06-14T10:03:00Z
5,view,"{""y"":5}",2026-06-14T10:04:00Z
6,signup,"{""plan"":""pro""}",2026-06-14T10:05:00Z
7,purchase,"{""amt"":19.95}",2026-06-14T10:06:00Z
CSV

# Target DDL the live harness runs before the load.
TARGET_DDL="CREATE TABLE IF NOT EXISTS public.raw_data (id int, event_type text, payload jsonb, created_at timestamptz);"

log "=== Scenario 101 gpload CSV sample-data generation ==="
log "DATA_DIR=${DATA_DIR}"
log "gpfdist PVC=${GPFDIST_PVC} | namespace=${NAMESPACE} | seed image=${GPFDIST_IMAGE}"

mkdir -p "${DATA_DIR}"
printf '%s\n' "${RAW_DATA_001}" > "${DATA_DIR}/raw_data_001.csv"
printf '%s\n' "${RAW_DATA_002}" > "${DATA_DIR}/raw_data_002.csv"

rows1=$(( $(printf '%s\n' "${RAW_DATA_001}" | wc -l) - 1 ))
rows2=$(( $(printf '%s\n' "${RAW_DATA_002}" | wc -l) - 1 ))
total=$(( rows1 + rows2 ))

log "PRODUCED ${DATA_DIR}/raw_data_001.csv (${rows1} data rows)"
log "PRODUCED ${DATA_DIR}/raw_data_002.csv (${rows2} data rows)"
log "TOTAL data rows to load into public.raw_data: ${total}"
log ""
log "TARGET DDL:"
log "  ${TARGET_DDL}"

if [ "$CAT_FILES" = true ]; then
  log ""
  log "----- raw_data_001.csv -----"
  cat "${DATA_DIR}/raw_data_001.csv"
  log "----- raw_data_002.csv -----"
  cat "${DATA_DIR}/raw_data_002.csv"
fi

# ---------------------------------------------------------------------------
# Optional: emit a seed Job manifest (recommended seed approach (a)). The Job
# mounts the SAME gpfdist PVC at /data and writes the two CSVs into
# /data/incoming using a heredoc, so the gpfdist Deployment then serves them.
# ---------------------------------------------------------------------------
if [ "$EMIT_SEED_JOB" = true ]; then
  log ""
  log "----- seed Job manifest (kubectl apply -f -) -----"
  # NOTE: the CSV bodies are embedded literally; the doubled inner quotes are
  # preserved so the jsonb payload parses on load.
  cat <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: gpfdist-seed-csv
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: cloudberry-operator
    scenario: "101"
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: seed
          image: ${GPFDIST_IMAGE}
          command: ["bash", "-lc"]
          args:
            - |
              set -e
              mkdir -p /data/incoming
              cat > /data/incoming/raw_data_001.csv <<'CSV001'
$(printf '%s' "${RAW_DATA_001}" | sed 's/^/              /')
              CSV001
              cat > /data/incoming/raw_data_002.csv <<'CSV002'
$(printf '%s' "${RAW_DATA_002}" | sed 's/^/              /')
              CSV002
              echo "seeded:"; ls -l /data/incoming
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: ${GPFDIST_PVC}
YAML
fi

log ""
log "=== gpload CSV sample-data generation complete ==="
log "Seed the PVC (recommended): bash gen-gpload-csv.sh --emit-seed-job | kubectl apply -f -"
log "Then verify: kubectl exec -n ${NAMESPACE} deploy/gpfdist-test-gpfdist -- ls /data/incoming"
log "HONESTY: data-loads proof = SELECT count(*) FROM public.raw_data > 0 after the gpload Job."
