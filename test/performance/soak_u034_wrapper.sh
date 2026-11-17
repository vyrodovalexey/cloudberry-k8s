#!/usr/bin/env bash
# U034 soak wrapper: preflight gate -> 60-min driver -> post verification -> teardown.
set -uo pipefail
NS="${SOAK_NS:-cloudberry-test}"; CLUSTER="acceptance-test"; COORD="${CLUSTER}-coordinator-0"
EVID=".opencode/output/u034-evidence"; DUR="${SOAK_DURATION:-3600}"
PSQL=/usr/local/cloudberry-db/bin/psql
FP_EXPECTED="222647789 80000 480000 960000 1314280100.00"
PXF_FP="175000 15312587500 15312587500"
mkdir -p "${EVID}"
log(){ echo "[$(date -u +%FT%TZ)] $*" | tee -a "${EVID}/wrapper.log"; }

psql_exec(){ kubectl exec -n "${NS}" "${COORD}" -c cloudberry -- "${PSQL}" -X -q -tA -d "$1" -c "$2"; }

log "=== preflight gate ==="
[[ "$(kubectl config current-context)" == "docker-desktop" ]] || { log "FAIL ctx"; exit 1; }
phase=$(kubectl -n ${NS} get cloudberrycluster ${CLUSTER} -o jsonpath='{.status.phase}')
[[ "${phase}" == "Running" ]] || { log "FAIL phase=${phase}"; exit 1; }
mir=$(kubectl -n ${NS} get cloudberrycluster ${CLUSTER} -o jsonpath='{.status.mirroringStatus}')
[[ "${mir}" == "InSync" ]] || { log "FAIL mirroring=${mir}"; exit 1; }
fp=$(psql_exec mydb "SELECT pg_database_size('mydb')||' '||(SELECT count(*) FROM customers)||' '||(SELECT count(*) FROM orders)||' '||(SELECT count(*) FROM line_items)||' '||(SELECT sum(qty*price) FROM line_items)")
[[ "${fp}" == "${FP_EXPECTED}" ]] || { log "FAIL fingerprint: ${fp}"; exit 1; }
log "preflight OK: Running/InSync, fingerprint=${fp}"
kubectl get pods -n ${NS} -o json > "${EVID}/pods-pre.json"

log "=== T0: launching driver (${DUR}s) ==="
T0_ISO=$(date -u +%FT%TZ); echo "${T0_ISO}" > "${EVID}/T0"
SOAK_EVID="${EVID}" SOAK_DURATION="${DUR}" python3 "$(dirname "$0")/soak_u034.py"
DRV_RC=$?
log "driver exited rc=${DRV_RC} at t+${SECONDS}s"

log "=== post-run verification ==="
T1_ISO=$(date -u +%FT%TZ); echo "${T1_ISO}" > "${EVID}/T1"
fp2=$(psql_exec mydb "SELECT pg_database_size('mydb')||' '||(SELECT count(*) FROM customers)||' '||(SELECT count(*) FROM orders)||' '||(SELECT count(*) FROM line_items)||' '||(SELECT sum(qty*price) FROM line_items)")
{ [[ "${fp2}" == "${FP_EXPECTED}" ]] && log "final fingerprint OK: ${fp2}"; } || log "FINAL FINGERPRINT FAIL: ${fp2}"
for f in s3_text s3_parquet s3_avro hdfs_text hdfs_parquet hdfs_avro; do
  r=$(psql_exec postgres "SELECT count(*)||' '||sum(id)||' '||sum(amount) FROM u034_ext.rext_${f}" 2>&1 | tail -1)
  [[ "${r}" == "${PXF_FP}" ]] && log "pxf final ${f}: OK" || log "pxf final ${f}: FAIL got=${r}"
done
kubectl get pods -n ${NS} -o json > "${EVID}/pods-post.json"
kubectl -n ${NS} get cloudberrycluster ${CLUSTER} -o json > "${EVID}/cr-post.json"
kubectl get events -n ${NS} --sort-by=.lastTimestamp -o json > "${EVID}/events-post.json" 2>/dev/null
kubectl logs -n ${NS} deployment/cloudberry-operator --since-time="${T0_ISO}" --timestamps > "${EVID}/operator-since-T0.log" 2>&1
log "operator reconcile lines since T0: $(grep -ci reconcil "${EVID}/operator-since-T0.log" || true)"
log "operator log size: $(wc -l < "${EVID}/operator-since-T0.log") lines"

log "=== teardown: drop u034_ext fixture (postgres DB, readable-only) ==="
kubectl exec -n ${NS} ${COORD} -c cloudberry -- ${PSQL} -X -q -d postgres -c "DROP SCHEMA IF EXISTS u034_ext CASCADE" && log "u034_ext dropped"

log "=== VM history (window T0..T1) ==="
VM="http://127.0.0.1:8428/api/v1/query"
vq(){ curl -sG --max-time 8 "${VM}" --data-urlencode "query=$1"; }
vq "max_over_time(kube_pod_container_status_restarts_total{namespace=\"${NS}\",pod=~\"${CLUSTER}-(coordinator|coordinator-standby|segment-).*\"}[2h])" > "${EVID}/vm-restarts-max.json"
vq "min_over_time(pg_up{namespace=\"${NS}\"}[2h])" > "${EVID}/vm-pgup-min.json"
log "restarts-max: $(cat "${EVID}/vm-restarts-max.json" | head -c 600)"
log "pgup-min: $(cat "${EVID}/vm-pgup-min.json" | head -c 600)"
log "=== wrapper complete ==="
