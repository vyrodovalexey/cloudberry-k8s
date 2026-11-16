#!/usr/bin/env bash
# =============================================================================
# U023 — Cloudberry 2.2.0 performance campaign driver (2026-11-16)
# =============================================================================
# SERIALIZED execution (U022 finding: parallel perf runs self-contend on the
# emulated DB). Runs every runnable PTC-220-* scenario >=3 times, captures
# per-run metrics + load-generator saturation checks + target-side (VM) samples
# into a JSONL results file, and enforces the campaign stop conditions:
#   - ABORT on any HTTP 5xx or connection error from a load run
#   - ABORT on go-bench FAIL (b.Fatalf)
#   - ABORT on mydb fingerprint drift (checked between scenarios)
#   - a run is flagged invalid if the generator process exceeds 80% of one core
#     or (fixed-rate phases) achieved RPS < 95% of offered
# Environment contract: local docker-desktop k8s, ns cloudberry-test, cluster
# acceptance-test; host otherwise QUIET (delegation: exclusive use).
# All result extraction is done in python3 from RAW outputs (never summaries).
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NS="cloudberry-test"
CLUSTER="acceptance-test"
COORD_POD="${CLUSTER}-coordinator-0"
API_LOCAL=8190; HEALTH_LOCAL=8181
VM="http://127.0.0.1:8428"
HEY="$HOME/go/bin/hey"; command -v hey >/dev/null 2>&1 && HEY="$(command -v hey)"
RUNS="${RUNS:-3}"
TS="$(date +%Y%m%d_%H%M%S)"
OUTDIR="${SCRIPT_DIR}/.yandextank/u023-${TS}"
RESULTS="${OUTDIR}/results.jsonl"
LOG="${OUTDIR}/campaign.log"
ABORT_FLAG=0
PF_API_PID=""; PF_HEALTH_PID=""

mkdir -p "${OUTDIR}"
exec > >(tee -a "${LOG}") 2>&1

log()  { echo "[$(date +%H:%M:%S)] $*"; }
emit() { echo "$2" >> "${RESULTS}"; log "RESULT $1"; }

vm_query() { curl -sG --max-time 5 "${VM}/api/v1/query" --data-urlencode "query=$1" 2>/dev/null; }

# --- target-side sample: coordinator/op memory + restarts (DEF-8 watch) -----
target_sample() {
  local coord_mem op_mem coord_rst
  coord_mem=$(vm_query "max(container_memory_working_set_bytes{pod=\"${COORD_POD}\",container=\"cloudberry\"})" | sed -n 's/.*"value":"\([0-9.e+]*\)".*/\1/p' | head -1)
  op_mem=$(vm_query 'max(container_memory_working_set_bytes{pod=~"cloudberry-operator-.*",container="cloudberry-operator"})' | sed -n 's/.*"value":"\([0-9.e+]*\)".*/\1/p' | head -1)
  coord_rst=$(vm_query "max(kube_pod_container_status_restarts_total{pod=\"${COORD_POD}\",container=\"cloudberry\"})" | sed -n 's/.*"value":"\([0-9.e+]*\)".*/\1/p' | head -1)
  echo "target_sample coord_mem=${coord_mem:-na} op_mem=${op_mem:-na} coord_restarts=${coord_rst:-na}"
}

mydb_fingerprint() {
  kubectl exec -n "${NS}" "${COORD_POD}" -c cloudberry -- \
    /usr/local/cloudberry-db/bin/psql -U gpadmin -d mydb -tA \
    -c "SELECT pg_database_size('mydb')||' '||(SELECT count(*) FROM customers)||' '||(SELECT count(*) FROM orders)||' '||(SELECT count(*) FROM line_items)||' '||(SELECT sum(qty*price) FROM line_items)" 2>/dev/null
}

FP_EXPECTED="222647789 80000 480000 960000 1314280100.00"

abort_campaign() {
  log "!!! CAMPAIGN ABORT: $*"
  echo "{\"ptc\":\"ABORT\",\"reason\":\"$*\",\"ts\":\"$(date -u +%FT%TZ)\"}" >> "${RESULTS}"
  ABORT_FLAG=1
}

# --- load-generator CPU sampler (ps %cpu ≈ lifetime average of the process) --
lg_start() { ( while kill -0 "$1" 2>/dev/null; do ps -p "$1" -o %cpu= 2>/dev/null; sleep 0.5; done; echo -1 ) > "$2" & }
lg_max()   { awk '$1>max{max=$1} END{print (max==-1?"na":max+0)}' "$1" 2>/dev/null || echo na; }

cleanup() {
  [[ -n "${PF_API_PID}" ]] && kill "${PF_API_PID}" 2>/dev/null
  [[ -n "${PF_HEALTH_PID}" ]] && kill "${PF_HEALTH_PID}" 2>/dev/null
  pkill -P $$ 2>/dev/null
  log "port-forwards stopped"
}
trap cleanup EXIT

# =============================================================================
# Preflight (target safety)
# =============================================================================
log "=== U023 campaign ${TS} — preflight ==="
[[ "$(kubectl config current-context)" == "docker-desktop" ]] || { abort_campaign "kube context is not docker-desktop"; exit 1; }
phase=$(kubectl -n ${NS} get cloudberrycluster ${CLUSTER} -o jsonpath='{.status.phase}')
[[ "${phase}" == "Running" ]] || { abort_campaign "cluster phase=${phase}"; exit 1; }
log "cluster ${CLUSTER}: ${phase}, context docker-desktop — authorized target (delegation brief)"
fp0=$(mydb_fingerprint)
[[ "${fp0}" == "${FP_EXPECTED}" ]] || { abort_campaign "mydb fingerprint drift at start: ${fp0}"; exit 1; }
log "mydb fingerprint OK: ${fp0}"

stray=$(pgrep -fl 'hey|go test' 2>/dev/null | grep -v "$$" | grep -cv 'run-campaign' || true)
[[ "${stray}" -gt 0 ]] && { abort_campaign "stray load processes on host: ${stray}"; exit 1; }

kubectl -n ${NS} port-forward "pod/${COORD_POD}" ${API_LOCAL}:8090 >/dev/null 2>&1 & PF_API_PID=$!
kubectl -n ${NS} port-forward "deployment/cloudberry-operator" ${HEALTH_LOCAL}:8081 >/dev/null 2>&1 & PF_HEALTH_PID=$!
sleep 4
PW=$(kubectl -n ${NS} get secret cloudberry-operator-admin-password -o jsonpath='{.data.password}' | base64 -d)
AUTH=$(printf 'admin:%s' "${PW}" | base64)
smoke=$(curl -s --max-time 5 "http://127.0.0.1:${API_LOCAL}/healthz")
log "API smoke /healthz: ${smoke}"
smoke2=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -u "admin:${PW}" "http://127.0.0.1:${API_LOCAL}/api/v1alpha1/clusters/${CLUSTER}")
log "API smoke authed cluster GET: HTTP ${smoke2}"
[[ "${smoke2}" == "200" ]] || { abort_campaign "authed smoke failed ${smoke2}"; exit 1; }
log "$(target_sample)"
log "=== preflight complete — run window open (exclusive use) ==="

fp_check() {
  local fp; fp=$(mydb_fingerprint)
  if [[ "${fp}" != "${FP_EXPECTED}" ]]; then abort_campaign "mydb fingerprint drift: ${fp}"; return 1; fi
  log "mydb fingerprint intact: ${fp}"
}

# =============================================================================
# PTC-220-01 — health endpoints A1..A4: 100 RPS x 60s (07-12 baseline parity)
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  for ep in healthz readyz; do
    for tgt in "api:${API_LOCAL}" "health:${HEALTH_LOCAL}"; do
      port="${tgt#*:}"
      hey_run_label="PTC-220-01.A_${ep}_${tgt%%:*}.run${run}"
      outfile="${OUTDIR}/${hey_run_label}.txt"
      "${HEY}" -q 20 -c 5 -z 60s -H "User-Agent: u023-campaign" \
        "http://127.0.0.1:${port}/${ep}" > "${outfile}" 2>&1 &
      hp=$!; lg_start "${hp}" "${outfile}.cpu"; wait "${hp}"; rc=$?
      lgmax=$(lg_max "${outfile}.cpu")
      python3 - "${outfile}" "${hey_run_label}" "${lgmax}" "${rc}" >> "${RESULTS}" <<'PYA'
import sys, re, json
f, label, lg, rc = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
txt=open(f).read()
codes={}
for m in re.finditer(r"^\[(\d+)\]\s+(\d+) responses", txt, re.M): codes[m.group(1)]=int(m.group(2))
total=sum(codes.values())
rps=re.search(r"Requests/sec:\s+([\d.]+)", txt)
p=lambda p: (lambda m: float(m.group(1)) if m else None)(re.search(r"%s%% in ([\d.]+) secs" % p, txt))
s5=sum(v for k,v in codes.items() if k.startswith("5"))
out={"ptc":"PTC-220-01","run":label,"requests":total,"codes":codes,"5xx":s5,
     "achieved_rps":float(rps.group(1)) if rps else None,
     "p50_s":p("50"),"p95_s":p("95"),"p99_s":p("99"),
     "loadgen_max_cpu": None if lg=="na" else float(lg), "hey_rc":int(rc)}
print(json.dumps(out))
PYA
      # stop condition enforcement: 5xx or connection error aborts the campaign
      if [[ "${rc}" -ne 0 ]] || python3 -c "
import sys,json
lines=[l for l in open('${RESULTS}') if '"${hey_run_label}"' in l]
sys.exit(0 if json.loads(lines[-1])['5xx']>0 else 1)"; then
        abort_campaign "hey ${hey_run_label}: rc=${rc} (see ${outfile})"; exit 1
      fi
      log "RESULT ${hey_run_label} (lg_cpu_max=${lgmax})"
      sleep 10
    done
  done
  fp_check || exit 1
  sleep 20
done

# =============================================================================
# PTC-220-02 — API read path (B1/B2): 9 sequential authed requests per endpoint
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  for ep in "clusters" "clusters/${CLUSTER}"; do
    f="${OUTDIR}/PTC-220-02.${ep//\//_}.run${run}.csv"
    for i in $(seq 1 9); do
      curl -s -o /dev/null -w '%{http_code},%{time_total}\n' --max-time 10 -u "admin:${PW}" \
        "http://127.0.0.1:${API_LOCAL}/api/v1alpha1/${ep}" >> "$f"
    done
    python3 - "$f" "${run}" "${ep}" >> "${RESULTS}" <<'PYB'
import sys, csv, json, statistics
rows=[r for r in csv.reader(open(sys.argv[1])) if len(r)==2]
codes={}
for r in rows: codes[r[0]]=codes.get(r[0],0)+1
t=[float(r[1]) for r in rows if r[0]=="200"]
print(json.dumps({"ptc":"PTC-220-02","run":"run"+sys.argv[2],"ep":sys.argv[3],
  "codes":codes,"n":len(rows),
  "ok_avg_ms":round(statistics.mean(t)*1000,1) if t else None,
  "ok_p50_ms":round(statistics.median(t)*1000,1) if t else None,
  "ok_max_ms":round(max(t)*1000,1) if t else None}))
PYB
    log "RESULT PTC-220-02 ${ep} run${run}"
    sleep 5
  done
  sleep 60   # rate-limit bucket replenishes between runs
done

# =============================================================================
# PTC-220-03 — rate-limiter knee (ADAPTED: configured limit is 600/min — the
# documented U022 config drift; the 07-12 baseline ran the default 10/min).
# 8 parallel curl loops, 20s, per-request CSV -> first-429 index + per-class
# latencies. All requests share the port-forward source IP 127.0.0.1.
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  f="${OUTDIR}/PTC-220-03.knee.run${run}.csv"
  ( for i in $(seq 1 8); do
      end=$(( $(date +%s) + 20 )); while [ $(date +%s) -lt $end ]; do
        curl -s -o /dev/null -w "%{http_code},%{time_total}\n" --max-time 5 -u "admin:${PW}" \
          "http://127.0.0.1:${API_LOCAL}/api/v1alpha1/clusters/${CLUSTER}"
      done
    done ) >> "$f" 2>/dev/null &
  XP=$!
  ( while kill -0 ${XP} 2>/dev/null; do
      ps -Ao %cpu,comm= | awk '$2=="curl"{s+=$1} END{print s+0}'; sleep 0.5
    done; echo -1 ) > "${OUTDIR}/PTC-220-03.knee.run${run}.cpu" &
  wait ${XP}
  lgmax=$(lg_max "${OUTDIR}/PTC-220-03.knee.run${run}.cpu")
  python3 - "$f" "${run}" "${lgmax}" >> "${RESULTS}" <<'PYC'
import sys, csv, json, statistics
rows=[r for r in csv.reader(open(sys.argv[1])) if len(r)==2]
first429=next((i+1 for i,r in enumerate(rows) if r[0]=="429"), None)
ok=[float(r[1]) for r in rows if r[0]=="200"]; rl=[float(r[1]) for r in rows if r[0]=="429"]
oth={}
for r in rows:
    if r[0] not in ("200","429"): oth[r[0]]=oth.get(r[0],0)+1
s5=sum(v for k,v in oth.items() if k.startswith("5"))
print(json.dumps({"ptc":"PTC-220-03","run":"run"+sys.argv[2],"requests":len(rows),
  "first_429_at":first429,"ok_n":len(ok),
  "ok_avg_ms":round(statistics.mean(ok)*1000,1) if ok else None,
  "ok_p95_ms":round(sorted(ok)[int(len(ok)*0.95)]*1000,1) if ok else None,
  "rl_n":len(rl),
  "rl_avg_ms":round(statistics.mean(rl)*1000,1) if rl else None,
  "other":oth or None,"loadgen_max_cpu": None if sys.argv[3]=="na" else float(sys.argv[3])}))
PYC
  log "RESULT PTC-220-03 run${run}"
  [[ -n "$(python3 -c "
import json,sys
lines=[l for l in open('${RESULTS}') if 'PTC-220-03' in l and 'run${run}' in l]
o=json.loads(lines[-1]); print('5XX' if any(k.startswith('5') for k in (o.get('other') or {})) else '')")" ]] \
    && { abort_campaign "5xx during knee run${run}"; exit 1; }
  sleep 120  # let the 600/min bucket fully drain before the next knee run
done

# =============================================================================
# PTC-220-04 — stepped load 5/10/25/50/100 RPS x 30s, authed (B4 parity)
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  for rps in 5 10 25 50 100; do
    label="PTC-220-04.step${rps}.run${run}"
    outfile="${OUTDIR}/${label}.txt"
    qps=$(( rps / 5 )); [[ ${qps} -lt 1 ]] && qps=1
    "${HEY}" -q ${qps} -c 5 -z 30s -H "User-Agent: u023-campaign" -H "Authorization: Basic ${AUTH}" \
      "http://127.0.0.1:${API_LOCAL}/api/v1alpha1/clusters/${CLUSTER}" > "${outfile}" 2>&1 &
    hp=$!; lg_start "${hp}" "${outfile}.cpu"; wait "${hp}"; rc=$?
    lgmax=$(lg_max "${outfile}.cpu")
    python3 - "${outfile}" "${label}" "${rps}" "${lgmax}" "${rc}" >> "${RESULTS}" <<'PYD'
import sys, re, json
f, label, rps, lg, rc = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4], sys.argv[5]
txt=open(f).read()
codes={}
for m in re.finditer(r"^\[(\d+)\]\s+(\d+) responses", txt, re.M): codes[m.group(1)]=int(m.group(2))
total=sum(codes.values())
r=re.search(r"Requests/sec:\s+([\d.]+)", txt)
p=lambda p: (lambda m: float(m.group(1)) if m else None)(re.search(r"%s%% in ([\d.]+) secs" % p, txt))
s5=sum(v for k,v in codes.items() if k.startswith("5"))
ach=float(r.group(1)) if r else None
print(json.dumps({"ptc":"PTC-220-04","run":label,"offered_rps":rps,"requests":total,
  "codes":codes,"5xx":s5,"achieved_rps":ach,
  "achieved_ok": (ach is None or ach>=0.95*rps),
  "p50_s":p("50"),"p95_s":p("95"),"p99_s":p("99"),
  "loadgen_max_cpu": None if lg=="na" else float(lg), "hey_rc":int(rc)}))
PYD
    # stop conditions: 5xx, connection error, or unsustained offered rate
    python3 -c "
import json,sys
o=json.loads([l for l in open('${RESULTS}') if '\"${label}\"' in l][-1])
sys.exit(1 if (o['5xx']>0 or o['hey_rc']!=0 or not o['achieved_ok']) else 0)" \
      || { abort_campaign "step ${label} failed stop-condition (see ${outfile})"; exit 1; }
    log "RESULT ${label} (lg_cpu_max=${lgmax})"
    sleep 10
  done
  fp_check || exit 1
  sleep 60
done

# =============================================================================
# PTC-220-05 — DB query benchmark on mydb (C1..C5), warmup + 3 timed passes
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  kubectl exec -i -n ${NS} ${COORD_POD} -c cloudberry -- /usr/local/cloudberry-db/bin/psql -U gpadmin -d mydb -f - \
    < "${SCRIPT_DIR}/sql/mydb-benchmark-u023.sql" > "${OUTDIR}/PTC-220-05.warmup.run${run}.log" 2>&1 || true
  for pass in 1 2 3; do
    kubectl exec -i -n ${NS} ${COORD_POD} -c cloudberry -- /usr/local/cloudberry-db/bin/psql -U gpadmin -d mydb -v ON_ERROR_STOP=1 -f - \
      < "${SCRIPT_DIR}/sql/mydb-benchmark-u023.sql" > "${OUTDIR}/PTC-220-05.pass${pass}.run${run}.log" 2>&1 \
      || { abort_campaign "db bench pass failed"; exit 1; }
  done
  python3 - "${OUTDIR}" "${run}" >> "${RESULTS}" <<'PYE'
import sys, os, re, json, statistics
d, run = sys.argv[1], sys.argv[2]
qt={f"C{i}":[] for i in range(1,6)}
for p in (1,2,3):
    cur=None
    for line in open(os.path.join(d, f"PTC-220-05.pass{p}.run{run}.log")):
        m=re.match(r"=== (C\d):", line)
        if m: cur=m.group(1); continue
        if cur and line.startswith("Time:") and " ms" in line:
            qt[cur].append(float(line.split()[1])); cur=None
avg={k: round(statistics.mean(v),1) if v else None for k,v in qt.items()}
p50={k: round(statistics.median(v),1) if v else None for k,v in qt.items()}
print(json.dumps({"ptc":"PTC-220-05","run":f"run{run}","avg_ms":avg,"p50_ms":p50,
  "samples":{k:len(v) for k,v in qt.items()}}))
PYE
  log "RESULT PTC-220-05 run${run}"
  fp_check || exit 1
  sleep 30
done

# =============================================================================
# PTC-220-06..16 — go-bench live scenarios (serialized; raw logs kept)
# =============================================================================
MINTS="$(date +%Y%m%d-%H%M%S)"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

go_bench() { # $1 ptc-tag  $2 bench regex  $3 benchtime  [$4... extra env]
  local tag="$1" regex="$2" btime="$3"; shift 3
  for run in $(seq 1 ${RUNS}); do
    local f="${OUTDIR}/${tag}.run${run}.log"
    env KUBECONFIG="${KUBECONFIG}" "$@" \
      go test ./test/perf/... -tags=e2e -run='^$' -bench="${regex}" \
      -benchtime="${btime}" -count=1 -timeout=25m -v > "${f}" 2>&1 &
    local gp=$!
    lg_start "${gp}" "${OUTDIR}/${tag}.run${run}.cpu"; wait "${gp}"; local rc=$?
    local lgmax; lgmax=$(lg_max "${OUTDIR}/${tag}.run${run}.cpu")
    if [[ ${rc} -ne 0 ]]; then
      abort_campaign "go bench ${tag} run${run} rc=${rc} (see ${f})"; return 1
    fi
    python3 - "${f}" "${tag}" "${run}" "${lgmax}" >> "${RESULTS}" <<'PYF'
import sys, re, json
f, tag, run, lg = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
metrics={}; fails=0; skips=0
for line in open(f):
    if re.search(r"^\s*--- FAIL", line): fails+=1
    if "SKIP" in line: skips+=1
    for v,k in re.findall(r"(\d+(?:\.\d+)?) (rows/sec|exported_rows/sec|avg_ms|rate_limited|ns/op)", line):
        metrics.setdefault(k, []).append(float(v))
print(json.dumps({"ptc":tag,"run":f"run{run}","fails":fails,"skips":skips,
  "loadgen_max_cpu": None if lg=="na" else float(lg),
  "metrics":{k:{"n":len(v),"min":min(v),"max":max(v),"mean":round(sum(v)/len(v),2)} for k,v in metrics.items()}}))
PYF
    python3 -c "
import json
o=json.loads([l for l in open('${RESULTS}') if '\"${tag}\"' in l and 'run${run}' in l][-1])
exit(1 if o['fails']>0 else 0)" || { abort_campaign "go bench ${tag} run${run} had FAILs (see ${f})"; return 1; }
    [[ $(grep -c "SKIP" "${f}") -gt 0 ]] && log "WARN: ${tag} run${run} contains SKIPs (gate/env) — see ${f}"
    log "RESULT ${tag} run${run} (lg_cpu_max=${lgmax})"
    sleep 30
  done
}

cd "${REPO_ROOT}"
# PTC-220-06: s96 s3 read (text -> FORMAT 'CSV') + write (text -> FORMAT 'TEXT');
# timestamped MinIO prefix per constraint.
go_bench PTC-220-06 "BenchmarkScenario96_" "3x" \
  SCENARIO96_OBJSTORE_LIVE=1 SCENARIO96_CLUSTER=${CLUSTER} \
  SCENARIO96_PERF_WRITE_RESOURCE="cloudberry-warehouse/perf-u023-${MINTS}/" || { cd "${SCRIPT_DIR}"; exit 1; }
fp_check || exit 1
# PTC-220-07: s97 hdfs:text read (CSV clause) + hdfs:sequencefile write (bridge)
go_bench PTC-220-07 "BenchmarkScenario97_" "3x" SCENARIO97_HADOOP_LIVE=1 SCENARIO97_CLUSTER=${CLUSTER} || { cd "${SCRIPT_DIR}"; exit 1; }
fp_check || exit 1
# PTC-220-08: s99 jdbc export (bridge clause)
go_bench PTC-220-08 "BenchmarkScenario99_" "3x" SCENARIO99_EXPORT_LIVE=1 SCENARIO99_CLUSTER=${CLUSTER} || { cd "${SCRIPT_DIR}"; exit 1; }
fp_check || exit 1

# PTC-220-09: s105 PXF status read (OIDC path — honest skip if Keycloak unreachable)
go_bench PTC-220-09 "BenchmarkScenario105_" "50x" SCENARIO105_PXF_LIVE=1 SCENARIO105_CLUSTER=${CLUSTER} || true

# PTC-220-10/11: s107 + s108 API read latency (basic auth; 429 tolerated)
go_bench PTC-220-10 "BenchmarkScenario107_" "50x" SCENARIO107_LIVE=1 SCENARIO107_CLUSTER=${CLUSTER} \
  SCENARIO107_API_BASE="http://127.0.0.1:${API_LOCAL}" SCENARIO107_API_USER=admin SCENARIO107_API_PASS="${PW}" || { cd "${SCRIPT_DIR}"; exit 1; }
go_bench PTC-220-11 "BenchmarkScenario108_" "50x" SCENARIO108_LIVE=1 SCENARIO108_CLUSTER=${CLUSTER} \
  SCENARIO108_API_BASE="http://127.0.0.1:${API_LOCAL}" SCENARIO108_API_USER=admin SCENARIO108_API_PASS="${PW}" || { cd "${SCRIPT_DIR}"; exit 1; }

# PTC-220-12: s109 VM query latency
go_bench PTC-220-12 "BenchmarkScenario109_" "30x" SCENARIO109_VM_BASE="${VM}" || { cd "${SCRIPT_DIR}"; exit 1; }

# PTC-220-13/14/15: admission + netpol + storage admission (dry-run / read-only)
go_bench PTC-220-13 "BenchmarkScenario110_" "20x" SCENARIO110_LIVE=1 SCENARIO110_NAMESPACE=${NS} || { cd "${SCRIPT_DIR}"; exit 1; }
go_bench PTC-220-14 "BenchmarkScenario111_" "20x" SCENARIO111_LIVE=1 SCENARIO111_CLUSTER=${CLUSTER} SCENARIO111_NAMESPACE=${NS} || { cd "${SCRIPT_DIR}"; exit 1; }
go_bench PTC-220-15 "BenchmarkScenario113_" "20x" SCENARIO113_LIVE=1 SCENARIO113_NAMESPACE=${NS} || { cd "${SCRIPT_DIR}"; exit 1; }

# PTC-220-16: s119 storage API (disabled-path; real status codes recorded)
go_bench PTC-220-16 "BenchmarkScenario119_" "30x" SCENARIO119_LIVE=1 SCENARIO119_CLUSTER=${CLUSTER} \
  SCENARIO119_TARGET="127.0.0.1:${API_LOCAL}" SCENARIO119_USER=admin SCENARIO119_PASS="${PW}" || { cd "${SCRIPT_DIR}"; exit 1; }
cd "${SCRIPT_DIR}"

# =============================================================================
# PTC-220-17 — in-process suite (make test-perf methodology), 3x
# =============================================================================
for run in $(seq 1 ${RUNS}); do
  f="${OUTDIR}/PTC-220-17.run${run}.log"
  ( cd "${REPO_ROOT}" && go test ./test/perf/... -tags=e2e -run='^$' -bench=. -benchmem -count=1 -timeout=30m -v ) > "${f}" 2>&1 &
  gp=$!; lg_start ${gp} "${OUTDIR}/PTC-220-17.run${run}.cpu"; wait ${gp}; rc=$?
  lgmax=$(lg_max "${OUTDIR}/PTC-220-17.run${run}.cpu")
  [[ ${rc} -eq 0 ]] || { abort_campaign "in-process suite run${run} rc=${rc} (see ${f})"; exit 1; }
  nb=$(grep -c '^Benchmark' "${f}")
  emit "PTC-220-17.run${run}" "{\"ptc\":\"PTC-220-17\",\"run\":\"run${run}\",\"exit\":0,\"benchmarks\":${nb},\"loadgen_max_cpu\":${lgmax:-null}}"
  sleep 10
done

log "$(target_sample)"
fp_final=$(mydb_fingerprint)
log "=== CAMPAIGN COMPLETE — abort=${ABORT_FLAG}, mydb=${fp_final} ==="
echo "{\"ptc\":\"END\",\"mydb_fingerprint\":\"${fp_final}\",\"aborted\":${ABORT_FLAG},\"ts\":\"$(date -u +%FT%TZ)\"}" >> "${RESULTS}"
