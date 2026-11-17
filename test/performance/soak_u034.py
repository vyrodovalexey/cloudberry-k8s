#!/usr/bin/env python3
"""
U034 — 1-hour acceptance-cluster soak driver (TASK-U034).

Streams (continuous, moderate steady-state; all deadlines bounded):
  sql      rotating U017-verified index/equality/range queries + one 3-way
           join aggregate per cycle, expected results pinned (closed form)
  pxf      one external-table select per ~120s, rotating 6 formats
           (U019 objects via u034_ext readable fixture; counts pinned)
  api      operator /healthz (unauth) + authed cluster GET ~1/10s via
           port-forward (0.1 authed RPS — far below the ~7 RPS bcrypt
           ceiling; the API has no token endpoint, basic auth only)
  wedge    DEF-8 watch: fresh coordinator connection (pg_isready + psql
           SELECT 1) every 30s; on failure captures pod READY state,
           restarts, and container logs at that instant
  observe  30s sampler: per-container cgroup memory (current/max/
           working_set), restart counts (delta → incident capture),
           OOMKilled events + lastState, CR phase/mirroring/conditions/
           lastReconcileTime, operator pod state
  annotate 5-min benign CR annotation (reconcile pressure) + mydb
           fingerprint assert

Environment contract (all overridable):
  SOAK_NS=cloudberry-test  SOAK_CLUSTER=acceptance-test  SOAK_DURATION=3600
  SOAK_EVID=<dir>          SOAK_VM=http://127.0.0.1:8428
  SOAK_API_PORT=8190       SOAK_HEALTH_PORT=8181
Requires: kubectl ctx docker-desktop, cluster Running, mydb fingerprint
matching FP_EXPECTED, u034_ext fixture present.

CSV outputs (one row per observation, ts=epoch_ms UTC):
  sql.csv pxf.csv api.csv wedge.csv observe.csv annotate.csv incidents/
"""
import base64, csv, json, os, queue, subprocess, sys, threading, time, urllib.request, urllib.error

NS = os.environ.get("SOAK_NS", "cloudberry-test")
CLUSTER = os.environ.get("SOAK_CLUSTER", "acceptance-test")
DURATION = int(os.environ.get("SOAK_DURATION", "3600"))
EVID = os.environ.get("SOAK_EVID", ".opencode/output/u034-evidence")
VM = os.environ.get("SOAK_VM", "http://127.0.0.1:8428")
API_PORT = os.environ.get("SOAK_API_PORT", "8190")
HEALTH_PORT = os.environ.get("SOAK_HEALTH_PORT", "8181")
COORD = f"{CLUSTER}-coordinator-0"
STANDBY = f"{CLUSTER}-coordinator-standby-0"
DB_PODS = [COORD, STANDBY, f"{CLUSTER}-segment-primary-0", f"{CLUSTER}-segment-primary-1",
           f"{CLUSTER}-segment-mirror-0", f"{CLUSTER}-segment-mirror-1"]
PSQL = "/usr/local/cloudberry-db/bin/psql"
PGIR = "/usr/local/cloudberry-db/bin/pg_isready"
FP_EXPECTED = "222647789 80000 480000 960000 1314280100.00"
PXF_FP_EXPECTED = "175000 15312587500 15312587500"
PXF_FORMATS = ["s3_text", "s3_parquet", "s3_avro", "hdfs_text", "hdfs_parquet", "hdfs_avro"]
MEM_LIMITS = {  # bytes; for headroom math
    (COORD, "cloudberry"): 2 * 2**30, (STANDBY, "cloudberry"): 2 * 2**30,
    **{(f"{CLUSTER}-segment-{r}-{i}", "cloudberry"): 2**30 for r in ("primary", "mirror") for i in (0, 1)},
    **{(f"{CLUSTER}-segment-{r}-{i}", "pxf"): 2**30 for r in ("primary", "mirror") for i in (0, 1)},
}
SQL_STMTS = [  # (id, sql, expected output string)
    ("S1", "SELECT id||'|'||region FROM customers WHERE email='user_4242@example.com'", "4242|eu-central"),
    ("S2", "SELECT count(*) FROM customers WHERE region='sa-east'", "10000"),
    ("S3", "SELECT count(*) FROM orders WHERE order_date BETWEEN '2023-06-01' AND '2023-06-30'", "19740"),
    ("S4", "SELECT count(*)||'|'||sum(amount) FROM orders WHERE customer_id=4242", "6|2260.46"),
    ("S5", "SELECT string_agg(id||'|'||qty||'|'||price, ' / ' ORDER BY id) FROM line_items WHERE order_id=100001",
     "100000|1|0.99 / 580000|1|300.99"),
    ("S6", "SELECT count(*) FROM orders WHERE order_date BETWEEN '2024-07-01' AND '2024-12-30'", "120231"),
]
JOIN_SQL = ("SELECT region||':'||count(*) FROM customers c JOIN orders o ON o.customer_id=c.id "
            "JOIN line_items li ON li.order_id=o.id GROUP BY c.region ORDER BY region")
JOIN_EXPECTED = ("af-south:120000\nap-northeast:120000\nap-south:120000\neu-central:120000\n"
                 "eu-west:120000\nsa-east:120000\nus-east:120000\nus-west:120000")

stop = threading.Event()
T0 = None
lock = threading.Lock()
counts = {s: {"ok": 0, "err": 0, "mismatch": 0} for s in ("sql", "pxf", "api", "wedge", "annotate", "fp")}
alerts = []

def now_ms(): return int(time.time() * 1000)
def iso(ts=None): return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts if ts is not None else time.time()))
def log(msg):
    with lock:
        print(f"[{iso()}] {msg}", flush=True)
        with open(os.path.join(EVID, "driver.log"), "a") as f:
            f.write(f"[{iso()}] {msg}\n")

def alert(msg):
    alerts.append((iso(), msg))
    log(f"ALERT: {msg}")

def write_row(name, row):
    with lock:
        with open(os.path.join(EVID, name), "a", newline="") as f:
            csv.writer(f).writerow(row)

def run(argv, timeout, capture_err=True):
    """Bounded subprocess; returns (rc, stdout, stderr_or_exception_text)."""
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, (p.stderr if capture_err else "")
    except subprocess.TimeoutExpired:
        return 124, "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 125, "", f"{type(e).__name__}: {e}"

def kexec(pod, container, cmd_argv, timeout=45):
    return run(["kubectl", "exec", "-n", NS, pod, "-c", container, "--"] + cmd_argv, timeout)

def psql(pod, db, sql, timeout=45, connect_timeout=10):
    return kexec(pod, "cloudberry",
                 ["sh", "-c", f"PGCONNECTTIMEOUT={connect_timeout} {PSQL} -X -q -tA -d {db} -c {json.dumps(sql)}"],
                 timeout)

# ---------------------------------------------------------------- streams --
def sql_stream():
    i = 0
    while not stop.is_set():
        for sid, sql, exp in SQL_STMTS:
            if stop.is_set(): return
            t = now_ms(); rc, out, err = psql(COORD, "mydb", sql); dur = now_ms() - t
            got = out.strip()
            status = "ok" if rc == 0 and got == exp else ("mismatch" if rc == 0 else "error")
            if status != "ok":
                counts["sql"][status] += 1
                log(f"sql {sid} {status}: rc={rc} got={got!r} err={err.strip()[:300]!r}")
            else:
                counts["sql"]["ok"] += 1
            write_row("sql.csv", [t, sid, status, dur, rc, got if status != "ok" else ""])
            stop.wait(1.0)
        if stop.is_set(): return
        t = now_ms(); rc, out, err = psql(COORD, "mydb", JOIN_SQL, timeout=60); dur = now_ms() - t
        got = out.strip()
        status = "ok" if rc == 0 and got == JOIN_EXPECTED else ("mismatch" if rc == 0 else "error")
        if status != "ok":
            counts["sql"][status] += 1
            alert(f"sql JOIN {status}: rc={rc} got={got!r} err={err.strip()[:300]!r}")
        else:
            counts["sql"]["ok"] += 1
        write_row("sql.csv", [t, "JOIN", status, dur, rc, got if status != "ok" else ""])
        i += 1
        stop.wait(2.0)

def pxf_stream():
    rot = 0
    while not stop.is_set():
        fmt = PXF_FORMATS[rot % len(PXF_FORMATS)]; rot += 1
        t = now_ms()
        rc, out, err = psql(COORD, "postgres", f"SELECT count(*)||' '||sum(id)||' '||sum(amount) FROM u034_ext.rext_{fmt}",
                            timeout=150)
        dur = now_ms() - t
        got = out.strip()
        status = "ok" if rc == 0 and got == PXF_FP_EXPECTED else ("mismatch" if rc == 0 else "error")
        counts["pxf"][status if status in counts["pxf"] else "err"] += 1
        if status != "ok":
            log(f"pxf {fmt} {status}: rc={rc} got={got!r} err={err.strip()[:300]!r}")
        write_row("pxf.csv", [t, fmt, status, dur, rc, got if status != "ok" else ""])
        stop.wait(120)

def api_stream(pf_restart_evt):
    global AUTH
    consecutive = 0
    while not stop.is_set():
        t = now_ms()
        h_rc, h_code, h_dur = -1, -1, -1
        a_rc, a_code, a_dur = -1, -1, -1
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{HEALTH_PORT}/healthz")
            s = time.time()
            with urllib.request.urlopen(req, timeout=8) as r:
                h_code, h_dur = r.status, int((time.time() - s) * 1000)
            h_rc = 0
        except Exception as e:
            h_rc = 1
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{API_PORT}/api/v1alpha1/clusters/{CLUSTER}",
                                         headers={"Authorization": "Basic " + AUTH})
            s = time.time()
            with urllib.request.urlopen(req, timeout=8) as r:
                a_code, a_dur = r.status, int((time.time() - s) * 1000)
            a_rc = 0
        except urllib.error.HTTPError as e:
            a_rc, a_code = e.code, e.code
        except Exception:
            a_rc = 1
        okk = h_rc == 0 and h_code == 200 and a_rc == 0 and a_code == 200
        counts["api"]["ok" if okk else "err"] += 1
        if not okk:
            log(f"api fail: healthz rc={h_rc} code={h_code}; authed rc={a_rc} code={a_code}")
            consecutive += 1
            if consecutive >= 3:
                log("api: >=3 consecutive failures — signalling port-forward restart")
                pf_restart_evt.set()
                consecutive = 0
        else:
            consecutive = 0
        write_row("api.csv", [t, h_rc, h_code, h_dur, a_rc, a_code, a_dur])
        stop.wait(10)

def wedge_probe():
    """Fresh connection probe. Returns (t, psql_ok, detail)."""
    t = now_ms()
    rc, out, err = kexec(COORD, "cloudberry",
                         ["sh", "-c",
                          f"PGCONNECTTIMEOUT=8 {PGIR} -q; echo PGIR_RC=$?; "
                          f"PGCONNECTTIMEOUT=8 {PSQL} -X -q -tA -d mydb -c 'SELECT 1'"],
                         timeout=25)
    dur = now_ms() - t
    pgir_ok = "PGIR_RC=0" in out
    psql_ok = rc == 0 and out.strip().endswith("1")
    detail = ""
    if not psql_ok:
        detail = f"rc={rc} out={out.strip()[:200]!r} err={err.strip()[:300]!r}"
    return t, dur, pgir_ok, psql_ok, detail

def wedge_stream():
    while not stop.is_set():
        t, dur, pgir_ok, psql_ok, detail = wedge_probe()
        status = "ok" if psql_ok else "FAIL"
        counts["wedge"]["ok" if psql_ok else "err"] += 1
        write_row("wedge.csv", [t, status, dur, pgir_ok, detail])
        if not psql_ok:
            alert(f"DEF-8 wedge probe FAIL: {detail}")
            capture_incident(t, "wedge-fail")
        stop.wait(30)

def capture_incident(t, kind):
    os.makedirs(os.path.join(EVID, "incidents"), exist_ok=True)
    tag = f"{kind}_{t}"
    rc, out, _ = run(["kubectl", "get", "pod", "-n", NS, COORD, "-o", "json"], 20)
    if rc == 0:
        with open(os.path.join(EVID, "incidents", tag + ".pod.json"), "w") as f:
            f.write(out)
        try:
            pj = json.loads(out)
            ready = [c["name"] + "=" + ("1" if c.get("ready") else "0") for c in pj["status"]["containerStatuses"]]
            rdy_cond = next((c["status"] for c in pj["status"].get("conditions", []) if c["type"] == "Ready"), "?")
            restarts = sum(c.get("restartCount", 0) for c in pj["status"]["containerStatuses"])
            log(f"incident {tag}: Ready={rdy_cond} containers[{','.join(ready)}] restarts={restarts}")
            if rdy_cond == "True":
                alert(f"DEF-8 SIGNATURE candidate: connection failure while pod Ready ({','.join(ready)})")
        except Exception as e:
            log(f"incident parse error: {e}")
    for cname in ("cloudberry",):
        rc, out, _ = run(["kubectl", "logs", "-n", NS, COORD, "-c", cname, "--tail=100", "--timestamps"], 25)
        if rc == 0:
            with open(os.path.join(EVID, "incidents", tag + f".{cname}.log"), "w") as f:
                f.write(out)
    rc, out, _ = run(["kubectl", "get", "events", "-n", NS, "--sort-by=.lastTimestamp", "-o", "json"], 20)
    if rc == 0:
        with open(os.path.join(EVID, "incidents", tag + ".events.json"), "w") as f:
            f.write(out)

def read_cgroup(pod, container):
    rc, out, _ = kexec(pod, container,
                       ["sh", "-c",
                        "cat /sys/fs/cgroup/memory.current; echo ---; grep -E '^(inactive_file|anon) ' /sys/fs/cgroup/memory.stat; echo ---; cat /sys/fs/cgroup/memory.max"],
                       timeout=20)
    if rc != 0:
        return None
    try:
        parts = [p.strip() for p in out.split("---")]
        cur = int(parts[0].split()[0])
        stats = dict(line.split() for line in parts[1].splitlines() if line.strip())
        mx = int(parts[2].split()[0])
        ws = max(cur - int(stats.get("inactive_file", 0)), 0)
        return cur, ws, mx
    except Exception:
        return None

def observe_stream(baseline_restarts):
    targets = [(p, "cloudberry") for p in DB_PODS] + [(f"{CLUSTER}-segment-{r}-{i}", "pxf")
                                                      for r in ("primary", "mirror") for i in (0, 1)]
    while not stop.is_set():
        t = now_ms()
        # memory
        for pod, ctr in targets:
            m = read_cgroup(pod, ctr)
            if m:
                cur, ws, mx = m
                write_row("observe.csv", [t, pod, ctr, "mem_current", cur])
                write_row("observe.csv", [t, pod, ctr, "mem_working_set", ws])
                lim = MEM_LIMITS.get((pod, ctr), mx)
                if cur >= 0.95 * lim:
                    alert(f"memory {pod}/{ctr} current={cur} >=95% of limit {lim}")
        # restarts / ready / OOM via pod json
        rc, out, _ = run(["kubectl", "get", "pods", "-n", NS, "-o", "json"], 25)
        if rc == 0:
            try:
                for p in json.loads(out)["items"]:
                    n = p["metadata"]["name"]
                    if not (n.startswith(CLUSTER) and any(k in n for k in ("coordinator", "segment"))):
                        continue
                    if p.get("status", {}).get("phase") != "Running":
                        alert(f"pod {n} phase={p.get('status', {}).get('phase')}")
                    for c in p["status"].get("containerStatuses", []):
                        rst = c.get("restartCount", 0)
                        write_row("observe.csv", [t, n, c["name"], "restarts", rst])
                        base = baseline_restarts.get((n, c["name"]), 0)
                        if rst > base:
                            alert(f"NEW RESTART {n}/{c['name']}: {base}->{rst}")
                            capture_incident(t, f"restart_{n.replace(CLUSTER+'-','')}_{c['name']}")
                            baseline_restarts[(n, c["name"])] = rst
                        ls = c.get("lastState", {}).get("terminated", {})
                        if ls.get("reason") == "OOMKilled":
                            alert(f"OOMKilled lastState {n}/{c['name']} at {ls.get('finishedAt')}")
                        if not c.get("ready", False):
                            alert(f"container not ready {n}/{c['name']}")
            except Exception as e:
                log(f"observe pod-parse error: {e}")
        # OOM events
        rc, out, _ = run(["kubectl", "get", "events", "-n", NS, "--field-selector", "reason=OOMKilling", "-o", "json"], 20)
        if rc == 0 and '"items":[]' not in out.replace(" ", ""):
            try:
                items = json.loads(out)["items"]
                if items:
                    alert(f"OOMKilling events present: {len(items)}")
            except Exception:
                pass
        # CR state
        rc, out, _ = run(["kubectl", "get", "cloudberrycluster", CLUSTER, "-n", NS, "-o", "json"], 20)
        if rc == 0:
            try:
                s = json.loads(out)["status"]
                write_row("observe.csv", [t, CLUSTER, "-", "phase", s.get("phase", "")])
                write_row("observe.csv", [t, CLUSTER, "-", "mirroring", s.get("mirroringStatus", "")])
                write_row("observe.csv", [t, CLUSTER, "-", "segmentsReady", s.get("segmentsReady", "")])
                write_row("observe.csv", [t, CLUSTER, "-", "lastReconcileTime", s.get("lastReconcileTime", "")])
                write_row("observe.csv", [t, CLUSTER, "-", "activeQueries", s.get("activeQueries", "")])
                write_row("observe.csv", [t, CLUSTER, "-", "queuedQueries", s.get("queuedQueries", "")])
                if s.get("phase") != "Running":
                    alert(f"CR phase={s.get('phase')}")
                if s.get("mirroringStatus") != "InSync":
                    alert(f"mirroringStatus={s.get('mirroringStatus')}")
                if s.get("failedSegments"):
                    alert(f"failedSegments={s.get('failedSegments')}")
            except Exception as e:
                log(f"observe CR-parse error: {e}")
        stop.wait(30)

def annotate_stream():
    n = 0
    while not stop.is_set():
        t = now_ms()
        rc, out, err = run(["kubectl", "annotate", "--overwrite", "-n", NS, "cloudberrycluster", CLUSTER,
                            f"soak.u034/tick={iso()}", "-o", "name"], 25)
        okk = rc == 0
        counts["annotate"]["ok" if okk else "err"] += 1
        write_row("annotate.csv", [t, n, "annotate", rc, out.strip()])
        if not okk:
            log(f"annotate rc={rc} err={err.strip()[:200]}")
        stop.wait(60)
        # fingerprint within the same 5-min cadence
        t2 = now_ms()
        rc, out, err = psql(COORD, "mydb",
                            "SELECT pg_database_size('mydb')||' '||(SELECT count(*) FROM customers)||' '||"
                            "(SELECT count(*) FROM orders)||' '||(SELECT count(*) FROM line_items)||' '||"
                            "(SELECT sum(qty*price) FROM line_items)", timeout=90)
        got = out.strip()
        okk = rc == 0 and got == FP_EXPECTED
        counts["fp"]["ok" if okk else "err"] += 1
        write_row("annotate.csv", [t2, n, "fingerprint", rc, got])
        if not okk:
            alert(f"FINGERPRINT DRIFT: rc={rc} got={got!r} err={err.strip()[:200]!r}")
        n += 1
        stop.wait(240)

# ---------------------------------------------------------------- supervisor
def main():
    global T0, AUTH
    os.makedirs(EVID, exist_ok=True)
    os.makedirs(os.path.join(EVID, "incidents"), exist_ok=True)
    log(f"soak start: ns={NS} cluster={CLUSTER} duration={DURATION}s evid={EVID}")

    # preflight inside driver (belt & braces; wrapper did the full gate)
    rc, out, _ = psql(COORD, "mydb", "SELECT 1", timeout=30)
    if rc != 0 or out.strip() != "1":
        log(f"PREFLIGHT FAIL: coordinator SELECT 1 rc={rc} out={out!r}")
        sys.exit(2)
    rc, out, _ = run(["kubectl", "get", "secret", "-n", NS, "cloudberry-operator-admin-password",
                      "-o", "jsonpath={.data.password}"], 20)
    pw = base64.b64decode(out.strip()).decode()
    AUTH = base64.b64encode(f"admin:{pw}".encode()).decode()

    # baseline restarts
    baseline = {}
    rc, out, _ = run(["kubectl", "get", "pods", "-n", NS, "-o", "json"], 25)
    for p in json.loads(out)["items"]:
        n = p["metadata"]["name"]
        if n.startswith(CLUSTER):
            for c in p.get("status", {}).get("containerStatuses", []):
                baseline[(n, c["name"])] = c.get("restartCount", 0)
    log(f"baseline restarts: {sorted(baseline.items())}")

    # port-forwards
    pfs = [subprocess.Popen(["kubectl", "-n", NS, "port-forward", "deployment/cloudberry-operator",
                             f"{API_PORT}:8090"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
           subprocess.Popen(["kubectl", "-n", NS, "port-forward", "deployment/cloudberry-operator",
                             f"{HEALTH_PORT}:8081"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
    time.sleep(4)
    pf_restart_evt = threading.Event()

    threads = [threading.Thread(target=sql_stream, name="sql", daemon=True),
               threading.Thread(target=pxf_stream, name="pxf", daemon=True),
               threading.Thread(target=api_stream, args=(pf_restart_evt,), name="api", daemon=True),
               threading.Thread(target=wedge_stream, name="wedge", daemon=True),
               threading.Thread(target=observe_stream, args=(baseline,), name="observe", daemon=True),
               threading.Thread(target=annotate_stream, name="annotate", daemon=True)]
    T0 = time.time()
    log(f"T0={iso(T0)} epoch={T0:.0f} — streams started: {[t.name for t in threads]}")
    for th in threads:
        th.start()

    deadline = T0 + DURATION
    last_hb = 0
    while time.time() < deadline:
        time.sleep(min(5, max(0.1, deadline - time.time())))
        if pf_restart_evt.is_set():
            pf_restart_evt.clear()
            log("restarting operator port-forwards")
            for p in pfs:
                p.terminate()
            time.sleep(2)
            pfs = [subprocess.Popen(["kubectl", "-n", NS, "port-forward", "deployment/cloudberry-operator",
                                     f"{API_PORT}:8090"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
                   subprocess.Popen(["kubectl", "-n", NS, "port-forward", "deployment/cloudberry-operator",
                                     f"{HEALTH_PORT}:8081"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
            time.sleep(4)
        if time.time() - last_hb >= 60:
            last_hb = time.time()
            elapsed = int(time.time() - T0)
            c = {k: dict(v) for k, v in counts.items()}
            log(f"heartbeat t+{elapsed}s {json.dumps(c)}")
    log(f"deadline reached t+{int(time.time()-T0)}s — stopping streams")
    stop.set()
    for th in threads:
        th.join(timeout=10)
    for p in pfs:
        p.terminate()
    log("port-forwards terminated")
    log(f"final counts: {json.dumps(counts)}")
    log(f"alerts ({len(alerts)}):")
    for ts, m in alerts:
        log(f"  {ts} {m}")
    log("driver done")

if __name__ == "__main__":
    main()
