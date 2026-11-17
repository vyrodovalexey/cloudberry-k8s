#!/usr/bin/env python3
"""U023 campaign analysis: reads results.jsonl (+ optional per-request CSVs),
emits per-scenario markdown tables, ASCII charts (latency-vs-rate, codes-vs-rate,
offered-vs-achieved, latency-over-time, target-utilization) to the artifacts dir.
Charts are generated from RAW data, never from summaries."""
import json, sys, os, subprocess, urllib.request, urllib.parse, datetime

RESULTS = sys.argv[1]
ART = sys.argv[2]
L7T_CSV = sys.argv[3] if len(sys.argv) > 3 else None   # latency-over-time evidence CSV
VM = "http://127.0.0.1:8428"
os.makedirs(ART, exist_ok=True)

rows = [json.loads(l) for l in open(RESULTS) if l.strip()]
def sel(ptc, field=None):
    out = [r for r in rows if r.get("ptc") == ptc and field is None or (r.get("ptc") == ptc and field in r)]
    return out

def f(x, nd=1):
    try: return f"{float(x):.{nd}f}"
    except (TypeError, ValueError): return "n/a"

lines = []
A = lines.append

# ---------- PTC-220-01 health endpoints ----------
A("## PTC-220-01 — health endpoints (100 RPS x 60s, hey)\n")
A("| Run | Endpoint | Port | Reqs | 2xx | p50 ms | p95 ms | p99 ms | RPS | gen CPU% |")
A("|---|---|---|---|---|---|---|---|---|---|")
for r in sorted([r for r in rows if r.get("ptc")=="PTC-220-01"], key=lambda r: r["run"]):
    ep = r["run"].split(".A_")[1].split(".run")[0]
    codes = r.get("codes", {})
    A(f"| {r['run'].split('.run')[1]} | /{ep.split('_')[0]} | {ep.split('_')[1]} | {r['requests']} | {codes.get('200',0)} | "
      f"{f(r.get('p50_s',0)*1000 if r.get('p50_s') else None)} | {f(r.get('p95_s',0)*1000 if r.get('p95_s') else None)} | "
      f"{f(r.get('p99_s',0)*1000 if r.get('p99_s') else None)} | {f(r.get('achieved_rps'),2)} | {f(r.get('loadgen_max_cpu'))} |")

# ---------- PTC-220-02 ----------
A("\n## PTC-220-02 — API read path (9 sequential authed requests)\n")
A("| Run | Endpoint | Codes | avg ms | p50 ms | max ms |")
A("|---|---|---|---|---|---|")
for r in sorted([r for r in rows if r.get("ptc")=="PTC-220-02"], key=lambda r: (r["run"], r["ep"])):
    A(f"| {r['run']} | GET /{r['ep'].split('/')[-1] if '/' in r['ep'] else 'clusters'} | {r['codes']} | {f(r['ok_avg_ms'])} | {f(r['ok_p50_ms'])} | {f(r['ok_max_ms'])} |")

# ---------- PTC-220-03 ----------
A("\n## PTC-220-03 — rate-limiter knee (configured limit 600/min; baseline ran 10/min)\n")
A("| Run | Reqs | first 429 at | 401 n / avg ms | 429 n / avg ms | other | gen CPU% |")
A("|---|---|---|---|---|---|---|")
for r in sorted([r for r in rows if r.get("ptc") in ("PTC-220-03","PTC-220-03E")], key=lambda r: (r.get("ptc",""), r["run"])):
    c401, c429 = r.get("c401") or {}, r.get("c429") or {}
    A(f"| {r.get('ptc')} {r['run']} | {r['requests']} | {r.get('first_429_at')} | {c401.get('n',0)} / {f(c401.get('avg_ms'))} | {c429.get('n',0)} / {f(c429.get('avg_ms'))} | {r.get('other')} | {f(r.get('loadgen_max_cpu'))} |")
for r in sorted([r for r in rows if r.get("ptc")=="PTC-220-03SEQ"], key=lambda r: r["run"]):
    A(f"| (SEQ, superseded) {r['run']} | {r['requests']} | none (see report §knee) | {r.get('ok_n',0)} / {f(r.get('ok_avg_ms'))} | {r.get('rl_n',0)} / {f(r.get('rl_avg_ms'))} | {r.get('other')} | {f(r.get('loadgen_max_cpu'))} |")

# ---------- PTC-220-04 + charts 1-3 ----------
b4 = sorted([r for r in rows if r.get("ptc")=="PTC-220-04"], key=lambda r: (r["offered_rps"], r["run"]))
A("\n## PTC-220-04 — stepped load (authed, 30s steps)\n")
A("| Step | Run | Reqs | 200 | 429 | other | p50 ms | p95 ms | p99 ms | achieved | ok |")
A("|---|---|---|---|---|---|---|---|---|---|---|")
for r in b4:
    codes = r.get("codes", {})
    other = sum(v for k,v in codes.items() if k not in ("200","429"))
    A(f"| {r['offered_rps']} | {r['run'].split('.run')[1]} | {r['requests']} | {codes.get('200',0)} | {codes.get('429',0)} | {other} | "
      f"{f(r.get('p50_s',0)*1000 if r.get('p50_s') else None)} | {f(r.get('p95_s',0)*1000 if r.get('p95_s') else None)} | "
      f"{f(r.get('p99_s',0)*1000 if r.get('p99_s') else None)} | {f(r.get('achieved_rps'),2)} | {r['achieved_ok']} |")

# aggregate per offered rps (mean over runs)
agg = {}
for r in b4:
    agg.setdefault(r["offered_rps"], []).append(r)
def chart(path, title, series, xlabel, ylabel):
    with open(path, "w") as fh:
        fh.write(title + "\n" + "="*len(title) + "\n")
        allv = []
        for _, vals in series.items():
            for item in vals:
                v = item[1] if isinstance(item, tuple) else item
                if v is not None: allv.append(v)
        if not allv:
            fh.write("(no data)\n"); return
        mx = max(allv); width = 46
        for x, vals in sorted(series.items()):
            for item in vals:
                name, v = (item if isinstance(item, tuple) else ("", item))
                if v is None: continue
                bar = "#" * max(1, int(v / mx * width)) if v > 0 else ""
                fh.write(f"{xlabel}={str(x):>4} {name:<5} {v:>10.1f} |{bar}\n")
        fh.write(f"({ylabel}; one # ≈ {mx/width:.1f})\n")

p50s, p95s, p99s = {}, {}, {}
for rps, rs in agg.items():
    p50s[rps] = [("p50", sum(x["p50_s"] for x in rs if x.get("p50_s"))*1000/len(rs))]
    p95s[rps] = [("p95", sum(x["p95_s"] for x in rs if x.get("p95_s"))*1000/len(rs))]
    p99s[rps] = [("p99", sum(x["p99_s"] for x in rs if x.get("p99_s"))*1000/len(rs))]
merged = {r: p50s[r]+p95s[r]+p99s[r] for r in p50s}
chart(f"{ART}/chart1-latency-vs-rate.txt", "Chart 1 — latency percentiles vs offered rate (means of 3 runs)", merged, "RPS", "ms")

codes2xx, codes429, offered, achieved = {}, {}, {}, {}
for rps, rs in agg.items():
    codes2xx[rps] = [("2xx", sum(x["codes"].get("200",0) for x in rs)/len(rs))]
    codes429[rps] = [("429", sum(x["codes"].get("429",0) for x in rs)/len(rs))]
    offered[rps] = [("offered", float(rps))]
    achieved[rps] = [("achieved", sum(x.get("achieved_rps") or 0 for x in rs)/len(rs))]
chart(f"{ART}/chart2-codes-vs-rate.txt", "Chart 2 — response codes vs offered rate (mean counts per 30s step)",
      {**{k: v for k,v in codes2xx.items()}, **{k: codes429[k] for k in codes429}}, "RPS", "responses/step")
chart(f"{ART}/chart3-offered-vs-achieved.txt", "Chart 3 — offered vs achieved RPS", {**offered, **achieved}, "RPS", "RPS")

# ---------- PTC-220-05 ----------
A("\n## PTC-220-05 — DB queries on mydb (C1–C5; warmup pass discarded, 3x3 timed)\n")
A("| Run | C1 avg | C2 avg | C3 avg | C4 avg | C5 avg (ms) |")
A("|---|---|---|---|---|---|")
for r in sorted([r for r in rows if r.get("ptc")=="PTC-220-05"], key=lambda r: r["run"]):
    a = r["avg_ms"]
    A(f"| {r['run']} | {f(a.get('C1'))} | {f(a.get('C2'))} | {f(a.get('C3'))} | {f(a.get('C4'))} | {f(a.get('C5'))} |")

# ---------- go-bench scenarios ----------
A("\n## PTC-220-06..16 — live go-bench scenarios (3 runs each)\n")
A("| PTC | Run | metrics (mean over iterations) | skips | gen CPU% |")
A("|---|---|---|---|---|")
for r in sorted([r for r in rows if "skips" in r and "metrics" in r], key=lambda r: (r["ptc"], r["run"])):
    m = "; ".join(f"{k}: n={v['n']} mean={v['mean']}" for k,v in (r.get("metrics") or {}).items()) or "(no metrics — see log)"
    A(f"| {r['ptc']} | {r['run']} | {m} | {r['skips']} | {f(r.get('loadgen_max_cpu'))} |")

# ---------- PTC-220-17 ----------
A("\n## PTC-220-17 — in-process suite\n")
for r in sorted([r for r in rows if r.get("ptc")=="PTC-220-17"], key=lambda r: r["run"]):
    A(f"- {r['run']}: exit={r['exit']}, benchmarks={r['benchmarks']}, gen CPU%={f(r.get('loadgen_max_cpu'))}")

# ---------- Chart 4: latency over time (optional CSV: ts,code,latency) ----------
if L7T_CSV and os.path.exists(L7T_CSV):
    import collections, statistics
    secs = collections.defaultdict(list)
    t0 = None
    for line in open(L7T_CSV):
        parts = line.strip().split(",")
        if len(parts) != 3: continue
        ts, code, lat = float(parts[0]), parts[1], float(parts[2])
        if t0 is None: t0 = ts
        secs[int(ts - t0)].append((code, lat))
    with open(f"{ART}/chart4-latency-over-time.txt", "w") as fh:
        fh.write("Chart 4 — steady-state latency over time (per-second p50/p95, ms)\n")
        fh.write("="*62 + "\n")
        mx = max((max(l for _,l in v) for v in secs.values()), default=1)
        for s in sorted(secs):
            lats = [l for c,l in secs[s] if c == "200"]
            if not lats: continue
            lats.sort()
            p50 = lats[len(lats)//2]; p95 = lats[int(len(lats)*0.95)]
            fh.write(f"t+{s:>3}s n={len(lats):<3} p50={p50*1000:8.1f} p95={p95*1000:8.1f} |{'#' * max(1,int(p95*1000/(mx*1000)*46))}\n")

# ---------- Chart 5: target utilization from VM ----------
def vmq(q):
    try:
        u = VM + "/api/v1/query?" + urllib.parse.urlencode({"query": q})
        return json.load(urllib.request.urlopen(u, timeout=10))
    except Exception:
        return None
with open(f"{ART}/chart5-target-utilization.txt", "w") as fh:
    fh.write("Chart 5 — target resource utilization (VictoriaMetrics)\n")
    fh.write("="*52 + "\n")
    for name, q in [
        ("coordinator mem (working set, MiB)",
         'max_over_time(container_memory_working_set_bytes{pod="acceptance-test-coordinator-0",container="cloudberry"}[1m]) / 1024 / 1024'),
        ("operator mem (working set, MiB)",
         'max_over_time(container_memory_working_set_bytes{pod=~"cloudberry-operator-.*",container="cloudberry-operator"}[1m]) / 1024 / 1024'),
        ("coordinator restarts (total)",
         'max_over_time(kube_pod_container_status_restarts_total{pod="acceptance-test-coordinator-0",container="cloudberry"}[1m])'),
    ]:
        res = vmq(q)
        if not res or not res.get("data", {}).get("result"):
            fh.write(f"{name}: (no data)\n"); continue
        r0 = res["data"]["result"][0]
        if "values" not in r0:
            if "value" in r0:
                fh.write(f"{name}: {float(r0['value'][1]):.1f} (instant)\n"); continue
            fh.write(f"{name}: (no series)\n"); continue
        pts = sorted((float(v[0]), float(v[1])) for v in r0["values"])
        if len(pts) == 1:
            fh.write(f"{name}: {pts[0][1]:.1f} (instant)\n"); continue
        mx = max(v for _, v in pts)
        for t, v in pts[::max(1, len(pts)//40)]:
            ts = datetime.datetime.fromtimestamp(t).strftime("%H:%M")
            fh.write(f"{ts} {name[:22]:<22} {v:>9.1f} |{'#' * max(0, int(v/mx*40))}\n")
        fh.write(f"peak={mx:.1f}\n\n")

open(f"{ART}/analysis-tables.md", "w").write("\n".join(lines) + "\n")
print("analysis written to", ART)
