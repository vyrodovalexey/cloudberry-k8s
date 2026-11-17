#!/usr/bin/env python3
"""Merge the three U023 campaign result sets into one results-all.jsonl.

Provenance rules (each line carries its origin):
- campaign-1 (083318): PTC-220-01 rows replaced by the raw-file reparse
  (parser bug lost codes/percentiles; raw .txt intact). PTC-220-02 kept.
  Its three PTC-220-03 rows were SEQUENTIAL loops (superseded methodology)
  -> relabeled PTC-220-03SEQ. Its two PTC-220-04 rows were measured in the
  bcrypt-congested window (see hang/degradation finding) -> relabeled
  PTC-220-04DEG (excluded from stepped-load tables; kept as evidence).
- campaign-2 (091536): PTC-220-02 rows relabeled PTC-220-02v (variance set;
  the block re-ran due to the resume off-by-one). Knee run1 csv recovered
  separately as evidence (no JSONL row existed - the hang).
- campaign-3 (latest u023-* dir): canonical rows for 03..17.
"""
import json, sys, os, glob, re

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # test/performance
ydirs = sorted(glob.glob(os.path.join(base, ".yandextank", "u023-2026*")))
assert len(ydirs) >= 4, f"expected >=4 campaign dirs, got {ydirs}"
c1, c2, c3, c4 = ydirs[-4], ydirs[-3], ydirs[-2], ydirs[-1]
out = os.path.join(os.path.dirname(ydirs[-1]), "results-all.jsonl")
rows = []

def load(d):
    f = os.path.join(d, "results.jsonl")
    if not os.path.exists(f): return []
    return [json.loads(l) for l in open(f) if l.strip()]

for r in load(c1):
    r["_src"] = os.path.basename(c1)
    if r.get("ptc") == "PTC-220-01": continue                     # superseded by reparse
    if r.get("ptc") == "PTC-220-03": r["ptc"] = "PTC-220-03SEQ"   # sequential (superseded)
    if r.get("ptc") == "PTC-220-04": r["ptc"] = "PTC-220-04DEG"   # congested-window evidence
    rows.append(r)
rep = os.path.join(c1, "PTC-220-01.reparse.jsonl")
if os.path.exists(rep):
    for l in open(rep):
        r = json.loads(l); r["_src"] = os.path.basename(c1); rows.append(r)

for r in load(c2):
    r["_src"] = os.path.basename(c2)
    if r.get("ptc") == "PTC-220-02": r["ptc"] = "PTC-220-02v"
    else: r["ptc"] = r.get("ptc", "?") + "HANG"  # anything else from the hung run
    rows.append(r)

# campaign-3 (121037 is c4; the one before is c3): guard-inversion run — only
# 01/02 executed (relabeled variance sets); nothing else emitted.
for r in load(c3):
    r["_src"] = os.path.basename(c3)
    if r.get("ptc") == "PTC-220-01": r["ptc"] = "PTC-220-01v"
    elif r.get("ptc") == "PTC-220-02": r["ptc"] = "PTC-220-02v2"
    else: r["ptc"] = r.get("ptc", "?") + "GUARD"
    rows.append(r)

for r in load(c4):
    r["_src"] = os.path.basename(c4)
    rows.append(r)

with open(out, "w") as fh:
    for r in rows: fh.write(json.dumps(r) + "\n")
print(f"merged {len(rows)} rows from {os.path.basename(c1)}, {os.path.basename(c2)}, {os.path.basename(c3)}, {os.path.basename(c4)} -> {out}")
from collections import Counter
print(Counter(r.get("ptc") for r in rows))
