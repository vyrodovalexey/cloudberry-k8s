#!/usr/bin/env python3
"""Move 5 panels from the collapsed "Backup & Restore Health" row (122) into the
top-level "Backup & Restore" section (row 39), refactored per-cluster.

Move: 128 Backup Job Status Over Time
      129 Backup Outcomes Rate (success vs failed)
      134 Restore Validation Outcomes (success vs failed)
      135 Restore/Migration Outcomes Rate (success vs failed)
      136 Restore Duration p95 (Migration)

Backup & Restore section currently has 3 panel rows: y=92, y=100, y=108
(ends at y=116 where the Data Loading row begins). Add 3 new rows:
  y=116: 128 (x0 w12)  + 129 (x12 w12)
  y=124: 134 (x0 w12)  + 135 (x12 w12)
  y=132: 136 (x0 w24)
Section grows by 24; shift everything at y>=116 down by 24.
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

MOVE = [128, 129, 134, 135, 136]
I = '{instance=~"$instance"}'
IC = '{instance=~"$instance",cluster!=""}'
CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

# Per-cluster exprs + legends + new positions.
NEW = {
    128: {
        "expr": f'sum by (cluster, operation) (cloudberry_backup_job_status{{instance=~"$instance",cluster!="",operation="backup"}}) or {CI0}',
        "legend": "{{cluster}}",
        "grid": {"x": 0, "y": 116, "w": 12, "h": 8},
    },
    129: {
        "expr": f'sum by (cluster, type, result) (rate(cloudberry_backup_total{IC}[5m])) or {CI0}',
        "legend": "{{cluster}} {{type}}/{{result}}",
        "grid": {"x": 12, "y": 116, "w": 12, "h": 8},
    },
    134: {
        "expr": f'sum by (cluster, result) (increase(cloudberry_restore_validation_total{IC}[1h])) or {CI0}',
        "legend": "{{cluster}} {{result}}",
        "grid": {"x": 0, "y": 124, "w": 12, "h": 8},
    },
    135: {
        "expr": f'sum by (cluster, result) (rate(cloudberry_restore_total{IC}[5m])) or {CI0}',
        "legend": "{{cluster}} {{result}}",
        "grid": {"x": 12, "y": 124, "w": 12, "h": 8},
    },
    136: {
        "expr": f'histogram_quantile(0.95, sum by (cluster, le) (rate(cloudberry_restore_duration_seconds_bucket{IC}[5m]))) or {CI0}',
        "legend": "{{cluster}} p95",
        "grid": {"x": 0, "y": 132, "w": 24, "h": 8},
    },
}

# 1) Extract the 5 panels out of row 122's nested panels.
row122 = next(p for p in panels if p.get("id") == 122)
moved = {p["id"]: p for p in row122["panels"] if p.get("id") in MOVE}
assert len(moved) == 5, f"expected {MOVE}, found {list(moved)}"
row122["panels"] = [p for p in row122["panels"] if p.get("id") not in MOVE]

# 2) Shift all TOP-LEVEL panels at y>=116 down by 24 (make room for 3 new rows).
#    (collapsed-row 122's own y stays consistent; its nested panels are internal.)
for p in panels:
    if p["gridPos"]["y"] >= 116:
        p["gridPos"]["y"] += 24

# 3) Refactor + reposition the moved panels and add them at top level.
for pid in MOVE:
    p = moved[pid]
    cfg = NEW[pid]
    p["gridPos"] = cfg["grid"]
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = cfg["expr"]
    p["targets"][0]["legendFormat"] = cfg["legend"]
    p["targets"][0].pop("instant", None)
    panels.append(p)

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved 5 health panels into Backup & Restore, refactored per-cluster")
