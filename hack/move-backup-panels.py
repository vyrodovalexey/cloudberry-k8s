#!/usr/bin/env python3
"""Move backup/restore/PXF panels into Cluster Overview, per-cluster, no No-data.

Panels: 42 Backup Size, 43 Restores(1h), 117 Backup Last Status,
        118 Time Since Last Successful Backup, 247 Partial Restores(1h),
        248 Failed Restores(1h), 277 PXF Status.

Placement: Cluster Overview has stat rows at y=7,11,15. y=15 has a free slot at
x=20. We add a new 4th stat row at y=19 for the rest. All panels become w=4,h=4.
Every other panel at y>=19 shifts down by 4 to make room for the new row.
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]


def get(pid):
    return next(p for p in panels if p.get("id") == pid)


INST = '{instance=~"$instance"}'
INSTC = '{instance=~"$instance",cluster!=""}'
CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

# New per-cluster exprs with 0 fallback.
EXPRS = {
    42: f'max by (cluster, namespace) (cloudberry_backup_size_bytes{INSTC}) or {CI0}',
    43: f'sum by (cluster, namespace) (increase(cloudberry_restore_total{INSTC}[1h])) or {CI0}',
    117: f'max by (cluster, namespace) (cloudberry_backup_last_status{INSTC}) or {CI0}',
    # Time since last successful backup: if no backup, show 0 (not time-since-epoch).
    118: (
        f'(time() - max by (cluster, namespace) (cloudberry_backup_last_success_timestamp{INSTC}))'
        f' or {CI0}'
    ),
    247: f'sum by (cluster, namespace) (increase(cloudberry_restore_total{{instance=~"$instance",cluster!="",result="partial"}}[1h])) or {CI0}',
    248: f'sum by (cluster, namespace) (increase(cloudberry_restore_total{{instance=~"$instance",cluster!="",result="error"}}[1h])) or {CI0}',
    277: f'max by (cluster, namespace) (cloudberry_pxf_status{INSTC}) or {CI0}',
}

MOVED = list(EXPRS.keys())

# Target grid positions in Cluster Overview: fill x=20,y=15 then new row y=19.
TARGET = {
    42: {"x": 20, "y": 15, "w": 4, "h": 4},
    43: {"x": 0, "y": 19, "w": 4, "h": 4},
    117: {"x": 4, "y": 19, "w": 4, "h": 4},
    118: {"x": 8, "y": 19, "w": 4, "h": 4},
    247: {"x": 12, "y": 19, "w": 4, "h": 4},
    248: {"x": 16, "y": 19, "w": 4, "h": 4},
    277: {"x": 20, "y": 19, "w": 4, "h": 4},
}

# 1) Shift everything currently at y>=19 down by 4 (new Cluster Overview row).
#    (Skip the panels we are about to move; we set their gridPos explicitly.)
for p in panels:
    if p.get("id") in MOVED:
        continue
    if p["gridPos"]["y"] >= 19:
        p["gridPos"]["y"] += 4

# 2) Apply expr, per-cluster legend, textMode, and new gridPos to moved panels.
for pid in MOVED:
    p = get(pid)
    p["type"] = "stat"
    p["gridPos"] = TARGET[pid]
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = EXPRS[pid]
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0]["instant"] = True
    p["targets"][0].pop("format", None)
    opts = p.setdefault("options", {})
    opts["textMode"] = "value_and_name"
    opts.setdefault("reduceOptions", {"calcs": ["lastNotNull"], "fields": "", "values": False})
    opts.setdefault("colorMode", "value")
    opts.setdefault("graphMode", "none")
    opts.setdefault("justifyMode", "auto")

# Preserve helpful units / mappings.
# Backup Size -> bytes
get(42)["fieldConfig"]["defaults"]["unit"] = "bytes"
# Time since last successful backup -> seconds duration
get(118)["fieldConfig"]["defaults"]["unit"] = "s"
# Backup Last Status -> mapping 1=Success/green, 0=Failed/red
get(117)["fieldConfig"]["defaults"]["mappings"] = [
    {"type": "value", "options": {
        "1": {"text": "Success", "color": "green", "index": 0},
        "0": {"text": "Failed", "color": "red", "index": 1},
    }}
]
get(117)["fieldConfig"]["defaults"]["thresholds"] = {
    "mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}],
}
# PXF Status -> mapping 1=Running/green, 0=Down/red
get(277)["fieldConfig"]["defaults"]["mappings"] = [
    {"type": "value", "options": {
        "1": {"text": "Running", "color": "green", "index": 0},
        "0": {"text": "Down", "color": "red", "index": 1},
    }}
]
get(277)["fieldConfig"]["defaults"]["thresholds"] = {
    "mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}],
}

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("backup/restore/PXF panels moved to Cluster Overview")
