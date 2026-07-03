#!/usr/bin/env python3
"""Move 4 health stat panels into Cluster Overview (new 5th stat row, per-cluster),
then remove the now-redundant "Backup & Restore Health" collapsed row (122) and
its remaining internal panels.

Move (resize to w4/h4, per-cluster, 0 fallback):
  123 Failed Backup Jobs (current)   -> x=0,  y=23
  124 Backups Failed (1h)            -> x=4,  y=23
  133 Restore Validation (1h)        -> x=8,  y=23
  132 Restore Validations Failed(1h) -> x=12, y=23

Remove: row 122 + remaining nested 125 (Last Backup Status), 126 (Last Successful
Backup), 127 (Time Since Last Successful Backup) — all already represented in
Cluster Overview (Backup Last Status id 117, Time Since id 118).
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

MOVE = [123, 124, 133, 132]
CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

NEW = {
    123: {
        "title": "Failed Backup Jobs",
        "expr": f'count by (cluster, namespace) (cloudberry_backup_job_status{{instance=~"$instance",cluster!="",operation="backup"}} == 3) or {CI0}',
        "grid": {"x": 0, "y": 23, "w": 4, "h": 4},
        "red_at_1": True,
    },
    124: {
        "title": "Backups Failed (1h)",
        "expr": f'sum by (cluster, namespace) (increase(cloudberry_backup_total{{instance=~"$instance",cluster!="",result="failed"}}[1h])) or {CI0}',
        "grid": {"x": 4, "y": 23, "w": 4, "h": 4},
        "red_at_1": True,
    },
    133: {
        "title": "Restore Validation (1h)",
        "expr": f'sum by (cluster, namespace) (increase(cloudberry_restore_validation_total{{instance=~"$instance",cluster!=""}}[1h])) or {CI0}',
        "grid": {"x": 8, "y": 23, "w": 4, "h": 4},
        "red_at_1": False,
    },
    132: {
        "title": "Restore Validations Failed (1h)",
        "expr": f'sum by (cluster, namespace) (increase(cloudberry_restore_validation_total{{instance=~"$instance",cluster!="",result="failed"}}[1h])) or {CI0}',
        "grid": {"x": 12, "y": 23, "w": 4, "h": 4},
        "red_at_1": True,
    },
}

# 1) Extract the 4 panels from row 122's nested list.
row122 = next(p for p in panels if p.get("id") == 122)
moved = {p["id"]: p for p in row122["panels"] if p.get("id") in MOVE}
assert len(moved) == 4, f"expected {MOVE}, found {list(moved)}"

# 2) Remove row 122 entirely (drops it + remaining nested 125/126/127).
root["panels"] = [p for p in panels if p.get("id") != 122]
panels = root["panels"]

# 3) Make room for the new Cluster Overview stat row at y=23:
#    shift everything currently at y>=23 down by 4.
for p in panels:
    if p["gridPos"]["y"] >= 23:
        p["gridPos"]["y"] += 4

# 4) Refactor + place the 4 moved panels at top level (y=23 row).
for pid in MOVE:
    p = moved[pid]
    cfg = NEW[pid]
    p["type"] = "stat"
    p["title"] = cfg["title"]
    p["gridPos"] = cfg["grid"]
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = cfg["expr"]
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0]["instant"] = True
    p["targets"][0].pop("format", None)
    steps = ([{"color": "green", "value": None}, {"color": "red", "value": 1}]
             if cfg["red_at_1"] else [{"color": "green", "value": None}])
    p["fieldConfig"] = {
        "defaults": {"unit": "short",
                     "thresholds": {"mode": "absolute", "steps": steps}},
        "overrides": [],
    }
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": "value", "graphMode": "none", "justifyMode": "auto",
        "textMode": "value_and_name", "orientation": "auto",
    }
    panels.append(p)

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved 4 health panels to Cluster Overview; removed row 122 + internals")
