#!/usr/bin/env python3
"""Move 8 gpfdist/role-setup stat panels into Cluster Overview, resized w4/h4,
per-cluster with 0 fallback.

Row 7 (y=31) free slots x=8,12,16,20; new row 8 (y=35) x=0,4,8,12.
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


CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

def total(metric):
    return f'sum by (cluster, namespace) (increase(cloudberry_{metric}{{instance=~"$instance",cluster!=""}}[1h])) or {CI0}'

def errors(metric):
    return f'sum by (cluster, namespace) (increase(cloudberry_{metric}{{instance=~"$instance",cluster!="",result="error"}}[1h])) or {CI0}'

# id -> (expr, grid, red_at_1)
NEW = {
    300: (total("gpfdist_reconcile_total"),       {"x": 8,  "y": 31, "w": 4, "h": 4}, False),
    301: (errors("gpfdist_reconcile_total"),      {"x": 12, "y": 31, "w": 4, "h": 4}, True),
    303: (total("pxf_extension_setup_total"),     {"x": 16, "y": 31, "w": 4, "h": 4}, False),
    304: (errors("pxf_extension_setup_total"),    {"x": 20, "y": 31, "w": 4, "h": 4}, True),
    306: (total("dataloader_role_setup_total"),   {"x": 0,  "y": 35, "w": 4, "h": 4}, False),
    307: (errors("dataloader_role_setup_total"),  {"x": 4,  "y": 35, "w": 4, "h": 4}, True),
    309: (total("exporter_role_setup_total"),     {"x": 8,  "y": 35, "w": 4, "h": 4}, False),
    310: (errors("exporter_role_setup_total"),    {"x": 12, "y": 35, "w": 4, "h": 4}, True),
}
MOVE = list(NEW)

# 1) Make room: shift everything at y>=35 down by 4 (new Cluster Overview row 8).
#    The 8 panels being moved are currently at y~395-419 (>=35), so EXCLUDE them.
for p in panels:
    if p.get("id") in MOVE:
        continue
    if p["gridPos"]["y"] >= 35:
        p["gridPos"]["y"] += 4

# 2) Refactor + reposition the moved panels.
for pid in MOVE:
    p = get(pid)
    expr, grid, red = NEW[pid]
    p["type"] = "stat"
    p["gridPos"] = grid
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = expr
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0]["instant"] = True
    p["targets"][0].pop("format", None)
    steps = ([{"color": "green", "value": None}, {"color": "red", "value": 1}]
             if red else [{"color": "green", "value": None}])
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

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved 8 role-setup panels into Cluster Overview")
