#!/usr/bin/env python3
"""Copy "Redistribution Progress" (49) and "Data Skew Coefficient" (50) into
Cluster Overview (new row 7) as per-cluster stats, AND convert the originals in
Storage & Scaling to timeseries (per-cluster).
"""
import json
import sys
import copy

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]


def get(pid):
    return next(p for p in panels if p.get("id") == pid)


CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'
REDIST = f'max by (cluster, namespace) (cloudberry_redistribution_progress{{instance=~"$instance",cluster!=""}}) or {CI0}'
SKEW = f'max by (cluster, namespace) (cloudberry_data_skew_coefficient{{instance=~"$instance",cluster!=""}}) or {CI0}'

# 1) Make room: new Cluster Overview row 7 at y=31; shift y>=31 down by 4.
for p in panels:
    if p["gridPos"]["y"] >= 31:
        p["gridPos"]["y"] += 4

# 2) Build the two Cluster Overview copies (stats, per-cluster, 0 fallback).
src_redist = get(49)
src_skew = get(50)

copy_redist = copy.deepcopy(src_redist)
copy_redist["id"] = 319
copy_redist["type"] = "stat"
copy_redist["gridPos"] = {"x": 0, "y": 31, "w": 4, "h": 4}
copy_redist["targets"] = [{"expr": REDIST, "legendFormat": "{{cluster}}", "instant": True, "refId": "A"}]
copy_redist["options"] = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "colorMode": "value", "graphMode": "none", "justifyMode": "auto",
    "textMode": "value_and_name", "orientation": "auto",
}
copy_redist.setdefault("fieldConfig", {}).setdefault("defaults", {})["unit"] = "percent"

copy_skew = copy.deepcopy(src_skew)
copy_skew["id"] = 320
copy_skew["type"] = "stat"
copy_skew["gridPos"] = {"x": 4, "y": 31, "w": 4, "h": 4}
copy_skew["targets"] = [{"expr": SKEW, "legendFormat": "{{cluster}}", "instant": True, "refId": "A"}]
copy_skew["options"] = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "colorMode": "value", "graphMode": "none", "justifyMode": "auto",
    "textMode": "value_and_name", "orientation": "auto",
}

panels.append(copy_redist)
panels.append(copy_skew)

# 3) Convert the ORIGINALS (49, 50) in Storage & Scaling to timeseries (per-cluster).
def to_timeseries(p, expr, unit=None):
    p["type"] = "timeseries"
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = expr
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0].pop("instant", None)
    fc = {
        "defaults": {
            "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10,
                       "showPoints": "auto", "spanNulls": True},
            "color": {"mode": "palette-classic"},
        },
        "overrides": [],
    }
    if unit:
        fc["defaults"]["unit"] = unit
    p["fieldConfig"] = fc
    p["options"] = {
        "legend": {"displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }

to_timeseries(get(49), REDIST, unit="percent")
to_timeseries(get(50), SKEW)

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("copied Redistribution/Skew to Cluster Overview; converted originals to timeseries")
