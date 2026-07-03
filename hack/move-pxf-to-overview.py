#!/usr/bin/env python3
"""Move 3 panels from the collapsed Data Loading row into Cluster Overview (row 6),
resized to w4/h4, per-cluster with 0 fallback.

Move: 250 PXF Servers Configured -> x=4,  y=27
      255 Data Loading Last Success -> x=8,  y=27
      278 PXF Extensions Installed -> x=12, y=27
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

MOVE = [250, 255, 278]
CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

NEW = {
    250: {
        "expr": f'max by (cluster, namespace) (cloudberry_pxf_servers_configured{{instance=~"$instance",cluster!=""}}) or {CI0}',
        "grid": {"x": 4, "y": 27, "w": 4, "h": 4},
    },
    255: {
        "expr": f'(time() - max by (cluster, namespace) (cloudberry_data_loading_job_last_success_timestamp{{instance=~"$instance",cluster!=""}})) or {CI0}',
        "grid": {"x": 8, "y": 27, "w": 4, "h": 4},
    },
    278: {
        "expr": f'max by (cluster, namespace) (cloudberry_pxf_extensions_installed{{instance=~"$instance",cluster!=""}}) or {CI0}',
        "grid": {"x": 12, "y": 27, "w": 4, "h": 4},
    },
}

# 1) Extract the 3 panels from the collapsed Data Loading row (256).
dl = next(p for p in panels if p.get("id") == 256)
moved = {p["id"]: p for p in dl["panels"] if p.get("id") in MOVE}
assert len(moved) == 3, f"expected {MOVE}, found {list(moved)}"
dl["panels"] = [p for p in dl["panels"] if p.get("id") not in MOVE]

# 2) Refactor + reposition + add to top level (Cluster Overview row 6).
for pid in MOVE:
    p = moved[pid]
    cfg = NEW[pid]
    p["type"] = "stat"
    p["gridPos"] = cfg["grid"]
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = cfg["expr"]
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0]["instant"] = True
    p["targets"][0].pop("format", None)
    opts = p.setdefault("options", {})
    opts["textMode"] = "value_and_name"
    opts.setdefault("reduceOptions", {"calcs": ["lastNotNull"], "fields": "", "values": False})
    opts.setdefault("colorMode", "value")
    opts.setdefault("graphMode", "none")
    opts.setdefault("justifyMode", "auto")
    panels.append(p)

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved PXF/Last-Success panels into Cluster Overview row 6")
