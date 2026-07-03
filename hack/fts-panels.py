#!/usr/bin/env python3
"""Apply FTS dashboard changes (correct layout):

FTS & High Availability section, original layout:
  y=33: Probe Rate(x0,w8), Probe Duration(x8,w8), Failures stat(x16,w4), Failovers stat(x20,w4)
  y=41: Segment Status(x0,w12), Replication Lag(x12,w12)
  y=49: Query Activity row (next section)

New layout:
  y=33: Probe Rate(x0,w8 ts), Probe Duration(x8,w8 ts), Failures(x16,w8 TS, h8)
  y=41: Segment Status(x0,w12), Replication Lag(x12,w12)   [unchanged]
  y=49: Failovers(x0,w12 TS, h8)                           [new row]
  y=57: Query Activity row (shifted +8)

Changes:
1. Probe Rate (21): per-cluster (by cluster, result).
2. Probe Duration (22): per-cluster (by cluster).
3. Failures (23) & Failovers (24): -> per-cluster timeseries.
4. Copy Failures/Failovers into Cluster Overview (y=15, x=12/16) as per-cluster
   stats with 0 fallback.
5. Shift all sections after FTS (y>=49) down by 8 for the new Failovers row.
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


INSTANCE = '{instance=~"$instance"}'

# --- Shift sections AFTER the FTS section down by 8 (everything at y>=49) -----
# Do this first, before we add/move FTS panels into y=49.
for p in panels:
    if p["gridPos"]["y"] >= 49:
        p["gridPos"]["y"] += 8

# 1) FTS Probe Rate (21) — per cluster + result
p21 = get(21)
p21["targets"][0]["expr"] = (
    f'sum by (cluster, result) (rate(cloudberry_fts_probe_total{INSTANCE}[5m]))'
)
p21["targets"][0]["legendFormat"] = "{{cluster}} / {{result}}"

# 2) FTS Probe Duration (22) — per cluster
p22 = get(22)
p22["targets"][0]["expr"] = (
    f'histogram_quantile(0.99, sum by (cluster, le) '
    f'(rate(cloudberry_fts_probe_duration_seconds_bucket{INSTANCE}[5m])))'
)
p22["targets"][0]["legendFormat"] = "{{cluster}} p99"


# 3) Convert Failures (23) + Failovers (24) to per-cluster timeseries
def to_timeseries(p, metric, title, gridpos):
    p["type"] = "timeseries"
    p["title"] = title
    p["gridPos"] = gridpos
    p["targets"][0]["expr"] = (
        f'sum by (cluster, namespace) (increase(cloudberry_{metric}{INSTANCE}[1h]))'
    )
    p["targets"][0]["legendFormat"] = "{{cluster}}"
    p["targets"][0].pop("instant", None)
    p["fieldConfig"] = {
        "defaults": {
            "custom": {
                "drawStyle": "line",
                "lineWidth": 1,
                "fillOpacity": 10,
                "showPoints": "auto",
                "spanNulls": True,
            },
            "unit": "short",
            "color": {"mode": "palette-classic"},
        },
        "overrides": [],
    }
    p["options"] = {
        "legend": {"displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }


# Failures -> y=33 x=16 w=8 h=8 (fills row 1 to full 24 width)
to_timeseries(get(23), "fts_probe_failures_total", "FTS Probe Failures (1h)",
              {"x": 16, "y": 33, "w": 8, "h": 8})
# Failovers -> y=49 x=0 w=12 h=8 (new row in FTS section, before shifted Query Activity@57)
to_timeseries(get(24), "fts_failover_total", "FTS Failovers (1h)",
              {"x": 0, "y": 49, "w": 12, "h": 8})


# 4) Copy Failures + Failovers into Cluster Overview (third stat row y=15),
#    free slots x=12, x=16. Per-cluster stat with 0 fallback.
def overview_copy(new_id, x, metric, title):
    return {
        "id": new_id,
        "type": "stat",
        "title": title,
        "description": f"{title} per deployed cluster. Shows 0 (not \"No data\") for a deployed cluster with none yet.",
        "datasource": {"type": "prometheus", "uid": "victoriametrics"},
        "gridPos": {"x": x, "y": 15, "w": 4, "h": 4},
        "targets": [
            {
                "expr": (
                    f'sum by (cluster, namespace) (increase(cloudberry_{metric}{{instance=~"$instance",cluster!=""}}[1h]))'
                    f' or (max by (cluster, namespace) (cloudberry_cluster_info{{instance=~"$instance",cluster!=""}}) * 0)'
                ),
                "legendFormat": "{{cluster}}",
                "instant": True,
                "refId": "A",
            }
        ],
        "fieldConfig": {
            "defaults": {
                "unit": "short",
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "red", "value": 1},
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
            "textMode": "value_and_name",
            "orientation": "auto",
        },
    }


panels.append(overview_copy(315, 12, "fts_probe_failures_total", "FTS Probe Failures (1h)"))
panels.append(overview_copy(316, 16, "fts_failover_total", "FTS Failovers (1h)"))

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("FTS changes applied")
