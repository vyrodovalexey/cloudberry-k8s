#!/usr/bin/env python3
"""Make all Backup & Restore panels per-cluster and never show "No data".

Each panel gets a per-cluster query (grouped by cluster) with a 0-baseline
fallback keyed to cloudberry_cluster_info, so a deployed cluster always shows a
0 (line/value) instead of "No data".
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


I = '{instance=~"$instance"}'
IC = '{instance=~"$instance",cluster!=""}'
# Per-cluster 0 baseline (rate-style => instantaneous 0 line per cluster).
CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

# id -> (new expr, legendFormat)
UPDATES = {
    # 40 Backup Operations Rate
    40: (
        f'sum by (cluster, type, result) (rate(cloudberry_backup_total{IC}[5m])) or {CI0}',
        "{{cluster}} {{type}}/{{result}}",
    ),
    # 41 Backup Duration (p99) per cluster
    41: (
        f'histogram_quantile(0.99, sum by (cluster, le) (rate(cloudberry_backup_duration_seconds_bucket{IC}[5m]))) or {CI0}',
        "{{cluster}} p99",
    ),
    # 119 Restore Duration p99/p50 per cluster
    119: (
        f'histogram_quantile(0.99, sum by (cluster, le) (rate(cloudberry_restore_duration_seconds_bucket{IC}[5m]))) or {CI0}',
        "{{cluster}} p99",
    ),
    # 120 Backup Retention Deleted per cluster
    120: (
        f'sum by (cluster, namespace) (increase(cloudberry_backup_retention_deleted_total{IC}[1h])) or {CI0}',
        "{{cluster}}",
    ),
    # 121 Backup Job Status per cluster (keep job_name for detail, ensure 0 fallback)
    121: (
        f'sum by (cluster, operation) (cloudberry_backup_job_status{IC}) or {CI0}',
        "{{cluster}} {{operation}}",
    ),
    # 130 Backup Duration by Type (p99) per cluster+type
    130: (
        f'histogram_quantile(0.99, sum by (cluster, type, le) (rate(cloudberry_backup_duration_seconds_bucket{IC}[5m]))) or {CI0}',
        "{{cluster}} {{type}}",
    ),
    # 131 Retention Deletions (1h) per cluster (stat)
    131: (
        f'sum by (cluster, namespace) (increase(cloudberry_backup_retention_deleted_total{IC}[1h])) or {CI0}',
        "{{cluster}}",
    ),
    # 246 Restore Operations by Result per cluster+result
    246: (
        f'sum by (cluster, result) (rate(cloudberry_restore_total{IC}[5m])) or {CI0}',
        "{{cluster}} {{result}}",
    ),
}

for pid, (expr, legend) in UPDATES.items():
    p = get(pid)
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = expr
    p["targets"][0]["legendFormat"] = legend
    # Add a second restore p50 target for the Restore Duration panel.
    if pid == 119:
        p["targets"].append({
            "expr": f'histogram_quantile(0.50, sum by (cluster, le) (rate(cloudberry_restore_duration_seconds_bucket{IC}[5m]))) or {CI0}',
            "legendFormat": "{{cluster}} p50",
            "refId": "B",
        })

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("updated Backup & Restore panels: per-cluster + 0 fallback")
