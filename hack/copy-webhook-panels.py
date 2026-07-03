#!/usr/bin/env python3
"""Copy "Webhook Denied (1h)" (106) and "Webhook Errors (1h)" (107) into Cluster
Overview as per-cluster stats. Originals stay in Security & Lifecycle.

webhook_admission_total has no cluster label (operator-global), so attribute the
count per deployed cluster via a cluster_info join (same pattern as Auth Attempts),
defaulting to 0.

Placement: Cluster Overview row 5 (y=23) has a free slot at x=20 -> Denied copy.
Add a new row 6 at y=27 -> Errors copy (x=0). Shift everything at y>=27 down by 4.
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


def per_cluster_join(result):
    """count per deployed cluster of webhook_admission_total{result=...} over 1h, default 0."""
    inst = '{instance=~"$instance"'
    return (
        f'(sum by (namespace) (increase(cloudberry_webhook_admission_total{inst},result="{result}"}}[1h]))'
        f' * on(namespace) group_right () '
        f'(max by (cluster, namespace) (cloudberry_cluster_info{inst},cluster!=""}}) * 0 + 1))'
        f' or (max by (cluster, namespace) (cloudberry_cluster_info{inst},cluster!=""}}) * 0)'
    )


# 1) Make room: shift everything at y>=27 down by 4 (new Cluster Overview row).
for p in panels:
    if p["gridPos"]["y"] >= 27:
        p["gridPos"]["y"] += 4

# 2) Build the two copies from the originals (deep-copy, new ids, new positions).
src_denied = get(106)
src_errors = get(107)

copy_denied = copy.deepcopy(src_denied)
copy_denied["id"] = 317
copy_denied["gridPos"] = {"x": 20, "y": 23, "w": 4, "h": 4}
copy_denied["targets"] = [{
    "expr": per_cluster_join("denied"),
    "legendFormat": "{{cluster}}",
    "instant": True,
    "refId": "A",
}]
copy_denied["options"]["textMode"] = "value_and_name"
copy_denied["description"] = "Webhook admissions denied in the last hour, per deployed cluster (operator-global metric joined by namespace)."

copy_errors = copy.deepcopy(src_errors)
copy_errors["id"] = 318
copy_errors["gridPos"] = {"x": 0, "y": 27, "w": 4, "h": 4}
copy_errors["targets"] = [{
    "expr": per_cluster_join("error"),
    "legendFormat": "{{cluster}}",
    "instant": True,
    "refId": "A",
}]
copy_errors["options"]["textMode"] = "value_and_name"
copy_errors["description"] = "Webhook admission errors in the last hour, per deployed cluster (operator-global metric joined by namespace)."

panels.append(copy_denied)
panels.append(copy_errors)

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("copied Webhook Denied/Errors into Cluster Overview (per-cluster)")
