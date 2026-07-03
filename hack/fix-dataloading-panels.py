#!/usr/bin/env python3
"""Make all Data Loading panels per-cluster and never show "No data".

Each panel: group by cluster (+ keep meaningful labels), per-cluster legend, and
a cluster_info*0 fallback so a deployed cluster always shows a 0 line/value.
For kube-state-metrics / pxf-actuator panels (different source, no cluster label),
add a per-cluster 0 baseline so they show 0 instead of "No data".
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
dl = next(p for p in root["panels"] if p.get("id") == 256)
nested = dl["panels"]


def get(pid):
    return next(p for p in nested if p.get("id") == pid)


CI0 = '(max by (cluster, namespace) (cloudberry_cluster_info{instance=~"$instance",cluster!=""}) * 0)'

# id -> (expr, legend)
U = {
    44:  (f'sum by (cluster, namespace) (cloudberry_data_loading_jobs_active{{instance=~"$instance",cluster!=""}}) or {CI0}',
          "{{cluster}}"),
    45:  (f'sum by (cluster, source_type) (rate(cloudberry_data_loading_rows_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}} {{source_type}}"),
    251: (f'sum by (cluster, job) (cloudberry_data_loading_job_status{{instance=~"$instance",cluster!=""}}) or {CI0}',
          "{{cluster}} {{job}}"),
    252: (f'sum by (cluster, job, source_type) (cloudberry_data_loading_rows_total{{instance=~"$instance",cluster!=""}}) or {CI0}',
          "{{cluster}} {{job}}/{{source_type}}"),
    253: (f'histogram_quantile(0.95, sum by (cluster, le, job) (rate(cloudberry_data_loading_job_duration_seconds_bucket{{instance=~"$instance",cluster!=""}}[5m]))) or {CI0}',
          "{{cluster}} {{job}} p95"),
    254: (f'sum by (cluster, job) (rate(cloudberry_data_loading_errors_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}} {{job}}"),
    257: (f'sum by (cluster, result) (rate(cloudberry_pxf_restart_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}} {{result}}"),
    279: (f'sum by (cluster, namespace) (increase(cloudberry_pxf_servers_changed_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}}"),
    258: (f'sum by (cluster, source_type) (rate(cloudberry_data_loading_rows_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}} {{source_type}}"),
    260: (f'sum by (cluster, source_type) (rate(cloudberry_data_loading_rows_total{{instance=~"$instance",cluster!="",source_type=~"hdfs|hive|hbase"}}[5m])) or {CI0}',
          "{{cluster}} {{source_type}}"),
    282: (f'max by (cluster, pod) (cloudberry_pxf_service_up{{instance=~"$instance",cluster!=""}}) or {CI0}',
          "{{cluster}} {{pod}}"),
    283: (f'sum by (cluster, source_type) (rate(cloudberry_data_loading_bytes_total{{instance=~"$instance",cluster!=""}}[5m])) or {CI0}',
          "{{cluster}} {{source_type}}"),
    # kube-state-metrics: keyed to namespace; add per-cluster 0 baseline by namespace.
    275: ('sum by (namespace, job_name) (kube_job_status_failed{instance=~"$instance",job_name=~".*-dataload-.*"}) '
          f'or {CI0}',
          "{{namespace}} {{job_name}}"),
    276: ('max by (namespace, deployment) (kube_deployment_status_replicas_available{instance=~"$instance",deployment=~".*-gpfdist"}) '
          f'or {CI0}',
          "{{namespace}} {{deployment}}"),
    # pxf-actuator (no cluster label); keep request rate but add 0 baseline per cluster.
    284: ('sum by (uri, method, status) (rate(http_server_requests_seconds_count{instance=~"$instance",job="pxf-actuator"}[5m])) '
          f'or {CI0}',
          "{{method}} {{uri}} {{status}}"),
}

for pid, (expr, legend) in U.items():
    p = get(pid)
    p["targets"] = [p["targets"][0]]
    p["targets"][0]["expr"] = expr
    p["targets"][0]["legendFormat"] = legend

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("refactored all Data Loading panels: per-cluster + 0 fallback")
