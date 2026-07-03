#!/usr/bin/env python3
"""
add-instance-var.py — Add an "instance" template variable to a Grafana dashboard
and inject `instance=~"$instance"` into the targeted metric selectors so users
can scope every panel to a chosen scrape instance (e.g. an operator pod).

Usage:
  add-instance-var.py <dashboard.json> <label_query_metric> <prefix1,prefix2,...>

- <label_query_metric>: a metric used to populate the variable's value list,
  representative of the dashboard's data source (e.g. cloudberry_reconcile_total).
- <prefixes>: comma-separated metric-name prefixes whose selectors get the
  instance matcher injected (e.g. "cloudberry_,kube_").

The injector is idempotent: running it twice does not double-inject. It only
rewrites bare `name` -> `name{instance=~"$instance"}` and
`name{labels}` -> `name{instance=~"$instance",labels}` for metric names that
start with one of the given prefixes, leaving recording-rule-free PromQL math,
functions and aggregations intact.
"""
import json
import re
import sys


def build_variable(label_query_metric: str) -> dict:
    return {
        "name": "instance",
        "type": "query",
        "label": "Operator instance",
        "description": "Scrape instance feeding this dashboard (operator pod / exporter).",
        "datasource": {"type": "prometheus", "uid": "victoriametrics"},
        "definition": f"label_values({label_query_metric}, instance)",
        "query": {
            "query": f"label_values({label_query_metric}, instance)",
            "refId": "InstanceVariableQuery",
        },
        "refresh": 2,            # on time range change
        "sort": 1,               # alphabetical asc
        "includeAll": True,
        "allValue": ".*",        # regex-friendly "All" (used with =~)
        "multi": True,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "options": [],
        "hide": 0,
        "regex": "",
        "skipUrlSync": False,
    }


def inject_instance(expr: str, prefixes) -> str:
    """Inject instance=~"$instance" into metric selectors starting with a prefix."""
    matcher = 'instance=~"$instance"'

    # Match a metric name (optionally followed by a {label set}).
    # Group 1: metric name; Group 2 (optional): the brace block including braces.
    name_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)(\{[^{}]*\})?')

    def repl(m):
        name = m.group(1)
        braces = m.group(2)
        if not any(name.startswith(p) for p in prefixes):
            return m.group(0)
        if braces is None:
            return f'{name}{{{matcher}}}'
        inner = braces[1:-1].strip()
        if 'instance=~"$instance"' in inner or 'instance="$instance"' in inner:
            return m.group(0)  # already injected
        if inner == "":
            return f'{name}{{{matcher}}}'
        return f'{name}{{{matcher},{inner}}}'

    return name_re.sub(repl, expr)


def walk_targets(node, prefixes):
    if isinstance(node, dict):
        if "expr" in node and isinstance(node["expr"], str):
            node["expr"] = inject_instance(node["expr"], prefixes)
        for v in node.values():
            walk_targets(v, prefixes)
    elif isinstance(node, list):
        for v in node:
            walk_targets(v, prefixes)


def main():
    path = sys.argv[1]
    label_metric = sys.argv[2]
    prefixes = tuple(p for p in sys.argv[3].split(",") if p)

    with open(path) as f:
        dash = json.load(f)

    # Some dashboards may be wrapped; operate on the dashboard object.
    root = dash.get("dashboard", dash)

    # 1) Add/replace the templating variable (idempotent by name).
    templating = root.setdefault("templating", {})
    var_list = templating.setdefault("list", [])
    var_list[:] = [v for v in var_list if v.get("name") != "instance"]
    var_list.insert(0, build_variable(label_metric))

    # 2) Inject the instance matcher into targeted selectors.
    for p in root.get("panels", []):
        walk_targets(p, prefixes)

    with open(path, "w") as f:
        json.dump(dash, f, indent=2)
        f.write("\n")

    print(f"updated {path}: variable=instance metric={label_metric} prefixes={prefixes}")


if __name__ == "__main__":
    main()
