#!/usr/bin/env python3
"""Move data-loading / PXF panels from Backup & Restore into the Data Loading row.

Panels to move (currently top-level, inside the Backup & Restore section):
  44  Data Loading Jobs Active   (timeseries)
  45  Data Loading Rows Rate     (timeseries)
  250 PXF Servers Configured     (stat)
  278 PXF Extensions Installed   (stat)

Target: the COLLAPSED "Data Loading" row (id 256). Its child panels live in
row.panels with their own y-coords (starting at 107). We insert the 4 moved
panels as a new first row of that section (y=107) and shift the existing nested
panels down by 8.
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

MOVE_IDS = [44, 45, 250, 278]

# 1) Extract the panels to move from the top level.
moved = {p["id"]: p for p in panels if p.get("id") in MOVE_IDS}
assert len(moved) == len(MOVE_IDS), f"expected {MOVE_IDS}, found {list(moved)}"
root["panels"] = [p for p in panels if p.get("id") not in MOVE_IDS]
panels = root["panels"]

# 2) Find the collapsed Data Loading row.
dl_row = next(p for p in panels if p.get("id") == 256)
assert dl_row.get("collapsed"), "Data Loading row is expected to be collapsed"
nested = dl_row["panels"]

# 3) Shift existing nested panels down by 8 to free the first row (y=107).
START_Y = 107
for p in nested:
    p["gridPos"]["y"] += 8

# 4) Place the 4 moved panels as the new first row at y=107 (each w=6, full 24).
layout = [
    (44, 0),    # Data Loading Jobs Active
    (45, 6),    # Data Loading Rows Rate
    (250, 12),  # PXF Servers Configured
    (278, 18),  # PXF Extensions Installed
]
for pid, x in layout:
    p = moved[pid]
    p["gridPos"] = {"x": x, "y": START_Y, "w": 6, "h": 8}

# Insert at the front of the nested panels (top of the section).
dl_row["panels"] = [moved[pid] for pid, _ in layout] + nested

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved data-loading/PXF panels into the Data Loading row")
