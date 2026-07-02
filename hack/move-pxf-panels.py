#!/usr/bin/env python3
"""Move 3 PXF/data-loading metric panels into the (collapsed) Data Loading row.

Move: 282 PXF Service Up per Segment (M.1)
      283 Data Loading Bytes Loaded (M.10)
      284 PXF Actuator Requests + Latency (M.2/M.3)

They currently live (top-level) under row 270 "PXF & Data-Loading Metrics".
After moving them, row 270 becomes empty -> remove it.
Close the resulting vertical gap by shifting the sections below up.
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

MOVE = [282, 283, 284]

# 1) Extract the 3 panels from the top level.
moved = {p["id"]: p for p in panels if p.get("id") in MOVE}
assert len(moved) == 3, f"expected {MOVE}, found {list(moved)}"

# 2) The now-empty row 270 is removed too.
root["panels"] = [p for p in panels if p.get("id") not in MOVE and p.get("id") != 270]
panels = root["panels"]

# 3) Append the 3 panels to the Data Loading collapsed row's nested panels.
dl = next(p for p in panels if p.get("id") == 256)
assert dl.get("collapsed"), "Data Loading row expected collapsed"
nested = dl["panels"]
bottom = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in nested)  # = 216

# Layout the 3 in a new row at the bottom of the Data Loading section.
moved[282]["gridPos"] = {"x": 0,  "y": bottom,     "w": 12, "h": 8}
moved[283]["gridPos"] = {"x": 12, "y": bottom,     "w": 12, "h": 8}
moved[284]["gridPos"] = {"x": 0,  "y": bottom + 8, "w": 24, "h": 8}
dl["panels"] = nested + [moved[282], moved[283], moved[284]]

# 4) Close the top-level gap left by removing row 270 + its 3 panels.
#    The section occupied y=311 (row) .. y=327 (panel 284 bottom). The next
#    top-level row ("API Business Operations") was at y=329. Shift everything
#    at y>=329 UP by (329-311)=18 so it follows "Log Streaming" (ends ~y=310).
for p in panels:
    if p["gridPos"]["y"] >= 329:
        p["gridPos"]["y"] -= 18

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("moved PXF/data-loading panels into Data Loading; removed empty row 270")
