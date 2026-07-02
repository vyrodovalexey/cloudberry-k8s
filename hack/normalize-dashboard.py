#!/usr/bin/env python3
"""Normalize & reorder the whole operator dashboard below the 5 reference
sections (Cluster Overview, Reconciliation, FTS & HA, Backup & Restore, Data
Loading). Combine fragmented/duplicated thematic sections:

  - Merge Query-themed sections (Query Activity, Query History, Plan Analysis,
    Query Operations) -> "Query Monitoring".
  - Merge API-themed sections (API Server (HTTP), Lifecycle & Cluster Operations,
    API Business Operations (New)) -> "API & Lifecycle Operations".

Then re-flow every section below Data Loading into clean, full-width rows (3
panels of w8, or 2 of w12), packed top-to-bottom with no overlaps/gaps, and
re-stack all sections vertically in a tidy order. Collapsed rows (Data Loading)
keep their nested panels untouched.
"""
import json
import sys

path = sys.argv[1]
with open(path) as f:
    dash = json.load(f)
root = dash.get("dashboard", dash)
panels = root["panels"]

# ---- Fix the pre-existing DUPLICATE panel id (290 used by both a row and a
# timeseries). Reassign the non-row one to a fresh id so members resolve
# unambiguously. ----------------------------------------------------------
_seen = {}
_maxid = max(
    (p["id"] for p in panels for p in ([p] + p.get("panels", [])) if isinstance(p.get("id"), int)),
    default=0,
)
for p in panels:
    pid = p.get("id")
    if pid in _seen:
        _maxid += 1
        # keep the row id stable; bump whichever is NOT a row, else bump this one.
        if _seen[pid].get("type") == "row" and p.get("type") != "row":
            p["id"] = _maxid
        elif p.get("type") == "row" and _seen[pid].get("type") != "row":
            _seen[pid]["id"] = _maxid
            _seen[pid] = p  # row now owns the original id
        else:
            p["id"] = _maxid
    else:
        _seen[pid] = p

# ---- index helpers ---------------------------------------------------------
by_id = {p["id"]: p for p in panels}
rows = [p for p in panels if p.get("type") == "row"]
row_by_id = {r["id"]: r for r in rows}


def section_panel_ids(row_id, start_y, end_y):
    """top-level non-row panels whose y is within [start_y, end_y)."""
    out = []
    for p in panels:
        if p.get("type") == "row":
            continue
        # only top-level (skip nested-in-collapsed-row)
        y = p["gridPos"]["y"]
        if start_y <= y < end_y:
            out.append(p)
    return out


# Determine each top-level row's panel members by y-band (rows are sorted).
sorted_rows = sorted(rows, key=lambda r: r["gridPos"]["y"])
bands = []  # (row, [member panel ids])
for i, r in enumerate(sorted_rows):
    y0 = r["gridPos"]["y"]
    y1 = sorted_rows[i + 1]["gridPos"]["y"] if i + 1 < len(sorted_rows) else 10**9
    # collapsed rows own their nested panels, not top-level band members
    if r.get("collapsed"):
        bands.append((r, []))
        continue
    members = [p["id"] for p in section_panel_ids(r["id"], y0, y1)]
    bands.append((r, members))

# ---- Desired final section order + merges ---------------------------------
# Reference sections keep their identity & member order (we only re-flow y).
# Merged sections combine member lists under a single (renamed) header; the
# extra headers are dropped.

# Map: header row id -> list of member panel ids it should contain after merge.
# And the order of sections top-to-bottom.

# Gather current members for convenience.
members_of = {r["id"]: m for r, m in bands}

# Merge groups: (keep_row_id, new_title, [row_ids_to_absorb_in_order])
merges = [
    # Query Monitoring = Query Activity + Plan Analysis + Query Operations + Query History
    (27, "Query Monitoring", [27, 64, 68, 58]),
    # API & Lifecycle Operations = API Server + Lifecycle & Cluster Ops + API Business Ops
    (200, "API & Lifecycle Operations", [200, 230, 290]),
]

merged_members = {}
absorbed_row_ids = set()
for keep_id, title, absorb_ids in merges:
    combined = []
    for rid in absorb_ids:
        combined.extend(members_of.get(rid, []))
        if rid != keep_id:
            absorbed_row_ids.add(rid)
    merged_members[keep_id] = combined
    row_by_id[keep_id]["title"] = title

# ---- Final ordered list of (row_id, [member ids]) --------------------------
# Keep the existing top-level order, but drop absorbed headers and use merged
# member lists for the kept headers.
final_sections = []
for r, m in bands:
    rid = r["id"]
    if rid in absorbed_row_ids:
        continue
    if rid in merged_members:
        final_sections.append((rid, merged_members[rid]))
    else:
        final_sections.append((rid, m))


# ---- Re-flow: assign y/x to rows + members --------------------------------
def reflow(member_ids, start_y):
    """Place members in full-width rows. Widths: prefer the panel's own width
    when it tiles cleanly; otherwise default by type. Returns next y."""
    y = start_y
    x = 0
    row_h = 8
    for pid in member_ids:
        p = by_id[pid]
        w = p["gridPos"].get("w", 8)
        t = p.get("type")
        # Normalize width to a clean tiling value.
        if t in ("stat", "gauge"):
            w = 6              # 4 stats per full-width row
        elif w >= 20:
            w = 24             # full-width panels keep their own row
        elif w in (6, 8, 12):
            pass               # already clean
        else:
            w = 12 if w > 8 else 8
        # A full-width panel starts a fresh row and occupies it alone.
        if w == 24:
            if x > 0:
                y += row_h
                x = 0
            p["gridPos"] = {"x": 0, "y": y, "w": 24, "h": row_h}
            y += row_h
            continue
        if x + w > 24:
            y += row_h
            x = 0
        p["gridPos"] = {"x": x, "y": y, "w": w, "h": row_h}
        x += w
    if x > 0:
        y += row_h
    return y


# Sections whose internal layout is already clean — preserve exactly; only
# re-stack their vertical position. Everything else gets re-flowed.
# Includes the 5 reference sections + already-normalized Storage & Scaling +
# the cohesive role-setup section.
REFERENCE = {1, 14, 20, 39, 256, 46}  # +Storage & Scaling


def relayout_block(member_ids, dy):
    """Shift a set of panels vertically by dy, preserving their relative x/y/w/h."""
    for pid in member_ids:
        by_id[pid]["gridPos"]["y"] += dy


cur_y = 0
for rid, members in final_sections:
    r = row_by_id[rid]
    if rid in REFERENCE:
        # Preserve exact internal layout; just move the whole block so its
        # header sits at cur_y.
        dy = cur_y - r["gridPos"]["y"]
        r["gridPos"]["y"] += dy
        if r.get("collapsed"):
            cur_y += 1  # header only; nested panels are internal
            continue
        if members:
            relayout_block(members, dy)
            bottom = max(by_id[m]["gridPos"]["y"] + by_id[m]["gridPos"]["h"] for m in members)
            cur_y = bottom
        else:
            cur_y += 1
        continue
    # Non-reference section: re-flow into clean rows.
    r["gridPos"] = {"x": 0, "y": cur_y, "w": 24, "h": 1}
    cur_y += 1
    cur_y = reflow(members, cur_y)

# ---- Drop absorbed header rows from the panel list ------------------------
root["panels"] = [p for p in panels if not (p.get("type") == "row" and p.get("id") in absorbed_row_ids)]

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(f"normalized: {len(final_sections)} sections, dropped {len(absorbed_row_ids)} merged headers")
