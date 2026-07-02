#!/usr/bin/env python3
"""Normalize sizes & ordering of panels inside the Backup & Restore section.

Current (messy, with overlaps/gaps after data-loading panels were moved out):
  40  Backup Operations Rate        y92 x0  w8 h8
  41  Backup Duration               y92 x8  w8 h8
  119 Restore Duration p99/p50      y96 x8  w8 h8   (overlaps 41)
  120 Backup Retention Deleted      y96 x16 w8 h8
  121 Backup Job Status             y104 x0 w12 h8
  130 Backup Duration by Type (p99) y104 x12 w12 h8
  131 Retention Deletions (1h)      y112 x16 w8 h8
  246 Restore Operations by Result  y120 x0 w12 h8

Normalized (3 clean full-width rows, uniform h=8, logical grouping
Backups -> Restores -> Retention):
  Row1 y=92 : 40 Backup Operations Rate (x0 w8)
              41 Backup Duration (x8 w8)
              130 Backup Duration by Type (x16 w8)
  Row2 y=100: 121 Backup Job Status (x0 w12)
              246 Restore Operations by Result (x12 w12)
  Row3 y=108: 119 Restore Duration p99/p50 (x0 w8)
              120 Backup Retention Deleted (x8 w8)
              131 Retention Deletions (1h) (x16 w8)

Section now ends at y=116 (was ~128). Shift all sections below (y>=128) UP by 12.
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


# New grid positions for the 8 Backup & Restore panels.
NEW = {
    40:  {"x": 0,  "y": 92,  "w": 8,  "h": 8},
    41:  {"x": 8,  "y": 92,  "w": 8,  "h": 8},
    130: {"x": 16, "y": 92,  "w": 8,  "h": 8},
    121: {"x": 0,  "y": 100, "w": 12, "h": 8},
    246: {"x": 12, "y": 100, "w": 12, "h": 8},
    119: {"x": 0,  "y": 108, "w": 8,  "h": 8},
    120: {"x": 8,  "y": 108, "w": 8,  "h": 8},
    131: {"x": 16, "y": 108, "w": 8,  "h": 8},
}
BR_IDS = set(NEW)

# 1) Shift sections below the Backup & Restore section UP by 12 (gap removal).
#    Those start at y>=128 (Data Loading / Storage & Scaling rows and beyond).
for p in panels:
    if p.get("id") in BR_IDS:
        continue
    if p["gridPos"]["y"] >= 128:
        p["gridPos"]["y"] -= 12

# 2) Apply normalized positions to the Backup & Restore panels.
for pid, gp in NEW.items():
    get(pid)["gridPos"] = gp

with open(path, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("normalized Backup & Restore section")
