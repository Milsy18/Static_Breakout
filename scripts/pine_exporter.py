#!/usr/bin/env python3
import json, datetime
from pathlib import Path

cut = json.load(open(r".\out\optim\cutoffs.json","r"))
ts  = datetime.datetime.now().strftime("%Y-%m-%d")
vals = [cut.get(str(k), cut.get(k, 0.9)) for k in range(1,10)]
arr  = ", ".join(f"{v:.3f}" for v in vals)

pine = f"""//@version=6
indicator("M18 Monotone Thresholds — {ts}", overlay=false, max_lines_count=500, max_labels_count=500)

var float[] CUT = array.from({arr})

level_clamped(level) =>
    lvl = math.round(level)
    math.clamp(lvl, 1, 9)

threshold_for_level(level) =>
    array.get(CUT, level_clamped(level)-1)

// Demo inputs (Tier B will compute p̂ on-chart)
mkt_level = input.int(5, "Market level (1–9)")
p_hat     = input.float(0.50, "Predicted TP prob (from model)", step=0.01)

thr  = threshold_for_level(mkt_level)
pass = p_hat >= thr

var table T = table.new(position.top_right, 4, 11, border_width=1)
if barstate.isfirst
    table.cell(T, 0, 0, "Lvl", text_color=color.white)
    table.cell(T, 1, 0, "Cutoff", text_color=color.white)
    table.cell(T, 2, 0, "Now p̂", text_color=color.white)
    table.cell(T, 3, 0, "Pass?", text_color=color.white)
    for i=1 to 9
        table.cell(T, 0, i, str.tostring(i))
        table.cell(T, 1, i, str.tostring(array.get(CUT, i-1), "#.###"))
        table.cell(T, 2, i, "")
        table.cell(T, 3, i, "")

if barstate.islast
    for i=1 to 9
        ok = p_hat >= array.get(CUT, i-1)
        table.cell(T, 2, i, i == mkt_level ? str.tostring(p_hat, "#.###") : "")
        table.cell(T, 3, i, ok ? "✅" : "—", text_color=ok ? color.lime : color.red)

plot(pass ? 1 : 0, "pass(level, p̂)", style=plot.style_columns, color= pass ? color.new(color.green, 0) : color.new(color.red, 70))
alertcondition(pass, "M18 Pass", "M18: level {{mkt_level}} pass (p̂={{p_hat}} ≥ thr={{thr}})")
"""
out = Path(r".\out\optim\m18_monotone_v6.pine")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(pine, encoding="utf-8")
print(f"[pine] wrote {out} with cutoffs: {vals}")
