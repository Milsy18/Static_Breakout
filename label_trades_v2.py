import pandas as pd
import numpy as np

IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_events_timed_full.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"

df = pd.read_csv(IN)

# --- Config from our earlier scan ---
tp_by_lvl = {1:0.65, 2:0.85, 3:0.90, 4:0.85, 5:0.95, 6:0.90, 7:0.95, 8:0.95, 9:0.95}
hold_by_lvl = {1:5, 2:5, 3:8, 4:6, 5:8, 6:6, 7:6, 8:7, 9:6}  # from tp_scan_by_level_fixed.csv

# Entry (prefer explicit column, else close_d0)
if "start_price" in df.columns:
    start = df["start_price"].astype(float)
else:
    start = df["close_d0"].astype(float)

lvl = df["market_level"].astype(int).clip(1, 9)
tp_pct = lvl.map(tp_by_lvl)
hold_d = lvl.map(hold_by_lvl)

# Exit close return at timed exit
close_cols = {d: f"close_d{d}" for d in sorted(set(hold_by_lvl.values()))}
missing = [c for c in close_cols.values() if c not in df.columns]
if missing:
    raise ValueError(f"Missing close columns: {missing}")

exit_close = df.lookup(df.index, hold_d.map(close_cols)) if hasattr(df, "lookup") else \
    df.to_dict("index")  # fallback if needed (older pandas removed lookup)

if isinstance(exit_close, dict):  # robust fallback
    exit_close = pd.Series([df.loc[i, close_cols[hold_d.iat[i]]] for i in range(len(df))], index=df.index)

exit_ret_close = (exit_close.astype(float) / start) - 1.0

# TP hit check: any HIGH within window >= start*(1+tp_pct)
def tp_hit_row(i):
    d = int(hold_d.iat[i])
    hi_cols = [f"high_d{j}" for j in range(d+1) if f"high_d{j}" in df.columns]
    if not hi_cols:
        return False
    mx = df.loc[i, hi_cols].astype(float).max()
    return mx >= start.iat[i] * (1.0 + tp_pct.iat[i])

tp_hit = pd.Series([tp_hit_row(i) for i in range(len(df))], index=df.index)

# New win definition (NO intraday 20% unless we actually hit TP)
win_flag = (tp_hit) | (exit_ret_close >= 0.20)

df_out = df.copy()
df_out["tp_pct_assumed"] = tp_pct
df_out["hold_days_assumed"] = hold_d
df_out["exit_ret_close"] = exit_ret_close
df_out["tp_hit"] = tp_hit.astype(int)
df_out["win_flag"] = win_flag.astype(int)

df_out.to_csv(OUT, index=False)

print(f"Wrote -> {OUT}")
print("Counts (win_flag):")
print(df_out["win_flag"].value_counts(dropna=False))
print("\nWin rate by level:")
print(df_out.groupby("market_level")["win_flag"].mean().round(3))
