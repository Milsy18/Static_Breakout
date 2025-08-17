import pandas as pd
from pathlib import Path

csv_path = r"C:\Users\milla\Static_Breakout\tp_scan_by_level_full.csv"
assert Path(csv_path).exists(), f"File not found: {csv_path}"

df = pd.read_csv(csv_path)

num_cols = ["tp_pct","avg_ret","median_ret","pct_pos","n_valid","tp_count","timed_count","hold_days","market_level"]
for c in num_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

m5 = df["market_level"] == 5
if m5.any():
    df.loc[m5, "avg_ret"] = df.loc[m5, "avg_ret"] / 100.0

out_path = r"C:\Users\milla\Static_Breakout\tp_scan_by_level_fixed.csv"
df.to_csv(out_path, index=False)
print(f"Wrote: {out_path}")
