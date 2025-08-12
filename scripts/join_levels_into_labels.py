#!/usr/bin/env python3
import argparse, pandas as pd
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--labels", required=True)
p.add_argument("--breakouts-with-levels", required=True)
p.add_argument("--out", required=True)
args = p.parse_args()

L = pd.read_parquet(args.labels)
B = pd.read_csv(args.breakouts_with_levels)

# Try to find the level column name robustly
cand = [c for c in B.columns if c.lower() in {
    "market_level_at_entry","market_level","mkt_level","level_at_entry","level"
}]
if not cand:
    raise SystemExit("No market level column found in breakouts_with_levels.csv")
lvl_col = cand[0]

# Parse times and merge on symbol + entry_time
for df in (L, B):
    if "entry_time" in df.columns:
        df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce")

cols = ["symbol","entry_time", lvl_col]
B2 = B.loc[:, cols].rename(columns={lvl_col:"market_level_at_entry"})
L2 = L.drop(columns=[c for c in ["market_level_at_entry"] if c in L.columns], errors="ignore")
M  = L2.merge(B2, on=["symbol","entry_time"], how="left")

# Save (overwrite labels_v2)
Path(args.out).parent.mkdir(parents=True, exist_ok=True)
M.to_parquet(args.out, index=False)

miss = int(M["market_level_at_entry"].isna().sum())
print(f"[levels->labels] rows={len(M)}, missing market_level_at_entry={miss}")
