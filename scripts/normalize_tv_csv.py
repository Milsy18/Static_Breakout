#!/usr/bin/env python3
import sys, pandas as pd
from pathlib import Path

if len(sys.argv)!=3:
    print("Usage: normalize_tv_csv.py <in> <out>")
    raise SystemExit(1)

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
df = pd.read_csv(src)
lc = {c.lower(): c for c in df.columns}

# TradingView export: time in milliseconds; otherwise accept 'date'
if "time" in lc:
    df["date"] = pd.to_datetime(df[lc["time"]], unit="ms", errors="coerce")
elif "date" in lc:
    df["date"] = pd.to_datetime(df[lc["date"]], errors="coerce")
else:
    raise SystemExit("No time/date column found")

close_col = lc.get("close") or lc.get("value")
if not close_col:
    raise SystemExit("No close/value column found")

out = df[["date", close_col]].rename(columns={close_col:"close"}).dropna().sort_values("date")
dst.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(dst, index=False)
print(f"wrote {dst} rows={len(out)}")
