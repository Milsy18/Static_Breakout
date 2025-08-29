import os
from pathlib import Path
import pandas as pd

ROOT = Path(".")
MAX_FILES = 2000

def quick_read(p):
    try:
        if p.suffix.lower()==".csv": return pd.read_csv(p, nrows=1000)
        elif p.suffix.lower() in [".parquet",".pq",".feather",".ftr"]: return pd.read_parquet(p)
    except Exception: pass
    return None

def classify(cols):
    lc = set([c.lower() for c in cols])
    if {"trade_id","bar_index","ret_from_entry","date"}.issubset(lc): return "TRADE_BAR_PATH"
    if {"open","high","low","close"}.issubset(lc) and ("date" in lc or "timestamp" in lc): return "OHLCV"
    if ("symbol" in lc and ("date" in lc or "timestamp" in lc)) and any(x in lc for x in ["ret_net","ret","pnl","pnl_pct"]):
        return "TRADE_SUMMARY"
    return None

rows=[]
for dirpath,_,files in os.walk(ROOT):
    dp = Path(dirpath)
    if any(x in dp.parts for x in [".git","node_modules",".venv","venv","__pycache__","dist","build",".mypy_cache"]): 
        continue
    for fn in files:
        if not fn.lower().endswith((".csv",".parquet",".pq",".feather",".ftr")): 
            continue
        p = Path(dirpath)/fn
        df = quick_read(p)
        if df is None or df.empty: 
            continue
        kind = classify(df.columns)
        if kind:
            rows.append((str(p), kind, len(df), len(df.columns), ", ".join(list(df.columns)[:12])))

# Write results
if rows:
    rows.sort(key=lambda r: {"TRADE_BAR_PATH":0,"OHLCV":1,"TRADE_SUMMARY":2}.get(r[1],9))
    with open("DATA_CANDIDATES.md","w",encoding="utf-8") as f:
        f.write("# Data Candidates Report\n\n")
        for title in ["TRADE_BAR_PATH","OHLCV","TRADE_SUMMARY"]:
            f.write(f"## {title}\n")
            any_written=False
            for path, klass, nrows, ncols, cols in rows:
                if klass==title:
                    f.write(f"- {path} — rows {nrows}, cols {ncols}\n  - columns: {cols}\n")
                    any_written=True
            if not any_written:
                f.write("_No candidates found._\n")
            f.write("\n")
    print("Wrote DATA_CANDIDATES.md")
else:
    print("No candidate data files found.")
