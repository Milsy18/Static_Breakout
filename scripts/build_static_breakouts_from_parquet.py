import subprocess
from pathlib import Path
import pandas as pd

SRC = Path("Data/Processed/labeled_holy_grail_static_221_windows_long.parquet")
OUT = Path("Data/Processed/static_breakouts.csv")

# ---- Load
df = pd.read_parquet(SRC)

# ---- Select & rename to target schema
need = [
    "symbol","entry_date","entry_price","market_level",
    "score_trd","score_vty","score_vol","score_mom","score_total",
    "exit_time","exit_price","exit_reason","success_bin"
]
missing = [c for c in need if c not in df.columns]
if missing:
    raise SystemExit(f"Required columns missing in source parquet: {missing}")

use = df[need].copy()
use.rename(columns={
    "market_level": "market_level_at_entry",
    "exit_time": "exit_date",
    "success_bin": "success",
}, inplace=True)

# ---- Types & derived fields
for c in ["entry_date","exit_date"]:
    use[c] = pd.to_datetime(use[c], errors="coerce")

use["days_in_trade"] = (use["exit_date"] - use["entry_date"]).dt.days
use["pct_return"] = (use["exit_price"] - use["entry_price"]) / use["entry_price"]

# force ints for success if possible
use["success"] = use["success"].fillna(0).astype("int64", errors="ignore")

# ---- Dedup on (symbol, entry_date)
use.sort_values(["symbol","entry_date","score_total"], ascending=[True, True, False], inplace=True)
use = use.drop_duplicates(subset=["symbol","entry_date"], keep="first")

# ---- Add source tag (commit hash if available)
def git_commit():
    try:
        return subprocess.check_output(["git","rev-parse","--short","HEAD"], text=True).strip()
    except Exception:
        return "unknown"
use["source"] = "static_build_v18@" + git_commit()

# ---- Order columns per spec
cols = [
    "symbol","entry_date","entry_price","exit_date","exit_price","exit_reason",
    "market_level_at_entry",
    "score_trd","score_vty","score_vol","score_mom","score_total",
    "success","days_in_trade","pct_return","source"
]
use = use[cols]

# ---- Final formatting
# Dates as YYYY-MM-DD strings
for c in ["entry_date","exit_date"]:
    use[c] = use[c].dt.strftime("%Y-%m-%d")

use.to_csv(OUT, index=False)
print(f"✅ Wrote {OUT} with {len(use):,} rows")
print("Exit reasons:\n", use["exit_reason"].value_counts(dropna=False).head(10))
