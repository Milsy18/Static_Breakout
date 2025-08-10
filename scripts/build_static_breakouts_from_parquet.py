import subprocess
from pathlib import Path
import numpy as np
import pandas as pd

SRC = Path("Data/Processed/labeled_holy_grail_static_221_windows_long.parquet")
OUT = Path("Data/Processed/static_breakouts.csv")

df = pd.read_parquet(SRC)

need = [
    "symbol","entry_date","entry_price","market_level",
    "score_trd","score_vty","score_vol","score_mom","score_total",
    "exit_time","exit_price","exit_reason","success_bin"
]
missing = [c for c in need if c not in df.columns]
if missing:
    raise SystemExit(f"Required columns missing in source parquet: {missing}")

use = df[need].copy().rename(columns={
    "market_level": "market_level_at_entry",
    "exit_time": "exit_date",
    "success_bin": "success",
})

# ---- Parse dates
for c in ["entry_date","exit_date"]:
    use[c] = pd.to_datetime(use[c], errors="coerce")

# ---- Coerce numerics
num_cols = ["entry_price","exit_price","score_trd","score_vty","score_vol","score_mom","score_total","market_level_at_entry"]
for c in num_cols:
    use[c] = pd.to_numeric(use[c], errors="coerce")

# ---- Success as 0/1 int
use["success"] = (
    use["success"]
    .replace({True:1, False:0, "True":1, "TRUE":1, "False":0, "FALSE":0})
    .pipe(pd.to_numeric, errors="coerce")
    .fillna(0)
    .astype("int8")
)

# ---- Derived fields (safe)
use["days_in_trade"] = (use["exit_date"] - use["entry_date"]).dt.days

with np.errstate(invalid="ignore", divide="ignore"):
    use["pct_return"] = np.where(
        (use["entry_price"] > 0) & np.isfinite(use["entry_price"]) & np.isfinite(use["exit_price"]),
        (use["exit_price"] - use["entry_price"]) / use["entry_price"],
        np.nan
    )

# ---- Dedup on (symbol, entry_date) keeping highest score_total
use.sort_values(["symbol","entry_date","score_total"], ascending=[True, True, False], inplace=True)
use = use.drop_duplicates(subset=["symbol","entry_date"], keep="first")

# ---- Source tag
def git_commit():
    try:
        return subprocess.check_output(["git","rev-parse","--short","HEAD"], text=True).strip()
    except Exception:
        return "unknown"
use["source"] = "static_build_v18@" + git_commit()

# ---- Final ordering
cols = [
    "symbol","entry_date","entry_price","exit_date","exit_price","exit_reason",
    "market_level_at_entry",
    "score_trd","score_vty","score_vol","score_mom","score_total",
    "success","days_in_trade","pct_return","source"
]
use = use[cols]

# ---- Format dates
for c in ["entry_date","exit_date"]:
    use[c] = use[c].dt.strftime("%Y-%m-%d")

use.to_csv(OUT, index=False)

# ---- Simple summary
print(f"✅ Wrote {OUT} with {len(use):,} rows")
print("Exit reasons (top 10):")
print(use["exit_reason"].value_counts(dropna=False).head(10).to_string())

# Warn if many NaNs remain
na = use.isna().sum().sort_values(ascending=False)
print("\nTop null counts:")
print(na.head(10).to_string())
