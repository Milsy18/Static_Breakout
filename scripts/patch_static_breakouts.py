from pathlib import Path
import pandas as pd

fp = Path("Data/Processed/static_breakouts.csv")
bak = Path("Data/Processed/static_breakouts_prepatch.csv")

df = pd.read_csv(fp)
df.to_csv(bak, index=False)

required = [
    "symbol","entry_date","entry_price","exit_date","exit_price","exit_reason",
    "market_level_at_entry","score_trd","score_vty","score_vol","score_mom","score_total",
    "success","days_in_trade"
]

# Coerce dates for sanity check
for c in ["entry_date","exit_date"]:
    df[c] = pd.to_datetime(df[c], errors="coerce")

# Nulls in any required field OR exit_reason == "MISSING"
mask_bad = df[required].isnull().any(axis=1) | (df["exit_reason"].fillna("")=="MISSING")

# Also drop obviously bad date order if any slipped through
mask_bad |= (df["exit_date"] < df["entry_date"])

removed = int(mask_bad.sum())
df_clean = df.loc[~mask_bad].copy()

# Reformat dates back to YYYY-MM-DD strings
for c in ["entry_date","exit_date"]:
    df_clean[c] = df_clean[c].dt.strftime("%Y-%m-%d")

df_clean.to_csv(fp, index=False)
print(f"Removed {removed} bad row(s). Final rows: {len(df_clean)}")
