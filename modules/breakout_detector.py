# build_macro_regime_data.py
# Create Data/Raw/macro_regime_data.csv from btc_d/usdt_d/total/total3 raw files.

import re
import sys
import glob
from pathlib import Path
import pandas as pd

RAW_DIR = Path("Data/Raw")
OUT_CSV = RAW_DIR / "macro_regime_data.csv"

# ---- utilities --------------------------------------------------------------

def to_day(s):
    s = pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s.dt.date)  # drop time/tz -> naive midnight

def find_one(patterns):
    for pat in patterns:
        hits = sorted(glob.glob(str(RAW_DIR / pat)))
        if hits:
            return Path(hits[0])
    return None

def coerce_numeric(series: pd.Series) -> pd.Series:
    # strip %, commas, spaces; then to_numeric
    return pd.to_numeric(series.astype(str).str.replace(r"[%,\s]", "", regex=True), errors="coerce")

def pick_value_column(df: pd.DataFrame):
    # try common names in order, otherwise last numeric
    candidates = ["close","value","dominance","dom","price","adj_close","total","marketcap","cap"]
    cols = [c.lower().strip().replace(".","_") for c in df.columns]
    rename = dict(zip(df.columns, cols))
    df = df.rename(columns=rename)

    for c in candidates:
        if c in df.columns:
            return df, c
    # otherwise choose last numeric column
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(pd.to_numeric(df[c], errors="ignore"))]
    if not numeric_cols:
        # try converting everything to numeric and re-evaluate
        tmp = df.apply(lambda s: pd.to_numeric(s, errors="coerce"))
        numeric_cols = [c for c in tmp.columns if tmp[c].notna().any()]
    if not numeric_cols:
        raise ValueError("Could not find a numeric value column in file.")
    return df, numeric_cols[-1]

def load_series(label: str, file_patterns, date_candidates=("date","time","timestamp")) -> pd.DataFrame:
    p = find_one(file_patterns)
    if not p:
        raise FileNotFoundError(f"{label}: could not locate any of {file_patterns} under {RAW_DIR}")
    df = pd.read_csv(p)
    # normalize headers
    df.columns = [str(c).lower().strip().replace(".","_") for c in df.columns]

    # date col
    date_col = next((c for c in date_candidates if c in df.columns), None)
    if date_col is None:
        raise KeyError(f"{label}: no date column found among {date_candidates} in {p.name}")
    df["date"] = to_day(df[date_col])

    # drop rows without date
    df = df.dropna(subset=["date"]).sort_values("date")

    # value col
    df, val_col = pick_value_column(df)
    df[label] = coerce_numeric(df[val_col])

    # minimal frame
    out = df[["date", label]].copy()

    # resample to daily, take last available value that day
    out = (out.groupby("date", as_index=False)[label].last()
              .sort_values("date")
              .reset_index(drop=True))
    return out

# ---- main build -------------------------------------------------------------

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    btc   = load_series("btc_d",   ["btc_d.csv",   "*btc*d*.csv"])
    usdt  = load_series("usdt_d",  ["usdt_d.csv",  "*usdt*d*.csv"])
    total = load_series("total_cap", ["total.csv", "*total*cap*.csv", "total_cap.csv"])
    total3= load_series("total3",  ["total3.csv",  "*total3*.csv", "total_ex_btc_eth.csv"])

    # merge all on calendar day
    df = btc.merge(usdt,  on="date", how="outer")
    df = df.merge(total,  on="date", how="outer")
    df = df.merge(total3, on="date", how="outer")
    df = df.sort_values("date").reset_index(drop=True)

    # forward/back fill small gaps
    for c in ["btc_d","usdt_d","total_cap","total3"]:
        df[c] = coerce_numeric(df[c])
    df[["btc_d","usdt_d","total_cap","total3"]] = df[["btc_d","usdt_d","total_cap","total3"]].ffill().bfill()

    # guard: drop any precomputed market_level if present
    if "market_level" in df.columns:
        df = df.drop(columns=["market_level"])

    # save
    df.to_csv(OUT_CSV, index=False)

    # quick report
    print(f"✅ wrote {OUT_CSV}")
    print("rows:", len(df), "| date range:", df['date'].min().date(), "→", df['date'].max().date())
    print(df.head().to_string(index=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ build failed:", e)
        sys.exit(1)

