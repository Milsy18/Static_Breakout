# scripts/align_breakout_schema.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
import numpy as np

PROC = Path("Data/Processed")
MAN_IN  = PROC/"manual_entry_breakouts.csv"          # your manual ledger (commit this file to repo)
STAT_IN = PROC/"static_master_breakouts.csv"         # produced by generator + label_exits.py

CANON_ORDER = [
    "symbol","entry_date","entry_price","market_level",
    "score_trd","score_vty","score_vol","score_mom","score_total","score_norm",
    "exit_date","exit_price","exit_reason",
    "ret_pct","hold_days","win"
]

# column synonyms → canonical
MAP = {
    # entry
    "date":"entry_date","entry time":"entry_date","entry_datetime":"entry_date",
    "entry":"entry_date",
    "entry price":"entry_price","open_price":"entry_price",
    "mkt_lvl":"market_level","market level":"market_level",
    # exits
    "exit":"exit_date","exit time":"exit_date","exit_datetime":"exit_date",
    "exit price":"exit_price","close_price":"exit_price",
    "reason":"exit_reason",
    # scores
    "trd":"score_trd","trend_score":"score_trd",
    "vty":"score_vty","volatility_score":"score_vty",
    "vol":"score_vol","volume_score":"score_vol",
    "mom":"score_mom","momentum_score":"score_mom",
    "total":"score_total","score_total_raw":"score_total",
    "score":"score_norm","score_norm_pct":"score_norm",
}

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace("  "," ").replace("\n"," ") for c in df.columns]
    df = df.rename(columns={c: MAP.get(c, c) for c in df.columns})
    return df

def _parse_types(df: pd.DataFrame) -> pd.DataFrame:
    # dates
    for c in ("entry_date","exit_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.floor("D")
    # numerics
    num_cols = ["entry_price","exit_price","score_trd","score_vty","score_vol","score_mom","score_total","score_norm","ret_pct"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "market_level" in df.columns:
        df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").round().astype("Int64")
    if "exit_reason" in df.columns:
        df["exit_reason"] = df["exit_reason"].astype("string").str.upper().str.strip()
        df["exit_reason"] = df["exit_reason"].replace({"TP":"TP","TIME":"TIME","RSI":"RSI","DEG":"DEG"})
    # derived
    if "ret_pct" not in df.columns and {"entry_price","exit_price"} <= set(df.columns):
        df["ret_pct"] = (df["exit_price"]/df["entry_price"] - 1.0) * 100.0
    if "hold_days" not in df.columns and {"entry_date","exit_date"} <= set(df.columns):
        df["hold_days"] = (df["exit_date"] - df["entry_date"]).dt.days.astype("Int64")
    if "win" not in df.columns and "ret_pct" in df.columns:
        df["win"] = (df["ret_pct"] > 0).astype("Int8")
    return df

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in CANON_ORDER:
        if c not in df.columns:
            df[c] = pd.NA
    return df[CANON_ORDER]

def _load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _norm_cols(df)
    df = _parse_types(df)
    df = _ensure_columns(df)
    # basic sanity
    df = df.dropna(subset=["symbol","entry_date","entry_price"], how="any")
    return df

def main():
    if not MAN_IN.exists():
        raise FileNotFoundError(f"Missing {MAN_IN}. Commit your manual file to this path.")
    if not STAT_IN.exists():
        raise FileNotFoundError(f"Missing {STAT_IN}. Generate it (generator + label_exits.py) or commit it.")

    man = _load(MAN_IN)
    stat = _load(STAT_IN)

    # write aligned
    (PROC/"manual_breakouts_aligned.csv").parent.mkdir(parents=True, exist_ok=True)
    man.to_csv(PROC/"manual_breakouts_aligned.csv", index=False)
    stat.to_csv(PROC/"static_breakouts_aligned.csv", index=False)

    both = pd.concat([man.assign(source="manual"), stat.assign(source="static")], ignore_index=True)
    both.to_csv(PROC/"master_breakouts_all.csv", index=False)

    print("✅ Wrote:")
    print(" - Data/Processed/manual_breakouts_aligned.csv", len(man))
    print(" - Data/Processed/static_breakouts_aligned.csv", len(stat))
    print(" - Data/Processed/master_breakouts_all.csv", len(both))

if __name__ == "__main__":
    main()
