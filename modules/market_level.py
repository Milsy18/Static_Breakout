# modules/market_level.py

import pandas as pd
import numpy as np

def normalize_series(series: pd.Series, lookback: int = 14, invert: bool = False) -> pd.Series:
    """Normalize to [0,1] over a rolling window. Optionally invert (for dominance)."""
    s = pd.to_numeric(series, errors="coerce")
    min_val = s.rolling(lookback, min_periods=1).min()
    max_val = s.rolling(lookback, min_periods=1).max()
    denom = (max_val - min_val).replace(0, np.nan)
    norm = (s - min_val) / denom
    if invert:
        norm = 1 - norm
    return norm.clip(0, 1)

def score_component(norm_series: pd.Series) -> pd.Series:
    """Map normalized series to 1–9 bucketed score."""
    return (norm_series * 8).round().clip(1, 9)

def _alias_columns(df: pd.DataFrame, candidates, target: str):
    """Rename the first matching candidate column to target (in place)."""
    for c in candidates:
        if c in df.columns:
            if c != target:
                df.rename(columns={c: target}, inplace=True)
            return
    # nothing found → let caller handle the error with a clear message

def compute_market_level(df_macro: pd.DataFrame) -> pd.DataFrame:
    """
    Compute regime 'market_level' (1..9) from macro series.
    Accepts flexible input column names and a precomputed fast-path.

    Expected logical fields (after normalization):
      - date (or time)
      - btc_d   : BTC dominance (%)
      - usdt_d  : USDT dominance (%)
      - total_cap : total crypto mkt cap
      - total3  : total mkt cap ex BTC & ETH
    """
    df = df_macro.copy()

    # --- normalize headers & dates ---
    df.columns = [str(c).lower().replace(".", "_").strip() for c in df.columns]
    if "date" not in df.columns and "time" in df.columns:
        df.rename(columns={"time": "date"}, inplace=True)
    if "date" not in df.columns:
        raise KeyError("macro dataframe must contain a 'date' or 'time' column")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # --- fast-path: if 'market_level' already provided, trust it ---
    if "market_level" in df.columns:
        out = df[["date", "market_level"]].copy()
        out["market_level"] = (
            pd.to_numeric(out["market_level"], errors="coerce")
            .fillna(5)
            .clip(1, 9)
            .astype(int)
        )
        return out

    # --- alias common column name variations ---
    _alias_columns(df, ["btc_d", "btc_dominance", "btcd", "btc_dom", "btc_d_close", "btcdominance", "btc_d%"], "btc_d")
    _alias_columns(df, ["usdt_d", "usdt_dom", "usdtd", "usdt_dominance", "usdt_d%"], "usdt_d")
    _alias_columns(df, ["total_cap", "total", "total_mcap", "total_marketcap", "total_mktcap", "totalcap"], "total_cap")
    _alias_columns(df, ["total3", "total_3", "total3_cap", "total3_mcap", "total_ex_btc_eth", "total3_usd"], "total3")

    required = ["btc_d", "usdt_d", "total_cap", "total3"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"missing macro columns: {missing}. Present: {list(df.columns)}")

    # ensure numeric
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # light gap-fill (dominance & cap series are smooth)
    df[required] = df[required].ffill().bfill()

    # --- scoring (same as your original) ---
    df["btc_norm"]     = normalize_series(df["btc_d"],   invert=True)
    df["usdt_norm"]    = normalize_series(df["usdt_d"],  invert=True)
    df["total_norm"]   = normalize_series(df["total_cap"])
    df["total3_norm"]  = normalize_series(df["total3"])

    df["btc_score"]    = score_component(df["btc_norm"])
    df["usdt_score"]   = score_component(df["usdt_norm"])
    df["total_score"]  = score_component(df["total_norm"])
    df["total3_score"] = score_component(df["total3_norm"])

    df["avg_raw"] = (df["btc_score"] + df["usdt_score"] + df["total_score"] + df["total3_score"]) / 4
    df["avg_smooth"] = (df["avg_raw"] + df["avg_raw"].shift(1)) / 2
    df["market_level"] = df["avg_smooth"].round().clip(1, 9).astype(int)

    return df[["date", "market_level"]]
