# modules/market_level.py

import numpy as np
import pandas as pd


# ---------- helpers ----------

def normalize_series(series: pd.Series, lookback: int = 14, invert: bool = False) -> pd.Series:
    """
    Rolling min-max normalization to [0,1]. Optionally invert (useful for dominance).
    """
    s = pd.to_numeric(series, errors="coerce")
    min_val = s.rolling(lookback, min_periods=1).min()
    max_val = s.rolling(lookback, min_periods=1).max()
    denom = (max_val - min_val).replace(0, np.nan)
    norm = (s - min_val) / denom
    if invert:
        norm = 1 - norm
    return norm.clip(0, 1)


def score_component(norm_series: pd.Series) -> pd.Series:
    """Map normalized series to 1–9 buckets."""
    return (norm_series * 8).round().clip(1, 9)


def _alias_columns(df: pd.DataFrame, candidates, target: str):
    """
    Rename the first existing name in `candidates` to `target` (in place).
    """
    for c in candidates:
        if c in df.columns:
            if c != target:
                df.rename(columns={c: target}, inplace=True)
            return


# ---------- main API ----------

def compute_market_level(df_macro: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a 1..9 'market_level' per date from macro inputs.

    Accepts flexible input headers (case/period-insensitive) and supports a
    safe fast-path: we will only reuse a precomputed 'market_level' column
    if the required raw inputs are NOT present. If raw inputs exist, we
    recompute the regime score.

    Logical fields expected (any common aliases accepted):
      - date (or time)
      - btc_d     : BTC dominance (%)
      - usdt_d    : USDT dominance (%)
      - total_cap : total crypto market cap
      - total3    : total cap ex BTC & ETH
    """
    # --- normalize headers & date ---
    df = df_macro.copy()
    df.columns = [str(c).lower().replace(".", "_").strip() for c in df.columns]
    if "date" not in df.columns and "time" in df.columns:
        df.rename(columns={"time": "date"}, inplace=True)
    if "date" not in df.columns:
        raise KeyError("macro dataframe must contain a 'date' or 'time' column")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # --- align common header variations to canonical names ---
    _alias_columns(df, ["btc_d", "btc_dominance", "btcd", "btc_dom", "btc_d_close", "btcdominance"], "btc_d")
    _alias_columns(df, ["usdt_d", "usdt_dom", "usdtd", "usdt_dominance"], "usdt_d")
    _alias_columns(df, ["total_cap", "total", "total_mcap", "total_marketcap", "total_mktcap", "totalcap"], "total_cap")
    _alias_columns(df, ["total3", "total_3", "total3_cap", "total3_mcap", "total_ex_btc_eth"], "total3")

    raw_cols = {"btc_d", "usdt_d", "total_cap", "total3"}

    # --- reuse precomputed market_level ONLY if raw inputs are absent ---
    if "market_level" in df.columns and not raw_cols.intersection(df.columns):
        out = df[["date", "market_level"]].copy()
        out["market_level"] = (
            pd.to_numeric(out["market_level"], errors="coerce")
            .fillna(5).clip(1, 9).astype(int)
        )
        return out

    # --- require raw inputs and make them numeric ---
    missing = [c for c in raw_cols if c not in df.columns]
    if missing:
        raise KeyError(f"missing macro columns: {missing}. Present: {list(df.columns)}")

    for c in raw_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[list(raw_cols)] = df[list(raw_cols)].ffill().bfill()

    # --- components (dominances inverted; caps positive) ---
    df["btc_norm"]    = normalize_series(df["btc_d"],  invert=True)
    df["usdt_norm"]   = normalize_series(df["usdt_d"], invert=True)
    df["total_norm"]  = normalize_series(df["total_cap"])
    df["total3_norm"] = normalize_series(df["total3"])

    df["btc_score"]    = score_component(df["btc_norm"])
    df["usdt_score"]   = score_component(df["usdt_norm"])
    df["total_score"]  = score_component(df["total_norm"])
    df["total3_score"] = score_component(df["total3_norm"])

    # --- combine & smooth ---
    df["avg_raw"] = (df["btc_score"] + df["usdt_score"] + df["total_score"] + df["total3_score"]) / 4.0
    df["avg_smooth"] = (df["avg_raw"] + df["avg_raw"].shift(1)) / 2.0

    # --- rank-based bucketing: guaranteed spread across 1..9 ---
    ser = df["avg_smooth"].dropna()
    if ser.empty:
        df["market_level"] = 5
    else:
        ranks = df["avg_smooth"].rank(pct=True, method="average")  # 0..1
        df["market_level"] = (
            np.ceil(ranks * 9)    # → 1..9
              .astype("Int64")
              .clip(1, 9)
              .astype(int)
        )

    return df[["date", "market_level"]]
