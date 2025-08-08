# modules/market_level.py

import numpy as np
import pandas as pd


def _alias_columns(df: pd.DataFrame, candidates, target: str):
    for c in candidates:
        if c in df.columns:
            if c != target:
                df.rename(columns={c: target}, inplace=True)
            return


def _to_day(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s.dt.date)


def _rolling_norm(s: pd.Series, lookback: int = 14, invert: bool = False) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    lo = s.rolling(lookback, min_periods=1).min()
    hi = s.rolling(lookback, min_periods=1).max()
    denom = (hi - lo).replace(0, np.nan)
    z = (s - lo) / denom
    if invert:
        z = 1 - z
    return z.clip(0, 1)


def _rank_norm(s: pd.Series, invert: bool = False) -> pd.Series:
    r = s.rank(pct=True, method="average")
    if invert:
        r = 1 - r
    return r.clip(0, 1)


def _score9(x: pd.Series) -> pd.Series:
    return (x * 8).round().clip(1, 9)


def compute_market_level(df_macro: pd.DataFrame) -> pd.DataFrame:
    """
    Robust market level (1..9) from macro inputs.
    Uses rolling min-max, with a fallback to global rank-normalization
    whenever rolling yields NaNs (e.g., stringy columns or zero-range windows).
    """
    df = df_macro.copy()
    df.columns = [str(c).lower().replace(".", "_").strip() for c in df.columns]

    if "date" not in df.columns and "time" in df.columns:
        df.rename(columns={"time": "date"}, inplace=True)
    if "date" not in df.columns:
        raise KeyError("macro dataframe must contain a 'date' or 'time' column")

    df["date"] = _to_day(df["date"])

    _alias_columns(df, ["btc_d", "btc_dominance", "btcd", "btc_dom", "btc_d_close"], "btc_d")
    _alias_columns(df, ["usdt_d", "usdt_dom", "usdtd", "usdt_dominance"], "usdt_d")
    _alias_columns(df, ["total_cap", "total", "total_mcap", "total_marketcap", "total_mktcap"], "total_cap")
    _alias_columns(df, ["total3", "total_3", "total3_cap", "total3_mcap", "total_ex_btc_eth"], "total3")

    need = ["btc_d", "usdt_d", "total_cap", "total3"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise KeyError(f"missing macro columns: {missing}, present: {list(df.columns)}")

    # Make sure they’re numeric
    for c in need:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[need] = df[need].ffill().bfill()

    # Rolling norms
    r_btc   = _rolling_norm(df["btc_d"],   lookback=14, invert=True)
    r_usdt  = _rolling_norm(df["usdt_d"],  lookback=14, invert=True)
    r_total = _rolling_norm(df["total_cap"], lookback=14, invert=False)
    r_t3    = _rolling_norm(df["total3"],  lookback=14, invert=False)

    # Rank fallback norms
    f_btc   = _rank_norm(df["btc_d"],   invert=True)
    f_usdt  = _rank_norm(df["usdt_d"],  invert=True)
    f_total = _rank_norm(df["total_cap"], invert=False)
    f_t3    = _rank_norm(df["total3"],  invert=False)

    # Use rolling when available else rank
    btc   = r_btc.fillna(f_btc)
    usdt  = r_usdt.fillna(f_usdt)
    total = r_total.fillna(f_total)
    t3    = r_t3.fillna(f_t3)

    # Score components & combine
    df["btc_score"]    = _score9(btc)
    df["usdt_score"]   = _score9(usdt)
    df["total_score"]  = _score9(total)
    df["total3_score"] = _score9(t3)

    df["avg_raw"]    = (df["btc_score"] + df["usdt_score"] + df["total_score"] + df["total3_score"]) / 4.0
    df["avg_smooth"] = (df["avg_raw"] + df["avg_raw"].shift(1)) / 2.0

    ser = df["avg_smooth"].dropna()
    if ser.empty:
        # as last resort, use rank on total cap to spread 1..9
        ranks = df["total_cap"].rank(pct=True, method="average")
    else:
        ranks = df["avg_smooth"].rank(pct=True, method="average")

    df["market_level"] = (
        np.ceil(ranks * 9)
          .astype("Int64")
          .clip(1, 9)
          .astype(int)
    )

    return df[["date", "market_level"]].sort_values("date").reset_index(drop=True)

