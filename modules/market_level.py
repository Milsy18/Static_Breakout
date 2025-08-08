
# modules/market_level.py

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------- helpers

def _coerce_macro(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, coerce types, and ensure one row per calendar day."""
    if df is None or len(df) == 0:
        raise ValueError("df_macro is empty")

    # lower/strip names and unify known synonyms
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    ren = {
        "time": "date",
        "timestamp": "date",
        "total": "total_cap",
    }
    df = df.rename(columns=ren)

    required = {"date", "btc_d", "usdt_d", "total_cap", "total3"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"missing macro columns: {missing}, present: {list(df.columns)}")

    # parse date -> midnight (no tz), keep only needed cols
    keep = ["date", "btc_d", "usdt_d", "total_cap", "total3"]
    out = df[keep].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")
    out = out.dropna(subset=["date"]).sort_values("date")

    # if duplicates per day, keep the last (already sorted)
    out = out.groupby("date", as_index=False).last()

    # numeric coercion + basic gap-fill to remove stray NaNs before scoring
    for c in ["btc_d", "usdt_d", "total_cap", "total3"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out[["btc_d", "usdt_d", "total_cap", "total3"]] = (
        out[["btc_d", "usdt_d", "total_cap", "total3"]].ffill().bfill()
    )

    return out


def normalize_series(s: pd.Series, lookback: int = 14, invert: bool = False) -> pd.Series:
    """Rolling min-max normalize to [0, 1]; optionally invert (dominance)."""
    s = pd.to_numeric(s, errors="coerce")
    roll_min = s.rolling(lookback, min_periods=1).min()
    roll_max = s.rolling(lookback, min_periods=1).max()
    denom = (roll_max - roll_min).replace(0, np.nan)

    norm = (s - roll_min) / denom
    if invert:
        norm = 1 - norm
    return norm.clip(0, 1)


def score_component(norm_series: pd.Series) -> pd.Series:
    """Map normalized [0,1] to integer 1..9 (nullable Int64)."""
    # multiply by 8, round to nearest, then +1 -> 1..9
    # keep as Int64 (nullable) to avoid dtype errors with NaN
    out = (norm_series * 8).round().clip(0, 8) + 1
    return out.astype("Int64")


def levels_from_history(values: pd.Series, dates: pd.Series, train_end: str = "2024-12-31") -> pd.Series:
    """
    Convert a continuous composite into 1..9 using fixed cutpoints
    computed on a *training* window only (no leakage).
    """
    vals = pd.to_numeric(values, errors="coerce")
    d = pd.to_datetime(dates, errors="coerce")

    # training slice
    mask_train = (d <= pd.to_datetime(train_end)) & vals.notna()
    train = vals[mask_train]

    # if training is tiny, fall back to all non-NA values
    if train.size < 100:
        train = vals.dropna()

    if train.empty:
        # degenerate fallback: all neutral
        return pd.Series(pd.array([5] * len(vals), dtype="Int64"), index=values.index)

    # choose cutpoints (tweak if you want different tail widths)
    qs = [0.05, 0.15, 0.30, 0.45, 0.60, 0.75, 0.85, 0.95]
    cuts = np.quantile(train, qs).tolist()
    # ensure strictly increasing (guard rare flat distributions)
    for i in range(1, len(cuts)):
        if cuts[i] <= cuts[i - 1]:
            cuts[i] = np.nextafter(cuts[i - 1], np.inf)

    bins = [-np.inf, *cuts, np.inf]
    labels = list(range(1, 10))
    lvl = pd.cut(vals, bins=bins, labels=labels)

    # return as nullable Int64 and fill early NaNs softly
    lvl = lvl.astype("Int64")
    # fill beginning gaps with nearest known level, then neutral if still missing
    lvl = lvl.ffill().bfill().fillna(pd.NA)
    return lvl


# ---------- main API

def compute_market_level(df_macro: pd.DataFrame, *, lookback: int = 14, train_end: str = "2024-12-31") -> pd.DataFrame:
    """
    Build a 1..9 market regime from macro inputs.
    Expects df_macro with columns: date, btc_d, usdt_d, total_cap, total3.
    Returns: DataFrame[date(datetime64[ns]), market_level(Int64)]
    """
    df = _coerce_macro(df_macro)

    # per-component rolling normalization
    df["btc_norm"] = normalize_series(df["btc_d"], lookback=lookback, invert=True)
    df["usdt_norm"] = normalize_series(df["usdt_d"], lookback=lookback, invert=True)
    df["total_norm"] = normalize_series(df["total_cap"], lookback=lookback, invert=False)
    df["total3_norm"] = normalize_series(df["total3"], lookback=lookback, invert=False)

    # map to 1..9 sub-scores
    df["btc_score"] = score_component(df["btc_norm"])
    df["usdt_score"] = score_component(df["usdt_norm"])
    df["total_score"] = score_component(df["total_norm"])
    df["total3_score"] = score_component(df["total3_norm"])

    # composite + light smoothing
    df["avg_raw"] = (
        df[["btc_score", "usdt_score", "total_score", "total3_score"]]
        .apply(pd.to_numeric, errors="coerce")
        .mean(axis=1)
    )
    df["avg_smooth"] = (df["avg_raw"] + df["avg_raw"].shift(1)) / 2

    # fixed historical quantile mapping -> 1..9
    df["market_level"] = levels_from_history(df["avg_smooth"], df["date"], train_end=train_end)

    # final tidy frame
    out = df[["date", "market_level"]].copy()

    # normalize date dtype to midnight (for merges) and ensure Int64 (nullable)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")
    out["market_level"] = out["market_level"].astype("Int64")

    # if any residual NA (should be rare), use neutral 5
    out["market_level"] = out["market_level"].fillna(5).astype("Int64")

    return out
