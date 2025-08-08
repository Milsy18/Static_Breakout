# modules/market_level.py
import pandas as pd
import numpy as np

REQ_COLS = {"date", "btc_d", "usdt_d", "total_cap", "total3"}

def _coerce_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lower/normalize col names and coerce types."""
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(".", "_") for c in out.columns]
    if "date" not in out.columns and "time" in out.columns:
        out = out.rename(columns={"time": "date"})
    missing = REQ_COLS.difference(set(out.columns))
    if missing:
        raise KeyError(f"missing macro columns: {missing}, present: {list(out.columns)}")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    for c in ("btc_d", "usdt_d", "total_cap", "total3"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    # one row per calendar day
    out = (out.sort_values("date")
              .drop_duplicates("date", keep="last")
              .reset_index(drop=True))
    return out

def _roll_norm(x: pd.Series, window: int, invert: bool = False) -> pd.Series:
    """Rolling min-max to [0,1], optionally inverted; NA-safe."""
    rmin = x.rolling(window, min_periods=1).min()
    rmax = x.rolling(window, min_periods=1).max()
    denom = (rmax - rmin).replace(0, np.nan)
    z = (x - rmin) / denom
    if invert:
        z = 1 - z
    return z.clip(0, 1)

def _score_1_9(z: pd.Series) -> pd.Series:
    """Map 0–1 to 1–9."""
    return (z * 8).round().clip(1, 9)

def compute_market_level(df_macro: pd.DataFrame, lookback: int = 30) -> pd.DataFrame:
    """
    Build integer market_level 1..9 from macro inputs.
    NA-safe: fills initial/gap NAs so .astype(int) won't blow up.
    """
    df = _coerce_cols(df_macro)

    # Normalize components (invert dominance series so 'lower' dominance = stronger risk-on)
    df["btc_norm"]    = _roll_norm(df["btc_d"],    lookback, invert=True)
    df["usdt_norm"]   = _roll_norm(df["usdt_d"],   lookback, invert=True)
    df["total_norm"]  = _roll_norm(df["total_cap"], lookback, invert=False)
    df["total3_norm"] = _roll_norm(df["total3"],    lookback, invert=False)

    # 1..9 component scores
    df["btc_score"]    = _score_1_9(df["btc_norm"])
    df["usdt_score"]   = _score_1_9(df["usdt_norm"])
    df["total_score"]  = _score_1_9(df["total_norm"])
    df["total3_score"] = _score_1_9(df["total3_norm"])

    # Average & light smoothing
    df["avg_raw"] = (df["btc_score"] + df["usdt_score"] + df["total_score"] + df["total3_score"]) / 4.0
    df["avg_smooth"] = df["avg_raw"].rolling(2, min_periods=1).mean()

    # Final level with robust NA handling
    ml = df[["date"]].copy()
    ml["market_level"] = df["avg_smooth"].round().clip(1, 9)

    # fill the rolling warm-up and any tiny gaps
    ml["market_level"] = (
        ml["market_level"]
        .ffill()
        .bfill()
        .fillna(5)
        .astype(int)
    )

    return ml

