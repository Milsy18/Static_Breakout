# modules/breakout_detector.py

from __future__ import annotations
import warnings
from pandas.errors import SettingWithCopyWarning

warnings.simplefilter("ignore", SettingWithCopyWarning)

import pandas as pd

from .entry_score import evaluate_entry
from .market_level import compute_market_level

# optional progress bar
try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(x, **kwargs):
        return x

__all__ = ["detect_breakouts"]


def _to_day(s: pd.Series) -> pd.Series:
    """Normalize any datetime-like to naive midnight (calendar day)."""
    s = pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s.dt.date)


def detect_breakouts(
    df_indicators: pd.DataFrame,
    df_macro: pd.DataFrame,
    static_adj: float = 0.0,
    std_mult: float = 0.5,
    lookback: int = 100,
) -> pd.DataFrame:
    """
    Detect breakout entries per symbol.

    Expects df_indicators with at least: ['date','symbol','close', ...].
    df_macro is used by compute_market_level() to derive the market regime.
    """
    # 1) compute market level from macro, then join to indicators
    df_market = compute_market_level(df_macro).copy()
    df_market["date"] = _to_day(df_market["date"])

    df = df_indicators.copy()
    df["date"] = _to_day(df["date"])

    df = df.merge(df_market, on="date", how="left")

    # quick sanity: how many rows actually received a market_level?
    if len(df):
        match_rate = float(df["market_level"].notna().mean())
        if match_rate < 0.5:
            print(f"[breakout_detector] warning: only {match_rate:.1%} of rows matched a market_level on date join")

    breakouts = []

    # 2) process symbol by symbol
    symbols = df["symbol"].dropna().unique()
    for symbol in tqdm(sorted(symbols), desc="Detecting breakouts"):
        df_sym = (
            df.loc[df["symbol"] == symbol]
              .sort_values("date")
              .reset_index(drop=True)
              .copy()
        )

        # trail of computed score_norm (from evaluate_entry results)
        score_trail: list[float] = []

        for i in range(len(df_sym)):
            row = df_sym.iloc[i]

            # regime level (fallback to neutral 5 if missing)
            ml = row.get("market_level")
            market_level = int(ml) if pd.notna(ml) else 5

            # trailing stats for dynamic threshold
            if score_trail:
                recent = pd.Series(score_trail[-lookback:])
                mean_score = float(recent.mean())
                std_score = float(recent.std())
                prev_score_norm = float(score_trail[-1])
            else:
                mean_score = 0.0
                std_score = 0.0
                prev_score_norm = 0.0

            row_dict = row.to_dict()
            row_dict["score_norm_prev"] = prev_score_norm

            result = evaluate_entry(
                row_dict, market_level, mean_score, std_score, static_adj, std_mult
            )

            score_trail.append(float(result.get("score_norm", 0.0)))

            if result.get("entry_signal"):
                out = {
                    "symbol": row_dict.get("symbol"),
                    "entry_date": row_dict.get("date"),
                    "entry_price": row_dict.get("close"),
                    "market_level": market_level,
                }
                out.update({k: v for k, v in result.items() if k.startswith("score_")})
                breakouts.append(out)

    return pd.DataFrame(breakouts)

