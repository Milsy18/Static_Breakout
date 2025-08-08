# modules/breakout_detector.py

import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter("ignore", SettingWithCopyWarning)

import pandas as pd

from .entry_score import evaluate_entry
from .market_level import compute_market_level

# optional nice progress bar
try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(x, **kwargs):
        return x


def detect_breakouts(
    df_indicators: pd.DataFrame,
    df_macro: pd.DataFrame,
    static_adj: float = 0.0,
    std_mult: float = 0.5,
    lookback: int = 100,
) -> pd.DataFrame:
    """
    Detect breakout entries per symbol.

    Expects df_indicators to contain at least: ['date','symbol','close', ...indicators]
    df_macro is passed to compute_market_level() to derive the market regime.

    Parameters
    ----------
    static_adj : float
        Constant adjustment added to the dynamic threshold (bias).
    std_mult : float
        Multiplier for the trailing standard deviation part of the threshold.
    lookback : int
        Window size used when computing the trailing mean/std of score_norm.

    Returns
    -------
    pd.DataFrame with one row per detected entry containing:
      ['symbol','entry_date','entry_price','market_level', 'score_*'...]
    """
    # 1) compute market level from macro, then join to indicators
    df_market = compute_market_level(df_macro)
    df = df_indicators.copy()
    df = df.merge(df_market, on="date", how="left")

    breakouts = []

    # 2) process symbol by symbol
    symbols = df["symbol"].dropna().unique()
    for symbol in tqdm(sorted(symbols), desc="Detecting breakouts"):
        df_sym = df.loc[df["symbol"] == symbol].sort_values("date").reset_index(drop=True)

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

            # build a safe dict (no chained assignment) to feed into evaluate_entry
            row_dict = row.to_dict()
            row_dict["score_norm_prev"] = prev_score_norm

            # evaluate this bar
            result = evaluate_entry(
                row_dict, market_level, mean_score, std_score, static_adj, std_mult
            )

            # keep trail for next iteration
            score_trail.append(float(result.get("score_norm", 0.0)))

            # record breakout
            if result.get("entry_signal"):
                out = {
                    "symbol": row_dict.get("symbol"),
                    "entry_date": row_dict.get("date"),
                    "entry_price": row_dict.get("close"),
                    "market_level": market_level,
                }
                # include all score_* fields from the result
                out.update({k: v for k, v in result.items() if k.startswith("score_")})
                breakouts.append(out)

    return pd.DataFrame(breakouts)

