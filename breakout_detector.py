# breakout_detector.py

import pandas as pd
from .entry_score    import evaluate_entry
from .market_level   import compute_market_level

def detect_breakouts(df_indicators, df_macro, static_adj=0.0, std_mult=0.5):
    # Compute market level from macro input
    df_market = compute_market_level(df_macro)

    # Join with indicator data
    df = df_indicators.copy()
    df = df.merge(df_market, on="date", how="left")

    breakouts = []

    for symbol in df["symbol"].unique():
        df_sym = df[df["symbol"] == symbol].sort_values("date").copy()

        # Precompute moving average of score_norm for dynamic threshold
        score_trail = []

        for i in range(len(df_sym)):
            row = df_sym.iloc[i]
            market_level = int(row["market_level"]) if not pd.isna(row["market_level"]) else 5

            # Dynamic threshold: mean + std over trailing N bars
            lookback = 100
            recent_scores = pd.Series(score_trail[-lookback:]) if score_trail else pd.Series(dtype=float)
            mean_score = recent_scores.mean() if not recent_scores.empty else 0
            std_score = recent_scores.std() if not recent_scores.empty else 0

            # Track score of previous bar
            if i > 0:
                row["score_norm_prev"] = df_sym.iloc[i - 1].get("score_norm", 0)
            else:
                row["score_norm_prev"] = 0

            result = evaluate_entry(row, market_level, mean_score, std_score, static_adj, std_mult)
            score_trail.append(result["score_norm"])

            if result["entry_signal"]:
                breakouts.append({
                    "symbol": row["symbol"],
                    "entry_date": row["date"],
                    "entry_price": row["close"],
                    "market_level": market_level,
                    **{k: v for k, v in result.items() if k.startswith("score_")}
                })

    return pd.DataFrame(breakouts)
