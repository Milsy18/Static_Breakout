"""
Build enriched feature set for M18 breakout model.

Reads the merged feature/label dataset (`final_merged_with_holy_grail_static.csv`)
and computes additional technical indicators designed to improve breakout
classification. Normalises indicators per market level (1..9) where possible.

Usage:
    python build_features.py --input <csv> --output <csv>

If --output is omitted, the script will print a preview of the enriched
DataFrame instead of writing to disk.
"""

from __future__ import annotations
import argparse
import os
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------
# Feature computation
# ----------------------------------------------------------------------

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # --- EMA ratios ---
    for a, b in [(10, 50), (10, 100), (10, 200), (50, 200)]:
        col = f"ema_ratio_{a}_{b}"
        if f"ema_{a}" in df.columns and f"ema_{b}" in df.columns:
            out[col] = df[f"ema_{a}"] / df[f"ema_{b}"]

    # --- ATR percentage ---
    if "atr" in df.columns and "close" in df.columns:
        out["atr_pct"] = df["atr"] / df["close"]

    # --- Bollinger Band width percentage ---
    if "bbw" in df.columns and "close" in df.columns:
        out["bbw_pct"] = df["bbw"] / df["close"]

    # --- Standard deviation percentage ---
    if "std" in df.columns and "close" in df.columns:
        out["std_pct"] = df["std"] / df["close"]

    # --- Intrabar range percentage ---
    if "high" in df.columns and "low" in df.columns and "close" in df.columns:
        out["range_pct"] = (df["high"] - df["low"]) / df["close"]

    # --- Z-scores for selected features ---
    for base in ["rsi", "adx", "cmf", "rvol", "volume", "obv", "total_score"]:
        if base in df.columns:
            mean, std = df[base].mean(), df[base].std()
            if std and std > 0:
                out[f"{base}_z"] = (df[base] - mean) / std

    # --- Hours since last breakout (temporal spacing) ---
    if "breakout_date" in df.columns:
        try:
            dates = pd.to_datetime(df["breakout_date"], errors="coerce")
            out["hours_since_last"] = (dates - dates.shift(1)).dt.total_seconds() / 3600.0
        except Exception:
            out["hours_since_last"] = np.nan

    # --- MACD impulse strength ---
    if "macd" in df.columns and "macd_signal" in df.columns:
        macd_diff = df["macd"] - df["macd_signal"]
        impulse = []
        run = 0
        for val in macd_diff:
            if val > 0:
                run += 1
            else:
                run = 0
            impulse.append(run)
        out["macd_impulse"] = impulse
        # z-score of impulse
        m, s = np.mean(impulse), np.std(impulse)
        out["macd_impulse_z"] = (np.array(impulse) - m) / s if s > 0 else 0

    # --- Composite volume-flow strength ---
    if "rvol" in out.columns and "cmf" in out.columns:
        out["vol_flow_strength"] = out["rvol"].fillna(0) * out["cmf"].fillna(0)

    return out

# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build enriched features for M18 model")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=False, help="Output CSV path")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    enriched = compute_features(df)

    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        enriched.to_csv(args.output, index=False)
        print(f"Enriched dataset written to {args.output} with shape {enriched.shape}")
    else:
        print(enriched.head())

if __name__ == "__main__":
    main()
