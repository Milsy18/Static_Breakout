# modules/market_level.py

import pandas as pd
import numpy as np

def normalize_series(series, lookback=14, invert=False):
    """Normalize to 0–1 range over rolling window. Optionally invert (for dominance)."""
    min_val = series.rolling(lookback).min()
    max_val = series.rolling(lookback).max()
    norm = (series - min_val) / (max_val - min_val)
    if invert:
        norm = 1 - norm
    return norm.clip(0, 1)

def score_component(norm_series):
    return (norm_series * 8).round().clip(1, 9)

def compute_market_level(df_macro):
    """df_macro must contain: date, btc_d, usdt_d, total_cap, total3"""
    df = df_macro.copy()

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
    df["market_level"] = df["avg_smooth"].round().clip(1, 9)

    return df[["date", "market_level"]]
