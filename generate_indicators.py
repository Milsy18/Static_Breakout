import os
import pandas as pd
import numpy as np
from ta import momentum, trend, volatility, volume

input_dir = "Data/Filtered_OHLCV"
output_path = "Data/Processed/per_bar_indicators_core.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

all_data = []

def safe_pct_change(series):
    return series.pct_change().replace([np.inf, -np.inf], np.nan)

for filename in os.listdir(input_dir):
    if not filename.endswith(".csv"):
        continue

    path = os.path.join(input_dir, filename)
    symbol = filename.replace(".csv", "")

    try:
        df = pd.read_csv(path)

        # === Clean malformed header rows ===
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.dropna(subset=["date", "open", "high", "low", "close", "volume"], inplace=True)
        df = df[["date", "open", "high", "low", "close", "volume"]].copy()
        df["symbol"] = symbol

        # === Trend
        df["ema5"] = trend.ema_indicator(df["close"], window=5)
        df["ema10"] = trend.ema_indicator(df["close"], window=10)
        df["ema50"] = trend.ema_indicator(df["close"], window=50)
        df["ema100"] = trend.ema_indicator(df["close"], window=100)
        df["ema200"] = trend.ema_indicator(df["close"], window=200)

        df["ema5_pct"] = (df["ema5"] - df["ema5"].shift(3)) / df["ema5"].shift(3) * 100
        df["ema10_pct"] = safe_pct_change(df["ema10"])
        df["ema50_pct"] = safe_pct_change(df["ema50"])
        df["ema100_pct"] = safe_pct_change(df["ema100"])
        df["ema200_pct"] = safe_pct_change(df["ema200"])

        df["adx"] = trend.adx(df["high"], df["low"], df["close"], window=14)

        # === Volatility
        df["atr"] = volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
        df["atr3"] = volatility.average_true_range(df["high"], df["low"], df["close"], window=3)
        df["atr_pct"] = df["atr3"] / df["close"]
        df["atr_ratio"] = df["atr"] / df["atr"].rolling(20).mean()
        df["stddev_pct"] = df["close"].rolling(20).std() / df["close"].rolling(20).mean() * 100
        df["bbw"] = (df["close"].rolling(20).mean() + 2 * df["close"].rolling(20).std()) - \
                    (df["close"].rolling(20).mean() - 2 * df["close"].rolling(20).std())
        df["rng"] = (df["high"] - df["low"]) / df["close"] * 100

        # === Volume
        df["obv"] = volume.on_balance_volume(df["close"], df["volume"])
        df["obv_norm"] = df["obv"] / df["obv"].rolling(20).mean()

        mfv = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"])
        mfv = mfv.replace([np.inf, -np.inf], 0).fillna(0) * df["volume"]
        df["cmf"] = mfv.rolling(20).sum() / df["volume"].rolling(20).sum()

        df["volSpike"] = df["volume"] / df["volume"].rolling(20).mean()
        df["volToPrice"] = df["volume"] / df["close"]
        df["volSlope"] = df["volume"].diff() / df["volume"].shift(1)

        # === Momentum
        df["rsi"] = momentum.rsi(df["close"], window=14)
        df["stoch"] = momentum.stoch(df["high"], df["low"], df["close"], window=14, smooth_window=3)
        macd_line = trend.macd(df["close"], window_slow=26, window_fast=12)
        macd_signal = trend.macd_signal(df["close"], window_slow=26, window_fast=12, window_sign=9)
        df["macd"] = macd_line
        df["macd_signal"] = macd_signal
        df["macd_slope"] = macd_line - macd_signal

        all_data.append(df)

    except Exception as e:
        print(f"⚠️ Failed on {symbol}: {e}")

# === Combine and Save ===
if all_data:
    df_all = pd.concat(all_data)
    df_all.to_csv(output_path, index=False)
    print(f"✅ Indicator dataset saved to {output_path}")
else:
    print("❌ No valid data processed. Please check your OHLCV files.")

