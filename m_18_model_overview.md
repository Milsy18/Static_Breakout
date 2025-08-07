# M18 Model – Structured Overview

## Objective
The **M18 Model** is a regime-aware, multi-module trading system purpose-built to **identify and capitalize on breakout setups with a high probability of producing 30%+ returns within a short-term window** (typically 5–11 bars). Rather than waiting for rare events, the model is engineered to detect **frequent, high-quality breakout candidates**, often surfacing **~1 actionable setup per day** from a curated asset universe.

---

## Design Philosophy
- **Conviction-Only Entries**: Trade setups must satisfy multiple green-zone thresholds — across trend, momentum, volatility, and volume — with dynamic cutoffs.
- **Macro-Aware**: All thresholds, score weights, and exit rules shift based on the real-time **Market Level** (M/L 1–9), which reflects macro crypto regime conditions.
- **Transparency & Explainability**: Each signal includes detailed modular scoring, entry/exit rationale, and is tracked in a structured breakout ledger.
- **Repeatable Edge**: Signals are generated with consistent logic that is fully back-testable and automatable.

---

## Modular Scoring Framework
Each bar is scored across **four modules**, producing a composite score out of 100:

| Module         | Max Score | Indicators Used                                |
|----------------|-----------|------------------------------------------------|
| **TREND** (TRD) | 54        | EMA(10, 50, 100, 200), ADX                     |
| **VOLATILITY** (VTY) | 21  | ATR%, ATR Ratio, StdDev %, BBW %, Range %     |
| **VOLUME** (VOL) | 16      | OBV, CMF, Volume Spike, Volume/Price, Slope    |
| **MOMENTUM** (MOM) | 9     | RSI, Stoch %K, MACD Line, MACD Hist            |

Each module uses a **green/yellow/red** scoring system (1.0 / 0.5 / 0.0) based on **regime-aware thresholds**.

---

## Market Level (M/L) Intelligence
A dynamic **Market Level score (1–9)** is derived daily from macro indicators:
- **BTC.D** (Bitcoin Dominance)
- **USDT.D** (Stablecoin Dominance)
- **TOTAL / TOTAL3** Market Cap

The model uses M/L to:
- Adjust thresholds for every sub-indicator
- Modify composite score weights
- Set exit timing and target levels

> Example: In bullish regimes (M/L 8–9), **momentum and volume carry more weight**, while in bearish regimes (M/L 1–3), **trend stability becomes more critical**.

---

## Entry Logic
To qualify for entry, a bar must meet all the following conditions:
1. **EMA5 Gatekeeper**: 3-day EMA5 % gain must exceed M/L-specific threshold.
2. **Score Normalized Threshold**: Must beat both a **static floor** and a **dynamic z-score** cutoff.
3. **Green Zone Confirmation**: Composite score and EMA5 slope must both be green.
4. **RSI Cap**: RSI must be below its regime-specific maximum (to avoid entering overheated trades).

If all are met, a trade is opened on the **next bar open**.

---

## Exit Logic
Trades are closed automatically under **any one** of the following regime-dependent conditions:
- ✅ **TP (Take-Profit)**: Price exceeds a target % above entry (e.g., 33–45% depending on M/L).
- ⏳ **TIME Exit**: Maximum bars-in-trade exceeded (5–11 bars depending on M/L).
- 📉 **RSI Exit**: RSI exceeds cap and then drops >5 points.
- ⚠️ **Degradation Exit**: Price falls below a regime-specific trailing threshold.

Each trade is tagged by **exit reason** and logged for analysis.

---

## Signal Frequency & Deployment
Backtesting shows the model identifies **~1 strong signal per day**, per ~200-coin universe. This provides:
- **Frequent opportunities**, even in moderate markets.
- A manageable, **high-conviction watchlist** each day.
- Robust coverage without signal overload.

---

## Additional Features
- **Z-Score Normalization**: OBV and CMF signals are normalized over 100 bars to prevent distortion.
- **Dashboard Table**: On-chart summary includes TRD/VTY/VOL/MOM, EMA trends, RSI status, A/D strength, trade state, and P/L.
- **Green Dot Markers**: Every entry is visually annotated with markers and signal strength for clarity.

---

## Use Cases
- **Manual Entry**: Traders can use the TradingView overlay to spot and act on breakouts.
- **Automated Scanning**: Python-based workflows scan every coin daily to log qualified breakouts.
- **Performance Refinement**: Every signal (manual or model-detected) is stored in a **Master Breakout Ledger** for further backtesting, analytics, and optimization.

