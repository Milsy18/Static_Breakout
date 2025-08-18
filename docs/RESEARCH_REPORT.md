# Research Report — Feature Expansion (M18 V18.0)

**Goal.** Improve OOS win_rate (primary) and pct_return (secondary) with robust, leak‑free indicators; keep behavior stable across market levels (1–9) and symbols.

## Starting point
- Source frame: `final_merged_with_holy_grail_static.csv`
- Core gates already present: RSI, ADX/DMI, CMF, RVOL, EMA% (10/50/200), BBW.
- Labels available: exit return/type; breakout dates; market_level.

## New features implemented (v18.0 add‑on)
- **Trend/structure:** `ema_ratio_10_50`, `ema_ratio_10_100`, `ema_ratio_10_200`, `ema_ratio_50_200`
- **Volatility/regime:** `atr_pct`, `bbw_pct`, `std_pct`, `range_pct` (intrabar)
- **Momentum/energy:** `macd_impulse`, `macd_impulse_z` (length of >0 MACD diff)
- **Volume/flow:** `vol_flow_strength = rvol * cmf`
- **Temporal:** `hours_since_last` (between breakout bars)
- **Z‑scores:** `rsi_z, adx_z, cmf_z, rvol_z, volume_z, obv_z, total_score_z`

All computed in `scripts/python/build_features.py`. No look‑ahead used.

## Quick importance screen (preview data, RF + permutation)
Train on 500‑row preview (balanced split). Acc ≈ 0.89, AUC ≈ 0.88 (sanity check only).  
Top contributors (|Δ|):
- **range_pct**, **std_pct**, **rsi**, **obv_z**, **ema_10**, **bbw_pct**, **ema_ratio_10_200**, **hours_since_last**, **ema_ratio_50_200**, **cmf**.

Low impact on sample: many generic z‑scores and redundant EMA ratios.

> Note: This was only a fast screener to rank candidates; the real selection happens in the optimization harness with walk‑forward CV by ML and symbol.

## Keep / Drop recommendation
- **Keep:** `range_pct`, `std_pct`, `bbw_pct`, `ema_ratio_10_200`, `hours_since_last`, plus core gates (RSI, ADX, CMF, RVOL, EMA%, BBW).
- **Consider keep (weak‑positive):** `vol_flow_strength`, `ema_ratio_50_200`, `obv_z`, `rsi_z`.
- **Deprioritise:** additional z‑scores/ratios with near‑zero Δ.

## Candidates queued (need OHLCV time series in‑script)
- Donchian width; Supertrend slope; KAMA%; VWAP deviations (session/anchored);
- StochRSI dwell time; MACD histogram impulse length (alt def with zero‑cross counts);
- Parkinson/Garman–Klass vol; realized/forecast vol ratio; entropy.
*These will be added once we extend the builder to rolling‑window OHLCV per symbol.*

## Next steps (ties to T2/T3)
1. **Optimization harness (T2):** rolling time‑series CV stratified by market_level and symbol; objective = OOS win_rate; tie‑breakers pct_return/expectancy and drawdown control. Export:
   - `out/best_params_per_ml.csv`
   - `out/gate_performance_by_ml.csv`
   - `out/equity_curves/*.png`, `out/ablation_*.png`
   - falsification tests (shuffled labels / permuted returns)
2. **Pine (T3):** inject per‑ML thresholds into `configs/pine_defaults_v18.yaml` and render:
   - `scripts/pine/m18_v18_indicator.pine`
   - `scripts/pine/m18_v18_strategy.pine`
   - Compile‑clean v6; single‑line assignments; table overlay; alertconditions.

## Guardrails
- No leakage; strict OOS validation.
- Per‑ML normalization via percentiles/z‑scores (portable thresholds).
- Costs/slippage explicit and configurable.
- Seeds fixed; CLI reproducible; plots + CSVs written under `/out`.
