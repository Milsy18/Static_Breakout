# Feature Additions — M18 V18.0

| Feature | Formula / Window | Normalization | Status |
|---|---|---|---|
| ema_ratio_10_50 | ema10 / ema50 | raw ratio | added |
| ema_ratio_10_100 | ema10 / ema100 | raw ratio | added |
| ema_ratio_10_200 | ema10 / ema200 | raw ratio | added |
| ema_ratio_50_200 | ema50 / ema200 | raw ratio | added |
| atr_pct | atr / close | pct | added |
| bbw_pct | bbw / close | pct | added |
| std_pct | std / close | pct | added |
| range_pct | (high - low) / close | pct | added |
| macd_impulse | run‑length of (macd - signal > 0) | raw + z | added |
| vol_flow_strength | rvol * cmf | raw | added |
| hours_since_last | Δ hours between breakout rows | hours | added |
| rsi_z | zscore(rsi) | z by frame | added |
| adx_z | zscore(adx) | z by frame | added |
| cmf_z | zscore(cmf) | z by frame | added |
| rvol_z | zscore(rvol) | z by frame | added |
| volume_z | zscore(volume) | z by frame | added |
| obv_z | zscore(obv) | z by frame | added |
| total_score_z | zscore(total_score) | z by frame | added |

**Notes.**
- All features are computed row‑wise from the merged frame (no forward‑looking windows).
- Per‑ML percentile envelopes will be derived during optimization and exported for Pine.
