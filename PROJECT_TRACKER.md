# Static_Breakout — Development Tracker (V18.0)

**Objective:** Deliver a schema‑clean Data/Processed/static_breakouts.csv that captures every model‑detected breakout for M18 optimisation (regime‑aware thresholds, entry/exit, profit per trade).  

---

## Status at a glance
- ✅ Data/Processed/holy_grail_static_221_dataset.parquet  
- ✅ Data/Processed/holy_grail_static_221_windows_long.parquet  
- ✅ Data/Processed/labeled_holy_grail_static_221_windows_long.parquet  
- ✅ Data/Processed/holy_grail_static_221_long_only.parquet  
- ⬜ Data/Processed/static_breakouts.csv (schema-validated)

---

## Task checklist (chronological)

- ✅ **Archive salvage & commit**  
  - [x] Add holy_grail_static_221_dataset.parquet (LFS)  
  - [x] Add holy_grail_static_221_windows_long.parquet (LFS)  
  - [x] Add labeled_holy_grail_static_221_windows_long.parquet (LFS)  
  - [x] Add holy_grail_static_221_long_only.parquet (LFS)

- ⬜ **Validate inputs present**
  - [ ] Data/Filtered_OHLCV/ complete for current universe  
  - [ ] Data/Raw/macro_regime_data.csv (or built via uild_macro_regime_data.py)

- ⬜ **Rebuild/verify per‑bar indicators**
  - [ ] Run scripts/generate_indicators.py as needed → confirm per_bar_indicators_core.csv matches current schema

- ⬜ **Static breakouts (final target)**
  - [ ] Run scripts/static_breakout_generator.py → create/refresh Data/Processed/static_breakouts.csv
  - [ ] **Schema gate** (no look‑ahead, required columns present)
  - [ ] **Counts gate** (events per symbol/date consistent with rules)
  - [ ] **Logic gate** (module scores, total score, M/L at entry, exit reason flags)

- ⬜ **Optimisation readiness**
  - [ ] Freeze static_breakouts.csv (tag commit)  
  - [ ] Kick off optimiser (thresholds/weights/gates)  
  - [ ] Emit updated Pine Script (drop‑in)

---

## Required columns for static_breakouts.csv
- symbol, entry_date, entry_price, exit_date, exit_price, exit_reason (TP/TIME/RSI/etc)  
- market_level_at_entry  
- score_trd, score_vty, score_vol, score_mom, score_total  
- success (1/0), days_in_trade, _multiple or pct_return  
- Audit: source (e.g., model), version/commit id

---

## Quick links (relative paths)
- Data → Data/Processed/  
- Scripts → scripts/  
- Model overview → M18 Entry_Exit Model.md  
- Pipeline roadmap → static_breakouts_pipeline_roadmap.md (if missing, we’ll add it)

---

_Last updated: 2025-08-10 16:22_
