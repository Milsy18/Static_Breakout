# SYSTEM MAP — Static_Breakout / M18 V18.0

## Purpose
Blueprint of directories, scripts, and data artifacts driving feature build → optimization → Pine deployment.

---

## Repo Root
- `scripts/python/`
  - **build_features.py** → enriches base dataset with extra features
  - (next) run_opt.py → optimization harness
  - (helpers) emit_pine_defaults.py, make_pine_defaults.py → Pine config
- `data/raw/` → original OHLCV + macro regime inputs
- `data/processed/`
  - final_holy_grail_static.csv (base merged frame)
  - final_holy_grail_enriched.csv (output from builder)
- `out/`
  - test_enriched.csv (preview run, sanity checks)
  - equity_curves/, ablation_*.png (to be produced by optimizer)
- `docs/`
  - RESEARCH_REPORT.md
  - features_added.md
  - **SYSTEM_MAP.md** (this file)

---

## Flow
1. **Feature Build**  
   `build_features.py` → takes `final_holy_grail_static.csv` → outputs enriched features.
2. **Optimization Harness**  
   `run_opt.py` (planned) → rolling CV across market levels → finds best gates, writes param CSVs and plots.
3. **Pine Export**  
   emit/make_pine_defaults.py → outputs `pine_defaults_v18.yaml` → Pine v6 scripts.
