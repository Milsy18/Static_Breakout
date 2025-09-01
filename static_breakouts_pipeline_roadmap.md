# Static Breakouts Pipeline – Roadmap v1.0

## Why this matters
A single, well-structured file (**`static_breakouts.csv`**) is the foundation for every downstream
notebook, dashboard and model.  
Missing columns or inconsistent logic propagate silent errors into every result.  
This roadmap locks down what that file must contain **and** how to keep it trustworthy as the
project evolves.

---

## 1 · Deliverables checklist

| ✔ | Component                            | File produced                                              | Script / tool                            | Key columns added                          |
|---|--------------------------------------|------------------------------------------------------------|------------------------------------------|--------------------------------------------|
| ⬜ | **Raw indicator matrix**            | `holy_grail_static_221_dataset.parquet`                    | `generate_indicators.py`                 | all *_score, `market_level`                |
| ⬜ | **Rolling 221-bar windows**         | `holy_grail_static_221_windows_long.parquet`               | `build_windows_all.py`                   | `t+1 … t+221` future closes                |
| ⬜ | **Exit-labelled windows**           | `labeled_holy_grail_static_221_windows_long.parquet`       | `process_and_label_windows_long.py`      | `exit_time`, `exit_price`, `exit_reason`, `exit_label` |
| ⬜ | **Long-only feature set**           | `holy_grail_static_221_long_only.parquet`                  | `merge_long_only.py`                     | `success_bin`                              |
| ⬜ | **Static breakouts** (**final**)    | `Data/Processed/static_breakouts.csv`                      | `static_breakout_generator.py`           | composite scores + entry/exit fields       |

*Mark each box off in pull-requests or releases.*

---

## 2 · Column contract for `static_breakouts.csv`

| Column              | dtype                | Description                                                            |
|---------------------|----------------------|------------------------------------------------------------------------|
| `symbol`            | string               | Trading pair, e.g. `BTC-USD`                                           |
| `entry_date`        | datetime64[ns, UTC]  | Date of long-entry signal                                              |
| `entry_price`       | float64              | Close price on entry bar                                               |
| `market_level`      | int8 (1-9)           | Macro regime at entry                                                  |
| `score_trd` … `score_mom` | float32        | Module sub-scores (54 + 21 + 16 + 9 scale)                              |
| `score_total`       | float32              | Raw composite score (0-100)                                            |
| `score_norm`        | float32              | Normalised conviction (0-1)                                            |
| `exit_time`         | datetime64[ns, UTC]  | Timestamp when trade exited                                            |
| `exit_price`        | float64              | Price on exit bar                                                      |
| `exit_reason`       | category {TP,TIME,RSI,DEG} | Exit trigger                                                          |
| `success_bin`       | int8 {0,1}           | 1 = `exit_price > entry_price`, else 0                                  |

_All future analytics (e.g. “score three days pre-breakout”, win-rate by module, draw-downs) can be built from this schema alone._

---

## 3 · Validation gates (add to CI)

1. **Schema test** – PyTest fixture asserts every column & dtype above exists.  
2. **Row-count parity** – `entry_date` count == `exit_time` count (every trade must close).  
3. **Success sanity** – `success_bin == (exit_price > entry_price)`.  
4. **Look-ahead leak** – `entry_date < exit_time` for all rows.  
5. **TP distance** – `exit_price >= entry_price * (1 + TP_pct)` when `exit_reason == "TP"`.

---

## 4 · Future-proof workflows

* **Branch-per-dataset refresh** – keep the 0.9 GB window file out of `main` until processed.  
* **Data versioning (DVC)** – cache windows & Parquet so a new OHLCV file triggers minimal recompute.  
* **Experiment folders** – store param sweeps under `Data/Experiments/…` to avoid polluting prod.  
* **Tag + release** – tag repo whenever `static_breakouts.csv` spec changes; pin dashboards to tag.

---

## 5 · Next analytical layers

1. **Look-back / look-ahead features** – e.g. “score_norm three days before breakout”.  
2. **Model training** – gradient boosting on sub-scores + macro regime to predict TP probability.  
3. **Portfolio simulation** – capital allocation by predicted edge & rolling re-training.  
4. **Dashboard** – Streamlit app to slice performance by symbol / regime / time-window.

Happy back-testing! 🚀
