# **Recommended order & plan**

1. **Establish the baseline (your \#2) DONE**

   * **Why first:** We need a trustworthy “current state” to measure every change against.

   * **What we’ll do:** Run/retrofit `summarize_current_model.py` to ingest your latest backtest/trade logs (CSV/Parquet) or a simple export from TradingView. Compute: win\_rate, profit factor, expectancy, Sharpe/SQN, max DD, MAR, avg & median trade duration, distribution of returns/durations, heatmaps by weekday/hour, and per-asset breakdown.

   * **Deliverables:** Cleaned script, a metrics table, and a few standard plots (equity curve, drawdown curve, duration histogram, return distribution). We’ll lock this in as “Baseline v18.9”.

2. **Audit & optimize the Exit logic for duration (your \#1)DONE**

   * **Why now:** Exit policy is the single biggest driver of expectancy and time-in-trade; we’ll iterate with the baseline in hand.

   * **What we’ll test (controlled, one factor at a time):**

     1. Time-based exits (N bars cap).

     2. Volatility-based exits (ATR stop/trail; Kalman/KAMA trailing).

     3. Structure exits (last swing/Donchian/Bollinger mid reversion).

     4. “Strength fades” (e.g., RSI/ADX/CMF/Momentum rollovers) and your M18-specific confidence gates.

   * **How we’ll judge:** Uplift in profit factor/expectancy with *stable or reduced* DD and a target duration band that matches your operational goals. We’ll also check sensitivity to parameter nudges to avoid overfit.

   * **Deliverables:** Exit bake-off report: side-by-side metrics, best 1–2 candidates, and Pine-ready parameter sets.

7. **Weekly → Daily correlation study (your \#3)**

   * **Why third:** With exits behaving, we can identify when daily entries succeed *conditional on weekly context*.

   * **What we’ll examine at the entry bar:** Weekly trend slope (EMA/SMMA), RSI/ADX/CMF, volatility regime, distance from weekly bands, “Weekly M18” score if available.

   * **Stats we’ll use:** Information value, AUROC for win/loss, mutual information, monotone bins (e.g., WoE), conditional expectancy tables.

   * **Goal:** Simple, robust weekly “green light” conditions that measurably boost expectancy on daily trades.

   * **Deliverables:** Feature ranking, threshold suggestions (e.g., “Weekly ADX\>18 & Weekly RSI\>48 improves PF by \+0.25”), and toggleable gates you can drop into Pine.
8. **Market-level / regime module (your \#4)**

   * **Why now:** Once we know which weekly contexts help, we generalize to broader regimes.

   * **Without CMC:** Use accessible proxies (e.g., BTC & ETH trend/vol; BTC.D proxy if you have it on TradingView; TOTAL/TOTAL2 index series; realized vs implied vol proxies if available).

   * **Module shape:** A small, standalone “RegimeScore 0–100” with transparent components (trend, vol, breadth/liquidity proxy), decoupled from M18 internals, returning gates and weights. Designed for easy integration into the Entry/Exit model.

   * **Deliverables:** Spec \+ reference implementation (Python for research; Pine-friendly formulae), uplift tests, and stability checks (rolling/WFO).
4. **ALIGN100 vs CONF100 correlation & cross-sectional use (your \#5)**

   * **Two angles:**

     1. **Per-trade calibration:** Correlation (Pearson/Spearman), reliability curves (binned score vs realized outcomes), and whether joint high scores predict larger R.

     2. **Cross-sectional ranking:** Using `ALIGN100 + CONF100` (or a weighted sum) to pick among assets on the same day—does top-quartile ranking outperform?

   * **Deliverables:** Plots \+ stats; recommendation on whether to use the sum, a weighted combo, or keep them orthogonal.
9. **CMC integration planning (your \#6)**

   * **Why last:** By this point, we’ll know exactly which external series would add signal (e.g., market cap breadth, dominance, funding/open interest if desired), so the CMC (or alternative) integration can be purposeful rather than exploratory.

   * **Deliverables:** Minimal, typed interface \+ caching strategy; data dictionary of fields that actually matter to your regime/score logic.  
   *   
   * 
6\) Develop Blow-Off-Top (BoT) Detection Module

   \- Purpose: Flag late-stage, parabolic risk to protect peak equity and guide de-risking.

   \- Inputs (market-wide): BTC, ETH, TOTAL, TOTAL2, TOTAL3 (D/W), BTC.D, funding/basis (if available).

   \- Signals (composite BoTScore 0–100):

     • Parabolic acceleration: slope & 2nd-derivative of price vs. EMA(50/200) on D/W  

     • Distance-from-trend: % above W/Monthly basis (e.g., WEMA200 z-score; Bollinger z)  

     • Volatility blowout: realized vol expansion; ATR% spikes; regime shifts  

     • Crowd/froth: BTC.D fast drop; Google Trends spike; perp funding/basis extremes  

     • On-chain (if accessible): MVRV z-score, NUPL “Euphoria”, SOPR\>1.1, LTH profit ratio  

     • Classic tops: Pi-Cycle Top cross (W), Mayer Multiple extremes

   \- States & actions:

     • Watch (BoTScore ≥ 60): tighten stops; reduce adds; stagger targets  

     • Risk-Off (≥ 75): halve size; enforce time-cap exits; raise trailing stop to fast basis  

     • Top (≥ 85 and ≥2 confirms): exit remainder on trigger (close \< fast basis or trail breach)

   \- Backtest acceptance (vs. baseline v18.9):

     • ≥ 25–40% reduction in post-peak max DD, with ≤ 10% sacrifice of peak equity  

     • False-positive rate ≤ 1 per rolling 180D (per asset/indices)  

     • Robust across BTC/ETH/TOTAL/TOTAL2/TOTAL3 and subperiods

   \- Deliverables:

     • Pine: \`bot\_score()\` module \+ state machine; visual overlays & alerts  

     • Python: research notebook, metric report, parameter sheet  

     • Ops: alert templates, runbook, and toggle in M18 model (entry/exit gating & sizing)

5) Implement Capital Preservation Schedule

Purpose: Translate extreme compounding potential into realistic, executable outcomes by locking in milestones and controlling position size.

Milestone thresholds (equity → per-trade cap):

$25K–$250K → $5K/trade (full deployment, 5 slots)

$250K–$500K → $7.5K/trade

$500K–$1M → $10K/trade

$1M–$2.5M → $20K/trade

$2.5M–$5M → $40K/trade

$5M → $50–75K/trade, 5–10 slots, ≤5–10% exposure

Rationale:

Aggressively bootstrap early growth, then progressively cap trade size to preserve life-changing capital and manage liquidity.

Floors established at $500K and $1M ensure irreversible wealth even if cycle ends abruptly.

Optimized to still capture late-cycle parabolic upside while limiting execution risk.

Expected outcome (Aug–Dec 2025): ~$3–3.2M balance by cycle peak, with $500K and $1M locked floors already secured.

Deliverables:

Pine/Python: dynamic position-sizing function keyed to equity thresholds

Validation: backtest vs. uncapped compounding, confirm lower variance and liquidity safety

Ops: runbook for milestone reviews and lock-in enforcement

# **What I need from you to start (no API keys required)**

* A recent **trade log/backtest export** that includes: timestamp in/out, asset, entry/exit price, PnL (or we’ll compute), M18 score & CONF100 at entry (if you have them), and any labels we already compute in Pine. If you don’t have M18/CONF in the log, include the OHLCV so we can recompute.

* (Optional) A few **sample tickers** you care about most for spot-checks.

# **Success criteria (so we know we’re done each step)**

* **Baseline:** Reproducible metrics \+ plots; hash of the dataset & code committed for repeatability.

* **Exit optimization:** At least one exit variant with **higher PF/expectancy** and **no worse drawdown**, plus **trade duration** within your operational preference.

* **Weekly→Daily study:** 1–3 weekly gates that add **statistically significant** uplift across assets and subperiods.

* **Regime module:** A single composite score with clear cutoffs that improves filter/weighting **out-of-sample**.

* **Score correlation:** A documented decision on using ALIGN100/CONF100 (alone or combined) for entry confidence and/or cross-sectional ranking.

* **CMC plan:** Concrete list of fields \+ schema and where they plug into the pipeline.

