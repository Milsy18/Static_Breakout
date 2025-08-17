# M18 Entry/Exit Model v18.0 â€” Build Tracker
_Last updated: 2025-08-14 22:35:57_

> **Regime scale:** `market_level` 1 = most **bearish** â†’ 9 = most **bullish**.

---

## 0) Purpose
Single source of truth for **decisions, evidence, and parameters** that will be implemented in the v18.0 Pine script. Everything below must be reproducible from the artifacts and commands recorded here.

---

## 1) Data Sources (current run)
- **Breakout events:** `m18-model2\\data\\breakout_events_timed_full.zip`
- **Per-bar indicators:** `Static_Breakout\\Data\\Processed\\per_bar_indicators_core.csv`
- **Macro / regime data:** `OneDrive\\Documents\\GitHub\\Static_Breakout_ARCHIVE_2025-08-08\\Data\\Raw\\macro_regime_data.csv`
- **Merged dataset (output):** `Static_Breakout\\holy_grail_static_dataset.csv` (+ ZIP)

**Merge logic**
- Join keys: `symbol` + `date` (per-bar) to `symbol` + `breakout_date` (events); macro on `date`.
- Collision handling: per-bar columns suffixed `_bar`, macro columns keep original names (except `date`).
- Assumptions:
  - `max_gain` is a fraction unless >1.5 (then treated as percentage / 100).
  - `duration_days` = days from breakout to **peak_date** (used as TP-within-window proxy).

---

## 2) Canonical Definitions (used in all analyses)
- **Take Profit (TP):** trade is considered TP if `max_gain â‰¥ TP%` **and** `duration_days â‰¤ hold_days` (reached within the holding window).
- **Timed exit:** TP not reached; exit value = `close(breakout_date + hold_days)`.
- **Other/early:** TP not reached **and** no valid close at `breakout_date + hold_days`.
- **Positive expectancy metrics:** we track `avg_ret`, `median_ret`, and `pct_pos` (>0).

> Rationale: this keeps the counting/return logic consistent across scans and is trivial to port to Pine.

---

## 3) Decisions To Date
### D-001 â€” Canonical data paths
- Selected the three inputs listed in Â§1.

### D-002 â€” Repository hygiene
- Archived older duplicates under `Archive_YYYYMMDD/` with duplicate detection by `Name + SizeBytes` and **.venv** excluded.

### D-003 â€” Hold-days scan (TP fixed at 33%)
Artifacts:
- Full grid: `Static_Breakout\\hold_days_scan_tp33_pnl.csv`
- Per-level recommendation: `Static_Breakout\\hold_days_recommendations_tp33.csv`

**Provisional interpretation**
- TP fraction is largely **insensitive** to `hold_days` (as expected).
- Optimal `hold_days` varies by regime; chosen by **max `avg_ret`**, tie-broken by **`pct_pos`** then **`n_valid`**.

> **Actionable:** Use `hold_days_recommendations_tp33.csv` as the regime-aware baseline for next-stage TP scans.

---

## 4) Parameters Under Review
- **TP% by regime:** To be scanned in Â§6 with the hold-days fixed to Â§3 recs.
- **Runner overrides:** Stage 2 (after TP scan). Candidate rules:
  - ATR-based trailing stop (e.g., exit if `close < ema20 - kÂ·ATR`).
  - % from peak trailing stop (e.g., exit if drawdown â‰¥ 10â€“15%).
  - Indicator reversal (e.g., `close < ema50` or `RSI < 50`), regime-gated (enable only on levels â‰¥ 6).
- **Risk filters to evaluate:** spread/volatility clamps, minimum volume/rvOL, and â€œno-tradeâ€ windows post-extreme BBW compression.

---

## 5) Derived Metrics for Pine (so far)
- `market_level` (1..9) interpreted as regime strength (1 = most bearish, 9 = most bullish).
- `tp_pct`: **initially 0.33** (33%). Will become regime-aware post Â§6.
- `hold_days`: read from `hold_days_recommendations_tp33.csv` (per-level).
- `exit classification` helper to port:
  - `tp = (max_gain >= tp_pct) and (duration_days <= hold_days[level])`
  - `timed = not tp and close_at(d = hold_days[level])`
  - `other = not tp and no close_at(d)`
- Utility: parse `max_gain` as percent vs fraction (threshold 1.5), same as research scripts.

---

## 6) Upcoming Experiments (tracked outputs)
### E-TP-001 â€” **TP% scan by regime** (hold_days fixed)
- **Goal:** For each `market_level`, pick `tp_pct` that maximizes `avg_ret` (tie-break: `pct_pos`, then `n_valid`).
- **TP grid:** 0.20, 0.25, 0.30, **0.33**, 0.35, 0.40, 0.45, 0.50.
- **Outputs:**
  - `Static_Breakout\\tp_scan_by_level_full.csv`
  - `Static_Breakout\\tp_recommendations_by_level.csv`
- **Notes:** Keep return definition identical to Â§2. Use the per-level `hold_days` from Â§3.

### E-RUN-001 â€” **Runner overrides on TP trades only**
- **Goal:** Determine if replacing fixed TP with a trailing exit improves `avg_ret` / reduces tail risk.
- **Candidates:** ATR-trail, %peak-trail, EMA/RSI reversal; likely regime-gated (â‰¥6).
- **Outputs:** `runner_eval_full.csv`, `runner_recommendations.csv`.

---

## 7) Reproducibility & Fingerprints (to fill locally)
> Run this PowerShell in the repo to record file hashes next time you update the tracker.

```powershell
$paths = @(
  "C:\\Users\\milla\\OneDrive\\Documents\\GitHub\\m18-model2\\data\\breakout_events_timed_full.zip",
  "C:\\Users\\milla\\Static_Breakout\\Data\\Processed\\per_bar_indicators_core.csv",
  "C:\\Users\\milla\\OneDrive\\Documents\\GitHub\\Static_Breakout_ARCHIVE_2025-08-08\\Data\\Raw\\macro_regime_data.csv",
  "C:\\Users\\milla\\Static_Breakout\\holy_grail_static_dataset.csv"
)
$rows = foreach ($p in $paths) { if (Test-Path $p) { $h = Get-FileHash $p -Algorithm SHA256; [pscustomobject]@{Path=$p; SHA256=$h.Hash} } }
$rows | Format-Table -AutoSize
```

---

## 8) Current Parameter Matrix (to be populated automatically)
> Run the snippet below to append the latest per-level `hold_days` into this document.

```powershell
$rec = "C:\\Users\\milla\\Static_Breakout\\hold_days_recommendations_tp33.csv"
$md  = "C:\\Users\\milla\\Static_Breakout\\docs\\M18_v18_Build_Tracker.md"
New-Item -ItemType Directory -Path (Split-Path $md -Parent) -Force | Out-Null

# Convert CSV -> Markdown table and append under this section
$tbl = Import-Csv $rec | Sort-Object market_level | ForEach-Object {
  "| $($_.market_level) | $($_.hold_days) | $($_.n_valid) | $($_.tp_count) | $($_.timed_count) | $($_.avg_ret) | $($_.median_ret) | $($_.pct_pos) |"
}
$header = "| market_level | hold_days | n_valid | tp_count | timed_count | avg_ret | median_ret | pct_pos |`n|---|---:|---:|---:|---:|---:|---:|---:|"
Add-Content -LiteralPath $md -Value "`n### Hold-days recommendations (TP=33%)`n$header`n$($tbl -join "`n")`n"
Write-Host "(Appended hold-days table to $md)"
```

---

## 9) Open Questions / TODO
- Should TP be raised in levels **6â€“9** and reduced in **1â€“3**? Decide after Â§6.
- Which runner rule dominates by regime (if any)?
- Add pre-trade filters (min rvOL, BBW percentile) if they materially de-risk entries.

---

## 10) Change Log
- **2025-08-14** â€” Created tracker; recorded data sources, merge logic, canonical definitions; logged hold-days scan artifacts; planned TP scan & runner analysis.


## Hold-days recommendations (TP=33%) — appended 2025-08-14 19:44
| market_level | hold_days | n_valid | tp_count | timed_count | avg_ret | median_ret | pct_pos |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5 | 404 | 218 | 186 | 0.385376 | 0.33 | 0.9926 |
| 2 | 5 | 232 | 115 | 117 | 0.421144 | 0.33 | 0.9871 |
| 3 | 8 | 355 | 235 | 120 | 0.454131 | 0.33 | 0.9803 |
| 4 | 6 | 302 | 187 | 115 | 0.397895 | 0.33 | 0.9868 |
| 5 | 8 | 189 | 122 | 67 | 26.755849 | 0.33 | 0.9735 |
| 6 | 6 | 286 | 148 | 138 | 0.436244 | 0.33 | 0.993 |
| 7 | 6 | 563 | 299 | 264 | 0.817746 | 0.33 | 0.9947 |
| 8 | 7 | 513 | 289 | 224 | 0.483272 | 0.33 | 0.9883 |
| 9 | 6 | 610 | 323 | 287 | 0.443677 | 0.33 | 0.9902 |









## TP% recommendations (hold_days fixed) — appended 2025-08-16 11:41
| market_level | hold_days | tp_pct | n_valid | tp_count | timed_count | avg_ret | median_ret | pct_pos |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5 | 0.65 | 377 | 30 | 347 | 0.414144 | 0.33627 | 0.9894 |
| 2 | 5 | 0.85 | 207 | 11 | 196 | 0.450476 | 0.337258 | 0.9662 |
| 3 | 8 | 0.9 | 300 | 12 | 288 | 0.536656 | 0.378316 | 0.93 |
| 4 | 6 | 0.85 | 276 | 14 | 262 | 0.462589 | 0.383887 | 0.9565 |
| 5 | 8 | 0.95 | 162 | 11 | 151 | 31.281075 | 0.438483 | 0.9383 |
| 6 | 6 | 0.9 | 262 | 11 | 251 | 0.537781 | 0.4165 | 0.9885 |
| 7 | 6 | 0.95 | 500 | 19 | 481 | 0.973445 | 0.400742 | 0.974 |
| 8 | 7 | 0.95 | 469 | 15 | 454 | 0.570858 | 0.411746 | 0.9701 |
| 9 | 6 | 0.95 | 561 | 23 | 538 | 0.511298 | 0.39973 | 0.9643 |
## TP grid matrices — appended 2025-08-16 10:54

**Files**
- avg_ret matrices & pct_pos matrices per hold-days: $(Split-Path C:\Users\milla\Static_Breakout\matrices -Leaf)\*matrix*.csv
- Sweet spots per level: $(Split-Path C:\Users\milla\Static_Breakout\matrices\sweet_spots_by_level.csv -Leaf)

### Sweet spots (constraints: n_valid ≥ 150, pct_pos ≥ 0.97)
| market_level | hold_days | tp_pct | n_valid | avg_ret | median_ret | pct_pos |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 5 | 0.65 | 377 | 0.414144 | 0.33627 | 0.9894 |
| 2 | 5 | 0.5 | 218 | 0.443389 | 0.363014 | 0.9771 |
| 3 | 8 | 0.35 | 348 | 0.467981 | 0.35 | 0.9741 |
| 4 | 6 | 0.4 | 294 | 0.418943 | 0.4 | 0.9728 |
| 5 | 8 | 0.33 | 189 | 26.755849 | 0.33 | 0.9735 |
| 6 | 6 | 0.9 | 262 | 0.537781 | 0.4165 | 0.9885 |
| 7 | 6 | 0.95 | 500 | 0.973445 | 0.400742 | 0.974 |
| 8 | 7 | 0.95 | 469 | 0.570858 | 0.411746 | 0.9701 |
| 9 | 6 | 0.75 | 565 | 0.503694 | 0.405599 | 0.9717 |


