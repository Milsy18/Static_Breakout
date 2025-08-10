# Static Breakouts — Validation Report

**File:** Data\Processed\static_breakouts.csv
**Rows:** 542

## Columns present
symbol, entry_date, entry_price, exit_date, exit_price, exit_reason, market_level_at_entry, score_trd, score_vty, score_vol, score_mom, score_total, success, days_in_trade, pct_return, source

## Required columns check
- Missing: None
- Extra: None

## Null counts (top 20)
symbol                   0
entry_date               0
entry_price              0
exit_date                0
exit_price               0
exit_reason              0
market_level_at_entry    0
score_trd                0
score_vty                0
score_vol                0
score_mom                0
score_total              0
success                  0
days_in_trade            0
pct_return               0
source                   0

## Top 10 symbols by count
symbol
1INCH-USD    542

## Exit reason distribution
exit_reason
TIME    542

## Sanity checks
- Rows with exit_date < entry_date: 0
