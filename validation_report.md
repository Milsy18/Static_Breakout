# Static Breakouts — Validation Report

**File:** Data\Processed\static_breakouts.csv
**Rows:** 543

## Columns present
symbol, entry_date, entry_price, exit_date, exit_price, exit_reason, market_level_at_entry, score_trd, score_vty, score_vol, score_mom, score_total, success, days_in_trade, pct_return, source

## Required columns check
- Missing: None
- Extra: None

## Null counts (top 20)
entry_date               1
entry_price              1
exit_date                1
exit_price               1
market_level_at_entry    1
score_trd                1
score_vty                1
score_vol                1
score_mom                1
score_total              1
days_in_trade            1
pct_return               1
symbol                   0
exit_reason              0
success                  0
source                   0

## Top 10 symbols by count
symbol
1INCH-USD    542
NaT            1

## Exit reason distribution
exit_reason
TIME       542
MISSING      1

## Sanity checks
- Rows with exit_date < entry_date: 0
