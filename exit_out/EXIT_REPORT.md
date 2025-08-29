# Exit Bake-off Report

**Chosen exit:** ATR trail × **1.25** (no cap)

- Trades: **385**, Win rate: **0.9506**, PF: **173.39**, Expectancy: **0.542798**, Median: **0.394776**

- Cost-adjusted (fees 5bps + slip 5bps): PF **170.42**, Expectancy **0.541798**, MDD **-0.431673**


## Top OOS (70/30 split)

- atr_trail 0.8: test PF **inf**, test expectancy **0.656403** (train PF 2636.856336)
- atr_trail 1.25: test PF **562.708987**, test expectancy **0.659360** (train PF 3199.594423)
- atr_trail 1.5: test PF **540.259314**, test expectancy **0.633008** (train PF 941.577358)
- atr_trail 1.0: test PF **502.015245**, test expectancy **0.646289** (train PF 2653.550488)
- atr_trail 1.75: test PF **501.188043**, test expectancy **0.618812** (train PF 591.057547)

## Leaderboard (top 10 by PF)

| family    | param    |        pf |   expectancy |   win_rate |       mdd |
|:----------|:---------|----------:|-------------:|-----------:|----------:|
| atr_trail | 1.25     | 1307.5    |     0.643974 |   0.994805 | -0.136166 |
| time_cap  | 6        |  270.673  |     0.508945 |   0.979221 | -0.212113 |
| time_cap  | 8        |  102.627  |     0.546403 |   0.948052 | -0.484735 |
| baseline  | last_bar |   90.1138 |     0.565431 |   0.932468 | -0.500769 |
| time_cap  | 10       |   90.1138 |     0.565431 |   0.932468 | -0.500769 |
| time_cap  | 12       |   90.1138 |     0.565431 |   0.932468 | -0.500769 |
| time_cap  | 15       |   90.1138 |     0.565431 |   0.932468 | -0.500769 |
| time_cap  | 20       |   90.1138 |     0.565431 |   0.932468 | -0.500769 |


## Artifacts

- exit_out/exit_rule.json
- exit_out/exits_atr_1p25_nocap.csv
- exit_out/trades_with_exit_final.csv
- exit_out/equity_atr_1p25_nocap.csv, exit_out/equity_atr_1p25_nocap_norm.csv
- exit_out/exit_equity_metrics.json, exit_out/cost_adjusted_metrics.json