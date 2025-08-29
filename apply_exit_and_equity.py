import json
from pathlib import Path
import pandas as pd
import numpy as np

TRADES = 'trades_clean_for_exit_tests.csv'
EXITS  = 'exit_out/exits_atr_1p25_nocap.csv'
OUTDIR = Path('exit_out')

def profit_factor(s):
    pos = s[s>0].sum()
    neg = -s[s<0].sum()
    if neg == 0: return float('inf') if pos>0 else np.nan
    return float(pos/neg)

OUTDIR.mkdir(parents=True, exist_ok=True)

trades = pd.read_csv(TRADES, parse_dates=['date']).sort_values('date').reset_index(drop=True)
if 'trade_id' not in trades.columns:
    trades = trades.reset_index().rename(columns={'index':'trade_id'})

exits  = pd.read_csv(EXITS, parse_dates=['exit_date'])
merged = trades.merge(exits, on='trade_id', how='left')

# clean + order
cols = [c for c in ['trade_id','symbol','date','exit_date','exit_bar_index','exit_ret'] if c in merged.columns]
merged = merged[cols].sort_values('date').reset_index(drop=True)
merged.to_csv(OUTDIR/'trades_with_exit_final.csv', index=False)

# equity & drawdown by trade order
rets = pd.to_numeric(merged['exit_ret'], errors='coerce').fillna(0.0)
eq = (1+rets).cumprod()
roll = eq.cummax()
dd = eq/roll - 1.0
eq_df = pd.DataFrame({'date': merged['date'], 'equity': eq, 'drawdown': dd})
eq_df.to_csv(OUTDIR/'equity_atr_1p25_nocap.csv', index=False)

# metrics
metrics = {
    'family': 'atr_trail', 'param': 1.25, 'cap_bars': None,
    'trades': int(len(rets)),
    'win_rate': float((rets>0).mean()),
    'pf': profit_factor(rets),
    'expectancy': float(rets.mean()),
    'median_ret': float(np.median(rets)),
    'mdd': float(dd.min()),
    'start_date': str(merged['date'].min()) if len(merged) else None,
    'end_date': str(merged['date'].max()) if len(merged) else None
}
(OUTDIR/'exit_equity_metrics.json').write_text(json.dumps(metrics, indent=2))
print('Wrote:',
      OUTDIR/'trades_with_exit_final.csv',
      OUTDIR/'equity_atr_1p25_nocap.csv',
      OUTDIR/'exit_equity_metrics.json')
