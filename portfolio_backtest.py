import json
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path('.')
OUT  = ROOT/'exit_out'
OUT.mkdir(parents=True, exist_ok=True)

# Inputs created earlier
bars  = pd.read_csv('trade_bars.csv', parse_dates=['date'])
exits = pd.read_csv(OUT/'exits_atr_1p25_nocap.csv', parse_dates=['exit_date'])

# Keep bars only up to the chosen exit bar for each trade
bars = bars.merge(exits[['trade_id','exit_bar_index']], on='trade_id', how='left')
bars = bars[bars['bar_index'] <= bars['exit_bar_index']].copy()

# Per-trade incremental returns (simple), derived from ret_from_entry
bars = bars.sort_values(['trade_id','bar_index'])
bars['ret_from_entry'] = bars['ret_from_entry'].astype(float)
bars['inc_mult'] = (1.0 + bars.groupby('trade_id')['ret_from_entry'].transform(lambda s: s.shift().fillna(0.0) + 1e-12))
bars['inc'] = ((1.0 + bars['ret_from_entry']) / bars['inc_mult']) - 1.0

# Guard against tiny numeric noise
bars['inc'] = bars['inc'].replace([np.inf, -np.inf], np.nan).fillna(0.0)

# Equal-weight portfolio: average the inc returns across all active trades each bar date
by_date = (bars.groupby('date')['inc'].mean().rename('ret').to_frame()
           .sort_index())

# Portfolio equity & DD
equity = (1.0 + by_date['ret']).cumprod()
roll   = equity.cummax()
dd     = equity/roll - 1.0

# Metrics
def pf(series):
    pos = series[series>0].sum()
    neg = -series[series<0].sum()
    if neg == 0: return float('inf') if pos>0 else np.nan
    return float(pos/neg)

metrics = dict(
    family='atr_trail', param=1.25, cap_bars=None,
    bars=len(by_date), start=str(by_date.index.min()), end=str(by_date.index.max()),
    pf=pf(by_date['ret']),
    win_rate=float((by_date['ret']>0).mean()),
    expectancy=float(by_date['ret'].mean()),
    median_ret=float(by_date['ret'].median()),
    mdd=float(dd.min())
)

# Write artifacts
curve = pd.DataFrame({'date': by_date.index, 'ret': by_date['ret'], 'equity': equity, 'drawdown': dd})
curve.to_csv(OUT/'portfolio_curve_equal_weight.csv', index=False)
(OUT/'portfolio_metrics.json').write_text(json.dumps(metrics, indent=2))
print('Wrote', OUT/'portfolio_curve_equal_weight.csv', 'and', OUT/'portfolio_metrics.json')
