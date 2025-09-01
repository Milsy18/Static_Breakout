import json, math
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path('.'); OUT = ROOT/'exit_out'; OUT.mkdir(parents=True, exist_ok=True)

# ----- inputs
bars  = pd.read_csv('trade_bars.csv', parse_dates=['date'])
exits = pd.read_csv(OUT/'exits_atr_1p25_nocap.csv', parse_dates=['exit_date'])

# costs (round-trip, in bps) applied once on EXIT BAR for each trade
FEES_BPS = 5
SLIP_BPS = 5
COST = (FEES_BPS + SLIP_BPS)/10000.0

# ----- per-trade incremental returns
bars = bars.merge(exits[['trade_id','exit_bar_index']], on='trade_id', how='left')
bars = bars[bars['bar_index'] <= bars['exit_bar_index']].copy()
bars = bars.sort_values(['trade_id','bar_index'])
bars['ret_from_entry'] = pd.to_numeric(bars['ret_from_entry'], errors='coerce').fillna(0.0)

# incremental step: r_inc = (1+r_t)/(1+r_{t-1}) - 1 for each trade
prev = bars.groupby('trade_id')['ret_from_entry'].shift().fillna(0.0)
bars['inc'] = (1.0 + bars['ret_from_entry'])/(1.0 + prev) - 1.0

# subtract one-time costs on the exit bar of each trade
is_exit = bars['bar_index'].eq(bars['exit_bar_index'])
bars.loc[is_exit, 'inc'] -= COST

# keep it tidy
bars['inc'].replace([np.inf, -np.inf], np.nan, inplace=True)
bars['inc'] = bars['inc'].fillna(0.0)

# ----- equal-weight portfolio across active trades each day
by_date = bars.groupby('date').agg(
    ret=('inc','mean'),
    n_trades=('inc','size'),
    active=('trade_id','nunique')
).sort_index()

# equity
equity = (1.0 + by_date['ret']).cumprod()
roll   = equity.cummax()
dd     = equity/roll - 1.0

# normalized & log
curve = pd.DataFrame({
    'date': by_date.index,
    'ret': by_date['ret'],
    'equity': equity,
    'equity_norm_100': 100*equity/equity.iloc[0],
    'log10_equity': np.log10(np.clip(equity, 1e-12, None)),
    'drawdown': dd,
    'active_trades': by_date['active']
})

# ----- metrics
def pf(series):
    pos = series[series>0].sum()
    neg = -series[series<0].sum()
    if neg == 0: return float('inf') if pos>0 else np.nan
    return float(pos/neg)

rets = curve['ret']
ann = 252  # trading days
mean = float(rets.mean())
vol  = float(rets.std(ddof=1)) if len(rets)>1 else float('nan')
sh   = (mean/vol*math.sqrt(ann)) if vol>0 else float('nan')
down = rets[rets<0]
sortino = (mean/(down.std(ddof=1) if len(down)>1 else np.nan))*math.sqrt(ann) if len(down)>1 else float('nan')
cagr = float(equity.iloc[-1]**(ann/len(rets)) - 1.0) if len(rets)>0 else float('nan')
mdd  = float(dd.min())
calmar = (cagr/abs(mdd)) if mdd<0 else float('inf')

metrics = dict(
    family='atr_trail', param=1.25, cap_bars=None,
    start=str(curve['date'].min()), end=str(curve['date'].max()),
    bars=int(len(curve)),
    pf=float(pf(rets)),
    expectancy=mean,
    win_rate=float((rets>0).mean()),
    vol_annual=float(vol*math.sqrt(ann)) if not math.isnan(vol) else float('nan'),
    sharpe=float(sh),
    sortino=float(sortino),
    cagr=float(cagr),
    mdd=float(mdd),
    calmar=float(calmar),
    fees_bps=FEES_BPS, slip_bps=SLIP_BPS
)

curve.to_csv(OUT/'portfolio_curve_equal_weight_costed.csv', index=False)
(OUT/'portfolio_metrics_costed.json').write_text(json.dumps(metrics, indent=2))
print('Wrote', OUT/'portfolio_curve_equal_weight_costed.csv', 'and', OUT/'portfolio_metrics_costed.json')
