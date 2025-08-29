import json, pandas as pd, numpy as np
from pathlib import Path

FEES_BPS = 5      # broker/exchange fee
SLIP_BPS = 5      # slippage
COST = (FEES_BPS + SLIP_BPS) / 10000.0

ex = pd.read_csv('exit_out/exits_atr_1p25_nocap.csv')
rets_net = pd.to_numeric(ex['exit_ret'], errors='coerce').fillna(0.0) - COST

def pf(s):
    pos=s[s>0].sum(); neg=-s[s<0].sum()
    return float('inf') if neg==0 and pos>0 else (float(pos/neg) if neg!=0 else np.nan)

eq = (1+rets_net).cumprod()
dd = eq/eq.cummax() - 1.0

metrics = dict(family='atr_trail',param=1.25,fees_bps=FEES_BPS,slip_bps=SLIP_BPS,
               trades=int(len(rets_net)),win_rate=float((rets_net>0).mean()),
               pf=float(pf(rets_net)),expectancy=float(rets_net.mean()),
               median_ret=float(np.median(rets_net)),mdd=float(dd.min()))
Path('exit_out').mkdir(parents=True, exist_ok=True)
Path('exit_out/cost_adjusted_metrics.json').write_text(json.dumps(metrics, indent=2))
print('Wrote exit_out/cost_adjusted_metrics.json')
