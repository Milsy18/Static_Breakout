import json, math
from pathlib import Path
import pandas as pd
import numpy as np

OUT = Path('exit_out')
curve = pd.read_csv(OUT/'portfolio_curve_equal_weight_costed.csv', parse_dates=['date'])

TARGET_VOL = 0.10   # 10% annual
MAX_LEV    = 3.0
LOOKBACK   = 60     # trading days
ANN        = 252

# realized daily vol (rolling)
ret = curve['ret'].astype(float)
roll_vol_d = ret.rolling(LOOKBACK, min_periods=LOOKBACK//2).std(ddof=1)
roll_vol_ann = roll_vol_d * np.sqrt(ANN)

# position sizing
pos = TARGET_VOL / roll_vol_ann.replace(0, np.nan)
pos = pos.clip(lower=0, upper=MAX_LEV).fillna(0.0)

# vol-targeted returns
vt_ret = ret * pos
equity = (1 + vt_ret).cumprod()
dd = equity / equity.cummax() - 1.0

def pf(series):
    pos = series[series>0].sum()
    neg = -series[series<0].sum()
    if neg == 0: return float('inf') if pos>0 else np.nan
    return float(pos/neg)

mean = float(vt_ret.mean())
sd   = float(vt_ret.std(ddof=1))
sharpe = (mean/sd * math.sqrt(ANN)) if sd>0 else float('nan')
down = vt_ret[vt_ret<0]
sortino = (mean/(down.std(ddof=1) if len(down)>1 else np.nan))*math.sqrt(ANN) if len(down)>1 else float('nan')
cagr = float(equity.iloc[-1]**(ANN/len(vt_ret)) - 1.0) if len(vt_ret)>0 else float('nan')
mdd  = float(dd.min())
calmar = (cagr/abs(mdd)) if mdd<0 else float('inf')

metrics = dict(
    target_vol=TARGET_VOL, max_leverage=MAX_LEV, lookback_days=LOOKBACK,
    pf=float(pf(vt_ret)),
    win_rate=float((vt_ret>0).mean()),
    expectancy=mean,
    vol_annual=float(sd*math.sqrt(ANN)) if sd==sd else float('nan'),
    sharpe=float(sharpe), sortino=float(sortino),
    cagr=float(cagr), mdd=float(mdd), calmar=float(calmar),
    avg_leverage=float(pos.replace([np.inf,-np.inf], np.nan).fillna(0).mean())
)

vt_curve = curve[['date']].copy()
vt_curve['ret'] = vt_ret
vt_curve['equity'] = equity
vt_curve['drawdown'] = dd
vt_curve['leverage'] = pos

vt_curve.to_csv(OUT/'portfolio_curve_eqw_costed_vt10.csv', index=False)
(OUT/'portfolio_metrics_costed_vt10.json').write_text(json.dumps(metrics, indent=2))
print('Wrote', OUT/'portfolio_curve_eqw_costed_vt10.csv', 'and', OUT/'portfolio_metrics_costed_vt10.json')
