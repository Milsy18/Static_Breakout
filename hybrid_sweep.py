import json, math, argparse
from pathlib import Path
import pandas as pd
import numpy as np

ROOT=Path('.'); OUT=ROOT/'exit_out'; OUT.mkdir(parents=True, exist_ok=True)
ANN=252; FEES_BPS=5; SLIP_BPS=5; COST=(FEES_BPS+SLIP_BPS)/10000.0

bars  = pd.read_csv('trade_bars.csv', parse_dates=['date'])
atrx  = pd.read_csv(OUT/'exits_atr_1p25_nocap.csv', parse_dates=['exit_date'])

def pf(s):
    pos=s[s>0].sum(); neg=-s[s<0].sum()
    return float('inf') if neg==0 and pos>0 else (float(pos/neg) if neg!=0 else float('nan'))

def ann_stats(rets, eq):
    m=float(rets.mean()); sd=float(rets.std(ddof=1))
    sh=(m/sd*np.sqrt(ANN)) if sd>0 else float('nan')
    dn=rets[rets<0]; so=(m/(dn.std(ddof=1) if len(dn)>1 else np.nan))*np.sqrt(ANN) if len(dn)>1 else float('nan')
    cagr=float(eq.iloc[-1]**(ANN/len(rets))-1.0) if len(rets)>0 else float('nan')
    mdd=float((eq/eq.cummax()-1).min()); calmar=(cagr/abs(mdd)) if mdd<0 else float('inf')
    return m, sd*np.sqrt(ANN) if sd==sd else float('nan'), sh, so, cagr, mdd, calmar

def run_cap(cap):
    hyb = atrx[['trade_id','exit_bar_index']].rename(columns={'exit_bar_index':'atr_idx'}).copy()
    hyb['hyb_idx'] = np.minimum(hyb['atr_idx'].astype(int), cap)

    pick = bars[['trade_id','bar_index','date','ret_from_entry']].rename(columns={'bar_index':'hyb_idx'})
    hyb = hyb.merge(pick, on=['trade_id','hyb_idx'], how='left', validate='one_to_one')
    hyb.rename(columns={'date':'exit_date','ret_from_entry':'exit_ret'}, inplace=True)
    hyb_out = hyb[['trade_id','hyb_idx','exit_date','exit_ret']].rename(columns={'hyb_idx':'exit_bar_index'})
    hyb_out.to_csv(OUT/f'exits_hybrid_atr1p25_cap{cap}.csv', index=False)

    bb = bars.merge(hyb_out[['trade_id','exit_bar_index']], on='trade_id', how='left')
    bb = bb[bb['bar_index'] <= bb['exit_bar_index']].copy().sort_values(['trade_id','bar_index'])
    bb['ret_from_entry']=pd.to_numeric(bb['ret_from_entry'], errors='coerce').fillna(0.0)
    prev = bb.groupby('trade_id')['ret_from_entry'].shift().fillna(0.0)
    bb['inc']=(1+bb['ret_from_entry'])/(1+prev)-1
    bb.loc[bb['bar_index'].eq(bb['exit_bar_index']), 'inc'] -= COST
    bb['inc']=bb['inc'].replace([np.inf,-np.inf], np.nan).fillna(0.0)

    by_date=(bb.groupby('date')['inc'].mean().rename('ret')).to_frame().sort_index()
    eq=(1+by_date['ret']).cumprod()

    # VT10 with 60d lookback, 3x cap
    rv = by_date['ret'].rolling(60, min_periods=30).std(ddof=1)*np.sqrt(ANN)
    lev = (0.10/rv).clip(0,3.0).fillna(0.0)
    vt_ret = by_date['ret']*lev
    vt_eq=(1+vt_ret).cumprod()

    m, volA, sh, so, cagr, mdd, calmar = ann_stats(vt_ret, vt_eq)
    metrics=dict(cap=cap, pf=float(pf(vt_ret)), sharpe=float(sh), sortino=float(so),
                 vol_annual=float(volA), cagr=float(cagr), mdd=float(mdd),
                 calmar=float(calmar), avg_leverage=float(lev.mean()))
    (OUT/f'portfolio_metrics_costed_hyb_cap{cap}_vt10.json').write_text(json.dumps(metrics, indent=2))
    return metrics

caps=[6,8,10,12]
rows=[run_cap(c) for c in caps]
df=pd.DataFrame(rows).sort_values(['calmar','sharpe'], ascending=[False,False])
df.to_csv(OUT/'hybrid_sweep_vt10.csv', index=False)
print(df.to_string(index=False))
