import json, math
from pathlib import Path
import pandas as pd
import numpy as np

ROOT=Path('.'); OUT=ROOT/'exit_out'; OUT.mkdir(parents=True, exist_ok=True)
CAP_N = 8                                # change this to 6/10/etc to try other caps
FEES_BPS, SLIP_BPS = 5, 5
COST = (FEES_BPS+SLIP_BPS)/10000.0
ANN = 252

# ----- Build hybrid exits (min of ATR exit and CAP_N)
bars  = pd.read_csv('trade_bars.csv', parse_dates=['date'])
atrx  = pd.read_csv(OUT/'exits_atr_1p25_nocap.csv', parse_dates=['exit_date'])
hyb = atrx[['trade_id','exit_bar_index']].rename(columns={'exit_bar_index':'atr_idx'}).copy()
hyb['hyb_idx'] = np.minimum(hyb['atr_idx'].astype(int), CAP_N)

pick = bars[['trade_id','bar_index','date','ret_from_entry']].rename(columns={'bar_index':'hyb_idx'})
hyb = hyb.merge(pick, on=['trade_id','hyb_idx'], how='left', validate='one_to_one')
hyb.rename(columns={'date':'exit_date','ret_from_entry':'exit_ret'}, inplace=True)
hyb[['trade_id','hyb_idx','exit_date','exit_ret']].rename(columns={'hyb_idx':'exit_bar_index'}) \
   .to_csv(OUT/f'exits_hybrid_atr1p25_cap{CAP_N}.csv', index=False)

# ----- Equal-weight portfolio with costs (applied on exit bar)
exits = pd.read_csv(OUT/f'exits_hybrid_atr1p25_cap{CAP_N}.csv', parse_dates=['exit_date'])
bb = bars.merge(exits[['trade_id','exit_bar_index']], on='trade_id', how='left')
bb = bb[bb['bar_index'] <= bb['exit_bar_index']].copy()
bb.sort_values(['trade_id','bar_index'], inplace=True)
bb['ret_from_entry']=pd.to_numeric(bb['ret_from_entry'], errors='coerce').fillna(0.0)
prev = bb.groupby('trade_id')['ret_from_entry'].shift().fillna(0.0)
bb['inc']=(1+bb['ret_from_entry'])/(1+prev)-1
bb.loc[bb['bar_index'].eq(bb['exit_bar_index']), 'inc'] -= COST
bb['inc']=bb['inc'].replace([np.inf,-np.inf], np.nan).fillna(0.0)

by_date=(bb.groupby('date')['inc'].mean().rename('ret')).to_frame().sort_index()
equity=(1+by_date['ret']).cumprod(); dd=equity/equity.cummax()-1

def pf(s):
    pos=s[s>0].sum(); neg=-s[s<0].sum()
    return float('inf') if neg==0 and pos>0 else (float(pos/neg) if neg!=0 else np.nan)

def ann_stats(rets, eq):
    m=float(rets.mean()); sd=float(rets.std(ddof=1))
    sh=(m/sd*np.sqrt(ANN)) if sd>0 else float('nan')
    dn=rets[rets<0]; so=(m/(dn.std(ddof=1) if len(dn)>1 else np.nan))*np.sqrt(ANN) if len(dn)>1 else float('nan')
    cagr=float(eq.iloc[-1]**(ANN/len(rets))-1.0) if len(rets)>0 else float('nan')
    mdd=float((eq/eq.cummax()-1).min()); calmar=(cagr/abs(mdd)) if mdd<0 else float('inf')
    return m, sd*np.sqrt(ANN) if sd==sd else float('nan'), sh, so, cagr, mdd, calmar

m, volA, sh, so, cagr, mdd, calmar = ann_stats(by_date['ret'], equity)
metrics=dict(exit='hybrid_atr1p25_or_cap', cap=CAP_N, fees_bps=FEES_BPS, slip_bps=SLIP_BPS,
             pf=float(pf(by_date['ret'])), win_rate=float((by_date['ret']>0).mean()),
             expectancy=float(m), vol_annual=float(volA), sharpe=float(sh), sortino=float(so),
             cagr=float(cagr), mdd=float(mdd), calmar=float(calmar))
(OUT/f'portfolio_metrics_costed_hyb_cap{CAP_N}.json').write_text(json.dumps(metrics, indent=2))
pd.DataFrame({'date':by_date.index,'ret':by_date['ret'],'equity':equity,'drawdown':dd}) \
  .to_csv(OUT/f'portfolio_curve_equal_weight_costed_hyb_cap{CAP_N}.csv', index=False)

# ----- 10% vol-target overlay (max 3×)
LOOKBACK=60; MAX_LEV=3.0
ret = by_date['ret'].astype(float)
rv = ret.rolling(LOOKBACK, min_periods=LOOKBACK//2).std(ddof=1)*np.sqrt(ANN)
lev = (0.10/rv).clip(0, MAX_LEV).fillna(0.0)
vt_ret = ret*lev
vt_eq=(1+vt_ret).cumprod(); vt_dd=vt_eq/vt_eq.cummax()-1
m, volA, sh, so, cagr, mdd, calmar = ann_stats(vt_ret, vt_eq)
vt_metrics=dict(exit='hybrid_atr1p25_or_cap', cap=CAP_N, vt='10%', max_leverage=MAX_LEV,
                pf=float(pf(vt_ret)), win_rate=float((vt_ret>0).mean()),
                expectancy=float(vt_ret.mean()), vol_annual=float(volA),
                sharpe=float(sh), sortino=float(so), cagr=float(cagr), mdd=float(mdd),
                calmar=float(calmar), avg_leverage=float(lev.mean()))
(OUT/f'portfolio_metrics_costed_hyb_cap{CAP_N}_vt10.json').write_text(json.dumps(vt_metrics, indent=2))
pd.DataFrame({'date':by_date.index,'ret':vt_ret,'equity':vt_eq,'drawdown':vt_dd,'leverage':lev}) \
  .to_csv(OUT/f'portfolio_curve_eqw_costed_hyb_cap{CAP_N}_vt10.csv', index=False)

print('HYBRID done for cap', CAP_N)
