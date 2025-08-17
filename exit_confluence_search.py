import re, numpy as np, pandas as pd
from itertools import product

IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_events_timed_full.csv"
LAB = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\exit_confluence_grid_results.csv"

TP   = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
HOLD = {1:5, 2:5, 3:8, 4:6, 5:8, 6:6, 7:6, 8:7, 9:6}

def day_cols(prefix, df):
    pat = re.compile(rf"^{re.escape(prefix)}_d(\-?\d+)$")
    out=[]
    for c in df.columns:
        m=pat.match(c)
        if m:
            t=int(m.group(1))
            if t>=0: out.append((t,c))
    out.sort()
    return [c for _,c in out]

def find_tp_day(highs, start, tp_pct):
    if not np.isfinite(start) or start<=0: return None
    for t,h in enumerate(highs):
        if np.isfinite(h) and (h/start-1.0)>=tp_pct:
            return t
    return None

def rsi_confluence_exit_day(rsi, macdh, adx, bbw, Y, DLT, Z, M, family, p1):
    had_momo=False
    rsi_peak=-np.inf
    adx_peak=-np.inf
    bbw_peak=-np.inf
    for t in range(len(rsi)):
        x=rsi[t]
        if np.isfinite(x):
            rsi_peak=max(rsi_peak,x)
            if rsi_peak>=Y: had_momo=True
        # track peaks for confirms
        if np.isfinite(adx[t]): adx_peak=max(adx_peak, adx[t])
        if np.isfinite(bbw[t]): bbw_peak=max(bbw_peak, bbw[t])

        if t>=M and had_momo:
            retr = (np.isfinite(x) and (rsi_peak - x) >= DLT) or (Z is not None and np.isfinite(x) and x<=Z)
            if not retr: 
                continue
            # confirm families
            ok=False
            if family=="macd_leq":
                ok = np.isfinite(macdh[t]) and (macdh[t] <= p1)   # p1 in {0.0, -0.02}
            elif family=="adx_drop":
                ok = np.isfinite(adx[t]) and np.isfinite(adx_peak) and (adx_peak - adx[t] >= p1)  # p1 in {5,10,15}
            elif family=="bbw_contract":
                ok = np.isfinite(bbw[t]) and np.isfinite(bbw_peak) and (bbw_peak>0) and ((bbw_peak - bbw[t]) / bbw_peak >= p1)  # p1 in {0.2,0.35,0.5}
            if ok:
                return t
    return None

def ret_at_idx(series, start, t):
    if not np.isfinite(start) or start<=0 or t is None or t>=len(series): return np.nan
    v=series[t]
    return (v/start-1.0) if np.isfinite(v) else np.nan

def ret_timed(close, start, hold):
    t=min(hold, len(close)-1)
    if not np.isfinite(start) or start<=0 or t<0: return np.nan
    v=close[t]
    return (v/start-1.0) if np.isfinite(v) else np.nan

# load/merge
df  = pd.read_csv(IN)
lab = pd.read_csv(LAB)[["symbol","breakout_date","win_flag"]]
df = df.merge(lab, on=["symbol","breakout_date"], how="left")

rsi_cols   = day_cols("rsi", df)
high_cols  = day_cols("high", df)
close_cols = day_cols("close", df)
macd_cols  = day_cols("macd", df)
sig_cols   = day_cols("macd_signal", df)
adx_cols   = day_cols("adx", df)
bbw_cols   = day_cols("bbw", df)

if not all([rsi_cols, high_cols, close_cols, macd_cols, sig_cols, adx_cols, bbw_cols]):
    raise RuntimeError("Missing one or more required *_d* series (rsi/high/close/macd/macd_signal/adx/bbw).")

Ys   = [60, 70, 80]          # RSI threshold
DLTs = [5, 10, 15]           # RSI retrace
Zs   = [None, 55, 50]        # Absolute floor
Ms   = [1, 2]                # Min bars post-trigger

families = [
    ("macd_leq",     [0.0, -0.02]),
    ("adx_drop",     [5, 10, 15]),
    ("bbw_contract", [0.20, 0.35, 0.50]),
]

rows=[]
for Y,DLT,Z,M in product(Ys,DLTs,Zs,Ms):
    for fam, params in families:
        for p1 in params:
            recs=[]
            for _,row in df.iterrows():
                lvl = int(row["market_level"]) if pd.notna(row["market_level"]) else None
                if lvl not in TP: 
                    continue
                start=row.get("start_price", np.nan)
                hold = HOLD[lvl]

                rsi   = row[rsi_cols].to_numpy(float, copy=False)
                highs = row[high_cols].to_numpy(float, copy=False)
                close = row[close_cols].to_numpy(float, copy=False)
                macdh = (row[macd_cols].to_numpy(float, copy=False) - row[sig_cols].to_numpy(float, copy=False))
                adx   = row[adx_cols].to_numpy(float, copy=False)
                bbw   = row[bbw_cols].to_numpy(float, copy=False)

                t_tp = find_tp_day(highs, start, TP[lvl])
                if t_tp is not None:
                    t_exit = t_tp
                    ret_exit = ret_at_idx(highs, start, t_tp)
                else:
                    t_re = rsi_confluence_exit_day(rsi, macdh, adx, bbw, Y, DLT, Z, M, fam, p1)
                    if t_re is not None:
                        t_exit = t_re
                        ret_exit = ret_at_idx(close, start, t_re)
                    else:
                        t_exit = hold
                        ret_exit = ret_timed(close, start, hold)

                ret_t = ret_timed(close, start, hold)
                winlbl = row.get("win_flag", np.nan)
                recs.append((lvl, winlbl, t_exit, ret_exit, ret_t))

            tmp = pd.DataFrame(recs, columns=["level","win_flag","t_exit","ret_exit","ret_timed"])
            win_mask  = (tmp["win_flag"]==1)
            loss_mask = (tmp["win_flag"]==0)

            win_retention = (tmp.loc[win_mask, "ret_exit"]>=0.20).mean() if win_mask.any() else np.nan
            loser_improve = (tmp.loc[loss_mask,"ret_exit"]-tmp.loc[loss_mask,"ret_timed"]).mean() if loss_mask.any() else np.nan
            overall_mean  = tmp["ret_exit"].mean()
            med_days      = float(np.nanmedian(tmp["t_exit"]))

            rows.append({
                "family":fam, "param":p1, "Y":Y, "Delta":DLT, "Z":("None" if Z is None else Z), "M":M,
                "overall_win_retention": None if pd.isna(win_retention) else round(win_retention,3),
                "loser_improvement_mean": None if pd.isna(loser_improve) else round(loser_improve,4),
                "overall_mean_return": round(overall_mean,4),
                "overall_median_days": round(med_days,2)
            })

res = pd.DataFrame(rows).sort_values(
    by=["overall_win_retention","overall_mean_return","loser_improvement_mean"],
    ascending=[False,False,False], na_position="last"
)
res.to_csv(OUT, index=False)
print("Saved grid ->", OUT)
print(res.head(20).to_string(index=False))
