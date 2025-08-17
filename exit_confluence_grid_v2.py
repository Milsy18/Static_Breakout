import re, numpy as np, pandas as pd
from itertools import product

LAB = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\exit_confluence_grid_results_v2.csv"

# Fallbacks if v2 file doesn't include these columns (it should)
TP_DEFAULT = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
HOLD_DEFAULT = {1:5,2:5,3:8,4:6,5:8,6:6,7:6,8:7,9:6}

df = pd.read_csv(LAB)

def have(cols): return all(c in df.columns for c in cols)

# -------- column discovery --------
def day_cols(prefix):
    pat = re.compile(rf"^{re.escape(prefix)}_d(-?\d+)$")
    items=[]
    for c in df.columns:
        m = pat.match(c)
        if m:
            t=int(m.group(1))
            if t>=0:
                items.append((t,c))
    items.sort()
    return [c for _,c in items]

rsi_cols   = day_cols("rsi")
macd_cols  = day_cols("macd")
sig_cols   = day_cols("macd_signal")
adx_cols   = day_cols("adx")
bbw_cols   = day_cols("bbw")
high_cols  = day_cols("high")
close_cols = day_cols("close")

must = [rsi_cols, macd_cols, sig_cols, adx_cols, bbw_cols, high_cols, close_cols]
if not all(len(x)>0 for x in must):
    missing = []
    if not rsi_cols: missing.append("rsi_d*")
    if not macd_cols: missing.append("macd_d*")
    if not sig_cols: missing.append("macd_signal_d*")
    if not adx_cols: missing.append("adx_d*")
    if not bbw_cols: missing.append("bbw_d*")
    if not high_cols: missing.append("high_d*")
    if not close_cols: missing.append("close_d*")
    raise SystemExit(f"Missing forward columns: {missing}")

max_fwd = min([len(rsi_cols), len(macd_cols), len(sig_cols), len(adx_cols), len(bbw_cols), len(high_cols), len(close_cols)]) - 1
if max_fwd < 1:
    raise SystemExit("Not enough forward days to evaluate exits.")

# entry / level / params per row
if "start_price" in df.columns:
    start_price = df["start_price"].astype(float).to_numpy()
elif "entry_price" in df.columns:
    start_price = df["entry_price"].astype(float).to_numpy()
elif "open_d0" in df.columns:
    start_price = df["open_d0"].astype(float).to_numpy()
else:
    start_price = df["close_d0"].astype(float).to_numpy()

level = df["market_level"].astype(int).clip(1,9).to_numpy()

if "tp_pct_assumed" in df.columns:
    tp_pct = df["tp_pct_assumed"].astype(float).to_numpy()
else:
    tp_pct = np.array([TP_DEFAULT.get(int(l),0.9) for l in level], dtype=float)

if "hold_days_assumed" in df.columns:
    hold_days = df["hold_days_assumed"].astype(int).to_numpy()
else:
    hold_days = np.array([HOLD_DEFAULT.get(int(l),6) for l in level], dtype=int)

# label
win_flag = df["win_flag"].astype(int).to_numpy() if "win_flag" in df.columns else None

# materialize forward arrays (N, T)
def mat(cols): return df[cols].to_numpy(dtype=float)
RSI  = mat(rsi_cols)
MACD = mat(macd_cols)
MSIG = mat(sig_cols)
ADX  = mat(adx_cols)
BBW  = mat(bbw_cols)
HIGH = mat(high_cols)
CLOSE= mat(close_cols)

# bound max index by available forward days and hold
cap_idx = np.minimum(hold_days, max_fwd)

def find_tp_day(i):
    sp = start_price[i]; tp = tp_pct[i]
    if not (np.isfinite(sp) and sp>0 and np.isfinite(tp)): return None
    upto = cap_idx[i]
    for t in range(0, upto+1):
        h = HIGH[i,t]
        if np.isfinite(h) and (h/sp - 1.0) >= tp:
            return t
    return None

def rsi_exit_day(i, Y, Delta, M):
    upto = cap_idx[i]
    r = RSI[i]
    # find first block of M bars all >= Y
    t_cross = None
    for t in range(0, upto+1):
        end = t+M
        if end-1 > upto: break
        seg = r[t:end]
        if np.all(np.isfinite(seg)) and np.all(seg >= Y):
            t_cross = t
            break
    if t_cross is None: return None
    # track peak after cross, then exit when retrace >= Delta
    peak = -np.inf
    for t in range(t_cross, upto+1):
        x = r[t]
        if np.isfinite(x):
            if x>peak: peak=x
            if np.isfinite(peak) and (peak - x) >= Delta:
                return t
    return None

def macd_hist_day(i, thr):
    upto = cap_idx[i]
    for t in range(0, upto+1):
        mh = MACD[i,t] - MSIG[i,t]
        if np.isfinite(mh) and mh <= thr:
            return t
    return None

def adx_drop_day(i, drop):
    upto = cap_idx[i]
    ap = -np.inf
    for t in range(0, upto+1):
        a = ADX[i,t]
        if np.isfinite(a):
            ap = max(ap, a)
            if np.isfinite(ap) and (ap - a) >= drop:
                return t
    return None

def bbw_contract_day(i, frac):
    upto = cap_idx[i]
    bp = -np.inf
    for t in range(0, upto+1):
        b = BBW[i,t]
        if np.isfinite(b):
            bp = max(bp, b)
            if np.isfinite(bp) and bp>0 and ((bp - b)/bp) >= frac:
                return t
    return None

def ret_from_close(i, t):
    sp = start_price[i]
    if t is None: return np.nan
    if not (np.isfinite(sp) and sp>0): return np.nan
    v = CLOSE[i,t]
    return (v/sp - 1.0) if np.isfinite(v) else np.nan

def ret_at_tp(i):
    # realize exactly the tp percentage on a TP fill
    return tp_pct[i]

def timed_day(i): return int(cap_idx[i])

def pick_exit(i, t_rsi, t_conf):
    # confluence: need BOTH signals; exit at the first bar when both have happened
    if (t_rsi is None) and (t_conf is None):
        return None
    if (t_rsi is None) or (t_conf is None):
        return None
    return max(t_rsi, t_conf)

# Search space (keep it tight; we already know Delta=5, M=3 are strong)
Y_grid = [70, 75, 80]
Delta_grid = [5]
M_grid = [3]

families = [
    ("none",        [None]),
    ("macd_leq",    [0.0, -0.05]),
    ("adx_drop",    [5, 10, 15]),
    ("bbw_contract",[0.20, 0.35, 0.50]),
]

rows=[]
N = len(df)

for Y,Delta,M in product(Y_grid, Delta_grid, M_grid):
    # precompute RSI days once
    t_rsi_all = [rsi_exit_day(i, Y, Delta, M) for i in range(N)]
    for fam, params in families:
        for p1 in params:
            t_conf_all = [None]*N
            if fam=="macd_leq":
                t_conf_all = [macd_hist_day(i, p1) for i in range(N)]
            elif fam=="adx_drop":
                t_conf_all = [adx_drop_day(i, p1) for i in range(N)]
            elif fam=="bbw_contract":
                t_conf_all = [bbw_contract_day(i, p1) for i in range(N)]
            # compute exits per trade
            exit_day = []
            exit_type = []
            exit_ret  = []
            timed_ret = []
            for i in range(N):
                t_tp = find_tp_day(i)
                td   = timed_day(i)
                # default timed
                t_ex = td
                ety  = "timed"
                r_ex = ret_from_close(i, td)
                # rsi-only or confluence
                if fam=="none":
                    t_r = t_rsi_all[i]
                    if t_r is not None:
                        t_ex, ety, r_ex = t_r, "rsi", ret_from_close(i, t_r)
                else:
                    t_r = t_rsi_all[i]; t_c = t_conf_all[i]
                    t_and = pick_exit(i, t_r, t_c)
                    if t_and is not None:
                        t_ex, ety, r_ex = t_and, f"rsi+{fam}", ret_from_close(i, t_and)
                # TP pre-emption if earlier
                if t_tp is not None and (t_ex is None or t_tp <= t_ex):
                    t_ex, ety, r_ex = t_tp, "tp", ret_at_tp(i)
                exit_day.append(t_ex if t_ex is not None else td)
                exit_type.append(ety)
                exit_ret.append(r_ex if np.isfinite(r_ex) else ret_from_close(i, td))
                timed_ret.append(ret_from_close(i, td))
            ex = pd.DataFrame({"exit_day":exit_day,"exit_type":exit_type,"exit_ret":exit_ret,"timed_ret":timed_ret})
            # metrics
            if win_flag is not None:
                winners = (win_flag==1)
                losers  = (win_flag==0)
                win_retention = float(np.mean((np.array(exit_ret) >= 0.20)[winners])) if winners.any() else np.nan
                loser_improve = (ex.loc[losers,"exit_ret"] - ex.loc[losers,"timed_ret"]).mean() if losers.any() else np.nan
            else:
                win_retention = np.nan; loser_improve = np.nan

            overall_mean = float(np.nanmean(exit_ret))
            med_days = float(np.nanmedian(exit_day))

            # % exit types
            et_counts = pd.Series(exit_type).value_counts(normalize=True)
            pct_tp    = float(et_counts.get("tp",0.0))
            pct_rsi   = float(et_counts.get("rsi",0.0))
            pct_conf  = float(et_counts[[c for c in et_counts.index if c.startswith("rsi+")]].sum()) if any(s.startswith("rsi+") for s in et_counts.index) else 0.0
            pct_timed = float(et_counts.get("timed",0.0))

            rows.append({
                "Y":Y,"Delta":Delta,"M":M,"family":fam,"param":p1,
                "overall_win_retention": round(win_retention,3) if pd.notna(win_retention) else None,
                "loser_improvement_mean": None if pd.isna(loser_improve) else round(loser_improve,4),
                "overall_mean_return": round(overall_mean,4),
                "overall_median_days": round(med_days,2),
                "pct_tp": round(pct_tp,3),
                "pct_rsi_only": round(pct_rsi,3),
                "pct_rsi_confluence": round(pct_conf,3),
                "pct_timed": round(pct_timed,3),
            })

res = pd.DataFrame(rows).sort_values(
    by=["overall_win_retention","overall_mean_return","loser_improvement_mean"],
    ascending=[False,False,False],
    na_position="last"
)
res.to_csv(OUT, index=False)
print("Saved confluence grid ->", OUT)
print(res.head(20).to_string(index=False))
