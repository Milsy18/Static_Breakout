import os, re, numpy as np, pandas as pd

# --- INPUT/OUTPUT: hardcoded so PS vars aren't needed ---
IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3_tp_policy_confluence.csv"

# --- RSI trigger ---
Y, DELTA, M = 75, 5, 3           # threshold, retrace, consecutive bars
DEFER_1_BAR = True               # defer exit one bar to reduce whipsaws
ALLOWED_LVLS = {4}               # only allow RSI exits at level 4

# --- Confluence veto thresholds ---
ADX_KEEP      = 5.0              # if ADX drop from peak < 5  -> veto RSI exit
BBW_KEEP      = 0.20             # if BBW contraction < 20%   -> veto RSI exit
MACD_KEEP_POS = 0.0              # if MACD histogram > 0      -> veto RSI exit

# --- TP thresholds & hold days by level ---
tp_by   = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
hold_by = {1:5,   2:5,   3:8,   4:6,   5:8,   6:6,   7:6,   8:7,   9:6}

df = pd.read_csv(IN)

def cols(prefix:str):
    ks=[]
    for c in df.columns:
        if re.fullmatch(rf"{re.escape(prefix)}_d-?\d+", c):
            k=int(c.split("d")[-1])
            if k>=0: ks.append((k,c))
    ks.sort()
    return [c for _,c in ks]

# arrays
RSI  = df[cols("rsi")].to_numpy(float)
MACD = df[cols("macd")].to_numpy(float)
MSIG = df[cols("macd_signal")].to_numpy(float)
ADX  = df[cols("adx")].to_numpy(float)
BBW  = df[cols("bbw")].to_numpy(float)
HIGH = df[cols("high")].to_numpy(float)
CLOSE= df[cols("close")].to_numpy(float)

# start price fallback
if "start_price" in df:
    start = df["start_price"].astype(float).to_numpy()
elif "entry_price" in df:
    start = df["entry_price"].astype(float).to_numpy()
elif "open_d0" in df:
    start = df["open_d0"].astype(float).to_numpy()
else:
    start = df["close_d0"].astype(float).to_numpy()

lvl  = df["market_level"].astype(int).clip(1,9).to_numpy()
tp   = np.array([tp_by.get(int(x),0.90) for x in lvl], dtype=float)
hold = np.array([hold_by.get(int(x),6)  for x in lvl], dtype=int)

T   = min(RSI.shape[1], MACD.shape[1], MSIG.shape[1], ADX.shape[1], BBW.shape[1], HIGH.shape[1], CLOSE.shape[1]) - 1
cap = np.minimum(hold, T)

def find_tp(i):
    thresh = start[i]*(1.0+tp[i])
    upto   = int(cap[i])
    for t in range(upto+1):
        if HIGH[i,t] >= thresh:
            return t
    return None

def rsi_cross_idx(i):
    r = RSI[i]; upto = int(cap[i])
    for t in range(upto+1):
        seg = r[t:t+M]
        if len(seg)==M and np.all(np.isfinite(seg)) and np.all(seg>=Y):
            return t
    return None

def rsi_exit_idx(i, t_cross):
    if t_cross is None: return None
    r = RSI[i]; upto = int(cap[i])
    peak = -np.inf
    for t in range(t_cross, upto+1):
        x = r[t]
        if np.isfinite(x) and x>peak: peak = x
        if np.isfinite(peak) and np.isfinite(x) and (peak - x) >= DELTA:
            return t
    return None

def adx_drop_now(i, t):
    a = ADX[i,:t+1]
    if np.any(np.isfinite(a)):
        m = np.nanmax(a)
        x = ADX[i,t]
        if np.isfinite(m) and np.isfinite(x):
            return (m - x)
    return np.inf

def bbw_contract_now(i, t):
    b = BBW[i,:t+1]
    if np.any(np.isfinite(b)):
        m = np.nanmax(b)
        x = BBW[i,t]
        if np.isfinite(m) and m>0 and np.isfinite(x):
            return (m - x)/m
    return np.inf

def macd_hist_now(i, t):
    x = MACD[i,t]; s = MSIG[i,t]
    return (x - s) if (np.isfinite(x) and np.isfinite(s)) else -np.inf

exit_day=[]; exit_type=[]; exit_ret=[]; timed_ret=[]

for i in range(len(df)):
    t_tp    = find_tp(i)
    t_timed = int(cap[i])

    # default: timed
    t_ex, ety = t_timed, "timed"

    # RSI candidate only if level allowed
    if int(lvl[i]) in ALLOWED_LVLS:
        tcross = rsi_cross_idx(i)
        t_rsi  = rsi_exit_idx(i, tcross)
        if t_rsi is not None and DEFER_1_BAR:
            t_rsi = min(t_rsi+1, t_timed)
        if t_rsi is not None:
            veto = False
            ad_drop = adx_drop_now(i, t_rsi)
            bbw_ctr = bbw_contract_now(i, t_rsi)
            mh      = macd_hist_now(i, t_rsi)
            if np.isfinite(ad_drop) and ad_drop < ADX_KEEP:   veto = True
            if np.isfinite(bbw_ctr) and bbw_ctr < BBW_KEEP:   veto = True
            if np.isfinite(mh) and mh > MACD_KEEP_POS:        veto = True
            if not veto:
                t_ex, ety = t_rsi, "rsi"

    # TP pre-emption
    if t_tp is not None and (t_ex is None or t_tp <= t_ex):
        t_ex, ety = t_tp, "tp"

    sp = start[i]
    ret_actual = (CLOSE[i,t_ex]/sp - 1.0) if np.isfinite(CLOSE[i,t_ex]) else (tp[i] if ety=="tp" else 0.0)
    ret_timed  = (CLOSE[i,int(cap[i])]/sp - 1.0) if np.isfinite(CLOSE[i,int(cap[i])]) else 0.0

    exit_day.append(t_ex); exit_type.append(ety); exit_ret.append(ret_actual); timed_ret.append(ret_timed)

d = df.assign(exit_day=exit_day, exit_type=exit_type, exit_ret=exit_ret, timed_ret=timed_ret)

# summaries
mix = d["exit_type"].value_counts(normalize=True).rename("%").round(3)*100
print("Exit mix (%):"); print(mix.astype(int).sort_index())

print("\nOverall (mean/median):")
print("actual_mean ", round(np.nanmean(d["exit_ret"]),4))
print("timed_mean  ", round(np.nanmean(d["timed_ret"]),4))
print("actual_median", round(np.nanmedian(d["exit_ret"]),4))
print("timed_median ", round(np.nanmedian(d["timed_ret"]),4))

g = d.groupby("market_level").agg(
    n=("exit_ret","size"),
    actual_mean =("exit_ret","mean"),
    timed_mean  =("timed_ret","mean"),
    actual_median=("exit_ret","median"),
    timed_median =("timed_ret","median"),
).round(4)
g["mean_uplift"]   = (g["actual_mean"]   - g["timed_mean"]).round(4)
g["median_uplift"] = (g["actual_median"] - g["timed_median"]).round(4)

print("\nBy level (means/medians and uplift vs. timed):")
print(g.to_string())

d.to_csv(OUT, index=False)
print("\nWrote ->", OUT)
