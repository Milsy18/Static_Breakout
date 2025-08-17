# rsi_exit_apply_policy.py
import os, re, sys
import numpy as np
import pandas as pd

# ---- INPUT / OUTPUT (edit if you keep files elsewhere) ----
IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3_tp_policy.csv"

# ---- Policy knobs ----
Y, DELTA, M = 75, 5, 3
ALLOW_RSI_LEVELS = {4}       # enable RSI exits only where uplift was positive
DEFER_1_BAR = True         # set True to defer the RSI exit by 1 bar

if not os.path.exists(IN):
    print("Input not found:", IN)
    sys.exit(1)

df = pd.read_csv(IN)

def day_cols(prefix: str):
    ks = []
    for c in df.columns:
        m = re.fullmatch(rf"{re.escape(prefix)}_d-?\d+", c)
        if m:
            k = int(c.split("d")[-1])
            ks.append((k, c))
    ks = [(k, c) for (k, c) in ks if k >= 0]
    ks.sort()
    return [c for _, c in ks]

RSI   = df[day_cols("rsi")].to_numpy(float)
HIGH  = df[day_cols("high")].to_numpy(float)
CLOSE = df[day_cols("close")].to_numpy(float)

if "start_price" in df.columns:
    START = df["start_price"].astype(float).to_numpy()
elif "open_d0" in df.columns:
    START = df["open_d0"].astype(float).to_numpy()
else:
    START = df["close_d0"].astype(float).to_numpy()

LVL = df["market_level"].astype(int).clip(1, 9).to_numpy()

# TP and timed-hold maps
tp_by   = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
hold_by = {1:5,   2:5,   3:8,   4:6,   5:8,   6:6,   7:6,   8:7,   9:6}

TP   = np.array([tp_by.get(int(x), 0.90) for x in LVL])
HOLD = np.array([hold_by.get(int(x), 6)  for x in LVL])

T = min(RSI.shape[1], HIGH.shape[1], CLOSE.shape[1]) - 1
cap = np.minimum(HOLD, T)

def find_tp(i: int):
    th = START[i] * (1.0 + TP[i])
    upto = int(cap[i])
    for t in range(upto + 1):
        if HIGH[i, t] >= th:
            return t
    return None

def rsi_cross_idx(i: int):
    r = RSI[i]; upto = int(cap[i])
    for t in range(upto + 1):
        seg = r[t:t+M]
        if len(seg) == M and np.all(np.isfinite(seg)) and np.all(seg >= Y):
            return t
    return None

def rsi_exit_idx(i: int, t_cross: int | None):
    if t_cross is None:
        return None
    r = RSI[i]; upto = int(cap[i])
    peak = -np.inf
    for t in range(t_cross, upto + 1):
        x = r[t]
        if np.isfinite(x) and x > peak:
            peak = x
        if np.isfinite(peak) and np.isfinite(x) and (peak - x) >= DELTA:
            return t
    return None

def close_at_timed(i: int):
    h = int(HOLD[i])
    # direct hit if available
    if 0 <= h <= 9:
        col = f"close_d{h}"
        if col in df.columns and np.isfinite(df.iloc[i][col]):
            return float(df.iloc[i][col])
    # walk back to the latest available close
    for k in range(min(h, 9), -1, -1):
        col = f"close_d{k}"
        if col in df.columns and np.isfinite(df.iloc[i][col]):
            return float(df.iloc[i][col])
    return np.nan

exit_day, exit_type, exit_ret, timed_ret = [], [], [], []

for i in range(len(df)):
    t_tp    = find_tp(i)
    t_timed = int(cap[i])
    t_ex, ety = t_timed, "timed"

    if int(LVL[i]) in ALLOW_RSI_LEVELS:
        t_cross = rsi_cross_idx(i)
        t_rsi   = rsi_exit_idx(i, t_cross)
        if t_rsi is not None:
            if DEFER_1_BAR:
                t_rsi = min(t_rsi + 1, t_timed)
            t_ex, ety = t_rsi, "rsi"

    if t_tp is not None and (t_ex is None or t_tp <= t_ex):
        t_ex, ety = t_tp, "tp"

    if t_ex is not None and np.isfinite(CLOSE[i, t_ex]):
        ret = CLOSE[i, t_ex] / START[i] - 1.0
    else:
        ret = np.nan

    tc = close_at_timed(i)
    tret = (tc / START[i] - 1.0) if np.isfinite(tc) else np.nan

    exit_day.append(t_ex); exit_type.append(ety); exit_ret.append(ret); timed_ret.append(tret)

out = df.assign(exit_day=exit_day, exit_type=exit_type, exit_ret=exit_ret, timed_ret=timed_ret)
out.to_csv(OUT, index=False)

mix = out["exit_type"].value_counts(normalize=True).mul(100).round(1)
print("Exit mix (%):"); print(mix.to_string())

overall = pd.Series({
    "actual_mean":  out["exit_ret"].mean(),
    "timed_mean":   out["timed_ret"].mean(),
    "actual_median":out["exit_ret"].median(),
    "timed_median": out["timed_ret"].median(),
}).round(4)
print("\nOverall (mean/median):"); print(overall.to_string())

g = out.groupby("market_level").agg(
    n=("market_level","size"),
    actual_mean=("exit_ret","mean"),
    timed_mean=("timed_ret","mean"),
    actual_median=("exit_ret","median"),
    timed_median=("timed_ret","median"),
).round(4)
g["mean_uplift"]   = (g["actual_mean"]   - g["timed_mean"]).round(4)
g["median_uplift"] = (g["actual_median"] - g["timed_median"]).round(4)
print("\nBy level (means/medians and uplift vs. timed):")
print(g.to_string())

print("\nWrote ->", OUT)

