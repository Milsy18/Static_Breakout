# --- rsi_exit_apply_y75_d5_m3_tp.py ---
# Apply exits with TP pre-emption first, else RSI drawdown exit, else timed hold.
# Params: Y=75, DELTA=5, M=3   (toggle DEFER_1_BAR below if desired)

import re
import numpy as np
import pandas as pd
from pathlib import Path

# ---- I/O ----
IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3_tp.csv"

# ---- Exit parameters ----
Y = 75          # RSI threshold
DELTA = 5       # drawdown from RSI peak to trigger exit
M = 3           # bars RSI must stay >= Y to confirm momentum before watching for drawdown
DEFER_1_BAR = False   # set True to wait one extra bar after RSI drawdown triggers

# Timed hold (cap) in bars; keep 5 to match your earlier runs/screens
HOLD_DAYS = 5

# Take-profit % by market level (same mapping we’ve been using)
tp_by = {1:0.65, 2:0.85, 3:0.90, 4:0.85, 5:0.95, 6:0.90, 7:0.95, 8:0.95, 9:0.95}

# ---- Helpers to grab forward series ----
def cols(df, prefix):
    # matches rsi_d0.., rsi_d1.. ; ignores negative day columns
    ks = []
    pat = re.compile(rf"{re.escape(prefix)}_d-?\d+$")
    for c in df.columns:
        if pat.fullmatch(c):
            try:
                k = int(c.split("d")[-1])
            except Exception:
                continue
            if k >= 0:
                ks.append((k, c))
    ks.sort()
    return [c for _, c in ks]

# ---- Load ----
df = pd.read_csv(IN)

# forward arrays
RSI   = df[cols(df, "rsi")  ].to_numpy(float)
HIGH  = df[cols(df, "high") ].to_numpy(float)
CLOSE = df[cols(df, "close")].to_numpy(float)

# start price (fallbacks)
if "start_price" in df:
    start = df["start_price"].astype(float).to_numpy()
elif "entry_price" in df:
    start = df["entry_price"].astype(float).to_numpy()
elif "open_d0" in df:
    start = df["open_d0"].astype(float).to_numpy()
else:
    start = df["close_d0"].astype(float).to_numpy()

lvl = df["market_level"].astype(int).clip(1, 9).to_numpy()
tp  = np.array([tp_by.get(int(x), 0.90) for x in lvl])

T = min(RSI.shape[1], HIGH.shape[1], CLOSE.shape[1])
hold_by = {1:5,2:5,3:8,4:6,5:8,6:6,7:6,8:7,9:6}
cap = np.array([min(hold_by.get(int(x),5), T-1) for x in lvl], dtype=int)


def find_tp(i):
    """first bar where HIGH >= start*(1+tp) within cap"""
    th = start[i] * (1.0 + tp[i])
    upto = int(cap[i])
    for t in range(upto + 1):
        if np.isfinite(HIGH[i, t]) and HIGH[i, t] >= th:
            return t
    return None

def rsi_cross_idx(i):
    """first index t where the next M bars of RSI are all >= Y"""
    r = RSI[i]
    upto = int(cap[i])
    for t in range(upto + 1):
        seg = r[t:t+M]
        if len(seg) == M and np.all(np.isfinite(seg)) and np.all(seg >= Y):
            return t
    return None

def rsi_drawdown_exit(i, t_cross):
    """after cross, exit when RSI has fallen DELTA from its running peak"""
    if t_cross is None:
        return None
    r = RSI[i]
    upto = int(cap[i])
    peak = -np.inf
    for t in range(t_cross, upto + 1):
        x = r[t]
        if np.isfinite(x) and x > peak:
            peak = x
        if np.isfinite(peak) and np.isfinite(x) and (peak - x) >= DELTA:
            return t
    return None

# ---- Walk trades ----
exit_day = []
exit_type = []
exit_ret = []
pct_tp = 0

for i in range(len(df)):
    t_tp    = find_tp(i)
    t_cross = rsi_cross_idx(i)
    t_rsi   = rsi_drawdown_exit(i, t_cross)

    # default to timed
    t_ex, ety = int(cap[i]), "timed"

    # use RSI if it fired
    if t_rsi is not None:
        t_ex, ety = t_rsi, "rsi"
        if DEFER_1_BAR:
            t_ex = min(t_ex + 1, int(cap[i]))

    # TP pre-empts everything if it occurs earlier or equal
    if t_tp is not None and t_tp <= t_ex:
        t_ex, ety = t_tp, "tp"
        pct_tp += 1

    # realized return
    if np.isfinite(CLOSE[i, t_ex]):
        ret = CLOSE[i, t_ex] / start[i] - 1.0
    else:
        # rare: if CLOSE NaN at TP bar, fall back to TP%
        ret = tp[i] if ety == "tp" else np.nan

    exit_day.append(t_ex)
    exit_type.append(ety)
    exit_ret.append(ret)

out = df.assign(exit_day=exit_day, exit_type=exit_type, exit_ret=exit_ret)

# ---- Console summary ----
print(f"Applied RSI+TP exit  Y={Y}, Δ={DELTA}, M={M}, DEFER_1_BAR={DEFER_1_BAR}")
print(f"Rows: {len(out)} | win_rate (from labels, if present): "
      f"{out['win_flag'].mean():.3f}" if 'win_flag' in out else f"Rows: {len(out)}")

print("\nexit_type mix (%):")
print(out["exit_type"].value_counts(normalize=True).rename("%").round(3))

by = out.groupby("market_level", as_index=True).agg(
    n=("exit_ret", "size"),
    win_rate=("win_flag", "mean") if "win_flag" in out else ("exit_ret", "size"),
    median_ret=("exit_ret", "median"),
    mean_ret=("exit_ret", "mean"),
    median_days=("exit_day", "median"),
    pct_tp=("exit_type", lambda s: (s=="tp").mean()),
    pct_rsi=("exit_type", lambda s: (s=="rsi").mean()),
    pct_timed=("exit_type", lambda s: (s=="timed").mean()),
).round(3)

print("\nBy level:\n")
print(by.reset_index().to_string(index=False))

Path(OUT).parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT, index=False)
print("\nWrote ->", OUT)
