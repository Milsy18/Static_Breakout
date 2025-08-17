# rsi_exit_apply_y75_d5_m3.py
import re
import numpy as np
import pandas as pd

# --- inputs/outputs: adjust if you keep files elsewhere ---
IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3.csv"

# --- RSI exit parameters ---
Y = 75      # threshold
DELTA = 5   # retrace from peak
M = 3       # consecutive bars above Y before we start watching retrace

# --- read data ---
df = pd.read_csv(IN)

def cols(prefix: str):
    ks = []
    for c in df.columns:
        if re.fullmatch(rf"{prefix}_d-?\d+", c):
            k = int(c.split("d")[-1])
            if k >= 0:
                ks.append((k, c))
    ks.sort()
    return [c for _, c in ks]

# indicator panels (d0..dN, only forward bars)
RSI   = df[cols("rsi")].to_numpy(float)
MACD  = df[cols("macd")].to_numpy(float)
MSIG  = df[cols("macd_signal")].to_numpy(float)  # not used here, but handy
ADX   = df[cols("adx")].to_numpy(float)          # not used here, but handy
BBW   = df[cols("bbw")].to_numpy(float)          # not used here, but handy
HIGH  = df[cols("high")].to_numpy(float)
CLOSE = df[cols("close")].to_numpy(float)

# start/level/hold
start = (df["start_price"] if "start_price" in df
         else df.get("entry_price", df["open_d0"] if "open_d0" in df else df["close_d0"])
        ).astype(float).to_numpy()
lvl   = df["market_level"].astype(int).clip(1, 9).to_numpy()

hold_by = {1:5,2:5,3:8,4:6,5:8,6:6,7:6,8:7,9:6}
hold   = np.array([hold_by.get(int(x), 6) for x in lvl])

# --- TP thresholds by level (same mapping we’ve been using) ---
tp_by = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
tp    = np.array([tp_by.get(int(x), 0.90) for x in lvl])

# bounds
T   = min(RSI.shape[1], HIGH.shape[1], CLOSE.shape[1]) - 1
cap = np.minimum(hold, T)

# --- helpers ---
def find_tp(i: int):
    """First bar where HIGH >= start*(1+tp[level]) up to cap."""
    th   = start[i] * (1.0 + tp[i])
    upto = int(cap[i])
    for t in range(upto + 1):
        if np.isfinite(HIGH[i, t]) and HIGH[i, t] >= th:
            return t
    return None

def rsi_cross_idx(i: int):
    """First bar where we have M consecutive bars with RSI >= Y."""
    r = RSI[i]; upto = int(cap[i])
    for t in range(upto + 1):
        seg = r[t:t+M]
        if len(seg) == M and np.all(np.isfinite(seg)) and np.all(seg >= Y):
            return t
    return None

def rsi_exit_idx(i: int, t_cross: int | None):
    """From first cross, track running peak; exit when peak - rsi >= DELTA."""
    if t_cross is None:
        return None
    r = RSI[i]; upto = int(cap[i])
    peak = -np.inf
    for t in range(t_cross, upto + 1):
        x = r[t]
        if np.isfinite(x) and x > peak:
            peak = x
        if np.isfinite(peak) and (peak - x) >= DELTA:
            return t
    return None

# --- simulate exits ---
exit_day, exit_type, exit_ret = [], [], []

for i in range(len(df)):
    t_timed = int(cap[i])

    # RSI path
    t_cross = rsi_cross_idx(i)
    t_rsi   = rsi_exit_idx(i, t_cross)

    # default = timed exit
    t_ex, ety = t_timed, "timed"

    # take RSI exit if it happens before timed
    if t_rsi is not None and t_rsi < t_ex:
        t_ex, ety = t_rsi, "rsi"

    # TP pre-emption: if TP hits earlier than current choice, take TP
    t_tp = find_tp(i)
    if t_tp is not None and t_tp <= t_ex:
        t_ex, ety = t_tp, "tp"

    # realized return
    if np.isfinite(CLOSE[i, t_ex]):
        ret = CLOSE[i, t_ex] / start[i] - 1.0
    else:
        ret = tp[i] if ety == "tp" else np.nan

    exit_day.append(t_ex)
    exit_type.append(ety)
    exit_ret.append(ret)

out = df.assign(exit_day=exit_day, exit_type=exit_type, exit_ret=exit_ret)
print(f"Applied RSI exit Y={Y}, Δ={DELTA}, M={M}")
print(out["exit_type"].value_counts(normalize=True).rename("%").round(3))
print("Rows:", len(out), "| win_rate (from labels, if present):",
      round(out["win_flag"].mean(), 3) if "win_flag" in out.columns else "n/a")

# quick by-level summary
by = out.groupby("market_level").agg(
    n=("exit_ret", "size"),
    win_rate=("win_flag", "mean"),
    median_ret=("exit_ret", "median"),
    mean_ret=("exit_ret", "mean"),
    median_days=("exit_day", "median"),
    pct_rsi=("exit_type", lambda s: (s == "rsi").mean()),
    pct_tp=("exit_type", lambda s: (s == "tp").mean()),
    pct_timed=("exit_type", lambda s: (s == "timed").mean()),
).round(3)
print("\nBy level:\n")
print(by)

out.to_csv(OUT, index=False)
print("\nWrote ->", OUT)
