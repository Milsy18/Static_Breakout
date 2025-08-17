import re, numpy as np, pandas as pd

# ---- params (hard-set) ----
Y = 75          # threshold to first reach
DELTA = 5       # pullback after the peak
M = 3           # must stay >=Y for at least M bars before we allow pullback check

IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3.csv"

# If you want level-specific timed exits, set them here; otherwise default=5
HOLD_BY_LEVEL = {lvl: 5 for lvl in range(1, 10)}

df = pd.read_csv(IN)

# locate RSI and Close columns like rsi_d0..rsi_d9, close_d0..close_d9
rsi_cols   = sorted([c for c in df.columns if re.fullmatch(r"rsi_d\d+", c)], key=lambda x: int(x.split("d")[-1]))
close_cols = sorted([c for c in df.columns if re.fullmatch(r"close_d\d+", c)], key=lambda x: int(x.split("d")[-1]))

if not rsi_cols or not close_cols:
    raise ValueError("Could not find forward RSI/close columns like rsi_d0.. or close_d0.. in the file.")

# choose entry price column
if "start_price" in df.columns:
    entry_col = "start_price"
elif "entry_price" in df.columns:
    entry_col = "entry_price"
elif "open_d0" in df.columns:
    entry_col = "open_d0"
else:
    raise ValueError("No entry price column found (looked for start_price, entry_price, open_d0).")

def apply_exit(row):
    level = int(row.get("market_level", 5)) if pd.notna(row.get("market_level", np.nan)) else 5
    hold_n = int(HOLD_BY_LEVEL.get(level, 5))

    rsi_seq   = row[rsi_cols].to_numpy(dtype=float)
    close_seq = row[close_cols].to_numpy(dtype=float)

    # limit by available forward days
    max_idx = min(hold_n, len(rsi_seq) - 1, len(close_seq) - 1)

    # find first cross >= Y with M-bar persistence
    t_cross = None
    for t in range(0, max_idx + 1):
        window = rsi_seq[t: t + M]
        if len(window) == M and np.all(np.isfinite(window)) and np.all(window >= Y):
            t_cross = t
            break

    exit_day = max_idx
    exit_type = "timed"

    if t_cross is not None:
        # find rsi peak AFTER the cross
        seg = rsi_seq[t_cross: max_idx + 1]
        if np.any(np.isfinite(seg)):
            t_peak_rel = int(np.nanargmax(seg))
            t_peak = t_cross + t_peak_rel
            rsi_peak = rsi_seq[t_peak]
            thresh = rsi_peak - DELTA
            # first bar on/after the peak where RSI <= peak - DELTA
            for t in range(t_peak, max_idx + 1):
                if np.isfinite(rsi_seq[t]) and rsi_seq[t] <= thresh:
                    exit_day = t
                    exit_type = "rsi"
                    break

    entry = float(row[entry_col])
    px = float(close_seq[exit_day])
    exit_ret = (px / entry) - 1.0

    return pd.Series({"exit_day": exit_day, "exit_type": exit_type, "exit_ret": exit_ret})

out = df.join(df.apply(apply_exit, axis=1))

# quick prints
n = len(out)
win_rate = out["win_flag"].mean() if "win_flag" in out.columns else np.nan
print(f"Applied RSI exit Y={Y}, Δ={DELTA}, M={M}")
print(f"Rows: {n} | win_rate (from labels, if present): {win_rate:.3f}" if not np.isnan(win_rate) else f"Rows: {n}")
print(out[["exit_type"]].value_counts(normalize=True).rename("%").round(3))
print("\nBy level:")
if "market_level" in out.columns:
    g = out.groupby("market_level").agg(
        n=("symbol","size") if "symbol" in out.columns else ("exit_day","size"),
        win_rate=("win_flag","mean") if "win_flag" in out.columns else ("exit_day",lambda s: np.nan),
        median_ret=("exit_ret","median"),
        mean_ret=("exit_ret","mean"),
        median_days=("exit_day","median"),
        pct_rsi=("exit_type", lambda s: (s=="rsi").mean())
    ).round(3)
    print(g.to_string())
else:
    print("market_level not found; skipping per-level table.")

out.to_csv(OUT, index=False)
print(f"\nWrote -> {OUT}")
