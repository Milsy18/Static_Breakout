import re, numpy as np, pandas as pd
from itertools import product

IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_events_timed_full.csv"
LAB = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_grid_results.csv"

TP   = {1:0.65,2:0.85,3:0.90,4:0.85,5:0.95,6:0.90,7:0.95,8:0.95,9:0.95}
HOLD = {1:5, 2:5, 3:8, 4:6, 5:8, 6:6, 7:6, 8:7, 9:6}

def day_cols(prefix, df):
    pat = re.compile(rf"^{re.escape(prefix)}_d(\-?\d+)$")
    out = []
    for c in df.columns:
        m = pat.match(c)
        if m:
            t = int(m.group(1))
            if t >= 0:
                out.append((t, c))
    out.sort()
    return [c for _,c in out]

def find_tp_day(highs, start, tp_pct):
    if not np.isfinite(start) or start <= 0: return None
    for t, h in enumerate(highs):
        if np.isfinite(h) and (h/start - 1.0) >= tp_pct:
            return t
    return None

def rsi_exit_day(rsi, Y, dlt, Z, min_hold):
    had_momo = False
    peak = -np.inf
    for t, x in enumerate(rsi):
        if not np.isfinite(x): 
            continue
        peak = max(peak, x)
        if peak >= Y:
            had_momo = True
        if t >= min_hold and had_momo:
            retr = (peak - x) >= dlt
            floor = (Z is not None) and (x <= Z)
            if retr or floor:
                return t
    return None

def ret_at_close(closes, start, t):
    if not np.isfinite(start) or start <= 0: return np.nan
    if t is None or t >= len(closes): return np.nan
    c = closes[t]
    return (c/start - 1.0) if np.isfinite(c) else np.nan

def ret_timed(closes, start, hold):
    t = min(hold, len(closes)-1)
    if not np.isfinite(start) or start <= 0 or t < 0: return np.nan
    c = closes[t]
    return (c/start - 1.0) if np.isfinite(c) else np.nan

# --- load data
df  = pd.read_csv(IN)
lab = pd.read_csv(LAB)[["symbol","breakout_date","win_flag"]]
df = df.merge(lab, on=["symbol","breakout_date"], how="left")

rsi_cols   = day_cols("rsi",   df)
high_cols  = day_cols("high",  df)
close_cols = day_cols("close", df)

if not rsi_cols or not high_cols or not close_cols:
    raise RuntimeError("Expected rsi_d*, high_d*, close_d* columns were not found.")

# --- parameter grid
Ys   = [60, 65, 70, 75, 80]
DLTs = [5, 7, 10, 12, 15]
Zs   = [None, 55, 50, 45]
Ms   = [1, 2, 3]

rows_out = []
for Y, DLT, Z, M in product(Ys, DLTs, Zs, Ms):
    recs = []
    for _, row in df.iterrows():
        lvl = int(row["market_level"]) if pd.notna(row["market_level"]) else None
        if lvl not in TP: 
            continue
        start = row.get("start_price", np.nan)
        hold  = HOLD[lvl]
        rsi   = row[rsi_cols].to_numpy(dtype=float, copy=False)
        highs = row[high_cols].to_numpy(dtype=float, copy=False)
        close = row[close_cols].to_numpy(dtype=float, copy=False)

        t_tp  = find_tp_day(highs, start, TP[lvl])
        t_re  = rsi_exit_day(rsi, Y, DLT, Z, M)

        if t_tp is not None:
            t_exit = t_tp
            ret_exit = (highs[t_tp]/start - 1.0) if (np.isfinite(highs[t_tp]) and np.isfinite(start) and start>0) else np.nan
        elif t_re is not None:
            t_exit = t_re
            ret_exit = ret_at_close(close, start, t_exit)
        else:
            t_exit = hold
            ret_exit = ret_timed(close, start, hold)

        ret_t = ret_timed(close, start, hold)
        winlbl = row.get("win_flag", np.nan)
        recs.append((lvl, winlbl, t_exit, ret_exit, ret_t))

    if not recs:
        continue
    tmp = pd.DataFrame(recs, columns=["level","win_flag","t_exit","ret_exit","ret_timed"])
    win_mask  = (tmp["win_flag"] == 1)
    loss_mask = (tmp["win_flag"] == 0)

    # robust metrics even if labels are all 1s
    win_retention = (tmp.loc[win_mask, "ret_exit"] >= 0.20).mean() if win_mask.any() else np.nan
    med_days      = float(np.nanmedian(tmp["t_exit"]))
    loser_improve = (tmp.loc[loss_mask, "ret_exit"] - tmp.loc[loss_mask, "ret_timed"]).mean() if loss_mask.any() else np.nan
    win_med_ret   = tmp.loc[win_mask, "ret_exit"].median() if win_mask.any() else np.nan
    overall_mean  = tmp["ret_exit"].mean()

    rows_out.append({
        "Y":Y, "Delta":DLT, "Z":("None" if Z is None else Z), "M":M,
        "overall_win_retention": None if pd.isna(win_retention) else round(win_retention,3),
        "overall_median_days": round(med_days,2),
        "loser_improvement_mean": None if pd.isna(loser_improve) else round(loser_improve,4),
        "winner_median_return": None if pd.isna(win_med_ret) else round(win_med_ret,4),
        "overall_mean_return": round(overall_mean,4)
    })

res = pd.DataFrame(rows_out).sort_values(
    by=["overall_win_retention","overall_mean_return","loser_improvement_mean"],
    ascending=[False,False,False], na_position="last"
)
res.to_csv(OUT, index=False)
print("Saved grid ->", OUT)
print(res.head(12).to_string(index=False))
