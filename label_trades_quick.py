import numpy as np, pandas as pd

IN  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_events_timed_full.csv"
OUT = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled.csv"

# TP% and hold-days by market level (your latest settings)
TP = {1:0.65, 2:0.85, 3:0.90, 4:0.85, 5:0.95, 6:0.90, 7:0.95, 8:0.95, 9:0.95}
HOLD = {1:5, 2:5, 3:8, 4:6, 5:8, 6:6, 7:6, 8:7, 9:6}

df = pd.read_csv(IN)
df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")

def peak_ret_hold(row):
    lvl = row["market_level"]
    start = row.get("start_price", np.nan)
    if pd.isna(lvl) or pd.isna(start) or start <= 0: return np.nan
    hold = int(HOLD.get(int(lvl), 0))
    best = -np.inf
    for d in range(0, hold+1):
        col = f"high_d{d}"
        if col in row and not pd.isna(row[col]):
            best = max(best, row[col]/start - 1.0)
    return best if np.isfinite(best) else np.nan

df["tp_pct_assumed"] = df["market_level"].map(TP)
df["peak_ret_hold"]  = df.apply(peak_ret_hold, axis=1)

# Win rule we agreed: TP hit OR peak return >= 20% before the (regime) timed exit
df["tp_hit"]   = (df["peak_ret_hold"] >= df["tp_pct_assumed"])
df["win_flag"] = (df["tp_hit"] | (df["peak_ret_hold"] >= 0.20)).fillna(False).astype(int)

df.to_csv(OUT, index=False)
print("Wrote ->", OUT)
print("n trades:", len(df), " | win-rate:", df["win_flag"].mean().round(3))
print(df.groupby("market_level")["win_flag"].mean().round(3))
