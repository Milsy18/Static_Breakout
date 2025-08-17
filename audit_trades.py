import re
import pandas as pd
from pathlib import Path

# === CONFIG ===
CSV_PATH = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_events_timed_full.csv"

# Assumption from your last tp_scan_by_level printout
TP_BY_LEVEL = {
    1: 0.65, 2: 0.85, 3: 0.90, 4: 0.85,
    5: 0.95, 6: 0.90, 7: 0.95, 8: 0.95, 9: 0.95
}

# === LOAD ===
df = pd.read_csv(CSV_PATH)

# Ensure columns we’ll use exist
req_cols = ["symbol","breakout_date","market_level","start_price","max_window_price","max_gain","duration_days"]
missing = [c for c in req_cols if c not in df.columns]
if missing:
    raise SystemExit(f"Missing required columns: {missing}")

# Identify forward-high columns (high_d0..high_dN), sorted by day
high_cols = sorted([c for c in df.columns if re.fullmatch(r"high_d\d+", c)], key=lambda x: int(x.split("_d")[1]))

def audit_row(row):
    sym = row["symbol"]
    bdate = row["breakout_date"]
    lvl = int(row["market_level"])
    start = float(row["start_price"])
    tp_pct = TP_BY_LEVEL.get(lvl, None)
    max_gain = float(row["max_gain"])
    dur = row.get("duration_days", float("nan"))

    # Compute forward returns from highs if available
    fwd_highs = [row[c] for c in high_cols if pd.notna(row[c])]
    fwd_ret = [(h/start - 1.0) for h in fwd_highs] if fwd_highs else []
    if fwd_ret:
        peak_ret = max(fwd_ret)
        peak_day = fwd_ret.index(peak_ret)
    else:
        peak_ret, peak_day = float("nan"), None

    # TP hit check (within available window)
    tp_hit = (tp_pct is not None) and (max_gain >= tp_pct)

    # Print concise audit
    print("="*88)
    print(f"{sym} | breakout: {bdate} | lvl {lvl} | start={start:.6f}")
    print(f"tp% (assumed by lvl): {tp_pct if tp_pct is not None else 'N/A'} | max_gain={max_gain:.4f} | tp_hit={tp_hit}")
    print(f"duration_days={dur} | peak_ret_from_highs={peak_ret:.4f} @ day {peak_day}")
    # Show first 10 forward returns for quick visual
    if fwd_ret:
        preview = ", ".join([f"{r:.3f}" for r in fwd_ret[:10]])
        print(f"first 10 fwd_high returns: [{preview}]")
    else:
        print("No forward highs found in columns.")
    print("="*88)

# Sample 3 trades for audit (stratify a bit by level if possible)
sample = df.sample(n=min(3, len(df)), random_state=42)
for _, row in sample.iterrows():
    audit_row(row)
