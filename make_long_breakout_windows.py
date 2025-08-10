import os
import pandas as pd

SRC = "Data/Processed/static_breakouts.csv"
OUT_DIR = "Data/Processed"
WINDOW_BARS = 221  # length of post-breakout window

os.makedirs(OUT_DIR, exist_ok=True)

# Load your base breakouts
df = pd.read_csv(SRC, parse_dates=["entry_date"])
print(f"Loaded {len(df)} breakouts from {SRC}")

# Group by symbol so we can slice each separately
for symbol, g in df.groupby("symbol"):
    out_fn = os.path.join(OUT_DIR, f"static_breakouts_long_{symbol}.csv")

    rows = []
    for _, row in g.iterrows():
        ent_dt = row["entry_date"]
        # load that symbol’s full OHLCV history from Filtered_OHLCV
        ohlcv_path = os.path.join("Data", "Filtered_OHLCV", f"{symbol}.csv")
        if not os.path.exists(ohlcv_path):
            print(f"⚠️ No OHLCV file for {symbol}, skipping.")
            continue
        hist = pd.read_csv(ohlcv_path, parse_dates=["date"])

        # find the entry bar
        ent_idx = hist.index[hist["date"] == ent_dt]
        if len(ent_idx) != 1:
            continue
        ent_idx = ent_idx[0]

        # slice entry bar and next N bars
        window = hist.iloc[ent_idx:ent_idx + WINDOW_BARS + 1].copy()
        # add a relative offset column (0 = entry bar)
        window["bar_offset"] = range(0, len(window))
        # join in the breakout’s metadata
        for col in row.index:
            if col not in window.columns:
                window[col] = row[col]

        rows.append(window)

    if rows:
        out_df = pd.concat(rows, ignore_index=True)
        out_df.to_csv(out_fn, index=False)
        print(f"✅ Wrote {out_fn} ({len(out_df)} rows)")
