# diag_levels.py  — deep-dive why market_level == 5 everywhere

import pandas as pd
from pathlib import Path

from modules.market_level import compute_market_level
from modules.breakout_detector import detect_breakouts

ROOT = Path(".")
P_IND = ROOT / "Data" / "Processed" / "per_bar_indicators_core.csv"
P_MAC = ROOT / "Data" / "Raw" / "macro_regime_data.csv"
P_BRK = ROOT / "Data" / "Processed" / "static_breakouts.csv"

def to_day(s):
    s = pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s.dt.date)

def main():
    print("\n=== 1) Load macro & compute market levels ===")
    mac_raw = pd.read_csv(P_MAC)
    ml = compute_market_level(mac_raw).copy()
    ml["date"] = to_day(ml["date"])
    print("macro rows:", len(ml), "| date range:", ml["date"].min(), "->", ml["date"].max())
    print("macro market_level spread:\n", ml["market_level"].value_counts().sort_index(), "\n")

    print("=== 2) Load indicators & join on calendar day ===")
    ind = pd.read_csv(P_IND, usecols=["date","symbol","close"])
    ind["date"] = to_day(ind["date"])
    print("ind rows:", len(ind), "| date range:", ind["date"].min(), "->", ind["date"].max())
    merged = ind.merge(ml, on="date", how="left")
    match_rate = float(merged["market_level"].notna().mean())
    print(f"join match % (calendar-day): {match_rate:.2%}")
    if match_rate < 0.99:
        # show a few problematic dates
        samp = merged.loc[merged["market_level"].isna(), "date"].drop_duplicates().head(10)
        print("sample unmapped dates:", samp.tolist())

    # write a small sample for eyeballing
    merged.head(50).to_csv(ROOT/"Data/Processed/_diag_join_sample.csv", index=False)
    print("wrote Data/Processed/_diag_join_sample.csv")

    print("\n=== 3) Run a small detect_breakouts sample (first 1 symbol) ===")
    one = ind[ind["symbol"] == ind["symbol"].iloc[0]].copy()
    out_small = detect_breakouts(one, mac_raw, static_adj=0.0, std_mult=0.5, lookback=100)
    if len(out_small):
        print("small run market_level spread:\n", out_small["market_level"].value_counts().sort_index())
    else:
        print("small run produced 0 entries")

    print("\n=== 4) If static_breakouts.csv exists, check its spread ===")
    if P_BRK.exists():
        d = pd.read_csv(P_BRK)
        if "market_level" in d.columns and len(d):
            print("static_breakouts market_level spread:\n", d["market_level"].value_counts().sort_index())
        else:
            print("static_breakouts.csv has no rows/column?")

if __name__ == "__main__":
    main()
