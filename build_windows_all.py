#!/usr/bin/env python

import pandas as pd
import glob
import os

def main():
    # — 1. find all per‐symbol window files in Data/Processed —
    pattern = os.path.join("Data", "Processed", "static_breakouts_long_*.csv")
    files = glob.glob(pattern)
    print(f"Found {len(files)} files to merge…")
    if not files:
        raise FileNotFoundError(f"No files matched {pattern}")

    # — 2. read & concat —
    df_list = []
    for fn in files:
        # only parse entry_date here
        df = pd.read_csv(fn, parse_dates=["entry_date"])
        # if exit_date is present, convert to datetime
        if "exit_date" in df.columns:
            df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")
        df_list.append(df)

    df_long = pd.concat(df_list, ignore_index=True)
    print(f"Total rows in merged long table: {len(df_long)}")

    # — 3. write out the combined long‐windows file —
    long_out = "holy_grail_static_221_windows_long.csv"
    df_long.to_csv(long_out, index=False)
    print(f"Wrote {long_out}")

    # — 4. pivot into wide “–5…+5” snapshot format —
    idx_cols = ["symbol", "entry_date", "exit_date", "exit_reason", "return_pct", "market_level"]
    pivot_cols = [c for c in df_long.columns if c not in idx_cols + ["bar_offset"]]

    df_wide = (
        df_long
        .set_index(idx_cols + ["bar_offset"])[pivot_cols]
        .unstack(level="bar_offset")
    )

    # flatten MultiIndex columns: e.g. ('rsi', -3) → 'rsi_m3'; ('mom', +2) → 'mom_p2'
    df_wide.columns = [
        f"{indicator}_{'p' if offset>0 else 'm'}{abs(int(offset))}"
        for indicator, offset in df_wide.columns
    ]

    df_wide = df_wide.reset_index()

    wide_out = "holy_grail_static_221_windows_wide.csv"
    df_wide.to_csv(wide_out, index=False)
    print(f"Wrote {wide_out}")

if __name__ == "__main__":
    main()


