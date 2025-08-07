#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def main():
    BASE_PARQUET = Path("holy_grail_static_221_dataset.parquet")
    WIN_PARQUET  = Path("labeled_holy_grail_static_221_windows_long.parquet")
    OUT_PARQUET  = Path("holy_grail_static_221_long_only.parquet")

    # 1) Load your base dataset
    print(f"Loading base dataset from {BASE_PARQUET}…")
    df_base = pd.read_parquet(BASE_PARQUET)

    # If the converter put 'index' in as a column, make it our index now:
    if "index" in df_base.columns:
        df_base = df_base.set_index("index")

    # 2) Load your labeled windows data
    print(f"Loading labeled windows from {WIN_PARQUET}…")
    df_win = pd.read_parquet(WIN_PARQUET)

    # Likewise, ensure that windows table is indexed by that same 'index'
    if "index" in df_win.columns:
        df_win = df_win.set_index("index")

    # 3) If you need a separate success_bin column, mirror exit_label → success_bin
    #    (most people use exit_label itself as the 0/1 outcome)
    if "exit_label" in df_win.columns and "success_bin" not in df_win.columns:
        df_win["success_bin"] = df_win["exit_label"]

    # 4) Join only the columns you actually have
    to_join = [c for c in ("exit_label", "success_bin") if c in df_win.columns]
    print(f"Joining {', '.join(to_join)} onto base dataset…")
    df_full = df_base.join(df_win[to_join], how="left")

    # 5) Reset the index back into a column so nothing mysteriously disappears
    df_full = df_full.reset_index()

    # 6) Write out your final long-only Parquet
    print(f"Writing merged Parquet to {OUT_PARQUET}…")
    df_full.to_parquet(OUT_PARQUET, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()


