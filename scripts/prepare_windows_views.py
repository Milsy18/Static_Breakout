import argparse
from functools import reduce
from pathlib import Path
import re
import json
import pandas as pd
import numpy as np

def suffix_for_offset(o: int) -> str:
    return f"t{o:+d}".replace("+", "+")  # t-5, t-1, t0, t+1, ...

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="Data/Processed/labeled_holy_grail_static_221_windows_long.parquet")
    ap.add_argument("--offsets", type=int, nargs="+", default=[-5, -1, 0, 1, 2, 10])
    ap.add_argument("--outdir", default="Data/Processed")
    args = ap.parse_args()

    src = Path(args.src)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(src)
    # Ensure types
    if "entry_date" in df.columns:
        df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    if "exit_time" in df.columns:
        df["exit_time"] = pd.to_datetime(df["exit_time"], errors="coerce")
    if "bar_offset" in df.columns:
        df["bar_offset"] = pd.to_numeric(df["bar_offset"], errors="coerce").astype("Int32")

    # Metadata columns we want to keep (only those that actually exist)
    meta_wanted = [
        "symbol","entry_date","entry_price","market_level","bar_offset",
        "score_trd","score_vty","score_vol","score_mom","score_total","score_norm",
        "exit_time","exit_price","exit_reason","exit_label","success_bin"
    ]
    meta_cols = [c for c in meta_wanted if c in df.columns]

    # Feature detection: numeric columns that are not metadata
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    feature_cols = [c for c in numeric_cols if c not in meta_cols and c != "bar_offset"]

    # Persist feature list
    (outdir / "feature_columns.json").write_text(json.dumps(feature_cols, indent=2), encoding="utf-8")

    # --- Build wide table across selected offsets ---
    keys = ["symbol","entry_date"]
    for k in keys:
        if k not in df.columns:
            raise SystemExit(f"Missing key column '{k}' in {src}")

    frames = []
    for o in args.offsets:
        dfo = df.loc[df["bar_offset"] == o, keys + feature_cols].copy()
        # rename features with suffix by offset
        suff = suffix_for_offset(o)
        dfo = dfo.rename(columns={c: f"{c}_{suff}" for c in feature_cols})
        frames.append(dfo)

    # Inner-join across offsets so each row = one breakout with all offsets present
    wide = reduce(lambda l, r: pd.merge(l, r, on=keys, how="inner"), frames)

    # Attach labels/meta from t0 row
    base_cols = [c for c in meta_cols if c not in ["bar_offset"]]  # no need to carry bar_offset now
    base = df.loc[df["bar_offset"] == 0, keys + base_cols].drop_duplicates(keys)
    wide = pd.merge(wide, base, on=keys, how="left")

    # Sort columns: keys, labels/meta, then features grouped by offset
    def sort_key(c):
        if c in keys: return (0, c)
        if c in base_cols: return (1, c)
        m = re.search(r"_(t[+\-]?\d+)$", c)
        if m:
            s = m.group(1)  # e.g., t-1
            try:
                off = int(s.replace("t",""))
            except:
                off = 9999
            return (2, off, c)
        return (3, c)

    cols_sorted = sorted(wide.columns, key=sort_key)
    wide = wide[cols_sorted]

    # Save outputs
    out_parquet = outdir / "breakout_windows_features.parquet"
    wide.to_parquet(out_parquet, index=False)

    # Also drop a tiny sample CSV for eyeballing (first 100 rows, 200 cols max)
    sample_cols = cols_sorted[:min(200, len(cols_sorted))]
    out_sample = outdir / "breakout_windows_features_sample.csv"
    wide[sample_cols].head(100).to_csv(out_sample, index=False)

    print(f"✅ Built {out_parquet} with shape {wide.shape}")
    print(f"📝 Sample: {out_sample}")

if __name__ == "__main__":
    main()
