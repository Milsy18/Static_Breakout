import argparse
import pandas as pd
from pathlib import Path

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--offsets", nargs="+", type=int, required=True)
    p.add_argument("--outdir", required=True)
    return p.parse_args()

def main():
    a = parse_args()
    outdir = Path(a.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(a.src)

    # Clean offsets and drop the NaT-like row
    df["bar_offset"] = pd.to_numeric(df["bar_offset"], errors="coerce")
    df = df.dropna(subset=["bar_offset"]).copy()
    df["bar_offset"] = df["bar_offset"].astype("int64")

    keys = ["symbol", "entry_date"]
    for k in keys:
        if k not in df.columns:
            raise SystemExit(f"Required key column missing: {k}")

    # Limit to requested offsets
    df = df[df["bar_offset"].isin(set(a.offsets))].copy()

    # De-dup
    df = (df.sort_values(keys + ["bar_offset"])
            .drop_duplicates(subset=keys + ["bar_offset"]))

    # Candidate features = numeric columns excluding non-features
    exclude = set(keys + ["index", "bar_offset",
                          "exit_time", "exit_price", "exit_reason",
                          "exit_label", "success_bin"])
    # Try to coerce numerics broadly
    candidates = [c for c in df.columns if c not in exclude]
    df[candidates] = df[candidates].apply(pd.to_numeric, errors="ignore")
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    feats = [c for c in numeric_cols if c not in exclude]

    # Fallback if nothing detected
    if not feats:
        fallback = ["score_trd","score_vty","score_vol","score_mom",
                    "score_total","score_norm","market_level"]
        feats = [c for c in fallback if c in df.columns]
        if not feats:
            raise SystemExit("No feature columns found after filtering.")

    # Pivot wide per offset
    wide = (df.set_index(keys + ["bar_offset"])[feats]
              .unstack("bar_offset"))
    wide.columns = [f"{feat}@t{int(off)}" for feat, off in wide.columns]
    wide = wide.reset_index()

    p_parq = outdir / "breakout_windows_features.parquet"
    p_csv  = outdir / "breakout_windows_features_sample.csv"
    wide.to_parquet(p_parq, index=False)
    wide.head(50).to_csv(p_csv, index=False)
    print(f"✅ Built {p_parq} with shape {tuple(wide.shape)}")
    print(f"📝 Sample: {p_csv}")

if __name__ == "__main__":
    main()
