import pandas as pd
import numpy as np
from pathlib import Path

IN = Path("Data/Processed/master_breakouts_all.csv")
OUTDIR = Path("Data/Processed/baseline_stats")
OUTDIR.mkdir(parents=True, exist_ok=True)

def load():
    # be flexible with date column name
    head = pd.read_csv(IN, nrows=0)
    parse = [c for c in ["entry_date","exit_time","exit_date"] if c in head.columns]
    df = pd.read_csv(IN, parse_dates=parse)
    if "exit_time" in df.columns: df = df.rename(columns={"exit_time":"exit_dt"})
    if "exit_date" in df.columns: df = df.rename(columns={"exit_date":"exit_dt"})
    for c in ("entry_price","exit_price","market_level"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ret_pct"] = (df["exit_price"] / df["entry_price"] - 1.0) * 100.0
    df["win"] = df["ret_pct"] > 0
    df["hold_days"] = (df["exit_dt"] - df["entry_date"]).dt.days
    df["exit_reason"] = df["exit_reason"].astype(str).fillna("UNKNOWN")
    return df

def summarize(df, by=None):
    grp = df if by is None else df.groupby(by, dropna=False)
    return (
        grp.agg(trades=("symbol","size"),
                win_rate=("win","mean"),
                avg_ret_pct=("ret_pct","mean"),
                med_ret_pct=("ret_pct","median"),
                med_hold_days=("hold_days","median"))
          .reset_index()
    )

def main():
    df = load()

    # 1) Quantiles for robust trimming (empirical, no hard-coded cutoffs)
    q = df["ret_pct"].quantile([0.001, 0.01, 0.99, 0.999]).to_dict()
    q001, q01, q99, q999 = q[0.001], q[0.01], q[0.99], q[0.999]

    # 2) Flag obviously suspect rows for inspection
    bad = (
        df["entry_price"].le(0) | df["exit_price"].le(0) |
        df["ret_pct"].lt(q001) | df["ret_pct"].gt(q999)
    )
    sus = df.loc[bad, ["symbol","entry_date","exit_dt","market_level","exit_reason",
                       "entry_price","exit_price","ret_pct","hold_days"]].copy()
    sus.to_csv(OUTDIR / "suspect_rows.csv", index=False)

    # 3) Trim for robust means (keep middle 98% by default)
    trimmed = df[(df["ret_pct"].ge(q01)) & (df["ret_pct"].le(q99))].copy()

    # 4) Summaries — raw vs trimmed
    overall_raw     = summarize(df)
    bylvl_raw       = summarize(df, by=["market_level"]).sort_values("market_level")
    byreason_raw    = summarize(df, by=["exit_reason"]).sort_values("exit_reason")

    overall_trim    = summarize(trimmed)
    bylvl_trim      = summarize(trimmed, by=["market_level"]).sort_values("market_level")
    byreason_trim   = summarize(trimmed, by=["exit_reason"]).sort_values("exit_reason")

    # Save everything
    overall_raw.to_csv(OUTDIR / "overall_raw.csv", index=False)
    bylvl_raw.to_csv(OUTDIR / "by_level_raw.csv", index=False)
    byreason_raw.to_csv(OUTDIR / "by_reason_raw.csv", index=False)

    overall_trim.to_csv(OUTDIR / "overall_trim01_99.csv", index=False)
    bylvl_trim.to_csv(OUTDIR / "by_level_trim01_99.csv", index=False)
    byreason_trim.to_csv(OUTDIR / "by_reason_trim01_99.csv", index=False)

    # Console preview
    fmt = {"win_rate": lambda x: f"{x*100:0.2f}%", "avg_ret_pct":"{:.2f}".format, "med_ret_pct":"{:.2f}".format}
    print("\n=== Quantiles (ret_pct) ===")
    print({k: round(v,2) for k,v in q.items()})
    print("\n=== Overall (RAW) ===")
    print(overall_raw.to_string(index=False, formatters=fmt))
    print("\n=== Overall (TRIM 1–99%) ===")
    print(overall_trim.to_string(index=False, formatters=fmt))
    print("\n=== By Exit Reason (RAW) ===")
    print(byreason_raw.to_string(index=False, formatters=fmt))
    print("\n=== By Exit Reason (TRIM 1–99%) ===")
    print(byreason_trim.to_string(index=False, formatters=fmt))
    print("\n• Wrote suspect_rows.csv and all summaries in", OUTDIR)

if __name__ == "__main__":
    main()
