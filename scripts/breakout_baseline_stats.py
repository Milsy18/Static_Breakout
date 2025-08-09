import os
import pandas as pd
from pathlib import Path

IN = Path("Data/Processed/master_breakouts_all.csv")
OUTDIR = Path("Data/Processed/baseline_stats")
OUTDIR.mkdir(parents=True, exist_ok=True)

def load_frame(path: Path) -> pd.DataFrame:
    # Be liberal with date parsing; support either exit_time or exit_date
    parse_cols = [c for c in ["entry_date","exit_time","exit_date"] if c in pd.read_csv(path, nrows=0).columns]
    df = pd.read_csv(path, parse_dates=parse_cols)
    # Standardize exit column name
    if "exit_time" in df.columns:
        df = df.rename(columns={"exit_time":"exit_dt"})
    elif "exit_date" in df.columns:
        df = df.rename(columns={"exit_date":"exit_dt"})
    else:
        raise KeyError("No exit_time/exit_date column found.")

    # Required columns sanity
    needed = {"symbol","entry_date","entry_price","exit_price","market_level","exit_reason","exit_dt"}
    missing = needed - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # Types & features
    df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")
    df["ret_pct"] = (df["exit_price"] / df["entry_price"] - 1.0) * 100.0
    df["hold_days"] = (df["exit_dt"] - df["entry_date"]).dt.days
    df["win"] = df["ret_pct"] > 0
    df["exit_reason"] = df["exit_reason"].fillna("UNKNOWN").astype(str)
    return df

def summarize(df: pd.DataFrame, by=None) -> pd.DataFrame:
    grp = df if by is None else df.groupby(by, dropna=False)
    s = grp.agg(
        trades=("symbol","size"),
        win_rate=("win","mean"),
        avg_ret_pct=("ret_pct","mean"),
        med_ret_pct=("ret_pct","median"),
        med_hold_days=("hold_days","median")
    ).reset_index()
    # nicer formatting for printing; keep raw in files
    return s

def main():
    df = load_frame(IN)

    overall = summarize(df)
    by_lvl  = summarize(df, by=["market_level"])
    by_reason = summarize(df, by=["exit_reason"])
    by_lvl_reason = summarize(df, by=["market_level","exit_reason"]).sort_values(["market_level","exit_reason"])

    # write outputs
    overall.to_csv(OUTDIR / "overall.csv", index=False)
    by_lvl.to_csv(OUTDIR / "by_level.csv", index=False)
    by_reason.to_csv(OUTDIR / "by_reason.csv", index=False)
    by_lvl_reason.to_csv(OUTDIR / "by_level_reason.csv", index=False)

    # quick console preview
    print("\n=== Overall ===")
    print(overall.to_string(index=False, formatters={"win_rate":lambda x:f"{x*100:0.2f}%","avg_ret_pct":"{:.2f}".format,"med_ret_pct":"{:.2f}".format}))
    print("\n=== By Market Level (head) ===")
    head = by_lvl.sort_values("market_level").head(12)
    print(head.to_string(index=False, formatters={"win_rate":lambda x:f"{x*100:0.2f}%","avg_ret_pct":"{:.2f}".format,"med_ret_pct":"{:.2f}".format}))
    print("\n=== By Exit Reason ===")
    print(by_reason.to_string(index=False, formatters={"win_rate":lambda x:f"{x*100:0.2f}%","avg_ret_pct":"{:.2f}".format,"med_ret_pct":"{:.2f}".format}))

if __name__ == "__main__":
    main()
