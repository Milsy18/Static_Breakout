#!/usr/bin/env python3
import argparse, os
import pandas as pd
from pathlib import Path

def load(src):
    df = pd.read_csv(src, parse_dates=["entry_date","exit_dt"], infer_datetime_format=True)
    # expected columns after align: symbol, entry_date, exit_dt, market_level, exit_reason,
    # entry_price, exit_price, ret_pct, hold_days, source
    need = {"symbol","entry_date","exit_dt","market_level","exit_reason","entry_price","exit_price","ret_pct"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {src}: {missing}")
    df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
    df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["entry_date","ret_pct"]).copy()
    df["year"] = df["entry_date"].dt.year
    return df

def trim_df(df, lo, hi):
    if lo is None or hi is None:
        return df, None
    qlo, qhi = df["ret_pct"].quantile([lo/100.0, hi/100.0])
    mask = (df["ret_pct"] >= qlo) & (df["ret_pct"] <= qhi)
    return df.loc[mask].copy(), (float(qlo), float(qhi))

def summarize(df):
    grp = df.groupby("year", dropna=False)
    out = grp.agg(
        trades=("ret_pct","size"),
        win_rate=("ret_pct", lambda s: (s>0).mean()),
        avg_ret_pct=("ret_pct","mean"),
        med_ret_pct=("ret_pct","median"),
        med_hold_days=("hold_days","median"),
        tp_share=("exit_reason", lambda s: (s=="TP").mean()),
        time_share=("exit_reason", lambda s: (s=="TIME").mean()),
        rsi_share=("exit_reason", lambda s: (s=="RSI").mean()),
    ).reset_index()
    # $1,000 per signal, no compounding, no position sizing
    out["total_gain_$"] = 1000.0 * (df.groupby("year")["ret_pct"].sum()/100.0).values
    out["roi_on_capital"] = out["total_gain_$"] / (1000.0*out["trades"])
    return out.sort_values("year")

def summarize_per_level(df):
    grp = df.groupby(["year","market_level"], dropna=False)
    out = grp.agg(
        trades=("ret_pct","size"),
        win_rate=("ret_pct", lambda s: (s>0).mean()),
        avg_ret_pct=("ret_pct","mean"),
        med_ret_pct=("ret_pct","median"),
        med_hold_days=("hold_days","median"),
        tp_share=("exit_reason", lambda s: (s=="TP").mean()),
        time_share=("exit_reason", lambda s: (s=="TIME").mean()),
        rsi_share=("exit_reason", lambda s: (s=="RSI").mean()),
        total_gain_$=("ret_pct", lambda s: 1000.0*s.sum()/100.0),
    ).reset_index().sort_values(["year","market_level"])
    out["roi_on_capital"] = out["total_gain_$"] / (1000.0*out["trades"])
    return out

def main():
    ap = argparse.ArgumentParser(description="Yearly P&L for $1k-per-signal.")
    ap.add_argument("--src", default="Data/Processed/master_breakouts_all.csv")
    ap.add_argument("--trim", nargs=2, type=float, metavar=("LO","HI"),
                    help="Percentile trim, e.g. --trim 1 99 (omit to use raw).")
    ap.add_argument("--per-level", action="store_true", help="Break out by market_level.")
    args = ap.parse_args()

    df = load(args.src)
    df_t, q = trim_df(df, *(args.trim or (None, None)))

    out_dir = Path("Data/Processed/yearly_pnl"); out_dir.mkdir(parents=True, exist_ok=True)

    label = "raw" if q is None else f"trim{int(args.trim[0])}_{int(args.trim[1])}"
    if args.per_level:
        res = summarize_per_level(df_t)
        out_path = out_dir / f"yearly_pnl_per_level_{label}.csv"
    else:
        res = summarize(df_t)
        out_path = out_dir / f"yearly_pnl_{label}.csv"

    res.to_csv(out_path, index=False)

    print(f"\n=== Yearly P&L ({label}) ===")
    print(res.to_string(index=False))
    print(f"\nWrote {out_path}")
    if q is not None:
        print(f"Applied two-sided trim on ret_pct at {q[0]:.2f}% / {q[1]:.2f}% (approx).")

if __name__ == "__main__":
    main()
