#!/usr/bin/env python3
import argparse
import pandas as pd
from pathlib import Path

EXIT_CANDIDATES = ["exit_dt", "exit_date", "exit_time"]

def load(src: str) -> pd.DataFrame:
    # read once, then normalize/parse
    df = pd.read_csv(src)

    # normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    # find an exit-date column we can use
    exit_col = next((c for c in EXIT_CANDIDATES if c in df.columns), None)
    if exit_col is None:
        raise ValueError(f"No exit-date column found. Expected one of {EXIT_CANDIDATES}, got {list(df.columns)}")

    # parse dates
    if "entry_date" in df.columns:
        df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce", utc=True)
    df[exit_col] = pd.to_datetime(df[exit_col], errors="coerce", utc=True)

    # unify name to 'exit_dt'
    if exit_col != "exit_dt":
        df = df.rename(columns={exit_col: "exit_dt"})

    # numeric coercions
    for c in ("ret_pct", "entry_price", "exit_price", "market_level", "hold_days"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # backfill hold_days if missing
    if "hold_days" not in df.columns or df["hold_days"].isna().all():
        if "entry_date" in df.columns:
            df["hold_days"] = (df["exit_dt"] - df["entry_date"]).dt.days

    return df

def trim_df(df: pd.DataFrame, lo: float | None, hi: float | None):
    if lo is None or hi is None:
        return df, None
    qlo, qhi = df["ret_pct"].quantile([lo/100.0, hi/100.0])
    mask = (df["ret_pct"] >= qlo) & (df["ret_pct"] <= qhi)
    return df.loc[mask].copy(), (float(qlo), float(qhi))

def summarize(df: pd.DataFrame) -> pd.DataFrame:
    agg = {
        "trades":        ("ret_pct", "size"),
        "win_rate":      ("ret_pct", lambda s: (s > 0).mean()),
        "avg_ret_pct":   ("ret_pct", "mean"),
        "med_ret_pct":   ("ret_pct", "median"),
        "med_hold_days": ("hold_days", "median"),
        "tp_share":      ("exit_reason",  lambda s: (s == "TP").mean()),
        "time_share":    ("exit_reason",  lambda s: (s == "TIME").mean()),
        "rsi_share":     ("exit_reason",  lambda s: (s == "RSI").mean()),
        "total_gain_$":  ("ret_pct",     lambda s: 1000.0 * s.sum() / 100.0),
    }
    out = df.groupby("year", dropna=False).agg(agg).reset_index().sort_values("year")
    out["roi_on_capital"] = out["total_gain_$"] / (1000.0 * out["trades"])
    return out

def summarize_per_level(df: pd.DataFrame) -> pd.DataFrame:
    agg = {
        "trades":        ("ret_pct", "size"),
        "win_rate":      ("ret_pct", lambda s: (s > 0).mean()),
        "avg_ret_pct":   ("ret_pct", "mean"),
        "med_ret_pct":   ("ret_pct", "median"),
        "med_hold_days": ("hold_days", "median"),
        "tp_share":      ("exit_reason",  lambda s: (s == "TP").mean()),
        "time_share":    ("exit_reason",  lambda s: (s == "TIME").mean()),
        "rsi_share":     ("exit_reason",  lambda s: (s == "RSI").mean()),
        "total_gain_$":  ("ret_pct",     lambda s: 1000.0 * s.sum() / 100.0),
    }
    out = (
        df.groupby(["year", "market_level"], dropna=False)
          .agg(agg).reset_index().sort_values(["year","market_level"])
    )
    out["roi_on_capital"] = out["total_gain_$"] / (1000.0 * out["trades"])
    return out

def main():
    ap = argparse.ArgumentParser(description="Yearly P&L for $1k-per-signal.")
    ap.add_argument("--src", default="Data/Processed/master_breakouts_all.csv")
    ap.add_argument("--trim", nargs=2, type=float, metavar=("LO","HI"),
                    help="Percentile trim, e.g. --trim 1 99 (omit for raw).")
    ap.add_argument("--per-level", action="store_true", help="Break out by market_level.")
    args = ap.parse_args()

    df = load(args.src)
    if "entry_date" not in df.columns:
        raise ValueError("Missing 'entry_date' after load().")

    df["year"] = df["entry_date"].dt.year
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
        print(f"Applied two-sided trim on ret_pct at approx {q[0]:.2f}% / {q[1]:.2f}%.")

if __name__ == "__main__":
    main()
