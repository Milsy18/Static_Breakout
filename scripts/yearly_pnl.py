#!/usr/bin/env python3
"""
Yearly P&L summary for M18 breakouts (overall and per market_level).

Usage examples (from repo root):
  .\.venv\Scripts\python.exe .\scripts\yearly_pnl.py --src Data\Processed\master_breakouts_all.csv --trim 1 99
  .\.venv\Scripts\python.exe .\scripts\yearly_pnl.py --src Data\Processed\master_breakouts_all.csv --trim 1 99 --per-level
"""

from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd


# ---------- IO & cleaning ----------

def load(src: str | Path) -> pd.DataFrame:
    """Load a breakout ledger and normalize columns we need."""
    src = Path(src)
    if not src.exists():
        raise FileNotFoundError(f"File not found: {src}")

    # read raw
    df = pd.read_csv(src)
    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # standardize date columns
    # accept any of: entry_date/entry_dt and exit_date/exit_dt/exit_time
    entry_col = next((c for c in ("entry_date", "entry_dt") if c in df.columns), None)
    exit_col  = next((c for c in ("exit_dt", "exit_date", "exit_time") if c in df.columns), None)
    if entry_col is None or exit_col is None:
        have = ", ".join(df.columns)
        raise ValueError(f"Missing entry/exit datetime columns. Have: [{have}]")

    df["entry_date"] = pd.to_datetime(df[entry_col], errors="coerce", utc=True)
    df["exit_dt"]    = pd.to_datetime(df[exit_col],  errors="coerce", utc=True)

    # price columns
    entry_price_col = next((c for c in ("entry_price", "open_price", "entry_close") if c in df.columns), None)
    exit_price_col  = next((c for c in ("exit_price", "close_price", "exit_close") if c in df.columns), None)

    # ret_pct: prefer existing; else compute if prices present
    if "ret_pct" in df.columns:
        df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
    elif entry_price_col and exit_price_col:
        ep = pd.to_numeric(df[entry_price_col], errors="coerce")
        xp = pd.to_numeric(df[exit_price_col],  errors="coerce")
        df["ret_pct"] = (xp - ep) / ep * 100.0
    else:
        raise ValueError("Neither ret_pct nor (entry_price + exit_price) present to compute returns.")

    # hold_days: prefer existing; else compute from dates
    if "hold_days" in df.columns:
        df["hold_days"] = pd.to_numeric(df["hold_days"], errors="coerce")
    else:
        df["hold_days"] = (df["exit_dt"] - df["entry_date"]).dt.days

    # market_level optional (only needed for --per-level)
    if "market_level" in df.columns:
        df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")

    # exit_reason optional
    if "exit_reason" not in df.columns:
        df["exit_reason"] = pd.NA
    else:
        df["exit_reason"] = df["exit_reason"].astype("string").str.upper().str.strip()

    # helper fields
    df["year"] = df["entry_date"].dt.year.astype("Int64")
    df["win"]  = (df["ret_pct"] > 0).astype(int)

    # drop rows with invalid core fields
    df = df.dropna(subset=["entry_date", "exit_dt", "ret_pct", "hold_days", "year"])

    return df


def trim_by_percentile(df: pd.DataFrame, low: float, high: float) -> pd.DataFrame:
    """Keep rows whose ret_pct is between low/high percentiles (inclusive)."""
    if not (0 <= low < high <= 100):
        raise ValueError("trim bounds must satisfy: 0 <= low < high <= 100")
    lo = np.percentile(df["ret_pct"].dropna(), low)
    hi = np.percentile(df["ret_pct"].dropna(), high)
    return df[(df["ret_pct"] >= lo) & (df["ret_pct"] <= hi)].copy()


# ---------- aggregation ----------

def summarize(df: pd.DataFrame, capital_per_trade: float = 1000.0) -> pd.DataFrame:
    """Yearly summary across all market levels."""
    # named aggregation dict (used with ** below)
    agg = {
        "trades":       ("ret_pct", "size"),
        "win_rate":     ("win", "mean"),
        "avg_ret_pct":  ("ret_pct", "mean"),
        "med_ret_pct":  ("ret_pct", "median"),
        "med_hold_days":("hold_days", "median"),
        "tp_share":     ("exit_reason", lambda s: float(np.mean(s == "TP"))),
        "rsi_share":    ("exit_reason", lambda s: float(np.mean(s == "RSI"))),
        "time_share":   ("exit_reason", lambda s: float(np.mean(s == "TIME"))),
        "total_gain_$": ("ret_pct",   lambda s: float(capital_per_trade * s.sum() / 100.0)),
    }
    out = (
        df.groupby("year", dropna=False)
          .agg(**agg)                    # <-- important fix: expand named-agg dict
          .reset_index()
          .sort_values("year")
    )
    # nice formatting
    for c in ("win_rate", "tp_share", "rsi_share", "time_share"):
        out[c] = (out[c] * 100.0).round(2)
    for c in ("avg_ret_pct", "med_ret_pct"):
        out[c] = out[c].round(2)
    out["med_hold_days"] = out["med_hold_days"].round(1)
    out["total_gain_$"]  = out["total_gain_$"].round(2)
    return out


def summarize_per_level(df: pd.DataFrame, capital_per_trade: float = 1000.0) -> pd.DataFrame:
    """Yearly summary split by market_level."""
    if "market_level" not in df.columns:
        raise ValueError("market_level column is required for --per-level")

    agg = {
        "trades":       ("ret_pct", "size"),
        "win_rate":     ("win", "mean"),
        "avg_ret_pct":  ("ret_pct", "mean"),
        "med_ret_pct":  ("ret_pct", "median"),
        "med_hold_days":("hold_days", "median"),
        "tp_share":     ("exit_reason", lambda s: float(np.mean(s == "TP"))),
        "rsi_share":    ("exit_reason", lambda s: float(np.mean(s == "RSI"))),
        "time_share":   ("exit_reason", lambda s: float(np.mean(s == "TIME"))),
        "total_gain_$": ("ret_pct",   lambda s: float(capital_per_trade * s.sum() / 100.0)),
    }
    out = (
        df.groupby(["year", "market_level"], dropna=False)
          .agg(**agg)                    # <-- important fix
          .reset_index()
          .sort_values(["year", "market_level"])
    )
    for c in ("win_rate", "tp_share", "rsi_share", "time_share"):
        out[c] = (out[c] * 100.0).round(2)
    for c in ("avg_ret_pct", "med_ret_pct"):
        out[c] = out[c].round(2)
    out["med_hold_days"] = out["med_hold_days"].round(1)
    out["total_gain_$"]  = out["total_gain_$"].round(2)
    return out


# ---------- CLI ----------

def main():
    p = argparse.ArgumentParser(description="Yearly P&L summary for M18 breakouts.")
    p.add_argument("--src", default="Data/Processed/master_breakouts_all.csv",
                   help="CSV to summarize (aligned schema).")
    p.add_argument("--trim", nargs=2, type=float, metavar=("LOW","HIGH"),
                   help="Percentile bounds to keep (e.g., --trim 1 99).")
    p.add_argument("--per-level", action="store_true",
                   help="Also print/save per-market-level yearly summary.")
    p.add_argument("--capital", type=float, default=1000.0,
                   help="Capital per trade in dollars for total_gain_$ (default: 1000).")
    args = p.parse_args()

    df = load(args.src)

    # optional trimming
    if args.trim:
        low, high = args.trim
        df_t = trim_by_percentile(df, low, high)
    else:
        df_t = df

    # summaries
    res = summarize(df_t, capital_per_trade=args.capital)
    print("\n=== Yearly P&L (overall) ===")
    print(res.to_string(index=False))

    out_dir = Path("Data/Processed/yearly_pnl")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_overall = out_dir / f"yearly_pnl_overall{'_trim' if args.trim else ''}.csv"
    res.to_csv(out_overall, index=False)

    if args.per_level:
        res_pl = summarize_per_level(df_t, capital_per_trade=args.capital)
        print("\n=== Yearly P&L (per market_level) ===")
        # widen a bit for readability
        with pd.option_context("display.max_rows", 200, "display.width", 200):
            print(res_pl.to_string(index=False))
        out_pl = out_dir / f"yearly_pnl_per_level{'_trim' if args.trim else ''}.csv"
        res_pl.to_csv(out_pl, index=False)

    print(f"\nWrote: {out_overall}")
    if args.per_level:
        print(f"Wrote: {out_pl}")


if __name__ == "__main__":
    main()
