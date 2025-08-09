#!/usr/bin/env python3
"""
Empirical expectancy grid + out-of-sample check for M18 breakouts.

Example:
  python scripts/expectancy_grid.py --src Data/Processed/master_breakouts_all.csv --trim 1 99 --train-years 2020 2023 --eval-years 2024 2025 --min-trades 300 --mask-scope level
"""

from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

EXIT_CANDIDATES = ["exit_dt", "exit_date", "exit_time"]

# ---------------- IO / cleaning ----------------

def load_aligned(src: str | Path) -> pd.DataFrame:
    p = Path(src)
    if not p.exists():
        raise FileNotFoundError(p)
    df = pd.read_csv(p)
    df.columns = [str(c).strip() for c in df.columns]

    # dates
    entry_col = "entry_date" if "entry_date" in df.columns else None
    exit_col = next((c for c in EXIT_CANDIDATES if c in df.columns), None)
    if entry_col is None or exit_col is None:
        raise ValueError(f"Need entry_date and one of {EXIT_CANDIDATES}. Have: {list(df.columns)}")

    df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce", utc=True)
    df["exit_dt"] = pd.to_datetime(df[exit_col], errors="coerce", utc=True)
    if "exit_dt" not in df.columns:
        df = df.rename(columns={exit_col: "exit_dt"})

    # returns
    if "ret_pct" in df.columns:
        df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
    else:
        ep = pd.to_numeric(df.get("entry_price"), errors="coerce")
        xp = pd.to_numeric(df.get("exit_price"), errors="coerce")
        if ep is None or xp is None:
            raise ValueError("ret_pct missing and cannot compute from prices.")
        df["ret_pct"] = (xp - ep) / ep * 100.0

    # hold days
    if "hold_days" in df.columns:
        df["hold_days"] = pd.to_numeric(df["hold_days"], errors="coerce")
    else:
        df["hold_days"] = (df["exit_dt"] - df["entry_date"]).dt.days

    # regime + reason
    if "market_level" in df.columns:
        df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")
    else:
        df["market_level"] = pd.NA
    df["exit_reason"] = df.get("exit_reason", pd.Series(index=df.index, dtype="string")).astype("string").str.upper().str.strip()

    # derived
    df["year"] = df["entry_date"].dt.year.astype("Int64")
    df = df.dropna(subset=["entry_date", "exit_dt", "ret_pct", "hold_days", "year"]).copy()
    df["win"] = (df["ret_pct"] > 0).astype(int)
    return df

def trim_by_global(df: pd.DataFrame, low: float, high: float) -> tuple[pd.DataFrame, tuple[float,float]]:
    qlo, qhi = df["ret_pct"].quantile([low/100.0, high/100.0])
    mask = (df["ret_pct"] >= qlo) & (df["ret_pct"] <= qhi)
    return df.loc[mask].copy(), (float(qlo), float(qhi))

# ---------------- aggregation ----------------

def grid_by_year_level(df: pd.DataFrame) -> pd.DataFrame:
    agg = {
        "trades":       ("ret_pct", "size"),
        "win_rate":     ("win", "mean"),
        "avg_ret_pct":  ("ret_pct", "mean"),
        "med_ret_pct":  ("ret_pct", "median"),
        "med_hold_days":("hold_days", "median"),
        "tp_share":     ("exit_reason", lambda s: float(np.mean(s == "TP"))),
        "time_share":   ("exit_reason", lambda s: float(np.mean(s == "TIME"))),
        "rsi_share":    ("exit_reason", lambda s: float(np.mean(s == "RSI"))),
    }
    out = (
        df.groupby(["year", "market_level"], dropna=False)
          .agg(**agg).reset_index().sort_values(["year","market_level"])
    )
    return out

def yearly_pnl(df: pd.DataFrame, capital_per_trade: float = 1000.0) -> pd.DataFrame:
    agg = {
        "trades":        ("ret_pct", "size"),
        "win_rate":      ("win", "mean"),
        "avg_ret_pct":   ("ret_pct", "mean"),
        "med_ret_pct":   ("ret_pct", "median"),
        "med_hold_days": ("hold_days", "median"),
        "tp_share":      ("exit_reason",  lambda s: float(np.mean(s == "TP"))),
        "time_share":    ("exit_reason",  lambda s: float(np.mean(s == "TIME"))),
        "rsi_share":     ("exit_reason",  lambda s: float(np.mean(s == "RSI"))),
        "total_gain_$":  ("ret_pct",     lambda s: float(capital_per_trade * s.sum() / 100.0)),
    }
    out = (
        df.groupby("year", dropna=False)
          .agg(**agg).reset_index().sort_values("year")
    )
    out["roi_on_capital"] = out["total_gain_$"] / (capital_per_trade * out["trades"])
    return out

# ---------------- mask building ----------------

def build_mask(
    grid: pd.DataFrame,
    train_years: tuple[int,int],
    min_trades: int,
    min_winrate: float,
    scope: str = "level"  # "level" or "year_level"
) -> pd.DataFrame:
    y0, y1 = train_years
    gtrain = grid[(grid["year"] >= y0) & (grid["year"] <= y1)].copy()

    if scope == "level":
        # aggregate across training years by level
        agg = {
            "trades":      ("trades","sum"),
            "win_rate":    ("win_rate","mean"),
            "avg_ret_pct": ("avg_ret_pct","mean"),
            "med_ret_pct": ("med_ret_pct","median"),
        }
        lvl = gtrain.groupby("market_level", dropna=False).agg(**agg).reset_index()
        mask = lvl[(lvl["trades"] >= min_trades) &
                   (lvl["avg_ret_pct"] > 0.0) &
                   (lvl["win_rate"] >= min_winrate)].copy()
        mask["include"] = True
        return mask.sort_values("market_level")
    elif scope == "year_level":
        yl = gtrain[
            (gtrain["trades"] >= min_trades) &
            (gtrain["avg_ret_pct"] > 0.0) &
            (gtrain["win_rate"] >= min_winrate)
        ].copy()
        yl["include"] = True
        return yl.sort_values(["year","market_level"])
    else:
        raise ValueError("scope must be 'level' or 'year_level'")

# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="Expectancy grid + OOS check.")
    ap.add_argument("--src", default="Data/Processed/master_breakouts_all.csv")
    ap.add_argument("--trim", nargs=2, type=float, metavar=("LOW","HIGH"), default=(1.0, 99.0),
                    help="Percentile bounds to keep globally (default: 1 99).")
    ap.add_argument("--train-years", nargs=2, type=int, metavar=("Y0","Y1"), default=(2020, 2023),
                    help="Inclusive train-year span for mask.")
    ap.add_argument("--eval-years", nargs=2, type=int, metavar=("Y0","Y1"), default=(2024, 2025),
                    help="Inclusive eval-year span.")
    ap.add_argument("--min-trades", type=int, default=300, help="Min trades for a regime to qualify.")
    ap.add_argument("--min-winrate", type=float, default=0.0, help="Optional minimum win-rate (0–1).")
    ap.add_argument("--mask-scope", choices=["level","year_level"], default="level",
                    help="Build mask per market_level (default) or per (year, level).")
    ap.add_argument("--capital", type=float, default=1000.0, help="Capital per trade for P&L.")
    args = ap.parse_args()

    outdir = Path("Data/Processed/expectancy")
    outdir.mkdir(parents=True, exist_ok=True)

    # load + trim
    df = load_aligned(args.src)
    df_t, q = trim_by_global(df, *args.trim)

    # grid
    grid = grid_by_year_level(df_t)
    grid.to_csv(outdir / "expectancy_grid.csv", index=False)

    # mask
    mask = build_mask(grid, tuple(args.train_years), args.min_trades, args.min_winrate, scope=args.mask_scope)
    mask_name = f"mask_{args.mask_scope}_train_{args.train_years[0]}_{args.train_years[1]}.csv"
    mask.to_csv(outdir / mask_name, index=False)

    # baseline eval P&L
    y0, y1 = args.eval_years
    eval_df = df_t[(df_t["year"] >= y0) & (df_t["year"] <= y1)].copy()
    pnl_baseline = yearly_pnl(eval_df, capital_per_trade=args.capital)
    pnl_baseline.to_csv(outdir / f"yearly_pnl_eval_baseline_{int(args.trim[0])}_{int(args.trim[1])}.csv", index=False)

    # apply mask
    if args.mask_scope == "level":
        keep_levels = set(mask["market_level"].dropna().astype(int).tolist())
        eval_masked = eval_df[eval_df["market_level"].isin(keep_levels)].copy()
    else:
        # year_level scope
        key = set(zip(mask["year"].astype(int), mask["market_level"].astype(int)))
        eval_masked = eval_df[list(zip(eval_df["year"].astype(int), eval_df["market_level"].astype(int)))\
                              ].copy() if key else eval_df.iloc[0:0].copy()
        eval_masked = eval_df[[ (int(y), int(ml)) in key for y, ml in zip(eval_df["year"], eval_df["market_level"]) ]]

    pnl_masked = yearly_pnl(eval_masked, capital_per_trade=args.capital)
    pnl_masked.to_csv(outdir / f"yearly_pnl_eval_masked_{args.mask_scope}_{int(args.trim[0])}_{int(args.trim[1])}.csv", index=False)

    # Console summary
    print(f"\nGlobal trim applied at approx ret_pct ∈ [{q[0]:.2f}, {q[1]:.2f}]")
    print("\n=== Train-derived MASK ===")
    with pd.option_context("display.max_rows", 200, "display.width", 160):
        print(mask.to_string(index=False))

    print("\n=== Eval P&L (baseline) ===")
    print(pnl_baseline.to_string(index=False))

    print(f"\n=== Eval P&L (masked by {args.mask_scope}) ===")
    print(pnl_masked.to_string(index=False))

    # Write also a lightweight README of what we did
    (outdir / "README.txt").write_text(
        f"Expectancy grid built from {args.src}\n"
        f"Trim: {args.trim[0]}–{args.trim[1]} percentiles (global)\n"
        f"Train years: {args.train_years[0]}–{args.train_years[1]} | Eval years: {args.eval_years[0]}–{args.eval_years[1]}\n"
        f"Mask scope: {args.mask_scope} | min_trades={args.min_trades} | min_winrate={args.min_winrate}\n"
        f"Files:\n"
        f" - expectancy_grid.csv\n"
        f" - {mask_name}\n"
        f" - yearly_pnl_eval_baseline_{int(args.trim[0])}_{int(args.trim[1])}.csv\n"
        f" - yearly_pnl_eval_masked_{args.mask_scope}_{int(args.trim[0])}_{int(args.trim[1])}.csv\n"
    )

if __name__ == "__main__":
    main()
