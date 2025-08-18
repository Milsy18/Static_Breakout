"""
run_opt.py — Optimization harness for M18 V18.0

This script provides a baseline for evaluating breakout signals by symbol
and market level. It reads an enriched dataset (as produced by
build_features.py), filters by optional symbol(s) and date range,
computes simple performance metrics (win_rate, avg_return, expectancy,
and naive max drawdown), and writes per‑market-level results to CSV.

To be extended with:
- ML‑aware gating thresholds from pine_defaults_v18.yaml
- Walk‑forward/nested cross‑validation
- Parameter search (Bayesian/successive halving)
- Time stop, ATR trailing stop, RSI veto, TP ladders, etc.
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

def parse_date_range(date_range: str):
    """Parse a date range in YYYY-MM:YYYY-MM format to start/end datetimes."""
    try:
        start_str, end_str = date_range.split(":")
        start_dt = datetime.strptime(start_str + "-01", "%Y-%m-%d")
        # Compute end of month by advancing one month and subtracting a day
        tmp = datetime.strptime(end_str + "-01", "%Y-%m-%d")
        if tmp.month == 12:
            end_dt = datetime(tmp.year + 1, 1, 1)
        else:
            end_dt = datetime(tmp.year, tmp.month + 1, 1)
        return start_dt, end_dt
    except Exception:
        raise ValueError("Date range must be in 'YYYY-MM:YYYY-MM' format")

def compute_max_drawdown(returns: pd.Series):
    """Compute a naïve max drawdown from a series of returns."""
    # Cumulate returns multiplicatively: (1+ret1)*(1+ret2)*...
    cumulative = (1 + returns).cumprod()
    peak = cumulative.expanding(min_periods=1).max()
    drawdown = (cumulative - peak) / peak
    return drawdown.min()  # negative number

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to enriched CSV")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--symbols", help="Comma-separated symbols to include (optional)")
    parser.add_argument("--oos", help="Date range e.g. '2023-01:2024-12' for out-of-sample")
    parser.add_argument("--ml", choices=["auto","off"], default="auto",
                        help="Market-level conditioning: auto (per-ML) or off (aggregate)")
    parser.add_argument("--fees_bps", type=float, default=0.0,
                        help="Commission/fee per trade in basis points")
    parser.add_argument("--slip_bps", type=float, default=0.0,
                        help="Slippage per trade in basis points")
    args = parser.parse_args()

    df = pd.read_csv(args.input, parse_dates=["breakout_date"])
    print(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns from {args.input}")

    # Filter by symbols if provided
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
        df = df[df["symbol"].isin(symbols)]
        print(f"Filtered to {len(symbols)} symbols → {df.shape[0]} rows")

    # Filter by date range if provided
    if args.oos:
        start_dt, end_dt = parse_date_range(args.oos)
        df = df[(df["breakout_date"] >= start_dt) & (df["breakout_date"] < end_dt)]
        print(f"Filtered to date range {start_dt.date()} – {end_dt.date()-pd.Timedelta(days=1)} → {df.shape[0]} rows")

    # Compute per-market-level metrics
    group_key = "market_level" if args.ml == "auto" and "market_level" in df.columns else None
    group = df.groupby(group_key) if group_key else [(None, df)]

    results = []
    fee = args.fees_bps / 10000.0
    slip = args.slip_bps / 10000.0

    for lvl, subset in group:
        # Compute returns adjusted for costs: subtract fee+slip twice (entry + exit)
        gross_ret = subset.get("exit_ret", np.nan)  # assume exit_ret is decimal, e.g. 0.05 = +5%
        if gross_ret is not np.nan:
            net_ret = gross_ret - 2*(fee+slip)
        else:
            net_ret = subset["exit_ret"]

        successes = (net_ret > 0).sum()
        total = len(subset)
        win_rate = successes / total if total > 0 else np.nan
        avg_return = net_ret.mean() if len(net_ret) > 0 else np.nan
        expectancy = (net_ret[net_ret > 0].mean() * (net_ret > 0).mean() +
                      net_ret[net_ret <= 0].mean() * (net_ret <= 0).mean()) if total > 0 else np.nan
        mdd = compute_max_drawdown(net_ret.fillna(0)) if len(net_ret) > 0 else np.nan

        results.append({
            "market_level": lvl if lvl is not None else "all",
            "trades": total,
            "win_rate": win_rate,
            "avg_return": avg_return,
            "expectancy": expectancy,
            "max_drawdown": mdd
        })

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "gate_performance_by_ml.csv"
    pd.DataFrame(results).to_csv(results_path, index=False)
    print(f"Wrote per-level metrics to {results_path}")

if __name__ == "__main__":
    main()
