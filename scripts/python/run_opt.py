
"""
run_opt.py — Optimization harness for M18 V18.0 (auto-discovery)

- If --input is omitted, auto-pick the most recent enriched CSV from ./out
  (prefers files with 'enriched' in the name; falls back to any CSV).
- Filters by optional symbols and date range (YYYY-MM:YYYY-MM).
- Computes per-market-level metrics: win_rate, avg_return, expectancy, max_drawdown.
"""

import argparse
from datetime import datetime
from pathlib import Path
import glob
import numpy as np
import pandas as pd

def parse_date_range(date_range: str):
    start_str, end_str = date_range.split(":")
    start_dt = datetime.strptime(start_str + "-01", "%Y-%m-%d")
    tmp = datetime.strptime(end_str + "-01", "%Y-%m-%d")
    end_dt = datetime(tmp.year + (1 if tmp.month == 12 else 0),
                      1 if tmp.month == 12 else tmp.month + 1, 1)
    return start_dt, end_dt

def compute_max_drawdown(returns: pd.Series):
    cumulative = (1 + returns.fillna(0)).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    return float(drawdown.min()) if len(drawdown) else np.nan

def auto_find_input():
    """Pick the newest enriched CSV in ./out; prefer names containing 'enriched'."""
    out_dir = Path("out")
    if not out_dir.exists():
        return None
    # Prefer enriched candidates
    candidates = sorted(
        [Path(p) for p in glob.glob(str(out_dir / "*enriched*.csv"))],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        # fallback to any CSV
        candidates = sorted(
            [Path(p) for p in glob.glob(str(out_dir / "*.csv"))],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    return candidates[0] if candidates else None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Path to enriched dataset CSV (optional; auto if omitted)")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--symbols", help="Comma-separated symbols to include (optional)")
    parser.add_argument("--oos", help="Date range 'YYYY-MM:YYYY-MM' (optional)")
    parser.add_argument("--ml", choices=["auto","off"], default="auto",
                        help="Market-level conditioning: auto (per-ML) or off (aggregate)")
    parser.add_argument("--fees_bps", type=float, default=0.0)
    parser.add_argument("--slip_bps", type=float, default=0.0)
    args = parser.parse_args()

    # Resolve input
    input_path = Path(args.input) if args.input else auto_find_input()
    if input_path is None or not Path(input_path).exists():
        raise FileNotFoundError(
            "Could not find an input CSV. Provide --input or place an enriched CSV in ./out "
            "(e.g., out/final_holy_grail_enriched.csv)."
        )
    print(f"Using input: {input_path}")

    df = pd.read_csv(input_path, parse_dates=["breakout_date"])
    print(f"Loaded {df.shape[0]} rows × {df.shape[1]} cols")

    # Optional filters
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
        df = df[df["symbol"].isin(symbols)]
        print(f"Filtered to symbols={symbols} → {df.shape[0]} rows")

    if args.oos:
        start_dt, end_dt = parse_date_range(args.oos)
        df = df[(df["breakout_date"] >= start_dt) & (df["breakout_date"] < end_dt)]
        print(f"Filtered to {start_dt.date()}–{(end_dt - pd.Timedelta(days=1)).date()} → {df.shape[0]} rows")

    group_key = "market_level" if args.ml == "auto" and "market_level" in df.columns else None
    groups = df.groupby(group_key) if group_key else [(None, df)]

    fee = args.fees_bps / 10000.0
    slip = args.slip_bps / 10000.0
    results = []

    for lvl, subset in groups:
        if "exit_ret" not in subset.columns:
            raise KeyError("Column 'exit_ret' not found in input; required for returns math.")
        net_ret = subset["exit_ret"] - 2 * (fee + slip)
        total = len(net_ret)
        win_rate = float((net_ret > 0).mean()) if total else np.nan
        avg_return = float(net_ret.mean()) if total else np.nan
        expectancy = (
            float(net_ret[net_ret > 0].mean()) * float((net_ret > 0).mean()) +
            float(net_ret[net_ret <= 0].mean()) * float((net_ret <= 0).mean())
        ) if total else np.nan
        mdd = compute_max_drawdown(net_ret)

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
    out_csv = out_dir / "gate_performance_by_ml.csv"
    pd.DataFrame(results).to_csv(out_csv, index=False)
    print(f"Wrote per-level metrics to {out_csv}")

if __name__ == "__main__":
    main()
