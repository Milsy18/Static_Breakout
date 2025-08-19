
"""
run_opt.py — Gate evaluation harness for M18 V18.0

Features:
- Auto-discovers the newest enriched CSV in ./out if --input is omitted (prefers *enriched*.csv).
- Filters by symbols and date range (YYYY-MM:YYYY-MM) if provided.
- Evaluates gates per market level with default thresholds:
    rsi_z, adx_z, cmf_z, rvol_z  -> threshold 0.0 (>=)
    ema_ratio_10_50, ema_ratio_50_200 -> threshold 1.0 (>=)
    bbw_pct -> per-ML median (<=)  [squeeze]
    range_pct -> per-ML median (>=) [expansion]
- Computes metrics for each gate, the strict confluence (all gates ON),
  and a k-of-n confluence (--kconfluence, default 5).
- Writes:
    out/gate_performance_by_ml.csv
    out/best_params_per_ml.csv
    out/opt_metadata.json
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------- Gate Config -----------------------------------

GATE_DEFS: Dict[str, Dict[str, object]] = {
    # z-score gates: bullish if >= 0
    "rsi":        {"feature": "rsi_z",              "threshold": 0.0,     "direction": ">="},
    "adx":        {"feature": "adx_z",              "threshold": 0.0,     "direction": ">="},
    "cmf":        {"feature": "cmf_z",              "threshold": 0.0,     "direction": ">="},
    "rvol":       {"feature": "rvol_z",             "threshold": 0.0,     "direction": ">="},
    # trend structure
    "ema10_50":   {"feature": "ema_ratio_10_50",    "threshold": 1.0,     "direction": ">="},
    "ema50_200":  {"feature": "ema_ratio_50_200",   "threshold": 1.0,     "direction": ">="},
    # regime/volatility
    "bbw":        {"feature": "bbw_pct",            "threshold": "median","direction": "<="},  # squeeze
    "range":      {"feature": "range_pct",          "threshold": "median","direction": ">="},  # expansion
}

RET_COL = "exit_ret"          # expected decimal return per trade, e.g. 0.05 = +5%
DATE_COL = "breakout_date"    # used for OOS filtering if present
ML_COL = "market_level"

# ------------------------------ Utilities ------------------------------------

def parse_date_range(r: str) -> Tuple[datetime, datetime]:
    start_str, end_str = r.split(":")
    start_dt = datetime.strptime(start_str + "-01", "%Y-%m-%d")
    tmp = datetime.strptime(end_str + "-01", "%Y-%m-%d")
    end_dt = datetime(tmp.year + (1 if tmp.month == 12 else 0),
                      1 if tmp.month == 12 else tmp.month + 1, 1)
    return start_dt, end_dt

def compute_max_drawdown(returns: pd.Series) -> float:
    """Naïve MDD on multiplicative equity curve."""
    if returns.empty:
        return float("nan")
    equity = (1 + returns.fillna(0.0)).cumprod()
    peak = equity.cummax()
    dd = (equity - peak) / peak
    return float(dd.min())

def auto_find_input() -> Optional[Path]:
    out_dir = Path("out")
    if not out_dir.exists():
        return None
    # Prefer enriched
    cands = sorted((Path(p) for p in glob.glob(str(out_dir / "*enriched*.csv"))),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    if not cands:
        cands = sorted((Path(p) for p in glob.glob(str(out_dir / "*.csv"))),
                       key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def metric_row(level, gate, trades, net_ret, note=""):
    """Compute metrics dictionary for a mask. If trades==0, zeros + note."""
    if trades == 0 or net_ret.empty:
        return {
            "market_level": level if level is not None else "all",
            "gate": gate,
            "trades": int(trades),
            "win_rate": 0.0,
            "avg_return": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "note": note or "no_trades",
        }
    win_rate = float((net_ret > 0).mean())
    avg_return = float(net_ret.mean())
    pos = net_ret[net_ret > 0]
    neg = net_ret[net_ret <= 0]
    expectancy = float(pos.mean() * (len(pos)/len(net_ret)) + neg.mean() * (len(neg)/len(net_ret)))
    mdd = compute_max_drawdown(net_ret)
    return {
        "market_level": level if level is not None else "all",
        "gate": gate,
        "trades": int(trades),
        "win_rate": win_rate,
        "avg_return": avg_return,
        "expectancy": expectancy,
        "max_drawdown": mdd,
        "note": note,
    }

# ---------------------------- Gate Evaluation --------------------------------

def evaluate_gates(df: pd.DataFrame, fees_bps: float, slip_bps: float,
                   ml_mode: str, kconfluence: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    results: List[dict] = []
    thresholds_out: List[dict] = []

    fee = fees_bps / 10000.0
    slip = slip_bps / 10000.0

    if RET_COL not in df.columns:
        raise KeyError(f"Required returns column '{RET_COL}' not found.")

    # group selection
    use_ml = (ml_mode == "auto" and ML_COL in df.columns)
    groups = df.groupby(ML_COL) if use_ml else [(None, df)]

    for level, subset in groups:
        # per-ML thresholds
        thr: Dict[str, float] = {}
        for gname, info in GATE_DEFS.items():
            feat = info["feature"]
            if feat not in subset.columns:
                thr[gname] = np.nan
                continue
            if info["threshold"] == "median":
                thr[gname] = float(subset[feat].median())
            else:
                thr[gname] = float(info["threshold"])
            thresholds_out.append({
                "market_level": level if level is not None else "all",
                "gate": gname,
                "threshold": thr[gname],
            })

        # individual gates
        conds: Dict[str, pd.Series] = {}
        for gname, info in GATE_DEFS.items():
            feat = info["feature"]
            if feat not in subset.columns:
                # record missing-feature row
                results.append(metric_row(level, gname, 0, pd.Series(dtype=float),
                                          note="missing_feature"))
                continue
            direction = info["direction"]
            t = thr[gname]
            s = subset[feat]
            if direction == ">=":
                cond = s >= t
            else:
                cond = s <= t
            conds[gname] = cond

            # compute metrics for this gate
            net_ret = subset.loc[cond, RET_COL] - 2 * (fee + slip)
            results.append(metric_row(level, gname, cond.sum(), net_ret))

        # strict confluence: all gates ON and features available
        if conds:
            strict_mask = pd.Series(True, index=subset.index)
            for g in conds.values():
                strict_mask &= g
            net_ret = subset.loc[strict_mask, RET_COL] - 2 * (fee + slip)
            results.append(metric_row(level, "confluence", strict_mask.sum(), net_ret))

            # k-of-n confluence
            k = max(1, min(kconfluence, len(conds)))
            # to avoid bool->int pitfalls, cast True/False to integers then sum row-wise
            stack = pd.concat([c.astype(int) for c in conds.values()], axis=1)
            k_mask = stack.sum(axis=1) >= k
            net_ret_k = subset.loc[k_mask, RET_COL] - 2 * (fee + slip)
            results.append(metric_row(level, f"k_confluence_{k}", k_mask.sum(), net_ret_k))

    return pd.DataFrame(results), pd.DataFrame(thresholds_out)

# --------------------------------- Main --------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to enriched CSV (auto if omitted)")
    ap.add_argument("--output", required=True, help="Output directory")
    ap.add_argument("--symbols", help="Comma-separated symbols to include (optional)")
    ap.add_argument("--oos", help="Date range 'YYYY-MM:YYYY-MM' (optional)")
    ap.add_argument("--ml", choices=["auto", "off"], default="auto", help="Per-ML conditioning")
    ap.add_argument("--fees_bps", type=float, default=0.0, help="Fee per trade (bps)")
    ap.add_argument("--slip_bps", type=float, default=0.0, help="Slippage per trade (bps)")
    ap.add_argument("--kconfluence", type=int, default=5, help="k-of-n gates requirement")
    args = ap.parse_args()

    # resolve input
    input_path = Path(args.input) if args.input else auto_find_input()
    if input_path is None or not input_path.exists():
        raise FileNotFoundError(
            "Could not find input CSV. Provide --input or place an enriched CSV in ./out "
            "(e.g., out/final_holy_grail_enriched.csv)."
        )
    print(f"Using input: {input_path}")

    parse_dates = [DATE_COL] if DATE_COL in pd.read_csv(input_path, nrows=0).columns else None
    df = pd.read_csv(input_path, parse_dates=parse_dates)
    print(f"Loaded {df.shape[0]} rows × {df.shape[1]} cols")

    # filters
    if args.symbols and "symbol" in df.columns:
        syms = [s.strip() for s in args.symbols.split(",")]
        df = df[df["symbol"].isin(syms)]
        print(f"Filtered to symbols={syms} → {df.shape[0]} rows")

    if args.oos and DATE_COL in df.columns and np.issubdtype(df[DATE_COL].dtype, np.datetime64):
        start_dt, end_dt = parse_date_range(args.oos)
        df = df[(df[DATE_COL] >= start_dt) & (df[DATE_COL] < end_dt)]
        print(f"Filtered to {start_dt.date()}–{(end_dt - pd.Timedelta(days=1)).date()} → {df.shape[0]} rows")
    elif args.oos:
        print("Warning: --oos provided but breakout_date column not found or not datetime; skipping OOS filter.")

    # evaluate
    perf_df, thr_df = evaluate_gates(df, args.fees_bps, args.slip_bps, args.ml, args.kconfluence)

    # outputs
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # main CSVs
    perf_csv = out_dir / "gate_performance_by_ml.csv"
    thr_csv = out_dir / "best_params_per_ml.csv"
    perf_df.to_csv(perf_csv, index=False)
    thr_df.to_csv(thr_csv, index=False)

    # metadata
    meta = {
        "resolved_input": str(input_path),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "symbols": args.symbols,
        "oos": args.oos,
        "ml_mode": args.ml,
        "fees_bps": args.fees_bps,
        "slip_bps": args.slip_bps,
        "kconfluence": args.kconfluence,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "levels": sorted(map(lambda x: "all" if x is None else int(x),
                             df[ML_COL].unique())) if ML_COL in df.columns else ["all"],
    }
    with open(out_dir / "opt_metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"Wrote performance → {perf_csv}")
    print(f"Wrote thresholds  → {thr_csv}")
    print(f"Wrote metadata    → {out_dir / 'opt_metadata.json'}")

if __name__ == "__main__":
    main()
