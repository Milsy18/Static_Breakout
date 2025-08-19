# scripts/python/run_opt.py
"""
run_opt.py — Gate evaluation harness for M18 V18.0

Features:
- Auto-discovers the newest enriched CSV in ./out if --input is omitted (prefers *enriched*.csv).
- Optional calibration split (--calib YYYY-MM:YYYY-MM) to de-leak data-derived thresholds.
- Filters by symbols and OOS date range (YYYY-MM:YYYY-MM) if provided.
- Evaluates gates per market level with default thresholds:
    rsi_z, adx_z, cmf_z, rvol_z       -> threshold 0.0 (>=)
    ema_ratio_10_50, ema_ratio_50_200 -> threshold 1.0 (>=)
    bbw_pct                           -> median (<=)  [squeeze]   (from calib if available)
    range_pct                         -> median (>=)  [expansion] (from calib if available)
- Computes metrics for each gate, strict confluence (all gates ON),
  a primary k-of-n confluence (--kconfluence, default 5), and optional extra ks (--k_sweep).
- Time-ordered drawdown (sort by breakout_date before equity curve).
- Writes:
    out/gate_performance_by_ml.csv
    out/best_params_per_ml.csv
    out/opt_metadata.json
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, UTC
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
    # regime/volatility (data-derived thresholds)
    "bbw":        {"feature": "bbw_pct",            "threshold": "median","direction": "<="},  # squeeze
    "range":      {"feature": "range_pct",          "threshold": "median","direction": ">="},  # expansion
}

RET_COL = "exit_ret"          # expected decimal return per trade, e.g. 0.05 = +5%
DATE_COL = "breakout_date"    # used for filtering and ordering
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
    """Equity-curve MDD on time-ordered returns."""
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

def net_returns_for_mask(subset: pd.DataFrame, mask: pd.Series, fee: float, slip: float) -> pd.Series:
    """Select rows by mask, order by DATE_COL if present, and return net returns."""
    if mask.sum() == 0:
        return pd.Series(dtype=float)
    sel = subset.loc[mask, [RET_COL] + ([DATE_COL] if DATE_COL in subset.columns else [])].copy()
    if DATE_COL in sel.columns:
        sel = sel.sort_values(DATE_COL)
    net = sel[RET_COL] - 2 * (fee + slip)
    return net.astype(float)

# ---------------------------- Gate Evaluation --------------------------------

def evaluate_gates(
    df: pd.DataFrame,
    fees_bps: float,
    slip_bps: float,
    ml_mode: str,
    kconfluence: int,
    k_sweep: Optional[List[int]] = None,
    calib_thresholds: Optional[Dict[Tuple[Optional[int], str], float]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Evaluate individual gates, strict confluence, primary k-of-n, and optional k-sweep.
    `calib_thresholds` maps (level, feature_name) -> threshold for 'median' gates.
    """
    results: List[dict] = []
    thresholds_out: List[dict] = []

    fee = fees_bps / 10000.0
    slip = slip_bps / 10000.0

    if RET_COL not in df.columns:
        raise KeyError(f"Required returns column '{RET_COL}' not found.")

    use_ml = (ml_mode == "auto" and ML_COL in df.columns)
    groups = df.groupby(ML_COL) if use_ml else [(None, df)]

    # normalize k_sweep
    ks_extra: List[int] = []
    if k_sweep:
        ks_extra = [int(x) for x in k_sweep if int(x) != int(kconfluence)]

    for level, subset in groups:
        # per-ML thresholds
        thr: Dict[str, float] = {}
        lvl_key = level if level is not None else None
        for gname, info in GATE_DEFS.items():
            feat = info["feature"]
            if feat not in subset.columns:
                thr[gname] = np.nan
                continue
            if info["threshold"] == "median":
                # Prefer precomputed calib threshold if provided
                key = (lvl_key, feat)
                if calib_thresholds and key in calib_thresholds:
                    thr[gname] = float(calib_thresholds[key])
                else:
                    thr[gname] = float(subset[feat].median())
            else:
                thr[gname] = float(info["threshold"])
            thresholds_out.append({
                "market_level": level if level is not None else "all",
                "gate": gname,
                "feature": feat,
                "threshold": thr[gname],
            })

        # Individual gates
        conds: Dict[str, pd.Series] = {}
        for gname, info in GATE_DEFS.items():
            feat = info["feature"]
            if feat not in subset.columns:
                results.append(metric_row(level, gname, 0, pd.Series(dtype=float), note="missing_feature"))
                continue
            direction = info["direction"]
            t = thr[gname]
            s = subset[feat]
            cond = (s >= t) if direction == ">=" else (s <= t)
            conds[gname] = cond

            net_ret = net_returns_for_mask(subset, cond, fee, slip)
            results.append(metric_row(level, gname, int(cond.sum()), net_ret))

        # strict confluence: all gates ON
        if conds:
            strict_mask = pd.Series(True, index=subset.index)
            for g in conds.values():
                strict_mask &= g
            net_ret = net_returns_for_mask(subset, strict_mask, fee, slip)
            results.append(metric_row(level, "confluence", int(strict_mask.sum()), net_ret))

            # primary k-of-n
            stack = pd.concat([c.astype(int) for c in conds.values()], axis=1)
            k_main = max(1, min(int(kconfluence), stack.shape[1]))
            k_mask = (stack.sum(axis=1) >= k_main)
            net_ret_k = net_returns_for_mask(subset, k_mask, fee, slip)
            results.append(metric_row(level, f"k_confluence_{k_main}", int(k_mask.sum()), net_ret_k))

            # optional extra ks
            for kx in ks_extra:
                kx = max(1, min(int(kx), stack.shape[1]))
                mk = (stack.sum(axis=1) >= kx)
                net_ret_kx = net_returns_for_mask(subset, mk, fee, slip)
                results.append(metric_row(level, f"k_confluence_{kx}", int(mk.sum()), net_ret_kx))

    return pd.DataFrame(results), pd.DataFrame(thresholds_out)

# --------------------------------- Main --------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to enriched CSV (auto if omitted)")
    ap.add_argument("--output", required=True, help="Output directory")
    ap.add_argument("--symbols", help="Comma-separated symbols to include (optional)")
    ap.add_argument("--calib", help="Calibration date range 'YYYY-MM:YYYY-MM' (optional)")
    ap.add_argument("--oos", help="OOS date range 'YYYY-MM:YYYY-MM' (optional)")
    ap.add_argument("--ml", choices=["auto", "off"], default="auto", help="Per-ML conditioning")
    ap.add_argument("--fees_bps", type=float, default=0.0, help="Fee per trade (bps)")
    ap.add_argument("--slip_bps", type=float, default=0.0, help="Slippage per trade (bps)")
    ap.add_argument("--kconfluence", type=int, default=5, help="Primary k-of-n gates requirement")
    ap.add_argument("--k_sweep", help="Comma-separated extra k values, e.g. '4,6,7' (optional)")
    args = ap.parse_args()

    # resolve input
    input_path = Path(args.input) if args.input else auto_find_input()
    if input_path is None or not input_path.exists():
        raise FileNotFoundError(
            "Could not find input CSV. Provide --input or place an enriched CSV in ./out "
            "(e.g., out/final_holy_grail_enriched.csv)."
        )
    print(f"Using input: {input_path}")

    # Read once to detect columns
    head_cols = pd.read_csv(input_path, nrows=0)
    parse_dates = [DATE_COL] if DATE_COL in head_cols.columns else None

    # Load full dataset
    df_full = pd.read_csv(input_path, parse_dates=parse_dates)
    print(f"Loaded {df_full.shape[0]} rows x {df_full.shape[1]} cols")

    # Filter by symbols (applies to both calib and oos)
    if args.symbols and "symbol" in df_full.columns:
        syms = [s.strip() for s in args.symbols.split(",")]
        df_full = df_full[df_full["symbol"].isin(syms)]
        print(f"Filtered to symbols={syms} | {df_full.shape[0]} rows")

    # Build CALIB and OOS slices (may be None)
    df_calib = None
    df_oos = df_full.copy()

    def slice_by_range(src: pd.DataFrame, rng: str) -> pd.DataFrame:
        if rng and DATE_COL in src.columns and np.issubdtype(src[DATE_COL].dtype, np.datetime64):
            start_dt, end_dt = parse_date_range(rng)
            out = src[(src[DATE_COL] >= start_dt) & (src[DATE_COL] < end_dt)]
            print(f"Filtered to {start_dt.date()}->{(end_dt - pd.Timedelta(days=1)).date()} | {out.shape[0]} rows")
            return out
        elif rng:
            print("Warning: date range provided but breakout_date missing/not datetime; skipping filter.")
        return src

    if args.calib:
        df_calib = slice_by_range(df_full, args.calib)

    if args.oos:
        df_oos = slice_by_range(df_full, args.oos)

    # Prepare calibration thresholds for median-based gates (bbw_pct, range_pct)
    calib_thresholds: Dict[Tuple[Optional[int], str], float] = {}
    if df_calib is not None and not df_calib.empty:
        # per-ML if available, else global "all"
        if ML_COL in df_calib.columns:
            for lvl, gdf in df_calib.groupby(ML_COL):
                for gname, info in GATE_DEFS.items():
                    if info["threshold"] == "median":
                        feat = info["feature"]
                        if feat in gdf.columns:
                            calib_thresholds[(lvl, feat)] = float(gdf[feat].median())
        else:
            for gname, info in GATE_DEFS.items():
                if info["threshold"] == "median":
                    feat = info["feature"]
                    if feat in df_calib.columns:
                        calib_thresholds[(None, feat)] = float(df_calib[feat].median())
        print(f"Calibrated thresholds computed for {len(calib_thresholds)} (level,feature) pairs.")

    # Evaluate
    ks = None
    if args.k_sweep:
        ks = [int(x.strip()) for x in args.k_sweep.split(",") if x.strip()]

    perf_df, thr_df = evaluate_gates(
        df=df_oos,
        fees_bps=args.fees_bps,
        slip_bps=args.slip_bps,
        ml_mode=args.ml,
        kconfluence=args.kconfluence,
        k_sweep=ks,
        calib_thresholds=calib_thresholds if calib_thresholds else None,
    )

    # outputs
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    perf_csv = out_dir / "gate_performance_by_ml.csv"
    thr_csv = out_dir / "best_params_per_ml.csv"
    perf_df.to_csv(perf_csv, index=False)
    thr_df.to_csv(thr_csv, index=False)

    # metadata
    meta = {
        "resolved_input": str(input_path),
        "rows_oos": int(df_oos.shape[0]),
        "rows_full": int(df_full.shape[0]),
        "cols": int(df_full.shape[1]),
        "symbols": args.symbols,
        "calib": args.calib,
        "oos": args.oos,
        "ml_mode": args.ml,
        "fees_bps": args.fees_bps,
        "slip_bps": args.slip_bps,
        "kconfluence": args.kconfluence,
        "k_sweep": ks,
        "timestamp": datetime.now(UTC).isoformat(),
        "levels": (sorted(map(int, df_oos[ML_COL].unique()))
                   if ML_COL in df_oos.columns else ["all"]),
    }
    with open(out_dir / "opt_metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"Wrote performance -> {perf_csv}")
    print(f"Wrote thresholds  -> {thr_csv}")
    print(f"Wrote metadata    -> {out_dir / 'opt_metadata.json'}")

if __name__ == "__main__":
    main()

