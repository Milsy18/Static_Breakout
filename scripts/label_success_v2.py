#!/usr/bin/env python3
# label_success_v2.py — Business labels for M18 breakouts (TP-before-TTL, TTL return, MFE/MAE, bars-to-TP)

import argparse, os, json
from pathlib import Path
from typing import Dict, Optional, Tuple
import numpy as np
import pandas as pd

DEFAULT_TP_MAP = {1: 0.22, 2: 0.25, 3: 0.28, 4: 0.30, 5: 0.33, 6: 0.36, 7: 0.40, 8: 0.42, 9: 0.45}
DEFAULT_TTL_MAP = {1: 5, 2: 6, 3: 6, 4: 7, 5: 8, 6: 9, 7: 10, 8: 11, 9: 11}

PRICE_LIKE_PATTERNS = (
    "price","open","high","low","close","hlc3","ohlc4","return","ret_","rtn_","pct",
    "gain","loss","drawdown","dd_","pnl","r_","perf","change"
)

def parse_mapping(s: Optional[str], default: Dict[int, float], name: str) -> Dict[int, float]:
    if not s:
        return default
    out = {}
    try:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            k, v = p.split(":")
            out[int(k.strip())] = float(v.strip())
        for k in default:
            out.setdefault(k, default[k])
        return out
    except Exception as e:
        raise ValueError(f"Failed to parse {name} mapping from '{s}': {e}")

def detect_time_col(df: pd.DataFrame) -> str:
    for c in ["entry_time","entry_ts","entry_date","date","ts","timestamp"]:
        if c in df.columns:
            return c
    raise KeyError("Could not find an entry time column.")

def load_ohlcv(prices_root: Path, symbol: str) -> pd.DataFrame:
    candidates = [
        prices_root / f"{symbol}.csv",
        prices_root / f"{symbol.replace('/', '-')}.csv",
        prices_root / f"{symbol.replace('-USD','')}-USD.csv",
        prices_root / f"{symbol.replace('-USDT','')}-USDT.csv",
    ]
    for p in candidates:
        if p.exists():
            df = pd.read_csv(p)
            df.columns = [c.strip().lower() for c in df.columns]
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            elif "timestamp" in df.columns:
                df["date"] = pd.to_datetime(df["timestamp"])
            else:
                raise KeyError(f"{p}: Expected a 'date' or 'timestamp' column.")
            need = {"open","high","low","close"}
            if not need.issubset(df.columns):
                raise KeyError(f"{p}: Missing OHLC columns.")
            return df[["date","open","high","low","close","volume"]].sort_values("date").reset_index(drop=True)
    raise FileNotFoundError(f"OHLCV not found for '{symbol}' under {prices_root}")

def drop_price_like(df: pd.DataFrame) -> pd.DataFrame:
    keep = []
    for c in df.columns:
        lc = c.lower()
        if any(tok in lc for tok in PRICE_LIKE_PATTERNS):
            continue
        keep.append(c)
    return df[keep]

def extract_t0_features(wide: pd.DataFrame) -> pd.DataFrame:
    cols = list(wide.columns)
    t0_cols = [c for c in cols if c.endswith("_t0") or c.startswith("t0_") or "_t0_" in c]
    if not t0_cols:
        t_markers = ("_t-","_tm","_t1","_t2","_t3","_t4","_t5","_t6","_t7","_t8","_t9","_t10","_t11")
        t0_cols = [c for c in cols if all(m not in c for m in t_markers)]
    base_cols = [c for c in ["symbol","entry_time","entry_ts","entry_date","date","market_level_at_entry","breakout_id"] if c in cols]
    t0 = wide[sorted(set(base_cols + t0_cols))].copy()
    t0 = drop_price_like(t0)
    return t0

def compute_labels_for_row(row: pd.Series, prices_root: Path, tp_map, ttl_map, time_col: str, entry_fill: str):
    symbol = row["symbol"]
    entry_time = pd.to_datetime(row[time_col])
    mlev = row.get("market_level_at_entry", np.nan)
    try:
        mlev = int(mlev) if not pd.isna(mlev) else 5
    except:
        mlev = 5
    TP = tp_map.get(mlev, tp_map[5])
    TTL = int(ttl_map.get(mlev, ttl_map[5]))

    ohlcv = load_ohlcv(prices_root, symbol)

    idx = int(ohlcv.index[ohlcv["date"] >= entry_time].min())
    if entry_fill == "close_at_entry":
        entry_px = float(ohlcv.loc[idx, "close"])
    else:
        if idx + 1 >= len(ohlcv):
            raise IndexError(f"{symbol} has no next bar after entry_time={entry_time}")
        entry_px = float(ohlcv.loc[idx+1, "open"])

    hi = ohlcv.loc[idx+1 : idx+TTL, "high"].to_numpy(dtype=float, copy=False)
    lo = ohlcv.loc[idx+1 : idx+TTL, "low"].to_numpy(dtype=float, copy=False)
    cl = ohlcv.loc[idx+TTL, "close"] if (idx+TTL) < len(ohlcv) else ohlcv.loc[len(ohlcv)-1, "close"]
    if hi.size == 0 or lo.size == 0:
        raise IndexError(f"{symbol}: not enough forward bars for TTL={TTL} at entry_time={entry_time}")

    mfe = (np.max(hi) / entry_px) - 1.0
    mae = (np.min(lo) / entry_px) - 1.0

    tp_levels = hi / entry_px - 1.0
    hit_idxs = np.where(tp_levels >= TP)[0]
    if hit_idxs.size > 0:
        bars_to_tp = int(hit_idxs[0] + 1)
        tp_hit = True
        exit_reason = "TP"
    else:
        bars_to_tp = None
        tp_hit = False
        exit_reason = "TIME"

    ttl_return = float(cl / entry_px - 1.0)
    return float(tp_hit), float(mfe), float(mae), float(ttl_return), bars_to_tp, exit_reason

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--breakouts", required=True)
    ap.add_argument("--prices-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--features", default=None)
    ap.add_argument("--out-features", default=None)
    ap.add_argument("--tp-map", default=None)
    ap.add_argument("--ttl-map", default=None)
    ap.add_argument("--entry-fill", default="next_open", choices=["next_open","close_at_entry"])
    args = ap.parse_args()

    tp_map = parse_mapping(args.tp_map, DEFAULT_TP_MAP, "tp-map")
    ttl_map = parse_mapping(args.ttl_map, DEFAULT_TTL_MAP, "ttl-map")

    prices_root = Path(args.prices_root)
    if not prices_root.exists():
        raise FileNotFoundError(f"--prices-root missing: {prices_root}")

    bo = pd.read_csv(args.breakouts)
    time_col = detect_time_col(bo)
    if "symbol" not in bo.columns:
        raise KeyError("Breakouts CSV must contain a 'symbol' column")
    bo[time_col] = pd.to_datetime(bo[time_col])

    rows, errors = [], []
    for _, row in bo.iterrows():
        try:
            tp_hit, mfe, mae, ttl_ret, bars_to_tp, exit_reason = compute_labels_for_row(
                row, prices_root, tp_map, ttl_map, time_col, args.entry_fill
            )
            rows.append({
                "symbol": row["symbol"],
                "entry_time": pd.to_datetime(row[time_col]),
                "market_level_at_entry": row.get("market_level_at_entry", np.nan),
                "tp_hit": bool(tp_hit),
                "bars_to_tp": bars_to_tp if bars_to_tp is not None else np.nan,
                "ttl_return": ttl_ret,
                "mfe": mfe,
                "mae": mae,
                "exit_reason_label": exit_reason,
            })
        except Exception as e:
            errors.append((row.get("symbol","?"), str(e)))

    labels = pd.DataFrame(rows).drop_duplicates(subset=["symbol","entry_time"]).reset_index(drop=True)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    labels.to_parquet(args.out, index=False)

    if args.features and args.out_features:
        wide = pd.read_parquet(args.features)
        t0 = extract_t0_features(wide)
        if "entry_time" not in t0.columns:
            tc = detect_time_col(t0)
            t0 = t0.rename(columns={tc: "entry_time"})
        t0["entry_time"] = pd.to_datetime(t0["entry_time"])
        merged = (t0.merge(labels, on=["symbol","entry_time"], how="inner")
                    .drop_duplicates(subset=["symbol","entry_time"]))
        merged.to_parquet(args.out_features, index=False)

    total = len(bo); labeled = len(labels)
    tp_rate = float(labels["tp_hit"].mean()) if labeled else 0.0
    print(f"[label_success_v2] processed={total}, labeled={labeled}, tp_rate={tp_rate:.3f}")
    if errors:
        print(f"[label_success_v2] {len(errors)} rows skipped.")
        for e in errors[:10]:
            print("  ->", e)

    cfg_path = str(Path(args.out).with_suffix("")) + "_config.json"
    with open(cfg_path, "w") as f:
        json.dump({
            "tp_map": tp_map,
            "ttl_map": ttl_map,
            "entry_fill": args.entry_fill,
            "breakouts_source": os.path.abspath(args.breakouts),
            "prices_root": os.path.abspath(args.prices_root),
        }, f, indent=2)

if __name__ == "__main__":
    main()
