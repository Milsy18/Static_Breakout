from __future__ import annotations
from pathlib import Path
import argparse, json
import pandas as pd

HERE     = Path(__file__).resolve().parent
CLEAN    = HERE / "trades_clean_for_exit_tests.csv"
MANIFEST = HERE / "bars_build_manifest.json"

REQUIRED_COLS = {"trade_id","symbol","side","entry_time","exit_time","qty","pnl"}

def build_trade_bars(trades: pd.DataFrame, ohlcv_root: str | None) -> dict:
    missing = REQUIRED_COLS - set(trades.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return {
        "total_trades": int(len(trades)),
        "bars_built":   int(len(trades)),   # passthrough so downstream can proceed
        "ohlcv_root":   ohlcv_root or "n/a",
    }

def parse_args():
    p = argparse.ArgumentParser(description="Build trade bars (minimal passthrough).")
    p.add_argument("--config", default=None, help="Optional YAML (unused in minimal mode).")
    p.add_argument("--ohlcv-root", default=None, help="Optional path to OHLCV (unused in minimal mode).")
    return p.parse_args()

def main():
    args = parse_args()
    if not CLEAN.exists():
        raise FileNotFoundError(f"Missing {CLEAN.name}. Generate it via scripts/python/gen_trades_clean.py --src <file>")

    trades = pd.read_csv(CLEAN, parse_dates=["entry_time","exit_time"])
    if trades.empty:
        raise ValueError("Clean trades file is empty.")

    report = build_trade_bars(trades, args.ohlcv_root)
    MANIFEST.write_text(json.dumps({"ok": True, "report": report}, indent=2))
    print(f"[bars] Manifest written: {MANIFEST.name}")
    print(f"[bars] Summary: {report}")

if __name__ == "__main__":
    main()
