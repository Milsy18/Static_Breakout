from pathlib import Path
import json, pandas as pd

HERE = Path(__file__).resolve().parent
CLEAN = HERE / "trades_clean_for_exit_tests.csv"
MANIFEST = HERE / "bars_build_manifest.json"

def build_trade_bars(trades: pd.DataFrame) -> dict:
    return {"total_trades": len(trades), "bars_built": 0, "ohlcv_root": None}

def main():
    if not CLEAN.exists():
        raise FileNotFoundError(f"Missing {CLEAN.name}. Generate it first.")
    trades = pd.read_csv(CLEAN, parse_dates=["entry_time","exit_time"])
    if trades.empty:
        raise ValueError("Clean trades file is empty.")
    report = build_trade_bars(trades)
    MANIFEST.write_text(json.dumps({"ok": True, "report": report}, indent=2))
    print(f"[bars] Manifest written: {MANIFEST.name}")
    print(f"[bars] Summary: {report}")

if __name__ == "__main__":
    main()
