
# scripts/python/gen_trades_clean.py
# Purpose: Normalize a trades file into ./trades_clean_for_exit_tests.csv for the exit harness.

from __future__ import annotations
from pathlib import Path
import argparse
import re
import time
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────────────────────────────────────
# Paths
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent              # project root (C:\Users\milla\static_breakout)
OUT  = ROOT / "trades_clean_for_exit_tests.csv"

# ──────────────────────────────────────────────────────────────────────────────
# Candidate file patterns if --src not provided (searched from ROOT)
CANDIDATE_PATTERNS = [
    r"exit_out/trades_with_exit_final\.csv$",
    r"out/trades_realized\.csv$",
    r".*trades.*realized.*\.csv$",
    r".*backtest.*trades.*\.csv$",
    r".*trades.*export.*\.csv$",
    r".*trades.*\.csv$",
]

# Column aliases (case-insensitive regex) → standardized names
ALIASES = {
    "symbol":       [r"^symbol$", r"^ticker$", r"^asset$"],
    "side":         [r"^side$", r"^direction$", r"^is_long$", r"^longshort$"],
    "entry_time":   [r"^entry_?time$", r"^time_?in$", r"^open_?time$", r"^timestamp_?in$", r"^entry_?date$", r"^date$"],
    "exit_time":    [r"^exit_?time$", r"^time_?out$", r"^close_?time$", r"^timestamp_?out$", r"^exit_?date$"],
    "entry_price":  [r"^entry_?price$", r"^price_?in$", r"^open_?price$"],
    "exit_price":   [r"^exit_?price$", r"^price_?out$", r"^close_?price$"],
    "qty":          [r"^qty$", r"^quantity$", r"^size$", r"^position_?size$", r"^contracts$"],
    "pnl":          [r"^pnl$", r"^profit$", r"^net_?pnl$", r"^realized_?pnl$", r"^exit_?ret$"],
    "trade_id":     [r"^trade_?id$", r"^id$", r"^order_?id$"],
    "m18_score":    [r"^m18_?score$", r"^score$", r"^align100$"],
    "conf100":      [r"^conf100$", r"^confidence$", r"^ready_?score$"],
}

# Final ordered columns expected by the downstream harness
KEEP_ORDER = [
    "trade_id", "symbol", "side",
    "entry_time", "entry_price",
    "exit_time", "exit_price",
    "qty", "pnl", "duration_mins",
    "m18_score", "conf100",
]

# ──────────────────────────────────────────────────────────────────────────────
def _find_col(cols: list[str], patterns: list[str]) -> str | None:
    for c in cols:
        for pat in patterns:
            if re.search(pat, c, flags=re.I):
                return c
    return None

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Rename using aliases
    mapping = {}
    cols = list(df.columns)
    for std, pats in ALIASES.items():
        c = _find_col(cols, pats)
        if c:
            mapping[c] = std
    df = df.rename(columns=mapping)

    # 2) Parse datetimes if present
    for dtcol in ["entry_time", "exit_time"]:
        if dtcol in df.columns:
            df[dtcol] = pd.to_datetime(df[dtcol], errors="coerce", utc=True).dt.tz_convert(None)

    # 3) Coerce numerics
    for num in ["entry_price", "exit_price", "qty", "pnl", "m18_score", "conf100"]:
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce")

    # 4) Defaults if missing
    if "qty" not in df.columns:
        df["qty"] = 1.0
    if "side" not in df.columns:
        df["side"] = "LONG"
    if "trade_id" not in df.columns:
        df["trade_id"] = np.arange(1, len(df) + 1, dtype=int)

    # 5) Compute pnl if still missing and we have prices
    if "pnl" not in df.columns:
        if {"entry_price", "exit_price", "qty", "side"}.issubset(df.columns):
            sign = np.where(df["side"].astype(str).str.upper().eq("LONG"), 1.0, -1.0)
            df["pnl"] = (df["exit_price"] - df["entry_price"]) * df["qty"].fillna(1.0) * sign
        else:
            df["pnl"] = np.nan

    # 6) Duration in minutes
    if "entry_time" in df.columns and "exit_time" in df.columns:
        dur = (df["exit_time"] - df["entry_time"]).dt.total_seconds() / 60.0
        df["duration_mins"] = dur
    else:
        df["duration_mins"] = np.nan

    # 7) Ensure final schema/order
    for k in KEEP_ORDER:
        if k not in df.columns:
            df[k] = np.nan
    out = df[KEEP_ORDER].sort_values("entry_time", na_position="last").reset_index(drop=True)
    return out

def _choose_source_from_arg_or_search(arg_path: str | None) -> Path:
    if arg_path:
        p = Path(arg_path).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"--src not found: {p}")
        return p

    # Try explicit patterns first
    matches: list[Path] = []
    for pat in CANDIDATE_PATTERNS:
        # Convert our regex-like pattern to actual glob by stripping anchors if present
        # and just scanning all CSVs to filter via regex:
        pass
    # Glob all csv/xlsx/parquet then regex-filter
    all_files = list(ROOT.rglob("*"))
    candidates = []
    for p in all_files:
        if not p.is_file():
            continue
        if p.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls", ".parquet"}:
            candidates.append(p)

    # Rank by last modified time, favor names matching 'trade|backtest|export'
    def score(p: Path) -> tuple[int, float]:
        name = p.name.lower()
        name_score = 1 if re.search(r"(trade|backtest|export)", name) else 0
        return (name_score, p.stat().st_mtime)

    candidates.sort(key=score, reverse=True)

    if not candidates:
        raise FileNotFoundError("No candidate files found (.csv/.tsv/.xlsx/.parquet) in project.")
    return candidates[0]

def _load_any(src: Path) -> pd.DataFrame:
    suf = src.suffix.lower()
    if suf == ".parquet":
        return pd.read_parquet(src)
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(src)
    if suf == ".tsv":
        return pd.read_csv(src, sep="\t")
    # default CSV
    return pd.read_csv(src)

def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize trades file for exit tests.")
    ap.add_argument("--src", help="Path to trades CSV/TSV/Parquet/Excel")
    args = ap.parse_args()

    src = _choose_source_from_arg_or_search(args.src)
    print(f"[gen] Using source: {src}")

    df = _load_any(src)
    if df.empty:
        raise ValueError("Source file loaded but is empty.")

    clean = _normalize(df)
    if clean.empty:
        raise ValueError("Normalized DataFrame is empty—check source columns.")

    OUT.write_text("")  # ensure file is created even if write fails late
    clean.to_csv(OUT, index=False)
    print(f"[gen] Wrote {OUT} with {len(clean)} rows.")
    print(f"[gen] Last modified: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OUT.stat().st_mtime))}")

if __name__ == "__main__":
    main()
