#!/usr/bin/env python3
"""
Label each rolling window with its exit time/price/reason, plus binary flags.
"""

import math
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm

SRC_PARQUET = Path("holy_grail_static_221_windows_long.parquet")
OUT_PARQUET = Path("labeled_holy_grail_static_221_windows_long.parquet")

# regime-aware exit parameters from M18
_TP_PCT = {1: .45, 2: .44, 3: .42, 4: .40, 5: .38, 6: .36, 7: .35, 8: .34, 9: .33}
_MAXBAR = {1: 11,  2: 10,  3:  9,  4:  8,  5:  7,  6:  7,  7:  6,  8:  6,  9:  5}

def _safe_level(val) -> int:
    """Convert market_level to integer 1–9, default=5 on failure."""
    try:
        lvl = int(round(float(val)))
        return max(1, min(9, lvl))
    except Exception:
        return 5

def compute_exit(row: pd.Series):
    """Compute (exit_time, exit_price, exit_reason) per M18 logic."""
    sym = row.get("symbol", "")
    if not isinstance(sym, str) or not sym:
        return pd.NaT, np.nan, "MISSING"

    # parse entry_date
    ent_dt = row.get("entry_date", pd.NaT)
    if not pd.api.types.is_datetime64_any_dtype(type(ent_dt)) and pd.isna(ent_dt):
        return pd.NaT, np.nan, "MISSING"

    # coerce entry_price → float
    try:
        ent_px = float(row.get("entry_price"))
    except Exception:
        return pd.NaT, np.nan, "MISSING"

    lvl = _safe_level(row.get("market_level", 5))
    tp_pct = _TP_PCT[lvl]
    max_b = _MAXBAR[lvl]
    tp_target = ent_px * (1 + tp_pct)

    # gather the next bars' close-prices
    closes = []
    for i in range(max_b + 1):
        key = sym if i == 0 else f"{sym}.{i}"
        val = row.get(key, np.nan)
        try:
            closes.append(float(val))
        except Exception:
            closes.append(np.nan)

    # 1) TP exit
    for i, px in enumerate(closes[1:], start=1):
        if not np.isnan(px) and px >= tp_target:
            return ent_dt + pd.Timedelta(hours=i), px, "TP"

    # 2) TIME exit at last valid bar ≤ max_b
    valid = [i for i, px in enumerate(closes[1:], start=1) if not np.isnan(px)]
    last_i = valid[-1] if valid else 0
    idx = min(last_i, max_b)
    exit_px = closes[idx] if idx and not np.isnan(closes[idx]) else ent_px
    return ent_dt + pd.Timedelta(hours=idx), exit_px, "TIME"

def main():
    print(f"Reading {SRC_PARQUET} …")
    pf = pq.ParquetFile(SRC_PARQUET)

    writer = None
    for rg in tqdm(range(pf.num_row_groups), desc="Row-groups"):
        df = pf.read_row_group(rg).to_pandas()

        exits = df.apply(compute_exit, axis=1, result_type="expand")
        exits.columns = ["exit_time", "exit_price", "exit_reason"]
        df = pd.concat([df, exits], axis=1)

        df["exit_label"]  = (df["exit_reason"] == "TP").astype("int8")
        df["success_bin"] = df["exit_label"]

        tbl = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(OUT_PARQUET, tbl.schema)
        writer.write_table(tbl)

    writer.close()
    print(f"✅  Wrote {OUT_PARQUET}")

if __name__ == "__main__":
    main()


