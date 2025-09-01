from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC  = ROOT / "trades_clean_for_exit_tests.csv"
DST  = ROOT / "trades_clean_exit_compat.csv"

df = pd.read_csv(SRC, parse_dates=["entry_time","exit_time"])
if "date" not in df.columns and "entry_time" in df.columns:
    df = df.rename(columns={"entry_time": "date"})
df.to_csv(DST, index=False)
print(f"[compat] Wrote {DST} with {len(df)} rows.")
