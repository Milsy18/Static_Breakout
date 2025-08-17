import pandas as pd
from pathlib import Path

# Resolve data root from config (falls back to the junction path if needed)
try:
    from config import get_data_root
    root = Path(get_data_root())
except Exception:
    root = Path(r"C:\Users\milla\Static_Breakout\Data\Processed")

files = [
    "final_breakout_trades_tp_policy.csv",
    "final_merged_with_holy_grail.csv",
    "final_merged_with_holy_grail_static.csv",
]

rows = []
for name in files:
    p = root / name
    rec = {"file": name, "path": str(p), "exists": p.exists()}
    if p.exists():
        rec["size_mb"] = round(p.stat().st_size / 1_000_000, 2)
        # These files are ~thousands of rows, so a full read is fine.
        df = pd.read_csv(p, low_memory=False)
        rec["rows"] = len(df)
        rec["cols"] = df.shape[1]
    rows.append(rec)

out = pd.DataFrame(rows, columns=["file","path","exists","rows","cols","size_mb"])
out_path = root / "_tracker_outputs.csv"
out.to_csv(out_path, index=False)

print(out.to_string(index=False))
print("\nWrote ->", out_path)
