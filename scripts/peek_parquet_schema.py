import sys
from pathlib import Path
import pandas as pd

targets = [
    ("long_only", Path("Data/Processed/holy_grail_static_221_long_only.parquet")),
    ("labeled_windows", Path("Data/Processed/labeled_holy_grail_static_221_windows_long.parquet")),
]

for name, fp in targets:
    if not fp.exists():
        print(f"❌ Missing: {fp}")
        continue
    try:
        df = pd.read_parquet(fp)
    except Exception as e:
        print(f"⚠️ Could not read {fp} (parquet engine missing?). Error: {e}")
        print("   Try: .\\.venv\\Scripts\\python.exe -m pip install pyarrow")
        continue
    print(f"\n=== {name} ===")
    print(f"file: {fp}")
    print(f"shape: {df.shape}")
    print("columns:")
    for c in df.columns:
        print(f" - {c}")
