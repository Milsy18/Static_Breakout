from pathlib import Path
import os, sys

def get_data_root() -> Path:
    p = os.environ.get("M18_DATA")
    if not p:
        sys.exit("ERROR: M18_DATA is not set. Point it to your data\\Processed folder.")
    root = Path(p)
    if not root.exists():
        sys.exit(f"ERROR: M18_DATA points to a non-existent path: {root}")
    return root
