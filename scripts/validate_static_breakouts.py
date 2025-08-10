import sys
from pathlib import Path
import pandas as pd

fp = Path("Data/Processed/static_breakouts.csv")
rp = Path("validation_report.md")

if not fp.exists():
    print(f"❌ File not found: {fp}")
    sys.exit(1)

df = pd.read_csv(fp)

required = [
    "symbol","entry_date","entry_price","exit_date","exit_price","exit_reason",
    "market_level_at_entry",
    "score_trd","score_vty","score_vol","score_mom","score_total",
    "success","days_in_trade","pct_return","source"
]

missing = [c for c in required if c not in df.columns]
extra   = [c for c in df.columns if c not in required]

def safe_value_counts(col):
    if col in df.columns:
        vc = df[col].value_counts()
        return vc.to_string()
    return f"(column '{col}' not present)"

with open(rp, "w", encoding="utf-8") as f:
    f.write("# Static Breakouts — Validation Report\n\n")
    f.write(f"**File:** {fp}\n")
    f.write(f"**Rows:** {len(df):,}\n\n")

    f.write("## Columns present\n")
    f.write(", ".join(df.columns) + "\n\n")

    f.write("## Required columns check\n")
    f.write(f"- Missing: {missing if missing else 'None'}\n")
    f.write(f"- Extra: {extra if extra else 'None'}\n\n")

    f.write("## Null counts (top 20)\n")
    f.write(df.isnull().sum().sort_values(ascending=False).head(20).to_string() + "\n\n")

    f.write("## Top 10 symbols by count\n")
    if "symbol" in df.columns:
        f.write(df["symbol"].value_counts().head(10).to_string() + "\n\n")
    else:
        f.write("(column 'symbol' not present)\n\n")

    f.write("## Exit reason distribution\n")
    f.write(safe_value_counts("exit_reason") + "\n\n")

    f.write("## Sanity checks\n")
    checks = []
    # no future exit dates before entry
    if all(col in df.columns for col in ["entry_date","exit_date"]):
        try:
            dfe = df.copy()
            dfe["entry_date"] = pd.to_datetime(dfe["entry_date"], errors="coerce")
            dfe["exit_date"]  = pd.to_datetime(dfe["exit_date"], errors="coerce")
            bad = (dfe["exit_date"] < dfe["entry_date"]).sum()
            checks.append(f"- Rows with exit_date < entry_date: {bad}")
        except Exception as e:
            checks.append(f"- Date comparison skipped (error: {e})")
    else:
        checks.append("- Date comparison skipped (missing entry_date/exit_date)")
    f.write("\n".join(checks) + "\n")

print("✅ validation_report.md written")
