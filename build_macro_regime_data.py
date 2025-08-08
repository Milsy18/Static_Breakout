import pandas as pd
from pathlib import Path

RAW = Path("Data/Raw")

def load_series(fname, out_name, value_col=None):
    df = pd.read_csv(RAW / fname)
    # normalize headers
    df.columns = [str(c).lower().strip().replace(".", "_") for c in df.columns]
    if "date" not in df.columns and "time" in df.columns:
        df = df.rename(columns={"time": "date"})
    if "date" not in df.columns:
        raise ValueError(f"{fname}: no 'date' or 'time' column")

    # choose the first non-date column if not specified
    if value_col is None:
        value_col = next(c for c in df.columns if c != "date")

    out = df[["date", value_col]].copy()
    out = out.rename(columns={value_col: out_name})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out

def main():
    btc   = load_series("btc_d.csv",   "btc_d")
    usdt  = load_series("usdt_d.csv",  "usdt_d")
    total = load_series("total.csv",   "total_cap")
    t3    = load_series("total3.csv",  "total3")

    df = btc.merge(usdt,  on="date", how="outer") \
            .merge(total, on="date", how="outer") \
            .merge(t3,    on="date", how="outer")

    # coerce numeric + fill tiny gaps
    for c in ["btc_d", "usdt_d", "total_cap", "total3"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("date")
    df[["btc_d","usdt_d","total_cap","total3"]] = (
        df[["btc_d","usdt_d","total_cap","total3"]].ffill().bfill()
    )

    # write clean ISO date
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    out = RAW / "macro_regime_data.csv"
    df.to_csv(out, index=False)
    print("wrote", out, "rows:", len(df), "cols:", list(df.columns))

if __name__ == "__main__":
    main()
