import pandas as pd
from pathlib import Path

RAW = Path("Data/Raw")

# Accept lots of possible timestamp names
DATE_CANDIDATES = ("date", "time", "timestamp", "unix", "unixtime", "unix_time", "epoch", "epoch_ms")

def parse_datetime(series: pd.Series) -> pd.DatetimeIndex:
    """Parse a timestamp series that might be strings or UNIX epoch (s/ms)."""
    s = series
    if pd.api.types.is_numeric_dtype(s):
        s = pd.to_numeric(s, errors="coerce")
        unit = "ms" if s.dropna().median() > 1e11 else "s"
        dt = pd.to_datetime(s, unit=unit, utc=True).dt.tz_convert(None)
    else:
        dt = pd.to_datetime(s, errors="coerce", utc=True).dt.tz_convert(None)
    return dt

def load_series(fname: str, out_name: str, value_col: str = "close") -> pd.DataFrame:
    """
    Load a CSV with an OHLCV layout, parse timestamp, roll to day, and keep one row/day.
    value_col defaults to 'close' (works for your files).
    """
    df = pd.read_csv(RAW / fname)
    df.columns = [str(c).lower().strip().replace(".", "_") for c in df.columns]

    date_col = next((c for c in DATE_CANDIDATES if c in df.columns), None)
    if not date_col:
        raise ValueError(f"{fname}: no date-like column found; saw columns {list(df.columns)}")

    # Parse timestamp → daily
    dt = parse_datetime(df[date_col]).dt.floor("D")

    # Choose the value column and make numeric
    if value_col not in df.columns:
        # fall back to “first non-date column”
        value_col = next(c for c in df.columns if c != date_col)
    val = pd.to_numeric(df[value_col], errors="coerce")

    out = pd.DataFrame({"date": dt, out_name: val}).dropna(subset=["date"])
    # Deduplicate within a day (take last non-null per day after sorting)
    out = out.sort_values("date").groupby("date", as_index=False).last()
    return out

def main():
    btc   = load_series("btc_d.csv",   "btc_d",   value_col="close")
    usdt  = load_series("usdt_d.csv",  "usdt_d",  value_col="close")
    total = load_series("total.csv",   "total_cap", value_col="close")
    t3    = load_series("total3.csv",  "total3",  value_col="close")

    # Outer-join on calendar day (now unique) and forward/back fill small gaps
    df = (btc.merge(usdt,  on="date", how="outer")
             .merge(total, on="date", how="outer")
             .merge(t3,    on="date", how="outer")
             .sort_values("date"))

    for c in ["btc_d", "usdt_d", "total_cap", "total3"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df[["btc_d","usdt_d","total_cap","total3"]] = df[["btc_d","usdt_d","total_cap","total3"]].ffill().bfill()

    # Pretty date for CSV
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    out = RAW / "macro_regime_data.csv"
    df.to_csv(out, index=False)
    print(f"wrote {out} rows:{len(df)} cols:{list(df.columns)}")

if __name__ == "__main__":
    main()
