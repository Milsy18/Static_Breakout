import pandas as pd
from pathlib import Path

RAW = Path("Data/Raw")

def load_series(fname: str, out_name: str, value_col: str | None = None) -> pd.DataFrame:
    """Load a 2+ column csv, normalize headers, coerce date to daily,
    collapse duplicate calendar days, and return ['date', out_name]."""
    p = RAW / fname
    df = pd.read_csv(p)

    # normalize headers
    df.columns = [str(c).lower().strip().replace(".", "_") for c in df.columns]

    # find/rename date-like column
    for cand in ("date", "time", "timestamp"):
        if cand in df.columns:
            if cand != "date":
                df = df.rename(columns={cand: "date"})
            break
    else:
        raise ValueError(f"{fname}: no 'date'/'time'/'timestamp' column found")

    # choose value column if not supplied
    if value_col is None:
        # prefer common names; otherwise first non-date column
        prefs = ["value", "close", out_name]
        non_date = [c for c in df.columns if c != "date"]
        for c in prefs:
            if c in df.columns and c != "date":
                value_col = c
                break
        if value_col is None:
            value_col = non_date[0]

    # parse datetime and floor to day
    s = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_convert(None)
    df["date"] = s.dt.floor("D")

    # keep only date + chosen value
    out = (
        df[["date", value_col]]
        .copy()
        .sort_values("date")
        .dropna(subset=["date"])
    )

    # numeric value, allow NaN
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")

    # collapse duplicates per calendar day (choose last observation of day)
    dup_cnt = int(out.duplicated(subset=["date"]).sum())
    if dup_cnt:
        print(f"[{fname}] collapsed {dup_cnt} duplicate day rows -> daily using last()")
    out = out.groupby("date", as_index=False)[value_col].last()

    # rename to unified name
    out = out.rename(columns={value_col: out_name})
    return out

def main():
    btc   = load_series("btc_d.csv",   "btc_d")
    usdt  = load_series("usdt_d.csv",  "usdt_d")
    total = load_series("total.csv",   "total_cap")
    t3    = load_series("total3.csv",  "total3")

    # merge on daily date
    df = (
        btc.merge(usdt,  on="date", how="outer")
           .merge(total, on="date", how="outer")
           .merge(t3,    on="date", how="outer")
           .sort_values("date")
           .reset_index(drop=True)
    )

    # forward/back fill across gaps
    for c in ["btc_d", "usdt_d", "total_cap", "total3"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[["btc_d", "usdt_d", "total_cap", "total3"]] = (
        df[["btc_d", "usdt_d", "total_cap", "total3"]].ffill().bfill()
    )

    # nice date format
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    out = RAW / "macro_regime_data.csv"
    df.to_csv(out, index=False)
    print("wrote", out, "| rows:", len(df), "| cols:", list(df.columns))

if __name__ == "__main__":
    main()

