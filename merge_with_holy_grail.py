import argparse, os, re, numpy as np, pandas as pd
from pathlib import Path

def normalize_symbol(s):
    return str(s).upper().strip() if pd.notna(s) else s

def to_date_series(s):
    """Try several conversions to datetime, normalize to date."""
    s_obj = pd.Series(s)
    # try pandas parse
    dt = pd.to_datetime(s_obj, errors="coerce", utc=False, infer_datetime_format=True)
    if dt.notna().mean() < 0.2:
        # maybe epoch ms
        dt = pd.to_datetime(s_obj, errors="coerce", unit="ms", utc=False)
    if dt.notna().mean() < 0.2:
        # maybe epoch s
        dt = pd.to_datetime(s_obj, errors="coerce", unit="s", utc=False)
    return dt.dt.normalize()

def find_date_col(df):
    # look for anything date/time-ish
    cols = df.columns.tolist()
    cand = [c for c in cols if re.search(r"(date|time|timestamp|ts|stamp)", c, re.I)]
    if not cand:
        return None
    # score: prefer names containing breakout/signal/entry
    def score(c):
        name = c.lower()
        s = 0
        if "breakout" in name: s += 5
        if "signal"   in name: s += 4
        if "entry"    in name: s += 3
        if name in {"date","time","timestamp"}: s += 2
        if "d0" in name or "p0" in name: s += 1
        return s
    cand.sort(key=lambda c: (score(c), -len(c)), reverse=True)
    return cand[0]

def read_trades(path):
    d = pd.read_csv(path, low_memory=False)
    if "symbol" not in d:
        raise SystemExit("Trades file missing 'symbol'")
    # ensure breakout_date column
    if "breakout_date" not in d:
        for c in ["date","event_date","signal_date","entry_date"]:
            if c in d.columns:
                d = d.rename(columns={c:"breakout_date"})
                break
    if "breakout_date" not in d:
        raise SystemExit("Trades file missing a recognizable date column.")
    d["symbol"] = d["symbol"].map(normalize_symbol)
    d["breakout_date"] = to_date_series(d["breakout_date"])
    return d

def read_hg(path):
    g = pd.read_csv(path, low_memory=False)
    # map ticker->symbol if needed
    if "symbol" not in g.columns and "ticker" in g.columns:
        g = g.rename(columns={"ticker":"symbol"})
    if "symbol" not in g.columns:
        raise SystemExit("Holy-grail file missing 'symbol'/'ticker' column.")
    g["symbol"] = g["symbol"].map(normalize_symbol)

    # find a date-like column automatically
    col = None
    for k in ["breakout_date","date","event_date","signal_date","entry_date"]:
        if k in g.columns: col = k; break
    if col is None:
        col = find_date_col(g)
    if col is None:
        raise SystemExit("Holy-grail missing a date column (breakout_date/date/event_date/…); "
                         "no date-like column found.")
    if col != "breakout_date":
        g = g.rename(columns={col:"breakout_date"})
    g["breakout_date"] = to_date_series(g["breakout_date"])

    # If it's a windows-long table, pick window==0 (or nearest to 0)
    if "window" in g.columns:
        # accept variants like 0, "0", "d0", "p0"
        mask0 = g["window"].astype(str).str.lower().isin(["0","d0","p0"])
        if mask0.any():
            g = g.loc[mask0].copy()
        else:
            # choose window closest to 0 per (symbol,breakout_date)
            wnum = pd.to_numeric(g["window"], errors="coerce")
            g = g.assign(__wabs=wnum.abs())
            g = g.sort_values(["symbol","breakout_date","__wabs"]).drop_duplicates(["symbol","breakout_date"])
            g = g.drop(columns="__wabs")
    else:
        # if no window, ensure uniqueness for the join keys
        g = g.drop_duplicates(["symbol","breakout_date"])

    return g

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--hg", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    print("Reading trades ->", args.trades)
    tr = read_trades(args.trades)
    print("Reading holy grail ->", args.hg)
    hg = read_hg(args.hg)

    out = tr.merge(hg, on=["symbol","breakout_date"], how="left", suffixes=("","_hg"))

    # coverage of HG fields
    hg_cols = [c for c in out.columns if c.endswith("_hg")]
    matched = out[hg_cols].notna().any(axis=1) if hg_cols else pd.Series(False, index=out.index)
    print(f"Merge coverage: {int(matched.sum())}/{len(out)} = {matched.mean():.1%}")

    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print("Wrote ->", args.out)

if __name__ == "__main__":
    main()
