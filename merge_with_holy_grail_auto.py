import argparse, os, re
from pathlib import Path
import numpy as np
import pandas as pd


# --------------------------- helpers ---------------------------

def norm_sym(s):
    """Normalize symbols across sources."""
    if pd.isna(s):
        return s
    s = str(s).upper().strip()
    s = re.sub(r'[^A-Z0-9]', '', s)   # OG/USDT -> OGUSDT, BTC-USDT -> BTCUSDT
    s = re.sub(r'(PERP)$', '', s)     # strip trailing PERP
    return s


def to_date(series):
    """Coerce a variety of date/time representations to normalized dates."""
    ser = pd.Series(series)
    for kwargs in (
        dict(errors="coerce"),            # regular parse
        dict(errors="coerce", unit="ms"), # epoch ms
        dict(errors="coerce", unit="s"),  # epoch s
    ):
        dt = pd.to_datetime(ser, **kwargs)
        if dt.notna().sum() >= 0.20 * len(dt):   # take it if it works for >=20%
            return dt.dt.normalize()
    # last resort (still normalized)
    return pd.to_datetime(ser, errors="coerce").dt.normalize()


def best_date_col(
    df,
    prefer=("breakout_date","date","event_date","signal_date","entry_date","timestamp","time","peak_date"),
):
    have = [c for c in prefer if c in df.columns]
    if have:
        return have[0]
    cand = [c for c in df.columns if re.search(r"(date|time|stamp)", c, re.I)]
    return cand[0] if cand else None


# --------------------------- loaders ---------------------------

def read_trades(path):
    d = pd.read_csv(path, low_memory=False)

    if "symbol" not in d.columns:
        raise SystemExit("Trades file missing 'symbol'")

    d["sym_std"] = d["symbol"].map(norm_sym)

    tdcol = best_date_col(d)
    if tdcol is None:
        raise SystemExit("Trades file missing any date-like column")
    d["trade_date"] = to_date(d[tdcol])

    # optional peak date on trades
    d["trade_peak_date"] = to_date(d["peak_date"]) if "peak_date" in d.columns else pd.NaT

    print(f"[TRADES] using date col: {tdcol} | non-null rate={d['trade_date'].notna().mean():.1%}")
    print(f"[TRADES] peak_date present: {'peak_date' in d.columns} | non-null rate={d['trade_peak_date'].notna().mean():.1%}")
    return d


def read_hg(path):
    g = pd.read_csv(path, low_memory=False)

    # normalize symbol column
    if "symbol" not in g.columns and "ticker" in g.columns:
        g = g.rename(columns={"ticker": "symbol"})
    if "symbol" not in g.columns:
        raise SystemExit("Holy-grail file missing 'symbol'/'ticker'")
    g["sym_std"] = g["symbol"].map(norm_sym)

    # main/date-like column on HG
    hdcol = best_date_col(g)
    g["hg_date"] = to_date(g[hdcol]) if hdcol else pd.NaT

    # explicit peak date if present
    if "peak_date" in g.columns:
        g["hg_peak_date"] = to_date(g["peak_date"])
    else:
        g["hg_peak_date"] = pd.NaT

    # derive entry date when possible: hg_entry_date = hg_peak_date - duration_days
    if "duration_days" in g.columns and g["hg_peak_date"].notna().any():
        dur = pd.to_numeric(g["duration_days"], errors="coerce")
        g["hg_entry_date"] = (g["hg_peak_date"] - pd.to_timedelta(dur, unit="D")).dt.normalize()
    else:
        g["hg_entry_date"] = pd.NaT

    # collapse windows tables: keep window==0/d0/p0 if present, else the closest to 0
    if "window" in g.columns:
        wtxt = g["window"].astype(str).str.lower()
        pick0 = wtxt.isin(["0", "d0", "p0"])
        if pick0.any():
            g = g.loc[pick0].copy()
        else:
            wnum = pd.to_numeric(g["window"], errors="coerce")
            g = (g.assign(__wabs=wnum.abs())
                   .sort_values(["sym_std", "hg_date", "__wabs"])
                   .drop_duplicates(["sym_std", "hg_date"], keep="first")
                   .drop(columns="__wabs", errors="ignore"))

    print(f"[HG] using date col: {hdcol if hdcol else 'None'} | non-null rate={g['hg_date'].notna().mean():.1%}")
    print(f"[HG] peak_date present: {'peak_date' in g.columns} | non-null rate={g['hg_peak_date'].notna().mean():.1%}")
    print(f"[HG] derived entry_date non-null rate={g['hg_entry_date'].notna().mean():.1%}")
    return g


# --------------------------- merges ---------------------------

def merge_exact(left, right, ldate, rdate):
    """Exact-date merge on normalized symbols and date columns."""
    m = left.merge(
        right, how="left",
        left_on=["sym_std", ldate],
        right_on=["sym_std", rdate],
        suffixes=("", "_hg"),
    )
    cov = m.filter(regex="_hg$").notna().any(axis=1).mean()
    return m, cov


def merge_nearest(left, right, ldate, rdate, tolerance="1D"):
    """Nearest-date (±tolerance) merge by symbol."""
    L = left[["sym_std", ldate]].dropna().rename(columns={ldate: "ld"}).sort_values(["ld", "sym_std"])
    R = right[["sym_std", rdate]].dropna().rename(columns={rdate: "rd"}).sort_values(["rd", "sym_std"])
    if R.empty:
        raise ValueError("Right key is all null; cannot do nearest-date merge.")

    pairs = pd.merge_asof(
        L, R,
        left_on="ld", right_on="rd", by="sym_std",
        direction="nearest", tolerance=pd.Timedelta(tolerance),
    )
    pairs = pairs.loc[pairs["rd"].notna(), ["sym_std", "ld", "rd"]]

    out = (left.assign(ld=left[ldate])
               .merge(pairs, on=["sym_std", "ld"], how="left")
               .merge(right.assign(rd=right[rdate]), on=["sym_std", "rd"], how="left", suffixes=("", "_hg"))
               .drop(columns=["ld", "rd"]))
    cov = out.filter(regex="_hg$").notna().any(axis=1).mean()
    return out, cov


# --------------------------- main ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--hg",     required=True)
    ap.add_argument("--out",    required=True)
    ap.add_argument("--tol",    default="1D", help="nearest-date tolerance, e.g. 1D, 2D, 12H")
    args = ap.parse_args()

    print("Reading trades ->", args.trades)
    tr = read_trades(args.trades)
    print("Reading holy grail ->", args.hg)
    hg = read_hg(args.hg)

    attempts = []
    # Try entry-date first (best for windowed HG files)
    attempts.append((f"EXACT trade_date vs hg_entry_date",                "trade_date",       "hg_entry_date", "exact"))
    attempts.append((f"NEAREST ±{args.tol} trade_date vs hg_entry_date", "trade_date",       "hg_entry_date", "nearest"))

    # Peak-date alignment
    attempts.append((f"EXACT trade_peak_date vs hg_peak_date",               "trade_peak_date", "hg_peak_date", "exact"))
    attempts.append((f"NEAREST ±{args.tol} trade_peak_date vs hg_peak_date","trade_peak_date", "hg_peak_date", "nearest"))

    # Fallback to HG's primary date column
    attempts.append((f"EXACT trade_date vs hg_date",               "trade_date", "hg_date", "exact"))
    attempts.append((f"NEAREST ±{args.tol} trade_date vs hg_date","trade_date", "hg_date", "nearest"))

    best_cov, best_out = -1.0, None
    for label, ldate, rdate, kind in attempts:
        if ldate not in tr.columns or rdate not in hg.columns:
            print(f"[{label}] skipped (missing columns)")
            continue
        try:
            if kind == "exact":
                tmp, cov = merge_exact(tr, hg, ldate, rdate)                    # <-- FIX: no tol here
            else:
                tmp, cov = merge_nearest(tr, hg, ldate, rdate, args.tol)        # tol only for nearest
            print(f"[{label}] coverage: {cov:.1%}")
        except Exception as e:
            print(f"[{label}] failed:", e)
            continue
        if cov > best_cov:
            best_cov, best_out = cov, tmp

    if best_out is None:
        raise SystemExit("All merge attempts failed.")

    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    best_out.to_csv(args.out, index=False)
    print(f"Wrote -> {args.out} | best coverage: {best_cov:.1%}")


if __name__ == "__main__":
    main()
