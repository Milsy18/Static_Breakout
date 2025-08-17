import argparse, os, re
from pathlib import Path
import numpy as np
import pandas as pd
import zipfile

# ---------- utility to read CSV or CSV-inside-zip ----------
def read_csv_maybe_zip(path, low_memory=False):
    p = str(path)
    if p.lower().endswith(".zip"):
        with zipfile.ZipFile(p) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise SystemExit("Zip contains no CSV.")
            # Prefer file that looks like 'holy' + 'static' if present
            pick = None
            for n in names:
                ln = n.lower()
                if "holy" in ln and "static" in ln:
                    pick = n; break
            if pick is None:
                pick = names[0]
            with z.open(pick) as f:
                df = pd.read_csv(f, low_memory=low_memory)
            print(f"[ZIP] using member: {pick}")
            return df
    return pd.read_csv(p, low_memory=low_memory)

def norm_sym(s):
    if pd.isna(s): return s
    s = str(s).upper().strip()
    s = re.sub(r'[^A-Z0-9]', '', s)
    s = re.sub(r'(PERP)$', '', s)
    return s

def to_date(series):
    ser = pd.Series(series)
    for kwargs in (dict(errors="coerce"), dict(errors="coerce", unit="ms"), dict(errors="coerce", unit="s")):
        dt = pd.to_datetime(ser, **kwargs)
        if dt.notna().sum() >= 0.2 * len(dt):
            return dt.dt.normalize()
    return pd.to_datetime(ser, errors="coerce").dt.normalize()

def best_date_col(df, prefer=("breakout_date","trade_date","date","event_date","signal_date",
                              "entry_date","timestamp","time","peak_date")):
    have = [c for c in prefer if c in df.columns]
    if have: return have[0]
    cand = [c for c in df.columns if re.search(r"(date|time|stamp)", c, re.I)]
    return cand[0] if cand else None

def read_trades(path):
    d = read_csv_maybe_zip(path, low_memory=False)
    if "symbol" not in d.columns: raise SystemExit("Trades file missing 'symbol'")
    d["sym_std"] = d["symbol"].map(norm_sym)
    tdcol = "breakout_date" if "breakout_date" in d.columns else best_date_col(d)
    if tdcol is None: raise SystemExit("Trades file missing any date-like column")
    d["trade_date"] = to_date(d[tdcol])
    d["trade_peak_date"] = to_date(d["peak_date"]) if "peak_date" in d.columns else pd.NaT
    print(f"[TRADES] using date col: {tdcol} | non-null rate={d['trade_date'].notna().mean():.1%}")
    print(f"[TRADES] peak_date present: {'peak_date' in d.columns} | non-null rate={d['trade_peak_date'].notna().mean():.1%}")
    return d

def read_hg(path):
    g = read_csv_maybe_zip(path, low_memory=False)
    if "symbol" not in g.columns and "ticker" in g.columns:
        g = g.rename(columns={"ticker":"symbol"})
    if "symbol" not in g.columns: raise SystemExit("Holy-grail file missing 'symbol'/'ticker'")
    g["sym_std"] = g["symbol"].map(norm_sym)

    hdcol = "breakout_date" if "breakout_date" in g.columns else best_date_col(g)
    g["hg_date"] = to_date(g[hdcol]) if hdcol else pd.NaT
    g["hg_peak_date"] = to_date(g["peak_date"]) if "peak_date" in g.columns else pd.NaT

    if "duration_days" in g.columns and g["hg_peak_date"].notna().any():
        dur = pd.to_numeric(g["duration_days"], errors="coerce")
        g["hg_entry_date"] = (g["hg_peak_date"] - pd.to_timedelta(dur, unit="D")).dt.normalize()
    else:
        g["hg_entry_date"] = pd.NaT

    if "window" in g.columns:
        wtxt = g["window"].astype(str).str.lower()
        pick0 = wtxt.isin(["0","d0","p0"])
        if pick0.any():
            g = g.loc[pick0].copy()
        else:
            wnum = pd.to_numeric(g["window"], errors="coerce")
            g = g.assign(__wabs=wnum.abs()).sort_values(["sym_std","hg_date","__wabs"])
            g = g.drop_duplicates(["sym_std","hg_date"], keep="first").drop(columns="__wabs", errors="ignore")

    print(f"[HG] using date col: {hdcol if hdcol else 'None'} | non-null rate={g['hg_date'].notna().mean():.1%}")
    print(f"[HG] peak_date present: {'peak_date' in g.columns} | non-null rate={g['hg_peak_date'].notna().mean():.1%}")
    print(f"[HG] derived entry_date non-null rate={g['hg_entry_date'].notna().mean():.1%}")
    return g

def coverage(df): return df.filter(regex="_hg$").notna().any(axis=1).mean()

def merge_exact(left, right, ldate, rdate, tag):
    m = left.merge(right, how="left",
                   left_on=["sym_std", ldate],
                   right_on=["sym_std", rdate],
                   suffixes=("","_hg"))
    m["match_strategy"] = np.where(m.filter(regex="_hg$").notna().any(axis=1), tag, m.get("match_strategy"))
    return m, coverage(m)

def merge_nearest(left, right, ldate, rdate, tol, tag):
    L = left[["sym_std", ldate]].dropna().rename(columns={ldate:"ld"}).sort_values(["ld","sym_std"])
    R = right[["sym_std", rdate]].dropna().rename(columns={rdate:"rd"}).sort_values(["rd","sym_std"])
    if R.empty: raise ValueError("Right key is all null; cannot do nearest-date merge.")
    pairs = pd.merge_asof(L, R, left_on="ld", right_on="rd", by="sym_std",
                          direction="nearest", tolerance=pd.Timedelta(tol))
    pairs = pairs[pairs["rd"].notna()][["sym_std","ld","rd"]]
    out = left.assign(ld=left[ldate]) \
              .merge(pairs, on=["sym_std","ld"], how="left") \
              .merge(right.assign(rd=right[rdate]), on=["sym_std","rd"], how="left", suffixes=("","_hg")) \
              .drop(columns=["ld","rd"])
    out["match_strategy"] = np.where(out.filter(regex="_hg$").notna().any(axis=1), tag, out.get("match_strategy"))
    return out, coverage(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--hg",     required=True)  # can be .csv or .zip
    ap.add_argument("--out",    required=True)
    ap.add_argument("--tol",    default="2D")
    args = ap.parse_args()

    print("Reading trades ->", args.trades); tr = read_trades(args.trades)
    print("Reading holy grail ->", args.hg);  hg = read_hg(args.hg)

    tr["match_strategy"] = ""
    best_cov, best_out = -1.0, None
    attempts = [
        ("EXACT trade_date vs hg_date", "trade_date", "hg_date", "exact"),
        (f"NEAREST ±{args.tol} trade_date vs hg_date", "trade_date", "hg_date", "nearest"),
        ("EXACT trade_peak_date vs hg_peak_date", "trade_peak_date", "hg_peak_date", "exact"),
        (f"NEAREST ±{args.tol} trade_peak vs hg_peak", "trade_peak_date", "hg_peak_date", "nearest"),
        ("EXACT trade_date vs hg_entry_date", "trade_date", "hg_entry_date", "exact"),
        (f"NEAREST ±{args.tol} trade vs hg_entry", "trade_date", "hg_entry_date", "nearest"),
    ]
    for label, ldate, rdate, kind in attempts:
        if ldate not in tr.columns or rdate not in hg.columns:
            print(f"[{label}] skipped (missing columns)"); continue
        try:
            tmp, cov = (merge_exact if kind=="exact" else merge_nearest)(tr, hg, ldate, rdate, args.tol, label)
            print(f"[{label}] coverage: {cov:.1%}")
        except Exception as e:
            print(f"[{label}] failed:", e); continue
        if cov > best_cov:
            best_cov, best_out = cov, tmp

    if best_out is None: raise SystemExit("All merge attempts failed.")
    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    best_out.to_csv(args.out, index=False)
    print(f"Wrote -> {args.out} | best coverage: {best_cov:.1%}")

if __name__ == "__main__":
    main()
