#!/usr/bin/env python3
# market_levels_v2.py — richer regime levels (momentum/vol/flows), optional calibration to labels (isotonic)
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression

REQ = ["BTC.D","TOTAL","TOTAL3","USDT.D"]

def load_series(root: Path, name: str) -> pd.DataFrame:
    # Accept NAME.csv (any case), columns: date[, close|value]
    cand = list(root.glob(f"{name}.csv")) + list(root.glob(f"{name.upper()}.csv")) + list(root.glob(f"{name.lower()}.csv"))
    if not cand: 
        return None
    df = pd.read_csv(cand[0])
    cols = {c.lower(): c for c in df.columns}
    if "date" not in cols: raise KeyError(f"{name}: need a 'date' column")
    df["date"] = pd.to_datetime(df[cols["date"]], errors="coerce")
    # prefer 'close' then 'value'
    valcol = "close" if "close" in cols else ("value" if "value" in cols else None)
    if not valcol: 
        # try 'index' or first numeric
        numcols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numcols: raise KeyError(f"{name}: need a numeric column (close/value)")
        valcol = numcols[0]
    df = df[["date", cols.get(valcol, valcol)]].rename(columns={cols.get(valcol, valcol): name})
    return df.dropna().sort_values("date").reset_index(drop=True)

def make_features(df: pd.DataFrame) -> pd.DataFrame:
    # daily pct changes
    for k in REQ:
        df[f"{k}_ret"] = df[k].pct_change()
    # momentum (risk-on when positive): 20d for TOTAL/TOTAL3
    df["mom_TOTAL_20"]  = df["TOTAL"].pct_change(20)
    df["mom_TOTAL3_20"] = df["TOTAL3"].pct_change(20)
    # dominance (risk-on when BTC.D and USDT.D falling): use negative 10d change
    df["neg_dBTCd_10"]  = -df["BTC.D"].pct_change(10)
    df["neg_dUSDTd_10"] = -df["USDT.D"].pct_change(10)
    # volatility penalty: 14d stdev of returns
    df["vol_BTC_14"]    = df["BTC.D_ret"].rolling(14).std()
    df["vol_TOTAL_14"]  = df["TOTAL_ret"].rolling(14).std()
    # composite risk-on score (heuristic weights; small neg on vol)
    score = (0.40*df["mom_TOTAL3_20"] + 0.25*df["mom_TOTAL_20"]
             + 0.15*df["neg_dUSDTd_10"] + 0.10*df["neg_dBTCd_10"]
             - 0.05*df["vol_BTC_14"]    - 0.05*df["vol_TOTAL_14"])
    # z-score
    df["score"] = (score - score.mean(skipna=True)) / (score.std(skipna=True) + 1e-12)
    return df

def to_levels_by_quantiles(s: pd.Series, n=9):
    q = s.quantile(np.linspace(0,1,n+1))
    # ensure strictly increasing cutpoints
    q = pd.Series(np.maximum.accumulate(q.values)).drop_duplicates().values
    bins = np.quantile(s.dropna(), np.linspace(0,1, min(n,len(q)-1)))
    bins = np.unique(bins)
    if len(bins) < 2:
        return pd.Series(np.full(len(s), 5), index=s.index)  # fallback neutral
    bins = np.r_[-np.inf, bins[1:-1], np.inf]
    lvl = pd.cut(s, bins=bins, labels=list(range(1, len(bins))), include_lowest=True).astype(float)
    # map NaN to 5
    return lvl.fillna(5).astype(int)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market-root", required=True, help="Folder with BTC.D.csv, TOTAL.csv, TOTAL3.csv, USDT.D.csv")
    ap.add_argument("--breakouts", required=True, help="static_breakouts.csv (must have symbol, entry_time)")
    ap.add_argument("--labels", default=None, help="labels_v2.parquet (optional, for calibration)")
    ap.add_argument("--out", required=True, help="Output parquet for per-row market levels")
    ap.add_argument("--export-ts", default=None, help="Optional parquet for daily level timeseries")
    ap.add_argument("--breakouts-out", default=None, help="Optional CSV of breakouts with market_level_at_entry")
    args = ap.parse_args()

    root = Path(args.market_root)
    missing = [k for k in REQ if load_series(root, k) is None]
    if missing:
        print(f"[mkt] ERROR: missing files under {root}: {', '.join(missing)}")
        sys.exit(1)

    # merge series by date
    dfs = [load_series(root, k) for k in REQ]
    m = dfs[0]
    for d in dfs[1:]:
        m = m.merge(d, on="date", how="outer")
    m = m.sort_values("date").dropna().reset_index(drop=True)

    m = make_features(m).dropna(subset=["score"]).reset_index(drop=True)

    # default: quantile levels from score
    lvl_series = to_levels_by_quantiles(m["score"], n=9)
    m["level_q"] = lvl_series

    # optional: calibrate via labels (isotonic score->TP probability, then quantiles on predicted prob)
    if args.labels:
        try:
            labels = pd.read_parquet(args.labels)
            labels["entry_time"] = pd.to_datetime(labels["entry_time"])
            # align label rows to daily date
            L = labels[["symbol","entry_time","tp_hit"]].copy()
            L["date"] = L["entry_time"].dt.floor("D")
            # join score to labels by date
            j = L.merge(m[["date","score"]], on="date", how="left").dropna(subset=["score"])
            if len(j) >= 50 and j["tp_hit"].nunique() > 1:
                iso = IsotonicRegression(out_of_bounds="clip")
                x = j["score"].to_numpy()
                y = j["tp_hit"].astype(int).to_numpy()
                iso.fit(x, y)
                # daily fitted prob
                m["p_hat"] = iso.predict(m["score"].to_numpy())
                # levels by p_hat quantiles
                lvl_p = to_levels_by_quantiles(pd.Series(m["p_hat"], index=m.index), n=9)
                m["level"] = lvl_p
                print(f"[mkt] calibrated with isotonic (n={len(j)})")
            else:
                m["p_hat"] = np.nan
                m["level"] = m["level_q"]
                print(f"[mkt] not enough label coverage for calibration; using quantiles")
        except Exception as e:
            m["p_hat"] = np.nan
            m["level"] = m["level_q"]
            print(f"[mkt] WARN: calibration failed: {e}; using quantiles")
    else:
        m["p_hat"] = np.nan
        m["level"] = m["level_q"]

    # export daily timeseries
    if args.export_ts:
        Path(args.export_ts).parent.mkdir(parents=True, exist_ok=True)
        m[["date","level","score","p_hat"]].to_parquet(args.export_ts, index=False)

    # map to breakouts
    bo = pd.read_csv(args.breakouts)
    if "symbol" not in bo.columns: 
        raise KeyError("breakouts file needs 'symbol'")
    # find a time column
    tcol = None
    for c in ["entry_time","entry_ts","entry_date","date","timestamp"]:
        if c in bo.columns: tcol=c; break
    if tcol is None: raise KeyError("breakouts file needs an entry time column (e.g., entry_time)")

    bo[tcol] = pd.to_datetime(bo[tcol])
    bo["date"] = bo[tcol].dt.floor("D")
    out = (bo.merge(m[["date","level"]], on="date", how="left")
             .rename(columns={"level":"market_level_at_entry"}))
    # fill any missing with neutral 5
    out["market_level_at_entry"] = out["market_level_at_entry"].fillna(5).astype(int)

    # write per-row levels parquet
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    outp = out[["symbol", tcol, "market_level_at_entry"]].rename(columns={tcol:"entry_time"})
    outp.to_parquet(args.out, index=False)

    # optional: write breakouts with levels for re-labeling
    if args.breakouts_out:
        out_bo = bo.copy()
        out_bo = out_bo.merge(outp, on=["symbol","date"], how="left")
        out_bo["market_level_at_entry"] = out_bo["market_level_at_entry"].fillna(5).astype(int)
        out_bo.drop(columns=["date"], inplace=True, errors="ignore")
        Path(args.breakouts_out).parent.mkdir(parents=True, exist_ok=True)
        out_bo.to_csv(args.breakouts_out, index=False)

    print(f"[mkt] wrote daily timeseries: {args.export_ts or '(skipped)'}")
    print(f"[mkt] wrote per-row parquet: {args.out} (rows={len(outp)})")
    if args.breakouts_out:
        print(f"[mkt] wrote breakouts+levels: {args.breakouts_out}")
