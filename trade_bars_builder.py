import argparse
from pathlib import Path
import pandas as pd, numpy as np, yaml

def load_cfg(p): 
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def true_range(df):
    pc = df["close"].shift(1)
    return pd.concat([(df["high"]-df["low"]).abs(),
                      (df["high"]-pc).abs(),
                      (df["low"]-pc).abs()], axis=1).max(axis=1)

def add_ind(df, atr_n=14, don_n=20, bb_n=20):
    df = df.copy()
    if {"high","low","close"}.issubset(df.columns):
        tr = true_range(df)
        df["atr"] = tr.rolling(atr_n, min_periods=1).mean()
        df["donchian_high"] = df["high"].rolling(don_n, min_periods=1).max()
        df["donchian_low"]  = df["low"].rolling(don_n,  min_periods=1).min()
    else:
        df["atr"]=df["donchian_high"]=df["donchian_low"]=np.nan
    df["bb_mid"] = df["close"].rolling(bb_n, min_periods=1).mean() if "close" in df.columns else np.nan
    return df

def load_ohlcv(sym, folder, pq):
    if pq and Path(pq).exists():
        d = pd.read_parquet(pq)
        d = d[d["symbol"]==sym].copy()
        # normalize colnames
        d.columns = [str(c).strip().lower() for c in d.columns]
        # date
        if "date" not in d.columns:
            for alt in ("time","timestamp","datetime"):
                if alt in d.columns: d = d.rename(columns={alt:"date"}); break
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        # numeric coercion
        for c in ("open","high","low","close","volume"):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")
        d = d.sort_values("date").dropna(subset=["open","high","low","close"]).reset_index(drop=True)
        return d

    f = Path(folder)/f"{sym}.csv"
    if not f.exists():
        for alt in (sym.replace("/","-"), sym.replace(":","-"), sym.replace("_","-")):
            g = Path(folder)/f"{alt}.csv"
            if g.exists(): f = g; break
    d = pd.read_csv(f)
    d.columns = [str(c).strip().lower() for c in d.columns]
    if "date" not in d.columns:
        for alt in ("time","timestamp","datetime"):
            if alt in d.columns: d = d.rename(columns={alt:"date"}); break
    # map common case-sensitive names if any slipped through
    ren = {}
    for c in ["Open","High","Low","Close","Volume"]:
        if c in d.columns: ren[c] = c.lower()
    if ren: d = d.rename(columns=ren)
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    for c in ("open","high","low","close","volume"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.sort_values("date").dropna(subset=["open","high","low","close"]).reset_index(drop=True)
    return d

def snap_idx(dates, ts):
    i = dates.searchsorted(ts, side="left")
    return None if i>=len(dates) else int(i)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    a = ap.parse_args()
    cfg = load_cfg(a.config)

    trades = pd.read_csv(cfg["paths"]["trades_csv"], parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    if "trade_id" not in trades.columns: trades["trade_id"] = np.arange(len(trades))
    has_exit = "exit_time" in trades.columns
    has_dur  = "duration" in trades.columns
    if not (has_exit or has_dur): 
        raise ValueError("Trades need 'duration' or 'exit_time'")

    rows = []
    for _, tr in trades.iterrows():
        sym = str(tr["symbol"])
        entry = pd.to_datetime(tr.get("entry_time", tr["date"]), errors="coerce")
        if pd.isna(entry): 
            continue

        o = load_ohlcv(sym, cfg["paths"]["ohlcv_folder"], cfg["paths"]["ohlcv_parquet"])
        if o is None or o.empty: 
            continue
        o = add_ind(o, 14, 20, 20)

        si = snap_idx(o["date"].values, np.datetime64(entry))
        if si is None: 
            continue

        if has_exit and not pd.isna(tr.get("exit_time", pd.NaT)):
            end = pd.to_datetime(tr["exit_time"], errors="coerce")
            w = o.loc[(o["date"]>=entry) & (o["date"]<=end)].copy().reset_index(drop=True)
        else:
            dur = int(tr["duration"])
            w = o.iloc[si:si+dur].copy().reset_index(drop=True)

        if w.empty or pd.isna(w.iloc[0].get("close", np.nan)):
            continue

        entry_close = float(w.iloc[0]["close"])
        w["ret_from_entry"] = w["close"]/entry_close - 1.0
        w["trade_id"] = tr["trade_id"]; w["symbol"] = sym; w["bar_index"] = np.arange(len(w))

        keep = ["trade_id","symbol","bar_index","date","open","high","low","close","volume",
                "ret_from_entry","atr","donchian_high","donchian_low","bb_mid"]
        for c in keep:
            if c not in w.columns: w[c] = np.nan
        rows.append(w[keep])

    if not rows:
        raise RuntimeError("No trade windows built; check timestamps/symbol names/OHLCV folder.")
    out = pd.concat(rows, ignore_index=True)
    out.to_csv(cfg["paths"]["out_csv"], index=False)
    print(f"Wrote {len(out)} rows to {cfg['paths']['out_csv']} across {out['trade_id'].nunique()} trades.")

if __name__ == "__main__": 
    main()
