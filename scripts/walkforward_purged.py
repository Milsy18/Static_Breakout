#!/usr/bin/env python3
# walkforward_purged.py — time-ordered folds with purge & embargo + baseline OOS metrics

import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

DEFAULT_TP_MAP = {1:0.22,2:0.25,3:0.28,4:0.30,5:0.33,6:0.36,7:0.40,8:0.42,9:0.45}

def realized_return(row, tp_map, fees_rtt):
    # Return used for baseline: if TP hit -> TP% for that M/L, else ttl_return
    mlev = row.get("market_level_at_entry")
    try:
        mlev = int(mlev) if pd.notna(mlev) else 5
    except:
        mlev = 5
    tp_pct = tp_map.get(mlev, tp_map[5])
    r = tp_pct if bool(row["tp_hit"]) else float(row["ttl_return"])
    return float(r) - float(fees_rtt)

def max_drawdown(series):
    # series is cumulative P&L (dollars). Return max drawdown (positive number)
    peak = -1e30
    mdd = 0.0
    for x in series:
        if x > peak: peak = x
        dd = peak - x
        if dd > mdd: mdd = dd
    return mdd

def cvar_losses(returns, alpha=0.95):
    # CVaR of losses from per-trade returns (%). Positive number.
    losses = np.array([min(0.0, r) for r in returns], dtype=float)  # <= 0
    if losses.size == 0: return 0.0
    q = np.quantile(losses, 1 - alpha)  # e.g., 5% worst (negative)
    tail = losses[losses <= q]
    return float(-tail.mean()) if tail.size else float(-q)

def build_folds(df, k, purge_days, embargo_days):
    # Time-ordered folds by entry_time quantiles
    df = df.sort_values("entry_time").reset_index(drop=True).copy()
    dates = df["entry_time"]
    qs = np.linspace(0, 1, k+1)
    bounds = [pd.to_datetime(dates.quantile(q)) for q in qs]
    folds = []
    for i in range(k):
        start, end = bounds[i], bounds[i+1]
        test_mask = (dates >= start) & (dates < end if i < k-1 else dates <= end)
        test = df.loc[test_mask].copy()

        # purge: drop from train any rows within +/- purge_days *on the same symbol* around each test event
        train = df.loc[~test_mask].copy()
        if purge_days > 0:
            test_sym = test[["symbol","entry_time"]]
            train["_purge"] = False
            # fast merge by symbol, then apply time window
            merged = train.merge(test_sym, on="symbol", how="left", suffixes=("", "_test"))
            delta = (merged["entry_time"] - merged["entry_time_test"]).dt.days.abs()
            train["_purge"] = merged["entry_time_test"].notna() & (delta <= purge_days)
            train = train.loc[~train["_purge"]].drop(columns=["_purge"])

        # embargo: drop training events occurring right after the test period (global time embargo)
        if embargo_days > 0:
            test_end = test["entry_time"].max()
            if pd.notna(test_end):
                emb_start = test_end
                emb_end = test_end + pd.Timedelta(days=int(embargo_days))
                train = train.loc[~((train["entry_time"] > emb_start) & (train["entry_time"] <= emb_end))]

        folds.append({
            "i": i+1,
            "test_idx": test.index.to_list(),
            "train_idx": train.index.to_list(),
            "start": str(test["entry_time"].min()),
            "end": str(test["entry_time"].max()),
            "n_test": int(len(test)),
            "n_train": int(len(train)),
        })
    return df, folds

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", required=True, help="Data/Processed/labels_v2.parquet")
    ap.add_argument("--out-dir", required=True, help="Output folder, e.g., out/wf")
    ap.add_argument("--folds", type=int, default=6)
    ap.add_argument("--purge-days", type=int, default=7)
    ap.add_argument("--embargo-days", type=int, default=14)
    ap.add_argument("--fees-rtt", type=float, default=0.004, help="round-trip fee/slippage, e.g., 0.004 = 0.4%")
    ap.add_argument("--tp-map", default=None, help="override TP map: '1:0.22,2:0.25,...,9:0.45'")
    args = ap.parse_args()

    tp_map = DEFAULT_TP_MAP.copy()
    if args.tp_map:
        for part in args.tp_map.split(","):
            k,v = part.split(":"); tp_map[int(k.strip())] = float(v.strip())

    df = pd.read_parquet(args.labels)
    if not {"symbol","entry_time","tp_hit","ttl_return"}.issubset(df.columns):
        raise ValueError("labels_v2 missing required columns")

    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["event_id"] = (df["symbol"].astype(str) + "|" + df["entry_time"].dt.strftime("%Y-%m-%d")).astype("category").cat.codes

    # Build folds
    df_ord, folds = build_folds(df, args.folds, args.purge_days, args.embargo_days)

    # Baseline OOS metrics (enter all events in each test fold)
    out_rows = []
    for f in folds:
        test = df_ord.loc[f["test_idx"]].sort_values("entry_time").copy()
        if len(test) == 0:
            continue
        rets = test.apply(lambda r: realized_return(r, tp_map, args.fees_rtt), axis=1).to_numpy()
        pnl = 1000.0 * rets  # $1k/trade, no sizing
        cum = pnl.cumsum()
        mdd = max_drawdown(cum)
        years = max(1e-9, (test["entry_time"].max() - test["entry_time"].min()).days / 365.25)
        profit_per_year = float(cum[-1]) / years if years > 0 else float(cum[-1])
        cvar95 = cvar_losses(rets, alpha=0.95)
        out_rows.append({
            "fold": f["i"],
            "period_start": f["start"],
            "period_end": f["end"],
            "n_test": f["n_test"],
            "n_train": f["n_train"],
            "tp_rate": float(test["tp_hit"].mean()) if len(test) else 0.0,
            "avg_ret_pct": float(rets.mean())*100.0,
            "profit_per_year_usd": profit_per_year,
            "max_drawdown_usd": mdd,
            "cvar95_loss_pct": cvar95*100.0
        })

    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(out/"wf_baseline_metrics.csv", index=False)
    with open(out/"wf_folds.json","w") as f: json.dump(folds, f, indent=2)
    df_ord[["event_id","symbol","entry_time"]].to_parquet(out/"events_index.parquet", index=False)

    print("[wf] wrote:", out/"wf_baseline_metrics.csv")
    print("[wf] wrote:", out/"wf_folds.json")
    if out_rows:
        tot_poy = sum(r["profit_per_year_usd"] for r in out_rows)/len(out_rows)
        print(f"[wf] folds={len(out_rows)}, avg Profit/Year=${tot_poy:,.0f}, avg tp_rate={np.mean([r['tp_rate'] for r in out_rows]):.3f}")

if __name__ == "__main__":
    import pandas as pd
    main()
