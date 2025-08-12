#!/usr/bin/env python3
# optim_monotone.py — regime-aware, monotone probability cutoffs via purged walk-forward
# Objective: maximize Profit/Year (USD) @ $1k/trade incl. fees; constraints: min trades/year; monotone cutoffs

import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

DEFAULT_TP_MAP = {1:0.22,2:0.25,3:0.28,4:0.30,5:0.33,6:0.36,7:0.40,8:0.42,9:0.45}
ALLOWED_LABEL_COLS = {"tp_hit","ttl_return","mfe","mae","bars_to_tp","exit_reason_label",
                      "symbol","entry_time","market_level_at_entry","breakout_id"}

def realized_return(row, tp_map, fees_rtt):
    mlev = row.get("market_level_at_entry")
    try: mlev = int(mlev) if pd.notna(mlev) else 5
    except: mlev = 5
    tp_pct = tp_map.get(mlev, tp_map[5])
    r = tp_pct if bool(row["tp_hit"]) else float(row["ttl_return"])
    return float(r) - float(fees_rtt)

def max_drawdown_usd(pnl_usd):
    peak = -1e30; mdd = 0.0
    for x in pnl_usd:
        peak = max(peak, x)
        mdd = max(mdd, peak - x)
    return float(mdd)

def cvar_losses_pct(returns, alpha=0.95):
    losses = np.minimum(returns, 0.0)
    if len(losses)==0: return 0.0
    q = np.quantile(losses, 1-alpha)
    tail = losses[losses <= q]
    return float(-tail.mean()) if len(tail) else float(-q)

def choose_features(df):
    feats = []
    for c in df.columns:
        if c in ALLOWED_LABEL_COLS: continue
        if pd.api.types.is_numeric_dtype(df[c]):
            feats.append(c)
    return feats

def enforce_monotone(cut):
    # bearish (1) -> bullish (9): thresholds must be non-increasing
    out = cut.copy()
    for k in range(2,10):
        out[k] = min(out[k], out[k-1])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", required=True)
    ap.add_argument("--labels", required=True)
    ap.add_argument("--folds-json", required=True)
    ap.add_argument("--events-index", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--fees-rtt", type=float, default=0.004)
    ap.add_argument("--tp-map", default=None)
    ap.add_argument("--min-trades-per-year", type=float, default=25.0)
    ap.add_argument("--lambda-cvar", type=float, default=0.0, help="penalty weight on CVaR (USD/year approx.)")
    ap.add_argument("--dd-cap-usd", type=float, default=None, help="if set, reject configs exceeding this MDD")
    ap.add_argument("--grid", default="0.30:0.95:14", help="prob threshold grid start:end:n (e.g., 0.30:0.95:14)")
    args = ap.parse_args()

    tp_map = DEFAULT_TP_MAP.copy()
    if args.tp_map:
        for part in args.tp_map.split(","):
            k,v = part.split(":"); tp_map[int(k.strip())] = float(v.strip())

    # Load data
    train = pd.read_parquet(args.train)
    labels = pd.read_parquet(args.labels)
    evix = pd.read_parquet(args.events_index)  # same order as wf json
    with open(args.folds_json, "r") as f:
        folds = json.load(f)

    # Join realized returns to train (for evaluation)
    key_cols = ["symbol","entry_time"]
    train["entry_time"] = pd.to_datetime(train["entry_time"])
    labels["entry_time"] = pd.to_datetime(labels["entry_time"])
    df = (train.merge(labels[key_cols+["tp_hit","ttl_return"]], on=key_cols, how="inner")
               .drop_duplicates(subset=key_cols)
               .reset_index(drop=True))

    # Add wf position for fold mapping
    evix["entry_time"] = pd.to_datetime(evix["entry_time"])
    df = (df.merge(evix.reset_index().rename(columns={"index":"wf_pos"}), on=key_cols, how="left"))
    if df["wf_pos"].isna().any():
        missing = int(df["wf_pos"].isna().sum())
        print(f"[optim] WARN: {missing} events lack wf_pos (not in events_index); they will be ignored.")
        df = df.dropna(subset=["wf_pos"]).reset_index(drop=True)
    df["wf_pos"] = df["wf_pos"].astype(int)

    # Features/target
    features = choose_features(df)
    if not features:
        raise ValueError("No numeric t0 features found to train on.")
    y = df["tp_hit"].astype(int).to_numpy()
    X = df[features].to_numpy()
    mlev = df["market_level_at_entry"].fillna(5).astype(int).to_numpy()
    entry_time = pd.to_datetime(df["entry_time"])

    # Build fold masks using wf_pos
    fold_masks = []
    for f in folds:
        test_mask = df["wf_pos"].isin(set(f["test_idx"]))
        train_mask = ~test_mask
        fold_masks.append((train_mask.to_numpy(), test_mask.to_numpy(), f))

    # Threshold grid
    gstart, gend, gn = [float(x) if i<2 else int(x) for i,x in enumerate(args.grid.split(":"))]
    grid = np.linspace(gstart, gend, gn)

    # Walk-forward training/prediction
    all_preds = []
    for (tr_mask, te_mask, f) in fold_masks:
        if te_mask.sum()==0: continue
        pipe = Pipeline([("scaler", StandardScaler()),
                         ("clf", LogisticRegression(max_iter=200, solver="lbfgs"))])
        pipe.fit(X[tr_mask], y[tr_mask])
        p = pipe.predict_proba(X[te_mask])[:,1]
        tmp = df.loc[te_mask, ["symbol","entry_time","market_level_at_entry","tp_hit","ttl_return"]].copy()
        tmp["p_tp"] = p
        all_preds.append(tmp)
    preds = pd.concat(all_preds, ignore_index=True).sort_values("entry_time").reset_index(drop=True)

    # Helper to evaluate a set of per-regime thresholds
    def eval_cutoffs(cut):
        pnl = []
        traded_times = []
        trades_per_year_by_regime = {k:0.0 for k in range(1,10)}

        for k in range(1,10):
            mask = (preds["market_level_at_entry"].fillna(5).astype(int) == k) & (preds["p_tp"] >= cut[k])
            sel = preds.loc[mask].copy()
            if len(sel)==0: continue
            # realized returns
            r = sel.apply(lambda r: realized_return(r, tp_map, args.fees_rtt), axis=1).to_numpy()
            usd = 1000.0 * r
            pnl.append(pd.DataFrame({"t": sel["entry_time"].to_numpy(), "usd": usd}))
            traded_times.append(sel["entry_time"])
            # trades/year for this regime
            years = max(1e-9, (sel["entry_time"].max() - sel["entry_time"].min()).days / 365.25)
            if years > 0: trades_per_year_by_regime[k] = len(sel) / years

        if not pnl:
            return {"obj": -1e18, "detail": {"profit_year": 0, "mdd": 0, "cvar": 0, "trades": 0}}

        pnl_df = pd.concat(pnl, ignore_index=True).sort_values("t")
        cum = pnl_df["usd"].cumsum().to_numpy()
        mdd = max_drawdown_usd(cum)

        # Approximate years spanned by all trades
        all_times = pd.concat(traded_times).sort_values()
        years_all = max(1e-9, (all_times.max() - all_times.min()).days / 365.25)
        profit_year = float(cum[-1]) / years_all if years_all>0 else float(cum[-1])

        # Global per-trade returns for CVaR
        # Recompute returns stream in the same order
        # (already in USD; convert back to pct by /1000 for CVaR penalty normalization if desired)
        usd_stream = pnl_df["usd"].to_numpy()
        pct_stream = usd_stream / 1000.0
        cvar = cvar_losses_pct(pct_stream, alpha=0.95)

        # Constraints
        for k in range(1,10):
            if trades_per_year_by_regime[k] < args.min_trades_per_year:
                return {"obj": -1e18, "detail": {"profit_year": profit_year, "mdd": mdd, "cvar": cvar, "trades": len(pnl_df)}}
        if args.dd_cap_usd is not None and mdd > args.dd_cap_usd:
            return {"obj": -1e18, "detail": {"profit_year": profit_year, "mdd": mdd, "cvar": cvar, "trades": len(pnl_df)}}

        # Objective
        obj = profit_year - (args.lambda_cvar * (cvar * args.min_trades_per_year * 1000.0))
        return {"obj": obj, "detail": {"profit_year": profit_year, "mdd": mdd, "cvar": cvar, "trades": len(pnl_df)}}

    # Search: independent best per regime, then enforce monotone
    best_per_regime = {}
    for k in range(1,10):
        best_thr, best_obj = None, -1e18
        for thr in grid:
            cut = {kk: 0.99 for kk in range(1,10)}  # very strict others
            cut[k] = thr
            score = eval_cutoffs(cut)
            if score["obj"] > best_obj:
                best_obj, best_thr = score["obj"], thr
        best_per_regime[k] = best_thr if best_thr is not None else 0.90

    # Enforce monotone thresholds
    cut_mono = enforce_monotone(best_per_regime)
    final = eval_cutoffs(cut_mono)

    # Save outputs
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    with open(out/"cutoffs.json","w") as f: json.dump(cut_mono, f, indent=2)
    with open(out/"summary.json","w") as f:
        json.dump({
            "features_used": choose_features(df),
            "min_trades_per_year": args.min_trades_per_year,
            "fees_rtt": args.fees_rtt,
            "tp_map": tp_map,
            "grid": list(map(float, grid)),
            "objective_value": final["obj"],
            "metrics": final["detail"],
            "best_raw_per_regime": best_per_regime
        }, f, indent=2)

    print(f"[optim] thresholds (monotone): {cut_mono}")
    print(f"[optim] Profit/Year=${final['detail']['profit_year']:,.0f} | MDD=${final['detail']['mdd']:,.0f} | CVaR%={final['detail']['cvar']*100:.2f} | trades={final['detail']['trades']}")
