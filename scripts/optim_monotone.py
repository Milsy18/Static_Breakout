#!/usr/bin/env python3
# optim_monotone.py — verbose, robust to _x/_y label columns
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

DEFAULT_TP_MAP = {1:0.22,2:0.25,3:0.28,4:0.30,5:0.33,6:0.36,7:0.40,8:0.42,9:0.45}

LABEL_BASE = ("tp_hit","ttl_return","mfe","mae","bars_to_tp","exit_reason_label")
ALLOWED_LABEL_COLS = set(LABEL_BASE) | {"symbol","entry_time","market_level_at_entry","breakout_id"}

def realized_return(row, tp_map, fees_rtt):
    mlev = row.get("market_level_at_entry")
    try: mlev = int(mlev) if pd.notna(mlev) else 5
    except: mlev = 5
    tp_pct = tp_map.get(mlev, tp_map[5])
    r = tp_pct if bool(row["tp_hit"]) else float(row["ttl_return"])
    return float(r) - float(fees_rtt)

def max_drawdown_usd(pnl_usd):
    peak=-1e30; mdd=0.0
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
    # numeric, and not labels or anything containing label substrings (handles *_x/_y)
    feats=[]
    for c in df.columns:
        lc=c.lower()
        if any(tok in lc for tok in LABEL_BASE): 
            continue
        if lc in {"symbol","entry_time","market_level_at_entry","breakout_id"}:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            feats.append(c)
    return feats

def enforce_monotone(cut):
    out=cut.copy()
    for k in range(2,10):
        out[k] = min(out[k], out[k-1])
    return out

def unify_labels(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure df has plain 'tp_hit' and 'ttl_return' columns even after merges that create *_x/_y
    for base in ("tp_hit","ttl_return"):
        if base in df.columns: 
            continue
        cand = [c for c in df.columns if c.startswith(base+"_")]
        if cand:
            # prefer *_y (from the right side), else *_x
            preferred = next((c for c in cand if c.endswith("_y")), cand[0])
            df[base] = df[preferred]
    return df

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
    ap.add_argument("--lambda-cvar", type=float, default=0.0)
    ap.add_argument("--dd-cap-usd", type=float, default=None)
    ap.add_argument("--grid", default="0.30:0.95:14")
    args = ap.parse_args()

    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    # Load
    train = pd.read_parquet(args.train)
    labels = pd.read_parquet(args.labels)
    evix   = pd.read_parquet(args.events_index)
    with open(args.folds_json,"r") as f: folds = json.load(f)

    key = ["symbol","entry_time"]
    train["entry_time"]=pd.to_datetime(train["entry_time"])
    labels["entry_time"]=pd.to_datetime(labels["entry_time"])

    # If train already has labels, just keep those; else merge from labels
    has_train_labels = ("tp_hit" in train.columns) and ("ttl_return" in train.columns)
    if has_train_labels:
        df = train.copy()
    else:
        df = (train.merge(labels[key+["tp_hit","ttl_return"]], on=key, how="inner")
                  .reset_index(drop=True))

    # Add wf_pos
    evix["entry_time"]=pd.to_datetime(evix["entry_time"])
    evix = evix.reset_index().rename(columns={"index":"wf_pos"})
    df = df.merge(evix[key+["wf_pos"]], on=key, how="left")

    # Unify and clean labels
    df = unify_labels(df)

    features = choose_features(df)
    info = {
        "rows_train": int(len(train)),
        "rows_labels": int(len(labels)),
        "rows_merged": int(len(df)),
        "missing_wf_pos": int(df["wf_pos"].isna().sum()),
        "n_features": int(len(features)),
        "folds": int(len(folds)),
        "fold_sizes_test": [int(len(f["test_idx"])) for f in folds][:10],
    }
    print("[optim] info:", json.dumps(info, default=int))

    if len(df)==0 or len(features)==0:
        with open(out/"summary.json","w") as f:
            json.dump({"objective_value":0,"metrics":{"profit_year":0,"mdd":0,"cvar":0,"trades":0},"debug":info}, f, indent=2)
        print("[optim] ABORT: empty df or no features; wrote summary.json")
        return

    # Build masks
    masks=[]
    for f in folds:
        te = df["wf_pos"].isin(set(f["test_idx"])).to_numpy()
        tr = ~te
        print(f"[optim] fold {f['i']}: train={int(tr.sum())}, test={int(te.sum())}")
        masks.append((tr, te, f))

    # Predict
    all_preds=[]
    for (tr, te, f) in masks:
        if te.sum()==0 or tr.sum()==0: 
            continue
        pipe = Pipeline([("scaler", StandardScaler()),
                         ("clf", LogisticRegression(max_iter=200, solver='lbfgs'))])
        ytr = df.loc[tr, "tp_hit"]
        # If ytr is bool/float, cast to int cleanly
        ytr = ytr.astype(int) if ytr.dtype!=int else ytr
        pipe.fit(df.loc[tr, features].to_numpy(), ytr.to_numpy())
        p = pipe.predict_proba(df.loc[te, features].to_numpy())[:,1]
        tmp = df.loc[te, ["symbol","entry_time","market_level_at_entry","tp_hit","ttl_return"]].copy()
        tmp["p_tp"]=p
        all_preds.append(tmp)

    if not all_preds:
        with open(out/"summary.json","w") as f:
            json.dump({"objective_value":0,"metrics":{"profit_year":0,"mdd":0,"cvar":0,"trades":0},"debug":info}, f, indent=2)
        print("[optim] ABORT: no test predictions produced; wrote summary.json")
        return

    preds = pd.concat(all_preds, ignore_index=True).sort_values("entry_time").reset_index(drop=True)

    # Grid
    gstart, gend, gn = [float(x) if i<2 else int(x) for i,x in enumerate(args.grid.split(":"))]
    grid = np.linspace(gstart, gend, gn)

    def eval_cutoffs(cut):
        pnl=[]; traded=[]
        for k in range(1,10):
            m = (preds["market_level_at_entry"].fillna(5).astype(int)==k) & (preds["p_tp"]>=cut[k])
            sel = preds.loc[m]
            if len(sel)==0: continue
            r = sel.apply(lambda r: ( (DEFAULT_TP_MAP.get(int(r["market_level_at_entry"]), DEFAULT_TP_MAP[5]) if bool(r["tp_hit"]) else float(r["ttl_return"])) - float(0.004) ), axis=1).to_numpy()
            usd = 1000.0*r
            pnl.append(pd.DataFrame({"t": pd.to_datetime(sel["entry_time"]).to_numpy(), "usd": usd}))
            traded.append(pd.to_datetime(sel["entry_time"]))
        if not pnl:
            return {"obj": -1e18, "detail": {"profit_year":0,"mdd":0,"cvar":0,"trades":0}}
        pnl_df = pd.concat(pnl).sort_values("t")
        cum = pnl_df["usd"].cumsum().to_numpy()
        mdd = max_drawdown_usd(cum)
        all_times = pd.concat(traded).sort_values()
        years = max(1e-9, (all_times.max() - all_times.min()).days/365.25)
        profit_year = float(cum[-1])/years if years>0 else float(cum[-1])
        cvar = cvar_losses_pct(pnl_df["usd"].to_numpy()/1000.0, alpha=0.95)
        return {"obj": profit_year, "detail": {"profit_year":profit_year,"mdd":mdd,"cvar":cvar,"trades":int(len(pnl_df))}}

    # Independent per-regime → monotone
    best={}
    for k in range(1,10):
        best_thr=None; best_obj=-1e18
        for thr in grid:
            cut = {kk:0.99 for kk in range(1,10)}; cut[k]=float(thr)
            sc = eval_cutoffs(cut)
            if sc["obj"]>best_obj: best_obj, best_thr = sc["obj"], float(thr)
        best[k]=best_thr if best_thr is not None else 0.9

    cut_mono = enforce_monotone(best)
    final = eval_cutoffs(cut_mono)

    with open(out/"cutoffs.json","w") as f: json.dump(cut_mono, f, indent=2)
    with open(out/"summary.json","w") as f:
        json.dump({"features_used": features, "grid": list(map(float,grid)), "objective_value": final["obj"],
                   "metrics": final["detail"], "best_raw_per_regime": best, "debug": info}, f, indent=2)

    print(f"[optim] thresholds (monotone): {cut_mono}")
    print(f"[optim] Profit/Year=${final['detail']['profit_year']:,.0f} | MDD=${final['detail']['mdd']:,.0f} | CVaR%={final['detail']['cvar']*100:.2f} | trades={final['detail']['trades']}")
if __name__=="__main__":
    main()
