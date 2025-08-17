import numpy as np, pandas as pd, re
from sklearn.metrics import roc_auc_score

IN_CSV = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled.csv"
OUT_CSV = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_threshold_search.csv"

HOLD_DAYS_BY_LEVEL = {1:5, 2:5, 3:8, 4:6, 5:8, 6:6, 7:6, 8:7, 9:6}

df = pd.read_csv(IN_CSV)
df["market_level"] = pd.to_numeric(df["market_level"], errors="coerce").astype("Int64")
df["win_flag"]     = pd.to_numeric(df["win_flag"], errors="coerce").fillna(0).astype(int)

# find all rsi_d* columns
rsi_cols = {int(m.group(1)):c for c in df.columns
            for m in [re.match(r"^rsi_d(\-?\d+)$", c)] if m}

rows = []
for lvl, hold in HOLD_DAYS_BY_LEVEL.items():
    sub = df[df["market_level"]==lvl]
    if sub.empty: 
        continue
    best = None

    for d in range(0, hold+1):
        if d not in rsi_cols: 
            continue
        col = rsi_cols[d]
        x = pd.to_numeric(sub[col], errors="coerce")
        y = sub["win_flag"]
        msk = x.notna() & y.notna()
        if msk.sum() < 100:  # need some support
            continue
        x = x[msk].values
        y = y[msk].values

        # AUC as a day-level separability score (higher is better)
        try:
            auc = roc_auc_score(y, x)
        except Exception:
            auc = np.nan

        # grid search threshold to maximize Youden's J = TPR - FPR
        # rule: predict WIN if RSI >= T (we want low-RSI to be risky)
        Ts = np.arange(30.0, 80.5, 0.5)
        J_best, T_best, tpr_best, fpr_best, acc_best = -1, np.nan, np.nan, np.nan, np.nan
        for T in Ts:
            yhat = (x >= T).astype(int)
            tp = ((y==1) & (yhat==1)).sum()
            fp = ((y==0) & (yhat==1)).sum()
            fn = ((y==1) & (yhat==0)).sum()
            tn = ((y==0) & (yhat==0)).sum()
            tpr = tp / (tp+fn) if (tp+fn)>0 else 0.0
            fpr = fp / (fp+tn) if (fp+tn)>0 else 0.0
            J    = tpr - fpr
            acc  = (tp+tn) / (tp+tn+fp+fn) if (tp+tn+fp+fn)>0 else 0.0
            if J > J_best:
                J_best, T_best, tpr_best, fpr_best, acc_best = J, T, tpr, fpr, acc

        rows.append({
            "market_level": lvl,
            "day": d,
            "n_obs": int(msk.sum()),
            "auc": auc,
            "best_threshold": T_best,
            "youden_J": J_best,
            "tpr": tpr_best,
            "fpr": fpr_best,
            "accuracy": acc_best,
        })

res = pd.DataFrame(rows).sort_values(["market_level","day"])
# choose best day per level by Youden J (tie-break by AUC)
winners = (res.sort_values(["market_level","youden_J","auc"], ascending=[True,False,False])
             .groupby("market_level").head(1)
             .rename(columns={"day":"best_day","best_threshold":"best_threshold_rsi"}))
winners["rule_summary"] = winners.apply(lambda r: f"exit if RSI < {r.best_threshold_rsi:.1f} any time ≤ day {int(r.best_day)}", axis=1)

res.to_csv(OUT_CSV, index=False)
print("Saved grid search detail ->", OUT_CSV)
print("\nBest per level:")
print(winners[["market_level","best_day","best_threshold_rsi","auc","youden_J","n_obs","rule_summary"]]
      .to_string(index=False, float_format=lambda x: f"{x:.3f}"))
