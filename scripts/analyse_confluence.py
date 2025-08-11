import argparse
from pathlib import Path
import math
import numpy as np
import pandas as pd

# ---- helpers ----
def suffix_for_offset(o: int) -> str:
    return f"t{o:+d}".replace("+","+")

def spearman_to_success(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    # rank transform (spearman) without scipy
    ranks = X.rank(method="average")
    ry = y.rank(method="average")
    corrs = ranks.corrwith(ry, method="pearson")
    return corrs.to_frame("spearman_r").sort_values("spearman_r", ascending=False)

def simple_auc(scores: np.ndarray, labels: np.ndarray) -> float:
    # Mann–Whitney U relation to AUC
    # Handles ties by averaging ranks
    s = pd.Series(scores)
    r = s.rank(method="average")
    n1 = int(labels.sum())
    n0 = int((1-labels).sum())
    if n1 == 0 or n0 == 0:
        return math.nan
    rank_sum_pos = float(r[labels == 1].sum())
    U = rank_sum_pos - n1 * (n1 + 1) / 2.0
    return U / (n1 * n0)

def select_block(df: pd.DataFrame, offsets, include_meta=True):
    cols = ["symbol","entry_date","market_level","exit_reason","success_bin"] if include_meta else []
    feat_cols = []
    for o in offsets:
        suff = suffix_for_offset(o)
        feat_cols += [c for c in df.columns if c.endswith(f"_{suff}")]
    cols += feat_cols
    return df[cols].copy(), feat_cols

def run_univariate(df: pd.DataFrame, feat_cols, out_csv: Path, group_name: str):
    # Labels
    y = df["success_bin"].astype(int).values
    X = df[feat_cols].astype(float)

    # Drop all-NaN or constant features
    nunique = X.nunique(dropna=True)
    keep = (nunique > 1)
    X = X.loc[:, keep]
    feat_cols = list(X.columns)

    # fill NaNs with median
    X = X.fillna(X.median(numeric_only=True))

    # Spearman
    sp = spearman_to_success(X, pd.Series(y))
    # AUC (per feature)
    aucs = []
    for c in X.columns:
        aucs.append(simple_auc(X[c].values, y))
    auc = pd.Series(aucs, index=X.columns, name="auc")

    out = pd.concat([sp, auc], axis=1).sort_values(["spearman_r"], ascending=False)
    out.insert(0, "group", group_name)
    out.insert(1, "feature", out.index)
    out.reset_index(drop=True, inplace=True)
    out.to_csv(out_csv, index=False)
    return out

def maybe_fit_l1(df: pd.DataFrame, feat_cols, out_csv: Path, group_name: str):
    try:
        from sklearn.preprocessing import StandardScaler
        from sklearn.linear_model import LogisticRegression
    except Exception as e:
        Path(out_csv).write_text("sklearn not available; L1 skipped.\n", encoding="utf-8")
        return None

    y = df["success_bin"].astype(int).values
    X = df[feat_cols].astype(float)

    # Drop constant cols
    nunique = X.nunique(dropna=True)
    keep = (nunique > 1)
    X = X.loc[:, keep]
    feat_cols = list(X.columns)

    # Impute + scale
    X = X.fillna(X.median(numeric_only=True))
    Xs = StandardScaler().fit_transform(X.values)

    # L1 logistic (sparse selection)
    clf = LogisticRegression(penalty="l1", solver="liblinear", C=1.0, max_iter=200)
    clf.fit(Xs, y)
    coefs = pd.Series(clf.coef_[0], index=feat_cols, name="l1_coef")
    nonzero = coefs[coefs != 0].sort_values(key=lambda s: s.abs(), ascending=False)
    out = nonzero.to_frame().reset_index().rename(columns={"index":"feature"})
    out.insert(0, "group", group_name)
    out.to_csv(out_csv, index=False)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="Data/Processed/breakout_windows_features.parquet")
    ap.add_argument("--offsets", type=int, nargs="+", default=[-5, -1, 0, 1, 2, 10])
    ap.add_argument("--outdir", default="out/analysis")
    args = ap.parse_args()

    inp = Path(args.input)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(inp)

    # A) Per-offset analysis (univariate + L1 if available)
    all_univar = []
    for o in args.offsets:
        block, feats = select_block(df, [o])
        name = f"offset_{o:+d}"
        uv_path = outdir / f"univar_{name}.csv"
        l1_path = outdir / f"l1_{name}.csv"
        print(f"Running {name}: {len(feats)} features")
        out_uv = run_univariate(block, feats, uv_path, name)
        all_univar.append(out_uv)
        maybe_fit_l1(block, feats, l1_path, name)

    pd.concat(all_univar).to_csv(outdir / "top_features_by_offset.csv", index=False)

    # B) Regime-aware (univariate only to keep it fast)
    if "market_level" in df.columns:
        regimes = sorted([x for x in df["market_level"].dropna().unique().tolist() if str(x) != "nan"])
        rows = []
        for o in args.offsets:
            suff = suffix_for_offset(o)
            feat_cols = [c for c in df.columns if c.endswith(f"_{suff}")]
            for reg in regimes:
                sub = df.loc[df["market_level"] == reg, ["success_bin"] + feat_cols]
                if len(sub) < 50:
                    continue
                out = run_univariate(sub, feat_cols, outdir / f"univar_regime_{reg}_offset_{o:+d}.csv", f"regime_{reg}_offset_{o:+d}")
                rows.append(out)
        if rows:
            pd.concat(rows).to_csv(outdir / "confluence_by_regime.csv", index=False)

    # C) TIME vs TP (if TP exists)
    if "exit_reason" in df.columns:
        has_tp = (df["exit_reason"].fillna("").str.upper() == "TP").any()
        if has_tp:
            rows = []
            for o in args.offsets:
                suff = suffix_for_offset(o)
                feats = [c for c in df.columns if c.endswith(f"_{suff}")]
                a = df.loc[df["exit_reason"].str.upper()=="TIME", feats].astype(float)
                b = df.loc[df["exit_reason"].str.upper()=="TP", feats].astype(float)
                if len(a) >= 30 and len(b) >= 30:
                    mu_a = a.mean()
                    mu_b = b.mean()
                    delta = (mu_b - mu_a)
                    # pooled sd for Cohen's d
                    sd = pd.concat([a, b]).std()
                    d = delta / sd.replace(0, np.nan)
                    out = pd.DataFrame({
                        "feature": feats,
                        "offset": o,
                        "mean_TIME": mu_a.values,
                        "mean_TP": mu_b.values,
                        "delta": delta.values,
                        "cohen_d": d.values,
                        "n_TIME": len(a),
                        "n_TP": len(b)
                    })
                    out.to_csv(outdir / f"exit_reason_diffs_offset_{o:+d}.csv", index=False)
                    rows.append(out)
            if rows:
                pd.concat(rows).to_csv(outdir / "exit_reason_diffs.csv", index=False)
        else:
            (outdir / "exit_reason_diffs.csv").write_text("No TP rows present; skipped.\n", encoding="utf-8")

    print("✅ Analysis complete. See:", outdir)

if __name__ == "__main__":
    main()
