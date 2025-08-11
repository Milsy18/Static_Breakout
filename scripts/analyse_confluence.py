# analyse_confluence.py
# Finds feature confluence by bar-offset (e.g., @t-1, @t0, @t1) using L1-logistic and simple correlations.

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore", category=FutureWarning)

DEFAULT_OFFSETS = [-5, -1, 0, 1, 2, 10]
KEYS = ["symbol", "entry_date"]   # expected identifier columns
LABEL_CANDIDATES = ["success_bin", "success", "pct_return"]  # in this order


def load_frame(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        sys.exit(f"[ERR] Input not found: {p}")
    if p.suffix.lower() == ".parquet":
        df = pd.read_parquet(p)
    else:
        df = pd.read_csv(p, parse_dates=["entry_date", "exit_date"], low_memory=False)
    # normalize column names a touch
    df.columns = [str(c).strip() for c in df.columns]
    return df


def pick_label(df: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Returns (y, label_name). Prefers success_bin; falls back to success (bool/0-1) or pct_return > 0.
    """
    for col in LABEL_CANDIDATES:
        if col in df.columns:
            s = df[col]
            if col == "pct_return":
                y = (s.astype(float) > 0).astype(int)
                return y, "pct_return>0"
            # try to coerce to {0,1}
            if s.dtype.kind in "biu":
                return s.astype(int), col
            s_str = s.astype(str).str.lower()
            if s_str.isin(["0", "1", "true", "false", "t", "f", "y", "n"]).any():
                y = s_str.isin(["1", "true", "t", "y"]).astype(int)
                return y, col
    sys.exit("[ERR] Could not determine a label (no success_bin/success/pct_return).")


def suffixes_for(off: int) -> list[str]:
    """
    Accept both modern '@t{off}' and legacy '@{off}' suffixes.
    """
    return [f"@t{off}", f"@{off}"]


def feature_columns_for_offset(df: pd.DataFrame, off: int) -> list[str]:
    """
    Pick numeric columns whose names end with one of the recognized suffixes for this offset.
    """
    cands = []
    for c in df.columns:
        name = str(c)
        if any(name.endswith(suf) for suf in suffixes_for(off)):
            cands.append(name)

    if not cands:
        return []

    # try to coerce to numeric to weed out non-numerics
    tmp = df[cands].apply(pd.to_numeric, errors="coerce")
    numeric = [c for c in tmp.columns if tmp[c].notna().any()]

    # drop obvious non-features if they slipped through
    drop_like = set(KEYS + ["exit_reason", "market_level", "market_level_at_entry",
                            "success", "success_bin", "pct_return"])
    numeric = [c for c in numeric if c not in drop_like]

    return numeric


def maybe_fit_l1(block: pd.DataFrame, feat_cols: list[str], out_csv: Path, label_name: str) -> dict:
    """
    Fits L1-logistic on feat_cols to predict y. Writes coefficients CSV.
    Returns a dict summary.
    """
    if len(feat_cols) == 0:
        return {"n_features": 0, "cv_auc_mean": None, "cv_auc_std": None}

    X_raw = block[feat_cols].apply(pd.to_numeric, errors="coerce").values
    y = block["_y_"].astype(int).values

    # impute + scale
    imp = SimpleImputer(strategy="median")
    X_imp = imp.fit_transform(X_raw)
    Xs = StandardScaler().fit_transform(X_imp)

    # cross-validated AUC for a sanity metric
    clf = LogisticRegression(penalty="l1", solver="liblinear", C=1.0, max_iter=2000)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs = cross_val_score(clf, Xs, y, cv=cv, scoring="roc_auc")
    auc_mean, auc_std = float(np.mean(aucs)), float(np.std(aucs))

    # fit on full for coefficients
    clf.fit(Xs, y)
    coefs = clf.coef_.ravel()

    coef_df = pd.DataFrame({
        "feature": feat_cols,
        "coef": coefs,
        "abs_coef": np.abs(coefs),
    }).sort_values("abs_coef", ascending=False)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    coef_df.to_csv(out_csv, index=False)

    return {"n_features": int(len(feat_cols)), "cv_auc_mean": auc_mean, "cv_auc_std": auc_std}


def simple_correlations(block: pd.DataFrame, feat_cols: list[str], out_csv: Path) -> None:
    """
    Writes Pearson correlations of each feature with the label.
    """
    if not feat_cols:
        return
    y = block["_y_"].astype(float)
    rows = []
    for c in feat_cols:
        s = pd.to_numeric(block[c], errors="coerce")
        if s.notna().sum() < 3:
            continue
        corr = s.corr(y)
        rows.append((c, corr))
    corr_df = pd.DataFrame(rows, columns=["feature", "pearson_corr"]).sort_values(
        "pearson_corr", ascending=False
    )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    corr_df.to_csv(out_csv, index=False)


def select_block(df: pd.DataFrame, off: int) -> tuple[pd.DataFrame, list[str]]:
    """
    Returns (block_df, feature_cols) for a given offset.
    Keeps useful context columns and the feature set for the offset.
    """
    feat_cols = feature_columns_for_offset(df, off)
    keep = [c for c in KEYS if c in df.columns]
    for extra in ["exit_reason", "market_level", "market_level_at_entry", "pct_return", "success_bin"]:
        if extra in df.columns and extra not in keep:
            keep.append(extra)
    cols = keep + feat_cols + ["_y_"]
    return df[cols].copy(), feat_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to labeled wide features (parquet/csv)")
    ap.add_argument("--outdir", required=True, help="Output directory for analysis CSVs")
    ap.add_argument("--offsets", nargs="*", type=int, default=DEFAULT_OFFSETS,
                    help="Offsets to analyze (e.g. -5 -1 0 1 2 10)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_frame(args.input)

    # build / attach label
    y, label_name = pick_label(df)
    df = df.copy()
    df["_y_"] = y.values

    summary = {}
    for off in args.offsets:
        # choose features for this offset (by suffix)
        feats = feature_columns_for_offset(df, off)
        print(f"Offset {off}: {len(feats)} features")
        block, feat_cols = select_block(df, off)

        # files per offset
        l1_csv = outdir / f"l1_coefs_t{off}.csv"
        cor_csv = outdir / f"correlations_t{off}.csv"

        # correlations
        simple_correlations(block, feat_cols, cor_csv)

        # l1
        stats = maybe_fit_l1(block, feat_cols, l1_csv, label_name)
        summary[str(off)] = {
            "n_features": stats["n_features"],
            "cv_auc_mean": stats["cv_auc_mean"],
            "cv_auc_std": stats["cv_auc_std"],
            "label": label_name,
        }

    # write a run summary
    with open(outdir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n=== Done ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
