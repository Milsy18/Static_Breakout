import argparse
import json
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

def load_breakouts(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # Parse dates & coerce numerics
    for c in ["entry_date","exit_date"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    num = ["entry_price","exit_price","pct_return","score_trd","score_vty","score_vol","score_mom","score_total","market_level_at_entry"]
    for c in num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["year"] = df["entry_date"].dt.year
    # keep only rows with valid returns & dates
    df = df.dropna(subset=["pct_return","entry_date","exit_date","score_trd","score_vty","score_vol","score_mom"])
    return df

def eval_config(df: pd.DataFrame, w, min_total: float, min_mod: float, year_weighting: str="equal", min_trades_year: int=1):
    # Composite from raw module scores at entry
    comp = (
        w[0]*df["score_trd"] +
        w[1]*df["score_vty"] +
        w[2]*df["score_vol"] +
        w[3]*df["score_mom"]
    )

    mask = (
        (comp >= min_total) &
        (df["score_trd"] >= min_mod) &
        (df["score_vty"] >= min_mod) &
        (df["score_vol"] >= min_mod) &
        (df["score_mom"] >= min_mod)
    )

    trades = df.loc[mask, ["year","pct_return"]].copy()
    if trades.empty:
        return {
            "metric": -1e9, "years_used": 0, "trades": 0,
            "mean_return_all": np.nan, "per_year": {}
        }

    # Per-year average profit/trade
    per_year = trades.groupby("year")["pct_return"].mean().to_dict()

    # Optionally drop thin years
    counts = trades.groupby("year")["pct_return"].count()
    valid_years = [y for y,c in counts.items() if c >= min_trades_year]
    if not valid_years:
        return {
            "metric": -1e9, "years_used": 0, "trades": len(trades),
            "mean_return_all": trades["pct_return"].mean(), "per_year": per_year
        }

    if year_weighting == "equal":
        metric = float(np.mean([per_year[y] for y in valid_years]))
    else:  # weight by #trades
        weights = counts.loc[valid_years].values
        vals = np.array([per_year[y] for y in valid_years])
        metric = float(np.average(vals, weights=weights))

    return {
        "metric": metric,
        "years_used": len(valid_years),
        "trades": int(len(trades)),
        "mean_return_all": float(trades["pct_return"].mean()),
        "per_year": {int(k): float(v) for k,v in per_year.items()}
    }

def write_pine(out_path: Path, w, min_total: float, min_mod: float):
    code = f"""// @version=5
indicator("M18 Entry (auto-generated)", overlay=true)

// ==== Auto-generated parameters (from optimiser) ====
w_trd = input.float(defval={w[0]:.6f}, title="Weight TRD", minval=0, maxval=1)
w_vty = input.float(defval={w[1]:.6f}, title="Weight VTY", minval=0, maxval=1)
w_vol = input.float(defval={w[2]:.6f}, title="Weight VOL", minval=0, maxval=1)
w_mom = input.float(defval={w[3]:.6f}, title="Weight MOM", minval=0, maxval=1)

min_total = input.float(defval={min_total:.6f}, title="Min total score", step=0.01)
min_mod   = input.float(defval={min_mod:.6f},   title="Min per-module score", step=0.01)

// TODO: replace these with your live module score series
score_trd = input.float(title="score_trd (wire real series)")
score_vty = input.float(title="score_vty (wire real series)")
score_vol = input.float(title="score_vol (wire real series)")
score_mom = input.float(title="score_mom (wire real series)")

score_comp = w_trd*score_trd + w_vty*score_vty + w_vol*score_vol + w_mom*score_mom

enter_long = (score_comp >= min_total) and (score_trd >= min_mod) and (score_vty >= min_mod) and (score_vol >= min_mod) and (score_mom >= min_mod)

plotshape(enter_long, title="Enter long", style=shape.triangleup, location=location.belowbar)
"""
    out_path.write_text(code, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="Data/Processed/static_breakouts.csv")
    ap.add_argument("--n-trials", type=int, default=400)
    ap.add_argument("--seed", type=int, default=18)
    ap.add_argument("--min-total-range", type=float, nargs=2, default=[0.0, 10.0])  # tune as needed
    ap.add_argument("--min-mod-range", type=float, nargs=2, default=[0.0, 5.0])     # tune as needed
    ap.add_argument("--year-weighting", choices=["equal","trades"], default="equal")
    ap.add_argument("--min-trades-year", type=int, default=1)
    ap.add_argument("--outdir", default="out/optim")
    args = ap.parse_args()

    df = load_breakouts(Path(args.input))
    rng = np.random.default_rng(args.seed)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    root = Path(args.outdir) / stamp
    root.mkdir(parents=True, exist_ok=True)

    rows = []
    best = None

    for i in range(1, args.n_trials+1):
        # sample weights that sum to 1
        w = rng.dirichlet(np.ones(4))
        min_total = rng.uniform(*args.min_total_range)
        min_mod   = rng.uniform(*args.min_mod_range)

        res = eval_config(
            df, w, min_total=min_total, min_mod=min_mod,
            year_weighting=args.year_weighting, min_trades_year=args.min_trades_year
        )
        row = {
            "trial": i,
            "w_trd": w[0], "w_vty": w[1], "w_vol": w[2], "w_mom": w[3],
            "min_total": min_total, "min_mod": min_mod,
            "metric": res["metric"],
            "years_used": res["years_used"],
            "trades": res["trades"],
            "mean_return_all": res["mean_return_all"]
        }
        rows.append(row)

        if best is None or res["metric"] > best["metric"]:
            best = {**row, "per_year": res["per_year"]}

        if i % 50 == 0:
            print(f"Trial {i}/{args.n_trials}  best_metric={best['metric']:.6f}  trades={best['trades']}")

    results = pd.DataFrame(rows).sort_values("metric", ascending=False)
    results_path = root / "results.csv"
    results.to_csv(results_path, index=False)

    # write best config
    best_path = root / "best_config.json"
    best_path.write_text(json.dumps(best, indent=2), encoding="utf-8")

    # generate pine
    pine_path = root / "m18_autogen.pine"
    write_pine(pine_path, [best["w_trd"],best["w_vty"],best["w_vol"],best["w_mom"]], best["min_total"], best["min_mod"])

    print("\n=== Optimisation complete ===")
    print(f"Trials: {args.n_trials}")
    print(f"Best metric (avg yearly profit/trade): {best['metric']:.6f}")
    print(f"Best trades: {best['trades']}  years_used: {best['years_used']}")
    print(f"Best weights: TRD={best['w_trd']:.3f} VTY={best['w_vty']:.3f} VOL={best['w_vol']:.3f} MOM={best['w_mom']:.3f}")
    print(f"Best thresholds: min_total={best['min_total']:.3f}  min_mod={best['min_mod']:.3f}")
    print(f"Saved: {results_path}")
    print(f"Saved: {best_path}")
    print(f"Saved: {pine_path}")

if __name__ == "__main__":
    main()
