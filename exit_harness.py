import argparse, json
from pathlib import Path
import pandas as pd, numpy as np, yaml

def profit_factor(s):
    pos = s[s>0].sum()
    neg = -s[s<0].sum()
    if neg == 0: return float('inf') if pos>0 else np.nan
    return float(pos/neg)

def equity_metrics(rets, dates=None):
    if dates is not None:
        rets = pd.Series(rets.values, index=pd.to_datetime(dates)).sort_index()
    eq = (1+rets).cumprod()
    roll = eq.cummax()
    dd = eq/roll - 1
    mdd = dd.min() if len(dd) else np.nan
    return {
        "trades": int(len(rets)),
        "win_rate": float((rets>0).mean()) if len(rets) else np.nan,
        "pf": profit_factor(rets),
        "expectancy": float(np.nanmean(rets)) if len(rets) else np.nan,
        "median_ret": float(np.nanmedian(rets)) if len(rets) else np.nan,
        "mdd": float(mdd) if pd.notna(mdd) else np.nan,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.config, "r", encoding="utf-8"))
    bars = pd.read_csv(cfg["paths"]["trade_bars_csv"], parse_dates=["date"])
    trades = pd.read_csv(cfg["paths"]["trades_csv"], parse_dates=["date"])
    if "trade_id" not in trades.columns:
        trades = trades.reset_index().rename(columns={"index":"trade_id"})
    out_dir = Path(cfg["paths"]["out_dir"]); out_dir.mkdir(parents=True, exist_ok=True)

    # Baseline = exit at last bar of each trade path
    last_ret = (bars.sort_values(["trade_id","bar_index"])
                  .groupby("trade_id")["ret_from_entry"].last()
                  .reindex(trades["trade_id"]).fillna(0.0))
    results = []
    base = equity_metrics(last_ret, trades["date"]); base.update({"family":"baseline","param":"last_bar"}); results.append(base)

    # Time caps
    for n in cfg["params"]["time_caps"]:
        rets = (bars[bars["bar_index"]<=n].sort_values(["trade_id","bar_index"])
                  .groupby("trade_id")["ret_from_entry"].last()
                  .reindex(trades["trade_id"]).fillna(method="ffill").fillna(0.0))
        met = equity_metrics(rets, trades["date"]); met.update({"family":"time_cap","param":int(n)}); results.append(met)

    # ATR trail (chandelier-style)
    if {"close","atr"}.issubset(set(c.lower() for c in bars.columns)):
        for mult in cfg["params"]["atr_multipliers"]:
            out = []
            for tid, g in bars.sort_values(["trade_id","bar_index"]).groupby("trade_id"):
                c = g["close"].values; a = g["atr"].values
                trail = np.maximum.accumulate(c) - float(mult)*a
                hit = np.where(c < trail)[0]
                out.append(float(g.iloc[max(hit[0]-1,0)]["ret_from_entry"]) if len(hit) else float(g.iloc[-1]["ret_from_entry"]))
            rets = pd.Series(out, index=bars["trade_id"].unique()).reindex(trades["trade_id"]).fillna(0.0)
            met = equity_metrics(rets, trades["date"]); met.update({"family":"atr_trail","param":float(mult)}); results.append(met)

    res = pd.DataFrame(results)
    res.to_csv(out_dir/"exit_bakeoff.csv", index=False)
    top = res.sort_values(["pf","expectancy"], ascending=False).head(2).to_dict(orient="records")
    (out_dir/"exit_summary.json").write_text(json.dumps(top, indent=2))
    print("Wrote", out_dir/"exit_bakeoff.csv", "and", out_dir/"exit_summary.json")

if __name__ == "__main__":
    main()
