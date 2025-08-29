import argparse, json
from pathlib import Path
import pandas as pd, numpy as np, yaml

def profit_factor(s):
    pos=s[s>0].sum(); neg=-s[s<0].sum()
    return float('inf') if neg==0 and pos>0 else (float(pos/neg) if neg!=0 else np.nan)

def equity_metrics(rets):
    eq=(1+rets).cumprod(); roll=eq.cummax(); dd=eq/roll-1
    mdd=dd.min() if len(dd) else np.nan
    return dict(trades=int(len(rets)),
                win_rate=float((rets>0).mean()) if len(rets) else np.nan,
                pf=profit_factor(rets),
                expectancy=float(np.nanmean(rets)) if len(rets) else np.nan,
                median_ret=float(np.nanmedian(rets)) if len(rets) else np.nan,
                mdd=float(mdd) if pd.notna(mdd) else np.nan)

def strat_time_cap(bars, trades, n):
    rets=(bars[bars["bar_index"]<=n].sort_values(["trade_id","bar_index"])
          .groupby("trade_id")["ret_from_entry"].last()
          .reindex(trades["trade_id"]).fillna(method="ffill").fillna(0.0))
    return rets

def strat_atr_trail(bars, trades, mult):
    out=[]
    for tid,g in bars.sort_values(["trade_id","bar_index"]).groupby("trade_id"):
        c=g["close"].values; a=g["atr"].values
        trail=np.maximum.accumulate(c) - float(mult)*a
        hit=np.where(c<trail)[0]
        out.append(float(g.iloc[max(hit[0]-1,0)]["ret_from_entry"]) if len(hit) else float(g.iloc[-1]["ret_from_entry"]))
    return pd.Series(out, index=bars["trade_id"].unique()).reindex(trades["trade_id"]).fillna(0.0)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--split", type=float, default=0.7, help="fraction of trades for train")
    a=ap.parse_args()

    cfg=yaml.safe_load(open(a.config, "r", encoding="utf-8"))
    bars=pd.read_csv(cfg["paths"]["trade_bars_csv"], parse_dates=["date"])
    trades=pd.read_csv(cfg["paths"]["trades_csv"], parse_dates=["date"])
    if "trade_id" not in trades.columns: trades=trades.reset_index().rename(columns={"index":"trade_id"})

    # split by trade date
    trades=trades.sort_values("date").reset_index(drop=True)
    cut=int(len(trades)*a.split); cut = max(1, min(len(trades)-1, cut))
    train_idx=trades.iloc[:cut].index
    test_idx =trades.iloc[cut:].index

    def eval_family(name, param_vals, fn):
        rows=[]
        for p in param_vals:
            rets = fn(bars, trades, p)
            tr_train = rets.iloc[train_idx]; tr_test = rets.iloc[test_idx]
            m_train=equity_metrics(tr_train); m_test=equity_metrics(tr_test)
            rows.append(dict(family=name, param=p,
                             train_pf=m_train["pf"], train_exp=m_train["expectancy"],
                             test_pf=m_test["pf"],  test_exp=m_test["expectancy"],
                             train_trades=m_train["trades"], test_trades=m_test["trades"]))
        return rows

    out=[]
    out+=eval_family("time_cap", cfg["params"]["time_caps"], strat_time_cap)
    if {"atr","close"}.issubset(set(c.lower() for c in bars.columns)):
        out+=eval_family("atr_trail", cfg["params"]["atr_multipliers"], strat_atr_trail)

    out_df=pd.DataFrame(out)
    out_dir=Path(cfg["paths"]["out_dir"]); out_dir.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_dir/"exit_oos_table.csv", index=False)
    top=(out_df.sort_values(["test_pf","test_exp"], ascending=False).head(5)
         .to_dict(orient="records"))
    (out_dir/"exit_oos_summary.json").write_text(json.dumps(top, indent=2))
    print("Wrote", out_dir/"exit_oos_table.csv", "and", out_dir/"exit_oos_summary.json")

if __name__=="__main__": main()
