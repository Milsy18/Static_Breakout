import argparse, json
from pathlib import Path
import pandas as pd, numpy as np

def export(mult, cap):
    bars  = pd.read_csv('trade_bars.csv', parse_dates=['date'])
    trades= pd.read_csv('trades_clean_for_exit_tests.csv', parse_dates=['date'])
    if 'trade_id' not in trades.columns:
        trades = trades.reset_index().rename(columns={'index':'trade_id'})

    rows=[]
    for tid, g in bars.sort_values(['trade_id','bar_index']).groupby('trade_id'):
        c = g['close'].astype(float).values
        a = g['atr'].astype(float).values
        trail = np.maximum.accumulate(c) - float(mult) * a
        hit = np.where(c < trail)[0]
        k = int(hit[0]) if len(hit) else int(len(g)-1)
        if cap is not None:
            k = min(k, int(cap))
        exit_row = g.iloc[k]
        rows.append({
            'trade_id': int(tid),
            'exit_bar_index': int(exit_row['bar_index']),
            'exit_date': pd.to_datetime(exit_row['date']).isoformat(),
            'exit_ret': float(exit_row['ret_from_entry']),
        })

    Path('exit_out').mkdir(parents=True, exist_ok=True)
    out = pd.DataFrame(rows).sort_values('trade_id').reset_index(drop=True)
    suffix = f"{str(mult).replace('.','p')}_nocap" if cap is None else f"{str(mult).replace('.','p')}_cap{int(cap)}"
    out_path = Path(f"exit_out/exits_atr_{suffix}.csv")
    out.to_csv(out_path, index=False)

    # quick metrics
    rets = out['exit_ret'].astype(float)
    pf = (rets[rets>0].sum()) / (-(rets[rets<0].sum())) if (rets[rets<0].sum()!=0) else float('inf')
    metrics = {
        'family':'atr_trail','param':float(mult),'cap_bars':(None if cap is None else int(cap)),
        'trades': int(len(rets)),'win_rate': float((rets>0).mean()),
        'pf': float(pf),'expectancy': float(rets.mean()),'median_ret': float(rets.median()),
    }
    (Path('exit_out')/f"chosen_exit_metrics_{suffix}.json").write_text(json.dumps(metrics, indent=2))
    print("Wrote", out_path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mult", type=float, required=True)
    ap.add_argument("--cap", type=str, default="none", help="integer bars or 'none'")
    a = ap.parse_args()
    cap = None if str(a.cap).lower() in ("", "none", "null") else int(a.cap)
    export(a.mult, cap)
