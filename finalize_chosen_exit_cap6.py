import json
from pathlib import Path
import pandas as pd
import numpy as np

OUT = Path('exit_out'); OUT.mkdir(exist_ok=True, parents=True)

# 1) Chosen exit artifacts already exist from the sweep:
exits_csv = OUT/'exits_hybrid_atr1p25_cap6.csv'

# 2) Quick per-trade stats from the chosen exits (uses the 'exit_ret' column)
ex = pd.read_csv(exits_csv)
rets = pd.to_numeric(ex.get('exit_ret', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
pos = rets[rets>0].sum()
neg = -rets[rets<0].sum()
pf  = float('inf') if neg==0 and pos>0 else (float(pos/neg) if neg!=0 else float('nan'))

metrics = {
  "family": "atr_trail",
  "param": 1.25,
  "cap_bars": 6,
  "trades": int(len(ex)),
  "win_rate": float((rets>0).mean()),
  "pf": float(pf),
  "expectancy": float(rets.mean()),
  "median_ret": float(rets.median())
}
(OUT/'chosen_exit_metrics_cap6.json').write_text(json.dumps(metrics, indent=2))

# 3) Write the rule the backtester/app can use
rule = {
  "name": "ATR Trail 1.25x OR 6 bars",
  "family": "atr_trail",
  "multiplier": 1.25,
  "cap_bars": 6,
  "inputs": ["close","atr"],
  "atr": {"window": 14, "method": "rolling_mean_true_range"},
  "timeframe": {"type": "D", "minutes": None},
  "source": "exit_harness",
  "artifact": "exits_hybrid_atr1p25_cap6.csv",
  "version": "v1"
}
(OUT/'exit_rule.json').write_text(json.dumps(rule, indent=2))

print('Wrote', OUT/'chosen_exit_metrics_cap6.json', 'and', OUT/'exit_rule.json')
