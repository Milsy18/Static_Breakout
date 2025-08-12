#!/usr/bin/env python3
import json
import pandas as pd
from pathlib import Path

summary = json.load(open(r".\out\optim\summary.json","r"))
labels  = pd.read_parquet(r".\Data\Processed\labels_v2.parquet")
levels  = labels["market_level_at_entry"].fillna(5).astype(int).clip(1,9)
labels  = labels.assign(mkt_level=levels)

stats = (labels.groupby("mkt_level")
         .agg(n=("tp_hit","size"),
              tp_rate=("tp_hit","mean"),
              avg_ttl=("ttl_return","mean"))
         .reset_index())
stats["tp_rate"] = stats["tp_rate"].round(3)
stats["avg_ttl"] = stats["avg_ttl"].round(4)

outdir = Path(r".\out\report"); outdir.mkdir(parents=True, exist_ok=True)
(stats.sort_values("mkt_level").to_csv(outdir/"label_stats_per_level.csv", index=False))

cutoffs = json.load(open(r".\out\optim\cutoffs.json","r"))
lines = []
lines += [
  "M18 Monotone — Optimization Summary",
  "====================================",
  f"Profit/Year: ${summary['metrics']['profit_year']:.0f}",
  f"Max Drawdown: ${summary['metrics']['mdd']:.0f}",
  f"CVaR 95%: {summary['metrics']['cvar']*100:.2f}%",
  f"Trades: {summary['metrics']['trades']}",
  "",
  "Cutoffs (level: threshold):",
  ", ".join(f"{i}:{cutoffs[str(i)]}" for i in range(1,10)),
]
(outdir/"summary.txt").write_text("\n".join(lines), encoding="utf-8")
print("[report] wrote", outdir/"summary.txt", "and", outdir/"label_stats_per_level.csv")
