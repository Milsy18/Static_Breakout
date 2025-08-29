import json, pandas as pd
from pathlib import Path

OUT = Path('exit_out')
def read_json(p):
    p = OUT/p
    return json.loads(p.read_text()) if p.exists() else None

bake = pd.read_csv(OUT/'exit_bakeoff.csv') if (OUT/'exit_bakeoff.csv').exists() else None
# sort numerically just in case
if bake is not None:
    for c in ['pf','expectancy','win_rate','mdd']:
        if c in bake.columns: bake[c] = pd.to_numeric(bake[c], errors='coerce')
    bake_top = (bake.sort_values(['pf','expectancy'], ascending=False)
                    .head(10)[['family','param','pf','expectancy','win_rate','mdd']])
else:
    bake_top = None

summary          = read_json('exit_summary.json')
oos_summary      = read_json('exit_oos_summary.json')
chosen_no_cap    = read_json('chosen_exit_metrics_1p25_nocap.json')
chosen_with_cap  = read_json('chosen_exit_metrics.json')
cost_metrics     = read_json('cost_adjusted_metrics.json')

lines = []
lines += ["# Exit Bake-off Report\n"]
lines += ["**Chosen exit:** ATR trail × **1.25** (no cap)\n"]
if chosen_no_cap:
    m = chosen_no_cap
    lines += [f"- Trades: **{m['trades']}**, Win rate: **{m['win_rate']:.4f}**, PF: **{m['pf']:.2f}**, "
              f"Expectancy: **{m['expectancy']:.6f}**, Median: **{m['median_ret']:.6f}**\n"]
if cost_metrics:
    mc = cost_metrics
    lines += [f"- Cost-adjusted (fees {mc['fees_bps']}bps + slip {mc['slip_bps']}bps): "
              f"PF **{mc['pf']:.2f}**, Expectancy **{mc['expectancy']:.6f}**, MDD **{mc['mdd']:.6f}**\n"]

if oos_summary:
    lines += ["\n## Top OOS (70/30 split)\n"]
    for r in oos_summary:
        lines += [f"- {r['family']} {r['param']}: test PF **{r['test_pf']:.6f}**, "
                  f"test expectancy **{r['test_exp']:.6f}** "
                  f"(train PF {r['train_pf']:.6f})"]

if bake_top is not None:
    lines += ["\n## Leaderboard (top 10 by PF)\n", bake_top.to_markdown(index=False), "\n"]

lines += ["## Artifacts\n"]
lines += [
  "- exit_out/exit_rule.json",
  "- exit_out/exits_atr_1p25_nocap.csv",
  "- exit_out/trades_with_exit_final.csv",
  "- exit_out/equity_atr_1p25_nocap.csv, exit_out/equity_atr_1p25_nocap_norm.csv",
  "- exit_out/exit_equity_metrics.json, exit_out/cost_adjusted_metrics.json",
]

OUT.mkdir(parents=True, exist_ok=True)
(OUT/'EXIT_REPORT.md').write_text("\n".join(lines), encoding='utf-8')
print("Wrote", OUT/'EXIT_REPORT.md')
