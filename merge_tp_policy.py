import pandas as pd

BASE = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled_v2.csv"
POL  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\rsi_exit_applied_y75_d5_m3_tp_policy.csv"
OUT  = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\final_breakout_trades_tp_policy.csv"

d   = pd.read_csv(BASE)
p   = pd.read_csv(POL)

keep = ["symbol","breakout_date","exit_day","exit_type","exit_ret","timed_ret"]
out  = d.merge(p[keep], on=["symbol","breakout_date"], how="left")

print("Rows:", len(out))
print("Exit mix (%):")
print(out["exit_type"].value_counts(normalize=True).mul(100).round(1).rename("%"))
print("\nBy level median exit_ret:")
print(out.groupby("market_level")["exit_ret"].median().round(3))

out.to_csv(OUT, index=False)
print("\nWrote ->", OUT)
