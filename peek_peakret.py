import pandas as pd, numpy as np
fp = r"C:\Users\milla\OneDrive\Documents\GitHub\m18-model2\data\Processed\breakout_trades_labeled.csv"
df = pd.read_csv(fp)
q = df["peak_ret_hold"].quantile([.1,.25,.5,.75,.9,.95,.99]).round(3)
print("Peak return quantiles overall:\n", q)
print("\nShare of trades with peak ≥ X before exit:")
for x in (0.20, 0.25, 0.30, 0.35, 0.40):
    print(f"{x:.2f} -> {(df['peak_ret_hold']>=x).mean():.3f}")
print("\nBy level (median, 75th, 90th):")
g = df.groupby("market_level")["peak_ret_hold"].quantile([.5,.75,.9]).unstack().round(3)
print(g.to_string())
