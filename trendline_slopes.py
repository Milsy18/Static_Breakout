import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

df = pd.read_csv(r"C:\Users\milla\Static_Breakout\tp_scan_by_level_fixed.csv")

results = []
for lvl in sorted(df['market_level'].unique()):
    sub = df[df['market_level'] == lvl]
    if sub['tp_pct'].nunique() > 1:
        X = sub[['tp_pct']].values
        y = sub['avg_ret'].values
        model = LinearRegression().fit(X, y)
        slope = model.coef_[0]
        intercept = model.intercept_
        results.append((lvl, slope, intercept))

print("Market Level | Slope | Intercept")
for lvl, slope, intercept in results:
    print(f"{lvl:12d} | {slope:.4f} | {intercept:.4f}")
