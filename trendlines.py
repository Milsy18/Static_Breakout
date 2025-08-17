import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression

# Load data
df = pd.read_csv(r"C:\Users\milla\Static_Breakout\tp_scan_by_level_fixed.csv")

fig, ax = plt.subplots(figsize=(8,6))
colors = plt.cm.viridis(np.linspace(0,1,df['market_level'].nunique()))

for lvl, color in zip(sorted(df['market_level'].unique()), colors):
    sub = df[df['market_level'] == lvl]
    if sub['tp_pct'].nunique() > 1:
        X = sub[['tp_pct']].values
        y = sub['avg_ret'].values
        model = LinearRegression().fit(X, y)
        xfit = np.linspace(X.min(), X.max(), 100).reshape(-1,1)
        yfit = model.predict(xfit)
        ax.scatter(X, y, label=f"Level {lvl}", alpha=0.4, color=color)
        ax.plot(xfit, yfit, color=color, lw=2)

ax.set_title("TP% vs Avg Return with Trendlines")
ax.set_xlabel("tp_pct")
ax.set_ylabel("avg_ret")
ax.legend(title="Market Level", bbox_to_anchor=(1.05,1), loc="upper left")
plt.tight_layout()
plt.savefig(r"C:\Users\milla\Static_Breakout\tp_vs_avgret_trendlines.png")
