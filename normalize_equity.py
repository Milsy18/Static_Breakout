import pandas as pd, numpy as np, json
from pathlib import Path

eq = pd.read_csv('exit_out/equity_atr_1p25_nocap.csv', parse_dates=['date'])
eq['equity_norm_100'] = 100 * (eq['equity'] / float(eq['equity'].iloc[0]))
eq['log10_equity'] = np.log10(eq['equity'])
eq.to_csv('exit_out/equity_atr_1p25_nocap_norm.csv', index=False)
print('Wrote exit_out/equity_atr_1p25_nocap_norm.csv')
