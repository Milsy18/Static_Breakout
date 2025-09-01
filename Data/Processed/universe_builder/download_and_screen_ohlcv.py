# download_and_screen_ohlcv.py

import ccxt
import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime

# ─── 1) Read your ~800 candidate symbols ───────────────────────────────────────
cand = pd.read_csv('candidates_800.csv')
univ = cand['symbol_usd'].tolist()

# ─── 2) Setup CCXT (Binance) and time boundaries ───────────────────────────────
exchange = ccxt.binance({'enableRateLimit': True})
START_TS = exchange.parse8601('2020-01-01T00:00:00Z')
END_TS   = exchange.parse8601('2025-06-30T23:59:59Z')

# ─── 3) Prepare output folder ─────────────────────────────────────────────────
out_dir = 'crypto_OHLCV_800'
os.makedirs(out_dir, exist_ok=True)

ccxt_passed = []
yf_passed   = []
failed      = []

# ─── 4) Loop through symbols, try CCXT, then yfinance ────────────────────────
for sym in univ:
    df = None

    # a) Try CCXT/Binance first
    pair = sym.replace('-USD','/USDT')
    try:
        bars = exchange.fetch_ohlcv(pair, '1d', since=START_TS)
        if bars:
            df = pd.DataFrame(bars, columns=['ts','open','high','low','close','volume'])
            df['date'] = pd.to_datetime(df['ts'], unit='ms')
            df.set_index('date', inplace=True)
            ccxt_passed.append(sym)
    except Exception:
        pass

    # b) Fallback to yfinance if CCXT failed or returned no bars
    if df is None or df.empty:
        try:
            tmp = yf.download(
                sym,
                start="2020-01-01",
                end="2025-07-01",
                progress=False,
                auto_adjust=False
            )
            if not tmp.empty:
                df = tmp.rename(columns={
                    'Open':'open','High':'high',
                    'Low':'low','Close':'close',
                    'Volume':'volume'
                })
                yf_passed.append(sym)
        except Exception:
            pass

    # c) If we have data, write the CSV; otherwise mark as failed
    if df is not None and not df.empty:
        df.to_csv(f"{out_dir}/{sym}.csv")
    else:
        failed.append(sym)

    # d) Pause to respect rate limits
    time.sleep(exchange.rateLimit / 1000)

# ─── 5) Save the CCXT and YF pass lists ────────────────────────────────────────
pd.DataFrame({'symbol': ccxt_passed}).to_csv('ccxt_passed.csv', index=False)
pd.DataFrame({'symbol': yf_passed}).to_csv(  'yf_passed.csv',   index=False)

# ─── 6) Build and save the union universe ────────────────────────────────────
final_univ = sorted(set(ccxt_passed) | set(yf_passed))
pd.DataFrame({'symbol': final_univ}).to_csv('asset_universe.csv', index=False)

# ─── 7) Print a summary ───────────────────────────────────────────────────────
print(f"✔ CCXT passes: {len(ccxt_passed)} symbols")
print(f"✔  YF passes: {len(yf_passed)} symbols")
print(f"✔     Union: {len(final_univ)} symbols")
print(f"✖  Failures: {len(failed)} symbols (sample): {failed[:10]}")
