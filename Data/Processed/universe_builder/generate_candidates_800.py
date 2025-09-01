# generate_candidates_800.py
import requests
import pandas as pd

# 1) Pull top 1000 coins by market cap (4 pages × 250)
all_coins = []
for page in range(1, 5):
    resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    250,
            "page":        page,
        },
    ).json()
    all_coins.extend(resp)

df = pd.DataFrame(all_coins)
df["symbol_usd"] = df["symbol"].str.upper() + "-USD"

# 2) Filter mid-caps $50M–$1B
midcaps = df[df.market_cap.between(50e6, 1e9)]

# 3) Sample up to 800 (or fewer if midcaps < 800)
n = min(800, len(midcaps))
sample = midcaps.sample(n=n, random_state=42)[["symbol_usd","market_cap"]]
sample.to_csv("candidates_800.csv", index=False)

print(f"✔ Generated candidates_800.csv with {len(sample)} symbols")
