import argparse, json
from pathlib import Path
import pandas as pd
import numpy as np

START_CAP = 25_000.0
DAYS = 93  # Aug 30 -> Nov 30 inclusive

def load_rows(csv_path):
    df = pd.read_csv(csv_path)
    # Make flexible: accept either 'label' or 'filter' style column names
    name_col = 'label' if 'label' in df.columns else ('path' if 'path' in df.columns else None)
    if name_col is None:
        raise SystemExit("Couldn't find a 'label'/'path' column in out/model_summary.csv")
    need = ['ret_mean','dur_mean']
    if 'avg_trades_per_day' in df.columns:
        trades_per_day_col = 'avg_trades_per_day'
    elif {'trades','n_days'}.issubset(df.columns):
        df['avg_trades_per_day'] = pd.to_numeric(df['trades'], errors='coerce') / pd.to_numeric(df['n_days'], errors='coerce')
        trades_per_day_col = 'avg_trades_per_day'
    else:
        raise SystemExit("Need either 'avg_trades_per_day' or both 'trades' and 'n_days' in model_summary.csv")

    keep = df[[name_col, 'ret_mean','dur_mean', trades_per_day_col]].copy()
    keep.rename(columns={name_col:'name', trades_per_day_col:'avg_trades_per_day'}, inplace=True)
    return keep

def pick_row(df, key):
    # Match exact or contains
    m = df[df['name'].str.lower()==key.lower()]
    if m.empty:
        m = df[df['name'].str.contains(key, case=False, na=False)]
    if m.empty:
        raise SystemExit(f"Could not find a row for '{key}' in model_summary.csv")
    return m.iloc[0].to_dict()

def project(row, slots):
    ret_d = float(row['ret_mean'])        # daily mean return
    dur_d = float(row['dur_mean'])        # days per trade
    trades_per_day = float(row['avg_trades_per_day'])
    per_trade_mult = (1.0 + ret_d)**dur_d
    expected_trades = trades_per_day * DAYS
    rounds = expected_trades / float(slots)
    final = START_CAP * (per_trade_mult ** rounds)
    return dict(
        slots=int(slots),
        per_trade_mult=per_trade_mult,
        expected_trades=expected_trades,
        rounds=rounds,
        final_balance=final
    )

def main():
    csv = Path('out/model_summary.csv')
    if not csv.exists():
        raise SystemExit("out/model_summary.csv not found.")

    df = load_rows(csv)
    pathA = pick_row(df, 'all')
    pathB = pick_row(df, 'ema5_pct1_le_-6.415')

    out = {'pathA': {'name': pathA['name']}, 'pathB': {'name': pathB['name']}}

    for N in (3,4,5):
        out['pathA'][f'N{N}'] = project(pathA, N)
        out['pathB'][f'N{N}'] = project(pathB, N)

    # Pretty print
    for tag, block in out.items():
        print(f"\n=== {tag.upper()} ({block['name']}) ===")
        for N in (3,4,5):
            r = block[f'N{N}']
            print(f"N={N}  | per-trade x={r['per_trade_mult']:.6f}  | rounds={r['rounds']:.2f}  "
                  f"| exp trades={r['expected_trades']:.2f}  | Final=")

if __name__ == '__main__':
    main()
