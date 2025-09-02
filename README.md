## Quickstart

# 1) setup
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2) generate clean trades
python .\scripts\python\gen_trades_clean.py --src .\trades_clean_exit_compat.csv

# 3) build bars
python .\run_trade_bars_builder.py --config .\exit_harness_config.yaml

# 4) run exits/metrics
python .\hybrid_exit_and_backtest.py --config .\exit_harness_config.yaml

# 5) portfolio projection
python .\nov30_projection.py --config .\exit_harness_config.yaml --autonomous
