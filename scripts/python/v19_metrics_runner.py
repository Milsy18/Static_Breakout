r"""
v19_metrics_runner.py — v19.0 baseline metrics & plots (START_CAP fixed at $25,000)

Console table columns:
  scope | n_trades | win_rate | ret_mean | ret_p50 | dur_mean | dur_p50 | avg_trades_per_day | n_days
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

START_CAP = 25_000.0

CANDIDATES = {
    "entry_time": ["entry_time","entry_ts","in","entry","timestamp_in","time_in"],
    "exit_time":  ["exit_time","exit_ts","out","exit","timestamp_out","time_out"],
    "entry_price":["entry_price","price_in","in_price","open_price","entry"],
    "exit_price": ["exit_price","price_out","out_price","close_price","exit"],
    "side":       ["side","direction","long_short","position","dir"],
    "pnl":        ["pnl","profit","net","pl"],
    "ret":        ["ret","return","pct_return","r"],
    "qty":        ["qty","size","position_size","qty_usd"],
    "dur_mins":   ["duration_mins","dur_mins","mins_in_trade","minutes_in_trade"]
}

def _find_col(df, keys):
    for k in keys:
        if k in df.columns:
            return k
    lower = {c.lower(): c for c in df.columns}
    for k in keys:
        if k.lower() in lower:
            return lower[k.lower()]
    return None

def _to_num(series: pd.Series) -> pd.Series:
    """Coerce numeric-like strings: strip spaces, $ , and any non [0-9.\-] before to_numeric."""
    s = series.astype(str).str.strip()
    s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def _to_bool_side(x):
    if pd.isna(x): return np.nan
    s = str(x).strip().lower()
    if s in ["long","buy","1","+1","l"]:  return 1.0
    if s in ["short","sell","-1","s"]:    return -1.0
    try:
        v = float(s)
        if v > 0: return 1.0
        if v < 0: return -1.0
    except: pass
    return np.nan

def _cagr(start_val, end_val, start_time, end_time):
    try:
        years = (pd.to_datetime(end_time) - pd.to_datetime(start_time)).days / 365.25
        if years <= 0: return np.nan
        return (end_val / start_val) ** (1/years) - 1
    except Exception:
        return np.nan

def _max_drawdown(equity):
    roll_max = equity.cummax()
    dd = equity/roll_max - 1.0
    return float(dd.min()), dd

def _sharpe(returns, risk_free=0.0):
    r = returns.dropna().astype(float)
    if len(r) < 2: return np.nan
    mean = r.mean() - risk_free
    std  = r.std(ddof=1)
    if std == 0: return np.nan
    return float(mean/std * np.sqrt(len(r)))

def main():
    ap = argparse.ArgumentParser(description="Compute v19.0 baseline metrics and plots (START_CAP=$25k).")
    ap.add_argument("--src", required=True, help="Trades file (CSV/TSV/Parquet/Excel)")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--print", dest="do_print", action="store_true", help="Print the 'all' scope summary table to stdout")
    ap.add_argument("--ret", dest="ret_col_forced", default=None, help="Force return column name (e.g., --ret pnl)")
    args = ap.parse_args()

    src = Path(args.src); outdir = Path(args.outdir)
    (outdir / "plots").mkdir(parents=True, exist_ok=True)

    print(f"[v19] Loading: {src}")
    if not src.exists(): raise FileNotFoundError(f"Source file not found: {src}")
    if src.suffix.lower() in [".csv",".txt"]: df = pd.read_csv(src)
    elif src.suffix.lower() == ".tsv":        df = pd.read_csv(src, sep="\t")
    elif src.suffix.lower() in [".parquet",".pq"]: df = pd.read_parquet(src)
    elif src.suffix.lower() in [".xlsx",".xls"]:    df = pd.read_excel(src)
    else: raise ValueError(f"Unsupported file type: {src.suffix}")
    if df.empty: raise ValueError("Trade file is empty.")
    print("[v19] Columns:", list(df.columns)[:24], "..." if df.shape[1] > 24 else "")

    # map columns
    cols = {k: _find_col(df, v) for k, v in CANDIDATES.items()}
    for tcol in ["entry_time","exit_time"]:
        if cols[tcol] and not np.issubdtype(df[cols[tcol]].dtype, np.datetime64):
            try: df[cols[tcol]] = pd.to_datetime(df[cols[tcol]])
            except Exception: pass

    # returns
    returns = None
    if args.ret_col_forced and args.ret_col_forced in df.columns:
        s = df[args.ret_col_forced].astype(str).str.strip()
        if s.str.contains("%").any():
            returns = pd.to_numeric(s.str.replace("%","",regex=False), errors="coerce")/100.0
        else:
            returns = pd.to_numeric(s, errors="coerce")

    if returns is None or returns.notna().sum() == 0:
        ret_col = cols["ret"]
        if ret_col and ret_col in df.columns:
            s = df[ret_col].astype(str).str.strip()
            if s.str_contains("%").any():
                returns = pd.to_numeric(s.str.replace("%","",regex=False), errors="coerce")/100.0
            else:
                returns = pd.to_numeric(s, errors="coerce")

    if returns is None or returns.notna().sum() == 0:
        # derive from prices (+ side if present) OR from pnl/qty/entry_price
        ep_in, ep_out, side = cols["entry_price"], cols["exit_price"], cols["side"]
        if ep_in and ep_out and (ep_in in df.columns) and (ep_out in df.columns):
            en = _to_num(df[ep_in]); ex = _to_num(df[ep_out])
            r = (ex / en) - 1.0
            if side and side in df.columns:
                s = df[side].map(_to_bool_side)
                returns = pd.Series(np.where(s == -1, -r, r), index=r.index)
            else:
                returns = r
        elif cols["pnl"] and cols["qty"] and cols["entry_price"]:
            val_in = _to_num(df[cols["entry_price"]]) * _to_num(df[cols["qty"]])
            returns = _to_num(df[cols["pnl"]]) / val_in.replace(0, np.nan)

    if returns is None or returns.notna().sum() == 0:
        raise ValueError("Could not find or derive per-trade return. Use --ret <colname> or provide prices (and side).")

    # durations (prefer duration_mins if present; else timestamps)
    dur_col = cols["dur_mins"]
    if dur_col:
        dur_hours = pd.to_numeric(df[dur_col], errors="coerce") / 60.0
        if cols["entry_time"] and cols["exit_time"]:
            n_days = max(1, int((df[cols["exit_time"]].max() - df[cols["entry_time"]].min()).days))
        else:
            total_hours = float(dur_hours.sum())
            n_days = int(np.ceil(total_hours / 24.0)) if total_hours == total_hours else np.nan
    elif cols["entry_time"] and cols["exit_time"]:
        dur_hours = (df[cols["exit_time"]] - df[cols["entry_time"]]).dt.total_seconds() / 3600.0
        n_days = max(1, int((df[cols["exit_time"]].max() - df[cols["entry_time"]].min()).days))
    else:
        dur_hours = pd.Series(index=df.index, dtype=float); n_days = np.nan

    # equity & drawdown
    equity = [START_CAP]
    for r in returns.fillna(0.0):
        equity.append(equity[-1] * (1 + r))
    equity = pd.Series(equity[1:], index=df.index)
    mdd, dd_series = _max_drawdown(equity)

    # metrics
    wins, losses = returns[returns > 0], returns[returns < 0]
    win_rate   = float((returns > 0).mean())
    prof_fact  = float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else np.inf
    expectancy = float(returns.mean())
    sharpe     = _sharpe(returns)
    start_t = df[cols["entry_time"]].min() if cols["entry_time"] else None
    end_t   = df[cols["exit_time"]].max() if cols["exit_time"] else None
    cagr = _cagr(START_CAP, float(equity.iloc[-1]), start_t, end_t) if (start_t is not None and end_t is not None) else np.nan
    mar  = float(cagr / abs(mdd)) if (cagr == cagr and mdd != 0) else np.nan
    avg_trades_per_day = float(len(df) / n_days) if (isinstance(n_days,(int,float)) and n_days and n_days==n_days) else np.nan

    # artifacts
    out_plots = outdir / "plots"; out_plots.mkdir(parents=True, exist_ok=True)
    pd.Series({
        "trades": int(len(df)),
        "start_capital": float(START_CAP),
        "end_equity": float(equity.iloc[-1]),
        "win_rate": win_rate,
        "profit_factor": prof_fact,
        "expectancy_per_trade": expectancy,
        "sharpe_trade_level": sharpe,
        "max_drawdown": float(mdd),
        "cagr": cagr, "mar": mar,
        "avg_trade_duration_hours": float(np.nanmean(dur_hours)) if len(dur_hours) else np.nan,
        "median_trade_duration_hours": float(np.nanmedian(dur_hours)) if len(dur_hours) else np.nan,
        "n_days": int(n_days) if n_days == n_days else np.nan,
        "avg_trades_per_day": avg_trades_per_day,
    }).to_json(outdir / "metrics.json", indent=2)

    pd.DataFrame([{
        "trades": int(len(df)), "start_capital": float(START_CAP), "end_equity": float(equity.iloc[-1]),
        "win_rate": win_rate, "profit_factor": prof_fact, "expectancy_per_trade": expectancy,
        "sharpe_trade_level": sharpe, "max_drawdown": float(mdd), "cagr": cagr, "mar": mar,
        "avg_trade_duration_hours": float(np.nanmean(dur_hours)) if len(dur_hours) else np.nan,
        "median_trade_duration_hours": float(np.nanmedian(dur_hours)) if len(dur_hours) else np.nan
    }]).to_csv(outdir / "metrics_table.csv", index=False)

    pd.DataFrame({"equity": equity, "drawdown": dd_series}).to_csv(outdir / "equity_curve.csv", index=False)

    # plots
    plt.figure(); equity.plot(); plt.title("Equity Curve (trade-level)"); plt.xlabel("Trade #"); plt.ylabel("Equity"); plt.tight_layout()
    plt.savefig(outdir / "plots" / "equity_curve.png", dpi=140); plt.close()
    plt.figure(); dd_series.plot(); plt.title("Drawdown (trade-level)"); plt.xlabel("Trade #"); plt.ylabel("Drawdown"); plt.tight_layout()
    plt.savefig(outdir / "plots" / "drawdown_curve.png", dpi=140); plt.close()
    plt.figure(); returns.hist(bins=50); plt.title("Per-Trade Return Distribution"); plt.xlabel("Return (fraction)"); plt.ylabel("Frequency"); plt.tight_layout()
    plt.savefig(outdir / "plots" / "return_hist.png", dpi=140); plt.close()
    if len(dur_hours.dropna()) > 0:
        plt.figure(); dur_hours.dropna().hist(bins=50); plt.title("Trade Duration (hours)"); plt.xlabel("Hours"); plt.ylabel("Frequency"); plt.tight_layout()
        plt.savefig(outdir / "plots" / "duration_hist.png", dpi=140); plt.close()

    # console table
    cols_out = ["scope","n_trades","win_rate","ret_mean","ret_p50","dur_mean","dur_p50","avg_trades_per_day","n_days"]
    row = {
        "scope": "all",
        "n_trades": int(len(df)),
        "win_rate": win_rate,
        "ret_mean": expectancy,
        "ret_p50": float(returns.median()),
        "dur_mean": float(np.nanmean(dur_hours)) if len(dur_hours) else np.nan,
        "dur_p50": float(np.nanmedian(dur_hours)) if len(dur_hours) else np.nan,
        "avg_trades_per_day": avg_trades_per_day,
        "n_days": int(n_days) if n_days == n_days else np.nan
    }
    table_df = pd.DataFrame([row], columns=cols_out)
    table_df.to_csv(outdir / "summary_console_table.csv", index=False)
    if args.do_print:
        with pd.option_context("display.float_format", "{:,.12f}".format):
            print("\n" + table_df.to_string(index=False))

    print(f"[v19] Done. Wrote metrics and plots to: {outdir}")

if __name__ == "__main__":
    main()
