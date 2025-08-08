# label_exits.py — regime-aware exits (hybrid: TP fixed at entry, RSI/Time tighten with current)
import pandas as pd
from tqdm import tqdm
from modules.market_level import compute_market_level

INDICATORS_PATH = "Data/Processed/per_bar_indicators_core.csv"
BREAKOUTS_PATH  = "Data/Processed/static_breakouts.csv"
MACRO_RAW_PATH  = "Data/Raw/macro_regime_data.csv"
OUT_PATH        = "Data/Processed/static_master_breakouts.csv"

# ---- Regime parameter maps (M18 defaults; tweak if your doc says otherwise) ----
EXIT_TP_BY_LVL    = {1:0.45, 2:0.44, 3:0.42, 4:0.40, 5:0.38, 6:0.36, 7:0.35, 8:0.34, 9:0.33}
EXIT_BARS_BY_LVL  = {1:11, 2:10, 3:9,  4:8,  5:7,  6:7,  7:6,  8:6,  9:5 }
RSI_MAX_BY_LVL    = {1:74.0, 2:75.0, 3:76.0, 4:78.0, 5:80.0, 6:82.0, 7:84.0, 8:85.0, 9:87.0}

def lvl(mapping, x, default):
    try:
        i = int(round(float(x)))
    except Exception:
        i = 5
    return mapping.get(i, default)

def norm(df):
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df

def load_inputs():
    b = norm(pd.read_csv(BREAKOUTS_PATH))
    i = norm(pd.read_csv(INDICATORS_PATH))
    keep = [c for c in ["date","symbol","close","rsi"] if c in i.columns]
    i = i[keep]
    # compute daily market level from macro file
    macro_raw = pd.read_csv(MACRO_RAW_PATH)
    ml = compute_market_level(norm(macro_raw))  # -> ['date','market_level']
    ml["date"] = pd.to_datetime(ml["date"])
    return b, i, ml

def label_one(sym_ind, entry_date, entry_px, entry_lvl, ml_series):
    """Hybrid exits: TP fixed at entry_lvl; RSI & Time use effective_lvl = min(entry_lvl, current_ml)."""
    # Fixed TP from entry regime
    tp_px = entry_px * (1.0 + lvl(EXIT_TP_BY_LVL, entry_lvl, 0.40))

    # start the forward window; we’ll enforce a dynamic cap as regime worsens
    fwd = sym_ind[sym_ind["date"] > entry_date].sort_values("date").copy()
    if fwd.empty:
        return entry_date, float(entry_px), "Time", 0

    prev_rsi = None
    bars_held = 0
    # dynamic max-hold cap that can only tighten (min so far)
    dyn_cap = float("inf")

    for r in fwd.itertuples(index=False):
        bars_held += 1
        # current market level for this date (falls back to entry_lvl if missing)
        ml_today = int(ml_series.get(r.date, entry_lvl))
        eff_lvl = min(int(entry_lvl), ml_today)

        # tighten the max-hold using current regime
        dyn_cap = min(dyn_cap, lvl(EXIT_BARS_BY_LVL, eff_lvl, 8))

        # TP first (fixed to entry regime)
        px  = float(r.close)
        if px >= tp_px:
            return r.date, px, "TP", bars_held

        # RSI exit: prev > rsi_max(eff_lvl) and now < rsi_max(eff_lvl) - 5
        rsi = float(getattr(r, "rsi", float("inf")))
        rsi_max = lvl(RSI_MAX_BY_LVL, eff_lvl, 85.0)
        if prev_rsi is not None and (prev_rsi > rsi_max) and (rsi < rsi_max - 5):
            return r.date, px, "RSI", bars_held
        prev_rsi = rsi

        # TIME exit if we’ve hit the tightened cap
        if bars_held >= dyn_cap:
            return r.date, px, "Time", bars_held

    # if we ran out of data before hitting a condition, exit on the last row
    last = fwd.iloc[-1]
    return last["date"], float(last["close"]), "Time", bars_held

def main():
    df_b, df_i, df_ml = load_inputs()
    df_i = df_i.sort_values(["symbol","date"]).reset_index(drop=True)

    # map score names to requested columns
    score_map = {
        "score_trd":"TRD","score_vty":"VTY","score_vol":"VOL","score_mom":"MOM",
        "score_total":"TOTAL","score_norm":"TOTAL"
    }

    # quick dict for market level by date
    ml_dict = dict(zip(df_ml["date"], df_ml["market_level"]))

    out = []
    for sym, grp in tqdm(df_b.groupby("symbol", sort=True), desc="Labelling exits"):
        sym_ind = df_i[df_i["symbol"] == sym]
        if sym_ind.empty:
            continue
        for _, br in grp.sort_values("entry_date").iterrows():
            edate = pd.to_datetime(br["entry_date"])
            epx   = float(br["entry_price"])
            lvl_entry = br.get("market_level", 5)

            xdate, xpx, reason, bars = label_one(sym_ind, edate, epx, lvl_entry, ml_dict)

            # collect module scores at entry
            scores = {}
            for k, v in br.items():
                lk = str(k).lower()
                if lk in score_map:
                    scores[score_map[lk]] = v
            if "TOTAL" not in scores:
                scores["TOTAL"] = sum(scores.get(s, 0) for s in ("TRD","VTY","VOL","MOM"))

            out.append({
                "symbol": sym,
                "entry_date": edate,
                "entry_price": epx,
                "exit_date": xdate,
                "exit_price": float(xpx),
                "exit_reason": reason,                     # TP / RSI / Time
                "market_level": int(round(float(lvl_entry))) if pd.notna(lvl_entry) else 5,
                "TRD": scores.get("TRD"),
                "VTY": scores.get("VTY"),
                "VOL": scores.get("VOL"),
                "MOM": scores.get("MOM"),
                "total_score": scores.get("TOTAL"),
                "ret_pct": (float(xpx) / epx) - 1.0,
                "hold_days": (pd.to_datetime(xdate) - edate).days if pd.notna(xdate) else 0,
            })

    out = pd.DataFrame(out)
    out.to_csv(OUT_PATH, index=False)
    print(f"✅ wrote {OUT_PATH} with {len(out):,} rows")

if __name__ == "__main__":
    main()
