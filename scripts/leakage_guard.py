#!/usr/bin/env python3
# leakage_guard.py — blocks future-bar & price-like leakage; validates labels
import argparse, re, sys
from pathlib import Path
import pandas as pd

PRICE_LIKE = ("price","open","high","low","close","hlc3","ohlc4","return","ret_","rtn_","pct",
              "gain","loss","drawdown","dd_","pnl","r_","perf","change","entry_price","exit_price")
FUTURE_MARKERS = tuple([f"_t{i}" for i in range(1,32)] + [f"t+{i}" for i in range(1,32)] + ["_tp1","_tp2","_tp3"])

# These columns are TARGETS/labels (may contain future info) and are allowed in train files.
ALLOWED_LABEL_COLS = {"tp_hit","ttl_return","mfe","mae","bars_to_tp","exit_reason_label","market_level_at_entry",
                      "symbol","entry_time","breakout_id"}

def scan(cols, toks, allow=set()):
    out = []
    for c in cols:
        if c in allow: 
            continue
        lc = c.lower()
        if any(t in lc for t in toks):
            out.append(c)
    return sorted(set(out))

def scan_neg(cols):   
    return sorted({c for c in cols if re.search(r"_t-\d+\b", c.lower()) and c not in ALLOWED_LABEL_COLS})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", required=True)
    ap.add_argument("--features", default=None)   # raw wide features (will likely fail due to price-like cols)
    ap.add_argument("--train", default=None)      # leak-safe training features (allowed to contain labels)
    ap.add_argument("--expect-t0-only", action="store_true")
    a = ap.parse_args()

    errs = warns = 0
    lab = pd.read_parquet(a.labels)
    if not {"symbol","entry_time"}.issubset(lab.columns):
        print("[guard] ERROR: labels missing symbol/entry_time"); sys.exit(1)
    dups = lab.duplicated(["symbol","entry_time"]).sum()
    if dups: print(f"[guard] ERROR: {dups} duplicate (symbol,entry_time) rows"); errs += 1
    if "market_level_at_entry" in lab.columns:
        ml = lab["market_level_at_entry"].dropna()
        try:
            ml = ml.astype(int)
            bad = (~ml.isin(range(1,10))).sum()
            if bad: print(f"[guard] ERROR: {bad} invalid market_level_at_entry"); errs += 1
        except Exception:
            print("[guard] WARN: market_level_at_entry not integer-castable"); warns += 1

    def chk(path, tag, allow_labels=False):
        nonlocal errs, warns
        if not path: return
        p = Path(path)
        if not p.exists(): print(f"[guard] WARN: {tag} not found: {p}"); warns += 1; return
        df = pd.read_parquet(p); cols = list(df.columns)
        allow = ALLOWED_LABEL_COLS if allow_labels else set()
        fut = scan(cols, FUTURE_MARKERS, allow)
        if fut: print(f"[guard] ERROR: {tag} has future-bar cols: {fut[:12]}{' ...' if len(fut)>12 else ''}"); errs += 1
        plc = scan(cols, PRICE_LIKE, allow)
        if plc: print(f"[guard] ERROR: {tag} has price-like cols: {plc[:12]}{' ...' if len(plc)>12 else ''}"); errs += 1
        if a.expect_t0_only:
            neg = scan_neg(cols)
            if neg: print(f"[guard] WARN: {tag} has negative-offset cols (ok for analysis, not training): {neg[:12]}{' ...' if len(neg)>12 else ''}"); warns += 1
        need = {"symbol"} | ({"entry_time"} if "entry_time" in cols else set())
        if not need.issubset(cols): print(f"[guard] ERROR: {tag} missing keys {need - set(cols)}"); errs += 1

    # Raw wide features are expected to fail (that’s fine)—we typically don’t pass them here.
    if a.features: chk(a.features, "features", allow_labels=False)
    if a.train:    chk(a.train,    "train",    allow_labels=True)

    if errs: print(f"[guard] FAIL: {errs} error(s), {warns} warning(s)."); sys.exit(1)
    print(f"[guard] OK: no leakage detected. {warns} warning(s).")

if __name__ == "__main__":
    main()
