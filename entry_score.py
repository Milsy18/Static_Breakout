# modules/entry_score.py

from .trend import score_trend
from .vty   import score_vty
from .vol   import score_vol
from .mom   import score_mom

def get_score_thresholds(market_level):
    return {
        "score_y": {
            1: 50.0, 2: 35.7, 3: 44.1, 4: 50.0, 5: 57.0,
            6: 50.0, 7: 64.8, 8: 74.6, 9: 74.6
        }.get(market_level, 57.0),
        "score_g": {
            1: 56.8, 2: 56.0, 3: 64.8, 4: 76.1, 5: 82.6,
            6: 79.9, 7: 85.0, 8: 87.2, 9: 86.5
        }.get(market_level, 82.6),
        "base_cutoff": {
            1: 0.65, 2: 0.66, 3: 0.68, 4: 0.70, 5: 0.72,
            6: 0.75, 7: 0.78, 8: 0.80, 9: 0.82
        }.get(market_level, 0.70)
    }

def get_weightings(market_level):
    wt_trend = {
        1: 1.4, 2: 1.3, 3: 1.2, 4: 1.1, 5: 1.0,
        6: 0.9, 7: 0.8, 8: 0.7, 9: 0.6
    }.get(market_level, 1.0)

    wt_mom = {
        1: 1.2, 2: 1.2, 3: 1.2, 4: 1.1, 5: 1.1,
        6: 1.2, 7: 1.3, 8: 1.4, 9: 1.5
    }.get(market_level, 1.0)

    wt_vol = {
        1: 0.8, 2: 1.0, 3: 1.0, 4: 1.1, 5: 1.2,
        6: 1.3, 7: 1.4, 8: 1.5, 9: 1.6
    }.get(market_level, 1.0)

    wt_volume_gate = {
        1: 1.0, 2: 1.0, 3: 1.1, 4: 1.1, 5: 1.1,
        6: 1.1, 7: 1.2, 8: 1.3, 9: 1.4
    }.get(market_level, 1.0)

    return wt_trend, wt_mom, wt_vol, wt_volume_gate

def evaluate_entry(row, market_level, mean_score, std_score, static_adj=0.0, std_mult=0.5):
    # === Compute module scores ===
    trd = score_trend(row, market_level)
    vty = score_vty(row)
    vol = score_vol(row, market_level)
    mom = score_mom(row)

    wt_trend, wt_mom, wt_vol, wt_volume = get_weightings(market_level)

    # === Composite Weighted Score ===
    score_raw = trd["score_trd"] * wt_trend + mom["score_mom"] * wt_mom + vty["score_vty"] * wt_vol + wt_volume
    score_norm = score_raw / (2 * (wt_trend + wt_mom + wt_vol + wt_volume))

    # === Thresholds ===
    thresholds = get_score_thresholds(market_level)
    static_cutoff = thresholds["base_cutoff"] + static_adj
    dyn_cutoff = mean_score + std_mult * std_score
    entry_cutoff = max(static_cutoff, dyn_cutoff)

    entry_signal = (
        score_norm > entry_cutoff and
        score_norm > row.get("score_norm_prev", 0)
    )

    return {
        **trd,
        **vty,
        **vol,
        **mom,
        "score_total": trd["score_trd"] + vty["score_vty"] + vol["score_vol"] + mom["score_mom"],
        "score_norm": score_norm,
        "entry_cutoff": entry_cutoff,
        "entry_signal": entry_signal
    }
