# modules/vty.py

def score_scale(value, yellow, green):
    if value >= green:
        return 1.0
    elif value >= yellow:
        return 0.5
    else:
        return 0.0

def score_vty(row):
    # Static thresholds from Pine Script v7.1
    y_ratio, g_ratio = 1.0, 1.2
    y_atr,   g_atr   = 1.0, 1.5
    y_std,   g_std   = 1.0, 1.3
    y_bbw,   g_bbw   = 4.0, 6.0
    y_rng,   g_rng   = 1.0, 2.0

    # Apply scoring
    v_ar = score_scale(row["atr_ratio"],  y_ratio, g_ratio)
    v_at = score_scale(row["atr_pct"],    y_atr,   g_atr)
    v_sd = score_scale(row["stddev_pct"], y_std,   g_std)
    v_bw = score_scale(row["bbw"],        y_bbw,   g_bbw)
    v_rg = score_scale(row["rng"],        y_rng,   g_rng)

    # Apply weights
    score = (
        v_ar * 5.0 +
        v_at * 4.0 +
        v_sd * 4.0 +
        v_bw * 4.0 +
        v_rg * 4.0
    )

    return {
        "score_vty": score,
        "atr_ratio": v_ar,
        "atr_pct": v_at,
        "stddev_pct": v_sd,
        "bbw": v_bw,
        "rng": v_rg
    }
