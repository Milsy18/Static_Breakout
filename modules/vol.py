# modules/vol.py

def get_vol_thresholds(market_level):
    # Returns thresholds in order:
    # [g_cmf, y_cmf, g_vtp, y_vtp, g_vs, y_vs, g_rvol, y_rvol]
    per_level = {
        1: [0.3, 0.2, 2.0, 1.5, 1.5, 1.0, 1.2, 0.8],
        2: [0.4, 0.3, 2.2, 1.6, 1.6, 1.1, 1.3, 0.9],
        3: [0.5, 0.4, 2.4, 1.7, 1.7, 1.2, 1.4, 1.0],
        4: [0.6, 0.5, 2.6, 1.8, 1.8, 1.3, 1.5, 1.1],
        5: [0.7, 0.6, 2.8, 1.9, 1.9, 1.4, 1.6, 1.2],
        6: [0.8, 0.7, 3.0, 2.0, 2.0, 1.5, 1.7, 1.3],
        7: [0.9, 0.8, 3.2, 2.1, 2.1, 1.6, 1.8, 1.4],
        8: [1.0, 0.9, 3.4, 2.2, 2.2, 1.7, 1.9, 1.5],
        9: [1.2, 1.0, 3.6, 2.4, 2.4, 1.8, 2.0, 1.6],
    }
    return per_level.get(market_level, [0.7, 0.6, 2.8, 1.9, 1.9, 1.4, 1.6, 1.2])

def score_scale(value, yellow, green):
    if value >= green:
        return 1.0
    elif value >= yellow:
        return 0.5
    else:
        return 0.0

def score_vol(row, market_level):
    obv_y, obv_g = 0.0, 0.1
    g_cmf, y_cmf, g_vtp, y_vtp, g_vs, y_vs, g_rvol, y_rvol = get_vol_thresholds(market_level)

    v_obv = score_scale(row["obv_norm"],     obv_y,    obv_g)
    v_cmf = score_scale(row["cmf"],          y_cmf,    g_cmf)
    v_vs  = score_scale(row["volSpike"],     y_vs,     g_vs)
    v_vtp = score_scale(row["volToPrice"],   y_vtp,    g_vtp)
    v_slope = score_scale(row["volSlope"],   y_rvol,   g_rvol)

    score = (
        v_obv * 4.0 +
        v_cmf * 3.5 +
        v_vs  * 3.5 +
        v_vtp * 2.5 +
        v_slope * 2.5
    )

    return {
        "score_vol": score,
        "obv": v_obv,
        "cmf": v_cmf,
        "volSpike": v_vs,
        "volToPrice": v_vtp,
        "volSlope": v_slope
    }
