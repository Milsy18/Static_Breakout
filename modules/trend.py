# modules/trend.py

def get_trend_thresholds(market_level):
    # These are static per Pine Script v7.1 (all zero for now — can be tuned)
    return {
        "e10_y": 0.00, "e10_g": 0.00,
        "e50_y": 0.00, "e50_g": 0.00,
        "e100_y": 0.00, "e100_g": 0.00,
        "e200_y": 0.00, "e200_g": 0.00,
        "adx_y": {
            1: 28.67, 2: 28.44, 3: 29.27, 4: 24.28, 5: 27.76,
            6: 28.78, 7: 27.52, 8: 31.61, 9: 33.66
        }.get(market_level, 28.67),
        "adx_g": {
            1: 35.49, 2: 36.09, 3: 37.79, 4: 32.63, 5: 40.19,
            6: 38.53, 7: 38.49, 8: 42.93, 9: 45.08
        }.get(market_level, 35.49),
    }

def score_scale(value, yellow, green):
    if value >= green:
        return 1.0
    elif value >= yellow:
        return 0.5
    else:
        return 0.0

def score_trend(row, market_level):
    thresholds = get_trend_thresholds(market_level)

    e10  = score_scale(row["ema10_pct"],  thresholds["e10_y"],  thresholds["e10_g"])
    e50  = score_scale(row["ema50_pct"],  thresholds["e50_y"],  thresholds["e50_g"])
    e100 = score_scale(row["ema100_pct"], thresholds["e100_y"], thresholds["e100_g"])
    e200 = score_scale(row["ema200_pct"], thresholds["e200_y"], thresholds["e200_g"])
    adx  = score_scale(row["adx"],        thresholds["adx_y"],  thresholds["adx_g"])

    score = (
        e10  * 11.2 +
        e50  * 9.9  +
        e100 * 10.5 +
        e200 * 9.2  +
        adx  * 13.2
    )

    return {
        "score_trd": score,
        "e10": e10,
        "e50": e50,
        "e100": e100,
        "e200": e200,
        "adx": adx
    }
