# modules/mom.py

def score_scale(value, yellow, green):
    if value >= green:
        return 1.0
    elif value >= yellow:
        return 0.5
    else:
        return 0.0

def score_mom(row):
    # Thresholds from M18 Pine v7.1
    y_rsi,   g_rsi   = 50.0, 60.0
    y_stoch, g_stoch = 20.0, 80.0
    y_macd,  g_macd  = 0.0,  0.0
    y_hist,  g_hist  = 0.0,  0.0

    m_rsi   = score_scale(row["rsi"],        y_rsi,   g_rsi)
    m_stoch = score_scale(row["stoch"],      y_stoch, g_stoch)
    m_macd  = score_scale(row["macd"],       y_macd,  g_macd)
    m_hist  = score_scale(row["macd_slope"], y_hist,  g_hist)

    score = (
        m_rsi   * 2.0 +
        m_stoch * 2.0 +
        m_macd  * 3.0 +
        m_hist  * 2.0
    )

    return {
        "score_mom": score,
        "rsi": m_rsi,
        "stoch": m_stoch,
        "macd": m_macd,
        "hist": m_hist
    }
