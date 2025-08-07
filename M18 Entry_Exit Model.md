//**@version=**6  
indicator("M18 Final Entry/Exit Model v7.1", overlay\=true)

// \=== HELPER FUNCTIONS \===  
// 3-value scoring: 1.0 if ≥ gThresh, 0.5 if ≥ yThresh, else 0.0  
scoreScale(val, yThresh, gThresh) \=\> val \>= gThresh ? 1.0 : val \>= yThresh ? 0.5 : 0.0

// \=== Helper: per-level VOL thresholds \===  
// Returns \[g\_cmf, y\_cmf, g\_vtp, y\_vtp, g\_vs, y\_vs, g\_rvol, y\_rvol\]  
getVolThresholds(level) \=\>  
    switch level  
        1 \=\> \[0.3,0.2,2.0,1.5,1.5,1.0,1.2,0.8\]  
        2 \=\> \[0.4,0.3,2.2,1.6,1.6,1.1,1.3,0.9\]  
        3 \=\> \[0.5,0.4,2.4,1.7,1.7,1.2,1.4,1.0\]  
        4 \=\> \[0.6,0.5,2.6,1.8,1.8,1.3,1.5,1.1\]  
        5 \=\> \[0.7,0.6,2.8,1.9,1.9,1.4,1.6,1.2\]  
        6 \=\> \[0.8,0.7,3.0,2.0,2.0,1.5,1.7,1.3\]  
        7 \=\> \[0.9,0.8,3.2,2.1,2.1,1.6,1.8,1.4\]  
        8 \=\> \[1.0,0.9,3.4,2.2,2.2,1.7,1.9,1.5\]  
        9 \=\> \[1.2,1.0,3.6,2.4,2.4,1.8,2.0,1.6\]  
        \=\> \[0.7,0.6,2.8,1.9,1.9,1.4,1.6,1.2\]

// \=== TREND MODULE COLOR THRESHOLDS \===  
trd\_y \= input.float(27.0, "TRD Yellow Threshold", step\=1)  
trd\_g \= input.float(43.0, "TRD Green Threshold",  step\=1)

// \=== TREND SUB-INDICATOR THRESHOLDS \===  
e10\_y  \= input.float(0.00, "EMA10 Yellow Thresh", step\=0.001)  
e10\_g  \= input.float(0.00, "EMA10 Green Thresh",  step\=0.001)  
e50\_y  \= input.float(0.00, "EMA50 Yellow Thresh", step\=0.001)  
e50\_g  \= input.float(0.00, "EMA50 Green Thresh",  step\=0.001)  
e100\_y \= input.float(0.00, "EMA100 Yellow Thresh",step\=0.001)  
e100\_g \= input.float(0.00, "EMA100 Green Thresh", step\=0.001)  
e200\_y \= input.float(0.00, "EMA200 Yellow Thresh",step\=0.001)  
e200\_g \= input.float(0.00, "EMA200 Green Thresh", step\=0.001)

// \=== VTY MODULE COLOR THRESHOLDS \===  
vty\_y  \= input.int(7,  "VTY Yellow Threshold", minval\=0)  
vty\_g  \= input.int(14, "VTY Green Threshold",  minval\=0)

// \=== VTY SUB-INDICATOR THRESHOLDS \===  
y\_ratio \= input.float(1.0,  "ATR Ratio Yellow",  step\=0.1)  
g\_ratio \= input.float(1.2,  "ATR Ratio Green",   step\=0.1)  
y\_atr   \= input.float(1.0,  "ATR% Yellow",       step\=0.1)  
g\_atr   \= input.float(1.5,  "ATR% Green",        step\=0.1)  
y\_std   \= input.float(1.0,  "StdDev% Yellow",    step\=0.1)  
g\_std   \= input.float(1.3,  "StdDev% Green",     step\=0.1)  
y\_bbw   \= input.float(4.0,  "BBW% Yellow",       step\=0.5)  
g\_bbw   \= input.float(6.0,  "BBW% Green",        step\=0.5)  
y\_rng   \= input.float(1.0,  "Range% Yellow",     step\=0.1)  
g\_rng   \= input.float(2.0,  "Range% Green",      step\=0.1)

// \=== VOL MODULE COLOR THRESHOLDS \===  
y\_obv   \= input.float(0.0,  "OBV Yellow Threshold", step\=0.1)  
g\_obv   \= input.float(0.1,  "OBV Green Threshold",  step\=0.1)

// (CMF/VT P/RVOL thresholds come from getVolThresholds(), so no more inputs needed there)

// \=== MOM MODULE COLOR THRESHOLDS \===  
y\_rsi   \= input.float(50.0, "RSI Yellow Threshold",     step\=0.5)  
g\_rsi   \= input.float(60.0, "RSI Green Threshold",      step\=0.5)  
y\_stoch \= input.float(20.0, "Stoch %K Yellow Threshold",step\=0.5)  
g\_stoch \= input.float(80.0, "Stoch %K Green Threshold", step\=0.5)  
y\_macd  \= input.float(0.0,  "MACD Line Yellow Threshold",step\=0.001)  
g\_macd  \= input.float(0.0,  "MACD Line Green Threshold", step\=0.001)  
y\_macds \= input.float(0.0,  "MACD Hist Yellow Threshold",step\=0.001)  
g\_macds \= input.float(0.0,  "MACD Hist Green Threshold", step\=0.001)

// \=== MARKET-LEVEL MODULE v6.10 (continuous 1–9 & 1-bar smoothing) \===  
// 1\) Fetch cap & dominance series  
btcD     \= request.security("CRYPTOCAP:BTC.D",  "D", close)  
usdtD    \= request.security("CRYPTOCAP:USDT.D", "D", close)  
total3   \= request.security("CRYPTOCAP:TOTAL3", "D", close)  
totalCap \= request.security("CRYPTOCAP:TOTAL",  "D", close)

// 2\) Normalize 0–1 over 14 bars (invert dominance for bullish logic)  
posTotal3   \= (total3   \- ta.lowest(total3,14))   / (ta.highest(total3,14)   \- ta.lowest(total3,14))  
posTotalCap \= (totalCap \- ta.lowest(totalCap,14)) / (ta.highest(totalCap,14) \- ta.lowest(totalCap,14))  
posUsdtD    \= 1 \- ((usdtD \- ta.lowest(usdtD,14))   / (ta.highest(usdtD,14)   \- ta.lowest(usdtD,14)))  
posBtcD     \= 1 \- ((btcD  \- ta.lowest(btcD,14))    / (ta.highest(btcD,14)    \- ta.lowest(btcD,14)))

// 3\) Map continuous \[0–1\] → \[1–9\]  
scoreComponent(x) \=\>  
    tmp \= math.round(x\*8) \+ 1  
    math.max(1, math.min(tmp,9))

// 4\) Sum & smooth  
rawSum   \= scoreComponent(posTotal3) \+ scoreComponent(posTotalCap) \+ scoreComponent(posUsdtD) \+ scoreComponent(posBtcD)  
rawAvg   \= rawSum / 4.0  
mktScore \= (rawAvg \+ nz(rawAvg\[1\])) \* 0.5

var **float** market\_level \= na  
market\_level := mktScore

// 6\) Unpack per-level VOL thresholds  
\[g\_cmf, y\_cmf, g\_vtp, y\_vtp, g\_vs, y\_vs, g\_rvol, y\_rvol\] \= getVolThresholds(market\_level)

// \=== INPUTS \===  
lookback   \= input.int(100, "Dynamic Lookback",        minval\=10)  
std\_mult   \= input.float(0.5, "StdDev Multiplier",      step\=0.1)  
static\_adj \= input.float(0.0, "Static Cutoff Adjustment",step\=0.01)

// \=== INDICATORS \===  
ema5    \= ta.ema(close,5)  
ema10   \= ta.ema(close,10)  
ema50   \= ta.ema(close,50)  
ema100  \= ta.ema(close,100)  
ema200  \= ta.ema(close,200)  
\[\_,\_,adx14\]           \= ta.dmi(14,14)  
\[macd\_line,macd\_signal,\_\] \= ta.macd(close,12,26,9)  
rsi    \= ta.rsi(close,14)  
atrs   \= ta.atr(14)  
bbw    \= (ta.sma(close,20)\+2\*ta.stdev(close,20)) \- (ta.sma(close,20)\-2\*ta.stdev(close,20))  
obv    \= ta.cum(close\>close\[1\]? volume : close\<close\[1\]? \-volume : 0)  
lengthCmf \= 20  
mfv       \= ((close\-low)\-(high\-close))/(high\-low)\*volume  
cumMfv    \= ta.cum(mfv)  
cumVol    \= ta.cum(volume)  
sumMfv20  \= cumMfv \- nz(cumMfv\[lengthCmf\])  
sumVol20  \= cumVol \- nz(cumVol\[lengthCmf\])  
cmf       \= sumMfv20/sumVol20

// \=== ADX Gate Thresholds \===  
adx\_y \= switch market\_level  
    1 \=\> 28.67  
    2 \=\> 28.44  
    3 \=\> 29.27  
    4 \=\> 24.28  
    5 \=\> 27.76  
    6 \=\> 28.78  
    7 \=\> 27.52  
    8 \=\> 31.61  
    9 \=\> 33.66  
    \=\> 28.67

adx\_g \= switch market\_level  
    1 \=\> 35.49  
    2 \=\> 36.09  
    3 \=\> 37.79  
    4 \=\> 32.63  
    5 \=\> 40.19  
    6 \=\> 38.53  
    7 \=\> 38.49  
    8 \=\> 42.93  
    9 \=\> 45.08  
    \=\> 35.49

// \=== MODULE SUB-SCORES \===  
// 1\) TREND MODULE (sum → 54\)  
w10  \= 11.2  
w50  \=  9.9  
w100 \= 10.5  
w200 \=  9.2  
wADX \= 13.2  
ema10\_pct  \= (ema10\-ema10\[1\])/ema10\[1\]  
ema50\_pct  \= (ema50\-ema50\[1\])/ema50\[1\]  
ema100\_pct \= (ema100\-ema100\[1\])/ema100\[1\]  
ema200\_pct \= (ema200\-ema200\[1\])/ema200\[1\]  
e10\_score  \= scoreScale(ema10\_pct,  e10\_y,  e10\_g)  
e50\_score  \= scoreScale(ema50\_pct,  e50\_y,  e50\_g)  
e100\_score \= scoreScale(ema100\_pct, e100\_y, e100\_g)  
e200\_score \= scoreScale(ema200\_pct, e200\_y, e200\_g)  
adx\_score  \= scoreScale(adx14,      adx\_y,  adx\_g)  
trend\_score\= e10\_score\*w10 \+ e50\_score\*w50 \+ e100\_score\*w100 \+ e200\_score\*w200 \+ adx\_score\*wADX

// 2\) VTY MODULE (sum → 21\)  
atrRatio  \= atrs/ta.sma(atrs,20)  
atrShort  \= ta.atr(3)  
atrPct    \= 100 \* atrShort/close  
stdDevPct \= ta.stdev(close,20)/ta.sma(close,20)\*100  
bandwidth \= (ta.sma(close,20)\+2\*ta.stdev(close,20)\-(ta.sma(close,20)\-2\*ta.stdev(close,20))) / ta.sma(close,20) \*100  
rangePct  \= (high\-low)/close\*100  
v\_ar      \= scoreScale(atrRatio,   y\_ratio,  g\_ratio)  
v\_at      \= scoreScale(atrPct,     y\_atr,    g\_atr)  
v\_sd      \= scoreScale(stdDevPct,  y\_std,    g\_std)  
v\_bw      \= scoreScale(bandwidth,  y\_bbw,    g\_bbw)  
v\_rg      \= scoreScale(rangePct,   y\_rng,    g\_rng)  
wAR \= 5.0  
wAP \= 4.0  
wSD \= 4.0  
wBW \= 4.0  
wRG \= 4.0  
vty\_score \= v\_ar\*wAR \+ v\_at\*wAP \+ v\_sd\*wSD \+ v\_bw\*wBW \+ v\_rg\*wRG

// 3\) VOL MODULE (sum → 16\)  
obv\_norm     \= obv/ta.sma(obv,20)  
volSpike     \= volume/ta.sma(volume,20)  
volToPrice   \= volume/close  
volSlope\_norm\= (volume\-volume\[1\])/volume\[1\]  
v\_obv        \= scoreScale(obv\_norm,     y\_obv,    g\_obv)  
v\_cmf        \= scoreScale(cmf,          y\_cmf,    g\_cmf)  
v\_volSpike   \= scoreScale(volSpike,     y\_vs,     g\_vs)  
v\_volToPrice \= scoreScale(volToPrice,   y\_vtp,    g\_vtp)  
v\_volSlope   \= scoreScale(volSlope\_norm,y\_rvol,   g\_rvol)  
wOBV    \= 4.0  
wCMF    \= 3.5  
wVSpike \= 3.5  
wVTP    \= 2.5  
wVSlope \= 2.5  
vol\_score    \= v\_obv\*wOBV \+ v\_cmf\*wCMF \+ v\_volSpike\*wVSpike \+ v\_volToPrice\*wVTP \+ v\_volSlope\*wVSlope  
volume\_score \= (obv\>obv\[1\]?1:0) \+ (volume\>ta.sma(volume,20)?1:0)

// 4\) MOM MODULE (sum → 9\)  
stochK   \= ta.stoch(close,high,low,14)  
macdLine \= ta.ema(close,12)\-ta.ema(close,26)  
macdHist \= macdLine\-ta.ema(macdLine,9)  
mom\_rsi     \= scoreScale(rsi,    y\_rsi,  g\_rsi)  
mom\_stoch   \= scoreScale(stochK, y\_stoch, g\_stoch)  
mom\_macd    \= scoreScale(macdLine, y\_macd,  g\_macd)  
mom\_macdsig \= scoreScale(macdHist, y\_macds, g\_macds)  
wRSI  \= 2.0  
wSTO  \= 2.0  
wMACD \= 3.0  
wHIST \= 2.0  
mom\_score \= mom\_rsi\*wRSI \+ mom\_stoch\*wSTO \+ mom\_macd\*wMACD \+ mom\_macdsig\*wHIST

// \=== COMPOSITE & ENTRY LOGIC \===  
total\_score \= trend\_score \+ vty\_score \+ vol\_score \+ mom\_score  // →100  
// weights per market level  
wt\_trend \= switch market\_level  
    1 \=\> 1.4  
    2 \=\> 1.3  
    3 \=\> 1.2  
    4 \=\> 1.1  
    5 \=\> 1.0  
    6 \=\> 0.9  
    7 \=\> 0.8  
    8 \=\> 0.7  
    9 \=\> 0.6  
    \=\> 1.0

// \=== WEIGHTS PER MARKET LEVEL \===  
wt\_mom \= switch market\_level  
    1 \=\> 1.2  
    2 \=\> 1.2  
    3 \=\> 1.2  
    4 \=\> 1.1  
    5 \=\> 1.1  
    6 \=\> 1.2  
    7 \=\> 1.3  
    8 \=\> 1.4  
    9 \=\> 1.5  
    \=\> 1.0

wt\_vol \= switch market\_level  
    1 \=\> 0.8  
    2 \=\> 1.0  
    3 \=\> 1.0  
    4 \=\> 1.1  
    5 \=\> 1.2  
    6 \=\> 1.3  
    7 \=\> 1.4  
    8 \=\> 1.5  
    9 \=\> 1.6  
    \=\> 1.0

wt\_volume \= switch market\_level  
    1 \=\> 1.0  
    2 \=\> 1.0  
    3 \=\> 1.1  
    4 \=\> 1.1  
    5 \=\> 1.1  
    6 \=\> 1.1  
    7 \=\> 1.2  
    8 \=\> 1.3  
    9 \=\> 1.4  
    \=\> 1.0

// \=== TOTAL SCORE COLOR THRESHOLDS \===  
score\_y \= switch market\_level  
    1 \=\> 50.0  
    2 \=\> 35.7  
    3 \=\> 44.1  
    4 \=\> 50.0  
    5 \=\> 57.0  
    6 \=\> 50.0  
    7 \=\> 64.8  
    8 \=\> 74.6  
    9 \=\> 74.6  
    \=\> 57.0

score\_g \= switch market\_level  
    1 \=\> 56.8  
    2 \=\> 56.0  
    3 \=\> 64.8  
    4 \=\> 76.1  
    5 \=\> 82.6  
    6 \=\> 79.9  
    7 \=\> 85.0  
    8 \=\> 87.2  
    9 \=\> 86.5  
    \=\> 82.6

// composite conviction score  
score\_raw  \= trend\_score\*wt\_trend \+ mom\_score\*wt\_mom \+ vty\_score\*wt\_vol \+ volume\_score\*wt\_volume  
score\_norm \= score\_raw / (2\*(wt\_trend\+wt\_mom\+wt\_vol\+wt\_volume))

// \=== ENTRY CUTOFFS: STATIC \+ DYNAMIC \===  
base\_cutoff \= switch market\_level  
    1 \=\> 0.65  
    2 \=\> 0.66  
    3 \=\> 0.68  
    4 \=\> 0.70  
    5 \=\> 0.72  
    6 \=\> 0.75  
    7 \=\> 0.78  
    8 \=\> 0.80  
    9 \=\> 0.82  
    \=\> 0.70

static\_cutoff \= base\_cutoff \+ static\_adj

mean\_score  \= ta.sma(score\_norm, lookback)  
std\_score   \= ta.stdev(score\_norm, lookback)  
dyn\_cutoff  \= mean\_score \+ std\_mult \* std\_score  
entry\_cutoff \= math.max(static\_cutoff, dyn\_cutoff)

// \=== REGIME-AWARE THRESHOLDS \===

// \=== ATR\_pct Gate Thresholds \===  
atr\_pct\_y \= switch market\_level  
    1 \=\> 0.13  
    2 \=\> 0.11  
    3 \=\> 0.11  
    4 \=\> 0.09  
    5 \=\> 0.10  
    6 \=\> 0.10  
    7 \=\> 0.09  
    8 \=\> 0.10  
    9 \=\> 0.09  
    \=\> 0.13

atr\_pct\_g \= switch market\_level  
    1 \=\> 0.21  
    2 \=\> 0.13  
    3 \=\> 0.13  
    4 \=\> 0.11  
    5 \=\> 0.13  
    6 \=\> 0.12  
    7 \=\> 0.11  
    8 \=\> 0.12  
    9 \=\> 0.11  
    \=\> 0.21

// \=== StdDev\_pct Gate Thresholds \===  
stddev\_pct\_y \= switch market\_level  
    1 \=\> 0.28  
    2 \=\> 0.09  
    3 \=\> 0.08  
    4 \=\> 0.06  
    5 \=\> 0.04  
    6 \=\> 0.06  
    7 \=\> 0.04  
    8 \=\> 0.07  
    9 \=\> 0.05  
    \=\> 0.28

stddev\_pct\_g \= switch market\_level  
    1 \=\> 1.57  
    2 \=\> 0.54  
    3 \=\> 0.40  
    4 \=\> 0.38  
    5 \=\> 0.40  
    6 \=\> 0.36  
    7 \=\> 0.27  
    8 \=\> 0.37  
    9 \=\> 0.35  
    \=\> 1.57

// \=== BBW\_pct Gate Thresholds \===  
bbw\_pct\_y \= switch market\_level  
    1 \=\> 1.39  
    2 \=\> 0.40  
    3 \=\> 0.35  
    4 \=\> 0.29  
    5 \=\> 0.18  
    6 \=\> 0.46  
    7 \=\> 0.22  
    8 \=\> 0.36  
    9 \=\> 0.22  
    \=\> 1.39

bbw\_pct\_g \= switch market\_level  
    1 \=\> 6.64  
    2 \=\> 2.46  
    3 \=\> 1.55  
    4 \=\> 1.90  
    5 \=\> 1.79  
    6 \=\> 2.25  
    7 \=\> 1.28  
    8 \=\> 1.85  
    9 \=\> 1.62  
    \=\> 6.64

// \=== Range\_pct Gate Thresholds \===  
range\_pct\_y \= switch market\_level  
    1 \=\> 0.20  
    2 \=\> 0.10  
    3 \=\> 0.12  
    4 \=\> 0.07  
    5 \=\> 0.08  
    6 \=\> 0.09  
    7 \=\> 0.09  
    8 \=\> 0.10  
    9 \=\> 0.08  
    \=\> 0.20

range\_pct\_g \= switch market\_level  
    1 \=\> 0.38  
    2 \=\> 0.16  
    3 \=\> 0.15  
    4 \=\> 0.12  
    5 \=\> 0.14  
    6 \=\> 0.15  
    7 \=\> 0.12  
    8 \=\> 0.14  
    9 \=\> 0.12  
    \=\> 0.38

// \=== OBV\_norm Gate Thresholds \===  
obv\_norm\_y \= switch market\_level  
    1 \=\> 1471909530.0  
    2 \=\>  878162573.0  
    3 \=\> 1546789142.0  
    4 \=\>  917848800.0  
    5 \=\>  329918615.0  
    6 \=\>  781991984.0  
    7 \=\> 1210247337.0  
    8 \=\>  998086676.0  
    9 \=\>  930592664.5  
    \=\> 1471909530.0

obv\_norm\_g \= switch market\_level  
    1 \=\> 6480254523.5  
    2 \=\> 3441636770.5  
    3 \=\> 4424146237.0  
    4 \=\> 4717222862.5  
    5 \=\> 3189725405.5  
    6 \=\> 4047751968.0  
    7 \=\> 5047756923.5  
    8 \=\> 4110746340.5  
    9 \=\> 4957767618.75  
    \=\> 6480254523.5

// \=== CMF Gate Thresholds \===  
cmf\_y \= switch market\_level  
    1 \=\> \-0.11  
    2 \=\> \-0.09  
    3 \=\> \-0.03  
    4 \=\>  0.02  
    5 \=\> \-0.03  
    6 \=\> \-0.04  
    7 \=\>  0.07  
    8 \=\>  0.05  
    9 \=\>  0.05  
    \=\> \-0.11

cmf\_g \= switch market\_level  
    1 \=\> \-0.03  
    2 \=\>  0.02  
    3 \=\>  0.07  
    4 \=\>  0.11  
    5 \=\>  0.07  
    6 \=\>  0.10  
    7 \=\>  0.15  
    8 \=\>  0.14  
    9 \=\>  0.15  
    \=\> \-0.03

// \=== RSI Gate Thresholds \===  
rsi\_y \= switch market\_level  
    1 \=\> 34.17  
    2 \=\> 39.12  
    3 \=\> 43.33  
    4 \=\> 46.43  
    5 \=\> 49.79  
    6 \=\> 48.95  
    7 \=\> 56.99  
    8 \=\> 58.23  
    9 \=\> 60.59  
    \=\> 34.17

rsi\_g \= switch market\_level  
    1 \=\> 40.42  
    2 \=\> 47.00  
    3 \=\> 50.45  
    4 \=\> 52.95  
    5 \=\> 55.65  
    6 \=\> 56.38  
    7 \=\> 63.82  
    8 \=\> 65.26  
    9 \=\> 69.89  
    \=\> 40.42

// \=== RSI Max Cap Threshold (Regime-Aware) \===  
rsi\_max \= switch market\_level  
    1 \=\> 74.0  
    2 \=\> 75.0  
    3 \=\> 76.0  
    4 \=\> 78.0  
    5 \=\> 80.0  
    6 \=\> 82.0  
    7 \=\> 84.0  
    8 \=\> 85.0  
    9 \=\> 87.0  
    \=\> 85.0

// \=== MACD\_line Gate Thresholds \===  
macd\_line\_y \= switch market\_level  
    1 \=\> \-0.08  
    2 \=\> \-0.01  
    3 \=\>  0.00  
    4 \=\>  0.00  
    5 \=\>  0.00  
    6 \=\>  0.00  
    7 \=\>  0.01  
    8 \=\>  0.02  
    9 \=\>  0.02  
    \=\> \-0.08

macd\_line\_g \= switch market\_level  
    1 \=\>  0.00  
    2 \=\>  0.00  
    3 \=\>  0.03  
    4 \=\>  0.04  
    5 \=\>  0.05  
    6 \=\>  0.02  
    7 \=\>  0.06  
    8 \=\>  0.13  
    9 \=\>  0.18  
    \=\>  0.00

// \=== MACD\_Signal Gate Thresholds \===  
macd\_signal\_y \= switch market\_level  
    1 \=\> \-0.02  
    2 \=\>  0.00  
    3 \=\>  0.00  
    4 \=\>  0.00  
    5 \=\>  0.01  
    6 \=\>  0.00  
    7 \=\>  0.00  
    8 \=\>  0.02  
    9 \=\>  0.01  
    \=\> \-0.02

macd\_signal\_g \= switch market\_level  
    1 \=\>  0.01  
    2 \=\>  0.02  
    3 \=\>  0.05  
    4 \=\>  0.06  
    5 \=\>  0.12  
    6 \=\>  0.03  
    7 \=\>  0.04  
    8 \=\>  0.13  
    9 \=\>  0.15  
    \=\>  0.01

// \=== TotalScore Gate Thresholds \===  
totalscore\_y \= switch market\_level  
    1 \=\> 50.00  
    2 \=\> 35.72  
    3 \=\> 44.08  
    4 \=\> 50.00  
    5 \=\> 56.95  
    6 \=\> 50.00  
    7 \=\> 64.76  
    8 \=\> 74.62  
    9 \=\> 74.55  
    \=\> 50.00

totalscore\_g \= switch market\_level  
    1 \=\> 56.82  
    2 \=\> 56.04  
    3 \=\> 64.80  
    4 \=\> 76.06  
    5 \=\> 82.56  
    6 \=\> 79.94  
    7 \=\> 85.00  
    8 \=\> 87.25  
    9 \=\> 86.46  
    \=\> 56.82

// \=== (3) Regime-Aware EMA5 d+3 Gate Thresholds \===  
ema5\_y \= switch market\_level  
    1 \=\> 11.48  
    2 \=\> 14.80  
    3 \=\> 15.57  
    4 \=\> 15.31  
    5 \=\> 19.07  
    6 \=\> 18.48  
    7 \=\> 20.10  
    8 \=\> 18.73  
    9 \=\> 19.07  
    \=\> 11.48

ema5\_g \= switch market\_level  
    1 \=\> 16.33  
    2 \=\> 18.81  
    3 \=\> 20.37  
    4 \=\> 21.06  
    5 \=\> 24.95  
    6 \=\> 23.95  
    7 \=\> 25.61  
    8 \=\> 25.04  
    9 \=\> 24.37  
    \=\> 16.33

// \=== (4) Compute EMA5 % change & gate \===  
ema5\_pct\_3 \= (ema5 \- ema5\[3\]) / ema5\[3\] \* 100  
ema5\_gate  \= ema5\_pct\_3 \>= ema5\_y

// \=== OBV (Custom) \===  
obv\_delta \= close \> close\[1\] ? volume : close \< close\[1\] ? \-volume : 0  
obv\_total\_custom \= ta.cum(obv\_delta)  
obv\_3 \= obv\_total\_custom\[3\]

// \=== CMF (Custom) \===  
mfv\_custom \= ((close \- low) \- (high \- close)) / (high \- low) \* volume  
mfv\_custom := na(mfv\_custom) ? 0 : mfv\_custom  
cmf\_20\_custom \= ta.sma(mfv\_custom, 20) / ta.sma(volume, 20)  
cmf\_3 \= cmf\_20\_custom\[3\]

// \=== Z-SCORE NORMALIZATION \===  
obv\_mean \= ta.sma(obv\_3, 100)  
obv\_std \= ta.stdev(obv\_3, 100)  
z\_obv \= (obv\_3 \- obv\_mean) / obv\_std

cmf\_mean \= ta.sma(cmf\_3, 100)  
cmf\_std \= ta.stdev(cmf\_3, 100)  
z\_cmf \= (cmf\_3 \- cmf\_mean) / cmf\_std

// \=== A/D Composite and Arrow \===  
ad\_comp \= z\_obv \+ z\_cmf  
ad\_arrow \= ad\_comp \> 0.75 ? "▲" : ad\_comp \< \-0.75 ? "▼" : "▬"  
ad\_color \= ad\_comp \> 0.75 ? color.green : ad\_comp \< \-0.75 ? color.red : color.gray

// \=== EMA5 vs EMA10 Differential \===  
ema10\_pct\_3 \= (ema10 \- close) / close \* 100  
ema\_diff \= ema5\_pct\_3 \- ema10\_pct\_3  
ema\_diff\_arrow \= ema\_diff \> 0.75 ? "▲" : ema\_diff \< \-0.75 ? "▼" : "▬"  
ema\_diff\_bg \= ema\_diff \> 0.75 ? color.green : ema\_diff \< \-0.75 ? color.red : color.yellow

// \=== RSI Slope Visual Signal \===  
rsi\_0 \= ta.rsi(close, 14)  
rsi\_3 \= rsi\_0\[3\]  
rsi\_diff \= rsi\_0 \- rsi\_3  
rsi\_arrow \= rsi\_diff \> 0.75 ? "▲" : rsi\_diff \< \-0.75 ? "▼" : "▬"  
rsi\_bg \= rsi \> rsi\_max ? color.red : rsi\_diff \> 0.75 ? color.green : rsi\_diff \< \-0.75 ? color.red : color.yellow

// \=== (5) Exit parameters replaced with multi-line switch blocks \===  
int\_lvl    \= int(math.round(market\_level))  
exit\_tp    \= switch int\_lvl  
    1 \=\> 0.45  
    2 \=\> 0.44  
    3 \=\> 0.42  
    4 \=\> 0.40  
    5 \=\> 0.38  
    6 \=\> 0.36  
    7 \=\> 0.35  
    8 \=\> 0.34  
    9 \=\> 0.33  
    \=\> 0.40

exit\_bars  \= switch int\_lvl  
    1 \=\> 11  
    2 \=\> 10  
    3 \=\>  9  
    4 \=\>  8  
    5 \=\>  7  
    6 \=\>  7  
    7 \=\>  6  
    8 \=\>  6  
    9 \=\>  5  
    \=\>  8

// \=== Trade State & Entry/Reset \===  
var **float** entry\_price \= na  
var **int**   entry\_bar   \= na  
var **bool**  in\_trade    \= false

ema5\_green           \= ema5\_pct\_3 \>= ema5\_g  
score\_green          \= (score\_norm \* 100) \>= score\_g

entry\_conditions\_met \= score\_norm \> entry\_cutoff and score\_norm \> score\_norm\[1\] and ema5\_green and score\_green and rsi \< rsi\_max

if entry\_conditions\_met\[1\]  
    if not in\_trade  
        // first green-dot: open new trade  
        entry\_price := open  
        entry\_bar   := bar\_index  
        in\_trade    := true  
    else  
        // subsequent green-dot: reset both time & price  
        entry\_price := open  
        entry\_bar   := bar\_index

// \=== Exit Signals & Logic \===  
bar\_in\_trade       \= in\_trade and (bar\_index \- entry\_bar \> 0)  
tp\_hit             \= bar\_in\_trade and close \>= entry\_price \* (1 \+ exit\_tp)  
time\_exit          \= bar\_in\_trade and (bar\_index \- entry\_bar) \>= exit\_bars  
rsi\_exit\_trigger   \= bar\_in\_trade and (rsi\[1\] \> rsi\_max) and (rsi \< rsi\_max \- 5)

deg\_thresh \= switch int\_lvl  
    1 \=\> 0.4143  
    2 \=\> 0.3944  
    3 \=\> 0.3634  
    4 \=\> 0.3803  
    5 \=\> 0.4131  
    6 \=\> 0.4121  
    7 \=\> 0.3928  
    8 \=\> 0.3676  
    9 \=\> 0.3965  
    \=\> 0.0

degradation\_exit \= bar\_in\_trade and low \<= entry\_price \* (1 \- deg\_thresh)

exit\_reason       \= tp\_hit ? "TP" : time\_exit ? "TIME" : rsi\_exit\_trigger ? "RSI" : degradation\_exit ? "DEG" : ""  
exit\_now          \= tp\_hit or time\_exit or rsi\_exit\_trigger or degradation\_exit

if exit\_now  
    in\_trade    := false  
    entry\_price := na  
    entry\_bar   := na

// \=== Chart Markers \===  
plotshape(entry\_conditions\_met\[1\], title\="Entry", location\=location.belowbar, color\=color.lime,      style\=shape.circle,      size\=size.tiny)  
plotshape(exit\_now,                  title\="Exit",  location\=location.abovebar, color\=color.red,       style\=shape.triangledown, size\=size.small)

// dashboard table  
var **table** dash \= table.new(position.bottom\_right, 2, 8, frame\_color\=color.gray, frame\_width\=1)  
if barstate.islast  
    ml\_arrow \= market\_level \> market\_level\[1\] ? "▲" : market\_level \< market\_level\[1\] ? "▼" : "▬"  
    table.cell(dash, 0, 0, text\="M/L: "\+ml\_arrow\+" "\+str.tostring(market\_level, "\#.1"), text\_color\=color.white, bgcolor\=color.black)  
    table.cell(dash, 1, 0, text\="Score: "\+str.tostring(total\_score,"\#.0")\+"/100", text\_color\=color.white, bgcolor\= total\_score\>=score\_g?color.green:total\_score\>=score\_y?color.yellow:color.red)  
    table.cell(dash, 0, 1, text\="TRD: "\+str.tostring(trend\_score,"\#.0")\+"/54", text\_color\=color.white, bgcolor\=trend\_score\>=trd\_g?color.green:trend\_score\>=trd\_y?color.yellow:color.red)  
    table.cell(dash, 1, 1, text\="VTY: "\+str.tostring(vty\_score,"\#.0")\+"/21", text\_color\=color.white, bgcolor\=vty\_score\>=vty\_g?color.green:vty\_score\>=vty\_y?color.yellow:color.red)  
    table.cell(dash, 0, 2, text\="VOL: "\+str.tostring(vol\_score,"\#.0")\+"/16", text\_color\=color.white, bgcolor\=vol\_score\>=g\_vtp?color.green:vol\_score\>=y\_vs?color.yellow:color.red)  
    table.cell(dash, 1, 2, text\="MOM: "\+str.tostring(mom\_score,"\#.0")\+"/9", text\_color\=color.white, bgcolor\=mom\_score\>=g\_macd?color.green:mom\_score\>=y\_rsi?color.yellow:color.red)  
    table.cell(dash, 0, 3, text\="EMA5: "\+(ema5\>ema5\[1\]?"↑":"↓")\+" "\+str.tostring(ema5\_pct\_3,"\#.1")\+"%", text\_color\=color.white, bgcolor\= ema5\_pct\_3\>=ema5\_g?color.green:ema5\_pct\_3\>=ema5\_y?color.yellow:color.red)  
    table.cell(dash, 1, 3, text\="EMAΔ: "\+ema\_diff\_arrow\+" "\+str.tostring(ema\_diff, "\#.1"), text\_color\=color.white, bgcolor\=ema\_diff\_bg)  
    table.cell(dash, 0, 4, text\="A/D: "\+ad\_arrow\+" "\+str.tostring(ad\_comp, "\#.1"), text\_color\=color.white, bgcolor\=ad\_comp \> 0.75 ? color.green : ad\_comp \< \-0.75 ? color.red : color.yellow)  
    table.cell(dash, 1, 4, text\="RSI: "\+rsi\_arrow\+" "\+str.tostring(rsi\_diff, "\#.1"), text\_color\=color.white, bgcolor\=rsi\_bg)  
    table.cell(dash, 0, 5, text\="InTrade: "\+(in\_trade?"Yes":"No"), text\_color\=color.white)  
    table.cell(dash, 1, 5, text\="P/L: "\+(in\_trade?str.tostring((close\-entry\_price)/entry\_price\*100,"\#.0")\+"%":""), text\_color\=color.white)  
    table.cell(dash, 0, 6, text\="EXIT", text\_color\=color.white, bgcolor\=color.red, text\_halign\=text.align\_center)  
    table.cell(dash, 1, 6, text\=exit\_reason, bgcolor\=exit\_reason \!= "" ? color.yellow : color.black, text\_color\=color.white, text\_halign\=text.align\_center)  
    table.cell(dash, 0, 7, text\="EP: "\+(in\_trade?str.tostring(entry\_price,"\#.\#\#\#\#\#\#\#\#"):""), text\_color\=color.white, bgcolor\=color.black)  
    table.cell(dash, 1, 7, text\="Bars: "\+(in\_trade?str.tostring(bar\_index\-entry\_bar):""), text\_color\=color.white, bgcolor\=color.black)  
