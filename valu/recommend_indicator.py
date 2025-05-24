import yfinance as yf
import pandas as pd
import ta

def get_indicator_priority_scores(ticker: str, start='2023-01-01', end='2025-01-01'):
    """기술적 지표 우선순위 점수 계산"""
    df = yf.download(ticker, start=start, end=end)
    df.columns = df.columns.droplevel(1)
    df.dropna(inplace=True)

    scores = {
        'RSI': 0,
        'MACD': 0,
        'Stochastic': 0,
        'BB': 0,
        'EMA': 0,
        'SMA': 0
    }

    # 1. RSI
    rsi = ta.momentum.RSIIndicator(df['Close']).rsi()
    latest_rsi = rsi.iloc[-1]
    if latest_rsi < 30:
        scores['RSI'] += 4  # 과매도
    elif latest_rsi > 70:
        scores['RSI'] += 2  # 과매수

    # 2. MACD
    macd = ta.trend.MACD(df['Close'])
    macd_diff = macd.macd_diff().iloc[-1]
    if macd_diff > 0:
        scores['MACD'] += 3  # Signal 상향 돌파
    else:
        scores['MACD'] += 1

    # 3. Stochastic Oscillator
    stoch = ta.momentum.StochasticOscillator(df['High'], df['Low'], df['Close'])
    stoch_k = stoch.stoch()
    stoch_d = stoch.stoch_signal()
    if stoch_k.iloc[-1] < 20 and stoch_k.iloc[-1] > stoch_d.iloc[-1]:
        scores['Stochastic'] += 4  # 골든크로스 + 과매도

    # 4. Bollinger Bands
    bb = ta.volatility.BollingerBands(df['Close'])
    lower_band = bb.bollinger_lband().iloc[-1]
    if df['Close'].iloc[-1] < lower_band:
        scores['BB'] += 4  # 하단 밴드 이탈

    # 5. EMA (12, 26)
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    if ema12.iloc[-2] < ema26.iloc[-2] and ema12.iloc[-1] > ema26.iloc[-1]:
        scores['EMA'] += 5  # 골든크로스
    elif ema12.iloc[-1] > ema26.iloc[-1]:
        scores['EMA'] += 2

    # 6. SMA (20, 60)
    sma20 = df['Close'].rolling(window=20).mean()
    sma60 = df['Close'].rolling(window=60).mean()
    if sma20.iloc[-2] < sma60.iloc[-2] and sma20.iloc[-1] > sma60.iloc[-1]:
        scores['SMA'] += 5  # 골든크로스
    elif sma20.iloc[-1] > sma60.iloc[-1]:
        scores['SMA'] += 2

    score_df = pd.DataFrame(list(scores.items()), columns=['Indicator', 'Score'])
    score_df.sort_values(by='Score', ascending=False, inplace=True)

    return df, score_df