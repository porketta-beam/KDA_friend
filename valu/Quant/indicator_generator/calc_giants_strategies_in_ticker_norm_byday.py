import pandas as pd
import logging

logger = logging.getLogger(__name__)

def normalize_score(series: pd.Series, reverse: bool = False) -> pd.Series:
    """
    시리즈를 0~1로 정규화.
    
    Args:
        series: 정규화할 Pandas Series
        reverse: True면 낮은 값이 높은 점수로 변환
    """
    if series.isna().all():
        return pd.Series(0.0, index=series.index)
    min_val, max_val = series.min(), series.max()
    if min_val == max_val:
        return pd.Series(1.0, index=series.index)
    normalized = (series - min_val) / (max_val - min_val)
    return 1 - normalized if reverse else normalized

def filter_benjamin_graham(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Benjamin Graham의 기준으로 주식을 필터링하고 스코어 계산.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_252'] >= 200000) &
        (df['Market_Cap'] >= 100000000000)
    )
    safety_margin = (
        (df['NCAV_to_MarketCap'] >= 1.0) |
        (df['PB_Ratio'] <= 1.2)
    )
    profitability = (
        (df['PE_Ratio'] <= 10) &
        (df['Dividend_Yield'] >= 2.5) &
        (df['ICR'] >= 5)
    )
    financial_stability = (
        (df['Debt_Ratio'] <= 0.5) &
        (df['Current_Ratio'] >= 2.0) &
        (df['Quick_Ratio'] >= 1.0)
    )
    earnings_consistency = (
        (df['Profit_Consistency'] == True) &
        (df['EPS_CAGR'] >= 5)
    )
    is_pick = (
        universe_filter &
        safety_margin &
        profitability &
        financial_stability &
        earnings_consistency
    )
    score_components = {
        'NCAV_to_MarketCap': 0.25,
        'PE_Ratio': 0.20,
        'Dividend_Yield': 0.10,
        'ICR': 0.10,
        'Debt_Ratio': 0.10,
        'Current_Ratio': 0.05,
        'EPS_CAGR': 0.20
    }
    total_score = pd.Series(0.0, index=df.index)
    for metric, weight in score_components.items():
        if metric not in df.columns:
            logger.warning(f"컬럼 {metric} 누락, 스코어 계산에서 제외")
            continue
        if metric in ['PE_Ratio', 'Debt_Ratio']:
            normalized = normalize_score(df[metric], reverse=True)
        else:
            normalized = normalize_score(df[metric], reverse=False)
        total_score += normalized * weight
    return is_pick, total_score

def filter_ken_fisher(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Ken Fisher의 기준으로 주식을 필터링하고 스코어 계산.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_252'] >= 300000) &
        (df['Market_Cap'] >= 100000000000)
    )
    value_filter = (
        (df['PSR'] < 1.0) &
        (df['FCF_Yield'] >= 5.0) &
        (df['PE_Ratio'] <= 20)
    )
    growth_filter = (
        (df['Sales_CAGR'] >= 12) &
        (df['FCF_CAGR'] >= 10)
    )
    profitability_stability = (
        (df['ROE'] >= 15) &
        (df['Debt_to_Equity'] <= 100)
    )
    momentum_filter = (
        (df['RS_Rank_excl_1M'] >= 0.7) &
        (df['RS_Rank_6M'] >= 0.6)
    )
    is_pick = (
        universe_filter &
        value_filter &
        growth_filter &
        profitability_stability &
        momentum_filter
    )
    score_components = {
        'PSR': 0.20,
        'FCF_Yield': 0.15,
        'Sales_CAGR': 0.15,
        'ROE': 0.15,
        '12M_Return_excl_1M': 0.20,
        '6M_Return': 0.15
    }
    total_score = pd.Series(0.0, index=df.index)
    for metric, weight in score_components.items():
        if metric not in df.columns:
            logger.warning(f"컬럼 {metric} 누락, 스코어 계산에서 제외")
            continue
        if metric == 'PSR':
            normalized = normalize_score(df[metric], reverse=True)
        else:
            normalized = normalize_score(df[metric], reverse=False)
        total_score += normalized * weight
    return is_pick, total_score

def filter_peter_lynch(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Peter Lynch GARP 전략으로 주식을 필터링하고 스코어 계산.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_50'] >= 500000) &
        (df['Market_Cap'] >= 500000000000)
    )
    growth_filter = (
        (df['Revenue_YoY'] >= 15) &
        (df['EPS_YoY'] >= 15) &
        (df['EPS_CAGR'] >= 12)
    )
    valuation_filter = (
        (df['PEG_Ratio'] < 1.0) &
        (df['PE_Ratio'] <= 25) &
        (df['PB_Ratio'] <= 3.0)
    )
    stability_filter = (
        (df['Debt_to_Equity'] <= 100) &
        (df['ICR'] >= 3)
    )
    profitability_filter = (
        (df['ROE'] >= 12) &
        (df['Operating_Margin'] >= 10)
    )
    momentum_filter = (
        (df['RS_Rank_6M'] >= 0.7) &
        (df['Near_52W_High'] == True)
    )
    is_pick = (
        universe_filter &
        growth_filter &
        valuation_filter &
        stability_filter &
        profitability_filter &
        momentum_filter
    )
    score_components = {
        'Revenue_YoY': 0.20,
        'EPS_CAGR': 0.20,
        'PEG_Ratio': 0.15,
        'PE_Ratio': 0.10,
        'ROE': 0.15,
        '6M_Return': 0.20
    }
    total_score = pd.Series(0.0, index=df.index)
    for metric, weight in score_components.items():
        if metric not in df.columns:
            logger.warning(f"컬럼 {metric} 누락, 스코어 계산에서 제외")
            continue
        if metric in ['PEG_Ratio', 'PE_Ratio']:
            normalized = normalize_score(df[metric], reverse=True)
        else:
            normalized = normalize_score(df[metric], reverse=False)
        total_score += normalized * weight
    return is_pick, total_score

def filter_jesse_livermore(df: pd.DataFrame) -> pd.Series:
    """
    Jesse Livermore의 기술적 전략으로 주식을 필터링.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_20'] >= 200000) &
        (df['Market_Cap'] >= 50000000000)
    )
    entry_signal = (
        (df['Close'] > df['Recent_20_Pivot_High']) &
        (df['Volume'] > df['Avg_Daily_Volume_20'] * 1.5)
    )
    return universe_filter & entry_signal

def filter_mark_minervini(df: pd.DataFrame) -> pd.Series:
    """
    Mark Minervini의 전략으로 주식을 필터링.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_252'] >= 100000) &
        (df['Market_Cap'] >= 30000000000)
    )
    fundamental_filter = (
        (df['Revenue_YoY'] >= 25) &
        (df['EPS_YoY'] >= 25) &
        (df['Net_Profit_Margin'] >= 15) &
        (df['ROE'] >= 20) &
        (df['Debt_Ratio'] <= 50)
    )
    technical_filter = (
        (df['Above_50MA'] == True) &
        (df['50MA_Uptrend_30d'] == True) &
        (df['MA_Alignment'] == True) &
        (df['Near_52W_High'] == True) &
        (df['Above_52W_Low'] == True) &
        (df['RS_Rank_excl_1W'] >= 0.7)
    )
    base_filter = (
        (df['35D_Volatility'] <= 15) &
        (df['Base_High'] == True)
    )
    vcp_filter = (
        (df['Base_3_6M'] == True) &
        (df['TR_2down'] == True) &
        (df['Correction_8_35'] == True) &
        (df['VCP'] == True)
    )
    return universe_filter & fundamental_filter & technical_filter & base_filter & vcp_filter

def filter_william_oneil(df: pd.DataFrame) -> pd.Series:
    """
    William O'Neil CANSLIM 전략으로 주식을 필터링.
    """
    universe_filter = (
        (df['Avg_Daily_Volume_50'] >= 200000) &
        (df['Market_Cap'] >= 100000000000)
    )
    earnings_filter = (
        (df['EPS_Q_YoY'] >= 25) &
        (df['EPS_YoY'] >= 20) &
        (df['EPS_CAGR'] >= 15)
    )
    new_filter = (df['Near_52W_High'] == True)
    supply_demand = (df['Volume'] >= df['Avg_Daily_Volume_50'] * 1.5)
    leader_filter = (df['RS_Rank_6M'] >= 0.8)
    market_filter = (
        (df['Above_200_MA'] == True) &
        (df['50MA_Uptrend_20d'] == True)
    )
    return universe_filter & earnings_filter & new_filter & supply_demand & leader_filter & market_filter