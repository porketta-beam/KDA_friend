import pandas as pd
import logging

logger = logging.getLogger(__name__)

def filter_benjamin_graham(df: pd.DataFrame) -> pd.Series:
    """
    Benjamin Graham의 기준으로 주식을 필터링.
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
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
    return is_pick

def filter_ken_fisher(df: pd.DataFrame) -> pd.Series:
    """
    Ken Fisher의 기준으로 주식을 필터링.
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
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
    return is_pick

def filter_peter_lynch(df: pd.DataFrame) -> pd.Series:
    """
    Peter Lynch GARP 전략으로 주식을 필터링.
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
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
    return is_pick

def filter_jesse_livermore(df: pd.DataFrame) -> pd.Series:
    """
    Jesse Livermore의 기술적 전략으로 주식을 필터링.
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
    """
    universe_filter = (
        (df['Avg_Daily_Volume_20'] >= 200000) &
        (df['Market_Cap'] >= 50000000000)
    )
    entry_signal = (
        (df['Entry_Signal'] == True)
    )
    return universe_filter & entry_signal

def filter_mark_minervini(df: pd.DataFrame) -> pd.Series:
    """
    Mark Minervini의 전략으로 주식을 필터링.
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
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
    
    Args:
        df: Single ticker DataFrame with Date index
    Returns:
        pd.Series: 필터링 조건 충족 여부 (index: Date)
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