# gpt

import pandas as pd
import numpy as np


def calculate_indicators(data: dict) -> pd.DataFrame:
    """
    Vectorized calculation of financial and technical indicators.
    Expects data dict with keys:
      - history: OHLCV DataFrame indexed by Date
      - quarterly_balance_sheet, quarterly_financials, quarterly_cash_flow: DataFrames indexed by quarter-end Date
      - financials, balance_sheet, cash_flow: annual DataFrames indexed by year-end Date
    """
    hist = data['history'].sort_index()
    idx = hist.index

    # Forward-fill quarterly series to daily frequency
    def ffill_quarter(qdf):
        return qdf.reindex(idx).ffill().bfill()

    bs = data['balance_sheet']
    fin = data['financials']
    cf = data['cash_flow']

    bs_q = ffill_quarter(data['quarterly_balance_sheet'])
    fin_q = ffill_quarter(data['quarterly_financials'])
    cf_q = ffill_quarter(data['quarterly_cash_flow'])

    # TTM sums (4 quarters)
    fin_ttm = data['quarterly_financials'].rolling(4).sum().rename(columns=lambda c: f"{c}_TTM")
    fin_ttm = fin_ttm.reindex(idx).ffill().bfill()
    bs_ttm = data['quarterly_balance_sheet'].rolling(4).sum().rename(columns=lambda c: f"{c}_TTM")
    bs_ttm = bs_ttm.reindex(idx).ffill().bfill()
    cf_ttm = data['quarterly_cash_flow'].rolling(4).sum().rename(columns=lambda c: f"{c}_TTM")
    cf_ttm = cf_ttm.reindex(idx).ffill().bfill()
        
    # Market cap
    shares = bs_q['Ordinary Shares Number'].replace(0, np.nan)
    market_cap = hist['Close'] * shares

    # # Build result DataFrame
    result = pd.DataFrame(index=idx)
    result['Close'] = hist['Close']
    result['Avg_Daily_Volume'] = hist['Volume'].rolling(252).mean()
    result['Market_Cap'] = market_cap


    # get()의 두 번째 인자는 키가 없을 때 반환할 디폴트값
    tca = bs_q.get('Total Current Assets', bs_q.get('Current Assets'))
    if tca is None:
        raise KeyError("유동자산 컬럼을 찾을 수 없습니다.")
    
    tcl = bs_q.get('Total Current Liabilities', bs_q.get('Current Liabilities'))
    if tca is None:
        raise KeyError("유동부채 컬럼을 찾을 수 없습니다.")
    
    capex_ttm = cf_ttm.get('Capital Expenditures_TTM', cf_ttm.get('Capital Expenditure_TTM'))
    if tca is None:
        raise KeyError("총자본적지출 컬럼을 찾을 수 없습니다.")
    
    capex = cf.get('Capital Expenditures', cf.get('Capital Expenditure'))
    if tca is None:
        raise KeyError("총자본적지출 컬럼을 찾을 수 없습니다.")
    
    interest_expense = fin_q['Interest Expense'] if 'Interest Expense' in fin_q.columns \
                  else fin_q['Interest Income'] - fin_q['Net Interest Income']
    if interest_expense is None:
        raise KeyError("이자비용 컬럼을 찾을 수 없습니다.")


    # Benjamin Graham
    result['NCAV_to_MarketCap'] = (tca - bs_q['Total Liabilities Net Minority Interest']) / market_cap
    result['PB_Ratio'] = market_cap / bs_q['Common Stock Equity']
    result['PE_Ratio'] = market_cap / fin_ttm['Net Income_TTM']
    result['Dividend_Yield'] = hist['Dividends'].rolling(252).sum() / hist['Close'] * 100
    result['ICR'] = fin_q['EBIT'] / interest_expense.abs()
    result['Debt_Ratio'] = bs_q['Total Liabilities Net Minority Interest'] / bs_q['Total Assets'] * 100
    result['Current_Ratio'] = tca / tcl
    result['Quick_Ratio'] = (tca - bs_q['Inventory']) / tcl

    # Profit consistency (annual)
    annual_income = fin['Net Income']
    good_years = annual_income.resample('A').sum() > 0
    consistent = good_years.rolling(5).apply(lambda x: x.all(), raw=True).reindex(idx, method='ffill')
    result['Profit_Consistency'] = consistent.astype(bool)

    # EPS CAGR (5 years)
    eps = fin['Net Income'] / bs['Ordinary Shares Number']
    eps_cagr = ((eps.resample('A').last().pct_change(5) + 1) ** (1/5) - 1) * 100
    result['EPS_CAGR'] = eps_cagr.reindex(idx, method='ffill')

    # Ken Fisher
    result['PSR'] = market_cap / fin_ttm['Total Revenue_TTM']
    fcf = cf_ttm['Operating Cash Flow_TTM'] - capex_ttm
    result['FCF_Yield'] = fcf / market_cap * 100
    revenue = fin['Total Revenue'].resample('A').last()
    result['Sales_CAGR'] = ((revenue.pct_change(5) + 1) ** (1/5) - 1).mul(100).reindex(idx, method='ffill')
    fcf_annual = (cf['Operating Cash Flow'] - capex)
    result['FCF_CAGR'] = ((fcf_annual.resample('A').last().pct_change(3) + 1) ** (1/3) - 1).mul(100).reindex(idx, method='ffill')
    result['ROE'] = fin_q['Net Income'].rolling(4).sum() / bs_q['Common Stock Equity'] * 100
    result['Debt_to_Equity'] = bs_q['Total Debt'] / bs_q['Common Stock Equity'] * 100

    # Returns
    result['12M_Return'] = hist['Close'].pct_change(252) * 100
    result['6M_Return'] = hist['Close'].pct_change(126) * 100

    # Mark Minervini technicals
    ma50 = hist['Close'].rolling(50).mean()
    ma150 = hist['Close'].rolling(150).mean()
    ma200 = hist['Close'].rolling(200).mean()
    result['Above_50MA'] = hist['Close'] > ma50
    result['50MA_Uptrend'] = ma50.diff(30) > 0
    result['MA_Alignment'] = (ma50 > ma150) & (ma150 > ma200)
    high_52w = hist['High'].rolling(252).max()
    low_52w = hist['Low'].rolling(252).min()
    result['Near_52W_High'] = hist['Close'] >= high_52w * 0.95
    result['Above_52W_Low'] = hist['Close'] > low_52w * 1.3
    result['35D_Volatility'] = (hist['High'].rolling(35).max() - hist['Low'].rolling(35).min()) / hist['Low'].rolling(35).min() * 100
    result['Base_High'] = hist['High'].rolling(180).apply(lambda x: x[-10:].max() == x.max())

    # VCP: approximate: price consolidation + volume drop
    vol = hist['Volume']
    base_vol = vol.shift(10).rolling(170).max()
    recent_vol = vol.shift(1).rolling(10).mean()
    result['VCP'] = (recent_vol <= base_vol * 0.5)

    return result
