import pandas as pd
import numpy as np
from datetime import timedelta

def get_quarter_data(df, date, include_previous=False):
    if df.empty or df.index.empty:
        return pd.Series(), pd.Series() if include_previous else pd.Series()
    date = pd.to_datetime(date)
    valid_dates = df.index[df.index <= date]
    if valid_dates.empty:
        closest_date = df.index[0] if not df.index.empty else None
    else:
        closest_date = valid_dates[-1]
    if closest_date is None:
        return pd.Series(), pd.Series() if include_previous else pd.Series()
    current_data = df.loc[closest_date]
    if not include_previous:
        return current_data
    previous_dates = df.index[df.index < closest_date]
    previous_data = df.loc[previous_dates[-1]] if not previous_dates.empty else pd.Series()
    return current_data, previous_data

def get_closest_annual_data(df, date):
    if df.empty or df.index.empty:
        return pd.Series()
    year = pd.to_datetime(date).year
    valid_years = df.index[df.index <= f"{year}-12-31"]
    closest_year = valid_years[-1] if not valid_years.empty else df.index[0]
    return df.loc[closest_year]

def calculate_indicators(data):
    if data is None:
        return None

    result_df = pd.DataFrame(index=data['history'].index)
    result_df['Close'] = data['history']['Close']

    # Benjamin Graham 지표
    result_df['Avg_Daily_Volume'] = data['history']['Volume'].rolling(window=252).mean()
    shares = data['quarterly_balance_sheet'].get('Ordinary Shares Number', pd.Series([1]))
    result_df['Market_Cap'] = data['history']['Close'] * shares.reindex(data['history'].index, method='ffill').iloc[:, 0]

    def calc_ncav(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        current_assets = balance.get('Total Current Assets', 0)
        total_liabilities = balance.get('Total Liabilities Net Minority Interest', 0)
        ncav = current_assets - total_liabilities
        market_cap = row['Market_Cap']
        return ncav / market_cap if market_cap > 0 else 0
    result_df['NCAV_to_MarketCap'] = result_df.apply(calc_ncav, axis=1)

    def calc_pb(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        book_value = balance.get('Common Stock Equity', 0)
        market_cap = row['Market_Cap']
        return market_cap / book_value if book_value > 0 else float('inf')
    result_df['PB_Ratio'] = result_df.apply(calc_pb, axis=1)

    def calc_pe(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        net_income_ttm = data['quarterly_financials']['Net Income'][:4].sum() if not data['quarterly_financials'].empty else 0
        market_cap = row['Market_Cap']
        return market_cap / net_income_ttm if net_income_ttm > 0 else float('inf')
    result_df['PE_Ratio'] = result_df.apply(calc_pe, axis=1)

    def calc_dividend_yield(row):
        dividends_ttm = data['history']['Dividends'][row.name - timedelta(days=365):row.name].sum()
        close = row['Close']
        return (dividends_ttm / close) * 100 if close > 0 else 0
    result_df['Dividend_Yield'] = result_df.apply(calc_dividend_yield, axis=1)

    def calc_icr(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        ebit = financials.get('EBIT', 0)
        interest_expense = financials.get('Interest Expense', 1)
        return ebit / abs(interest_expense) if interest_expense != 0 else float('inf')
    result_df['ICR'] = result_df.apply(calc_icr, axis=1)

    def calc_debt_ratio(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        total_liabilities = balance.get('Total Liabilities Net Minority Interest', 0)
        total_assets = balance.get('Total Assets', 1)
        return (total_liabilities / total_assets) * 100 if total_assets > 0 else 100
    result_df['Debt_Ratio'] = result_df.apply(calc_debt_ratio, axis=1)

    def calc_current_ratio(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        current_assets = balance.get('Total Current Assets', 0)
        current_liabilities = balance.get('Total Current Liabilities', 1)
        return current_assets / current_liabilities if current_liabilities > 0 else 0
    result_df['Current_Ratio'] = result_df.apply(calc_current_ratio, axis=1)

    def calc_quick_ratio(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        current_assets = balance.get('Total Current Assets', 0)
        inventory = balance.get('Inventory', 0)
        current_liabilities = balance.get('Total Current Liabilities', 1)
        return (current_assets - inventory) / current_liabilities if current_liabilities > 0 else 0
    result_df['Quick_Ratio'] = result_df.apply(calc_quick_ratio, axis=1)

    def calc_profit_consistency(row):
        financials = data['financials']
        years = sorted(financials.index.year.unique())[-5:]
        if len(years) < 5:
            return False
        for year in years:
            net_income = financials.loc[str(year), 'Net Income'] if str(year) in financials.index else 0
            if net_income <= 0:
                return False
        return True
    result_df['Profit_Consistency'] = result_df.apply(calc_profit_consistency, axis=1)

    def calc_eps_cagr(row):
        financials = data['financials']
        balance = data['balance_sheet']
        years = sorted(financials.index.year.unique())[-5:]
        if len(years) < 5:
            return 0
        eps_values = []
        for year in years:
            net_income = financials.loc[str(year), 'Net Income'] if str(year) in financials.index else 0
            shares = balance.loc[str(year), 'Ordinary Shares Number'] if str(year) in balance.index else 1
            eps = net_income / shares if shares > 0 else 0
            eps_values.append(eps)
        return ((eps_values[-1] / eps_values[0]) ** (1/5) - 1) * 100 if eps_values[0] > 0 else 0
    result_df['EPS_CAGR'] = result_df.apply(calc_eps_cagr, axis=1)

    # Ken Fisher 지표
    def calc_psr(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        revenue_ttm = data['quarterly_financials']['Total Revenue'][:4].sum() if not data['quarterly_financials'].empty else 0
        market_cap = row['Market_Cap']
        return market_cap / revenue_ttm if revenue_ttm > 0 else float('inf')
    result_df['PSR'] = result_df.apply(calc_psr, axis=1)

    def calc_fcf_yield(row):
        cashflow = get_quarter_data(data['quarterly_cash_flow'], row.name)[0]
        op_cashflow_ttm = data['quarterly_cash_flow']['Operating Cash Flow'][:4].sum() if not data['quarterly_cash_flow'].empty else 0
        capex_ttm = data['quarterly_cash_flow']['Capital Expenditures'][:4].sum() if not data['quarterly_cash_flow'].empty else 0
        fcf = op_cashflow_ttm - capex_ttm
        market_cap = row['Market_Cap']
        return (fcf / market_cap) * 100 if market_cap > 0 else 0
    result_df['FCF_Yield'] = result_df.apply(calc_fcf_yield, axis=1)

    def calc_sales_cagr(row):
        financials = data['financials']
        years = sorted(financials.index.year.unique())[-5:]
        if len(years) < 5:
            return 0
        revenues = [financials.loc[str(year), 'Total Revenue'] if str(year) in financials.index else 0 for year in years]
        return ((revenues[-1] / revenues[0]) ** (1/5) - 1) * 100 if revenues[0] > 0 else 0
    result_df['Sales_CAGR'] = result_df.apply(calc_sales_cagr, axis=1)

    def calc_fcf_cagr(row):
        cashflow = data['cash_flow']
        years = sorted(cashflow.index.year.unique())[-3:]
        if len(years) < 3:
            return 0
        fcf_values = []
        for year in years:
            op_cashflow = cashflow.loc[str(year), 'Operating Cash Flow'] if str(year) in cashflow.index else 0
            capex = cashflow.loc[str(year), 'Capital Expenditures'] if str(year) in cashflow.index else 0
            fcf = op_cashflow - capex
            fcf_values.append(fcf)
        return ((fcf_values[-1] / fcf_values[0]) ** (1/3) - 1) * 100 if fcf_values[0] > 0 else 0
    result_df['FCF_CAGR'] = result_df.apply(calc_fcf_cagr, axis=1)

    def calc_roe(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        net_income_ttm = data['quarterly_financials']['Net Income'][:4].sum() if not data['quarterly_financials'].empty else 0
        equity = balance.get('Common Stock Equity', 1)
        return (net_income_ttm / equity) * 100 if equity > 0 else 0
    result_df['ROE'] = result_df.apply(calc_roe, axis=1)

    def calc_debt_to_equity(row):
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        total_debt = balance.get('Total Debt', 0)
        equity = balance.get('Common Stock Equity', 1)
        return (total_debt / equity) * 100 if equity > 0 else 100
    result_df['Debt_to_Equity'] = result_df.apply(calc_debt_to_equity, axis=1)

    def calc_12m_return(row):
        past_12m = row.name - timedelta(days=365)
        past_1m = row.name - timedelta(days=30)
        close_series = data['history']['Close']
        close_12m = close_series[close_series.index <= past_12m].iloc[-1] if not close_series.empty else np.nan
        close_1m = close_series[close_series.index <= past_1m].iloc[-1] if not close_series.empty else np.nan
        return ((close_1m / close_12m) - 1) * 100 if pd.notna(close_12m) and pd.notna(close_1m) else np.nan
    result_df['12M_Return'] = result_df.apply(calc_12m_return, axis=1)

    def calc_6m_return(row):
        past_6m = row.name - timedelta(days=180)
        close_series = data['history']['Close']
        close_6m = close_series[close_series.index <= past_6m].iloc[-1] if not close_series.empty else np.nan
        close = row['Close']
        return ((close / close_6m) - 1) * 100 if pd.notna(close_6m) else np.nan
    result_df['6M_Return'] = result_df.apply(calc_6m_return, axis=1)

    # Mark Minervini 지표
    def calc_annual_sales_growth(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        revenue_ttm = data['quarterly_financials']['Total Revenue'][:4].sum() if not data['quarterly_financials'].empty else 0
        prev_revenue_ttm = data['quarterly_financials']['Total Revenue'][4:8].sum() if len(data['quarterly_financials']) >= 8 else 0
        return ((revenue_ttm / prev_revenue_ttm) - 1) * 100 if prev_revenue_ttm > 0 else 0
    result_df['Annual_Sales_Growth'] = result_df.apply(calc_annual_sales_growth, axis=1)

    def calc_annual_eps_growth(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        net_income_ttm = data['quarterly_financials']['Net Income'][:4].sum() if not data['quarterly_financials'].empty else 0
        prev_net_income_ttm = data['quarterly_financials']['Net Income'][4:8].sum() if len(data['quarterly_financials']) >= 8 else 0
        shares = balance.get('Ordinary Shares Number', 1)
        eps_ttm = net_income_ttm / shares if shares > 0 else 0
        prev_eps_ttm = prev_net_income_ttm / shares if shares > 0 else 0
        return ((eps_ttm / prev_eps_ttm) - 1) * 100 if prev_eps_ttm > 0 else 0
    result_df['Annual_EPS_Growth'] = result_df.apply(calc_annual_eps_growth, axis=1)

    def calc_net_margin(row):
        financials = get_quarter_data(data['quarterly_financials'], row.name)[0]
        net_income_ttm = data['quarterly_financials']['Net Income'][:4].sum() if not data['quarterly_financials'].empty else 0
        revenue_ttm = data['quarterly_financials']['Total Revenue'][:4].sum() if not data['quarterly_financials'].empty else 1
        return (net_income_ttm / revenue_ttm) * 100 if revenue_ttm > 0 else 0
    result_df['Net_Margin'] = result_df.apply(calc_net_margin, axis=1)

    # 분기별 성장률 지표 (Minervini 추가)
    def calc_quarterly_revenue_growth(row):
        current_financials, prev_financials = get_quarter_data(data['quarterly_financials'], row.name, include_previous=True)
        current_revenue = current_financials.get('Total Revenue', 0)
        prev_revenue = prev_financials.get('Total Revenue', 0)
        return ((current_revenue / prev_revenue) - 1) * 100 if prev_revenue > 0 else 0
    result_df['Quarterly_Revenue_Growth'] = result_df.apply(calc_quarterly_revenue_growth, axis=1)

    def calc_quarterly_eps_growth(row):
        current_financials, prev_financials = get_quarter_data(data['quarterly_financials'], row.name, include_previous=True)
        balance = get_quarter_data(data['quarterly_balance_sheet'], row.name)[0]
        current_net_income = current_financials.get('Net Income', 0)
        prev_net_income = prev_financials.get('Net Income', 0)
        shares = balance.get('Ordinary Shares Number', 1)
        current_eps = current_net_income / shares if shares > 0 else 0
        prev_eps = prev_net_income / shares if shares > 0 else 0
        return ((current_eps / prev_eps) - 1) * 100 if prev_eps > 0 else 0
    result_df['Quarterly_EPS_Growth'] = result_df.apply(calc_quarterly_eps_growth, axis=1)

    def calc_quarterly_net_margin_growth(row):
        current_financials, prev_financials = get_quarter_data(data['quarterly_financials'], row.name, include_previous=True)
        current_net_income = current_financials.get('Net Income', 0)
        current_revenue = current_financials.get('Total Revenue', 1)
        prev_net_income = prev_financials.get('Net Income', 0)
        prev_revenue = prev_financials.get('Total Revenue', 1)
        current_margin = current_net_income / current_revenue if current_revenue > 0 else 0
        prev_margin = prev_net_income / prev_revenue if prev_revenue > 0 else 0
        return ((current_margin / prev_margin) - 1) * 100 if prev_margin > 0 else 0
    result_df['Quarterly_Net_Margin_Growth'] = result_df.apply(calc_quarterly_net_margin_growth, axis=1)

    # 기술적 지표 (Mark Minervini)
    result_df['Above_50MA'] = data['history']['Close'] > data['history']['Close'].rolling(50).mean()
    result_df['50MA_Uptrend'] = data['history']['Close'].rolling(50).mean().diff(30) > 0
    result_df['MA_Alignment'] = (data['history']['Close'].rolling(50).mean() > data['history']['Close'].rolling(150).mean()) & \
                                (data['history']['Close'].rolling(150).mean() > data['history']['Close'].rolling(200).mean())
    result_df['Near_52W_High'] = data['history']['Close'] >= data['history']['High'].rolling(252).max() * 0.95
    result_df['Above_52W_Low'] = data['history']['Close'] > data['history']['Low'].rolling(252).min() * 1.3

    def calc_35d_volatility(row):
        high = data['history']['High'][row.name - timedelta(days=35):row.name].max()
        low = data['history']['Low'][row.name - timedelta(days=35):row.name].min()
        return ((high - low) / low) * 100 if low > 0 else 0
    result_df['35D_Volatility'] = result_df.apply(calc_35d_volatility, axis=1)

    def calc_base_high(row):
        base_high = data['history']['High'][row.name - timedelta(days=180):row.name].max()
        recent_high = data['history']['High'][row.name - timedelta(days=10):row.name].max()
        return recent_high == base_high
    result_df['Base_High'] = result_df.apply(calc_base_high, axis=1)

    def calc_vcp(row):
        base_start = row.name - timedelta(days=180)
        base_end = row.name - timedelta(days=90)
        tr = data['history']['High'][base_start:base_end] - data['history']['Low'][base_start:base_end]
        tr_decreases = (tr.diff() < 0).rolling(20).sum() >= 2
        correction = ((data['history']['High'][base_start:base_end].max() - data['history']['Low'][base_start:base_end].min()) / 
                      data['history']['High'][base_start:base_end].max()) * 100
        volume_drop = data['history']['Volume'][row.name - timedelta(days=10):row.name].mean() <= \
                      data['history']['Volume'][base_start:base_end].max() * 0.5
        return tr_decreases and (8 <= correction <= 35) and volume_drop
    result_df['VCP'] = result_df.apply(calc_vcp, axis=1)

    return result_df