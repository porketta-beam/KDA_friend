import pandas as pd
import numpy as np
import vectorbt as vbt
import bt
from pathlib import Path
from typing import Dict, List, Optional, Callable, Tuple
import warnings
warnings.filterwarnings('ignore')

class FinancialIndicators:
    """
    재무 지표 계산을 위한 클래스
    각 지표는 기준일자를 받아서 해당 시점의 값을 반환
    vectorbt와 bt를 활용한 효율적인 연산 구현
    """
    
    @staticmethod
    def load_data(ticker: str, data_path: str = "./data") -> Dict[str, pd.DataFrame]:
        """티커별 데이터 로드"""
        data_path = Path(data_path)
        data = {}
        
        try:
            data['history'] = pd.read_feather(data_path / f"{ticker}_stock_data.feather")
            data['financials'] = pd.read_feather(data_path / f"{ticker}_financials.feather")
            data['quarterly_financials'] = pd.read_feather(data_path / f"{ticker}_quarterly_financials.feather")
            data['balance_sheet'] = pd.read_feather(data_path / f"{ticker}_balance_sheet.feather")
            data['quarterly_balance_sheet'] = pd.read_feather(data_path / f"{ticker}_quarterly_balance_sheet.feather")
            data['cash_flow'] = pd.read_feather(data_path / f"{ticker}_cash_flow.feather")
            data['quarterly_cash_flow'] = pd.read_feather(data_path / f"{ticker}_quarterly_cash_flow.feather")
            
            # 날짜를 인덱스로 설정
            for key in data:
                if 'Date' in data[key].columns:
                    data[key].set_index('Date', inplace=True)
                elif data[key].index.name != 'Date':
                    data[key].index = pd.to_datetime(data[key].index)
                    
        except FileNotFoundError as e:
            print(f"Data file not found for {ticker}: {e}")
            return {}
            
        return data
    
    # ===== 기본 지표들 (Graham 전략용) =====
    @staticmethod
    def market_cap(data: Dict, date: str) -> float:
        """시가총액 계산"""
        try:
            hist = data['history']
            if date in hist.index:
                price = hist.loc[date, 'Close']
                shares = 1_000_000_000  # 임시값
                return price * shares
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def pb_ratio(data: Dict, date: str) -> float:
        """P/B 비율 계산"""
        try:
            hist = data['history']
            bs = data['balance_sheet']
            
            if date not in hist.index:
                return np.nan
                
            price = hist.loc[date, 'Close']
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            book_value = bs.loc[bs_date, 'Total Stockholder Equity'] if 'Total Stockholder Equity' in bs.columns else np.nan
            shares = 1_000_000_000  # 임시값
            
            if book_value > 0 and shares > 0:
                book_value_per_share = book_value / shares
                return price / book_value_per_share
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def pe_ratio(data: Dict, date: str) -> float:
        """P/E 비율 계산"""
        try:
            hist = data['history']
            fin = data['financials']
            
            if date not in hist.index:
                return np.nan
                
            price = hist.loc[date, 'Close']
            fin_date = fin.index[fin.index <= date][-1] if any(fin.index <= date) else fin.index[0]
            net_income = fin.loc[fin_date, 'Net Income'] if 'Net Income' in fin.columns else np.nan
            shares = 1_000_000_000  # 임시값
            
            if net_income > 0 and shares > 0:
                eps = net_income / shares
                return price / eps
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def dividend_yield(data: Dict, date: str) -> float:
        """배당수익률 계산"""
        try:
            hist = data['history']
            cf = data['cash_flow']
            
            if date not in hist.index:
                return np.nan
                
            price = hist.loc[date, 'Close']
            cf_date = cf.index[cf.index <= date][-1] if any(cf.index <= date) else cf.index[0]
            dividends = abs(cf.loc[cf_date, 'Dividends Paid']) if 'Dividends Paid' in cf.columns else 0
            shares = 1_000_000_000  # 임시값
            
            if dividends > 0 and shares > 0 and price > 0:
                dividend_per_share = dividends / shares
                return (dividend_per_share / price) * 100
            return 0
        except:
            return 0
    
    @staticmethod
    def current_ratio(data: Dict, date: str) -> float:
        """유동비율 계산"""
        try:
            bs = data['balance_sheet']
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            current_assets = bs.loc[bs_date, 'Current Assets'] if 'Current Assets' in bs.columns else np.nan
            current_liabilities = bs.loc[bs_date, 'Current Liabilities'] if 'Current Liabilities' in bs.columns else np.nan
            
            if current_liabilities > 0:
                return current_assets / current_liabilities
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def debt_ratio(data: Dict, date: str) -> float:
        """부채비율 계산"""
        try:
            bs = data['balance_sheet']
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            total_debt = bs.loc[bs_date, 'Total Debt'] if 'Total Debt' in bs.columns else 0
            total_assets = bs.loc[bs_date, 'Total Assets'] if 'Total Assets' in bs.columns else np.nan
            
            if total_assets > 0:
                return (total_debt / total_assets) * 100
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def interest_coverage_ratio(data: Dict, date: str) -> float:
        """이자보상배율 계산"""
        try:
            fin = data['financials']
            fin_date = fin.index[fin.index <= date][-1] if any(fin.index <= date) else fin.index[0]
            net_income = fin.loc[fin_date, 'Net Income'] if 'Net Income' in fin.columns else np.nan
            interest_expense = abs(fin.loc[fin_date, 'Interest Expense']) if 'Interest Expense' in fin.columns else 0
            
            if interest_expense > 0:
                ebit = net_income + interest_expense  # 간소화된 계산
                return ebit / interest_expense
            return float('inf')
        except:
            return np.nan
    
    @staticmethod
    def ncav_ratio(data: Dict, date: str) -> float:
        """NCAV/시가총액 비율 계산"""
        try:
            bs = data['balance_sheet']
            market_cap = FinancialIndicators.market_cap(data, date)
            
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            current_assets = bs.loc[bs_date, 'Current Assets'] if 'Current Assets' in bs.columns else np.nan
            total_liabilities = bs.loc[bs_date, 'Total Liab'] if 'Total Liab' in bs.columns else np.nan
            
            if market_cap > 0 and not pd.isna(current_assets) and not pd.isna(total_liabilities):
                ncav = current_assets - total_liabilities
                return ncav / market_cap
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def eps_cagr_5y(data: Dict, date: str) -> float:
        """EPS 5년 CAGR 계산"""
        try:
            fin = data['financials']
            five_years_ago = pd.Timestamp(date) - pd.DateOffset(years=5)
            recent_data = fin[fin.index <= date]
            old_data = fin[fin.index <= five_years_ago]
            
            if len(recent_data) == 0 or len(old_data) == 0:
                return np.nan
                
            recent_ni = recent_data.iloc[-1]['Net Income'] if 'Net Income' in recent_data.columns else np.nan
            old_ni = old_data.iloc[-1]['Net Income'] if 'Net Income' in old_data.columns else np.nan
            shares = 1_000_000_000  # 임시값
            
            if recent_ni > 0 and old_ni > 0 and shares > 0:
                recent_eps = recent_ni / shares
                old_eps = old_ni / shares
                cagr = ((recent_eps / old_eps) ** (1/5) - 1) * 100
                return cagr
            return np.nan
        except:
            return np.nan
    
    # ===== Ken Fisher 전략용 지표들 =====
    @staticmethod
    def price_to_sales_ratio(data: Dict, date: str) -> float:
        """Price-to-Sales Ratio 계산"""
        try:
            hist = data['history']
            fin = data['financials']
            
            if date not in hist.index:
                return np.nan
                
            price = hist.loc[date, 'Close']
            fin_date = fin.index[fin.index <= date][-1] if any(fin.index <= date) else fin.index[0]
            total_revenue = fin.loc[fin_date, 'Total Revenue'] if 'Total Revenue' in fin.columns else np.nan
            shares = 1_000_000_000  # 임시값
            
            if total_revenue > 0 and shares > 0:
                sales_per_share = total_revenue / shares
                return price / sales_per_share
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def fcf_yield(data: Dict, date: str) -> float:
        """Free Cash Flow Yield 계산"""
        try:
            cf = data['cash_flow']
            market_cap = FinancialIndicators.market_cap(data, date)
            
            cf_date = cf.index[cf.index <= date][-1] if any(cf.index <= date) else cf.index[0]
            operating_cf = cf.loc[cf_date, 'Operating Cash Flow'] if 'Operating Cash Flow' in cf.columns else np.nan
            capex = abs(cf.loc[cf_date, 'Capital Expenditures']) if 'Capital Expenditures' in cf.columns else 0
            
            if not pd.isna(operating_cf) and market_cap > 0:
                fcf = operating_cf - capex
                return (fcf / market_cap) * 100
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def sales_cagr_5y(data: Dict, date: str) -> float:
        """5년 매출 CAGR 계산"""
        try:
            fin = data['financials']
            five_years_ago = pd.Timestamp(date) - pd.DateOffset(years=5)
            recent_data = fin[fin.index <= date]
            old_data = fin[fin.index <= five_years_ago]
            
            if len(recent_data) == 0 or len(old_data) == 0:
                return np.nan
                
            recent_sales = recent_data.iloc[-1]['Total Revenue'] if 'Total Revenue' in recent_data.columns else np.nan
            old_sales = old_data.iloc[-1]['Total Revenue'] if 'Total Revenue' in old_data.columns else np.nan
            
            if recent_sales > 0 and old_sales > 0:
                cagr = ((recent_sales / old_sales) ** (1/5) - 1) * 100
                return cagr
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def fcf_cagr_3y(data: Dict, date: str) -> float:
        """3년 자유현금흐름 CAGR 계산"""
        try:
            cf = data['cash_flow']
            three_years_ago = pd.Timestamp(date) - pd.DateOffset(years=3)
            recent_data = cf[cf.index <= date]
            old_data = cf[cf.index <= three_years_ago]
            
            if len(recent_data) == 0 or len(old_data) == 0:
                return np.nan
                
            # 최근 FCF 계산
            recent_cf_data = recent_data.iloc[-1]
            recent_ocf = recent_cf_data.get('Operating Cash Flow', np.nan)
            recent_capex = abs(recent_cf_data.get('Capital Expenditures', 0))
            recent_fcf = recent_ocf - recent_capex if not pd.isna(recent_ocf) else np.nan
            
            # 3년 전 FCF 계산
            old_cf_data = old_data.iloc[-1]
            old_ocf = old_cf_data.get('Operating Cash Flow', np.nan)
            old_capex = abs(old_cf_data.get('Capital Expenditures', 0))
            old_fcf = old_ocf - old_capex if not pd.isna(old_ocf) else np.nan
            
            if recent_fcf > 0 and old_fcf > 0:
                cagr = ((recent_fcf / old_fcf) ** (1/3) - 1) * 100
                return cagr
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def roe(data: Dict, date: str) -> float:
        """ROE 계산"""
        try:
            fin = data['financials']
            bs = data['balance_sheet']
            
            fin_date = fin.index[fin.index <= date][-1] if any(fin.index <= date) else fin.index[0]
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            
            net_income = fin.loc[fin_date, 'Net Income'] if 'Net Income' in fin.columns else np.nan
            equity = bs.loc[bs_date, 'Total Stockholder Equity'] if 'Total Stockholder Equity' in bs.columns else np.nan
            
            if equity > 0 and not pd.isna(net_income):
                return (net_income / equity) * 100
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def debt_to_equity_ratio(data: Dict, date: str) -> float:
        """Debt-to-Equity 비율 계산"""
        try:
            bs = data['balance_sheet']
            bs_date = bs.index[bs.index <= date][-1] if any(bs.index <= date) else bs.index[0]
            
            total_debt = bs.loc[bs_date, 'Total Debt'] if 'Total Debt' in bs.columns else 0
            equity = bs.loc[bs_date, 'Total Stockholder Equity'] if 'Total Stockholder Equity' in bs.columns else np.nan
            
            if equity > 0:
                return (total_debt / equity) * 100
            return np.nan
        except:
            return np.nan
    
    # ===== Mark Minervini 전략용 지표들 (vectorbt 활용) =====
    @staticmethod
    def annual_sales_growth(data: Dict, date: str) -> float:
        """연간 매출 성장률 계산"""
        try:
            fin = data['quarterly_financials']  # 분기별 데이터 사용
            one_year_ago = pd.Timestamp(date) - pd.DateOffset(years=1)
            
            recent_data = fin[fin.index <= date]
            old_data = fin[fin.index <= one_year_ago]
            
            if len(recent_data) == 0 or len(old_data) == 0:
                return np.nan
                
            recent_sales = recent_data.iloc[-1]['Total Revenue'] if 'Total Revenue' in recent_data.columns else np.nan
            old_sales = old_data.iloc[-1]['Total Revenue'] if 'Total Revenue' in old_data.columns else np.nan
            
            if recent_sales > 0 and old_sales > 0:
                return ((recent_sales / old_sales) - 1) * 100
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def annual_eps_growth(data: Dict, date: str) -> float:
        """연간 EPS 성장률 계산"""
        try:
            fin = data['quarterly_financials']
            one_year_ago = pd.Timestamp(date) - pd.DateOffset(years=1)
            
            recent_data = fin[fin.index <= date]
            old_data = fin[fin.index <= one_year_ago]
            
            if len(recent_data) == 0 or len(old_data) == 0:
                return np.nan
                
            recent_ni = recent_data.iloc[-1]['Net Income'] if 'Net Income' in recent_data.columns else np.nan
            old_ni = old_data.iloc[-1]['Net Income'] if 'Net Income' in old_data.columns else np.nan
            shares = 1_000_000_000  # 임시값
            
            if recent_ni > 0 and old_ni > 0 and shares > 0:
                recent_eps = recent_ni / shares
                old_eps = old_ni / shares
                return ((recent_eps / old_eps) - 1) * 100
            return np.nan
        except:
            return np.nan
    
    @staticmethod
    def net_margin(data: Dict, date: str) -> float:
        """순이익률 계산"""
        try:
            fin = data['financials']
            fin_date = fin.index[fin.index <= date][-1] if any(fin.index <= date) else fin.index[0]
            
            net_income = fin.loc[fin_date, 'Net Income'] if 'Net Income' in fin.columns else np.nan
            total_revenue = fin.loc[fin_date, 'Total Revenue'] if 'Total Revenue' in fin.columns else np.nan
            
            if total_revenue > 0 and not pd.isna(net_income):
                return (net_income / total_revenue) * 100
            return np.nan
        except:
            return np.nan

class TechnicalIndicators:
    """
    기술적 지표 계산 클래스 (vectorbt 활용)
    """
    
    @staticmethod
    def calculate_momentum_indicators(price_series: pd.Series) -> Dict[str, pd.Series]:
        """
        모멘텀 지표들을 vectorbt로 효율적으로 계산
        """
        # vectorbt의 returns 계산
        returns = vbt.returns.nb.pct_change_1d_nb(price_series.values)
        returns_series = pd.Series(returns, index=price_series.index)
        
        # 12개월 리턴 (최근 1개월 제외)
        ret_12m_exc1 = returns_series.rolling(252).apply(lambda x: (1 + x).prod() - 1).shift(21)
        
        # 6개월 리턴
        ret_6m = returns_series.rolling(126).apply(lambda x: (1 + x).prod() - 1)
        
        return {
            'ret_12m_exc1': ret_12m_exc1,
            'ret_6m': ret_6m,
            'returns': returns_series
        }
    
    @staticmethod
    def calculate_moving_averages(price_series: pd.Series) -> Dict[str, pd.Series]:
        """
        이동평균들을 vectorbt로 계산
        """
        # vectorbt의 moving average 사용
        ma_50 = vbt.MA.run(price_series, window=50, short_name='MA50').ma
        ma_150 = vbt.MA.run(price_series, window=150, short_name='MA150').ma
        ma_200 = vbt.MA.run(price_series, window=200, short_name='MA200').ma
        
        return {
            'ma_50': ma_50,
            'ma_150': ma_150,
            'ma_200': ma_200
        }
    
    @staticmethod
    def calculate_price_levels(price_series: pd.Series) -> Dict[str, pd.Series]:
        """
        52주 최고가/최저가 등 계산
        """
        # 52주 최고가/최저가
        high_52w = price_series.rolling(252).max()
        low_52w = price_series.rolling(252).min()
        
        # 현재가 대비 52주 최고가 비율
        price_vs_high_52w = price_series / high_52w
        
        # 현재가 대비 52주 최저가 비율
        price_vs_low_52w = price_series / low_52w
        
        return {
            'high_52w': high_52w,
            'low_52w': low_52w,
            'price_vs_high_52w': price_vs_high_52w,
            'price_vs_low_52w': price_vs_low_52w
        }
    
    @staticmethod
    def calculate_volatility_patterns(price_series: pd.Series, volume_series: pd.Series) -> Dict[str, pd.Series]:
        """
        변동성 축소 패턴 관련 지표들 계산
        """
        # 35일 최고-최저 변동폭
        high_35d = price_series.rolling(35).max()
        low_35d = price_series.rolling(35).min()
        range_35d = ((high_35d - low_35d) / low_35d) * 100
        
        # 거래량 관련
        volume_ma_20 = volume_series.rolling(20).mean()
        volume_ratio = volume_series / volume_ma_20
        
        return {
            'range_35d': range_35d,
            'volume_ma_20': volume_ma_20,
            'volume_ratio': volume_ratio
        }

class IndicatorProcessor:
    """
    지표값 계산 및 저장을 위한 클래스 (개선된 버전)
    """
    
    def __init__(self, data_path: str = "./data", output_path: str = "./indicators"):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True)
        
    def get_ticker_list(self) -> List[str]:
        """데이터 폴더에서 티커 리스트 추출"""
        tickers = []
        for file in self.data_path.glob("*_stock_data.feather"):
            ticker = file.stem.replace("_stock_data", "")
            tickers.append(ticker)
        return tickers
    
    def calculate_all_indicators(self, ticker: str) -> pd.DataFrame:
        """특정 티커의 모든 지표값 계산 (기술적 지표 포함)"""
        data = FinancialIndicators.load_data(ticker, self.data_path)
        
        if not data:
            return pd.DataFrame()
        
        # 기준 날짜는 주가 데이터의 날짜 사용
        hist = data['history']
        dates = hist.index
        
        # 기술적 지표들을 vectorbt로 미리 계산
        price_series = hist['Close']
        volume_series = hist['Volume'] if 'Volume' in hist.columns else pd.Series(index=hist.index)
        
        momentum_indicators = TechnicalIndicators.calculate_momentum_indicators(price_series)
        ma_indicators = TechnicalIndicators.calculate_moving_averages(price_series)
        price_level_indicators = TechnicalIndicators.calculate_price_levels(price_series)
        volatility_indicators = TechnicalIndicators.calculate_volatility_patterns(price_series, volume_series)
        
        indicators_data = []
        
        for date in dates:
            date_str = date.strftime('%Y-%m-%d')
            
            # 기본 재무 지표들
            row = {
                'Date': date,
                # Graham 전략 지표들
                'market_cap': FinancialIndicators.market_cap(data, date_str),
                'pb_ratio': FinancialIndicators.pb_ratio(data, date_str),
                'pe_ratio': FinancialIndicators.pe_ratio(data, date_str),
                'dividend_yield': FinancialIndicators.dividend_yield(data, date_str),
                'current_ratio': FinancialIndicators.current_ratio(data, date_str),
                'debt_ratio': FinancialIndicators.debt_ratio(data, date_str),
                'icr': FinancialIndicators.interest_coverage_ratio(data, date_str),
                'ncav_ratio': FinancialIndicators.ncav_ratio(data, date_str),
                'eps_cagr_5y': FinancialIndicators.eps_cagr_5y(data, date_str),
                
                # Ken Fisher 전략 지표들
                'psr': FinancialIndicators.price_to_sales_ratio(data, date_str),
                'fcf_yield': FinancialIndicators.fcf_yield(data, date_str),
                'sales_cagr_5y': FinancialIndicators.sales_cagr_5y(data, date_str),
                'fcf_cagr_3y': FinancialIndicators.fcf_cagr_3y(data, date_str),
                'roe': FinancialIndicators.roe(data, date_str),
                'debt_to_equity': FinancialIndicators.debt_to_equity_ratio(data, date_str),
                
                # Mark Minervini 전략 지표들
                'annual_sales_growth': FinancialIndicators.annual_sales_growth(data, date_str),
                'annual_eps_growth': FinancialIndicators.annual_eps_growth(data, date_str),
                'net_margin': FinancialIndicators.net_margin(data, date_str),
                
                # 기술적 지표들 (vectorbt로 미리 계산된 값)
                'ret_12m_exc1': momentum_indicators['ret_12m_exc1'].loc[date] if date in momentum_indicators['ret_12m_exc1'].index else np.nan,
                'ret_6m': momentum_indicators['ret_6m'].loc[date] if date in momentum_indicators['ret_6m'].index else np.nan,
                'ma_50': ma_indicators['ma_50'].loc[date] if date in ma_indicators['ma_50'].index else np.nan,
                'ma_150': ma_indicators['ma_150'].loc[date] if date in ma_indicators['ma_150'].index else np.nan,
                'ma_200': ma_indicators['ma_200'].loc[date] if date in ma_indicators['ma_200'].index else np.nan,
                'high_52w': price_level_indicators['high_52w'].loc[date] if date in price_level_indicators['high_52w'].index else np.nan,
                'low_52w': price_level_indicators['low_52w'].loc[date] if date in price_level_indicators['low_52w'].index else np.nan,
                'price_vs_high_52w': price_level_indicators['price_vs_high_52w'].loc[date] if date in price_level_indicators['price_vs_high_52w'].index else np.nan,
                'price_vs_low_52w': price_level_indicators['price_vs_low_52w'].loc[date] if date in price_level_indicators['price_vs_low_52w'].index else np.nan,
                'range_35d': volatility_indicators['range_35d'].loc[date] if date in volatility_indicators['range_35d'].index else np.nan,
                'volume_ratio': volatility_indicators['volume_ratio'].loc[date] if date in volatility_indicators['volume_ratio'].index else np.nan,
                
                # 현재가
                'close_price': price_series.loc[date]
            }
            
            indicators_data.append(row)
        
        df = pd.DataFrame(indicators_data)
        df.set_index('Date', inplace=True)
        
        # Forward fill for financial statement data
        financial_columns = ['market_cap', 'pb_ratio', 'pe_ratio', 'dividend_yield', 'current_ratio', 
                           'debt_ratio', 'icr', 'ncav_ratio', 'eps_cagr_5y', 'psr', 'fcf_yield', 
                           'sales_cagr_5y', 'fcf_cagr_3y', 'roe', 'debt_to_equity', 
                           'annual_sales_growth', 'annual_eps_growth', 'net_margin']
        
        df[financial_columns] = df[financial_columns].fillna(method='ffill')
        
        return df
    
    def process_all_tickers(self):
        