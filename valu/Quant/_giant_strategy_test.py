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
        """모든 티커의 지표 계산 및 저장"""
        tickers = self.get_ticker_list()
        print(f"Processing {len(tickers)} tickers...")
        
        for i, ticker in enumerate(tickers, 1):
            try:
                print(f"[{i}/{len(tickers)}] Processing {ticker}...")
                indicators_df = self.calculate_all_indicators(ticker)
                
                if not indicators_df.empty:
                    # feather 형식으로 저장
                    output_file = self.output_path / f"{ticker}_indicators.feather"
                    indicators_df.reset_index().to_feather(output_file)
                    print(f"  → Saved {ticker}_indicators.feather")
                else:
                    print(f"  → No data for {ticker}")
                    
            except Exception as e:
                print(f"  → Error processing {ticker}: {e}")
                
        print("All tickers processed!")
    
    def load_ticker_indicators(self, ticker: str) -> pd.DataFrame:
        """특정 티커의 저장된 지표 로드"""
        file_path = self.output_path / f"{ticker}_indicators.feather"
        if file_path.exists():
            df = pd.read_feather(file_path)
            df.set_index('Date', inplace=True)
            return df
        return pd.DataFrame()

class ModularScreeningStrategy:
    """
    모듈식 스크리닝 전략 클래스
    각 전략은 독립적으로 실행 가능하며 결합도 가능
    """
    
    def __init__(self, indicator_processor: IndicatorProcessor):
        self.processor = indicator_processor
        
    # ===== Benjamin Graham 전략 =====
    def graham_defensive_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        그레이엄의 방어적 투자자 스크린
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. 시가총액 >= 2억 달러
            if pd.isna(row['market_cap']) or row['market_cap'] < 200_000_000:
                return False
                
            # 2. P/B < 1.5
            if pd.isna(row['pb_ratio']) or row['pb_ratio'] >= 1.5:
                return False
                
            # 3. P/E < 15
            if pd.isna(row['pe_ratio']) or row['pe_ratio'] >= 15:
                return False
                
            # 4. 배당수익률 > 2%
            if pd.isna(row['dividend_yield']) or row['dividend_yield'] < 2:
                return False
                
            # 5. 유동비율 > 2
            if pd.isna(row['current_ratio']) or row['current_ratio'] < 2:
                return False
                
            # 6. 부채비율 < 50%
            if pd.isna(row['debt_ratio']) or row['debt_ratio'] >= 50:
                return False
                
            # 7. 이자보상배율 > 2.5
            if pd.isna(row['icr']) or row['icr'] < 2.5:
                return False
                
            # 8. EPS 5년 CAGR > 3%
            if pd.isna(row['eps_cagr_5y']) or row['eps_cagr_5y'] < 3:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    def graham_enterprising_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        그레이엄의 적극적 투자자 스크린 (NCAV 포함)
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. NCAV/시가총액 > 0.66 (Net Current Asset Value)
            if pd.isna(row['ncav_ratio']) or row['ncav_ratio'] < 0.66:
                return False
                
            # 2. P/E < 10
            if pd.isna(row['pe_ratio']) or row['pe_ratio'] >= 10:
                return False
                
            # 3. P/B < 1.2
            if pd.isna(row['pb_ratio']) or row['pb_ratio'] >= 1.2:
                return False
                
            # 4. 부채비율 < 30%
            if pd.isna(row['debt_ratio']) or row['debt_ratio'] >= 30:
                return False
                
            # 5. 유동비율 > 1.5
            if pd.isna(row['current_ratio']) or row['current_ratio'] < 1.5:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    # ===== Ken Fisher 전략 =====
    def fisher_psr_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        Ken Fisher의 Price-to-Sales 전략
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. PSR < 0.75
            if pd.isna(row['psr']) or row['psr'] >= 0.75:
                return False
                
            # 2. 시가총액 >= 1억 달러
            if pd.isna(row['market_cap']) or row['market_cap'] < 100_000_000:
                return False
                
            # 3. FCF 수익률 > 5%
            if pd.isna(row['fcf_yield']) or row['fcf_yield'] < 5:
                return False
                
            # 4. 5년 매출 CAGR > 5%
            if pd.isna(row['sales_cagr_5y']) or row['sales_cagr_5y'] < 5:
                return False
                
            # 5. 3년 FCF CAGR > 10%
            if pd.isna(row['fcf_cagr_3y']) or row['fcf_cagr_3y'] < 10:
                return False
                
            # 6. ROE > 15%
            if pd.isna(row['roe']) or row['roe'] < 15:
                return False
                
            # 7. Debt-to-Equity < 50%
            if pd.isna(row['debt_to_equity']) or row['debt_to_equity'] >= 50:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    # ===== Mark Minervini 전략 =====
    def minervini_template_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        Mark Minervini의 Trend Template
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. 현재가 > 150일 이동평균 > 200일 이동평균
            if (pd.isna(row['ma_150']) or pd.isna(row['ma_200']) or 
                row['close_price'] <= row['ma_150'] or row['ma_150'] <= row['ma_200']):
                return False
                
            # 2. 150일 이동평균이 상승 추세 (단순화: 현재 MA150 > 10일 전 MA150)
            # 실제로는 과거 데이터와 비교해야 함
            
            # 3. 200일 이동평균이 상승 추세
            # 실제로는 과거 데이터와 비교해야 함
            
            # 4. 50일 이동평균 > 150일, 200일 이동평균
            if (pd.isna(row['ma_50']) or 
                row['ma_50'] <= row['ma_150'] or row['ma_50'] <= row['ma_200']):
                return False
                
            # 5. 현재가가 52주 최고가의 75% 이상
            if pd.isna(row['price_vs_high_52w']) or row['price_vs_high_52w'] < 0.75:
                return False
                
            # 6. 현재가가 52주 최저가의 25% 이상 상승
            if pd.isna(row['price_vs_low_52w']) or row['price_vs_low_52w'] < 1.25:
                return False
                
            # 7. 연간 매출 성장률 > 25%
            if pd.isna(row['annual_sales_growth']) or row['annual_sales_growth'] < 25:
                return False
                
            # 8. 연간 EPS 성장률 > 25%
            if pd.isna(row['annual_eps_growth']) or row['annual_eps_growth'] < 25:
                return False
                
            # 9. 순이익률 > 8%
            if pd.isna(row['net_margin']) or row['net_margin'] < 8:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    def minervini_volatility_contraction(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        Minervini의 변동성 축소 패턴
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. 35일간 최고가-최저가 변동폭이 25% 이하
            if pd.isna(row['range_35d']) or row['range_35d'] > 25:
                return False
                
            # 2. 기본 템플릿 조건들도 만족
            if not self.minervini_template_screen(indicators_df, date):
                return False
                
            # 3. 거래량이 평균보다 50% 이상 증가 (브레이크아웃 확인)
            if pd.isna(row['volume_ratio']) or row['volume_ratio'] < 1.5:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    # ===== 모멘텀 전략 =====
    def momentum_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        모멘텀 기반 스크린 (Jegadeesh & Titman 스타일)
        """
        try:
            row = indicators_df.loc[date]
            
            # 1. 12개월 리턴 (최근 1개월 제외) > 20%
            if pd.isna(row['ret_12m_exc1']) or row['ret_12m_exc1'] < 0.20:
                return False
                
            # 2. 6개월 리턴 > 10%
            if pd.isna(row['ret_6m']) or row['ret_6m'] < 0.10:
                return False
                
            # 3. 현재가 > 200일 이동평균
            if pd.isna(row['ma_200']) or row['close_price'] <= row['ma_200']:
                return False
                
            # 4. 시가총액 >= 5천만 달러 (유동성 확보)
            if pd.isna(row['market_cap']) or row['market_cap'] < 50_000_000:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    # ===== 복합 전략 =====
    def combined_value_growth_screen(self, indicators_df: pd.DataFrame, date: str) -> bool:
        """
        가치+성장 복합 전략
        """
        try:
            row = indicators_df.loc[date]
            
            # 가치 조건들
            # 1. P/E < 20
            if pd.isna(row['pe_ratio']) or row['pe_ratio'] >= 20:
                return False
                
            # 2. P/B < 3
            if pd.isna(row['pb_ratio']) or row['pb_ratio'] >= 3:
                return False
                
            # 3. PSR < 2
            if pd.isna(row['psr']) or row['psr'] >= 2:
                return False
                
            # 성장 조건들
            # 4. EPS 5년 CAGR > 10%
            if pd.isna(row['eps_cagr_5y']) or row['eps_cagr_5y'] < 10:
                return False
                
            # 5. 매출 5년 CAGR > 8%
            if pd.isna(row['sales_cagr_5y']) or row['sales_cagr_5y'] < 8:
                return False
                
            # 6. ROE > 12%
            if pd.isna(row['roe']) or row['roe'] < 12:
                return False
                
            # 품질 조건들
            # 7. 부채비율 < 60%
            if pd.isna(row['debt_ratio']) or row['debt_ratio'] >= 60:
                return False
                
            # 8. 유동비율 > 1.2
            if pd.isna(row['current_ratio']) or row['current_ratio'] < 1.2:
                return False
                
            return True
            
        except (KeyError, IndexError):
            return False
    
    def screen_ticker(self, ticker: str, date: str, strategies: List[str] = None) -> Dict[str, bool]:
        """
        특정 티커에 대해 선택된 전략들 실행
        """
        if strategies is None:
            strategies = ['graham_defensive', 'graham_enterprising', 'fisher_psr', 
                         'minervini_template', 'minervini_volatility', 'momentum', 
                         'combined_value_growth']
        
        indicators_df = self.processor.load_ticker_indicators(ticker)
        if indicators_df.empty or date not in indicators_df.index:
            return {strategy: False for strategy in strategies}
        
        results = {}
        strategy_map = {
            'graham_defensive': self.graham_defensive_screen,
            'graham_enterprising': self.graham_enterprising_screen,
            'fisher_psr': self.fisher_psr_screen,
            'minervini_template': self.minervini_template_screen,
            'minervini_volatility': self.minervini_volatility_contraction,
            'momentum': self.momentum_screen,
            'combined_value_growth': self.combined_value_growth_screen
        }
        
        for strategy in strategies:
            if strategy in strategy_map:
                results[strategy] = strategy_map[strategy](indicators_df, date)
            else:
                results[strategy] = False
                
        return results

class PortfolioBacktester:
    """
    포트폴리오 백테스트 클래스 (bt 라이브러리 활용)
    """
    
    def __init__(self, screening_strategy: ModularScreeningStrategy):
        self.screening_strategy = screening_strategy
        
    def create_portfolio_weights(self, tickers: List[str], date: str, strategy: str, 
                                max_positions: int = 20) -> Dict[str, float]:
        """
        특정 날짜에 특정 전략으로 포트폴리오 가중치 생성
        """
        selected_stocks = []
        
        for ticker in tickers:
            results = self.screening_strategy.screen_ticker(ticker, date, [strategy])
            if results.get(strategy, False):
                # 추가 정보 수집 (랭킹용)
                indicators_df = self.screening_strategy.processor.load_ticker_indicators(ticker)
                if not indicators_df.empty and date in indicators_df.index:
                    row = indicators_df.loc[date]
                    selected_stocks.append({
                        'ticker': ticker,
                        'pe_ratio': row.get('pe_ratio', np.nan),
                        'pb_ratio': row.get('pb_ratio', np.nan),
                        'market_cap': row.get('market_cap', np.nan)
                    })
        
        if not selected_stocks:
            return {}
        
        # 전략별 랭킹 방식
        if strategy in ['graham_defensive', 'graham_enterprising']:
            # P/E 낮은 순으로 정렬
            selected_stocks.sort(key=lambda x: x['pe_ratio'] if not pd.isna(x['pe_ratio']) else float('inf'))
        elif strategy == 'fisher_psr':
            # 시가총액 큰 순으로 정렬
            selected_stocks.sort(key=lambda x: x['market_cap'] if not pd.isna(x['market_cap']) else 0, reverse=True)
        else:
            # 기본적으로 시가총액 큰 순
            selected_stocks.sort(key=lambda x: x['market_cap'] if not pd.isna(x['market_cap']) else 0, reverse=True)
        
        # 상위 max_positions개 선택
        selected_stocks = selected_stocks[:max_positions]
        
        # 동일 가중치
        weight = 1.0 / len(selected_stocks)
        return {stock['ticker']: weight for stock in selected_stocks}
    
    def backtest_strategy(self, strategy: str, start_date: str, end_date: str, 
                         rebalance_freq: str = 'M', max_positions: int = 20) -> bt.Backtest:
        """
        특정 전략의 백테스트 실행
        """
        # 티커 리스트 가져오기
        tickers = self.screening_strategy.processor.get_ticker_list()
        
        # 가격 데이터 로드 (간소화된 버전)
        price_data = {}
        for ticker in tickers[:50]:  # 처음 50개만 테스트
            try:
                hist_data = FinancialIndicators.load_data(ticker)
                if hist_data and 'history' in hist_data:
                    price_data[ticker] = hist_data['history']['Close']
            except:
                continue
        
        if not price_data:
            raise ValueError("No price data available")
        
        # 가격 데이터프레임 생성
        prices_df = pd.DataFrame(price_data)
        prices_df = prices_df.loc[start_date:end_date]
        
        # 리밸런싱 알고리즘 정의
        class StrategyAlgo(bt.Algo):
            def __init__(self, strategy_name, screening_strategy, max_positions):
                self.strategy_name = strategy_name
                self.screening_strategy = screening_strategy
                self.max_positions = max_positions
                
            def __call__(self, target):
                date = target.now.strftime('%Y-%m-%d')
                available_tickers = list(target.universe.columns)
                
                weights = self.screening_strategy.create_portfolio_weights(
                    available_tickers, date, self.strategy_name, self.max_positions
                )
                
                target.temp['weights'] = weights
                return True
        
        # 백테스트 전략 구성
        strategy_algo = StrategyAlgo(strategy, self.screening_strategy, max_positions)
        
        bt_strategy = bt.Strategy(
            strategy,
            [
                bt.algos.RunMonthly() if rebalance_freq == 'M' else bt.algos.RunQuarterly(),
                strategy_algo,
                bt.algos.WeighTarget(),
                bt.algos.Rebalance()
            ]
        )
        
        # 백테스트 실행
        backtest = bt.Backtest(bt_strategy, prices_df)
        return backtest

# 사용 예제
def main():
    """
    메인 실행 함수 - 전체 파이프라인 데모
    """
    print("=== Modular Graham Strategy System ===")
    
    # 1. 지표 프로세서 초기화 및 지표 계산
    processor = IndicatorProcessor(data_path="./data", output_path="./indicators")
    
    # 모든 티커의 지표 계산 (최초 1회만 실행)
    # processor.process_all_tickers()
    
    # 2. 스크리닝 전략 초기화
    screener = ModularScreeningStrategy(processor)
    
    # 3. 특정 티커들에 대한 스크리닝 테스트
    test_date = "2023-12-31"
    test_tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
    
    print(f"\n=== Screening Results for {test_date} ===")
    for ticker in test_tickers:
        results = screener.screen_ticker(ticker, test_date)
        print(f"\n{ticker}:")
        for strategy, passed in results.items():
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {strategy}: {status}")
    
    # 4. 백테스트 실행 (옵션)
    try:
        backtester = PortfolioBacktester(screener)
        
        print(f"\n=== Backtesting Graham Defensive Strategy ===")
        backtest = backtester.backtest_strategy(
            strategy="graham_defensive",
            start_date="2020-01-01",
            end_date="2023-12-31",
            rebalance_freq="M",
            max_positions=15
        )
        
        result = bt.run(backtest)
        print(f"Final Portfolio Value: ${result.stats.loc['End', 'graham_defensive']:,.2f}")
        print(f"Total Return: {result.stats.loc['Total Return', 'graham_defensive']:.2%}")
        print(f"CAGR: {result.stats.loc['CAGR', 'graham_defensive']:.2%}")
        print(f"Max Drawdown: {result.stats.loc['Max Drawdown', 'graham_defensive']:.2%}")
        print(f"Sharpe Ratio: {result.stats.loc['Sharpe', 'graham_defensive']:.2f}")
        
    except Exception as e:
        print(f"Backtesting error: {e}")
    
    print("\n=== Analysis Complete ===")

if __name__ == "__main__":
    main()