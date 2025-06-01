from typing import Optional
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from .constants import FINANCIAL_DATA_TYPES

class DataProcessor:
    """다운로드된 데이터를 처리하는 클래스."""
    
    @staticmethod
    def process_stock_data(
        new_data: pd.DataFrame,
        existing_file: Path,
        ticker: str
    ) -> Optional[pd.DataFrame]:
        """주식 가격 데이터 처리."""
        try:
            existing_data = None
            if existing_file.exists():
                existing_data = pd.read_feather(existing_file)
                existing_data['Date'] = pd.to_datetime(existing_data['Date'], errors='coerce')
                existing_data = existing_data.set_index('Date')
                if existing_data.index.tz is not None:
                    existing_data.index = existing_data.index.tz_convert(None)

            if new_data.empty:
                return existing_data
            
            df = pd.concat([existing_data, new_data]) if existing_data is not None else new_data
            df = df[~df.index.duplicated(keep='last')]
            df.index = pd.to_datetime(df.index, errors='coerce').normalize()
            
            return df

        except Exception as e:
            print(f"{ticker} 데이터 처리 중 오류: {e}")
            return None

    @staticmethod
    def process_financial_data(df: pd.DataFrame) -> pd.DataFrame:
        """재무 데이터 처리."""
        df = df.T
        df.index.name = 'Date'
        df = df.apply(pd.to_numeric, errors='coerce')
        df = df.interpolate(method='linear', axis=0, limit_direction='both')
        df = df.sort_index(ascending=True)
        return df
    
    @staticmethod
    def merge_financial_data(
        new_data: pd.DataFrame,
        existing_file: Path,
        ticker: str,
        data_type: str
    ) -> Optional[pd.DataFrame]:
        """기존 재무 데이터와 새로운 재무 데이터를 병합."""
        try:
            existing_data = None
            if existing_file.exists():
                existing_data = pd.read_feather(existing_file)
                existing_data['Date'] = pd.to_datetime(existing_data['Date'], errors='coerce')
                existing_data = existing_data.set_index('Date')
                if existing_data.index.tz is not None:
                    existing_data.index = existing_data.index.tz_convert(None)

            if new_data.empty:
                return existing_data

            # 새로운 데이터 처리
            new_data_processed = DataProcessor.process_financial_data(new_data)
            
            # 기존 데이터와 병합
            df = pd.concat([existing_data, new_data_processed]) if existing_data is not None else new_data_processed
            df = df[~df.index.duplicated(keep='last')]  # 중복 날짜는 최신 데이터로 유지
            df.index = pd.to_datetime(df.index, errors='coerce').normalize()
            
            return df

        except Exception as e:
            print(f"{ticker} {data_type} 데이터 병합 중 오류: {e}")
            return None