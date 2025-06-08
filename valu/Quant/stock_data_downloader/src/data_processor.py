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
            existing_data: Optional[pd.DataFrame] = None
            
            # 1) 기존 파일에서 읽어오기
            if existing_file.exists():
                existing_data = pd.read_feather(existing_file)
                existing_data['Date'] = pd.to_datetime(existing_data['Date'], errors='coerce')
                existing_data = existing_data.set_index('Date')
                
                # 기존 인덱스도 로컬 날짜 기준으로 정규화 후 tz 제거
                existing_data.index = existing_data.index.normalize()
                existing_data.index = existing_data.index.tz_localize(None)

            # 2) 새 데이터가 없다면 기존 데이터 반환
            if new_data.empty:
                return existing_data

            # 3) 기존 + 새 데이터 병합
            df = pd.concat([existing_data, new_data]) if existing_data is not None else new_data
            df = df[~df.index.duplicated(keep='last')]

            # 4) 병합 후 인덱스도 로컬 날짜 기준으로 정규화 후 tz 제거
            df.index = pd.to_datetime(df.index, errors='coerce')
            df.index = df.index.normalize()
            df.index = df.index.tz_localize(None)
            
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
            existing_data: Optional[pd.DataFrame] = None
            
            # 1) 기존 파일에서 읽어오기
            if existing_file.exists():
                existing_data = pd.read_feather(existing_file)
                existing_data['Date'] = pd.to_datetime(existing_data['Date'], errors='coerce')
                existing_data = existing_data.set_index('Date')
                
                # 기존 인덱스도 로컬 날짜 기준으로 정규화 후 tz 제거
                existing_data.index = existing_data.index.normalize()
                existing_data.index = existing_data.index.tz_localize(None)

            # 2) 새 데이터가 없다면 기존 데이터 반환
            if new_data.empty:
                return existing_data

            # 3) 새로운 재무 데이터 전처리
            new_data_processed = DataProcessor.process_financial_data(new_data)
            
            # 4) 기존 + 새 데이터 병합
            df = (
                pd.concat([existing_data, new_data_processed])
                if existing_data is not None
                else new_data_processed
            )
            df = df[~df.index.duplicated(keep='last')]

            # 5) 병합 후 인덱스도 로컬 날짜 기준으로 정규화 후 tz 제거
            df.index = pd.to_datetime(df.index, errors='coerce')
            df.index = df.index.normalize()
            df.index = df.index.tz_localize(None)
            
            return df

        except Exception as e:
            print(f"{ticker} {data_type} 데이터 병합 중 오류: {e}")
            return None
