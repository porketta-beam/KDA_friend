from typing import Dict, Optional
import pandas as pd
from pathlib import Path
from .constants import FINANCIAL_DATA_TYPES
from .data_processor import DataProcessor

class DataSaver:
    """처리된 데이터를 저장하는 클래스."""
    
    def __init__(self, base_path: str | Path):
        """저장 경로 초기화."""
        self.base_path = Path(base_path).resolve()  # 절대 경로로 변환
        self._setup_directories()

    def _setup_directories(self) -> None:
        """필요한 디렉토리 생성."""
        for subdir in FINANCIAL_DATA_TYPES.values():
            (self.base_path / subdir).mkdir(parents=True, exist_ok=True)
        (self.base_path / "stock_data").mkdir(parents=True, exist_ok=True)

    def save_stock_data(self, df: Optional[pd.DataFrame], ticker: str) -> bool:
        """주식 가격 데이터 저장."""
        if df is None or df.empty:
            return False
        
        stock_file = self.base_path / "stock_data" / f"{ticker}_data.feather"
        df.reset_index().to_feather(stock_file)
        print(f"{ticker} 주식 데이터가 저장되었습니다.")
        return True

    def save_financial_data(self, financial_data: Dict[str, pd.DataFrame], ticker: str) -> None:
        """재무 데이터 저장 (기존 데이터와 병합)."""
        for data_type, df in financial_data.items():
            if df.empty:
                continue
            file_path = self.base_path / data_type / f"{ticker}_{data_type}.feather"
            
            # 기존 데이터와 병합
            df_processed = DataProcessor.merge_financial_data(df, file_path, ticker, data_type)
            
            if df_processed is not None and not df_processed.empty:
                df_processed.reset_index().to_feather(file_path)
                print(f"{ticker} {data_type} 데이터가 저장되었습니다.")