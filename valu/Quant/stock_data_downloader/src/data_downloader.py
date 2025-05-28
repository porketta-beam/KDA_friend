from typing import Union, List, Optional, Dict
import pandas as pd
import yfinance as yf
from curl_cffi import requests
import asyncio
import logging
from pathlib import Path
from .constants import REQUEST_TIMEOUT, FINANCIAL_DATA_TYPES

class DataDownloader:
    """Yahoo Finance에서 주식 및 재무 데이터를 다운로드하는 클래스."""
    
    def __init__(self, log_file: Optional[Union[str, Path]] = None, max_concurrent: int = 3):
        """다운로더 초기화."""
        self.session = requests.Session(impersonate="chrome")
        self.logger = self._setup_logger(log_file)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.max_retries = 3
        self.retry_delay = 5

    def _setup_logger(self, log_file: Optional[Union[str, Path]]) -> logging.Logger:
        """로깅 설정."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.ERROR)
        if log_file:
            log_file = Path(log_file).resolve()  # 절대 경로로 변환
            log_file.parent.mkdir(parents=True, exist_ok=True)  # 로그 디렉토리 생성
            handler = logging.FileHandler(log_file, encoding='utf-8')
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)
        return logger

    def _log_error(self, message: str) -> None:
        """에러 메시지를 로그와 콘솔에 기록."""
        self.logger.error(message)
        print(f"에러: {message}")

    async def _fetch_with_retry(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """재시도 로직을 포함한 주식 데이터 가져오기."""
        for attempt in range(self.max_retries):
            try:
                async with self.semaphore:
                    data = yf.Ticker(ticker, session=self.session).history(
                        start=start_date,
                        end=end_date
                    )
                    await asyncio.sleep(REQUEST_TIMEOUT)
                    if data.index.tz is not None:
                        data.index = data.index.tz_convert(None)
                    return data
            except Exception as e:
                if attempt < self.max_retries - 1:
                    self._log_error(f"{ticker} 다운로드 시도 {attempt + 1} 실패, 재시도 중: {e}")
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    self._log_error(f"{ticker} 주식 데이터 다운로드 실패: {e}")
                    return pd.DataFrame()

    async def download_stock_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """주식 가격 데이터 다운로드."""
        return await self._fetch_with_retry(ticker, start_date, end_date)

    async def download_financial_data(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """재무 데이터 다운로드."""
        try:
            async with self.semaphore:
                t = yf.Ticker(ticker)
                data = {
                    name: getattr(t, key)
                    for key, name in FINANCIAL_DATA_TYPES.items()
                }
                await asyncio.sleep(REQUEST_TIMEOUT)
                return data
        except Exception as e:
            self._log_error(f"{ticker} 재무 데이터 다운로드 중 오류: {e}")
            return {}