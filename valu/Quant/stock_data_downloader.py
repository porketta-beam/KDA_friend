from datetime import datetime

def log_error(error_message, log_file_path):
    """Append error message to the log file."""
    with open(log_file_path, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now()} - {error_message}\n")



import os
from typing import Optional, Union, List
import yfinance as yf
import pandas as pd
from datetime import timedelta, datetime
import time
from curl_cffi import requests
# from logger import log_error

session = requests.Session(impersonate="chrome")

def download_stock_data(
    ticker: Union[str, List[str]],
    start_date: str,
    end_date: str,
    stock_save_dir: str,
    log_file: Optional[str] = None
):
    """주식 가격 데이터를 다운로드하고 업데이트합니다.
    ticker에 리스트를 넘기면 내부적으로 각각 순차 처리합니다.
    """
    # 1) ticker가 리스트라면, 재귀 호출로 각각 처리
    if isinstance(ticker, list):
        for t in ticker:
            download_stock_data(t, start_date, end_date, stock_save_dir, log_file)
        return  # 리스트 모드에서는 여기서 끝

    # 2) 이후 로직은 기존과 동일하게 단일 ticker 처리
    try:
        stock_file = os.path.join(stock_save_dir, f'{ticker}_data.feather')

        existing = None
        if os.path.exists(stock_file):
            existing = pd.read_feather(stock_file)
            existing['Date'] = pd.to_datetime(existing['Date'], utc=False, errors='coerce')
            existing = existing.set_index('Date')
            if existing.index.tz is not None:
                existing.index = existing.index.tz_convert(None)
            last_date = existing.index.max()
            if pd.notna(last_date):
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                print(f'{ticker} 기존 데이터의 날짜 형식이 잘못되었습니다.')
                existing = None

        new_data = yf.Ticker(ticker, session=session).history(start=start_date, end=end_date)
        time.sleep(0.5)

        if new_data.empty:
            print(f'{ticker}에 대한 새 데이터가 없습니다.')
            return

        # 타임존 제거 & 결합
        if new_data.index.tz is not None:
            new_data.index = new_data.index.tz_convert(None)

        df = pd.concat([existing, new_data]) if existing is not None else new_data
        df = df[~df.index.duplicated(keep='last')]

        # 인덱스 정리
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=False, errors='coerce')
        if df.index.tz is not None:
            df.index = df.index.tz_convert(None)
        df.index = df.index.normalize()

        df.reset_index().to_feather(stock_file)
        print(f'{ticker} 데이터가 업데이트되었습니다.')

    except Exception as e:
        error_message = f"{ticker} 다운로드 중 오류: {e}"
        print(error_message)
        if log_file:
            log_error(error_message, log_file)

if __name__ == "__main__":
    tickers = ["005930.KS", "000660.KS", "035420.KS"]  # 여러 티커 지정
    start = "2020-01-01"
    end   = "2025-05-25"
    save_dir = "src_data/stock_data"

    download_stock_data(tickers, start, end, save_dir)







from pathlib import Path
import pandas as pd
import yfinance as yf
import datetime

def setup_data_dirs(base_path: Path):
    """기본 경로 아래에 필요한 서브폴더를 모두 생성"""
    subdirs = [
        'financials',
        'quarterly_financials',
        'balance_sheet',
        'quarterly_balance_sheet',
        'cash_flow',
        'quarterly_cash_flow'
    ]
    for sub in subdirs:
        (base_path / sub).mkdir(parents=True, exist_ok=True)

def fetch_and_save(ticker: str, base_path: Path, start: str, end: str):
    """한 티커에 대해 yfinance로 재무 데이터를 받아와서 각 폴더에 feather로 저장"""
    t = yf.Ticker(ticker)

    def save_fin(df, folder, name):
        df = df.T
        df.index.name = 'Date'
        # 1) object → numeric 강제 변환
        df = df.apply(pd.to_numeric, errors='coerce')
        # 2) 선형 보간
        df = df.interpolate(method='linear', axis=0, limit_direction='both')
        # 3) Date 오름차순 정렬 (최신이 아래)
        df = df.sort_index(ascending=True)
        # 4) 저장
        df.reset_index().to_feather(base_path / folder / name)

    # 연간 손익계산서
    save_fin(t.financials,              'financials',               f"{ticker}_financials.feather")
    # 분기 손익계산서
    save_fin(t.quarterly_financials,    'quarterly_financials',     f"{ticker}_quarterly_financials.feather")
    # 연간 대차대조표
    save_fin(t.balance_sheet,           'balance_sheet',            f"{ticker}_balance_sheet.feather")
    # 분기 대차대조표
    save_fin(t.quarterly_balance_sheet, 'quarterly_balance_sheet',  f"{ticker}_quarterly_balance_sheet.feather")
    # 연간 현금흐름표
    save_fin(t.cashflow,                'cash_flow',                f"{ticker}_cash_flow.feather")
    # 분기 현금흐름표
    save_fin(t.quarterly_cashflow,      'quarterly_cash_flow',      f"{ticker}_quarterly_cash_flow.feather")

def main():
    base = Path("src_data")
    setup_data_dirs(base)

    tickers = ["005930.KS", "000660.KS", "035420.KS"]
    start = "2010-01-01"
    end   = datetime.date.today().strftime("%Y-%m-%d")

    for tk in tickers:
        print(f"▶ 처리 중: {tk}")
        try:
            fetch_and_save(tk, base, start, end)
        except Exception as e:
            print(f"  ⚠️ 에러 ({tk}): {e}")

if __name__ == "__main__":
    main()
