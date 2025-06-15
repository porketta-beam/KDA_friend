import json
from pathlib import Path
import asyncio
import pandas import pd
import logging
from datetime import datetime, timedelta
from src.data_downloader import DataDownloader
from src.data_processor import DataProcessor
from src.data_saver import DataSaver

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),  # 콘솔 출력
        logging.FileHandler('src_data/errors.log', encoding='utf-8')  # 파일 출력
    ]
)
logger = logging.getLogger(__name__)

def load_tickers_from_csv(csv_path: str | Path) -> list:
    """CSV 파일에서 티커 리스트를 로드."""
    try:
        csv_path = Path(csv_path).resolve()
        df = pd.read_csv(csv_path)
        if "Ticker" not in df.columns:
            raise ValueError("CSV 파일에 'Ticker' 열이 없습니다.")
        return df["Ticker"].dropna().tolist()
    except Exception as e:
        logger.error(f"티커 CSV 파일 로드 중 오류: {e}")
        return []

async def process_ticker(ticker: str, downloader: DataDownloader, saver: DataSaver, start_date: str, end_date: str):
    """단일 티커 처리."""
    print(f"▶ 처리 중: {ticker}")
    try:
        # 주식 가격 데이터
        stock_file = saver.base_path / "stock_data" / f"{ticker}_data.feather"
        last_date = None
        if stock_file.exists():
            df_existing = pd.read_feather(stock_file)
            df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
            last_date = df_existing["Date"].max()
        
        effective_start_date = start_date
        if last_date and pd.notna(last_date):
            effective_start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

        stock_data = await downloader.download_stock_prices(ticker, effective_start_date, end_date)
        processed_stock = DataProcessor.process_stock_data(stock_data, stock_file, ticker)
        saver.save_stock_data(processed_stock, ticker)
        
        # 재무 데이터
        financial_data = await downloader.download_financial_data(ticker)
        saver.save_financial_data(financial_data, ticker)

    except Exception as e:
        logger.error(f"{ticker} 처리 중 오류: {e}")

async def main():
    logger.info("코드 실행 시작")
    
    # main.py의 디렉토리를 기준으로 설정 파일 경로 계산
    config_path = Path(__file__).parent / "config" / "settings.json"
    
    # 설정 파일 로드
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"설정 파일을 찾을 수 없습니다: {config_path}")
        return
    except Exception as e:
        logger.error(f"설정 파일 로드 중 오류: {e}")
        return

    # base_path를 절대 경로로 변환
    base_path = (Path(__file__).parent / config["base_path"]).resolve()
    
    # 티커 CSV와 로그 파일 경로 계산
    ticker_csv_path = base_path / "ticker_list" / "tickers.csv"
    log_file_path = base_path / "errors.log"
    
    # 티커 CSV 디렉토리 생성
    ticker_csv_path.parent.mkdir(parents=True, exist_ok=True)

    # 티커 리스트 로드
    tickers = load_tickers_from_csv(ticker_csv_path)
    if not tickers:
        logger.error("티커 리스트가 비어 있습니다. 프로그램을 종료합니다.")
        return

    # 초기화
    downloader = DataDownloader(log_file=log_file_path, max_concurrent=3)
    saver = DataSaver(base_path=base_path)
    
    end_date = datetime.now().strftime("%Y-%m-%d")

    # 병렬 처리
    tasks = [
        process_ticker(ticker, downloader, saver, config["start_date"], end_date)
        for ticker in tickers
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("코드 실행 종료")

if __name__ == "__main__":
    asyncio.run(main())