import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('calculate_rs.log')]
)
logger = logging.getLogger(__name__)

def save_ticker_rs(ticker: str, indicators_dir: Path, rs_ranks: dict, all_data: dict[str, pd.DataFrame]) -> tuple[str, bool]:
    """
    단일 티커에 RS_Rank를 저장.

    Args:
        ticker (str): 주식 티커 심볼
        indicators_dir (Path): indicators 폴더 경로
        rs_ranks (dict): 날짜별 RS_Rank {date: rank}
        all_data (dict): 모든 티커의 데이터프레임 {ticker: df}

    Returns:
        tuple: (ticker, 성공여부)
    """
    try:
        df = all_data[ticker].copy()
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # RS_Rank 추가
        rs_series = pd.Series(rs_ranks.get(ticker, {}))
        df['RS_Rank'] = rs_series.reindex(df.index, fill_value=float('nan'))
        
        # 저장
        output_path = indicators_dir / f"{ticker}_indicator.feather"
        df.reset_index().to_feather(output_path)
        logger.info(f"{ticker}의 RS_Rank가 {output_path}에 저장되었습니다.")
        return ticker, True
    except Exception as e:
        logger.error(f"오류: {ticker}의 RS_Rank 저장 중 오류 - {e}")
        return ticker, False

def calculate_rs():
    """
    모든 티커의 데이터를 읽어 날짜별 상대강도(RS) 순위를 계산하고 indicator 파일에 저장.
    """
    # 경로 설정
    base_dir = Path(__file__).parent.parent / "src_data"
    indicators_dir = base_dir / "indicators"
    
    # 모든 티커 데이터 로드
    all_data = {}
    tickers = [f.stem.replace('_indicator', '') for f in indicators_dir.glob('*_indicator.feather')]
    if not tickers:
        logger.error("src_data/indicators 폴더에 indicator 파일이 없습니다.")
        return
    
    for ticker in tickers:
        file_path = indicators_dir / f"{ticker}_indicator.feather"
        try:
            df = pd.read_feather(file_path)
            df['Date'] = pd.to_datetime(df['Date'])
            all_data[ticker] = df
        except Exception as e:
            logger.error(f"오류: {file_path} 읽기 중 오류 - {e}")
    
    logger.info(f"{len(all_data)}개의 티커를 처리합니다.")
    
    # 날짜별 RS_Rank 계산
    dates = sorted(set().union(*(df['Date'] for df in all_data.values())))
    rs_ranks_by_ticker = {ticker: {} for ticker in tickers}
    
    for date in dates:
        returns = {}
        for ticker, df in all_data.items():
            df_date = df[df['Date'] == date]
            if not df_date.empty and '12M_Return_excl_1W' in df_date.columns:
                value = df_date['12M_Return_excl_1W'].iloc[0]
                if pd.notna(value):
                    returns[ticker] = value
        if returns:
            try:
                ranks = pd.Series(returns).rank(pct=True, ascending=False)
                for ticker, rank in ranks.items():
                    rs_ranks_by_ticker[ticker][date] = rank
            except Exception as e:
                logger.warning(f"날짜 {date}의 RS_Rank 계산 중 오류 - {e}")
    
    # 병렬 처리로 저장
    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(save_ticker_rs, ticker, indicators_dir, rs_ranks_by_ticker, all_data)
            for ticker in tickers
        ]
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "✔" if success else "✖"
            logger.info(f"{ticker}: {status}")
            success_count += int(success)
    
    logger.info(f"{success_count}/{len(tickers)}개의 티커를 성공적으로 처리했습니다.")

if __name__ == "__main__":
    calculate_rs()