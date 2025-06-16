import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import time

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('calculate_rs.log')]
)
logger = logging.getLogger(__name__)

def save_ticker_rs(ticker: str, indicators_dir: Path, rs_ranks: dict, all_data: dict) -> tuple[str, bool]:
    """
    단일 티커에 RS_Rank_excl_1W, RS_Rank_excl_1M, RS_Rank_6M을 저장.

    Args:
        ticker (str): 주식 티커 심볼
        indicators_dir (Path): indicators 폴더 경로
        rs_ranks (dict): 티커별 날짜별 RS 순위 {'excl_1W': {date: rank}, 'excl_1M': {date: rank}, '6M': {date: rank}}
        all_data (dict): 모든 티커의 데이터프레임 {ticker: df}

    Returns:
        tuple: (ticker, 성공여부)
    """
    try:
        df = all_data[ticker].copy()
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # RS_Rank_excl_1W 추가
        rs_series_excl_1w = pd.Series(rs_ranks[ticker]['excl_1W'])
        df['RS_Rank_excl_1W'] = rs_series_excl_1w.reindex(df.index, fill_value=float('nan'))
        
        # RS_Rank_excl_1M 추가
        rs_series_excl_1m = pd.Series(rs_ranks[ticker]['excl_1M'])
        df['RS_Rank_excl_1M'] = rs_series_excl_1m.reindex(df.index, fill_value=float('nan'))
        
        # RS_Rank_6M 추가
        rs_series_6m = pd.Series(rs_ranks[ticker]['6M'])
        df['RS_Rank_6M'] = rs_series_6m.reindex(df.index, fill_value=float('nan'))
        
        # 저장
        output_path = indicators_dir / f"{ticker}_indicator.feather"
        df.reset_index().to_feather(output_path)
        logger.info(f"{ticker}의 RS 계산 완료.")
        return ticker, True
    except Exception as e:
        logger.error(f"오류: {ticker}의 RS 순위 저장 중 오류 - {e}")
        return ticker, False

def calculate_rs():
    """
    모든 티커의 데이터를 읽어 날짜별 상대강도(RS) 순위를 계산하고 indicator 파일에 저장.
    12M_Return_excl_1W, 12M_Return_excl_1M, 6M_Return에 대한 순위(RS_Rank_excl_1W, RS_Rank_excl_1M, RS_Rank_6M)를 계산.
    """
    # 실행 시간 측정 시작
    total_start_time = time.time()

    # 경로 설정
    base_dir = Path(__file__).parent.parent / "src_data"
    indicators_dir = base_dir / "indicators"
    
    # 데이터 로드 시간 측정
    load_start_time = time.time()
    
    # 모든 티커 데이터 로드
    all_data = {}
    tickers = [f.stem.replace('_indicator', '') for f in indicators_dir.glob('*_indicator.feather')]
    if not tickers:
        logger.error("src_data/indicators 폴더에 indicator 파일이 없습니다.")
        return
    
    for ticker in tickers:
        file_path = indicators_dir / f"{ticker}_indicator.feather"
        try:
            # 필요한 컬럼만 로드
            df = pd.read_feather(file_path, columns=['Date', '12M_Return_excl_1W', '12M_Return_excl_1M', '6M_Return'])
            df['Date'] = pd.to_datetime(df['Date'])
            # 데이터 타입 최적화
            for col in ['12M_Return_excl_1W', '12M_Return_excl_1M', '6M_Return']:
                if col in df.columns:
                    df[col] = df[col].astype('float32')
            all_data[ticker] = df
        except Exception as e:
            logger.error(f"오류: {file_path} 읽기 중 오류 - {e}")
    
    logger.info(f"{len(all_data)}개의 티커를 처리합니다.")
    logger.info(f"데이터 로드 시간: {time.time() - load_start_time:.2f}초")
    
    # RS 계산 시간 측정
    rs_start_time = time.time()
    
    # MultiIndex DataFrame 생성
    dfs = []
    for ticker, df in all_data.items():
        df = df.copy()
        df['Ticker'] = ticker
        dfs.append(df)
    
    if not dfs:
        logger.error("처리할 데이터가 없습니다.")
        return
    
    combined_df = pd.concat(dfs)
    combined_df.set_index(['Date', 'Ticker'], inplace=True)
    
    # 날짜별 RS 순위 계산
    rs_ranks_by_ticker = {ticker: {'excl_1W': {}, 'excl_1M': {}, '6M': {}} for ticker in tickers}
    for date, group in combined_df.groupby(level='Date'):
        try:
            # RS_Rank_excl_1W
            if '12M_Return_excl_1W' in group.columns:
                ranks_excl_1w = group['12M_Return_excl_1W'].rank(pct=True, ascending=False)
                for ticker in ranks_excl_1w.index.get_level_values('Ticker'):
                    if pd.notna(ranks_excl_1w[(date, ticker)]):
                        rs_ranks_by_ticker[ticker]['excl_1W'][date] = ranks_excl_1w[(date, ticker)]
            
            # RS_Rank_excl_1M
            if '12M_Return_excl_1M' in group.columns:
                ranks_excl_1m = group['12M_Return_excl_1M'].rank(pct=True, ascending=False)
                for ticker in ranks_excl_1m.index.get_level_values('Ticker'):
                    if pd.notna(ranks_excl_1m[(date, ticker)]):
                        rs_ranks_by_ticker[ticker]['excl_1M'][date] = ranks_excl_1m[(date, ticker)]
            
            # RS_Rank_6M
            if '6M_Return' in group.columns:
                ranks_6m = group['6M_Return'].rank(pct=True, ascending=False)
                for ticker in ranks_6m.index.get_level_values('Ticker'):
                    if pd.notna(ranks_6m[(date, ticker)]):
                        rs_ranks_by_ticker[ticker]['6M'][date] = ranks_6m[(date, ticker)]
        except Exception as e:
            logger.warning(f"날짜 {date}의 RS 순위 계산 중 오류 - {e}")
    
    logger.info(f"RS 계산 시간: {time.time() - rs_start_time:.2f}초")
    
    # 저장 시간 측정
    save_start_time = time.time()
    
    # 병렬 처리로 저장
    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(save_ticker_rs, ticker, indicators_dir, rs_ranks_by_ticker, all_data)
            for ticker in tickers
        ]
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "O" if success else "X"
            logger.info(f"{ticker}: {status}")
            success_count += int(success)
    
    logger.info(f"{success_count}/{len(tickers)}개의 티커를 성공적으로 처리했습니다.")
    logger.info(f"저장 시간: {time.time() - save_start_time:.2f}초")
    logger.info(f"총 실행 시간: {time.time() - total_start_time:.2f}초")

if __name__ == "__main__":
    calculate_rs()