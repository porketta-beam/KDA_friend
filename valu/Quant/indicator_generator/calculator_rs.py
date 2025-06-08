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
    
    # 날짜별 RS 순위 계산
    dates = sorted(set().union(*(df['Date'] for df in all_data.values())))
    rs_ranks_by_ticker = {ticker: {'excl_1W': {}, 'excl_1M': {}, '6M': {}} for ticker in tickers}
    
    for date in dates:
        returns_excl_1w = {}
        returns_excl_1m = {}
        returns_6m = {}
        for ticker, df in all_data.items():
            df_date = df[df['Date'] == date]
            if df_date.empty:
                continue
            
            # 12M_Return_excl_1W 처리
            if '12M_Return_excl_1W' in df_date.columns:
                value_excl_1w = df_date['12M_Return_excl_1W'].iloc[0]
                if pd.notna(value_excl_1w):
                    returns_excl_1w[ticker] = value_excl_1w
            
            # 12M_Return_excl_1M 처리
            if '12M_Return_excl_1M' in df_date.columns:
                value_excl_1m = df_date['12M_Return_excl_1M'].iloc[0]
                if pd.notna(value_excl_1m):
                    returns_excl_1m[ticker] = value_excl_1m
            
            # 6M_Return 처리
            if '6M_Return' in df_date.columns:
                value_6m = df_date['6M_Return'].iloc[0]
                if pd.notna(value_6m):
                    returns_6m[ticker] = value_6m
        
        # RS_Rank_excl_1W 계산
        if returns_excl_1w:
            try:
                ranks_excl_1w = pd.Series(returns_excl_1w).rank(pct=True, ascending=False)
                for ticker, rank in ranks_excl_1w.items():
                    rs_ranks_by_ticker[ticker]['excl_1W'][date] = rank
            except Exception as e:
                logger.warning(f"날짜 {date}의 RS_Rank_excl_1W 계산 중 오류 - {e}")
        
        # RS_Rank_excl_1M 계산
        if returns_excl_1m:
            try:
                ranks_excl_1m = pd.Series(returns_excl_1m).rank(pct=True, ascending=False)
                for ticker, rank in ranks_excl_1m.items():
                    rs_ranks_by_ticker[ticker]['excl_1M'][date] = rank
            except Exception as e:
                logger.warning(f"날짜 {date}의 RS_Rank_excl_1M 계산 중 오류 - {e}")
        
        # RS_Rank_6M 계산
        if returns_6m:
            try:
                ranks_6m = pd.Series(returns_6m).rank(pct=True, ascending=False)
                for ticker, rank in ranks_6m.items():
                    rs_ranks_by_ticker[ticker]['6M'][date] = rank
            except Exception as e:
                logger.warning(f"날짜 {date}의 RS_Rank_6M 계산 중 오류 - {e}")
    
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

if __name__ == "__main__":
    calculate_rs()