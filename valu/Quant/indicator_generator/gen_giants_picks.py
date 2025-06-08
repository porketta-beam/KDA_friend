import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings

from .calc_giants_strategies import (
    filter_benjamin_graham,
    filter_ken_fisher,
    filter_peter_lynch,
    filter_jesse_livermore,
    filter_mark_minervini,
    filter_william_oneil
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('generate_giants_picks.log')]
)
logger = logging.getLogger(__name__)

def load_all_data(input_path: Path) -> pd.DataFrame:
    """
    모든 종목의 데이터를 로드하여 MultiIndex DataFrame으로 반환.
    
    Args:
        input_path: 입력 feather 파일 디렉토리
    Returns:
        pd.DataFrame: MultiIndex (Date, Ticker) DataFrame
    """
    ticker_files = list(input_path.glob("*_indicator.feather"))
    all_data = []
    
    for file in ticker_files:
        ticker = file.stem.replace("_indicator", "")
        try:
            df = pd.read_feather(file)
            if df.empty:
                logger.warning(f"{ticker}의 데이터가 비어 있습니다.")
                continue
            df['Date'] = pd.to_datetime(df['Date'])
            df['Ticker'] = ticker
            df.set_index(['Date', 'Ticker'], inplace=True)
            all_data.append(df)
        except Exception as e:
            logger.error(f"{ticker} 데이터 로드 중 오류: {str(e)}")
            continue
    
    if not all_data:
        logger.error("로드된 데이터가 없습니다.")
        return pd.DataFrame()
    
    return pd.concat(all_data)

def process_ticker(ticker: str, all_data: pd.DataFrame, output_path: Path) -> tuple[str, bool]:
    """
    단일 종목의 데이터를 처리하여 거장 필터링 결과를 생성.
    
    Args:
        ticker: 종목 코드
        all_data: MultiIndex (Date, Ticker) DataFrame
        output_path: 출력 feather 파일 디렉토리
    Returns:
        tuple: (ticker, 성공여부)
    """
    try:
        if ticker not in all_data.index.get_level_values('Ticker'):
            logger.warning(f"{ticker}의 데이터가 없습니다.")
            return ticker, False
            
        df = all_data.xs(ticker, level='Ticker')
        
        # 결과 DataFrame 초기화
        result_cols = [
            'Date', 'Ticker',
            'Graham_Pick', 'Graham_Score',
            'Fisher_Pick', 'Fisher_Score',
            'Lynch_Pick', 'Lynch_Score',
            'Livermore_Pick',
            'Minervini_Pick',
            'ONeil_Pick'
        ]
        result = pd.DataFrame(index=df.index, columns=result_cols)
        result['Date'] = df.index
        result['Ticker'] = ticker
        
        # 각 거장 필터 적용
        try:
            graham_pick, graham_score = filter_benjamin_graham(all_data, ticker)
            result['Graham_Pick'] = graham_pick
            result['Graham_Score'] = graham_score
        except Exception as e:
            logger.error(f"{ticker} Graham 필터 처리 중 오류: {str(e)}")
            result['Graham_Pick'] = False
            result['Graham_Score'] = 0.0
            
        try:
            fisher_pick, fisher_score = filter_ken_fisher(all_data, ticker)
            result['Fisher_Pick'] = fisher_pick
            result['Fisher_Score'] = fisher_score
        except Exception as e:
            logger.error(f"{ticker} Fisher 필터 처리 중 오류: {str(e)}")
            result['Fisher_Pick'] = False
            result['Fisher_Score'] = 0.0
            
        try:
            lynch_pick, lynch_score = filter_peter_lynch(all_data, ticker)
            result['Lynch_Pick'] = lynch_pick
            result['Lynch_Score'] = lynch_score
        except Exception as e:
            logger.error(f"{ticker} Lynch 필터 처리 중 오류: {str(e)}")
            result['Lynch_Pick'] = False
            result['Lynch_Score'] = 0.0
            
        try:
            result['Livermore_Pick'] = filter_jesse_livermore(df)
        except Exception as e:
            logger.error(f"{ticker} Livermore 필터 처리 중 오류: {str(e)}")
            result['Livermore_Pick'] = False
            
        try:
            result['Minervini_Pick'] = filter_mark_minervini(df)
        except Exception as e:
            logger.error(f"{ticker} Minervini 필터 처리 중 오류: {str(e)}")
            result['Minervini_Pick'] = False
            
        try:
            result['ONeil_Pick'] = filter_william_oneil(df)
        except Exception as e:
            logger.error(f"{ticker} O'Neil 필터 처리 중 오류: {str(e)}")
            result['ONeil_Pick'] = False
        
        # 출력 디렉토리 생성
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 결과 저장
        output_file = output_path / f"{ticker}_giants_picks.feather"
        result.reset_index(drop=True).to_feather(output_file)
        logger.info(f"{ticker} 거장 필터링 완료")
        return ticker, True
        
    except Exception as e:
        logger.error(f"{ticker} 처리 중 오류: {str(e)}")
        return ticker, False

def main():
    """
    모든 종목에 대해 거장 필터를 수행.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(base_dir, os.pardir))

    input_path = Path(os.path.join(parent_dir, 'src_data', 'indicators'))
    output_path = Path(os.path.join(parent_dir, 'src_data', 'giants_pick'))
    
    # 모든 종목 데이터 로드
    all_data = load_all_data(input_path)
    if all_data.empty:
        logger.warning("지표 파일이 없습니다.")
        return
    
    tickers = all_data.index.get_level_values('Ticker').unique()
    
    # 병렬 처리로 각 종목 처리
    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_ticker, ticker, all_data, output_path)
            for ticker in tickers
        ]
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "O" if success else "X"
            logger.info(f"{ticker}: {status}")
            success_count += int(success)
    
    logger.info(f"{success_count}/{len(tickers)}개의 티커를 성공적으로 처리했습니다.")

if __name__ == "__main__":
    warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
    main()