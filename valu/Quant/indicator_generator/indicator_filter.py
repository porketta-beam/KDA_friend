import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed


def filter_benjamin_graham(df: pd.DataFrame) -> pd.Series:
    """
    Benjamin Graham의 기준으로 주식을 필터링.
    """
    return (
        (df['Avg_Daily_Volume_252'] >= 200000) &
        (df['Market_Cap'] >= 100000000000) &
        (df['PE_Ratio'] <= 15) &
        (df['PB_Ratio'] <= 1.5) &
        (df['Current_Ratio'] >= 2)
    )


def filter_peter_lynch(df: pd.DataFrame) -> pd.Series:
    """
    Peter Lynch의 기준으로 주식을 필터링.
    """
    return (
        (df['Avg_Daily_Volume_252'] >= 200000) &
        (df['Market_Cap'] >= 100000000000) &
        (df['PEG_Ratio'] <= 1) &
        (df['Operating_Margin'] >= 15)
    )


def filter_william_oneil(df: pd.DataFrame) -> pd.Series:
    """
    William O'Neil의 기준으로 주식을 필터링 (RS 포함).
    """
    return (
        (df['Avg_Daily_Volume_252'] >= 200000) &
        (df['Market_Cap'] >= 100000000000) &
        (df['EPS_Q_YoY'] >= 25) &
        (df['Above_200_MA'] == True) &
        (df['RS_Rank'] <= 0.2)  # 상위 20% (가정: RS_Rank는 0~1 사이)
    )


def process_ticker(ticker: str, base_dir: Path) -> tuple[str, bool]:
    """
    단일 티커에 대해 모든 날짜에 대해 거장별 필터링을 수행하고 결과를 저장.

    Args:
        ticker (str): 주식 ticker 심볼
        base_dir (Path): src_data 디렉토리 경로

    Returns:
        tuple: (ticker, 성공여부)
    """
    input_path = base_dir / "indicators" / f"{ticker}_indicator.feather"
    output_dir = base_dir / "giants_pick"
    output_dir.mkdir(exist_ok=True)
    
    try:
        # 입력 데이터 읽기
        df = pd.read_feather(input_path)
        df['Date'] = pd.to_datetime(df['Date'])
        
        if df.empty:
            print(f"오류: {ticker} 데이터가 비어 있습니다.")
            return ticker, False
        
        # 거장별 필터링 함수
        giants_filters = {
            'Benjamin_Graham': filter_benjamin_graham,
            'Peter_Lynch': filter_peter_lynch,
            'William_ONeil': filter_william_oneil
        }
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame({
            'Ticker': [ticker] * len(df),
            'Date': df['Date']
        })
        
        # 각 거장의 선정 여부 추가
        for giant, filter_func in giants_filters.items():
            result_df[f'Is_{giant}_Pick'] = filter_func(df)
        
        # 결과 저장
        output_path = output_dir / f"{ticker}_giants_pick.feather"
        try:
            result_df.to_feather(output_path)
            print(f"결과가 {output_path}에 저장되었습니다 (티커: {ticker}).")
            return ticker, True
        except Exception as e:
            print(f"오류: {output_path} 저장 중 오류 발생 (티커: {ticker}) - {e}")
            return ticker, False
    
    except FileNotFoundError:
        print(f"오류: {input_path} 파일을 찾을 수 없습니다.")
        return ticker, False
    except Exception as e:
        print(f"오류: {ticker} 처리 중 오류 - {e}")
        return ticker, False


def filter_giants_pick():
    """
    src_data/indicators의 모든 티커에 대해 모든 날짜에 대해 거장별 필터링을 수행.
    """
    # 경로 설정
    base_dir = Path(__file__).parent.parent / "src_data"
    indicators_dir = base_dir / "indicators"
    
    # 티커 목록 가져오기
    tickers = [f.stem.replace('_indicator', '') for f in indicators_dir.glob('*_indicator.feather')]
    if not tickers:
        print("오류: src_data/indicators 폴더에 indicator 파일이 없습니다.")
        return
    
    print(f"{len(tickers)}개의 티커를 처리합니다.")
    
    # 병렬 처리
    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_ticker, ticker, base_dir)
            for ticker in tickers
        ]
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "✔" if success else "✖"
            print(f"{ticker}: {status}")
            success_count += int(success)
    
    print(f"{success_count}/{len(tickers)}개의 티커를 성공적으로 처리했습니다.")


if __name__ == "__main__":
    filter_giants_pick()