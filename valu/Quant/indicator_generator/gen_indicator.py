#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from .calculator_indicator import calculate_indicators
from .data_loader import load_data


def process_ticker(ticker: str, data_dir: Path) -> tuple[str, bool]:
    """
    ticker의 데이터를 불러오고, 지표를 계산하여 Feather 파일로 저장.
    (ticker, 성공여부) 튜플 반환.
    """
    try:
        df = load_data(ticker, data_dir)
        indicators = calculate_indicators(df)
        if indicators is None:
            return ticker, False

        out_dir = data_dir / "indicators"
        out_dir.mkdir(exist_ok=True)
        indicators.to_feather(out_dir / f"{ticker}_indicator.feather")
        return ticker, True

    except Exception as e:
        print(f"{ticker} 처리 중 오류: {e}")
        return ticker, False


def load_tickers(ticker_file: Path) -> list[str]:
    """
    'Ticker' 열이 있는 CSV 파일에서 ticker 목록을 읽음.
    'Ticker' 열이 없으면 KeyError 발생.
    """
    df = pd.read_csv(ticker_file)
    if 'Ticker' not in df.columns:
        raise KeyError("'Ticker' 열이 tickers.csv에 없습니다.")
    return df['Ticker'].astype(str).tolist()


def main():
    base_dir = Path(__file__).parent.parent / "src_data"
    ticker_file = base_dir / "ticker_list" / "tickers.csv"

    try:
        tickers = load_tickers(ticker_file)
    except Exception as e:
        print(f"ticker 목록 로드 실패: {e}")
        return

    print(f"{len(tickers)}개의 ticker를 로드했습니다.")

    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_ticker, t, base_dir): t for t in tickers}
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "O" if success else "X"
            print(f"{ticker}: {status}")
            success_count += int(success)

    print(f"{success_count}/{len(tickers)}개의 ticker를 성공적으로 처리했습니다.")


if __name__ == "__main__":
    main()