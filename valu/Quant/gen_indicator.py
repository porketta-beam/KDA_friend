import pandas as pd
from pathlib import Path
from multiprocessing import Pool, cpu_count
from data_loader import load_data
from indicator_calculator import calculate_indicators

def process_ticker(ticker_data):
    ticker, data_path = ticker_data
    try:
        data = load_data(ticker, data_path)
        result = calculate_indicators(data)
        if result is not None:
            output_path = Path(data_path) / "indicators"
            output_path.mkdir(parents=True, exist_ok=True)
            result.to_feather(output_path / f"{ticker}_indicator.feather")
            print(f"Processed {ticker}")
            return ticker, True
        else:
            return ticker, False
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return ticker, False

if __name__ == "__main__":
    # data_path를 현재 실행 중인 .py 파일의 위치를 기준으로 설정
    data_path = Path(__file__).parent / "src_data"
    
    # src_data/ticker_list/tickers.csv 파일에서 티커 리스트 로드
    ticker_file_path = data_path / "ticker_list" / "tickers.csv"
    try:
        ticker_df = pd.read_csv(ticker_file_path)
        if 'Ticker' not in ticker_df.columns:
            raise ValueError("Error: 'Ticker' column not found in tickers.csv")
        tickers = ticker_df['Ticker'].tolist()
        print(f"Loaded {len(tickers)} tickers from {ticker_file_path}")
    except FileNotFoundError:
        print(f"Error: {ticker_file_path} not found")
        tickers = []
    except Exception as e:
        print(f"Error loading tickers.csv: {e}")
        tickers = []

    if not tickers:
        print("No tickers to process. Exiting.")
        exit(1)

    num_cores = cpu_count()

    with Pool(processes=num_cores) as pool:
        results = pool.map(process_ticker, [(ticker, data_path) for ticker in tickers])
    
    success = sum(1 for _, status in results if status)
    print(f"Successfully processed {success}/{len(tickers)} tickers")