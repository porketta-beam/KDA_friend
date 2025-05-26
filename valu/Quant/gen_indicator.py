#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from data_loader import load_data
from test_indicator_calculator import calculate_indicators


def process_ticker(ticker: str, data_dir: Path) -> tuple[str, bool]:
    """
    Load data for a ticker, calculate indicators, and save the result to a Feather file.
    Returns a tuple of (ticker, success_flag).
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
        print(f"Error processing {ticker}: {e}")
        return ticker, False


def load_tickers(ticker_file: Path) -> list[str]:
    """
    Read tickers from a CSV file with a 'Ticker' column.
    Raises KeyError if the column is missing.
    """
    df = pd.read_csv(ticker_file)
    if 'Ticker' not in df.columns:
        raise KeyError("Missing 'Ticker' column in tickers.csv")
    return df['Ticker'].astype(str).tolist()


def main():
    base_dir = Path(__file__).parent / "src_data"
    ticker_file = base_dir / "ticker_list" / "tickers.csv"

    try:
        tickers = load_tickers(ticker_file)
    except Exception as e:
        print(f"Failed to load tickers: {e}")
        return

    print(f"Loaded {len(tickers)} tickers")

    success_count = 0
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_ticker, t, base_dir): t for t in tickers}
        for future in as_completed(futures):
            ticker, success = future.result()
            status = "✔" if success else "✖"
            print(f"{ticker}: {status}")
            success_count += int(success)

    print(f"Successfully processed {success_count}/{len(tickers)} tickers")


if __name__ == "__main__":
    main()
