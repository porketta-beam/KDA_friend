import pandas as pd
from pathlib import Path

def check_sorted(df, name):
    if df.empty:
        print(f"Warning: {name} is empty. Returning empty DataFrame.")
        return df
    if not df.index.is_monotonic_increasing:
        print(f"Warning: {name} is not sorted in ascending order. Sorting now.")
        df = df.sort_index()
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        print(f"Warning: {name} index is not datetime. Converting now.")
        if 'Date' in df.columns:
            df = df.set_index('Date')
        elif 'QuarterEnd' in df.columns:
            df = df.set_index('QuarterEnd')
        df.index = pd.to_datetime(df.index, errors='coerce')
    return df

def load_data(ticker, data_path):
    base_path = Path(data_path)
    stock_data_path = base_path / "stock_data"
    financials_path = base_path / "financials"
    quarterly_financials_path = base_path / "quarterly_financials"
    balance_sheet_path = base_path / "balance_sheet"
    quarterly_balance_sheet_path = base_path / "quarterly_balance_sheet"
    cash_flow_path = base_path / "cash_flow"
    quarterly_cash_flow_path = base_path / "quarterly_cash_flow"

    data = {}
    try:
        data['history'] = check_sorted(
            pd.read_feather(stock_data_path / f"{ticker}_data.feather").set_index('Date'), 'history')
        print(f"Successfully loaded {ticker} history with index: {data['history'].index[:5]}")

        financials_df = pd.read_feather(financials_path / f"{ticker}_financials.feather")
        financials_df = financials_df.set_index('Date' if 'Date' in financials_df.columns else financials_df.index)
        data['financials'] = check_sorted(financials_df, 'financials')

        quarterly_financials_df = pd.read_feather(quarterly_financials_path / f"{ticker}_quarterly_financials.feather")
        quarterly_financials_df = quarterly_financials_df.set_index(
            'QuarterEnd' if 'QuarterEnd' in quarterly_financials_df.columns else 'Date')
        data['quarterly_financials'] = check_sorted(quarterly_financials_df, 'quarterly_financials')

        balance_sheet_df = pd.read_feather(balance_sheet_path / f"{ticker}_balance_sheet.feather")
        balance_sheet_df = balance_sheet_df.set_index('Date' if 'Date' in balance_sheet_df.columns else balance_sheet_df.index)
        data['balance_sheet'] = check_sorted(balance_sheet_df, 'balance_sheet')

        quarterly_balance_sheet_df = pd.read_feather(quarterly_balance_sheet_path / f"{ticker}_quarterly_balance_sheet.feather")
        quarterly_balance_sheet_df = quarterly_balance_sheet_df.set_index(
            'QuarterEnd' if 'QuarterEnd' in quarterly_balance_sheet_df.columns else 'Date')
        data['quarterly_balance_sheet'] = check_sorted(quarterly_balance_sheet_df, 'quarterly_balance_sheet')

        cash_flow_df = pd.read_feather(cash_flow_path / f"{ticker}_cash_flow.feather")
        cash_flow_df = cash_flow_df.set_index('Date' if 'Date' in cash_flow_df.columns else cash_flow_df.index)
        data['cash_flow'] = check_sorted(cash_flow_df, 'cash_flow')

        quarterly_cash_flow_df = pd.read_feather(quarterly_cash_flow_path / f"{ticker}_quarterly_cash_flow.feather")
        quarterly_cash_flow_df = quarterly_cash_flow_df.set_index(
            'QuarterEnd' if 'QuarterEnd' in quarterly_cash_flow_df.columns else 'Date')
        data['quarterly_cash_flow'] = check_sorted(quarterly_cash_flow_df, 'quarterly_cash_flow')
    except KeyError as e:
        print(f"Error: {ticker} missing required column (e.g., 'Date' or 'QuarterEnd'): {e}")
        return None
    except FileNotFoundError as e:
        print(f"Error: File not found for {ticker}: {e}")
        return None
    except Exception as e:
        print(f"Error loading data for {ticker}: {e}")
        return None
    return data