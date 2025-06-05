from utils.constants import logger
import yfinance as yf
import pandas as pd 
from typing import List, Optional
from datetime import date


def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens a MultiIndex DataFrame from yfinance into long-form format,
    drops duplicates, nulls, and malformed rows.

    Returns
    -------
    pd.DataFrame
    """
    if isinstance(df.columns, pd.MultiIndex):
        df = df.stack(level=0).reset_index()
        df = df.rename(columns={'level_1': 'Ticker'})

    df = df.drop_duplicates()
    df = df.dropna(axis=0)

    # Filter out any rows that don't contain expected columns
    expected_cols = {'Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume'}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        logger.warning(f"Missing expected columns: {missing_cols}. Output may be malformed.")
    
    # Remove rows missing key columns (if they exist)
    df = df[df.columns.intersection(expected_cols)]

    # Final cleanup: ensure no partial rows survive
    df = df.dropna(subset=expected_cols.intersection(df.columns))

    return df


    
def download_stock_data(
    tickers: List[str], 
    start_date: Optional[str] = None, 
    is_full_load: Optional[bool] = True) -> pd.DataFrame:
    """
    Download historical stock data using yfinance

    Parameters
    ----------
    tickers : List[str]
        List of stock ticker symbols
    start_date : Optional[str], optional
        Start date in 'YYYY-MM-DD' format, by default None
    end_date : Optional[str], optional
        End date in 'YYYY-MM-DD' format, by default None
    is_full_load : Optional[bool], optional
        Downloading the last 10 years if True -- else from start to end date, by default True

    Returns
    -------
    pd.DataFrame
    """
    try:
        if is_full_load:
            logger.info(f"Downloading last 10 years data for {tickers}")
            df = yf.download(tickers=tickers, period='10y', group_by='ticker')
        else:
            if not start_date:
                raise ValueError("start_date and end_date must be provided if is_full_load is False.")
            logger.info(f"Downloading data for {tickers} from {start_date} to {date.today()}")
            df = yf.download(tickers=tickers, start=start_date, group_by='ticker')
        
        return df
    except Exception as e:
        logger.error(f"Failed to download stock data {e}")
        raise

def save_data_locally(df: pd.DataFrame, save_path) -> None:
    """Saves the DataFrame as csv into the defined path"""
    df.to_csv(save_path, index=False)