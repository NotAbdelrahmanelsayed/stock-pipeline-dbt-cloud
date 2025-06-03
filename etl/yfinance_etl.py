from utils.constants import logger
import yfinance as yf
import pandas as pd 
from typing import List, Optional



def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens a MultiIndex DataFrame from yfinance into long-form format,
    drop duplicates, drop null rows.

    Returns
    -------
    pd.DataFrame
        Flattened DataFrame with columns including 'Ticker' and other metrics.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df = df.stack(level=0).reset_index()
        df = df.rename(columns={'level_1': 'Ticker'})
    
    df = df.drop_duplicates()
    df = df.dropna(axis=0)

    return df

    
def download_stock_data(
    tickers: List[str], 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
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
            if not start_date or not end_date:
                raise ValueError("start_date and end_date must be provided if is_full_load is False.")
            logger.info(f"Downloading data for {tickers} from {start_date} to {end_date}")
            df = yf.download(tickers=tickers, start=start_date, end=end_date, group_by='ticker')
        
        return df
    except Exception as e:
        logger.error(f"Failed to download stock data {e}")
        raise

def save_data_locally(df: pd.DataFrame, save_path) -> None:
    """Saves the DataFrame as csv into the defined path"""
    df.to_csv(save_path, index=False)