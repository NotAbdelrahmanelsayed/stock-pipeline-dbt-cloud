from etl.yfinance_etl import download_stock_data, flatten_multi_index, save_data_locally
from utils.constants import TICKERS, logger, RAW_DATA_FILE_PATH
from typing import Optional, List
import pandas as pd 


def initial_download_stock_data():
    data = download_stock_data(TICKERS, is_full_load=True)
    df = flatten_multi_index(data)
    save_data_locally(df, RAW_DATA_FILE_PATH)
    logger.info(df.head())
    
# def incremental_download_stock_data(
# tickers: List[str], 
# start_date: Optional[str] = None, 
# end_date: Optional[str] = None, 
# is_full_load: Optional[bool] = True):
#     pass