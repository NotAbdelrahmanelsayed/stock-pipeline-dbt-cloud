from etl.yfinance_etl import download_stock_data, transform, save_data_locally
from utils.constants import TICKERS, logger, RAW_DATA_FILE_PATH
from typing import Optional, List
import pandas as pd 


def initial_download_stock_data():
    data = download_stock_data(TICKERS, is_full_load=True)
    df = transform(data)
    save_data_locally(df, RAW_DATA_FILE_PATH)
    logger.info(f"data saved successfully into {RAW_DATA_FILE_PATH}.")


def delta_download_stock_data(**kwargs):
    ti = kwargs['ti']
    max_date = ti.xcom_pull(task_ids='decide_load_type', key='max_date')

    data = download_stock_data(TICKERS, is_full_load=False, start_date=max_date)
    df = transform(data)
#################################### THIS FUNCTION SHOULD USE GENERATE FILE_NAME ####################################
    save_data_locally(df, RAW_DATA_FILE_PATH)
    logger.info(f"data saved successfully into {RAW_DATA_FILE_PATH}.")