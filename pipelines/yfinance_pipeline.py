from etl.yfinance_etl import download_stock_data, transform, save_data_locally
from utils.constants import TICKERS, logger, RAW_DATA_FILE_PATH
from utils.filename_utils import generate_filename
from typing import Optional, List
import pandas as pd 


def initial_download_stock_data():
    data = download_stock_data(TICKERS, is_full_load=True)
    df = transform(data)
    file_name = generate_filename()
    save_data_locally(df, file_name)
    logger.info(f"data saved successfully into {file_name}.")


def delta_download_stock_data(**kwargs):
    ti = kwargs['ti']
    max_date = ti.xcom_pull(task_ids='decide_load_type', key='max_date')

    data = download_stock_data(TICKERS, is_full_load=False, start_date=max_date)
    df = transform(data)

    file_name = generate_filename(max_date)
    save_data_locally(df, file_name)
    logger.info(f"data saved successfully into {file_name}.")
    
    # Push the file_name 
    ti.xcom_push(key='file_name', value=file_name)
