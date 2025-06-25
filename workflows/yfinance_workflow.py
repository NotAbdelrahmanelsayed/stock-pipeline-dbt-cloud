from etl.yfinance_etl import (
    download_stock_data,
    transform_stock_data,
    save_data_locally,
)
from utils.constants import TICKERS, logger, get_local_file_path


def extract_full_stock_prices(ti):
    raw_data = download_stock_data(TICKERS, is_full_load=True)
    transformed_data = transform_stock_data(raw_data)
    local_path = get_local_file_path()
    save_data_locally(transformed_data, local_path)
    logger.info(f"data saved successfully into {local_path}.")

    # Push the file_path
    ti.xcom_push(key="file_name", value=local_path)


def extract_incremental_stock_prices(ti):
    max_date = ti.xcom_pull(task_ids="decide_load_type", key="max_date")

    raw_data = download_stock_data(TICKERS, is_full_load=False, start_date=max_date)
    transformed_data = transform_stock_data(raw_data)

    local_path = get_local_file_path(max_date)
    save_data_locally(transformed_data, local_path)
    logger.info(f"data saved successfully into {local_path}.")

    # Push the file_path
    ti.xcom_push(key="file_name", value=local_path)
