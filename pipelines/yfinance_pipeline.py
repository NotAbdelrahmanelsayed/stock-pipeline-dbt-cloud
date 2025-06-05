from etl.yfinance_etl import download_stock_data, transform, save_data_locally
from utils.constants import TICKERS, logger, get_local_file_path



def initial_download_stock_data():
    data = download_stock_data(TICKERS, is_full_load=True)
    df = transform(data)
    file_path = get_local_file_path()
    save_data_locally(df, file_path)
    logger.info(f"data saved successfully into {file_path}.")


def delta_download_stock_data(**kwargs):
    ti = kwargs['ti']
    max_date = ti.xcom_pull(task_ids='decide_load_type', key='max_date')

    data = download_stock_data(TICKERS, is_full_load=False, start_date=max_date)
    df = transform(data)

    file_path = get_local_file_path(max_date)
    save_data_locally(df, file_path)
    logger.info(f"data saved successfully into {file_path}.")

    # Push the file_path
    ti.xcom_push(key='file_name', value=file_path)
