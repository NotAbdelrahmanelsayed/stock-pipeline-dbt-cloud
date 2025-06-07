from google.cloud import bigquery
from typing import Union
from utils.constants import logger
from datetime import timedelta
import pandas as pd 



def get_last_loaded_date(client: bigquery.Client, table_id, **kwargs) -> Union[str, None]:
    """Get the maximum date in BigQuery table
    Parameters
    ----------
    client : google.cloud.bigquery.Client
        BigQuery client instance.
    table_id : str
        The full BigQuery table id 
        Example: `project_id.dataset_name.table_name`
    Returns
    -------
    Union[str, None]
        date as a string or None
    """
    ""
    ti = kwargs['ti']
    QUERY = f"SELECT MAX(Date) FROM {table_id}"
    try:
        logger.info(f"Connecting to {table_id}....")
        results = client.query_and_wait(QUERY)
        max_date = None 
        for row in results:
            max_date = row[0]
        if max_date:
            logger.info(f"Last date from {table_id}: {max_date}\n")
            logger.info(f"Delta load activated....")
            ti.xcom_push(key='max_date', value=max_date+timedelta(days=1))
        return "extract_stock_data_delta" 
    except Exception as e:
        logger.info(f"Table {table_id} Not found or empty")
        logger.info(f"Initial load activated....")
        return "extract_stock_data_full"
    
def check_data_quality(**kwargs) -> bool:
    """Check the staged data file if it's empty 
        before uploading it to GCS or BigQuery to ensure data quality.

    Returns
    -------
    bool
        True: There is data inside
        False: The data file is empty
    """
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='extract_stock_data_delta', key='file_name') \
                 or ti.xcom_pull(task_ids='extract_stock_data_full', key='file_name')
    
    df = pd.read_csv(file_path)
    
    return len(df) > 2