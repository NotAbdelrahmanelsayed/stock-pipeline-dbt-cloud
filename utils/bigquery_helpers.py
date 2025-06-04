from google.cloud import bigquery
from typing import Union


def get_last_loaded_date(client: bigquery.Client, table_id) -> Union[str, None]:
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
    QUERY = f"SELECT MAX(Date) FROM {table_id}"
    try:
        results = client.query_and_wait(QUERY)
        max_date = None 
        for row in results:
            max_date = row[0]
        return max_date
    except Exception as e:
        "max_date is not"
        return None