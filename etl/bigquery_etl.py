from utils.constants import logger
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def from_gsc_to_bigquery_table(client: bigquery.Client, table_id: str, data_uri: str):
    """
    Upload data from GCS to BigQuery

    Parameters
    ----------
    client : google.cloud.bigquery.Client
        BigQuery client instance.
    table_id : str
        The full BigQuery table id
        Example: `project_id.dataset_name.table_name`
    data_uri : str
        The GCS URI of the uploaded file.
        Example: 'gs://stock-pipeline-dbt-cloud/raw/raw_test.csv'
    """
    try:
        logger.info(f"Starting BigQuery load job: {data_uri} → {table_id}")

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Date", "DATE"),
                bigquery.SchemaField("Ticker", "STRING"),
                bigquery.SchemaField("Open", "FLOAT64"),
                bigquery.SchemaField("High", "FLOAT64"),
                bigquery.SchemaField("Low", "FLOAT64"),
                bigquery.SchemaField("Close", "FLOAT64"),
                bigquery.SchemaField("Volume", "INT64"),
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        load_job = client.load_table_from_uri(data_uri, table_id, job_config=job_config)

        result = load_job.result()  # Wait for the job to complete
        logger.info(
            f"Load job finished. {result.output_rows} rows loaded into {table_id}."
        )
    except Exception as e:
        logger.error(f"Failed to load data from {data_uri} into {table_id}: {e}")
        raise


def create_dataset_if_not_exists(
    client: bigquery.Client,
    dataset_id: str,
    location: str = "US",
) -> bool:
    """
    Create a BigQuery dataset if it does not already exist.

    Parameters
    ----------
    client : bigquery.Client
        BigQuery client instance.
    dataset_id : str
        The full ID of the dataset in the form 'project.dataset'.
    location : str
        Location for the dataset (default is 'US').

    Returns
    -------
    bool
        True if the dataset was created or already exists, False otherwise.
    """
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset `{dataset_id}` already exists.")
        return True
    except NotFound:
        logger.info(f"Dataset `{dataset_id}` not found. Creating it.")

    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset `{dataset_id}` in location `{dataset.location}`.")
        return True
    except Exception as e:
        logger.error(f"Failed to create dataset `{dataset_id}`: {e}")
        return False
