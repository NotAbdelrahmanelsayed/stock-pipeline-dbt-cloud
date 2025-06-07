from pathlib import Path
from google.oauth2 import service_account
from utils.constants import logger
from google.cloud import bigquery



def initialize_bigquery_client(service_account_path: Path) -> bigquery.Client:
    """
    Initialize a Google BigQuery client.

    Parameters
    ----------
    service_account_path : Union[str, Path]
        Path to the service account JSON file.

    Returns
    -------
    google.cloud.bigquery.Client
        BigQuery client instance.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info("BigQuery Credentials Initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Couldn't create a BigQuery client: {e}")
        raise


def from_gsc_to_bigquery_table(
    client: bigquery.Client,
    table_id: str,
    data_uri: str):
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
                bigquery.SchemaField("Date","DATE"),
                bigquery.SchemaField("Ticker","STRING"),
                bigquery.SchemaField("Open","FLOAT64"),
                bigquery.SchemaField("High","FLOAT64"),
                bigquery.SchemaField("Low","FLOAT64"),
                bigquery.SchemaField("Close","FLOAT64"),
                bigquery.SchemaField("Volume","INT64")],
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV
                )
        load_job = client.load_table_from_uri(
            data_uri,
            table_id,
            job_config=job_config)

        result = load_job.result() # Wait for the job to complete
        logger.info(f"Load job finished. {result.output_rows} rows loaded into {table_id}.")
    except Exception as e:
        logger.error(f"Failed to load data from {data_uri} into {table_id}: {e}")
        raise