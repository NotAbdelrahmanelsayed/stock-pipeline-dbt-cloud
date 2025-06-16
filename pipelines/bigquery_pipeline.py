from utils.cloud_clients import initialize_bigquery_client
from etl.bigquery_etl import (
    from_gsc_to_bigquery_table,
    create_dataset_if_not_exists,
)
from utils.constants import (
    SERVICE_ACCOUNT_FILE,
    GCS_RAW_DATA_PATH,
    TABLE_ID,
    DATASET_ID,
    logger,
)


def upload_to_bigquery(ti):
    try:
        # Pull data_uri from Xcom
        data_uri = ti.xcom_pull(task_ids="upload_stock_data_to_gcs", key="data_uri")

        # Initialize BigQuery client
        bigquery_client = initialize_bigquery_client(SERVICE_ACCOUNT_FILE)

        # Create dataset if not exist
        create_dataset_if_not_exists(bigquery_client, DATASET_ID)
        # Upload the data from Google Cloud Storage to BigQuery
        from_gsc_to_bigquery_table(bigquery_client, TABLE_ID, data_uri)
        logger.info(
            f"File {GCS_RAW_DATA_PATH} successfully uploaded to {TABLE_ID} from airflow"
        )
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
