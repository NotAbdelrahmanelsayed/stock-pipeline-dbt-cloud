from etl.bigquery_etl import initialize_bigquery_client, from_gsc_to_bigquery_table
from utils.constants import SERVICE_ACCOUNT_FILE, GCS_RAW_DATA_PATH, TABLE_ID, logger

# Create_data_set_if_not_exist: function


def upload_to_bigquery(**kwargs):
    try:
        # Pull data_uri from Xcom
        ti = kwargs["ti"]
        data_uri = ti.xcom_pull(task_ids="upload_to_gcs", key="data_uri")

        # Initialize BigQuery client
        bigquery_client = initialize_bigquery_client(SERVICE_ACCOUNT_FILE)
        # Upload the data from Google Cloud Storage to BigQuery
        from_gsc_to_bigquery_table(bigquery_client, TABLE_ID, data_uri)
        logger.info(
            f"File {GCS_RAW_DATA_PATH} successfully uploaded to {TABLE_ID} from airflow"
        )
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
