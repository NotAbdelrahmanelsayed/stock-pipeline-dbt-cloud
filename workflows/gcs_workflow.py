from pathlib import Path
from utils.constants import (
    SERVICE_ACCOUNT_FILE,
    BUCKET_NAME,
    GCS_RAW_DATA_PATH,
    logger,
)
from etl.gcs_etl import upload_blob, create_bucket_if_not_exists
from utils.cloud_clients import initialize_gcs_client


def stage_to_gcs(ti) -> str:
    """Upload local data to Google Cloud Storage."""
    try:
        # Get the blob name
        local_path = ti.xcom_pull(
            task_ids="extract_stock_data_delta", key="file_name"
        ) or ti.xcom_pull(task_ids="extract_stock_data_full", key="file_name")
        blob_name = f"{GCS_RAW_DATA_PATH}/{Path(local_path).name}"

        # Initialize google cloud storage client
        gcs_client = initialize_gcs_client(SERVICE_ACCOUNT_FILE)

        # Create GCS bucket if not exist
        create_bucket_if_not_exists(gcs_client, BUCKET_NAME)

        # Upload the data to google cloud storage
        data_uri = upload_blob(gcs_client, BUCKET_NAME, local_path, blob_name)
        logger.info(f"File {local_path} successfully uploaded to GSC from airflow")

        # Push data_uri to Xcom
        ti.xcom_push(key="data_uri", value=data_uri)
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
