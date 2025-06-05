from pathlib import Path
from utils.constants import SERVICE_ACCOUNT_FILE, BUCKET_NAME, GCS_RAW_DATA_PATH, logger
from etl.gcs_etl import initialize_gcs_client, upload_blob

def upload_to_gcs(**kwargs) -> str:
    """Upload local data to Google Cloud Storage."""
    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids='extract_stock_data_delta', key='file_name') \
                 or ti.xcom_pull(task_ids='extract_stock_data_full', key='file_name')
        blob_name = f"{GCS_RAW_DATA_PATH}/{Path(file_path).name}"

        # Initialize google cloud storage client
        gcs_client = initialize_gcs_client(SERVICE_ACCOUNT_FILE)
        # Upload the data to google cloud storage
        data_uri = upload_blob(gcs_client, BUCKET_NAME, file_path, blob_name)
        logger.info(f"File {file_path} successfully uploaded to GSC from airflow")
        
        # Push data_uri to Xcom
        ti.xcom_push(key='data_uri', value=data_uri)
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise

