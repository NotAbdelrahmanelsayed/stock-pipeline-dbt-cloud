from utils.constants import SERVICE_ACC_FILE, BUCKET_NAME, RAW_DATA_FILE_PATH, GSC_RAW_DATA_PATH, TABLE_ID, logger
from etl.gcs_etl import initialize_gcs_client, upload_blob
from etl.bigquery_etl import initialize_bigquery_client, from_gsc_to_bigquery_table

def google_cloud_pipeline():
    """upload data to BigQuery

    local_data -> GCS -> BigQuery
    """
    try:
        # Initialze google cloud storage client
        gcs_client = initialize_gcs_client(SERVICE_ACC_FILE)
        # Upload the data to google cloud storage
        data_uri = upload_blob(gcs_client, BUCKET_NAME, RAW_DATA_FILE_PATH, GSC_RAW_DATA_PATH)
        logger.info(f"File {GSC_RAW_DATA_PATH} successfully uploaded to GSC from airflow")
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
    try:
        # Initialze BigQuery client
        bigquery_client = initialize_bigquery_client(SERVICE_ACC_FILE)
        # Upload the data from Google Cloud Storage to BigQuery
        from_gsc_to_bigquery_table(bigquery_client, TABLE_ID, data_uri)
        logger.info(f"File {GSC_RAW_DATA_PATH} successfully uploaded to {TABLE_ID} from airflow")
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
