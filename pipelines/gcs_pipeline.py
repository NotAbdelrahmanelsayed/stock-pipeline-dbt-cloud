from utils.constants import SERVICE_ACC_FILE, BUCKET_NAME, RAW_DATA_FILE_PATH, GSC_RAW_DATA_PATH, logger
from etl.gcs_etl import initialize_gcs_client, upload_blob

def upload_to_gcs():
    try:
        # Initialze google cloud storage client
        client = initialize_gcs_client(SERVICE_ACC_FILE)
        # Upload the data to google cloud storage
        data_uri = upload_blob(client, BUCKET_NAME, RAW_DATA_FILE_PATH, GSC_RAW_DATA_PATH)
        logger.info(f"File {GSC_RAW_DATA_PATH} successfully uploaded to GSC from airflow")
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise
